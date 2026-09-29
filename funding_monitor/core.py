"""Single-event-loop state owner. No network or database I/O in this module."""
from collections import defaultdict
from dataclasses import asdict
import math
import time
from engine import QuantitativeEngine, average_fill
from .alerts import Alerts
from .models import Funding, normalize_book, record, route_dict


class Monitor:
    def __init__(self, config, publish=lambda event: None):
        self.config = config
        self.publish = publish
        self.routes = {}
        self.instruments = {}
        self.routes_by_instrument = defaultdict(set)
        self.books = {}
        self.funding = {}
        self.engines = {}
        self.latest = {}
        self.invalid_events = 0
        self.alerts = Alerts(config.alert_cooldown_seconds, publish)

    def set_routes(self, routes, wall=None):
        # Called on a subscription generation change; warm-up starts again.
        self.routes = {r.key: r for r in routes}
        self.instruments = {i.key: i for r in routes for i in (r.spot, r.perp)}
        self.routes_by_instrument = defaultdict(set)
        self.engines = {}
        for r in routes:
            for i in (r.spot, r.perp):
                self.routes_by_instrument[i.key].add(r.key)
            sf = self.config.fees.get(f"{r.spot.venue}:spot")
            pf = self.config.fees.get(f"{r.perp.venue}:perp")
            if sf is not None and pf is not None:
                self.engines[r.key] = QuantitativeEngine(sf, pf, window_size=self.config.window_size)
        self.books.clear()
        self.funding.clear()
        self.latest.clear()
        self.alerts.prune(self.routes)
        self.publish(record("universe", "active", {"routes": [route_dict(r) for r in routes]}, wall))

    def ingest(self, event, now=None, wall=None):
        now = time.monotonic() if now is None else now
        wall = time.time() if wall is None else wall
        key, kind, payload = event["key"], event["kind"], event["payload"]
        if key not in self.instruments:
            return
        self.publish(event)  # Deliberate fan-out; persistence never consumes state updates.
        if kind == "invalidate":
            channel = payload.get("channel", "all")
            if channel in {"book", "all"}:
                self.books.pop(key, None)
            if channel in {"funding", "all"}:
                self.funding.pop(key, None)
            for route_key in self.routes_by_instrument[key]:
                if route_key in self.engines:
                    self.engines[route_key].basis_history.clear()
            return
        try:
            timestamp = float(event["ts"])
            if not math.isfinite(timestamp) or timestamp > wall + self.config.max_book_skew_seconds:
                raise ValueError("Invalid receive timestamp")
            age = max(0.0, wall - timestamp)
            received = now - age
            if kind == "book":
                book = normalize_book(payload, self.instruments[key], received, event["ts"])
                previous = self.books.get(key)
                if previous and previous.exchange_ms is not None and book.exchange_ms is not None:
                    if book.exchange_ms < previous.exchange_ms:
                        return
                self.books[key] = book
            elif kind == "funding":
                rate, interval = float(payload["rate"]), float(payload["interval_hours"])
                next_ms = payload.get("next_ms")
                next_ms = None if next_ms is None else float(next_ms)
                if not math.isfinite(rate) or not math.isfinite(interval) or interval <= 0:
                    raise ValueError("Invalid funding rate or interval")
                if next_ms is not None and not math.isfinite(next_ms):
                    raise ValueError("Invalid next funding timestamp")
                self.funding[key] = Funding(rate, interval, received, next_ms,
                                            payload.get("source", "exchange_current_estimate"))
        except (KeyError, IndexError, TypeError, ValueError, OverflowError):
            self.invalid_events += 1
            if kind == "book":
                self.books.pop(key, None)
            elif kind == "funding":
                self.funding.pop(key, None)
            for route_key in self.routes_by_instrument[key]:
                if route_key in self.engines:
                    self.engines[route_key].basis_history.clear()

    def _book_problem(self, book, now, wall):
        cfg = self.config
        if book is None:
            return "missing_book"
        if now - book.received > cfg.max_book_age_seconds:
            return "stale_book"
        if book.exchange_ms is not None:
            age = wall - book.exchange_ms / 1000
            if age > cfg.max_book_age_seconds or age < -cfg.max_book_skew_seconds:
                return "exchange_clock_or_stale_book"
        return None

    def calculate(self, route, now, wall):
        cfg = self.config
        result = {"base": route.spot.base, "bucket": route.bucket, "status": "ready",
                  "signal_active": False, "proxy": route.proxy,
                  "spot_venue": route.spot.venue, "perp_venue": route.perp.venue,
                  "quote_conversion": "configured_constant", "notional_usd": cfg.trade_notional_usd}
        spot, perp = self.books.get(route.spot.key), self.books.get(route.perp.key)
        engine = self.engines.get(route.key)
        problem = self._book_problem(spot, now, wall) or self._book_problem(perp, now, wall)
        if not problem and abs(spot.received - perp.received) > cfg.max_book_skew_seconds:
            problem = "book_time_skew"
        if not problem and spot.exchange_ms is not None and perp.exchange_ms is not None:
            if abs(spot.exchange_ms - perp.exchange_ms) / 1000 > cfg.max_book_skew_seconds:
                problem = "exchange_time_skew"
        if problem or engine is None:
            if engine:
                engine.basis_history.clear()
            result["status"] = problem or "missing_fees"
            return result
        raw = engine.calculate_basis_spread(perp.mid, spot.mid)
        smoothed = engine.get_smoothed_basis(raw)
        quantity = cfg.trade_notional_usd / spot.asks[0][0]
        buy = average_fill(spot.asks, quantity)
        sell = average_fill(perp.bids, quantity)
        result.update(mid_basis_bps=raw, smoothed_basis_bps=smoothed,
                      top_entry_basis_bps=engine.calculate_basis_spread(perp.bids[0][0], spot.asks[0][0]),
                      spot_age_seconds=now - spot.received, perp_age_seconds=now - perp.received,
                      samples=len(engine.basis_history), base_quantity=quantity)
        if buy is None or sell is None:
            result["status"] = "insufficient_depth"
            return result
        entry = engine.calculate_basis_spread(sell, buy)
        result.update(entry_basis_bps=entry, spot_fill_usd=buy, perp_fill_usd=sell)
        f = self.funding.get(route.perp.key)
        if f is None or now - f.received > cfg.max_funding_age_seconds:
            result["status"] = "missing_funding" if f is None else "stale_funding"
            return result
        if f.next_ms is not None and f.next_ms <= wall * 1000:
            result["status"] = "funding_settlement_pending_refresh"
            return result
        result.update(funding_rate=f.rate, funding_interval_hours=f.interval_hours,
                      funding_age_seconds=now - f.received, next_funding_ms=f.next_ms,
                      funding_source=f.source)
        # Financing is keyed to the purchased spot asset's funding source, not perp venue.
        loan = cfg.financing.get(f"{route.spot.venue}:{route.spot.quote}")
        if loan is None:
            apr, fraction = 0.0, 0.0
            result["financing"] = "owned_cash"
        else:
            apr, fraction = loan.get("apr"), loan["borrowed_fraction"]
            result["financing"] = dict(loan)
            if loan["currency"] != route.spot.quote or (fraction > 0 and apr is None):
                result["status"] = "unknown_financing"
                return result
            apr = 0.0 if apr is None else apr
        gross = engine.calculate_annualized_yield(f.rate, 24 / f.interval_hours)
        metrics = engine.calculate_net_adjusted_yield(gross, cfg.holding_days, apr, fraction,
                                                     cfg.round_trip_slippage_bps)
        result.update(metrics, gross_funding_apr_pct=gross * 100)
        spread = max((b.asks[0][0] - b.bids[0][0]) / b.mid * 10000 for b in (spot, perp))
        result["max_leg_spread_bps"] = spread
        if spread > cfg.max_spread_bps:
            result["status"] = "wide_spread"
        elif route.proxy and not cfg.allow_proxy_signals:
            result["status"] = "proxy_comparison_only"
        elif len(engine.basis_history) < cfg.warmup_samples:
            result["status"] = "warming_up"
        else:
            result["signal_active"] = (entry >= cfg.min_entry_basis_bps and smoothed > 0
                                       and metrics["annualized_net_yield_pct"] >= cfg.min_net_yield_pct)
        return result

    def sample(self, now=None, wall=None):
        now = time.monotonic() if now is None else now
        wall = time.time() if wall is None else wall
        for key, route in self.routes.items():
            result = self.calculate(route, now, wall)
            self.latest[key] = result
            self.publish(record("signal", key, result, wall))
            self.alerts.check(key, "opportunity", result["signal_active"], now, wall)
            # Missing/stale prices cannot establish that a divergence recovered.
            if "entry_basis_bps" in result:
                self.alerts.check(key, "basis_divergence",
                                  abs(result["entry_basis_bps"]) >= self.config.divergence_bps,
                                  now, wall)
            f = self.funding.get(route.perp.key)
            # Keep prior alert state while funding is unknown; do not report false recovery.
            if f and now - f.received <= self.config.max_funding_age_seconds:
                if f.next_ms is None or f.next_ms > wall * 1000:
                    self.alerts.check(key, "funding_negative", f.rate < 0, now, wall,
                                      "Positive funding pays the short; negative funding costs the short.")
            unhealthy = result["status"] not in {"ready", "warming_up", "proxy_comparison_only"}
            self.alerts.check(key, "data_or_liquidity_gate", unhealthy, now, wall, result["status"])
        return self.latest
