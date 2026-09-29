"""Public CCXT adapters. Imports are lazy so demo/replay need only Python."""
import asyncio
import logging
import math
import random
import re
from .models import Instrument, record


def source_specs(venues):
    specs = []
    if "binance" in venues:
        specs += [("binance", "spot", "binance"), ("binance", "perp", "binanceusdm")]
    if "bybit" in venues:
        specs += [("bybit", "spot", "bybit"), ("bybit", "perp", "bybit")]
    if "hyperliquid" in venues:
        specs += [("hyperliquid", "perp", "hyperliquid")]
    return specs


def make_client(spec, websocket=False):
    try:
        if websocket:
            import ccxt.pro as ccxt
        else:
            import ccxt.async_support as ccxt
    except ImportError as exc:
        raise RuntimeError("Live mode requires: python -m pip install -e '.[live]'") from exc
    _, kind, name = spec
    options = {"defaultType": "spot" if kind == "spot" else "swap"}
    if kind == "perp":
        options["defaultSubType"] = "linear"
    return getattr(ccxt, name)({"enableRateLimit": True, "timeout": 20000,
                              "options": options})


def request_params(spec):
    venue, kind, _ = spec
    if venue == "bybit":
        return {"category": "spot" if kind == "spot" else "linear"}
    return {"type": "spot" if kind == "spot" else "swap"}


def market_instruments(spec, markets, tickers, config):
    venue, kind, _ = spec
    result = []
    for symbol, market in markets.items():
        if market.get("active") is not True:
            continue
        if kind == "spot" and not market.get("spot"):
            continue
        if kind == "perp" and not (market.get("swap") and market.get("linear")):
            continue
        quote, base = market.get("quote"), market.get("base")
        if quote not in config.quote_usd or not base or not re.fullmatch(r"[A-Za-z0-9]+", base):
            continue
        if base.endswith(("UP", "DOWN", "BULL", "BEAR")):
            continue
        settle = market.get("settle") or quote
        if kind == "perp" and settle != quote:
            continue
        try:
            volume = float(tickers.get(symbol, {}).get("quoteVolume")) * config.quote_usd[quote]
            size = float(market.get("contractSize")) if kind == "perp" else 1.0
            if not math.isfinite(volume) or volume < 0 or not math.isfinite(size) or size <= 0:
                continue
        except (TypeError, ValueError):
            continue
        result.append(Instrument(venue, symbol, market["id"], base, quote, settle, kind,
                                 size, config.quote_usd[quote], volume))
    return result


async def discover(config):
    async def one(spec):
        client = make_client(spec)
        try:
            markets = await client.load_markets()
            tickers = await client.fetch_tickers(params=request_params(spec))
            instruments = market_instruments(spec, markets, tickers, config)
            logging.info("Discovered %s/%s: %d supported markets with turnover", spec[0], spec[1], len(instruments))
            if not instruments:
                raise ValueError(f"No supported markets with turnover from {spec[0]}/{spec[1]}")
            return instruments
        finally:
            await client.close()
    # All configured sources must succeed: no ranking a silently partial universe.
    results = await asyncio.gather(*(one(s) for s in source_specs(config.venues)), return_exceptions=True)
    errors = [r for r in results if isinstance(r, BaseException)]
    if errors:
        raise RuntimeError("Discovery incomplete: " + "; ".join(str(e) for e in errors))
    return [instrument for group in results for instrument in group]


def interval_hours(value):
    if not isinstance(value, str):
        raise ValueError("Missing funding interval")
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)([hm])", value)
    if not match:
        raise ValueError(f"Unrecognized funding interval: {value}")
    hours = float(match[1]) / (60 if match[2] == "m" else 1)
    if hours <= 0:
        raise ValueError("Funding interval must be positive")
    return hours


def funding_payload(spec, rate, intervals):
    venue = spec[0]
    info = rate.get("info") or {}
    if venue == "binance":
        # fundingInfo lists adjusted contracts; default applies only after a successful fetch.
        adjusted = intervals.get(rate["symbol"])
        hours = interval_hours(adjusted["interval"]) if adjusted else 8.0
    elif venue == "hyperliquid":
        hours = 1.0
    elif info.get("fundingIntervalHour") is not None:
        hours = float(info["fundingIntervalHour"])
    else:
        hours = interval_hours(rate.get("interval"))
    return {"rate": float(rate["fundingRate"]), "interval_hours": hours,
            "next_ms": rate.get("fundingTimestamp"), "source": "exchange_current_estimate"}


async def stream_group(spec, instruments, config, emit):
    """On any group failure, discard that client's caches and rebuild subscriptions."""
    delay = 1.0
    while True:
        client = None
        tasks = []
        started = asyncio.get_running_loop().time()
        try:
            client = make_client(spec, websocket=True)
            await client.load_markets()

            async def book_loop(instrument):
                while True:
                    raw = await asyncio.wait_for(client.watch_order_book(instrument.symbol),
                                                 config.book_timeout_seconds)
                    # Copy CCXT's mutable cache. Persist the same bounded depth used for calculations.
                    payload = {"bids": [list(x[:2]) for x in raw["bids"][:20]],
                               "asks": [list(x[:2]) for x in raw["asks"][:20]],
                               "timestamp": raw.get("timestamp")}
                    emit(record("book", instrument.key, payload))

            async def funding_loop():
                by_symbol = {i.symbol: i for i in instruments if i.kind == "perp"}
                while True:
                    intervals = {}
                    if spec[0] == "binance":
                        intervals = await client.fetch_funding_intervals(list(by_symbol))
                    # Hyperliquid's info endpoint reserves `type` for its operation name.
                    params = {"category": "linear"} if spec[0] == "bybit" else {}
                    rates = await client.fetch_funding_rates(list(by_symbol), params=params)
                    for symbol, instrument in by_symbol.items():
                        try:
                            payload = funding_payload(spec, rates[symbol], intervals)
                            emit(record("funding", instrument.key, payload))
                        except (KeyError, TypeError, ValueError) as exc:
                            emit(record("invalidate", instrument.key, {"channel": "funding", "reason": str(exc)}))
                    await asyncio.sleep(config.funding_poll_seconds)

            tasks = [asyncio.create_task(book_loop(i)) for i in instruments]
            if spec[1] == "perp":
                tasks.append(asyncio.create_task(funding_loop()))
            logging.info("Subscribed %s %s: %d books", spec[0], spec[1], len(instruments))
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logging.warning("Feed reconnect %s/%s: %s", spec[0], spec[1], exc)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for i in instruments:
                emit(record("invalidate", i.key, {"channel": "all", "reason": "connection_closed"}))
            if client is not None:
                try:
                    await client.close()
                except Exception as exc:
                    logging.warning("Client close: %s", exc)
        if asyncio.get_running_loop().time() - started > 60:
            delay = 1
        await asyncio.sleep(delay + random.uniform(0, delay * 0.2))
        delay = min(60, delay * 2)


async def live_streams(routes, config, emit):
    instruments = {i.key: i for r in routes for i in (r.spot, r.perp)}
    tasks = []
    try:
        for spec in source_specs(config.venues):
            group = [i for i in instruments.values() if (i.venue, i.kind) == spec[:2]]
            if group:
                tasks.append(asyncio.create_task(stream_group(spec, group, config, emit)))
        if tasks:
            await asyncio.gather(*tasks)
        else:
            await asyncio.Future()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
