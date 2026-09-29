import asyncio
from dataclasses import replace
import json
import math
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from engine import QuantitativeEngine, average_fill
from funding_monitor.adapters import funding_payload, interval_hours, market_instruments, source_specs
from funding_monitor.alerts import Alerts
from funding_monitor.app import run
from funding_monitor.config import Config, load_config
from funding_monitor.core import Monitor
from funding_monitor.demo import demo_instruments
from funding_monitor.models import Instrument, Route, normalize_book, record
from funding_monitor.replay import export_sqlite, replay
from funding_monitor.storage import BatchSink, ilp_line
from funding_monitor.universe import SelectionGate, select_universe


def instrument(base="BTC", venue="binance", kind="spot", volume=10_000_000):
    symbol = f"{base}/USDT" + (":USDT" if kind == "perp" else "")
    return Instrument(venue, symbol, base + "USDT", base, "USDT", "USDT", kind,
                      volume_usd=volume)


def route(base="BTC"):
    return Route(instrument(base), instrument(base, kind="perp"), "high")


def book_event(i, mid, ts=100, size=1000):
    return record("book", i.key, {"bids": [[mid - 0.01, size]],
                                  "asks": [[mid + 0.01, size]], "timestamp": ts * 1000}, ts)


def seed(monitor, r, ts=100, spot=100, perp=101, funding=0.0002):
    monitor.ingest(book_event(r.spot, spot, ts), now=ts, wall=ts)
    monitor.ingest(book_event(r.perp, perp, ts), now=ts, wall=ts)
    monitor.ingest(record("funding", r.perp.key, {"rate": funding, "interval_hours": 8,
                                                 "next_ms": (ts + 3600) * 1000}, ts), now=ts, wall=ts)


class EngineTests(unittest.TestCase):
    def test_known_cost_example(self):
        engine = QuantitativeEngine(0.001, 0.0005)
        self.assertAlmostEqual(engine.calculate_basis_spread(101, 100), 100)
        gross = engine.calculate_annualized_yield(0.0001, 3)
        self.assertAlmostEqual(gross, 0.1095)
        result = engine.calculate_net_adjusted_yield(gross, 30, 0.05, 0.5, 10)
        self.assertAlmostEqual(result["annualized_net_yield_pct"], (0.1095 - 0.025 - 0.004 * 365 / 30) * 100)

    def test_depth_weighted_and_unfillable(self):
        self.assertAlmostEqual(average_fill([(100, 1), (102, 2)], 2), 101)
        self.assertIsNone(average_fill([(100, 1)], 2))

    def test_invalid_inputs(self):
        e = QuantitativeEngine(0, 0)
        for bad in (0, -1, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                e.calculate_basis_spread(100, bad)
        with self.assertRaises(ValueError):
            e.calculate_net_adjusted_yield(0.1, 0, 0)


class UniverseTests(unittest.TestCase):
    def test_disjoint_ranked_buckets(self):
        cfg = Config()
        routes, ranking = select_universe(demo_instruments(cfg), cfg)
        groups = {g: {r.spot.base for r in routes if r.bucket == g} for g in ("high", "mid", "low")}
        self.assertEqual([len(v) for v in groups.values()], [5, 5, 5])
        self.assertEqual(len(set.union(*groups.values())), 15)
        self.assertEqual(ranking[0]["base"], "BTC")
        self.assertEqual({r.spot.base for r in routes if r.bucket == "low"}, {"ATOM", "NEAR", "AAVE", "FIL", "ETC"})

    def test_missing_leg_and_low_volume_excluded(self):
        cfg = Config(assets=["BTC", "ETH", "SOL"])
        items = [instrument(), instrument(kind="perp"), instrument("ETH"),
                 instrument("SOL", volume=1), instrument("SOL", kind="perp")]
        routes, _ = select_universe(items, cfg)
        self.assertEqual({r.spot.base for r in routes}, {"BTC"})

    def test_confirmation_resets_after_oscillation(self):
        gate = SelectionGate(2)
        self.assertTrue(gate.accept([route()]))
        self.assertFalse(gate.accept([route("ETH")]))
        self.assertTrue(gate.accept([route()]))
        self.assertFalse(gate.accept([route("ETH")]))
        self.assertTrue(gate.accept([route("ETH")]))


class StateTests(unittest.TestCase):
    def setUp(self):
        self.cfg = Config(warmup_samples=1)
        self.events = []
        self.monitor = Monitor(self.cfg, self.events.append)
        self.btc, self.eth = route(), route("ETH")
        self.monitor.set_routes([self.btc, self.eth], 100)

    def test_interleaved_assets_and_histories_are_isolated(self):
        seed(self.monitor, self.btc)
        seed(self.monitor, self.eth, spot=200, perp=199)
        values = self.monitor.sample(100, 100)
        self.assertAlmostEqual(values[self.btc.key]["mid_basis_bps"], 100)
        self.assertAlmostEqual(values[self.eth.key]["mid_basis_bps"], -50)
        self.assertTrue(values[self.btc.key]["signal_active"])
        self.assertFalse(values[self.eth.key]["signal_active"])
        self.assertEqual(list(self.monitor.engines[self.btc.key].basis_history), [100])

    def test_funding_events_do_not_append_history(self):
        seed(self.monitor, self.btc)
        seed(self.monitor, self.btc)
        self.assertEqual(len(self.monitor.engines[self.btc.key].basis_history), 0)
        self.monitor.sample(100, 100)
        self.assertEqual(len(self.monitor.engines[self.btc.key].basis_history), 1)

    def test_missing_funding_is_not_zero(self):
        self.monitor.ingest(book_event(self.btc.spot, 100), 100, 100)
        self.monitor.ingest(book_event(self.btc.perp, 101), 100, 100)
        value = self.monitor.sample(100, 100)[self.btc.key]
        self.assertEqual(value["status"], "missing_funding")
        self.assertFalse(value["signal_active"])

    def test_stale_and_disconnect_reset_history(self):
        seed(self.monitor, self.btc)
        self.monitor.sample(100, 100)
        value = self.monitor.sample(120, 120)[self.btc.key]
        self.assertEqual(value["status"], "stale_book")
        self.assertFalse(value["signal_active"])
        self.assertFalse(self.monitor.engines[self.btc.key].basis_history)
        self.monitor.ingest(record("invalidate", self.btc.perp.key, {"channel": "all"}, 120), 120, 120)
        self.assertNotIn(self.btc.perp.key, self.monitor.books)
        self.assertNotIn(self.btc.perp.key, self.monitor.funding)

    def test_exchange_time_and_skew_gates(self):
        seed(self.monitor, self.btc)
        stale = book_event(self.btc.perp, 101, 100)
        stale["payload"]["timestamp"] = 1000
        self.monitor.books.clear()
        self.monitor.ingest(book_event(self.btc.spot, 100), 100, 100)
        self.monitor.ingest(stale, 100, 100)
        self.assertEqual(self.monitor.sample(100, 100)[self.btc.key]["status"], "exchange_clock_or_stale_book")
        seed(self.monitor, self.btc)
        self.monitor.ingest(book_event(self.btc.spot, 100, 105), 105, 105)
        self.assertEqual(self.monitor.sample(105, 105)[self.btc.key]["status"], "book_time_skew")

    def test_out_of_order_and_invalid_book(self):
        seed(self.monitor, self.btc)
        self.monitor.ingest(book_event(self.btc.spot, 1, 99), 101, 101)
        self.assertEqual(self.monitor.books[self.btc.spot.key].mid, 100)
        event = book_event(self.btc.spot, 100)
        event["payload"]["bids"] = [[110, 1]]
        self.monitor.ingest(event, 100, 100)
        self.assertNotIn(self.btc.spot.key, self.monitor.books)
        self.assertEqual(self.monitor.invalid_events, 1)

    def test_unknown_borrow_and_missing_fees_block(self):
        self.cfg.financing = {"binance:USDT": {"lender": "binance", "currency": "USDT",
                               "product": "margin", "apr": None, "borrowed_fraction": 1}}
        seed(self.monitor, self.btc)
        self.assertEqual(self.monitor.sample(100, 100)[self.btc.key]["status"], "unknown_financing")
        self.cfg.fees = {}
        self.monitor.set_routes([self.btc], 100)
        seed(self.monitor, self.btc)
        self.assertEqual(self.monitor.sample(100, 100)[self.btc.key]["status"], "missing_fees")

    def test_proxy_and_insufficient_depth(self):
        proxy = Route(self.btc.spot, replace(self.btc.perp, venue="hyperliquid"), "high")
        self.monitor.set_routes([proxy], 100)
        seed(self.monitor, proxy)
        self.assertEqual(self.monitor.sample(100, 100)[proxy.key]["status"], "proxy_comparison_only")
        self.monitor.ingest(book_event(proxy.perp, 101, size=0.001), 100, 100)
        self.assertEqual(self.monitor.sample(100, 100)[proxy.key]["status"], "insufficient_depth")

    def test_funding_expiry_and_inversion(self):
        seed(self.monitor, self.btc)
        self.monitor.sample(100, 100)
        seed(self.monitor, self.btc, ts=101, funding=-0.001)
        value = self.monitor.sample(101, 101)[self.btc.key]
        self.assertFalse(value["signal_active"])
        self.assertTrue(any(e["kind"] == "alert" and e["payload"]["name"] == "funding_negative" for e in self.events))
        self.monitor.funding[self.btc.perp.key].next_ms = 101000
        self.assertEqual(self.monitor.sample(101, 101)[self.btc.key]["status"], "funding_settlement_pending_refresh")


class AdapterTests(unittest.TestCase):
    def test_normalization_contract_size_fx(self):
        i = replace(instrument(kind="perp"), contract_size=0.01, quote_usd=0.99)
        book = normalize_book({"bids": [[100, 3]], "asks": [[102, 4]]}, i, 0, 0)
        self.assertEqual(book.bids, [(99, 0.03)])

    def test_funding_intervals(self):
        rate = {"symbol": "BTC/USDT:USDT", "fundingRate": 0.0001, "info": {}}
        self.assertEqual(funding_payload(("binance",), rate, {})["interval_hours"], 8)
        self.assertEqual(funding_payload(("binance",), rate, {rate["symbol"]: {"interval": "4h"}})["interval_hours"], 4)
        self.assertEqual(funding_payload(("hyperliquid",), rate, {})["interval_hours"], 1)
        rate["info"]["fundingIntervalHour"] = "2"
        self.assertEqual(funding_payload(("bybit",), rate, {})["interval_hours"], 2)
        self.assertEqual(interval_hours("30m"), 0.5)
        with self.assertRaises(ValueError):
            interval_hours(None)

    def test_discovery_filters(self):
        market = {"id": "BTCUSDT", "base": "BTC", "quote": "USDT", "active": True, "spot": True}
        markets = {"BTC/USDT": market, "OFF/USDT": {**market, "active": False}}
        tickers = {"BTC/USDT": {"quoteVolume": 1234}, "OFF/USDT": {"quoteVolume": 9999}}
        items = market_instruments(("binance", "spot", "binance"), markets, tickers, Config())
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].volume_usd, 1234)


class AlertTests(unittest.TestCase):
    def test_cooldown_and_recovery(self):
        events = []
        alerts = Alerts(10, events.append)
        for now in (0, 1, 5, 11):
            alerts.check("r", "opportunity", True, now, now)
        alerts.check("r", "opportunity", False, 12, 12)
        alerts.check("r", "opportunity", False, 13, 13)
        self.assertEqual([e["payload"]["status"] for e in events], ["active", "active", "recovered"])


class StorageTests(unittest.IsolatedAsyncioTestCase):
    async def test_batch_retry_then_drain_and_replay(self):
        with tempfile.TemporaryDirectory() as folder:
            cfg = Config(storage="sqlite", sqlite_path=f"{folder}/data.db",
                         flush_seconds=0.01, warmup_samples=1)
            sink = BatchSink(cfg)
            monitor = Monitor(cfg, sink.submit)
            r = route()
            monitor.set_routes([r], 100)
            seed(monitor, r)
            sink.submit(record("clock", "sample", {}, 100))
            expected = monitor.sample(100, 100)[r.key]
            original = sink.write_batch
            calls = 0

            def failing_once(batch):
                nonlocal calls
                calls += 1
                if calls == 1:
                    raise OSError("simulated database outage")
                original(batch)

            with patch.object(sink, "write_batch", failing_once):
                worker = asyncio.create_task(sink.run())
                await asyncio.wait_for(sink.queue.join(), 4)
                worker.cancel()
                await asyncio.gather(worker, return_exceptions=True)
            self.assertEqual(sink.failures, 1)
            self.assertEqual(sink.dropped, 0)
            capture = f"{folder}/capture.jsonl"
            self.assertGreater(export_sqlite(cfg.sqlite_path, capture), 0)
            replayed, samples = replay(capture, cfg, lambda e: None)
            self.assertEqual(samples, 1)
            self.assertEqual(replayed.latest[r.key], expected)

    async def test_queue_overflow_is_nonblocking_and_visible(self):
        sink = BatchSink(Config(queue_size=1))
        sink.submit(record("book", "x", {}))
        sink.submit(record("book", "x", {}))
        self.assertEqual(sink.queue.qsize(), 1)
        self.assertEqual(sink.dropped, 1)

    async def test_short_demo_shutdown_drains(self):
        with tempfile.TemporaryDirectory() as folder:
            cfg = Config(assets=["BTC", "ETH"], venues=["binance"], storage="sqlite",
                         sqlite_path=f"{folder}/demo.db",
                         sample_seconds=0.05, display_seconds=10, flush_seconds=0.02, warmup_samples=1)
            with patch("funding_monitor.app.display"):
                monitor, sink = await run(cfg, "demo", duration=0.2)
            self.assertEqual(len(monitor.routes), 2)
            self.assertEqual(sink.queue.qsize(), 0)
            self.assertEqual(sink.inflight, 0)
            self.assertGreater(sink.written, 0)

    def test_ilp_payload_and_timestamp(self):
        line = ilp_line(record("signal", "a b,c=d", {"status": 'say "hi"\nnow'}, 1))
        self.assertTrue(line.startswith("monitor_events,kind=signal,event_key=a\\ b\\,c\\=d "))
        self.assertTrue(line.endswith(" 1000000000"))
        self.assertNotIn("\n", line)


class ConfigTests(unittest.TestCase):
    def test_questdb_is_default_storage(self):
        cfg = Config().validate()
        self.assertEqual(cfg.storage, "questdb")
        self.assertEqual(cfg.questdb_url, "http://127.0.0.1:9000/write?precision=n")

    def test_invalid_config_fails_early(self):
        for cfg in (Config(bucket_size=0), Config(sample_seconds=0), Config(quote_usd={"USDT": math.nan}),
                    Config(venues=["unknown"]), Config(window_size=1, warmup_samples=2)):
            with self.assertRaises(ValueError):
                cfg.validate()


class LiveAdapterLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_hyperliquid_funding_does_not_override_info_operation(self):
        from funding_monitor.adapters import stream_group
        received = asyncio.Event()
        calls = []
        closed = []
        i = replace(instrument(kind="perp"), venue="hyperliquid")

        class Client:
            async def load_markets(self):
                return {}

            async def watch_order_book(self, symbol):
                await asyncio.Future()

            async def fetch_funding_rates(self, symbols, params):
                calls.append(params)
                return {i.symbol: {"symbol": i.symbol, "fundingRate": 0.0001}}

            async def close(self):
                closed.append(True)

        events = []

        def emit(event):
            events.append(event)
            if event["kind"] == "funding":
                received.set()

        with patch("funding_monitor.adapters.make_client", return_value=Client()):
            task = asyncio.create_task(stream_group(("hyperliquid", "perp", "hyperliquid"), [i], Config(), emit))
            await asyncio.wait_for(received.wait(), 1)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(calls, [{}])
        self.assertEqual(closed, [True])
        self.assertEqual(events[-1]["kind"], "invalidate")
        self.assertEqual(events[0]["payload"]["interval_hours"], 1)

    async def test_reconnection_invalidates_and_replaces_client(self):
        from funding_monitor.adapters import stream_group
        recovered = asyncio.Event()
        created = []
        events = []
        i = instrument()

        class Client:
            def __init__(self, fails):
                self.fails = fails
                self.closed = False
                self.sent = False

            async def load_markets(self):
                return {}

            async def watch_order_book(self, symbol):
                if self.fails:
                    raise OSError("simulated disconnect")
                if self.sent:
                    await asyncio.Future()
                self.sent = True
                return {"bids": [[100, 1]], "asks": [[101, 1]], "timestamp": 100000}

            async def close(self):
                self.closed = True

        def factory(*args, **kwargs):
            client = Client(not created)
            created.append(client)
            return client

        def emit(event):
            events.append(event)
            if event["kind"] == "book":
                recovered.set()

        with patch("funding_monitor.adapters.make_client", side_effect=factory):
            task = asyncio.create_task(stream_group(("binance", "spot", "binance"), [i], Config(), emit))
            await asyncio.wait_for(recovered.wait(), 3)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(len(created), 2)
        self.assertTrue(all(c.closed for c in created))
        self.assertEqual(events[0]["kind"], "invalidate")
        self.assertTrue(any(e["kind"] == "book" for e in events))

    def test_spot_client_does_not_force_derivative_subtype(self):
        import sys
        import types
        from funding_monitor.adapters import make_client
        module = types.ModuleType("ccxt.async_support")
        module.binance = lambda options: options
        root = types.ModuleType("ccxt")
        root.async_support = module
        with patch.dict(sys.modules, {"ccxt": root, "ccxt.async_support": module}):
            options = make_client(("binance", "spot", "binance"))["options"]
        self.assertEqual(options["defaultType"], "spot")
        self.assertNotIn("defaultSubType", options)


class DataQualityRegressionTests(unittest.TestCase):
    def test_invalid_receive_times_and_malformed_books_reset_warmup(self):
        cfg = Config(warmup_samples=1)
        monitor = Monitor(cfg)
        r = route()
        monitor.set_routes([r], 100)
        for bad in (math.nan, math.inf, -math.inf, 200):
            seed(monitor, r)
            monitor.sample(100, 100)
            event = book_event(r.spot, 100)
            event["ts"] = bad
            monitor.ingest(event, 100, 100)
            self.assertNotIn(r.spot.key, monitor.books)
            self.assertEqual(len(monitor.engines[r.key].basis_history), 0)
        seed(monitor, r)
        event = book_event(r.spot, 100)
        event["payload"]["bids"] = [[100]]
        monitor.ingest(event, 100, 100)
        self.assertNotIn(r.spot.key, monitor.books)
        self.assertEqual(monitor.invalid_events, 5)

    def test_stale_book_does_not_report_divergence_recovery(self):
        events = []
        monitor = Monitor(Config(warmup_samples=1, divergence_bps=50), events.append)
        r = route()
        monitor.set_routes([r], 100)
        seed(monitor, r)
        monitor.sample(100, 100)
        monitor.sample(200, 200)
        divergence = [e for e in events if e["kind"] == "alert"
                      and e["payload"]["name"] == "basis_divergence"]
        self.assertEqual([e["payload"]["status"] for e in divergence], ["active"])
        seed(monitor, r, ts=201, perp=100)
        monitor.sample(201, 201)
        self.assertTrue(any(e["kind"] == "alert" and e["payload"]["name"] == "basis_divergence"
                            and e["payload"]["status"] == "recovered" for e in events))

    def test_unknown_contract_size_excluded(self):
        market = {"id": "BTCUSDT", "base": "BTC", "quote": "USDT", "settle": "USDT",
                  "active": True, "swap": True, "linear": True}
        for size in (None, 0, -1, math.nan):
            items = market_instruments(("binance", "perp", "binanceusdm"),
                {"BTC/USDT:USDT": {**market, "contractSize": size}},
                {"BTC/USDT:USDT": {"quoteVolume": 1e8}}, Config())
            self.assertEqual(items, [])

    def test_invalid_timestamp_cannot_poison_storage_batch(self):
        sink = BatchSink(Config())
        for bad in (math.nan, math.inf, -math.inf):
            sink.submit(record("book", "x", {}, bad))
        self.assertEqual(sink.queue.qsize(), 0)
        self.assertEqual(sink.dropped, 3)


if __name__ == "__main__":
    unittest.main()
