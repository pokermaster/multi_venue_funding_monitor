"""Lifecycle, periodic sampling, universe refresh and the CLI presentation layer."""
import argparse
import asyncio
from dataclasses import asdict
import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import signal
import time
from .adapters import discover, live_streams
from .config import load_config
from .core import Monitor
from .demo import run_demo
from .models import record
from .replay import export_sqlite, replay
from .storage import BatchSink
from .universe import SelectionGate, select_universe


def display(monitor, sink):
    print(f"\n{time.strftime('%H:%M:%S')} routes={len(monitor.routes)} "
          f"invalid_events={monitor.invalid_events} storage={json.dumps(sink.health())}", flush=True)
    print(f"{'ASSET':8} {'BUCKET':7} {'SPOT -> PERP':27} {'BASIS':>8} {'ENTRY':>8} {'NET APR':>9}  STATUS")
    for key, row in sorted(monitor.latest.items()):
        def fmt(name):
            v = row.get(name)
            return '--' if v is None else f'{v:.2f}'
        venues = row['spot_venue'] + ' -> ' + row['perp_venue']
        route = monitor.routes[key]
        quotes = f" {route.spot.quote}/{route.perp.quote}"
        status = "OPPORTUNITY" if row["signal_active"] else row["status"]
        print(f"{row['base']:8} {row['bucket']:7} {venues:27} "
              f"{fmt('mid_basis_bps'):>8} {fmt('entry_basis_bps'):>8} "
              f"{fmt('annualized_net_yield_pct'):>9}  {status}{quotes}")
    print("Basis/entry: bps; net APR: projected funding carry %, reference notional. Fees/FX are configured assumptions.", flush=True)


async def live_producer(monitor, config):
    gate = SelectionGate(config.selection_confirmations)
    stream_task = None
    signature = None
    try:
        while True:
            try:
                print("STARTING DISCOVERY", flush=True)
                start = time.perf_counter()
                instruments = await discover(config)
                print(
                    "DISCOVERY FINISHED:",
                    len(instruments),
                    "instruments in",
                    time.perf_counter() - start,
                    "seconds",
                    flush=True
                )
                print("INSTRUMENT COUNT:", len(instruments), flush=True) # debug statement
                print("ABOUT TO SELECT UNIVERSE", flush=True) # debug statement
                routes, ranking = select_universe(instruments, config)
                print("FINISHED SELECTING UNIVERSE", len(routes), flush=True) # debug statement
                monitor.publish(record("ranking", "live", {"ranking": ranking,
                                       "quote_usd": config.quote_usd, "synthetic": False}))
                accepted = gate.accept(routes)
                # Remove known-ineligible markets immediately, even during selection hysteresis.
                valid = {i.key for i in instruments if i.volume_usd >= (
                    config.min_spot_volume_usd if i.kind == "spot" else config.min_perp_volume_usd)}
                chosen = routes if accepted else [r for r in monitor.routes.values()
                                                  if r.spot.key in valid and r.perp.key in valid]
                # Include conversion and contract metadata in generation identity.
                # Volume-only changes do not need to restart subscriptions.
                structural = [(r.key, r.bucket, r.spot.contract_size, r.perp.contract_size,
                               r.spot.quote_usd, r.perp.quote_usd) for r in chosen]
                new_signature = tuple(structural)
                if new_signature != signature:
                    if stream_task:
                        stream_task.cancel()
                        await asyncio.gather(stream_task, return_exceptions=True)
                    monitor.set_routes(chosen)
                    stream_task = asyncio.create_task(live_streams(chosen, config, monitor.ingest))
                    signature = new_signature
                missing = set(config.assets) - {r.spot.base for r in chosen}
                if missing:
                    logging.warning("Requested assets ineligible: %s", sorted(missing))
                if not chosen:
                    logging.warning("No eligible routes; check venue access, volume floors and asset filters")
                logging.info("Discovery complete: %d eligible ranked assets, %d active routes", len(ranking), len(chosen))
                wait = config.refresh_seconds
            except Exception as exc:
                logging.error("Universe refresh failed; retaining prior subscriptions: %s", exc)
                monitor.publish(record("health", "discovery", {"status": "failed", "error": str(exc)}))
                wait = min(60, config.refresh_seconds)
            if stream_task:
                done, _ = await asyncio.wait([stream_task], timeout=wait)
                if done:
                    await stream_task
                    raise RuntimeError("Subscription manager exited unexpectedly")
            else:
                await asyncio.sleep(wait)
    finally:
        if stream_task:
            stream_task.cancel()
            await asyncio.gather(stream_task, return_exceptions=True)


async def run(config, mode, duration=None):
    sink = BatchSink(config)
    monitor = Monitor(config, sink.submit)
    sink.submit(record("config", mode, asdict(config)))
    writer = asyncio.create_task(sink.run())
    producer = asyncio.create_task(run_demo(monitor, config) if mode == "demo" else live_producer(monitor, config))
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    installed_signals = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
            installed_signals.append(sig)
        except (NotImplementedError, RuntimeError):
            pass

    async def sampler():
        while True:
            now, wall = time.monotonic(), time.time()
            sink.submit(record("clock", "sample", {}, wall))
            monitor.sample(now, wall)
            await asyncio.sleep(config.sample_seconds)

    async def screen():
        while True:
            display(monitor, sink)
            sink.submit(record("health", "runtime", {**sink.health(),
                               "invalid_events": monitor.invalid_events,
                               "active_routes": len(monitor.routes)}))
            await asyncio.sleep(config.display_seconds)

    workers = [producer, asyncio.create_task(sampler()), asyncio.create_task(screen()), writer]
    stopper = asyncio.create_task(stop.wait())
    timer = asyncio.create_task(asyncio.sleep(duration)) if duration is not None else None
    try:
        done, _ = await asyncio.wait(workers + [stopper] + ([timer] if timer else []),
                                     return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            if task in workers:
                await task
                raise RuntimeError("Background worker stopped unexpectedly")
    finally:
        for task in workers[:-1] + [stopper] + ([timer] if timer else []):
            task.cancel()
        await asyncio.gather(*workers[:-1], stopper, *([timer] if timer else []), return_exceptions=True)
        try:
            await asyncio.wait_for(sink.queue.join(), config.shutdown_timeout_seconds)
        except asyncio.TimeoutError:
            logging.error("Shutdown persistence incomplete: %s", sink.health())
        writer.cancel()
        await asyncio.gather(writer, return_exceptions=True)
        for sig in installed_signals:
            loop.remove_signal_handler(sig)
        logging.info("Final storage health: %s", sink.health())
    return monitor, sink


def main():
    parser = argparse.ArgumentParser(description="Multi-venue basis/funding research monitor (no trading)")
    parser.add_argument("--mode", choices=["demo", "live", "replay"], default="demo")
    parser.add_argument("--config", help="JSON configuration; unspecified fields use defaults")
    parser.add_argument("--duration", type=float, help="Stop after this many seconds")
    parser.add_argument("--replay", help="Input JSONL capture for replay mode")
    parser.add_argument("--replay-output", help="Write replayed signals/alerts as JSONL (new file)")
    parser.add_argument("--export", metavar="JSONL", help="Export configured SQLite database to a new JSONL file and exit")
    parser.add_argument("--log", default="logs/monitor.log")
    args = parser.parse_args()
    config = load_config(args.config)
    if args.duration is not None and (not 0 < args.duration < float('inf')):
        parser.error("--duration must be positive and finite")
    Path(args.log).parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=[logging.StreamHandler(), RotatingFileHandler(args.log, maxBytes=5_000_000, backupCount=3)])
    if args.export:
        print(f"Exported {export_sqlite(config.sqlite_path, args.export)} records to {args.export}")
        return
    if args.mode == "replay":
        if not args.replay:
            parser.error("--mode replay requires --replay PATH")
        output = open(args.replay_output, "x") if args.replay_output else None
        try:
            def publish(event):
                if output and event["kind"] in {"signal", "alert"}:
                    output.write(json.dumps(event, allow_nan=False) + "\n")
            monitor, samples = replay(args.replay, config, publish)
            display(monitor, BatchSink(config))
            print(f"Replayed {samples} sampling ticks")
        finally:
            if output:
                output.close()
        return
    logging.info("Mode=%s; configured FX=%s; fees are assumptions, not account-fetched", args.mode, config.quote_usd)
    try:
        asyncio.run(run(config, args.mode, args.duration))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
