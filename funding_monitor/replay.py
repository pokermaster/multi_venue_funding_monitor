"""Replay application recordings on a virtual clock, without network access."""
import json
from pathlib import Path
import sqlite3
from .core import Monitor
from .models import Instrument, Route


def export_sqlite(database, output):
    count = 0
    # Read-only access; export ordered ingestion history, not sorted exchange timestamps.
    with sqlite3.connect(Path(database).resolve().as_uri() + "?mode=ro", uri=True) as conn:
        with open(output, "x") as handle:
            for ts, kind, key, payload in conn.execute(
                    "SELECT ts, kind, event_key, payload FROM monitor_events ORDER BY id"):
                if kind in {"universe", "book", "funding", "invalidate", "clock"}:
                    handle.write(json.dumps({"ts": ts, "kind": kind, "key": key,
                                             "payload": json.loads(payload)}) + "\n")
                    count += 1
    return count


def replay(path, config, publish):
    monitor = Monitor(config, publish)
    samples = 0
    previous_clock = None
    with open(path) as handle:
        for line in handle:
            event = json.loads(line)
            wall = float(event["ts"])
            if event["kind"] == "universe":
                routes = [Route(Instrument(**r["spot"]), Instrument(**r["perp"]), r["bucket"])
                          for r in event["payload"]["routes"]]
                monitor.set_routes(routes, wall)
            elif event["kind"] == "clock":
                # Clock markers preserve the actual sampling cadence and ordering.
                now = wall if previous_clock is None else max(wall, previous_clock)
                monitor.sample(now=now, wall=wall)
                previous_clock = now
                samples += 1
            elif event["kind"] in {"book", "funding", "invalidate"}:
                monitor.ingest(event, now=wall, wall=wall)
    if not samples:
        raise ValueError("Replay requires clock records from an application capture")
    return monitor, samples
