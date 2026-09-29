"""Nonblocking fan-out to a bounded queue; batch I/O runs in a worker thread."""
import asyncio
import json
import logging
import math
import os
from pathlib import Path
import sqlite3
import time
import urllib.request


def json_payload(event):
    return json.dumps(event["payload"], separators=(",", ":"), allow_nan=False)


def ilp_line(event):
    def tag(value):
        return str(value).replace("\\", "\\\\").replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\=")
    payload = json_payload(event).replace("\\", "\\\\").replace('"', '\\"')
    # New table avoids incompatible schema changes to the original BTC-only tables.
    return (f'monitor_events,kind={tag(event["kind"])},event_key={tag(event["key"])} '
            f'payload="{payload}" {int(event["ts"] * 1_000_000_000)}')


class BatchSink:
    def __init__(self, config):
        self.config = config
        self.queue = asyncio.Queue(maxsize=config.queue_size)
        self.dropped = 0
        self.written = 0
        self.failures = 0
        self.inflight = 0
        self.last_error = None
        self.oldest_queued = None

    def submit(self, event):
        if self.config.storage == "none":
            return
        try:
            # Verify serializability before it can poison a retried batch.
            json_payload(event)
            if not math.isfinite(float(event["ts"])):
                raise ValueError("Non-finite event timestamp")
            self.queue.put_nowait(event)
        except (asyncio.QueueFull, KeyError, ValueError, TypeError, OverflowError):
            self.dropped += 1
            if self.dropped == 1 or self.dropped % 1000 == 0:
                logging.error("Persistence dropped events=%d (queue full or invalid JSON)", self.dropped)

    def write_batch(self, batch):
        cfg = self.config
        if cfg.storage == "sqlite":
            Path(cfg.sqlite_path).parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(cfg.sqlite_path, timeout=5) as conn:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("CREATE TABLE IF NOT EXISTS monitor_events (id INTEGER PRIMARY KEY, ts REAL NOT NULL, kind TEXT NOT NULL, event_key TEXT NOT NULL, payload TEXT NOT NULL)")
                conn.execute("CREATE INDEX IF NOT EXISTS monitor_kind_ts ON monitor_events(kind, ts)")
                conn.executemany("INSERT INTO monitor_events(ts, kind, event_key, payload) VALUES (?, ?, ?, ?)",
                                 [(e["ts"], e["kind"], e["key"], json_payload(e)) for e in batch])
        elif cfg.storage == "questdb":
            body = ("\n".join(ilp_line(e) for e in batch) + "\n").encode()
            headers = {"Content-Type": "text/plain; charset=utf-8"}
            if os.getenv("QUESTDB_TOKEN"):
                headers["Authorization"] = "Bearer " + os.environ["QUESTDB_TOKEN"]
            request = urllib.request.Request(cfg.questdb_url, body, headers, method="POST")
            with urllib.request.urlopen(request, timeout=10) as response:
                response.read()

    async def run(self):
        while True:
            batch = [await self.queue.get()]
            deadline = asyncio.get_running_loop().time() + self.config.flush_seconds
            while len(batch) < self.config.batch_size:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    break
                try:
                    batch.append(await asyncio.wait_for(self.queue.get(), remaining))
                except asyncio.TimeoutError:
                    break
            self.inflight = len(batch)
            self.oldest_queued = batch[0]["ts"]
            delay = 1
            while True:
                try:
                    await asyncio.to_thread(self.write_batch, batch)
                    self.written += len(batch)
                    self.last_error = None
                    break
                except Exception as exc:
                    self.failures += 1
                    self.last_error = str(exc)
                    logging.error("Storage retry; %d buffered rows: %s", len(batch), exc)
                    await asyncio.sleep(delay)
                    delay = min(30, delay * 2)
            for _ in batch:
                self.queue.task_done()
            self.inflight = 0
            self.oldest_queued = None

    def health(self):
        return {"queued": self.queue.qsize(), "inflight": self.inflight,
                "written": self.written, "dropped": self.dropped, "failures": self.failures,
                "last_error": self.last_error,
                "batch_age_seconds": None if self.oldest_queued is None else time.time() - self.oldest_queued}
