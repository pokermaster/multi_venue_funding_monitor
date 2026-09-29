"""Deterministic synthetic markets; never represents live rankings or returns."""
import asyncio
import time
from .models import Instrument, record
from .universe import select_universe


def demo_instruments(config):
    assets = ["BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "ADA", "AVAX", "LINK",
              "DOT", "LTC", "BCH", "UNI", "ATOM", "NEAR", "AAVE", "FIL", "ETC"]
    result = []
    for rank, base in enumerate(assets):
        volume = (len(assets) - rank) * 5_000_000
        for venue in config.venues:
            kinds = ["perp"] if venue == "hyperliquid" else ["spot", "perp"]
            for kind in kinds:
                quote = "USDC" if venue == "hyperliquid" else "USDT"
                if quote not in config.quote_usd:
                    continue
                symbol = f"{base}/{quote}" + (f":{quote}" if kind == "perp" else "")
                result.append(Instrument(venue, symbol, base + quote, base, quote, quote, kind,
                                         quote_usd=config.quote_usd[quote], volume_usd=volume))
    return result


async def run_demo(monitor, config):
    routes, ranking = select_universe(demo_instruments(config), config)
    monitor.publish(record("ranking", "demo", {"ranking": ranking, "synthetic": True}))
    monitor.set_routes(routes)
    start = time.monotonic()
    while True:
        elapsed = time.monotonic() - start
        wall = time.time()
        for i in monitor.instruments.values():
            price = {"BTC": 60000, "ETH": 3000}.get(i.base, 100)
            if i.kind == "perp":
                price *= 1.005 if i.venue == "binance" else 1.006
            raw_price = price / i.quote_usd
            depth = 100_000 / price
            # A thin market exercises the liquidity gate without corrupting other assets.
            if i.base == "ETC" and i.kind == "perp":
                depth = 0.01
            monitor.ingest(record("book", i.key, {
                "bids": [[raw_price * (1 - 0.0001 * n), depth] for n in (1, 2, 3)],
                "asks": [[raw_price * (1 + 0.0001 * n), depth] for n in (1, 2, 3)],
                "timestamp": wall * 1000}, wall))
            if i.kind == "perp":
                hours = 1 if i.venue == "hyperliquid" else 8
                # One asset inverts after 12 seconds and recovers after 24.
                rate = -0.0001 if i.base == "BTC" and 12 <= elapsed < 24 else 0.0002
                monitor.ingest(record("funding", i.key, {"rate": rate * hours / 8,
                    "interval_hours": hours, "next_ms": (wall + 3600) * 1000,
                    "source": "synthetic_demo"}, wall))
        await asyncio.sleep(0.5)
