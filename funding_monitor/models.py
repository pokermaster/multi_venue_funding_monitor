from dataclasses import asdict, dataclass
import math
import time


@dataclass(frozen=True)
class Instrument:
    venue: str
    symbol: str
    native_id: str
    base: str
    quote: str
    settle: str
    kind: str
    contract_size: float = 1.0
    quote_usd: float = 1.0
    volume_usd: float = 0.0

    @property
    def key(self):
        return f"{self.venue}|{self.kind}|{self.symbol}"


@dataclass(frozen=True)
class Route:
    spot: Instrument
    perp: Instrument
    bucket: str

    @property
    def key(self):
        return f"{self.spot.key}->{self.perp.key}"

    @property
    def proxy(self):
        return self.spot.quote != self.perp.quote or self.perp.venue == "hyperliquid"


@dataclass
class Book:
    bids: list
    asks: list
    received: float
    wall_time: float
    exchange_ms: float | None = None

    @property
    def mid(self):
        return (self.bids[0][0] + self.asks[0][0]) / 2


@dataclass
class Funding:
    rate: float
    interval_hours: float
    received: float
    next_ms: float | None
    source: str = "exchange_current_estimate"


def record(kind, key, payload, wall_time=None):
    return {"kind": kind, "key": key, "ts": time.time() if wall_time is None else wall_time,
            "payload": payload}


def route_dict(route):
    return asdict(route)


def normalize_book(raw, instrument, now=None, wall=None):
    """CCXT reconstructs depth. Convert contract amounts to base units here."""
    sides = []
    for name, reverse in (("bids", True), ("asks", False)):
        levels = []
        for row in raw.get(name, []):
            price, size = float(row[0]), float(row[1]) * instrument.contract_size
            if not all(math.isfinite(v) for v in (price, size)) or price <= 0 or size < 0:
                raise ValueError("Non-finite/invalid order book level")
            if size:
                levels.append((price * instrument.quote_usd, size))
        levels.sort(reverse=reverse)
        if not levels:
            raise ValueError("Empty order book side")
        sides.append(levels)
    if sides[0][0][0] >= sides[1][0][0]:
        raise ValueError("Locked or crossed order book")
    exchange_ms = raw.get("timestamp")
    if exchange_ms is not None:
        exchange_ms = float(exchange_ms)
        if not math.isfinite(exchange_ms):
            raise ValueError("Invalid exchange timestamp")
    return Book(*sides, time.monotonic() if now is None else now,
                time.time() if wall is None else wall, exchange_ms)
