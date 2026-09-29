"""Strict JSON configuration. Public feeds require no credentials."""
from dataclasses import dataclass, field, fields
import json
import math


@dataclass
class Config:
    venues: list[str] = field(default_factory=lambda: ["binance", "bybit", "hyperliquid"])
    assets: list[str] = field(default_factory=list)
    bucket_size: int = 5
    min_spot_volume_usd: float = 1_000_000
    min_perp_volume_usd: float = 1_000_000
    refresh_seconds: float = 3600
    selection_confirmations: int = 2
    quote_usd: dict = field(default_factory=lambda: {"USDT": 1.0, "USDC": 1.0})
    excluded_bases: list[str] = field(default_factory=lambda: ["USDT", "USDC", "DAI", "TUSD", "FDUSD", "USDE"])
    allow_proxy_signals: bool = False
    sample_seconds: float = 1
    display_seconds: float = 5
    max_book_age_seconds: float = 10
    max_book_skew_seconds: float = 3
    max_funding_age_seconds: float = 180
    funding_poll_seconds: float = 60
    book_timeout_seconds: float = 30
    window_size: int = 50
    warmup_samples: int = 10
    trade_notional_usd: float = 1000
    max_spread_bps: float = 30
    min_entry_basis_bps: float = 5
    divergence_bps: float = 100
    min_net_yield_pct: float = 5
    holding_days: float = 30
    round_trip_slippage_bps: float = 10
    fees: dict = field(default_factory=lambda: {
        "binance:spot": 0.001, "binance:perp": 0.0005,
        "bybit:spot": 0.001, "bybit:perp": 0.00055,
        "hyperliquid:perp": 0.00045})
    financing: dict = field(default_factory=dict)
    alert_cooldown_seconds: float = 300
    queue_size: int = 20000
    batch_size: int = 500
    flush_seconds: float = 1
    storage: str = "questdb"
    sqlite_path: str = "data/monitor.sqlite3"
    questdb_url: str = "http://127.0.0.1:9000/write?precision=n"
    shutdown_timeout_seconds: float = 15

    def validate(self):
        if type(self.allow_proxy_signals) is not bool:
            raise ValueError("allow_proxy_signals must be a JSON boolean")
        if not isinstance(self.assets, list) or any(not isinstance(a, str) or not a for a in self.assets):
            raise ValueError("assets must be a list of nonempty canonical base names")
        if not self.venues or len(set(self.venues)) != len(self.venues) or set(self.venues) - {"binance", "bybit", "hyperliquid"}:
            raise ValueError("venues must be unique supported exchange names")
        if self.storage not in {"sqlite", "questdb", "none"}:
            raise ValueError("storage must be sqlite, questdb or none")
        for name in ("bucket_size", "selection_confirmations", "window_size", "warmup_samples", "queue_size", "batch_size"):
            value = getattr(self, name)
            if type(value) is not int or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if self.warmup_samples > self.window_size:
            raise ValueError("warmup_samples cannot exceed window_size")
        for f in fields(self):
            value = getattr(self, f.name)
            if isinstance(value, (float, int)) and not isinstance(value, bool):
                if not math.isfinite(value) or value < 0:
                    raise ValueError(f"{f.name} must be finite and nonnegative")
        for name in ("refresh_seconds", "sample_seconds", "display_seconds", "funding_poll_seconds",
                     "book_timeout_seconds", "holding_days", "trade_notional_usd", "flush_seconds",
                     "max_book_age_seconds", "max_funding_age_seconds", "shutdown_timeout_seconds"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if not self.quote_usd or any(not math.isfinite(v) or v <= 0 for v in self.quote_usd.values()):
            raise ValueError("quote_usd must contain positive finite conversion assumptions")
        if any(not math.isfinite(v) or v < 0 or v >= 1 for v in self.fees.values()):
            raise ValueError("fee rates must be finite decimals in [0, 1)")
        for key, loan in self.financing.items():
            if set(loan) - {"lender", "currency", "product", "apr", "borrowed_fraction"}:
                raise ValueError(f"Unknown financing fields: {key}")
            for required in ("lender", "currency", "product", "borrowed_fraction"):
                if required not in loan:
                    raise ValueError(f"Missing financing {required}: {key}")
            if not all(isinstance(loan[n], str) and loan[n] for n in ("lender", "currency", "product")):
                raise ValueError(f"Financing identity must contain nonempty strings: {key}")
            if loan["borrowed_fraction"] is None:
                raise ValueError(f"Financing borrowed_fraction cannot be null: {key}")
            for name in ("apr", "borrowed_fraction"):
                v = loan.get(name)
                if v is not None and (not math.isfinite(v) or v < 0):
                    raise ValueError(f"Invalid financing {name}: {key}")
        return self


def load_config(path=None):
    if path is None:
        return Config().validate()
    with open(path) as handle:
        data = json.load(handle)
    unknown = set(data) - {f.name for f in fields(Config)}
    if unknown:
        raise ValueError(f"Unknown configuration keys: {sorted(unknown)}")
    return Config(**data).validate()
