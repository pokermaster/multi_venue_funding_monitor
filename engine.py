"""Pure calculations; rates are decimals internally and display yields are percent."""
from collections import deque
import math
import statistics


def finite(value, name, *, positive=False, nonnegative=False):
    value = float(value)
    if not math.isfinite(value) or (positive and value <= 0) or (nonnegative and value < 0):
        raise ValueError(f"Invalid {name}: {value}")
    return value


class QuantitativeEngine:
    """One instance per route. History is sampled by the application clock."""

    def __init__(self, spot_fee_rate, perp_fee_rate, borrow_apr=0.0, window_size=50):
        self.spot_fee_rate = finite(spot_fee_rate, "spot fee", nonnegative=True)
        self.perp_fee_rate = finite(perp_fee_rate, "perp fee", nonnegative=True)
        self.borrow_apr = finite(borrow_apr, "borrow APR", nonnegative=True)
        if not isinstance(window_size, int) or window_size < 1:
            raise ValueError("window_size must be a positive integer")
        self.window_size = window_size
        self.basis_history = deque(maxlen=window_size)

    def calculate_basis_spread(self, perp_price, spot_price):
        spot = finite(spot_price, "spot price", positive=True)
        perp = finite(perp_price, "perp price", positive=True)
        return (perp - spot) / spot * 10_000

    def calculate_annualized_yield(self, current_funding_rate, epochs_per_day=3):
        return (finite(current_funding_rate, "funding rate")
                * finite(epochs_per_day, "epochs/day", positive=True) * 365)

    def calculate_net_adjusted_yield(self, gross_annual_yield, holding_period_days,
                                     borrow_apr, borrowed_fraction=1.0, slippage_bps=0.0):
        days = finite(holding_period_days, "holding days", positive=True)
        borrow = finite(borrow_apr, "borrow APR", nonnegative=True)
        fraction = finite(borrowed_fraction, "borrowed fraction", nonnegative=True)
        gross = finite(gross_annual_yield, "gross yield")
        fees = 2 * (self.spot_fee_rate + self.perp_fee_rate)
        cost = fees + finite(slippage_bps, "round-trip slippage", nonnegative=True) / 10_000
        period = (gross - borrow * fraction) * days / 365 - cost
        return {"round_trip_fee_pct": fees * 100,
                "round_trip_cost_bps": cost * 10_000,
                "period_net_return_pct": period * 100,
                "annualized_net_yield_pct": period * 365 / days * 100}

    def get_smoothed_basis(self, current_basis):
        self.basis_history.append(finite(current_basis, "basis"))
        return statistics.mean(self.basis_history)

    def evaluate_entry_signal(self, smoothed_basis, net_annual_yield, min_yield_threshold=5.0):
        return smoothed_basis > 0 and net_annual_yield > min_yield_threshold


def average_fill(levels, base_quantity):
    """Walk an already sorted book. None means visible depth cannot fill it."""
    remaining = finite(base_quantity, "quantity", positive=True)
    cost = 0.0
    for price, size in levels:
        amount = min(remaining, size)
        cost += amount * price
        remaining -= amount
        if remaining <= base_quantity * 1e-10:
            return cost / base_quantity
    return None
