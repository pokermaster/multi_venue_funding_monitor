from collections import deque
import statistics

class QuantitativeEngine:
    def __init__(self, spot_fee_rate: float, perp_fee_rate: float, borrow_apr: float = 0.0, window_size: int = 100):
        """
        Initialize calculation engine with exchange fees and capital costs.
        Fees should be passed as decimals (0.1% = 0.001)
        """
        self.spot_fee_rate = spot_fee_rate
        self.perp_fee_rate = perp_fee_rate
        self.borrow_apr = borrow_apr

        self.window_size = window_size
        self.basis_history = deque(maxlen=window_size)

    def calculate_basis_spread(self, perp_price: float, spot_price: float) -> float:
        """
        Computes real-time basis spread in basis points (bps)
        """
        if spot_price <=0:
            raise ValueError("Spot price must be strictly positive.")

        return ((perp_price - spot_price) / spot_price) * 10000

    def calculate_annualized_yield(self, current_funding_rate: float, epochs_per_day: int = 3) -> float:
        """
        Calculate gross annualized carry yield based on current funding rates.
        Default epochs_per_day is 3 (standard 8-hour funding windows).
        """
        return current_funding_rate * epochs_per_day * 365

    def calculate_net_adjusted_yield(self, gross_annual_yield: float, holding_period_days: float) -> dict:
        """
        Adjusts the yield profile by deducting maker/taker fees and borrowing costs.
        Assumes a full delta-neutral cycle: buying spot, shorting perp, and unwinding both.
        """

        # Round-trip trading fees (Entry + Exit for both Spot and Perp)
        round_trip_fees = (self.spot_fee_rate * 2) + (self.perp_fee_rate * 2)

        # Prorate gross yield and borrow costs to the specific holding period
        prorated_gross = gross_annual_yield * (holding_period_days / 365)
        prorated_borrow = self.borrow_apr * (holding_period_days / 365)

        # Calculate net yield for period
        net_period_return = prorated_gross - round_trip_fees - prorated_borrow

        # Reannualize net return
        net_annualized = net_period_return * (365 / holding_period_days)

        return {
            "round_trip_fee_pct": round_trip_fees * 100,
            "period_net_return_pct": net_period_return * 100,
            "annualized_net_yield_pct": net_annualized * 100
        }

    def get_smoothed_basis(self, current_basis: float) -> float:
        """
        Append the current basis to the rolling window and returns the moving average.
        """
        self.basis_history.append(current_basis)

        # only calculate average if we have sufficient data points
        if len(self.basis_history) < self.window_size // 2:
            return current_basis

        return statistics.mean(self.basis_history)

    def evaluate_entry_signal(self, smoothed_basis: float, net_annual_yield: float, min_yield_threshold: float = 5.0) -> bool:
        """
        Determines if current market condition meets the minimum criteria to enter
        """
        if smoothed_basis > 0 and net_annual_yield > min_yield_threshold:
            return True
        return False
    