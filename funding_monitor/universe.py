"""Deterministic asset ranking, route construction and selection hysteresis."""
from collections import defaultdict
from .models import Route

print("LOADED universe.py:", __file__, flush=True)


def select_universe(instruments, config):
    print("ENTERED select_universe", flush=True)
    spots, perps = defaultdict(list), defaultdict(list)
    for instrument in instruments:
        if instrument.base in config.excluded_bases:
            continue
        target = spots if instrument.kind == "spot" else perps
        floor = config.min_spot_volume_usd if instrument.kind == "spot" else config.min_perp_volume_usd
        if instrument.volume_usd >= floor:
            target[instrument.base].append(instrument)
    print("SPOT Bases:", spots.keys())
    print("PERP Bases:", perps.keys())
    eligible = spots.keys() & perps.keys()
    print("ELIGIBLE:", eligible)
    ranked = sorted(eligible, key=lambda base: (-sum(x.volume_usd for x in spots[base]), base))
    if config.assets:
        selected = {b: "manual" for b in config.assets if b in eligible}
    else:
        n = config.bucket_size
        high = ranked[:n]
        remaining = [b for b in ranked if b not in high]
        median = (len(ranked) - 1) / 2
        mid = sorted(remaining, key=lambda b: (abs(ranked.index(b) - median), b))[:n]
        low = [b for b in reversed(ranked) if b not in high and b not in mid][:n]
        selected = {**dict.fromkeys(high, "high"), **dict.fromkeys(mid, "mid"), **dict.fromkeys(low, "low")}
    routes = [Route(s, p, bucket) for base, bucket in selected.items()
              for s in spots[base] for p in perps[base]]
    routes.sort(key=lambda r: r.key)
    ranking = [{"base": b, "rank": i + 1, "spot_volume_usd": sum(x.volume_usd for x in spots[b]),
                "bucket": selected.get(b)} for i, b in enumerate(ranked)]
    return routes, ranking


class SelectionGate:
    def __init__(self, confirmations):
        self.confirmations = confirmations
        self.current = None
        self.pending = None
        self.count = 0

    def accept(self, routes):
        signature = tuple(sorted((r.key, r.bucket) for r in routes))
        if self.current is None or signature == self.current:
            self.current, self.pending, self.count = signature, None, 0
            return True
        self.count = self.count + 1 if signature == self.pending else 1
        self.pending = signature
        if self.count >= self.confirmations:
            self.current, self.pending, self.count = signature, None, 0
            return True
        return False
