"""Phase 4: statistical anomaly detection on per-minute series."""
import math
import statistics
from collections import defaultdict, deque
from datetime import timedelta


def static_threshold(values, limit):
    """Indices where the value exceeds a fixed limit."""
    return [i for i, v in enumerate(values) if v > limit]


def rolling_average_spike(values, window, factor):
    """Indices where value > factor * average of the previous `window` values.
    Running sum keeps this O(n)."""
    flagged, recent, total = [], deque(), 0.0
    for i, v in enumerate(values):
        if len(recent) == window and v > factor * (total / window):
            flagged.append(i)
        recent.append(v)
        total += v
        if len(recent) > window:
            total -= recent.popleft()
    return flagged


def rolling_zscore(values, window=15, threshold=4.0, direction="both", min_std=0.0):
    """Indices whose z-score vs the recent *normal* baseline exceeds threshold.

    Flagged points are not added to the baseline, so an incident does not
    inflate the mean/std and hide itself. direction: "up", "down" or "both".
    min_std stops tiny natural wiggles from looking huge on quiet metrics.
    """
    baseline = deque(maxlen=window)
    flagged = []
    for i, v in enumerate(values):
        if len(baseline) == window:
            mean = statistics.fmean(baseline)
            std = max(statistics.pstdev(baseline), min_std)
            z = (v - mean) / std if std else (0.0 if v == mean else math.inf)
            hit = ((direction in ("up", "both") and z > threshold) or
                   (direction in ("down", "both") and z < -threshold))
            if hit:
                flagged.append(i)
                continue
        baseline.append(v)
    return flagged


def group_into_incidents(indices, max_gap=1):
    """Merge nearby flagged indices into (start, end) ranges."""
    incidents = []
    for i in sorted(set(indices)):
        if incidents and i - incidents[-1][1] <= max_gap:
            incidents[-1][1] = i
        else:
            incidents.append([i, i])
    return [tuple(r) for r in incidents]


def regional_error_surges(region_stats, threshold=0.2, min_requests=10):
    """(region, minute, rate) rows where a region's error rate is too high."""
    return sorted(
        (region, minute, errs / count)
        for (region, minute), (count, errs) in region_stats.items()
        if count >= min_requests and errs / count > threshold
    )


def sustained_regional_surges(surges, min_minutes=3):
    """Keep only regions with >= min_minutes consecutive bad minutes.

    Returns sorted (region, start_minute, end_minute). One-minute blips are
    usually small-sample noise, not a real outage.
    """
    by_region = defaultdict(list)
    for region, minute, _rate in surges:
        by_region[region].append(minute)
    result = []
    for region, minutes in by_region.items():
        minutes.sort()
        run = [minutes[0]]
        for m in minutes[1:] + [None]:  # None is a sentinel that closes the last run
            if m is not None and m - run[-1] <= timedelta(minutes=1):
                run.append(m)
                continue
            if len(run) >= min_minutes:
                result.append((region, run[0], run[-1]))
            run = [m]
    return sorted(result)