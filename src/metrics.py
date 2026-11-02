"""Phase 3: metrics, written by hand with the standard library only."""
import heapq
import math
from collections import Counter, defaultdict


def count_by(events, key):
    return Counter(e[key] for e in events)


def top_k(counter, k):
    """k most common (name, count) pairs, O(n log k)."""
    return heapq.nlargest(k, counter.items(), key=lambda kv: kv[1])


def average(values):
    return sum(values) / len(values) if values else None


def median(values):
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


def percentile(values, p):
    """Nearest-rank percentile: smallest value with >= p% of data at/below it."""
    if not values:
        return None
    ordered = sorted(values)
    rank = max(math.ceil(p / 100 * len(ordered)), 1)
    return ordered[rank - 1]


def is_error(event):
    return event["status_code"] >= 500


def error_rate(events):
    events = list(events)
    return sum(is_error(e) for e in events) / len(events) if events else 0.0


def to_minute(ts):
    return ts.replace(second=0, microsecond=0)


def minute_series(events):
    """One row per minute: count, error_rate, p95/p99 latency (sorted by time)."""
    buckets = defaultdict(lambda: {"count": 0, "errors": 0, "lat": []})
    for e in events:
        b = buckets[to_minute(e["timestamp"])]
        b["count"] += 1
        b["errors"] += is_error(e)
        b["lat"].append(e["latency_ms"])
    return [
        {"minute": m, "count": b["count"],
         "error_rate": b["errors"] / b["count"],
         "p95_latency": percentile(b["lat"], 95),
         "p99_latency": percentile(b["lat"], 99)}
        for m, b in sorted(buckets.items())
    ]


def region_minute_stats(events):
    """{(region, minute): [request_count, error_count]}"""
    stats = defaultdict(lambda: [0, 0])
    for e in events:
        s = stats[(e["region"], to_minute(e["timestamp"]))]
        s[0] += 1
        s[1] += is_error(e)
    return stats


def endpoint_performance(events):
    """{endpoint: {count, avg_latency, p95_latency, error_rate}}"""
    groups = defaultdict(list)
    for e in events:
        groups[e["endpoint"]].append(e)
    return {
        ep: {"count": len(evs),
             "avg_latency": average([e["latency_ms"] for e in evs]),
             "p95_latency": percentile([e["latency_ms"] for e in evs], 95),
             "error_rate": error_rate(evs)}
        for ep, evs in groups.items()
    }