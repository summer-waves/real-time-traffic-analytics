"""Phase 1: synthetic e-commerce traffic generator with controlled incidents."""
import json
import random
from datetime import timedelta

REGIONS = ["us-west", "us-east", "eu-west", "ap-south"]

# endpoint -> (action, user_type, relative weight)
ENDPOINTS = {
    "/search": ("search", "buyer", 50),
    "/listing": ("view_listing", "buyer", 25),
    "/cart": ("add_to_cart", "buyer", 10),
    "/checkout": ("purchase", "buyer", 5),
    "/seller/listings": ("manage_listing", "seller", 5),
    "/login": ("login", "buyer", 5),
}
_NAMES = list(ENDPOINTS)
_WEIGHTS = [v[2] for v in ENDPOINTS.values()]


def generate_event(rng, ts, n):
    """Build one normal (healthy) event."""
    endpoint = rng.choices(_NAMES, weights=_WEIGHTS)[0]
    action, user_type, _ = ENDPOINTS[endpoint]
    return {
        "request_id": f"r-{n:08d}",
        "timestamp": ts.isoformat(),
        "user_type": user_type,
        "action": action,
        "endpoint": endpoint,
        "region": rng.choice(REGIONS),
        "status_code": rng.choices([200, 404, 500], weights=[97, 2, 1])[0],
        # lognormal = long right tail, like real latencies (median ~90 ms)
        "latency_ms": round(rng.lognormvariate(4.5, 0.5), 1),
    }


def _apply_incident(rng, event, inc):
    """Degrade a single event according to an active incident."""
    kind = inc["kind"]
    if kind == "error_surge" and rng.random() < inc.get("prob", 0.4):
        event["status_code"] = 500
    elif kind == "latency_spike":
        event["latency_ms"] = round(event["latency_ms"] * inc.get("factor", 5), 1)
    elif (kind == "regional_outage" and event["region"] == inc["region"]
          and rng.random() < inc.get("prob", 0.9)):
        event["status_code"] = 503


def generate_traffic(start, minutes, events_per_minute, seed=42,
                     incidents=(), duplicate_rate=0.0):
    """Yield events minute by minute (a generator, never a big list).

    incidents: dicts like {"kind": "traffic_spike", "start_min": 30,
    "end_min": 40, "factor": 4}. Kinds: traffic_spike, traffic_drop,
    error_surge, latency_spike, regional_outage (needs "region").
    """
    rng = random.Random(seed)  # own RNG => same seed, same data
    n = 0
    for minute in range(minutes):
        active = [i for i in incidents if i["start_min"] <= minute < i["end_min"]]
        rate = events_per_minute
        for inc in active:
            if inc["kind"] in ("traffic_spike", "traffic_drop"):
                rate *= inc["factor"]
        count = rng.randint(int(rate * 0.9), int(rate * 1.1))  # natural noise
        batch = []
        for _ in range(count):
            n += 1
            ts = start + timedelta(minutes=minute, seconds=rng.randrange(60))
            event = generate_event(rng, ts, n)
            for inc in active:
                _apply_incident(rng, event, inc)
            batch.append(event)
        batch.sort(key=lambda e: e["timestamp"])
        for e in batch:
            yield e
            if duplicate_rate and rng.random() < duplicate_rate:
                yield dict(e)  # same request_id again, to test dedup


def write_jsonl(events, path, malformed_rate=0.0, seed=0):
    """Write one JSON object per line; optionally sprinkle in broken lines."""
    rng = random.Random(seed)
    with open(path, "w") as f:
        for e in events:
            if malformed_rate and rng.random() < malformed_rate:
                f.write('{"request_id": "broken", \n')
            f.write(json.dumps(e) + "\n")
