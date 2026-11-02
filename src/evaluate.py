"""Robustness check: run the whole pipeline on many random seeds and score the
detectors against the planted incidents (recall, boundary accuracy, false alarms).

Run from the project root:  python -m src.evaluate [number_of_seeds]
"""
import os
import sys
import tempfile
from collections import Counter
from datetime import timedelta

from src import generator, metrics
from src.ingestion import read_events
from src.main import INCIDENTS, START, detect_incidents

# Which detector kinds count as a "hit" for each planted incident. The regional
# outage also raises the system-wide error rate, so either detector may catch it.
HIT_KINDS = {
    "traffic_spike": {"traffic_volume"},
    "error_surge": {"error_rate"},
    "latency_spike": {"p95_latency"},
    "regional_outage": {"regional_errors", "error_rate"},
}


def planted_windows():
    """(kind, first_minute, last_minute) for every planted incident."""
    return [(i["kind"],
             START + timedelta(minutes=i["start_min"]),
             START + timedelta(minutes=i["end_min"] - 1))
            for i in INCIDENTS]


def detect_for_seed(seed):
    """Generate a fresh dataset for `seed`, ingest it and return detected incidents."""
    with tempfile.TemporaryDirectory() as folder:
        path = os.path.join(folder, "traffic.jsonl")
        events = generator.generate_traffic(START, 120, 100, seed=seed,
                                            incidents=INCIDENTS, duplicate_rate=0.01)
        generator.write_jsonl(events, path, malformed_rate=0.002)
        clean = list(read_events(path))
    return detect_incidents(metrics.minute_series(clean), metrics.region_minute_stats(clean))


def main(num_seeds=40):
    planted = planted_windows()
    detected, exact, false_alarms = Counter(), Counter(), []

    for seed in range(num_seeds):
        found = detect_for_seed(seed)
        for d in found:  # a detection that overlaps no planted window is a false alarm
            if not any(d["start"] <= end and d["end"] >= start for _, start, end in planted):
                false_alarms.append((seed, d))
        for kind, start, end in planted:
            hits = [d for d in found if d["kind"] in HIT_KINDS[kind]
                    and d["start"] <= end and d["end"] >= start]
            if hits:
                detected[kind] += 1
                if any(d["start"] == start and d["end"] == end for d in hits):
                    exact[kind] += 1

    print(f"Seeds evaluated: {num_seeds}")
    for kind, _, _ in planted:
        print(f"  {kind:16s} detected {detected[kind]}/{num_seeds}, "
              f"exact start and end minute {exact[kind]}/{num_seeds}")
    print(f"False-alarm incidents: {len(false_alarms)} "
          f"(in {len({s for s, _ in false_alarms})} of {num_seeds} seeds)")
    for seed, d in false_alarms:
        print(f"  seed {seed}: {d['kind']} {d['start']:%H:%M}-{d['end']:%H:%M}")


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 40)
