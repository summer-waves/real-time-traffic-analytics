"""Run the whole pipeline: generate -> ingest -> metrics -> detect (-> store)."""
import argparse
from collections import Counter
from datetime import datetime

from src import anomalies, generator, metrics
from src.ingestion import read_events

PATH = "data/raw/traffic.jsonl"
START = datetime(2026, 11, 1, 10, 0)

INCIDENTS = [
    {"kind": "traffic_spike", "start_min": 30, "end_min": 40, "factor": 4},
    {"kind": "error_surge", "start_min": 60, "end_min": 70, "prob": 0.4},
    {"kind": "latency_spike", "start_min": 90, "end_min": 100, "factor": 5},
    {"kind": "regional_outage", "start_min": 105, "end_min": 110, "region": "eu-west"},
]


def detect_incidents(series, region_stats):
    """Run every detector; return incident dicts sorted by start time."""
    found = []

    def add(kind, indices, min_minutes=1):
        for start, end in anomalies.group_into_incidents(indices):
            if end - start + 1 < min_minutes:  # skip one-minute blips on noisy metrics
                continue
            found.append({"kind": kind, "region": None,
                          "start": series[start]["minute"], "end": series[end]["minute"]})

    add("traffic_volume", anomalies.rolling_zscore(
        [r["count"] for r in series], threshold=4, min_std=3))
    add("error_rate", anomalies.rolling_zscore(
        [r["error_rate"] for r in series], threshold=4, direction="up", min_std=0.01),
        min_minutes=2)
    add("p95_latency", anomalies.rolling_zscore(
        [r["p95_latency"] for r in series], threshold=4, direction="up", min_std=10),
        min_minutes=2)

    surges = anomalies.regional_error_surges(region_stats, threshold=0.5, min_requests=10)
    for region, start, end in anomalies.sustained_regional_surges(surges, min_minutes=3):
        found.append({"kind": "regional_errors", "region": region, "start": start, "end": end})

    return sorted(found, key=lambda i: i["start"])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--store", action="store_true",
                        help="also save events, metrics and incidents to PostgreSQL")
    args = parser.parse_args()

    events = generator.generate_traffic(START, 120, 100, seed=42, incidents=INCIDENTS,
                                        duplicate_rate=0.01)
    generator.write_jsonl(events, PATH, malformed_rate=0.002)

    stats = Counter()
    clean = list(read_events(PATH, stats))  # list() because we scan it several times
    print("Ingestion:", dict(stats))

    series = metrics.minute_series(clean)
    incidents = detect_incidents(series, metrics.region_minute_stats(clean))
    for inc in incidents:
        where = f" [{inc['region']}]" if inc["region"] else ""
        print(f"{inc['kind']}{where}: {inc['start']:%H:%M} - {inc['end']:%H:%M}")
    print("Top endpoints:", metrics.top_k(metrics.count_by(clean, "endpoint"), 3))

    if args.store:
        from src import storage  # imported here so the core runs without psycopg2
        conn = storage.connect()
        try:
            storage.init_schema(conn)
            n = storage.insert_events(conn, clean)
            storage.save_minute_metrics(conn, series)
            storage.save_incidents(conn, incidents)
            print(f"Stored {n} new events, {len(series)} minutes, {len(incidents)} incidents")
        finally:
            conn.close()


if __name__ == "__main__":
    main()