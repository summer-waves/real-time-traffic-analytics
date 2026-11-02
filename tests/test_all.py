import json
from collections import Counter
from datetime import datetime

from src import anomalies, generator, metrics
from src.ingestion import read_events
from src.validation import validate_event

START = datetime(2026, 11, 1, 10, 0)


def test_generator_is_reproducible():
    a = list(generator.generate_traffic(START, 5, 20, seed=1))
    b = list(generator.generate_traffic(START, 5, 20, seed=1))
    assert a == b


def test_percentile_edge_cases():
    assert metrics.percentile([], 95) is None
    assert metrics.percentile([7], 99) == 7
    assert metrics.percentile(list(range(1, 101)), 95) == 95


def test_median():
    assert metrics.median([3, 1, 2]) == 2
    assert metrics.median([1, 2, 3, 4]) == 2.5


def test_validation_rejects_bad_events():
    assert validate_event({}) == "missing request_id"
    ok = next(generator.generate_traffic(START, 1, 5))
    assert validate_event(ok) is None
    ok["status_code"] = True
    assert validate_event(ok) == "bad type for status_code"


def test_ingestion_handles_bad_lines_and_duplicates(tmp_path):
    p = tmp_path / "e.jsonl"
    good = next(generator.generate_traffic(START, 1, 5))
    p.write_text("not json\n" + json.dumps(good) + "\n" + json.dumps(good) + "\n")
    stats = Counter()
    out = list(read_events(str(p), stats))
    assert len(out) == 1
    assert stats["malformed"] == 1 and stats["duplicates"] == 1


def test_zscore_finds_spike_not_noise():
    values = [100, 101, 99, 100, 102, 98] * 3 + [500] + [100] * 3
    assert anomalies.rolling_zscore(values, window=10, threshold=4) == [18]


def test_group_into_incidents():
    assert anomalies.group_into_incidents([3, 4, 5, 9, 10]) == [(3, 5), (9, 10)]


def test_rolling_average_spike():
    assert anomalies.rolling_average_spike([10, 10, 10, 10, 50], 4, 3) == [4]


def test_sustained_regional_surges_drops_blips():
    from datetime import timedelta
    t0 = datetime(2026, 11, 1, 11, 0)
    surges = [("a", t0, 0.9),                                   # one-minute blip
              ("b", t0, 0.9), ("b", t0 + timedelta(minutes=1), 0.9),
              ("b", t0 + timedelta(minutes=2), 0.9)]            # real outage
    assert anomalies.sustained_regional_surges(surges, min_minutes=3) == [
        ("b", t0, t0 + timedelta(minutes=2))]


def test_pipeline_finds_all_planted_incidents(tmp_path):
    from src.main import INCIDENTS, START, detect_incidents
    path = tmp_path / "t.jsonl"
    generator.write_jsonl(
        generator.generate_traffic(START, 120, 100, seed=42, incidents=INCIDENTS), str(path))
    clean = list(read_events(str(path)))
    found = detect_incidents(metrics.minute_series(clean), metrics.region_minute_stats(clean))
    got = [(i["kind"], i["region"], f"{i['start']:%H:%M}", f"{i['end']:%H:%M}") for i in found]
    assert got == [
        ("traffic_volume", None, "10:30", "10:39"),
        ("error_rate", None, "11:00", "11:09"),
        ("p95_latency", None, "11:30", "11:39"),
        ("error_rate", None, "11:45", "11:49"),
        ("regional_errors", "eu-west", "11:45", "11:49"),
    ]