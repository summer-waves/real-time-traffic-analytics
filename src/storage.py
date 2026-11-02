"""Phase 5: PostgreSQL persistence and queries (psycopg2)."""
import os
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

DEFAULT_DSN = "postgresql://postgres:postgres@localhost:5432/traffic"
SCHEMA_PATH = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"


def connect(dsn=None):
    """Use DATABASE_URL if set, otherwise the docker-compose defaults."""
    return psycopg2.connect(dsn or os.environ.get("DATABASE_URL", DEFAULT_DSN))


def init_schema(conn):
    with conn.cursor() as cur:
        cur.execute(SCHEMA_PATH.read_text())
    conn.commit()


def _batched(iterable, size):
    """Group any iterable into lists of `size` without loading it all."""
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def insert_events(conn, events, batch_size=1000):
    """Bulk insert; rows whose request_id already exists are skipped.
    Returns the number of rows actually inserted."""
    sql = """
        INSERT INTO events
            (request_id, ts, user_type, action, endpoint, region, status_code, latency_ms)
        VALUES %s
        ON CONFLICT (request_id) DO NOTHING
    """
    inserted = 0
    with conn.cursor() as cur:
        for batch in _batched(events, batch_size):
            rows = [(e["request_id"], e["timestamp"], e.get("user_type"), e.get("action"),
                     e["endpoint"], e["region"], e["status_code"], e["latency_ms"])
                    for e in batch]
            # page_size=len(rows) => one statement, so cur.rowcount is the true count
            execute_values(cur, sql, rows, page_size=len(rows))
            inserted += cur.rowcount
    conn.commit()
    return inserted


def save_minute_metrics(conn, series):
    """Upsert, so re-running the pipeline updates rows instead of failing."""
    sql = """
        INSERT INTO minute_metrics (minute, request_count, error_rate, p95_latency, p99_latency)
        VALUES %s
        ON CONFLICT (minute) DO UPDATE SET
            request_count = EXCLUDED.request_count,
            error_rate    = EXCLUDED.error_rate,
            p95_latency   = EXCLUDED.p95_latency,
            p99_latency   = EXCLUDED.p99_latency
    """
    rows = [(r["minute"], r["count"], r["error_rate"], r["p95_latency"], r["p99_latency"])
            for r in series]
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()


def save_incidents(conn, incidents):
    """Incidents are derived data, so we replace them wholesale on every run."""
    with conn.cursor() as cur:
        cur.execute("TRUNCATE incidents RESTART IDENTITY")
        execute_values(
            cur,
            "INSERT INTO incidents (kind, region, start_ts, end_ts) VALUES %s",
            [(i["kind"], i["region"], i["start"], i["end"]) for i in incidents],
        )
    conn.commit()


def _fetch(conn, sql, params=None):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params or None)
        return cur.fetchall()


def _where(region=None, endpoint=None):
    """Build an optional WHERE clause. Values go in `params` (never into the SQL
    text), so psycopg2 escapes them and SQL injection is not possible."""
    clauses, params = [], []
    if region:
        clauses.append("region = %s")
        params.append(region)
    if endpoint:
        clauses.append("endpoint = %s")
        params.append(endpoint)
    return ("WHERE " + " AND ".join(clauses)) if clauses else "", params


def fetch_minute_metrics(conn):
    return _fetch(conn, "SELECT * FROM minute_metrics ORDER BY minute")


def fetch_minute_series(conn, region=None, endpoint=None):
    """Per-minute metrics. With no filter, read the pre-aggregated table (fast).
    With a filter, compute the same columns from the raw events."""
    if not region and not endpoint:
        return fetch_minute_metrics(conn)
    where, params = _where(region, endpoint)
    return _fetch(conn, f"""
        SELECT date_trunc('minute', ts)                        AS minute,
               COUNT(*)                                        AS request_count,
               AVG((status_code >= 500)::int)::float           AS error_rate,
               percentile_disc(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95_latency,
               percentile_disc(0.99) WITHIN GROUP (ORDER BY latency_ms) AS p99_latency
        FROM events {where}
        GROUP BY date_trunc('minute', ts)
        ORDER BY minute""", params)


def fetch_summary(conn, region=None, endpoint=None):
    """One row of overall numbers for the summary tiles."""
    where, params = _where(region, endpoint)
    rows = _fetch(conn, f"""
        SELECT COUNT(*)                                        AS requests,
               AVG(latency_ms)::float                          AS avg_latency,
               percentile_disc(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95_latency,
               percentile_disc(0.99) WITHIN GROUP (ORDER BY latency_ms) AS p99_latency,
               AVG((status_code >= 500)::int)::float           AS error_rate
        FROM events {where}""", params)
    return rows[0]


def fetch_status_codes(conn, region=None, endpoint=None):
    where, params = _where(region, endpoint)
    return _fetch(conn, f"""
        SELECT status_code, COUNT(*) AS requests
        FROM events {where}
        GROUP BY status_code ORDER BY status_code""", params)


def fetch_region_traffic(conn, endpoint=None):
    where, params = _where(endpoint=endpoint)
    return _fetch(conn, f"""
        SELECT region, COUNT(*) AS requests
        FROM events {where}
        GROUP BY region ORDER BY requests DESC""", params)


def fetch_region_error_series(conn, endpoint=None):
    """Per-region, per-minute 5xx rate (one line per region on the dashboard)."""
    where, params = _where(endpoint=endpoint)
    return _fetch(conn, f"""
        SELECT region,
               date_trunc('minute', ts)              AS minute,
               COUNT(*)                              AS requests,
               AVG((status_code >= 500)::int)::float AS error_rate
        FROM events {where}
        GROUP BY region, date_trunc('minute', ts)
        ORDER BY minute, region""", params)


def fetch_endpoint_performance(conn, region=None):
    # percentile_disc picks an actual value, matching our nearest-rank percentile().
    # (percentile_cont would interpolate between values.)
    where, params = _where(region=region)
    return _fetch(conn, f"""
        SELECT endpoint,
               COUNT(*)                                        AS requests,
               AVG(latency_ms)::float                          AS avg_latency,
               percentile_disc(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95_latency,
               AVG((status_code >= 500)::int)::float           AS error_rate
        FROM events {where}
        GROUP BY endpoint ORDER BY requests DESC""", params)


def fetch_filter_options(conn):
    """Values for the dashboard dropdowns."""
    return {
        "regions": [r["region"] for r in
                    _fetch(conn, "SELECT DISTINCT region FROM events ORDER BY region")],
        "endpoints": [r["endpoint"] for r in
                      _fetch(conn, "SELECT DISTINCT endpoint FROM events ORDER BY endpoint")],
    }


def fetch_incidents(conn):
    return _fetch(conn, "SELECT kind, region, start_ts, end_ts FROM incidents ORDER BY start_ts")