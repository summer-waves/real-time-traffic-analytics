-- Raw events. request_id as PRIMARY KEY gives us deduplication for free.
CREATE TABLE IF NOT EXISTS events (
    request_id  TEXT PRIMARY KEY,
    ts          TIMESTAMP NOT NULL,
    user_type   TEXT,
    action      TEXT,
    endpoint    TEXT NOT NULL,
    region      TEXT NOT NULL,
    status_code INTEGER NOT NULL,
    latency_ms  DOUBLE PRECISION NOT NULL
);
-- Time-range queries ("last hour") and per-region time queries.
CREATE INDEX IF NOT EXISTS idx_events_ts        ON events (ts);
CREATE INDEX IF NOT EXISTS idx_events_region_ts ON events (region, ts);

-- Pre-aggregated per-minute metrics (what the dashboard charts read).
CREATE TABLE IF NOT EXISTS minute_metrics (
    minute        TIMESTAMP PRIMARY KEY,
    request_count INTEGER NOT NULL,
    error_rate    DOUBLE PRECISION NOT NULL,
    p95_latency   DOUBLE PRECISION,
    p99_latency   DOUBLE PRECISION
);

-- Detected incidents (region is NULL for whole-system incidents).
CREATE TABLE IF NOT EXISTS incidents (
    id       SERIAL PRIMARY KEY,
    kind     TEXT NOT NULL,
    region   TEXT,
    start_ts TIMESTAMP NOT NULL,
    end_ts   TIMESTAMP NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_incidents_start ON incidents (start_ts);
