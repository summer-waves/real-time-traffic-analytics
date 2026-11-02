# Real-Time Traffic Analytics & Anomaly Detection

A Python pipeline that generates synthetic e-commerce traffic containing **known, controlled incidents**, ingests and validates it as a stream, computes service-level metrics, detects anomalies with statistical methods, persists everything to PostgreSQL, and serves an interactive Plotly Dash dashboard.

The core (generation, ingestion, metrics, detection) uses only the Python standard library, so every algorithm can be written, reasoned about and tested in isolation. Because the incidents are planted, detector quality is *measurable*: each detection is scored against ground truth (recall, boundary accuracy, false alarms) instead of being judged by eye.

## Contents

1. [Results at a glance](#1-results-at-a-glance)
2. [Architecture](#2-architecture)
3. [Data](#3-data)
4. [Methodology](#4-methodology)
5. [Findings from the dashboard](#5-findings-from-the-dashboard)
6. [Detector evaluation and tuning log](#6-detector-evaluation-and-tuning-log)
7. [Limitations and threats to validity](#7-limitations-and-threats-to-validity)
8. [Next steps](#8-next-steps)
9. [Running the project](#9-running-the-project)
10. [Repository layout](#10-repository-layout)

---

## 1. Results at a glance

| | |
|---|---|
| Dataset | 120 simulated minutes (2026-11-01 10:00 to 11:59), about 100 requests/min baseline, seed 42 |
| Raw to clean | 15,287 lines, of which 30 malformed JSON lines and 171 duplicate request IDs were rejected, leaving **15,086** events |
| Planted incidents detected | **4 of 4**, each with the exact planted start and end minute |
| False alarms (seed 42) | 0 |
| Robustness (100 seeds) | 100% recall and exact boundaries for all four incident types; 2 false alarms in 100 runs (both 2-minute P95 alerts, diagnosed in [section 6](#6-detector-evaluation-and-tuning-log)) |
| Whole-run figures | 129 ms average latency, 338 ms P95, 752 ms P99, 4.16% HTTP 5xx rate |

| Planted incident | Window | Detector that fired | Detected window |
|---|---|---|---|
| Traffic spike (4x arrival rate) | 10:30 to 10:39 | `traffic_volume` | 10:30 to 10:39 |
| Error surge (40% of requests forced to HTTP 500, all regions) | 11:00 to 11:09 | `error_rate` | 11:00 to 11:09 |
| Latency spike (latency x5) | 11:30 to 11:39 | `p95_latency` | 11:30 to 11:39 |
| Regional outage (eu-west, 90% of requests answered 503) | 11:45 to 11:49 | `regional_errors` (eu-west) and `error_rate` (system-wide) | 11:45 to 11:49 (both) |

The regional outage is reported twice, once per detector, so the run produces five incident records for four planted incidents.

---

## 2. Architecture

```
generator.py ──► data/raw/traffic.jsonl ──► ingestion.py ──► metrics.py ──► anomalies.py
 seeded RNG,      one JSON object           stream, validate,  per-minute     rolling z-score,
 incident         per line                  deduplicate        series,        persistence rules
 injection                                                     percentiles
                                                  │                              │
                                                  └────────► PostgreSQL ◄────────┘
                                                      events · minute_metrics · incidents
                                                                  │
                                                          dashboard/app.py
                                                          (Plotly Dash, filters)
```

| Module | Responsibility | Key functions |
|---|---|---|
| `src/generator.py` | Seeded synthetic traffic with controlled incidents | `generate_event`, `generate_traffic`, `_apply_incident`, `write_jsonl` |
| `src/validation.py` | Schema and type checks for one event | `validate_event` |
| `src/ingestion.py` | Streaming JSONL reader, dedup, rejection counters | `read_events` (generator) |
| `src/metrics.py` | Aggregation and statistics, written by hand | `percentile`, `median`, `minute_series`, `region_minute_stats`, `endpoint_performance`, `top_k` |
| `src/anomalies.py` | Statistical detectors | `rolling_zscore`, `rolling_average_spike`, `static_threshold`, `group_into_incidents`, `regional_error_surges`, `sustained_regional_surges` |
| `src/storage.py` | PostgreSQL persistence and dashboard queries | `insert_events`, `save_minute_metrics`, `fetch_*` |
| `src/main.py` | Orchestration and incident detection rules | `detect_incidents`, `main` |
| `src/evaluate.py` | Multi-seed scoring against planted incidents | `main` |
| `dashboard/app.py` | Dash application with region and endpoint filters | `serve_layout`, `update` |

---

## 3. Data

### 3.1 Event schema

One JSON object per line:

```json
{"request_id": "r-00000123", "timestamp": "2026-11-01T10:15:32", "user_type": "buyer",
 "action": "search", "endpoint": "/search", "region": "us-west", "status_code": 200, "latency_ms": 142.3}
```

| Field | Role downstream |
|---|---|
| `request_id` | Unique key: deduplication in ingestion and the primary key in PostgreSQL |
| `timestamp` | Per-minute bucketing and rolling windows |
| `region`, `endpoint` | Group-by keys for regional and endpoint metrics |
| `status_code` | 5xx error rate and failure-mode classification |
| `latency_ms` | Average, median, P95 and P99 |
| `user_type`, `action` | Descriptive context (buyer or seller; search, view_listing, add_to_cart, purchase, manage_listing, login) |

### 3.2 Generator parameters

| Property | Setting |
|---|---|
| Arrival rate | 100 requests/min, with the per-minute count drawn uniformly from 90 to 110 (plus a random second within the minute) |
| Endpoint mix | `/search` 50%, `/listing` 25%, `/cart` 10%, `/checkout` 5%, `/seller/listings` 5%, `/login` 5% |
| Region | Uniform over `ap-south`, `eu-west`, `us-east`, `us-west` |
| Baseline status codes | 200 at 97%, 404 at 2%, 500 at 1% |
| Latency | Lognormal(mu = 4.5, sigma = 0.5) in ms, a long right tail like real service latencies |
| Reproducibility | One dedicated `random.Random(seed)` instance, so the same seed always produces the same file |

Analytic properties of the baseline latency distribution, compared with the observed values outside the incident window:

| | Median | Mean | P95 | P99 |
|---|---|---|---|---|
| Analytic, lognormal(4.5, 0.5) | 90.0 ms | 102.0 ms | 204.9 ms | 288.0 ms |
| Observed, outside the latency incident | 90.8 ms | 102.6 ms | 205.9 ms | 288.6 ms |

### 3.3 Injected data-quality faults and the ingestion funnel

The generator also injects ingestion faults: about 0.2% of lines are preceded by a truncated JSON fragment, and 1% of events are emitted twice with the same `request_id`.

| Stage | Count |
|---|---|
| Lines in the file | 15,287 |
| Malformed JSON, skipped (counted as `malformed`) | 30 |
| Parsed events | 15,257 |
| Duplicate `request_id`, skipped (counted as `duplicates`) | 171 |
| **Accepted events** | **15,086** |

Ingestion streams the file one line at a time. Memory grows only with the set of unique IDs used for deduplication.

### 3.4 Planted incidents and what the data actually shows

| Metric in the incident window | Expected from the generator | Observed (seed 42) |
|---|---|---|
| Arrival rate, 10:30 to 10:39 | 400 req/min (4 x 100) | 400.7 req/min (4,007 requests) |
| 5xx rate, 11:00 to 11:09 | 0.40 + 0.60 x 0.01 = 40.6% | 39.4% (n = 1,006) |
| Latency median / P95 / P99, 11:30 to 11:39 | 5x healthy: 450 / 1,025 / 1,440 ms | 438 / 1,001 / 1,430 ms (n = 997) |
| 5xx rate, eu-west, 11:45 to 11:49 | 0.90 + 0.10 x 0.01 = 90.1% | 91.5% (n = 106) |
| 5xx rate, other three regions, same minutes | 1% baseline | 0.5% |
| 5xx rate outside both error windows | 1.0% | 0.98% |

The observed values match the generator's parameters, which confirms that the injected incidents behave as designed before any detector is evaluated.

---

## 4. Methodology

### 4.1 Metrics (`src/metrics.py`)

- **Percentiles** use the nearest-rank definition: the smallest value with at least p% of the data at or below it, i.e. the element at rank `ceil(p/100 x n)` of the sorted list. This is the same definition as PostgreSQL's `percentile_disc`, so the Python and SQL implementations agree. Interpolating definitions (such as `numpy.percentile` or `percentile_cont`) can return values that never occurred. Cost is O(n log n) per call because of the sort; a selection algorithm would make a single percentile O(n) on average.
- **Per-minute series**: each event is assigned to `timestamp.replace(second=0, microsecond=0)` in a dictionary of buckets (O(n)), then summarised into request count, 5xx rate, P95 and P99.
- **Error rate** counts HTTP 5xx only. 4xx responses (here, 404) are client-side outcomes and are excluded from the error signal.
- **Region-minute statistics** (request and error counts per region per minute) feed the regional detector; **endpoint performance** (count, average, P95, 5xx rate) feeds the dashboard.

### 4.2 Anomaly detection (`src/anomalies.py`, rules in `src/main.py`)

| Signal | Detector | Parameters | Persistence rule |
|---|---|---|---|
| Traffic volume | Rolling z-score, two-sided | window 15, threshold 4, sigma floor 3 req/min | none |
| 5xx rate | Rolling z-score, upper only | window 15, threshold 4, sigma floor 0.01 (one percentage point) | at least 2 consecutive minutes |
| P95 latency | Rolling z-score, upper only | window 15, threshold 4, sigma floor 10 ms | at least 2 consecutive minutes |
| Regional 5xx | Static threshold per region-minute | rate above 50% with at least 10 requests | at least 3 consecutive minutes in that region |

Design notes:

- **Contamination-free baseline.** The z-score is `(x - mean) / max(std, sigma_floor)` against the previous 15 minutes. Flagged points are *not* added to the baseline. Otherwise an incident inflates its own mean and standard deviation and partly hides itself.
- **Sigma floors.** On a quiet signal (5xx rate near 1%) the standard deviation approaches zero and trivial wiggles score enormous z-values. The floor sets the smallest change worth caring about for each signal.
- **Cold start.** The first 15 minutes only fill the baseline, so nothing can be flagged during them.
- **Persistence.** Per-minute percentiles and regional rates are computed from small samples, so single-minute excursions are common (see sections 5 and 6). `group_into_incidents` merges flagged minutes (gap of at most one minute) into ranges, and the persistence rules above discard short ones.
- **Complexity.** The z-score detector recomputes the mean and standard deviation of the window at every step, which is O(n x w) time and O(w) space. A running-sum or Welford formulation would make it O(n).
- **Present but not in the default pipeline.** `rolling_average_spike` (unit-tested) and `static_threshold` are implemented as building blocks; `detect_incidents` uses the z-score detectors.

### 4.3 Storage design (`sql/schema.sql`, `src/storage.py`)

| Table | Design |
|---|---|
| `events` | `request_id` is the primary key, so inserting with `ON CONFLICT DO NOTHING` makes ingestion idempotent. Indexes on `(ts)` and `(region, ts)` serve time-range and per-region time queries. Bulk inserts use `execute_values` in batches of 1,000. |
| `minute_metrics` | Pre-aggregated per-minute series, upserted on `minute`. |
| `incidents` | Derived data, replaced wholesale on each run. |

The dashboard's unfiltered view reads the pre-aggregated `minute_metrics` table. When a region or endpoint filter is active, the same columns are recomputed from `events` with `percentile_disc`. Filter values are passed as query parameters, never concatenated into SQL.

---

## 5. Findings from the dashboard

Two dropdowns filter the page by region and endpoint. A breakdown chart ignores the filter it is broken down by: the region charts respond only to the endpoint filter, and the endpoint charts respond only to the region filter. Shaded bands on the volume chart mark detected incidents; the darker band at 11:45 to 11:49 is two overlapping detections.

### 5.1 System-wide view

<p align="center">
  <img src="docs/images/11-latency-all-traffic.png" width="780" alt="System-wide P95 and P99 latency per minute"><br>
  <em>Figure 1. System-wide P95 and P99 latency per minute.</em>
</p>

**Observation.** In healthy minutes P95 sits near 200 ms (range 164 to 290 ms) and P99 mostly between about 225 and 485 ms (5th to 95th percentile), with occasional excursions up to 679 ms. From 11:30 to 11:39 P95 rises to roughly 800 to 1,350 ms and P99 to about 1,750 ms, then both return to baseline at 11:40 with no recovery tail, because the injected fault is stateless.

**Interpretation.**
- The incident-window quantiles (median 438, P95 1,001, P99 1,430 ms) are about five times the healthy analytic values (90, 205, 288 ms). Multiplying a lognormal by 5 shifts its log-mean by ln 5 (about 1.61) and leaves the shape unchanged, which is exactly what the data shows.
- The whole-run tiles (average 129 ms, P95 338 ms, P99 752 ms) differ sharply from the healthy-only values (102.6, 205.9, 288.6 ms). The degraded window holds 997 of 15,086 requests (6.6%), more than the 5% and 1% tail budgets of P95 and P99, so the whole-run percentiles are quantiles of a *mixture*. A two-component lognormal mixture with that 6.6% weight reproduces the observed values: mean 129.0 ms (observed 129.0), P95 344 ms (observed 338), P99 754 ms (observed 752).

**Implication.** Long-window aggregates blur incidents: a 10-minute fault moved the run's P99 by a factor of 2.6. Alerting has to run on the per-minute series.

**Estimator noise at small n.** With about 100 requests per minute, nearest-rank P99 is the sample maximum when fewer than 100 requests arrive in the minute, and the second-largest value for 100 to 199 requests. At 11:59 only 91 requests arrived, so the P99 spike at the right edge of the chart (about 680 ms) is a single `/search` request taking 679 ms, not an incident. Three of the 100 healthy minutes have a P99 above 500 ms. For a lognormal, the asymptotic standard error of a sample percentile is `sqrt(p(1-p)/n) / f(x_p)`, which gives about 22 ms for P95 and about 54 ms for P99 at n = 100. The empirical standard deviation of healthy-minute P95 over 40 seeds is 22.2 ms, in agreement. This is why the latency detector runs on P95 and P99 is displayed but not alerted on.

<p align="center">
  <img src="docs/images/10-error-rate-all-traffic.png" width="780" alt="System-wide HTTP 5xx error rate per minute"><br>
  <em>Figure 2. System-wide HTTP 5xx error rate per minute.</em>
</p>

**Observation.** The baseline is about 1% (0.98% measured outside both error windows, against the generator's 1% HTTP 500 weight). The surge at 11:00 to 11:09 averages about 39% with a peak near 48%. A second, smaller bump peaks near 24% at 11:45 to 11:49.

**Interpretation.**
- The planted surge forces 40% of requests to 500 on top of the 1% baseline, so about 40.6% is expected. The jaggedness is binomial sampling noise: with n of about 100 per minute the standard deviation is `sqrt(0.406 x 0.594 / 100)`, roughly 4.9 percentage points.
- The second bump is not a second surge. It is one of four regions failing at about 90%, which contributes roughly 0.25 x 0.9 = 22.5% system-wide. Figure 4 resolves this.

<p align="center">
  <img src="docs/images/09-status-codes-all-traffic.png" width="460" alt="Requests by HTTP status code, all traffic"><br>
  <em>Figure 3. Requests by HTTP status code, all traffic.</em>
</p>

| Status | Requests | Share |
|---|---|---|
| 200 | 14,172 | 93.94% |
| 404 | 286 | 1.90% |
| 500 | 531 | 3.52% |
| 503 | 97 | 0.64% |

The counts sum to 15,086, so no events are lost between the file, the database and the chart. The 5xx total is 628, which is the 4.16% headline error rate.

**Failure-mode signature.** The 4.16% figure alone is a weak signal, since the surge window supplies most of the excess over the 1% baseline. The status-code split is more informative: all 97 of the `503` responses occur in eu-west and none anywhere else, whereas `500` responses are spread across every region. In this data, `500` marks an application fault across the fleet and `503` marks an availability fault confined to one region.

### 5.2 Regional blast radius

<p align="center">
  <img src="docs/images/07-error-rate-by-region.png" width="780" alt="5xx error rate per minute, one line per region"><br>
  <em>Figure 4. 5xx error rate per minute for each region.</em>
</p>

**Observation.** During 11:00 to 11:09 all four regions rise together, to between roughly 15% and 57% per region-minute. During 11:45 to 11:49 only eu-west leaves baseline, at 83%, 88%, 100%, 96% and 91% (91.5% over the window, against 0.5% for the other three regions).

**Interpretation.** The two incidents have different blast radii. A synchronised, fleet-wide rise points to a shared dependency, configuration or deployment; an isolated rise in one region points to that region's infrastructure. The remediation differs (roll back or fix the shared cause versus fail traffic over from the affected region), so the per-region view changes the response, not just the diagnosis. The regional lines are noisy because each region-minute holds only about 25 requests, which gives a binomial standard deviation near 10 points at a 40% error rate. That is why the regional detector requires at least 10 requests per region-minute and at least 3 consecutive bad minutes.

<p align="center">
  <img src="docs/images/08-traffic-by-region.png" width="460" alt="Share of requests by region"><br>
  <em>Figure 5. Share of requests by region.</em>
</p>

Traffic is split almost evenly: ap-south 25.3%, us-east 25.2%, us-west 24.8%, eu-west 24.7%. This is expected from uniform region sampling and works as a negative control: the eu-west outage did not change its share of traffic, because the generator does not model clients failing over. In production, traffic shifting away from a failing region would itself be a useful signal.

### 5.3 Endpoint view

<p align="center">
  <img src="docs/images/06-endpoint-p95-all-regions.png" width="460" alt="P95 latency by endpoint, all regions"><br>
  <em>Figure 6. P95 latency by endpoint, all regions.</em>
</p>

| Endpoint | Requests | Avg latency (ms) | P95 (ms) | 5xx rate | z vs pooled 4.16% |
|---|---|---|---|---|---|
| `/search` | 7,491 | 129.0 | 334.5 | 4.21% | +0.18 |
| `/listing` | 3,810 | 129.8 | 358.5 | 3.99% | -0.54 |
| `/cart` | 1,502 | 127.8 | 346.0 | 4.26% | +0.19 |
| `/checkout` | 769 | 122.1 | 276.9 | 5.72% | +2.16 |
| `/seller/listings` | 764 | 128.3 | 286.5 | 3.53% | -0.87 |
| `/login` | 750 | 134.0 | 356.9 | 3.47% | -0.95 |

**Observation.** The request mix reproduces the configured weights (49.7%, 25.3%, 10.0%, 5.1%, 5.1%, 5.0%). P95 varies from 277 to 359 ms across endpoints, and `/checkout` shows the highest 5xx rate at 5.72%.

**Interpretation.** Neither difference supports an endpoint-specific fault, and the generator contains none: every endpoint draws latency from the same distribution and receives faults independently of the endpoint.
- *Latency.* The whole-run latency distribution is a mixture, so the density at P95 is low and the P95 estimator is noisy: the approximate standard error is about 42 ms at n of about 750 and about 13 ms for `/search`. The observed spread is within that noise.
- *Error rate.* For `/checkout`, z = (5.72% - 4.16%) / `sqrt(0.0416 x 0.9584 / 769)` = +2.16. With six simultaneous comparisons, a two-sided 5% family-wise (Bonferroni) threshold is |z| of about 2.64, which `/checkout` does not reach. The other endpoints have |z| below 1.

**Implication.** A dashboard that ranks endpoints by raw error rate or P95 will surface small-sample noise. Rankings need a minimum volume or a significance test before they trigger an alert.

### 5.4 Drill-down: eu-west

Selecting `eu-west` in the region dropdown shows 3,733 requests, 131 ms average latency, 344 ms P95, 781 ms P99 and a 5.89% 5xx rate.

<p align="center">
  <img src="docs/images/05-volume-eu-west.png" width="780" alt="Request volume per minute, eu-west"><br>
  <em>Figure 7. Request volume per minute, eu-west, with detected incidents shaded.</em>
</p>

Baseline volume is about 25 requests/min, one quarter of the fleet. The 10:30 traffic spike appears proportionally, at roughly 90 to 120 requests/min. The outage minutes (11:45 to 11:49) show no systematic change in volume: the outage is visible in status codes, not in request counts.

<p align="center">
  <img src="docs/images/04-latency-eu-west.png" width="780" alt="P95 and P99 latency per minute, eu-west"><br>
  <em>Figure 8. P95 and P99 latency per minute, eu-west.</em>
</p>

The latency spike is clearly visible from 11:30 to 11:39, with P99 reaching about 1,650 ms. Here P95 and P99 nearly coincide. With about 25 requests per minute, nearest-rank P95 is the second-largest observation and P99 is the maximum, so the two lines are separated by a single rank. Percentiles over such small groups are order statistics of a handful of points rather than stable estimates.

<p align="center">
  <img src="docs/images/03-error-rate-eu-west.png" width="780" alt="5xx error rate per minute, eu-west"><br>
  <em>Figure 9. 5xx error rate per minute, eu-west.</em>
</p>

Two events are visible: the fleet-wide surge at 11:00 to 11:09 (up to about 47%) and the regional outage at 11:45 to 11:49 (up to 100%). Within eu-west the regional outage has roughly twice the amplitude of the fleet-wide surge.

<p align="center">
  <img src="docs/images/02-status-codes-eu-west.png" width="460" alt="Requests by HTTP status code, eu-west"><br>
  <em>Figure 10. Requests by HTTP status code, eu-west.</em>
</p>

| Status | eu-west requests |
|---|---|
| 200 | 3,447 |
| 404 | 66 |
| 500 | 123 |
| 503 | 97 |

The 220 5xx responses (5.89%) exceed the fleet-wide 4.16% entirely because of the 97 `503` responses, which are 100% of the fleet's `503` count. The 123 `500` responses are 23% of the fleet's 531, close to eu-west's 25% traffic share, as expected for a fleet-wide fault.

<p align="center">
  <img src="docs/images/01-endpoint-p95-eu-west.png" width="460" alt="P95 latency by endpoint, eu-west"><br>
  <em>Figure 11. P95 latency by endpoint, eu-west.</em>
</p>

In eu-west, `/login` has the highest P95 (452 ms), but it has only 182 requests, so its P95 is the 10th-largest of 182 values (rank 173). The generator has no regional `/login` problem, so this is the same small-sample effect described in section 5.3, and it illustrates why filtered slices need volume guards before they are ranked.

---

## 6. Detector evaluation and tuning log

### 6.1 Scoring protocol

Ground truth is the list of planted windows. A planted incident counts as **detected** if any detection of an accepted kind overlaps its window (the regional outage accepts either `regional_errors` or system-wide `error_rate`). A detection that overlaps no planted window is a **false alarm**. Boundary accuracy is whether the detected start and end minute equal the planted ones. `python -m src.evaluate` runs this for any number of seeds.

### 6.2 Results

| Run | Recall | Exact start and end | False alarms |
|---|---|---|---|
| Seed 42 | 4 of 4 | 4 of 4 | 0 |
| 40 seeds (`python -m src.evaluate`) | 40/40 for every incident type | 40/40 for every type | 2 (seeds 27 and 30) |
| 100 seeds | 100/100 for every incident type | 100/100 for every type | 2 (the same two seeds) |

Boundary accuracy is measured retrospectively, on completed minutes. A live alert from a persistence-filtered detector would fire one to two minutes after onset.

### 6.3 Tuning log

1. **Early versions** (plain z-score detectors and a per-region threshold, with no persistence rules) found all four incidents but also raised three false alarms on seed 42: regional blips for ap-south at 11:01 (57%) and us-east at 11:05 (52%), and a one-minute P95 alert at 11:58. Both regional blips occurred during the fleet-wide 40% surge: with only about 25 requests per region-minute, individual regions cross the 50% threshold by chance. The P95 alert came from a per-minute percentile computed on about 100 samples.
2. **Persistence rules** (at least 3 consecutive minutes for regional errors, at least 2 for 5xx rate and P95) removed all three, giving zero false alarms on seed 42.
3. **Multi-seed evaluation** exposed two residual false alarms, both 2-minute `p95_latency` alerts (seed 27 at 11:12 to 11:13, seed 30 at 10:47 to 10:48). Diagnosis: the healthy-minute P95 has a true standard deviation of about 22 ms, but the 15-point baseline window happened to be unusually quiet in both cases (sample standard deviation 7.6 to 7.7 ms), so the 10 ms sigma floor produced a threshold of about 242 to 247 ms, only 1.8 true standard deviations above the mean. Two consecutive minutes with P95 between 248 and 283 ms then passed the persistence rule.
4. **Tested fix (not applied to the repository code).** Raising the P95 sigma floor from 10 ms to 22 ms (the empirical standard deviation) gave 0 false alarms over 100 seeds with recall and boundary accuracy unchanged. It is a one-line change in `detect_incidents` (`min_std=10` to `min_std=22` for the P95 detector).

Caveat: the diagnostic seeds are the same seeds used for scoring, so these figures are in-sample for this generator. They show that the detectors behave as designed; they are not an out-of-sample accuracy estimate for real traffic.

---

## 7. Limitations and threats to validity

- **Synthetic and stationary.** Arrivals and latencies are independent and identically distributed, with no daily or weekly seasonality and no autocorrelation. Real traffic has both, and a 15-minute rolling window would raise false alarms on ordinary ramps.
- **Uncoupled faults.** Each incident affects one signal only. Real systems couple saturation to latency and errors, so a 4x traffic spike would also move latency and 5xx rate.
- **No failover or retries.** A failing region keeps its share of traffic, and failed requests are not retried.
- **Homogeneous latency.** All endpoints and regions share one latency distribution, so any endpoint or regional latency difference in the dashboard is noise by construction.
- **Parameters tuned on the generator.** Thresholds and floors were set against this synthetic data and not validated on production telemetry.
- **Untested path.** The generator supports `traffic_drop`, but it is not in the planted set or the evaluation.
- **Memory.** `main.py` loads the clean events into a list because several passes need them. That is fine at tens of thousands of events; at millions, the aggregations should be computed in a single streaming pass.
- **Batch, not streaming detection.** Detectors run over completed minutes. The pipeline is organised for streaming, but nothing here consumes a live feed.
- **Dashboard scale.** Filtered views compute `percentile_disc` over the full `events` table (O(n log n) per query). At scale, use rollup tables, materialised views or a time-series database.

---

## 8. Next steps

- **Statistics.** Replace the z-score on proportions with binomial or Poisson tests, whose variance depends on the sample size; use robust scale estimates (median and MAD) or EWMA baselines; compare each region with the rest of the fleet instead of a fixed 50% threshold.
- **Seasonality.** Baselines by minute of day and day of week, or residuals from a seasonal decomposition.
- **Machine learning.** Multivariate detection on minute-level feature vectors (count, 5xx rate, P50, P95, P99) with Isolation Forest or one-class models, and change-point methods (CUSUM, Bayesian online change-point detection) for level shifts.
- **Evaluation.** Precision, recall and F1 with detection delay, threshold sweeps, and held-out seeds and scenarios.
- **Cross-check.** A Pandas and NumPy implementation of the metrics to benchmark against the pure-Python versions.
- **Infrastructure.** A Kafka-style stream for ingestion and a Docker image for the application itself.

---

## 9. Running the project

Requires Python 3.11 or newer (developed and tested on 3.11 and 3.12) and Docker for PostgreSQL.

```bash
python -m venv .venv
.venv\Scripts\Activate.ps1          # Windows PowerShell; on macOS or Linux: source .venv/bin/activate
pip install -r requirements.txt

python -m src.main                  # core pipeline only; prints detected incidents
docker compose up -d                # start PostgreSQL
python -m src.main --store          # same, and saves events, metrics and incidents
python dashboard/app.py             # open http://127.0.0.1:8050

python -m pytest                    # 10 tests, including an end-to-end detection check
python -m src.evaluate 40           # multi-seed scoring against planted incidents
```

Expected output of `python -m src.main` for seed 42:

```
Ingestion: {'accepted': 15086, 'malformed': 30, 'duplicates': 171}
traffic_volume: 10:30 - 10:39
error_rate: 11:00 - 11:09
p95_latency: 11:30 - 11:39
error_rate: 11:45 - 11:49
regional_errors [eu-west]: 11:45 - 11:49
Top endpoints: [('/search', 7491), ('/listing', 3810), ('/cart', 1502)]
```

The database connection defaults to `postgresql://postgres:postgres@localhost:5432/traffic` (matching `docker-compose.yml`) and can be overridden with the `DATABASE_URL` environment variable. Inserts skip existing `request_id` values, so regenerating data with a different size or seed requires a clean database first: `docker compose down -v`, then `docker compose up -d`.

---

## 10. Repository layout

```
traffic-analytics/
├── data/raw/                 generated JSONL (git-ignored)
├── docs/images/              dashboard screenshots used in this README
├── src/
│   ├── generator.py          synthetic traffic and incident injection
│   ├── validation.py         per-event schema checks
│   ├── ingestion.py          streaming JSONL reader with dedup
│   ├── metrics.py            percentiles, per-minute and per-region aggregates
│   ├── anomalies.py          statistical detectors
│   ├── storage.py            PostgreSQL persistence and queries
│   ├── main.py               pipeline entry point and detection rules
│   └── evaluate.py           multi-seed evaluation
├── dashboard/app.py          Plotly Dash application
├── sql/schema.sql            table and index definitions
├── tests/test_all.py         unit and end-to-end tests
├── docker-compose.yml        PostgreSQL service
├── requirements.txt
└── README.md
```