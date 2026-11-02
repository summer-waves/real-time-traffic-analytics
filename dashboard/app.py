"""Phase 6: Plotly Dash dashboard reading from PostgreSQL.

Run:  python dashboard/app.py   then open http://127.0.0.1:8050

Two dropdowns (region, endpoint) filter the page. A breakdown chart ignores the
filter it is broken down by: the region charts respond to the endpoint filter
only, and the endpoint charts respond to the region filter only.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # so `import src` works

import pandas as pd
import plotly.express as px
from dash import Dash, Input, Output, dash_table, dcc, html

from src import storage

# suppress_callback_exceptions: the layout is built per page load, so Dash
# can't check the component ids when the app starts.
app = Dash(__name__, suppress_callback_exceptions=True)

MINUTE_COLUMNS = ["minute", "request_count", "error_rate", "p95_latency", "p99_latency"]


def tile(label, value):
    """One summary card: a small label above a big number."""
    return html.Div(
        [html.Div(label, style={"fontSize": "13px", "color": "#555"}),
         html.Div(value, style={"fontSize": "26px", "fontWeight": "bold"})],
        style={"flex": 1, "padding": "12px 16px", "border": "1px solid #ddd",
               "borderRadius": "8px", "background": "#fafafa"},
    )


def fmt(value, template):
    """Format a number, or show a dash when the value is missing."""
    return "-" if value is None else template.format(value)


def serve_layout():
    """Dash calls this on every page load, so a refresh shows fresh data."""
    conn = storage.connect()
    try:
        options = storage.fetch_filter_options(conn)
        incidents = storage.fetch_incidents(conn)
    finally:
        conn.close()

    if not options["regions"]:
        return html.Div("No data yet. Run:  python -m src.main --store")

    incident_rows = [
        {"kind": i["kind"], "region": i["region"] or "all",
         "start": i["start_ts"].strftime("%Y-%m-%d %H:%M"),
         "end": i["end_ts"].strftime("%Y-%m-%d %H:%M")}
        for i in incidents
    ]
    return html.Div([
        html.H1("Traffic Analytics & Anomaly Detection"),
        html.Div([
            dcc.Dropdown(id="region-filter", options=options["regions"],
                         placeholder="All regions", style={"flex": 1}),
            dcc.Dropdown(id="endpoint-filter", options=options["endpoints"],
                         placeholder="All endpoints", style={"flex": 1}),
        ], style={"display": "flex", "gap": "12px", "marginBottom": "16px"}),
        html.Div(id="tiles", style={"display": "flex", "gap": "12px"}),
        dcc.Graph(id="volume"),
        dcc.Graph(id="latency"),
        dcc.Graph(id="errors"),
        html.Div([dcc.Graph(id="status", style={"flex": 1}),
                  dcc.Graph(id="region-pie", style={"flex": 1})],
                 style={"display": "flex"}),
        dcc.Graph(id="region-errors"),
        html.Div([dcc.Graph(id="endpoint-bar", style={"flex": 1}),
                  html.Div(dash_table.DataTable(
                      id="endpoint-table",
                      columns=[{"name": n, "id": i} for n, i in (
                          ("Endpoint", "endpoint"), ("Requests", "requests"),
                          ("Avg latency (ms)", "avg_latency"),
                          ("P95 latency (ms)", "p95_latency"),
                          ("Error rate (%)", "error_pct"))],
                  ), style={"flex": 1, "paddingTop": "40px"})],
                 style={"display": "flex"}),
        html.H2("Incident history (all traffic, not filtered)"),
        dash_table.DataTable(
            data=incident_rows,
            columns=[{"name": c.title(), "id": c} for c in ("kind", "region", "start", "end")],
        ),
    ], style={"maxWidth": "1100px", "margin": "0 auto", "fontFamily": "sans-serif"})


app.layout = serve_layout


@app.callback(
    Output("tiles", "children"),
    Output("volume", "figure"),
    Output("latency", "figure"),
    Output("errors", "figure"),
    Output("status", "figure"),
    Output("region-pie", "figure"),
    Output("region-errors", "figure"),
    Output("endpoint-bar", "figure"),
    Output("endpoint-table", "data"),
    Input("region-filter", "value"),
    Input("endpoint-filter", "value"),
)
def update(region, endpoint):
    """Runs on page load and whenever a dropdown changes (value None = all)."""
    conn = storage.connect()
    try:
        summary = storage.fetch_summary(conn, region, endpoint)
        minutes = storage.fetch_minute_series(conn, region, endpoint)
        statuses = storage.fetch_status_codes(conn, region, endpoint)
        region_traffic = storage.fetch_region_traffic(conn, endpoint)
        region_errors = storage.fetch_region_error_series(conn, endpoint)
        endpoints = storage.fetch_endpoint_performance(conn, region)
        incidents = storage.fetch_incidents(conn)
    finally:
        conn.close()

    scope = " / ".join(x for x in (region, endpoint) if x) or "all traffic"

    tiles = [
        tile("Total requests", fmt(summary["requests"], "{:,}")),
        tile("Avg latency", fmt(summary["avg_latency"], "{:.0f} ms")),
        tile("P95 latency", fmt(summary["p95_latency"], "{:.0f} ms")),
        tile("P99 latency", fmt(summary["p99_latency"], "{:.0f} ms")),
        tile("Error rate (5xx)", fmt(summary["error_rate"], "{:.2%}")),
    ]

    m = pd.DataFrame(minutes, columns=MINUTE_COLUMNS)
    m["error_pct"] = m["error_rate"] * 100

    volume = px.line(m, x="minute", y="request_count",
                     title=f"Request volume per minute ({scope})")
    for inc in incidents:  # shade each detected incident on the volume chart
        if region is None or inc["region"] in (None, region):
            volume.add_vrect(x0=inc["start_ts"], x1=inc["end_ts"], opacity=0.2,
                             line_width=0, fillcolor="red")

    latency = px.line(m, x="minute", y=["p95_latency", "p99_latency"],
                      title=f"P95 / P99 latency in ms ({scope})")
    errors = px.line(m, x="minute", y="error_pct",
                     title=f"HTTP 5xx error rate in % ({scope})")

    # Plain lists here (not a DataFrame) so the status codes stay text labels.
    status = px.bar(x=[str(r["status_code"]) for r in statuses],
                    y=[r["requests"] for r in statuses],
                    labels={"x": "status code", "y": "requests"},
                    text_auto=True, title=f"Requests by status code ({scope})")

    region_pie = px.pie(pd.DataFrame(region_traffic, columns=["region", "requests"]),
                        names="region", values="requests",
                        title=f"Traffic by region ({endpoint or 'all endpoints'})")

    re_df = pd.DataFrame(region_errors, columns=["region", "minute", "requests", "error_rate"])
    re_df["error_pct"] = re_df["error_rate"] * 100
    region_err_fig = px.line(re_df, x="minute", y="error_pct", color="region",
                             title=f"5xx error rate by region in % ({endpoint or 'all endpoints'})")

    ep_df = pd.DataFrame(endpoints, columns=["endpoint", "requests", "avg_latency",
                                             "p95_latency", "error_rate"])
    endpoint_fig = px.bar(ep_df, x="endpoint", y="p95_latency",
                          title=f"P95 latency by endpoint in ms ({region or 'all regions'})")
    table_rows = [
        {"endpoint": r["endpoint"], "requests": r["requests"],
         "avg_latency": round(r["avg_latency"], 1), "p95_latency": round(r["p95_latency"], 1),
         "error_pct": round(r["error_rate"] * 100, 2)}
        for r in endpoints
    ]

    return (tiles, volume, latency, errors, status, region_pie,
            region_err_fig, endpoint_fig, table_rows)


if __name__ == "__main__":
    app.run(debug=True)