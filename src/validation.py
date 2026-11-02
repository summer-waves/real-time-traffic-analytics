"""Phase 2a: validate a single event dictionary."""
from datetime import datetime

REQUIRED = {
    "request_id": str,
    "timestamp": str,
    "endpoint": str,
    "region": str,
    "status_code": int,
    "latency_ms": (int, float),
}


def validate_event(event):
    """Return None if the event is valid, otherwise a short reason string."""
    if not isinstance(event, dict):
        return "not an object"
    for field, types in REQUIRED.items():
        value = event.get(field)
        if value is None:
            return f"missing {field}"
        # bool is a subclass of int in Python, so reject it explicitly
        if isinstance(value, bool) or not isinstance(value, types):
            return f"bad type for {field}"
    try:
        datetime.fromisoformat(event["timestamp"])
    except ValueError:
        return "bad timestamp"
    if event["latency_ms"] < 0:
        return "negative latency"
    return None
