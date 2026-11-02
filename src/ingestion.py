"""Phase 2b: stream a JSONL file one event at a time."""
import json
from collections import Counter
from datetime import datetime

from src.validation import validate_event


def read_events(path, stats=None):
    """Yield clean events; count what was rejected in `stats` (a Counter)."""
    if stats is None:
        stats = Counter()
    seen = set()  # request_ids already yielded (grows with unique events)
    with open(path) as f:  # iterating the file reads one line at a time
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                stats["malformed"] += 1
                continue
            if validate_event(event) is not None:
                stats["invalid"] += 1
                continue
            if event["request_id"] in seen:
                stats["duplicates"] += 1
                continue
            seen.add(event["request_id"])
            event["timestamp"] = datetime.fromisoformat(event["timestamp"])
            stats["accepted"] += 1
            yield event
