import hashlib
import json
from collections.abc import Mapping
from typing import Any


def payload_hash(event_type: str, payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        {"event_type": event_type, "payload": payload},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()
