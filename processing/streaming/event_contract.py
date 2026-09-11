"""Shared validation for the flat v1 JSON Schema (no external dependency)."""
import json
import math
import re
from pathlib import Path


def load_contract(path) -> dict:
    return json.loads(Path(path).read_text())


def validate_event(event, schema: dict) -> dict:
    if not isinstance(event, dict):
        raise ValueError("Event must be an object")
    missing = set(schema["required"]) - set(event)
    if missing:
        raise ValueError("Missing fields: " + ",".join(sorted(missing)))
    extra = set(event) - set(schema["properties"])
    if extra:
        raise ValueError("Unexpected fields: " + ",".join(sorted(extra)))
    for name, rule in schema["properties"].items():
        value = event[name]
        kind = rule["type"]
        valid_type = ((kind == "string" and isinstance(value, str)) or
                      (kind == "integer" and type(value) is int) or
                      (kind == "number" and type(value) in (int, float)))
        if not valid_type:
            raise ValueError("Invalid type: " + name)
        if kind in ("integer", "number"):
            if not math.isfinite(value):
                raise ValueError("Non-finite number: " + name)
            if value < rule.get("minimum", -math.inf) or value > rule.get("maximum", math.inf):
                raise ValueError("Out of range: " + name)
        if "enum" in rule and value not in rule["enum"]:
            raise ValueError("Invalid enum: " + name)
        if "minLength" in rule and len(value) < rule["minLength"]:
            raise ValueError("Empty string: " + name)
        if "pattern" in rule and not re.search(rule["pattern"], value):
            raise ValueError("Invalid format: " + name)
    return event


def serialize_event(event: dict, schema: dict) -> bytes:
    validate_event(event, schema)
    return json.dumps(event, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def valid_json(payload, schema: dict) -> bool:
    return not validation_errors(payload, schema)


def validation_errors(payload, schema: dict) -> list:
    if payload is None:
        return ["Null payload"]
    try:
        event = json.loads(payload)
    except (ValueError, TypeError):
        return ["Malformed JSON"]
    try:
        validate_event(event, schema)
        return []
    except (ValueError, TypeError, OverflowError) as exc:
        return [str(exc)]
