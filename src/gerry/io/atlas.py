from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any, Iterator, TextIO

from gerry.io.graph import normalize_unit_id


def open_atlas_text(path: str | Path) -> TextIO:
    """Open an Atlas .jsonl.gz file in text mode."""
    atlas_path = Path(path)

    if not atlas_path.exists():
        raise FileNotFoundError(f"Atlas file not found: {atlas_path}")

    if atlas_path.suffixes[-2:] != [".jsonl", ".gz"]:
        raise ValueError(f"Atlas file must have a .jsonl.gz extension: {atlas_path}")

    return gzip.open(atlas_path, "rt", encoding="utf-8")


def iter_atlas_records(path: str | Path) -> Iterator[Any]:
    """Yield parsed JSON records from an Atlas .jsonl.gz file line by line."""
    with open_atlas_text(path) as fh:
        for line_number, raw_line in enumerate(fh, start=1):
            line = raw_line.strip()
            if not line:
                continue

            try:
                yield json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Failed to parse Atlas JSON on line {line_number}."
                ) from exc


def read_atlas_header(path: str | Path, header_lines: int = 3) -> list[Any]:
    """Read and return the Atlas header records."""
    header: list[Any] = []

    for record in iter_atlas_records(path):
        header.append(record)
        if len(header) == header_lines:
            return header

    raise ValueError(
        f"Atlas file contains fewer than {header_lines} parsed records."
    )


def iter_plan_records(path: str | Path, header_lines: int = 3) -> Iterator[dict[str, Any]]:
    """Yield only plan records after skipping Atlas header metadata."""
    for index, record in enumerate(iter_atlas_records(path)):
        if index < header_lines:
            continue

        if not isinstance(record, dict):
            raise ValueError(
                f"Atlas plan record after header must be an object, found {type(record).__name__}."
            )

        yield record


def find_assignment_payload(plan_record: dict[str, Any]) -> Any:
    """Locate the district-assignment payload inside a parsed Atlas plan record."""
    for key in ("districting", "assignment"):
        payload = plan_record.get(key)
        if _looks_like_assignment_payload(payload):
            return payload

    for value in plan_record.values():
        if _looks_like_assignment_payload(value):
            return value

    raise ValueError("Atlas plan record does not contain a recognizable assignment payload.")


def extract_plan_assignment(plan_record: dict[str, Any]) -> dict[str, int | str]:
    """Extract normalized unit -> district assignment mapping from an Atlas plan record."""
    payload = find_assignment_payload(plan_record)
    assignment: dict[str, int | str] = {}

    if isinstance(payload, dict):
        items = payload.items()
    elif isinstance(payload, list):
        items = _iter_assignment_items_from_list(payload)
    else:
        raise ValueError("Atlas assignment payload must be a dict or list of singleton dicts.")

    for raw_unit_id, raw_district in items:
        unit_id = _normalize_assignment_unit_id(raw_unit_id)
        district = _normalize_district_label(raw_district)

        if unit_id in assignment:
            raise ValueError(f"Duplicate unit ID in Atlas assignment payload: {unit_id}")

        assignment[unit_id] = district

    if not assignment:
        raise ValueError("Atlas assignment payload is empty.")

    return assignment


def peek_first_plan_assignment(path: str | Path) -> dict[str, int | str]:
    """Convenience helper to extract the first plan assignment from an Atlas file."""
    for plan_record in iter_plan_records(path):
        return extract_plan_assignment(plan_record)

    raise ValueError("Atlas file does not contain any plan records.")


def _looks_like_assignment_payload(payload: object) -> bool:
    """Return whether an object plausibly stores unit-to-district assignments."""
    if isinstance(payload, dict):
        return bool(payload)

    if isinstance(payload, list) and payload:
        return all(isinstance(item, dict) and len(item) == 1 for item in payload[:5])

    return False


def _iter_assignment_items_from_list(
    payload: list[object],
) -> Iterator[tuple[object, object]]:
    """Yield assignment pairs from a list of singleton dicts."""
    for index, item in enumerate(payload):
        if not isinstance(item, dict) or len(item) != 1:
            raise ValueError(
                "Atlas list-based assignment payload must contain singleton dict entries."
            )

        raw_unit_id, raw_district = next(iter(item.items()))
        yield raw_unit_id, raw_district


def _normalize_assignment_unit_id(raw_unit_id: object) -> str:
    """Normalize Atlas unit identifiers to the graph-comparable string form."""
    candidate = raw_unit_id

    if isinstance(raw_unit_id, str):
        stripped = raw_unit_id.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            try:
                parsed = json.loads(stripped)
            except json.JSONDecodeError:
                parsed = None

            if isinstance(parsed, list) and len(parsed) == 1:
                candidate = parsed[0]
            else:
                candidate = stripped

    return normalize_unit_id(candidate)


def _normalize_district_label(raw_district: object) -> int | str:
    """Normalize district labels while keeping their original int-or-string form."""
    if isinstance(raw_district, bool):
        raise ValueError("District label must be an int or string, not bool.")

    if isinstance(raw_district, int):
        return raw_district

    if isinstance(raw_district, str):
        return normalize_unit_id(raw_district)

    raise ValueError(
        f"District label must be an int or string, found {type(raw_district).__name__}."
    )
