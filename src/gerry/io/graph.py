from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_graph_json(path: str | Path) -> dict[str, Any]:
    """Load a graph JSON file and return the parsed object."""
    graph_path = Path(path)

    with graph_path.open("r", encoding="utf-8") as fh:
        try:
            graph_data = json.load(fh)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON in graph file: {graph_path}") from exc

    if not isinstance(graph_data, dict):
        raise ValueError("Graph JSON must be a top-level object.")

    nodes = graph_data.get("nodes")
    if not isinstance(nodes, list):
        raise ValueError("Graph JSON must contain a 'nodes' list.")

    return graph_data


def normalize_unit_id(value: object) -> str:
    """Normalize a graph or atlas unit identifier to a comparable string form."""
    if value is None:
        raise ValueError("Unit ID cannot be null.")

    normalized = str(value).strip()
    if not normalized:
        raise ValueError("Unit ID cannot be empty.")

    return normalized


def extract_graph_unit_ids(graph_data: dict[str, Any]) -> list[str]:
    """Extract normalized node/unit IDs from graph JSON."""
    nodes = graph_data.get("nodes")
    if not isinstance(nodes, list):
        raise ValueError("Graph JSON must contain a 'nodes' list.")

    unit_ids: list[str] = []
    seen_ids: set[str] = set()

    for index, node in enumerate(nodes):
        unit_id = _extract_node_unit_id(node, index)
        if unit_id in seen_ids:
            raise ValueError(f"Duplicate graph unit ID found: {unit_id}")

        seen_ids.add(unit_id)
        unit_ids.append(unit_id)

    if not unit_ids:
        raise ValueError("Graph JSON does not contain any unit IDs.")

    return unit_ids


def validate_graph_unit_ids(
    graph_data: dict[str, Any], expected_count: int | None = None
) -> None:
    """Validate graph unit IDs for existence, uniqueness, and optional count."""
    unit_ids = extract_graph_unit_ids(graph_data)

    if not unit_ids:
        raise ValueError("Graph JSON does not contain any unit IDs.")

    if len(unit_ids) != len(set(unit_ids)):
        raise ValueError("Graph unit IDs must be unique.")

    if expected_count is not None and len(unit_ids) != expected_count:
        raise ValueError(
            f"Expected {expected_count} graph unit IDs, found {len(unit_ids)}."
        )


def load_graph_unit_ids(path: str | Path, expected_count: int | None = None) -> list[str]:
    """Load, validate, and return normalized graph unit IDs from a graph file."""
    graph_data = load_graph_json(path)
    validate_graph_unit_ids(graph_data, expected_count=expected_count)
    return extract_graph_unit_ids(graph_data)


def _extract_node_unit_id(node: object, index: int) -> str:
    """Build the comparable graph unit ID for one node."""
    if not isinstance(node, dict):
        raise ValueError(f"Graph node at index {index} must be an object.")

    if "county" not in node:
        raise ValueError(f"Graph node at index {index} is missing 'county'.")
    if "prec_id" not in node:
        raise ValueError(f"Graph node at index {index} is missing 'prec_id'.")

    county = normalize_unit_id(node["county"])
    precinct = normalize_unit_id(node["prec_id"])

    # Atlas files encode units as COUNTY_prec_id, so the graph needs the same key shape.
    return normalize_unit_id(f"{county}_{precinct}")
