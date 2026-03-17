from __future__ import annotations

from itertools import islice
from pathlib import Path
from typing import Any

from gerry.io.atlas import extract_plan_assignment, iter_plan_records, read_atlas_header
from gerry.io.graph import load_graph_unit_ids


def compare_unit_sets(
    assignment_unit_ids: list[str],
    graph_unit_ids: list[str],
) -> dict[str, Any]:
    """Compare assignment IDs against graph IDs and return summary diagnostics."""
    assignment_unique = set(assignment_unit_ids)
    graph_unique = set(graph_unit_ids)

    assignment_duplicates = sorted(
        unit_id for unit_id in assignment_unique if assignment_unit_ids.count(unit_id) > 1
    )
    graph_duplicates = sorted(
        unit_id for unit_id in graph_unique if graph_unit_ids.count(unit_id) > 1
    )

    return {
        "assignment_count": len(assignment_unit_ids),
        "graph_count": len(graph_unit_ids),
        "assignment_duplicate_count": len(assignment_unit_ids) - len(assignment_unique),
        "graph_duplicate_count": len(graph_unit_ids) - len(graph_unique),
        "assignment_duplicates": assignment_duplicates,
        "graph_duplicates": graph_duplicates,
        "missing_from_assignment": sorted(graph_unique - assignment_unique),
        "extra_in_assignment": sorted(assignment_unique - graph_unique),
    }


def validate_plan_units_against_graph(
    plan_assignment: dict[str, int | str],
    graph_unit_ids: list[str],
) -> None:
    """Raise if a plan assignment does not match the graph unit IDs exactly."""
    diagnostics = compare_unit_sets(list(plan_assignment.keys()), graph_unit_ids)

    if diagnostics["assignment_count"] != diagnostics["graph_count"]:
        raise ValueError(
            "Plan assignment size does not match graph unit count: "
            f"{diagnostics['assignment_count']} vs {diagnostics['graph_count']}."
        )

    if diagnostics["missing_from_assignment"] or diagnostics["extra_in_assignment"]:
        raise ValueError(
            "Plan assignment unit IDs do not match graph unit IDs exactly. "
            f"Missing sample: {_format_examples(diagnostics['missing_from_assignment'])}. "
            f"Extra sample: {_format_examples(diagnostics['extra_in_assignment'])}."
        )


def validate_atlas_file_against_graph(
    atlas_path: str | Path,
    graph_path: str | Path,
    max_plans: int | None = 5,
    expected_graph_count: int | None = 2650,
) -> dict[str, Any]:
    """Validate one Atlas file against the graph for the first N plans and return a summary."""
    graph_unit_ids = load_graph_unit_ids(graph_path, expected_count=expected_graph_count)
    header = read_atlas_header(atlas_path)

    plan_iter = iter_plan_records(atlas_path)
    if max_plans is not None:
        plan_iter = islice(plan_iter, max_plans)

    plans_checked = 0
    for plan_record in plan_iter:
        plan_assignment = extract_plan_assignment(plan_record)
        validate_plan_units_against_graph(plan_assignment, graph_unit_ids)
        plans_checked += 1

    if plans_checked == 0:
        raise ValueError("Atlas file does not contain any plan records to validate.")

    return {
        "atlas_path": str(Path(atlas_path)),
        "graph_path": str(Path(graph_path)),
        "graph_unit_count": len(graph_unit_ids),
        "plans_checked": plans_checked,
        "header_count": len(header),
        "validation_passed": True,
    }


def _format_examples(unit_ids: list[str], limit: int = 5) -> str:
    """Format a readable, truncated sample of unit IDs for error messages."""
    if not unit_ids:
        return "[]"

    sample = unit_ids[:limit]
    suffix = "" if len(unit_ids) <= limit else f", ... ({len(unit_ids)} total)"
    return "[" + ", ".join(sample) + "]" + suffix
