from pathlib import Path

from gerry.io import (
    compare_unit_sets,
    load_graph_unit_ids,
    peek_first_plan_assignment,
    validate_atlas_file_against_graph,
    validate_plan_units_against_graph,
)


ATLAS_PATH = Path("data/raw/atlas/nc_ush/cyclewalk_gamma0.0/run1.jsonl.gz")
GRAPH_PATH = Path("data/raw/graphs/NC_pct21.json")


def test_compare_unit_sets_reports_no_differences_for_matching_ids() -> None:
    summary = compare_unit_sets(["B", "A"], ["A", "B"])

    assert summary["assignment_count"] == 2
    assert summary["graph_count"] == 2
    assert summary["assignment_duplicate_count"] == 0
    assert summary["graph_duplicate_count"] == 0
    assert summary["missing_from_assignment"] == []
    assert summary["extra_in_assignment"] == []


def test_validate_plan_units_against_graph_passes_on_first_plan() -> None:
    graph_unit_ids = load_graph_unit_ids(GRAPH_PATH, expected_count=2650)
    assignment = peek_first_plan_assignment(ATLAS_PATH)

    validate_plan_units_against_graph(assignment, graph_unit_ids)


def test_validate_atlas_file_against_graph_passes_for_representative_file() -> None:
    summary = validate_atlas_file_against_graph(ATLAS_PATH, GRAPH_PATH)

    assert summary["graph_unit_count"] == 2650
    assert summary["plans_checked"] == 5
    assert summary["header_count"] == 3
    assert summary["validation_passed"] is True
