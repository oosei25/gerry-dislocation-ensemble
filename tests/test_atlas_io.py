from pathlib import Path

from gerry.io import (
    extract_plan_assignment,
    iter_plan_records,
    load_graph_unit_ids,
    peek_first_plan_assignment,
    read_atlas_header,
)


ATLAS_PATH = Path("data/raw/atlas/nc_ush/cyclewalk_gamma0.0/run1.jsonl.gz")
GRAPH_PATH = Path("data/raw/graphs/NC_pct21.json")


def test_read_atlas_header_returns_three_records() -> None:
    header = read_atlas_header(ATLAS_PATH)

    assert len(header) == 3


def test_iter_plan_records_yields_at_least_one_record() -> None:
    first_plan = next(iter_plan_records(ATLAS_PATH))

    assert isinstance(first_plan, dict)
    assert "districting" in first_plan


def test_extract_plan_assignment_returns_mapping() -> None:
    first_plan = next(iter_plan_records(ATLAS_PATH))
    assignment = extract_plan_assignment(first_plan)

    assert isinstance(assignment, dict)
    assert assignment
    assert all(isinstance(unit_id, str) for unit_id in assignment)


def test_first_plan_assignment_has_expected_graph_coverage() -> None:
    graph_unit_ids = load_graph_unit_ids(GRAPH_PATH, expected_count=2650)
    assignment = peek_first_plan_assignment(ATLAS_PATH)

    assert len(assignment) == len(graph_unit_ids)
    assert set(assignment) == set(graph_unit_ids)
