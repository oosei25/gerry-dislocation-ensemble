from pathlib import Path

from gerry.io import (
    extract_graph_unit_ids,
    load_graph_json,
    load_graph_unit_ids,
)


GRAPH_PATH = Path("data/raw/graphs/NC_pct21.json")


def test_load_graph_json_returns_dict() -> None:
    graph_data = load_graph_json(GRAPH_PATH)

    assert isinstance(graph_data, dict)


def test_extract_graph_unit_ids_returns_nonempty_list() -> None:
    graph_data = load_graph_json(GRAPH_PATH)
    unit_ids = extract_graph_unit_ids(graph_data)

    assert isinstance(unit_ids, list)
    assert unit_ids


def test_graph_unit_ids_are_unique() -> None:
    graph_data = load_graph_json(GRAPH_PATH)
    unit_ids = extract_graph_unit_ids(graph_data)

    assert len(unit_ids) == len(set(unit_ids))


def test_graph_unit_count_is_2650() -> None:
    graph_data = load_graph_json(GRAPH_PATH)
    unit_ids = extract_graph_unit_ids(graph_data)

    assert len(unit_ids) == 2650


def test_load_graph_unit_ids_expected_count_passes() -> None:
    unit_ids = load_graph_unit_ids(GRAPH_PATH, expected_count=2650)

    assert len(unit_ids) == 2650
