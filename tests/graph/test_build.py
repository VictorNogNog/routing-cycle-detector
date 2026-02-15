"""Tests for graph construction helpers."""

from routing_cycle_detector.graph.build import build_grouped_adjacency


def test_build_grouped_adjacency_tracks_groups_and_out_degree() -> None:
    records = [
        (b"A", b"B", b"CLM001", b"200"),
        (b"A", b"C", b"CLM001", b"200"),
        (b"A", b"C", b"CLM001", b"200"),  # duplicate edge
        (b"X", b"Y", b"CLM002", b"404"),
    ]

    edges, max_out_degree = build_grouped_adjacency(records)

    group_1 = (b"CLM001", b"200")
    group_2 = (b"CLM002", b"404")

    assert edges[group_1][b"A"] == {b"B", b"C"}
    assert edges[group_2][b"X"] == {b"Y"}
    assert max_out_degree[group_1] == 2
    assert max_out_degree[group_2] == 1
