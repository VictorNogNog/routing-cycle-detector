"""Tests for the graph module."""

import tempfile
from pathlib import Path

from routing_cycle_detector.graph import process_bucket
from routing_cycle_detector.graph.cycle import find_cycle_functional, find_longest_cycle


class TestProcessBucket:
    """Test cases for process_bucket function."""

    def test_detects_triangle_cycle(self) -> None:
        """Test detection of a 3-node cycle: Epic -> Availity -> Optum -> Epic."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".bin", delete=False, encoding="utf-8"
        ) as f:
            f.write("Epic|Availity|CLM001|200\n")
            f.write("Availity|Optum|CLM001|200\n")
            f.write("Optum|Epic|CLM001|200\n")
            bucket_path = f.name

        try:
            result = process_bucket(bucket_path)
            assert result is not None
            assert result[0] == b"CLM001"
            assert result[1] == b"200"
            assert result[2] == 3
        finally:
            Path(bucket_path).unlink()

    def test_detects_two_node_cycle(self) -> None:
        """Test detection of a 2-node cycle."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".bin", delete=False, encoding="utf-8"
        ) as f:
            f.write("A|B|CLM001|200\n")
            f.write("B|A|CLM001|200\n")
            bucket_path = f.name

        try:
            result = process_bucket(bucket_path)
            assert result is not None
            assert result[2] == 2
        finally:
            Path(bucket_path).unlink()

    def test_self_loop_not_counted(self) -> None:
        """Test that self-loops (length 1) return None (not meaningful)."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".bin", delete=False, encoding="utf-8"
        ) as f:
            f.write("NodeA|NodeA|CLM001|200\n")
            bucket_path = f.name

        try:
            result = process_bucket(bucket_path)
            # Self-loop has length 1, but we only count cycles >= 2
            assert result is None
        finally:
            Path(bucket_path).unlink()

    def test_finds_longest_among_multiple_cycles(self) -> None:
        """Test that the longest cycle is returned when multiple exist."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".bin", delete=False, encoding="utf-8"
        ) as f:
            # 2-node cycle for CLM001
            f.write("A|B|CLM001|200\n")
            f.write("B|A|CLM001|200\n")

            # 4-node cycle for CLM002
            f.write("W|X|CLM002|200\n")
            f.write("X|Y|CLM002|200\n")
            f.write("Y|Z|CLM002|200\n")
            f.write("Z|W|CLM002|200\n")

            bucket_path = f.name

        try:
            result = process_bucket(bucket_path)
            assert result is not None
            assert result[0] == b"CLM002"
            assert result[2] == 4
        finally:
            Path(bucket_path).unlink()

    def test_returns_none_for_no_cycles(self) -> None:
        """Test that None is returned when no cycles exist."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".bin", delete=False, encoding="utf-8"
        ) as f:
            f.write("A|B|CLM001|200\n")
            f.write("B|C|CLM001|200\n")
            f.write("C|D|CLM001|200\n")
            bucket_path = f.name

        try:
            result = process_bucket(bucket_path)
            assert result is None
        finally:
            Path(bucket_path).unlink()

    def test_separates_by_claim_and_status(self) -> None:
        """Test that cycles are only detected within same claim_id+status."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".bin", delete=False, encoding="utf-8"
        ) as f:
            # Would form a cycle if claim/status were ignored
            f.write("A|B|CLM001|200\n")
            f.write("B|A|CLM001|404\n")  # Different status!
            bucket_path = f.name

        try:
            result = process_bucket(bucket_path)
            assert result is None
        finally:
            Path(bucket_path).unlink()

    def test_deduplicates_edges(self) -> None:
        """Test that duplicate edges don't inflate out-degree."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".bin", delete=False, encoding="utf-8"
        ) as f:
            # Same edge repeated - should still be functional graph
            f.write("A|B|CLM001|200\n")
            f.write("A|B|CLM001|200\n")
            f.write("A|B|CLM001|200\n")
            f.write("B|A|CLM001|200\n")
            bucket_path = f.name

        try:
            result = process_bucket(bucket_path)
            assert result is not None
            assert result[2] == 2  # 2-node cycle
        finally:
            Path(bucket_path).unlink()


class TestFindLongestCycle:
    """Test cases for find_longest_cycle function."""

    def test_empty_graph(self) -> None:
        """Test with empty adjacency."""
        assert find_longest_cycle({}, True) == 0

    def test_single_node_self_loop(self) -> None:
        """Test self-loop returns 0 (not counted as meaningful cycle)."""
        adj = {b"A": {b"A"}}
        # Functional graph path
        assert find_longest_cycle(adj, True) == 0

    def test_two_node_cycle(self) -> None:
        """Test 2-node cycle."""
        adj = {b"A": {b"B"}, b"B": {b"A"}}
        assert find_longest_cycle(adj, True) == 2

    def test_triangle(self) -> None:
        """Test 3-node cycle."""
        adj = {b"A": {b"B"}, b"B": {b"C"}, b"C": {b"A"}}
        assert find_longest_cycle(adj, True) == 3

    def test_no_cycle_linear(self) -> None:
        """Test linear graph with no cycle."""
        adj = {b"A": {b"B"}, b"B": {b"C"}, b"C": {b"D"}}
        assert find_longest_cycle(adj, True) == 0

    def test_general_graph_with_branching(self) -> None:
        """Test general graph (not functional) with DFS path."""
        # A -> B, A -> C, B -> C, C -> A (multiple paths, cycle of 3)
        adj = {b"A": {b"B", b"C"}, b"B": {b"C"}, b"C": {b"A"}}
        assert find_longest_cycle(adj, False) == 3


class TestFindCycleFunctional:
    """Test cases for find_cycle_functional (O(N) algorithm)."""

    def test_simple_cycle(self) -> None:
        """Test simple functional graph cycle."""
        adj = {b"A": {b"B"}, b"B": {b"C"}, b"C": {b"A"}}
        assert find_cycle_functional(adj) == 3

    def test_rho_shaped_graph(self) -> None:
        """Test rho-shaped graph (tail leading to cycle)."""
        adj = {b"X": {b"A"}, b"A": {b"B"}, b"B": {b"C"}, b"C": {b"A"}}
        assert find_cycle_functional(adj) == 3

    def test_multiple_components(self) -> None:
        """Test graph with multiple disconnected components."""
        adj = {
            b"A": {b"B"},
            b"B": {b"A"},  # 2-cycle
            b"X": {b"Y"},
            b"Y": {b"Z"},
            b"Z": {b"X"},  # 3-cycle
        }
        result = find_cycle_functional(adj)
        assert result == 3

    def test_self_loop_not_counted(self) -> None:
        """Test that self-loops are not counted as meaningful cycles."""
        adj = {b"A": {b"A"}}
        assert find_cycle_functional(adj) == 0
