"""Cycle detection algorithms used by bucket processing."""

from routing_cycle_detector.graph.types import AdjacencyMap


def find_longest_cycle(adj: AdjacencyMap, is_functional: bool) -> int:
    """
    Find the longest simple cycle in a directed graph.

    Args:
        adj: Adjacency sets mapping source -> set(destinations).
        is_functional: True if max out-degree <= 1 (use O(N) algorithm).

    Returns:
        Length of the longest simple cycle, or 0 if none found.
    """
    if not adj:
        return 0

    if is_functional:
        return find_cycle_functional(adj)
    return find_cycle_dfs(adj)


def find_cycle_functional(adj: AdjacencyMap) -> int:
    """
    O(N) cycle detection for functional graphs (out-degree <= 1).

    Walks each path and detects cycles when revisiting a node in the current path.
    """
    # Build a simple next-node mapping.
    next_node: dict[bytes, bytes] = {}
    for src, dests in adj.items():
        if dests:
            next_node[src] = next(iter(dests))

    if not next_node:
        return 0

    # Track globally visited nodes (already processed).
    globally_visited: set[bytes] = set()
    longest = 0

    # Also consider nodes that are only destinations (might be part of cycles).
    all_nodes = set(next_node.keys())
    for dest in next_node.values():
        all_nodes.add(dest)

    for start in all_nodes:
        if start in globally_visited:
            continue

        # Walk the path, tracking nodes in current walk with their positions.
        path_order: dict[bytes, int] = {}
        current = start
        pos = 0

        while current is not None:
            if current in globally_visited:
                break

            if current in path_order:
                # Found a cycle.
                cycle_len = pos - path_order[current]
                if cycle_len >= 2 and cycle_len > longest:
                    longest = cycle_len
                break

            path_order[current] = pos
            pos += 1
            current = next_node.get(current)

        globally_visited.update(path_order.keys())

    return longest


def find_cycle_dfs(adj: AdjacencyMap) -> int:
    """
    DFS-based cycle detection for general graphs.

    Uses minimum start node rule to avoid counting the same cycle multiple times:
    - Sort all nodes
    - When starting DFS from node at index i, only explore neighbors with index >= i
    - This ensures each cycle is found exactly once (from its minimum node)
    """
    # Get all nodes that have outgoing edges.
    nodes_with_edges = list(adj.keys())
    if not nodes_with_edges:
        return 0

    # Sort nodes for consistent ordering.
    nodes_sorted = sorted(nodes_with_edges)
    idx_map: dict[bytes, int] = {node: i for i, node in enumerate(nodes_sorted)}

    longest = 0

    def dfs(node: bytes, start: bytes, start_idx: int, path: set[bytes], depth: int) -> None:
        """DFS with backtracking, only exploring nodes with index >= start_idx."""
        nonlocal longest

        for neighbor in adj.get(node, set()):
            if neighbor == start and depth >= 1:
                # Found a cycle back to start (length = depth + 1).
                cycle_len = depth + 1
                if cycle_len > longest:
                    longest = cycle_len
            elif neighbor not in path:
                # Only continue to nodes with index >= start_idx.
                neighbor_idx = idx_map.get(neighbor)
                if neighbor_idx is not None and neighbor_idx >= start_idx:
                    path.add(neighbor)
                    dfs(neighbor, start, start_idx, path, depth + 1)
                    path.remove(neighbor)

    # Start DFS from each node.
    for i, start_node in enumerate(nodes_sorted):
        path = {start_node}
        dfs(start_node, start_node, i, path, 0)

    return longest
