from collections import defaultdict


def process_bucket(bucket_path: str) -> tuple[bytes, bytes, int] | None:
    """
    Process a single bucket file to find the longest cycle.

    Builds adjacency sets grouped by (claim_id, status_code) and finds
    the longest simple cycle within each group. All processing uses bytes
    until final result.

    Args:
        bucket_path: Path to the bucket file.

    Returns:
        Tuple of (claim_id, status_code, cycle_length) as bytes, or None if no cycles.
    """
    # Build adjacency sets: edges[(claim, status)][source] -> set(destinations)
    # Also track if each group is a functional graph (max out-degree <= 1)
    edges: dict[tuple[bytes, bytes], dict[bytes, set[bytes]]] = defaultdict(
        lambda: defaultdict(set)
    )
    # Track max out-degree per group to detect functional graphs
    max_out_degree: dict[tuple[bytes, bytes], int] = defaultdict(int)

    with open(bucket_path, "rb") as f:
        for line in f:
            line = line.rstrip(b"\n\r")
            if not line:
                continue

            parts = line.split(b"|", 3)
            if len(parts) < 4:
                continue

            source = parts[0]
            dest = parts[1]
            claim_id = parts[2]
            status = parts[3]

            key = (claim_id, status)
            adj = edges[key]

            # Add edge (deduplicates via set)
            old_size = len(adj[source])
            adj[source].add(dest)
            new_size = len(adj[source])

            # Update max out-degree if this edge was new
            if new_size > old_size:
                if new_size > max_out_degree[key]:
                    max_out_degree[key] = new_size

    best_result: tuple[bytes, bytes, int] | None = None

    for key, adj in edges.items():
        is_functional = max_out_degree[key] <= 1
        cycle_len = _find_longest_cycle(adj, is_functional)
        if cycle_len > 0:
            if best_result is None or cycle_len > best_result[2]:
                best_result = (key[0], key[1], cycle_len)

    return best_result


def _find_longest_cycle(adj: dict[bytes, set[bytes]], is_functional: bool) -> int:
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
        return _find_cycle_functional(adj)
    else:
        return _find_cycle_dfs(adj)


def _find_cycle_functional(adj: dict[bytes, set[bytes]]) -> int:
    """
    O(N) cycle detection for functional graphs (out-degree <= 1).

    Walks each path and detects cycles when revisiting a node in the current path.
    """
    # Build a simple next-node mapping
    next_node: dict[bytes, bytes] = {}
    for src, dests in adj.items():
        if dests:
            next_node[src] = next(iter(dests))

    if not next_node:
        return 0

    # Track globally visited nodes (already processed)
    globally_visited: set[bytes] = set()
    longest = 0

    # Also consider nodes that are only destinations (might be part of cycles)
    all_nodes = set(next_node.keys())
    for dest in next_node.values():
        all_nodes.add(dest)

    for start in all_nodes:
        if start in globally_visited:
            continue

        # Walk the path, tracking nodes in current walk with their positions
        path_order: dict[bytes, int] = {}
        current = start
        pos = 0

        while current is not None:
            if current in globally_visited:
                break

            if current in path_order:
                # Found a cycle
                cycle_len = pos - path_order[current]
                if cycle_len >= 2 and cycle_len > longest:
                    longest = cycle_len
                break

            path_order[current] = pos
            pos += 1
            current = next_node.get(current)

        globally_visited.update(path_order.keys())

    return longest


def _find_cycle_dfs(adj: dict[bytes, set[bytes]]) -> int:
    """
    DFS-based cycle detection for general graphs.

    Uses minimum start node rule to avoid counting the same cycle multiple times:
    - Sort all nodes
    - When starting DFS from node at index i, only explore neighbors with index >= i
    - This ensures each cycle is found exactly once (from its minimum node)
    """
    # Get all nodes that have outgoing edges
    nodes_with_edges = list(adj.keys())
    if not nodes_with_edges:
        return 0

    # Sort nodes for consistent ordering
    nodes_sorted = sorted(nodes_with_edges)
    idx_map: dict[bytes, int] = {node: i for i, node in enumerate(nodes_sorted)}

    longest = 0

    def dfs(node: bytes, start: bytes, start_idx: int, path: set[bytes], depth: int) -> None:
        """DFS with backtracking, only exploring nodes with index >= start_idx."""
        nonlocal longest

        for neighbor in adj.get(node, set()):
            if neighbor == start and depth >= 1:
                # Found a cycle back to start (length = depth + 1)
                cycle_len = depth + 1
                if cycle_len > longest:
                    longest = cycle_len
            elif neighbor not in path:
                # Only continue to nodes with index >= start_idx
                neighbor_idx = idx_map.get(neighbor)
                if neighbor_idx is not None and neighbor_idx >= start_idx:
                    path.add(neighbor)
                    dfs(neighbor, start, start_idx, path, depth + 1)
                    path.remove(neighbor)

    # Start DFS from each node
    for i, start_node in enumerate(nodes_sorted):
        path = {start_node}
        dfs(start_node, start_node, i, path, 0)

    return longest
