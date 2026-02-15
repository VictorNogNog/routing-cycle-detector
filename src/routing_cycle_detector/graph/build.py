"""Graph construction utilities for grouped bucket records."""

from collections import defaultdict
from collections.abc import Iterable

from routing_cycle_detector.graph.types import (
    BucketRecord,
    GroupedAdjacency,
    GroupKey,
    OutDegreeByGroup,
)


def build_grouped_adjacency(
    records: Iterable[BucketRecord],
) -> tuple[GroupedAdjacency, OutDegreeByGroup]:
    """
    Build grouped adjacency sets and out-degree metadata.

    The returned adjacency map is grouped by (claim_id, status_code):
    edges[(claim_id, status)][source] -> set(destinations)
    """
    edges: GroupedAdjacency = defaultdict(lambda: defaultdict(set))
    max_out_degree: OutDegreeByGroup = defaultdict(int)

    for source, dest, claim_id, status in records:
        key: GroupKey = (claim_id, status)
        adj = edges[key]

        # Add edge (deduplicates via set).
        old_size = len(adj[source])
        adj[source].add(dest)
        new_size = len(adj[source])

        # Track max out-degree using only unique edges.
        if new_size > old_size and new_size > max_out_degree[key]:
            max_out_degree[key] = new_size

    return edges, max_out_degree
