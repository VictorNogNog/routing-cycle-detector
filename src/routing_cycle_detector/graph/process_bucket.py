"""High-level bucket graph processing orchestration."""

from routing_cycle_detector.graph.build import build_grouped_adjacency
from routing_cycle_detector.graph.cycle import find_longest_cycle
from routing_cycle_detector.graph.parse import read_bucket_records
from routing_cycle_detector.graph.types import BucketResult


def process_bucket(bucket_path: str) -> BucketResult | None:
    """
    Process a single bucket file to find the longest cycle.

    Builds adjacency sets grouped by (claim_id, status_code) and finds
    the longest simple cycle within each group. All processing uses bytes
    until final result.
    """
    records = read_bucket_records(bucket_path)
    edges, max_out_degree = build_grouped_adjacency(records)

    best_result: BucketResult | None = None

    for key, adj in edges.items():
        is_functional = max_out_degree[key] <= 1
        cycle_len = find_longest_cycle(adj, is_functional)
        if cycle_len > 0 and (best_result is None or cycle_len > best_result.cycle_length):
            best_result = BucketResult(key[0], key[1], cycle_len)

    return best_result
