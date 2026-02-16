"""Shared type definitions for graph processing."""

from dataclasses import dataclass

type GroupKey = tuple[bytes, bytes]
type AdjacencyMap = dict[bytes, set[bytes]]
type GroupedAdjacency = dict[GroupKey, AdjacencyMap]
type OutDegreeByGroup = dict[GroupKey, int]
type BucketRecord = tuple[bytes, bytes, bytes, bytes]


@dataclass(frozen=True, slots=True)
class BucketResult:
    """Result of processing a single bucket: the longest cycle found."""

    claim_id: bytes
    status_code: bytes
    cycle_length: int
