"""Shared type definitions for graph processing."""

type GroupKey = tuple[bytes, bytes]
type AdjacencyMap = dict[bytes, set[bytes]]
type GroupedAdjacency = dict[GroupKey, AdjacencyMap]
type OutDegreeByGroup = dict[GroupKey, int]
type BucketRecord = tuple[bytes, bytes, bytes, bytes]
type BucketResult = tuple[bytes, bytes, int]
