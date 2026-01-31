"""Tests for the partition module."""

import tempfile
import zlib
from pathlib import Path

from routing_cycle_detector.partition import LRUFileCache, partition_to_buckets


class TestPartitionToBuckets:
    """Test cases for partition_to_buckets function."""

    def test_partitions_lines_to_correct_buckets(self) -> None:
        """Test that lines are routed to buckets based on claim_id|status hash."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False, encoding="utf-8"
        ) as f:
            f.write("A|B|CLM001|200\n")
            f.write("C|D|CLM001|200\n")  # Same claim+status, same bucket
            f.write("E|F|CLM002|404\n")
            input_path = f.name

        with tempfile.TemporaryDirectory() as tmp_dir:
            try:
                bucket_paths, _stats = partition_to_buckets(input_path, 4, tmp_dir)

                assert len(bucket_paths) > 0

                # Read all bucket contents
                all_lines = []
                for bucket_path in bucket_paths:
                    with open(bucket_path, "rb") as bf:
                        all_lines.extend(bf.readlines())

                assert len(all_lines) == 3

                # Verify CLM001|200 lines are in the same bucket
                clm001_bucket = zlib.crc32(b"CLM001|200") & 3
                expected_path = Path(tmp_dir) / f"bucket_{clm001_bucket:04d}.bin"

                with open(expected_path, "rb") as bf:
                    bucket_lines = bf.readlines()

                clm001_lines = [line for line in bucket_lines if b"CLM001|200" in line]
                assert len(clm001_lines) == 2

            finally:
                Path(input_path).unlink()

    def test_handles_empty_lines(self) -> None:
        """Test that empty lines are skipped."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False, encoding="utf-8"
        ) as f:
            f.write("A|B|CLM001|200\n")
            f.write("\n")
            f.write("   \n")
            f.write("C|D|CLM002|404\n")
            input_path = f.name

        with tempfile.TemporaryDirectory() as tmp_dir:
            try:
                bucket_paths, _stats = partition_to_buckets(input_path, 4, tmp_dir)

                total_lines = 0
                for bucket_path in bucket_paths:
                    with open(bucket_path, "rb") as bf:
                        total_lines += len(bf.readlines())

                assert total_lines == 2

            finally:
                Path(input_path).unlink()

    def test_returns_only_non_empty_buckets(self) -> None:
        """Test that only non-empty bucket paths are returned."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False, encoding="utf-8"
        ) as f:
            f.write("A|B|CLM001|200\n")
            input_path = f.name

        with tempfile.TemporaryDirectory() as tmp_dir:
            try:
                bucket_paths, _stats = partition_to_buckets(input_path, 1024, tmp_dir)

                assert len(bucket_paths) == 1

                with open(bucket_paths[0], "rb") as bf:
                    content = bf.read()
                assert len(content) > 0

            finally:
                Path(input_path).unlink()

    def test_preserves_raw_line_content(self) -> None:
        """Test that raw line content is preserved in buckets."""
        original_line = "NodeA|NodeB|CLM001|200"

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False, encoding="utf-8"
        ) as f:
            f.write(f"{original_line}\n")
            input_path = f.name

        with tempfile.TemporaryDirectory() as tmp_dir:
            try:
                bucket_paths, _stats = partition_to_buckets(input_path, 4, tmp_dir)

                with open(bucket_paths[0], "rb") as bf:
                    content = bf.read().decode("utf-8").strip()

                assert content == original_line

            finally:
                Path(input_path).unlink()


class TestLRUFileCache:
    """Test cases for LRUFileCache."""

    def test_evicts_lru_when_full(self) -> None:
        """Test that LRU handle is evicted when cache is full."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            cache = LRUFileCache(max_handles=2, tmp_dir=tmp_path)

            try:
                # Write to 3 buckets with max_handles=2
                cache.write(0, b"data0\n")
                cache.write(1, b"data1\n")
                cache.write(2, b"data2\n")  # Should evict bucket 0

                # Bucket 0 should be closed and flushed
                # Buckets 1 and 2 should still be open
                assert len(cache._cache) == 2
                assert 0 not in cache._cache
                assert 1 in cache._cache
                assert 2 in cache._cache

            finally:
                cache.close_all()

            # All data should be written
            assert (tmp_path / "bucket_0000.bin").read_bytes() == b"data0\n"
            assert (tmp_path / "bucket_0001.bin").read_bytes() == b"data1\n"
            assert (tmp_path / "bucket_0002.bin").read_bytes() == b"data2\n"

    def test_moves_to_end_on_access(self) -> None:
        """Test that accessed handles are moved to end (most recently used)."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            cache = LRUFileCache(max_handles=2, tmp_dir=tmp_path)

            try:
                cache.write(0, b"data0\n")
                cache.write(1, b"data1\n")
                cache.write(0, b"more0\n")  # Access bucket 0 again
                cache.write(2, b"data2\n")  # Should evict bucket 1 (LRU)

                assert len(cache._cache) == 2
                assert 1 not in cache._cache  # Bucket 1 was evicted
                assert 0 in cache._cache
                assert 2 in cache._cache

            finally:
                cache.close_all()
