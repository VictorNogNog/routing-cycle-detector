"""Tests for the reader module."""

import tempfile
from pathlib import Path

from src.reader import ClaimRecord, stream_lines


class TestStreamLines:
    """Test cases for stream_lines function."""

    def test_parses_valid_line(self) -> None:
        """Test that a valid pipe-delimited line is parsed correctly."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False, encoding="utf-8"
        ) as f:
            f.write("NodeA|NodeB|CLM001|200\n")
            temp_path = f.name

        try:
            records = list(stream_lines(temp_path))
            assert len(records) == 1
            assert records[0] == ClaimRecord(
                source="NodeA",
                dest="NodeB",
                claim_id="CLM001",
                status="200",
            )
        finally:
            Path(temp_path).unlink()

    def test_parses_multiple_lines(self) -> None:
        """Test parsing multiple valid lines."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False, encoding="utf-8"
        ) as f:
            f.write("A|B|CLM001|200\n")
            f.write("C|D|CLM002|404\n")
            f.write("E|F|CLM003|500\n")
            temp_path = f.name

        try:
            records = list(stream_lines(temp_path))
            assert len(records) == 3
            assert records[0].source == "A"
            assert records[1].claim_id == "CLM002"
            assert records[2].status == "500"
        finally:
            Path(temp_path).unlink()

    def test_skips_empty_lines(self) -> None:
        """Test that empty lines are skipped."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False, encoding="utf-8"
        ) as f:
            f.write("A|B|CLM001|200\n")
            f.write("\n")
            f.write("   \n")
            f.write("C|D|CLM002|404\n")
            temp_path = f.name

        try:
            records = list(stream_lines(temp_path))
            assert len(records) == 2
        finally:
            Path(temp_path).unlink()

    def test_handles_trailing_newline(self) -> None:
        """Test that trailing newlines don't affect parsing."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False, encoding="utf-8"
        ) as f:
            f.write("NodeA|NodeB|CLM001|200\n")
            f.write("NodeC|NodeD|CLM002|404")  # No trailing newline
            temp_path = f.name

        try:
            records = list(stream_lines(temp_path))
            assert len(records) == 2
            assert records[0].status == "200"
            assert records[1].status == "404"
        finally:
            Path(temp_path).unlink()

    def test_handles_utf8_content(self) -> None:
        """Test that UTF-8 content is handled correctly."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False, encoding="utf-8"
        ) as f:
            f.write("Nœud_α|Nœud_β|CLM001|200\n")
            temp_path = f.name

        try:
            records = list(stream_lines(temp_path))
            assert len(records) == 1
            assert records[0].source == "Nœud_α"
            assert records[0].dest == "Nœud_β"
        finally:
            Path(temp_path).unlink()

    def test_is_generator(self) -> None:
        """Test that stream_lines returns a generator (lazy evaluation)."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False, encoding="utf-8"
        ) as f:
            f.write("A|B|CLM001|200\n")
            temp_path = f.name

        try:
            result = stream_lines(temp_path)
            # Should be a generator, not a list
            assert hasattr(result, "__next__")
            assert hasattr(result, "__iter__")
        finally:
            Path(temp_path).unlink()
