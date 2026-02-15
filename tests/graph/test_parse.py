"""Tests for graph parsing helpers."""

from routing_cycle_detector.graph.parse import iter_bucket_records, parse_bucket_line


def test_parse_bucket_line_valid() -> None:
    record = parse_bucket_line(b"A|B|CLM001|200\n")
    assert record == (b"A", b"B", b"CLM001", b"200")


def test_parse_bucket_line_invalid() -> None:
    assert parse_bucket_line(b"") is None
    assert parse_bucket_line(b"\n") is None
    assert parse_bucket_line(b"just|three|parts\n") is None


def test_iter_bucket_records_filters_invalid_lines() -> None:
    lines = [
        b"A|B|CLM001|200\n",
        b"\n",
        b"bad|line|only\n",
        b"C|D|CLM002|404\n",
    ]
    records = list(iter_bucket_records(lines))
    assert records == [
        (b"A", b"B", b"CLM001", b"200"),
        (b"C", b"D", b"CLM002", b"404"),
    ]
