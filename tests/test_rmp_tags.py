"""Tests for RMP tag normalization (case-insensitive mapping, canonical-only output)."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock


def _load_rmp_scraper_module():
    """Load rmp-scraper.py without importing transformers/torch (stubbed)."""
    mock_pipe = MagicMock(return_value=[{"label": "neutral", "score": 0.5}])
    tr = MagicMock()
    tr.pipeline = MagicMock(return_value=mock_pipe)
    sys.modules["transformers"] = tr

    sys.modules["rmp_client"] = MagicMock()
    sys.modules["rmp_client"].RMPClient = MagicMock

    sb = MagicMock()
    sb.create_client = MagicMock()
    sys.modules["supabase"] = sb

    pe = MagicMock()
    pe.APIError = type("APIError", (Exception,), {})
    sys.modules["postgrest"] = MagicMock()
    sys.modules["postgrest.exceptions"] = pe

    root = Path(__file__).resolve().parent.parent
    path = root / "apps" / "scrapers" / "rmp-scraper.py"
    spec = importlib.util.spec_from_file_location("rmp_scraper_under_test", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_mod = _load_rmp_scraper_module()
normalize_rmp_tags = _mod.normalize_rmp_tags


def test_tough_grader_casing_variants_equivalent():
    expected = normalize_rmp_tags(["Tough grader"])
    assert normalize_rmp_tags(["TOUGH GRADER"]) == expected
    assert normalize_rmp_tags(["tough grader"]) == expected
    assert normalize_rmp_tags(["  Tough  Grader  "]) == expected
    assert expected == ["grading", "hard"]


def test_lots_of_homework_casing():
    expected = normalize_rmp_tags(["Lots of homework"])
    assert normalize_rmp_tags(["LOTS OF HOMEWORK"]) == expected
    assert expected == ["workload"]


def test_duplicate_raw_tags_dedupe_canonical():
    """Same RMP label repeated or mixed case should not duplicate canonical entries."""
    out = normalize_rmp_tags(["Tough grader", "TOUGH GRADER", "tough grader"])
    assert out == ["grading", "hard"]


def test_multiple_distinct_tags_merge_sorted():
    out = normalize_rmp_tags(["Lots of homework", "Tough grader"])
    assert out == ["grading", "hard", "workload"]


def test_empty_and_none_safe():
    assert normalize_rmp_tags([]) == []
    assert normalize_rmp_tags(None) == []


def test_unknown_tag_dropped():
    assert normalize_rmp_tags(["Totally unknown RMP label"]) == []


def test_whitespace_only_tag_skipped():
    assert normalize_rmp_tags(["", "  ", "\t"]) == []
