"""Unit tests for validate module (structure of report)."""
import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ["PROJECT_ROOT"] = str(ROOT)


def test_report_structure():
    """Data quality report has expected keys."""
    report_path = ROOT / "output" / "data_quality_report.json"
    if not report_path.exists():
        # Run pipeline once to generate, or skip
        import pytest
        pytest.skip("Run pipeline first to generate output/data_quality_report.json")
    with open(report_path, "r", encoding="utf-8") as f:
        report = json.load(f)
    assert "run_ts" in report
    assert "overall_pass" in report
    assert "checks" in report
    assert "summary" in report
    assert report["summary"].get("total_listings") is not None
    assert report["summary"].get("total_reviews") is not None
