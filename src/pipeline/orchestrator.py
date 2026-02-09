"""
Orchestrator: Run the full pipeline Extract -> Validate -> Transform -> Load.
Uses config and DB connection from env; writes data quality report to output/.
"""
import os
import sys
from datetime import date, datetime
from pathlib import Path

# Ensure project root is on path when running as script
_PROJECT_ROOT = os.getenv("PROJECT_ROOT") or Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.utils.db_connector import get_connection
from src.utils.logger import get_logger


def run_pipeline(config_path: str | None = None) -> bool:
    """
    Run: extract (read CSV, stage) -> validate (DQ report) -> transform (clean, derive) -> load (SCD2 + facts).
    Returns True if load completed (even if some DQ checks failed); False on fatal error.
    """
    import yaml
    if config_path is None:
        config_path = Path(_PROJECT_ROOT) / "src" / "config" / "config.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    log_dir = config["paths"].get("logs_dir", "logs")
    logger = get_logger("pipeline", log_dir=Path(_PROJECT_ROOT) / log_dir)
    load_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    load_date = date.today()

    logger.info("Pipeline started; load_id=%s, load_date=%s", load_id, load_date)

    try:
        with get_connection() as conn:
            # --- Extract ---
            from src.pipeline.extract import extract, stage_to_sql
            listings_df, reviews_df = extract(config)
            logger.info("Extract: read %d listings, %d reviews", len(listings_df), len(reviews_df))
            stage_to_sql(conn, listings_df, reviews_df, load_id)
            logger.info("Staging: loaded into staging_listings and staging_reviews")

            # --- Validate ---
            from src.pipeline.validate import run_validation, write_report
            report = run_validation(conn, config)
            out_dir = config["paths"].get("output_dir", "output")
            report_path = write_report(report, out_dir)
            logger.info("Validate: report written to %s; overall_pass=%s", report_path, report["overall_pass"])
            for c in report["checks"]:
                if not c["pass"]:
                    logger.warning("DQ check failed: %s - %s", c["name"], c["message"])

            # --- Transform ---
            from src.pipeline.transform import run_transform
            listings_clean, reviews_clean = run_transform(conn, config)
            logger.info("Transform: cleaned %d listings, %d reviews", len(listings_clean), len(reviews_clean))

            # --- Load ---
            from src.pipeline.load import run_load
            run_load(conn, listings_clean, reviews_clean, load_date)
            logger.info("Load: dimensions and facts updated for load_date=%s", load_date)

    except Exception as e:
        logger.exception("Pipeline failed: %s", e)
        return False

    logger.info("Pipeline finished successfully")
    return True


if __name__ == "__main__":
    success = run_pipeline()
    sys.exit(0 if success else 1)
