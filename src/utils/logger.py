"""Centralized logging for the ETL pipeline."""
import logging
import os
import sys
from pathlib import Path
from datetime import datetime


def get_logger(
    name: str = "pipeline",
    log_dir: str | None = None,
    level: int = logging.INFO,
) -> logging.Logger:
    """Create and return a configured logger with optional file handler."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(level)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    if log_dir:
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(
            Path(log_dir) / f"pipeline_execution_{datetime.now().strftime('%Y%m%d')}.log",
            encoding="utf-8",
        )
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger
