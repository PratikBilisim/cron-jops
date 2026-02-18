from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Optional, Union


def setup_logging(log_level: str = "INFO", log_file: str = "logs/cron_service.log") -> None:
    # Dosya adına tarih ekle
    log_dir = os.path.dirname(log_file)
    log_filename = os.path.basename(log_file)
    name, ext = os.path.splitext(log_filename)
    
    # Günlük log dosyası adı: cron_service_2026-02-17.log
    dated_filename = f"{name}_{datetime.now().strftime('%Y-%m-%d')}{ext}"
    dated_log_file = os.path.join(log_dir, dated_filename)
    
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)

    numeric_level: int = getattr(logging, log_level.upper(), logging.INFO)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logger = logging.getLogger()
    logger.setLevel(numeric_level)

    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    file_handler = logging.FileHandler(dated_log_file, encoding='utf-8')
    file_handler.setLevel(numeric_level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    logger.info(f"Logging configured - Level: {log_level}, File: {dated_log_file}")


def log_execution_start() -> None:
    logger = logging.getLogger(__name__)
    logger.info("=" * 50)
    logger.info("CRON JOB EXECUTION STARTED")
    logger.info(f"Execution Time: {datetime.now()}")
    logger.info("=" * 50)


def log_execution_end(success: bool, duration: float = 0):
    logger = logging.getLogger(__name__)
    logger.info("=" * 50)
    status = "COMPLETED SUCCESSFULLY" if success else "COMPLETED WITH ERRORS"
    logger.info(f"CRON JOB EXECUTION {status}")
    logger.info(f"Duration: {duration:.2f} seconds")
    logger.info(f"End Time: {datetime.now()}")
    logger.info("=" * 50)
