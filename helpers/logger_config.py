import logging
import logging.config
import os
from datetime import datetime


def setup_logger(name=None):
    log_format = "%(asctime)s [%(levelname)s] %(name)s - %(message)s [%(filename)s:%(lineno)d]"
    log_level = logging.DEBUG

    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    if not logger.handlers:  # Prevent duplicate handlers if re-imported
        formatter = logging.Formatter(log_format)
        # Console handler
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        # Resolve base directory relative to main script
        base_dir = os.path.dirname(os.path.abspath(__file__))  # e.g. subdir/helpers
        main_dir = os.path.abspath(os.path.join(base_dir, ".."))  # go to main/
        log_dir = os.path.join(main_dir, "logs")
        os.makedirs(log_dir, exist_ok=True)

        # Create log filename with today's date
        timestamp = datetime.now().strftime("%Y-%m-%d")
        log_filename = f"app_{timestamp}.log"

        # File handler
        file_path = os.path.join(log_dir, log_filename)
        fh = logging.FileHandler(file_path)
        fh.setFormatter(logging.Formatter(log_format))
        logger.addHandler(fh)
    logger.propagate = False
    return logger
