import logging
import os
from datetime import datetime

def setup_logger(name=None, ui=False):
    """
    Sets up a logger.
    - name: logger name (usually __name__)
    - ui: if True, writes to app_ui_YYYY-MM-DD.log
    """
    log_format = "%(asctime)s [%(levelname)s] %(name)s - %(message)s [%(filename)s:%(lineno)d]"
    log_level = logging.DEBUG

    logger_name = name if not ui else f"{name}_ui"
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)

    if not logger.handlers:  # Prevent duplicate handlers on re-import
        formatter = logging.Formatter(log_format)

        # --- Console handler ---
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        # --- File handler ---
        base_dir = os.path.dirname(os.path.abspath(__file__))  # helpers/
        main_dir = os.path.abspath(os.path.join(base_dir, ".."))  # backend/
        log_dir = os.path.join(main_dir, "logs")
        os.makedirs(log_dir, exist_ok=True)

        timestamp = datetime.now().strftime("%Y-%m-%d")
        log_filename = f"app_{timestamp}.log" if not ui else f"app_ui_{timestamp}.log"
        file_path = os.path.join(log_dir, log_filename)

        fh = logging.FileHandler(file_path, encoding="utf-8")  # always use utf-8
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    # --- Critical fix: disable propagation ---
    # Without this, ui_logger messages propagate to root logger and appear in app.log too
    logger.propagate = False

    return logger
