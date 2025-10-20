from pathlib import Path
import shutil
import sys, os
from datetime import datetime

# Add project root to path

from helpers.logger_config import setup_logger
from main import extract_all_settings, retrieveTables, utils, summary, process_repository

# Initialize loggers
backend_logger = setup_logger(__name__)
ui_logger = setup_logger("ui_runner", ui=True)


def run_extraction(json_paths, tsv_path, output_folder):
    """
    Wraps your main.process_repository() logic for programmatic calls.
    Logs both backend and UI-level events.
    Returns a list of file paths for download.
    """
    folder = Path(output_folder)
    folder.mkdir(parents=True, exist_ok=True)

    ui_logger.info(f"Extraction started for folder: {folder}")
    backend_logger.info(f"Starting run_extraction() with JSONs: {json_paths}, TSV: {tsv_path}")

    try:
        # Run the core process
        results = process_repository(json_paths, tsv_path)

        if not results:
            backend_logger.warning(f"No results returned from process_repository() for folder {folder}")
            ui_logger.warning(f"Extraction completed but no files generated in {folder}")
            return []

        # Filter and resolve valid output paths
        output_files = [Path(v) for v in results.values() if v]

        ui_logger.info(f"Extraction completed successfully — {len(output_files)} file(s) generated.")
        backend_logger.info(f"Generated files: {output_files}")

        return output_files

    except Exception as e:
        backend_logger.exception(f"Extraction failed for folder {folder}: {e}")
        ui_logger.exception(f"Error during extraction in folder {folder}: {e}")
        raise  # re-raise so FastAPI endpoint can return a proper error


if __name__ == "__main__":
    try:
        folder_path = r"C:\Users\VIT\OneDrive - QlikTech Inc\QlikVit\Customers\Dennis"
        qem_export_path = os.path.join(folder_path, "AemTasks_2025-10-08_13.48.03.491.tsv")

        backend_logger.info(f"Manual execution started for {folder_path}")
        process_repository(folder_path, qem_export_path)
        backend_logger.info("Manual execution completed successfully.")

    except Exception as e:
        backend_logger.exception(f"Manual run failed: {e}")
        print(f"\n❌ Error: {e}")
        sys.exit(1)
