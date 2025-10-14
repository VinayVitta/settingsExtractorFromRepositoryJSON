from pathlib import Path
import shutil
import sys, os
from datetime import datetime
# Import your main extraction functions
sys.path.append(str(Path(__file__).resolve().parents[2]))
from main import extract_all_settings, retrieveTables, utils, summary, process_repository

def run_extraction(json_paths, tsv_path, output_folder):
    """
    Wraps your main.py logic, but for programmatic call
    Returns a list of file paths for downloads
    """
    """
    Wraps main.process_repository()
    """
    folder = Path(output_folder)
    folder.mkdir(parents=True, exist_ok=True)

    # Call the updated main method with list of JSONs
    results = process_repository(json_paths, tsv_path)

    # Return filenames for frontend
    return [Path(v) for v in results.values() if v]


if __name__ == "__main__":
    try:
        # Example configuration (replace with your own paths)
        folder_path = r"C:\Users\VIT\OneDrive - QlikTech Inc\QlikVit\Customers\Dennis"
        qem_export_path = os.path.join(folder_path, "AemTasks_2025-10-08_13.48.03.491.tsv")

        process_repository(folder_path, qem_export_path)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)