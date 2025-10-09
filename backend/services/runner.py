from pathlib import Path
import shutil
import sys

# Import your main extraction functions
sys.path.append(str(Path(__file__).resolve().parents[2]))
from main import extract_all_settings, retrieveTables, utils, summary

def run_extraction(json_paths, tsv_path, output_folder):
    """
    Wraps your main.py logic, but for programmatic call
    Returns a list of file paths for downloads
    """
    # You can reuse most of your main.py logic here
    # For brevity, just simulate output
    # In practice: copy all logic from main.py and replace file paths with these arguments
    output_files = []

    # Example: run extraction on first JSON file
    df = extract_all_settings(json_paths[0])
    output_path = Path(output_folder) / "task_settings.csv"
    utils.write_dataframe_to_csv(df, output_path)
    output_files.append(str(output_path.name))

    return output_files
