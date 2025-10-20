import json
import os
import re
import sys
import csv
import pandas as pd


# Function to read JSON data from a file
def read_json_from_file(file_path):
    with open(file_path, 'r', encoding="utf-8-sig") as file:
        data = json.load(file)
    return data


# For logging INFO

def get_non_info_settings(config):
    # Check if the config is None, not a dictionary, or is a list
    if not config or not isinstance(config, dict):
        return None
    non_info_settings = []

    # Iterate over the configuration dictionary
    for setting, value in config.items():
        # Check if the value is not 'INFO'
        if value != "INFO" and setting != '$type' and not isinstance(value, dict):
            non_info_settings.append(setting)

    return non_info_settings or None


def write_dataframe_to_csv(df, csv_file_path):
    try:
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.replace(r'[\r\n]+', ' ', regex=True)
        df.to_csv(csv_file_path, index=False, na_rep='NULL')
        sys.stdout.write(f"Successfully wrote data to {csv_file_path}")
    except Exception as e:
        print(f"Error writing to CSV file: {e}")


def load_and_prefix_columns(file_path, prefix="qem_"):
    """
    Loads a tab-separated or comma-separated file and appends a prefix to all column names.

    Args:
        file_path (str): Path to the file.
        prefix (str): Prefix to prepend to each column name.

    Returns:
        pd.DataFrame: DataFrame with prefixed column names.
    """
    # Load the file (try CSV first, fallback to tab-delimited)
    try:
        df = pd.read_csv(file_path)
    except Exception:
        df = pd.read_csv(file_path, delimiter='\t')

    # Prefix column names
    df.columns = [f"{prefix}{col.strip()}" for col in df.columns]
    return df


def clean_multiline_tsv(input_file_path, output_file_path=None):
    """
    Cleans a TSV file with multiline quoted fields by reading full content and re-parsing.

    Args:
        input_file_path (str): Path to the source TSV file.
        output_file_path (str, optional): Destination path for cleaned file. If not provided,
                                          '_cleaned' is appended to the input filename.

    Returns:
        str: Path to cleaned output file.
    """
    if not output_file_path:
        base, ext = os.path.splitext(input_file_path)
        output_file_path = f"{base}_cleaned{ext}"

    # Read full content so quoted newlines are preserved
    with open(input_file_path, 'r', encoding='utf-8') as infile:
        content = infile.read()

    # Parse with csv.reader handling quotes and tabs
    rows = list(csv.reader(content.splitlines(), delimiter='\t', quotechar='"'))

    # Filter out any empty rows if needed
    cleaned_rows = [row for row in rows if any(cell.strip() for cell in row)]

    # Write cleaned rows
    with open(output_file_path, 'w', encoding='utf-8', newline='') as outfile:
        writer = csv.writer(outfile, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerows(cleaned_rows)

    print(f"✅ Cleaned TSV written to: {output_file_path}")
    return output_file_path



# Example usage
file_path = r"C:\Users\VIT\OneDrive - QlikTech Inc\QlikVit\Customers\EdwardJones\PlatformReview\filecloud-20250430192131\AemTasks_2025-04-25_13.35.53.8.tsv"
#df = load_and_prefix_columns(file_path, prefix="qem_")

#print(df.head())  # Display first few rows

#input_path = r"C:\Users\VIT\OneDrive - QlikTech Inc\QlikVit\Customers\Ally\HealthCheck\AemTasks_2025-05-14_15.16.58.954.tsv"
#cleaned_path = clean_multiline_tsv(input_path)

