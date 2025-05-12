import json
import os
import re
import sys

import pandas as pd


# Function to read JSON data from a file
def read_json_from_file(file_path):
    with open(file_path, 'r') as file:
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


# Example usage
file_path = r"C:\Users\VIT\OneDrive - QlikTech Inc\QlikVit\Customers\EdwardJones\PlatformReview\filecloud-20250430192131\AemTasks_2025-04-25_13.35.53.8.tsv"
df = load_and_prefix_columns(file_path, prefix="qem_")

print(df.head())  # Display first few rows
