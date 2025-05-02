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