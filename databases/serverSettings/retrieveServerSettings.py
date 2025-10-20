import os
import json
import re
import pandas as pd
from helpers.logger_config import setup_logger

# Configure logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

logging = setup_logger(__name__)


def extract_server_settings(json_data):
    """
    Extracts task settings from a JSON data structure for a specific task name.
    Returns a tuple of (data, column_names) or empty lists if not found.
    """
    logging.info(f"Initiate fetching Replicate server settings")
    replicate_server = None
    description = json_data.get('description', '')
    match = re.search(r"Host name:\s([a-zA-Z0-9.-]+)", description)
    if match:
        replicate_server = match.group(1)

    # Get all Configs
    replication_environment = json_data.get('cmd.replication_definition', {}).get('replication_environment', [])
    notifications = json_data.get('cmd.replication_definition', {}).get('notifications', [])
    scheduler = json_data.get('cmd.replication_definition', {}).get('scheduler', [])
    disk_utilization_configuration = json_data.get('cmd.replication_definition', {}).get('disk_utilization_configuration', [])
    memory_utilization_configuration = json_data.get('cmd.replication_definition', {}).get('memory_utilization_configuration', [])
    replicate_version = json_data.get('_version', '')
    # print(replicate_version)
    data = []
    column_names = []
    # server_data['replicate_server'] = replicate_server
    row_data = {
        'replicate_server': replicate_server,
        # Replication Envi
        'enable_auto_roll_over_logs': 'true' if replication_environment.get('enable_auto_roll_over') else 'false',
        'roll_over_max_age_days_logs': replication_environment.get('roll_over_max_age_days'),
        'roll_over_max_size_mb_logs': replication_environment.get('roll_over_max_size_mb'),
        # Notifications pending
        # Scheduler
        # Disk Utilization
        'disk_utilization_configuration': 'true' if disk_utilization_configuration.get('is_enabled') else 'false',
        'high_disk_storage_percent': disk_utilization_configuration.get('high_storage_percent', 90) if disk_utilization_configuration.get('is_enabled') else None,
        'critical_disk_storage_percent': disk_utilization_configuration.get('critical_storage_percent', 95) if disk_utilization_configuration.get('is_enabled') else None,
        # Memory Utilization
        'memory_utilization_configuration': 'true' if memory_utilization_configuration.get('is_enabled') else 'false',
        'high_memory_storage_percent': memory_utilization_configuration.get('high_storage_percent', 80) if memory_utilization_configuration.get(
            'is_enabled') else None,
        'critical_memory_storage_percent': memory_utilization_configuration.get('critical_storage_percent', 90) if memory_utilization_configuration.get(
            'is_enabled') else None,
        'replicate_version': replicate_version.get('version')
    }
    data.append(row_data)
    column_names = list(row_data.keys())
    return data, column_names


def extract_server_data_to_dataframe(json_file_path):
    try:
        with open(json_file_path, 'r', encoding="utf-8-sig") as f:
            json_data = json.load(f)
        data, column_names = extract_server_settings(json_data)
        if data:
            return pd.DataFrame(data, columns=column_names)
        else:
            logging.warning("May be empty DF in server settings")
            return pd.DataFrame()
        # return pd.DataFrame(data, columns=column_names) if data else pd.DataFrame()
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"Error reading JSON file: {e}")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return pd.DataFrame()


def main():
    json_file_path = r"C:\Users\VIT\OneDrive - QlikTech Inc\QlikVit\Customers\CN\HC\Replication_Definition.json"
    json_file_path2 = r"C:\MySW\Attunity\Replicate\data\imports\Replication_Definition_def.json"
    df = extract_server_data_to_dataframe(json_file_path)
    # print(df.to_string(index=False))


if __name__ == "__main__":
    main()