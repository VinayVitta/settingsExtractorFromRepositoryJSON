import os
import json
import re
import pandas as pd
from helpers.logger_config import setup_logger
from cron_descriptor import get_description

# Configure logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

logging = setup_logger(__name__)


def extract_notification_settings(json_data):
    """
    Extracts task schedule settings from JSON data.
    Returns a tuple of (data, column_names).
    """
    logging.info(f" Initiate fetching Replicate server task Notification details")
    replicate_server = None

    # Extract replicate server from description if available
    description = json_data.get('description', '')
    match = re.search(r"Host name:\s([a-zA-Z0-9.-]+)", description)
    if match:
        replicate_server = match.group(1)
        logging.debug(f"Replicate server extracted: {replicate_server}")

    # Get scheduler section
    notification = json_data.get('cmd.replication_definition', {}).get('notifications', {})
    notifications = notification.get("notifications_list", [])
    logging.debug(f"Total Notifications found: {len(notifications)}")

    data = []

    for notify in notifications:
        notification_name = notify.get("name")
        notification_status = 'disabled' if notify.get("enabled") is False else 'enabled'
        notification_trigger_type = notify.get("trigger_type")
        notification_ui_id = notify.get("ui_id")
        task_name = notify.get("tasks")

        row_data = {
            'replicate_server': replicate_server,
            'notification_name': notification_name,
            'notification_status': notification_status,
            'notification_trigger_type': notification_trigger_type,
            'notification_ui_id': notification_ui_id,
            'task_name': task_name
        }

        data.append(row_data)

    column_names = list(data[0].keys()) if data else []

    return data, column_names


def extract_notifications_to_dataframe(json_file_path):
    try:
        with open(json_file_path, 'r') as f:
            json_data = json.load(f)
        data, column_names = extract_notification_settings(json_data)
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
    json_file_path2 = r"C:\Users\VIT\OneDrive - QlikTech Inc\QlikVit\Customers\Ally\HealthCheck\LatestFIles\OnPremQlikProduction_2.json"
    df = extract_notifications_to_dataframe(json_file_path2)
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()