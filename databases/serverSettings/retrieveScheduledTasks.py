import os
import json
import re
import pandas as pd
from helpers.logger_config import setup_logger
from cron_descriptor import get_description

# Configure logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

logging = setup_logger(__name__)


def extract_schedule_settings(json_data):
    """
    Extracts task schedule settings from JSON data.
    Returns a tuple of (data, column_names).
    """
    logging.info(f" Initiate fetching Replicate server task schedule details")
    replicate_server = None

    # Extract replicate server from description if available
    description = json_data.get('description', '')
    match = re.search(r"Host name:\s([a-zA-Z0-9.-]+)", description)
    if match:
        replicate_server = match.group(1)
        logging.debug(f"Replicate server extracted: {replicate_server}")

    # Get scheduler section
    scheduler = json_data.get('cmd.replication_definition', {}).get('scheduler', {})
    jobs = scheduler.get("jobs", [])
    logging.debug(f"Total jobs found: {len(jobs)}")

    data = []

    for job in jobs:
        job_name = job.get("name")
        command_id = job.get("command_id")
        schedule = job.get("schedule")
        task_name = job.get("task")
        operation = job.get("command_requests", {}).get("execute_req", {}).get("operation")
        flags = job.get("command_requests", {}).get("execute_req", {}).get("flags")

        try:
            readable_schedule = describe_flexible_cron(schedule)
        except Exception as e:
            readable_schedule = f"Invalid schedule: {schedule} ({e})"
            logging.warning(f"Error parsing schedule for job '{job_name}': {e}")

        # logging.debug(f"job: {job_name}, Command ID: {command_id}, Schedule: {schedule}, Readable: {readable_schedule}")

        row_data = {
            'replicate_server': replicate_server,
            'job_name': job_name,
            'command_id': command_id,
            'task_name': task_name,
            'schedule': schedule,
            'readable_schedule': readable_schedule,
            'operation': operation,
            'flags': flags
        }

        data.append(row_data)

    column_names = list(data[0].keys()) if data else []

    return data, column_names



def extract_server_data_to_dataframe(json_file_path):
    try:
        with open(json_file_path, 'r', encoding="utf-8-sig") as f:
            json_data = json.load(f)
        data, column_names = extract_schedule_settings(json_data)
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


from calendar import month_name

dow_map = {
    "0": "Sunday", "1": "Monday", "2": "Tuesday", "3": "Wednesday",
    "4": "Thursday", "5": "Friday", "6": "Saturday", "7": "Sunday"
}


def describe_flexible_cron(expr: str) -> str:
    fields = expr.strip().split()
    n = len(fields)

    if n == 6:
        # Possibly: minute hour dom month dow year
        minute, hour, dom, month, dow, year = fields
        time_str = f"{hour.zfill(2)}:{minute.zfill(2)}"

        # Day of week readable
        if dow == "*":
            days = "every day"
        else:
            days = ", ".join(dow_map.get(d, f"day-{d}") for d in dow.split(","))

        return f"Scheduled at {time_str} on {days} of every month in {year if year != '*' else 'any year'}"

    elif n == 7:
        # Quartz-style: second minute hour dom month dow year
        second, minute, hour, dom, month, dow, year = fields
        time_str = f"{hour.zfill(2)}:{minute.zfill(2)}:{second.zfill(2)}"

        if dow == "*" or dow == "?":
            days = f"on day {dom} of {month if month != '*' else 'every month'}"
        else:
            days = ", ".join(dow_map.get(d, f"day-{d}") for d in dow.split(","))

        return f"Scheduled at {time_str} on {days} in {year if year != '*' else 'any year'}"

    else:
        return f"Unsupported cron format: expected 6 or 7 fields, got {n}"


# Example
#cron = "0 11 * * 1,2,3,4,5 *"
#print(parse_quartz_cron(cron))

def main():
    json_file_path = r"C:\Users\VIT\OneDrive - QlikTech Inc\QlikVit\Customers\CN\HC\Replication_Definition.json"
    json_file_path2 = r"C:\Users\VIT\OneDrive - QlikTech Inc\QlikVit\Customers\Ally\HealthCheck\LatestFIles\OnPremQlikProduction_2.json"
    df = extract_server_data_to_dataframe(json_file_path2)
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()