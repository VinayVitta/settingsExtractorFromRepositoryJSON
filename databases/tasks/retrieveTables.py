import pandas as pd
import json,re

def extract_tables(json_data):
    """
    Extracts SAP-related source table configuration from a JSON replication definition.

    Args:
        json_data (dict): The JSON data containing replication definition.

    Returns:
        pandas.DataFrame: A DataFrame of SAP source table settings.
    """
    rows = []
    tasks = json_data.get('cmd.replication_definition', {}).get('tasks', [])
    databases = json_data.get('cmd.replication_definition', {}).get('databases', [])
    replicate_json_hostname = json_data.get('description', '')
    tables_replicate_server = (re.search(r"Host name:\s([a-zA-Z0-9.-]+)", replicate_json_hostname)).group(
        1) if replicate_json_hostname and re.search(r"Host name:\s([a-zA-Z0-9.-]+)",
                                                    replicate_json_hostname) else None
    for task in tasks:
        tables_task_name = task.get('task', {}).get('name')
        tables_source_ep_name = task.get('source', {}).get('rep_source', {}).get('source_name')
        tables_target_ep_name = task.get('targets', [{}])[0].get('rep_target', {}).get('target_name')
        tables_task_type = 'logstream' if task.get('task', {}).get('task_type') == '_LOG_STREAM' else 'replication'

        row_base = {
            'tables_replicate_server': tables_replicate_server,
            'tables_task_name': tables_task_name,
            'tables_task_type': tables_task_type,
            'tables_source_ep_name': tables_source_ep_name,
            'tables_target_ep_name': tables_target_ep_name
        }

        # Tables
        table_list = task.get('source', {}).get('source_tables', {}).get('explicit_included_tables', [])
        if not table_list:
            print(f"No tables found for task: {tables_task_name}")
            continue

        for table in table_list:
            row = row_base.copy()
            row['schema_name'] = table.get('owner')
            row['table_name'] = table.get('name')
            rows.append(row)

    return pd.DataFrame(rows)


def extract_tables_dataframe(json_file_path):
    """
    Reads a JSON file and extracts table data into a DataFrame.

    Args:
        json_file_path (str): Path to the JSON file.

    Returns:
        pandas.DataFrame: A Pandas DataFrame containing the extracted data.
                          Returns an empty DataFrame on error.
    """
    try:
        with open(json_file_path, 'r') as f:
            json_content = json.load(f)
        return extract_tables(json_content)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error: {e}")
        return pd.DataFrame()


def write_dataframe_to_csv(df, csv_file_path):
    """
    Writes a Pandas DataFrame to a CSV file.

    Args:
        df (pandas.DataFrame): The Pandas DataFrame to write to CSV.
        csv_file_path (str): The path to the CSV file.
    """
    try:
        df.to_csv(csv_file_path, index=False, na_rep='NULL')
        print(f"Data successfully written to {csv_file_path}")
    except Exception as e:
        print(f"Error writing to CSV: {e}")


def main():
    """
    Main function to orchestrate the data extraction and CSV writing process.
    """
    json_file_path = r"C:\Users\VIT\Downloads\Replication_Definition_ketan.json"
    csv_file_path = r"C:\Users\VIT\Downloads\sql_server_settings.csv"

    df = extract_tables_dataframe(json_file_path)
    if not df.empty:
        # Uncomment to write to CSV
        # write_dataframe_to_csv(df, csv_file_path)
        print(df.fillna('NULL').to_string(index=False))
    else:
        print("No data to write to CSV.")


if __name__ == "__main__":
    main()
