import pandas as pd
import json


def extract_postgres_settings(json_data, source_ep_name):
    """
    Extracts PostgreSQL-specific settings from a JSON data structure.

    Args:
        json_data (dict): The JSON data containing the replication definition.
        source_ep_name (str): The name of the source endpoint.

    Returns:
        tuple: A tuple containing:
            - data (list of dict): A list of dictionaries with extracted settings.
            - column_names (list of str): List of column names.
    """
    databases = json_data.get('cmd.replication_definition', {}).get('databases', [])
    data = []
    column_names = []

    for database in databases:
        if database['name'] == source_ep_name and 'POSTGRE' in database['type_id']:
            db_settings = database.get('db_settings', {})

            row_data = {
                'source_endpoint_name': database.get('name'),
                'source_db_type': database.get('type_id'),
                'source_db_role': database.get('role'),
                'source_db_user': db_settings.get('username', 'default'),
                'source_server': db_settings.get('server', 'default'),
                'src_postgres_database': db_settings.get('database', 'default'),
                'src_postgres_captureDDLs': db_settings.get('captureDDLs', False),
                'src_postgres_heartbeatEnable': db_settings.get('heartbeatEnable', False),
                'src_postgres_heartbeatSchema': db_settings.get('heartbeatSchema', 'default')
            }

            data.append(row_data)
            column_names = list(row_data.keys())  # Automatically get all column names
            break

    return data, column_names


def extract_postgres_data_to_dataframe(json_path, source_name):
    """
    Reads JSON, extracts PostgreSQL data, returns it as a DataFrame.

    Args:
        json_path (str): Path to the JSON file.
        source_name (str): Source endpoint name.

    Returns:
        pandas.DataFrame: A DataFrame of extracted PostgreSQL settings.
    """
    try:
        with open(json_path, 'r') as f:
            json_content = json.load(f)
        data, column_names = extract_postgres_settings(json_content, source_name)
        return pd.DataFrame(data, columns=column_names) if data else pd.DataFrame()
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error: {e}")
        return pd.DataFrame()


def write_dataframe_to_csv(df, csv_file_path):
    """
    Writes a Pandas DataFrame to a CSV file.

    Args:
        df (pandas.DataFrame): The data to write.
        csv_file_path (str): Path to save the CSV.
    """
    try:
        df.to_csv(csv_file_path, index=False, na_rep='NULL')
        print(f"Data successfully written to {csv_file_path}")
    except Exception as e:
        print(f"Error writing to CSV: {e}")


def main():
    file_path = r"C:\Users\VIT\OneDrive - QlikTech Inc\QlikVit\Customers\Discover\HealthCheck_02252025\aws-cpm-set1.json"
    csv_path = r"C:\path\to\output_postgres_settings.csv"
    source_name = "src_pg_tas_ext_acct"

    df = extract_postgres_data_to_dataframe(file_path, source_name)
    if not df.empty:
        print(df.fillna('NULL').to_string(index=False))
        # write_dataframe_to_csv(df, csv_path)
    else:
        print("No data extracted.")


if __name__ == "__main__":
    main()
