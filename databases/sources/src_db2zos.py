import pandas as pd
import json
import re


def extract_db2zos_settings(json_data, source_ep_name):
    """
    Extracts Oracle-specific settings from a JSON data structure.

    Args:
        json_data (dict): The JSON data containing the replication definition.
        source_ep_name (str): The name of the source endpoint.

    Returns:
        tuple: A tuple containing:
            - data (list of dict): A list of dictionaries, where each dictionary
              represents a row of data.  Returns an empty list if no relevant data is found.
            - column_names (list of str): A list of column names.
              Returns an empty list if no relevant data is found.
    """
    tasks = json_data.get('cmd.replication_definition', {}).get('tasks', [])
    databases = json_data.get('cmd.replication_definition', {}).get('databases', [])
    data = []  # Initialize an empty list to store the extracted data
    column_names = []

    for database in databases:
        if database['name'] == source_ep_name and database['type_id'] == 'DB2ZOS_NATIVE_COMPONENT_TYPE':
            db_settings = database.get('db_settings', {})  # Safely get nested settings
            db2zos_source_server = db_settings.get('server', None)

            # Construct the data dictionary.  Use get() with defaults.
            row_data = {
                'source_server': db_settings.get('server', 'default'),
                'source_endpoint_name': database.get('name'),
                'source_db_type': database.get('type_id'),
                'source_db_role': database.get('role'),
                'source_db_user': db_settings.get('username'),
                'source_logstreamstagingtask': db_settings.get('logstreamstagingtask', 'None'),
                'source_database': db_settings.get('databaseName'),
                'source_db2zos_connectMode': db_settings.get('connectMode'),
                'source_db2zos_ifi306SpName': db_settings.get('ifi306SpName'),
                'source_db2zos_ignoreCreateTable': db_settings.get('ignoreCreateTable'),

            }
            data.append(row_data)

            # Define column names *only* when data is found and processed
            column_names = ['source_server', 'source_endpoint_name', 'source_db_type', 'source_db_role', 'source_db_user',
                            'source_logstreamstagingtask', 'source_database', 'source_db2zos_connectMode', 'source_db2zos_ifi306SpName',
                            'source_db2zos_ignoreCreateTable'
                            ]
            break  # important:  Exit the loop after finding the matching database
    return data, column_names  # Return the data and column names


def extract_db2zos_data_to_dataframe(json_data, source_name):
    """
    Extracts relevant data from JSON and converts it into a Pandas DataFrame.

    Args:
        json_data (str): Path to the JSON file.
        source_name (str): the source name

    Returns:
        pandas.DataFrame: A Pandas DataFrame containing the extracted data.
                        Returns an empty DataFrame on error.
    """
    try:
        with open(json_data, 'r') as f:
            json_content = json.load(f)
        data, column_names = extract_db2zos_settings(json_content, source_name)  # Pass json_content, not file path
        if data:
            return pd.DataFrame(data, columns=column_names)
        else:
            return pd.DataFrame()  # Return empty DataFrame
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error


def write_dataframe_to_csv(df, csv_file_path):
    """
    Writes a Pandas DataFrame to a CSV file.

    Args:
        df (pandas.DataFrame): The Pandas DataFrame to write to CSV.
        csv_file_path (str): The path to the CSV file.
    """
    try:
        df.to_csv(csv_file_path, index=False)
        print(f"Data successfully written to {csv_file_path}")
    except Exception as e:
        print(f"Error writing to CSV: {e}")


def main():
    """
    Main function to orchestrate the data extraction and CSV writing process.
    """
    file_path_nondef = r"C:\Users\VIT\OneDrive - QlikTech Inc\QlikVit\Customers\EdwardJones\PlatformReview\filecloud-20250430192131\tlpreplcdc-004.json"
    csv_file_path = r"C:\Users\VIT\Downloads\oracle_settings.csv"
    source_name = 'DB2_Direct_Connect'

    df = extract_db2zos_data_to_dataframe(file_path_nondef, source_name)
    if not df.empty:  # Check if the dataframe is empty
        #write_dataframe_to_csv(df, csv_file_path)
        print(df.fillna('NULL').to_string(index=False))
    else:
        print("No data to write to CSV.")


if __name__ == "__main__":
    main()
