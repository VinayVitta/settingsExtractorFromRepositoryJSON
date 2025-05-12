import pandas as pd
import json


def extract_sap_hana_settings(json_data, source_ep_name):
    """
    Extracts SAP HANA-specific settings from a JSON data structure.

    Args:
        json_data (dict): The JSON data containing the replication definition.
        source_ep_name (str): The name of the source endpoint.

    Returns:
        tuple: A tuple containing:
            - data (list of dict): A list of dictionaries, where each dictionary represents a row of data.
            - column_names (list of str): A list of column names.
    """
    databases = json_data.get('cmd.replication_definition', {}).get('databases', [])
    data = []
    column_names = []

    for database in databases:
        if database['name'] == source_ep_name and database['type_id'] in ['SAP_APPLICATION_COMPONENT_TYPE', 'SAP_HANA_SRC_COMPONENT_TYPE']:
            db_settings = database.get('db_settings', {})
            row_data = {
                'source_endpoint_name': database.get('name'),
                'source_db_type': database.get('type_id'),
                'source_db_role': database.get('role'),
                'source_db_user': db_settings.get('username'),
                'source_server': db_settings.get('server'),
                'source_client': db_settings.get('client'),
                'source_backend_db': db_settings.get('backend_db'),
                'source_instance_number': db_settings.get('instance_number'),
                'source_cleanup_interval': db_settings.get('cleanup_interval'),
                'source_log_retention_period': db_settings.get('log_retention_period'),
                'source_logTableTriggerBasedMode': db_settings.get('logTableTriggerBasedMode'),
                'source_logstreamstagingtask': db_settings.get('logstreamstagingtask'),
                'source_rfc_call_batch': db_settings.get('rfc_call_batch'),
                'source_connection_type': db_settings.get('connection_type'),
                'source_server_group': db_settings.get('server_group'),
                'source_message_server_service': db_settings.get('message_server_service'),
                'source_r3_system': db_settings.get('r3_system'),
                'source_store_only_tdline_in_stxtl_clustd': db_settings.get('store_only_tdline_in_stxtl_clustd')
            }

            # Handle backend DB lookup
            backend_db_name = db_settings.get('backend_db')
            if backend_db_name:
                for db in databases:
                    if db['name'] == backend_db_name:
                        backend_settings = db.get('db_settings', {})
                        row_data.update({
                            'backend_db_host': backend_settings.get('server'),
                            'backend_db_user': backend_settings.get('username'),
                            'backend_db_instance_number': backend_settings.get('instance_number'),
                            'backend_db_cleanup_interval': backend_settings.get('cleanup_interval'),
                            'backend_db_log_retention_period': backend_settings.get('log_retention_period'),
                            'backend_db_logTableTriggerBasedMode': backend_settings.get('logTableTriggerBasedMode'),
                            'backend_db_logstreamstagingtask': backend_settings.get('logstreamstagingtask')
                        })
                        break

            data.append(row_data)
            column_names = list(row_data.keys())
            break

    return data, column_names


def extract_sap_hana_data_to_dataframe(json_file_path, source_name):
    """
    Extracts SAP HANA data from JSON and converts it into a Pandas DataFrame.

    Args:
        json_file_path (str): Path to the JSON file.
        source_name (str): the source name

    Returns:
        pandas.DataFrame: A Pandas DataFrame containing the extracted data.
    """
    try:
        with open(json_file_path, 'r') as f:
            json_content = json.load(f)
        data, column_names = extract_sap_hana_settings(json_content, source_name)
        if data:
            return pd.DataFrame(data, columns=column_names)
        else:
            return pd.DataFrame()
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
    json_path = r"C:\Users\VIT\OneDrive - QlikTech Inc\QlikVit\Customers\Exxon\HCSep2024\Replication_Definition.json"
    csv_output_path = r"C:\path\to\output\sap_hana_settings.csv"
    source_endpoint_name = 'G9 SAP LST -  SALES TRANSACTION'

    df = extract_sap_hana_data_to_dataframe(json_path, source_endpoint_name)
    if not df.empty:
        # write_dataframe_to_csv(df, csv_output_path)
        print(df.fillna('NULL').to_string(index=False))
    else:
        print("No data to write to CSV.")


if __name__ == "__main__":
    main()
