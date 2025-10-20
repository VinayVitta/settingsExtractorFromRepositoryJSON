import pandas as pd
import json
import re
from helpers.logger_config import setup_logger

logging = setup_logger(__name__)
def extract_oracle_settings(json_data, source_ep_name):
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
        if database['name'] == source_ep_name and database['type_id'] == 'ORACLE_COMPONENT_TYPE':
            logging.debug("Initiate Oracle Settings Extractor")
            db_settings = database.get('db_settings', {})  # Safely get nested settings
            orc_source_server = db_settings.get('server', None)

            # Extract host and service name from TNS, handling potential errors
            tns = db_settings.get('server', '')
            host_pattern = re.compile(r'\(HOST=([^)]+)\)')
            service_pattern = re.compile(r'SERVICE_NAME=([^)]+)')
            orc_source_servers = host_pattern.findall(tns) if host_pattern.search(tns) else [tns] if tns else [None]  #handle if tns is None
            orc_source_service = service_pattern.search(tns).group(1) if service_pattern.search(tns) else tns if tns else None

            asm_tns = db_settings.get('asm_server')
            asm_server = host_pattern.findall(asm_tns) if asm_tns and host_pattern.search(asm_tns) else [asm_tns] if asm_tns else [None]
            # Construct the data dictionary.  Use get() with defaults.
            row_data = {
                'source_server': orc_source_servers[0] if orc_source_servers else None,
                'source_db_type': database.get('type_id'),
                'source_db_role': database.get('role'),
                'source_db_user': db_settings.get('username'),
                'source_logstreamstagingtask': db_settings.get('logstreamstagingtask', 'None'),
                'src_oracle_source_service': orc_source_service,
                'src_oracle_useLogminerReader': db_settings.get('useLogminerReader'),
                'src_oracle_asm_server': asm_server[0] if asm_server else None,
                'src_oracle_asm_user': db_settings.get('asm_user'),
                'src_oracle_useBfile': db_settings.get('useBfile', 'default'),
                'src_oracle_addSupplementalLogging': db_settings.get('addSupplementalLogging', 'default'),
                'src_oracle_accessAlternateDirectly': db_settings.get('accessAlternateDirectly', 'default'),
                'src_oracle_readAheadBlocks': db_settings.get('readAheadBlocks', 'default'),
                'src_oracle_archivedLogDestId': db_settings.get('archivedLogDestId', 'default'),
                'src_oracle_securityDbEncryption': 'Enabled' if db_settings.get('securityDbEncryption') else 'Disabled',
                'src_oracle_additionalArchivedLogDestId': db_settings.get('additionalArchivedLogDestId'),
                'src_oracle_useZeroDestid': db_settings.get('useZeroDestid'),
                'src_oracle_asmUsePLSQLArray': db_settings.get('asmUsePLSQLArray'),
                'src_oracle_skipValidationLongNames': db_settings.get('skipValidationLongNames'),
                'src_oracle_parallelASMReadThreads': db_settings.get('parallelASMReadThreads'),
                'source_endpoint_name': database.get('name')
            }
            data.append(row_data)

            # Define column names *only* when data is found and processed
            column_names = list(row_data.keys())
            break  # important:  Exit the loop after finding the matching database
    return data, column_names  # Return the data and column names


def extract_oracle_data_to_dataframe(json_data, source_name):
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
        data, column_names = extract_oracle_settings(json_content, source_name)  # Pass json_content, not file path
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
    file_path_nondef = r"C:\Users\VIT\Downloads\Replication_Definition_ketan.json"
    csv_file_path = r"C:\Users\VIT\Downloads\oracle_settings.csv"
    source_name = 'txsomp01'

    df = extract_oracle_data_to_dataframe(file_path_nondef, source_name)
    if not df.empty:  # Check if the dataframe is empty
        #write_dataframe_to_csv(df, csv_file_path)
        print(df.fillna('NULL').to_string(index=False))
    else:
        print("No data to write to CSV.")


if __name__ == "__main__":
    main()
