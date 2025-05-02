import pandas as pd
import json
import re

def extract_snowflake_settings(json_data, target_ep_name):
    """
    Extracts Snowflake-specific settings from a JSON data structure.

    Args:
        json_data (dict): The JSON data containing the replication definition.
        target_ep_name (str): The name of the target endpoint.

    Returns:
        tuple: A tuple containing:
            - data (list of dict): A list of dictionaries, where each dictionary
              represents a row of data. Returns an empty list if no relevant data is found.
            - column_names (list of str): A list of column names.
              Returns an empty list if no relevant data is found.
    """
    databases = json_data.get('cmd.replication_definition', {}).get('databases', [])
    data = []
    column_names = []

    for database in databases:
        if database['name'] == target_ep_name and database['type_id'] in ['SNOWFLAKE_AZURE_COMPONENT_TYPE', 'SNOWFLAKE_COMPONENT_TYPE']:
            db_settings = database.get('db_settings', {})

            row_data = {
                'target_endpoint_name': database.get('name'),
                'target_additional_properties': db_settings.get('additionalConnectionProperties'),
                'target_username': db_settings.get('username'),
                'target_server': db_settings.get('server'),
                'target_database': db_settings.get('database'),
                'target_maxFileSize': db_settings.get('maxFileSize'),
                'target_updateOneRow': db_settings.get('updateOneRow'),
                'target_loadTimeout': db_settings.get('loadTimeout'),
                'target_afterConnectScript': db_settings.get('afterConnectScript'),
                'target_executeTimeout': db_settings.get('executeTimeout'),
                'target_warehouse': db_settings.get('warehouse'),
                'target_stagingtype': db_settings.get('stagingtype'),
                'target_maxparalleltransfers': db_settings.get('maxparalleltransfers'),
                'target_parallelPut': db_settings.get('parallelPut'),
                'target_blobstorageaccountname': db_settings.get('blobstorageaccountname'),
                'target_blobstoragecontainer': db_settings.get('blobstoragecontainer'),
                'target_blobstoragefolder': db_settings.get('blobstoragefolder'),
                'target_safeguardPolicy': db_settings.get('target_safeguardPolicy'),
            }
            data.append(row_data)
            column_names = ['target_endpoint_name', 'target_additional_properties', 'target_username',
                            'target_server', 'target_database', 'target_maxFileSize', 'target_updateOneRow',
                            'target_loadTimeout', 'target_afterConnectScript', 'target_executeTimeout',
                            'target_warehouse', 'target_stagingtype', 'target_maxparalleltransfers',
                            'target_parallelPut', 'target_blobstorageaccountname', 'target_blobstoragecontainer',
                            'target_blobstoragefolder', 'target_safeguardPolicy']
            break

    return data, column_names
def extract_snowflake_data_to_dataframe(json_file, target_name):
    """
    Reads a JSON file, extracts relevant Snowflake data, and returns a Pandas DataFrame.

    Args:
        json_file (str): The path to the JSON file.
         target_name (str): The target name

    Returns:
        pandas.DataFrame: A Pandas DataFrame containing the extracted data, or an
        empty DataFrame if an error occurs or no data is found.
    """
    try:
        with open(json_file, 'r') as f:
            json_data = json.load(f)
        data, column_names = extract_snowflake_settings(json_data, target_name)
        if data:
            return pd.DataFrame(data, columns=column_names)
        else:
            return pd.DataFrame()
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error reading JSON file: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return pd.DataFrame()



def write_dataframe_to_csv(df, csv_file):
    """
    Writes the given Pandas DataFrame to a CSV file.

    Args:
        df (pandas.DataFrame): The DataFrame to write to CSV.
        csv_file (str): The path to the CSV file to create.
    """
    try:
        df.to_csv(csv_file, index=False)
        print(f"Successfully wrote data to {csv_file}")
    except Exception as e:
        print(f"Error writing to CSV file: {e}")
def main():
    """
    Main function to orchestrate the extraction and CSV writing.
    """
    json_file_path = r"C:\Users\VIT\Downloads\Replication_Definition_ketan.json"
    csv_file_path = r"C:\Users\VIT\Downloads\snowflake_settings.csv"
    target_name = 'SNOWFLAKE_TEST_TARGET' #chnage source name

    df = extract_snowflake_data_to_dataframe(json_file_path, target_name)
    if not df.empty:
        write_dataframe_to_csv(df, csv_file_path)
        print(df.fillna('NULL').to_string(index=False))
    else:
        print("No data was extracted. CSV file was not written.")


if __name__ == "__main__":
    main()
