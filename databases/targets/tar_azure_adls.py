import pandas as pd
import json
import re


def extract_azure_adls_settings(json_data, target_ep_name):
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
        if database['name'] == target_ep_name and database['type_id'] == 'AZURE_ADLS_COMPONENT_TYPE':
            db_settings = database.get('db_settings', {})

            row_data = {
                'target_endpoint_name': database.get('name'),
                'target_additional_properties': db_settings.get('additionalConnectionProperties'),
                'target_username': db_settings.get('storageAccount'),
                'target_server': db_settings.get('adlstenantid'),
                'target_maxFileSize': db_settings.get('maxFileSize'),
                'target_adls_folder': db_settings.get('adlsFolder'),
                'target_adls_addColumnName': db_settings.get('addColumnName'),
                'target_adls_cdcMaxBatchInterval': db_settings.get('cdcMaxBatchInterval'),
                'target_adls_compressionType': db_settings.get('compressionType'),
                'target_adls_createMetadata': db_settings.get('createMetadata'),
                'target_adls_fileFormat': db_settings.get('fileFormat'),
                'target_adls_adlstenantid': db_settings.get('adlstenantid'),
                'target_adls_adlsclientappid': db_settings.get('adlsclientappid'),
                'target_adls_proxyHost': db_settings.get('proxyHost'),
                'target_adls_proxyPort': db_settings.get('proxyPort'),
                'target_adls_proxyScheme': db_settings.get('proxyScheme'),
                'target_adls_useProxyServer': db_settings.get('useProxyServer'),
                'target_adls_storageType': db_settings.get('storageType'),
                'target_adls_fileSystem': db_settings.get('fileSystem'),
                'target_adls_proxyStorage': db_settings.get('proxyStorage'),
                'target_adls_proxyActiveDirectory': db_settings.get('proxyActiveDirectory'),
                'target_db_type': database.get('type_id'),
            }
            data.append(row_data)
            column_names = list(row_data.keys())
            break

    return data, column_names
def extract_azure_adls_data_to_dataframe(json_file, target_name):
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
        with open(json_file, 'r', encoding="utf-8-sig") as f:
            json_data = json.load(f)
        data, column_names = extract_azure_adls_settings(json_data, target_name)
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
    json_file_path = r"C:\Users\VIT\OneDrive - QlikTech Inc\QlikVit\Customers\EdwardJones\PlatformReview\filecloud-20250430192131\tlpreplcdc-004.json"
    csv_file_path = r"C:\Users\VIT\Downloads\snowflake_settings.csv"
    target_name = 'tgt_adls_protect_IAAonlineac_prq' #chnage source name

    df = extract_azure_adls_data_to_dataframe(json_file_path, target_name)
    if not df.empty:
        # write_dataframe_to_csv(df, csv_file_path)
        print(df.fillna('NULL').to_string(index=False))
    else:
        print("No data was extracted. CSV file was not written.")


if __name__ == "__main__":
    main()
