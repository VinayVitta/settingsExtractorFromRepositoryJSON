# Handling Null settings
import pandas as pd

import json




def extract_logstream_settings(json_data, target_ep_name):

    databases = json_data.get('cmd.replication_definition', {}).get('databases', [])
    data = []
    column_names = []

    for database in databases:
        # print(database['name'])
        if database['name'] == target_ep_name and database['type_id'] in ['LOG_STREAM_COMPONENT_TYPE']:
            db_settings = database.get('db_settings', {})

            row_data = {
                'target_endpoint_name': database.get('name'),
                'target_db_type': database.get('type_id'),
                'logStream_path': db_settings.get('path'),
                'logStream_retention': db_settings.get('retentionmaxagehours'),
            }
            data.append(row_data)
            column_names = list(row_data.keys())
            break

    return data, column_names
def extract_logstream_data_to_dataframe(json_file, target_name):
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
        data, column_names = extract_logstream_settings(json_data, target_name)
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
    target_name = 'prod038_LogStream' #chnage source name

    null_df = extract_logstream_data_to_dataframe(json_file_path, target_name)
    print('aaaa')
    if not null_df.empty:
        # write_dataframe_to_csv(df, csv_file_path)
        print("not empty")
        print(null_df.fillna('NULL').to_string(index=False))
    else:
        print("No data was extracted. CSV file was not written.")


if __name__ == "__main__":
    main()
