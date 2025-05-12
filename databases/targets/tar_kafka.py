import pandas as pd
import json


def extract_kafka_settings(json_data, target_ep_name):
    """
    Extracts Kafka-specific settings from a JSON data structure.

    Args:
        json_data (dict): The JSON data containing the replication definition.
        target_ep_name (str): The name of the Kafka target endpoint.

    Returns:
        tuple: A tuple containing:
            - data (list of dict): List of Kafka settings dictionaries.
            - column_names (list of str): List of column names.
    """
    databases = json_data.get('cmd.replication_definition', {}).get('databases', [])
    data = []
    column_names = []

    for database in databases:
        if database['name'] == target_ep_name and database['type_id'] == 'KAFKA_COMPONENT_TYPE':
            db_settings = database.get('db_settings', {})
            row_data = {
                'target_endpoint_name': database.get('name'),
                'target_db_type': database.get('type_id'),
                'target_db_role': database.get('role'),
                'kafka_brokers': db_settings.get('brokers', ''),
                'kafka_topic': db_settings.get('topic', ''),
                'kafka_partition_mapping': db_settings.get('partitionMapping', ''),
                'kafka_message_key': db_settings.get('messageKey', ''),
                'kafka_use_ssl': db_settings.get('useSSL', ''),
                'kafka_ssl_ca_path': db_settings.get('sslCAPath', ''),
                'kafka_auth_type': db_settings.get('authType', ''),
                'kafka_auth_public_key_file': db_settings.get('authPublicKeyFile', ''),
                'kafka_auth_private_key_file': db_settings.get('authPrivateKeyFile', ''),
                'kafka_auth_private_key_pass': db_settings.get('authPrivateKeyPass', ''),
                'kafka_envelope_data_messages': db_settings.get('EnvelopDataMessages', ''),
                'kafka_use_avro_logical_types': db_settings.get('useAvroLogicalTypes', ''),
                'kafka_key_format': db_settings.get('keyFormat', ''),
                'kafka_csr_servers': db_settings.get('CsrServers', ''),
                'kafka_csr_use_ssl': db_settings.get('CsrUseSsl', ''),
                'kafka_csr_ssl_ca_path': db_settings.get('CsrSslCaPath', ''),
                'kafka_csr_auth_type': db_settings.get('CsrAuthType', ''),
                'kafka_csr_client_cert_path': db_settings.get('CsrClientCertificatePath', ''),
                'kafka_csr_client_private_key_path': db_settings.get('CsrClientPrivateKeyPath', ''),
                'kafka_csr_subject_compat_mode': db_settings.get('CsrNewSubjectCompatibilityMode', ''),
                'kafka_csr_subject_strategy': db_settings.get('csrSubjectNameStrategy', ''),
            }
            data.append(row_data)
            column_names = list(row_data.keys())
            break
    return data, column_names


def extract_kafka_data_to_dataframe(json_file_path, target_name):
    """
    Loads JSON, extracts Kafka data, and returns it as a DataFrame.

    Args:
        json_file_path (str): Path to the JSON file.
        target_name (str): The Kafka target endpoint name.

    Returns:
        pd.DataFrame: Extracted Kafka settings.
    """
    try:
        with open(json_file_path, 'r') as f:
            json_content = json.load(f)
        data, column_names = extract_kafka_settings(json_content, target_name)
        if data:
            return pd.DataFrame(data, columns=column_names)
        else:
            return pd.DataFrame()
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error: {e}")
        return pd.DataFrame()


def write_dataframe_to_csv(df, csv_file_path):
    """
    Saves DataFrame to a CSV file.

    Args:
        df (pd.DataFrame): Data to save.
        csv_file_path (str): Output file path.
    """
    try:
        df.to_csv(csv_file_path, index=False, na_rep='NULL')
        print(f"Data successfully written to {csv_file_path}")
    except Exception as e:
        print(f"Error writing to CSV: {e}")


def main():
    """
    Main function to run the Kafka settings extractor.
    """
    json_path = r"C:\Users\VIT\OneDrive - QlikTech Inc\QlikVit\Customers\EdwardJones\PlatformReview\filecloud-20250430192131\tlpreplcdc-004.json"
    output_csv = r"C:\path\to\your\kafka_settings.csv"
    target_endpoint_name = "ANN_annuityDB2"

    df = extract_kafka_data_to_dataframe(json_path, target_endpoint_name)
    if not df.empty:
        # write_dataframe_to_csv(df, output_csv)
        print(df.fillna("NULL").to_string(index=False))
    else:
        print("No data extracted.")


if __name__ == "__main__":
    main()
