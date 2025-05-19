from datetime import datetime
import os
# All Supported sources
import databases.sources.src_oracle as src_oracle
import databases.sources.src_sqlserver as src_sqlserver
import databases.sources.src_hana_app_db as src_hana
import databases.sources.src_db2zos as src_db2zos
import databases.sources.src_postgres as src_postgres
import databases.sources.src_mongodb as src_mongodb
# All Targets
import databases.targets.tar_snowflake as tar_snowflake
import databases.targets.tar_azure_adls as tar_azure_adls
import databases.targets.tar_logStream as tar_logStream
import databases.targets.tar_kafka as tar_kafka
# import databases.targets.tar_databricks as tar_databricks
import databases.tasks.retrieveTaskSettings as retrieveTaskSettings
import databases.tasks.retrieveTables as retrieveTables
import helpers.utils as utils
import pandas as pd
import sys

# Registry for source database extractors
source_extractors = {
    "ORACLE_COMPONENT_TYPE": src_oracle.extract_oracle_settings,
    "SQL_SERVER_COMPONENT_TYPE": src_sqlserver.extract_sql_server_settings,
    "HANA_COMPONENT_TYPE": src_hana.extract_sap_hana_settings,
    "DB2ZOS_NATIVE_COMPONENT_TYPE": src_db2zos.extract_db2zos_settings,
    "RDS_POSTGRESQL_COMPONENT_TYPE": src_postgres.extract_postgres_settings,
    "CUSTOM_COMPONENT_TYPE": src_mongodb.extract_mongodb_settings
}

# Registry for target database extractors
target_extractors = {
    "SNOWFLAKE_COMPONENT_TYPE": tar_snowflake.extract_snowflake_settings,
    "SNOWFLAKE_AZURE_COMPONENT_TYPE": tar_snowflake.extract_snowflake_settings,
    "AZURE_ADLS_COMPONENT_TYPE": tar_azure_adls.extract_azure_adls_settings,
    "LOG_STREAM_COMPONENT_TYPE": tar_logStream.extract_logstream_settings,
    "KAFKA_COMPONENT_TYPE": tar_kafka.extract_kafka_settings
    # "DATABRICKS_COMPONENT_TYPE": tar_databricks.extract_databricks_settings,
}


def extract_all_settings(json_file_path):
    json_data = utils.read_json_from_file(json_file_path)
    if not json_data:
        return pd.DataFrame()

    tasks = json_data['cmd.replication_definition']['tasks']
    databases = json_data['cmd.replication_definition']['databases']

    all_dataframes = []

    for task in tasks:
        task_name = task['task'].get('name')
        source_name = task['source']['rep_source'].get('source_name')
        target_name = task['targets'][0]['rep_target'].get('target_name')

        task_df = pd.DataFrame()
        source_df = pd.DataFrame()
        target_df = pd.DataFrame()

        # Extract task settings
        task_result = retrieveTaskSettings.extract_task_settings(json_data, task_name)
        if isinstance(task_result, tuple) and len(task_result) == 2:
            task_data, task_columns = task_result
            if task_data:
                task_df = pd.DataFrame(task_data, columns=task_columns)

        # Extract source settings
        for database in databases:
            if database['name'] == source_name:
                db_type = database['type_id']
                extractor = source_extractors.get(db_type)
                if extractor:
                    source_data, source_columns = extractor(json_data, source_name)
                    if source_data:
                        source_df = pd.DataFrame(source_data, columns=source_columns)
                break

        # Extract target settings
        for database in databases:
            if database['name'] == target_name:
                db_type = database['type_id']
                extractor = target_extractors.get(db_type)
                if extractor:
                    target_data, target_columns = extractor(json_data, target_name)
                    if target_data:
                        target_df = pd.DataFrame(target_data, columns=target_columns)
                break

        # Handle empty DataFrames
        if task_df.empty:
            task_df = pd.DataFrame([{}])
        if source_df.empty:
            source_df = pd.DataFrame([{}])
        if target_df.empty:
            target_df = pd.DataFrame([{}])

        # Combine horizontally
        combined_row = pd.concat([
            task_df.reset_index(drop=True),
            source_df.reset_index(drop=True),
            target_df.reset_index(drop=True)
        ], axis=1)

        all_dataframes.append(combined_row)

    non_empty_rows = [
        df for df in all_dataframes
        if not df.empty and not df.isna().all(axis=1).all()
    ]
    if non_empty_rows:
        result = pd.concat(non_empty_rows, ignore_index=True)
        # turn all real NaN/None into the string 'NULL'
        #return result.fillna('NULL')
        # return pd.concat(result.fillna('NULL'), ignore_index=True) if non_empty_rows else pd.DataFrame()
        return result.fillna('NULL')
    else:
        return pd.DataFrame()


if __name__ == "__main__":
    json_file_path = r"C:\Users\VIT\OneDrive - QlikTech Inc\QlikVit\Customers\EdwardJones\PlatformReview\filecloud-20250430192131\tlpreplcdc-004.json"
    csv_file_path = r"C:\Users\VIT\OneDrive - QlikTech Inc\QlikVit\Customers\EdwardJones\PlatformReview\filecloud-20250430192131\tlpreplcdc-004.csv"
    qem_export_path = r"C:\Users\VIT\OneDrive - QlikTech Inc\QlikVit\Customers\EdwardJones\PlatformReview\filecloud-20250430192131\AemTasks_2025-04-25_13.35.53.8.tsv"

    # For results
    # Extract directory
    # Get base directory from the input JSON path
    dir_path = os.path.dirname(json_file_path)

    # Generate timestamped subfolder
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_subdir = os.path.join(dir_path, f"run_output_{timestamp}")
    os.makedirs(output_subdir, exist_ok=True)

    # File paths within the new subfolder
    task_settings_path = os.path.join(output_subdir, f"taskSettings_{timestamp}.csv")
    tables_path = os.path.join(output_subdir, f"tables_{timestamp}.csv")
    qem_export_result_path = os.path.join(output_subdir, f"qem_data_{timestamp}.csv")
    task_settings_tables_qem_path = os.path.join(output_subdir, f"exportRepositoryCSV_{timestamp}.csv")

    # Extract Task Settings
    task_settings_df = extract_all_settings(json_file_path)
    if not task_settings_df.empty:
        print("Task settings are not empty. Continuing...")
        utils.write_dataframe_to_csv(task_settings_df, task_settings_path)
    else:
        print("No task data extracted.")

    # Extract Tables
    tables_df = retrieveTables.extract_tables_dataframe(json_file_path)
    if not tables_df.empty:
        print("Tables list is not empty. Continuing...")
        utils.write_dataframe_to_csv(tables_df, tables_path)
    else:
        print("No table data extracted.")

    # Load and prefix QEM data
    qem_export_df = utils.load_and_prefix_columns(qem_export_path, prefix="qem_")
    if not qem_export_df.empty:
        print("QEM data is not empty. Continuing...")
        utils.write_dataframe_to_csv(qem_export_df, qem_export_result_path)
    else:
        print("No QEM export data extracted.")

    # Merge all DataFrames
    merged_df = task_settings_df.merge(
        qem_export_df, left_on='task_name', right_on='qem_Task', how='left'
    ).merge(
        tables_df, left_on='task_name', right_on='tables_task_name', how='outer'
    )

    # print(merged_df.fillna('NULL').to_string(index=False))
    utils.write_dataframe_to_csv(merged_df, task_settings_tables_qem_path)

