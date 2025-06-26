"""
==============================================================================
Script Name   : settingsExtractorFromRepositoryJSON - MAIN
Author        : Vinay Vitta - Qlik Professional Services
Created Date  : May 2025
Python Version: 3.10
Organization  : Qlik
License       : Qlik PS - QDI
Description   :
    This is the main script - which will read JSON and QEM TSV and exports task settings and Word doc.

Usage         :
    pass the JSON files and QEM Export to run this

Dependencies  :
    - Refer READ me for all dependencies

Notes         :
    - Refer READ me.
    - More source and Targets can be included.

==============================================================================
"""
import glob
from datetime import datetime
import os
from time import sleep

# All Supported sources
import databases.sources.src_oracle as src_oracle
import databases.sources.src_sqlserver as src_sqlserver
import databases.sources.src_sqlserver_mscdc as src_sqlserver_mscdc
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
import databases.serverSettings.retrieveServerSettings as retrieveServerSettings
import databases.serverSettings.retrieveScheduledTasks as retrieveScheduledTasks
import databases.serverSettings.retrieveNotifications as retrieveNotifications
import helpers.utils as utils
import pandas as pd
import helpers.summary as summary
import sys


# For writing to BQ
from google.cloud import bigquery
from google.oauth2 import service_account
import helpers.bigQueryWriteData as bigQueryWriteData

# Registry for source database extractors
source_extractors = {
    "ORACLE_COMPONENT_TYPE": src_oracle.extract_oracle_settings,
    "SQL_SERVER_COMPONENT_TYPE": src_sqlserver.extract_sql_server_settings,
    "SAP_APPLICATION_COMPONENT_TYPE": src_hana.extract_sap_hana_settings,
    "SAP_HANA_SRC_COMPONENT_TYPE": src_hana.extract_sap_hana_settings,
    "SAPDB_COMPONENT_TYPE": src_hana.extract_sap_hana_settings,
    "DB2ZOS_NATIVE_COMPONENT_TYPE": src_db2zos.extract_db2zos_settings,
    "RDS_POSTGRESQL_COMPONENT_TYPE": src_postgres.extract_postgres_settings,
    "CUSTOM_COMPONENT_TYPE": src_mongodb.extract_mongodb_settings,
    "AZURE_SQL_MSCDC_SOURCE_COMPONENT_TYPE": src_sqlserver_mscdc.extract_sql_server_mscdc_settings,
    "MICROSOFT_SQL_SERVER_MSCDC_SOURCE_COMPONENT_TYPE": src_sqlserver_mscdc.extract_sql_server_mscdc_settings,
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
    # json_file_path = r"C:\Users\VIT\OneDrive - QlikTech Inc\QlikVit\Customers\EdwardJones\PlatformReview\filecloud-20250430192131\tlpreplcdc-004.json"
    # csv_file_path = r"C:\Users\VIT\OneDrive - QlikTech Inc\QlikVit\Customers\EdwardJones\PlatformReview\filecloud-20250430192131\tlpreplcdc-004.csv"
    # qem_export_path = r"C:\Users\VIT\OneDrive - QlikTech Inc\QlikVit\Customers\EdwardJones\PlatformReview\filecloud-20250430192131\AemTasks_2025-04-25_13.35.53.8.tsv"

    # Example list of JSON files
    #json_file_paths = [
    #    r"C:\Users\VIT\OneDrive - QlikTech Inc\QlikVit\Customers\EdwardJones\PlatformReview\filecloud-20250430192131\tlpreplcdc-004.json",
    #    r"C:\Users\VIT\OneDrive - QlikTech Inc\QlikVit\Customers\EdwardJones\PlatformReview\filecloud-20250430192131\tlpreplcdc-005.json"
    #]
    folder_path = r"C:\Users\VIT\OneDrive - QlikTech Inc\QlikVit\Customers\Ally\HealthCheck\LatestFIles"
    qem_export_path = r"C:\Users\VIT\OneDrive - QlikTech Inc\QlikVit\Customers\Ally\HealthCheck\LatestFIles\AemTasks_2025-06-11_15.23.08.565.tsv"

    # Clean QEM Export file
    cleaned_qem_export_path = utils.clean_multiline_tsv(qem_export_path)
    # Use glob to get all JSON files in the folder
    json_file_paths = glob.glob(os.path.join(folder_path, "*.json"))
    # Output folder setup
    dir_path = os.path.dirname(json_file_paths[0])
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_subdir = os.path.join(dir_path, f"run_output_{timestamp}")
    os.makedirs(output_subdir, exist_ok=True)

    # Output file paths
    task_settings_path = os.path.join(output_subdir, f"taskSettings_{timestamp}.csv")
    tables_path = os.path.join(output_subdir, f"tables_{timestamp}.csv")
    qem_export_result_path = os.path.join(output_subdir, f"qem_data_{timestamp}.csv")
    task_settings_tables_qem_path = os.path.join(output_subdir, f"exportRepositoryCSV_{timestamp}.csv")
    server_settings_path = os.path.join(output_subdir, f"serverSettings_{timestamp}.csv")
    server_schedule_settings_path = os.path.join(output_subdir, f"serverScheduleSettings_{timestamp}.csv")
    server_notification_settings_path = os.path.join(output_subdir, f"serverNotifications_{timestamp}.csv")
    summary_path = os.path.join(output_subdir, f"task_summary_{timestamp}.docx")

    # Load all task settings and tables
    all_task_settings = []
    all_tables = []
    all_server_settings = []
    all_scheduled_tasks = []
    all_notifications = []

    for json_path in json_file_paths:
        # Fetch task settings for each JSON

        task_settings_df = extract_all_settings(json_path)
        tables_df = retrieveTables.extract_tables_dataframe(json_path)

        if not task_settings_df.empty:
            all_task_settings.append(task_settings_df)
        if not tables_df.empty:
            all_tables.append(tables_df)

        # Fetch all server settings
        server_settings_df = retrieveServerSettings.extract_server_data_to_dataframe(json_path)
        if not server_settings_df.empty:
            all_server_settings.append(server_settings_df)

        # Fetch Schedule information
        scheduled_tasks_df = retrieveScheduledTasks.extract_server_data_to_dataframe(json_path)
        if not scheduled_tasks_df.empty:
            all_scheduled_tasks.append(scheduled_tasks_df)

        # Fetch notifications information
        notification_df = retrieveNotifications.extract_notifications_to_dataframe(json_path)
        if not notification_df.empty:
            all_notifications.append(notification_df)

    # Combine as servers - server settings
    combined_server_settings_df = pd.concat(all_server_settings, ignore_index=True)
    utils.write_dataframe_to_csv(combined_server_settings_df, server_settings_path)
    # print(combined_server_settings_df.to_string(index=False))

    # Combine Scheduled tasks
    combined_scheduled_tasks_df = pd.concat(all_scheduled_tasks, ignore_index=True)
    utils.write_dataframe_to_csv(combined_scheduled_tasks_df, server_schedule_settings_path)
    # print(combined_scheduled_tasks_df.to_string(index=False))

    # Combine Scheduled tasks
    combined_notifications_df = pd.concat(all_notifications, ignore_index=True)
    utils.write_dataframe_to_csv(combined_notifications_df, server_notification_settings_path)
    # print(combined_scheduled_tasks_df.to_string(index=False))

    # Combine all task settings and tables
    combined_task_settings_df = pd.concat(all_task_settings, ignore_index=True)
    combined_tables_df = pd.concat(all_tables, ignore_index=True)

    # Export intermediate files
    utils.write_dataframe_to_csv(combined_task_settings_df, task_settings_path)
    utils.write_dataframe_to_csv(combined_tables_df, tables_path)

    # Load QEM data
    qem_export_df = utils.load_and_prefix_columns(cleaned_qem_export_path, prefix="qem_")
    utils.write_dataframe_to_csv(qem_export_df, qem_export_result_path)

    # Merge everything
    merged_df = combined_task_settings_df.merge(
        qem_export_df, left_on='task_name', right_on='qem_Task', how='left'
    ).merge(
        combined_tables_df, left_on='task_name', right_on='tables_task_name', how='outer'
    )

    utils.write_dataframe_to_csv(merged_df, task_settings_tables_qem_path)
    # for creating summary in word document
    # summary.create_summary(task_settings_tables_qem_path, summary_path)





