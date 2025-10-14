"""
==============================================================================
 Script Name   : settingsExtractorFromRepositoryJSON - MAIN
 Author        : Vinay Vitta - Qlik Professional Services
 Created Date  : May 2025
 Python Version: 3.10
 Organization  : Qlik
 License       : Qlik PS - QDI
 Description   :
     This is the main driver script that processes Qlik Replicate repository
     JSON exports and QEM TSV exports to extract task, source, target, and
     server settings. The output includes consolidated CSV files and a Word
     summary report.

 Usage         :
     Provide input folder path (containing JSONs) and QEM export TSV file.
     The script automatically:
         1. Cleans the QEM export file.
         2. Extracts all settings from JSONs.
         3. Merges outputs and creates CSVs and Word summary.

 Dependencies  :
     - helpers.utils
     - helpers.summary
     - pandas, glob, os, pathlib
     - All extract modules under databases.sources, targets, tasks, serverSettings

 Notes         :
     - Ensure required helper and database modules are in the PYTHONPATH.
     - Refer README for more configuration details.
==============================================================================
"""

import os
import glob
import sys
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, List

# Import helper modules
import helpers.utils as utils
import helpers.summary as summary

# Import database source/target/task extraction modules
import databases.sources.src_oracle as src_oracle
import databases.sources.src_sqlserver as src_sqlserver
import databases.sources.src_sqlserver_mscdc as src_sqlserver_mscdc
import databases.sources.src_hana_app_db as src_hana
import databases.sources.src_db2zos as src_db2zos
import databases.sources.src_postgres as src_postgres
import databases.sources.src_mongodb as src_mongodb
import databases.targets.tar_snowflake as tar_snowflake
import databases.targets.tar_azure_adls as tar_azure_adls
import databases.targets.tar_logStream as tar_logStream
import databases.targets.tar_kafka as tar_kafka
import databases.tasks.retrieveTaskSettings as retrieveTaskSettings
import databases.tasks.retrieveTables as retrieveTables
import databases.serverSettings.retrieveServerSettings as retrieveServerSettings
import databases.serverSettings.retrieveScheduledTasks as retrieveScheduledTasks
import databases.serverSettings.retrieveNotifications as retrieveNotifications

# Optional: GCP BigQuery write support
from google.cloud import bigquery
from google.oauth2 import service_account
import helpers.bigQueryWriteData as bigQueryWriteData

# ==============================================================================
# Registry Mappings
# ==============================================================================

SOURCE_EXTRACTORS = {
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

TARGET_EXTRACTORS = {
    "SNOWFLAKE_COMPONENT_TYPE": tar_snowflake.extract_snowflake_settings,
    "SNOWFLAKE_AZURE_COMPONENT_TYPE": tar_snowflake.extract_snowflake_settings,
    "AZURE_ADLS_COMPONENT_TYPE": tar_azure_adls.extract_azure_adls_settings,
    "LOG_STREAM_COMPONENT_TYPE": tar_logStream.extract_logstream_settings,
    "KAFKA_COMPONENT_TYPE": tar_kafka.extract_kafka_settings,
}


# ==============================================================================
# Core Processing Logic
# ==============================================================================

def extract_all_settings(json_file_path: str) -> pd.DataFrame:
    """
    Extracts all task, source, and target settings for a single JSON file.

    Args:
        json_file_path (str): Full path to the repository JSON file.

    Returns:
        pd.DataFrame: Combined dataframe with all extracted settings.
    """
    json_data = utils.read_json_from_file(json_file_path)
    if not json_data:
        return pd.DataFrame()

    json_file_name = Path(json_file_path).stem
    tasks = json_data['cmd.replication_definition'].get('tasks', [])
    databases = json_data['cmd.replication_definition'].get('databases', [])

    all_rows = []

    for task in tasks:
        task_name = task['task'].get('name')
        source_name = task['source']['rep_source'].get('source_name')
        target_name = task['targets'][0]['rep_target'].get('target_name')

        # --- Task Settings ---
        task_data, task_cols = retrieveTaskSettings.extract_task_settings(json_file_name, json_data, task_name)
        task_df = pd.DataFrame(task_data, columns=task_cols) if task_data else pd.DataFrame([{}])

        # --- Source Settings ---
        source_df = pd.DataFrame([{}])
        for db in databases:
            if db['name'] == source_name:
                extractor = SOURCE_EXTRACTORS.get(db['type_id'])
                if extractor:
                    src_data, src_cols = extractor(json_data, source_name)
                    source_df = pd.DataFrame(src_data, columns=src_cols) if src_data else pd.DataFrame([{}])
                break

        # --- Target Settings ---
        target_df = pd.DataFrame([{}])
        for db in databases:
            if db['name'] == target_name:
                extractor = TARGET_EXTRACTORS.get(db['type_id'])
                if extractor:
                    tar_data, tar_cols = extractor(json_data, target_name)
                    target_df = pd.DataFrame(tar_data, columns=tar_cols) if tar_data else pd.DataFrame([{}])
                break

        combined = pd.concat(
            [task_df.reset_index(drop=True),
             source_df.reset_index(drop=True),
             target_df.reset_index(drop=True)],
            axis=1
        )
        all_rows.append(combined)

    if not all_rows:
        return pd.DataFrame()

    result = pd.concat(all_rows, ignore_index=True).fillna("NULL")
    return result


# ==============================================================================
# Main Function
# ==============================================================================

def process_repository(folder_path_or_files, qem_export_path: str) -> Dict[str, str]:
    """
    Orchestrates the end-to-end extraction and export process.

    Args:
        folder_path (str): Folder containing all repository JSON files.
        qem_export_path (str): Path to the QEM TSV export.

    Returns:
        Dict[str, str]: Dictionary mapping output file names to file paths.
    """
    # Detect input type
    if isinstance(folder_path_or_files, (list, tuple)):
        json_file_paths = folder_path_or_files
        # Use folder of first JSON for output
        folder_path = str(Path(json_file_paths[0]).parent)
    else:
        folder_path = folder_path_or_files
        json_file_paths = glob.glob(os.path.join(folder_path, "*.json"))

    if not json_file_paths:
        raise FileNotFoundError(f"No JSON files found in {folder_path}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = os.path.join(folder_path, f"run_output_{timestamp}")
    os.makedirs(output_dir, exist_ok=True)

    # Clean QEM export file
    cleaned_qem_path = utils.clean_multiline_tsv(qem_export_path)

    # Collect all extracted data
    task_settings_list, tables_list = [], []
    server_settings_list, schedules_list, notifications_list = [], [], []

    for json_path in json_file_paths:
        json_file_name = Path(json_path).stem
        print(f"Processing: {json_file_name}")

        # --- Task & Tables ---
        task_df = extract_all_settings(json_path)
        table_df = retrieveTables.extract_tables_dataframe(json_file_name, json_path)
        if not task_df.empty:
            task_settings_list.append(task_df)
        if not table_df.empty:
            tables_list.append(table_df)

        # --- Server-level Settings ---
        server_df = retrieveServerSettings.extract_server_data_to_dataframe(json_path)
        schedule_df = retrieveScheduledTasks.extract_server_data_to_dataframe(json_path)
        notification_df = retrieveNotifications.extract_notifications_to_dataframe(json_path)

        if not server_df.empty: server_settings_list.append(server_df)
        if not schedule_df.empty: schedules_list.append(schedule_df)
        if not notification_df.empty: notifications_list.append(notification_df)

    # Combine and export
    def _combine_and_write(dfs: List[pd.DataFrame], filename: str) -> str:
        if not dfs: return ""
        combined = pd.concat(dfs, ignore_index=True)
        output_path = os.path.join(output_dir, filename)
        utils.write_dataframe_to_csv(combined, output_path)
        return output_path

    output_paths = {
        "server_settings": _combine_and_write(server_settings_list, f"serverSettings_{timestamp}.csv"),
        "server_schedules": _combine_and_write(schedules_list, f"serverSchedules_{timestamp}.csv"),
        "notifications": _combine_and_write(notifications_list, f"serverNotifications_{timestamp}.csv"),
        "task_settings": _combine_and_write(task_settings_list, f"taskSettings_{timestamp}.csv"),
        "tables": _combine_and_write(tables_list, f"tables_{timestamp}.csv"),
    }

    # Load and merge QEM data
    qem_df = pd.read_csv(cleaned_qem_path, sep="\t", engine="python")
    # Add prefix
    qem_df = qem_df.add_prefix("qem_")
    # Find the correct columns (case-insensitive)
    qem_task_col = next((c for c in qem_df.columns if c.lower() == "qem_task"), None)
    qem_server_col = next((c for c in qem_df.columns if c.lower() == "qem_server"), None)

    if not qem_task_col or not qem_server_col:
        raise KeyError(f"Required QEM columns not found. Columns: {qem_df.columns.tolist()}")
    qem_path = os.path.join(output_dir, f"qem_data_{timestamp}.csv")
    utils.write_dataframe_to_csv(qem_df, qem_path)
    output_paths["qem_export"] = qem_path

    combined_task_df = pd.concat(task_settings_list, ignore_index=True)
    combined_tables_df = pd.concat(tables_list, ignore_index=True)

    # --- Merge Task Settings with QEM Export ---
    task_qem_merged = combined_task_df.merge(
        qem_df,
        left_on=['task_name', 'json_file_name'],
        right_on=[qem_task_col, qem_server_col],
        how='left'
    )

    # Write intermediate output: Task + QEM merged file
    task_qem_path = os.path.join(output_dir, f"task_settings_qem_merge_{timestamp}.csv")
    utils.write_dataframe_to_csv(task_qem_merged, task_qem_path)
    output_paths["task_qem_merge"] = task_qem_path

    # --- Merge Task+QEM with Tables ---
    merged_df = task_qem_merged.merge(
        combined_tables_df,
        left_on=['task_name', 'json_file_name'],
        right_on=['tables_task_name', 'tables_json_file_name'],
        how='outer'
    )

    # Write final combined output
    merged_path = os.path.join(output_dir, f"exportRepositoryCSV_{timestamp}.csv")
    utils.write_dataframe_to_csv(merged_df, merged_path)
    output_paths["merged"] = merged_path

    # Generate Word summary
    summary_path = os.path.join(output_dir, f"task_summary_{timestamp}.docx")
    summary.create_summary(merged_path, summary_path)
    output_paths["summary_doc"] = summary_path

    print("\n✅ Processing completed successfully!")
    for k, v in output_paths.items():
        print(f"{k:20}: {v}")

    return output_paths


# ==============================================================================
# Entry Point
# ==============================================================================

if __name__ == "__main__":
    try:
        # Example configuration (replace with your own paths)
        folder_path = r"C:\Users\VIT\OneDrive - QlikTech Inc\QlikVit\Customers\Dennis"
        qem_export_path = os.path.join(folder_path, "AemTasks_2025-10-08_13.48.03.491.tsv")

        process_repository(folder_path, qem_export_path)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)
