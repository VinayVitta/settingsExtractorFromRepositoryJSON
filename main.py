import databases.sources.src_oracle as src_oracle
import databases.sources.src_sqlserver as src_sqlserver
import databases.sources.src_hana_app_db as src_hana
import databases.sources.src_db2zos as src_db2zos
import databases.targets.tar_snowflake as tar_snowflake
import databases.targets.tar_azure_adls as tar_azure_adls
# import databases.targets.tar_databricks as tar_databricks
import databases.tasks.retrieveTaskSettings as retrieveTaskSettings
import helpers.utils as utils
import pandas as pd
import sys

# Registry for source database extractors
source_extractors = {
    "ORACLE_COMPONENT_TYPE": src_oracle.extract_oracle_settings,
    "SQL_SERVER_COMPONENT_TYPE": src_sqlserver.extract_sql_server_settings,
    "HANA_COMPONENT_TYPE": src_hana.extract_hana_settings,
    "DB2ZOS_COMPONENT_TYPE": src_db2zos.extract_db2zos_settings,
}

# Registry for target database extractors
target_extractors = {
    "SNOWFLAKE_COMPONENT_TYPE": tar_snowflake.extract_snowflake_settings,
    "SNOWFLAKE_AZURE_COMPONENT_TYPE": tar_snowflake.extract_snowflake_settings,
    "AZURE_ADLS_COMPONENT_TYPE": tar_azure_adls.extract_azure_adls_settings #,
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
    return pd.concat(non_empty_rows, ignore_index=True) if non_empty_rows else pd.DataFrame()


if __name__ == "__main__":
    json_file_path = r"C:\Users\VIT\OneDrive - QlikTech Inc\QlikVit\Customers\EdwardJones\PlatformReview\filecloud-20250430192131\tlpreplcdc-004.json"
    csv_file_path = r"C:\\Users\\VIT\\Downloads\\Replication_Definition_ketan_tables_new3.csv"

    combined_df = extract_all_settings(json_file_path)
    if not combined_df.empty:
        print(combined_df.fillna('NULL').to_string(index=False))
        # utils.write_dataframe_to_csv(combined_df, csv_file_path)
    else:
        print("No data extracted.")
