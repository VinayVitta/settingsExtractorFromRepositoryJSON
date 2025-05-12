import pandas as pd
import json
import re

def extract_task_settings(json_data, target_task_name):
    """
    Extracts task settings from a JSON data structure for a specific task name.

    Args:
        json_data (dict): The JSON data containing the replication definition.
        target_task_name (str): The name of the task to extract settings for.

    Returns:
        tuple: A tuple containing:
            - data (list of dict): A list containing a single dictionary with the
              task's data, or an empty list if the task is not found.
            - column_names (list of str): A list of column names.
              Returns an empty list if the task is not found.
    """
    tasks = json_data.get('cmd.replication_definition', {}).get('tasks', [])
    data = []
    column_names = [
        'replicate_server', 'task_name', 'task_type', 'source_name', 'target_names', 'lob_max_size',
        'table_count', 'target_table_schema', 'replication_hist_timeslot',
        'attrep_exceptions_table', 'attrep_status_table', 'attrep_suspended_table',
        'attrep_history_table', 'full_load', 'full_load_drop_target_tables',
        'full_load_do_nothing', 'full_load_truncate_target_tables',
        'create_pk_after_data_load', 'stop_task_after_full_load',
        'stop_task_after_cached_events', 'max_full_load_tables',
        'transaction_consistency_timeout', 'full_load_commit_rate', 'apply_changes',
        'cdc_when_source_table_dropped', 'cdc_when_source_truncate',
        'cdc_when_source_ddl', 'store_changes', 'store_changes_suffix',
        'store_changes_column_prefix', 'store_changes_handle_DDL',
        'store_changes_on_update', 'change_table_creation', 'header_columns_change_seq',
        'header_columns_change_oper', 'header_columns_change_mask',
        'header_columns_change_stream', 'header_columns_change_operation',
        'header_columns_change_tran_id', 'header_columns_change_timestamp',
        'statements_cache_size', 'cdc_batch_apply', 'cdc_batch_min',
        'cdc_batch_max', 'cdc_batch_memory_limit', 'cdc_bulk_parallel_apply',
        'cdc_bulk_parallel_apply_threads', 'cdc_transaction_memory',
        'cdc_transaction_keep_time', 'cdc_statement_cache',
        'cdc_store_recovery_in_target', 'pk_changes_handle_delete_insert',
        'use_merge_for_batch', 'error_policy_apply_conflicts', 'delete_policy',
        'insert_policy', 'update_policy', 'escalation_policy', 'stream_buffers_number', 'stream_buffer_size',
        'target_ep_name'
    ]

    for task in tasks:
        if task.get('task', {}).get('name') == target_task_name:
            task_data = {}
            replicate_json_hostname = json_data.get('description', '')
            replicate_server = (re.search(r"Host name:\s([a-zA-Z0-9.-]+)", replicate_json_hostname)).group(
                1) if replicate_json_hostname and re.search(r"Host name:\s([a-zA-Z0-9.-]+)",
                                                                                                    replicate_json_hostname) else None
            task_data['replicate_server'] = replicate_server
            task_data['task_name'] = task.get('task', {}).get('name')
            task_type = 'logstream' if '_LOG_STREAM' == task.get('task', {}).get('task_type') else 'replication'
            task_data['task_type'] = task_type
            task_data['source_name'] = task.get('source', {}).get('rep_source', {}).get('source_name')
            target_names = task.get('targets', [{}])[0].get('rep_target', {}).get('target_name')
            task_data['target_names'] = target_names
            task_settings = task.get('task_settings', {})

            if task_type == 'replication':
                common_settings = task_settings.get('common_settings', {})
                target_settings = task_settings.get('target_settings', {})
                sorter_settings = task_settings.get('sorter_settings', {})
                error_behavior = task.get('error_behavior', {}).get('apply_error_behavior', {})

                task_data['lob_max_size'] = common_settings.get('lob_max_size', 8)
                task_data['lob_max_size'] = 'UnlimitedLob' if task_data['lob_max_size'] == 0 else task_data['lob_max_size']
                task_data['table_count'] = len(
                    task.get('source', {}).get('source_tables', {}).get('explicit_included_tables', []))
                task_data['target_table_schema'] = target_settings.get('default_schema', 'NULL')
                task_data['replication_hist_timeslot'] = common_settings.get('history_timeslot', 5)
                task_data['attrep_exceptions_table'] = 'Disabled' if common_settings.get(
                    'exception_table_enabled') else 'Enabled'
                task_data['attrep_status_table'] = 'Enabled' if common_settings.get(
                    'status_table_enabled') else 'Disabled'
                task_data['attrep_suspended_table'] = 'Enabled' if common_settings.get(
                    'suspended_tables_table_enabled') else 'Disabled'
                task_data['attrep_history_table'] = 'Enabled' if common_settings.get(
                    'history_table_enabled') else 'Disabled'
                task_data['full_load'] = 'Disabled' if common_settings.get('full_load_enabled') else 'Enabled'
                task_data['full_load_drop_target_tables'] = 'Disable' if target_settings.get(
                    'artifacts_cleanup_enabled') else 'Enable'
                task_data['full_load_do_nothing'] = 'Enable' if target_settings.get('drop_table_if_exists') and not target_settings.get(
                    'truncate_table_if_exists') else 'Disable'
                task_data['full_load_truncate_target_tables'] = 'Enable' if target_settings.get(
                    'truncate_table_if_exists') else 'Disable'
                task_data['create_pk_after_data_load'] = 'Enable' if target_settings.get(
                    'create_pk_after_data_load') else 'Disable'
                task_data['stop_task_after_full_load'] = 'Enable' if common_settings.get(
                    'stop_task_after_full_load') else 'Disable'
                task_data['stop_task_after_cached_events'] = 'Enable' if common_settings.get(
                    'stop_task_after_cached_events') else 'Disable'
                task_data['max_full_load_tables'] = task_settings.get('full_load_sub_tasks', 5)
                task_data['transaction_consistency_timeout'] = sorter_settings.get('transaction_consistency_timeout', 600)
                task_data['full_load_commit_rate'] = target_settings.get('max_transaction_size', 10000)
                task_data['apply_changes'] = 'Disable' if common_settings.get('apply_changes_enabled') else 'Enable'
                task_data['cdc_when_source_table_dropped'] = target_settings.get('handle_drop_ddl', 'True')
                task_data['cdc_when_source_truncate'] = target_settings.get('handle_truncate_ddl', 'True')
                task_data['cdc_when_source_ddl'] = target_settings.get('handle_column_ddl', 'True')
                task_data['store_changes'] = 'Enable' if common_settings.get('save_changes_enabled') else 'Disable'

                if task_data['store_changes'] == 'Enable':
                    change_table_settings = common_settings.get('change_table_settings', {})
                    task_data['store_changes_suffix'] = change_table_settings.get('table_suffix', '__ct')
                    task_data['store_changes_column_prefix'] = change_table_settings.get('column_prefix', 'header__')
                    task_data['store_changes_handle_DDL'] = 'Ignore' if change_table_settings.get(
                        'handle_ddl') else 'Apply_to_change_table'
                    task_data['store_changes_on_update'] = 'Store_after_image_only' if change_table_settings.get(
                        'skip_before_image') else 'store_before_and_after_image'
                    task_data['change_table_creation'] = change_table_settings.get('start_table_behaviour',
                                                                                  'drop_and_create_change_table')
                    header_columns_settings = change_table_settings.get('header_columns_settings', {})
                    task_data['header_columns_change_seq'] = 'Disable' if header_columns_settings.get(
                        'change_seq') else 'Enable'
                    task_data['header_columns_change_oper'] = 'Disable' if header_columns_settings.get(
                        'change_oper') else 'Enable'
                    task_data['header_columns_change_mask'] = 'Disable' if header_columns_settings.get(
                        'change_mask') else 'Enable'
                    task_data['header_columns_change_stream'] = 'Disable' if header_columns_settings.get(
                        'stream_position') else 'Enable'
                    task_data['header_columns_change_operation'] = 'Disable' if header_columns_settings.get(
                        'operation') else 'Enable'
                    task_data['header_columns_change_tran_id'] = 'Disable' if header_columns_settings.get(
                        'transaction_id') else 'Enable'
                    task_data['header_columns_change_timestamp'] = 'Disable' if header_columns_settings.get(
                        'timestamp') else 'Enable'
                else:
                    task_data['store_changes_suffix'] = 'NULL'
                    task_data['store_changes_column_prefix'] = 'NULL'
                    task_data['store_changes_handle_DDL'] = 'NULL'
                    task_data['store_changes_on_update'] = 'NULL'
                    task_data['change_table_creation'] = 'NULL'
                    task_data['header_columns_change_seq'] = 'NULL'
                    task_data['header_columns_change_oper'] = 'NULL'
                    task_data['header_columns_change_mask'] = 'NULL'
                    task_data['header_columns_change_stream'] = 'NULL'
                    task_data['header_columns_change_operation'] = 'NULL'
                    task_data['header_columns_change_tran_id'] = 'NULL'
                    task_data['header_columns_change_timestamp'] = 'NULL'

                task_data['statements_cache_size'] = target_settings.get('statements_cache_size', 50)
                task_data['cdc_batch_apply'] = 'disable' if common_settings.get('batch_apply_enabled') else 'enable'
                task_data['cdc_batch_min'] = common_settings.get('batch_apply_timeout_min', 1)
                task_data['cdc_batch_max'] = common_settings.get('batch_apply_timeout', 30)
                task_data['cdc_batch_memory_limit'] = common_settings.get('batch_apply_memory_limit', 500)
                task_data['cdc_bulk_parallel_apply'] = 'enable' if common_settings.get(
                    'batch_apply_use_parallel_bulk') else 'disable'
                task_data['cdc_bulk_parallel_apply_threads'] = common_settings.get('parallel_bulk_max_num_threads',
                                                                                  'NULL')
                task_data['cdc_transaction_memory'] = sorter_settings.get('local_transactions_storage', {}).get(
                    'memory_limit_total', 1024)
                task_data['cdc_transaction_keep_time'] = sorter_settings.get('local_transactions_storage', {}).get(
                    'memory_keep_time', 60)
                task_data['cdc_statement_cache'] = target_settings.get('statements_cache_size', 50)
                task_data['cdc_store_recovery_in_target'] = 'enable' if common_settings.get(
                    'recovery_table_enabled') else 'disable'
                task_data['pk_changes_handle_delete_insert'] = 'enable' if common_settings.get(
                    'write_pk_changes_as_delete_insert') else 'disable'
                task_data['use_merge_for_batch'] = 'enable' if common_settings.get('batch_optimize_by_merge') else 'disable'
                task_data['error_policy_apply_conflicts'] = 'task_policy' if 'error_behavior' in task else 'global_policy'
                if task_data['error_policy_apply_conflicts'] == 'task_policy':
                    task_data['delete_policy'] = error_behavior.get('delete_policy', 'ignore')
                    task_data['insert_policy'] = error_behavior.get('insert_policy', 'ignore')
                    task_data['update_policy'] = error_behavior.get('update_policy', 'ignore')
                    task_data['escalation_policy'] = error_behavior.get('escalation_policy', 'ignore')
                else:
                    task_data['delete_policy'] = 'global_policy'
                    task_data['insert_policy'] = 'global_policy'
                    task_data['update_policy'] = 'global_policy'
                    task_data['escalation_policy'] = 'global_policy'

                task_data['stream_buffers_number'] = common_settings.get('stream_buffers_number', 'default')
                task_data['stream_buffer_size'] = common_settings.get('stream_buffer_size', 'default')
                task_data['target_ep_name'] = task.get('targets', [{}])[0].get('rep_target', {}).get('target_name')


            elif task_type == 'logstream':
                common_settings = task_settings.get('common_settings', {})
                task_data['lob_max_size'] = 8 if 'lob_max_size' not in common_settings else common_settings.get(
                    'lob_max_size')
                task_data['table_count'] = len(
                    task.get('source', {}).get('source_tables', {}).get('explicit_included_tables', []))
                task_data['stream_buffers_number'] = common_settings.get('stream_buffers_number', 'default')
                task_data['stream_buffer_size'] = common_settings.get('stream_buffer_size', 'default')
                task_data['full_load'] = 'LogStream_Task'
                task_data['target_ep_name'] = task.get('targets', [{}])[0].get('rep_target', {}).get('target_name')
                task_data['target_db_type'] = 'LOG_STREAM_COMPONENT_TYPE'

            data.append(task_data)
            return data, column_names  # Return the data for the found task

    return [], []  # Return empty lists if the task is not found



def extract_data_to_dataframe(json_file_path, target_task_name):
    """
    Reads a JSON file, extracts task data for a specific task name, and returns a Pandas DataFrame.

    Args:
        json_file_path (str): The path to the JSON file.
        target_task_name (str): The name of the task to extract data for.

    Returns:
        pandas.DataFrame: A Pandas DataFrame containing the extracted task data,
                        or an empty DataFrame if an error occurs or the task is not found.
    """
    try:
        with open(json_file_path, 'r') as f:
            json_data = json.load(f)
        data, column_names = extract_task_settings(json_data, target_task_name)
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



def write_dataframe_to_csv(df, csv_file_path):
    """
    Writes a Pandas DataFrame to a CSV file.

    Args:
        df (pandas.DataFrame): The Pandas DataFrame to write to CSV.
        csv_file_path (str): The path to the CSV file.
    """
    try:
        df.to_csv(csv_file_path, index=False)
        print(f"Successfully wrote data to {csv_file_path}")
    except Exception as e:
        print(f"Error writing to CSV file: {e}")



def main():
    """
    Main function to orchestrate the data extraction and CSV writing.
    """
    json_file_path = r"C:\Users\VIT\Downloads\Replication_Definition_ketan.json"
    csv_file_path = r"C:\Users\VIT\Downloads\task_settings.csv"
    target_task_name = "ERXDW_TXAVRP01_IMG_OLTP_SnowflakeProd"  # Replace with the task name you want to extract
    df = extract_data_to_dataframe(json_file_path, target_task_name)
    if not df.empty:
        # write_dataframe_to_csv(df, csv_file_path)
        print(df)  # print the dataframe
    else:
        print(f"No data was extracted for task: {target_task_name}. CSV file was not written.")



if __name__ == "__main__":
    main()
