import os
import json
import re
import pandas as pd
from helpers.logger_config import setup_logger

# Configure logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

logging = setup_logger(__name__)


def extract_task_settings(json_file_name,json_data, target_task_name):
    """
    Extracts task settings from a JSON data structure for a specific task name.
    Returns a tuple of (data, column_names) or empty lists if not found.
    """
    tasks = json_data.get('cmd.replication_definition', {}).get('tasks', [])
    data = []

    column_names = [
        'json_file_name', 'replicate_server', 'task_name', 'task_type', 'source_name', 'target_names', 'lob_max_size',
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
        'statements_cache_size', 'cdc_apply_method', 'min_transaction_size_tran_apply',
        'commit_timeout_tran_apply', 'cdc_batch_min',
        'cdc_batch_max', 'cdc_batch_memory_limit', 'cdc_bulk_parallel_apply',
        'cdc_bulk_parallel_apply_threads', 'cdc_transaction_memory',
        'cdc_transaction_keep_time', 'cdc_statement_cache',
        'cdc_store_recovery_in_target', 'pk_changes_handle_delete_insert',
        'use_merge_for_batch', 'error_policy_apply_conflicts', 'delete_policy',
        'insert_policy', 'update_policy', 'escalation_policy', 'stream_buffers_number',
        'stream_buffer_size', 'target_ep_name'
    ]

    for task in tasks:
        if task.get('task', {}).get('name') != target_task_name:
            continue

        task_data = {}
        replicate_server = None
        description = json_data.get('description', '')
        match = re.search(r"Host name:\s([a-zA-Z0-9.-]+)", description)
        if match:
            replicate_server = match.group(1)

        task_data['json_file_name'] = json_file_name
        task_data['replicate_server'] = replicate_server
        task_data['task_name'] = task.get('task', {}).get('name')
        task_type = 'logstream' if '_LOG_STREAM' == task.get('task', {}).get('task_type') else 'replication'
        task_data['task_type'] = task_type

        source = task.get('source', {}).get('rep_source', {})
        task_data['source_name'] = source.get('source_name')

        target = task.get('targets', [{}])[0].get('rep_target', {})
        task_data['target_names'] = target.get('target_name')

        task_settings = task.get('task_settings', {})
        common = task_settings.get('common_settings', {})
        target_set = task_settings.get('target_settings', {})
        sorter = task_settings.get('sorter_settings', {})
        error_behavior = task.get('error_behavior', {}).get('apply_error_behavior', {})

        if task_type == 'replication':
            included_tables = task.get('source', {}).get('source_tables', {}).get('explicit_included_tables', [])
            task_data.update({
                'lob_max_size': 'UnlimitedLob' if common.get('lob_max_size', 8) == 0 else common.get('lob_max_size', 8),
                'table_count': len(included_tables),
                'target_table_schema': target_set.get('default_schema', 'NULL'),
                'replication_hist_timeslot': common.get('history_timeslot', 5),
                'attrep_exceptions_table': 'Disabled' if common.get('exception_table_enabled') else 'Enabled',
                'attrep_status_table': 'Enabled' if common.get('status_table_enabled') else 'Disabled',
                'attrep_suspended_table': 'Enabled' if common.get('suspended_tables_table_enabled') else 'Disabled',
                'attrep_history_table': 'Enabled' if common.get('history_table_enabled') else 'Disabled',
                'full_load': 'Disabled' if common.get('full_load_enabled') else 'Enabled',
                'full_load_drop_target_tables': 'Disable' if target_set.get('artifacts_cleanup_enabled') else 'Enable',
                'full_load_do_nothing': 'Enable' if target_set.get('drop_table_if_exists') and not target_set.get(
                    'truncate_table_if_exists') else 'Disable',
                'full_load_truncate_target_tables': 'Enable' if target_set.get('truncate_table_if_exists') else 'Disable',
                'create_pk_after_data_load': 'Enable' if target_set.get('create_pk_after_data_load') else 'Disable',
                'stop_task_after_full_load': 'Enable' if common.get('stop_task_after_full_load') else 'Disable',
                'stop_task_after_cached_events': 'Enable' if common.get('stop_task_after_cached_events') else 'Disable',
                'max_full_load_tables': task_settings.get('full_load_sub_tasks', 5),
                'transaction_consistency_timeout': sorter.get('transaction_consistency_timeout', 600),
                'full_load_commit_rate': target_set.get('max_transaction_size', 10000),
                'apply_changes': 'disable' if common.get('apply_changes_enabled') else 'enable',
                'cdc_when_source_table_dropped': target_set.get('handle_drop_ddl', 'True'),
                'cdc_when_source_truncate': target_set.get('handle_truncate_ddl', 'True'),
                'cdc_when_source_ddl': target_set.get('handle_column_ddl', 'True'),
                'store_changes': 'Enable' if common.get('save_changes_enabled') else 'Disable',
                'statements_cache_size': target_set.get('statements_cache_size', 50),
                'cdc_apply_method': 'transaction_apply' if common.get('batch_apply_enabled') else 'batch_apply',
                'min_transaction_size_tran_apply': target_set.get('min_transaction_size', 1000) if common.get('batch_apply_enabled') else 'NA',
                'commit_timeout_tran_apply': target_set.get('commit_timeout', 1000) if common.get('batch_apply_enabled') else 'NA',
                'cdc_batch_min': common.get('batch_apply_timeout_min', 1) if not common.get('batch_apply_enabled') else 'NA',
                'cdc_batch_max': common.get('batch_apply_timeout', 30) if not common.get('batch_apply_enabled') else 'NA',
                'cdc_batch_memory_limit': common.get('batch_apply_memory_limit', 500) if not common.get('batch_apply_enabled') else 'NA',
                'cdc_bulk_parallel_apply': 'enable' if common.get('batch_apply_use_parallel_bulk') else 'disable',
                'cdc_bulk_parallel_apply_threads': common.get('parallel_bulk_max_num_threads', 'NULL'),
                'cdc_transaction_memory': sorter.get('local_transactions_storage', {}).get('memory_limit_total', 1024),
                'cdc_transaction_keep_time': sorter.get('local_transactions_storage', {}).get('memory_keep_time', 60),
                'cdc_statement_cache': target_set.get('statements_cache_size', 50),
                'cdc_store_recovery_in_target': 'enable' if common.get('recovery_table_enabled') else 'disable',
                'pk_changes_handle_delete_insert': 'enable' if common.get('write_pk_changes_as_delete_insert') else 'disable',
                'use_merge_for_batch': 'enable' if common.get('batch_optimize_by_merge') else 'disable',
                'error_policy_apply_conflicts': 'task_policy' if 'error_behavior' in task else 'global_policy',
                'stream_buffers_number': common.get('stream_buffers_number', 'default'),
                'stream_buffer_size': common.get('stream_buffer_size', 'default'),
                'target_ep_name': target.get('target_name')
            })

            if task_data['error_policy_apply_conflicts'] == 'task_policy':
                task_data['delete_policy'] = error_behavior.get('delete_policy', 'ignore')
                task_data['insert_policy'] = error_behavior.get('insert_policy', 'ignore')
                task_data['update_policy'] = error_behavior.get('update_policy', 'ignore')
                task_data['escalation_policy'] = error_behavior.get('escalation_policy', 'ignore')
            else:
                task_data.update({k: 'global_policy' for k in ['delete_policy', 'insert_policy', 'update_policy', 'escalation_policy']})

            if task_data['store_changes'] == 'Enable':
                change_table = common.get('change_table_settings', {})
                headers = change_table.get('header_columns_settings', {})
                task_data.update({
                    'store_changes_suffix': change_table.get('table_suffix', '__ct'),
                    'store_changes_column_prefix': change_table.get('column_prefix', 'header__'),
                    'store_changes_handle_DDL': 'Ignore' if change_table.get('handle_ddl') else 'Apply_to_change_table',
                    'store_changes_on_update': 'Store_after_image_only' if change_table.get('skip_before_image') else 'store_before_and_after_image',
                    'change_table_creation': change_table.get('start_table_behaviour', 'drop_and_create_change_table'),
                    'header_columns_change_seq': 'Disable' if headers.get('change_seq') else 'Enable',
                    'header_columns_change_oper': 'Disable' if headers.get('change_oper') else 'Enable',
                    'header_columns_change_mask': 'Disable' if headers.get('change_mask') else 'Enable',
                    'header_columns_change_stream': 'Disable' if headers.get('stream_position') else 'Enable',
                    'header_columns_change_operation': 'Disable' if headers.get('operation') else 'Enable',
                    'header_columns_change_tran_id': 'Disable' if headers.get('transaction_id') else 'Enable',
                    'header_columns_change_timestamp': 'Disable' if headers.get('timestamp') else 'Enable'
                })
            else:
                for field in ['store_changes_suffix', 'store_changes_column_prefix', 'store_changes_handle_DDL',
                              'store_changes_on_update', 'change_table_creation',
                              'header_columns_change_seq', 'header_columns_change_oper', 'header_columns_change_mask',
                              'header_columns_change_stream', 'header_columns_change_operation',
                              'header_columns_change_tran_id', 'header_columns_change_timestamp']:
                    task_data[field] = 'NULL'

        elif task_type == 'logstream':
            included_tables = task.get('source', {}).get('source_tables', {}).get('explicit_included_tables', [])
            task_data.update({
                'lob_max_size': common.get('lob_max_size', 8),
                'table_count': len(included_tables),
                'stream_buffers_number': common.get('stream_buffers_number', 'default'),
                'stream_buffer_size': common.get('stream_buffer_size', 'default'),
                'full_load': 'LogStream_Task',
                'target_ep_name': target.get('target_name'),
                'target_db_type': 'LOG_STREAM_COMPONENT_TYPE',
                'cdc_apply_method': 'transaction_apply' if not common.get('batch_apply_enabled') else 'batch_apply',
                'min_transaction_size_tran_apply': sorter.get('memory_limit_total', 1000),
                'commit_timeout_tran_apply': sorter.get('memory_keep_time', 60),
            })

        data.append(task_data)
        return data, column_names

    return [], []


def extract_data_to_dataframe(json_file_path, target_task_name):
    try:
        with open(json_file_path, 'r') as f:
            json_data = json.load(f)
        data, column_names = extract_task_settings(json_data, target_task_name)
        return pd.DataFrame(data, columns=column_names) if data else pd.DataFrame()
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"Error reading JSON file: {e}")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return pd.DataFrame()


def write_dataframe_to_csv(df, csv_file_path):
    try:
        df.to_csv(csv_file_path, index=False)
        logging.info(f"Successfully wrote data to {csv_file_path}")
    except Exception as e:
        logging.error(f"Error writing to CSV: {e}")


def main():
    json_file_path = os.getenv("JSON_FILE_PATH", r"C:\Users\VIT\Downloads\Replication_Definition_ketan.json")
    csv_file_path = os.getenv("CSV_FILE_PATH", r"C:\Users\VIT\Downloads\task_settings.csv")
    target_task_name = os.getenv("TARGET_TASK_NAME", "ERXDW_TXAVRP01_IMG_OLTP_SnowflakeProd")

    df = extract_data_to_dataframe(json_file_path, target_task_name)
    if not df.empty:
        write_dataframe_to_csv(df, csv_file_path)
        print(df)
    else:
        logging.warning(f"No data extracted for task: {target_task_name}")


if __name__ == "__main__":
    main()
