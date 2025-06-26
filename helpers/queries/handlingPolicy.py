# Tasks DDL handling
ddl_handling = """
SELECT 
    --'DDL Handling' AS Description,
    CAST(target_db_type AS VARCHAR) AS target_db_type,
    CAST(cdc_when_source_table_dropped AS VARCHAR) AS cdc_when_source_table_dropped,
    CAST(cdc_when_source_truncate AS VARCHAR) AS cdc_when_source_truncate,
    CAST(cdc_when_source_ddl AS VARCHAR) AS cdc_when_source_ddl_altered,
    CAST(store_changes_handle_DDL AS VARCHAR) AS store_changes_handle_DDL,
    COUNT(DISTINCT task_name) AS DistinctTaskCount,
    COUNT(CAST(table_count AS INTEGER)) AS TotalTables
FROM data_df
WHERE LOWER(task_type) = 'replication' 
  AND LOWER(qem_State) = 'running'
GROUP BY 
    CAST(target_db_type AS VARCHAR),
    CAST(cdc_when_source_table_dropped AS VARCHAR),
    CAST(cdc_when_source_truncate AS VARCHAR),
    CAST(cdc_when_source_ddl AS VARCHAR),
    CAST(store_changes_handle_DDL AS VARCHAR) 
"""

# Task policy global or task
handling_policy = """
SELECT 
    --'Server/Task Policy' AS Description,
    CAST(target_db_type AS VARCHAR) AS target_db_type,
    CAST(error_policy_apply_conflicts AS VARCHAR) AS error_policy_apply_conflicts,
    COUNT(DISTINCT task_name) AS DistinctTaskCount,
    COUNT(CAST(table_count AS INTEGER)) AS TotalTables
FROM data_df
WHERE LOWER(task_type) = 'replication' 
  AND LOWER(qem_State) = 'running'
GROUP BY 
    CAST(target_db_type AS VARCHAR),
    CAST(error_policy_apply_conflicts AS VARCHAR)
"""

error_handling = """
SELECT 
    --'Error Handling' AS Description,
    CAST(target_db_type AS VARCHAR) AS target_db_type,
    CAST(delete_policy AS VARCHAR) AS delete_policy,
    CAST(insert_policy AS VARCHAR) AS insert_policy,
    CAST(update_policy AS VARCHAR) AS update_policy,
    COUNT(DISTINCT task_name) AS DistinctTaskCount,
    COUNT(CAST(table_count AS INTEGER)) AS TotalTables
FROM data_df
WHERE LOWER(task_type) = 'replication' 
  AND LOWER(qem_State) = 'running'
GROUP BY 
    CAST(target_db_type AS VARCHAR),
    CAST(delete_policy AS VARCHAR),
    CAST(insert_policy AS VARCHAR),
    CAST(update_policy AS VARCHAR),
    CAST(error_policy_apply_conflicts AS VARCHAR)
"""