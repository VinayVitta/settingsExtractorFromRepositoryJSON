
batch_tuning = """
SELECT 
   -- 'Batch Tuning' AS Description,
    CAST(cdc_batch_min AS INTEGER) AS MinBatchTime,
    CAST(cdc_batch_max AS INTEGER) AS MaxBatchTime,
    CAST(cdc_batch_memory_limit AS INTEGER) AS BatchMemory,
    COUNT(DISTINCT task_name) AS DistinctTaskCount,
    COUNT(CAST(table_count AS INTEGER)) AS TotalTables
FROM data_df
WHERE LOWER(task_type) = 'replication' 
  AND LOWER(qem_State) = 'running'
GROUP BY 
    CAST(cdc_batch_min AS INTEGER),
    CAST(cdc_batch_max AS INTEGER),
    CAST(cdc_batch_memory_limit AS INTEGER)
"""

transaction_memory = """
SELECT 
    --'Transaction mem offloading' AS Description,
    CAST(cdc_transaction_memory AS INTEGER) AS CDCTransactionMemory,
    CAST(cdc_transaction_keep_time AS INTEGER) AS CDCTransactionKeepTime,
    COUNT(DISTINCT task_name) AS DistinctTaskCount,
    COUNT(CAST(table_count AS INTEGER)) AS TotalTables
FROM data_df
WHERE LOWER(task_type) = 'replication' 
  AND LOWER(qem_State) = 'running'
GROUP BY 
    CAST(cdc_transaction_memory AS INTEGER),
    CAST(cdc_transaction_keep_time AS INTEGER)
"""

control_tables = """
SELECT 
    --'Control Tables' AS Description,
    CAST(attrep_history_table AS VARCHAR) AS attrep_history_table,
    CAST(attrep_status_table AS VARCHAR) AS attrep_status_table,
    CAST(attrep_suspended_table AS VARCHAR) AS attrep_suspended_table,
    CAST(target_db_type AS VARCHAR) AS target_db_type,
    COUNT(DISTINCT task_name) AS DistinctTaskCount,
    COUNT(CAST(table_count AS INTEGER)) AS TotalTables
FROM data_df
WHERE LOWER(task_type) = 'replication' 
  AND LOWER(qem_State) = 'running'
GROUP BY 
    CAST(attrep_history_table AS VARCHAR),
    CAST(attrep_status_table AS VARCHAR),
    CAST(attrep_suspended_table AS VARCHAR),
    CAST(target_db_type AS VARCHAR)
"""

lob_size = """
SELECT 
    --'LOB' AS Description,
    CAST(lob_max_size AS INTEGER) AS lob_max_size,
    CAST(target_db_type AS VARCHAR) AS target_db_type,
    COUNT(DISTINCT task_name) AS DistinctTaskCount,
    COUNT(CAST(table_count AS INTEGER)) AS TotalTables
FROM data_df
WHERE LOWER(task_type) = 'replication' 
  AND LOWER(qem_State) = 'running'
GROUP BY 
    CAST(lob_max_size AS INTEGER),
    CAST(target_db_type AS VARCHAR)
"""