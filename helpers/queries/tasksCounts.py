# 1. Total Number of Tasks
total_tasks_query = """
SELECT 
    'Total Tasks' AS Description,
    COUNT(DISTINCT task_name) AS DistinctTaskCount,
    COUNT(CAST(table_count AS INTEGER)) AS TotalTables
FROM data_df
"""

# 2. Total Running State Tasks
running_tasks_query = """
SELECT 
    'Running State Tasks' AS Description,
    COUNT(DISTINCT task_name) AS DistinctTaskCount,
    COUNT(CAST(table_count AS INTEGER)) AS TotalTables
FROM data_df
WHERE LOWER(qem_State) = 'running'
"""

# 3. Total LogStream Tasks
logstream_tasks_query = """
SELECT 
    'LogStream Tasks' AS Description,
    COUNT(DISTINCT task_name) AS DistinctTaskCount,
    COUNT(CAST(table_count AS INTEGER)) AS TotalTables
FROM data_df
WHERE LOWER(task_type) = 'logstream' AND
LOWER(qem_State) = 'running'
"""

# 3.1 Total LogStream Tasks
replication_tasks_query = """
SELECT 
    'Replication Tasks' AS Description,
    COUNT(DISTINCT task_name) AS DistinctTaskCount,
    COUNT(CAST(table_count AS INTEGER)) AS TotalTables
FROM data_df
WHERE LOWER(task_type) = 'replication' AND
LOWER(qem_State) = 'running'
"""

# 4. Total Apply Changes Tasks
apply_changes_query = """
SELECT 
    'Apply Changes(Only) Tasks' AS Description,
    COUNT(DISTINCT task_name) AS DistinctTaskCount,
    COUNT(CAST(table_count AS INTEGER)) AS TotalTables
FROM data_df
WHERE (LOWER(apply_changes) = 'enable' AND LOWER(store_changes) LIKE 'disable') AND
LOWER(qem_State) = 'running'
"""

# 5. Total Store Changes Tasks
store_changes_query = """
SELECT 
    'Store Changes(Only) Tasks' AS Description,
    COUNT(DISTINCT task_name) AS DistinctTaskCount,
    COUNT(CAST(table_count AS INTEGER)) AS TotalTables
FROM data_df
WHERE (LOWER(store_changes) = 'enable' AND LOWER(apply_changes) = 'disable')AND
LOWER(qem_State) = 'running'
"""

# 6. Total Apply and Store Changes Tasks
apply_store_changes_query = """
SELECT 
    'Apply and Store Changes Tasks' AS Description,
    COUNT(DISTINCT task_name) AS DistinctTaskCount,
    COUNT(CAST(table_count AS INTEGER)) AS TotalTables
FROM data_df
WHERE LOWER(apply_changes) LIKE '%enable%' AND LOWER(store_changes) LIKE '%enable%' AND
LOWER(qem_State) = 'running'
"""

# 7. Total Tasks without LogStream
no_logstream_query = """
SELECT 
    'Tasks without LogStream' AS Description,
    COUNT(DISTINCT task_name) AS DistinctTaskCount,
    COUNT(CAST(table_count AS INTEGER)) AS TotalTables
FROM data_df
WHERE LOWER(task_type) = 'replication' AND
(source_logstreamstagingtask IS NULL OR LOWER(source_logstreamstagingtask) = 'none') AND
LOWER(qem_State) = 'running' AND
(LOWER(apply_changes) LIKE '%enable%' OR LOWER(store_changes) LIKE '%enable%') 

"""


