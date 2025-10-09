# LogStream and child tasks count

totalChildTasksForEachParent =  """
WITH unique_logstreams AS (
    SELECT DISTINCT TRIM(task_name) AS logstream_task_name
    FROM data_df
    WHERE LOWER(task_type) = 'logstream'
    AND LOWER(qem_State) = 'running'
),
unique_running_replications AS (
    SELECT DISTINCT TRIM(task_name) AS replication_task_name,
           TRIM(source_logstreamstagingtask) AS parent_logstream
    FROM data_df
    WHERE LOWER(task_type) = 'replication'
      AND LOWER(qem_State) = 'running'
)

SELECT 
    ul.logstream_task_name AS LogStreamTask,
    COUNT(ur.replication_task_name) AS RunningReplicationTaskCount
FROM unique_logstreams AS ul
LEFT JOIN unique_running_replications AS ur
    ON LOWER(ur.parent_logstream) = LOWER(ul.logstream_task_name)
GROUP BY ul.logstream_task_name
ORDER BY RunningReplicationTaskCount DESC;
"""

multipleLogStreamSameSourceDB = """
WITH logstream_tasks AS (
    SELECT DISTINCT
        TRIM(task_name) AS task_name,
        TRIM(source_server) AS source_server,
        TRIM(replicate_server) AS replicate_server
    FROM data_df
    WHERE LOWER(task_type) = 'logstream'
    AND LOWER(qem_State) = 'running'
),
source_server_counts AS (
    SELECT 
        source_server
    FROM logstream_tasks
    GROUP BY source_server
    HAVING COUNT(*) > 1
)

SELECT 
    lt.task_name AS TaskName,
    lt.replicate_server AS ReplicateServer,
    lt.source_server AS SourceServer
FROM logstream_tasks AS lt
JOIN source_server_counts AS ssc
    ON lt.source_server = ssc.source_server
ORDER BY lt.source_server, lt.task_name;

"""

losgtreamwithNoChild =  """
WITH unique_logstreams AS (
    SELECT DISTINCT TRIM(task_name) AS logstream_task_name
    FROM data_df
    WHERE LOWER(task_type) = 'logstream'
      AND LOWER(qem_State) = 'running'
),
unique_running_replications AS (
    SELECT DISTINCT TRIM(source_logstreamstagingtask) AS parent_logstream
    FROM data_df
    WHERE LOWER(task_type) = 'replication'
      AND LOWER(qem_State) = 'running'
)

-- Select LogStreams that are NOT used as a parent
SELECT logstream_task_name
FROM unique_logstreams
WHERE logstream_task_name NOT IN (
    SELECT parent_logstream
    FROM unique_running_replications
    WHERE parent_logstream IS NOT NULL
);
"""