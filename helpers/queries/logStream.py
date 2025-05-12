# LogStream and child tasks count

totalChildTasksForEachParent =  """
WITH unique_logstreams AS (
    SELECT DISTINCT TRIM(task_name) AS logstream_task_name
    FROM data_df
    WHERE LOWER(task_type) = 'logstream'
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