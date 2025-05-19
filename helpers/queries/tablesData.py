duplicate_replication_multiple_targets =  """
WITH DuplicateEntries AS (
    SELECT 
        "table_name" AS table_name,
        "schema_name" AS schema_name,
        "source_server" AS source_server,
        COUNT(*) AS DuplicateCount
    FROM data_df
    WHERE 
        LOWER("task_type") NOT LIKE '%logstream%' 
        AND "table_name" IS NOT NULL
        AND (
            LOWER(COALESCE("apply_changes", '')) LIKE '%enable%' OR 
            LOWER(COALESCE("store_changes", '')) LIKE '%enable%'
        )
        AND LOWER("qem_State") = 'running'
    GROUP BY 
        table_name,
        schema_name,
        source_server
    HAVING 
        COUNT(*) > 1
)

SELECT 
    md."task_name",
    md."table_name",
    md."schema_name",
    md."source_server",
    md."target_db_type",
    md."target_server",
    md."apply_changes",
    md."store_changes",
    md."qem_State",
    de.DuplicateCount,
    COUNT(*) OVER (
        PARTITION BY 
            md."table_name", 
            md."schema_name", 
            md."source_server", 
            md."target_db_type", 
            md."target_server"
    ) AS TotalOccurrences
FROM data_df md
JOIN DuplicateEntries de ON 
    md."table_name" = de.table_name AND 
    md."schema_name" = de.schema_name AND 
    md."source_server" = de.source_server
WHERE 
    LOWER(md."task_type") NOT LIKE '%logstream%' 
    AND md."table_name" IS NOT NULL
    AND (
        LOWER(COALESCE(md."apply_changes", '')) LIKE '%enable%' OR 
        LOWER(COALESCE(md."store_changes", '')) LIKE '%enable%'
    )
    AND LOWER(md."qem_State") = 'running'
ORDER BY md."table_name";
"""

duplicate_replication_same_targets =  """
WITH DuplicateEntries AS (
    SELECT 
        "table_name" AS table_name,
        "schema_name" AS schema_name,
        "source_server" AS source_server,
        "target_db_type" AS target_db_type,
        "target_server" AS target_server,
        COUNT(*) AS DuplicateCount
    FROM data_df
    WHERE 
        LOWER("task_type") NOT LIKE '%logstream%' 
        AND "table_name" IS NOT NULL
        AND (
            LOWER(COALESCE("apply_changes", '')) LIKE '%enable%' OR 
            LOWER(COALESCE("store_changes", '')) LIKE '%enable%'
        )
        AND LOWER("qem_State") = 'running'
    GROUP BY 
        table_name,
        schema_name,
        source_server,
        target_db_type,
        target_server
    HAVING 
        COUNT(*) > 1
)

SELECT 
    md."task_name",
    md."table_name",
    md."schema_name",
    md."source_server",
    md."target_db_type",
    md."target_server",
    md."apply_changes",
    md."store_changes",
    md."qem_State",
    de.DuplicateCount,
    COUNT(*) OVER (
        PARTITION BY 
            md."table_name", 
            md."schema_name", 
            md."source_server", 
            md."target_db_type", 
            md."target_server"
    ) AS TotalOccurrences
FROM data_df md
JOIN DuplicateEntries de ON 
    md."table_name" = de.table_name AND 
    md."schema_name" = de.schema_name AND 
    md."source_server" = de.source_server AND
COALESCE(md."target_db_type", '') = COALESCE(de."target_db_type", '') AND
COALESCE(md."target_server", '') = COALESCE(de."target_server", '')
WHERE 
    LOWER(md."task_type") NOT LIKE '%logstream%' 
    AND md."table_name" IS NOT NULL
    AND (
        LOWER(COALESCE(md."apply_changes", '')) LIKE '%enable%' OR 
        LOWER(COALESCE(md."store_changes", '')) LIKE '%enable%'
    )
    AND LOWER(md."qem_State") = 'running'
ORDER BY md."table_name";
"""