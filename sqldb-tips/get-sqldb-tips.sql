DECLARE @TipDefinition table (
                             tip_id smallint not null primary key,
                             tip_name nvarchar(50) not null unique,
                             confidence_percent decimal(5,2) not null,
                             tip_url nvarchar(200) not null
                             );

DECLARE @DetectedTip table (
                           tip_id smallint not null primary key,
                           details nvarchar(max) null
                           );

DECLARE @ReturnAllTips bit = 1; -- Debug flag to return all tips regardless of database state

-- Configurable thresholds
DECLARE @HighLogRateThresholdPercent decimal(5,2) = 80, -- Minimum log rate as percentage of SLO limit that is considered as being too high in the "Log rate close to limit" tip
        @GuidLeadingColumnObjectMinSizeMB int = 1024, -- Minimum table size to be considered in the "GUID leading columns in btree indexes" tip
        @UsedToMaxsizeSpaceThresholdRatio decimal(3,2) = 0.8, -- The ratio of used space to database MAXSIZE that is considered as being too high in the "Used space close to MAXSIZE" tip
        @AllocatedToMaxsizeSpaceThresholdRatio decimal(3,2) = 0.8, -- The ratio of allocated space to database MAXSIZE that is considered as being too high in the "Allocated space close to MAXSIZE" tip
        @UsedToAllocatedSpaceThresholdRatio decimal(3,2) = 0.3, -- The ratio of used space to allocated space that is considered as being too low in the "Allocated space much larger than used space" tip
        @UsedToAllocatedSpaceDbMinSizeMB int = 10240, -- Minimum database size to be considered for the "Allocated space much larger than used space" tip
        @CPUThrottlingDelayThresholdPercent decimal(5,2) = 20, -- Minimum percentage of CPU RG delay to be considered as significant CPU throttling in "Recent CPU throttling" tip
        @IndexReadWriteThresholdRatio decimal(3,2) = 0.5, -- The ratio of all index reads to index writes that is considered as being too low in the "Low reads nonclustered indexes" tip
        @CompressionPartitionUpdateRatioThreshold1 decimal(3,2) = 0.2, -- The maximum ratio of updates to all operations to define "infrequent updates" in the "Data compression opportunities" tip
        @CompressionPartitionUpdateRatioThreshold2 decimal(3,2) = 0.5, -- The maximum ratio of updates to all operations to define "more frequent but not frequent enough updates" in the "Data compression opportunities" tip
        @CompressionPartitionScanRatioThreshold1 decimal(3,2) = 0.5, -- The minimum ratio of scans to all operations to define "frequent enough scans" in the "Data compression opportunities" tip
        @CompressionCPUHeadroomThreshold1 decimal(5,2) = 60, -- Maximum CPU usage percentage to be considered as sufficient CPU headroom in the "Data compression opportunities" tip
        @CompressionCPUHeadroomThreshold2 decimal(5,2) = 80, -- Minimum CPU usage percentage to be considered as insufficient CPU headroom in the "Data compression opportunities" tip
        @CompressionMinResourceStatSamples smallint = 30, -- Minimum required number of resource stats sampling intervals in the "Data compression opportunities" tip
        @SingleUsePlanSizeThresholdMB int = 512, -- Minimum required per-db size of single-use plans to be considered as significant in the "Plan cache bloat from single-use plans" tip
        @SingleUseTotalPlanSizeRatioThreshold decimal(3,2) = 0.3 -- The minimum ratio of single-use plans size to total plan size per database to be considered as significant in the "Plan cache bloat from single-use plans" tip
;

SET NOCOUNT ON;

-- Avoid blocking user DDL due to shared locks reading metadata
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

BEGIN TRY

IF IS_ROLEMEMBER('public') <> 1
   OR
   NOT EXISTS (
              SELECT 1 
              FROM sys.fn_my_permissions(NULL, 'DATABASE')
              WHERE permission_name = 'VIEW DATABASE STATE'
              )
    THROW 50001, 'Insufficient permissions.', 1;

-- Bail out if CPU utilization in the last 1 minute is very high
IF EXISTS (
          SELECT TOP (4) *
          FROM sys.dm_db_resource_stats
          WHERE avg_cpu_percent > 95
                OR
                avg_instance_cpu_percent > 95
          ORDER BY end_time DESC
          )
    THROW 50002, 'CPU utilization is too high. Execute script at a later time.', 1;

-- Define all tips
INSERT INTO @TipDefinition (tip_id, tip_name, confidence_percent, tip_url)
VALUES
(1000, 'Excessive MAXDOP on all replicas', 90, 'https://github.com/microsoft/azure-sql-tools/wiki/Azure-SQL-Database-tips#excessive-maxdop-on-all-replicas'),
(1010, 'Excessive MAXDOP on primary', 90, 'https://github.com/microsoft/azure-sql-tools/wiki/Azure-SQL-Database-tips#excessive-maxdop-on-primary'),
(1020, 'Excessive MAXDOP on secondaries', 90, 'https://github.com/microsoft/azure-sql-tools/wiki/Azure-SQL-Database-tips#excessive-maxdop-on-secondaries'),
(1030, 'Compatibility level is not current', 70, 'https://github.com/microsoft/azure-sql-tools/wiki/Azure-SQL-Database-tips#compatibility-level-is-not-current'),
(1040, 'Auto-create stats is disabled', 95, 'https://github.com/microsoft/azure-sql-tools/wiki/Azure-SQL-Database-tips#auto-create-stats-is-disabled'),
(1050, 'Auto-update stats is disabled', 95, 'https://github.com/microsoft/azure-sql-tools/wiki/Azure-SQL-Database-tips#auto-update-stats-is-disabled'),
(1060, 'RCSI is disabled', 80, 'https://github.com/microsoft/azure-sql-tools/wiki/Azure-SQL-Database-tips#rcsi-is-disabled'),
(1070, 'Query Store is disabled', 90, 'https://github.com/microsoft/azure-sql-tools/wiki/Azure-SQL-Database-tips#query-store-is-disabled'),
(1071, 'Query Store is read-only', 90, 'https://github.com/microsoft/azure-sql-tools/wiki/Azure-SQL-Database-tips#query-store-is-read-only'),
(1072, 'Query Store capture mode is NONE', 90, 'https://github.com/microsoft/azure-sql-tools/wiki/Azure-SQL-Database-tips#query-store-capture-mode-is-none'),
(1080, 'AUTO_SHRINK is enabled', 99, 'https://github.com/microsoft/azure-sql-tools/wiki/Azure-SQL-Database-tips#auto_shrink-is-enabled'),
(1100, 'Btree indexes have GUID leading columns', 60, 'https://github.com/microsoft/azure-sql-tools/wiki/Azure-SQL-Database-tips#btree-indexes-have-guid-leading-columns'),
(1110, 'FLGP auto-tuning is disabled', 95, 'https://github.com/microsoft/azure-sql-tools/wiki/Azure-SQL-Database-tips#flgp-auto-tuning-is-disabled'),
(1120, 'Used space is close to MAXSIZE', 80, 'https://github.com/microsoft/azure-sql-tools/wiki/Azure-SQL-Database-tips#used-space-is-close-to-maxsize'),
(1130, 'Allocated space is close to MAXSIZE', 60, 'https://github.com/microsoft/azure-sql-tools/wiki/Azure-SQL-Database-tips#allocated-space-is-close-to-maxsize'),
(1140, 'Allocated space is much larger than used space', 50, 'https://github.com/microsoft/azure-sql-tools/wiki/Azure-SQL-Database-tips#allocated-space-is-much-larger-than-used-space'),
(1150, 'Recent CPU throttling found', 90, 'https://github.com/microsoft/azure-sql-tools/wiki/Azure-SQL-Database-tips#recent-cpu-throttling-found'),
(1160, 'Recent out of memory errors found', 80, 'https://github.com/microsoft/azure-sql-tools/wiki/Azure-SQL-Database-tips#recent-out-of-memory-errors-found'),
(1170, 'Nonclustered indexes with low reads found', 60, 'https://github.com/microsoft/azure-sql-tools/wiki/Azure-SQL-Database-tips#nonclustered-indexes-with-low-reads-found'),
(1180, 'Data compression opportunities', 60, 'https://github.com/microsoft/azure-sql-tools/wiki/Azure-SQL-Database-tips#data-compression-opportunities'),
(1190, 'Log rate is close to limit', 70, 'https://github.com/microsoft/azure-sql-tools/wiki/Azure-SQL-Database-tips#log-rate-is-close-to-limit'),
(1200, 'Plan cache is bloated by single-use plans', 90, 'https://github.com/microsoft/azure-sql-tools/wiki/Azure-SQL-Database-tips#plan-cache-is-bloated-by-single-use-plans')
;

-- MAXDOP
INSERT INTO @DetectedTip (tip_id)
SELECT 1000 AS tip_id
FROM sys.database_scoped_configurations
WHERE name = N'MAXDOP'
      AND
      value NOT BETWEEN 1 AND 8
      AND
      (value_for_secondary IS NULL OR value_for_secondary NOT BETWEEN 1 AND 8)
      AND
      (SELECT COUNT(1) FROM sys.dm_os_schedulers WHERE status = 'VISIBLE ONLINE') > 8
UNION
SELECT 1010 AS tip_id
FROM sys.database_scoped_configurations
WHERE name = N'MAXDOP'
      AND
      value NOT BETWEEN 1 AND 8
      AND
      value_for_secondary BETWEEN 1 AND 8
      AND
      (SELECT COUNT(1) FROM sys.dm_os_schedulers WHERE status = 'VISIBLE ONLINE') > 8
UNION
SELECT 1020 AS tip_id
FROM sys.database_scoped_configurations
WHERE name = N'MAXDOP'
      AND
      value BETWEEN 1 AND 8
      AND
      value_for_secondary NOT BETWEEN 1 AND 8
      AND
      (SELECT COUNT(1) FROM sys.dm_os_schedulers WHERE status = 'VISIBLE ONLINE') > 8
;

-- Compatibility level
INSERT INTO @DetectedTip (tip_id)
SELECT 1030 AS tip_id
FROM sys.dm_exec_valid_use_hints AS h
CROSS JOIN sys.databases AS d
WHERE h.name LIKE 'QUERY[_]OPTIMIZER[_]COMPATIBILITY[_]LEVEL[_]%'
      AND
      d.name = DB_NAME()
      AND
      TRY_CAST(RIGHT(h.name, CHARINDEX('_', REVERSE(h.name)) - 1) AS smallint) > d.compatibility_level
HAVING COUNT(1) > 1 -- Consider the last two compat levels (including the one possibly in preview) as current
;

-- Auto-stats
INSERT INTO @DetectedTip (tip_id)
SELECT 1040 AS tip_id
FROM sys.databases
WHERE name = DB_NAME()
      AND
      is_auto_create_stats_on = 0
UNION
SELECT 1050 AS tip_id
FROM sys.databases
WHERE name = DB_NAME()
      AND
      is_auto_update_stats_on = 0
;

-- RCSI
INSERT INTO @DetectedTip (tip_id)
SELECT 1060 AS tip_id
FROM sys.databases
WHERE name = DB_NAME()
      AND
      is_read_committed_snapshot_on = 0
;

-- Query Store
INSERT INTO @DetectedTip (tip_id, details)
SELECT 1070 AS tip_id,
       NULL AS details
FROM sys.database_query_store_options
WHERE actual_state_desc = 'OFF'
UNION
SELECT 1071 AS tip_id,
       CASE readonly_reason
           WHEN 1 THEN 'Database is in read-only mode.'
           WHEN 2 THEN 'Database is in single-user mode.'
           WHEN 4 THEN 'Database in in emergency mode.'
           WHEN 8 THEN 'Database is a read-only replica.'
           WHEN 65536 THEN 'The size of Query Store has reached the limit set by MAX_STORAGE_SIZE_MB option.'
           WHEN 131072 THEN 'The number of queries in Query Store has reached the limit for the service objective. Remove unneeded queries or scale up to a higher service objective.'
           WHEN 262144 THEN 'The size of in-memory Query Store data has reached maximum limit. Query Store will be in read-only state while this data is being persisted in the database.'
           WHEN 524288 THEN 'Database has reached its maximum size limit.'
       END
       AS details
FROM sys.database_query_store_options
WHERE actual_state_desc = 'READ_ONLY'
UNION
SELECT 1072 AS tip_id,
       NULL AS details
FROM sys.database_query_store_options
WHERE query_capture_mode_desc = 'NONE'
;

-- Auto-shrink
INSERT INTO @DetectedTip (tip_id)
SELECT 1080 AS tip_id
FROM sys.databases
WHERE name = DB_NAME()
      AND
      is_auto_shrink_on = 1
;

-- Btree indexes with uniqueidentifier leading columns
WITH object_size AS
(
SELECT p.object_id,
       SUM(au.used_pages) * 8 / 1024. AS object_size_mb
FROM sys.partitions AS p
INNER JOIN sys.allocation_units AS au
ON (
   (p.hobt_id = au.container_id AND au.type_desc IN ('IN_ROW_DATA','ROW_OVERFLOW_DATA'))
   OR
   (p.partition_id = au.container_id AND au.type_desc = 'LOB_DATA')
   )
WHERE p.index_id IN (0,1) -- clustered index or heap
GROUP BY p.object_id
)
INSERT INTO @DetectedTip (tip_id, details)
SELECT 1100 AS tip_id,
       STRING_AGG(CAST(CONCAT(
                             'schema: ', QUOTENAME(OBJECT_SCHEMA_NAME(o.object_id)) COLLATE DATABASE_DEFAULT, 
                             ', object: ', QUOTENAME(o.name) COLLATE DATABASE_DEFAULT, 
                             ', index: ', QUOTENAME(i.name) COLLATE DATABASE_DEFAULT, 
                             ', type: ', i.type_desc COLLATE DATABASE_DEFAULT
                             ) AS nvarchar(max)), CONCAT(CHAR(13), CHAR(10))) AS details
FROM sys.objects AS o
INNER JOIN sys.indexes AS i
ON o.object_id = i.object_id
INNER JOIN sys.index_columns AS ic
ON i.object_id = ic.object_id
   AND
   i.index_id = ic.index_id
INNER JOIN sys.columns AS c
ON i.object_id = c.object_id
   AND
   ic.object_id = c.object_id
   AND
   ic.column_id = c.column_id
INNER JOIN sys.types AS t
ON c.system_type_id = t.system_type_id
INNER JOIN object_size AS os
ON o.object_id = os.object_id
WHERE i.type_desc IN ('CLUSTERED','NONCLUSTERED') -- Btree indexes
      AND
      ic.key_ordinal = 1 -- leading column
      AND
      t.name = 'uniqueidentifier'
      AND
      i.is_hypothetical = 0
      AND
      i.is_disabled = 0
      AND
      o.is_ms_shipped = 0
      AND
      os.object_size_mb > @GuidLeadingColumnObjectMinSizeMB -- consider larger tables only
      AND
      -- data type is uniqueidentifier or an alias data type derived from uniqueidentifier
      EXISTS (
             SELECT 1
             FROM sys.types AS t1
             LEFT JOIN sys.types AS t2
             ON t1.system_type_id = t2.system_type_id
             WHERE t1.name = 'uniqueidentifier'
                   AND
                   c.user_type_id = t2.user_type_id
             )
HAVING COUNT(1) > 0
;

-- APRC
INSERT INTO @DetectedTip (tip_id)
SELECT 1110 AS tip_id
FROM sys.database_automatic_tuning_options
WHERE name = 'FORCE_LAST_GOOD_PLAN'
      AND
      actual_state_desc <> 'ON'
;

-- Used space close to maxsize
INSERT INTO @DetectedTip (tip_id)
SELECT 1120 AS tip_id
FROM sys.database_files
WHERE type_desc = 'ROWS'
      AND
      CAST(DATABASEPROPERTYEX(DB_NAME(), 'MaxSizeInBytes') AS bigint) <> -1 -- not applicable to Hyperscale
HAVING SUM(CAST(FILEPROPERTY(name, 'SpaceUsed') AS bigint) * 8 / 1024.)
       >
       @UsedToMaxsizeSpaceThresholdRatio * CAST(DATABASEPROPERTYEX(DB_NAME(), 'MaxSizeInBytes') AS bigint) / 1024. / 1024 -- used space > n% of db maxsize
;

-- Allocated space close to maxsize
INSERT INTO @DetectedTip (tip_id)
SELECT 1130 AS tip_id
FROM sys.database_files
WHERE type_desc = 'ROWS'
      AND
      CAST(DATABASEPROPERTYEX(DB_NAME(), 'MaxSizeInBytes') AS bigint) <> -1 -- not applicable to Hyperscale
      AND
      DATABASEPROPERTYEX(DB_NAME(), 'Edition') IN ('Premium','BusinessCritical')
HAVING SUM(CAST(size AS bigint) * 8 / 1024.)
       >
       @AllocatedToMaxsizeSpaceThresholdRatio * CAST(DATABASEPROPERTYEX(DB_NAME(), 'MaxSizeInBytes') AS bigint) / 1024. / 1024 -- allocated space > n% of db maxsize
;

-- Allocated space >> used space
WITH allocated_used_space AS
(
SELECT SUM(CAST(size AS bigint)) AS allocated_space,
       SUM(CAST(FILEPROPERTY(name, 'SpaceUsed') AS bigint)) AS used_space
FROM sys.database_files
WHERE type_desc = 'ROWS'
)
INSERT INTO @DetectedTip (tip_id)
SELECT 1140 AS tip_id
FROM allocated_used_space
WHERE used_space * 8 / 1024. > @UsedToAllocatedSpaceDbMinSizeMB -- not relevant for small databases
      AND
      @UsedToAllocatedSpaceThresholdRatio * allocated_space > used_space -- allocated space is more than N times used space
      AND
      DATABASEPROPERTYEX(DB_NAME(), 'Edition') IN ('Premium','BusinessCritical')
;

-- Recent CPU throttling
WITH cpu_throttling AS
(
SELECT SUM(duration_ms) / 60000 AS recent_history_duration_minutes,
       SUM(IIF(delta_cpu_active_ms > 0 AND delta_cpu_delayed_ms > 0, 1, 0)) AS count_cpu_delayed_intervals,
       CAST(AVG(IIF(delta_cpu_active_ms > 0 AND delta_cpu_delayed_ms > 0, CAST(delta_cpu_delayed_ms AS decimal(12,0)) / delta_cpu_active_ms, NULL)) * 100 AS decimal(5,2)) AS avg_cpu_delay_percent
FROM sys.dm_resource_governor_workload_groups_history_ex
WHERE name like 'UserPrimaryGroup.DB%'
)
INSERT INTO @DetectedTip (tip_id, details)
SELECT 1150 AS tip_id,
       CONCAT(
             'In the last ', recent_history_duration_minutes, 
             ' minutes, there were ', count_cpu_delayed_intervals, 
             ' occurrence(s) of CPU throttling. On average, CPU was throttled by ', avg_cpu_delay_percent, '%.'
             ) AS details
FROM cpu_throttling
WHERE avg_cpu_delay_percent > @CPUThrottlingDelayThresholdPercent
;

-- Recent out of memory errors
WITH oom AS
(
SELECT SUM(duration_ms) / 60000 AS recent_history_duration_minutes,
       SUM(delta_out_of_memory_count) AS count_oom
FROM sys.dm_resource_governor_resource_pools_history_ex
WHERE name LIKE 'SloSharedPool%'
      OR
      name LIKE 'UserPool%'
)
INSERT INTO @DetectedTip (tip_id, details)
SELECT 1160 AS tip_id,
       CONCAT(
             'In the last ', recent_history_duration_minutes, 
             ' minutes, there were ', count_oom, 
             ' out of memory errors.'
             ) AS details
FROM oom
WHERE count_oom > 0
;

-- Little used nonclustered indexes
WITH index_usage AS
(
SELECT STRING_AGG(CONCAT(
                        QUOTENAME(OBJECT_SCHEMA_NAME(o.object_id)) COLLATE DATABASE_DEFAULT, '.', 
                        QUOTENAME(o.name) COLLATE DATABASE_DEFAULT, '.', 
                        QUOTENAME(i.name) COLLATE DATABASE_DEFAULT, 
                        ' (reads: ', ius.user_seeks + ius.user_scans + ius.user_lookups, ' writes: ', ius.user_updates, ')'
                        ), CONCAT(CHAR(13), CHAR(10))) AS details
FROM sys.dm_db_index_usage_stats AS ius
INNER JOIN sys.indexes AS i
ON ius.object_id = i.object_id
   AND
   ius.index_id = i.index_id
INNER JOIN sys.objects AS o
ON i.object_id = o.object_id
   AND
   ius.object_id = o.object_id
WHERE ius.database_id = DB_ID()
      AND
      i.type_desc = 'NONCLUSTERED'
      AND
      i.is_primary_key = 0
      AND
      i.is_unique_constraint = 0
      AND
      o.is_ms_shipped = 0
      AND
      (ius.user_seeks + ius.user_scans + ius.user_lookups) * @IndexReadWriteThresholdRatio < ius.user_updates
)
INSERT INTO @DetectedTip (tip_id, details)
SELECT 1170 AS tip_id,
       CONCAT('For time period starting from ', CONVERT(varchar(20), sqlserver_start_time, 120), ':', CHAR(13), CHAR(10), iu.details) AS details
FROM index_usage AS iu
CROSS JOIN sys.dm_os_sys_info AS si
WHERE iu.details IS NOT NULL
;

-- Compression candidates
WITH 
recent_cpu_usage AS
(
SELECT AVG(avg_cpu_percent) AS avg_cpu_percent,
       DATEDIFF(minute, MIN(end_time), MAX(end_time)) AS recent_cpu_minutes
FROM sys.dm_db_resource_stats
),
partition_size AS
(
SELECT p.object_id,
       p.index_id,
       p.partition_number,
       p.data_compression_desc,
       SUM(au.used_pages) * 8 / 1024. AS partition_size_mb
FROM sys.partitions AS p
INNER JOIN sys.allocation_units AS au
ON (
   (p.hobt_id = au.container_id AND au.type_desc IN ('IN_ROW_DATA','ROW_OVERFLOW_DATA'))
   OR
   (p.partition_id = au.container_id AND au.type_desc = 'LOB_DATA')
   )
GROUP BY p.object_id,
         p.index_id,
         p.partition_number,
         p.data_compression_desc
),
-- Look at index stats for each partition of an index
partition_stats AS
(
SELECT o.object_id,
       i.name AS index_name,
       p.partition_number,
       p.partition_size_mb,
       ios.leaf_update_count / NULLIF((ios.range_scan_count + ios.leaf_insert_count + ios.leaf_delete_count + ios.leaf_update_count + ios.leaf_page_merge_count + ios.singleton_lookup_count), 0) AS update_ratio,
       ios.range_scan_count / NULLIF((ios.range_scan_count + ios.leaf_insert_count + ios.leaf_delete_count + ios.leaf_update_count + ios.leaf_page_merge_count + ios.singleton_lookup_count), 0) AS scan_ratio
FROM sys.objects AS o
INNER JOIN sys.indexes AS i
ON o.object_id = i.object_id
INNER JOIN partition_size AS p
ON i.object_id = p.object_id
   AND
   i.index_id = p.index_id
CROSS APPLY sys.dm_db_index_operational_stats(DB_ID(), o.object_id, i.index_id, p.partition_number) AS ios -- assumption: a representative workload has populated index operational stats
WHERE i.type_desc IN ('CLUSTERED','NONCLUSTERED','HEAP')
      AND
      p.data_compression_desc IN ('NONE','ROW') -- partitions already PAGE compressed are out of scope
      AND
      o.is_ms_shipped = 0
      AND
      i.is_hypothetical = 0
      AND
      i.is_disabled = 0
      AND
      NOT EXISTS (
                 SELECT 1
                 FROM sys.tables AS t
                 WHERE t.object_id = o.object_id
                       AND
                       t.is_external = 1
                 )
),
partition_compression AS
(
SELECT ps.object_id,
       ps.index_name,
       ps.partition_number,
       ps.partition_size_mb,
       CASE WHEN -- do not choose page compression when no index stats are available and update_ratio and scan_ratio are NULL, due to low confidence
                 (
                 ps.update_ratio < @CompressionPartitionUpdateRatioThreshold1 -- infrequently updated
                 OR 
                 (
                 ps.update_ratio BETWEEN @CompressionPartitionUpdateRatioThreshold1 AND @CompressionPartitionUpdateRatioThreshold2 
                 AND 
                 ps.scan_ratio > @CompressionPartitionScanRatioThreshold1
                 ) -- more frequently updated but also more frequently scanned
                 ) 
                 AND 
                 rcu.avg_cpu_percent < @CompressionCPUHeadroomThreshold1 -- there is ample CPU headroom
                 AND 
                 rcu.recent_cpu_minutes > @CompressionMinResourceStatSamples -- there is a sufficient number of CPU usage stats
            THEN 'PAGE'
            WHEN rcu.avg_cpu_percent < @CompressionCPUHeadroomThreshold2 -- there is some CPU headroom
                 AND 
                 rcu.recent_cpu_minutes > @CompressionMinResourceStatSamples -- there is a sufficient number of CPU usage stats
            THEN 'ROW'
            WHEN rcu.avg_cpu_percent > @CompressionCPUHeadroomThreshold2 -- there is no CPU headroom, can't use compression
                 AND 
                 rcu.recent_cpu_minutes > @CompressionMinResourceStatSamples -- there is a sufficient number of CPU usage stats
            THEN 'NONE'
            ELSE NULL -- not enough CPU usage stats to decide
       END
       AS new_compression_type
FROM partition_stats AS ps
CROSS JOIN recent_cpu_usage AS rcu
),
partition_compression_interval
AS
(
SELECT object_id,
       index_name,
       new_compression_type,
       partition_number,
       partition_size_mb,
       partition_number - ROW_NUMBER() OVER (
                                            PARTITION BY object_id, index_name, new_compression_type
                                            ORDER BY partition_number
                                            ) 
       AS interval_group -- used to pack contiguous partition intervals for the same object, index, compression type
FROM partition_compression
),
packed_partition_group AS
(
SELECT object_id,
       index_name,
       new_compression_type,
       SUM(partition_size_mb) AS partition_range_size_mb,
       CONCAT(MIN(partition_number), '-', MAX(partition_number)) AS partition_range
FROM partition_compression_interval
GROUP BY object_id,
         index_name,
         new_compression_type,
         interval_group
HAVING COUNT(1) > 0
)
INSERT INTO @DetectedTip (tip_id, details)
SELECT 1180 AS tip_id,
       STRING_AGG(
                 CAST(CONCAT(
                            'schema: ', QUOTENAME(OBJECT_SCHEMA_NAME(object_id)), 
                            ', object: ', QUOTENAME(OBJECT_NAME(object_id)), 
                            ', index: ', QUOTENAME(index_name), 
                            ', partition range: ', partition_range, 
                            ', partition range size (MB): ', FORMAT(partition_range_size_mb, 'N'), 
                            ', new compression type: ', new_compression_type
                            ) AS nvarchar(max)),
                 CONCAT(CHAR(13), CHAR(10))
                 ) 
                 WITHIN GROUP (ORDER BY object_id, index_name, partition_range, partition_range_size_mb, new_compression_type)
       AS details
FROM packed_partition_group
;

-- High log rate
WITH
log_rate_snapshot AS
(
SELECT end_time,
       avg_log_write_percent,
       IIF(avg_log_write_percent > @HighLogRateThresholdPercent, 1, 0) AS high_log_rate_indicator
FROM sys.dm_db_resource_stats
),
pre_packed_log_rate_snapshot AS
(
SELECT end_time,
       avg_log_write_percent,
       high_log_rate_indicator,
       ROW_NUMBER() OVER (ORDER BY end_time) -- row number across all readings, in increasing chronological order
       -
       SUM(high_log_rate_indicator) OVER (ORDER BY end_time ROWS UNBOUNDED PRECEDING) -- running count of all intervals where log rate exceeded the threshold
       AS grouping_helper -- this difference remains constant while log rate is above the threshold, and can be used to collapse/pack an interval using aggregation
FROM log_rate_snapshot
),
packed_log_rate_snapshot AS
(
SELECT MIN(end_time) AS min_end_time,
       MAX(end_time) AS max_end_time,
       AVG(avg_log_write_percent) AS avg_log_write_percent
FROM pre_packed_log_rate_snapshot
WHERE high_log_rate_indicator = 1
GROUP BY grouping_helper
),
log_rate_top_stat AS
(
SELECT MAX(DATEDIFF(second, min_end_time, max_end_time)) AS top_log_rate_duration_seconds,
       MAX(avg_log_write_percent) AS top_log_write_percent,
       COUNT(1) AS count_high_log_write_intervals
FROM packed_log_rate_snapshot 
)
INSERT INTO @DetectedTip (tip_id, details)
SELECT 1190 AS tip_id,
       CONCAT(
             'In the last hour, there were  ', count_high_log_write_intervals, 
             ' interval(s) with log rate staying above ', @HighLogRateThresholdPercent, 
             '%. The longest such interval lasted ', top_log_rate_duration_seconds, 
             ' seconds, and the highest log rate was ', top_log_write_percent, 
             '%.'
             ) AS details
FROM log_rate_top_stat 
WHERE count_high_log_write_intervals > 0
;

-- Plan cache bloat from single-use plans
WITH plan_cache_db_summary AS
(
SELECT t.dbid AS database_id, -- In an elastic pool, return data for all databases
       DB_NAME(t.dbid) AS database_name,
       SUM(IIF(cp.usecounts = 1, cp.size_in_bytes / 1024. / 1024, 0)) AS single_use_db_plan_cache_size_mb,
       SUM(cp.size_in_bytes / 1024. / 1024) AS total_db_plan_cache_size_mb
FROM sys.dm_exec_cached_plans AS cp
CROSS APPLY sys.dm_exec_sql_text(cp.plan_handle) AS t
WHERE cp.objtype IN ('Adhoc','Prepared')
      AND
      cp.cacheobjtype = 'Compiled Plan'
      AND
      t.dbid BETWEEN 5 AND 30000 -- Exclude system databases
GROUP BY t.dbid
)
INSERT INTO @DetectedTip (tip_id, details)
SELECT 1200 AS tip_id,
       STRING_AGG(
                 CAST(CONCAT(
                            'database (id: ',
                            database_id,
                            ', name: ' + database_name, -- database name is only available for current database, include for usability if available
                            '), single use plans take ',
                            FORMAT(single_use_db_plan_cache_size_mb, 'N'),
                            ' MB, or ',
                            FORMAT(single_use_db_plan_cache_size_mb / total_db_plan_cache_size_mb, 'P'),
                            ' of total cached plans for this database.'
                            ) AS nvarchar(max)),
                 CONCAT(CHAR(13), CHAR(10))
                 ) 
                 WITHIN GROUP (ORDER BY database_name DESC, database_id)
       AS details
FROM plan_cache_db_summary
WHERE single_use_db_plan_cache_size_mb >= @SingleUsePlanSizeThresholdMB -- sufficiently large total size of single-use plans for a database
      AND
      single_use_db_plan_cache_size_mb / total_db_plan_cache_size_mb > @SingleUseTotalPlanSizeRatioThreshold -- single-use plans take more than n% of total plan cache size
HAVING COUNT(1) > 0
;

-- Return detected tips
SELECT td.tip_id,
       td.tip_name,
       td.confidence_percent,
       td.tip_url,
       dt.details
FROM @TipDefinition AS td
OUTER APPLY (
            SELECT dt.details AS [processing-instruction(details)]
            FROM @DetectedTip AS dt
            WHERE dt.tip_id = td.tip_id
                  AND
                  dt.details IS NOT NULL
            FOR XML PATH (''), TYPE
            ) dt (details)
WHERE dt.details IS NOT NULL
      OR
      @ReturnAllTips = 1
ORDER BY tip_id
;

END TRY
BEGIN CATCH
    THROW;
END CATCH;
