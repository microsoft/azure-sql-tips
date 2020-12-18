/*
Returns a set of tips aiming to improve database design, health, and performance of an Azure SQL DB database or elastic pool.
For a detailed description and the latest version of the script, see https://aka.ms/sqldbtips

v20201217.1
*/

DECLARE @ReturnAllTips bit = 0; -- Debug flag to return all tips regardless of database state

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

-- Configurable thresholds
DECLARE @HighLogRateThresholdPercent decimal(5,2) = 80, -- Minimum log rate as percentage of SLO limit that is considered as being too high in the "Log rate close to limit" tip
        @GuidLeadingColumnObjectMinSizeMB int = 1024, -- Minimum table size to be considered in the "GUID leading columns in btree indexes" tip
        @UsedToMaxsizeSpaceThresholdRatio decimal(3,2) = 0.8, -- The ratio of used space to database MAXSIZE that is considered as being too high in the "Used space close to MAXSIZE" tip
        @AllocatedToMaxsizeSpaceThresholdRatio decimal(3,2) = 0.8, -- The ratio of allocated space to database MAXSIZE that is considered as being too high in the "Allocated space close to MAXSIZE" tip
        @UsedToAllocatedSpaceThresholdRatio decimal(3,2) = 0.3, -- The ratio of used space to allocated space that is considered as being too low in the "Allocated space much larger than used space" tip
        @UsedToAllocatedSpaceDbMinSizeMB int = 10240, -- Minimum database size to be considered for the "Allocated space much larger than used space" tip
        @CPUThrottlingDelayThresholdPercent decimal(5,2) = 20, -- Minimum percentage of CPU RG delay to be considered as significant CPU throttling in "Recent CPU throttling" tip
        @IndexReadWriteThresholdRatio decimal(3,2) = 0.1, -- The ratio of all index reads to index writes that is considered as being too low in the "Low reads nonclustered indexes" tip
        @CompressionPartitionUpdateRatioThreshold1 decimal(3,2) = 0.2, -- The maximum ratio of updates to all operations to define "infrequent updates" in the "Data compression opportunities" tip
        @CompressionPartitionUpdateRatioThreshold2 decimal(3,2) = 0.5, -- The maximum ratio of updates to all operations to define "more frequent but not frequent enough updates" in the "Data compression opportunities" tip
        @CompressionPartitionScanRatioThreshold1 decimal(3,2) = 0.5, -- The minimum ratio of scans to all operations to define "frequent enough scans" in the "Data compression opportunities" tip
        @CompressionCPUHeadroomThreshold1 decimal(5,2) = 60, -- Maximum CPU usage percentage to be considered as sufficient CPU headroom in the "Data compression opportunities" tip
        @CompressionCPUHeadroomThreshold2 decimal(5,2) = 80, -- Minimum CPU usage percentage to be considered as insufficient CPU headroom in the "Data compression opportunities" tip
        @CompressionMinResourceStatSamples smallint = 30, -- Minimum required number of resource stats sampling intervals in the "Data compression opportunities" tip
        @SingleUsePlanSizeThresholdMB int = 512, -- Minimum required per-db size of single-use plans to be considered as significant in the "Plan cache bloat from single-use plans" tip
        @SingleUseTotalPlanSizeRatioThreshold decimal(3,2) = 0.3, -- The minimum ratio of single-use plans size to total plan size per database to be considered as significant in the "Plan cache bloat from single-use plans" tip
        @MissingIndexAvgUserImpactThreshold decimal(5,2) = 80, -- The minimum user impact for a missing index to be considered as significant in the "Missing indexes" tip
        @RedoQueueSizeThresholdMB int = 1024, -- The minimum size of redo queue on secondaries to be considered as significant in the "Redo queue is large" tip
        @GroupIORGAtLimitThresholdRatio decimal(3,2) = 0.9, -- The minimum ratio of governed IOPS issued to workload group IOPS limit that is considered significant in the "IOPS at SLO workload group limit" tip
        @GroupIORGImpactRatio decimal(3,2) = 0.8, -- The minimum ratio of IO RG delay time to total IO stall time that is considered significant in the "Significant workload group IO RG impact" tip
        @PoolIORGAtLimitThresholdRatio decimal(3,2) = 0.9, -- The minimum ratio of governed IOPS issued to resource pool IOPS limit that is considered significant in the "IOPS at SLO resource pool limit" tip
        @PoolIORGImpactRatio decimal(3,2) = 0.8, -- The minimum ratio of IO RG delay time to total IO stall time that is considered significant in the "Significant resource pool IO RG impact" tip
        @PVSMinimumSizeThresholdGB int = 100, -- The minimum size of persistent version store (PVS) to be considered significant in the "PVS is large" tip
        @PVSToMaxSizeMinThresholdRatio decimal(3,2) = 0.3, -- The minimum ratio of PVS size to database maxsize to be considered significant in the "PVS is large" tip
        @CCICandidateMinSizeGB int = 10, -- The minimum table size to be considered in the "CCI candidates" tip
        @HighGeoReplLagMinThresholdSeconds int = 10, -- The minimum geo-replication lag to be considered significant in the "Geo-replication health" tip
        @RecentGeoReplTranTimeWindowLengthSeconds int = 300 -- The length of time window that defines recent geo-replicated transactions in the "Geo-replication health" tip
;

SET NOCOUNT ON;
SET LOCK_TIMEOUT 5000; -- abort if a concurrent DDL operation holds a lock on metadata

BEGIN TRY

-- Bail out if recent CPU utilization is very high, to avoid impacting workloads
IF EXISTS (
          SELECT 1
          FROM (
               SELECT avg_cpu_percent,
                      avg_instance_cpu_percent,
                      LEAD(end_time) OVER (ORDER BY end_time) AS next_end_time
               FROM sys.dm_db_resource_stats
               ) AS rs
          WHERE next_end_time IS NULL
                AND
                (
                rs.avg_cpu_percent > 98
                OR
                rs.avg_instance_cpu_percent > 95
                )
          )
    THROW 50010, 'CPU utilization is too high. Execute script at a later time.', 1;

-- Define all tips
INSERT INTO @TipDefinition (tip_id, tip_name, confidence_percent, tip_url)
VALUES
(1000, 'Excessive MAXDOP on all replicas',                   90, 'https://aka.ms/sqldbtips#1000'),
(1010, 'Excessive MAXDOP on primary',                        90, 'https://aka.ms/sqldbtips#1010'),
(1020, 'Excessive MAXDOP on secondaries',                    90, 'https://aka.ms/sqldbtips#1020'),
(1030, 'Compatibility level is not current',                 70, 'https://aka.ms/sqldbtips#1030'),
(1040, 'Auto-create stats is disabled',                      95, 'https://aka.ms/sqldbtips#1040'),
(1050, 'Auto-update stats is disabled',                      95, 'https://aka.ms/sqldbtips#1050'),
(1060, 'RCSI is disabled',                                   80, 'https://aka.ms/sqldbtips#1060'),
(1070, 'Query Store is disabled',                            90, 'https://aka.ms/sqldbtips#1070'),
(1071, 'Query Store is read-only',                           90, 'https://aka.ms/sqldbtips#1071'),
(1072, 'Query Store capture mode is NONE',                   90, 'https://aka.ms/sqldbtips#1072'),
(1080, 'AUTO_SHRINK is enabled',                             99, 'https://aka.ms/sqldbtips#1080'),
(1100, 'Btree indexes have GUID leading columns',            60, 'https://aka.ms/sqldbtips#1100'),
(1110, 'FLGP auto-tuning is disabled',                       95, 'https://aka.ms/sqldbtips#1110'),
(1120, 'Used space is close to MAXSIZE',                     80, 'https://aka.ms/sqldbtips#1120'),
(1130, 'Allocated space is close to MAXSIZE',                60, 'https://aka.ms/sqldbtips#1130'),
(1140, 'Allocated space is much larger than used space',     50, 'https://aka.ms/sqldbtips#1140'),
(1150, 'Recent CPU throttling found',                        90, 'https://aka.ms/sqldbtips#1150'),
(1160, 'Recent out of memory errors found',                  80, 'https://aka.ms/sqldbtips#1160'),
(1165, 'Recent memory grant waits and timeouts found',       70, 'https://aka.ms/sqldbtips#1165'),
(1170, 'Nonclustered indexes with low reads found',          60, 'https://aka.ms/sqldbtips#1170'),
(1180, 'Data compression opportunities',                     60, 'https://aka.ms/sqldbtips#1180'),
(1190, 'Log rate is close to limit',                         70, 'https://aka.ms/sqldbtips#1190'),
(1200, 'Plan cache is bloated by single-use plans',          90, 'https://aka.ms/sqldbtips#1200'),
(1210, 'Missing indexes',                                    70, 'https://aka.ms/sqldbtips#1210'),
(1220, 'Redo queue is large',                                60, 'https://aka.ms/sqldbtips#1220'),
(1230, 'Data IOPS are close to workload group limit',        70, 'https://aka.ms/sqldbtips#1230'),
(1240, 'Workload group IO governance impact is significant', 40, 'https://aka.ms/sqldbtips#1240'),
(1250, 'Data IOPS are close to resource pool limit',         70, 'https://aka.ms/sqldbtips#1250'),
(1260, 'Resouce pool IO governance impact is significant',   40, 'https://aka.ms/sqldbtips#1260'),
(1270, 'Persistent Version Store size is large',             70, 'https://aka.ms/sqldbtips#1270'),
(1280, 'Paused resumable index operations found',            90, 'https://aka.ms/sqldbtips#1280'),
(1290, 'Clustered columnstore candidates found',             50, 'https://aka.ms/sqldbtips#1290'),
(1300, 'Geo-replication state may be unhealthy',             70, 'https://aka.ms/sqldbtips#1300')
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

-- Query Store state
WITH qs AS
(
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
)
INSERT INTO @DetectedTip (tip_id, details)
SELECT  tip_id, details
FROM qs
WHERE DATABASEPROPERTYEX(DB_NAME(), 'Updateability') = 'READ_WRITE' -- only produce this on primary
;

-- Auto-shrink
INSERT INTO @DetectedTip (tip_id)
SELECT 1080 AS tip_id
FROM sys.databases
WHERE name = DB_NAME()
      AND
      is_auto_shrink_on = 1
;

-- Btree indexes with uniqueidentifier leading column
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

-- Force plan auto-tuning
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
             ' occurrence(s) of CPU throttling. On average, CPU was throttled by ', FORMAT(avg_cpu_delay_percent, '#,0.00'), '%.'
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
WHERE -- Consider user resource pool only
      name LIKE 'SloSharedPool%'
      OR
      name LIKE 'UserPool%'
)
INSERT INTO @DetectedTip (tip_id, details)
SELECT 1160 AS tip_id,
       CONCAT(
             'In the last ', recent_history_duration_minutes, 
             ' minutes, there were ', count_oom, 
             ' out of memory errors in the ',
             IIF(dso.service_objective = 'ElasticPool', CONCAT(QUOTENAME(dso.elastic_pool_name), ' elastic pool.'), CONCAT(QUOTENAME(DB_NAME(dso.database_id)), ' database.'))
             ) AS details
FROM oom
CROSS JOIN sys.database_service_objectives AS dso
WHERE count_oom > 0
      AND
      dso.database_id = DB_ID()
;

-- Recent memory grant waits and timeouts
WITH memgrant AS
(
SELECT SUM(duration_ms) / 60000 AS recent_history_duration_minutes,
       SUM(delta_memgrant_waiter_count) AS count_memgrant_waiter,
       SUM(delta_memgrant_timeout_count) AS count_memgrant_timeout
FROM sys.dm_resource_governor_resource_pools_history_ex
WHERE -- Consider user resource pool only
      name LIKE 'SloSharedPool%'
      OR
      name LIKE 'UserPool%'
)
INSERT INTO @DetectedTip (tip_id, details)
SELECT 1165 AS tip_id,
       CONCAT(
             'In the last ', recent_history_duration_minutes, 
             ' minutes, there were ', count_memgrant_waiter, 
             ' requests waiting for a memory grant, and ', count_memgrant_timeout,
             ' memory grant timeouts in the ',
             IIF(dso.service_objective = 'ElasticPool', CONCAT(QUOTENAME(dso.elastic_pool_name), ' elastic pool.'), CONCAT(QUOTENAME(DB_NAME(dso.database_id)), ' database.'))
             ) AS details
FROM memgrant
CROSS JOIN sys.database_service_objectives AS dso
WHERE (count_memgrant_waiter > 0 OR count_memgrant_timeout > 0)
      AND
      dso.database_id = DB_ID()
;

-- Little used nonclustered indexes
WITH index_usage AS
(
SELECT STRING_AGG(
                 CAST(CONCAT(
                            QUOTENAME(OBJECT_SCHEMA_NAME(o.object_id)) COLLATE DATABASE_DEFAULT, '.', 
                            QUOTENAME(o.name) COLLATE DATABASE_DEFAULT, '.', 
                            QUOTENAME(i.name) COLLATE DATABASE_DEFAULT, 
                            ' (reads: ', FORMAT(ius.user_seeks + ius.user_scans + ius.user_lookups, '#,0'), ' writes: ', FORMAT(ius.user_updates, '#,0'), ')'
                            ) AS nvarchar(max)), 
                 CONCAT(CHAR(13), CHAR(10))
                 ) AS details
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
      (ius.user_seeks + ius.user_scans + ius.user_lookups) * 1. / NULLIF(ius.user_updates, 0) < @IndexReadWriteThresholdRatio
)
INSERT INTO @DetectedTip (tip_id, details)
SELECT 1170 AS tip_id,
       CONCAT('Since database engine startup at ', CONVERT(varchar(20), si.sqlserver_start_time, 120), ' UTC:', CHAR(13), CHAR(10), iu.details) AS details
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
       i.type_desc AS index_type,
       p.partition_number,
       p.partition_size_mb,
       ios.leaf_update_count * 1. / NULLIF((ios.range_scan_count + ios.leaf_insert_count + ios.leaf_delete_count + ios.leaf_update_count + ios.leaf_page_merge_count + ios.singleton_lookup_count), 0) AS update_ratio,
       ios.range_scan_count * 1. / NULLIF((ios.range_scan_count + ios.leaf_insert_count + ios.leaf_delete_count + ios.leaf_update_count + ios.leaf_page_merge_count + ios.singleton_lookup_count), 0) AS scan_ratio
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
       ps.index_type,
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
       index_type,
       new_compression_type,
       partition_number,
       partition_size_mb,
       partition_number - ROW_NUMBER() OVER (
                                            PARTITION BY object_id, index_name, new_compression_type
                                            ORDER BY partition_number
                                            ) 
       AS interval_group -- used to pack contiguous partition intervals for the same object, index, compression type
FROM partition_compression
WHERE new_compression_type IS NOT NULL
),
packed_partition_group AS
(
SELECT object_id,
       index_name,
       index_type,
       new_compression_type,
       SUM(partition_size_mb) AS partition_range_size_mb,
       CONCAT(MIN(partition_number), '-', MAX(partition_number)) AS partition_range
FROM partition_compression_interval
GROUP BY object_id,
         index_name,
         index_type,
         new_compression_type,
         interval_group
HAVING COUNT(1) > 0
),
packed_partition_group_agg AS
(
SELECT STRING_AGG(
                 CAST(CONCAT(
                            'schema: ', QUOTENAME(OBJECT_SCHEMA_NAME(object_id)) COLLATE DATABASE_DEFAULT,
                            ', object: ', QUOTENAME(OBJECT_NAME(object_id)) COLLATE DATABASE_DEFAULT, 
                            ', index: ' +  QUOTENAME(index_name) COLLATE DATABASE_DEFAULT, 
                            ', index type: ', index_type COLLATE DATABASE_DEFAULT,
                            ', partition range: ', partition_range, 
                            ', partition range size (MB): ', FORMAT(partition_range_size_mb, 'N'), 
                            ', new compression type: ', new_compression_type
                            ) AS nvarchar(max)),
                 CONCAT(CHAR(13), CHAR(10))
                 ) 
                 WITHIN GROUP (ORDER BY object_id, index_name, partition_range, partition_range_size_mb, new_compression_type)
       AS details
FROM packed_partition_group
HAVING COUNT(1) > 0
)
INSERT INTO @DetectedTip (tip_id, details)
SELECT 1180 AS tip_id,
       CONCAT('Since database engine startup at ', CONVERT(varchar(20), si.sqlserver_start_time, 120), ' UTC:', CHAR(13), CHAR(10), ppga.details) AS details
FROM packed_partition_group_agg AS ppga
CROSS JOIN sys.dm_os_sys_info AS si
WHERE ppga.details IS NOT NULL
;

-- High log rate
WITH
log_rate_snapshot AS
(
SELECT end_time,
       avg_log_write_percent,
       IIF(avg_log_write_percent > @HighLogRateThresholdPercent, 1, 0) AS high_log_rate_indicator
FROM sys.dm_db_resource_stats
WHERE DATABASEPROPERTYEX(DB_NAME(), 'Updateability') = 'READ_WRITE' -- only produce this on primary
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
             '%. The longest such interval lasted ', FORMAT(top_log_rate_duration_seconds, '#,0'),
             ' seconds, and the highest log rate was ', FORMAT(top_log_write_percent, '#,0.00'),
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
      t.dbid BETWEEN 5 AND 30000 -- exclude system databases
GROUP BY t.dbid
)
INSERT INTO @DetectedTip (tip_id, details)
SELECT 1200 AS tip_id,
       STRING_AGG(
                 CAST(CONCAT(
                            'database (id: ', database_id,
                            ', name: ' + QUOTENAME(database_name), -- database name is only available for current database, include for usability if available
                            '), single use plans take ', FORMAT(single_use_db_plan_cache_size_mb, 'N'),
                            ' MB, or ', FORMAT(single_use_db_plan_cache_size_mb / total_db_plan_cache_size_mb, 'P'),
                            ' of total cached plans for this database.'
                            ) AS nvarchar(max)),
                 CONCAT(CHAR(13), CHAR(10))
                 ) 
                 WITHIN GROUP (ORDER BY database_name DESC, database_id)
       AS details
FROM plan_cache_db_summary
WHERE single_use_db_plan_cache_size_mb >= @SingleUsePlanSizeThresholdMB -- sufficiently large total size of single-use plans for a database
      AND
      single_use_db_plan_cache_size_mb * 1. / total_db_plan_cache_size_mb > @SingleUseTotalPlanSizeRatioThreshold -- single-use plans take more than n% of total plan cache size
HAVING COUNT(1) > 0
;

-- Missing indexes
WITH missing_index_agg AS
(
SELECT STRING_AGG(
                 CAST(CONCAT(
                            'object_name: ',
                            d.statement,
                            ', equality columns: ' + d.equality_columns,
                            ', inequality columns: ' + d.inequality_columns,
                            ', included columns: ' + d.included_columns,
                            ', unique compiles: ', FORMAT(gs.unique_compiles, '#,0'),
                            ', user seeks: ', FORMAT(gs.user_seeks, '#,0'),
                            ', user scans: ', FORMAT(gs.user_scans, '#,0'),
                            ', avg user impact: ', gs.avg_user_impact, '%.'
                            ) AS nvarchar(max)),
                 CONCAT(CHAR(13), CHAR(10))
                 ) 
                 WITHIN GROUP (ORDER BY avg_user_impact DESC, statement)
       AS details
FROM sys.dm_db_missing_index_group_stats AS gs
INNER JOIN sys.dm_db_missing_index_groups AS g
ON gs.group_handle = g.index_group_handle
INNER JOIN sys.dm_db_missing_index_details AS d
ON g.index_handle = d.index_handle
WHERE gs.avg_user_impact > @MissingIndexAvgUserImpactThreshold
HAVING COUNT(1) > 0
)
INSERT INTO @DetectedTip (tip_id, details)
SELECT 1210 AS tip_id,
       CONCAT('Since database engine startup at ', CONVERT(varchar(20), si.sqlserver_start_time, 120), ' UTC:', CHAR(13), CHAR(10), mia.details) AS details
FROM missing_index_agg AS mia
CROSS JOIN sys.dm_os_sys_info AS si
WHERE mia.details IS NOT NULL
;

-- Redo queue is large
INSERT INTO @DetectedTip (tip_id, details)
SELECT 1220 AS tip_id,
       CONCAT(
             'Current redo queue size: ',
             FORMAT(redo_queue_size / 1024., 'N'),
             ' MB. Most recent sampling of redo rate: ',
             FORMAT(redo_rate / 1024., 'N'),
             ' MB/s.'
             )
       AS details
FROM sys.dm_database_replica_states
WHERE DATABASEPROPERTYEX(DB_NAME(), 'Edition') IN ('Premium','BusinessCritical')
      AND
      is_primary_replica = 0
      AND
      is_local = 1
      AND
      redo_queue_size / 1024. > @RedoQueueSizeThresholdMB
;

-- Data IO reaching user workload group SLO limit, or significant IO RG impact at user workload group level
WITH
io_rg_snapshot AS
(
SELECT wgh.snapshot_time,
       wgh.duration_ms,
       wgh.delta_reads_issued / (wgh.duration_ms / 1000.) AS read_iops,
       wgh.delta_writes_issued / (wgh.duration_ms / 1000.) AS write_iops, -- this is commonly zero, most writes are background writes to data files
       (wgh.reads_throttled - LAG(wgh.reads_throttled) OVER (ORDER BY snapshot_time)) / (wgh.duration_ms / 1000.) AS read_iops_throttled, -- SQL IO RG throttling, not storage throttling
       (wgh.delta_read_bytes / (wgh.duration_ms / 1000.)) / 1024. / 1024 AS read_throughput_mbps,
       wgh.delta_background_writes / (wgh.duration_ms / 1000.) AS background_write_iops, -- checkpoint, lazy writer, PVS
       (wgh.delta_background_write_bytes / (wgh.duration_ms / 1000.)) / 1024. / 1024 AS background_write_throughput_mbps,
       wgh.delta_read_stall_queued_ms, -- time spent in SQL IO RG
       wgh.delta_read_stall_ms, -- total time spent completing the IO, including SQL IO RG time
       rg.primary_group_max_io, -- workload group IOPS limit
       IIF(
          wgh.delta_reads_issued
          +
          IIF(rg.govern_background_io = 0, wgh.delta_background_writes, 0) -- depending on SLO, background write IO may or may not be accounted toward workload group IOPS limit
          >
          CAST(rg.primary_group_max_io AS bigint) * wgh.duration_ms / 1000 * @GroupIORGAtLimitThresholdRatio, -- over n% of IOPS budget for this interval 
          1,
          0
          ) AS reached_iops_limit_indicator,
       IIF(
          wgh.delta_read_stall_queued_ms * 1. / NULLIF(wgh.delta_read_stall_ms, 0)
          >
          @GroupIORGImpactRatio,
          1,
          0
          ) AS significant_io_rg_impact_indicator -- over n% of IO stall is spent in SQL IO RG
FROM sys.dm_resource_governor_workload_groups_history_ex AS wgh
CROSS JOIN sys.dm_user_db_resource_governance AS rg
WHERE rg.database_id = DB_ID()
      AND
      wgh.name like 'UserPrimaryGroup.DB%'
),
pre_packed_io_rg_snapshot AS
(
SELECT SUM(duration_ms) OVER (ORDER BY (SELECT 'no order')) / 60000 AS recent_history_duration_minutes,
       duration_ms,
       snapshot_time,
       read_iops,
       write_iops,
       read_iops_throttled,
       background_write_iops,
       delta_read_stall_queued_ms,
       delta_read_stall_ms,
       read_throughput_mbps,
       background_write_throughput_mbps,
       primary_group_max_io,
       reached_iops_limit_indicator,
       significant_io_rg_impact_indicator,
       ROW_NUMBER() OVER (ORDER BY snapshot_time) -- row number across all readings, in increasing chronological order
       -
       SUM(reached_iops_limit_indicator) OVER (ORDER BY snapshot_time ROWS UNBOUNDED PRECEDING) -- running count of all intervals where the threshold was exceeded
       AS limit_grouping_helper, -- this difference remains constant while the threshold is exceeded, and can be used to collapse/pack an interval using aggregation
       ROW_NUMBER() OVER (ORDER BY snapshot_time)
       -
       SUM(significant_io_rg_impact_indicator) OVER (ORDER BY snapshot_time ROWS UNBOUNDED PRECEDING)
       AS impact_grouping_helper
FROM io_rg_snapshot
WHERE read_iops_throttled IS NOT NULL -- discard the earliest row where the difference with previous snapshot is not defined
),
-- each row is an interval where IOPS was continuously at limit, with aggregated IO stats
packed_io_rg_snapshot_limit AS
(
SELECT MIN(recent_history_duration_minutes) AS recent_history_duration_minutes,
       MIN(snapshot_time) AS min_snapshot_time,
       MAX(snapshot_time) AS max_snapshot_time,
       AVG(duration_ms) AS avg_snapshot_duration_ms,
       SUM(delta_read_stall_queued_ms) AS total_read_throttled_time_ms,
       SUM(delta_read_stall_ms) AS total_read_time_ms,
       AVG(read_iops) AS avg_read_iops,
       MAX(read_iops) AS max_read_iops,
       AVG(write_iops) AS avg_write_iops,
       MAX(write_iops) AS max_write_iops,
       AVG(background_write_iops) AS avg_background_write_iops,
       MAX(background_write_iops) AS max_background_write_iops,
       AVG(read_iops_throttled) AS avg_read_iops_throttled,
       MAX(read_iops_throttled) AS max_read_iops_throttled,
       AVG(read_throughput_mbps) AS avg_read_throughput_mbps,
       MAX(read_throughput_mbps) AS max_read_throughput_mbps,
       AVG(background_write_throughput_mbps) AS avg_background_write_throughput_mbps,
       MAX(background_write_throughput_mbps) AS max_background_write_throughput_mbps,
       MIN(primary_group_max_io) AS primary_group_max_io
FROM pre_packed_io_rg_snapshot
WHERE reached_iops_limit_indicator = 1
GROUP BY limit_grouping_helper
),
-- each row is an interval where IO RG impact remained over the significance threshold, with aggregated IO stats
packed_io_rg_snapshot_impact AS
(
SELECT MIN(recent_history_duration_minutes) AS recent_history_duration_minutes,
       MIN(snapshot_time) AS min_snapshot_time,
       MAX(snapshot_time) AS max_snapshot_time,
       AVG(duration_ms) AS avg_snapshot_duration_ms,
       SUM(delta_read_stall_queued_ms) AS total_read_throttled_time_ms,
       SUM(delta_read_stall_ms) AS total_read_time_ms,
       AVG(read_iops) AS avg_read_iops,
       MAX(read_iops) AS max_read_iops,
       AVG(write_iops) AS avg_write_iops,
       MAX(write_iops) AS max_write_iops,
       AVG(background_write_iops) AS avg_background_write_iops,
       MAX(background_write_iops) AS max_background_write_iops,
       AVG(read_iops_throttled) AS avg_read_iops_throttled,
       MAX(read_iops_throttled) AS max_read_iops_throttled,
       AVG(read_throughput_mbps) AS avg_read_throughput_mbps,
       MAX(read_throughput_mbps) AS max_read_throughput_mbps,
       AVG(background_write_throughput_mbps) AS avg_background_write_throughput_mbps,
       MAX(background_write_throughput_mbps) AS max_background_write_throughput_mbps,
       MIN(primary_group_max_io) AS primary_group_max_io
FROM pre_packed_io_rg_snapshot
WHERE significant_io_rg_impact_indicator = 1
GROUP BY impact_grouping_helper
),
-- one row, a summary across all intervals where IOPS was continuously at limit
packed_io_rg_snapshot_limit_agg AS
(
SELECT MIN(recent_history_duration_minutes) AS recent_history_duration_minutes,
       MAX(DATEDIFF(second, min_snapshot_time, max_snapshot_time) + avg_snapshot_duration_ms / 1000.) AS longest_io_rg_at_limit_duration_seconds,
       COUNT(1) AS count_io_rg_at_limit_intervals,
       SUM(total_read_time_ms) AS total_read_time_ms,
       SUM(total_read_throttled_time_ms) AS total_read_throttled_time_ms,
       AVG(avg_read_iops) AS avg_read_iops,
       MAX(max_read_iops) AS max_read_iops,
       AVG(avg_write_iops) AS avg_write_iops,
       MAX(max_write_iops) AS max_write_iops,
       AVG(avg_background_write_iops) AS avg_background_write_iops,
       MAX(max_background_write_iops) AS max_background_write_iops,
       AVG(avg_read_throughput_mbps) AS avg_read_throughput_mbps,
       MAX(max_read_throughput_mbps) AS max_read_throughput_mbps,
       AVG(avg_background_write_throughput_mbps) AS avg_background_write_throughput_mbps,
       MAX(max_background_write_throughput_mbps) AS max_background_write_throughput_mbps,
       MIN(primary_group_max_io) AS primary_group_max_io
FROM packed_io_rg_snapshot_limit
),
-- one row, a summary across all intervals where IO RG impact remained over the significance threshold
packed_io_rg_snapshot_impact_agg AS
(
SELECT MIN(recent_history_duration_minutes) AS recent_history_duration_minutes,
       MAX(DATEDIFF(second, min_snapshot_time, max_snapshot_time) + avg_snapshot_duration_ms / 1000.) AS longest_io_rg_impact_duration_seconds,
       COUNT(1) AS count_io_rg_impact_intervals,
       SUM(total_read_time_ms) AS total_read_time_ms,
       SUM(total_read_throttled_time_ms) AS total_read_throttled_time_ms,
       AVG(avg_read_iops) AS avg_read_iops,
       MAX(max_read_iops) AS max_read_iops,
       AVG(avg_write_iops) AS avg_write_iops,
       MAX(max_write_iops) AS max_write_iops,
       AVG(avg_background_write_iops) AS avg_background_write_iops,
       MAX(max_background_write_iops) AS max_background_write_iops,
       AVG(avg_read_throughput_mbps) AS avg_read_throughput_mbps,
       MAX(max_read_throughput_mbps) AS max_read_throughput_mbps,
       AVG(avg_background_write_throughput_mbps) AS avg_background_write_throughput_mbps,
       MAX(max_background_write_throughput_mbps) AS max_background_write_throughput_mbps
FROM packed_io_rg_snapshot_impact
)
INSERT INTO @DetectedTip (tip_id, details)
SELECT 1230 AS tip_id,
       CONCAT(
             'In the last ', recent_history_duration_minutes,
             ' minutes, there were ', count_io_rg_at_limit_intervals, 
             ' time interval(s) when total data IO approached the workload group (database-level) IOPS limit of the service objective, ', FORMAT(primary_group_max_io, '#,0'), ' IOPS.', CHAR(13), CHAR(10),
             'Across these intervals, aggregate IO statistics were: ', CHAR(13), CHAR(10),
             'longest interval duration: ', FORMAT(longest_io_rg_at_limit_duration_seconds, '#,0'), ' seconds; ', CHAR(13), CHAR(10),
             'total read IO time: ', FORMAT(total_read_time_ms, '#,0'), ' milliseconds; ', CHAR(13), CHAR(10),
             'total throttled read IO time: ', FORMAT(total_read_throttled_time_ms, '#,0'), ' milliseconds; ', CHAR(13), CHAR(10),
             'average read IOPS: ', FORMAT(avg_read_iops, '#,0'), '; ', CHAR(13), CHAR(10),
             'maximum read IOPS: ', FORMAT(max_read_iops, '#,0'), '; ', CHAR(13), CHAR(10),
             'average write IOPS: ', FORMAT(avg_write_iops, '#,0'), '; ', CHAR(13), CHAR(10),
             'maximum write IOPS: ', FORMAT(max_write_iops, '#,0'), '; ', CHAR(13), CHAR(10),
             'average background write IOPS: ', FORMAT(avg_background_write_iops, '#,0'), '; ', CHAR(13), CHAR(10),
             'maximum background write IOPS: ', FORMAT(max_background_write_iops, '#,0'), '; ', CHAR(13), CHAR(10),
             'average read IO throughput: ', FORMAT(avg_read_throughput_mbps, '#,0.00'), ' MBps; ', CHAR(13), CHAR(10),
             'maximum read IO throughput: ', FORMAT(max_read_throughput_mbps, '#,0.00'), ' MBps; ', CHAR(13), CHAR(10),
             'average background write IO throughput: ', FORMAT(avg_background_write_throughput_mbps, '#,0.00'), ' MBps; ', CHAR(13), CHAR(10),
             'maximum background write IO throughput: ', FORMAT(max_background_write_throughput_mbps, '#,0.00'), ' MBps.'
             )
       AS details
FROM packed_io_rg_snapshot_limit_agg
WHERE count_io_rg_at_limit_intervals > 0
UNION
SELECT 1240 AS tip_id,
       CONCAT(
             'In the last ', recent_history_duration_minutes,
             ' minutes, there were ', count_io_rg_impact_intervals, 
             ' time interval(s) when workload group (database-level) resource governance for the selected service objective was significantly delaying IO.', CHAR(13), CHAR(10),
             'Across these intervals, aggregate IO statistics were: ', CHAR(13), CHAR(10),
             'longest interval duration: ', FORMAT(longest_io_rg_impact_duration_seconds, '#,0'), ' seconds; ', CHAR(13), CHAR(10),
             'total read IO time: ', FORMAT(total_read_time_ms, '#,0'), ' milliseconds; ', CHAR(13), CHAR(10),
             'total throttled read IO time: ', FORMAT(total_read_throttled_time_ms, '#,0'), ' milliseconds; ', CHAR(13), CHAR(10),
             'average read IOPS: ', FORMAT(avg_read_iops, '#,0'), '; ', CHAR(13), CHAR(10),
             'maximum read IOPS: ', FORMAT(max_read_iops, '#,0'), '; ', CHAR(13), CHAR(10),
             'average write IOPS: ', FORMAT(avg_write_iops, '#,0'), '; ', CHAR(13), CHAR(10),
             'maximum write IOPS: ', FORMAT(max_write_iops, '#,0'), '; ', CHAR(13), CHAR(10),
             'average background write IOPS: ', FORMAT(avg_background_write_iops, '#,0'), '; ', CHAR(13), CHAR(10),
             'maximum background write IOPS: ', FORMAT(max_background_write_iops, '#,0'), '; ', CHAR(13), CHAR(10),
             'average read IO throughput: ', FORMAT(avg_read_throughput_mbps, '#,0.00'), ' MBps; ', CHAR(13), CHAR(10),
             'maximum read IO throughput: ', FORMAT(max_read_throughput_mbps, '#,0.00'), ' MBps; ', CHAR(13), CHAR(10),
             'average background write IO throughput: ', FORMAT(avg_background_write_throughput_mbps, '#,0.00'), ' MBps; ', CHAR(13), CHAR(10),
             'maximum background write IO throughput: ', FORMAT(max_background_write_throughput_mbps, '#,0.00'), ' MBps.'
             )
       AS details
FROM packed_io_rg_snapshot_impact_agg
WHERE count_io_rg_impact_intervals > 0
;

-- Data IO reaching user resource pool SLO limit, or significant IO RG impact at user resource pool level
WITH
io_rg_snapshot AS
(
SELECT rph.snapshot_time,
       rph.duration_ms,
       rph.delta_read_io_issued / (rph.duration_ms / 1000.) AS read_iops,
       rph.delta_write_io_issued / (rph.duration_ms / 1000.) AS write_iops, -- this is commonly zero, most writes are background writes to data files
       rph.delta_read_io_throttled / (rph.duration_ms / 1000.) AS read_iops_throttled, -- SQL IO RG throttling, not storage throttling
       (rph.delta_read_bytes / (rph.duration_ms / 1000.)) / 1024. / 1024 AS read_throughput_mbps,
       rph.delta_read_io_stall_queued_ms, -- time spent in SQL IO RG
       rph.delta_read_io_stall_ms, -- total time spent completing the IO, including SQL IO RG time
       rg.pool_max_io, -- resource pool IOPS limit
       IIF(
          rph.delta_read_io_issued
          >
          CAST(rg.pool_max_io AS bigint) * rph.duration_ms / 1000 * @PoolIORGAtLimitThresholdRatio, -- over n% of IOPS budget for this interval 
          1,
          0
          ) AS reached_iops_limit_indicator,
       IIF(
          rph.delta_read_io_stall_queued_ms * 1. / NULLIF(rph.delta_read_io_stall_ms, 0)
          >
          @PoolIORGImpactRatio,
          1,
          0
          ) AS significant_io_rg_impact_indicator -- over n% of IO stall is spent in SQL IO RG
FROM sys.dm_resource_governor_resource_pools_history_ex AS rph
CROSS JOIN sys.dm_user_db_resource_governance AS rg
WHERE rg.database_id = DB_ID()
      AND
      -- Consider user resource pool only
      (
      rph.name LIKE 'SloSharedPool%'
      OR
      rph.name LIKE 'UserPool%'
      )
      AND
      rg.pool_max_io > 0 -- resource pool IO is governed
),
pre_packed_io_rg_snapshot AS
(
SELECT SUM(duration_ms) OVER (ORDER BY (SELECT 'no order')) / 60000 AS recent_history_duration_minutes,
       duration_ms,
       snapshot_time,
       read_iops,
       write_iops,
       read_iops_throttled,
       delta_read_io_stall_queued_ms,
       delta_read_io_stall_ms,
       read_throughput_mbps,
       pool_max_io,
       reached_iops_limit_indicator,
       significant_io_rg_impact_indicator,
       ROW_NUMBER() OVER (ORDER BY snapshot_time) -- row number across all readings, in increasing chronological order
       -
       SUM(reached_iops_limit_indicator) OVER (ORDER BY snapshot_time ROWS UNBOUNDED PRECEDING) -- running count of all intervals where the threshold was exceeded
       AS limit_grouping_helper, -- this difference remains constant while the threshold is exceeded, and can be used to collapse/pack an interval using aggregation
       ROW_NUMBER() OVER (ORDER BY snapshot_time)
       -
       SUM(significant_io_rg_impact_indicator) OVER (ORDER BY snapshot_time ROWS UNBOUNDED PRECEDING)
       AS impact_grouping_helper
FROM io_rg_snapshot
),
-- each row is an interval where IOPS was continuously at limit, with aggregated IO stats
packed_io_rg_snapshot_limit AS
(
SELECT MIN(recent_history_duration_minutes) AS recent_history_duration_minutes,
       MIN(snapshot_time) AS min_snapshot_time,
       MAX(snapshot_time) AS max_snapshot_time,
       AVG(duration_ms) AS avg_snapshot_duration_ms,
       SUM(delta_read_io_stall_queued_ms) AS total_read_throttled_time_ms,
       SUM(delta_read_io_stall_ms) AS total_read_time_ms,
       AVG(read_iops) AS avg_read_iops,
       MAX(read_iops) AS max_read_iops,
       AVG(write_iops) AS avg_write_iops,
       MAX(write_iops) AS max_write_iops,
       AVG(read_iops_throttled) AS avg_read_iops_throttled,
       MAX(read_iops_throttled) AS max_read_iops_throttled,
       AVG(read_throughput_mbps) AS avg_read_throughput_mbps,
       MAX(read_throughput_mbps) AS max_read_throughput_mbps,
       MIN(pool_max_io) AS pool_max_io
FROM pre_packed_io_rg_snapshot
WHERE reached_iops_limit_indicator = 1
GROUP BY limit_grouping_helper
),
-- each row is an interval where IO RG impact remained over the significance threshold, with aggregated IO stats
packed_io_rg_snapshot_impact AS
(
SELECT MIN(recent_history_duration_minutes) AS recent_history_duration_minutes,
       MIN(snapshot_time) AS min_snapshot_time,
       MAX(snapshot_time) AS max_snapshot_time,
       AVG(duration_ms) AS avg_snapshot_duration_ms,
       SUM(delta_read_io_stall_queued_ms) AS total_read_throttled_time_ms,
       SUM(delta_read_io_stall_ms) AS total_read_time_ms,
       AVG(read_iops) AS avg_read_iops,
       MAX(read_iops) AS max_read_iops,
       AVG(write_iops) AS avg_write_iops,
       MAX(write_iops) AS max_write_iops,
       AVG(read_iops_throttled) AS avg_read_iops_throttled,
       MAX(read_iops_throttled) AS max_read_iops_throttled,
       AVG(read_throughput_mbps) AS avg_read_throughput_mbps,
       MAX(read_throughput_mbps) AS max_read_throughput_mbps,
       MIN(pool_max_io) AS pool_max_io
FROM pre_packed_io_rg_snapshot
WHERE significant_io_rg_impact_indicator = 1
GROUP BY impact_grouping_helper
),
-- one row, a summary across all intervals where IOPS was continuously at limit
packed_io_rg_snapshot_limit_agg AS
(
SELECT MIN(recent_history_duration_minutes) AS recent_history_duration_minutes,
       MAX(DATEDIFF(second, min_snapshot_time, max_snapshot_time) + avg_snapshot_duration_ms / 1000.) AS longest_io_rg_at_limit_duration_seconds,
       COUNT(1) AS count_io_rg_at_limit_intervals,
       SUM(total_read_time_ms) AS total_read_time_ms,
       SUM(total_read_throttled_time_ms) AS total_read_throttled_time_ms,
       AVG(avg_read_iops) AS avg_read_iops,
       MAX(max_read_iops) AS max_read_iops,
       AVG(avg_write_iops) AS avg_write_iops,
       MAX(max_write_iops) AS max_write_iops,
       AVG(avg_read_throughput_mbps) AS avg_read_throughput_mbps,
       MAX(max_read_throughput_mbps) AS max_read_throughput_mbps,
       MIN(pool_max_io) AS pool_max_io
FROM packed_io_rg_snapshot_limit
),
-- one row, a summary across all intervals where IO RG impact remained over the significance threshold
packed_io_rg_snapshot_impact_agg AS
(
SELECT MIN(recent_history_duration_minutes) AS recent_history_duration_minutes,
       MAX(DATEDIFF(second, min_snapshot_time, max_snapshot_time) + avg_snapshot_duration_ms / 1000.) AS longest_io_rg_impact_duration_seconds,
       COUNT(1) AS count_io_rg_impact_intervals,
       SUM(total_read_time_ms) AS total_read_time_ms,
       SUM(total_read_throttled_time_ms) AS total_read_throttled_time_ms,
       AVG(avg_read_iops) AS avg_read_iops,
       MAX(max_read_iops) AS max_read_iops,
       AVG(avg_write_iops) AS avg_write_iops,
       MAX(max_write_iops) AS max_write_iops,
       AVG(avg_read_throughput_mbps) AS avg_read_throughput_mbps,
       MAX(max_read_throughput_mbps) AS max_read_throughput_mbps
FROM packed_io_rg_snapshot_impact
)
INSERT INTO @DetectedTip (tip_id, details)
SELECT 1250 AS tip_id,
       CONCAT(
             'In the last ', l.recent_history_duration_minutes,
             ' minutes, there were ', l.count_io_rg_at_limit_intervals, 
             ' time interval(s) when total data IO approached the resource pool IOPS limit of the service objective ', IIF(dso.service_objective = 'ElasticPool', CONCAT('for elastic pool ', QUOTENAME(dso.elastic_pool_name)), ''), ', ', FORMAT(l.pool_max_io, '#,0'), ' IOPS.', CHAR(13), CHAR(10),
             'Across these intervals, aggregate IO statistics were: ', CHAR(13), CHAR(10),
             'longest interval duration: ', FORMAT(l.longest_io_rg_at_limit_duration_seconds, '#,0'), ' seconds; ', CHAR(13), CHAR(10),
             'total read IO time: ', FORMAT(l.total_read_time_ms, '#,0'), ' milliseconds; ', CHAR(13), CHAR(10),
             'total throttled read IO time: ', FORMAT(l.total_read_throttled_time_ms, '#,0'), ' milliseconds; ', CHAR(13), CHAR(10),
             'average read IOPS: ', FORMAT(l.avg_read_iops, '#,0'), '; ', CHAR(13), CHAR(10),
             'maximum read IOPS: ', FORMAT(l.max_read_iops, '#,0'), '; ', CHAR(13), CHAR(10),
             'average write IOPS: ', FORMAT(l.avg_write_iops, '#,0'), '; ', CHAR(13), CHAR(10),
             'maximum write IOPS: ', FORMAT(l.max_write_iops, '#,0'), '; ', CHAR(13), CHAR(10),
             'average read IO throughput: ', FORMAT(l.avg_read_throughput_mbps, '#,0.00'), ' MBps; ', CHAR(13), CHAR(10),
             'maximum read IO throughput: ', FORMAT(l.max_read_throughput_mbps, '#,0.00'), ' MBps.'
             )
       AS details
FROM packed_io_rg_snapshot_limit_agg AS l
CROSS JOIN sys.database_service_objectives AS dso
WHERE l.count_io_rg_at_limit_intervals > 0
      AND
      dso.database_id = DB_ID()
UNION
SELECT 1260 AS tip_id,
       CONCAT(
             'In the last ', i.recent_history_duration_minutes,
             ' minutes, there were ', i.count_io_rg_impact_intervals, 
             ' time interval(s) when resource pool resource governance for the selected service objective was significantly delaying IO', IIF(dso.service_objective = 'ElasticPool', CONCAT(' for elastic pool ', QUOTENAME(dso.elastic_pool_name)), ''), '.', CHAR(13), CHAR(10),
             'Across these intervals, aggregate IO statistics were: ', CHAR(13), CHAR(10),
             'longest interval duration: ', FORMAT(i.longest_io_rg_impact_duration_seconds, '#,0'), ' seconds; ', CHAR(13), CHAR(10),
             'total read IO time: ', FORMAT(i.total_read_time_ms, '#,0'), ' milliseconds; ', CHAR(13), CHAR(10),
             'total throttled read IO time: ', FORMAT(i.total_read_throttled_time_ms, '#,0'), ' milliseconds; ', CHAR(13), CHAR(10),
             'average read IOPS: ', FORMAT(i.avg_read_iops, '#,0'), '; ', CHAR(13), CHAR(10),
             'maximum read IOPS: ', FORMAT(i.max_read_iops, '#,0'), '; ', CHAR(13), CHAR(10),
             'average write IOPS: ', FORMAT(i.avg_write_iops, '#,0'), '; ', CHAR(13), CHAR(10),
             'maximum write IOPS: ', FORMAT(i.max_write_iops, '#,0'), '; ', CHAR(13), CHAR(10),
             'average read IO throughput: ', FORMAT(i.avg_read_throughput_mbps, '#,0.00'), ' MBps; ', CHAR(13), CHAR(10),
             'maximum read IO throughput: ', FORMAT(i.max_read_throughput_mbps, '#,0.00'), ' MBps.'
             )
       AS details
FROM packed_io_rg_snapshot_impact_agg AS i
CROSS JOIN sys.database_service_objectives AS dso
WHERE i.count_io_rg_impact_intervals > 0
      AND
      dso.database_id = DB_ID()
;

-- Large PVS
WITH 
db_allocated_size AS
(
SELECT SUM(size * 8.) AS db_allocated_size_kb
FROM sys.database_files
WHERE type_desc = 'ROWS'
),
pvs_db_stats AS
(
SELECT pvss.persistent_version_store_size_kb / 1024. / 1024 AS persistent_version_store_size_gb,
       pvss.online_index_version_store_size_kb / 1024. / 1024 AS online_index_version_store_size_gb,
       pvss.current_aborted_transaction_count,
       pvss.aborted_version_cleaner_start_time,
       pvss.aborted_version_cleaner_end_time,
       dt.database_transaction_begin_time AS oldest_transaction_begin_time,
       asdt.session_id AS active_transaction_session_id,
       asdt.elapsed_time_seconds AS active_transaction_elapsed_time_seconds
FROM sys.dm_tran_persistent_version_store_stats AS pvss
CROSS JOIN db_allocated_size AS das
LEFT JOIN sys.dm_tran_database_transactions AS dt
ON pvss.oldest_active_transaction_id = dt.transaction_id
   AND
   pvss.database_id = dt.database_id
LEFT JOIN sys.dm_tran_active_snapshot_database_transactions AS asdt
ON pvss.min_transaction_timestamp = asdt.transaction_sequence_num
   OR
   pvss.online_index_min_transaction_timestamp = asdt.transaction_sequence_num
WHERE pvss.database_id = DB_ID()
      AND
      (
      persistent_version_store_size_kb > @PVSMinimumSizeThresholdGB * 1024 * 1024 -- PVS is larger than n GB
      OR
      persistent_version_store_size_kb > @PVSToMaxSizeMinThresholdRatio * das.db_allocated_size_kb -- PVS is larger than n% of database allocated size
      )
)
INSERT INTO @DetectedTip (tip_id, details)
SELECT 1270 AS tip_id,
       CONCAT(
             'PVS size (GB): ', FORMAT(persistent_version_store_size_gb, 'N'), CHAR(13), CHAR(10),
             'online index version store size (GB): ', FORMAT(online_index_version_store_size_gb, 'N'), CHAR(13), CHAR(10),
             'current aborted transaction count: ', FORMAT(current_aborted_transaction_count, '#,0'), CHAR(13), CHAR(10),
             'aborted transaction version cleaner start time: ', ISNULL(CONVERT(varchar(20), aborted_version_cleaner_start_time, 120), 'N/A'), CHAR(13), CHAR(10),
             'aborted transaction version cleaner end time: ', ISNULL(CONVERT(varchar(20), aborted_version_cleaner_end_time, 120), 'N/A'), CHAR(13), CHAR(10),
             'oldest transaction begin time: ',  ISNULL(CONVERT(varchar(30), oldest_transaction_begin_time, 121), 'N/A'), CHAR(13), CHAR(10),
             'active transaction session_id: ', ISNULL(CAST(active_transaction_session_id AS varchar(11)), 'N/A'), CHAR(13), CHAR(10),
             'active transaction elapsed time (seconds): ', ISNULL(CAST(active_transaction_elapsed_time_seconds AS varchar(11)), 'N/A')
             )
       AS details
FROM pvs_db_stats
;

-- Paused resumable index DDL
WITH resumable_index_op AS
(
SELECT OBJECT_SCHEMA_NAME(iro.object_id) AS schema_name,
       OBJECT_NAME(iro.object_id) AS object_name,
       iro.name AS index_name,
       i.type_desc AS index_type,
       iro.percent_complete,
       iro.start_time,
       iro.last_pause_time,
       iro.total_execution_time AS total_execution_time_minutes,
       iro.page_count * 8 / 1024. AS index_operation_allocated_space_mb,
       IIF(CAST(dsc.value AS int) = 0, NULL, DATEDIFF(minute, CURRENT_TIMESTAMP, DATEADD(minute, CAST(dsc.value AS int), iro.last_pause_time))) AS time_to_auto_abort_minutes,
       iro.sql_text
FROM sys.index_resumable_operations AS iro
LEFT JOIN sys.indexes AS i -- new index being created will not be present, thus using outer join
ON iro.object_id = i.object_id
   AND
   iro.index_id = i.index_id
CROSS JOIN sys.database_scoped_configurations AS dsc
WHERE iro.state_desc = 'PAUSED'
      AND
      dsc.name = 'PAUSED_RESUMABLE_INDEX_ABORT_DURATION_MINUTES'
)
INSERT INTO @DetectedTip (tip_id, details)
SELECT 1280 AS tip_id,
       STRING_AGG(
                 CAST(CONCAT(
                            'schema name: ', QUOTENAME(schema_name) COLLATE DATABASE_DEFAULT, CHAR(13), CHAR(10),
                            'object name: ', QUOTENAME(object_name) COLLATE DATABASE_DEFAULT, CHAR(13), CHAR(10),
                            'index name: ', QUOTENAME(index_name) COLLATE DATABASE_DEFAULT, CHAR(13), CHAR(10),
                            'index type: ' + index_type COLLATE DATABASE_DEFAULT + CHAR(13) + CHAR(10),
                            'percent complete: ', FORMAT(percent_complete, '#,0.00'), '%', CHAR(13), CHAR(10),
                            'start time: ', CONVERT(varchar(20), start_time, 120), CHAR(13), CHAR(10),
                            'last pause time: ', CONVERT(varchar(20), last_pause_time, 120), CHAR(13), CHAR(10),
                            'total execution time (minutes): ', FORMAT(total_execution_time_minutes, '#,0'), CHAR(13), CHAR(10),
                            'space allocated by resumable index operation (MB): ', FORMAT(index_operation_allocated_space_mb, '#,0.00'), CHAR(13), CHAR(10),
                            'time remaining to auto-abort (minutes): ' + FORMAT(time_to_auto_abort_minutes, '#,0') + CHAR(13) + CHAR(10),
                            'index operation SQL statement: ', sql_text COLLATE DATABASE_DEFAULT, CHAR(13), CHAR(10)
                            ) AS nvarchar(max)),
                 CONCAT(CHAR(13), CHAR(10))
                 )
                 WITHIN GROUP (ORDER BY schema_name, object_name, index_name)
FROM resumable_index_op 
HAVING COUNT(1) > 0
;

-- CCI candidates
WITH
candidate_partition AS
(
SELECT p.object_id,
       p.index_id,
       p.partition_number,
       p.rows,
       SUM(au.used_pages) * 8 / 1024. AS partition_size_mb
FROM sys.partitions AS p
INNER JOIN sys.allocation_units AS au
ON (
   (p.hobt_id = au.container_id AND au.type_desc IN ('IN_ROW_DATA','ROW_OVERFLOW_DATA'))
   OR
   (p.partition_id = au.container_id AND au.type_desc = 'LOB_DATA')
   )
WHERE p.data_compression_desc IN ('NONE','ROW','PAGE')
      AND
      -- exclude all partitions of tables with NCCI, and all NCI partitions of tables with CCI
      NOT EXISTS (
                 SELECT 1
                 FROM sys.partitions AS pncci
                 WHERE pncci.object_id = p.object_id
                       AND
                       pncci.index_id NOT IN (0,1)
                       AND
                       pncci.data_compression_desc IN ('COLUMNSTORE','COLUMNSTORE_ARCHIVE')
                 UNION
                 SELECT 1
                 FROM sys.partitions AS pnci
                 WHERE pnci.object_id = p.object_id
                       AND
                       pnci.index_id = 1
                       AND
                       pnci.data_compression_desc IN ('COLUMNSTORE','COLUMNSTORE_ARCHIVE')
                 )
GROUP BY p.object_id,
         p.index_id,
         p.partition_number,
         p.rows
),
table_operational_stats AS -- summarize operational stats for heap, CI, and NCI
(
SELECT cp.object_id,
       SUM(IIF(cp.index_id IN (0,1), partition_size_mb, 0)) AS table_size_mb, -- exclude NCI size
       SUM(IIF(cp.index_id IN (0,1), 1, 0)) AS partition_count,
       SUM(ios.leaf_insert_count) AS lead_insert_count,
       SUM(ios.leaf_update_count) AS leaf_update_count,
       SUM(ios.leaf_delete_count + ios.leaf_ghost_count) AS leaf_delete_count,
       SUM(ios.range_scan_count) AS range_scan_count,
       SUM(ios.singleton_lookup_count) AS singleton_lookup_count
FROM candidate_partition AS cp
CROSS APPLY sys.dm_db_index_operational_stats(DB_ID(), cp.object_id, cp.index_id, cp.partition_number) AS ios -- assumption: a representative workload has populated index operational stats for relevant tables
GROUP BY cp.object_id
),
cci_candidate_table AS
(
SELECT QUOTENAME(OBJECT_SCHEMA_NAME(t.object_id)) COLLATE DATABASE_DEFAULT AS schema_name,
       QUOTENAME(t.name)  COLLATE DATABASE_DEFAULT AS table_name,
       tos.table_size_mb,
       tos.partition_count,
       tos.lead_insert_count AS insert_count,
       tos.leaf_update_count AS update_count,
       tos.leaf_delete_count AS delete_count,
       tos.singleton_lookup_count AS singleton_lookup_count,
       tos.range_scan_count AS range_scan_count,
       ius.user_seeks AS seek_count,
       ius.user_scans AS full_scan_count,
       ius.user_lookups AS lookup_count
FROM sys.tables AS t
INNER JOIN sys.indexes AS i
ON t.object_id = i.object_id
INNER JOIN table_operational_stats AS tos
ON t.object_id = tos.object_id
INNER JOIN sys.dm_db_index_usage_stats AS ius
ON t.object_id = ius.object_id
   AND
   i.index_id = ius.index_id
WHERE i.type IN (0,1) -- clustered index or heap
      AND
      tos.table_size_mb > @CCICandidateMinSizeGB / 1024. -- consider sufficiently large tables only
      AND
      t.is_ms_shipped = 0
      AND
      -- at least one partition is columnstore compressible
      EXISTS (
             SELECT 1
             FROM candidate_partition AS cp
             WHERE cp.object_id = t.object_id
                   AND
                   cp.rows >= 102400
             )
      AND
      -- conservatively require a CCI candidate to have no updates, seeks, or lookups
      tos.leaf_update_count = 0
      AND
      tos.singleton_lookup_count = 0
      AND
      ius.user_lookups = 0
      AND
      ius.user_seeks = 0
      AND
      ius.user_scans > 0 -- require a CCI candidate to have some full scans
),
cci_candidate_details AS
(
SELECT STRING_AGG(
                 CAST(CONCAT(
                            'schema: ', schema_name, ', ',
                            'table: ', table_name, ', ',
                            'table size (MB): ', FORMAT(table_size_mb, '#,0.00'), ', ',
                            'partition count: ', FORMAT(partition_count, '#,0'), ', ',
                            'inserts: ', FORMAT(insert_count, '#,0'), ', ',
                            'updates: ', FORMAT(update_count, '#,0'), ', ',
                            'deletes: ', FORMAT(delete_count, '#,0'), ', ',
                            'singleton lookups: ', FORMAT(singleton_lookup_count, '#,0'), ', ',
                            'range scans: ', FORMAT(range_scan_count, '#,0'), ', ',
                            'seeks: ', FORMAT(seek_count, '#,0'), ', ',
                            'full scans: ', FORMAT(full_scan_count, '#,0'), ', ',
                            'lookups: ', FORMAT(lookup_count, '#,0')
                            ) AS nvarchar(max)),
                 CONCAT(CHAR(13), CHAR(10))
                 )
                 WITHIN GROUP (ORDER BY schema_name, table_name)
       AS details
FROM cci_candidate_table
)
INSERT INTO @DetectedTip (tip_id, details)
SELECT 1290 AS tip_id,
       CONCAT('Since database engine startup at ', CONVERT(varchar(20), si.sqlserver_start_time, 120), ' UTC:', CHAR(13), CHAR(10), ccd.details) AS details
FROM cci_candidate_details AS ccd
CROSS JOIN sys.dm_os_sys_info AS si
WHERE ccd.details IS NOT NULL
;

-- Geo-replication health
WITH 
geo_replication_link_details AS
(
SELECT STRING_AGG(
                 CAST(CONCAT(
                            'link GUID: ', link_guid, ', ',
                            'local server: ' + QUOTENAME(@@SERVERNAME) + ', ',
                            'local database: ' + QUOTENAME(DB_NAME()) + ', ',
                            'partner server: ' + QUOTENAME(partner_server) + ', ',
                            'partner database: ' + QUOTENAME(partner_database) + ', ',
                            'geo-replication role: ' + role_desc + ', ',
                            'last replication time: ' + CAST(last_replication AS varchar(40)) + ', ',
                            'geo-replication lag (seconds): ' + FORMAT(replication_lag_sec, '#,0') + ', ',
                            'geo-replication state: ' + replication_state_desc
                            ) AS nvarchar(max)),
                 CONCAT(CHAR(13), CHAR(10))
                 )
                 WITHIN GROUP (ORDER BY partner_server, partner_database)
       AS details
FROM sys.dm_geo_replication_link_status
WHERE (replication_state_desc <> 'CATCH_UP' OR replication_state_desc IS NULL)
      OR
      -- high replication lag for recent transactions
      (
      replication_state_desc = 'CATCH_UP'
      AND
      replication_lag_sec > @HighGeoReplLagMinThresholdSeconds
      AND
      last_replication > DATEADD(second, -@RecentGeoReplTranTimeWindowLengthSeconds, SYSDATETIMEOFFSET())
      )
HAVING COUNT(1) > 0
)
INSERT INTO @DetectedTip (tip_id, details)
SELECT 1300 AS tip_id,
       details
FROM geo_replication_link_details
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
ORDER BY confidence_percent DESC
;

END TRY
BEGIN CATCH
    SET LOCK_TIMEOUT -1; -- revert to default

    THROW;
END CATCH;
