DECLARE @TipDefinition table (
                             tip_id smallint not null primary key,
                             tip_name nvarchar(50) not null unique,
                             tip_description nvarchar(4000) not null,
                             sample_command nvarchar(4000) null,
                             urls nvarchar(4000) null
                             );

DECLARE @DetectedTip table (
                           tip_id smallint not null primary key,
                           details nvarchar(max) null
                           );

DECLARE @ReturnAllTips bit = 1, -- Debug flag to return all tips regardless of database state
        @HighLogRateThresholdPercent decimal(5,2) = 80;

SET NOCOUNT ON;

-- Define all tips
INSERT INTO @TipDefinition (tip_id, tip_name, tip_description, sample_command, urls)
VALUES
(1000, 'Excessive MAXDOP on all replicas', 'Maximum degree of parallelism for primary and secondary replicas is not in the recommended range of 1 to 8. Depending on workload, this may cause unnecessary resource utilization.', 'ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = <1-8>; ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET MAXDOP = PRIMARY;', 'https://aka.ms/sqldbtips-1000'),
(1010, 'Excessive MAXDOP on primary', 'Maximum degree of parallelism for the primary replica is not in the recommended range of 1 to 8. Depending on workload, this may cause unnecessary resource utilization.', 'ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = <1-8>;', 'https://aka.ms/sqldbtips-1010'),
(1020, 'Excessive MAXDOP on secondaries', 'Maximum degree of parallelism for secondary replicas is not in the recommended range of 1 to 8. Depending on workload, this may cause unnecessary resource utilization.', 'ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET MAXDOP = <1-8>;', 'https://aka.ms/sqldbtips-1020'),
(1030, 'Older compatibility level', 'Database compatibility level is not current. Some recent features and improvements are not available in this database. Consider using latest supported compatibility level, but note that changing compatibility level may require testing.', 'ALTER DATABASE CURRENT SET COMPATIBILITY_LEVEL = <N>;', 'https://aka.ms/sqldbtips-1030'),
(1040, 'Disabled auto-create stats', 'Auto-create statistics is disabled. This causes sub-optimal query plans. Enable auto-create statistics.', 'ALTER DATABASE CURRENT SET AUTO_CREATE_STATISTICS ON;', 'https://aka.ms/sqldbtips-1040'),
(1050, 'Disabled auto-update stats', 'Auto-update statistics is disabled. This causes sub-optimal query plans. Enable auto-update statistics.', 'ALTER DATABASE CURRENT SET AUTO_UPDATE_STATISTICS ON;', 'https://aka.ms/sqldbtips-1050'),
(1060, 'Disabled RCSI', 'Read Committed Snapshot Isolation (RCSI) is disabled. This may cause unnecessary lock blocking for DML queries.', 'ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON;', 'https://aka.ms/sqldbtips-1060'),
(1070, 'Disabled Query Store', 'Query Store is disabled. This complicates performance troubleshooting and disables some auto-tuning features.', 'ALTER DATABASE CURRENT SET QUERY_STORE = ON;', 'https://aka.ms/sqldbtips-1070'),
(1071, 'Read-only Query Store', 'Query Store is read-only. This complicates performance troubleshooting and disables some auto-tuning features.', 'ALTER DATABASE CURRENT SET QUERY_STORE (OPERATION_MODE = READ_WRITE);', 'https://aka.ms/sqldbtips-1071'),
(1072, 'Query Store capture mode is NONE', 'Query Store does not capture new queries. This complicates performance troubleshooting and disables some auto-tuning features.', 'ALTER DATABASE CURRENT SET QUERY_STORE (QUERY_CAPTURE_MODE = AUTO);', 'https://aka.ms/sqldbtips-1072'),
(1080, 'Enabled AUTO_SHRINK', 'While shrinking a database may be required in response to one-time data growth events and/or to occasionally reduce allocated space, it should not be executed continuously by enabling auto-shrink. Auto-shrink causes persistent resource utilization that will impact regular database workloads.', 'ALTER DATABASE CURRENT SET AUTO_SHRINK OFF;', 'https://aka.ms/sqldbtips-1080'),
(1090, 'PAGE_VERIFY is not CHECKSUM', 'The PAGE_VERIFY database option is not set to CHECKSUM. Using CHECKSUM is recommeded for better data integrity protection.', NULL, 'https://aka.ms/sqldbtips-1090'),
(1100, 'GUID leading columns in btree indexes', 'The "details" column contains a list of btree indexes with leading columns of data type uniqueidentifier (GUID) for larger objects. Such indexes are subject to high fragmentation and low page density as data is modified. This leads to increased disk space and memory usage. Avoid this pattern in physical database design, particularly for clustered btree indexes. To increase page density and release space, rebuild indexes.', null, 'https://aka.ms/sqldbtips-1100'),
(1110, 'Disabled FLGP auto-tuning', 'The FORCE_LAST_GOOD_PLAN auto-tuning option is not enabled. Query plan regressions will not be fixed automatically via plan forcing.', 'ALTER DATABASE CURRENT SET AUTOMATIC_TUNING (FORCE_LAST_GOOD_PLAN = ON);', 'https://aka.ms/sqldbtips-1110'),
(1120, 'Used space close to MAXSIZE', 'Used data space within the database is close to maximum configured database size. To allow continued data growth, increase maximum database size, or scale up to a service tier or service objective that supports higher maximum database size, or implement data compression, or delete unneeded data.', 'ALTER DATABASE <database_name> MODIFY (MAXSIZE = <N> GB);', 'https://aka.ms/sqldbtips-1120'),
(1130, 'Allocated space close to MAXSIZE', 'Space allocated for data files is close to maximum configured database size. If used space is not close to maximum configured database size and significant used space growth is not expected, consider shrinking data files to reduce allocated space.', 'DBCC SHRINKFILE (<file_id>, TRUNCATEONLY); DBCC SHRINKFILE (<file_id>, <allocated_space_target_in_mb>);', 'https://aka.ms/sqldbtips-1130'),
(1140, 'Allocated space much larger than used space', 'Space allocated for data files is much larger than used data space. If significant used space growth is not expected, consider shrinking data files to reduce allocated space.', 'DBCC SHRINKFILE (<file_id>, TRUNCATEONLY); DBCC SHRINKFILE (<file_id>, <allocated_space_target_in_mb>);', 'https://aka.ms/sqldbtips-1140'),
(1150, 'Recent CPU throttling', 'Significant CPU throttling has recently occurred, as noted in the "details" column. If workload performance is insufficient, tune query workload to consume less CPU, or scale up to a service objective with more CPU capacity, or both.', NULL, 'https://aka.ms/sqldbtips-1150'),
(1160, 'Recent out of memory errors', 'Out of memory errors have recently occurred, as noted in the "details" column. Tune query workload to consume less memory, or scale up to a service objective with more memory, or both.', NULL, 'https://aka.ms/sqldbtips-1160'),
(1170, 'Low reads nonclustered indexes', 'The "details" column contains a list of nonclustered indexes where the number of read operations is much less than the number of write (index update) operations. As data changes, indexes are updated, taking time and resources. The resource overhead of updating indexes with few reads may be higher than their benefit. If the data in "details" is for a sufficiently long period that covers infrequent workloads, consider dropping these indexes.', NULL, 'https://aka.ms/sqldbtips-1170'),
(1180, 'Data compression opportunities', 'The "details" column contains a list of objects, indexes, and partitions showing their suggested new data compression type, based on recent workload sampling and heuristics. To improve suggestion accuracy, obtain this result while a representative workload is running, or shortly thereafter.', NULL, 'https://aka.ms/sqldbtips-1180'),
(1190, 'Log rate close to limit', 'There are recent occurrences of log rate approaching the limit of the service objective, as noted in the "details" column. To improve performance of bulk data modifications including data loading, consider tuning the workload to reduce log rate, or consider scaling to a service objective with a higher maximum log rate.', NULL, 'https://aka.ms/sqldbtips-1190')
;

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
INSERT INTO @DetectedTip (tip_id)
SELECT 1070 AS tip_id
FROM sys.database_query_store_options
WHERE actual_state_desc = 'OFF'
UNION
SELECT 1071 AS tip_id
FROM sys.database_query_store_options
WHERE actual_state_desc = 'READ_ONLY'
UNION
SELECT 1072 AS tip_id
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

-- Page verify
INSERT INTO @DetectedTip (tip_id)
SELECT 1090 AS tip_id
FROM sys.databases
WHERE name = DB_NAME()
      AND
      page_verify_option_desc <> 'CHECKSUM'
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
       STRING_AGG(CAST(CONCAT('schema: ', QUOTENAME(OBJECT_SCHEMA_NAME(o.object_id)) COLLATE DATABASE_DEFAULT, ', object: ', QUOTENAME(o.name) COLLATE DATABASE_DEFAULT, ', index: ', QUOTENAME(i.name) COLLATE DATABASE_DEFAULT, ', type: ', i.type_desc COLLATE DATABASE_DEFAULT) AS nvarchar(max)), CONCAT(CHAR(13), CHAR(10))) AS details
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
      os.object_size_mb > 1024 -- consider larger tables only
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
HAVING SUM(CAST(FILEPROPERTY(name, 'SpaceUsed') AS bigint) * 8 / 1024.) > 0.8 * CAST(DATABASEPROPERTYEX(DB_NAME(), 'MaxSizeInBytes') AS bigint) / 1024. / 1024 -- used space > 80% of db maxsize
;

-- Allocated space close to maxsize
INSERT INTO @DetectedTip (tip_id)
SELECT 1130 AS tip_id
FROM sys.database_files
WHERE type_desc = 'ROWS'
      AND
      CAST(DATABASEPROPERTYEX(DB_NAME(), 'MaxSizeInBytes') AS bigint) <> -1 -- not applicable to Hyperscale
HAVING SUM(CAST(size AS bigint) * 8 / 1024.) > 0.8 * CAST(DATABASEPROPERTYEX(DB_NAME(), 'MaxSizeInBytes') AS bigint) / 1024. / 1024 -- allocated space > 80% of db maxsize
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
WHERE used_space * 8 / 1024. > 10240 -- 10 GB and higher, not relevant for small databases
      AND
      0.5 * allocated_space > used_space -- allocated space is more than 2x used space
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
       CONCAT('In the last ', recent_history_duration_minutes, ' minutes, there were ', count_cpu_delayed_intervals, ' occurrences of CPU throttling. On average, CPU was throttled by ', avg_cpu_delay_percent, '%.') AS details
FROM cpu_throttling
WHERE avg_cpu_delay_percent > 20
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
       CONCAT('In the last ', recent_history_duration_minutes, ' minutes, there were ', count_oom, ' out of memory errors.') AS details
FROM oom
WHERE count_oom > 0
;

-- Little used nonclustered indexes
WITH index_usage AS
(
SELECT STRING_AGG(CONCAT(QUOTENAME(OBJECT_SCHEMA_NAME(o.object_id)) COLLATE DATABASE_DEFAULT, '.', QUOTENAME(o.name) COLLATE DATABASE_DEFAULT, '.', QUOTENAME(i.name) COLLATE DATABASE_DEFAULT, ' (reads: ', ius.user_seeks + ius.user_scans + ius.user_lookups, ' writes: ', ius.user_updates, ')'), CONCAT(CHAR(13), CHAR(10))) AS details
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
      (ius.user_seeks + ius.user_scans + ius.user_lookups) * 0.5 < ius.user_updates
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
partition_stats AS
(
SELECT o.object_id,
       i.name AS index_name,
       p.partition_number,
       ios.leaf_update_count / NULLIF((ios.range_scan_count + ios.leaf_insert_count + ios.leaf_delete_count + ios.leaf_update_count + ios.leaf_page_merge_count + ios.singleton_lookup_count), 0) AS update_ratio,
       ios.range_scan_count / NULLIF((ios.range_scan_count + ios.leaf_insert_count + ios.leaf_delete_count + ios.leaf_update_count + ios.leaf_page_merge_count + ios.singleton_lookup_count), 0) AS scan_ratio
FROM sys.objects AS o
INNER JOIN sys.indexes AS i
ON o.object_id = i.object_id
INNER JOIN sys.partitions AS p
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
),
partition_compression AS
(
SELECT ps.object_id,
       ps.index_name,
       ps.partition_number,
       CASE WHEN -- do not choose page compression when no index stats are available and update_ratio and scan_ratio are NULL due to low confidence
                 (
                 ps.update_ratio < 0.2 -- infrequently updated
                 OR 
                 (ps.update_ratio BETWEEN 0.2 AND 0.5 AND ps.scan_ratio > 0.5) -- more frequently updated but also frequently scanned
                 ) 
                 AND 
                 rcu.avg_cpu_percent < 60 -- there is ample CPU headroom
                 AND 
                 rcu.recent_cpu_minutes > 30 -- there is a sufficient number of CPU usage stats
            THEN 'PAGE'
            WHEN rcu.avg_cpu_percent < 80 -- there is some CPU headroom
                 AND 
                 rcu.recent_cpu_minutes > 30 -- there is a sufficient number of CPU usage stats
            THEN 'ROW'
            WHEN rcu.avg_cpu_percent > 80 -- there is no CPU headroom, can't use compression
                 AND 
                 rcu.recent_cpu_minutes > 30 -- there is a sufficient number of CPU usage stats
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
                 CAST(CONCAT('schema: ', QUOTENAME(OBJECT_SCHEMA_NAME(object_id)), ', object: ', QUOTENAME(OBJECT_NAME(object_id)), ', index: ', QUOTENAME(index_name), ', partition range: ', partition_range, ', new compression type: ', new_compression_type) AS nvarchar(max)), CONCAT(CHAR(13), CHAR(10))
                 ) WITHIN GROUP (ORDER BY object_id, index_name, partition_range, new_compression_type)
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
       SUM(high_log_rate_indicator) OVER (ORDER BY end_time ROWS UNBOUNDED PRECEDING) -- running sum of all intervals where log rate exceeded the threshold
       AS grouping_helper -- this difference remains constant while log rate is above the threshold
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
       CONCAT('In the last hour, there were  ', count_high_log_write_intervals, ' intervals with log rate staying above ', @HighLogRateThresholdPercent, '%. The longest such interval lasted ', top_log_rate_duration_seconds, ' seconds, and the highest log rate was ', top_log_write_percent, '%.') AS details
FROM log_rate_top_stat 
WHERE count_high_log_write_intervals > 0
;

-- Return detected tips
SELECT td.tip_id,
       td.tip_name,
       td.tip_description,
       td.sample_command,
       td.urls,
       dt.details
FROM @TipDefinition AS td
LEFT JOIN @DetectedTip AS dt
ON dt.tip_id = td.tip_id
WHERE dt.tip_id IS NOT NULL
      OR
      @ReturnAllTips = 1
ORDER BY tip_id;

END TRY
BEGIN CATCH
    THROW;
END CATCH;

