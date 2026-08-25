#pragma once

#include <cstddef>
#include <string_view>

namespace pipeline_defaults {

// Worker / parallelism (hardcoded; 24 Kafka partitions must divide evenly)
constexpr int kApplyWorkerCount = 4;  // 24 = 4 × 6 partitions each
constexpr int kCaptureWorkerCount = 1;
constexpr int kFullLoadParallelTables = 2;
constexpr int kFullLoadWorkers = 4;
/** MSSQL/FreeTDS: fewer concurrent connections than MariaDB to avoid source hangs. */
constexpr int kMssqlFullLoadParallelTables = 1;
constexpr int kMssqlFullLoadWorkers = 2;
constexpr int kMssqlFullLoadCopyProgressInterval = 10;
/**
 * Intra-collection copy workers, split by _id range. Lower than the MariaDB count because
 * each one holds its own MongoDB cursor and lake connection for the whole collection.
 */
constexpr int kMongoFullLoadWorkers = 4;
/**
 * Composite / non-integer PKs are split by sampling boundary rows along the PK index,
 * which costs about source_rows × (workers - 1) / 2 index entries. Below this row count
 * the scan is not worth it and the table is copied by a single worker.
 */
constexpr long long kFullLoadSliceSampleMinRows = 100000;

// Kafka
constexpr std::string_view kKafkaBootstrapDefault = "localhost:9092";
constexpr std::string_view kKafkaConsumerGroupPrefix = "datalake-cdc-apply";
constexpr std::string_view kKafkaTopicMode = "bucketed";
constexpr int kKafkaTopicBuckets = 64;
constexpr int kKafkaTopicPartitions = 24;

// Capture producer / slice
constexpr int kCaptureProducerLingerMs = 5;
constexpr int kCaptureProducerBatchSize = 10000;
constexpr int kCaptureProducerQueueMaxMessages = 500000;
constexpr int kCaptureProducerQueueMaxKbytes = 1048576;  // 1 GB
constexpr int kCaptureIdlePollSeconds = 3;
constexpr int kCaptureQuietExitLaggingChunks = 3;
constexpr int kCaptureHeartbeatSeconds = 60;
/** Min age before clearing stuck cdc_in_progress rows (seconds). */
constexpr int kCdcInProgressStaleSeconds = 300;
constexpr int kCaptureMaxEventsDefault = 2000000;
constexpr bool kMssqlCaptureReplayOnIdle = false;

// Apply slice
constexpr int kApplyPollTimeoutMs = 100;
constexpr int kApplyFetchMaxBytes = 52428800;           // 50 MB
constexpr int kApplyMaxPartitionFetchBytes = 10485760;  // 10 MB
constexpr int kApplyEmptyPollQuietThreshold = 3;
constexpr int kApplyMaxTableStalenessSeconds = 900;
constexpr int kApplyInactiveSeconds = 3600;
constexpr int kApplyBatchStatsRetentionDays = 3;
/** Hourly prune (America/Costa_Rica hour bucket). No pg_cron / no Postgres restart. */
constexpr long long kRetentionMaintenanceAdvisoryLockKey = 90420055001LL;
/** Small batches + one commit per C++ call — short statements under scrapers. */
constexpr int kApplyBatchStatsPruneBatchSizeDefault = 500;
constexpr int kLogsPurgeBatchSizeDefault = 500;
/** Cap per prune per hourly run (stats idle+age share this; logs separate). 100k → 200 batches. */
constexpr int kRetentionPruneMaxRowsPerRun = 1000000;
constexpr int kApplyBatchStatsPruneMaxBatchesDefault =
    kRetentionPruneMaxRowsPerRun / kApplyBatchStatsPruneBatchSizeDefault;
constexpr int kLogsPurgeMaxBatchesDefault =
    kRetentionPruneMaxRowsPerRun / kLogsPurgeBatchSizeDefault;
/** Pause between single-batch prune commits so apply/upsert can proceed. */
constexpr int kRetentionPruneBatchPauseMs = 50;
/** Session-independent xact lock key inside cdc_catalog.purge_logs_batched. */
constexpr long long kLogsPurgeAdvisoryLockKey = 90420057001LL;
/** pg_advisory_xact_lock class for apply_position upsert (key2 = hashtext object uk). */
constexpr int kApplyPositionUpsertLockClass = 90420059;
constexpr int kApplyPositionUpsertMaxAttempts = 3;
/** Retries for catalog row UPDATEs on deadlock (40P01); same call frequency, transient backoff. */
constexpr int kCatalogUpdateDeadlockMaxAttempts = 5;
constexpr int kCatalogUpdateDeadlockBaseSleepMs = 25;
/** Kafka offset + lake business-PK delete+insert; no cdc_applied_events ledger. */
constexpr bool kApplyDedupEnabled = false;
constexpr bool kApplyAuditEnabled = false;
constexpr int kApplyQueuedMinMessages = 100000;
constexpr int kApplyFetchWaitMaxMs = 500;
constexpr int kApplyMaxEventsDefault = 2000000;

// Full load
constexpr int kFullLoadMaxFailRetries = 5;
constexpr int kFullLoadFailedCooldownMinutes = 240;
constexpr int kFullLoadSourceSleepMs = 0;
constexpr int kFullLoadStaleInProgressMinutes = 30;
constexpr int kMssqlFullLoadStaleInProgressMinutes = 10;
/** Max wall time for daemon fork+exec `DataSync full-load --conn-id` subprocess (0 = disabled). */
constexpr int kFullLoadDaemonSubprocessTimeoutMinutes = 0;
constexpr int kPgFullLoadReconnectMaxAttempts = 0;
constexpr int kPgFullLoadReconnectBaseMs = 500;
constexpr int kPgFullLoadReconnectMaxMs = 60000;
constexpr int kFullLoadTruncateMaxRetries = 3;
constexpr int kFullLoadCopyProgressLogInterval = 10;
constexpr bool kFullLoadRowCountVerify = true;
constexpr long long kFullLoadRowCountVerifyLargeTableThreshold = 10000000LL;
constexpr double kFullLoadRowCountVerifyTolerancePct = 0.0001;
/** Max lake rows above live source before streaming verify fails (duplicate-load guard). */
constexpr double kFullLoadStreamingVerifyMaxAboveLivePct = 0.05;
constexpr int kLakePartitionMonthsAhead = 3;
constexpr int kMariadbReconnectMaxAttempts = 0;
constexpr int kMariadbReconnectBaseMs = 500;
constexpr int kMariadbReconnectMaxMs = 60000;
constexpr int kMssqlReconnectMaxAttempts = 0;
constexpr int kMssqlReconnectBaseMs = 500;
constexpr int kMssqlReconnectMaxMs = 60000;

// DDL
constexpr bool kDdlSyncColumns = true;
constexpr bool kDdlSyncIndexes = true;
constexpr bool kDdlSyncForeignKeys = true;
constexpr std::size_t kDdlSyncSampleSize = 1000;

// Catalog discover
constexpr int kCatalogSyncIntervalRounds = 12;
constexpr std::size_t kCatalogChunkSize = 500;
constexpr int kCatalogBatchSleepMs = 200;
constexpr std::size_t kCatalogFetchPageSize = 2000;
constexpr std::size_t kCatalogDiscoverPageSize = 5000;

/** Partition kafka lag (high watermark − offset) at/above this → apply_health_rag AMBER. */
constexpr long long kKafkaConsumerLagWarnMessages = 1000;
/** Partition kafka lag at/above this → apply_health_rag RED. */
constexpr long long kKafkaConsumerLagRedMessages = 50000;

/** Capture position silent this long → status stale (Health DB, not logs). */
constexpr int kCaptureHealthAlertStaleSeconds = 300;

// Apply / full-load batch sizes (smaller = less RAM per COPY, more commits)
constexpr std::size_t kFullLoadBatchSizeDefault = 20000;
constexpr int kApplyBatchSizeDefault = 8000;
/** Max PK rows per DELETE statement when applying CDC deletes to mirror tables. */
constexpr std::size_t kApplyDeleteChunkSizeDefault = 2000;
/** Per-statement cap on lake PG apply connections (0 = disabled). */
constexpr int kApplyLakeStatementTimeoutMsDefault = 600000;  // 10 min
/** Roll back idle apply transactions on lake PG (0 = disabled). */
constexpr int kApplyLakeIdleInTxnTimeoutMsDefault = 600000;
/** Lake apply: disable synchronous_commit for faster WAL flush. */
constexpr bool kApplyLakeSynchronousCommitOffDefault = true;
/** Cap sort/hash RAM per apply backend (N workers × 2 DBs). */
constexpr const char* kApplyWorkMem = "16MB";
constexpr const char* kApplyTempBuffers = "16MB";
constexpr int kLogsRetentionDaysDefault = 3;

}  // namespace pipeline_defaults
