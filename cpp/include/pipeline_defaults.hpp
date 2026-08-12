#pragma once

#include <cstddef>
#include <string_view>

namespace pipeline_defaults {

// Worker / parallelism (hardcoded; override via runtime_config apply_worker_count)
constexpr int kApplyWorkerCount = 12;
/** Dedicated apply consumers per conn for catalog.hot tables (per_table Kafka topic). */
constexpr int kHotApplyConsumerCount = 3;
constexpr int kCaptureWorkerCount = 1;
constexpr int kFullLoadParallelTables = 2;
constexpr int kFullLoadWorkers = 4;
/** MSSQL/FreeTDS: fewer concurrent connections than MariaDB to avoid source hangs. */
constexpr int kMssqlFullLoadParallelTables = 1;
constexpr int kMssqlFullLoadWorkers = 2;
constexpr int kMssqlFullLoadCopyProgressInterval = 10;

// Kafka
constexpr std::string_view kKafkaBootstrapDefault = "localhost:9092";
constexpr std::string_view kKafkaConsumerGroupPrefix = "datalake-cdc-apply";
constexpr std::string_view kKafkaTopicMode = "bucketed";
constexpr int kKafkaTopicBuckets = 64;
/** Must divide evenly by kApplyWorkerCount and kHotApplyConsumerCount (24 = 12 cold × 2, 24 = 3 hot × 8). */
constexpr int kKafkaTopicPartitions = 24;

// Capture producer / slice
constexpr int kCaptureProducerLingerMs = 5;
constexpr int kCaptureProducerBatchSize = 10000;
constexpr int kCaptureProducerQueueMaxMessages = 500000;
constexpr int kCaptureProducerQueueMaxKbytes = 1048576;
constexpr int kCaptureIdlePollSeconds = 3;
constexpr int kCaptureQuietExitLaggingChunks = 3;
constexpr int kCaptureHeartbeatSeconds = 60;
/** Min age before clearing stuck cdc_in_progress rows (seconds). */
constexpr int kCdcInProgressStaleSeconds = 300;
constexpr int kCaptureMaxEventsDefault = 2000000;
constexpr bool kMssqlCaptureReplayOnIdle = false;

// Apply slice
constexpr int kApplyPollTimeoutMs = 100;
constexpr int kApplyFetchMaxBytes = 52428800;
constexpr int kApplyMaxPartitionFetchBytes = 10485760;
constexpr int kApplyEmptyPollQuietThreshold = 3;
constexpr int kApplyMaxTableStalenessSeconds = 900;
constexpr int kApplyInactiveSeconds = 3600;
constexpr int kApplyBatchStatsRetentionDays = 3;
/** Scheduled retention prune: local hour (America/Costa_Rica) — 03:00 CST. */
constexpr int kRetentionMaintenanceLocalHour = 3;
constexpr int kRetentionMaintenanceAdvisoryLockKey = 90420055001;
/** Small batches + one commit per C++ call — short statements under scrapers. */
constexpr int kApplyBatchStatsPruneBatchSizeDefault = 500;
constexpr int kApplyBatchStatsPruneMaxBatchesDefault = 10000;
/** Dedup ledger is huge — higher throughput than logs/stats or backlog never drains. */
constexpr int kAppliedEventsPruneBatchSizeDefault = 5000;
constexpr int kAppliedEventsPruneMaxBatchesDefault = 50000;
constexpr int kLogsPurgeBatchSizeDefault = 500;
constexpr int kLogsPurgeMaxBatchesDefault = 10000;
/** Pause between single-batch prune commits so apply/upsert can proceed. */
constexpr int kRetentionPruneBatchPauseMs = 50;
/** Session-independent xact lock key inside cdc_catalog.purge_logs_batched. */
constexpr long long kLogsPurgeAdvisoryLockKey = 90420057001LL;
/** Session lock: one DataSync process runs startup schema migrate per deploy wave. */
constexpr long long kSchemaMigrateAdvisoryLockKey = 90420058001LL;
/** pg_advisory_xact_lock class for apply_position upsert (key2 = hashtext object uk). */
constexpr int kApplyPositionUpsertLockClass = 90420059;
constexpr int kApplyPositionUpsertMaxAttempts = 3;
/** Retries for catalog row UPDATEs on deadlock (40P01); same call frequency, transient backoff. */
constexpr int kCatalogUpdateDeadlockMaxAttempts = 5;
constexpr int kCatalogUpdateDeadlockBaseSleepMs = 25;
/** Discover orphan cleanup: max rows deleted from cdc_applied_events per batch. */
constexpr int kDiscoverOrphanAppliedEventsBatchSize = 500;
constexpr bool kApplyDedupEnabled = true;
constexpr bool kApplyAuditEnabled = true;
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
constexpr std::size_t kDdlSyncSampleSize = 1000;

// Catalog discover
constexpr int kCatalogSyncIntervalRounds = 12;
constexpr std::size_t kCatalogChunkSize = 500;
constexpr int kCatalogBatchSleepMs = 200;
constexpr std::size_t kCatalogFetchPageSize = 2000;
constexpr std::size_t kCatalogDiscoverPageSize = 5000;

// Apply health alerts (from apply_batch_stats / apply_health_rag — not reconcile)
constexpr int kApplyHealthAlertLookbackMinutes = 15;
/** Exact table kafka_consumer_lag at/above this → apply_health_rag AMBER (kafka_backlog). */
constexpr long long kKafkaConsumerLagWarnMessages = 1000;
/** Exact table kafka_consumer_lag at/above this → apply_health_rag RED (kafka_backlog_critical). */
constexpr long long kKafkaConsumerLagRedMessages = 50000;

// Capture health alerts (capture_position.updated_at staleness)
constexpr int kCaptureHealthAlertStaleSeconds = 300;
constexpr int kCaptureHealthAlertFailSeconds = 3600;

// Reconcile-lite (COUNT + MAX pk + MAX ts)
constexpr int kReconcileLiteRetentionDays = 30;
constexpr int kReconcileLiteTsLagWarnSeconds = 60;
constexpr int kReconcileLiteTsLagFailSeconds = 300;

// RuntimeConfig keys — defaults when absent in DB
constexpr std::size_t kFullLoadBatchSizeDefault = 50000;
constexpr int kApplyBatchSizeDefault = 20000;
constexpr int kHotApplyBatchSizeDefault = 30000;
/** Max PK rows per DELETE statement when applying CDC deletes to mirror tables. */
constexpr std::size_t kApplyDeleteChunkSizeDefault = 5000;
/** Per-statement cap on lake PG apply connections (0 = disabled). */
constexpr int kApplyLakeStatementTimeoutMsDefault = 300000;
/** Roll back idle apply transactions on lake PG (0 = disabled). */
constexpr int kApplyLakeIdleInTxnTimeoutMsDefault = 600000;
/** Lake apply: disable synchronous_commit for faster WAL flush (hot always; cold when true). */
constexpr bool kApplyLakeSynchronousCommitOffDefault = true;
/** Per-table exact lag scan timeout (inactive / quiet tables at slice end). */
constexpr int kTableLagScanTimeoutMsDefault = 120000;
/** 0 = unlimited messages scanned per table lag probe. */
constexpr long long kTableLagScanMaxMessagesDefault = 0;
/** End-of-slice lag-only drain polls after main apply loop. */
constexpr int kApplyLagDrainQuietPollsDefault = 5;
constexpr int kLogsRetentionDaysDefault = 3;
constexpr int kAppliedEventsRetentionDaysDefault = 3;

}  // namespace pipeline_defaults
