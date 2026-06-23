#pragma once

#include <cstddef>
#include <string_view>

namespace pipeline_defaults {

// Worker / parallelism (hardcoded)
constexpr int kApplyWorkerCount = 8;
/** Dedicated apply consumers per conn for catalog.hot tables (per_table Kafka topic). */
constexpr int kHotApplyConsumerCount = 3;
constexpr int kCaptureWorkerCount = 1;
constexpr int kFullLoadParallelTables = 2;
constexpr int kFullLoadWorkers = 4;

// Kafka
constexpr std::string_view kKafkaBootstrapDefault = "localhost:9092";
constexpr std::string_view kKafkaConsumerGroupPrefix = "datalake-cdc-apply";
constexpr std::string_view kKafkaTopicMode = "bucketed";
constexpr int kKafkaTopicBuckets = 64;
/** Must be a multiple of kApplyWorkerCount and kHotApplyConsumerCount for partition sharding. */
constexpr int kKafkaTopicPartitions = 24;

// Capture producer / slice
constexpr int kCaptureProducerLingerMs = 5;
constexpr int kCaptureProducerBatchSize = 10000;
constexpr int kCaptureProducerQueueMaxMessages = 500000;
constexpr int kCaptureProducerQueueMaxKbytes = 1048576;
constexpr int kCaptureIdlePollSeconds = 3;
constexpr int kCaptureQuietExitLaggingChunks = 3;
constexpr int kCaptureHeartbeatSeconds = 60;
constexpr int kCaptureMaxEventsDefault = 2000000;
constexpr bool kMssqlCaptureReplayOnIdle = false;

// Apply slice
constexpr int kApplyPollTimeoutMs = 100;
constexpr int kApplyFetchMaxBytes = 52428800;
constexpr int kApplyMaxPartitionFetchBytes = 10485760;
constexpr int kApplyEmptyPollQuietThreshold = 3;
constexpr int kApplyMaxTableStalenessSeconds = 900;
constexpr int kApplyInactiveSeconds = 3600;
constexpr int kApplyBatchStatsRetentionDays = 30;
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
constexpr int kLakePartitionMonthsAhead = 3;
constexpr int kMariadbReconnectMaxAttempts = 0;
constexpr int kMariadbReconnectBaseMs = 500;
constexpr int kMariadbReconnectMaxMs = 60000;

// DDL
constexpr bool kDdlSyncColumns = true;
constexpr std::size_t kDdlSyncSampleSize = 1000;

// Catalog discover
constexpr int kCatalogSyncIntervalRounds = 12;
constexpr std::size_t kCatalogChunkSize = 500;
constexpr int kCatalogBatchSleepMs = 200;

// Reconcile
constexpr int kReconcileRowAbsTolerance = 50;
constexpr double kReconcileRowPctTolerance = 0.01;
constexpr int kReconcileRowWarnAbsTolerance = 10;
constexpr double kReconcileRowWarnPctTolerance = 0.005;
constexpr long long kReconcileLargeTableMinRows = 100000;
constexpr int kReconcileLargeTableAbsCap = 5000;
constexpr int kReconcileApplyLagWarnSeconds = 600;
constexpr int kReconcileApplyLagFailSeconds = 1800;
constexpr int kReconcilePkSampleSize = 100;
constexpr int kReconcileMaxTables = 0;
constexpr bool kReconcileEnabled = true;
constexpr long long kReconcileKafkaLagWarn = 100;
constexpr long long kReconcileKafkaLagFail = 10000;
constexpr int kReconcileCaptureLagWarnSeconds = 300;
constexpr int kReconcileCaptureLagFailSeconds = 900;

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
constexpr int kReconcileIntervalHoursDefault = 4;
constexpr int kLogsRetentionDaysDefault = 7;
constexpr int kAppliedEventsRetentionDaysDefault = 7;

}  // namespace pipeline_defaults
