#pragma once

#include <optional>

struct HostMetricsSlice {
    double host_cpu_percent{-1};
    long long host_mem_used_mb{-1};
    int host_mem_percent{-1};
    long long host_net_rx_mb{-1};
    long long host_net_tx_mb{-1};
    long long process_rss_mb{-1};
};

/** Process RSS in MB from /proc/self/status (VmRSS). Returns -1 if unavailable. */
long long process_rss_mb();

/** Hint glibc to return freed heap pages to the OS (Linux only). */
void trim_process_heap();

/** Samples /proc (Linux) from mark_start through current_snapshot(). */
class HostMetricsSampler {
public:
    void mark_start();
    HostMetricsSlice current_snapshot() const;

private:
    struct Sample {
        long long cpu_total{0};
        long long cpu_idle{0};
        long long mem_total_kb{0};
        long long mem_avail_kb{0};
        long long net_rx_bytes{0};
        long long net_tx_bytes{0};
        long long process_rss_kb{0};
        bool valid{false};
    };

    Sample start_{};
};
