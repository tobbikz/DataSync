#include "host_metrics.hpp"

#include <fstream>
#include <sstream>
#include <string>

#if defined(__linux__)
#include <malloc.h>
#endif

namespace {

bool read_process_rss_kb(long long& rss_kb) {
    std::ifstream in("/proc/self/status");
    if (!in) {
        return false;
    }
    std::string line;
    while (std::getline(in, line)) {
        if (line.rfind("VmRSS:", 0) == 0) {
            std::istringstream iss(line.substr(6));
            iss >> rss_kb;
            return rss_kb > 0;
        }
    }
    return false;
}

bool read_cpu_jiffies(long long& total, long long& idle) {
    std::ifstream in("/proc/stat");
    if (!in) {
        return false;
    }
    std::string line;
    if (!std::getline(in, line) || line.rfind("cpu ", 0) != 0) {
        return false;
    }
    std::istringstream iss(line);
    std::string label;
    long long user = 0;
    long long nice = 0;
    long long system = 0;
    long long idle_v = 0;
    long long iowait = 0;
    long long irq = 0;
    long long softirq = 0;
    long long steal = 0;
    iss >> label >> user >> nice >> system >> idle_v >> iowait >> irq >> softirq >> steal;
    if (!iss) {
        return false;
    }
    total = user + nice + system + idle_v + iowait + irq + softirq + steal;
    idle = idle_v + iowait;
    return total > 0;
}

bool read_mem_kb(long long& total_kb, long long& avail_kb) {
    std::ifstream in("/proc/meminfo");
    if (!in) {
        return false;
    }
    std::string line;
    bool got_total = false;
    bool got_avail = false;
    while (std::getline(in, line)) {
        if (line.rfind("MemTotal:", 0) == 0) {
            std::istringstream iss(line.substr(9));
            iss >> total_kb;
            got_total = true;
        } else if (line.rfind("MemAvailable:", 0) == 0) {
            std::istringstream iss(line.substr(13));
            iss >> avail_kb;
            got_avail = true;
        }
        if (got_total && got_avail) {
            return total_kb > 0;
        }
    }
    return false;
}

bool read_net_bytes(long long& rx_bytes, long long& tx_bytes) {
    std::ifstream in("/proc/net/dev");
    if (!in) {
        return false;
    }
    std::string line;
    std::getline(in, line);
    std::getline(in, line);
    rx_bytes = 0;
    tx_bytes = 0;
    while (std::getline(in, line)) {
        if (line.find(':') == std::string::npos) {
            continue;
        }
        const auto colon = line.find(':');
        std::string iface = line.substr(0, colon);
        while (!iface.empty() && iface.front() == ' ') {
            iface.erase(iface.begin());
        }
        if (iface == "lo") {
            continue;
        }
        std::istringstream iss(line.substr(colon + 1));
        long long rx = 0;
        long long tx = 0;
        long long dummy = 0;
        iss >> rx;
        for (int i = 0; i < 7; ++i) {
            iss >> dummy;
        }
        iss >> tx;
        if (!iss) {
            continue;
        }
        rx_bytes += rx;
        tx_bytes += tx;
    }
    return true;
}

}  // namespace

long long process_rss_mb() {
    long long rss_kb = 0;
    if (!read_process_rss_kb(rss_kb)) {
        return -1;
    }
    return rss_kb / 1024;
}

void trim_process_heap() {
#if defined(__linux__)
    malloc_trim(0);
#endif
}

void HostMetricsSampler::mark_start() {
    start_ = {};
    start_.valid = read_cpu_jiffies(start_.cpu_total, start_.cpu_idle) &&
                   read_mem_kb(start_.mem_total_kb, start_.mem_avail_kb) &&
                   read_net_bytes(start_.net_rx_bytes, start_.net_tx_bytes);
    read_process_rss_kb(start_.process_rss_kb);
}

HostMetricsSlice HostMetricsSampler::current_snapshot() const {
    HostMetricsSlice out;
    if (!start_.valid) {
        return out;
    }
    Sample now{};
    now.valid = read_cpu_jiffies(now.cpu_total, now.cpu_idle) && read_mem_kb(now.mem_total_kb, now.mem_avail_kb) &&
                read_net_bytes(now.net_rx_bytes, now.net_tx_bytes);
    read_process_rss_kb(now.process_rss_kb);
    if (!now.valid) {
        return out;
    }
    const long long cpu_delta = now.cpu_total - start_.cpu_total;
    const long long idle_delta = now.cpu_idle - start_.cpu_idle;
    if (cpu_delta > 0) {
        out.host_cpu_percent =
            static_cast<double>(cpu_delta - idle_delta) * 100.0 / static_cast<double>(cpu_delta);
    }
    if (now.process_rss_kb > 0) {
        out.host_mem_used_mb = now.process_rss_kb / 1024;
        out.process_rss_mb = out.host_mem_used_mb;
        if (now.mem_total_kb > 0) {
            out.host_mem_percent =
                static_cast<int>((now.process_rss_kb * 100) / now.mem_total_kb);
        }
    }
    const long long rx_delta = now.net_rx_bytes - start_.net_rx_bytes;
    const long long tx_delta = now.net_tx_bytes - start_.net_tx_bytes;
    if (rx_delta >= 0) {
        out.host_net_rx_mb = rx_delta / (1024 * 1024);
    }
    if (tx_delta >= 0) {
        out.host_net_tx_mb = tx_delta / (1024 * 1024);
    }
    return out;
}
