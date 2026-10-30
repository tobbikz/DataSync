#include <atomic>

/** Test-only definition of the daemon shutdown flag used by snapshot_signal. */
std::atomic<bool> g_shutdown{false};
