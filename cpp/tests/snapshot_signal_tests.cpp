#include "snapshot_signal.hpp"

#include <cstdlib>
#include <iostream>

static int g_failures = 0;

#define EXPECT(cond)                                                                 \
    do {                                                                             \
        if (!(cond)) {                                                               \
            std::cerr << "FAIL " << __FILE__ << ":" << __LINE__ << " " #cond << '\n'; \
            ++g_failures;                                                            \
        }                                                                            \
    } while (0)

int main() {
    using namespace snapshot_signal;

    EXPECT(action_to_string(Action::ExecuteIncremental) == "execute_incremental");
    EXPECT(action_to_string(Action::ExecuteBlocking) == "execute_blocking");
    EXPECT(action_to_string(Action::Pause) == "pause");
    EXPECT(action_to_string(Action::Resume) == "resume");
    EXPECT(action_to_string(Action::Stop) == "stop");
    EXPECT(action_to_string(Action::ReloadConfig) == "reload_config");
    EXPECT(action_to_string(Action::Log) == "log");
    EXPECT(action_to_string(Action::Notify) == "notify");

    EXPECT(action_from_string("pause").has_value());
    EXPECT(*action_from_string("stop") == Action::Stop);
    EXPECT(*action_from_string("reload_config") == Action::ReloadConfig);
    EXPECT(*action_from_string("notify") == Action::Notify);
    EXPECT(!action_from_string("nope").has_value());

    EXPECT(status_to_string(Status::Pending) == "pending");
    EXPECT(status_from_string("running").has_value());
    EXPECT(*status_from_string("completed") == Status::Completed);
    EXPECT(!status_from_string("bogus").has_value());

    if (g_failures) {
        std::cerr << g_failures << " failure(s)\n";
        return 1;
    }
    std::cout << "snapshot_signal_tests OK\n";
    return 0;
}
