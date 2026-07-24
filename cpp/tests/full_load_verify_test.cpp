#include "full_load_common.hpp"
#include "pipeline_defaults.hpp"

#include <iostream>
#include <string>

namespace {

int fail_msg(const std::string& msg) {
    std::cerr << "FAIL " << msg << "\n";
    return 1;
}

full_load::RowCountVerifyRequest req(
    long long baseline,
    long long lake,
    long long live,
    bool streaming) {
    full_load::RowCountVerifyRequest r;
    r.baseline_source_rows = baseline;
    r.lake_rows = lake;
    r.source_rows_live = live;
    r.capture_during_full_load = streaming;
    r.rows_loaded = lake;
    return r;
}

}  // namespace

int main() {
    int failures = 0;

    // Prod incident: lake slightly above baseline with CDC concurrent → must PASS
    {
        const auto v = full_load::verify_full_load_row_counts(req(796323809, 798526086, 799000000, true));
        if (!v.ok) {
            failures += fail_msg("streaming verify should pass when lake > baseline within live bound");
        }
        if (v.verify_mode != "baseline_snapshot_streaming") {
            failures += fail_msg("expected baseline_snapshot_streaming mode");
        }
    }

    // Incomplete COPY: lake below baseline → FAIL
    {
        const auto v = full_load::verify_full_load_row_counts(req(796000000, 700000000, 798000000, true));
        if (v.ok) {
            failures += fail_msg("streaming verify should fail when lake below baseline");
        }
    }

    // Duplicate guard: lake far above live source → FAIL
    {
        const auto v =
            full_load::verify_full_load_row_counts(req(796000000, 1399219596, 798000000, true));
        if (v.ok) {
            failures += fail_msg("streaming verify should fail on duplicate-scale lake rows");
        }
    }

    // Non-streaming baseline still uses symmetric tolerance path
    {
        full_load::RowCountVerifyRequest r;
        r.baseline_source_rows = 100;
        r.lake_rows = 100;
        r.source_rows_live = 100;
        r.capture_during_full_load = false;
        r.rows_loaded = 100;
        const auto v = full_load::verify_full_load_row_counts(r);
        if (!v.ok) {
            failures += fail_msg("non-streaming exact small table verify");
        }
    }

    if (failures == 0) {
        std::cout << "full_load_verify_test: ok\n";
    }
    return failures;
}
