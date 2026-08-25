#include "full_load_slice.hpp"
#include "test_assert.hpp"

#include <string>
#include <vector>

using full_load::PkSlice;
using full_load::TupleOp;

namespace {

std::string ident(const std::string& name) {
    return "\"" + name + "\"";
}

std::string literal(std::size_t, const std::string& value) {
    return "'" + value + "'";
}

std::string clause(
    const std::vector<std::string>& cols,
    const std::vector<std::string>& values,
    TupleOp op) {
    return full_load::lexicographic_tuple_clause(cols, values, op, ident, literal);
}

int tuple_cmp(const std::vector<std::string>& a, const std::vector<std::string>& b) {
    const std::size_t n = std::min(a.size(), b.size());
    for (std::size_t i = 0; i < n; ++i) {
        if (a[i] < b[i]) {
            return -1;
        }
        if (a[i] > b[i]) {
            return 1;
        }
    }
    return 0;
}

bool slice_contains(const PkSlice& slice, const std::vector<std::string>& value) {
    if (slice.has_begin && tuple_cmp(value, slice.begin) < 0) {
        return false;
    }
    if (slice.has_end && tuple_cmp(value, slice.end) >= 0) {
        return false;
    }
    return true;
}

/** Every sample must land in exactly one slice: no gap, no overlap. */
void expect_exactly_one_slice(
    const std::vector<PkSlice>& slices,
    const std::vector<std::vector<std::string>>& samples,
    const char* label) {
    for (const auto& sample : samples) {
        int matches = 0;
        for (const auto& slice : slices) {
            if (slice_contains(slice, sample)) {
                matches += 1;
            }
        }
        expect_eq_int(matches, 1, label);
    }
}

}  // namespace

int main() {
    const std::vector<std::string> single = {"id"};
    const std::vector<std::string> composite = {"tenant", "id"};

    // Single column collapses to a plain comparison.
    expect_true(clause(single, {"10"}, TupleOp::Gt) == "((\"id\" > '10'))", "single gt");
    expect_true(clause(single, {"10"}, TupleOp::Ge) == "((\"id\" >= '10'))", "single ge");
    expect_true(clause(single, {"10"}, TupleOp::Lt) == "((\"id\" < '10'))", "single lt");
    expect_true(clause(single, {"10"}, TupleOp::Le) == "((\"id\" <= '10'))", "single le");

    // Composite expands lexicographically; only the deepest arm carries the equality.
    expect_true(
        clause(composite, {"acme", "10"}, TupleOp::Gt) ==
            "((\"tenant\" > 'acme') OR (\"tenant\" = 'acme' AND \"id\" > '10'))",
        "composite gt");
    expect_true(
        clause(composite, {"acme", "10"}, TupleOp::Ge) ==
            "((\"tenant\" > 'acme') OR (\"tenant\" = 'acme' AND \"id\" >= '10'))",
        "composite ge");
    expect_true(
        clause(composite, {"acme", "10"}, TupleOp::Lt) ==
            "((\"tenant\" < 'acme') OR (\"tenant\" = 'acme' AND \"id\" < '10'))",
        "composite lt");
    expect_true(
        clause(composite, {"acme", "10"}, TupleOp::Le) ==
            "((\"tenant\" < 'acme') OR (\"tenant\" = 'acme' AND \"id\" <= '10'))",
        "composite le");

    expect_true(clause(single, {}, TupleOp::Gt).empty(), "no values yields no clause");
    expect_true(clause({}, {"10"}, TupleOp::Gt).empty(), "no columns yields no clause");

    // A short value vector only constrains the prefix it can actually compare.
    expect_true(
        clause(composite, {"acme"}, TupleOp::Ge) == "((\"tenant\" >= 'acme'))",
        "partial tuple uses prefix only");

    // Boundary offsets split the row count evenly and stay strictly inside it.
    const auto offsets = full_load::slice_boundary_offsets(1000, 4);
    expect_eq_int(static_cast<int>(offsets.size()), 3, "three boundaries for four workers");
    expect_true(offsets[0] == 250 && offsets[1] == 500 && offsets[2] == 750, "even offsets");
    expect_true(full_load::slice_boundary_offsets(1000, 1).empty(), "single worker samples nothing");
    expect_true(full_load::slice_boundary_offsets(0, 4).empty(), "empty table samples nothing");
    expect_true(full_load::slice_boundary_offsets(-1, 4).empty(), "unknown row count samples nothing");
    // More workers than rows must not produce duplicate or out-of-range offsets.
    const auto tiny = full_load::slice_boundary_offsets(3, 8);
    for (std::size_t i = 0; i < tiny.size(); ++i) {
        expect_true(tiny[i] > 0 && tiny[i] < 3, "offset inside table");
        if (i) {
            expect_true(tiny[i] > tiny[i - 1], "offsets strictly increasing");
        }
    }

    // N boundaries produce N+1 slices, with the outer ones unbounded.
    const auto slices = full_load::slices_from_boundaries({{"b"}, {"d"}});
    expect_eq_int(static_cast<int>(slices.size()), 3, "two boundaries yield three slices");
    expect_true(!slices.front().has_begin, "first slice has no lower bound");
    expect_true(!slices.back().has_end, "last slice has no upper bound");
    expect_true(slices[1].begin == std::vector<std::string>{"b"}, "middle slice begin");
    expect_true(slices[1].end == std::vector<std::string>{"d"}, "middle slice end");

    expect_exactly_one_slice(slices, {{"a"}, {"b"}, {"c"}, {"d"}, {"e"}}, "single column coverage");

    const auto composite_slices =
        full_load::slices_from_boundaries({{"acme", "2"}, {"beta", "1"}});
    expect_eq_int(static_cast<int>(composite_slices.size()), 3, "composite slice count");
    expect_exactly_one_slice(
        composite_slices,
        {{"acme", "1"}, {"acme", "2"}, {"acme", "9"}, {"beta", "0"}, {"beta", "1"}, {"zeta", "5"}},
        "composite coverage");

    // Skewed keys can sample the same tuple twice; empty slices must not reach a worker.
    const auto deduped = full_load::slices_from_boundaries({{"b"}, {"b"}, {"d"}});
    expect_eq_int(static_cast<int>(deduped.size()), 3, "duplicate boundaries collapse");
    expect_exactly_one_slice(deduped, {{"a"}, {"b"}, {"c"}, {"d"}}, "deduped coverage");

    const auto unsliced = full_load::slices_from_boundaries({});
    expect_eq_int(static_cast<int>(unsliced.size()), 1, "no boundaries yields one slice");
    expect_true(
        !unsliced[0].has_begin && !unsliced[0].has_end, "single slice is fully unbounded");

    // A bounded slice constrains both ends; an unbounded one adds nothing.
    const std::string where =
        full_load::slice_where_clause(single, slices[1], ident, literal);
    expect_true(
        where == " AND ((\"id\" >= 'b')) AND ((\"id\" < 'd'))", "slice where clause");
    expect_true(
        full_load::slice_where_clause(single, unsliced[0], ident, literal).empty(),
        "unbounded slice adds no predicate");

    // Round-trip must preserve positions: a NULL component that shifted the tuple would
    // silently rewrite the keyset predicate of a composite key.
    const std::vector<std::string> with_null = {"acme", "", "42"};
    const auto round_tripped = full_load::pk_values_from_json(full_load::pk_values_to_json(with_null));
    expect_eq_int(static_cast<int>(round_tripped.size()), 3, "round trip keeps arity");
    expect_true(round_tripped == with_null, "round trip keeps values");

    nlohmann::json with_json_null = nlohmann::json::array({"acme", nullptr, "42"});
    const auto from_null = full_load::pk_values_from_json(with_json_null);
    expect_eq_int(static_cast<int>(from_null.size()), 3, "json null keeps arity");
    expect_true(from_null[0] == "acme" && from_null[1].empty() && from_null[2] == "42",
                "json null maps to empty component");

    // Numeric jsonb written by older builds must still decode positionally.
    nlohmann::json numeric = nlohmann::json::array({7, 42});
    const auto from_numeric = full_load::pk_values_from_json(numeric);
    expect_eq_int(static_cast<int>(from_numeric.size()), 2, "numeric arity");
    expect_true(from_numeric[0] == "7" && from_numeric[1] == "42", "numeric decode");

    expect_true(full_load::pk_values_from_json(nlohmann::json(nullptr)).empty(), "null jsonb");
    expect_true(full_load::pk_values_from_json(nlohmann::json("x")).empty(), "scalar jsonb");

    expect_true(full_load::slice_bound_to_json({"a"}, false).is_null(), "absent bound is null");
    expect_true(full_load::slice_bound_to_json({"a"}, true).is_array(), "present bound is array");

    return 0;
}
