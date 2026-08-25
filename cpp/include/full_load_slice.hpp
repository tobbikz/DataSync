#pragma once

#include <nlohmann/json.hpp>

#include <cstddef>
#include <functional>
#include <string>
#include <vector>

namespace full_load {

enum class TupleOp {
    Gt,
    Ge,
    Lt,
    Le,
};

/**
 * Half-open PK range copied by a single full-load worker.
 * An absent bound means unbounded, which is what the first and last slice need:
 * non-numeric keys cannot be incremented to build an exclusive upper bound.
 */
struct PkSlice {
    std::vector<std::string> begin;
    std::vector<std::string> end;
    bool has_begin{false};
    bool has_end{false};
};

using IdentQuoter = std::function<std::string(const std::string&)>;

/** Quotes a PK value; receives the column index so callers can quote per column type. */
using LiteralQuoter = std::function<std::string(std::size_t, const std::string&)>;

/** Row offsets to sample so each of `workers` slices holds a similar row count. */
std::vector<long long> slice_boundary_offsets(long long source_rows, int workers);

/**
 * Turns sampled boundary tuples into contiguous half-open slices.
 * N boundaries produce N+1 slices; duplicate boundaries (skewed keys) are dropped
 * so no empty slice is handed to a worker.
 */
std::vector<PkSlice> slices_from_boundaries(
    const std::vector<std::vector<std::string>>& boundaries);

/**
 * Lexicographic tuple predicate over the PK columns, e.g. for Gt on (a, b):
 * `(a > v0) OR (a = v0 AND b > v1)`. Returns empty when there is nothing to compare.
 */
std::string lexicographic_tuple_clause(
    const std::vector<std::string>& pk_cols,
    const std::vector<std::string>& values,
    TupleOp op,
    const IdentQuoter& ident,
    const LiteralQuoter& literal);

/** ` AND (...)` terms for both slice bounds; empty when the slice is unbounded. */
std::string slice_where_clause(
    const std::vector<std::string>& pk_cols,
    const PkSlice& slice,
    const IdentQuoter& ident,
    const LiteralQuoter& literal);

/**
 * PK tuple <-> jsonb. Positions are preserved on both directions: a JSON null maps to
 * the empty string, which is how every engine already represents a NULL PK component.
 * Dropping it would shift the remaining components of a composite key.
 */
nlohmann::json pk_values_to_json(const std::vector<std::string>& values);
std::vector<std::string> pk_values_from_json(const nlohmann::json& values);

/** Slice bounds as stored in full_load_checkpoint; absent bounds serialize to null. */
nlohmann::json slice_bound_to_json(const std::vector<std::string>& bound, bool present);

}  // namespace full_load
