#include "full_load_slice.hpp"

#include <algorithm>
#include <sstream>

namespace full_load {

namespace {

/** Comparison for every column but the last one, where equality is handled by the OR arm. */
const char* strict_op(TupleOp op) {
    return (op == TupleOp::Gt || op == TupleOp::Ge) ? " > " : " < ";
}

const char* final_op(TupleOp op) {
    switch (op) {
        case TupleOp::Gt:
            return " > ";
        case TupleOp::Ge:
            return " >= ";
        case TupleOp::Lt:
            return " < ";
        case TupleOp::Le:
            return " <= ";
    }
    return " > ";
}

}  // namespace

std::vector<long long> slice_boundary_offsets(long long source_rows, int workers) {
    std::vector<long long> offsets;
    if (workers <= 1 || source_rows <= 0) {
        return offsets;
    }
    offsets.reserve(static_cast<std::size_t>(workers - 1));
    for (int i = 1; i < workers; ++i) {
        const long long offset = (source_rows * i) / workers;
        if (offset <= 0 || offset >= source_rows) {
            continue;
        }
        if (!offsets.empty() && offsets.back() == offset) {
            continue;
        }
        offsets.push_back(offset);
    }
    return offsets;
}

std::vector<PkSlice> slices_from_boundaries(
    const std::vector<std::vector<std::string>>& boundaries) {
    std::vector<std::vector<std::string>> unique;
    unique.reserve(boundaries.size());
    for (const auto& boundary : boundaries) {
        if (boundary.empty()) {
            continue;
        }
        if (!unique.empty() && unique.back() == boundary) {
            continue;
        }
        unique.push_back(boundary);
    }

    std::vector<PkSlice> slices;
    slices.reserve(unique.size() + 1);
    for (std::size_t i = 0; i <= unique.size(); ++i) {
        PkSlice slice;
        if (i > 0) {
            slice.begin = unique[i - 1];
            slice.has_begin = true;
        }
        if (i < unique.size()) {
            slice.end = unique[i];
            slice.has_end = true;
        }
        slices.push_back(std::move(slice));
    }
    return slices;
}

std::string lexicographic_tuple_clause(
    const std::vector<std::string>& pk_cols,
    const std::vector<std::string>& values,
    TupleOp op,
    const IdentQuoter& ident,
    const LiteralQuoter& literal) {
    const std::size_t depth_count = std::min(pk_cols.size(), values.size());
    if (depth_count == 0 || !ident || !literal) {
        return {};
    }

    std::ostringstream clause;
    clause << "(";
    for (std::size_t depth = 0; depth < depth_count; ++depth) {
        if (depth) {
            clause << " OR ";
        }
        clause << "(";
        for (std::size_t eq = 0; eq < depth; ++eq) {
            clause << ident(pk_cols[eq]) << " = " << literal(eq, values[eq]) << " AND ";
        }
        clause << ident(pk_cols[depth])
               << ((depth + 1 == depth_count) ? final_op(op) : strict_op(op))
               << literal(depth, values[depth]);
        clause << ")";
    }
    clause << ")";
    return clause.str();
}

std::string slice_where_clause(
    const std::vector<std::string>& pk_cols,
    const PkSlice& slice,
    const IdentQuoter& ident,
    const LiteralQuoter& literal) {
    std::string out;
    if (slice.has_begin) {
        const std::string lower =
            lexicographic_tuple_clause(pk_cols, slice.begin, TupleOp::Ge, ident, literal);
        if (!lower.empty()) {
            out += " AND " + lower;
        }
    }
    if (slice.has_end) {
        const std::string upper =
            lexicographic_tuple_clause(pk_cols, slice.end, TupleOp::Lt, ident, literal);
        if (!upper.empty()) {
            out += " AND " + upper;
        }
    }
    return out;
}

nlohmann::json pk_values_to_json(const std::vector<std::string>& values) {
    nlohmann::json arr = nlohmann::json::array();
    for (const auto& value : values) {
        arr.push_back(value);
    }
    return arr;
}

std::vector<std::string> pk_values_from_json(const nlohmann::json& values) {
    std::vector<std::string> out;
    if (!values.is_array()) {
        return out;
    }
    for (const auto& v : values) {
        if (v.is_string()) {
            out.push_back(v.get<std::string>());
        } else if (v.is_number_integer()) {
            out.push_back(std::to_string(v.get<long long>()));
        } else if (v.is_number_unsigned()) {
            out.push_back(std::to_string(v.get<unsigned long long>()));
        } else if (v.is_number_float()) {
            out.push_back(std::to_string(v.get<double>()));
        } else if (v.is_null()) {
            out.emplace_back();
        } else {
            out.push_back(v.dump());
        }
    }
    return out;
}

nlohmann::json slice_bound_to_json(const std::vector<std::string>& bound, bool present) {
    if (!present) {
        return nlohmann::json(nullptr);
    }
    return pk_values_to_json(bound);
}

}  // namespace full_load
