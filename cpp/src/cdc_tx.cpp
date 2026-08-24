#include "cdc_tx.hpp"

#include <cctype>
#include <cstdlib>
#include <string>

long long cdc_tx_id_from_hex(std::string_view hex) {
    std::string digits;
    digits.reserve(hex.size());
    for (char c : hex) {
        if (std::isxdigit(static_cast<unsigned char>(c))) {
            digits.push_back(c);
        }
    }
    if (digits.empty()) {
        return 0;
    }
    if (digits.size() > 15) {
        digits.resize(15);
    }
    return std::strtoll(digits.c_str(), nullptr, 16);
}
