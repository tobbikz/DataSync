#pragma once

#include <cstdlib>
#include <iostream>
#include <string>

inline void expect_true(bool cond, const char* message) {
    if (!cond) {
        std::cerr << "FAIL: " << message << '\n';
        std::exit(1);
    }
}

inline void expect_eq_int(int got, int expected, const char* message) {
    if (got != expected) {
        std::cerr << "FAIL: " << message << " (got " << got << " expected " << expected << ")\n";
        std::exit(1);
    }
}
