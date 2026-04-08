#include "StringUtils.hpp"
#include <cassert>
#include <iostream>
#include <string_view>

void test_trim_view_trims_leading_and_trailing_whitespace() {
    using namespace std::literals;
    assert(StringUtils::trim_view("  value\t\n"sv) == "value"sv);
    assert(StringUtils::trim_view("\r\n  padded text  \f"sv) == "padded text"sv);
}

void test_trim_view_returns_empty_for_whitespace_only() {
    using namespace std::literals;
    assert(StringUtils::trim_view("   \t\n\r"sv) == ""sv);
    assert(StringUtils::trim_view(""sv) == ""sv);
}

void test_iequals_ascii_matches_case_insensitively() {
    assert(StringUtils::iequals_ascii("NULL", "null"));
    assert(StringUtils::iequals_ascii("n/A", "n/a"));
    assert(StringUtils::iequals_ascii("Na", "na"));
}

void test_iequals_ascii_rejects_different_strings() {
    assert(!StringUtils::iequals_ascii("nulls", "null"));
    assert(!StringUtils::iequals_ascii("na", "null"));
    assert(!StringUtils::iequals_ascii("n-a", "n/a"));
}

int main() {
    test_trim_view_trims_leading_and_trailing_whitespace();
    test_trim_view_returns_empty_for_whitespace_only();
    test_iequals_ascii_matches_case_insensitively();
    test_iequals_ascii_rejects_different_strings();

    std::cout << "TestStringUtils passed" << std::endl;
    return 0;
}
