#include "TimestampUtils.hpp"
#include <string>
#include <chrono>
#include <optional>
#include <variant>
#include <sstream>
#include <cctype>
#include <stdexcept>
#include <iomanip>
#include <ctime>
#include <algorithm>
#include <string_view>
#include <cctz/civil_time.h>
#include <cctz/time_zone.h>
#include "StringUtils.hpp"

namespace {
const cctz::time_zone& get_cached_local_time_zone() {
    static const cctz::time_zone tz = cctz::local_time_zone();
    return tz;
}
}

const std::unordered_map<std::string, int64_t> TimestampUtils::precision_map = {
    {"ns", 1},
    {"us", 1000LL},
    {"ms", 1000000LL},
    {"s", 1000000000LL}
};

int64_t TimestampUtils::get_precision_multiplier(const std::string& precision) {
    if (precision == "s")  return 1;
    if (precision == "ms") return 1000LL;
    if (precision == "us") return 1000000LL;
    if (precision == "ns") return 1000000000LL;
    throw std::runtime_error("Invalid timestamp precision: " + precision);
}

std::tuple<int64_t, int64_t> TimestampUtils::get_precision_factor(
    const std::string& from_precision,
    const std::string& to_precision) {

    int64_t from_factor = precision_map.at(from_precision);
    int64_t to_factor = precision_map.at(to_precision);

    return {from_factor, to_factor};
}

int64_t TimestampUtils::convert_timestamp_precision(
    int64_t ts,
    const std::string& from_precision,
    const std::string& to_precision) {

    if (from_precision == to_precision) return ts;

    auto [multiplier, divisor] = get_precision_factor(from_precision, to_precision);
    return (ts * multiplier) / divisor;
}

double TimestampUtils::convert_timestamp_precision_double(
    int64_t ts,
    const std::string& from_precision,
    const std::string& to_precision) {

    if (from_precision == to_precision) return static_cast<double>(ts);

    auto [multiplier, divisor] = get_precision_factor(from_precision, to_precision);
    return static_cast<double>(ts) * static_cast<double>(multiplier) / static_cast<double>(divisor);
}

int64_t TimestampUtils::convert_to_timestamp(const std::string& precision) {
    auto now = std::chrono::system_clock::now();
    if (precision == "ms") {
        return std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    } else if (precision == "us") {
        return std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
    } else if (precision == "ns") {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
    } else if (precision == "s") {
        return std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
    } else {
        throw std::runtime_error("Invalid timestamp precision: " + precision);
    }
}

// ============================================================================
// Fractional seconds helper
// ============================================================================

// Helper function to extract fractional seconds and convert to specified precision
// Input: fractional part view (e.g., "123" from ".123456")
// Output: nanoseconds (int64_t value)
// Supports up to 9 decimal digits (nanosecond precision)
static int64_t parse_fractional_to_nanos(std::string_view frac_view) {
    if (frac_view.empty()) {
        return 0;
    }

    int64_t frac_value = 0;
    int frac_digits = 0;
    for (char c : frac_view) {
        unsigned char uc = static_cast<unsigned char>(c);
        if (!std::isdigit(uc)) {
            break;
        }
        if (frac_digits < 9) {
            frac_value = frac_value * 10 + static_cast<int64_t>(c - '0');
            ++frac_digits;
        } else {
            break;
        }
    }

    if (frac_digits == 0) {
        return 0;
    }

    // Convert to nanoseconds by padding with zeros on the right
    // Examples:
    // ".1"       -> "100000000" ns (100 million ns = 100 ms)
    // ".123"     -> "123000000" ns (123 million ns = 123 ms)
    // ".123456"  -> "123456000" ns (123.456 million ns = 123.456 ms)
    // ".123456789" -> "123456789" ns (exactly 123.456789 ms)
    for (int i = frac_digits; i < 9; ++i) {
        frac_value *= 10;
    }

    return frac_value;
}

// ============================================================================
// Performance optimization helpers
// ============================================================================

bool TimestampUtils::is_numeric_string(const std::string& str) {
    if (str.empty()) return false;
    // Check for optional leading +/-
    size_t start = 0;
    if (str[0] == '+' || str[0] == '-') {
        if (str.length() < 2) return false;
        start = 1;
    }
    // Check if all remaining characters are digits
    return std::all_of(str.begin() + start, str.end(),
                       [](unsigned char c) { return std::isdigit(c); });
}

int64_t TimestampUtils::parse_numeric_timestamp(const std::string& str) {
    try {
        return std::stoll(str);
    } catch (const std::out_of_range&) {
        throw std::runtime_error("Timestamp value out of range: " + str);
    }
}

// Parse ISO time for UTC using strptime + timegm (no locks, no locale)
int64_t TimestampUtils::parse_iso_utc_time(const std::string& iso_str,
                                            const std::string& precision) {
    struct tm time_struct = {};

    // Extract fractional seconds if present (supports up to 9 digits: nanosecond precision)
    int64_t fractional_nanos = 0;
    std::string parse_str = iso_str;
    std::string_view iso_view(iso_str);
    size_t dot_pos = iso_view.find('.');
    if (dot_pos != std::string::npos) {
        fractional_nanos = parse_fractional_to_nanos(iso_view.substr(dot_pos + 1));
        // Remove fractional part for strptime parsing
        parse_str.assign(iso_view.substr(0, dot_pos));
    }

    // Use strptime instead of std::get_time to avoid C++ locale locks
    // Performance: strptime is significantly faster in multi-threaded contexts
#if defined(_WIN32)
    // Windows doesn't have strptime, fallback to manual parsing
    int year, month, day, hour, minute, second;
    int matched = sscanf(parse_str.c_str(), "%d-%d-%d %d:%d:%d",
                        &year, &month, &day, &hour, &minute, &second);
    if (matched != 6) {
        throw std::runtime_error("Invalid timestamp format: " + iso_str);
    }
    time_struct.tm_year = year - 1900;
    time_struct.tm_mon = month - 1;
    time_struct.tm_mday = day;
    time_struct.tm_hour = hour;
    time_struct.tm_min = minute;
    time_struct.tm_sec = second;
    time_struct.tm_isdst = 0;  // UTC, no DST
#else
    // Unix: Use strptime for better performance (avoids C++ locale)
    if (!strptime(parse_str.c_str(), "%Y-%m-%d %H:%M:%S", &time_struct)) {
        throw std::runtime_error("Invalid timestamp format: " + iso_str);
    }
    time_struct.tm_isdst = 0;  // UTC, no DST
#endif

    // Convert to time_t using timegm (no glibc locks, no timezone conversion)
#if defined(_WIN32)
    time_t time_val = _mkgmtime(&time_struct);
#else
    time_t time_val = timegm(&time_struct);
#endif

    if (time_val == -1) {
        throw std::runtime_error("Failed to convert timestamp: " + iso_str);
    }

    // Convert base timestamp to nanoseconds and add fractional part
    int64_t nanos_val = static_cast<int64_t>(time_val) * 1000000000LL + fractional_nanos;

    // Return timestamp according to precision
    if (precision == "s")  return nanos_val / 1000000000LL;
    if (precision == "ms") return nanos_val / 1000000LL;
    if (precision == "us") return nanos_val / 1000LL;
    if (precision == "ns") return nanos_val;

    return nanos_val / 1000000LL;  // Default to milliseconds
}

// Parse ISO time for local time using cctz (thread-safe, correct DST handling)
int64_t TimestampUtils::parse_iso_local_time(const std::string& iso_str,
                                              const std::string& precision) {
    try {
        std::string_view iso_view(iso_str);
        size_t dot_pos = iso_view.find('.');

        // Use cctz for thread-safe, correct local time parsing
        // cctz handles DST correctly without global locks
        const auto& tz = get_cached_local_time_zone();
        std::chrono::system_clock::time_point tp;

        int64_t nanos = 0;
        if (dot_pos == std::string::npos) {
            if (!cctz::parse("%Y-%m-%d %H:%M:%S", iso_str, tz, &tp)) {
                throw std::runtime_error("Invalid timestamp format: " + iso_str);
            }
            nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(
                tp.time_since_epoch()).count();
        } else {
            int64_t fractional_nanos = parse_fractional_to_nanos(iso_view.substr(dot_pos + 1));
            std::string parse_str;
            parse_str.assign(iso_view.substr(0, dot_pos));
            if (!cctz::parse("%Y-%m-%d %H:%M:%S", parse_str, tz, &tp)) {
                throw std::runtime_error("Invalid timestamp format: " + iso_str);
            }
            nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(
                tp.time_since_epoch()).count();
            nanos += fractional_nanos;
        }

        // Return timestamp according to precision
        if (precision == "s")  return nanos / 1000000000LL;
        if (precision == "ms") return nanos / 1000000LL;
        if (precision == "us") return nanos / 1000LL;
        if (precision == "ns") return nanos;

        return nanos / 1000000LL;  // Default to milliseconds
    } catch (const std::exception& e) {
        throw std::runtime_error(std::string("Failed to parse local timestamp: ") + e.what());
    }
}

int64_t TimestampUtils::parse_iso_timestamp(const std::string& timestamp,
                                             const std::string& precision,
                                             bool is_utc) {
    if (is_utc) {
        return parse_iso_utc_time(timestamp, precision);
    } else {
        return parse_iso_local_time(timestamp, precision);
    }
}

// ============================================================================
// Main parsing function with performance optimization
// ============================================================================

int64_t TimestampUtils::parse_timestamp(const std::variant<int64_t, std::string>& timestamp, const std::string& precision) {
    if (std::holds_alternative<int64_t>(timestamp)) {
        return std::get<int64_t>(timestamp);
    }

    const std::string& time_str = std::get<std::string>(timestamp);
    std::string trimmed = time_str;
    StringUtils::remove_all_spaces(trimmed);

    // Pure numeric timestamp
    if (is_numeric_string(trimmed)) {
        return parse_numeric_timestamp(trimmed);
    }

    // Support "now" or "now()" and "now()+10s" etc.
    if (trimmed.rfind("now", 0) == 0) {
        int64_t base = convert_to_timestamp(precision);
        size_t pos = trimmed.find_first_of("+-", 3);
        if (pos != std::string::npos) {
            char op = trimmed[pos];
            std::string offset_str = trimmed.substr(pos + 1);
            StringUtils::trim(offset_str);

            // Find unit (if any)
            size_t unit_pos = offset_str.find_first_not_of("0123456789");
            std::string number_part = offset_str.substr(0, unit_pos);
            std::string unit_part;
            if (unit_pos != std::string::npos)
                unit_part = offset_str.substr(unit_pos);

            int64_t offset = 0;
            try {
                offset = std::stoll(number_part);
            } catch (...) {
                throw std::runtime_error("Invalid offset in now() expression: " + trimmed);
            }

            // Unit conversion
            int64_t multiplier = 1;
            if (unit_part == "ns") multiplier = 1LL;
            else if (unit_part == "us") multiplier = 1000LL;
            else if (unit_part == "ms") multiplier = 1000LL * 1000LL;
            else if (unit_part == "s")  multiplier = 1000LL * 1000LL * 1000LL;
            else if (unit_part == "m")  multiplier = 60LL * 1000LL * 1000LL * 1000LL;
            else if (unit_part == "h")  multiplier = 60LL * 60LL * 1000LL * 1000LL * 1000LL;
            else if (unit_part == "d")  multiplier = 24LL * 60LL * 60LL * 1000LL * 1000LL * 1000LL;
            else if (unit_part.empty()) {
                // Use precision
                if (precision == "ns") multiplier = 1LL;
                else if (precision == "us") multiplier = 1000LL;
                else if (precision == "ms") multiplier = 1000LL * 1000LL;
                else if (precision == "s")  multiplier = 1000LL * 1000LL * 1000LL;
                else if (precision == "m")  multiplier = 60LL * 1000LL * 1000LL * 1000LL;
                else if (precision == "h")  multiplier = 60LL * 60LL * 1000LL * 1000LL * 1000LL;
                else if (precision == "d")  multiplier = 24LL * 60LL * 60LL * 1000LL * 1000LL * 1000LL;
                else throw std::runtime_error("Unknown precision: " + precision);
            } else {
                throw std::runtime_error("Unknown time unit: " + unit_part);
            }

            int64_t delta = offset * multiplier;
            int64_t delta_in_precision = delta;
            if (precision == "ns") {
                // do nothing
            } else if (precision == "us") {
                delta_in_precision /= 1000LL;
            } else if (precision == "ms") {
                delta_in_precision /= 1000000LL;
            } else if (precision == "s") {
                delta_in_precision /= 1000000000LL;
            } else {
                throw std::runtime_error("Unknown precision: " + precision);
            }

            if (op == '+') return base + delta_in_precision;
            else return base - delta_in_precision;
        }
        return base;
    }

    // Standard path: ISO time format
    std::string iso_str = time_str;
    StringUtils::trim(iso_str);
    bool is_utc = false;
    if (iso_str.size() > 1 && iso_str.back() == 'Z') {
        iso_str.pop_back();
        is_utc = true;
    }
    size_t t_pos = iso_str.find('T');
    if (t_pos != std::string::npos) {
        iso_str[t_pos] = ' ';
    }

    return parse_iso_timestamp(iso_str, precision, is_utc);
}

int64_t TimestampUtils::parse_step(const std::variant<int64_t, std::string>& step, const std::string& precision) {
    if (std::holds_alternative<int64_t>(step)) {
        return std::get<int64_t>(step);
    }

    const std::string& step_str = std::get<std::string>(step);
    std::string trimmed = step_str;
    StringUtils::remove_all_spaces(trimmed);

    size_t unit_pos = trimmed.find_first_not_of("0123456789");
    std::string number_part = trimmed.substr(0, unit_pos);
    std::string unit_part;
    if (unit_pos != std::string::npos)
        unit_part = trimmed.substr(unit_pos);

    if (number_part.empty())
        throw std::runtime_error("Invalid timestamp step string: " + trimmed);

    int64_t value = 0;
    try {
        value = std::stoll(number_part);
    } catch (...) {
        throw std::runtime_error("Invalid number in timestamp step string: " + trimmed);
    }

    int64_t multiplier = 1;
    if (unit_part == "ns") multiplier = 1LL;
    else if (unit_part == "us") multiplier = 1000LL;
    else if (unit_part == "ms") multiplier = 1000LL * 1000LL;
    else if (unit_part == "s")  multiplier = 1000LL * 1000LL * 1000LL;
    else if (unit_part.empty()) {
        if (precision == "ns") multiplier = 1LL;
        else if (precision == "us") multiplier = 1000LL;
        else if (precision == "ms") multiplier = 1000LL * 1000LL;
        else if (precision == "s")  multiplier = 1000LL * 1000LL * 1000LL;
        else throw std::runtime_error("Unknown timestap precision: " + precision);
    } else {
        throw std::runtime_error("Unknown timestap step unit: " + unit_part);
    }

    int64_t step_val = value * multiplier;
    if (precision == "ns") {
        return step_val;
    } else if (precision == "us") {
        return step_val / 1000LL;
    } else if (precision == "ms") {
        return step_val / 1000000LL;
    } else if (precision == "s") {
        return step_val / 1000000000LL;
    } else {
        throw std::runtime_error("Unknown precision: " + precision);
    }
}