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
#include "StringUtils.hpp"


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

int64_t TimestampUtils::parse_timestamp(const std::variant<int64_t, std::string>& timestamp, const std::string& precision) {
    if (std::holds_alternative<int64_t>(timestamp)) {
        return std::get<int64_t>(timestamp);
    }

    const std::string& time_str = std::get<std::string>(timestamp);
    std::string trimmed = time_str;
    StringUtils::remove_all_spaces(trimmed);


    // Support "now" or "now()" and "now()+10s" etc.
    if (trimmed.rfind("now", 0) == 0) {
        int64_t base = convert_to_timestamp(precision);
        size_t pos = trimmed.find_first_of("+-", 3); // after "now"
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


    // Try to convert string to integer timestamp
    if (!trimmed.empty() && std::all_of(trimmed.begin(), trimmed.end(), ::isdigit)) {
        try {
            return std::stoll(trimmed);
        } catch (const std::out_of_range&) {
            throw std::runtime_error("Timestamp value out of range: " + trimmed);
        }
    }

    // Parse ISO time format
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

    tm time_struct = {};
    std::istringstream ss(iso_str);
    ss >> std::get_time(&time_struct, "%Y-%m-%d %H:%M:%S");
    if (ss.fail()) {
        throw std::runtime_error("Invalid timestamp format: " + trimmed);
    }

    time_t time_val;
    if (is_utc) {
#if defined(_WIN32)
        time_val = _mkgmtime(&time_struct);
#else
        time_val = timegm(&time_struct);
#endif
    } else {
        // Let mktime decide DST rather than defaulting to tm_isdst=0
        time_struct.tm_isdst = -1;
        time_val = mktime(&time_struct);
    }

    int64_t ms_val = static_cast<int64_t>(time_val) * 1000;

    // Return timestamp according to precision
    if (precision == "s")  return ms_val / 1000;
    if (precision == "ms") return ms_val;
    if (precision == "us") return ms_val * 1000;
    if (precision == "ns") return ms_val * 1000000;

    return ms_val;
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