#include "TimestampUtils.h"
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
#include "StringUtils.h"


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
    StringUtils::trim(trimmed);

    // 支持 "now" 或 "now()" 字符串语义
    if (trimmed == "now" || trimmed == "now()") {
        return convert_to_timestamp(precision);
    }

    // 尝试将字符串转换为整型时间戳
    try {
        return std::stoll(trimmed);
    } catch (const std::invalid_argument&) {

    } catch (const std::out_of_range&) {
        throw std::runtime_error("Timestamp value out of range: " + trimmed);
    }

    // 解析 ISO 时间格式
    tm time_struct = {};
    std::istringstream ss(trimmed);
    ss >> std::get_time(&time_struct, "%Y-%m-%d %H:%M:%S");
    if (ss.fail()) {
        throw std::runtime_error("Invalid timestamp format: " + trimmed);
    }

    time_t time_val = mktime(&time_struct);
    int64_t ms_val = static_cast<int64_t>(time_val) * 1000;

    // 根据精度返回时间戳
    if (precision == "s")  return ms_val / 1000;
    if (precision == "ms") return ms_val;
    if (precision == "us") return ms_val * 1000;
    if (precision == "ns") return ms_val * 1000000;

    return ms_val;
}