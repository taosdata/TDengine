#pragma once

#include <string>
#include <chrono>
#include <variant>
#include <unordered_map>

class TimestampUtils {
public:
    static int64_t get_precision_multiplier(const std::string& precision);

    static std::tuple<int64_t, int64_t> get_precision_factor(
        const std::string& from_precision, 
        const std::string& to_precision);
    
    static int64_t convert_timestamp_precision(
        int64_t ts, 
        const std::string& from_precision, 
        const std::string& to_precision);

    static int64_t convert_to_timestamp(const std::string& precision);
    static int64_t parse_timestamp(const std::variant<int64_t, std::string>& timestamp, const std::string& precision);
private:
    TimestampUtils() = delete;
    static const std::unordered_map<std::string, int64_t> precision_map;
};