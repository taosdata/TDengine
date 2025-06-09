#pragma once

#include <string>
#include <chrono>
#include <optional>
#include <variant>
#include <tuple>
#include <sstream>
#include <cctype>
#include <stdexcept>
#include <iomanip>
#include <ctime>
#include <algorithm>
#include "StringUtils.h"

struct TimestampOriginalConfig {
    size_t timestamp_index = 0;                     // Index of the original timestamp column (starting from 0)
    std::string precision = "ms";                   // Time precision, options: "s", "ms", "us", "ns"

    struct OffsetConfig {
        std::string offset_type;                    // Timestamp offset type: "relative" or "absolute"
        std::variant<std::string, int64_t> value;   // Offset value or starting timestamp

        // 解析结果缓存
        std::tuple<int, int, int, int> relative_offset = {0, 0, 0, 0}; // (years, months, days, seconds)
        int64_t absolute_value = 0;
        bool parsed = false;

        OffsetConfig(const std::string& type, const std::variant<std::string, int64_t>& val, const std::string& precision) 
            : offset_type(type), value(val) 
        {
            if (offset_type == "relative") {
                if (!std::holds_alternative<std::string>(value)) {
                    throw std::runtime_error("Relative offset must be string");
                }
                relative_offset = parse_time_offset(std::get<std::string>(value));
                parsed = true;
            } else if (offset_type == "absolute") {
                absolute_value = TimestampOriginalConfig::parse_timestamp_value(value, precision);
                parsed = true;
            }
        }

        // 解析relative偏移字符串
        static std::tuple<int, int, int, int> parse_time_offset(const std::string& offset_str) {
            if (offset_str.empty()) {
                throw std::runtime_error("Empty offset string");
            }
            char sign = offset_str[0];
            if (sign != '+' && sign != '-') {
                throw std::runtime_error("Invalid sign character: " + std::string(1, sign));
            }
            int multiplier = (sign == '+') ? 1 : -1;
            int years = 0, months = 0, days = 0, seconds = 0;
            size_t pos = 1;
            while (pos < offset_str.size()) {
                size_t num_end = pos;
                while (num_end < offset_str.size() && std::isdigit(offset_str[num_end])) {
                    ++num_end;
                }
                if (num_end == pos) {
                    throw std::runtime_error("Missing number at position: " + std::to_string(pos));
                }
                int value;
                try {
                    value = std::stoi(offset_str.substr(pos, num_end - pos));
                } catch (...) {
                    throw std::runtime_error("Invalid number format at position: " + std::to_string(pos));
                }
                pos = num_end;
                if (pos >= offset_str.size()) {
                    throw std::runtime_error("Missing unit after number at position: " + std::to_string(pos));
                }
                char unit = offset_str[pos++];
                const int scaled_value = value * multiplier;
                switch (unit) {
                    case 'y': years   += scaled_value; break;
                    case 'm': months  += scaled_value; break;
                    case 'd': days    += scaled_value; break;
                    case 's': seconds += scaled_value; break;
                    case 'h': seconds += scaled_value * 3600; break;
                    default:
                        throw std::runtime_error("Invalid time unit: " + std::string(1, unit));
                }
            }
            return {years, months, days, seconds};
        }
    };

    std::optional<OffsetConfig> offset_config;


    // 解析时间戳值
    static int64_t parse_timestamp_value(const std::variant<std::string, int64_t>& value, const std::string& precision) {
        if (std::holds_alternative<int64_t>(value)) {
            return std::get<int64_t>(value);
        }
    
        const std::string& time_str = std::get<std::string>(value);
        std::string trimmed = time_str;
        StringUtils::trim(trimmed);
    
        // 支持 "now" 或 "now()" 字符串语义
        if (trimmed == "now" || trimmed == "now()") {
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
};