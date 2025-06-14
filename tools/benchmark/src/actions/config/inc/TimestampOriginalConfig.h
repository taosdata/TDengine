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
#include "TimestampUtils.h"

struct TimestampOriginalConfig {
    size_t timestamp_index = 0;                     // Index of the original timestamp column (starting from 0)
    std::string timestamp_precision = "ms";         // Time precision, options: "s", "ms", "us", "ns"

    struct OffsetConfig {
        std::string offset_type;                    // Timestamp offset type: "relative" or "absolute"
        std::variant<int64_t, std::string> value;   // Offset value or starting timestamp

        // 解析结果缓存
        std::tuple<int, int, int, int, int> relative_offset = {0, 0, 0, 0, 0}; // (years, months, days, hours, seconds)
        int64_t absolute_value = 0;
        bool parsed = false;

        OffsetConfig(const std::string& type, const std::variant<int64_t, std::string>& val, const std::string& precision) 
            : offset_type(type), value(val)
        {
            parse_offset(precision);
        }

        void parse_offset(const std::string& precision) {
            if (offset_type == "relative") {
                if (!std::holds_alternative<std::string>(value)) {
                    throw std::runtime_error("Relative offset must be string");
                }
                relative_offset = parse_time_offset(std::get<std::string>(value));
                parsed = true;
            } else if (offset_type == "absolute") {
                absolute_value = TimestampUtils::parse_timestamp(value, precision);
                parsed = true;
            }
        }

        // 解析relative偏移字符串
        static std::tuple<int, int, int, int, int> parse_time_offset(const std::string& offset_str) {
            if (offset_str.empty()) {
                throw std::runtime_error("Empty offset string");
            }
            char sign = offset_str[0];
            if (sign != '+' && sign != '-') {
                throw std::runtime_error("Invalid sign character: " + std::string(1, sign));
            }
            int multiplier = (sign == '+') ? 1 : -1;
            int years = 0, months = 0, days = 0, hours = 0, seconds = 0;
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
                    case 'h': hours   += scaled_value; break;
                    case 's': seconds += scaled_value; break;
                    default:
                        throw std::runtime_error("Invalid time unit: " + std::string(1, unit));
                }
            }
            return {years, months, days, hours, seconds};
        }
    };

    std::optional<OffsetConfig> offset_config;
};