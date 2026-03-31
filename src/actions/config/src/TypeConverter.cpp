#include "TypeConverter.hpp"
#include "StringUtils.hpp"
#include <codecvt>
#include <sstream>
#include <vector>
#include <stdexcept>
#include <algorithm>
#include <cctype>
#include <charconv>
#include <limits>
#include <system_error>
#include <string_view>

namespace TypeConverter {

    namespace {
        std::string_view trim_view(std::string_view sv) {
            while (!sv.empty() && std::isspace(static_cast<unsigned char>(sv.front()))) {
                sv.remove_prefix(1);
            }
            while (!sv.empty() && std::isspace(static_cast<unsigned char>(sv.back()))) {
                sv.remove_suffix(1);
            }
            return sv;
        }

        bool iequals_ascii(std::string_view input, std::string_view expected_lower) {
            if (input.size() != expected_lower.size()) {
                return false;
            }
            for (size_t index = 0; index < input.size(); ++index) {
                unsigned char ch = static_cast<unsigned char>(input[index]);
                if (static_cast<char>(std::tolower(ch)) != expected_lower[index]) {
                    return false;
                }
            }
            return true;
        }

        template <typename T>
        T parse_integral(std::string_view sv) {
            // std::from_chars does not accept leading '+' for any integer type,
            // but std::stoi/stoll/stoull (the previous implementation) did. Skip it for compatibility.
            if (!sv.empty() && sv.front() == '+') {
                sv.remove_prefix(1);
            }
            T parsed{};
            auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), parsed);
            if (ec != std::errc()) {
                throw std::runtime_error("Invalid integer value");
            }
            // Allow trailing non-digit characters (e.g. "221.5" → 221)
            // to match the previous std::stoi/stol/stoll behavior
            return parsed;
        }

        template <typename T>
        T parse_floating(std::string_view sv) {
#if defined(__cpp_lib_to_chars) && (!defined(__GNUC__) || defined(__clang__) || (__GNUC__ >= 11))
            T parsed{};
            auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), parsed);
            if (ec == std::errc() && ptr == sv.data() + sv.size()) {
                return parsed;
            }
#endif

            std::string fallback(sv);
            if constexpr (std::is_same_v<T, float>) {
                return std::stof(fallback);
            }
            return std::stod(fallback);
        }
    }

    template <typename T>
    T convert_value(const std::string& value) {
        std::string_view trimmed = trim_view(value);

        if constexpr (std::is_same_v<T, bool>) {
            if (trimmed == "1" || iequals_ascii(trimmed, "true") || iequals_ascii(trimmed, "t")) {
                return true;
            }
            if (trimmed == "0" || iequals_ascii(trimmed, "false") || iequals_ascii(trimmed, "f")) {
                return false;
            }
            throw std::runtime_error("Invalid boolean value: " + std::string(trimmed));
        } else if constexpr (std::is_integral_v<T>) {
            return parse_integral<T>(trimmed);
        } else if constexpr (std::is_floating_point_v<T>) {
            return parse_floating<T>(trimmed);
        } else if constexpr (std::is_same_v<T, std::string>) {
            return std::string(trimmed);
        } else if constexpr (std::is_same_v<T, std::u16string>) {
            return StringUtils::utf8_to_u16string(std::string(trimmed));
        } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
            return std::vector<uint8_t>(trimmed.begin(), trimmed.end());
        } else {
            throw std::runtime_error("Unsupported type conversion.");
        }
    }

    ColumnType convert_to_type(const std::string& value, ColumnTypeTag target_type) {
        try {
            switch (target_type) {
                case ColumnTypeTag::BOOL:
                    return convert_value<bool>(value);
                case ColumnTypeTag::TINYINT:
                    return convert_value<int8_t>(value);
                case ColumnTypeTag::TINYINT_UNSIGNED:
                    return convert_value<uint8_t>(value);
                case ColumnTypeTag::SMALLINT:
                    return convert_value<int16_t>(value);
                case ColumnTypeTag::SMALLINT_UNSIGNED:
                    return convert_value<uint16_t>(value);
                case ColumnTypeTag::INT:
                    return convert_value<int32_t>(value);
                case ColumnTypeTag::INT_UNSIGNED:
                    return convert_value<uint32_t>(value);
                case ColumnTypeTag::BIGINT:
                    return convert_value<int64_t>(value);
                case ColumnTypeTag::BIGINT_UNSIGNED:
                    return convert_value<uint64_t>(value);
                case ColumnTypeTag::FLOAT:
                    return convert_value<float>(value);
                case ColumnTypeTag::DOUBLE:
                    return convert_value<double>(value);
                case ColumnTypeTag::DECIMAL:
                    throw std::runtime_error("Decimal type conversion not implemented.");
                case ColumnTypeTag::NCHAR:
                    return convert_value<std::u16string>(value);
                case ColumnTypeTag::VARCHAR:
                case ColumnTypeTag::BINARY:
                case ColumnTypeTag::JSON:
                    return convert_value<std::string>(value);
                case ColumnTypeTag::VARBINARY:
                    return convert_value<std::vector<uint8_t>>(value);
                case ColumnTypeTag::GEOMETRY:
                    throw std::runtime_error("Geometry type conversion not implemented.");
                default:
                    throw std::runtime_error("Unknown ColumnTypeTag.");
            }
        } catch (const std::exception& e) {
            std::stringstream ss;
            ss << "Failed to convert value '" << value << "' to target type: " << e.what();
            throw std::runtime_error(ss.str());
        }
    }
}