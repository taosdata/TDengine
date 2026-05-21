#include "TypeConverter.hpp"
#include "StringUtils.hpp"
#include <codecvt>
#include <sstream>
#include <vector>
#include <stdexcept>
#include <charconv>
#include <limits>
#include <system_error>
#include <string_view>

namespace TypeConverter {

    namespace {
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
        std::string_view input = StringUtils::trim_view(value);

        if constexpr (std::is_same_v<T, bool>) {
            if (input == "1" || StringUtils::iequals_ascii(input, "true") || StringUtils::iequals_ascii(input, "t")) {
                return true;
            }
            if (input == "0" || StringUtils::iequals_ascii(input, "false") || StringUtils::iequals_ascii(input, "f")) {
                return false;
            }
            throw std::runtime_error("Invalid boolean value: " + std::string(input));
        } else if constexpr (std::is_integral_v<T>) {
            return parse_integral<T>(input);
        } else if constexpr (std::is_floating_point_v<T>) {
            return parse_floating<T>(input);
        } else if constexpr (std::is_same_v<T, std::string>) {
            return std::string(input);
        } else if constexpr (std::is_same_v<T, std::u16string>) {
            return StringUtils::utf8_to_u16string(std::string(input));
        } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
            return std::vector<uint8_t>(input.begin(), input.end());
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

    template bool convert_value<bool>(const std::string& value);
    template int8_t convert_value<int8_t>(const std::string& value);
    template uint8_t convert_value<uint8_t>(const std::string& value);
    template int16_t convert_value<int16_t>(const std::string& value);
    template uint16_t convert_value<uint16_t>(const std::string& value);
    template int32_t convert_value<int32_t>(const std::string& value);
    template uint32_t convert_value<uint32_t>(const std::string& value);
    template int64_t convert_value<int64_t>(const std::string& value);
    template uint64_t convert_value<uint64_t>(const std::string& value);
    template float convert_value<float>(const std::string& value);
    template double convert_value<double>(const std::string& value);
    template std::string convert_value<std::string>(const std::string& value);
    template std::u16string convert_value<std::u16string>(const std::string& value);
    template std::vector<uint8_t> convert_value<std::vector<uint8_t>>(const std::string& value);
}