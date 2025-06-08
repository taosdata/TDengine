#include "CSVUtils.h"
#include "StringUtils.h"

namespace CSVUtils {

    template <typename T>
    T convert_value(const std::string& value) {
        std::string trimmed = value;
        StringUtils::trim(trimmed);

        if constexpr (std::is_same_v<T, bool>) {
            std::string lower;
            std::transform(trimmed.begin(), trimmed.end(), std::back_inserter(lower),
                           [](unsigned char c) { return std::tolower(c); });

            if (lower == "true" || lower == "1" || lower == "t") {
                return true;
            }
            if (lower == "false" || lower == "0" || lower == "f") {
                return false;
            }
            throw std::runtime_error("Invalid boolean value: " + trimmed);
        } else if constexpr (std::is_integral_v<T>) {
            if constexpr (std::is_signed_v<T>) {
                return static_cast<T>(std::stoll(trimmed));
            } else {
                return static_cast<T>(std::stoull(trimmed));
            }
        } else if constexpr (std::is_floating_point_v<T>) {
            if constexpr (std::is_same_v<T, float>) {
                return std::stof(trimmed);
            } else {
                return std::stod(trimmed);
            }
        } else if constexpr (std::is_same_v<T, std::string>) {
            return trimmed;
        } else if constexpr (std::is_same_v<T, std::u16string>) {
            std::u16string utf16;
            for (char c : trimmed) {
                utf16.push_back(static_cast<char16_t>(c));
            }
            return utf16;
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