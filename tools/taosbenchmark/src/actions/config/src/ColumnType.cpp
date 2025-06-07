#include "ColumnType.h"
#include <codecvt>
#include <locale>

std::string u16string_to_utf8(const std::u16string& u16str) {
    std::wstring_convert<std::codecvt_utf8_utf16<char16_t>, char16_t> converter;
    return converter.to_bytes(u16str);
}

// Overload << operator for ColumnType
std::ostream& operator<<(std::ostream& os, const ColumnType& column) {
    std::visit([&os](auto&& value) {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, Timestamp>) {
            os << "Timestamp(" << value << ")";
        } else if constexpr (std::is_same_v<T, bool>) {
            os << (value ? "true" : "false");
        } else if constexpr (std::is_same_v<T, int8_t> || std::is_same_v<T, uint8_t> ||
                             std::is_same_v<T, int16_t> || std::is_same_v<T, uint16_t> ||
                             std::is_same_v<T, int32_t> || std::is_same_v<T, uint32_t> ||
                             std::is_same_v<T, int64_t> || std::is_same_v<T, uint64_t>) {
            os << static_cast<int64_t>(value);
        } else if constexpr (std::is_same_v<T, float> || std::is_same_v<T, double>) {
            os << value;
        } else if constexpr (std::is_same_v<T, Decimal>) {
            os << "Decimal(" << value.value << ")";
        } else if constexpr (std::is_same_v<T, std::u16string>) {
            // os << "NChar(" << std::string(value.begin(), value.end()) << ")";
            os  << u16string_to_utf8(value);
        } else if constexpr (std::is_same_v<T, std::string>) {
            os << "String(" << value << ")";
        } else if constexpr (std::is_same_v<T, JsonValue>) {
            os << "Json(" << value.raw_json << ")";
        } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
            os << "VarBinary(";
            for (size_t i = 0; i < value.size(); ++i) {
                os << static_cast<int>(value[i]);
                if (i < value.size() - 1) os << ",";
            }
            os << ")";
        } else if constexpr (std::is_same_v<T, Geometry>) {
            os << "Geometry(" << value.wkt << ")";
        } else {
            os << "UnknownType";
        }
    }, column);
    return os;
}

// Overload << operator for RowType
std::ostream& operator<<(std::ostream& os, const RowType& row) {
    os << "[";
    for (size_t i = 0; i < row.size(); ++i) {
        os << row[i];
        if (i < row.size() - 1) os << ", ";
    }
    os << "]";
    return os;
}