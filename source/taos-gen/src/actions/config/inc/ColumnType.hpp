#pragma once

#include "StringUtils.hpp"
#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <fmt/ostream.h>
#include <iostream>
#include <variant>
#include <string>
#include <vector>
#include <cstdint>
#include <type_traits>


using Timestamp = int64_t;

struct Decimal {
    std::string value;
};


// JSON type (can be replaced with nlohmann::json)
struct JsonValue {
    std::string raw_json;
};


// Geometry type (example: WKT format)
struct Geometry {
    std::string wkt; // e.g. "POINT(10 20)"
};

enum class ColumnTypeTag {
    UNKNOWN,
    BOOL,
    TINYINT,
    TINYINT_UNSIGNED,
    SMALLINT,
    SMALLINT_UNSIGNED,
    INT,
    INT_UNSIGNED,
    BIGINT,
    BIGINT_UNSIGNED,
    FLOAT,
    DOUBLE,
    DECIMAL,
    NCHAR,       // std::u16string
    VARCHAR,     // std::string (varchar/binary)
    BINARY,      // std::string (varchar/binary)
    JSON,        // std::string (json)
    VARBINARY,   // std::vector<uint8_t>
    GEOMETRY,    // Geometry
    MAX
};

using ColumnType = std::variant<
    bool,                 // bool
    int8_t,               // tinyint
    uint8_t,              // tinyint unsigned
    int16_t,              // smallint
    uint16_t,             // smallint unsigned
    int32_t,              // int
    uint32_t,             // int unsigned
    int64_t,              // bigint
    uint64_t,             // bigint unsigned
    float,                // float
    double,               // double
    Decimal,              // decimal
    std::u16string,       // nchar (UTF-16)
    std::string,          // varchar / binary
    JsonValue,            // json
    std::vector<uint8_t>, // varbinary
    Geometry              // geometry
>;

using ColumnTypeVector = std::vector<ColumnType>;
using RowType = ColumnTypeVector;

template <>
struct fmt::formatter<ColumnType> {
    constexpr auto parse(fmt::format_parse_context& ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(const ColumnType& column, FormatContext& ctx) const {
        return std::visit(
            [&](auto&& value) {
                using T = std::decay_t<decltype(value)>;
                auto out = ctx.out();
                if constexpr (std::is_same_v<T, std::monostate>) {
                    return fmt::format_to(out, "NULL");
                } else if constexpr (std::is_same_v<T, bool>) {
                    return fmt::format_to(out, "{}", value ? "true" : "false");
                } else if constexpr (std::is_arithmetic_v<T>) {
                    return fmt::format_to(out, "{}", value);
                } else if constexpr (std::is_same_v<T, Decimal>) {
                    return fmt::format_to(out, "Decimal({})", value.value);
                } else if constexpr (std::is_same_v<T, std::string>) {
                    return fmt::format_to(out, "{}", value);
                } else if constexpr (std::is_same_v<T, std::u16string>) {
                    return fmt::format_to(out, "{}", StringUtils::u16string_to_utf8(value));
                } else if constexpr (std::is_same_v<T, JsonValue>) {
                    return fmt::format_to(out, "{}", value.raw_json);
                } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
                    return fmt::format_to(out, "VarBinary({})", fmt::join(value, ","));
                } else if constexpr (std::is_same_v<T, Geometry>) {
                    return fmt::format_to(out, "Geometry({})", value.wkt);
                } else {
                    return fmt::format_to(out, "UnknownType");
                }
            },
            column);
    }
};

// Overload << operator for ColumnType
inline std::ostream& operator<<(std::ostream& os, const ColumnType& column) {
    fmt::print(os, "{}", column);
    return os;
}

// Overload << operator for RowType
inline std::ostream& operator<<(std::ostream& os, const RowType& row) {
    fmt::print(os, "[{}]", fmt::join(row, ", "));
    return os;
}

template <typename T, typename Variant>
struct variant_index;

template <typename T, typename... Types>
struct variant_index<T, std::variant<Types...>> {
    static_assert(
        (std::is_same_v<T, Types> || ...),
        "Type not found in variant"
    );

    static constexpr std::size_t index() {
        constexpr bool matches[] = { std::is_same_v<T, Types>... };
        for (std::size_t i = 0; i < sizeof...(Types); ++i) {
            if (matches[i]) return i;
        }
        return static_cast<std::size_t>(-1);
    }

    static constexpr std::size_t value = index();
};

namespace ColumnTypeTraits {
    constexpr bool needs_quotes(ColumnTypeTag tag) noexcept {
        switch (tag) {
            case ColumnTypeTag::NCHAR:
            case ColumnTypeTag::VARCHAR:
            case ColumnTypeTag::BINARY:
            case ColumnTypeTag::JSON:
                return true;
            default:
                return false;
        }
    }

    constexpr bool is_numeric(ColumnTypeTag tag) noexcept {
        switch (tag) {
            case ColumnTypeTag::TINYINT:
            case ColumnTypeTag::TINYINT_UNSIGNED:
            case ColumnTypeTag::SMALLINT:
            case ColumnTypeTag::SMALLINT_UNSIGNED:
            case ColumnTypeTag::INT:
            case ColumnTypeTag::INT_UNSIGNED:
            case ColumnTypeTag::BIGINT:
            case ColumnTypeTag::BIGINT_UNSIGNED:
            case ColumnTypeTag::FLOAT:
            case ColumnTypeTag::DOUBLE:
            case ColumnTypeTag::DECIMAL:
                return true;
            default:
                return false;
        }
    }

    constexpr bool is_integer(ColumnTypeTag tag) noexcept {
        switch (tag) {
            case ColumnTypeTag::TINYINT:
            case ColumnTypeTag::TINYINT_UNSIGNED:
            case ColumnTypeTag::SMALLINT:
            case ColumnTypeTag::SMALLINT_UNSIGNED:
            case ColumnTypeTag::INT:
            case ColumnTypeTag::INT_UNSIGNED:
            case ColumnTypeTag::BIGINT:
            case ColumnTypeTag::BIGINT_UNSIGNED:
                return true;
            default:
                return false;
        }
    }
}