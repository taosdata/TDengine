#include "ColumnConfig.hpp"
#include "StringUtils.hpp"
#include "taos.h"
#include <stdexcept>
#include <cfloat>
#include <limits>
#include <regex>
#include <cassert>
#include <unordered_map>

ColumnTypeTag ColumnConfig::get_type_tag(const std::string& type_str) {
    static const std::unordered_map<std::string, ColumnTypeTag> type_tag_map = {
        {"bool",                ColumnTypeTag::BOOL},
        {"tinyint",             ColumnTypeTag::TINYINT},
        {"tinyint unsigned",    ColumnTypeTag::TINYINT_UNSIGNED},
        {"smallint",            ColumnTypeTag::SMALLINT},
        {"smallint unsigned",   ColumnTypeTag::SMALLINT_UNSIGNED},
        {"int",                 ColumnTypeTag::INT},
        {"int unsigned",        ColumnTypeTag::INT_UNSIGNED},
        {"timestamp",           ColumnTypeTag::BIGINT},
        {"bigint",              ColumnTypeTag::BIGINT},
        {"bigint unsigned",     ColumnTypeTag::BIGINT_UNSIGNED},
        {"float",               ColumnTypeTag::FLOAT},
        {"double",              ColumnTypeTag::DOUBLE},
        {"decimal",             ColumnTypeTag::DECIMAL},
        {"nchar",               ColumnTypeTag::NCHAR},
        {"varchar",             ColumnTypeTag::VARCHAR},
        {"binary",              ColumnTypeTag::BINARY},
        {"json",                ColumnTypeTag::JSON},
        {"varbinary",           ColumnTypeTag::VARBINARY},
        {"geometry",            ColumnTypeTag::GEOMETRY}
    };

    std::string lower_type = StringUtils::to_lower(type_str);
    auto it = type_tag_map.find(lower_type);
    if (it != type_tag_map.end()) {
        return it->second;
    }
    throw std::runtime_error("Unsupported type: " + lower_type);
}

size_t ColumnConfig::get_type_index() const noexcept{
    static constexpr size_t type_indices[] = {
        0,                                                          // UNKNOWN
        variant_index<bool, ColumnType>::value,                     // BOOL
        variant_index<int8_t, ColumnType>::value,                   // TINYINT
        variant_index<uint8_t, ColumnType>::value,                  // TINYINT_UNSIGNED
        variant_index<int16_t, ColumnType>::value,                  // SMALLINT
        variant_index<uint16_t, ColumnType>::value,                 // SMALLINT_UNSIGNED
        variant_index<int32_t, ColumnType>::value,                  // INT
        variant_index<uint32_t, ColumnType>::value,                 // INT_UNSIGNED
        variant_index<int64_t, ColumnType>::value,                  // BIGINT
        variant_index<uint64_t, ColumnType>::value,                 // BIGINT_UNSIGNED
        variant_index<float, ColumnType>::value,                    // FLOAT
        variant_index<double, ColumnType>::value,                   // DOUBLE
        variant_index<Decimal, ColumnType>::value,                  // DECIMAL
        variant_index<std::u16string, ColumnType>::value,           // NCHAR
        variant_index<std::string, ColumnType>::value,              // VARCHAR
        variant_index<std::string, ColumnType>::value,              // BINARY
        variant_index<JsonValue, ColumnType>::value,                // JSON
        variant_index<std::vector<uint8_t>, ColumnType>::value,     // VARBINARY
        variant_index<Geometry, ColumnType>::value                  // GEOMETRY
    };
    static_assert(sizeof(type_indices)/sizeof(type_indices[0]) == static_cast<size_t>(ColumnTypeTag::MAX), "type_indices size mismatch");

    size_t idx = static_cast<size_t>(type_tag);
    if (idx < sizeof(type_indices)/sizeof(type_indices[0])) {
        return type_indices[idx];
    }

    assert(false && "Invalid type_tag in get_type_index");
    return 0;
}

double ColumnConfig::get_min_value() const noexcept {
    static constexpr double min_values[] = {
        -1.0,                             // UNKNOWN
        0.0,                              // BOOL
        static_cast<double>(INT8_MIN),    // TINYINT
        0.0,                              // TINYINT_UNSIGNED
        static_cast<double>(INT16_MIN),   // SMALLINT
        0.0,                              // SMALLINT_UNSIGNED
        static_cast<double>(INT16_MIN),   // INT
        0.0,                              // INT_UNSIGNED
        static_cast<double>(INT16_MIN),   // BIGINT
        0.0,                              // BIGINT_UNSIGNED
        static_cast<double>(INT16_MIN),   // FLOAT
        static_cast<double>(INT16_MIN),   // DOUBLE
        static_cast<double>(INT16_MIN),   // DECIMAL
        -1.0,                             // NCHAR
        -1.0,                             // VARCHAR
        -1.0,                             // BINARY
        -1.0,                             // JSON
        -1.0,                             // VARBINARY
        -1.0                              // GEOMETRY
    };
    static_assert(sizeof(min_values)/sizeof(min_values[0]) == static_cast<size_t>(ColumnTypeTag::MAX), "min_values size mismatch");

    size_t idx = static_cast<size_t>(type_tag);
    if (idx < sizeof(min_values)/sizeof(min_values[0])) {
        return min_values[idx];
    }
    return -1.0;
}

double ColumnConfig::get_max_value() const noexcept {
    static constexpr double max_values[] = {
        -1.0,                             // UNKNOWN
        1.0,                              // BOOL
        static_cast<double>(INT8_MAX),    // TINYINT
        static_cast<double>(UINT8_MAX),   // TINYINT_UNSIGNED
        static_cast<double>(INT16_MAX),   // SMALLINT
        static_cast<double>(UINT16_MAX),  // SMALLINT_UNSIGNED
        static_cast<double>(INT16_MAX),   // INT
        static_cast<double>(UINT16_MAX),  // INT_UNSIGNED
        static_cast<double>(INT16_MAX),   // BIGINT
        static_cast<double>(UINT16_MAX),  // BIGINT_UNSIGNED
        static_cast<double>(INT16_MAX),   // FLOAT
        static_cast<double>(INT16_MAX),   // DOUBLE
        static_cast<double>(INT16_MAX),   // DECIMAL
        -1.0,                             // NCHAR
        -1.0,                             // VARCHAR
        -1.0,                             // BINARY
        -1.0,                             // JSON
        -1.0,                             // VARBINARY
        -1.0                              // GEOMETRY
    };
    static_assert(sizeof(max_values)/sizeof(max_values[0]) == static_cast<size_t>(ColumnTypeTag::MAX), "max_values size mismatch");

    size_t idx = static_cast<size_t>(type_tag);
    if (idx < sizeof(max_values)/sizeof(max_values[0])) {
        return max_values[idx];
    }
    return -1.0;
}

ColumnConfig::ColumnConfig(
    const std::string& name,
    const std::string& type
) : name(name), type(type) {
    parse_type();
}

ColumnConfig::ColumnConfig(
    const std::string& name,
    const std::string& type,
    std::optional<std::string> gen_type
) : name(name), type(type) {
    parse_type();
    this->gen_type = gen_type;
}

ColumnConfig::ColumnConfig(
    const std::string& name,
    const std::string& type,
    size_t count,
    std::optional<std::string> gen_type
) : name(name), type(type), count(count) {
    parse_type();
    this->gen_type = gen_type;
}

ColumnConfig::ColumnConfig(
    const std::string& name,
    const std::string& type,
    std::optional<std::string> gen_type,
    std::optional<double> min,
    std::optional<double> max
) : name(name), type(type) {
    parse_type();
    this->gen_type = gen_type;
    this->min = min;
    this->max = max;
}

ColumnConfig::ColumnConfig(
    const std::string& name,
    const std::string& type,
    const std::string& ts_precision,
    const std::string& ts_start,
    const std::string& ts_step
) : name(name), type(type) {
    ts.strategy_type = "generator";
    ts.generator = TimestampGeneratorConfig{ts_start, ts_precision, ts_step};
    parse_type();
}

ColumnConfig::ColumnConfig(
    const std::string& name,
    const std::string& type,
    ExpressionTag,
    const std::string& expr
) : name(name), type(type), gen_type("expression"), formula(expr) {
    parse_type();
}

ColumnConfig::ColumnConfig(
    const std::string& name,
    const std::string& type,
    std::vector<std::string> values
) : name(name), type(type) {
    set_values_from_strings(values);
    parse_type();
}

ColumnConfig::ColumnConfig(
    const std::string& name,
    const std::string& type,
    std::vector<double> values
) : name(name), type(type) {
    set_values_from_doubles(values);
    parse_type();
}

void ColumnConfig::parse_type() {
    StringUtils::trim(type);
    std::string lower_type = StringUtils::to_lower(type);

    std::smatch match;
    // varchar/binary/nchar/geometry(len)
    static const std::regex varlen_regex(R"((varchar|binary|nchar|varbinary|geometry)\s*\(\s*(\d+)\s*\))", std::regex::icase);
    // decimal(precision, scale)
    static const std::regex decimal_regex(R"(decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\))", std::regex::icase);

    if (std::regex_match(lower_type, match, varlen_regex)) {
        // Variable length type
        std::string base = match[1].str();
        int len_val = std::stoi(match[2].str());
        type_tag = get_type_tag(base);
        type_index = get_type_index();
        len = len_val;
        if (type_tag == ColumnTypeTag::NCHAR) {
            cap = len_val * 4;
        } else {
            cap = len_val;
        }
        precision.reset();
        scale.reset();
        return;
    }
    if (std::regex_match(lower_type, match, decimal_regex)) {
        // decimal(precision, scale)
        type_tag = ColumnTypeTag::DECIMAL;
        type_index = variant_index<Decimal, ColumnType>::value;
        precision = std::stoi(match[1].str());
        scale = std::stoi(match[2].str());
        len.reset();
        cap.reset();
        return;
    }

    // Compatible with decimal(precision)
    static const std::regex decimal1_regex(R"(decimal\s*\(\s*(\d+)\s*\))", std::regex::icase);
    if (std::regex_match(lower_type, match, decimal1_regex)) {
        type_tag = ColumnTypeTag::DECIMAL;
        type_index = variant_index<Decimal, ColumnType>::value;
        precision = std::stoi(match[1].str());
        scale.reset();
        len.reset();
        cap.reset();
        return;
    }

    // Other types (no parameters)
    type_tag = get_type_tag(lower_type);
    type_index = get_type_index();

    if (type_tag == ColumnTypeTag::VARCHAR
        || type_tag == ColumnTypeTag::BINARY
        || type_tag == ColumnTypeTag::NCHAR
        || type_tag == ColumnTypeTag::VARBINARY
        || type_tag == ColumnTypeTag::GEOMETRY) {

        throw std::runtime_error("Variable length types must specify length: " + lower_type);
    } else if (type_tag == ColumnTypeTag::DECIMAL) {
        // DECIMAL type requires precision and scale
        if (!precision.has_value() || !scale.has_value()) {
            throw std::runtime_error("DECIMAL type must specify precision and scale: " + lower_type);
        }
    } else if (type_tag == ColumnTypeTag::JSON) {
        len = 200;
        cap = len;
    } else {
        len.reset();
        cap.reset();
    }

    precision.reset();
    scale.reset();
}

bool ColumnConfig::is_var_length() const noexcept {
    static constexpr bool varlen_flags[] = {
        false, // UNKNOWN
        false, // BOOL
        false, // TINYINT
        false, // TINYINT_UNSIGNED
        false, // SMALLINT
        false, // SMALLINT_UNSIGNED
        false, // INT
        false, // INT_UNSIGNED
        false, // BIGINT
        false, // BIGINT_UNSIGNED
        false, // FLOAT
        false, // DOUBLE
        false, // DECIMAL
        true,  // NCHAR
        true,  // VARCHAR
        true,  // BINARY
        true,  // JSON
        true,  // VARBINARY
        true   // GEOMETRY
    };
    static_assert(sizeof(varlen_flags)/sizeof(varlen_flags[0]) == static_cast<size_t>(ColumnTypeTag::MAX), "varlen_flags size mismatch");

    size_t idx = static_cast<size_t>(type_tag);
    if (idx < sizeof(varlen_flags)/sizeof(varlen_flags[0])) {
        return varlen_flags[idx];
    }
    return false;
}

size_t ColumnConfig::get_fixed_type_size() const noexcept{
    // DECIMAL special handling
    if (type_tag == ColumnTypeTag::DECIMAL) {
        if (precision && *precision <= 18)
            return sizeof(int64_t);      // DECIMAL64
        else
            return sizeof(int64_t) * 2;  // DECIMAL128
    }

    // Static lookup table
    static constexpr size_t type_sizes[] = {
        0,                 // UNKNOWN
        sizeof(bool),      // BOOL
        sizeof(int8_t),    // TINYINT
        sizeof(int8_t),    // TINYINT_UNSIGNED
        sizeof(int16_t),   // SMALLINT
        sizeof(int16_t),   // SMALLINT_UNSIGNED
        sizeof(int32_t),   // INT
        sizeof(int32_t),   // INT_UNSIGNED
        sizeof(int64_t),   // BIGINT
        sizeof(int64_t),   // BIGINT_UNSIGNED
        sizeof(float),     // FLOAT
        sizeof(double),    // DOUBLE
        0,                 // DECIMAL (handled separately)
        0,                 // NCHAR (variable length)
        0,                 // VARCHAR (variable length)
        0,                 // BINARY (variable length)
        0,                 // JSON (variable length)
        0,                 // VARBINARY (variable length)
        0                  // GEOMETRY (variable length)
    };
    static_assert(sizeof(type_sizes)/sizeof(type_sizes[0]) == static_cast<size_t>(ColumnTypeTag::MAX), "type_sizes size mismatch");

    size_t idx = static_cast<size_t>(type_tag);
    if (idx < sizeof(type_sizes)/sizeof(type_sizes[0]) && type_sizes[idx] > 0) {
        return type_sizes[idx];
    }
    return 0;
}

// Convert to TDengine internal type
int ColumnConfig::get_taos_type() const noexcept {
    // DECIMAL special handling
    if (type_tag == ColumnTypeTag::DECIMAL) {
        return (precision && *precision <= 18) ?
            TSDB_DATA_TYPE_DECIMAL64 :
            TSDB_DATA_TYPE_DECIMAL;
    }

    // Static lookup table
    static constexpr int taos_types[] = {
        TSDB_DATA_TYPE_NULL,      // UNKNOWN
        TSDB_DATA_TYPE_BOOL,      // BOOL
        TSDB_DATA_TYPE_TINYINT,   // TINYINT
        TSDB_DATA_TYPE_UTINYINT,  // TINYINT_UNSIGNED
        TSDB_DATA_TYPE_SMALLINT,  // SMALLINT
        TSDB_DATA_TYPE_USMALLINT, // SMALLINT_UNSIGNED
        TSDB_DATA_TYPE_INT,       // INT
        TSDB_DATA_TYPE_UINT,      // INT_UNSIGNED
        TSDB_DATA_TYPE_BIGINT,    // BIGINT
        TSDB_DATA_TYPE_UBIGINT,   // BIGINT_UNSIGNED
        TSDB_DATA_TYPE_FLOAT,     // FLOAT
        TSDB_DATA_TYPE_DOUBLE,    // DOUBLE
        0,                        // DECIMAL (handled separately)
        TSDB_DATA_TYPE_NCHAR,     // NCHAR
        TSDB_DATA_TYPE_VARCHAR,   // VARCHAR
        TSDB_DATA_TYPE_BINARY,    // BINARY (treated as VARCHAR in TDengine)
        TSDB_DATA_TYPE_JSON,      // JSON
        TSDB_DATA_TYPE_VARBINARY, // VARBINARY
        TSDB_DATA_TYPE_GEOMETRY   // GEOMETRY
    };
    static_assert(sizeof(taos_types)/sizeof(taos_types[0]) == static_cast<size_t>(ColumnTypeTag::MAX), "taos_types size mismatch");

    size_t idx = static_cast<size_t>(type_tag);
    if (idx < sizeof(taos_types)/sizeof(taos_types[0]) && taos_types[idx] != 0) {
        return taos_types[idx];
    }
    return TSDB_DATA_TYPE_NULL;
}

void ColumnConfig::set_values_from_strings(const std::vector<std::string>& values) {
    str_values = values;
    values_count = str_values.size();
    dbl_values.clear();

    if (type_tag == ColumnTypeTag::BOOL) {
        for (const auto& val : str_values) {
            std::string lower = StringUtils::to_lower(val);

            if (lower != "true" && lower != "false" && lower != "0" && lower != "1") {
                throw std::runtime_error("Invalid boolean value in 'values' for column: " + name);
            }

            dbl_values.push_back((lower == "true" || lower == "1") ? 1.0 : 0.0);
        }
    }
}

void ColumnConfig::set_values_from_doubles(const std::vector<double>& values) {
    dbl_values = values;
    values_count = dbl_values.size();
    str_values.clear();
}