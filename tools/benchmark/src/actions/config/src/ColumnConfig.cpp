#include "ColumnConfig.h"
#include <stdexcept>
#include "taos.h"

ColumnTypeTag ColumnConfig::get_type_tag(const std::string& type_str) {
    std::string lower_type = StringUtils::to_lower(type_str);
    if (lower_type == "bool")
        return ColumnTypeTag::BOOL;
    if (lower_type == "tinyint")
        return ColumnTypeTag::TINYINT;
    if (lower_type == "tinyint unsigned")
        return ColumnTypeTag::TINYINT_UNSIGNED;
    if (lower_type == "smallint")
        return ColumnTypeTag::SMALLINT;
    if (lower_type == "smallint unsigned")
        return ColumnTypeTag::SMALLINT_UNSIGNED;
    if (lower_type == "int")
        return ColumnTypeTag::INT;
    if (lower_type == "int unsigned")
        return ColumnTypeTag::INT_UNSIGNED;
    if (lower_type == "bigint")
        return ColumnTypeTag::BIGINT;
    if (lower_type == "bigint unsigned")
        return ColumnTypeTag::BIGINT_UNSIGNED;
    if (lower_type == "float")
        return ColumnTypeTag::FLOAT;
    if (lower_type == "double")
        return ColumnTypeTag::DOUBLE;
    if (lower_type == "decimal")
        return ColumnTypeTag::DECIMAL;
    if (lower_type == "nchar")
        return ColumnTypeTag::NCHAR;
    if (lower_type == "varchar" || lower_type == "binary") 
        return ColumnTypeTag::VARCHAR; // std::string
    if (lower_type == "json") 
        return ColumnTypeTag::JSON; // std::string (json)
    if (lower_type == "varbinary")
        return ColumnTypeTag::VARBINARY;
    if (lower_type == "geometry")
        return ColumnTypeTag::GEOMETRY;
    throw std::runtime_error("Unsupported type: " + lower_type);
}

ColumnConfig::ColumnConfig(
    const std::string& name,
    const std::string& type
) : name(name), type(type) {
    type_tag = get_type_tag(type);
}

ColumnConfig::ColumnConfig(
    const std::string& name,
    const std::string& type,
    std::optional<std::string> gen_type
) : name(name), type(type), gen_type(gen_type) {
    type_tag = get_type_tag(type);
}

ColumnConfig::ColumnConfig(
    const std::string& name,
    const std::string& type,
    std::optional<std::string> gen_type,
    std::optional<int> len
) : name(name), type(type), gen_type(gen_type), len(len) {
    type_tag = get_type_tag(type);
}

ColumnConfig::ColumnConfig(
    const std::string& name,
    const std::string& type,
    std::optional<std::string> gen_type,
    std::optional<double> min,
    std::optional<double> max
) : name(name), type(type), gen_type(gen_type), min(min), max(max) {
    type_tag = get_type_tag(type);
}

void ColumnConfig::calc_type_tag() {
    type_tag = get_type_tag(type);
}

bool ColumnConfig::is_var_length() const noexcept {
    switch (type_tag) {
        case ColumnTypeTag::VARCHAR:
        case ColumnTypeTag::BINARY:
        case ColumnTypeTag::NCHAR:
        case ColumnTypeTag::VARBINARY:
        case ColumnTypeTag::GEOMETRY:
        case ColumnTypeTag::JSON:
            return true;
        default:
            return false;
    }
}


size_t ColumnConfig::get_fixed_type_size() const {
    switch (type_tag) {
        case ColumnTypeTag::BOOL:
            return sizeof(bool);
        case ColumnTypeTag::TINYINT:
        case ColumnTypeTag::TINYINT_UNSIGNED:
            return sizeof(int8_t);
        case ColumnTypeTag::SMALLINT:
        case ColumnTypeTag::SMALLINT_UNSIGNED:
            return sizeof(int16_t);
        case ColumnTypeTag::INT:
        case ColumnTypeTag::INT_UNSIGNED:
            return sizeof(int32_t);
        case ColumnTypeTag::BIGINT:
        case ColumnTypeTag::BIGINT_UNSIGNED:
            return sizeof(int64_t);
        case ColumnTypeTag::FLOAT:
            return sizeof(float);
        case ColumnTypeTag::DOUBLE:
            return sizeof(double);
        case ColumnTypeTag::DECIMAL:
            return (precision && *precision <= 18) ? 
                   sizeof(int64_t) :     // DECIMAL64
                   sizeof(int64_t) * 2;  // DECIMAL128
        default:
            throw std::runtime_error("Not a fixed-length type: " + 
                std::to_string(static_cast<int>(type_tag)));
    }
}

// Convert to TDengine internal type
int ColumnConfig::get_taos_type() const noexcept{
    switch (type_tag) {
        case ColumnTypeTag::BOOL:
            return TSDB_DATA_TYPE_BOOL;
        case ColumnTypeTag::TINYINT:
            return TSDB_DATA_TYPE_TINYINT;
        case ColumnTypeTag::TINYINT_UNSIGNED:
            return TSDB_DATA_TYPE_UTINYINT;
        case ColumnTypeTag::SMALLINT:
            return TSDB_DATA_TYPE_SMALLINT;
        case ColumnTypeTag::SMALLINT_UNSIGNED:
            return TSDB_DATA_TYPE_USMALLINT;
        case ColumnTypeTag::INT:
            return TSDB_DATA_TYPE_INT;
        case ColumnTypeTag::INT_UNSIGNED:
            return TSDB_DATA_TYPE_UINT;
        case ColumnTypeTag::BIGINT:
            return TSDB_DATA_TYPE_BIGINT;
        case ColumnTypeTag::BIGINT_UNSIGNED:
            return TSDB_DATA_TYPE_UBIGINT;
        case ColumnTypeTag::FLOAT:
            return TSDB_DATA_TYPE_FLOAT;
        case ColumnTypeTag::DOUBLE:
            return TSDB_DATA_TYPE_DOUBLE;
        case ColumnTypeTag::DECIMAL:
            return (precision && *precision <= 18) ? 
                    TSDB_DATA_TYPE_DECIMAL64 : 
                    TSDB_DATA_TYPE_DECIMAL;
        case ColumnTypeTag::NCHAR:
            return TSDB_DATA_TYPE_NCHAR;
        case ColumnTypeTag::VARCHAR:
        case ColumnTypeTag::BINARY:
            return TSDB_DATA_TYPE_VARCHAR;
        case ColumnTypeTag::JSON:
            return TSDB_DATA_TYPE_JSON;
        case ColumnTypeTag::VARBINARY:
            return TSDB_DATA_TYPE_VARBINARY;
        case ColumnTypeTag::GEOMETRY:
            return TSDB_DATA_TYPE_GEOMETRY;
        default:
            return TSDB_DATA_TYPE_NULL;
    }
}
