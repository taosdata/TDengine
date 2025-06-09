#include "ColumnConfig.h"
#include <stdexcept>

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