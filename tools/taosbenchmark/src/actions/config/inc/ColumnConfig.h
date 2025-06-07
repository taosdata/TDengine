#pragma once

#include <string>
#include <vector>
#include <optional>
#include <cstdint>
#include "ColumnType.h"


struct ColumnConfig {
    std::string name;
    std::string type;
    ColumnTypeTag type_tag;
    bool primary_key = false;
    std::optional<int> len;
    int count = 1;
    std::optional<int> precision;
    std::optional<int> scale;
    std::optional<std::string> properties;
    std::optional<std::string> gen_type;
    std::optional<float> null_ratio;

    // Attributes for gen_type=random
    std::optional<double> min;
    std::optional<double> max;
    std::optional<std::string> dec_min;
    std::optional<std::string> dec_max;
    std::optional<std::string> corpus;
    std::optional<bool> chinese;
    std::optional<std::vector<std::string>> values;

    // Attributes for gen_type=order
    std::optional<int64_t> order_min;
    std::optional<int64_t> order_max;

    // Attributes for gen_type=function
    struct FunctionConfig {
        std::string expression; // 完整的函数表达式
        std::string function;   // 函数名，例如 sinusoid、counter 等
        double multiple = 1.0;  // 倍率
        double addend = 0.0;    // 加数
        int random = 0;         // 随机部分的范围
        double base = 0.0;      // 基值
        std::optional<double> min; // 函数参数：最小值
        std::optional<double> max; // 函数参数：最大值
        std::optional<int> period; // 函数参数：周期
        std::optional<int> offset; // 函数参数：偏移量
    };
    std::optional<FunctionConfig> function_config;


    static ColumnTypeTag get_type_tag(const std::string& type_str) {
        if (type_str == "bool")
            return ColumnTypeTag::BOOL;
        if (type_str == "tinyint")
            return ColumnTypeTag::TINYINT;
        if (type_str == "tinyint unsigned")
            return ColumnTypeTag::TINYINT_UNSIGNED;
        if (type_str == "smallint")
            return ColumnTypeTag::SMALLINT;
        if (type_str == "smallint unsigned")
            return ColumnTypeTag::SMALLINT_UNSIGNED;
        if (type_str == "int")
            return ColumnTypeTag::INT;
        if (type_str == "int unsigned")
            return ColumnTypeTag::INT_UNSIGNED;
        if (type_str == "bigint")
            return ColumnTypeTag::BIGINT;
        if (type_str == "bigint unsigned")
            return ColumnTypeTag::BIGINT_UNSIGNED;
        if (type_str == "float")
            return ColumnTypeTag::FLOAT;
        if (type_str == "double")
            return ColumnTypeTag::DOUBLE;
        if (type_str == "decimal")
            return ColumnTypeTag::DECIMAL;
        if (type_str == "nchar")
            return ColumnTypeTag::NCHAR;
        if (type_str == "varchar" || type_str == "binary") 
            return ColumnTypeTag::VARCHAR; // std::string
        if (type_str == "json") 
            return ColumnTypeTag::JSON; // std::string (json)
        if (type_str == "varbinary")
            return ColumnTypeTag::VARBINARY;
        if (type_str == "geometry")
            return ColumnTypeTag::GEOMETRY;
        throw std::runtime_error("Unsupported type: " + type_str);
    }

    ColumnConfig() = default;

    ColumnConfig(
        const std::string& name,
        const std::string& type,
        std::optional<std::string> gen_type
    ) : name(name), type(type), gen_type(gen_type) {
        type_tag = get_type_tag(type);
    }

    ColumnConfig(
        const std::string& name,
        const std::string& type,
        std::optional<std::string> gen_type,
        std::optional<int> len
    ) : name(name), type(type), gen_type(gen_type), len(len) {
        type_tag = get_type_tag(type);
    }

    ColumnConfig(
        const std::string& name,
        const std::string& type,
        std::optional<std::string> gen_type,
        std::optional<double> min,
        std::optional<double> max
    ) : name(name), type(type), gen_type(gen_type), min(min), max(max) {
        type_tag = get_type_tag(type);
    }

    void calc_type_tag() {
        type_tag = get_type_tag(type);
    }   
};
