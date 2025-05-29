#pragma once

#include <string>
#include <vector>
#include <optional>
#include <cstdint>
#include "ColumnType.h"


struct ColumnConfig {
    std::string name;
    std::string type;
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

    ColumnConfig() = default;

    ColumnConfig(
        const std::string& name,
        const std::string& type,
        std::optional<std::string> gen_type
    ) : name(name), type(type), gen_type(gen_type) {}

    ColumnConfig(
        const std::string& name,
        const std::string& type,
        std::optional<std::string> gen_type,
        std::optional<int> len
    ) : name(name), type(type), gen_type(gen_type), len(len) {}

    ColumnConfig(
        const std::string& name,
        const std::string& type,
        std::optional<std::string> gen_type,
        std::optional<double> min,
        std::optional<double> max
    ) : name(name), type(type), gen_type(gen_type), min(min), max(max) {}

};
