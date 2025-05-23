#ifndef STEP_H
#define STEP_H

#include <string>
#include <variant>
#include <yaml-cpp/yaml.h>
#include "CreateDatabaseConfig.h"
#include "CreateSuperTableConfig.h"
#include "CreateChildTableConfig.h"
#include "InsertDataConfig.h"
#include "QueryDataConfig.h"
#include "SubscribeDataConfig.h"

using ActionConfigVariant = std::variant<
    std::monostate,
    CreateDatabaseConfig,
    CreateSuperTableConfig,
    CreateChildTableConfig,
    InsertDataConfig,
    QueryDataConfig,
    SubscribeDataConfig
>;

struct Step {
    std::string name; // 步骤名称
    std::string uses; // 使用的操作类型
    YAML::Node with;  // 原始参数配置
    ActionConfigVariant action_config; // 泛化字段，用于存储不同类型的 Action 配置
};

#endif // STEP_H