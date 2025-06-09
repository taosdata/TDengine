#pragma once

#include <string>
#include <variant>
#include <yaml-cpp/yaml.h>
#include "ActionConfigVariant.h"


struct Step {
    std::string name; // 步骤名称
    std::string uses; // 使用的操作类型
    YAML::Node with;  // 原始参数配置
    ActionConfigVariant action_config; // 泛化字段，用于存储不同类型的 Action 配置
};