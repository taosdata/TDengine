#pragma once

#include <string>
#include <variant>
#include <yaml-cpp/yaml.h>
#include "ActionConfigVariant.hpp"

struct Step {
    std::string name; // Step name
    std::string uses; // Action type used
    YAML::Node with;  // Original parameter config
    ActionConfigVariant action_config; // Generalized field for different Action config types
};