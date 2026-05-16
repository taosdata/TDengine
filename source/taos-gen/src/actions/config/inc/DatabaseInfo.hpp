#pragma once

#include <string>
#include <optional>

struct DatabaseInfo {
    std::string name;
    std::string precision = "ms"; // Default time precision is milliseconds
    bool drop_if_exists = true;
    std::optional<std::string> properties;
};