#ifndef DATABASE_INFO_H
#define DATABASE_INFO_H

#include <string>
#include <optional>

struct DatabaseInfo {
    std::string name;
    std::string precision = "ms"; // 默认时间精度为毫秒
    bool drop_if_exists = true;
    std::optional<std::string> properties;
};

#endif // DATABASE_INFO_H