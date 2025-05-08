#ifndef COMMON_CONFIG_H
#define COMMON_CONFIG_H

#include <string>
#include <vector>
#include <map>
#include <optional>
#include <variant>



// 连接信息
struct ConnectionConfig {
    std::string host = "localhost";
    int port = 6030;
    std::string user = "root";
    std::string password = "taosdata";
};

// 数据格式化配置
struct DataFormatConfig {
    std::string format_type = "sql"; // "sql", "stmt", "schemaless"

    struct StmtConfig {
        std::string version = "v2"; // "v1", "v2"
    };

    struct SchemalessConfig {
        std::string protocol = "line"; // "line", "telnet", "json", "taos-json"
    };

    std::optional<StmtConfig> stmt;
    std::optional<SchemalessConfig> schemaless;
};

// 数据通道配置
struct DataChannelConfig {
    std::string channel_type = "native"; // "native", "websocket", "restful"
};

#endif // COMMON_CONFIG_H