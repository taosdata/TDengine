#ifndef DATA_FORMAT_H
#define DATA_FORMAT_H

#include <string>

struct DataFormat {
    std::string format_type = "sql";

    struct StmtConfig {
        std::string version = "v2"; // "v1" or "v2"
    } stmt_config;

    struct SchemalessConfig {
        std::string protocol = "line"; // "line", "telnet", "json", or "taos-json"
    } schemaless_config;

    struct CSVConfig {
        std::string delimiter = ",";          // Default delimiter is a comma
        std::string quote_character = "\"";   // Default quote character
        std::string escape_character = "\\"; // Default escape character
    } csv_config;
};

#endif // DATA_FORMAT_H