#ifndef TABLE_NAME_CONFIG_H
#define TABLE_NAME_CONFIG_H

#include <string>

struct TableNameConfig {
    std::string source_type; // 数据来源类型：generator 或 csv

    struct Generator {
        std::string prefix;
        int count;
        int from = 0; // 默认起始下标为 0
    } generator;

    struct CSV {
        std::string file_path;
        bool has_header = true;
        std::string delimiter = ",";
        int column_index = 0;
    } csv;
};

#endif // TABLE_NAME_CONFIG_H