#ifndef TAGS_CONFIG_H
#define TAGS_CONFIG_H


#include "SuperTableInfo.h"
#include <string>
#include <vector>


struct TagsConfig {
    std::string source_type; // 数据来源类型：generator 或 csv

    struct Generator {
        std::vector<SuperTableInfo::Column> schema; // 标签列的 Schema 定义
    } generator;

    struct CSV {
        std::string file_path;
        bool has_header = true;
        std::string delimiter = ",";
        int exclude_index = -1; // 默认不剔除任何列
    } csv;
};

#endif // TAGS_CONFIG_H