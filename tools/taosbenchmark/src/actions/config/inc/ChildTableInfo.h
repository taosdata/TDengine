#ifndef CHILD_TABLE_INFO_H
#define CHILD_TABLE_INFO_H

#include "TableNameConfig.h"
#include "TagsConfig.h"

struct ChildTableInfo {
    TableNameConfig table_name;     // 子表名称配置
    TagsConfig tags;                // 标签配置
};

#endif // CHILD_TABLE_INFO_H