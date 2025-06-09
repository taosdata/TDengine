#ifndef CREATE_CHILD_TABLE_CONFIG_H
#define CREATE_CHILD_TABLE_CONFIG_H

#include "ConnectionInfo.h"
#include "DataFormat.h"
#include "DataChannel.h"
#include "DatabaseInfo.h"
#include "SuperTableInfo.h"
#include "ChildTableInfo.h"

struct CreateChildTableConfig {
    ConnectionInfo connection_info;  // 数据库连接信息
    DataFormat data_format;
    DataChannel data_channel;
    DatabaseInfo database_info;      // 数据库信息
    SuperTableInfo super_table_info; // 超级表信息
    ChildTableInfo child_table_info; // 子表信息

    struct BatchConfig {
        int size = 1000;       // 每批创建的子表数量
        int concurrency = 10;  // 并发执行的批次数量
    } batch;
};

#endif // CREATE_CHILD_TABLE_CONFIG_H