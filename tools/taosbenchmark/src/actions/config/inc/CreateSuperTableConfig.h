#ifndef CREATE_SUPER_TABLE_CONFIG_H
#define CREATE_SUPER_TABLE_CONFIG_H

#include "ConnectionInfo.h"
#include "DatabaseInfo.h"
#include "SuperTableInfo.h"

struct CreateSuperTableConfig {
    ConnectionInfo connection_info;
    DatabaseInfo database_info;
    SuperTableInfo super_table_info;
};

#endif // CREATE_SUPER_TABLE_CONFIG_H