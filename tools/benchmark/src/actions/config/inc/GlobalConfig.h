#ifndef GLOBAL_CONFIG_H
#define GLOBAL_CONFIG_H

#include <string>
#include "ConnectionInfo.h"
#include "DatabaseInfo.h"
#include "SuperTableInfo.h"
#include "DataFormat.h"
#include "DataChannel.h"

struct GlobalConfig {
    bool confirm_prompt = false;
    std::string log_dir = "log/";
    std::string cfg_dir = "/etc/taos/";
    ConnectionInfo connection_info;
    DataFormat data_format;
    DataChannel data_channel;
    DatabaseInfo database_info;
    SuperTableInfo super_table_info;
};

#endif // GLOBAL_CONFIG_H