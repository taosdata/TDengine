#pragma once

#include <string>
#include "SchemaConfig.hpp"
#include "DatabaseInfo.hpp"
#include "SuperTableInfo.hpp"
#include "DataFormat.hpp"
#include "DataChannel.hpp"
#include "PluginExtensions.hpp"

struct GlobalConfig {
    bool confirm_prompt = false;
    bool verbose = false;
    std::string log_dir = "log/";
    std::string log_file = "";
    std::string cfg_dir = "/etc/taos/";
    std::string yaml_cfg_dir = "./";
    // TDengineConfig connection_info;
    DataFormat data_format;
    DataChannel data_channel;
    DatabaseInfo database_info;
    SuperTableInfo super_table_info;

    SchemaConfig schema;
    PluginExtensions extensions;
};
