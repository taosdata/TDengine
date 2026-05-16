#pragma once

#include "TDengineConfig.hpp"
#include "CheckpointInfo.hpp"

struct CreateDatabaseConfig {
    TDengineConfig tdengine;
    CheckpointInfo checkpoint_info;
};
