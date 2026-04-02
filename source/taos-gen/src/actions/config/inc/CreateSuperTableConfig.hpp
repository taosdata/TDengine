#pragma once

#include "TDengineConfig.hpp"
#include "SchemaConfig.hpp"

struct CreateSuperTableConfig {
    TDengineConfig tdengine;
    SchemaConfig schema;
};
