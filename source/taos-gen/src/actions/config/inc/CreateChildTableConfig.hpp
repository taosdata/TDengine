#pragma once

#include "TDengineConfig.hpp"
#include "SchemaConfig.hpp"
#include "ChildTableInfo.hpp"

struct CreateChildTableConfig {
    TDengineConfig tdengine;
    SchemaConfig schema;

    struct BatchConfig {
        int size = 1000;       // Number of child tables per batch
        int concurrency = 10;  // Number of concurrent batches
    } batch;
};