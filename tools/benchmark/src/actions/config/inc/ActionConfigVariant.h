#pragma once

#include "CreateDatabaseConfig.h"
#include "CreateSuperTableConfig.h"
#include "CreateChildTableConfig.h"
#include "InsertDataConfig.h"
#include "QueryDataConfig.h"
#include "SubscribeDataConfig.h"

using ActionConfigVariant = std::variant<
    std::monostate,
    CreateDatabaseConfig,
    CreateSuperTableConfig,
    CreateChildTableConfig,
    InsertDataConfig,
    QueryDataConfig,
    SubscribeDataConfig
>;