#pragma once

#include "CreateDatabaseConfig.hpp"
#include "CreateSuperTableConfig.hpp"
#include "CreateChildTableConfig.hpp"
#include "InsertDataConfig.hpp"
#include "QueryDataConfig.hpp"
#include "SubscribeDataConfig.hpp"
#include "CheckpointActionConfig.hpp"

using ActionConfigVariant = std::variant<
    std::monostate,
    CreateDatabaseConfig,
    CreateSuperTableConfig,
    CreateChildTableConfig,
    InsertDataConfig,
    QueryDataConfig,
    SubscribeDataConfig,
    CheckpointActionConfig
>;
