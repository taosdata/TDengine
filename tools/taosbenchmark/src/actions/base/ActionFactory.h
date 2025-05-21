#pragma once
#include <memory>
#include "ActionBase.h"
#include "CreateDatabaseAction.h"
#include "CreateSuperTableAction.h"
#include "CreateChildTableAction.h"
#include "InsertDataAction.h"
#include "QueryDataAction.h"
#include "SubscribeDataAction.h"
#include "parameter/ConfigData.h"


class ActionFactory {
public:
    static std::unique_ptr<ActionBase> createAction(
        const Step& step,
        const ActionConfigVariant& config) 
    {
        if (step.uses == "actions/create-database") {
            return std::make_unique<CreateDatabaseAction>(
                std::get<CreateDatabaseConfig>(config));
        } else if (step.uses == "actions/create-super-table") {
            return std::make_unique<CreateSuperTableAction>(
                std::get<CreateSuperTableConfig>(config));
        } else if (step.uses == "actions/create-child-table") {
            return std::make_unique<CreateChildTableAction>(
                std::get<CreateChildTableConfig>(config));
        } else if (step.uses == "actions/insert-data") {
            return std::make_unique<InsertDataAction>(
                std::get<InsertDataConfig>(config));
        } else if (step.uses == "actions/query-data") {
            return std::make_unique<QueryDataAction>(
                std::get<QueryDataConfig>(config));
        } else if (step.uses == "actions/subscribe-data") {
            return std::make_unique<SubscribeDataAction>(
                std::get<SubscribeDataConfig>(config));
        }

        throw std::invalid_argument("Unsupported action type: " + step.uses);
    }
};