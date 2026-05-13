#include "StepExecutionStrategy.hpp"
#include "ActionFactory.hpp"
#include "CreateDatabaseAction.hpp"
#include "CreateSuperTableAction.hpp"
#include "CreateChildTableAction.hpp"
#include "InsertDataAction.hpp"
#include "QueryDataAction.hpp"
#include "SubscribeDataAction.hpp"
#include "LogUtils.hpp"
#include <iostream>
#include <mutex>


// Implementation of production environment strategy
bool ProductionStepStrategy::execute(const Step& step) {
    try {
        LogUtils::info("Executing step: {} ({})", step.name, step.uses);

        auto action = ActionFactory::instance().create_action(global_, step.uses, step.action_config);
        action->execute();

        LogUtils::info("Step completed: {}", step.name);
        return true;

    } catch (const std::exception& e) {
        LogUtils::error("Failed to execute step: {}, reason: \"{}\"", step.name, e.what());
        return false;
    }
}


// Implementation of debug environment strategy
static std::mutex log_mutex;
bool DebugStepStrategy::execute(const Step& step) {
    std::lock_guard<std::mutex> lock(log_mutex);

    LogUtils::info("Executing step: {} ({})", step.name, step.uses);

    if (step.uses == "tdengine/create-database") {
        LogUtils::info("Action type: Create Database");
    } else if (step.uses == "tdengine/create-super-table") {
        LogUtils::info("Action type: Create Super Table");
    } else if (step.uses == "tdengine/create-child-table") {
        LogUtils::info("Action type: Create Child Table");
    } else if (step.uses == "tdengine/insert") {
        LogUtils::info("Action type: Insert Data");
    } else if (step.uses == "tdengine/query") {
        LogUtils::info("Action type: Query Data");
    } else if (step.uses == "tdengine/subscribe") {
        LogUtils::info("Action type: Subscribe Data");
    } else {
        LogUtils::error("Unknown action type: {}", step.uses);
        return false;
    }

    LogUtils::info("Step completed: {}", step.name);
    return true;
}