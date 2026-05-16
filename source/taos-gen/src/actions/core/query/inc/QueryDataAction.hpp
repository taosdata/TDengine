#pragma once
#include "ActionBase.hpp"
#include "ActionFactory.hpp"
#include "QueryDataConfig.hpp"
#include "LogUtils.hpp"
#include <iostream>

class QueryDataAction : public ActionBase {
public:
    explicit QueryDataAction(const GlobalConfig& global, const QueryDataConfig& config) : global_(global), config_(config) {}

    void execute() override {
        LogUtils::info("Querying data from database: {}", config_.source.connection_info.host);

        // Implement the specific data query logic here
    }

private:
    const GlobalConfig& global_;
    QueryDataConfig config_;

    // Register QueryDataAction to ActionFactory
    inline static bool registered_ = []() {
        ActionFactory::instance().register_action(
            "tdengine/query",
            [](const GlobalConfig& global, const ActionConfigVariant& config) {
                return std::make_unique<QueryDataAction>(global, std::get<QueryDataConfig>(config));
            });
        return true;
    }();
};