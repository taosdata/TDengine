#pragma once

#include <iostream>
#include "ActionBase.hpp"
#include "ActionFactory.hpp"
#include "CreateDatabaseConfig.hpp"
#include "DatabaseConnector.hpp"


class CreateDatabaseAction : public ActionBase {
public:
    explicit CreateDatabaseAction(const GlobalConfig& global, const CreateDatabaseConfig& config) : global_(global), config_(config) {}

    void execute() override;

private:
    const GlobalConfig& global_;
    CreateDatabaseConfig config_;

    std::unique_ptr<DatabaseConnector> connector_;

    void prepare_connector();

    // Register CreateDatabaseAction to ActionFactory
    inline static bool registered_ = []() {
        ActionFactory::instance().register_action(
            "tdengine/create-database",
            [](const GlobalConfig& global, const ActionConfigVariant& config) {
                return std::make_unique<CreateDatabaseAction>(global, std::get<CreateDatabaseConfig>(config));
            });
        return true;
    }();
};