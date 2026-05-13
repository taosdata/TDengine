#pragma once
#include <iostream>
#include "ActionBase.hpp"
#include "ActionFactory.hpp"
#include "CreateSuperTableConfig.hpp"
#include "DatabaseConnector.hpp"

class CreateSuperTableAction : public ActionBase {
public:
    explicit CreateSuperTableAction(const GlobalConfig& global, const CreateSuperTableConfig& config) : global_(global), config_(config) {}

    void execute() override;

private:
    const GlobalConfig& global_;
    CreateSuperTableConfig config_;

    std::unique_ptr<DatabaseConnector> connector_;

    void prepare_connector();

    // Register CreateSuperTableAction to ActionFactory
    inline static bool registered_ = []() {
        ActionFactory::instance().register_action(
            "tdengine/create-super-table",
            [](const GlobalConfig& global, const ActionConfigVariant& config) {
                return std::make_unique<CreateSuperTableAction>(global, std::get<CreateSuperTableConfig>(config));
            });
        return true;
    }();
};