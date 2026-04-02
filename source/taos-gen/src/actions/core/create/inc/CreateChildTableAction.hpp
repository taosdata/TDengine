#pragma once
#include <iostream>
#include "ActionBase.hpp"
#include "ActionFactory.hpp"
#include "CreateChildTableConfig.hpp"
#include "DatabaseConnector.hpp"


class CreateChildTableAction : public ActionBase {
public:
    explicit CreateChildTableAction(const GlobalConfig& global, const CreateChildTableConfig& config) : global_(global), config_(config) {}

    void execute() override;

private:
    const GlobalConfig& global_;
    CreateChildTableConfig config_;

    // Register CreateChildTableAction to ActionFactory
    inline static bool registered_ = []() {
        ActionFactory::instance().register_action(
            "tdengine/create-child-table",
            [](const GlobalConfig& global, const ActionConfigVariant& config) {
                return std::make_unique<CreateChildTableAction>(global, std::get<CreateChildTableConfig>(config));
            });
        return true;
    }();
};