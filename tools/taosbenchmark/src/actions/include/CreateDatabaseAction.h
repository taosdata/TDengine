#pragma once

#include <iostream>
#include "ActionBase.h"
#include "ActionFactory.h"
#include "CreateDatabaseConfig.h"


class CreateDatabaseAction : public ActionBase {
public:
    explicit CreateDatabaseAction(const CreateDatabaseConfig& config) : config_(config) {}

    void execute() override;

private:
    CreateDatabaseConfig config_;

    // 注册 CreateDatabaseAction 到 ActionFactory
    inline static bool registered_ = []() {
        ActionFactory::instance().register_action(
            "actions/create-database",
            [](const ActionConfigVariant& config) {
                return std::make_unique<CreateDatabaseAction>(std::get<CreateDatabaseConfig>(config));
            });
        return true;
    }();
};
