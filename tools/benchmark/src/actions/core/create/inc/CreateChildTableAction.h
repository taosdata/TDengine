#pragma once
#include <iostream>
#include "ActionBase.h"
#include "ActionFactory.h"
#include "CreateChildTableConfig.h"
#include "DatabaseConnector.h"


class CreateChildTableAction : public ActionBase {
public:
    explicit CreateChildTableAction(const CreateChildTableConfig& config) : config_(config) {}

    void execute() override;

private:
    CreateChildTableConfig config_;

    // 注册 CreateChildTableAction 到 ActionFactory
    inline static bool registered_ = []() {
        ActionFactory::instance().register_action(
            "actions/create-child-table",
            [](const ActionConfigVariant& config) {
                return std::make_unique<CreateChildTableAction>(std::get<CreateChildTableConfig>(config));
            });
        return true;
    }();
};