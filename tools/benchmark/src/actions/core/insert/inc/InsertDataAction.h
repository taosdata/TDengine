#pragma once
#include <iostream>
#include "ActionBase.h"
#include "ActionFactory.h"
#include "InsertDataConfig.h"
#include "DatabaseConnector.h"


class InsertDataAction : public ActionBase {
public:
    explicit InsertDataAction(const InsertDataConfig& config) : config_(config) {}

    void execute() override;

private:
    InsertDataConfig config_;

    // 注册 InsertDataAction 到 ActionFactory
    inline static bool registered_ = []() {
        ActionFactory::instance().register_action(
            "actions/insert-data",
            [](const ActionConfigVariant& config) {
                return std::make_unique<InsertDataAction>(std::get<InsertDataConfig>(config));
            });
        return true;
    }();
};