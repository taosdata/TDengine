#pragma once
#include "ActionBase.h"
#include "ActionFactory.h"
#include "InsertDataConfig.h"
#include <iostream>

class InsertDataAction : public ActionBase {
public:
    explicit InsertDataAction(const InsertDataConfig& config) : config_(config) {}

    void execute() override {
        std::cout << "Inserting data into table: " << config_.target.tdengine.super_table_info.name << std::endl;
        // 在此处实现具体的数据插入逻辑
    }

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