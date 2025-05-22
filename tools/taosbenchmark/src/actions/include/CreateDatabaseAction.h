#pragma once
#include "ActionBase.h"
#include "ActionFactory.h"
#include "CreateDatabaseConfig.h"
#include <iostream>

class CreateDatabaseAction : public ActionBase {
public:
    explicit CreateDatabaseAction(const CreateDatabaseConfig& config) : config_(config) {}

    void execute() override {
        std::cout << "Creating database: " << config_.database_info.name << std::endl;
        // 在此处实现具体的创建数据库逻辑
    }

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
