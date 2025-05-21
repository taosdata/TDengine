#pragma once
#include "ActionBase.h"
#include "parameter/ConfigData.h"
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
};