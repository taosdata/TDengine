#pragma once
#include "ActionBase.h"
#include "parameter/ConfigData.h"
#include <iostream>

class CreateSuperTableAction : public ActionBase {
public:
    explicit CreateSuperTableAction(const CreateSuperTableConfig& config) : config_(config) {}

    void execute() override {
        std::cout << "Creating super table: " << config_.super_table_info.name << std::endl;
        // 在此处实现具体的创建超级表逻辑
    }

private:
    CreateSuperTableConfig config_;
};