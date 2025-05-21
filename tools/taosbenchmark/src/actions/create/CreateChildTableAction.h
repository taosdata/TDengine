#pragma once
#include "ActionBase.h"
#include "parameter/ConfigData.h"
#include <iostream>

class CreateChildTableAction : public ActionBase {
public:
    explicit CreateChildTableAction(const CreateChildTableConfig& config) : config_(config) {}

    void execute() override {
        std::cout << "Creating child table for super table: " << config_.super_table_info.name << std::endl;
        // 在此处实现具体的创建子表逻辑
    }

private:
    CreateChildTableConfig config_;
};