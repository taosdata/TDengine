#pragma once
#include "ActionBase.h"
#include "ActionFactory.h"
#include "CreateChildTableConfig.h"
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