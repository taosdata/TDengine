#pragma once
#include "ActionBase.h"
#include "ActionFactory.h"
#include "CreateSuperTableConfig.h"
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

    // 注册 CreateSuperTableAction 到 ActionFactory
    inline static bool registered_ = []() {
        ActionFactory::instance().register_action(
            "actions/create-super-table",
            [](const ActionConfigVariant& config) {
                return std::make_unique<CreateSuperTableAction>(std::get<CreateSuperTableConfig>(config));
            });
        return true;
    }();
};