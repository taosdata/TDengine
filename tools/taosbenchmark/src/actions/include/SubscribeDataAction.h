#pragma once
#include "ActionBase.h"
#include "ActionFactory.h"
#include "SubscribeDataConfig.h"
#include <iostream>

class SubscribeDataAction : public ActionBase {
public:
    explicit SubscribeDataAction(const SubscribeDataConfig& config) : config_(config) {}

    void execute() override {
        std::cout << "Subscribing to data from topics: ";
        for (const auto& topic : config_.control.subscribe_control.topics) {
            std::cout << topic.name << " ";
        }
        std::cout << std::endl;
        // 在此处实现具体的数据订阅逻辑
    }

private:
    SubscribeDataConfig config_;

    // 注册 SubscribeDataAction 到 ActionFactory
    inline static bool registered_ = []() {
        ActionFactory::instance().register_action(
            "actions/subscribe-data",
            [](const ActionConfigVariant& config) {
                return std::make_unique<SubscribeDataAction>(std::get<SubscribeDataConfig>(config));
            });
        return true;
    }();
};