#pragma once
#include "ActionBase.h"
#include "Step.h"
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <functional>


class ActionFactory {
public:
    using ActionCreator = std::function<std::unique_ptr<ActionBase>(const ActionConfigVariant&)>;

    static ActionFactory& instance() {
        static ActionFactory factory;
        return factory;
    }

    void register_action(const std::string& action_type, ActionCreator creator) {
        std::lock_guard<std::mutex> lock(mutex_);
        creators_[action_type] = std::move(creator);
    }

    std::unique_ptr<ActionBase> create_action(const std::string& action_type, const ActionConfigVariant& config) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = creators_.find(action_type);
        if (it != creators_.end()) {
            return it->second(config);
        }
        throw std::invalid_argument("Unsupported action type: " + action_type);
    }

private:
    std::unordered_map<std::string, ActionCreator> creators_;
    std::mutex mutex_;
};