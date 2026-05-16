#pragma once
#include "ISinkPlugin.hpp"
#include "InsertDataConfig.hpp"
#include "ActionRegisterInfo.hpp"
#include <memory>
#include <unordered_map>
#include <mutex>
#include <functional>

class SinkPluginFactory {
public:
    using Builder = std::function<std::unique_ptr<ISinkPlugin>(
        const InsertDataConfig&,
        const ColumnConfigInstanceVector&,
        const ColumnConfigInstanceVector&,
        size_t,
        std::shared_ptr<ActionRegisterInfo>
    )>;

    static SinkPluginFactory& instance() {
        static SinkPluginFactory inst;
        return inst;
    }

    static void register_sink_plugin(const std::string& target, Builder builder) {
        auto& inst = instance();
        std::lock_guard<std::mutex> lock(inst.mutex_);
        inst.plugins_[target] = std::move(builder);
    }

    static std::unique_ptr<ISinkPlugin> create_sink_plugin(
        const InsertDataConfig& config,
        const ColumnConfigInstanceVector& col_instances,
        const ColumnConfigInstanceVector& tag_instances,
        size_t no = 0,
        std::shared_ptr<ActionRegisterInfo> action_info = nullptr
    ) {
        auto& inst = instance();
        std::lock_guard<std::mutex> lock(inst.mutex_);
        if (auto it = inst.plugins_.find(config.target_type); it != inst.plugins_.end()) {
            return it->second(config, col_instances, tag_instances, no, std::move(action_info));
        }
        throw std::invalid_argument("Unsupported sink plugin type: " + config.target_type);
    }

private:
    std::unordered_map<std::string, Builder> plugins_;
    std::mutex mutex_;
};