#pragma once

#include <string>
#include <optional>
#include "ColumnConfig.hpp"


class ColumnConfigInstance {
public:
    ColumnConfigInstance(const ColumnConfig& config, size_t index = 1) : config_(config) {
        config_.parse_type();

        if (config_.count > 1) {
            name_ = config_.name + std::to_string(index);
        } else {
            name_ = config_.name;
        }
    }

    const std::string& name() const {
        return name_;
    }

    const std::string& type() const {
        return config_.type;
    }

    ColumnTypeTag type_tag() const {
        return config_.type_tag;
    }    

    const std::optional<std::string>& gen_type() const {
        return config_.gen_type;
    }

    const ColumnConfig& config() const {
        return config_;
    }

private:
    std::string name_;
    ColumnConfig config_;
};

using ColumnConfigInstanceVector = std::vector<ColumnConfigInstance>;

class ColumnConfigInstanceFactory {
public:
    static ColumnConfigInstanceVector create(const ColumnConfig& config) {
        ColumnConfigInstanceVector instances;
        instances.reserve(config.count);
        for (size_t i = 1; i <= config.count; ++i) {
            instances.emplace_back(config, i);
        }
        return instances;
    }

    static ColumnConfigInstanceVector create(const ColumnConfigVector& configs) {
        ColumnConfigInstanceVector instances;
        for (const auto& config : configs) {
            auto config_instances = create(config);
            instances.insert(instances.end(), config_instances.begin(), config_instances.end());
        }
        return instances;
    }

};
