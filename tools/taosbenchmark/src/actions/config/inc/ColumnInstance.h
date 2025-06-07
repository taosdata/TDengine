#pragma once

#include <string>
#include <optional>
#include "ColumnConfig.h"


class ColumnInstance {
public:
    ColumnInstance(const ColumnConfig& config, size_t index = 1) : config_(config) {
        config_.calc_type_tag();

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

    const ColumnConfig& config() const {
        return config_;
    }

private:
    std::string name_;
    ColumnConfig config_;
};


class ColumnInstanceFactory {
public:
    static std::vector<ColumnInstance> create(const ColumnConfig& config) {
        std::vector<ColumnInstance> instances;
        instances.reserve(config.count);
        for (size_t i = 1; i <= config.count; ++i) {
            instances.emplace_back(config, i);
        }
        return instances;
    }
};
