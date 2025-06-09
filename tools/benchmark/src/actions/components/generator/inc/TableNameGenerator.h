#pragma once

#include <vector>
#include <string>
#include "TableNameConfig.h"


class TableNameGenerator {
public:
    explicit TableNameGenerator(const TableNameConfig::Generator& config) : config_(config) {}
    ~TableNameGenerator() = default;

    std::vector<std::string> generate() const;

private:
    TableNameConfig::Generator config_;
};