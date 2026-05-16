#pragma once

#include <string>
#include <vector>
#include <optional>
#include "TableNameConfig.hpp"
#include "CSVReader.hpp"


class TableNameCSVReader {
public:
    explicit TableNameCSVReader(const TableNameConfig::CSV& config);

    TableNameCSVReader(const TableNameCSVReader&) = delete;
    TableNameCSVReader& operator=(const TableNameCSVReader&) = delete;
    TableNameCSVReader(TableNameCSVReader&&) = delete;
    TableNameCSVReader& operator=(TableNameCSVReader&&) = delete;

    ~TableNameCSVReader() = default;

    std::vector<std::string> generate() const;

private:
    TableNameConfig::CSV config_;

    void validate_config() const;
};
