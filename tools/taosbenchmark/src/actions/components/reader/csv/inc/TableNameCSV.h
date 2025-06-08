#pragma once

#include <string>
#include <vector>
#include <optional>
#include "TableNameConfig.h"
#include "CSVReader.h"


class TableNameCSV {
public:
    explicit TableNameCSV(const TableNameConfig::CSV& config);
    
    TableNameCSV(const TableNameCSV&) = delete;
    TableNameCSV& operator=(const TableNameCSV&) = delete;
    TableNameCSV(TableNameCSV&&) = delete;
    TableNameCSV& operator=(TableNameCSV&&) = delete;
    
    ~TableNameCSV() = default;
    
    std::vector<std::string> generate() const;

private:
    TableNameConfig::CSV config_;
    
    void validate_config() const;
};
