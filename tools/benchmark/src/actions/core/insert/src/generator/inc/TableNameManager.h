#pragma once

#include <vector>
#include <string>
#include <memory>
#include <stdexcept>
#include "InsertDataConfig.h"
#include "TableNameGenerator.h"
#include "TableNameCSV.h"

class TableNameManager {
public:
    explicit TableNameManager(const InsertDataConfig& config);

    // Generate all table names based on config
    std::vector<std::string> generate_table_names();

    // Split table names based on thread allocation strategy
    std::vector<std::vector<std::string>> split_for_threads();

private:
    const InsertDataConfig& config_;
    std::vector<std::string> table_names_;

    // Split methods for different strategies
    std::vector<std::vector<std::string>> split_by_index_range();
    std::vector<std::vector<std::string>> split_by_vgroup_binding();

    // Helper method to perform equal splits
    std::vector<std::vector<std::string>> split_equally(size_t thread_count);
};