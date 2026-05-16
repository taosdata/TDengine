#include "TableNameManager.hpp"
#include <cmath>
#include <iostream>

TableNameManager::TableNameManager(const InsertDataConfig& config)
    : config_(config), chunk_size_(1) {}

std::vector<std::string> TableNameManager::generate_table_names() {
    if (!table_names_.empty()) {
        return table_names_;
    }

    try {
        if (config_.schema.tbname.source_type == "generator") {
            TableNameGenerator generator(config_.schema.tbname.generator);
            table_names_ = generator.generate();
        }
        else if (config_.schema.tbname.source_type == "csv") {
            TableNameCSVReader csv_reader(config_.schema.tbname.csv);
            table_names_ = csv_reader.generate();
        }
        else {
            throw std::runtime_error("Unsupported table name source type: " +
                config_.schema.tbname.source_type);
        }

        return table_names_;
    }
    catch (const std::exception& e) {
        throw std::runtime_error("Failed to generate table names: " + std::string(e.what()));
    }
}

std::vector<std::vector<std::string>> TableNameManager::split_for_threads() {
    if (table_names_.empty()) {
        generate_table_names();
    }

    if (config_.thread_allocation == "index_range") {
        return split_by_index_range();
    }
    else if (config_.thread_allocation == "vgroup_binding") {
        return split_by_vgroup_binding();
    }
    else {
        throw std::runtime_error("Unsupported thread allocation strategy: " +
            config_.thread_allocation);
    }
}

std::vector<std::vector<std::string>> TableNameManager::split_by_index_range() {
    return split_equally(config_.schema.generation.generate_threads.value());
}

std::vector<std::vector<std::string>> TableNameManager::split_equally(size_t thread_count) {
    if (thread_count == 0) {
        throw std::invalid_argument("Thread count cannot be zero");
    }

    if (table_names_.empty()) {
        return {};
    }

    std::vector<std::vector<std::string>> result(thread_count);

    // Calculate base size and remainder
    size_t total_size = table_names_.size();
    size_t base_size = total_size / thread_count;
    size_t remainder = total_size % thread_count;

    // Current position in the source vector
    size_t current_pos = 0;

    // Distribute tables to threads
    for (size_t i = 0; i < thread_count; i++) {
        // Calculate chunk size for this thread
        size_t chunk_size = base_size + (i < remainder ? 1 : 0);
        chunk_size_ = std::max(chunk_size, chunk_size_);

        // Extract chunk for this thread
        auto start = table_names_.begin() + current_pos;
        auto end = start + chunk_size;
        result[i].insert(result[i].end(), start, end);

        current_pos += chunk_size;
    }

    return result;
}

std::vector<std::vector<std::string>> TableNameManager::split_by_vgroup_binding() {
    throw std::runtime_error("vgroup_binding strategy not implemented yet");
}