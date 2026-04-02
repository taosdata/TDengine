#include "PatternGenerator.hpp"

std::vector<PatternGenerator::Token> PatternGenerator::parse_pattern(const std::string& pattern) {
    std::vector<Token> tokens;
    std::regex re(R"(\{([^}]+)\})");
    std::sregex_iterator it(pattern.begin(), pattern.end(), re);
    std::sregex_iterator end;

    std::ptrdiff_t last_pos = 0;
    while (it != end) {
        if (it->position() > last_pos) {
            tokens.push_back({
                Token::TEXT,
                pattern.substr(static_cast<size_t>(last_pos), static_cast<size_t>(it->position() - last_pos))
            });
        }

        tokens.push_back({
            Token::PLACEHOLDER,
            it->str(1)
        });

        last_pos = it->position() + it->length();
        ++it;
    }

    if (last_pos < static_cast<std::ptrdiff_t>(pattern.length())) {
        tokens.push_back({
            Token::TEXT,
            pattern.substr(static_cast<size_t>(last_pos))
        });
    }

    return tokens;
}

std::string PatternGenerator::get_value_as_string(const std::string& key, const MemoryPool::TableBlock& data, size_t row_index) const {
    if (key == "table") {
        return data.table_name ? data.table_name : "UNKNOWN_TABLE";
    }
    if (key == "ts") {
        if (data.timestamps && row_index < data.used_rows) {
            return std::to_string(data.timestamps[row_index]);
        }
        return "INVALID_TS";
    }

    // Data column handling
    auto it = col_index_map_.find(key);
    if (it != col_index_map_.end()) {
        try {
            return data.get_column_cell_as_string(row_index, it->second);
        } catch (const std::exception& e) {
            return "{ERROR:" + std::string(e.what()) + "}";
        }
    }

    // Tag column handling
    it = tag_index_map_.find(key);
    if (it != tag_index_map_.end()) {
        try {
            return data.get_tag_cell_as_string(row_index, it->second);
        } catch (const std::exception& e) {
            return "{ERROR:" + std::string(e.what()) + "}";
        }
    }

    // Not found
    return "{COL_NOT_FOUND:" + key + "}";
}

void PatternGenerator::build_mapping(const ColumnConfigInstanceVector& col_instances, const ColumnConfigInstanceVector& tag_instances) {
    // Build a mapping from column name to index
    col_index_map_.clear();
    col_index_map_.reserve(col_instances.size());
    for (size_t i = 0; i < col_instances.size(); i++) {
        col_index_map_[col_instances[i].name()] = i;
    }

    // Build a mapping from tag name to index
    tag_index_map_.clear();
    tag_index_map_.reserve(tag_instances.size());
    for (size_t i = 0; i < tag_instances.size(); i++) {
        tag_index_map_[tag_instances[i].name()] = i;
    }
}