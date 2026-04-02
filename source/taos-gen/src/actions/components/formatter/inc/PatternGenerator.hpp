#pragma once

#include "ColumnConfigInstance.hpp"
#include "MemoryPool.hpp"

#include <fmt/core.h>
#include <fmt/format.h>
#include <vector>
#include <string>
#include <unordered_map>
#include <sstream>
#include <regex>

class PatternGenerator {
public:
    virtual std::string generate(const MemoryPool::TableBlock& data, size_t row_index) const = 0;

    virtual ~PatternGenerator() = default;

protected:
    struct Token {
        enum Type { TEXT, PLACEHOLDER } type;
        std::string value;
    };

    static std::vector<Token> parse_pattern(const std::string& pattern);
    std::string get_value_as_string(const std::string& key, const MemoryPool::TableBlock& data, size_t row_index) const;
    void build_mapping(const ColumnConfigInstanceVector& col_instances, const ColumnConfigInstanceVector& tag_instances);

    std::vector<Token> tokens_;
    std::unordered_map<std::string, size_t> col_index_map_;
    std::unordered_map<std::string, size_t> tag_index_map_;
};