#include "TopicGenerator.hpp"

TopicGenerator::TopicGenerator(const std::string& pattern,
                               const ColumnConfigInstanceVector& col_instances,
                               const ColumnConfigInstanceVector& tag_instances) {
    // Parse the pattern
    tokens_ = parse_pattern(pattern);

    // Build mapping
    build_mapping(col_instances, tag_instances);
}

std::string TopicGenerator::generate(const MemoryPool::TableBlock& data,
                                     size_t row_index) const {
    fmt::memory_buffer buffer;

    for (const auto& token : tokens_) {
        if (token.type == Token::TEXT) {
            fmt::format_to(std::back_inserter(buffer), "{}", token.value);
        } else {
            fmt::format_to(std::back_inserter(buffer), "{}", get_value_as_string(token.value, data, row_index));
        }
    }
    return fmt::to_string(buffer);
}