#pragma once
#include "PatternGenerator.hpp"

class KeyGenerator final : public PatternGenerator {
public:
    KeyGenerator(const std::string& pattern,
                 const std::string& serializer,
                 const ColumnConfigInstanceVector& col_instances,
                 const ColumnConfigInstanceVector& tag_instances);

    std::string generate(const MemoryPool::TableBlock& data, size_t row_index) const override;

private:
    enum class SerializerType {
        STRING_UTF8,
        INT8, UINT8,
        INT16, UINT16,
        INT32, UINT32,
        INT64, UINT64
    };

    SerializerType serializer_type_;
    std::string single_placeholder_key_;    // Used for integer serializers

    static SerializerType string_to_serializer(const std::string& s);

    template<typename T>
    std::string serialize_to_big_endian(T value) const;

    static const bool is_little_endian_;
};