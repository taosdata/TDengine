#pragma once

#include <stdexcept>
#include <cstring>
#include <variant>
#include "ColumnConfigInstance.hpp"

namespace ColumnConverter {
    // Forward conversion function
    using FixedHandler = void(*)(const ColumnType&, void*, size_t);
    using VarHandler = size_t(*)(const ColumnType&, char*, size_t);

    // Reverse conversion function
    using FixedToColumnHandler = ColumnType(*)(const void*);
    using VarToColumnHandler = ColumnType(*)(const char*, size_t);
    using ToStringHandler = std::string(*)(const ColumnType&);

    struct ColumnHandler {
        FixedHandler to_fixed = nullptr;
        VarHandler to_var = nullptr;
        FixedToColumnHandler to_column_fixed = nullptr;
        VarToColumnHandler to_column_var = nullptr;
        ToStringHandler to_string = nullptr;
    };

    // Fixed-length type handler
    template<typename T>
    void fixed_type_handler(const ColumnType& value, void* dest, size_t size) {
        const T& val = std::get<T>(value);
        (void)size;
        // if (sizeof(T) != size) {
        //     throw std::runtime_error("Fixed-length type size mismatch");
        // }
        memcpy(dest, &val, sizeof(T));
    }

    template<>
    inline void fixed_type_handler<Decimal>(const ColumnType& value, void* dest, size_t /*size*/) {
        const Decimal& val = std::get<Decimal>(value);
        new (dest) Decimal(val);
    }

    template<typename T>
    ColumnType fixed_to_column(const void* src) {
        return *static_cast<const T*>(src);
    }

    // Variable-length type handler
    size_t string_type_handler(const ColumnType& value, char* dest, size_t max_len);
    size_t u16string_type_handler(const ColumnType& value, char* dest, size_t max_len);
    size_t json_type_handler(const ColumnType& value, char* dest, size_t max_len);
    size_t binary_type_handler(const ColumnType& value, char* dest, size_t max_len);

    ColumnType string_to_column(const char* src, size_t len);
    ColumnType u16string_to_column(const char* src, size_t len);
    ColumnType json_to_column(const char* src, size_t len);
    ColumnType binary_to_column(const char* src, size_t len);

    std::string column_to_string(const ColumnType& value);

    ColumnHandler create_handler_for_column(const ColumnConfigInstance& col_config);
    std::vector<ColumnHandler> create_handlers_for_columns(const ColumnConfigInstanceVector& col_instances);
}