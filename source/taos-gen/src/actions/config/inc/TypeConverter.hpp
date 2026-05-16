#pragma once

#include <string>
#include "ColumnType.hpp"

namespace TypeConverter {

    // Convert a string value to a specific type
    template <typename T>
    T convert_value(const std::string& value);

    // Convert a string value to a ColumnType based on ColumnTypeTag
    ColumnType convert_to_type(const std::string& value, ColumnTypeTag target_type);

}