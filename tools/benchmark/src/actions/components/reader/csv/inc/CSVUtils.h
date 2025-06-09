#pragma once

#include <string>
#include <vector>
#include <stdexcept>
#include <algorithm>
#include <cctype>
#include <variant>
#include <sstream>
#include "ColumnType.h"

namespace CSVUtils {

    // Convert a string value to a specific type
    template <typename T>
    T convert_value(const std::string& value);

    // Convert a string value to a ColumnType based on ColumnTypeTag
    ColumnType convert_to_type(const std::string& value, ColumnTypeTag target_type);

}