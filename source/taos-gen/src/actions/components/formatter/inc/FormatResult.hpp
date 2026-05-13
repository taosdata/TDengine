#pragma once
#include "BaseInsertData.hpp"
#include <string>
#include <variant>
#include <vector>

// General format result type
using InsertFormatResult = std::unique_ptr<BaseInsertData>;
using FormatResult = std::variant<std::string, std::vector<std::string>, InsertFormatResult>;