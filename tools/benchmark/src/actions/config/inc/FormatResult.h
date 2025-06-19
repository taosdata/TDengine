#pragma once

#include <string>
#include <cstdint>
#include <variant>


struct SqlInsertData {
    int64_t start_time;
    int64_t end_time;
    size_t total_rows;
    std::string data;
};

struct StmtV2InsertData {
    int64_t start_time;
    int64_t end_time;
    size_t total_rows;
    std::string data;
};


// 通用格式化结果类型
using FormatResult = std::variant<std::string, SqlInsertData, StmtV2InsertData>;

