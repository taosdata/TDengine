#pragma once
#include "ISinkContext.hpp"
#include <string>
#include <memory>

struct StmtContext final : public ISinkContext {
    std::string sql;
    explicit StmtContext(std::string s) : sql(std::move(s)) {}
};