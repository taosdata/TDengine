#pragma once
#include "ActionBase.h"
#include "parameter/ConfigData.h"
#include <iostream>

class QueryDataAction : public ActionBase {
public:
    explicit QueryDataAction(const QueryDataConfig& config) : config_(config) {}

    void execute() override {
        std::cout << "Querying data from database: " << config_.source.connection_info.host << std::endl;
        // 在此处实现具体的数据查询逻辑
    }

private:
    QueryDataConfig config_;
};