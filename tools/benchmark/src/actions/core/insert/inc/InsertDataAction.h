#pragma once

#include <iostream>
#include "ActionBase.h"
#include "ActionFactory.h"
#include "InsertDataConfig.h"
#include "DatabaseConnector.h"
#include "ColumnConfigInstance.h"
#include "TableDataManager.h"
#include "DataPipeline.h"
#include "FormatResult.h"


class InsertDataAction : public ActionBase {
public:
    explicit InsertDataAction(const InsertDataConfig& config) : config_(config) {}

    void execute() override;


private:
    InsertDataConfig config_;

    ColumnConfigInstanceVector create_column_instances(const InsertDataConfig& config) const;

    void producer_thread_function(
        size_t producer_id,
        const std::vector<std::string>& assigned_tables,
        const ColumnConfigInstanceVector& col_instances,
        DataPipeline<FormatResult>& pipeline,
        std::shared_ptr<TableDataManager> data_manager);

    void consumer_thread_function(
        size_t consumer_id,
        DataPipeline<FormatResult>& pipeline,
        std::atomic<bool>& running);

    // 注册 InsertDataAction 到 ActionFactory
    inline static bool registered_ = []() {
        ActionFactory::instance().register_action(
            "actions/insert-data",
            [](const ActionConfigVariant& config) {
                return std::make_unique<InsertDataAction>(std::get<InsertDataConfig>(config));
            });
        return true;
    }();
};