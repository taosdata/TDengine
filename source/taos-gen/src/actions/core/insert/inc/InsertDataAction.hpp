#pragma once

#include "Latch.hpp"
#include "ActionBase.hpp"
#include "ActionFactory.hpp"
#include "InsertDataConfig.hpp"
#include "DatabaseConnector.hpp"
#include "ColumnConfigInstance.hpp"
#include "TableDataManager.hpp"
#include "DataPipeline.hpp"
#include "FormatResult.hpp"
#include "ISinkPlugin.hpp"
#include "GarbageCollector.hpp"
#include <iostream>

class InsertDataAction : public ActionBase {
public:
    explicit InsertDataAction(const GlobalConfig& global, const InsertDataConfig& config)
            : global_(global), config_(config) {
        col_instances_ = create_column_instances();
        tag_instances_ = create_tag_instances();
    }

    void execute() override;

    void init_cache_units_data(MemoryPool& pool, size_t num_cached_batches, size_t max_tables_per_block, size_t max_rows_per_table);

private:
    const GlobalConfig& global_;
    InsertDataConfig config_;
    ColumnConfigInstanceVector col_instances_;
    ColumnConfigInstanceVector tag_instances_;
    bool is_checkpoint_recover_ = false;
    std::atomic<bool> stop_execution_{false};

    void set_realtime_priority();
    void set_thread_affinity(size_t thread_id, bool reverse = false, const std::string& purpose = "");

    ColumnConfigInstanceVector create_column_instances() const;
    ColumnConfigInstanceVector create_tag_instances() const;
    void print_sink_plugin_times(const std::vector<std::unique_ptr<ISinkPlugin>>& plugins);

    void producer_thread_function(
        size_t producer_id,
        const std::vector<std::string>& assigned_tables,
        DataPipeline<FormatResult>& pipeline,
        std::shared_ptr<TableDataManager> data_manager,
        const ISinkPlugin* plugin);

    void consumer_thread_function(
        size_t consumer_id,
        DataPipeline<FormatResult>& pipeline,
        std::atomic<bool>& running,
        ISinkPlugin* plugin,
        std::optional<ConnectorSource>& conn_source,
        GarbageCollector<FormatResult>& gc,
        Latch& startup_latch);
};