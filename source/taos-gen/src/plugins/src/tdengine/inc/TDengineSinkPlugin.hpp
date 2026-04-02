#pragma once
#include "BaseSinkPlugin.hpp"
#include "SinkPluginFactory.hpp"
#include "TDengineConfig.hpp"
#include "SqlInsertDataFormatter.hpp"
#include "StmtInsertDataFormatter.hpp"
#include "DatabaseConnector.hpp"
#include "CheckpointAction.hpp"
#include "ActionRegisterInfo.hpp"


class TDengineSinkPlugin : public BaseSinkPlugin {
public:
    TDengineSinkPlugin(const InsertDataConfig& config,
                       const ColumnConfigInstanceVector& col_instances,
                       const ColumnConfigInstanceVector& tag_instances,
                       size_t no = 0,
                       std::shared_ptr<ActionRegisterInfo> action_info = nullptr);
    ~TDengineSinkPlugin() override;

    bool connect() override;
    bool connect_with_source(std::optional<ConnectorSource>& conn_source) override;
    void close() noexcept override;
    bool is_connected() const override;

    bool prepare() override;
    FormatResult format(MemoryPool::MemoryBlock* block, bool is_checkpoint_recover = false) const override;
    bool write(const BaseInsertData& data) override;

private:
    template<typename PayloadT>
    bool handle_insert(const BaseInsertData& data);

    std::unique_ptr<DatabaseConnector> connector_;
    std::unique_ptr<IInsertDataFormatter> formatter_;
    const TDengineConfig* tc_;

    inline static bool registered_ = []() {
        SinkPluginFactory::register_sink_plugin(
            "tdengine",
            [](const InsertDataConfig& config, const ColumnConfigInstanceVector& col_instances, const ColumnConfigInstanceVector& tag_instances, size_t no, std::shared_ptr<ActionRegisterInfo> action_info) {
                return std::make_unique<TDengineSinkPlugin>(config, col_instances, tag_instances, no, std::move(action_info));
            });
        return true;
    }();
};