#pragma once
#include "BaseSinkPlugin.hpp"
#include "SinkPluginFactory.hpp"
#include "InfluxDBConfig.hpp"
#include "InfluxDBInsertDataFormatter.hpp"
#include "InfluxDBClient.hpp"

class InfluxDBSinkPlugin : public BaseSinkPlugin {
public:
    InfluxDBSinkPlugin(const InsertDataConfig& config,
                       const ColumnConfigInstanceVector& col_instances,
                       const ColumnConfigInstanceVector& tag_instances,
                       size_t no = 0);
    ~InfluxDBSinkPlugin() override;

    bool connect() override;
    void close() noexcept override;
    bool is_connected() const override;

    FormatResult format(MemoryPool::MemoryBlock* block, bool is_checkpoint_recover) const override;
    bool write(const BaseInsertData& data) override;

    void set_client(std::unique_ptr<InfluxDBClient> client);
    InfluxDBClient* get_client();

private:
    template<typename PayloadT>
    bool handle_insert(const BaseInsertData& data);

    std::unique_ptr<InfluxDBClient> client_;
    std::unique_ptr<InfluxDBInsertDataFormatter> formatter_;

    inline static bool registered_ = []() {
        SinkPluginFactory::register_sink_plugin(
            "influxdb",
            [](const InsertDataConfig& config, const ColumnConfigInstanceVector& col_instances, const ColumnConfigInstanceVector& tag_instances, size_t no, std::shared_ptr<ActionRegisterInfo>) {
                return std::make_unique<InfluxDBSinkPlugin>(config, col_instances, tag_instances, no);
            });
        return true;
    }();
};
