#pragma once
#include "BaseSinkPlugin.hpp"
#include "SinkPluginFactory.hpp"
#include "MqttConfig.hpp"
#include "MqttInsertDataFormatter.hpp"
#include "MqttClient.hpp"

class MqttSinkPlugin : public BaseSinkPlugin {
public:
    explicit MqttSinkPlugin(const InsertDataConfig& config,
                            const ColumnConfigInstanceVector& col_instances,
                            const ColumnConfigInstanceVector& tag_instances,
                            size_t no = 0);
    ~MqttSinkPlugin() override;

    bool connect() override;
    void close() noexcept override;
    bool is_connected() const override;

    FormatResult format(MemoryPool::MemoryBlock* block, bool is_checkpoint_recover) const override;
    bool write(const BaseInsertData& data) override;

    void set_client(std::unique_ptr<MqttClient> client);
    MqttClient* get_client();

private:
    template<typename PayloadT>
    bool handle_insert(const BaseInsertData& data);

    std::unique_ptr<MqttClient> client_;
    std::unique_ptr<MqttInsertDataFormatter> formatter_;

    inline static bool registered_ = []() {
        SinkPluginFactory::register_sink_plugin(
            "mqtt",
            [](const InsertDataConfig& config, const ColumnConfigInstanceVector& col_instances, const ColumnConfigInstanceVector& tag_instances, size_t no, std::shared_ptr<ActionRegisterInfo>) {
                return std::make_unique<MqttSinkPlugin>(config, col_instances, tag_instances, no);
            });
        return true;
    }();
};