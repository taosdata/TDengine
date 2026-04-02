#include "KafkaSinkPlugin.hpp"
#include "TimeRecorder.hpp"
#include "LogUtils.hpp"
#include <stdexcept>

KafkaSinkPlugin::KafkaSinkPlugin(const InsertDataConfig& config,
                                 const ColumnConfigInstanceVector& col_instances,
                                 const ColumnConfigInstanceVector& tag_instances,
                                 size_t no)
    : BaseSinkPlugin(config, col_instances, tag_instances) {

    formatter_ = std::make_unique<KafkaInsertDataFormatter>(config_.data_format);
    context_ = formatter_->init(config_, col_instances_, tag_instances_);

    const auto* kc = get_plugin_config<KafkaConfig>(config.extensions, "kafka");
    if (!kc) {
        throw std::runtime_error("Kafka configuration not found in PluginExtensions");
    }

    const auto* kf = get_format_opt<KafkaFormatOptions>(config.data_format, "kafka");
    if (kf == nullptr) {
        throw std::runtime_error("Kafka format options not found in DataFormat");
    }

    if (no == 0) {
        LogUtils::info("Inserting data into: {}", kc->get_sink_info());
    }

    client_ = std::make_unique<KafkaClient>(*kc, *kf, no);
}

KafkaSinkPlugin::~KafkaSinkPlugin() {
    close();
}

bool KafkaSinkPlugin::connect() {
    if (is_connected()) {
        return true;
    }

    try {
        return client_->connect();
    } catch (const std::exception& e) {
        LogUtils::error("KafkaSinkPlugin connection failed: {}", e.what());
        return false;
    }
}

void KafkaSinkPlugin::close() noexcept {
    if (client_) {
        try {
            client_->close();
        } catch (const std::exception& e) {
            LogUtils::error("Exception during KafkaSinkPlugin close: {}", e.what());
        }
        client_.reset();
    }
}

bool KafkaSinkPlugin::is_connected() const {
    return client_ && client_->is_connected();
}

FormatResult KafkaSinkPlugin::format(MemoryPool::MemoryBlock* block, bool is_checkpoint_recover) const {
    if (!formatter_) {
        throw std::runtime_error("Formatter is not initialized");
    }

    return formatter_->format(block, is_checkpoint_recover);
}

bool KafkaSinkPlugin::write(const BaseInsertData& data) {
    if (!client_ || !client_->is_connected()) {
        throw std::runtime_error("KafkaSinkPlugin is not connected");
    }

    // Apply time interval strategy
    apply_time_interval_strategy(data.start_time, data.end_time);

    // Execute write
    bool success = false;
    try {
        if (data.type == KAFKA_TYPE_ID) {
            success = execute_with_retry([&] {
                return handle_insert<KafkaInsertData>(data);
            }, "kafka message insert");
        } else {
            throw std::runtime_error(
                "Unsupported data type for KafkaSinkPlugin: " +
                std::string(data.type.name())
            );
        }
    } catch (const std::exception& e) {
        if (config_.failure_handling.on_failure == "exit") {
            throw;
        }
    }

    update_write_state(data, success);

    return success;
}

template<typename PayloadT>
bool KafkaSinkPlugin::handle_insert(const BaseInsertData& data) {
    if (time_strategy_.is_literal_strategy()) {
        update_play_metrics(data);
    }

    TimeRecorder timer;
    const auto* payload = data.payload_as<PayloadT>();
    if (!payload) {
        throw std::runtime_error("KafkaSinkPlugin: missing payload for requested type");
    }

    bool success = client_->execute(*payload);
    write_metrics_.add_sample(timer.elapsed());

    return success;
}

void KafkaSinkPlugin::set_client(std::unique_ptr<KafkaClient> client) {
    client_ = std::move(client);
}

KafkaClient* KafkaSinkPlugin::get_client() {
    return client_.get();
}