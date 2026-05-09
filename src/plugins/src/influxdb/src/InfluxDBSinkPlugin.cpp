#include "InfluxDBSinkPlugin.hpp"
#include "TimeRecorder.hpp"
#include "LogUtils.hpp"
#include <stdexcept>

InfluxDBSinkPlugin::InfluxDBSinkPlugin(const InsertDataConfig& config,
                                       const ColumnConfigInstanceVector& col_instances,
                                       const ColumnConfigInstanceVector& tag_instances,
                                       size_t no)
    : BaseSinkPlugin(config, col_instances, tag_instances) {

    formatter_ = std::make_unique<InfluxDBInsertDataFormatter>(config_.data_format);
    context_ = formatter_->init(config_, col_instances_, tag_instances_);

    const auto* ic = get_plugin_config<InfluxDBConfig>(config.extensions, "influxdb");
    if (!ic) {
        throw std::runtime_error("InfluxDB configuration not found in PluginExtensions");
    }

    const auto* fo = get_format_opt<InfluxDBFormatOptions>(config.data_format, "influxdb");
    if (!fo) {
        throw std::runtime_error("InfluxDB format options not found in DataFormat");
    }

    if (no == 0) {
        LogUtils::info("Inserting data into: {}", ic->get_sink_info());
    }

    client_ = std::make_unique<InfluxDBClient>(*ic, *fo);
}

void InfluxDBSinkPlugin::set_client(std::unique_ptr<InfluxDBClient> client) {
    client_ = std::move(client);
}

InfluxDBClient* InfluxDBSinkPlugin::get_client() {
    return client_.get();
}

InfluxDBSinkPlugin::~InfluxDBSinkPlugin() {
    close();
}

bool InfluxDBSinkPlugin::connect() {
    if (is_connected()) return true;

    try {
        return client_->connect();
    } catch (const std::exception& e) {
        LogUtils::error("InfluxDBSinkPlugin connection failed: {}", e.what());
        return false;
    }
}

void InfluxDBSinkPlugin::close() noexcept {
    if (client_) {
        try {
            client_->close();
        } catch (const std::exception& e) {
            LogUtils::error("Exception during InfluxDBSinkPlugin close: {}", e.what());
        }
    }
}

bool InfluxDBSinkPlugin::is_connected() const {
    return client_ && client_->is_connected();
}

FormatResult InfluxDBSinkPlugin::format(MemoryPool::MemoryBlock* block, bool is_checkpoint_recover) const {
    if (!formatter_) {
        throw std::runtime_error("Formatter is not initialized");
    }
    return formatter_->format(block, is_checkpoint_recover);
}

bool InfluxDBSinkPlugin::write(const BaseInsertData& data) {
    if (!client_ || !client_->is_connected()) {
        throw std::runtime_error("InfluxDBSinkPlugin is not connected");
    }

    apply_time_interval_strategy(data.start_time, data.end_time);

    bool success = false;
    try {
        if (data.type == INFLUXDB_TYPE_ID) {
            success = execute_with_retry([&] {
                return handle_insert<InfluxDBInsertData>(data);
            }, "influxdb write");
        } else {
            throw std::runtime_error(
                "Unsupported data type for InfluxDBSinkPlugin: " +
                std::string(data.type.name()));
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
bool InfluxDBSinkPlugin::handle_insert(const BaseInsertData& data) {
    if (time_strategy_.is_literal_strategy()) {
        update_play_metrics(data);
    }

    TimeRecorder timer;
    const auto* payload = data.payload_as<PayloadT>();
    if (!payload) {
        throw std::runtime_error("InfluxDBSinkPlugin: missing payload for requested type");
    }

    bool success = client_->execute(*payload);
    write_metrics_.add_sample(timer.elapsed());

    return success;
}
