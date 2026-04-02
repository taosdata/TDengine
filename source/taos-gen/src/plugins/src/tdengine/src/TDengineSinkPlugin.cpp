#include "TDengineSinkPlugin.hpp"
#include "ConnectorFactory.hpp"
#include "TimeRecorder.hpp"
#include "StmtContext.hpp"
#include "LogUtils.hpp"
#include <stdexcept>

TDengineSinkPlugin::TDengineSinkPlugin(const InsertDataConfig& config,
                                       const ColumnConfigInstanceVector& col_instances,
                                       const ColumnConfigInstanceVector& tag_instances,
                                       size_t no,
                                       std::shared_ptr<ActionRegisterInfo> action_info)
    : BaseSinkPlugin(config, col_instances, tag_instances, std::move(action_info)) {

    if (config_.data_format.format_type == "sql") {
        formatter_ = std::make_unique<SqlInsertDataFormatter>(config_.data_format);
    } else if (config_.data_format.format_type == "stmt") {
        formatter_ = std::make_unique<StmtInsertDataFormatter>(config_.data_format);
    } else {
        throw std::invalid_argument("Unsupported format type: " + config_.data_format.format_type);
    }

    context_ = formatter_->init(config_, col_instances_, tag_instances_);

    tc_ = get_plugin_config<TDengineConfig>(config_.extensions, "tdengine");
    if (!tc_) {
        throw std::runtime_error("TDengine configuration not found in PluginExtensions");
    }

    if (no == 0) {
        LogUtils::info("Inserting data into: {}", tc_->get_sink_info());
    }
}

TDengineSinkPlugin::~TDengineSinkPlugin() {
    close();
}

bool TDengineSinkPlugin::connect() {
    if (!connector_) {
        connector_ = ConnectorFactory::create(*tc_);
    }

    if (connector_->is_connected()) {
        return true;
    }

    return connector_->connect();
}

bool TDengineSinkPlugin::connect_with_source(std::optional<ConnectorSource>& conn_source) {
    if (connector_ && connector_->is_connected()) {
        return true;
    }

    try {
        // Create connector
        if (conn_source) {
            connector_ = conn_source->get_connector();
            return connector_->is_connected();
        } else {
            connector_ = ConnectorFactory::create(*tc_);
            return connector_->connect();
        }
    } catch (const std::exception& e) {
        LogUtils::error("TDengineSinkPlugin connection failed: {}", e.what());
        connector_.reset();
        return false;
    }
}

void TDengineSinkPlugin::close() noexcept {
    if (connector_) {
        try {
            connector_->close();
        } catch (const std::exception& e) {
            LogUtils::error("Exception during TDengineSinkPlugin close: {}", e.what());
        }
        connector_.reset();
    }
}

bool TDengineSinkPlugin::is_connected() const {
    return connector_ && connector_->is_connected();
}

bool TDengineSinkPlugin::prepare() {
    if (!is_connected()) {
        throw std::runtime_error("TDengineSinkPlugin is not connected");
    }

    if (const auto* stmt = dynamic_cast<const StmtContext*>(context_.get())) {
        return connector_->prepare(stmt->sql);
    }

    return true;
}

FormatResult TDengineSinkPlugin::format(MemoryPool::MemoryBlock* block, bool is_checkpoint_recover) const {
    if (!formatter_) {
        throw std::runtime_error("Formatter is not initialized");
    }

    return formatter_->format(block, is_checkpoint_recover);
}

bool TDengineSinkPlugin::write(const BaseInsertData& data) {
    if (!connector_) {
        throw std::runtime_error("TDengineSinkPlugin is not connected");
    }

    // Apply time interval strategy
    apply_time_interval_strategy(data.start_time, data.end_time);

    // Execute write
    bool success = false;
    try {
        if (data.type == SQL_TYPE_ID) {
            success = execute_with_retry([&] {
                return handle_insert<SqlInsertData>(data);
            }, "sql insert");
        } else if (data.type == STMTV2_TYPE_ID) {
            success = execute_with_retry([&] {
                return handle_insert<StmtV2InsertData>(data);
            }, "stmt v2 insert");
        } else {
            throw std::runtime_error(
                "Unsupported data type for TDengineSinkPlugin: " +
                std::string(data.type.name())
            );
        }
    } catch (const std::exception& e) {
        // Handling after retry failure
        if (config_.failure_handling.on_failure == "exit") {
            throw;
        }
        // Otherwise, ignore the error and continue
    }

    update_write_state(data, success);
    notify(data, success);
    return success;
}

template<typename PayloadT>
bool TDengineSinkPlugin::handle_insert(const BaseInsertData& data) {
    if (time_strategy_.is_literal_strategy()) {
        update_play_metrics(data);
    }

    TimeRecorder timer;
    const auto* payload = data.payload_as<PayloadT>();
    if (!payload) {
        throw std::runtime_error("TDengineSinkPlugin: missing payload for requested type");
    }

    bool success = connector_->execute(*payload);
    write_metrics_.add_sample(timer.elapsed());

    return success;
}
