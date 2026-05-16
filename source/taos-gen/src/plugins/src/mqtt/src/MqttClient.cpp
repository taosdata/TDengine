#include "MqttClient.hpp"
#include "LogUtils.hpp"
#include "SignalManager.hpp"
#include <sstream>
#include <iomanip>

#include <mqtt/async_client.h>
#include <mqtt/message.h>
#include <mqtt/connect_options.h>
#include <mqtt/properties.h>

PahoMqttClient::PahoMqttClient(const MqttConfig& config, const MqttFormatOptions& format, size_t no)
    : config_(config), format_(format), no_(no) {

    LogUtils::debug("Creating MQTT client #{}", no_);

#if !defined(_WIN32)
    SignalManager::register_signal(SIGPIPE, [this](int){
        sigpipe_seen_.store(true, std::memory_order_relaxed);
    }, false);
#endif

    std::string client_id = config.generate_client_id(no);
    mqtt::create_options create_opts;
    create_opts.set_server_uri(config.uri);
    create_opts.set_client_id(client_id);
    create_opts.set_max_buffered_messages(static_cast<int>(config.max_buffered_messages));

    client_ = std::make_unique<mqtt::async_client>(create_opts);

    client_->set_connection_lost_handler([this](const std::string& cause) {
        LogUtils::error("MQTT client #{} connection lost: {}", no_, cause);
        is_connected_ = false;
    });

    client_->set_connected_handler([this](const std::string& cause) {
        LogUtils::debug("MQTT client #{} connected: {}", no_, cause);
        is_connected_ = true;
    });

    default_props_.add(mqtt::property(mqtt::property::USER_PROPERTY, "content-type", format.content_type));

    if (!format.compression.empty()) {
        default_props_.add(mqtt::property(mqtt::property::USER_PROPERTY, "compression", format.compression));
    }

    if (!format.encoding.empty()) {
        default_props_.add(mqtt::property(mqtt::property::USER_PROPERTY, "encoding", format.encoding));
    }

    if (format_.qos > 0) {
        token_wait_thread_ = std::thread(&PahoMqttClient::token_wait_func, this);
    }
}

PahoMqttClient::~PahoMqttClient() {
    LogUtils::debug("Destroying MQTT client #{}", no_);
    close();
}

bool PahoMqttClient::connect() {
    if (is_connected_) {
        return true;
    }

    mqtt::connect_options conn_opts;
    conn_opts.set_user_name(config_.user);
    conn_opts.set_password(config_.password);
    conn_opts.set_keep_alive_interval(config_.keep_alive);
    conn_opts.set_clean_session(config_.clean_session);
    conn_opts.set_automatic_reconnect(true);

    try {
        client_->connect(conn_opts)->wait();
        is_connected_ = true;
        return true;
    } catch (const mqtt::exception& e) {
        LogUtils::error("MQTT connection failed: {}", e.what());
        return false;
    }
}

bool PahoMqttClient::is_connected() const {
    // return client_ && client_->is_connected();
    return is_connected_ && client_;
}

void PahoMqttClient::disconnect() {
    if (client_ && is_connected()) {
        try {
            mqtt::disconnect_options disc;
            disc.set_timeout(3000); // ms
            auto tok = client_->disconnect(disc);
            (void)tok->wait_for(std::chrono::milliseconds(3000));
        } catch (const mqtt::exception& e) {
            LogUtils::error("MQTT disconnect failed: {}", e.what());
        }
    }
}

bool PahoMqttClient::publish(const MqttMessageBatch& msgs) {
    if (closed_.load(std::memory_order_relaxed))
        return false;

    auto it = msgs.begin();

    while (it != msgs.end()) {
        const auto& [topic, payload] = *it;

        auto pubmsg = mqtt::make_message(
            topic,
            payload.data(),
            payload.size(),
            format_.qos,
            format_.retain,
            default_props_
        );

        try {
            auto token = client_->publish(pubmsg);
            if (format_.qos > 0) {
                token_queue_.put(std::move(token));
            }
            ++it;
        } catch (const mqtt::exception& e) {
            std::string msg = e.what();
            if (msg.find("No more messages can be buffered") != std::string::npos ||
                msg.find("MQTT error [-12]") != std::string::npos) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            } else {
                throw std::runtime_error(std::string("MQTT publish failed: ") + msg);
            }
        }
    }
    return true;
}

void PahoMqttClient::flush(std::chrono::seconds timeout) {
    if (!client_ || !is_connected()) return;

    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (sigpipe_seen_.load(std::memory_order_relaxed)) {
            LogUtils::warn("MQTT client #{} saw SIGPIPE while flushing; aborting flush", no_);
            break;
        }

        auto pending = client_->get_pending_delivery_tokens();
        if (pending.empty()) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

void PahoMqttClient::close() {
    if (closed_.exchange(true)) {
        return;
    }

    if (client_) {
        flush(std::chrono::seconds(60));
    }

    // stop token waiter after draining current tokens
    if (token_wait_thread_.joinable()) {
        token_queue_.put(nullptr);
        token_wait_thread_.join();
    }

    // disconnect
    disconnect();

    client_.reset();
}

// MqttClient implementation
MqttClient::MqttClient(const MqttConfig& config,
                       const MqttFormatOptions& format,
                       size_t no) {
    client_ = std::make_unique<PahoMqttClient>(config, format, no);
}

MqttClient::~MqttClient() = default;

bool MqttClient::connect() {
    return client_->connect();
}

bool MqttClient::is_connected() const {
    return client_ && client_->is_connected();
}

void MqttClient::close() {
    if (client_) {
        client_->close();
    }
}

bool MqttClient::execute(const MqttInsertData& data) {
    if (!is_connected()) {
        return false;
    }

    return client_->publish(data);
}