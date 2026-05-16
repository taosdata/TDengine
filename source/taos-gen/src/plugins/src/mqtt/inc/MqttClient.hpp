#pragma once

#include "MqttConfig.hpp"
#include "MqttFormatOptions.hpp"
#include "MqttInsertData.hpp"

#include <mqtt/async_client.h>
#include <mqtt/properties.h>
#include <memory>
#include <vector>
#include <functional>
#include <atomic>

// Forward declaration
namespace mqtt {
    class async_client;
    class message;
    class connect_options;
    class properties;
    using const_message_ptr = std::shared_ptr<const message>;
}

// MQTT client interface
class IMqttClient {
public:
    virtual ~IMqttClient() = default;

    virtual bool connect() = 0;
    virtual bool is_connected() const = 0;
    virtual void disconnect() = 0;
    virtual bool publish(const MqttMessageBatch& msgs) = 0;
    virtual void close() = 0;
};

// MQTT client implementation wrapper
class PahoMqttClient : public IMqttClient {
public:
    PahoMqttClient(const MqttConfig& config, const MqttFormatOptions& format, size_t no = 0);

    ~PahoMqttClient();

    bool connect() override;
    bool is_connected() const override;
    void disconnect() override;
    bool publish(const MqttMessageBatch& msgs) override;
    void close() override;

private:
    const MqttConfig& config_;
    const MqttFormatOptions& format_;
    size_t no_;
    std::unique_ptr<mqtt::async_client> client_;
    std::thread token_wait_thread_;
    mqtt::properties default_props_;
    mqtt::thread_queue<mqtt::delivery_token_ptr> token_queue_;
    bool is_connected_ = false;
    std::atomic<bool> closed_{false};
    std::atomic<bool> sigpipe_seen_{false};

    void token_wait_func() {
        while (true) {
            mqtt::delivery_token_ptr token = token_queue_.get();
            if (!token)
                break;
            token->wait();
        }
    }

    void flush(std::chrono::seconds timeout);
};

class MqttClient {
public:
    MqttClient(const MqttConfig& config, const MqttFormatOptions& format, size_t no = 0);
    ~MqttClient();

    // Connect to MQTT broker
    bool connect();
    bool is_connected() const;
    void close();
    bool execute(const MqttInsertData& data);

    void set_client(std::unique_ptr<IMqttClient> client) {
        client_ = std::move(client);
    }

private:
    std::unique_ptr<IMqttClient> client_;
};