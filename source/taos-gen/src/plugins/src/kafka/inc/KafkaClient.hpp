#pragma once

#include "KafkaConfig.hpp"
#include "KafkaFormatOptions.hpp"
#include "KafkaInsertData.hpp"
#include "ColumnConfigInstance.hpp"
#include <memory>

// Forward declaration
namespace RdKafka {
    class Producer;
    class Conf;
}

// Kafka client interface
class IKafkaClient {
public:
    virtual ~IKafkaClient() = default;
    virtual bool connect() = 0;
    virtual bool is_connected() const = 0;
    virtual void close() = 0;
    virtual bool produce(const KafkaMessageBatch& msgs) = 0;
};

// librdkafka client implementation
class RdKafkaClient : public IKafkaClient {
public:
    explicit RdKafkaClient(const KafkaConfig& config, const KafkaFormatOptions& format, size_t no = 0);
    ~RdKafkaClient() override;

    bool connect() override;
    bool is_connected() const override;
    void close() override;
    bool produce(const KafkaMessageBatch& msgs) override;

private:
    const KafkaConfig& config_;
    const KafkaFormatOptions& format_;
    size_t no_;
    std::unique_ptr<RdKafka::Producer> producer_;
    bool is_connected_ = false;
};


class KafkaClient {
public:
    KafkaClient(const KafkaConfig& config, const KafkaFormatOptions& format, size_t no = 0);
    ~KafkaClient();

    bool connect();
    bool is_connected() const;
    void close();
    bool execute(const KafkaInsertData& data);

    void set_client(std::unique_ptr<IKafkaClient> client) {
        client_ = std::move(client);
    }

private:
    std::unique_ptr<IKafkaClient> client_;
};