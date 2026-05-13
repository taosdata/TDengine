#include "KafkaClient.hpp"
#include "LogUtils.hpp"
#include <librdkafka/rdkafkacpp.h>
#include <stdexcept>
#include <sstream>

// --- RdKafkaClient Implementation ---
RdKafkaClient::RdKafkaClient(const KafkaConfig& config, const KafkaFormatOptions& format, size_t no)
    : config_(config), format_(format), no_(no) {

    LogUtils::debug("Creating Kafka client #{}", no_);
}

RdKafkaClient::~RdKafkaClient() {
    LogUtils::debug("Destroying Kafka client #{}", no_);
    close();
}

bool RdKafkaClient::connect() {
    if (is_connected_) {
        return true;
    }

    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    std::string errstr;

    // Set bootstrap.servers
    if (conf->set("bootstrap.servers", config_.bootstrap_servers, errstr) != RdKafka::Conf::CONF_OK) {
        LogUtils::error("Failed to set bootstrap.servers '{}': {}", config_.bootstrap_servers, errstr);
        return false;
    }

    // Set client.id
    std::string client_id = config_.generate_client_id(no_);
    if (conf->set("client.id", client_id, errstr) != RdKafka::Conf::CONF_OK) {
        LogUtils::error("Failed to set client.id '{}': {}", client_id, errstr);
        return false;
    }

    // Iterate over and set all generic options from the map
    for (const auto& [key, value] : config_.rdkafka_options) {
        if (conf->set(key, value, errstr) != RdKafka::Conf::CONF_OK) {
            LogUtils::error("Failed to set Kafka option '{}' to '{}': {}", key, value, errstr);
            return false;
        }
    }

    if (conf->set("acks", format_.acks, errstr) != RdKafka::Conf::CONF_OK) {
        LogUtils::error("Failed to set acks {}: {}", format_.acks, errstr);
        return false;
    }

    if (conf->set("compression.codec", format_.compression, errstr) != RdKafka::Conf::CONF_OK) {
        LogUtils::error("Failed to set compression.codec '{}': {}", format_.compression, errstr);
        return false;
    }

    // Create the producer
    producer_.reset(RdKafka::Producer::create(conf.get(), errstr));
    if (!producer_) {
        LogUtils::error("Failed to create Kafka producer: {}", errstr);
        return false;
    }

    // Actively verify the connection by fetching metadata.
    // This forces the client to connect to a broker and authenticate.
    RdKafka::Metadata* metadata_ptr = nullptr;
    RdKafka::ErrorCode err = producer_->metadata(true, nullptr, &metadata_ptr, 5000);
    if (err != RdKafka::ERR_NO_ERROR) {
        LogUtils::error("Failed to connect to Kafka cluster and fetch metadata: {}", RdKafka::err2str(err));
        producer_.reset();
        return false;
    }
    if (metadata_ptr) {
        delete metadata_ptr;
    }

    is_connected_ = true;
    return true;
}

bool RdKafkaClient::is_connected() const {
    return is_connected_ && producer_;
}

void RdKafkaClient::close() {
    if (producer_) {
        producer_->flush(-1);
        producer_.reset();
    }
    is_connected_ = false;
}

bool RdKafkaClient::produce(const KafkaMessageBatch& msgs) {
    auto it = msgs.begin();

    while (it != msgs.end()) {
        const auto& [key, payload] = *it;

        RdKafka::ErrorCode err = producer_->produce(
            config_.topic,
            RdKafka::Topic::PARTITION_UA,
            RdKafka::Producer::RK_MSG_COPY, // Using COPY is safe as payload's lifetime is limited to the loop.
            const_cast<char*>(payload.c_str()), payload.size(),
            const_cast<char*>(key.c_str()), key.size(),
            0,
            nullptr
        );

        if (err == RdKafka::ERR_NO_ERROR) {
            ++it;
            continue;
        }

        if (err == RdKafka::ERR__QUEUE_FULL) {
            producer_->poll(100);
            continue;
        } else {
            throw std::runtime_error("Kafka produce failed: " + RdKafka::err2str(err));
        }
    }

    producer_->poll(0);
    return true;
}

// --- KafkaClient Implementation ---
KafkaClient::KafkaClient(const KafkaConfig& config,
                         const KafkaFormatOptions& format,
                         size_t no) {
    client_ = std::make_unique<RdKafkaClient>(config, format, no);
}

KafkaClient::~KafkaClient() = default;

bool KafkaClient::connect() {
    return client_->connect();
}

bool KafkaClient::is_connected() const {
    return client_ && client_->is_connected();
}

void KafkaClient::close() {
    if (client_) {
        client_->close();
    }
}

bool KafkaClient::execute(const KafkaInsertData& data) {
    if (!is_connected()) {
        return false;
    }
    return client_->produce(data);
}
