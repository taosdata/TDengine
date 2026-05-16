#pragma once
#include "ISinkConfig.hpp"
#include "ProcessUtils.hpp"
#include "LogUtils.hpp"
#include <string>
#include <vector>
#include <optional>
#include <unordered_map>

struct KafkaConfig : ISinkConfig {
    bool enabled = false;
    std::string topic = "tsbench-topic";
    std::string bootstrap_servers = "localhost:9092";
    std::string host = "localhost";
    int port = 9092;
    std::string client_id = "taosgen";

    KafkaConfig() {
        // --- Set queue.buffering.max.messages based on free memory ---
        double free_mem_gb = ProcessUtils::get_system_free_memory_gb();
        long long queue_size = 1000000;

        if (free_mem_gb > 0.0) {
            queue_size = static_cast<long long>(free_mem_gb) * 100000LL;
        }

        // Apply a minimum of 100,000 (librdkafka default) and a maximum of 10,000,000
        queue_size = std::max(100000LL, queue_size);
        queue_size = std::min(10000000LL, queue_size);

        rdkafka_options["queue.buffering.max.messages"] = std::to_string(queue_size);
        rdkafka_options["linger.ms"] = "50";

        LogUtils::debug("Kafka librdkafka default option: queue.buffering.max.messages = {}", rdkafka_options["queue.buffering.max.messages"]);
        LogUtils::debug("Kafka librdkafka default option: linger.ms = {}", rdkafka_options["linger.ms"]);
    }

    void update_bootstrap_servers_from_host_port() {
        // If only one server is specified, replace it entirely
        if (bootstrap_servers.find(',') == std::string::npos) {
            bootstrap_servers = host + ":" + std::to_string(port);
        }
        // If multiple servers are specified, replace only the first one
        else {
            size_t pos = bootstrap_servers.find(',');
            bootstrap_servers = host + ":" + std::to_string(port) + bootstrap_servers.substr(pos);
        }
    }

    std::string generate_client_id(size_t no = 0) const {
        return client_id + "-" + std::to_string(no);
    }

    std::string get_sink_info() const override {
        return "Kafka(" + bootstrap_servers + ")";
    }

    std::string get_sink_type() const override { return "Kafka"; }

    bool is_enabled() const override { return enabled; }

    // --- Generic librdkafka Options ---
    // Allows specifying any librdkafka-supported optional parameters.
    // Examples:
    // - security.protocol: "sasl_ssl"
    // - sasl.mechanism: "PLAIN"
    // - sasl.username: "user"
    // - sasl.password: "password"
    std::unordered_map<std::string, std::string> rdkafka_options;
};
