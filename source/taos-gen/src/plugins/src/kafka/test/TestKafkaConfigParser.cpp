#include "ConfigParser.hpp"
#include "KafkaConfigParser.hpp"
#include "KafkaRegistrar.hpp"
#include <iostream>
#include <sstream>
#include <cassert>
#include <yaml-cpp/yaml.h>

KafkaConfig* get_kafka_config(InsertDataConfig& config) {
    return get_plugin_config_mut<KafkaConfig>(config.extensions, "kafka");
}

KafkaFormatOptions* get_kafka_format_options(InsertDataConfig& config) {
    return get_format_opt_mut<KafkaFormatOptions>(config.data_format, "kafka");
}

void test_KafkaConfig() {
    std::string yaml = R"(
topic: my-kafka-topic
bootstrap_servers: "kafka1:9092,kafka2:9092"
client_id: kafka-client-001
rdkafka_options:
  "security.protocol": "sasl_ssl"
  "sasl.mechanisms": "PLAIN"
  "queue.buffering.max.messages": "500000"
  "linger.ms": "100"
)";
    YAML::Node node = YAML::Load(yaml);
    KafkaConfig kafka = node.as<KafkaConfig>();
    assert(kafka.topic == "my-kafka-topic");
    assert(kafka.bootstrap_servers == "kafka1:9092,kafka2:9092");
    assert(kafka.client_id == "kafka-client-001");
    assert(kafka.rdkafka_options.size() == 4);
    assert(kafka.rdkafka_options["security.protocol"] == "sasl_ssl");
    assert(kafka.rdkafka_options["sasl.mechanisms"] == "PLAIN");
    assert(kafka.rdkafka_options["queue.buffering.max.messages"] == "500000");
    assert(kafka.rdkafka_options["linger.ms"] == "100");
}

void test_KafkaConfig_unknown_key() {
    std::string yaml = R"(
topic: my-topic
unknown_key: "some_value"
)";
    YAML::Node node = YAML::Load(yaml);
    try {
        KafkaConfig kafka = node.as<KafkaConfig>();
        assert(false && "Should throw for unknown key in kafka config");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("Unknown configuration key in kafka config") != std::string::npos);
    }
}

void test_InsertDataConfig_kafka() {
    std::string yaml = R"(
target: kafka
kafka:
  bootstrap_servers: "localhost:9092"
  topic: "default-topic"
schema:
  name: test_schema
  columns:
    - name: c1
      type: INT
key_pattern: "key-{c1}"
key_serializer: "string-utf8"
value_serializer: influx
tbname_key: "device_id"
acks: "all"
compression: "snappy"
records_per_message: 100
)";
    YAML::Node node = YAML::Load(yaml);
    InsertDataConfig idc = node.as<InsertDataConfig>();
    auto* kc = get_plugin_config_mut<KafkaConfig>(idc.extensions, "kafka");
    (void)kc;
    assert(kc != nullptr);
    assert(idc.target_type == "kafka");
    assert(kc->bootstrap_servers == "localhost:9092");
    assert(kc->topic == "default-topic");
    assert(idc.schema.name == "test_schema");
    assert(idc.data_format.format_type == "kafka");

    auto* kf = get_kafka_format_options(idc);
    (void)kf;
    assert(kf != nullptr);
    assert(kf->key_pattern == "key-{c1}");
    assert(kf->key_serializer == "string-utf8");
    assert(kf->value_serializer == "influx");
    assert(kf->tbname_key == "device_id");
    assert(kf->acks == "all");
    assert(kf->compression == "snappy");
    assert(kf->records_per_message == 100);
}

int main() {
    register_kafka_plugin_config_hooks();
    test_KafkaConfig();
    test_KafkaConfig_unknown_key();
    test_InsertDataConfig_kafka();

    std::cout << "All ConfigParser YAML tests passed!" << std::endl;
    return 0;
}