#include "ConfigParser.hpp"
#include "MqttConfigParser.hpp"
#include "MqttRegistrar.hpp"
#include <iostream>
#include <sstream>
#include <cassert>
#include <yaml-cpp/yaml.h>

MqttConfig* get_mqtt_config(InsertDataConfig& config) {
    return get_plugin_config_mut<MqttConfig>(config.extensions, "mqtt");
}

MqttFormatOptions* get_mqtt_format_options(InsertDataConfig& config) {
    return get_format_opt_mut<MqttFormatOptions>(config.data_format, "mqtt");
}

void test_Mqtt() {
    std::string yaml = R"(
uri: mqtt.example.com:1883
user: testuser
password: testpass
client_id: client-001
keep_alive: 60
clean_session: true
max_buffered_messages: 1000
)";
    YAML::Node node = YAML::Load(yaml);
    MqttConfig mqtt = node.as<MqttConfig>();
    assert(mqtt.uri == "mqtt.example.com:1883");
    assert(mqtt.user == "testuser");
    assert(mqtt.password == "testpass");
    assert(mqtt.client_id == "client-001");
    assert(mqtt.keep_alive == 60);
    assert(mqtt.clean_session == true);
    assert(mqtt.max_buffered_messages == 1000);
}

void test_InsertDataConfig_mqtt() {
    std::string yaml = R"(
target: mqtt
mqtt:
  uri: "tcp://localhost:1883"
  user: "user1"
schema:
  name: test_schema
  columns:
    - name: c1
      type: INT
format: json
topic: "test/topic/{tag1}"
compression: gzip
encoding: GBK
tbname_key: "table"
qos: 2
retain: true
records_per_message: 10
)";
    YAML::Node node = YAML::Load(yaml);
    InsertDataConfig idc = node.as<InsertDataConfig>();
    auto* mc = get_mqtt_config(idc);
    (void)mc;
    assert(mc != nullptr);
    assert(idc.target_type == "mqtt");
    assert(mc->uri == "tcp://localhost:1883");
    assert(mc->user == "user1");
    assert(idc.schema.name == "test_schema");
    assert(idc.data_format.format_type == "mqtt");

    auto* mf = get_mqtt_format_options(idc);
    (void)mf;
    assert(mf != nullptr);
    assert(mf->content_type == "json");
    assert(mf->topic == "test/topic/{tag1}");
    assert(mf->compression == "gzip");
    assert(mf->encoding == "GBK");
    assert(mf->tbname_key == "table");
    assert(mf->qos == 2);
    assert(mf->retain == true);
    assert(mf->records_per_message == 10);
}

int main() {
    register_mqtt_plugin_config_hooks();
    test_Mqtt();
    test_InsertDataConfig_mqtt();

    std::cout << "All ConfigParser YAML tests passed!" << std::endl;
    return 0;
}