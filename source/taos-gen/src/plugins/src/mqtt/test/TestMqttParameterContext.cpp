#include "ParameterContext.hpp"
#include "PluginRegistrar.hpp"
#include "MqttConfig.hpp"
#include "LogUtils.hpp"
#include "ScopedEnvVar.hpp"
#include <cassert>
#include <iostream>

// Test command line parameter parsing
void test_commandline_merge() {
    ParameterContext ctx;
    const char* argv[] = {
        "dummy_program",
        "--host=cli.host",
        "--port=1234",
        "--user=cli_user",
        "--password=cli_pass"
    };
    ctx.init(5, const_cast<char**>(argv));

    const auto& global = ctx.get_global_config();
    const auto& mc = get_plugin_config<MqttConfig>(global.extensions, "mqtt");
    (void)mc;
    assert(mc != nullptr);
    assert(mc->uri == "tcp://cli.host:1234");

    std::cout << "Commandline merge test passed.\n";
}

// Test environment variable merge
void test_environment_merge() {
    ParameterContext ctx;

    ScopedEnvVar host_guard("TAOS_HOST", "env.host");
    ScopedEnvVar port_guard("TAOS_PORT", "5678");

    const char* argv[] = {"dummy"};
    ctx.init(1, const_cast<char**>(argv));

    const auto& global = ctx.get_global_config();
    const auto& mc = get_plugin_config<MqttConfig>(global.extensions, "mqtt");
    (void)mc;
    assert(mc != nullptr);
    assert(mc->uri == "tcp://env.host:5678");

    std::cout << "Environment merge test passed.\n";
}

void test_mqtt_global_config() {
    ParameterContext ctx;
    YAML::Node config = YAML::Load(R"(
mqtt:
  uri: "tcp://mqtt.broker:1883"
)");
    ctx.merge_yaml(config);
    const auto& global = ctx.get_global_config();
    const auto& mc = get_plugin_config<MqttConfig>(global.extensions, "mqtt");
    (void)mc;
    assert(mc != nullptr);
    assert(mc->uri == "tcp://mqtt.broker:1883");

    std::cout << "MQTT global config test passed.\n";
}

int main() {
    register_plugin_hooks();
    test_commandline_merge();
    test_environment_merge();
    test_mqtt_global_config();

    std::cout << "All tests passed!\n";
    return 0;
}
