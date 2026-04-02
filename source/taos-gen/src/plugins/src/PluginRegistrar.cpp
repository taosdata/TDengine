#include "PluginRegistrar.hpp"

void register_plugin_hooks() {
    register_tdengine_plugin_config_hooks();
    register_mqtt_plugin_config_hooks();
    register_kafka_plugin_config_hooks();
}