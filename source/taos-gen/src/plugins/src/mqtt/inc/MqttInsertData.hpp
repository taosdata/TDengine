#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <typeindex>

using TopicPayloadPair = std::pair<std::string, std::string>;
using MqttMessageBatch = std::vector<TopicPayloadPair>;
using MqttInsertData = MqttMessageBatch;
inline std::type_index MQTT_TYPE_ID = std::type_index(typeid(MqttInsertData));
inline uint64_t MQTT_TYPE_HASH = MQTT_TYPE_ID.hash_code();