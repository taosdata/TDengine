#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <typeindex>

using KeyValuePayload = std::pair<std::string, std::string>;
using KafkaMessageBatch = std::vector<KeyValuePayload>;
using KafkaInsertData = KafkaMessageBatch;
inline std::type_index KAFKA_TYPE_ID = std::type_index(typeid(KafkaInsertData));
inline uint64_t KAFKA_TYPE_HASH = KAFKA_TYPE_ID.hash_code();