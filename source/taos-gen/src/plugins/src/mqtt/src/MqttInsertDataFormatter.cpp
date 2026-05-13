#include "MqttInsertData.hpp"
#include "MqttInsertDataFormatter.hpp"
#include <stdexcept>

MqttInsertDataFormatter::MqttInsertDataFormatter(const DataFormat& format) : format_(format) {
    format_options_ = get_format_opt<MqttFormatOptions>(format_, "mqtt");
    if (!format_options_) {
        throw std::runtime_error("MQTT formatter options not found in DataFormat");
    }
}

FormatResult MqttInsertDataFormatter::format(MemoryPool::MemoryBlock* batch, bool is_checkpoint_recover) const {
    (void)is_checkpoint_recover;
    if (!batch || batch->total_rows == 0) {
        return FormatResult("");
    }

    return format_json(batch);
}

FormatResult MqttInsertDataFormatter::format_json(MemoryPool::MemoryBlock* batch) const {

    CompressionType compression_type = string_to_compression(format_options_->compression);
    EncodingType encoding_type = string_to_encoding(format_options_->encoding);

    MqttInsertData msg_batch;
    msg_batch.reserve((batch->total_rows + format_options_->records_per_message - 1) / format_options_->records_per_message);

    TopicGenerator topic_generator(format_options_->topic, *col_instances_, *tag_instances_);


    if (format_options_->records_per_message == 1) {
        nlohmann::ordered_json json_data;

        for (size_t table_idx = 0; table_idx < batch->used_tables; ++table_idx) {
            auto& table_block = batch->tables[table_idx];
            for (size_t row_idx = 0; row_idx < table_block.used_rows; ++row_idx) {
                std::string topic = topic_generator.generate(table_block, row_idx);
                RowSerializer::to_json_inplace(*col_instances_, *tag_instances_, table_block, row_idx, format_options_->tbname_key, json_data);
                std::string payload = json_data.dump();

                // Encoding conversion
                if (encoding_type != EncodingType::UTF8) {
                    payload = EncodingConverter::convert(payload, EncodingType::UTF8, encoding_type);
                }

                // Compression
                if (compression_type != CompressionType::NONE) {
                    payload = Compressor::compress(payload, compression_type);
                }

                msg_batch.emplace_back(std::move(topic), std::move(payload));
                json_data.clear();
            }
        }
    } else {
        nlohmann::ordered_json json_array = nlohmann::ordered_json::array();
        std::string first_record_topic;
        nlohmann::ordered_json record_json;

        for (size_t table_idx = 0; table_idx < batch->used_tables; ++table_idx) {
            auto& table_block = batch->tables[table_idx];
            for (size_t row_idx = 0; row_idx < table_block.used_rows; ++row_idx) {
                // If this is the first record of a new batch, generate the topic
                if (json_array.empty()) {
                    first_record_topic = topic_generator.generate(table_block, row_idx);
                }

                // Add the current record's JSON to the array
                RowSerializer::to_json_inplace(*col_instances_, *tag_instances_, table_block, row_idx, format_options_->tbname_key, record_json);
                json_array.emplace_back(std::move(record_json));
                record_json.clear();

                // If the batch is full, create the message and reset
                if (json_array.size() >= format_options_->records_per_message) {
                    std::string payload = json_array.dump();

                    // Encoding conversion
                    if (encoding_type != EncodingType::UTF8) {
                        payload = EncodingConverter::convert(payload, EncodingType::UTF8, encoding_type);
                    }

                    // Compression
                    if (compression_type != CompressionType::NONE) {
                        payload = Compressor::compress(payload, compression_type);
                    }

                    msg_batch.emplace_back(std::move(first_record_topic), std::move(payload));
                    json_array.clear();
                }
            }
        }

        // Add any remaining records from the last partial batch
        if (!json_array.empty()) {
            std::string payload = json_array.dump();
            if (encoding_type != EncodingType::UTF8) {
                payload = EncodingConverter::convert(payload, EncodingType::UTF8, encoding_type);
            }
            if (compression_type != CompressionType::NONE) {
                payload = Compressor::compress(payload, compression_type);
            }
            msg_batch.emplace_back(std::move(first_record_topic), std::move(payload));
        }
    }

    auto payload = BaseInsertData::make_with_payload(batch, *col_instances_, *tag_instances_, std::move(msg_batch));
    return FormatResult(std::move(payload));
}