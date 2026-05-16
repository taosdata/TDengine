#pragma once

#include "IFormatter.hpp"
#include "FormatterFactory.hpp"
#include "MqttInsertData.hpp"
#include "MqttFormatOptions.hpp"
#include "TopicGenerator.hpp"
#include "Compressor.hpp"
#include "EncodingConverter.hpp"
#include "RowSerializer.hpp"
#include <nlohmann/json.hpp>

class MqttInsertDataFormatter final : public IInsertDataFormatter {
public:
    explicit MqttInsertDataFormatter(const DataFormat& format);

    FormatResult format(MemoryPool::MemoryBlock* batch,
                        bool is_checkpoint_recover = false) const override;

private:
    const DataFormat& format_;
    const MqttFormatOptions* format_options_;

    FormatResult format_json(MemoryPool::MemoryBlock* batch) const;

    inline static bool registered_ = []() {
        FormatterFactory::register_formatter<InsertDataConfig>(
            "mqtt",
            [](const DataFormat& format) {
                return std::make_unique<MqttInsertDataFormatter>(format);
            });
        return true;
    }();
};