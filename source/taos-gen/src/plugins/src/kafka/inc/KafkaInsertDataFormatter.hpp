#pragma once

#include "IFormatter.hpp"
#include "FormatterFactory.hpp"
#include "KafkaInsertData.hpp"
#include "KafkaFormatOptions.hpp"
#include "KeyGenerator.hpp"
#include "RowSerializer.hpp"
#include <nlohmann/json.hpp>

class KafkaInsertDataFormatter final : public IInsertDataFormatter {
public:
    explicit KafkaInsertDataFormatter(const DataFormat& format);

    FormatResult format(MemoryPool::MemoryBlock* batch,
                        bool is_checkpoint_recover = false) const override;

private:
    const DataFormat& format_;
    const KafkaFormatOptions* format_options_;

    FormatResult format_json(MemoryPool::MemoryBlock* batch) const;

    FormatResult format_influx(MemoryPool::MemoryBlock* batch) const;

    inline static bool registered_ = []() {
        FormatterFactory::register_formatter<InsertDataConfig>(
            "kafka",
            [](const DataFormat& format) {
                return std::make_unique<KafkaInsertDataFormatter>(format);
            });
        return true;
    }();
};