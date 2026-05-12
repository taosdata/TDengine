#pragma once

#include "IFormatter.hpp"
#include "FormatterFactory.hpp"
#include "InfluxDBInsertData.hpp"
#include "InfluxDBFormatOptions.hpp"
#include "RowSerializer.hpp"

class InfluxDBInsertDataFormatter final : public IInsertDataFormatter {
public:
    explicit InfluxDBInsertDataFormatter(const DataFormat& format);

    FormatResult format(MemoryPool::MemoryBlock* batch,
                        bool is_checkpoint_recover = false) const override;

private:
    const DataFormat& format_;
    const InfluxDBFormatOptions* format_options_;

    inline static bool registered_ = []() {
        FormatterFactory::register_formatter<InsertDataConfig>(
            "influxdb",
            [](const DataFormat& format) {
                return std::make_unique<InfluxDBInsertDataFormatter>(format);
            });
        return true;
    }();
};
