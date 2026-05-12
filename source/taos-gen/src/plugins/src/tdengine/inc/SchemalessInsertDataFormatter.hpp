#pragma once

#include "IFormatter.hpp"
#include "FormatterFactory.hpp"
#include "SchemalessFormatOptions.hpp"
#include "SchemalessInsertData.hpp"

class SchemalessInsertDataFormatter final : public IInsertDataFormatter {
public:
    explicit SchemalessInsertDataFormatter(const DataFormat& format);

    FormatResult format(MemoryPool::MemoryBlock* batch,
                        bool is_checkpoint_recover = false) const override;

private:
    const DataFormat& format_;
    const SchemalessFormatOptions* format_options_;

    int map_precision(const std::string& precision) const;

    inline static bool registered_ = []() {
        FormatterFactory::register_formatter<InsertDataConfig>(
            "schemaless",
            [](const DataFormat& format) {
                return std::make_unique<SchemalessInsertDataFormatter>(format);
            });
        return true;
    }();
};
