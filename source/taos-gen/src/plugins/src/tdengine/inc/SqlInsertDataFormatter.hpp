#pragma once

#include "IFormatter.hpp"
#include "FormatterFactory.hpp"
#include "SqlFormatOptions.hpp"
#include "SqlInsertData.hpp"
#include <limits>
#include <string>

class SqlInsertDataFormatter final : public IInsertDataFormatter {
public:
    explicit SqlInsertDataFormatter(const DataFormat& format);

    FormatResult format(MemoryPool::MemoryBlock* batch,
                        bool is_checkpoint_recover = false) const override;

private:
    const DataFormat& format_;
    const SqlFormatOptions* format_options_;

    inline static bool registered_ = []() {
        FormatterFactory::register_formatter<InsertDataConfig>(
            "sql",
            [](const DataFormat& format) {
                return std::make_unique<SqlInsertDataFormatter>(format);
            });
        return true;
    }();
};