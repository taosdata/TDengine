#pragma once

#include "IFormatter.hpp"
#include "FormatterFactory.hpp"
#include "StmtFormatOptions.hpp"
#include "StmtV2InsertData.hpp"
#include "ISinkContext.hpp"

class StmtInsertDataFormatter final : public IInsertDataFormatter {
public:
    explicit StmtInsertDataFormatter(const DataFormat& format);

    std::unique_ptr<ISinkContext> init(const InsertDataConfig& config,
                                       const ColumnConfigInstanceVector& col_instances,
                                       const ColumnConfigInstanceVector& tag_instances) override;

    FormatResult format(MemoryPool::MemoryBlock* batch,
                        bool is_checkpoint_recover = false) const override;

private:
    const DataFormat& format_;
    const StmtFormatOptions* format_options_;

    inline static bool registered_ = []() {
        FormatterFactory::register_formatter<InsertDataConfig>(
            "stmt",
            [](const DataFormat& format) {
                return std::make_unique<StmtInsertDataFormatter>(format);
            });
        return true;
    }();
};