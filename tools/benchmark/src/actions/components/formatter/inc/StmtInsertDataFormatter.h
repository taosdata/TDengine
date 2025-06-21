#pragma once
#include <sstream>
#include <limits> 
#include "taos.h"
#include "IFormatter.h"
#include "FormatterFactory.h"


class StmtInsertDataFormatter final : public IInsertDataFormatter {
public:
    explicit StmtInsertDataFormatter(const DataFormat& format) : format_(format) {}

    FormatResult format(const InsertDataConfig& config, 
                        const ColumnConfigInstanceVector& col_instances, 
                        MultiBatch&& batch) const {

        if (batch.table_batches.empty()) {
            return FormatResult("");
        } else {
            if (format_.stmt_config.version != "v2") {
                throw std::invalid_argument("Unsupported stmt version: " + format_.stmt_config.version);
            }

            StmtV2InsertData sql_data{
                BaseInsertData{
                    .start_time = batch.start_time,
                    .end_time = batch.end_time,
                    .total_rows = batch.total_rows
                },
                .data = StmtV2Data(col_instances, std::move(batch))
            };
            return sql_data;
        }
    }

private:
    DataFormat format_;

    inline static bool registered_ = []() {
        FormatterFactory::instance().register_formatter<InsertDataConfig>(
            "stmt",
            [](const DataFormat& format) {
                return std::make_unique<StmtInsertDataFormatter>(format);
            });
        return true;
    }();
};