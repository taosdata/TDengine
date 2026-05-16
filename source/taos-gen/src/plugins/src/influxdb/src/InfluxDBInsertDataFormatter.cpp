#include "InfluxDBInsertDataFormatter.hpp"
#include <fmt/format.h>
#include <stdexcept>

InfluxDBInsertDataFormatter::InfluxDBInsertDataFormatter(const DataFormat& format)
    : format_(format) {
    format_options_ = get_format_opt<InfluxDBFormatOptions>(format_, "influxdb");
    if (!format_options_) {
        throw std::runtime_error("InfluxDB formatter options not found in DataFormat");
    }
}

FormatResult InfluxDBInsertDataFormatter::format(MemoryPool::MemoryBlock* batch,
                                                  bool is_checkpoint_recover) const {
    (void)is_checkpoint_recover;
    if (!batch || batch->total_rows == 0) {
        return FormatResult("");
    }

    fmt::memory_buffer line_buffer;
    line_buffer.reserve(1048576);
    int32_t total_rows = 0;

    for (size_t tbl_idx = 0; tbl_idx < batch->used_tables; ++tbl_idx) {
        auto& table_block = batch->tables[tbl_idx];
        if (table_block.used_rows == 0) continue;

        for (size_t row_idx = 0; row_idx < table_block.used_rows; ++row_idx) {
            size_t pos_before = line_buffer.size();
            if (total_rows > 0) {
                line_buffer.push_back('\n');
            }

            if (!RowSerializer::to_influx_inplace(
                cols(), tags(), table_block, row_idx,
                config().schema.name, format_options_->tbname_key, line_buffer)) {
                line_buffer.resize(pos_before);
                continue;
            }
            total_rows++;
        }
    }

    InfluxDBInsertData payload(fmt::to_string(line_buffer), total_rows);

    auto result = BaseInsertData::make_with_payload(
        batch, cols(), tags(), std::move(payload));

    return FormatResult(std::move(result));
}
