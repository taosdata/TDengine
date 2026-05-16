#include "SchemalessInsertDataFormatter.hpp"
#include "RowSerializer.hpp"
#include <taos.h>
#include <stdexcept>
#include <fmt/format.h>

SchemalessInsertDataFormatter::SchemalessInsertDataFormatter(const DataFormat& format)
    : format_(format) {
    format_options_ = get_format_opt<SchemalessFormatOptions>(format_, "schemaless");
    if (!format_options_) {
        throw std::runtime_error("Schemaless formatter options not found in DataFormat");
    }
}

int SchemalessInsertDataFormatter::map_precision(const std::string& precision) const {
    if (precision == "ms") return TSDB_SML_TIMESTAMP_MILLI_SECONDS;
    if (precision == "us") return TSDB_SML_TIMESTAMP_MICRO_SECONDS;
    if (precision == "ns") return TSDB_SML_TIMESTAMP_NANO_SECONDS;
    return TSDB_SML_TIMESTAMP_MILLI_SECONDS;
}

FormatResult SchemalessInsertDataFormatter::format(MemoryPool::MemoryBlock* batch,
                                                   bool is_checkpoint_recover) const {
    (void)is_checkpoint_recover;
    if (!batch || batch->total_rows == 0) {
        return FormatResult("");
    }

    const std::string& measurement = config().schema.name;
    int precision = map_precision(config().timestamp_precision);

    fmt::memory_buffer line_buffer;
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
                cols(), tags(), table_block, row_idx, measurement, format_options_->tbname_key, line_buffer,
                IntSuffixMode::TDENGINE)) {
                line_buffer.resize(pos_before);
                continue;
            }
            total_rows++;
        }
    }

    SchemalessInsertData sml_data(
        fmt::to_string(line_buffer),
        total_rows,
        TSDB_SML_LINE_PROTOCOL,
        precision);

    auto payload = BaseInsertData::make_with_payload(
        batch, cols(), tags(), std::move(sml_data));

    return FormatResult(std::move(payload));
}
