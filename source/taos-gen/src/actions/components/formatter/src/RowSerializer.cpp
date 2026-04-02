#include "RowSerializer.hpp"
#include <stdexcept>

nlohmann::ordered_json RowSerializer::to_json(
    const ColumnConfigInstanceVector& col_instances,
    const ColumnConfigInstanceVector& tag_instances,
    const MemoryPool::TableBlock& table,
    size_t row_index,
    const std::string& tbname_key) {

    if (row_index >= table.used_rows) {
        throw std::out_of_range("Row index " + std::to_string(row_index) + " is out of range for table with " + std::to_string(table.used_rows) + " used rows");
    }

    nlohmann::ordered_json json_data;

    // Add table name
    if (!tbname_key.empty() && table.table_name) {
        json_data[tbname_key] = table.table_name;
    }

    // Add timestamp
    if (table.timestamps) {
        json_data["ts"] = table.timestamps[row_index];
    }

    auto serialize_cell = [&](const auto& instances, size_t idx,
                              const char* context,
                              auto get_cell_fn) -> void {
        const auto& inst = instances[idx];
        try {
            const auto cell = get_cell_fn(row_index, idx);

            std::visit([&](const auto& value) {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, Decimal>) {
                    json_data[inst.name()] = value.value;
                } else if constexpr (std::is_same_v<T, JsonValue>) {
                    json_data[inst.name()] = value.raw_json;
                } else if constexpr (std::is_same_v<T, Geometry>) {
                    json_data[inst.name()] = value.wkt;
                } else if constexpr (std::is_same_v<T, std::u16string>) {
                    json_data[inst.name()] = StringUtils::u16string_to_utf8(value);
                } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
                    json_data[inst.name()] = std::string(value.begin(), value.end());
                } else {
                    json_data[inst.name()] = value;
                }
            }, cell);

        } catch (const std::exception& e) {
            throw std::runtime_error(
                std::string("RowSerializer::to_json failed for ") + context +
                    " '" + std::string(inst.name()) + "': " + e.what()
            );
        }
    };

    // Add data columns
    for (size_t i = 0; i < col_instances.size(); ++i) {
        serialize_cell(col_instances, i, "column",
                       [&](size_t r, size_t c) -> ColumnType {
                           return table.get_column_cell(r, c);
                       });
    }

    // Add tag columns
    for (size_t i = 0; i < tag_instances.size(); ++i) {
        serialize_cell(tag_instances, i, "tag",
                       [&](size_t, size_t c) -> ColumnType {
                           return table.get_tag_cell(0, c);
                       });
    }

    return json_data;
}

void RowSerializer::to_json_inplace(
    const ColumnConfigInstanceVector& col_instances,
    const ColumnConfigInstanceVector& tag_instances,
    const MemoryPool::TableBlock& table,
    size_t row_index,
    const std::string& tbname_key,
    nlohmann::ordered_json& out) {

    if (row_index >= table.used_rows) {
        throw std::out_of_range("Row index " + std::to_string(row_index) + " is out of range for table with " + std::to_string(table.used_rows) + " used rows");
    }

    // Add table name
    if (!tbname_key.empty() && table.table_name) {
        out[tbname_key] = table.table_name;
    }

    // Add timestamp
    if (table.timestamps) {
        out["ts"] = table.timestamps[row_index];
    }

    auto serialize_cell = [&](const auto& instances, size_t idx,
                              const char* context,
                              auto get_cell_fn) -> void {
        const auto& inst = instances[idx];
        try {
            const auto cell = get_cell_fn(row_index, idx);

            std::visit([&](const auto& value) {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, Decimal>) {
                    out[inst.name()] = value.value;
                } else if constexpr (std::is_same_v<T, JsonValue>) {
                    out[inst.name()] = value.raw_json;
                } else if constexpr (std::is_same_v<T, Geometry>) {
                    out[inst.name()] = value.wkt;
                } else if constexpr (std::is_same_v<T, std::u16string>) {
                    out[inst.name()] = StringUtils::u16string_to_utf8(value);
                } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
                    out[inst.name()] = std::string(value.begin(), value.end());
                } else {
                    out[inst.name()] = value;
                }
            }, cell);

        } catch (const std::exception& e) {
            throw std::runtime_error(
                std::string("RowSerializer::to_json_inplace failed for ")
                    + context + " '" + std::string(inst.name()) + "': " + e.what());
        }
    };

    // Add data columns
    for (size_t i = 0; i < col_instances.size(); ++i) {
        serialize_cell(col_instances, i, "column",
                    [&](size_t r, size_t c) -> ColumnType {
                        return table.get_column_cell(r, c);
                    });
    }

    // Add tag columns
    for (size_t i = 0; i < tag_instances.size(); ++i) {
        serialize_cell(tag_instances, i, "tag",
                    [&](size_t, size_t c) -> ColumnType {
                        return table.get_tag_cell(0, c);
                    });
    }
}

static inline void append_escape_measure_or_key(fmt::memory_buffer& out, std::string_view s) {
    for (char ch : s) {
        if (ch == ' ' || ch == ',' || ch == '=') out.push_back('\\');
        out.push_back(ch);
    }
}

static inline void append_escape_tag_value(fmt::memory_buffer& out, std::string_view s) {
    for (char ch : s) {
        if (ch == ' ' || ch == ',' || ch == '=') out.push_back('\\');
        out.push_back(ch);
    }
}

static inline void append_escape_field_string(fmt::memory_buffer& out, std::string_view s) {
    out.push_back('"');
    for (char ch : s) {
        if (ch == '"' || ch == '\\') out.push_back('\\');
        out.push_back(ch);
    }
    out.push_back('"');
}

static inline std::string to_utf8_for_text_like(const ColumnType& cell) {
    return std::visit([&](const auto& value) -> std::string {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, std::string>) {
            return value;
        } else if constexpr (std::is_same_v<T, std::u16string>) {
            return StringUtils::u16string_to_utf8(value);
        } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
            return std::string(value.begin(), value.end());
        } else if constexpr (std::is_same_v<T, JsonValue>) {
            return value.raw_json;
        } else if constexpr (std::is_same_v<T, Geometry>) {
            return value.wkt;
        } else if constexpr (std::is_same_v<T, Decimal>) {
            return fmt::format("{}", value.value);
        } else {
            return fmt::format("{}", value);
        }
    }, cell);
}

void RowSerializer::to_influx_inplace(
    const ColumnConfigInstanceVector& col_instances,
    const ColumnConfigInstanceVector& tag_instances,
    const MemoryPool::TableBlock& table,
    size_t row_index,
    fmt::memory_buffer& out) {

    if (row_index >= table.used_rows) {
        throw std::out_of_range("Row index " + std::to_string(row_index) + " is out of range for table with " + std::to_string(table.used_rows) + " used rows");
    }

    // measurement
    const char* measurement = table.table_name ? table.table_name : "unknown";
    append_escape_measure_or_key(out, measurement);

    // tags
    for (size_t tag_idx = 0; tag_idx < tag_instances.size(); ++tag_idx) {
        const auto& inst = tag_instances[tag_idx];
        const auto cell = table.get_tag_cell(0, tag_idx);
        out.push_back(',');
        append_escape_measure_or_key(out, inst.name());
        out.push_back('=');
        const auto value_str = to_utf8_for_text_like(cell);
        append_escape_tag_value(out, value_str);
    }

    // Space separator
    out.push_back(' ');

    // Fields
    bool first_field = true;
    for (size_t col_idx = 0; col_idx < col_instances.size(); ++col_idx) {
        const auto& inst = col_instances[col_idx];
        if (!first_field) out.push_back(',');
        first_field = false;

        append_escape_measure_or_key(out, inst.name());
        out.push_back('=');

        const auto cell = table.get_column_cell(row_index, col_idx);
        std::visit([&](const auto& value) {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, bool>) {
                fmt::format_to(std::back_inserter(out), "{}", value ? "true" : "false");
            } else if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>) {
                fmt::format_to(std::back_inserter(out), "{}i", value);
            } else if constexpr (std::is_floating_point_v<T>) {
                fmt::format_to(std::back_inserter(out), "{}", value);
            } else {
                const auto s = to_utf8_for_text_like(cell);
                append_escape_field_string(out, s);
            }
        }, cell);
    }

    // Timestamp
    if (table.timestamps) {
        fmt::format_to(std::back_inserter(out), " {}", table.timestamps[row_index]);
    }
}
