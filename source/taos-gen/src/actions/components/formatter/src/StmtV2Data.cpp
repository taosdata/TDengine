#include "StmtV2Data.hpp"
#include <cstring>
#include <stdexcept>
#include <algorithm>


StmtV2Data::StmtV2Data(const ColumnConfigInstanceVector& col_instances, MultiBatch&& batch) 
    : batch_(std::move(batch)), col_instances_(col_instances) {
    
    const size_t table_count = batch_.table_batches.size();
    if (table_count == 0) return;

    bindv_.count = static_cast<int>(table_count);
    table_names_.reserve(table_count);
    column_bind_ptrs_.reserve(table_count);
    
    // Pre-calculate total column count (including timestamp)
    const size_t total_col_count = col_instances_.size() + 1;

    table_memories_.reserve(table_count);
    column_binds_.reserve(table_count);

    // Process each sub-table
    for (auto& [table_name, rows] : batch_.table_batches) {
        if (rows.empty()) continue;
        
        table_names_.push_back(table_name.c_str());
        const size_t row_count = rows.size();
        
        // Create column binding structure for current sub-table
        std::vector<TAOS_STMT2_BIND> col_binds;
        col_binds.reserve(total_col_count);
        
        // Allocate memory block for current sub-table
        TableMemory table_mem;
        table_mem.column_data.resize(total_col_count);
        
        // Process timestamp column (first column)
        {
            auto& mem = table_mem.column_data[0];
            mem.buffer.resize(row_count * sizeof(int64_t));
            mem.lengths.assign(row_count, sizeof(int64_t));
            mem.is_nulls.assign(row_count, 0);

            int64_t* ts_buf = reinterpret_cast<int64_t*>(mem.buffer.data());
            for (size_t i = 0; i < row_count; ++i) {
                ts_buf[i] = rows[i].timestamp;
            }
            
            col_binds.push_back(TAOS_STMT2_BIND{
                TSDB_DATA_TYPE_TIMESTAMP,
                ts_buf,
                mem.lengths.data(),
                mem.is_nulls.data(),
                static_cast<int>(row_count)
            });
        }
        
        // Process data columns
        for (size_t col_idx = 0; col_idx < col_instances_.size(); col_idx++) {
            const auto& col_config = col_instances_[col_idx];
            auto& mem = table_mem.column_data[col_idx + 1];
            
            // Get column type and attributes
            const int taos_type = col_config.config().get_taos_type();
            const bool is_var_len = col_config.config().is_var_length();
            const size_t element_size = is_var_len ? col_config.config().len.value()
                                                   : col_config.config().get_fixed_type_size();

            mem.is_nulls.assign(row_count, 0);
            
            if (!is_var_len) {
                mem.buffer.resize(row_count * element_size);
                mem.lengths.assign(row_count, 0);
                
                char* col_buf = mem.buffer.data();
                const size_t stride = element_size;
                
                for (size_t row_idx = 0; row_idx < row_count; ++row_idx) {
                    const auto& col_data = rows[row_idx].columns[col_idx];
                    
                    std::visit([&](const auto& value) {
                        using T = std::decay_t<decltype(value)>;
                        if constexpr (std::is_arithmetic_v<T> || 
                                     std::is_same_v<T, bool> || 
                                     std::is_same_v<T, Decimal>) 
                        {
                            memcpy(col_buf + row_idx * stride, &value, stride);
                        }
                        else {
                            throw std::runtime_error("Unsupported fixed-length type");
                        }
                    }, col_data);
                }
            } else {
                // Calculate the total memory requirement in advance
                size_t total_bytes = 0;
                std::vector<size_t> data_lengths(row_count);
                
                for (size_t row_idx = 0; row_idx < row_count; ++row_idx) {
                    const auto& col_data = rows[row_idx].columns[col_idx];
                    
                    std::visit([&](const auto& value) {
                        using T = std::decay_t<decltype(value)>;
                        if constexpr (std::is_same_v<T, std::string>) {
                            data_lengths[row_idx] = std::min(value.size(), element_size);
                        }
                        else if constexpr (std::is_same_v<T, std::u16string>) {
                            data_lengths[row_idx] = std::min(value.size() * sizeof(char16_t), element_size);
                        }
                        else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
                            data_lengths[row_idx] = std::min(value.size(), element_size);
                        }
                        else {
                            throw std::runtime_error("Unsupported var-length type");
                        }
                        total_bytes += data_lengths[row_idx];
                    }, col_data);
                }
                
                // Allocate memory at one time
                mem.buffer.resize(total_bytes);
                mem.lengths.resize(row_count);
                
                char* col_buf = mem.buffer.data();
                size_t offset = 0;
                
                for (size_t row_idx = 0; row_idx < row_count; ++row_idx) {
                    const auto& col_data = rows[row_idx].columns[col_idx];
                    const size_t data_len = data_lengths[row_idx];
                    
                    std::visit([&](const auto& value) {
                        using T = std::decay_t<decltype(value)>;
                        if constexpr (std::is_same_v<T, std::string>) {
                            memcpy(col_buf + offset, value.data(), data_len);
                        }
                        else if constexpr (std::is_same_v<T, std::u16string>) {
                            memcpy(col_buf + offset, value.data(), data_len);
                        }
                        else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
                            memcpy(col_buf + offset, value.data(), data_len);
                        }
                    }, col_data);
                    
                    mem.lengths[row_idx] = static_cast<int32_t>(data_len);
                    offset += data_len;
                }
            }

            col_binds.push_back(TAOS_STMT2_BIND{
                taos_type,
                mem.buffer.data(),
                mem.lengths.data(),
                mem.is_nulls.data(),
                static_cast<int>(row_count)
            });
        }
        
        // Save current sub-table data
        column_binds_.push_back(std::move(col_binds));
        table_memories_.push_back(std::move(table_mem));
    }
    
    // Set pointer arrays
    column_bind_ptrs_.reserve(column_binds_.size());
    for (auto& binds : column_binds_) {
        column_bind_ptrs_.push_back(binds.data());
    }
    
    // Set BINDV structure
    bindv_.tbnames = const_cast<char**>(table_names_.data());
    bindv_.tags = nullptr;  // Tags not handled for now
    bindv_.bind_cols = column_bind_ptrs_.data();
}

StmtV2Data::StmtV2Data(StmtV2Data&& other) noexcept 
    : batch_(std::move(other.batch_))
    , col_instances_(other.col_instances_)
    , table_names_(std::move(other.table_names_))
    , column_binds_(std::move(other.column_binds_))
    , column_bind_ptrs_(std::move(other.column_bind_ptrs_))
    , table_memories_(std::move(other.table_memories_)) {

    bindv_ = TAOS_STMT2_BINDV{};
    if (!table_names_.empty()) {
        bindv_.count = static_cast<int>(table_names_.size());
        bindv_.tbnames = const_cast<char**>(table_names_.data());
        bindv_.tags = nullptr;  // Tags not handled for now
        bindv_.bind_cols = column_bind_ptrs_.data();
    }

    other.bindv_ = TAOS_STMT2_BINDV{};
}


size_t StmtV2Data::row_count() const noexcept {
    size_t total = 0;
    for (const auto& [_, rows] : batch_.table_batches) {
        total += rows.size();
    }
    return total;
}

size_t StmtV2Data::column_count() const noexcept {
    if (batch_.table_batches.empty()) return 0;
    const auto& first_table = batch_.table_batches.begin()->second;
    if (first_table.empty()) return 0;
    return first_table.front().columns.size();
}

const TAOS_STMT2_BINDV* StmtV2Data::bindv_ptr() const noexcept {
    return &bindv_;
}
