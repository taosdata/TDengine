#pragma once

#include <string>
#include <cstdint>
#include <cstring>
#include <variant>
#include <vector>
#include "taos.h"
#include "taosws.h"
#include "TableData.h"


class SqlData {
public:
    explicit SqlData(std::string&& sql_str)
        : sql_buffer_(std::move(sql_str)) 
    {
        c_str_ = sql_buffer_.data();
    }

    SqlData(const SqlData&) = delete;
    SqlData& operator=(const SqlData&) = delete;

    SqlData(SqlData&&) = default;
    SqlData& operator=(SqlData&&) = default;

    const char* c_str() const noexcept { return c_str_; }
    size_t size() const noexcept { return sql_buffer_.size(); }
    const std::string& str() const noexcept { return sql_buffer_; }

private:
    std::string sql_buffer_;
    const char* c_str_ = nullptr;
};


class IStmtData {
public:
    virtual ~IStmtData() = default;
    virtual size_t row_count() const noexcept = 0;
    virtual size_t column_count() const noexcept = 0;
    virtual void bind_to_stmt(void* stmt) const = 0;
};

class StmtV2Data : public IStmtData {
public:
    explicit StmtV2Data(const ColumnConfigInstanceVector& col_instances, MultiBatch&& batch) 
        : batch_(std::move(batch)), col_instances_(col_instances) {
        
        size_t table_count = batch_.table_batches.size();
        if (table_count == 0) return;

        bindv_.count = static_cast<int>(table_count);
        table_names_.reserve(table_count);
        column_bind_ptrs_.reserve(table_count);
        
        // 预计算总列数（包括时间戳）
        size_t total_col_count = col_instances_.size() + 1;
        
        // 处理每个子表
        for (auto& [table_name, rows] : batch_.table_batches) {
            if (rows.empty()) continue;
            
            table_names_.push_back(table_name.c_str());
            size_t row_count = rows.size();
            
            // 为当前子表创建列绑定结构
            std::vector<TAOS_STMT2_BIND> col_binds;
            col_binds.reserve(total_col_count);
            
            // 为当前子表分配内存块
            TableMemory table_mem;
            table_mem.column_data.resize(total_col_count);
            
            // 处理时间戳列 (第一列)
            {
                auto& mem = table_mem.column_data[0];
                mem.buffer.resize(row_count * sizeof(int64_t));
                mem.lengths.resize(row_count);
                mem.is_nulls.resize(row_count, 0); // 全部初始化为非NULL
                
                // 复制时间戳数据
                int64_t* ts_buf = reinterpret_cast<int64_t*>(mem.buffer.data());
                for (size_t i = 0; i < row_count; i++) {
                    ts_buf[i] = rows[i].timestamp;
                    mem.lengths[i] = sizeof(int64_t);
                }
                
                col_binds.push_back(TAOS_STMT2_BIND{
                    TSDB_DATA_TYPE_TIMESTAMP,
                    ts_buf,
                    mem.lengths.data(),
                    mem.is_nulls.data(),
                    static_cast<int>(row_count)
                });
            }
            
            // 处理数据列
            for (size_t col_idx = 0; col_idx < col_instances_.size(); col_idx++) {
                const auto& col_config = col_instances_[col_idx];
                auto& mem = table_mem.column_data[col_idx + 1];
                
                // 获取列类型和属性
                int taos_type = col_config.config().get_taos_type();
                bool is_var_len = col_config.config().is_var_length();
                size_t element_size = is_var_len ? col_config.config().len.value()
                                                 : col_config.config().get_fixed_type_size();
                
                // 分配内存
                mem.buffer.resize(row_count * element_size);
                mem.lengths.resize(row_count);
                mem.is_nulls.resize(row_count, 0); // 暂时全部非NULL
                
                // 复制列数据
                size_t current_offset = 0;
                char* col_buf = mem.buffer.data();
                for (size_t row_idx = 0; row_idx < row_count; row_idx++) {
                    const auto& col_data = rows[row_idx].columns[col_idx];
                    
                    // 处理变长类型
                    if (is_var_len) {
                        std::visit([&](const auto& value) {
                            using T = std::decay_t<decltype(value)>;
                            if constexpr (std::is_same_v<T, std::string>) {
                                size_t data_len = std::min(value.length(), element_size);
                                memcpy(col_buf + current_offset, value.data(), data_len);
                                mem.lengths[row_idx] = static_cast<int32_t>(data_len);
                                current_offset += data_len;
                            }
                            else if constexpr (std::is_same_v<T, std::u16string>) {
                                size_t data_len = std::min(value.length() * sizeof(char16_t), element_size);
                                memcpy(col_buf + current_offset, value.data(), data_len);
                                mem.lengths[row_idx] = static_cast<int32_t>(data_len);
                                current_offset += data_len;
                            }
                            else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
                                size_t data_len = std::min(value.size(), element_size);
                                memcpy(col_buf + current_offset, value.data(), data_len);
                                mem.lengths[row_idx] = static_cast<int32_t>(data_len);
                                current_offset += data_len;
                            }
                        }, col_data);
                    } 
                    // 处理固定长度类型
                    else {
                        std::visit([&](const auto& value) {
                            using T = std::decay_t<decltype(value)>;
                            if constexpr (std::is_arithmetic_v<T> || std::is_same_v<T, bool>) {
                                // 对于算术类型和bool类型
                                memcpy(col_buf + row_idx * element_size, &value, element_size);
                                mem.lengths[row_idx] = 0;
                            }
                            else if constexpr (std::is_same_v<T, Decimal>) {
                                // 特殊处理 Decimal 类型
                                memcpy(col_buf + row_idx * element_size, &value, element_size);
                                mem.lengths[row_idx] = 0;
                            }
                            else {
                                throw std::runtime_error("Unsupported data type for fixed-length column");
                            }
                        }, col_data);
                    }
                    
                    // TODO: 空值处理 (需要RowData支持)
                }

                if (is_var_len) {
                    mem.buffer.resize(current_offset);
                }

                col_binds.push_back(TAOS_STMT2_BIND{
                    taos_type,
                    col_buf,
                    mem.lengths.data(),
                    mem.is_nulls.data(),
                    static_cast<int>(row_count)
                });
            }
            
            // 保存当前子表的数据
            column_binds_.push_back(std::move(col_binds));
            table_memories_.push_back(std::move(table_mem));
        }
        
        // 设置指针数组
        for (auto& binds : column_binds_) {
            column_bind_ptrs_.push_back(binds.data());
        }
        
        // 设置BINDV结构
        bindv_.tbnames = const_cast<char**>(table_names_.data());
        bindv_.tags = nullptr;  // 暂不处理tag
        bindv_.bind_cols = column_bind_ptrs_.data();
    }
    
    StmtV2Data(StmtV2Data&& other) noexcept 
        : batch_(std::move(other.batch_))
        , col_instances_(other.col_instances_)
        , table_names_(std::move(other.table_names_))
        , column_binds_(std::move(other.column_binds_))
        , column_bind_ptrs_(std::move(other.column_bind_ptrs_))
        , table_memories_(std::move(other.table_memories_))
        , bindv_(other.bindv_) {
        // 更新 bindv_ 中的指针
        bindv_.tbnames = const_cast<char**>(table_names_.data());
        bindv_.tags = bindv_.tags;
        bindv_.bind_cols = column_bind_ptrs_.data();
    }

    size_t row_count() const noexcept override {
        size_t total = 0;
        for (const auto& [_, rows] : batch_.table_batches) {
            total += rows.size();
        }
        return total;
    }
    
    size_t column_count() const noexcept override {
        if (batch_.table_batches.empty()) return 0;
        const auto& first_table = batch_.table_batches.begin()->second;
        if (first_table.empty()) return 0;
        return first_table.front().columns.size();
    }
    
    void bind_to_stmt(void* stmt) const override {
        if (bindv_.count == 0) return;
        
        int code = taos_stmt2_bind_param(static_cast<TAOS_STMT*>(stmt), const_cast<TAOS_STMT2_BINDV*>(&bindv_), -1);
        if (code != 0) {
            throw std::runtime_error(std::string("Failed to bind parameters: ") + 
                                   taos_stmt2_error(static_cast<TAOS_STMT*>(stmt)));
        }
    }
    
private:
    // 内存管理辅助结构
    struct ColumnMemory {
        std::vector<char> buffer;      // 列数据存储
        std::vector<int32_t> lengths;  // 每行数据长度
        std::vector<char> is_nulls;    // 空值指示器
    };
    
    struct TableMemory {
        std::vector<ColumnMemory> column_data;
    };
    
    MultiBatch batch_;
    const ColumnConfigInstanceVector& col_instances_;
    std::vector<const char*> table_names_;
    std::vector<std::vector<TAOS_STMT2_BIND>> column_binds_;
    std::vector<TAOS_STMT2_BIND*> column_bind_ptrs_;
    std::vector<TableMemory> table_memories_;
    TAOS_STMT2_BINDV bindv_{};
};

struct BaseInsertData {
    int64_t start_time;
    int64_t end_time;
    size_t total_rows;
};

struct SqlInsertData : public BaseInsertData {
    SqlData data;
};

struct StmtV2InsertData : public BaseInsertData {
    StmtV2Data data;
};


// 通用格式化结果类型
using FormatResult = std::variant<std::string, SqlInsertData, StmtV2InsertData>;

