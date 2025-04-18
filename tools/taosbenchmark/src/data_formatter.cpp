#include <variant>
#include <unordered_map>
#include <nlohmann/json.hpp>
#include <vector>
#include <set>
#include <functional>
#include <string>
#include <memory>
#include <sstream>
#include <taos.h>


// 数据格式化模块

// 基础数据类型定义
struct DataPoint {
    std::chrono::nanoseconds timestamp;
    std::variant<
        int32_t, uint32_t, int64_t, uint64_t,
        float, double, std::string, std::vector<uint8_t>
    > value;
};

// 数据格式化抽象层
class IDataFormatter {
public:
    virtual ~IDataFormatter() = default;
    virtual std::string format(const TableRecord& record) = 0;
    virtual std::string batch_format(const std::vector<TableRecord>& records) = 0;
};



class SqlData {
public:
    explicit SqlData(std::string&& sql_str)
        : sql_buffer_(std::move(sql_str)) 
    {
        // 保证内存连续且不可变
        sql_buffer_.shrink_to_fit();
        c_str_ = sql_buffer_.data();
    }

    // 禁止拷贝
    SqlData(const SqlData&) = delete;
    SqlData& operator=(const SqlData&) = delete;

    // 允许移动
    SqlData(SqlData&&) = default;
    SqlData& operator=(SqlData&&) = default;

    const char* c_str() const noexcept { return c_str_; }
    size_t size() const noexcept { return sql_buffer_.size(); }

private:
    std::string sql_buffer_;  // 原始SQL数据
    const char* c_str_ = nullptr; // C接口指针
};


/// 数据格式化实现 - SQL语句格式化

class SqlFormatter : public IDataFormatter {
public:
    SqlFormatter(const SuperTableSchema& schema, 
                const ParameterContext& ctx)
        : schema_(schema),
          ctx_(ctx),
          angle_(ctx.get<int>("start_timestamp") % 360),
          timestamp_step_(ctx.get<int>("timestamp_step")),
          batch_size_(ctx.get<int>("batch_size")) {}

    std::unique_ptr<SqlData> batch_format() override {
        try {
            std::ostringstream oss;
            oss.reserve(estimate_batch_size()); // 预分配内存
            
            for (int i = 0; i < batch_size_; ++i) {
                generate_single_row(oss, i);
                update_angle();
            }
            
            return std::make_unique<SqlData>(oss.str());

        } catch (const std::exception& e) {
            handle_error(e);
            return nullptr;
        }
    }

private:
    void generate_single_row(std::ostringstream& oss, int64_t seq) {
        oss << "INSERT INTO " << schema_.table_name << " VALUES (";

        bool first_field = true;
        for (const auto& field : schema_.fields) {
            if (!first_field) oss << ", ";
            first_field = false;

            generate_field_value(oss, field, seq);
        }

        oss << ");\n"; // 分号结束语句
    }

    void generate_field_value(std::ostringstream& oss, 
                             const FieldMeta& field, 
                             int64_t seq) {
        if (field.is_null) {
            oss << "NULL";
            return;
        }

        std::visit([&](auto&& type) {
            using T = std::decay_t<decltype(type)>;
            
            if constexpr (std::is_same_v<T, std::string>) {
                generate_string_value(oss, field, seq);
            } else if constexpr (std::is_same_v<T, Decimal128>) {
                generate_decimal_value(oss, field, seq);
            } else {
                oss << generate_scalar_value<T>(field, seq);
            }
        }, field.data_type);
    }

    template<typename T>
    T generate_scalar_value(const FieldMeta& field, int64_t seq) {
        // 复用已有的数据生成逻辑
        return DataGenerator::generate_value<T>(field, seq, angle_);
    }

    void generate_string_value(std::ostringstream& oss,
                              const FieldMeta& field,
                              int64_t seq) {
        std::string value = DataGenerator::generate_string(field, seq);
        oss << "'" << escape_sql_string(value) << "'";
    }

    void generate_decimal_value(std::ostringstream& oss,
                               const FieldMeta& field,
                               int64_t seq) {
        Decimal128 dec = DataGenerator::generate_decimal(field, seq, angle_);
        char buffer[64];
        decimal128ToString(&dec, field.precision, field.scale, buffer, sizeof(buffer));
        oss << buffer;
    }

    static std::string escape_sql_string(const std::string& input) {
        std::string output;
        output.reserve(input.length() * 1.1);
        
        for (char c : input) {
            if (c == '\'') output += '\'';
            output += c;
        }
        return output;
    }

    size_t estimate_batch_size() const {
        // 根据字段数和类型估算批次大小
        size_t avg_row_len = 100; // 基础长度
        avg_row_len += schema_.fields.size() * 20; // 每个字段估算
        return avg_row_len * batch_size_;
    }

    void update_angle() {
        angle_ += timestamp_step_ / ctx_.get<int>("angle_step");
        if (angle_ > 360) angle_ -= 360;
    }

    const SuperTableSchema& schema_;
    const ParameterContext& ctx_;
    int angle_;
    int timestamp_step_;
    int batch_size_;
};


///////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////

// 使用variant封装所有支持的数据类型
using StmtValue = std::variant<
    bool, int8_t, uint8_t, int16_t, uint16_t,
    int32_t, uint32_t, int64_t, uint64_t,
    float, double, std::string
>;

// 字段元数据（对应C结构Field）
struct FieldMeta {
    TSDB_DATA_TYPE type;
    size_t length;
    std::string name;
    
    // 数据生成策略
    struct {
        double min;
        double max;
        std::vector<StmtValue> enum_values;
        std::function<StmtValue(int64_t)> generator;
    } data_policy;
};

/////////////////////////////////////////////////////////////////////

// STMT通用数据接口
class IStmtData {
public:
    virtual ~IStmtData() = default;
    virtual size_t row_count() const noexcept = 0;
    virtual size_t column_count() const noexcept = 0;
    virtual void bind_to_stmt(void* stmt) const = 0;
};


// STMT v1数据结构
class StmtV1Data : public IStmtData {
public:
    struct ChildTableBatch {
        // 子表元数据
        std::string table_name;
        TAOS_MULTI_BIND tag_bind;  // 标签绑定
        
        // 列数据绑定
        struct ColumnBinding {
            TAOS_MULTI_BIND bind;
            std::vector<uint8_t> data_buffer;
            std::vector<int32_t> lengths;
            std::vector<char> is_null;
        };
        std::vector<ColumnBinding> columns;

        // 批量信息
        size_t batch_size;
        size_t current_row = 0;
    };

    explicit StmtV1Data(const SuperTableSchema& schema)
        : schema_(schema),
          protocol_(schema.protocol_version),
          angle_(schema.start_timestamp % 360) {}

    // 添加子表数据批次
    void add_child_batch(std::string table_name, 
                        TAOS_BIND&& tags,
                        std::vector<ColumnBinding>&& columns,
                        size_t batch_size) 
    {
        batches_.emplace_back(ChildTableBatch{
            std::move(table_name),
            std::move(tags),
            std::move(columns),
            batch_size
        });
    }


    void bind_to_stmt(void* stmt) const override {
        auto* taos_stmt = static_cast<TAOS_STMT*>(stmt);
        for (size_t i = 0; i < columns_.size(); ++i) {
            if (taos_stmt_bind_param(taos_stmt, &columns_[i].bind) != 0) {
                throw std::runtime_error("STMT v1 bind failed");
            }
        }
    }

    // 绑定到STMT的完整流程
    void execute_batch(TAOS_STMT* stmt) const {
        // 1. 初始化STMT
        if (taos_stmt_prepare(stmt, schema_.sql_template.c_str(), 0) != 0) {
            throw_stmt_error(stmt);
        }

        // 2. 遍历所有子表批次
        for (const auto& batch : batches_) {
            // 3. 设置表名和标签
            if (taos_stmt_set_tbname_tags(stmt, 
                batch.table_name.c_str(), 
                &batch.tag_bind) != 0) 
            {
                throw_stmt_error(stmt);
            }

            // 4. 绑定列数据
            std::vector<TAOS_MULTI_BIND> binds;
            binds.reserve(batch.columns.size());
            for (const auto& col : batch.columns) {
                binds.push_back(col.bind);
            }

            if (taos_stmt_bind_param_batch(stmt, binds.data()) != 0) {
                throw_stmt_error(stmt);
            }

            // 5. 添加批次
            if (taos_stmt_add_batch(stmt) != 0) {
                throw_stmt_error(stmt);
            }
        }

        // 6. 执行最终提交
        if (taos_stmt_execute(stmt) != 0) {
            throw_stmt_error(stmt);
        }
    }

private:
    static void throw_stmt_error(TAOS_STMT* stmt) {
        const char* err = taos_stmt_errstr(stmt);
        throw std::runtime_error("STMT operation failed: " + std::string(err));
    }

    const SuperTableSchema& schema_;
    std::vector<ChildTableBatch> batches_;
    int protocol_;
    int angle_;
};


// STMT v2数据结构
class StmtV2Data : public IStmtData {
public:
    struct TableBinding {
        std::string table_name;
        std::vector<TAOS_STMT2_BIND> tag_binds;
        std::vector<TAOS_STMT2_BIND> column_binds;
    };

    explicit StmtV2Data(size_t num_tables, size_t batch_size)
        : bindings_(num_tables),
          batch_size_(batch_size) {}

    template<typename T>
    void add_table_data(size_t table_idx, 
                       const std::string& table_name,
                       const std::vector<T>& tags,
                       const std::vector<std::vector<T>>& columns) {
        auto& binding = bindings_.at(table_idx);
        binding.table_name = table_name;
        
        // 处理标签
        for (const auto& tag : tags) {
            TAOS_STMT2_BIND bind = create_bind(tag);
            binding.tag_binds.push_back(bind);
        }
        
        // 处理列数据
        for (const auto& col : columns) {
            TAOS_STMT2_BIND bind = create_bind(col);
            binding.column_binds.push_back(bind);
        }
    }

    void bind_to_stmt(void* stmt) const override {
        auto* taos_stmt2 = static_cast<TAOS_STMT2*>(stmt);
        TAOS_STMT2_BINDV bindv{
            .count = static_cast<int>(bindings_.size()),
            .tbnames = const_cast<char**>(table_names_.data()),
            .tags = tag_binds_.data(),
            .bind_cols = column_binds_.data()
        };
        
        if (taos_stmt2_bind_param(taos_stmt2, &bindv, -1) != 0) {
            throw std::runtime_error("STMT v2 bind failed");
        }
    }

private:
    template<typename T>
    TAOS_STMT2_BIND create_bind(const std::vector<T>& data) {
        TAOS_STMT2_BIND bind{
            .buffer_type = get_taos_type<T>(),
            .buffer = const_cast<T*>(data.data()),
            .length = nullptr,
            .is_null = nullptr,
            .num = static_cast<int>(data.size())
        };
        return bind;
    }

    std::vector<TableBinding> bindings_;
    std::vector<char*> table_names_;
    size_t batch_size_;
};

// STMT格式化器基类
class StmtFormatter : public IDataFormatter {
public:
    explicit StmtFormatter(std::shared_ptr<ParameterContext> ctx)
        : ctx_(std::move(ctx)) {}

    std::unique_ptr<IStmtData> generate_batch(size_t batch_size) override {
        auto data = create_stmt_data(batch_size);
        fill_data(*data, batch_size);
        return data;
    }

protected:
    virtual std::unique_ptr<IStmtData> create_stmt_data(size_t batch_size) = 0;
    virtual void fill_data(IStmtData& data, size_t batch_size) = 0;

    std::shared_ptr<ParameterContext> ctx_;
};


// 数据格式化实现 - STMT参数绑定写入格式化  
// STMT v1格式化器
class StmtV1Formatter : public StmtFormatter {
public:
    StmtV1Formatter(const SuperTableSchema& schema, 
                   const ParameterContext& ctx)
        : StmtFormatter(schema, ctx),
          child_table_count_(ctx.get<int>("child_table_count")),
          batch_size_(ctx.get<int>("batch_size")) {}

    std::unique_ptr<IStmtData> generate_batch() override {
        auto data = std::make_unique<StmtV1Data>(schema_);

        // 生成多个子表的数据
        for (int i = 0; i < child_table_count_; ++i) {
            generate_child_batch(*data, i);
        }

        return data;
    }

private:
    void generate_child_batch(StmtV1Data& data, int child_idx) {
        // 生成子表名
        std::string table_name = generate_table_name(child_idx);

        // 生成标签数据
        auto tag_bind = generate_tags(child_idx);

        // 生成列数据
        std::vector<StmtV1Data::ColumnBinding> columns;
        columns.reserve(schema_.columns.size());
        
        for (const auto& col_meta : schema_.columns) {
            columns.push_back(generate_column(col_meta));
        }

        // 添加到批次
        data.add_child_batch(
            std::move(table_name),
            std::move(tag_bind),
            std::move(columns),
            batch_size_
        );
    }

    void handle_variable_length(ColumnBinding& cb, const std::vector<std::string>& strs) {
        size_t total_len = 0;
        for (const auto& s : strs) {
            total_len += s.size();
        }
    
        cb.data_buffer.resize(total_len);
        cb.lengths.reserve(strs.size());
        
        size_t offset = 0;
        for (const auto& s : strs) {
            std::memcpy(cb.data_buffer.data() + offset, s.data(), s.size());
            cb.lengths.push_back(s.size());
            offset += s.size();
        }
    
        cb.bind.buffer_length = 0; // 表示变长类型
    }

    StmtV1Data::ColumnBinding generate_column(const FieldMeta& meta) {
        StmtV1Data::ColumnBinding cb;
        cb.bind.buffer_type = get_taos_type(meta.type);
        cb.bind.num = batch_size_;

        // 填充数据
        std::visit([&](auto&& type) {
            using T = std::decay_t<decltype(type)>;
            std::vector<T> values;
            values.reserve(batch_size_);
            
            for (int i = 0; i < batch_size_; ++i) {
                values.push_back(generate_value<T>(meta, i));
            }

            cb.data_buffer.resize(values.size() * sizeof(T));
            std::memcpy(cb.data_buffer.data(), values.data(), cb.data_buffer.size());
            cb.lengths.assign(batch_size_, sizeof(T));
            cb.is_null.assign(batch_size_, 0);

            cb.bind.buffer = cb.data_buffer.data();
            cb.bind.buffer_length = sizeof(T);
            cb.bind.length = cb.lengths.data();
            cb.bind.is_null = cb.is_null.data();
        }, meta.data_type);

        return cb;
    }

    TAOS_BIND generate_tags(int child_idx) {
        TAOS_BIND tag_bind{};
        std::vector<uint8_t> tag_data;

        // 根据子表索引生成唯一标签
        std::visit([&](auto&& type) {
            using T = std::decay_t<decltype(type)>;
            T tag_value = generate_tag_value<T>(child_idx);
            
            tag_data.resize(sizeof(T));
            std::memcpy(tag_data.data(), &tag_value, sizeof(T));

            tag_bind.buffer_type = get_taos_type(typeid(T));
            tag_bind.buffer = tag_data.data();
            tag_bind.buffer_length = sizeof(T);
            tag_bind.length = nullptr; // 固定长度类型不需要
            tag_bind.is_null = nullptr; // 标签不能为NULL
        }, schema_.tag_type);

        return tag_bind;
    }

    int child_table_count_;
    int batch_size_;
};


// STMT v2格式化器
class StmtV2Formatter : public StmtFormatter {
public:
    using StmtFormatter::StmtFormatter;

protected:
    std::unique_ptr<IStmtData> create_stmt_data(size_t batch_size) override {
        auto num_tables = ctx_->get<int>("child_table_count");
        return std::make_unique<StmtV2Data>(num_tables, batch_size);
    }

    void fill_data(IStmtData& data, size_t batch_size) override {
        auto& stmt_v2_data = dynamic_cast<StmtV2Data&>(data);
        const auto& stb_meta = ctx_->get_super_table_meta();
        
        for (int tbl = 0; tbl < ctx_->get<int>("child_table_count"); ++tbl) {
            generate_table_data(stmt_v2_data, tbl, stb_meta, batch_size);
        }
    }

private:
    void generate_table_data(StmtV2Data& data,
                            int table_idx,
                            const SuperTableMeta& meta,
                            size_t batch_size) {
        // 生成标签数据
        std::vector<std::string> tags;
        for (const auto& tag_meta : meta.tags) {
            tags.push_back(generate_tag_value(tag_meta, table_idx));
        }
        
        // 生成列数据
        std::vector<std::vector<double>> columns;
        for (const auto& col_meta : meta.columns) {
            std::vector<double> values;
            for (size_t i = 0; i < batch_size; ++i) {
                values.push_back(generate_column_value(col_meta, i));
            }
            columns.push_back(values);
        }
        
        data.add_table_data(table_idx, 
                           generate_table_name(table_idx),
                           tags,
                           columns);
    }
};




//////////////////////////////////////////////////////////////////////



///////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////


class SchemalessInsertBatch {
public:
    // 移动构造保证数据所有权转移
    SchemalessInsertBatch(std::vector<std::string>&& lines, 
                         int protocol,
                         int precision)
        : lines_(std::move(lines)),
          protocol_(protocol),
          precision_(precision) {
        prepare_c_pointers();
    }

    // 禁止拷贝（保证线程安全）
    SchemalessInsertBatch(const SchemalessInsertBatch&) = delete;
    SchemalessInsertBatch& operator=(const SchemalessInsertBatch&) = delete;

    // 访问接口
    char** c_lines() { return c_lines_.data(); }
    int num_lines() const { return static_cast<int>(lines_.size()); }
    int protocol() const { return protocol_; }
    int precision() const { return precision_; }

private:
    void prepare_c_pointers() {
        c_lines_.reserve(lines_.size());
        for (auto& line : lines_) {
            // 保证字符串内存连续且不会被修改
            line.shrink_to_fit();
            c_lines_.push_back(&line[0]);
        }
    }

    std::vector<std::string> lines_;  // 原始数据
    std::vector<char*> c_lines_;      // C接口需要的指针数组
    const int protocol_;              // 协议类型
    const int precision_;             // 时间精度
};


// 数据格式化实现 - 行协议格式化
class SchemalessFormatter : public IDataFormatter {
public:
    SchemalessFormatter(const SuperTableSchema& schema, const ParameterContext& ctx)
        : schema_(schema),
        ctx_(ctx),
        protocol_(ctx.get<int>("line_protocol")),
        precision_(ctx.get<int>("precision", TSDB_SML_TIMESTAMP_MILLI_SECONDS)), // 默认毫秒
        angle_(ctx.get<int>("start_timestamp") % 360),
        timestamp_step_(ctx.get<int>("timestamp_step")) {}


    std::unique_ptr<SchemalessInsertBatch> batch_format(int64_t loop) override {
        std::vector<std::string> batch;
        batch.reserve(records.size());

        try {
            for (int64_t k = 0; k < loop; ++k) {
                batch.push_back(generate_single_row(k));
                update_angle();
            }
        } catch (const std::exception& e) {
            handle_error(e);
            return nullptr;
        }

        return std::make_unique<SchemalessInsertBatch>(
            std::move(batch), protocol_, precision_);
    }

private:
    std::string generate_single_row(int64_t seq) {
        switch (protocol_) {
            case TSDB_SML_LINE_PROTOCOL:
                return generate_line_protocol(seq);
            case TSDB_SML_TELNET_PROTOCOL:
                return generate_telnet_protocol(seq);
            default:
                return generate_json_protocol(seq);
        }
    }

    std::string generate_line_protocol(int64_t seq) {
        std::ostringstream oss;
        
        // Measurement and tags
        oss << schema_.stb_name << ",";
        generate_tags(oss, seq, ",", "=");
        
        // Fields
        oss << " ";
        generate_fields(oss, seq, ",", "=");
        
        // Timestamp
        oss << " " << generate_timestamp(seq);
        
        std::string result = oss.str();
        replace_last_comma(result);
        return result;
    }

    std::string generate_telnet_protocol(int64_t seq) {
        std::ostringstream oss;
        
        // Tags
        generate_tags(oss, seq, " ", "=");
        
        // Fields
        generate_fields(oss, seq, " ", "=");
        
        // Timestamp
        oss << generate_timestamp(seq);
        
        std::string result = oss.str();
        replace_last_space(result);
        return result;
    }

    std::string generate_json_protocol(int64_t seq) {
        std::ostringstream oss;
        oss << "{";
        
        // Tags
        generate_tags_json(oss, seq);
        
        // Fields
        generate_fields_json(oss, seq);
        
        // Timestamp
        oss << "\"timestamp\":" << generate_timestamp(seq) << "}";
        
        return oss.str();
    }

    void generate_tags(std::ostringstream& oss, int64_t seq, 
                      const std::string& delim, const std::string& assign) {
        for (const auto& tag : schema_.tags) {
            auto value = generate_value(tag, seq);
            format_field(oss, tag, value, true, delim, assign);
        }
    }

    void generate_fields(std::ostringstream& oss, int64_t seq,
                        const std::string& delim, const std::string& assign) {
        for (const auto& field : schema_.columns) {
            auto value = generate_value(field, seq);
            format_field(oss, field, value, false, delim, assign);
        }
    }

    template <typename T>
    void format_field(std::ostringstream& oss, const FieldMeta& meta,
                     const T& value, bool is_tag,
                     const std::string& delim, const std::string& assign) {
        if (protocol_ == TSDB_SML_TELNET_PROTOCOL && !is_tag) {
            oss << value_to_string(value, meta.type) << delim;
        } else {
            oss << meta.name << assign << value_to_string(value, meta.type) << delim;
        }
    }

    std::string value_to_string(const StmtValue& value, TSDB_DATA_TYPE type) {
        return std::visit([&](auto&& arg) {
            using T = std::decay_t<decltype(arg)>;
            
            if constexpr (std::is_same_v<T, std::string>) {
                return format_string_value(arg, type);
            } else if constexpr (std::is_same_v<T, bool>) {
                return arg ? "true" : "false";
            } else {
                return append_type_suffix(std::to_string(arg), type);
            }
        }, value);
    }

    std::string format_string_value(const std::string& value, TSDB_DATA_TYPE type) {
        switch (type) {
            case TSDB_DATA_TYPE_NCHAR:
                return "L\"" + value + "\"";
            case TSDB_DATA_TYPE_JSON:
                return value; // JSON需要特殊处理
            default:
                return "\"" + value + "\"";
        }
    }

    std::string append_type_suffix(const std::string& value, TSDB_DATA_TYPE type) {
        switch (type) {
            case TSDB_DATA_TYPE_TINYINT:   return value + "i8";
            case TSDB_DATA_TYPE_UTINYINT:  return value + "u8";
            case TSDB_DATA_TYPE_SMALLINT:  return value + "i16";
            case TSDB_DATA_TYPE_USMALLINT: return value + "u16";
            case TSDB_DATA_TYPE_INT:       return value + "i32";
            case TSDB_DATA_TYPE_UINT:      return value + "u32";
            case TSDB_DATA_TYPE_BIGINT:    return value + "i64";
            case TSDB_DATA_TYPE_UBIGINT:   return value + "u64";
            case TSDB_DATA_TYPE_FLOAT:    return value + "f32";
            case TSDB_DATA_TYPE_DOUBLE:   return value + "f64";
            default: return value;
        }
    }

    void update_angle() {
        angle_ += timestamp_step_ / ctx_.get<int>("angle_step");
        if (angle_ > 360) angle_ -= 360;
    }

    uint64_t generate_timestamp(int64_t seq) {
        return ctx_.get<uint64_t>("base_timestamp") + 
              (seq * ctx_.get<uint64_t>("timestamp_step"));
    }

    static void replace_last_comma(std::string& s) {
        if (!s.empty() && s.back() == ',') s.back() = ' ';
    }

    static void replace_last_space(std::string& s) {
        if (!s.empty() && s.back() == ' ') s.pop_back();
    }

    // 生成字段值的核心逻辑（复用StmtFormatter的实现）
    StmtValue generate_value(const FieldMeta& meta, int64_t seq) {
        // ...与StmtFormatter相同的生成逻辑...
    }

    // 成员变量
    const SuperTableSchema& schema_;
    const ParameterContext& ctx_;
    int protocol_;
    int angle_;
    uint64_t timestamp_step_;
};




///////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////



// 数据格式化实现 - CSV格式化
class CsvFormatter : public IDataFormatter {
public:
    std::string format(const TableRecord& record) override {
    }
};



