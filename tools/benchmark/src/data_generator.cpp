#include <vector>
#include <memory>
#include <random>
#include <fstream>
#include <taos.h>

// 数据生成器模块
// 数据生成策略模块

class DataGenerator {
public:
    explicit DataGenerator(const ParameterContext& ctx) : ctx_(ctx) {}
    virtual ~DataGenerator() = default;
    
    template<typename T>
    T generate_value(int64_t seq) {
        if (use_buffer_) {
            return get_from_buffer<T>(seq);
        } else {
            return realtime_generate<T>(seq);
        }
    }

protected:
    virtual void init_buffer() {
        if (ctx_.get<bool>("enable_buffer", false)) {
            buffer_size_ = ctx_.get<size_t>("buffer_size", 10000);
            use_buffer_ = true;
            fill_buffer();
        }
    }

    template<typename T>
    void fill_buffer() {
        buffer_.reserve(buffer_size_);
        for (size_t i = 0; i < buffer_size_; ++i) {
            buffer_.push_back(realtime_generate<T>(i));
        }
    }

    template<typename T>
    T get_from_buffer(int64_t seq) {
        size_t idx = seq % buffer_size_;
        return std::any_cast<T>(buffer_[idx]);
    }

    const ParameterContext& ctx_;
    std::vector<std::any> buffer_;
    size_t buffer_size_ = 0;
    bool use_buffer_ = false;
};


// 随机数生成策略
class RandomGenerator : public DataGenerator {
public:
    RandomGenerator(const ParameterContext& ctx)
        : DataGenerator(ctx), dist_(ctx.get<double>("min", 0.0),
                                   ctx.get<double>("max", 1.0)) 
    {
        init_buffer();
    }

    template<typename T>
    T realtime_generate(int64_t seq) {
        if (ctx_.contains("enum_values")) {
            auto& values = ctx_.get<std::vector<T>>("enum_values");
            return values[seq % values.size()];
        }
        return static_cast<T>(dist_(rng_));
    }

    template<typename T>
    T generate(const ColumnMeta& meta) {
        if (!meta.values.empty()) {
            // 从预定义值列表随机选择
            return random_choice(meta.values);
        }
        if (meta.function) {
            // 使用生成函数
            return meta.function(current_timestamp_);
        }
        // 普通范围随机
        return uniform_random(meta.min, meta.max);
    }

private:
    std::mt19937 rng_{std::random_device{}()};
    std::uniform_real_distribution<double> dist_;
};


// 字符串生成策略
class StringGenerator : public DataGenerator {
public:
    StringGenerator(const ParameterContext& ctx)
        : DataGenerator(ctx),
          dict_(load_dictionary(ctx.get<std::string>("dict_path"))) 
    {
        init_buffer();
    }

    std::string realtime_generate(int64_t seq) {
        switch (ctx_.get<StringMode>("string_mode")) {
            case StringMode::RANDOM:
                return generate_random(seq);
            case StringMode::DICT:
                return generate_from_dict(seq);
            case StringMode::PATTERN:
                return generate_pattern(seq);
            default:
                throw std::invalid_argument("Unknown string generation mode");
        }
    }

private:
    std::string generate_random(int64_t seq) {
        const size_t len = ctx_.get<size_t>("str_len", 10);
        static const char charset[] = "abcdefghijklmnopqrstuvwxyz";
        std::string str;
        str.reserve(len);
        
        std::uniform_int_distribution<> dist(0, sizeof(charset)-2);
        for (size_t i=0; i<len; ++i) {
            str += charset[dist(rng_)];
        }
        return str;
    }

    std::vector<std::string> dict_;
    std::mt19937 rng_{std::random_device{}()};
};


// 波形函数数据生成策略
class WaveformGenerator : public DataGenerator {
public:
    WaveformGenerator(const ParameterContext& ctx)
        : DataGenerator(ctx),
          amplitude_(ctx.get<double>("amplitude", 1.0)),
          frequency_(ctx.get<double>("frequency", 1.0)),
          phase_(ctx.get<double>("phase", 0.0)) 
    {
        init_buffer();
    }

    template<typename T>
    T realtime_generate(int64_t seq) {
        double angle = 2 * M_PI * frequency_ * seq + phase_;
        return static_cast<T>(amplitude_ * std::sin(angle));
    }
};


// CSV文件生成策略
class CsvGenerator : public DataGenerator {
public:
    CsvGenerator(const ParameterContext& ctx)
        : DataGenerator(ctx),
          file_path_(ctx.get<std::string>("csv_path")) 
    {
        init_buffer();
        if (!use_buffer_) {
            file_.open(file_path_);
        }
    }

    template<typename T>
    T realtime_generate(int64_t seq) {
        if (use_buffer_) {
            return DataGenerator::get_from_buffer<T>(seq);
        }
        
        std::string line;
        if (!std::getline(file_, line)) {
            file_.clear();
            file_.seekg(0);
            std::getline(file_, line);
        }
        return parse_line<T>(line);
    }

    TableRecord generate() override {}

    std::vector<TableRecord> generate_batch(size_t batch_size) override {
        std::vector<TableRecord> batch;
        std::string line;
        while (batch.size() < batch_size && std::getline(reader_, line)) {
            batch.push_back(parse_csv_line(line));
        }
        return batch;
    }


private:
    template<typename T>
    T parse_line(const std::string& line) {
        // CSV解析具体实现
        return T{};
    }

    std::string file_path_;
    std::ifstream file_;
};


/////////////////////////////////////////////////////////////////////

class GeneratorFactory {
public:
    using Creator = std::function<std::unique_ptr<DataGenerator>(const ParameterContext&)>;
    
    static void register_type(const std::string& type, Creator creator) {
        registry()[type] = creator;
    }
    
    static std::unique_ptr<DataGenerator> create(const ParameterContext& ctx) {
        auto type = ctx.get<std::string>("generator_type");
        return registry().at(type)(ctx);
    }

private:
    static std::map<std::string, Creator>& registry() {
        static std::map<std::string, Creator> instance;
        return instance;
    }
};

// 注册自定义生成器
GeneratorFactory::register_type("custom", [](auto& ctx){
    return std::make_unique<CustomGenerator>(ctx);
});


class RecordPool {
public:
    TableRecord* acquire() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (pool_.empty()) {
            return new TableRecord;
        }
        auto* record = pool_.back();
        pool_.pop_back();
        return record;
    }

    void release(TableRecord* record) {
        std::lock_guard<std::mutex> lock(mutex_);
        pool_.push_back(record);
    }

private:
    std::vector<TableRecord*> pool_;
    std::mutex mutex_;
};



// 随机数据生成示例
TableRecord RandomGenerator::generate() {
    TableRecord record;
    record.timestamp = get_current_ns();
    for (const auto& col : schema_.columns) {
        record.columns.push_back(generate_column(col));
    }
    return record;
}

// CSV数据读取示例
TableRecord CsvGenerator::parse_line(const std::string& line) {
    auto parts = split_csv_line(line);
    return TableRecord{
        .table_name = parts[0],
        .timestamp = parse_timestamp(parts[1]),
        .columns = parse_columns(parts[2])
    };
}
