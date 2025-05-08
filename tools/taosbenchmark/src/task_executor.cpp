
#include "param.h"

// 基础任务类型定义
enum class TaskType {
    INSERT,
    QUERY,
    TMQ,
    WRITE,
    SUPER_TABLE_QUERY,
    SPECIFIED_QUERY,
    MIXED_QUERY
};

// 统一任务配置基类
struct BaseTaskConfig {
    TaskType type;
    int query_times = 1;
    std::vector<std::string> sql_templates;
    std::unordered_map<std::string, std::variant<int, std::string>> params;
};

// 超级表查询配置
struct SuperTableQueryConfig : BaseTaskConfig {
    std::string stblname;
    std::vector<std::string> child_tables;
    int threads_per_group = 1;
    std::string result_file;
};

// 指定SQL查询配置
struct SpecifiedQueryConfig : BaseTaskConfig {
    bool mixed_mode = false;
    std::vector<std::pair<std::string, std::string>> sql_result_mapping;
};


auto generator = std::make_unique<CsvFileGenerator>("data.csv", 1000);
auto writer = WriterFactory::create({
    .protocol = Protocol::TCP_NATIVE,
    .format = Format::STMT
});
AsyncWriteEngine engine(std::move(generator), {std::move(writer)});
engine.start();
// ...监控运行状态
engine.stop();


// 数据库任务封装类
class DatabaseTask {
public:
    DatabaseTask(const ParameterContext& params, size_t db_index)
        : params_(params), db_index_(db_index) 
    {
        init_components();
    }

    void start() {
        producer_ = std::thread(&DatabaseTask::generate_data, this);
        for (auto& t : consumers_) {
            t = std::thread(&DatabaseTask::consume_data, this);
        }
    }

    void stop() { /*...*/ }

private:
    void init_components() {
        // 从参数上下文中获取数据库配置
        auto db_config = params_.get<std::vector<DatabaseMeta>>("databases")[db_index_];
        
        // 初始化数据生成器
        generator_ = create_generator(db_config);
        
        // 初始化写入器
        for (const auto& stb : db_config.super_tables) {
            writers_.push_back(create_writer(stb));
        }
    }

    void generate_data() {
        const size_t total_rows = params_.get<int>("insert_rows");
        const size_t batch_size = params_.get<int>("batch_size", 1000);
        
        while (!generator_->is_finished()) {
            auto batch = generator_->generate_batch(batch_size);
            for (auto& record : batch) {
                pipeline_.push(std::move(record));
            }
        }
    }

    void consume_data() {
        TableRecord record;
        while (running_ || !pipeline_.empty()) {
            if (pipeline_.try_pop(record)) {
                route_to_writer(record);
            }
        }
    }

    std::unique_ptr<DataGenerator> create_generator(const DatabaseMeta& db) {
        if (params_.get<std::string>("data_source") == "csv") {
            return std::make_unique<CsvGenerator>(db.csv_config);
        } else {
            return std::make_unique<RandomGenerator>(db.schema);
        }
    }

    std::unique_ptr<DataWriter> create_writer(const SuperTableMeta& stb) {
        WriterConfig config{
            .protocol = params_.get<std::string>("protocol"),
            .db_name = stb.db_name,
            .batch_size = stb.batch_create_tbl_num
        };
        return WriterFactory::create(config);
    }

    ParameterContext params_;
    size_t db_index_;
    DataPipeline<TableRecord> pipeline_;
    std::unique_ptr<DataGenerator> generator_;
    std::vector<std::unique_ptr<DataWriter>> writers_;
    std::thread producer_;
    std::vector<std::thread> consumers_;
};



// 统一写入任务执行器
class UnifiedWriteExecutor : public ITaskExecutor {
public:
    UnifiedWriteExecutor(const ParameterContext& params)
        : params_(params) 
    {
        init_database_writers();
    }

    void prepare() override {
        // 初始化所有数据库连接
        for (auto& writer : writers_) {
            writer->prepare();
        }
    }

    void execute() override {
        DataGenerator generator = create_generator();
        ThreadPool pool(params_.get<size_t>("threads"));

        while (!generator.is_finished()) {
            auto batch = generator.generate_batch(batch_size_);
            for (auto& record : batch) {
                pool.enqueue([this, record] {
                    auto& writer = select_writer(record);
                    writer.write(record);
                });
            }
        }

        pool.wait_finish();
        flush_all();
    }

private:
    void init_database_writers() {
        const auto& dbs = params_.get<std::vector<DatabaseMeta>>("databases");
        for (const auto& db : dbs) {
            writers_.push_back(WriterFactory::create_writer(params_, db));
        }
    }

    DataGenerator create_generator() {
        if (params_.get<std::string>("data_source") == "csv") {
            return CsvGenerator(params_.get<std::string>("csv_path"));
        }
        return RandomGenerator(params_.get<DataSchema>("schema"));
    }

    DataWriter& select_writer(const TableRecord& record) {
        // 根据记录中的数据库标识选择写入器
        size_t index = hash(record.db_name) % writers_.size();
        return *writers_[index];
    }

    void flush_all() {
        for (auto& writer : writers_) {
            writer->flush();
        }
    }

    ParameterContext params_;
    std::vector<std::unique_ptr<DataWriter>> writers_;
    size_t batch_size_;
};



// 统一任务执行器接口
class ITaskExecutor {
public:
    virtual ~ITaskExecutor() = default;
    virtual void prepare(const ParameterContext& params) = 0;
    virtual void execute() = 0;
    virtual void collect_metrics(QueryMetrics& metrics) = 0;
};


// 任务工厂
class TaskFactory {
public:
    static std::unique_ptr<ITaskExecutor> create(const ParameterContext& params, const TaskMeta& task) {
        switch (task.task_type) {
            case TaskType::INSERT:
                return std::make_unique<ITaskExecutor>(InsertTaskFactory.create(params, task));

            case TaskType::QUERY:
                return std::make_unique<ITaskExecutor>(QueryTaskFactory.create(params, task));

            case TaskType::TMQ:
                return std::make_unique<ITaskExecutor>(TmqTaskFactory.create(params, task));

            default:
                throw std::invalid_argument("Unsupported task type");
        }
    }
};


class InsertTaskFactory {
public:
    static std::unique_ptr<ITaskExecutor> create(const ParameterContext& params, const TaskMeta& task) {
        switch (task.task_type) {
            case TaskType::INSERT:
                return std::make_unique<ITaskExecutor>(InsertTaskFactory.create(params, task));

            case TaskType::QUERY:
                return std::make_unique<ITaskExecutor>(QueryTaskFactory.create(params, task));

            case TaskType::TMQ:
                return std::make_unique<ITaskExecutor>(TmqTaskFactory.create(params, task));

            default:
                throw std::invalid_argument("Unsupported task type");
        }
    }
};








// 超级表查询执行器实现
class SuperTableQueryExecutor : public ITaskExecutor {
public:
    explicit SuperTableQueryExecutor(const ParameterContext& params)
        : params_(params) {}

    void prepare() override {
        // 解析子表列表
        auto stblname = params_.get<std::string>("super_table_query.stblname");
        child_tables_ = fetch_child_tables(stblname);

        // 构建线程任务分组
        int thread_num = params_.get<int>("super_table_query.threads");
        group_tables(thread_num);
    }

    void execute() override {
        ThreadPool pool(params_.get<int>("threads"));
        
        for (auto& group : table_groups_) {
            pool.enqueue([this, &group] {
                process_table_group(group);
            });
        }

        pool.wait_finish();
    }

private:
    void group_tables(int thread_num) {
        size_t per_group = child_tables_.size() / thread_num;
        auto begin = child_tables_.begin();
        
        for (int i = 0; i < thread_num; ++i) {
            auto end = std::next(begin, per_group);
            if (i == thread_num - 1) end = child_tables_.end();
            
            table_groups_.emplace_back(begin, end);
            begin = end;
        }
    }

    void process_table_group(const std::vector<std::string>& tables) {
        auto conn = conn_pool_.get(connection_config());
        int query_times = params_.get<int>("query_times");

        for (const auto& table : tables) {
            for (const auto& sql_tpl : params_.get<std::vector<std::string>>("sqls")) {
                std::string actual_sql = replace_table_name(sql_tpl, table);
                
                for (int i = 0; i < query_times; ++i) {
                    auto result = conn.execute(actual_sql);
                    handle_result(result, table);
                }
            }
        }
    }

    ParameterContext params_;
    std::vector<std::string> child_tables_;
    std::vector<std::vector<std::string>> table_groups_;
};

// 混合查询执行器
class MixedQueryExecutor : public ITaskExecutor {
public:
    explicit MixedQueryExecutor(const ParameterContext& params)
        : params_(params) {}

    void prepare() override {
        sqls_ = params_.get<std::vector<SqlConfig>>("specified_table_query.sqls");
        thread_num_ = params_.get<int>("specified_table_query.threads");
    }

    void execute() override {
        std::vector<std::thread> workers;
        for (int i = 0; i < thread_num_; ++i) {
            workers.emplace_back([this, i] {
                process_worker_task(i);
            });
        }

        for (auto& t : workers) {
            t.join();
        }
    }

private:
    void process_worker_task(int worker_id) {
        auto conn = conn_pool_.get(connection_config());
        int query_times = params_.get<int>("query_times");

        for (const auto& sql_cfg : sqls_) {
            for (int i = 0; i < query_times; ++i) {
                auto result = conn.execute(sql_cfg.sql);
                handle_result(result, sql_cfg.result_file);
            }
        }
    }

    ParameterContext params_;
    std::vector<SqlConfig> sqls_;
    int thread_num_;
};

// 普通查询执行器
class NormalQueryExecutor : public ITaskExecutor {
public:
    void execute() override {
        int thread_num = params_.get<int>("specified_table_query.threads");
        int query_times = params_.get<int>("query_times");

        for (const auto& sql_cfg : sqls_) {
            std::vector<std::thread> workers;
            for (int i = 0; i < thread_num; ++i) {
                workers.emplace_back([&] {
                    auto conn = conn_pool_.get(connection_config());
                    for (int j = 0; j < query_times; ++j) {
                        auto result = conn.execute(sql_cfg.sql);
                        handle_result(result, sql_cfg.result_file);
                    }
                });
            }
            for (auto& t : workers) t.join();
        }
    }
};
