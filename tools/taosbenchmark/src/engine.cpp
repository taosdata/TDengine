#include <queue>
#include <vector>
#include <memory>
#include <thread>
#include <functional>
#include <variant>

#include "param.h"



auto config = load_json_config("test_case.json");
auto generator = DataGeneratorFactory::create(config);
std::vector<std::unique_ptr<DataWriter>> writers;
writers.push_back(WriterFactory::create_file_writer(config));
writers.push_back(WriterFactory::create_taos_writer(config));
AsyncWriteEngine engine(std::move(generator), std::move(writers));
engine.start();


class PluginManager {
    public:
        void register_generator(const std::string& name, GeneratorCreator creator);
        void register_writer(const std::string& protocol, WriterCreator creator);
};


// 增强型写入引擎
class AsyncWriteEngine {
public:
    explicit AsyncWriteEngine(const ParameterContext& params)
        : params_(params) 
    {
        init_database_tasks();
    }

    void start() {
        for (auto& task : db_tasks_) {
            task->start();
        }
    }

    void stop() { /*...*/ }

private:
    void init_database_tasks() {
        size_t db_count = params_.get<std::vector<DatabaseMeta>>("databases").size();
        for (size_t i = 0; i < db_count; ++i) {
            db_tasks_.push_back(std::make_unique<DatabaseTask>(params_, i));
        }
    }

    const ParameterContext& params_;
    std::vector<std::unique_ptr<DatabaseTask>> db_tasks_;
};

// 核心写入引擎
class AsyncWriteEngine {
public:
    AsyncWriteEngine(
        std::unique_ptr<DataGenerator> generator,
        std::vector<std::unique_ptr<DataWriter>> writers,
        size_t concurrency = std::thread::hardware_concurrency()
    ) : generator_(std::move(generator)),
        writers_(std::move(writers)),
        running_(false) {}

    void start() {
        running_ = true;
        
        // 启动生成线程
        producer_ = std::thread([this] {
            constexpr size_t batch_size = 1000;
            while (running_ && !generator_->is_finished()) {
                auto batch = generator_->generate_batch(batch_size);
                for (auto& record : batch) {
                    pipeline_.push(std::move(record));
                }
            }
        });

        // 启动消费线程
        for (size_t i = 0; i < writers_.size(); ++i) {
            consumers_.emplace_back([this, i] {
                TableRecord record;
                while (running_ || pipeline_.size() > 0) {
                    if (pipeline_.try_pop(record)) {
                        writers_[i]->write(record);
                    } else {
                        std::this_thread::yield();
                    }
                }
                writers_[i]->flush();
            });
        }
    }

    void stop() {
        running_ = false;
        if (producer_.joinable()) producer_.join();
        for (auto& t : consumers_) {
            if (t.joinable()) t.join();
        }
    }

private:
    std::unique_ptr<DataGenerator> generator_;
    std::vector<std::unique_ptr<DataWriter>> writers_;
    DataPipeline<TableRecord> pipeline_;
    std::thread producer_;
    std::vector<std::thread> consumers_;
    std::atomic<bool> running_;
};


class Scheduler {
public:
    void add_task(ITask* task) {
        if (task->type() == WRITE) {
            write_queue_.enqueue(task);
        } else {
            query_queue_.enqueue(task);
        }
    }
    
    void run() {
        while (auto task = get_next_task()) {
            auto executor = ThreadPool::get_executor();
            executor->submit(task);
        }
    }
};

ParameterContext params;
params.load("config.json");
params.validate(); // 检查必填参数

auto engine = std::make_unique<UnifiedBenchmarkEngine>(params);


// 统一执行引擎核心类
class UnifiedBenchmarkEngine {
public:
    explicit UnifiedBenchmarkEngine(const ParameterContext& params)
        : params_(params) {
        init_tasks();
    }

    void run() {
        // 初始化连接池
        init_connection_pool();

        // 启动监控线程
        monitor_ = std::thread([this] {
            while (!stopped_) {
                collect_system_metrics();
                std::this_thread::sleep_for(1s);
            }
        });

        // 执行所有任务
        execute_tasks();
    }

    void stop() {
        stopped_ = true;
        if (monitor_.joinable()) monitor_.join();
    }

private:
    void init_tasks() {
        // task: insert/query/tmq
        for (auto& task : params_.get<std::vector<TaskMeta>>("tasks")) {
            tasks_.push_back(std::make_unique<ITaskExecutor>(TaskFactory::create(params_, task)));
        }
    }

    void execute_tasks() {
        std::vector<std::future<void>> futures;
        
        // 并行执行独立任务
        for (auto& task : tasks_) {
            futures.push_back(std::async(std::launch::async, [&task] {
                task->prepare();
                task->execute();
            }));
        }

        // 等待所有任务完成
        for (auto& f : futures) {
            f.get();
        }
    }



    ParameterContext params_;
    std::vector<std::unique_ptr<ITaskExecutor>> tasks_;
    // ConnectionPool conn_pool_;
    std::thread monitor_;
    std::atomic<bool> stopped_{false};
};



class StrategyInjector {
public:
    void apply_strategy(const std::string& name, 
                      std::function<void(DatabaseTask&)> strategy) {
        strategies_[name] = strategy;
    }

    void execute(const std::string& name, DatabaseTask& task) {
        if (strategies_.count(name)) {
            strategies_[name](task);
        }
    }
};

AsyncWriteEngine engine(params); // 自动创建多个DatabaseTask


engine.start();

// 监控线程
std::thread monitor([&] {
    while (running) {
        print_stats(engine.get_stats());
        std::this_thread::sleep_for(1s);
    }
});

engine.stop();
monitor.join();

