#include "InsertDataAction.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <variant>
#include <type_traits>
#include "FormatterRegistrar.h"
#include "FormatterFactory.h"
#include "TableNameManager.h"
#include "TableDataManager.h"


void InsertDataAction::execute() {
    std::cout << "Inserting data into: " << config_.target.target_type << std::endl;
    if (config_.target.target_type == "tdengine") {
        std::cout << " @ "
                  << config_.target.tdengine.connection_info.host << ":"
                  << config_.target.tdengine.connection_info.port << std::endl;
    } else if (config_.target.target_type == "file_system") {
        std::cout << " @ " << config_.target.file_system.output_dir << std::endl;
    } else {
        throw std::invalid_argument("Unsupported target type: " + config_.target.target_type);
    }

    try {
        // 1. 生成所有子表名并按生产者线程数切分
        TableNameManager name_manager(config_);
        auto all_names = name_manager.generate_table_names();
        
        // 校验生成的表名
        if (all_names.empty()) {
            throw std::runtime_error("No table names were generated");
        }
        
        auto split_names = name_manager.split_for_threads();
        
        // 校验切分结果
        if (split_names.size() != config_.control.data_generation.generate_threads) {
            throw std::runtime_error(
                "Split names count (" + std::to_string(split_names.size()) + 
                ") does not match generate_threads (" + 
                std::to_string(config_.control.data_generation.generate_threads) + ")"
            );
        }

        // 打印分配信息
        for (size_t i = 0; i < split_names.size(); i++) {
            std::cout << "Producer thread " << i << " will handle " 
                     << split_names[i].size() << " tables" << std::endl;
        }
        
        // 2. 创建列配置实例
        auto col_instances = create_column_instances(config_);

        // 3. 初始化数据管道
        const size_t producer_thread_count = config_.control.data_generation.generate_threads;
        const size_t consumer_thread_count = config_.control.insert_control.insert_threads;
        const size_t queue_capacity = config_.control.data_generation.queue_capacity;
        
        // 创建数据管道
        DataPipeline<FormatResult> pipeline(producer_thread_count, consumer_thread_count, queue_capacity);

        // 4. 启动消费者线程
        std::vector<std::thread> consumer_threads;
        consumer_threads.reserve(consumer_thread_count);

        auto consumer_running = std::make_unique<std::atomic<bool>[]>(consumer_thread_count);
        for (size_t i = 0; i < consumer_thread_count; i++) {
            consumer_running[i].store(true);
        }

        // Start consumer threads
        for (size_t i = 0; i < consumer_thread_count; i++) {
            consumer_threads.emplace_back([&, i] {
                consumer_thread_function(i, pipeline, consumer_running[i]);
            });
        }

        // 5. 启动生产者线程
        std::vector<std::shared_ptr<TableDataManager>> data_managers;
        data_managers.reserve(producer_thread_count);
        
        std::vector<std::thread> producer_threads;
        producer_threads.reserve(producer_thread_count);

        auto producer_finished = std::make_unique<std::atomic<bool>[]>(producer_thread_count);
        for (size_t i = 0; i < producer_thread_count; i++) {
            producer_finished[i].store(false);
        }

        std::atomic<size_t> active_producers(producer_thread_count);

        for (size_t i = 0; i < producer_thread_count; i++) {
            auto data_manager = std::make_shared<TableDataManager>(config_, col_instances);
            data_managers.push_back(data_manager);

            producer_threads.emplace_back([=, &pipeline, &active_producers, &producer_finished] {
                try {
                    producer_thread_function(i, split_names[i], col_instances, pipeline, data_manager);
                    producer_finished[i].store(true);
                } catch (const std::exception& e) {
                    std::cerr << "Producer thread " << i << " failed: " << e.what() << std::endl;
                }
                active_producers--;
            });
        }

        // 6. 监控
        const auto start_time = std::chrono::steady_clock::now();
        size_t last_total_rows = 0;
        auto last_time = start_time;
        
        while (active_producers > 0) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            
            // 计算所有生产者生成的总行数
            size_t total_rows = 0;
            for (const auto& manager : data_managers) {
                total_rows += manager->get_total_rows_generated();
            }

            // 获取当前时间点
            const auto now = std::chrono::steady_clock::now();
            
            // 计算这次统计的实际时间间隔（秒）
            const auto interval = std::chrono::duration<double>(now - last_time).count();
            
            // 计算实时速率（行/秒）
            const double rows_per_sec = interval > 0 ? 
                static_cast<double>(total_rows - last_total_rows) / interval : 0.0;
            
            // 更新上次的值
            last_total_rows = total_rows;
            last_time = now;
            
            // 计算总运行时间
            const auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - start_time);
            
            std::cout << "Runtime: " << duration.count() << "s | "
                     << "Rate: " << std::fixed << std::setprecision(1) << rows_per_sec << " rows/s | "
                     << "Total: " << total_rows << " rows | "
                     << "Queue: " << pipeline.total_queued() << " items\n";
        }

        // 7. 等待生产者完成
        for (auto& t : producer_threads) {
            if (t.joinable()) t.join();
        }
        
        // 终止管道（通知消费者）
        pipeline.terminate();
        
        // 8. 等待消费者完成
        // for (size_t i = 0; i < consumer_thread_count; i++) {
        //     consumer_running[i] = false;
        // }
        
        for (auto& t : consumer_threads) {
            if (t.joinable()) t.join();
        }

        std::cout << "InsertDataAction completed successfully" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "InsertDataAction failed: " << e.what() << std::endl;
        throw;
    }
}

void InsertDataAction::producer_thread_function(
    size_t producer_id,
    const std::vector<std::string>& assigned_tables,
    const ColumnConfigInstanceVector& col_instances,
    DataPipeline<FormatResult>& pipeline,
    std::shared_ptr<TableDataManager> data_manager)
{
    // 初始化数据管理器
    if (!data_manager->init(assigned_tables)) {
        throw std::runtime_error("TableDataManager initialization failed for producer " + std::to_string(producer_id));
    }

    // 创建格式化器
    auto formatter = FormatterFactory::instance().create_formatter<InsertDataConfig>(config_.control.data_format);

    // 数据生成循环
    while (auto batch = data_manager->next_multi_batch()) {
        // 格式化数据
        FormatResult formatted_result = formatter->format(config_, col_instances, batch.value());

        // Debug: 打印格式化结果信息
        std::visit([producer_id](const auto& result) {
            using T = std::decay_t<decltype(result)>;
            
            if constexpr (std::is_same_v<T, SqlInsertData>) {
                std::cout << "Producer " << producer_id 
                          << ": sql data, rows: " << result.total_rows
                          << ", time range: [" << result.start_time 
                          << ", " << result.end_time << "]"
                          << ", length: " << result.data.str().length() 
                          << " bytes" << std::endl;
            } else if constexpr (std::is_same_v<T, StmtV2InsertData>) {
                std::cout << "Producer " << producer_id 
                          << ": stmt v2 data, rows: " << result.total_rows
                          << ", time range: [" << result.start_time 
                          << ", " << result.end_time << "]"
                        //   << ", length: " << result.data.length() 
                          << " bytes" << std::endl;
            } else if constexpr (std::is_same_v<T, std::string>) {
                std::cout << "Producer " << producer_id 
                          << ": unknown format result type: " 
                          << typeid(result).name() << ", content: " 
                          << result.substr(0, 100) 
                          << (result.length() > 100 ? "..." : "") 
                          << ", length: " << result.length() 
                          << " bytes" << std::endl;

                throw std::runtime_error("Unknown format result type: " + std::string(typeid(result).name()));
            }
        }, formatted_result);
        
        // 将数据推送到管道
        pipeline.push_data(producer_id, std::move(formatted_result));

        std::cout << "Producer " << producer_id << ": Pushed batch for table(s): "
                << batch->table_batches.size() << ", total rows: " << batch->total_rows 
                << ", queue size: " << pipeline.total_queued() << std::endl;
    }
}

void InsertDataAction::consumer_thread_function(
    size_t consumer_id,
    DataPipeline<FormatResult>& pipeline,
    std::atomic<bool>& running) 
{
    // 创建数据库连接器
    std::unique_ptr<DatabaseConnector> connector;
    try {
        connector = DatabaseConnector::create(config_.control.data_channel, 
                                              config_.target.tdengine.connection_info);
        connector->connect();
    } catch (const std::exception& e) {
        std::cerr << "Consumer " << consumer_id << " failed to connect: " << e.what() << std::endl;
        return;
    }
    
    // 失败重试逻辑
    const auto& failure_cfg = config_.control.insert_control.failure_handling;
    size_t retry_count = 0;
    
    // 数据处理循环
    while (running) {
        auto result = pipeline.fetch_data(consumer_id);

        switch (result.status) {
        case DataPipeline<FormatResult>::Status::Success:
            try {
                std::visit([&](const auto& formatted_result) {
                    using T = std::decay_t<decltype(formatted_result)>;
                    
                    if constexpr (std::is_same_v<T, SqlInsertData>) {
                        std::cout << "Consumer " << consumer_id 
                                 << ": Executed SQL with " << formatted_result.total_rows 
                                 << " rows" << std::endl;
                        connector->execute(formatted_result.data.str());
                    } else if constexpr (std::is_same_v<T, StmtV2InsertData>) {
                        std::cout << "Consumer " << consumer_id 
                                 << ": Executed STMT with " << formatted_result.total_rows 
                                 << " rows" << std::endl;
                        // connector->execute(formatted_result.data);
                    } else {
                        throw std::runtime_error("Unknown format result type: " + std::string(typeid(formatted_result).name()));
                    }
                }, *result.data);

                retry_count = 0;
            } catch (const std::exception& e) {
                std::cerr << "Consumer " << consumer_id << " write failed: " << e.what() << std::endl;
                
                // 处理失败
                if (retry_count < failure_cfg.max_retries) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(failure_cfg.retry_interval_ms));
                    retry_count++;
                } else if (failure_cfg.on_failure == "exit") {
                    std::cerr << "Consumer " << consumer_id << " exiting after " 
                         << retry_count << " retries" << std::endl;
                    return;
                }
            }
            break;
            
        case DataPipeline<FormatResult>::Status::Terminated:
            // 管道已终止，退出线程
            std::cout << "Consumer " << consumer_id << " received termination signal" << std::endl;
            connector->close();
            return;
            
        case DataPipeline<FormatResult>::Status::Timeout:
            // 短暂休眠
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            break;
        }
    }

    // 关闭连接
    connector->close();
}

ColumnConfigInstanceVector InsertDataAction::create_column_instances(const InsertDataConfig& config) const {
    try {
        const ColumnConfigVector& schema = [&]() -> const ColumnConfigVector& {
            if (config_.source.columns.source_type == "generator") {
                if (config_.source.columns.generator.schema.empty()) {
                    throw std::invalid_argument("Schema configuration is empty");
                }
                return config_.source.columns.generator.schema;
            } else if (config_.source.columns.source_type == "csv") {
                if (config_.source.columns.csv.schema.empty()) {
                    throw std::invalid_argument("CSV schema configuration is empty");
                }
                return config_.source.columns.csv.schema;
            }
            throw std::invalid_argument("Unsupported source type: " + config.source.columns.source_type);
        }();
        
        return ColumnConfigInstanceFactory::create(schema);
    } catch (const std::exception& e) {
        throw std::runtime_error(std::string("Failed to create column instances: ") + e.what());
    }
}
