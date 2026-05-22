#include "InsertDataAction.hpp"
#include "ColumnsCSVReader.hpp"
#include "LogUtils.hpp"
#include "SignalManager.hpp"
#include "FormatterRegistrar.hpp"
#include "FormatterFactory.hpp"
#include "TableNameManager.hpp"
#include "TableDataManager.hpp"
#include "SinkPluginFactory.hpp"
#include "BaseSinkPlugin.hpp"
#include "TimeRecorder.hpp"
#include "ProcessUtils.hpp"
#include <cstring>
#include <iostream>
#include <chrono>
#include <thread>
#include <optional>
#include <variant>
#include <type_traits>
#include <iomanip>
#include <cctz/civil_time.h>
#include <cctz/time_zone.h>

#if !defined(_WIN32)
#include <sched.h>
#include <pthread.h>
#endif


void InsertDataAction::set_realtime_priority() {
#if defined(__linux__)
    struct sched_param param;
    param.sched_priority = sched_get_priority_max(SCHED_FIFO);

    if (pthread_setschedparam(pthread_self(), SCHED_FIFO, &param) != 0) {
        LogUtils::warn("Failed to set real-time priority. "
            "Requires root privileges or CAP_SYS_NICE capability.");
    }
#endif
}

void InsertDataAction::set_thread_affinity(size_t thread_id, bool reverse, const std::string& purpose) {
#if defined(__linux__)
    // Get available CPU cores
    unsigned int num_cores = std::thread::hardware_concurrency();
    if (num_cores == 0) num_cores = 1;

    // Calculate core ID, supporting forward or reverse binding
    size_t core_id = reverse
        ? (num_cores - 1 - (thread_id % num_cores))
        : (thread_id % num_cores);

    // Set affinity
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);

    int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
        LogUtils::warn(
            "Failed to set thread affinity for thread {} to core {} (error code: {}, {})",
            thread_id,
            core_id,
            rc,
            strerror(rc)
        );
        return;
    }

    if (global_.verbose) {
        // Print binding info if verbose mode is enabled
        LogUtils::debug("{}Thread {} bound to core {}{}",
            (purpose.empty() ? "" : (purpose + " ")),
            thread_id,
            core_id,
            reverse ? " (reverse binding)" : " (forward binding)"
        );
    }
#else
    (void)thread_id;
    (void)reverse;
    (void)purpose;
#endif
}

void InsertDataAction::execute() {
    std::optional<ConnectorSource> conn_source;

    try {
        // Generate all child table names and split by producer thread count
        TableNameManager name_manager(config_);
        auto all_names = name_manager.generate_table_names();

        // Check generated table names
        if (all_names.empty()) {
            throw std::runtime_error("No table names were generated");
        }

        const auto split_names = name_manager.split_for_threads();

        // Check split result
        if (split_names.size() != config_.schema.generation.generate_threads.value()) {
            throw std::runtime_error(
                "Split names count (" + std::to_string(split_names.size()) +
                ") does not match generate_threads (" +
                std::to_string(config_.schema.generation.generate_threads.value()) + ")"
            );
        }

        // Print assignment info
        for (size_t i = 0; i < split_names.size(); i++) {
            LogUtils::info("Producer thread {} will handle {} tables", i, split_names[i].size());
        }

        // Initialize data pipeline
        const size_t producer_thread_count = config_.schema.generation.generate_threads.value();
        const size_t consumer_thread_count = config_.insert_threads;
        const size_t queue_capacity = config_.queue_capacity;
        const double queue_warmup_ratio = config_.queue_warmup_ratio;
        const bool shared_queue = config_.shared_queue;
        const size_t rows_per_request = config_.schema.generation.rows_per_batch;
        const size_t interlace_rows = config_.schema.generation.interlace_mode.rows;
        const size_t rows_per_table = static_cast<size_t>(config_.schema.generation.rows_per_table);
        const bool tables_reuse_data = config_.schema.generation.tables_reuse_data;
        const size_t num_cached_batches = config_.schema.generation.data_cache.enabled ?
            config_.schema.generation.data_cache.num_cached_batches : 0;

        size_t num_blocks = queue_capacity * consumer_thread_count;
        size_t max_tables_per_block = std::min(name_manager.chunk_size(), rows_per_request);
        size_t max_rows_per_table = std::min(rows_per_table, rows_per_request);

        if (config_.schema.generation.interlace_mode.enabled) {
            max_tables_per_block = std::min(max_tables_per_block, (rows_per_request + interlace_rows - 1) / interlace_rows);
            max_rows_per_table = std::min(max_rows_per_table, interlace_rows);

        } else {
            max_tables_per_block = std::min(max_tables_per_block, (rows_per_request + rows_per_table - 1) / rows_per_table);
        }

        auto pool = std::make_unique<MemoryPool>(
            num_blocks, max_tables_per_block, max_rows_per_table,
            col_instances_, tag_instances_, tables_reuse_data, num_cached_batches
        );

        if (config_.schema.generation.data_cache.enabled) {
            LogUtils::info("Generation data cache mode enabled with {} cache units.", pool->get_cache_units_count());

            init_cache_units_data(*pool, num_cached_batches, max_tables_per_block, max_rows_per_table);
        }

        // Create data pipeline
        DataPipeline<FormatResult> pipeline(shared_queue, producer_thread_count, consumer_thread_count, queue_capacity);
        Latch startup_latch(consumer_thread_count);

        // Start consumer threads
        std::vector<std::thread> consumer_threads;
        consumer_threads.reserve(consumer_thread_count);

        std::vector<std::unique_ptr<ISinkPlugin>> sink_plugins;
        sink_plugins.reserve(consumer_thread_count);

        const size_t group_size = 10;
        GarbageCollector<FormatResult> gc((consumer_thread_count + group_size - 1) / group_size);

        auto action_info = std::make_shared<ActionRegisterInfo>();
        if (config_.checkpoint_info.enabled && config_.data_format.format_type == "stmt") {
            LogUtils::info("Starting checkpoint configuration construction...");
            CheckpointActionConfig checkpoint_config(this->config_);
            auto action = ActionFactory::instance().create_action(global_, "actions/checkpoint", checkpoint_config);
            action_info->action = std::move(action);
            (action_info->action.value())->execute();
            this->is_checkpoint_recover_ = true;
            LogUtils::info("Checkpointing is enabled. CheckpointAction initialized");
        }

        // Create all sink plugin instances
        for (size_t i = 0; i < consumer_thread_count; i++) {
            LogUtils::debug("Creating sink plugin instance for consumer thread #{}", i);
            sink_plugins.push_back(SinkPluginFactory::create_sink_plugin(config_, col_instances_, tag_instances_, i, action_info));
        }

        auto consumer_running = std::make_unique<std::atomic<bool>[]>(consumer_thread_count);
        for (size_t i = 0; i < consumer_thread_count; i++) {
            consumer_running[i].store(true);
        }

        // Start producer threads
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
            auto data_manager = std::make_shared<TableDataManager>(*pool, config_, col_instances_, tag_instances_);
            data_managers.push_back(data_manager);

            producer_threads.emplace_back([this, i, &split_names, &pipeline, data_manager, &active_producers, &producer_finished, &sink_plugins] {
                try {
                    if (config_.thread_affinity) {
                        set_thread_affinity(i, false, "Producer");
                    }
                    if (config_.thread_realtime) {
                        set_realtime_priority();
                    }
                    producer_thread_function(i, split_names[i], pipeline, data_manager, sink_plugins[0].get());
                    producer_finished[i].store(true);
                } catch (const std::exception& e) {
                    throw std::runtime_error("Producer thread " + std::to_string(i) + " failed: " + e.what());
                }
                active_producers--;
            });
        }

        (void)ProcessUtils::get_cpu_usage_percent();
        int64_t wait_seconds = std::min(static_cast<int64_t>(5), static_cast<int64_t>(producer_thread_count));
        std::this_thread::sleep_for(std::chrono::seconds(wait_seconds));

        while (true) {
            size_t total_queued = pipeline.total_queued();
            double queue_ratio = std::min(static_cast<double>(total_queued) / num_blocks, 1.0);

            LogUtils::info("Queue fill ratio: {:.2f}%, target: {:.2f}%", queue_ratio * 100, queue_warmup_ratio * 100);

            if (queue_ratio >= queue_warmup_ratio) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }

        // Start consumer threads
        if (config_.target_type == "tdengine") {
            const auto* tc = get_plugin_config<TDengineConfig>(config_.extensions, config_.target_type);
            if (tc == nullptr) {
                throw std::invalid_argument("Unsupported target type: " + config_.target_type);
            }

            conn_source.emplace(*tc);
        }

        for (size_t i = 0; i < consumer_thread_count; i++) {
            consumer_threads.emplace_back([this, i, &pipeline, &consumer_running, &sink_plugins, &conn_source, &gc, &startup_latch] {
                if (config_.thread_affinity) {
                    set_thread_affinity(i, true, "Consumer");
                }
                if (config_.thread_realtime) {
                    set_realtime_priority();
                }
                consumer_thread_function(i, pipeline, consumer_running[i], sink_plugins[i].get(), conn_source, gc, startup_latch);
            });
        }

        // Wait for consumers to be ready
        startup_latch.wait([this] { return stop_execution_.load() || SignalManager::interrupted(); });
        const auto start_time = std::chrono::steady_clock::now();

        // Monitor
        size_t last_total_rows = 0;
        auto last_time = start_time;
        size_t max_total_rows = all_names.size() * rows_per_table;
        int total_col_width = std::to_string(max_total_rows).length();

        while (active_producers > 0 && !stop_execution_.load()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));

            // Propagate global interrupt to local flag
            if (SignalManager::interrupted()) {
                stop_execution_.store(true);
                break;
            }

            // Calculate total rows generated by all producers
            size_t total_rows = 0;
            for (const auto& manager : data_managers) {
                total_rows += manager->get_total_rows_generated();
            }

            // Get current time
            const auto now = std::chrono::steady_clock::now();

            // Calculate actual interval (seconds)
            const auto interval = std::chrono::duration<double>(now - last_time).count();

            // Calculate real-time rate (rows/sec)
            const double rows_per_sec = interval > 0 ?
                static_cast<double>(total_rows - last_total_rows) / interval : 0.0;

            // Update last values
            last_total_rows = total_rows;
            last_time = now;

            // Calculate total runtime
            auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - start_time).count();

            LogUtils::info(
                "Runtime: {:4}s | Rate: {:8} rows/s | Total: {:{}} rows | Queue: {:3} items | CPU Usage: {:7.2f}% | Memory Usage: {} | Thread Count: {:3}",
                duration,
                static_cast<long long>(rows_per_sec),
                total_rows,
                total_col_width,
                pipeline.total_queued(),
                ProcessUtils::get_cpu_usage_percent(),
                ProcessUtils::get_memory_usage_human_readable(),
                ProcessUtils::get_thread_count()
            );
        }

        if (!stop_execution_.load()) {
            LogUtils::info("All producer threads have finished");
        }

        // Wait for all data to be sent
        size_t last_queue_size = pipeline.total_queued();
        auto last_check_time = std::chrono::steady_clock::now();

        while (pipeline.total_queued() > 0 && !stop_execution_.load()) {
            if (SignalManager::interrupted()) {
                stop_execution_.store(true);
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));

            auto current_time = std::chrono::steady_clock::now();
            auto interval = std::chrono::duration<double>(current_time - last_check_time).count();
            auto duration = std::chrono::duration_cast<std::chrono::seconds>(current_time - start_time).count();

            if (interval >= 1.0) {
                size_t current_queue_size = pipeline.total_queued();
                double process_rate = (last_queue_size - current_queue_size) / interval;

                LogUtils::info(
                    "Runtime: {:4}s | Remaining Queue: {:3} items | Processing Rate: {:6.2f} items/s | CPU Usage: {:7.2f}% | Memory Usage: {} | Thread Count: {:3}",
                    duration,
                    current_queue_size,
                    process_rate,
                    ProcessUtils::get_cpu_usage_percent(),
                    ProcessUtils::get_memory_usage_human_readable(),
                    ProcessUtils::get_thread_count()
                );

                last_queue_size = current_queue_size;
                last_check_time = current_time;
            }
        }

        const auto end_time = std::chrono::steady_clock::now();
        (void)end_time;

        // Terminate pipeline
        pipeline.terminate();

        // Wait for producers to finish
        for (auto& t : producer_threads) {
            if (t.joinable()) t.join();
        }

        // Wait for consumers to finish
        // for (size_t i = 0; i < consumer_thread_count; i++) {
        //     consumer_running[i] = false;
        // }

        for (auto& t : consumer_threads) {
            if (t.joinable()) t.join();
        }

        if (stop_execution_.load()) {
            throw std::runtime_error("Exception detected during execution, aborting...");
        }

        // Calculate final total rows
        size_t final_total_rows = 0;
        for (const auto& manager : data_managers) {
            final_total_rows += manager->get_total_rows_generated();
        }

        if (global_.verbose) {
            print_sink_plugin_times(sink_plugins);
        }

        // Obtain the minimum value of the start_write_time() for all sink plugins
        auto min_start_write_time = std::chrono::steady_clock::time_point::max();
        for (const auto& plugin : sink_plugins) {
            auto t = plugin->start_write_time();
            if (t < min_start_write_time) {
                min_start_write_time = t;
            }
        }

        // Obtain the maximum value of the end_write_time() for all sink plugins
        std::vector<std::chrono::steady_clock::time_point> end_write_times;
        end_write_times.reserve(sink_plugins.size());
        auto max_end_write_time = std::chrono::steady_clock::time_point::min();
        for (const auto& plugin : sink_plugins) {
            auto t = plugin->end_write_time();
            end_write_times.push_back(t);
            if (t > max_end_write_time) {
                max_end_write_time = t;
            }
        }

        // Calculate total wait time (sum of each thread's waiting time after its own finish)
        double total_wait_time = 0.0;
        for (const auto& t : end_write_times) {
            total_wait_time += std::chrono::duration<double>(max_end_write_time - t).count();
        }
        double avg_wait_time = total_wait_time / consumer_thread_count; // average per thread

        // Calculate total duration (seconds)
        const auto total_duration = std::chrono::duration<double>(max_end_write_time - min_start_write_time).count() - avg_wait_time;

        // Calculate average insert rate
        const double avg_rows_per_sec = total_duration > 0 ?
            static_cast<double>(final_total_rows) / total_duration : 0.0;

        // Print performance statistics
        LogUtils::info("");
        LogUtils::info("=================================================== Insert Summary Statistics ========================================================");
        LogUtils::info("Insert Threads: {}", consumer_thread_count);
        LogUtils::info("Total Rows: {}", final_total_rows);
        LogUtils::info("Total Duration: {:.2f} seconds", total_duration);
        LogUtils::info("Average Rate: {:.2f} rows/second", avg_rows_per_sec);
        LogUtils::info("=======================================================================================================================================");

        // Collect performance metrics
        ActionMetrics global_play_metrics;
        ActionMetrics global_write_metrics;

        for (const auto& plugin : sink_plugins) {
            global_play_metrics.merge_from(plugin->get_play_metrics());
            global_write_metrics.merge_from(plugin->get_write_metrics());
        }

        global_play_metrics.calculate();
        global_write_metrics.calculate();

        // Print performance statistics
        double thread_latency = global_write_metrics.get_sum() / consumer_thread_count / 1000;
        double effective_ratio = thread_latency / total_duration * 100.0;
        double framework_ratio = (1 - thread_latency / total_duration) * 100.0;
        TimeIntervalStrategy time_strategy(config_.time_interval, config_.timestamp_precision);

        LogUtils::info("");
        LogUtils::info("=================================================== Insert Latency & Efficiency Metrics ==============================================");
        LogUtils::info("Total Operations: {}", global_write_metrics.get_samples().size());
        LogUtils::info("Total Duration: {:.2f} seconds", total_duration);
        LogUtils::info("Pure Insert Latency: {:.2f} seconds", thread_latency);
        LogUtils::info("Effective Time Ratio: {:.2f}%", effective_ratio);
        LogUtils::info("Framework Overhead: {:.2f}%", framework_ratio);
        LogUtils::info("Idle Time After Finish: {:.2f} seconds", avg_wait_time);

        if (time_strategy.is_literal_strategy()) {
            LogUtils::info("Play Latency Distribution: {}", global_play_metrics.get_summary());
        }

        LogUtils::info("Write Latency Distribution: {}", global_write_metrics.get_summary());
        LogUtils::info("======================================================================================================================================");
        LogUtils::info("");

        // Clean up resources
        for (auto& plugin : sink_plugins) {
            plugin->close();
        }

        if (action_info && action_info->action) {
            CheckpointAction::stop_all();
        }
        LogUtils::info("InsertDataAction completed successfully");

    } catch (const std::exception& e) {
        // LogUtils::error("InsertDataAction failed: {}", e.what());
        throw;
    }
}

void InsertDataAction::init_cache_units_data(
    MemoryPool& pool,
    size_t num_cached_batches,
    size_t max_tables_per_block,
    size_t max_rows_per_table
) {
    for (size_t table_idx = 0; table_idx < max_tables_per_block; ++table_idx) {
        std::string table_name = DEFAULT_TABLE_NAME;
        RowDataGenerator generator(table_name, config_, col_instances_);

        for (size_t cache_idx = 0; cache_idx < num_cached_batches; ++cache_idx) {
            std::vector<RowData> data_rows;
            data_rows.reserve(max_rows_per_table);

            for (size_t row = 0; row < max_rows_per_table; ++row) {
                auto row_opt = generator.next_row();
                if (row_opt.has_value()) {
                    data_rows.push_back(std::move(row_opt.value()));
                } else {
                    if (cache_idx + 1 != num_cached_batches) {
                        throw std::runtime_error("RowDataGenerator could not generate enough data for cache initialization");
                    }
                }
            }

            if (!data_rows.size()) {
                throw std::runtime_error("RowDataGenerator generated zero rows for cache " + std::to_string(cache_idx) + " initialization");
            }

            pool.fill_cache_unit_data(cache_idx, table_idx, data_rows);
        }

        if (global_.verbose) {
            LogUtils::debug(
                "Initialized cache data for table index: {} with {} rows per batch, {} cached batches",
                table_idx,
                max_rows_per_table,
                num_cached_batches
            );
        }

        if (config_.schema.generation.tables_reuse_data) {
            break;
        }
    }
}

void InsertDataAction::producer_thread_function(
    size_t producer_id,
    const std::vector<std::string>& assigned_tables,
    DataPipeline<FormatResult>& pipeline,
    std::shared_ptr<TableDataManager> data_manager,
    const ISinkPlugin* plugin)
{
    if (assigned_tables.empty()) {
        LogUtils::info("Producer thread {} has no assigned tables", producer_id);
        return;
    }

    // Initialize data manager
    if (!data_manager->init(assigned_tables)) {
        LogUtils::error("TableDataManager initialization failed for producer {}", producer_id);
        stop_execution_.store(true);
        return;
    }

    // Data generation loop
    while (!stop_execution_.load() && !SignalManager::interrupted()) {
        auto batch = data_manager->next_multi_batch();
        if (!batch.has_value()) {
            break;
        }

        // size_t batch_size = batch->table_batches.size();
        // size_t total_rows = batch->total_rows;

        // Format data
        FormatResult formatted_result = plugin->format(batch.value(), is_checkpoint_recover_);

        // Push data to pipeline
        try {
            pipeline.push_data(producer_id, std::move(formatted_result));
        } catch (const std::exception& e) {
            LogUtils::error("Producer {} could not push data, reason: {}", producer_id, e.what());
            break;
        }

        // std::cout << "Producer " << producer_id << ": Pushed batch for table(s): "
        //           << batch_size << ", total rows: " << total_rows
        //           << ", queue size: " << pipeline.total_queued() << std::endl;
    }
}

void InsertDataAction::consumer_thread_function(
    size_t consumer_id,
    DataPipeline<FormatResult>& pipeline,
    std::atomic<bool>& running,
    ISinkPlugin* plugin,
    std::optional<ConnectorSource>& conn_source,
    GarbageCollector<FormatResult>& gc,
    Latch& startup_latch)
{
    // Failure retry logic
    const auto& failure_cfg = config_.failure_handling;
    size_t failed_count = 0;

    auto handle_startup_error = [&](const auto&... args) {
        LogUtils::error(args...);
        stop_execution_.store(true);
        startup_latch.interrupt();
    };

    // Connect to sink target
    if (!plugin->connect_with_source(conn_source)) {
        handle_startup_error("Failed to connect sink plugin for consumer {}", consumer_id);
        return;
    }

    if (!plugin->prepare()) {
        handle_startup_error("Failed to prepare sink plugin for thread {}", consumer_id);
    }

    startup_latch.count_down();

    // Data processing loop
    (void)running;
    while (!stop_execution_.load() && !SignalManager::interrupted()) {
        auto result = pipeline.fetch_data(consumer_id);

        switch (result.status) {
        case DataPipeline<FormatResult>::Status::Success:
            try {
                // Use sink plugin to execute write
                std::visit([&](const auto& formatted_result) {
                    using T = std::decay_t<decltype(formatted_result)>;
                    if constexpr (std::is_same_v<T, InsertFormatResult>) {
                        auto success = plugin->write(*formatted_result);
                        if (!success) {
                            failed_count++;
                        }

                        // retry_count = 0;

                        // if constexpr (std::is_same_v<T, SqlInsertData> || std::is_same_v<T, StmtV2InsertData>) {
                        //     std::cout << "Consumer " << consumer_id
                        //              << ": Executed SQL with " << formatted_result.total_rows
                        //              << " rows" << std::endl;
                        // }

                    } else {
                        throw std::runtime_error("Unknown format result type: " + std::string(typeid(formatted_result).name()));
                    }

                }, *result.data);

                gc.dispose(std::move(*result.data));

            } catch (const std::exception& e) {
                LogUtils::error("Consumer " + std::to_string(consumer_id) + " write failed: " + e.what());

                // Handle failure
                failed_count++;
                if (failure_cfg.on_failure == "exit") {
                    stop_execution_.store(true);
                    return;
                }
            }
            break;

        case DataPipeline<FormatResult>::Status::Terminated:
            // Pipeline terminated, exit thread
            LogUtils::debug("Consumer {} received termination signal", consumer_id);
            if (failed_count > 0) {
                LogUtils::warn("Consumer {} had {} failed write attempts", consumer_id, failed_count);
            }
            return;

        case DataPipeline<FormatResult>::Status::Timeout:
            // Short sleep
            // std::this_thread::sleep_for(std::chrono::milliseconds(10));
            break;
        }
    }
}

ColumnConfigInstanceVector InsertDataAction::create_column_instances() const {
    try {
        const auto& schema = config_.schema.columns_cfg.get_schema();
        if (schema.empty()) {
            throw std::invalid_argument("Schema configuration is empty");
        }
        return ColumnConfigInstanceFactory::create(schema);
    } catch (const std::exception& e) {
        throw std::runtime_error(std::string("Failed to create column instances: ") + e.what());
    }
}

ColumnConfigInstanceVector InsertDataAction::create_tag_instances() const {
    try {
        const auto& schema = config_.schema.tags_cfg.get_schema();
        return ColumnConfigInstanceFactory::create(schema);
    } catch (const std::exception& e) {
        throw std::runtime_error(std::string("Failed to create tag instances: ") + e.what());
    }
}

void InsertDataAction::print_sink_plugin_times(const std::vector<std::unique_ptr<ISinkPlugin>>& plugins) {
    auto format_time = [](const std::chrono::system_clock::time_point& tp) -> std::string {
        static const cctz::time_zone local_tz = cctz::local_time_zone();
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()) % 1000;
        std::string base = cctz::format("%Y-%m-%d %H:%M:%S", tp, local_tz);
        std::ostringstream oss;
        oss << base << "." << std::setfill('0') << std::setw(3) << ms.count();
        return oss.str();
    };

    for (size_t i = 0; i < plugins.size(); ++i) {
        auto start = plugins[i]->start_write_time();
        auto end = plugins[i]->end_write_time();

        auto start_sys = std::chrono::system_clock::now() + std::chrono::duration_cast<std::chrono::system_clock::duration>(start - std::chrono::steady_clock::now());
        auto end_sys = std::chrono::system_clock::now() + std::chrono::duration_cast<std::chrono::system_clock::duration>(end - std::chrono::steady_clock::now());

        LogUtils::debug(
            "SinkPlugin {} start_write_time: {}, end_write_time: {}",
            i,
            format_time(start_sys),
            format_time(end_sys)
        );
    }
}