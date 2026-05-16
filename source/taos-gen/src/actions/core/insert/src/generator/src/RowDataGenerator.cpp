#include "RowDataGenerator.hpp"
#include "StringUtils.hpp"
#include "CSVDataManager.hpp"
#include "FilePathResolver.hpp"
#include "StreamingCSVRowSource.hpp"
#include "PreloadCSVRowSource.hpp"
#include <stdexcept>
#include <cstdlib>
#include <ctime>
#include <cmath>


RowDataGenerator::RowDataGenerator(const std::string& table_name,
                                  const InsertDataConfig& config,
                                  const ColumnConfigInstanceVector& instances,
                                  bool use_cache,
                                  std::shared_ptr<ICSVRowSource> external_csv_source)
    : table_name_(table_name),
      config_(config),
      columns_config_(config.schema.columns_cfg),
      instances_(instances),
      target_precision_(config.timestamp_precision),
      use_cache_(use_cache),
      csv_source_(std::move(external_csv_source)) {

    init_cached_row();
    init_raw_source();

    // if (config_.schema.generation.data_cache.enabled) {
    //     init_cache();
    // }

    if (config_.schema.generation.data_disorder.enabled) {
        init_disorder();
    }
}

// Initialize cached row memory
void RowDataGenerator::init_cached_row() {
    if (use_cache_) return;

    cached_row_.columns.resize(instances_.size());

    for (size_t i = 0; i < instances_.size(); ++i) {
        const auto& config = instances_[i].config();

        // Pre-allocate memory for variable-length types
        switch (config.type_tag) {
            case ColumnTypeTag::VARCHAR:
            case ColumnTypeTag::BINARY:
            case ColumnTypeTag::JSON:
            {
                std::string s;
                s.reserve(config.len.value());
                cached_row_.columns[i] = std::move(s);
                break;
            }
            case ColumnTypeTag::NCHAR:
            {
                std::u16string s;
                s.reserve(config.len.value());
                cached_row_.columns[i] = std::move(s);
                break;
            }
            case ColumnTypeTag::VARBINARY:
            {
                std::vector<uint8_t> v;
                v.reserve(config.len.value());
                cached_row_.columns[i] = std::move(v);
                break;
            }
            default:
                // No special handling required for fixed-length types
                break;
        }
    }
}

void RowDataGenerator::init_cache() {
    // Pre-generate data to fill cache
    cache_.clear();
    cache_.reserve(config_.schema.generation.data_cache.num_cached_batches);
    while (cache_.size() < config_.schema.generation.data_cache.num_cached_batches && has_more()) {
        if (auto row = fetch_raw_row()) {
            cache_.push_back(*row);
        } else {
            break;
        }
    }
}

void RowDataGenerator::init_disorder() {
    // Initialize disorder intervals
    disorder_intervals_.clear();
    for (const auto& interval : config_.schema.generation.data_disorder.intervals) {
        DisorderInterval disorder_interval;
        disorder_interval.start_time = TimestampUtils::parse_timestamp(interval.time_start, target_precision_);
        disorder_interval.end_time = TimestampUtils::parse_timestamp(interval.time_end, target_precision_);
        disorder_interval.ratio = interval.ratio;
        disorder_interval.latency_range = interval.latency_range;
        disorder_intervals_.push_back(disorder_interval);
    }
}

void RowDataGenerator::init_raw_source() {
    source_exhausted_ = false;

    if (columns_config_.source_type == "generator") {
        init_generator();
        total_rows_ = config_.schema.generation.rows_per_table;
        timestamp_generator_ = std::make_unique<TimestampGenerator>(
            columns_config_.generator.timestamp_strategy.timestamp_config);
    } else if (columns_config_.source_type == "csv") {
        init_csv_reader();
        // total_rows_ is set inside init_csv_reader()
    } else {
        throw std::invalid_argument("Unsupported source_type: " + columns_config_.source_type);
    }
}

void RowDataGenerator::init_generator() {
    use_generator_ = true;

    // Create row generator
    if (!use_cache_) {
        row_generator_ = std::make_unique<RowGenerator>(table_name_, instances_);
    }
}

void RowDataGenerator::init_csv_reader() {
    use_generator_ = false;

    const auto& csv_config = columns_config_.csv;
    const auto& loading_mode = config_.schema.columns_cfg.csv.loading_mode;
    bool has_generator_ts = csv_config.timestamp_strategy.strategy_type == "generator";

    if (loading_mode == "streaming") {
        // Streaming mode
        if (!csv_source_) {
            // No external source provided, create our own
            if (csv_config.file_path.empty()) {
                throw std::invalid_argument("CSV file path is not specified.");
            }
            auto resolved_paths = FilePathResolver::resolve(csv_config.file_path);
            if (resolved_paths.size() == 1) {
                LogUtils::debug("Opening CSV file: {}", resolved_paths[0]);
            } else {
                LogUtils::debug("Resolved {} CSV files from path: {}", resolved_paths.size(), csv_config.file_path);
            }
            std::string csv_precision = csv_config.timestamp_strategy.get_precision();
            csv_source_ = std::make_shared<StreamingCSVRowSource>(
                resolved_paths,
                csv_config.has_header,
                csv_config.delimiter.empty() ? ',' : csv_config.delimiter[0],
                instances_,
                csv_config.timestamp_strategy,
                csv_precision,
                target_precision_,
                csv_config.repeat_read
            );
        }
        total_rows_ = config_.schema.generation.rows_per_table;
    } else {
        // Preload mode

        // Degenerate case: use_cache + generator ts => only need timestamp generation
        if (use_cache_ && has_generator_ts) {
            csv_source_ = std::make_shared<PreloadCSVRowSource>(
                csv_config.timestamp_strategy.generator,
                target_precision_
            );
            total_rows_ = config_.schema.generation.rows_per_table;
            return;
        }

        if (csv_config.file_path.empty()) {
            throw std::invalid_argument("CSV file path is not specified.");
        }

        std::string csv_precision = csv_config.timestamp_strategy.get_precision();

        // Get table data
        auto [using_default_table, table_data_ptr] = CSVDataManager::get_table_data(
            csv_config, instances_, table_name_
        );

        if (!table_data_ptr) {
            throw std::runtime_error("Table '" + table_name_ + "' not found in CSV file '" + csv_config.file_path + "'");
        }

        const auto& table_data = *table_data_ptr;
        if (table_data.rows.empty()) {
            throw std::runtime_error("No data found for table '" + table_name_ + "' in CSV file '" + csv_config.file_path + "'");
        }

        if (using_default_table) {
            // Use shared cache for default_table
            auto shared_rows = CSVDataManager::get_shared_rows(
                csv_config.file_path,
                table_data,
                csv_precision,
                target_precision_
            );
            if (has_generator_ts) {
                csv_source_ = std::make_shared<PreloadCSVRowSource>(
                    csv_config.timestamp_strategy.generator,
                    target_precision_,
                    std::move(shared_rows),
                    csv_config.repeat_read
                );
            } else {
                csv_source_ = std::make_shared<PreloadCSVRowSource>(
                    std::move(shared_rows),
                    csv_config.repeat_read
                );
            }
        } else {
            // Convert to RowData for specific table
            auto rows = CSVDataManager::convert_table_data_to_row_data(table_data, csv_precision, target_precision_);
            if (has_generator_ts) {
                csv_source_ = std::make_shared<PreloadCSVRowSource>(
                    csv_config.timestamp_strategy.generator,
                    target_precision_,
                    std::move(rows),
                    csv_config.repeat_read
                );
            } else {
                csv_source_ = std::make_shared<PreloadCSVRowSource>(
                    std::move(rows),
                    csv_config.repeat_read
                );
            }
        }

        // Set total_rows_
        if (csv_config.repeat_read) {
            total_rows_ = config_.schema.generation.rows_per_table;
        } else {
            total_rows_ = std::min(static_cast<int64_t>(csv_source_->total_rows()), config_.schema.generation.rows_per_table);
        }
    }
}

std::optional<RowData> RowDataGenerator::next_row() {
    if (generated_rows_ >= total_rows_) {
        return std::nullopt;
    }

    // Process delay queue
    process_delay_queue();

    // Prefer to get data from cache
    if (!cache_.empty()) {
        auto row = cache_.back();
        cache_.pop_back();
        generated_rows_++;
        return row;
    }

    // Get data from raw source
    auto row_opt = fetch_raw_row();
    if (!row_opt) {
        return std::nullopt;
    }

    // Update current timeline
    current_timestamp_ = row_opt->get().timestamp;

    // Apply disorder strategy
    auto delayed = apply_disorder(*row_opt);
    if (!delayed) {
        generated_rows_++;
    }

    return row_opt;
}

int RowDataGenerator::next_row(MemoryPool::TableBlock& table_block) {
    if (generated_rows_ >= total_rows_) {
        return 0;
    }

    // Process delay queue
    process_delay_queue();

    // Prefer to get data from cache
    if (!cache_.empty()) {
        auto row = cache_.back();
        cache_.pop_back();

        // Add row to memory pool
        table_block.add_row(row);
        generated_rows_++;
        return 1;
    }

    // Get data from raw source
    auto row_opt = fetch_raw_row();
    if (!row_opt) {
        return 0;
    }

    // Update timeline
    current_timestamp_ = row_opt->get().timestamp;

    // Apply disorder strategy
    bool delayed = apply_disorder(*row_opt);
    if (delayed) {
        return -1;
    }

    // Write directly to memory pool
    generated_rows_++;
    table_block.add_row(*row_opt);

    return 1;
}

bool RowDataGenerator::has_more() const {
    if (generated_rows_ >= total_rows_) {
        return false;
    }

    if (use_generator_) {
        return true;
    }

    if (!cache_.empty()) {
        return true;
    }

    return !source_exhausted_;
}

void RowDataGenerator::reset() {
    generated_rows_ = 0;
    current_timestamp_ = 0;
    source_exhausted_ = false;
    cache_.clear();
    decltype(delay_queue_)().swap(delay_queue_);
    if (timestamp_generator_) {
        timestamp_generator_->reset();
    }
    if (csv_source_) {
        csv_source_->reset();
    }
}

std::optional<std::reference_wrapper<RowData>> RowDataGenerator::fetch_raw_row() {
    if (use_generator_) {
        generate_from_generator();
        return std::ref(cached_row_);
    }

    // CSV path - preload and streaming unified
    auto row = csv_source_->next();
    if (!row) {
        source_exhausted_ = true;
        return std::nullopt;
    }

    if (use_cache_) {
        cached_row_.timestamp = row->timestamp;  // Only need timestamp
    } else {
        cached_row_ = std::move(*row);
    }
    return std::ref(cached_row_);
}

bool RowDataGenerator::apply_disorder(RowData& row) {
    if (disorder_intervals_.empty()) {
        return false;
    }

    // Check if data timestamp is in disorder interval
    for (const auto& interval : disorder_intervals_) {
        if (row.timestamp >= interval.start_time && row.timestamp < interval.end_time) {
            // Decide whether to delay by probability
            if (static_cast<double>(rand()) / RAND_MAX < interval.ratio) {
                int latency = rand() % interval.latency_range;
                int64_t deliver_time = row.timestamp + latency;

                // Put into delay queue
                delay_queue_.push(DelayedRow{deliver_time, row});
                row.timestamp = -1;
                return true;
            }
        }
    }
    return false;
}

void RowDataGenerator::process_delay_queue() {
    while (!delay_queue_.empty()) {
        const auto& top = delay_queue_.top();
        if (top.deliver_timestamp <= current_timestamp_) {
            cache_.push_back(top.row);
            delay_queue_.pop();
        } else {
            break;
        }
    }
}

void RowDataGenerator::generate_from_generator() {
    // Generate timestamp
    cached_row_.timestamp = TimestampUtils::convert_timestamp_precision(timestamp_generator_->generate(),
        timestamp_generator_->timestamp_precision(), target_precision_);

    if (!use_cache_) {
        // Generate column data
        row_generator_->generate(cached_row_.columns);
    }

    return;
}
