#include "TableDataManager.hpp"
#include "FilePathResolver.hpp"
#include "StreamingCSVRowSource.hpp"
#include <algorithm>
#include <deque>
#include <limits>
#include <iostream>

namespace {

class ChunkReplayCoordinator {
public:
    ChunkReplayCoordinator(std::shared_ptr<ICSVRowSource> base_source, size_t refill_rows)
        : base_source_(std::move(base_source)), refill_rows_(std::max<size_t>(1, refill_rows)) {}

    size_t create_cursor() {
        cursors_.push_back(0);
        return cursors_.size() - 1;
    }

    std::optional<RowData> next(size_t cursor_id) {
        if (cursor_id >= cursors_.size()) {
            return std::nullopt;
        }

        size_t& cursor = cursors_[cursor_id];
        if (!ensure_available(cursor)) {
            return std::nullopt;
        }

        RowData row = buffer_[cursor - base_index_];
        ++cursor;
        compact_buffer();
        return row;
    }

    bool has_more(size_t cursor_id) const {
        if (cursor_id >= cursors_.size()) {
            return false;
        }

        size_t cursor = cursors_[cursor_id];
        if (cursor < base_index_ + buffer_.size()) {
            return true;
        }

        return !source_exhausted_;
    }

    void reset_all() {
        base_source_->reset();
        source_exhausted_ = false;
        base_index_ = 0;
        buffer_.clear();
        for (auto& cursor : cursors_) {
            cursor = 0;
        }
    }

private:
    bool ensure_available(size_t cursor) {
        while (cursor >= base_index_ + buffer_.size() && !source_exhausted_) {
            size_t loaded = 0;
            while (loaded < refill_rows_) {
                auto row = base_source_->next();
                if (!row) {
                    source_exhausted_ = true;
                    break;
                }
                buffer_.push_back(std::move(*row));
                ++loaded;
            }

            if (loaded == 0) {
                break;
            }
        }

        return cursor < base_index_ + buffer_.size();
    }

    void compact_buffer() {
        if (buffer_.empty() || cursors_.empty()) {
            return;
        }

        const auto min_it = std::min_element(cursors_.begin(), cursors_.end());
        if (min_it == cursors_.end() || *min_it <= base_index_) {
            return;
        }

        size_t drop = *min_it - base_index_;
        if (drop > buffer_.size()) {
            drop = buffer_.size();
        }

        while (drop-- > 0) {
            buffer_.pop_front();
            ++base_index_;
        }
    }

private:
    std::shared_ptr<ICSVRowSource> base_source_;
    size_t refill_rows_ = 1;
    size_t base_index_ = 0;
    std::deque<RowData> buffer_;
    std::vector<size_t> cursors_;
    bool source_exhausted_ = false;
};

class ChunkReplayCursorSource : public ICSVRowSource {
public:
    ChunkReplayCursorSource(std::shared_ptr<ChunkReplayCoordinator> coordinator, size_t cursor_id)
        : coordinator_(std::move(coordinator)), cursor_id_(cursor_id) {}

    std::optional<RowData> next() override {
        return coordinator_->next(cursor_id_);
    }

    bool has_more() const override {
        return coordinator_->has_more(cursor_id_);
    }

    void reset() override {
        coordinator_->reset_all();
    }

    size_t total_rows() const override {
        return 0;
    }

private:
    std::shared_ptr<ChunkReplayCoordinator> coordinator_;
    size_t cursor_id_;
};

}


TableDataManager::TableDataManager(MemoryPool& pool,
                                   const InsertDataConfig& config,
                                   const ColumnConfigInstanceVector& col_instances,
                                   const ColumnConfigInstanceVector& tag_instances)
    : pool_(pool), config_(config), col_instances_(col_instances), tag_instances_(tag_instances) {

    // Set interlace rows
    if (config_.schema.generation.interlace_mode.enabled) {
        interlace_rows_ = config_.schema.generation.interlace_mode.rows;
    } else {
        interlace_rows_ = std::numeric_limits<int64_t>::max();
    }

    // Initialize flow control
    if (config_.schema.generation.flow_control.enabled) {
        rate_limiter_ = std::make_unique<RateLimiter>(
            config_.schema.generation.flow_control.rate_limit
        );
    }
}

bool TableDataManager::init(const std::vector<std::string>& table_names) {
    if (table_names.empty()) {
        LogUtils::error("TableDataManager initialized with empty table list");
        return false;
    }

    active_table_count_ = table_names.size();
    table_states_.clear();
    table_states_.reserve(table_names.size());

    try {
        // Create shared streaming source if in streaming mode
        std::shared_ptr<ICSVRowSource> shared_streaming_source;
        std::shared_ptr<ChunkReplayCoordinator> chunk_replay_coordinator;
        if (config_.schema.columns_cfg.source_type == "csv"
            && config_.schema.columns_cfg.csv.loading_mode == "streaming") {
            const auto& csv_config = config_.schema.columns_cfg.csv;
            auto resolved_paths = FilePathResolver::resolve(csv_config.file_path);
            std::string csv_precision = csv_config.timestamp_strategy.get_precision();
            auto base_streaming_source = std::make_shared<StreamingCSVRowSource>(
                resolved_paths,
                csv_config.has_header,
                csv_config.delimiter.empty() ? ',' : csv_config.delimiter[0],
                col_instances_,
                csv_config.timestamp_strategy,
                csv_precision,
                config_.timestamp_precision,
                csv_config.repeat_read
            );

            if (csv_config.tbname_index < 0) {
                size_t refill_rows = config_.schema.generation.rows_per_batch;
                if (config_.schema.generation.interlace_mode.enabled) {
                    refill_rows = static_cast<size_t>(config_.schema.generation.interlace_mode.rows);
                }
                if (refill_rows == 0) {
                    refill_rows = 1;
                }
                chunk_replay_coordinator = std::make_shared<ChunkReplayCoordinator>(
                    base_streaming_source,
                    refill_rows
                );
            } else {
                shared_streaming_source = base_streaming_source;
            }
        }

        // Create RowDataGenerator for each table
        for (const auto& table_name : table_names) {
            TableState state;
            try {
                std::shared_ptr<ICSVRowSource> source_for_table = shared_streaming_source;
                if (chunk_replay_coordinator) {
                    source_for_table = std::make_shared<ChunkReplayCursorSource>(
                        chunk_replay_coordinator,
                        chunk_replay_coordinator->create_cursor()
                    );
                }

                state.table_name = table_name;
                state.generator = std::make_unique<RowDataGenerator>(
                    table_name,
                    config_,
                    col_instances_,
                    pool_.is_cache_mode(),
                    source_for_table
                );
                state.rows_generated = 0;
                state.interlace_counter = 0;
                state.completed = false;

                if (tag_instances_.size() > 0
                    && config_.data_format.support_tags
                    && config_.schema.tags_cfg.source_type == "generator"
                ) {
                    std::vector<ColumnType> tag_values = generate_tags_for_table(table_name);
                    state.tags_ptr = pool_.register_table_tags(table_name, tag_values);
                }

                table_states_.push_back(std::move(state));
            } catch (const std::exception& e) {
                // std::cerr << "Failed to create RowDataGenerator for table: " << table_name
                //          << " - " << e.what() << std::endl;
                // return false;
                // state.completed = true;
                LogUtils::error("Failed to create RowDataGenerator for table {}: {}", table_name, e.what());
                throw;
            }
        }

        current_table_index_ = 0;
        return true;
    } catch (const std::exception& e) {
        LogUtils::error("Failed to initialize TableDataManager: {}", e.what());
        return false;
    }
}

std::vector<ColumnType> TableDataManager::generate_tags_for_table(const std::string& table_name) {
    if (tag_instances_.empty()) {
        return {};
    }

    RowGenerator tag_generator(table_name, tag_instances_);
    std::vector<ColumnType> tag_values = tag_generator.generate();
    return tag_values;
}

void TableDataManager::acquire_tokens(int64_t tokens) {
    rate_limiter_->acquire(tokens);
}

size_t TableDataManager::get_total_rows_generated() const {
    return total_rows_generated_.load(std::memory_order_relaxed);
}

const ColumnConfigInstanceVector& TableDataManager::get_column_instances() const {
    return col_instances_;
}

std::optional<MemoryPool::MemoryBlock*> TableDataManager::next_multi_batch() {
    // if (!has_more()) {
    //     return std::nullopt;
    // }

    // Get maximum rows per request from config
    size_t max_rows = config_.schema.generation.rows_per_batch;
    if (max_rows == 0) {
        max_rows = std::numeric_limits<size_t>::max();
    }

    MemoryPool::MemoryBlock* batch = collect_batch_data(max_rows);
    if (batch == nullptr) {
        return std::nullopt;
    }

    total_rows_generated_.fetch_add(batch->total_rows, std::memory_order_relaxed);
    return batch;
}

MemoryPool::MemoryBlock* TableDataManager::collect_batch_data(size_t max_rows) {
    // Get memory block from memory pool
    MemoryPool::MemoryBlock* block = pool_.acquire_block(sequence_num_);
    // if (!block) {
    //     return nullptr;  // No available memory block
    // }

    // Initialize memory block state
    // block->reset();
    int64_t start_time = std::numeric_limits<int64_t>::max();
    int64_t end_time = std::numeric_limits<int64_t>::min();
    size_t total_rows = 0;
    size_t table_loops = 0;
    const size_t max_loops = table_states_.size();
    prev_table_index_ = current_table_index_;

    while (total_rows < max_rows &&
           has_more() &&
           table_loops < max_loops &&
           current_table_index_ >= prev_table_index_ &&
           block->used_tables < block->tables.size())
    {
        TableState* table_state = get_next_active_table();
        if (!table_state) break;

        // Get current table slot
        auto& table_block = block->tables[block->used_tables];
        table_block.table_name = table_state->table_name.c_str();
        table_block.tags_ptr = table_state->tags_ptr;

        // Calculate number of rows that can be generated
        size_t remaining = max_rows - total_rows;
        size_t rows_to_generate = std::min(
            calculate_rows_to_generate(*table_state),
            std::min(remaining, table_block.max_rows)
        );

        // Generate data directly in the memory block
        for (size_t i = 0; i < rows_to_generate; ++i) {
            auto row_opt = table_state->generator->next_row(table_block);
            if (row_opt > 0) {
                // Update statistics
                const int64_t ts = table_block.timestamps[table_block.used_rows - 1];
                start_time = std::min(start_time, ts);
                end_time = std::max(end_time, ts);

                total_rows++;
                table_state->rows_generated++;
                table_state->interlace_counter++;
            } else if (row_opt == 0) {
                if (!table_state->completed) {
                    table_state->completed = true;
                    if (active_table_count_ > 0) --active_table_count_;
                }
                break;
            } else {
                --i;
                continue;
            }

            // Flow control processing
            if (rate_limiter_) {
                acquire_tokens(1);
            }
        }

        // If the table has generated data, increase the used table count
        if (table_block.used_rows > 0) {
            block->used_tables++;
        }

        // Update table state
        if (!table_state->completed &&
            table_state->rows_generated >= config_.schema.generation.rows_per_table) {
            table_state->completed = true;
            if (active_table_count_ > 0) --active_table_count_;
        }

        if (table_state->completed ||
            table_state->interlace_counter >= interlace_rows_) {
            advance_to_next_table();
        }

        table_loops++;
    }

    if (total_rows == 0) {
        // No data generated, release the block
        block->release();
        return nullptr;
    }

    // Update memory block metadata
    block->start_time = start_time;
    block->end_time = end_time;
    block->total_rows = total_rows;

    if (current_table_index_ <= prev_table_index_) {
        ++sequence_num_;
    }

    return block;
}

bool TableDataManager::has_more() const {
    return active_table_count_ > 0;
}

std::string TableDataManager::current_table() const {
    if (table_states_.empty()) return "";
    return table_states_[current_table_index_].table_name;
}

const std::vector<TableDataManager::TableState>& TableDataManager::table_states() const {
    return table_states_;
}

TableDataManager::TableState* TableDataManager::get_next_active_table() {
    if (table_states_.empty()) return nullptr;

    // Loop through tables to find one with available data
    size_t start_index = current_table_index_;

    do {
        TableState& state = table_states_[current_table_index_];

        // Check if table still has data and is not completed
        if (!state.completed &&
            state.rows_generated < config_.schema.generation.rows_per_table &&
            state.generator->has_more())
        {
            return &state;
        }

        // Move to next table
        current_table_index_ = (current_table_index_ + 1) % table_states_.size();
    } while (current_table_index_ != start_index);

    return nullptr;
}

size_t TableDataManager::calculate_rows_to_generate(TableState& state) const {
    // Calculate remaining row limit
    int64_t remaining_rows = std::min(
        config_.schema.generation.rows_per_table - state.rows_generated,
        state.generator->has_more() ? std::numeric_limits<int64_t>::max() : 0
    );

    // Calculate number of rows to generate this time
    int64_t rows_to_generate = std::min(
        interlace_rows_ - state.interlace_counter,
        remaining_rows
    );

    // Consider flow control
    // if (config_.schema.generation.flow_control.rate_limit > 0) {
    //     rows_to_generate = std::min(
    //         rows_to_generate,
    //         config_.schema.generation.flow_control.rate_limit / static_cast<int>(table_order_.size())
    //     );
    // }

    return std::max(static_cast<size_t>(1), static_cast<size_t>(rows_to_generate));
}

void TableDataManager::advance_to_next_table() {
    if (table_states_.empty()) return;

    // Reset interlace counter for current table
    table_states_[current_table_index_].interlace_counter = 0;

    // Move to next table
    current_table_index_ = (current_table_index_ + 1) % table_states_.size();
}