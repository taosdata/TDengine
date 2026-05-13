#pragma once

#include "ColumnConfigInstance.hpp"
#include "concurrentqueue.hpp"
#include "blockingconcurrentqueue.hpp"
#include "ColumnConverter.hpp"
#include "CheckpointData.hpp"
#include "TableData.hpp"
#include "taos.h"
#include <vector>
#include <atomic>
#include <memory>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <stdexcept>

class MemoryPool {
public:
    struct TableBase {
        struct Column {
            bool is_fixed;
            size_t element_size;            // Fixed column: element size
            size_t max_length;              // Variable column: max length

            // Fixed column data pointer
            void* fixed_data = nullptr;

            // Variable column data
            char* var_data = nullptr;      // Continuous storage area
            int32_t* lengths = nullptr;    // Length per row
            size_t* var_offsets = nullptr; // Offset per row in variable data area
            size_t current_offset = 0;     // Current write offset

            // NULL marker
            char* is_nulls = nullptr;
        };

        size_t max_rows = 0;
        size_t used_rows = 0;
        std::vector<Column> columns;
        const std::vector<ColumnConverter::ColumnHandler>* col_handlers_ptr = nullptr;
        const std::vector<ColumnConverter::ColumnHandler>* tag_handlers_ptr = nullptr;

        void fill_row(size_t row_index, const RowData& row);
    };

    struct CachedTableBlock : public TableBase {
        bool data_prefilled = false;

        void fill_cached_data(size_t row_index, const RowData& row);
        void fill_cached_data_batch(const std::vector<RowData>& rows, size_t start_row = 0);
    };

    struct TableBlock : public TableBase {
        struct Tags {
            std::string table_name;
            std::vector<TableBase::Column> columns;
            void* data_chunk = nullptr;
            size_t data_chunk_size = 0;
            std::vector<TAOS_STMT2_BIND> bind_tags;

            Tags() = default;
            ~Tags();

            Tags(const Tags&) = delete;
            Tags& operator=(const Tags&) = delete;

            Tags(Tags&& other) noexcept;
            Tags& operator=(Tags&& other) noexcept;
        };

        const char* table_name;
        int64_t* timestamps = nullptr;
        const CachedTableBlock* cached_table_block = nullptr;
        const Tags* tags_ptr = nullptr;

        void init_from_cache(const CachedTableBlock& cached_block);
        void add_row_cached(const RowData& row);
        void add_rows_cached(const std::vector<RowData>& rows);
        void add_row(const RowData& row);
        void add_rows(const std::vector<RowData>& rows);

        template <typename Cols, typename Handlers>
        ColumnType get_cell_impl(size_t row_index, size_t col_index,
                                 const char* context,
                                 const Cols& cols,
                                 const Handlers& handlers) const;
        ColumnType get_column_cell(size_t row_index, size_t col_index) const;
        ColumnType get_tag_cell(size_t row_index, size_t col_index) const;
        std::string get_column_cell_as_string(size_t row_index, size_t col_index) const;
        std::string get_tag_cell_as_string(size_t row_index, size_t col_index) const;
    };

    struct MemoryBlock {
        std::vector<TableBlock> tables;
        int64_t start_time = std::numeric_limits<int64_t>::max();
        int64_t end_time = std::numeric_limits<int64_t>::min();
        size_t total_rows = 0;
        size_t used_tables = 0;                                 // Actual used table count
        size_t tag_count = 0;
        size_t col_count = 0;
        size_t cache_index = 0;
        bool in_use = false;
        MemoryPool* owning_pool = nullptr;

        void* data_chunk = nullptr;                             // Continuous memory block for all data
        size_t data_chunk_size = 0;                             // Memory block size

        TAOS_STMT2_BINDV bindv_{};
        std::vector<const char*> tbnames_;                      // Table name pointer array
        std::vector<TAOS_STMT2_BIND*> bind_tags_ptrs_;          // Bind tags pointer array
        std::vector<TAOS_STMT2_BIND*> bind_cols_ptrs_;          // Bind cols pointer array
        std::vector<std::vector<TAOS_STMT2_BIND>> bind_lists_;  // Bind data storage
        std::vector<CheckpointData> checkpoint_data_list_;      // Checkpoint info for each table

        void release();
        void free_data_chunk();

        void init_bindv();
        void build_bindv(bool is_checkpoint_recover = false);
        void reset();
    };

    struct CacheUnit {
        std::vector<CachedTableBlock> tables;
        void* data_chunk = nullptr;
        size_t data_chunk_size = 0;
        std::atomic<size_t> prefilled_count{0};

        CacheUnit() = default;
        ~CacheUnit();

        CacheUnit(const CacheUnit&) = delete;
        CacheUnit& operator=(const CacheUnit&) = delete;

        CacheUnit(CacheUnit&& other) noexcept;
        CacheUnit& operator=(CacheUnit&& other) noexcept;
    };

    MemoryPool(size_t num_blocks,
               size_t max_tables_per_block,
               size_t max_rows_per_table,
               const ColumnConfigInstanceVector& col_instances,
               const ColumnConfigInstanceVector& tag_instances,
               bool tables_reuse_data = false,
               size_t num_cached_blocks = 0
            );


    ~MemoryPool();

    // Get a free memory block (thread-safe)
    MemoryBlock* acquire_block(size_t sequence_num = 0);

    // Return a memory block (thread-safe)
    void release_block(MemoryBlock* block);

    // Write MultiBatch data into MemoryBlock
    MemoryBlock* convert_to_memory_block(MultiBatch&& batch);

    const std::vector<ColumnConverter::ColumnHandler>& col_handlers() const;
    const std::vector<ColumnConverter::ColumnHandler>& tag_handlers() const;

    bool is_cache_mode() const;
    CacheUnit* get_cache_unit(size_t index);
    size_t get_cache_units_count() const;
    bool fill_cache_unit_data(size_t cache_index, size_t table_index, const std::vector<RowData>& data_rows);
    bool is_cache_unit_prefilled(size_t cache_index) const;

    MemoryPool::TableBlock::Tags* register_table_tags(const std::string& table_name, const std::vector<ColumnType>& tag_values);
    const TableBlock::Tags* get_table_tags(const std::string& table_name) const;
    bool has_table_tags(const std::string& table_name) const;

private:
    struct TagsManager {
        const ColumnConfigInstanceVector& tag_instances_;
        std::vector<ColumnConverter::ColumnHandler> tag_handlers_;
        std::unordered_map<std::string, std::unique_ptr<TableBlock::Tags>> tags_map_;
        mutable std::mutex tags_map_mutex_;

        size_t fixed_data_size_ = 0;
        size_t var_meta_size_ = 0;
        size_t var_data_size_ = 0;
        size_t common_meta_size_ = 0;
        size_t total_size_ = 0;

        TagsManager(const ColumnConfigInstanceVector& tag_instances);

        void calculate_memory_size();
        void init_table_tags(TableBlock::Tags& tags, const std::vector<ColumnType>& tag_values);
        MemoryPool::TableBlock::Tags* register_tags(const std::string& table_name, const std::vector<ColumnType>& tag_values);
        const TableBlock::Tags* get_tags(const std::string& table_name) const;
        bool has_tags(const std::string& table_name) const;
    };

    size_t max_tables_per_block_;
    size_t max_rows_per_table_;
    const ColumnConfigInstanceVector& col_instances_;
    const ColumnConfigInstanceVector& tag_instances_;
    std::vector<ColumnConverter::ColumnHandler> col_handlers_;
    std::vector<ColumnConverter::ColumnHandler> tag_handlers_;
    std::vector<MemoryBlock> blocks_;
    moodycamel::BlockingConcurrentQueue<MemoryBlock*> free_queue_;

    // Cache related members
    bool tables_reuse_data_ = false;
    size_t num_cached_blocks_ = 0;
    std::vector<CacheUnit> cache_units_;

    // Memory size calculation
    size_t timestamps_size_ = 0;
    size_t common_meta_size_ = 0;
    size_t fixed_data_size_ = 0;
    size_t var_meta_size_ = 0;
    size_t var_data_size_ = 0;
    size_t total_cache_size_ = 0;

    std::unique_ptr<TagsManager> tags_manager_;

    void init_cache_units();
    void init_normal_blocks();
    void init_cached_blocks();
};