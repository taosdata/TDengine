#pragma once

#include "ColumnsCSVReader.hpp"
#include "TimestampUtils.hpp"
#include "LogUtils.hpp"
#include <mutex>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <future>


class CSVFileCache {
public:
    using TableMap = std::unordered_map<std::string, TableData>;

    const TableMap& get_data(const ColumnsCSV& config, const std::optional<ColumnConfigInstanceVector>& instances) {
        std::call_once(once_flag_, [this, &config, &instances]() {
            try {
                LogUtils::debug("Loading CSV file: {}", config.file_path);
                auto columns_csv = std::make_unique<ColumnsCSVReader>(config, instances);
                data_ = columns_csv->generate();
            } catch (const std::exception& e) {
                error_ = e.what();
            }
        });

        if (error_) {
            throw std::runtime_error("Failed to load CSV file '" + config.file_path + "': " + *error_);
        }
        return data_;
    }

private:
    std::once_flag once_flag_;
    TableMap data_;
    std::optional<std::string> error_;
};


class CSVDataManager {
public:
    using SharedRows = std::shared_ptr<const std::vector<RowData>>;

    // Get or create shared rows cache from table data
    static SharedRows get_shared_rows(const std::string& file_path,
                                     const TableData& table_data,
                                     const std::string& csv_precision,
                                     const std::string& target_precision) {
        auto& inst = instance();
        const std::string cache_key = build_shared_rows_cache_key_internal(
            file_path,
            csv_precision,
            target_precision
        );
        return inst.get_shared_rows_internal(table_data, csv_precision, target_precision, cache_key);
    }

    // Load file cache and get table data
    static std::pair<bool, const TableData*> get_table_data(const ColumnsCSV& csv_config,
                                                            const std::optional<ColumnConfigInstanceVector>& instances,
                                                            const std::string& table_name) {
        auto& inst = instance();
        std::shared_ptr<CSVFileCache> file_cache = inst.get_cache_for_file_internal(csv_config.file_path);
        const auto& all_tables = file_cache->get_data(csv_config, instances);

        auto it = all_tables.find(table_name);
        bool using_default = false;

        if (it == all_tables.end()) {
            it = all_tables.find(DEFAULT_TABLE_NAME);
            using_default = (it != all_tables.end());
        }

        if (it == all_tables.end()) {
            return {false, nullptr};
        }

        return {using_default, &it->second};
    }

    // Reset all caches
    static void reset() {
        auto& inst = instance();
        std::scoped_lock lock(inst.mutex_, inst.shared_rows_mutex_);
        inst.file_caches_.clear();
        inst.shared_rows_cache_.clear();
        inst.shared_rows_inflight_.clear();
    }

    static std::vector<RowData> convert_table_data_to_row_data(const TableData& table_data,
                                                               const std::string& csv_precision,
                                                               const std::string& target_precision) {
        std::vector<RowData> rows;
        rows.reserve(table_data.rows.size());
        for (size_t i = 0; i < table_data.rows.size(); i++) {
            RowData row;
            row.timestamp = TimestampUtils::convert_timestamp_precision(
                table_data.timestamps[i],
                csv_precision,
                target_precision
            );
            row.columns = table_data.rows[i];
            rows.push_back(std::move(row));
        }
        return rows;
    }

private:
    static CSVDataManager& instance() {
        static CSVDataManager manager;
        return manager;
    }

    CSVDataManager() = default;
    ~CSVDataManager() = default;
    CSVDataManager(const CSVDataManager&) = delete;
    CSVDataManager& operator=(const CSVDataManager&) = delete;

    std::shared_ptr<CSVFileCache> get_cache_for_file_internal(const std::string& file_path) {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = file_caches_.find(file_path);
        if (it == file_caches_.end()) {
            it = file_caches_.emplace(file_path, std::make_shared<CSVFileCache>()).first;
        }
        return it->second;
    }

    SharedRows get_shared_rows_internal(const TableData& table_data,
                                        const std::string& csv_precision,
                                        const std::string& target_precision,
                                        const std::string& cache_key) {
        std::shared_ptr<std::promise<SharedRows>> build_promise;
        std::shared_future<SharedRows> wait_future;

        {
            std::lock_guard<std::mutex> lock(shared_rows_mutex_);

            auto cache_it = shared_rows_cache_.find(cache_key);
            if (cache_it != shared_rows_cache_.end()) {
                return cache_it->second;
            }

            auto inflight_it = shared_rows_inflight_.find(cache_key);
            if (inflight_it != shared_rows_inflight_.end()) {
                wait_future = inflight_it->second;
            } else {
                build_promise = std::make_shared<std::promise<SharedRows>>();
                wait_future = build_promise->get_future().share();
                shared_rows_inflight_.emplace(cache_key, wait_future);
            }
        }

        if (!build_promise) {
            return wait_future.get();
        }

        try {
            auto rows = std::make_shared<std::vector<RowData>>(
                convert_table_data_to_row_data(table_data, csv_precision, target_precision)
            );

            SharedRows result;
            {
                std::lock_guard<std::mutex> lock(shared_rows_mutex_);
                auto cache_it = shared_rows_cache_.find(cache_key);
                if (cache_it != shared_rows_cache_.end()) {
                    result = cache_it->second;
                } else {
                    shared_rows_cache_.emplace(cache_key, rows);
                    result = rows;
                }

                // Keep inflight marker until the shared future is fulfilled.
                build_promise->set_value(result);
                shared_rows_inflight_.erase(cache_key);
            }
            return result;
        } catch (...) {
            {
                std::lock_guard<std::mutex> lock(shared_rows_mutex_);

                // Keep inflight marker until the shared future is fulfilled.
                build_promise->set_exception(std::current_exception());
                shared_rows_inflight_.erase(cache_key);
            }
            throw;
        }
    }

    // Cache key uses DEFAULT_TABLE_NAME because shared rows are only
    // created when using default table (tbname_index = -1)
    static std::string build_shared_rows_cache_key_internal(const std::string& file_path,
                                                            const std::string& csv_precision,
                                                            const std::string& target_precision) {
        return file_path + "|" + DEFAULT_TABLE_NAME + "|" + csv_precision + "|" + target_precision;
    }

    std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<CSVFileCache>> file_caches_;

    std::mutex shared_rows_mutex_;
    std::unordered_map<std::string, SharedRows> shared_rows_cache_;
    std::unordered_map<std::string, std::shared_future<SharedRows>> shared_rows_inflight_;
};