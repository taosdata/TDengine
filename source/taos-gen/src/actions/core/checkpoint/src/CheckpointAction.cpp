#include "CheckpointAction.hpp"
#include "LogUtils.hpp"
#include "SignalManager.hpp"
#include <chrono>
#include <fstream>
#include <cstdio> // For std::remove
#include <nlohmann/json.hpp>

std::atomic<bool> CheckpointAction::global_stop_flag_{false};
std::atomic<bool> CheckpointAction::global_interrupt_flag_{false};
std::mutex CheckpointAction::global_mutex_;
std::condition_variable CheckpointAction::global_cv_;
std::atomic<int> CheckpointAction::active_threads_count_{0};

namespace {
const TDengineConfig& get_tdengine_config_from_global(const GlobalConfig& global) {
    const auto* tc = get_plugin_config<TDengineConfig>(global.extensions, "tdengine");
    if (tc == nullptr) {
        throw std::runtime_error("TDengine configuration not found in global extensions.");
    }
    return *tc;
}
} // namespace


CheckpointAction::~CheckpointAction() {
    if (timer_thread_.joinable()) {
        timer_thread_.join();   // Wait for the thread to finish
    }
}

void CheckpointAction::execute() {
    if (config_.enabled) {
        {
            std::lock_guard<std::mutex> lock(global_mutex_);
            active_threads_count_++;
        }
        // Start the timer in a separate thread
        timer_thread_ = std::thread([this]() {
            LogUtils::info("[Checkpoint] Timer thread started");
            this->run_timer();
        });

        LogUtils::info("Starting checkpoint timer with an interval of {} seconds.", config_.interval_sec);
    }
}

void CheckpointAction::run_timer() {
    const size_t interval_sec = config_.interval_sec;
    // Loop until stop_flag_ is set or global interrupt received
    while (!global_stop_flag_.load() && !SignalManager::interrupted()) {
        // Wait for the specified interval using short polling to respond quickly to interrupts
        {
            std::unique_lock<std::mutex> lock(global_mutex_);
            auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(interval_sec);
            while (!global_stop_flag_.load() && !SignalManager::interrupted()
                   && std::chrono::steady_clock::now() < deadline) {
                global_cv_.wait_for(lock, std::chrono::milliseconds(200));
            }
        }
        if (global_stop_flag_.load() || SignalManager::interrupted()) break;
        try {
            save_checkpoint();
        } catch (const std::exception& e) {
            LogUtils::error("[Checkpoint] Error saving checkpoint: {}", e.what());
        }
    }

    // If interrupted by signal, set internal flags for consistency
    if (SignalManager::interrupted()) {
        global_interrupt_flag_.store(true);
        global_stop_flag_.store(true);
    }

    if (!global_interrupt_flag_.load()) {
        try {
            delete_checkpoint();
        } catch (const std::exception& e) {
            LogUtils::error("[Checkpoint] Error deleting checkpoint: {}", e.what());
        }
    }

    {
        std::lock_guard<std::mutex> lock(global_mutex_);
        active_threads_count_--; // Decrement thread count
    }

    global_cv_.notify_all();
    LogUtils::info("Checkpoint timer stopped");
}


void CheckpointAction::wait_for_all_to_stop() {
    std::unique_lock<std::mutex> lock(global_mutex_);
    // Wait until active_threads_count_ becomes 0
    global_cv_.wait(lock, [] { return active_threads_count_ == 0; });
}

void CheckpointAction::stop_all(bool is_interrupt) {
    request_stop(is_interrupt);
    CheckpointAction::wait_for_all_to_stop();
}

void CheckpointAction::request_stop(bool is_interrupt) {
    if (is_interrupt) {
        global_interrupt_flag_.store(true);
    }
    global_stop_flag_.store(true);
    global_cv_.notify_all();
}

void CheckpointAction::save_checkpoint() {
    std::lock_guard<std::mutex> lock(map_mutex_);
    if (checkpoint_map_.empty() || static_cast<size_t>(config_.tableCount) > checkpoint_map_.size()) {
        LogUtils::info("[Checkpoint] No progress data to save. {}", checkpoint_map_.size());
        return;
    }
    // Find the entry with the smallest last_checkpoint_time
    auto min_it = std::min_element(
        checkpoint_map_.begin(),
        checkpoint_map_.end(),
        [](const auto& a, const auto& b) {
            return a.second.last_checkpoint_time < b.second.last_checkpoint_time;
        }
    );
    std::string min_table_name = min_it->first;
    // Save checkpoint_data to a file or database
    std::string file_path = global_.yaml_cfg_dir + "_" + get_tdengine_config_from_global(global_).database + "_" + global_.schema.name + "_checkpoints.json"; // Example file path
    nlohmann::json json_data;
    json_data["table_name"] = min_table_name;
    json_data["last_checkpoint_time"] = min_it->second.last_checkpoint_time;
    std::ofstream ofs(file_path);
    if (ofs) {
        ofs << json_data.dump(4) << std::endl; // Pretty print with 4 spaces indentation
        ofs.close();
        LogUtils::info(
            "[Checkpoint] Saved progress for table '{}' at timestamp {} with write count {} starting from {} write count {} to {}",
            min_table_name,
            min_it->second.last_checkpoint_time,
            min_it->second.writeCount,
            config_.start_timestamp,
            (min_it->second.last_checkpoint_time - config_.start_timestamp) / config_.timestamp_step + 1,
            file_path
        );
    } else {
        LogUtils::error("[Checkpoint] Failed to open file for writing: {}", file_path);
    }
}

void CheckpointAction::notify(const std::any& payload) {
    if (payload.type() == typeid(std::reference_wrapper<const std::vector<CheckpointData>>)) {
        try {
            const auto& wrapper = std::any_cast<std::reference_wrapper<const std::vector<CheckpointData>>>(payload);
            update_checkpoint(wrapper.get());
        } catch (const std::bad_any_cast& e) {
            LogUtils::error("[Checkpoint] update_checkpoint call failed: {}", e.what());
        }
    }
}


void CheckpointAction::update_checkpoint(const std::vector<CheckpointData>& data_list) {
    if (data_list.empty()) {
        LogUtils::info("[Checkpoint] No data provided for update");
        return;
    }

    std::lock_guard<std::mutex> lock(map_mutex_);
    for (const auto& data : data_list) {
        auto it = checkpoint_map_.find(data.table_name);
        if (it != checkpoint_map_.end()) {
            // Key exists, update it.
            it->second.last_checkpoint_time = data.last_checkpoint_time;
        } else {
            // Key doesn't exist, insert new entry.
            checkpoint_map_[data.table_name] = data;
        }
    }
    // std::cout << "[Checkpoint] Updated checkpoint data for " << data_list.size() << " entries. Current map size: " << checkpoint_map_.size() << std::endl;
}

bool CheckpointAction::is_recover(const GlobalConfig& global, const CheckpointInfo& config) {
    if (!config.enabled) {
        return false;
    }

    std::string file_path = global.yaml_cfg_dir + "_" + get_tdengine_config_from_global(global).database + "_" + global.schema.name + "_checkpoints.json"; // Example file path
    std::ifstream ifs(file_path);
    if (!ifs) {
        return false; // Return if file doesn't exist
    }
    ifs.close();
    return true;
}

void CheckpointAction::checkpoint_recover(const GlobalConfig& global, InsertDataConfig& config) {
    if (config.target_type != "tdengine" || !config.checkpoint_info.enabled) {
        return;
    }

    std::string file_path = global.yaml_cfg_dir + "_" + get_tdengine_config_from_global(global).database + "_" + global.schema.name + "_checkpoints.json"; // Example file path
    std::ifstream ifs(file_path);
    if (!ifs) {
        return; // Return if file doesn't exist
    }

    nlohmann::json json_data;
    ifs >> json_data;
    ifs.close();

    CheckpointData checkpoint_data;
    int64_t start_timestamp = TimestampUtils::parse_timestamp(
        config.schema.columns_cfg.generator.timestamp_strategy.timestamp_config.start_timestamp,
        config.schema.columns_cfg.generator.timestamp_strategy.timestamp_config.timestamp_precision
    );

    int timestamp_step = std::get<Timestamp>(config.schema.columns_cfg.generator.timestamp_strategy.timestamp_config.timestamp_step);

    checkpoint_data.table_name = json_data["table_name"].get<std::string>();
    checkpoint_data.last_checkpoint_time = json_data["last_checkpoint_time"].get<std::int64_t>();
    checkpoint_data.writeCount = (checkpoint_data.last_checkpoint_time - start_timestamp) / timestamp_step;

    if (config.schema.generation.rows_per_table <= checkpoint_data.writeCount) {
        return;
    }
    config.schema.columns_cfg.generator.timestamp_strategy.timestamp_config.start_timestamp = (Timestamp)checkpoint_data.last_checkpoint_time;
    config.schema.generation.rows_per_table -= checkpoint_data.writeCount;
}

void CheckpointAction::delete_checkpoint() {
    std::lock_guard<std::mutex> lock(map_mutex_);
    std::string file_path = global_.yaml_cfg_dir + "_" + get_tdengine_config_from_global(global_).database + "_" + global_.schema.name + "_checkpoints.json";
    std::remove(file_path.c_str());
    checkpoint_map_.clear();
    LogUtils::info("[Checkpoint] delete checkpoint file and cleared all in-memory checkpoint data");
}
