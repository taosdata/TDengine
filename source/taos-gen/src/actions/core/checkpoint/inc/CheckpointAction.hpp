#pragma once
#include <iostream>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <string>
#include <unordered_map>
#include <cstdint>
#include "ActionBase.hpp"
#include "ActionFactory.hpp"
#include "CheckpointActionConfig.hpp"
#include "InsertDataConfig.hpp"
#include "CheckpointData.hpp"

class CheckpointAction : public ActionBase {
public:
    static void register_signal_handlers();

    explicit CheckpointAction(const GlobalConfig& global, const CheckpointActionConfig& config) : global_(global), config_(config) {}

     ~CheckpointAction(); // Add destructor to manage thread lifecycle

    void execute() override;
    void notify(const std::any& payload) override;

    void update_checkpoint(const std::vector<CheckpointData>& data_list);

    static void stop_all(bool is_interrupt = false);

    static void checkpoint_recover(const GlobalConfig& global, InsertDataConfig& config);

    static bool is_recover(const GlobalConfig& global, const CheckpointInfo& config);

private:
    void run_timer();
    void save_checkpoint();
    void delete_checkpoint();
    static void wait_for_all_to_stop();

    const GlobalConfig& global_;
    CheckpointActionConfig config_;

    // For the checkpoint map
    mutable std::mutex map_mutex_;
    std::unordered_map<std::string, CheckpointData> checkpoint_map_;

    // For the timer thread
    std::thread timer_thread_;
    static std::atomic<bool> global_stop_flag_;
    static std::atomic<bool> global_interrupt_flag_;
    static std::mutex global_mutex_;
    static std::condition_variable global_cv_;
    static std::atomic<int> active_threads_count_;

    // Register CheckpointAction to ActionFactory
    inline static bool registered_ = []() {
        ActionFactory::instance().register_action(
            "actions/checkpoint",
            [](const GlobalConfig& global, const ActionConfigVariant& config) {
                return std::make_unique<CheckpointAction>(global, std::get<CheckpointActionConfig>(config));
            });
        return true;
    }();
};