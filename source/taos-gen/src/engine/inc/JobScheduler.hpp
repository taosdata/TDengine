#pragma once

#include <vector>
#include <thread>
#include <memory>
#include <atomic>
#include <stdexcept>
#include <chrono>

#include "ConfigData.hpp"
#include "JobDAG.hpp"
#include "ThreadSafeQueue.hpp"
#include "StepExecutionStrategy.hpp"


// Job scheduler class
class JobScheduler {
public:
    // Constructor, initialize scheduler
    explicit JobScheduler(const ConfigData& config);

    // Constructor, allow custom strategy
    explicit JobScheduler(const ConfigData& config, std::unique_ptr<StepExecutionStrategy> strategy);

    // Factory method for testing
    static std::unique_ptr<JobScheduler> create_for_testing(const ConfigData& config) {
        return std::make_unique<JobScheduler>(
            config, std::make_unique<DebugStepStrategy>(config.global)
        );
    }

    // Run scheduler
    bool run();

    bool has_failure() const { return stop_execution_.load(); }

private:
    // Worker thread loop
    void worker_loop();

    const ConfigData& config_;                              // Configuration data
    std::unique_ptr<JobDAG> dag_;                           // Job dependency graph
    std::shared_ptr<ThreadSafeQueue> queue_;                // Thread-safe queue
    std::atomic<int> remaining_jobs_;                       // Remaining jobs count
    std::mutex done_mutex_;
    std::condition_variable done_cv_;
    std::unique_ptr<StepExecutionStrategy> step_strategy_;  // Step execution strategy

    std::atomic<bool> stop_execution_{false};
};