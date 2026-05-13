#include "JobScheduler.hpp"
#include <iostream>


JobScheduler::JobScheduler(const ConfigData& config)
    : JobScheduler(config, std::make_unique<ProductionStepStrategy>(config.global)) {
}

JobScheduler::JobScheduler(const ConfigData& config, std::unique_ptr<StepExecutionStrategy> strategy)
    : config_(config),
      dag_(std::make_unique<JobDAG>(config.jobs)),
      queue_(std::make_shared<ThreadSafeQueue>()),
      remaining_jobs_(config.jobs.size()),
      step_strategy_(std::move(strategy)) {

    if (dag_->has_cycle()) {
        throw std::runtime_error("DAG contains cycles");
    }

    // Initialize queue
    for (auto* node : dag_->get_initial_nodes()) {
        queue_->enqueue(node);
    }
}

bool JobScheduler::run() {
    // Create thread pool
    const size_t concurrency = config_.concurrency;
    std::vector<std::thread> workers;
    for (size_t i = 0; i < concurrency; ++i) {
        workers.emplace_back([this] { worker_loop(); });
    }

    // Wait for all jobs to complete
    {
        std::unique_lock<std::mutex> lock(done_mutex_);
        done_cv_.wait(lock, [this] { return remaining_jobs_ == 0 || stop_execution_.load(); });
    }

    // Stop queue and wait for threads
    queue_->stop();
    for (auto& worker : workers) {
        if (worker.joinable()) worker.join();
    }

    return !stop_execution_.load();
}

void JobScheduler::worker_loop() {
    while (true) {
        if (stop_execution_.load()) return;

        DAGNode* node = queue_->dequeue();
        if (!node) return;

        // Execute job steps
        for (const auto& step : node->job.steps) {
            if (stop_execution_.load()) return;

            bool success = step_strategy_->execute(step);
            if (!success) {
                stop_execution_.store(true);
                LogUtils::error(
                    "Job step execution failed, exiting (job: {}, step: {})",
                    node->job.name,
                    step.name
                );
                queue_->stop();
                {
                    std::unique_lock<std::mutex> lock(done_mutex_);
                    done_cv_.notify_all();
                }
                return;
            }
        }

        // Update successor nodes
        for (auto* successor : node->successors) {
            int prev = successor->in_degree.fetch_sub(1);
            if (prev == 1) { // Now in-degree is 0
                queue_->enqueue(successor);
            }
        }

        int left = remaining_jobs_.fetch_sub(1);
        if (left == 1) {
            std::unique_lock<std::mutex> lock(done_mutex_);
            done_cv_.notify_all();
        }
    }
}