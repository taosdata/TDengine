#include "JobScheduler.h"
#include <iostream>


// 默认构造函数，使用生产环境策略
JobScheduler::JobScheduler(const ConfigData& config)
    : JobScheduler(config, std::make_unique<ProductionStepStrategy>()) {
}

// 新的构造函数实现
JobScheduler::JobScheduler(const ConfigData& config, std::unique_ptr<StepExecutionStrategy> strategy)
    : config_(config),
      dag_(std::make_unique<JobDAG>(config.jobs)),
      queue_(std::make_shared<ThreadSafeQueue>()),
      remaining_jobs_(config.jobs.size()),
      step_strategy_(std::move(strategy)) {

    if (dag_->has_cycle()) {
        throw std::runtime_error("DAG contains cycles");
    }

    // 初始化队列
    for (auto* node : dag_->get_initial_nodes()) {
        queue_->enqueue(node);
    }
}

void JobScheduler::run() {
    // 创建线程池
    const size_t concurrency = config_.concurrency;
    std::vector<std::thread> workers;
    for (size_t i = 0; i < concurrency; ++i) {
        workers.emplace_back([this] { worker_loop(); });
    }

    // 等待所有作业完成
    {
        std::unique_lock<std::mutex> lock(done_mutex_);
        done_cv_.wait(lock, [this] { return remaining_jobs_ == 0; });
    }

    // 停止队列并等待线程
    queue_->stop();
    for (auto& worker : workers) {
        if (worker.joinable()) worker.join();
    }
}

void JobScheduler::worker_loop() {
    while (true) {
        DAGNode* node = queue_->dequeue();
        if (!node) return;

        // 执行作业步骤
        for (const auto& step : node->job.steps) {
            step_strategy_->execute(step);
        }

        // 更新后继节点
        for (auto* successor : node->successors) {
            int prev = successor->in_degree.fetch_sub(1);
            if (prev == 1) { // 现在入度为0
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
