#include "JobScheduler.h"
#include <iostream>

JobScheduler::JobScheduler(const ConfigData& config) 
    : config_(config),
      dag_(std::make_unique<JobDAG>(config.jobs)),
      queue_(std::make_shared<ThreadSafeQueue>()),
      remaining_jobs_(config.jobs.size()) {

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
        workers.emplace_back([this] { workerLoop(); });
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

void JobScheduler::workerLoop() {
    while (true) {
        DAGNode* node = queue_->dequeue();
        if (!node) return;

        // 执行作业步骤
        for (const auto& step : node->job.steps) {
            execute_step(step);
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


void JobScheduler::execute_step(const Step& step) {
    // 打印调试信息
    std::cout << "Executing step: " << step.name << " (" << step.uses << ")" << std::endl;

    // 根据 step.uses 执行不同的操作
    if (step.uses == "actions/create-database") {
        const auto& config = std::get<CreateDatabaseConfig>(step.action_config);
        std::cout << "Creating database: " << config.database_info.name << std::endl;
        // 在此处调用具体的创建数据库逻辑
    } else if (step.uses == "actions/create-super-table") {
        const auto& config = std::get<CreateSuperTableConfig>(step.action_config);
        std::cout << "Creating super table: " << config.super_table_info.name << std::endl;
        // 在此处调用具体的创建超级表逻辑
    } else if (step.uses == "actions/create-child-table") {
        const auto& config = std::get<CreateChildTableConfig>(step.action_config);
        std::cout << "Creating child table for super table: " << config.super_table_info.name << std::endl;
        // 在此处调用具体的创建子表逻辑
    } else if (step.uses == "actions/insert-data") {
        const auto& config = std::get<InsertDataConfig>(step.action_config);
        std::cout << "Inserting data into table: " << config.target.tdengine.super_table_info.name << std::endl;
        // 在此处调用具体的数据插入逻辑
    } else if (step.uses == "actions/query-data") {
        const auto& config = std::get<QueryDataConfig>(step.action_config);
        std::cout << "Querying data from database: " << config.source.connection_info.host << std::endl;
        // 在此处调用具体的数据查询逻辑
    } else if (step.uses == "actions/subscribe-data") {
        const auto& config = std::get<SubscribeDataConfig>(step.action_config);
        std::cout << "Subscribing to data from topics: ";
        for (const auto& topic : config.control.subscribe_control.topics) {
            std::cout << topic.name << " ";
        }
        std::cout << std::endl;
        // 在此处调用具体的数据订阅逻辑
    } else {
        std::cerr << "Unknown action type: " << step.uses << std::endl;
        throw std::runtime_error("Unknown action type: " + step.uses);
    }

    // 打印调试信息，表示步骤完成
    std::cout << "Step completed: " << step.name << std::endl;
}

