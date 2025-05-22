#ifndef JOB_SCHEDULER_H
#define JOB_SCHEDULER_H

#include <vector>
#include <thread>
#include <memory>
#include <atomic>
#include <stdexcept>
#include <chrono>

#include "ConfigData.h"
#include "JobDAG.h"
#include "ThreadSafeQueue.h"
#include "StepExecutionStrategy.h"


// 作业调度器类
class JobScheduler {
public:
    // 构造函数，初始化调度器
    explicit JobScheduler(const ConfigData& config);

    // 新的构造函数，允许传入自定义策略
    explicit JobScheduler(const ConfigData& config, std::unique_ptr<StepExecutionStrategy> strategy);

    // 测试专用工厂方法
    static std::unique_ptr<JobScheduler> create_for_testing(const ConfigData& config) {
        return std::make_unique<JobScheduler>(
            config, std::make_unique<DebugStepStrategy>()
        );
    }

    // 运行调度器
    void run();

private:
    // 工作线程循环
    void worker_loop();

    const ConfigData& config_;                        // 配置数据
    std::unique_ptr<JobDAG> dag_;                     // 作业依赖图
    std::shared_ptr<ThreadSafeQueue> queue_;          // 线程安全队列
    std::atomic<int> remaining_jobs_;                 // 剩余作业计数
    std::mutex done_mutex_;
    std::condition_variable done_cv_;
    std::unique_ptr<StepExecutionStrategy> step_strategy_; // 步骤执行策略
};

#endif // JOB_SCHEDULER_H