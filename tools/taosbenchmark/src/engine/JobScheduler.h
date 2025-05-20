#ifndef JOB_SCHEDULER_H
#define JOB_SCHEDULER_H

#include <vector>
#include <thread>
#include <memory>
#include <atomic>
#include <stdexcept>
#include <chrono>

#include "JobDAG.h"
#include "ThreadSafeQueue.h"

// 作业调度器类
class JobScheduler {
public:
    // 构造函数，初始化调度器
    explicit JobScheduler(const ConfigData& config);

    // 运行调度器
    void run();

private:
    // 工作线程循环
    void workerLoop();

    // 执行单个步骤
    void execute_step(const Step& step);

    const ConfigData& config_;                        // 配置数据
    std::unique_ptr<JobDAG> dag_;                     // 作业依赖图
    std::shared_ptr<ThreadSafeQueue> queue_;          // 线程安全队列
    std::atomic<int> remaining_jobs_;                 // 剩余作业计数
    std::mutex done_mutex_;
    std::condition_variable done_cv_;
};

#endif // JOB_SCHEDULER_H