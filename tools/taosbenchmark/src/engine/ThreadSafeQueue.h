#ifndef THREAD_SAFE_QUEUE_H
#define THREAD_SAFE_QUEUE_H

#include <queue>
#include <mutex>
#include <condition_variable>

#include "JobDAG.h"


class ThreadSafeQueue {
public:
    // 将节点加入队列
    void enqueue(DAGNode* node);

    // 从队列中取出节点
    DAGNode* dequeue();

    // 停止队列
    void stop();

private:
    std::queue<DAGNode*> queue_;              // 队列存储 DAGNode 指针
    std::mutex mtx_;                          // 互斥锁保护队列
    std::condition_variable cv_;              // 条件变量用于同步
    bool stop_ = false;                       // 停止标志
};

#endif // THREAD_SAFE_QUEUE_H