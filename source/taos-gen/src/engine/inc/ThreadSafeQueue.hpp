#pragma once

#include <queue>
#include <mutex>
#include <condition_variable>

#include "JobDAG.hpp"

class ThreadSafeQueue {
public:
    // Add node to queue
    void enqueue(DAGNode* node);

    // Remove node from queue
    DAGNode* dequeue();

    // Stop the queue
    void stop();

private:
    std::queue<DAGNode*> queue_;              // Queue storing DAGNode pointers
    std::mutex mtx_;                          // Mutex for queue protection
    std::condition_variable cv_;              // Condition variable for synchronization
    bool stop_ = false;                       // Stop flag
};
