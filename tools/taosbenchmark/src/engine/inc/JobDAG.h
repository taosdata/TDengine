#ifndef JOB_DAG_H
#define JOB_DAG_H

#include <vector>
#include <string>
#include <unordered_map>
#include <atomic>
#include <memory>

#include "Job.h"


// DAG 节点结构
struct DAGNode {
    Job job;                              // 作业信息
    std::atomic<int> in_degree{0};        // 入度
    std::vector<DAGNode*> successors;    // 后继节点列表
};

// JobDAG 类，用于构建和操作作业依赖图
class JobDAG {
public:
    // 构造函数，接受作业列表并构建 DAG
    explicit JobDAG(const std::vector<Job>& jobs);

    // 检查是否存在循环依赖
    bool has_cycle() const;

    // 获取初始节点（入度为 0 的节点）
    std::vector<DAGNode*> get_initial_nodes() const;

private:
    std::unordered_map<std::string, DAGNode*> key_to_node_; // 作业键到节点的映射
    std::vector<std::unique_ptr<DAGNode>> nodes_;           // 所有节点的存储
};

#endif // JOB_DAG_H