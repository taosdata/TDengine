#include "JobDAG.h"
#include <queue>

JobDAG::JobDAG(const std::vector<Job>& jobs) {
    // 构建节点
    for (const auto& job : jobs) {
        auto node = std::make_unique<DAGNode>();
        node->job = job;
        key_to_node_[job.key] = node.get();
        nodes_.push_back(std::move(node));
    }

    // 构建边
    for (const auto& job : jobs) {
        DAGNode* current = key_to_node_[job.key];
        for (const auto& dep_key : job.needs) {
            auto it = key_to_node_.find(dep_key);
            if (it == key_to_node_.end()) {
                throw std::runtime_error("Missing dependency: " + dep_key);
            }
            DAGNode* dep_node = it->second;
            dep_node->successors.push_back(current);
            current->in_degree++;
        }
    }
}

bool JobDAG::has_cycle() const {
    std::queue<DAGNode*> q;
    std::unordered_map<DAGNode*, int> in_degree_copy;
    int processed = 0;

    for (const auto& pair : key_to_node_) {
        in_degree_copy[pair.second] = pair.second->in_degree;
        if (in_degree_copy[pair.second] == 0) {
            q.push(pair.second);
        }
    }

    while (!q.empty()) {
        auto* node = q.front();
        q.pop();
        processed++;

        for (auto* successor : node->successors) {
            if (--in_degree_copy[successor] == 0) {
                q.push(successor);
            }
        }
    }

    return processed != key_to_node_.size();
}

std::vector<DAGNode*> JobDAG::get_initial_nodes() const {
    std::vector<DAGNode*> initial;
    for (const auto& pair : key_to_node_) {
        if (pair.second->in_degree == 0) {
            initial.push_back(pair.second);
        }
    }
    return initial;
}

