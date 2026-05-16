#pragma once

#include <vector>
#include <string>
#include <unordered_map>
#include <atomic>
#include <memory>

#include "Job.hpp"


// DAG node structure
struct DAGNode {
    Job job;                              // Job information
    std::atomic<int> in_degree{0};        // In-degree
    std::vector<DAGNode*> successors;     // List of successor nodes
};

// JobDAG class for building and operating job dependency graph
class JobDAG {
public:
    // Constructor, takes job list and builds DAG
    explicit JobDAG(const std::vector<Job>& jobs);

    // Check if there is a cycle
    bool has_cycle() const;

    // Get initial nodes (nodes with in-degree 0)
    std::vector<DAGNode*> get_initial_nodes() const;

private:
    std::unordered_map<std::string, DAGNode*> key_to_node_; // Mapping from job key to node
    std::vector<std::unique_ptr<DAGNode>> nodes_;           // Storage for all nodes
};
