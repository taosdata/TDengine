#include "JobScheduler.hpp"
#include <iostream>
#include <sstream>
#include <cassert>
#include <vector>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <mutex>


struct ConfigWithDependencies {
    ConfigData config;
    std::unordered_map<std::string, std::unordered_set<std::string>> dependencies;
};

// Build complex config data and dependency graph
ConfigWithDependencies build_complex_config_with_dependencies() {
    ConfigData config;
    config.concurrency = 3;

    // Define steps
    Step create_database_step{"Create Database", "tdengine/create-database", YAML::Node(), {}};
    Step create_super_table_step{"Create Super Table", "tdengine/create-super-table", YAML::Node(), {}};
    Step create_second_child_table_step{"Create Second Child Table", "tdengine/create-child-table", YAML::Node(), {}};
    Step create_minute_child_table_step{"Create Minute Child Table", "tdengine/create-child-table", YAML::Node(), {}};
    Step insert_second_data_step{"Insert Second-Level Data", "tdengine/insert", YAML::Node(), {}};
    Step insert_minute_data_step{"Insert Minute-Level Data", "tdengine/insert", YAML::Node(), {}};
    Step query_super_table_step{"Query Super Table", "tdengine/query", YAML::Node(), {}};
    Step subscribe_data_step{"Subscribe Data", "tdengine/subscribe", YAML::Node(), {}};

    // Define jobs
    Job create_database_job{"create-database", "Create Database", {}, {create_database_step}};
    Job create_super_table_job{"create-super-table", "Create Super Table", {"create-database"}, {create_super_table_step}};
    Job create_second_child_table_job{"create-second-child-table", "Create Second Child Table", {"create-super-table"}, {create_second_child_table_step}};
    Job create_minute_child_table_job{"create-minute-child-table", "Create Minute Child Table", {"create-super-table"}, {create_minute_child_table_step}};
    Job insert_second_data_job{"insert-second-data", "Insert Second-Level Data", {"create-second-child-table"}, {insert_second_data_step}};
    Job insert_minute_data_job{"insert-minute-data", "Insert Minute-Level Data", {"create-minute-child-table"}, {insert_minute_data_step}};
    Job query_super_table_job{"query-super-table", "Query Super Table", {"create-second-child-table", "create-minute-child-table"}, {query_super_table_step}};
    Job subscribe_data_job{"subscribe-data", "Subscribe Data", {"create-second-child-table", "create-minute-child-table"}, {subscribe_data_step}};

    // Add jobs to config
    config.jobs = {
        create_database_job,
        create_super_table_job,
        create_second_child_table_job,
        create_minute_child_table_job,
        insert_second_data_job,
        insert_minute_data_job,
        query_super_table_job,
        subscribe_data_job
    };

    // Build dependency graph
    std::unordered_map<std::string, std::unordered_set<std::string>> dependencies;
    for (const auto& job : config.jobs) {
        dependencies[job.key] = std::unordered_set<std::string>(job.needs.begin(), job.needs.end());
    }

    return {config, dependencies};
}

void test_job_scheduler_base() {
    // Build complex config data
    auto result = build_complex_config_with_dependencies();
    const ConfigData& config = result.config;
    // const auto& dependencies = result.dependencies;

    // Create scheduler using create_for_testing factory method
    auto scheduler = JobScheduler::create_for_testing(config);

    // Run scheduler
    bool success = scheduler->run();
    (void)success;
    assert(success);

    // Print test passed info
    std::cout << "test_job_scheduler_base passed!" << std::endl;
}

void validate_execution_order(const std::vector<std::string>& actual_order,
                              const std::unordered_map<std::string, std::unordered_set<std::string>>& dependencies) {
    // Set of completed jobs
    std::unordered_set<std::string> completed_jobs;

    // Validate step order according to dependencies
    for (const auto& step : actual_order) {
        // Extract job key
        std::string job_key;
        if (step.find("Create Database") != std::string::npos) {
            job_key = "create-database";
        } else if (step.find("Create Super Table") != std::string::npos) {
            job_key = "create-super-table";
        } else if (step.find("Create Second Child Table") != std::string::npos) {
            job_key = "create-second-child-table";
        } else if (step.find("Create Minute Child Table") != std::string::npos) {
            job_key = "create-minute-child-table";
        } else if (step.find("Insert Second-Level Data") != std::string::npos) {
            job_key = "insert-second-data";
        } else if (step.find("Insert Minute-Level Data") != std::string::npos) {
            job_key = "insert-minute-data";
        } else if (step.find("Query Super Table") != std::string::npos) {
            job_key = "query-super-table";
        } else if (step.find("Subscribe Data") != std::string::npos) {
            job_key = "subscribe-data";
        }

        // Check if dependencies are completed
        if (dependencies.find(job_key) != dependencies.end()) {
            for (const auto& dependency : dependencies.at(job_key)) {
                std::cout << "Validating that " << job_key << " depends on " << dependency << std::endl;
                assert(completed_jobs.find(dependency) != completed_jobs.end() && "Dependency not satisfied");
            }
        }

        // Mark current job as completed
        completed_jobs.insert(job_key);
    }
}

void test_job_scheduler_with_order() {
    // Build complex config data
    auto result = build_complex_config_with_dependencies();
    const ConfigData& config = result.config;
    const auto& dependencies = result.dependencies;

    // Capture output
    std::ostringstream output_buffer;
    std::streambuf* original_cout = std::cout.rdbuf(); // Save original buffer
    std::cout.rdbuf(output_buffer.rdbuf());            // Redirect std::cout

    // Create scheduler using create_for_testing factory method
    auto scheduler = JobScheduler::create_for_testing(config);

    // Run scheduler
    bool success = scheduler->run();
    (void)success;
    assert(success);

    // Restore std::cout
    std::cout.rdbuf(original_cout);

    // Get output content
    std::string output = output_buffer.str();

    // Parse step order from output
    std::vector<std::string> actual_order;
    std::istringstream output_stream(output);
    std::string line;
    while (std::getline(output_stream, line)) {
        if (line.find("Executing step:") != std::string::npos) {
            actual_order.push_back(line);
        }
    }

    // Validate step order according to dependencies
    validate_execution_order(actual_order, dependencies);

    // Print test passed info
    std::cout << "test_job_scheduler_with_order passed!" << std::endl;
}

void test_job_scheduler_with_delay() {

    // Debug strategy: print job and step execution order
    class DelayStepStrategy : public StepExecutionStrategy {
    public:
        DelayStepStrategy(const GlobalConfig& global) : StepExecutionStrategy(global) {}

        bool execute(const Step& step) override {
            static std::mutex log_mutex;

            // Print debug info
            {
                std::lock_guard<std::mutex> lock(log_mutex);
                std::cout << "Executing step: " << step.name << " (" << step.uses << ")" << std::endl;
            }

            if (step.uses == "tdengine/create-database") {
                {
                    std::lock_guard<std::mutex> lock(log_mutex);
                    std::cout << "Action type: Create Database" << std::endl;
                }
                std::this_thread::sleep_for(std::chrono::seconds(7));
            } else if (step.uses == "tdengine/create-super-table") {
                {
                    std::lock_guard<std::mutex> lock(log_mutex);
                    std::cout << "Action type: Create Super Table" << std::endl;
                }
                std::this_thread::sleep_for(std::chrono::seconds(6));
            } else if (step.uses == "tdengine/create-child-table") {
                {
                    std::lock_guard<std::mutex> lock(log_mutex);
                    std::cout << "Action type: Create Child Table" << std::endl;
                }
                if (step.name == "Create Second Child Table") {
                    std::this_thread::sleep_for(std::chrono::seconds(5));
                } else if (step.name == "Create Minute Child Table") {
                    std::this_thread::sleep_for(std::chrono::seconds(4));
                }
                std::this_thread::sleep_for(std::chrono::seconds(3));
            } else if (step.uses == "tdengine/insert") {
                {
                    std::lock_guard<std::mutex> lock(log_mutex);
                    std::cout << "Action type: Insert Data" << std::endl;
                }
                if (step.name == "Insert Second-Level Data") {
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                } else if (step.name == "Insert Minute-Level Data") {
                    std::this_thread::sleep_for(std::chrono::seconds(3));
                }
                std::this_thread::sleep_for(std::chrono::seconds(3));
            } else if (step.uses == "tdengine/query") {
                {
                    std::lock_guard<std::mutex> lock(log_mutex);
                    std::cout << "Action type: Query Data" << std::endl;
                }
            } else if (step.uses == "tdengine/subscribe") {
                {
                    std::lock_guard<std::mutex> lock(log_mutex);
                    std::cout << "Action type: Subscribe Data" << std::endl;
                }
                std::this_thread::sleep_for(std::chrono::seconds(2));
            } else {
                {
                    std::lock_guard<std::mutex> lock(log_mutex);
                    std::cerr << "Unknown action type: " << step.uses << std::endl;
                }
                throw std::runtime_error("Unknown action type: " + step.uses);
            }

            {
                std::lock_guard<std::mutex> lock(log_mutex);
                std::cout << "Step completed: " << step.name << std::endl;
            }
            return true;
        }
    };

    // Build complex config data
    auto result = build_complex_config_with_dependencies();
    const ConfigData& config = result.config;
    const auto& dependencies = result.dependencies;

    // Capture output
    std::ostringstream output_buffer;
    std::streambuf* original_cout = std::cout.rdbuf(); // Save original buffer
    std::cout.rdbuf(output_buffer.rdbuf());            // Redirect std::cout


    // Use debug strategy
    auto delay_strategy = std::make_unique<DelayStepStrategy>(config.global);

    // Create scheduler
    JobScheduler scheduler(config, std::move(delay_strategy));

    // Run scheduler
    bool success = scheduler.run();
    (void)success;
    assert(success);

    // Restore std::cout
    std::cout.rdbuf(original_cout);

    // Get output content
    std::string output = output_buffer.str();
    std::cout << output << std::endl;

    // Parse step order from output
    std::vector<std::string> actual_order;
    std::istringstream output_stream(output);
    std::string line;
    while (std::getline(output_stream, line)) {
        if (line.find("Executing step:") != std::string::npos) {
            actual_order.push_back(line);
        }
    }

    // Validate step order according to dependencies
    validate_execution_order(actual_order, dependencies);

    // Print test passed info
    std::cout << "test_job_scheduler_with_delay passed!" << std::endl;
}

int main() {
    test_job_scheduler_base();
    test_job_scheduler_with_order();
    test_job_scheduler_with_delay();
    return 0;
}