#include "JobScheduler.h"
#include <iostream>
#include <sstream>
#include <cassert>
#include <vector>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>


struct ConfigWithDependencies {
    ConfigData config;
    std::unordered_map<std::string, std::unordered_set<std::string>> dependencies;
};

// 构建复杂的配置数据和依赖关系图
ConfigWithDependencies build_complex_config_with_dependencies() {
    ConfigData config;
    config.concurrency = 3;

    // 定义步骤
    Step create_database_step{"Create Database", "actions/create-database", YAML::Node(), {}};
    Step create_super_table_step{"Create Super Table", "actions/create-super-table", YAML::Node(), {}};
    Step create_second_child_table_step{"Create Second Child Table", "actions/create-child-table", YAML::Node(), {}};
    Step create_minute_child_table_step{"Create Minute Child Table", "actions/create-child-table", YAML::Node(), {}};
    Step insert_second_data_step{"Insert Second-Level Data", "actions/insert-data", YAML::Node(), {}};
    Step insert_minute_data_step{"Insert Minute-Level Data", "actions/insert-data", YAML::Node(), {}};
    Step query_super_table_step{"Query Super Table", "actions/query-data", YAML::Node(), {}};
    Step subscribe_data_step{"Subscribe Data", "actions/subscribe-data", YAML::Node(), {}};

    // 定义作业
    Job create_database_job{"create-database", "Create Database", {}, {create_database_step}};
    Job create_super_table_job{"create-super-table", "Create Super Table", {"create-database"}, {create_super_table_step}};
    Job create_second_child_table_job{"create-second-child-table", "Create Second Child Table", {"create-super-table"}, {create_second_child_table_step}};
    Job create_minute_child_table_job{"create-minute-child-table", "Create Minute Child Table", {"create-super-table"}, {create_minute_child_table_step}};
    Job insert_second_data_job{"insert-second-data", "Insert Second-Level Data", {"create-second-child-table"}, {insert_second_data_step}};
    Job insert_minute_data_job{"insert-minute-data", "Insert Minute-Level Data", {"create-minute-child-table"}, {insert_minute_data_step}};
    Job query_super_table_job{"query-super-table", "Query Super Table", {"create-second-child-table", "create-minute-child-table"}, {query_super_table_step}};
    Job subscribe_data_job{"subscribe-data", "Subscribe Data", {"create-second-child-table", "create-minute-child-table"}, {subscribe_data_step}};

    // 添加作业到配置
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

    // 构建依赖关系图
    std::unordered_map<std::string, std::unordered_set<std::string>> dependencies;
    for (const auto& job : config.jobs) {
        dependencies[job.key] = std::unordered_set<std::string>(job.needs.begin(), job.needs.end());
    }

    return {config, dependencies};
}



void test_job_scheduler_base() {
    // 构建复杂的配置数据
    auto result = build_complex_config_with_dependencies();
    const ConfigData& config = result.config;
    const auto& dependencies = result.dependencies;

    // 使用 create_for_testing 工厂方法创建调度器
    auto scheduler = JobScheduler::create_for_testing(config);

    // 运行调度器
    scheduler->run();

    // 打印测试通过信息
    std::cout << "test_job_scheduler_base passed!" << std::endl;
}


void validate_execution_order(const std::vector<std::string>& actual_order, 
                              const std::unordered_map<std::string, std::unordered_set<std::string>>& dependencies) {
    // 已完成的作业集合
    std::unordered_set<std::string> completed_jobs;

    // 验证步骤顺序是否符合依赖关系
    for (const auto& step : actual_order) {
        // 提取作业 key
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

        // 检查依赖是否已完成
        if (dependencies.find(job_key) != dependencies.end()) {
            for (const auto& dependency : dependencies.at(job_key)) {
                assert(completed_jobs.find(dependency) != completed_jobs.end() && "Dependency not satisfied");
            }
        }

        // 将当前作业标记为已完成
        completed_jobs.insert(job_key);
    }
}



void test_job_scheduler_with_order() {
    // 构建复杂的配置数据
    auto result = build_complex_config_with_dependencies();
    const ConfigData& config = result.config;
    const auto& dependencies = result.dependencies;

    // 捕获输出
    std::ostringstream output_buffer;
    std::streambuf* original_cout = std::cout.rdbuf(); // 保存原始缓冲区
    std::cout.rdbuf(output_buffer.rdbuf());            // 重定向 std::cout

    // 使用 create_for_testing 工厂方法创建调度器
    auto scheduler = JobScheduler::create_for_testing(config);

    // 运行调度器
    scheduler->run();

    // 恢复 std::cout
    std::cout.rdbuf(original_cout);

    // 获取输出内容
    std::string output = output_buffer.str();

    // 解析输出的步骤顺序
    std::vector<std::string> actual_order;
    std::istringstream output_stream(output);
    std::string line;
    while (std::getline(output_stream, line)) {
        if (line.find("Executing step:") != std::string::npos) {
            actual_order.push_back(line);
        }
    }

    // 验证步骤顺序是否符合依赖关系
    validate_execution_order(actual_order, dependencies);

    // 打印测试通过信息
    std::cout << "test_job_scheduler_with_order passed!" << std::endl;
}



void test_job_scheduler_with_delay() {

    // 调试策略：打印作业和步骤的执行顺序
    class DelayStepStrategy : public StepExecutionStrategy {
    public:
        void execute(const Step& step) override {
            // 打印调试信息
            std::cout << "Executing step: " << step.name << " (" << step.uses << ")" << std::endl;
        
            if (step.uses == "actions/create-database") {
                std::cout << "Action type: Create Database" << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(7));
            } else if (step.uses == "actions/create-super-table") {
                std::cout << "Action type: Create Super Table" << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(6));
            } else if (step.uses == "actions/create-child-table") {
                std::cout << "Action type: Create Child Table" << std::endl;
                if (step.name == "Create Second Child Table") {
                    std::this_thread::sleep_for(std::chrono::seconds(5));
                } else if (step.name == "Create Minute Child Table") {
                    std::this_thread::sleep_for(std::chrono::seconds(4));
                }
                std::this_thread::sleep_for(std::chrono::seconds(3));
            } else if (step.uses == "actions/insert-data") {
                std::cout << "Action type: Insert Data" << std::endl;
                if (step.name == "Insert Second-Level Data") {
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                } else if (step.name == "Insert Minute-Level Data") {
                    std::this_thread::sleep_for(std::chrono::seconds(3));
                }
                std::this_thread::sleep_for(std::chrono::seconds(3));
            } else if (step.uses == "actions/query-data") {
                std::cout << "Action type: Query Data" << std::endl;
            } else if (step.uses == "actions/subscribe-data") {
                std::cout << "Action type: Subscribe Data" << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(2));
            } else {
                std::cerr << "Unknown action type: " << step.uses << std::endl;
                throw std::runtime_error("Unknown action type: " + step.uses);
            }

            std::cout << "Step completed: " << step.name << std::endl;        }
    };


    // 构建复杂的配置数据
    auto result = build_complex_config_with_dependencies();
    const ConfigData& config = result.config;
    const auto& dependencies = result.dependencies;

    // 捕获输出
    std::ostringstream output_buffer;
    std::streambuf* original_cout = std::cout.rdbuf(); // 保存原始缓冲区
    std::cout.rdbuf(output_buffer.rdbuf());            // 重定向 std::cout


    // 使用调试策略
    auto delay_strategy = std::make_unique<DelayStepStrategy>();

    // 创建调度器
    JobScheduler scheduler(config, std::move(delay_strategy));

    // 运行调度器
    scheduler.run();

    // 恢复 std::cout
    std::cout.rdbuf(original_cout);

    // 获取输出内容
    std::string output = output_buffer.str();
    std::cout << output << std::endl;

    // 解析输出的步骤顺序
    std::vector<std::string> actual_order;
    std::istringstream output_stream(output);
    std::string line;
    while (std::getline(output_stream, line)) {
        if (line.find("Executing step:") != std::string::npos) {
            actual_order.push_back(line);
        }
    }

    // 验证步骤顺序是否符合依赖关系
    validate_execution_order(actual_order, dependencies);

    // 打印测试通过信息
    std::cout << "test_job_scheduler_with_delay passed!" << std::endl;
}


int main() {
    // 测试调度框架
    test_job_scheduler_base();
    test_job_scheduler_with_order();
    test_job_scheduler_with_delay();
    return 0;
}