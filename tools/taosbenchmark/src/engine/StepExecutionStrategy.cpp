#include "StepExecutionStrategy.h"
#include <iostream>


// 生产环境策略的实现
void ProductionStepStrategy::execute(const Step& step) {
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


// 调试环境策略的实现
void DebugStepStrategy::execute(const Step& step) {
    // 打印调试信息
    std::cout << "Executing step: " << step.name << " (" << step.uses << ")" << std::endl;

    if (step.uses == "actions/create-database") {
        std::cout << "Action type: Create Database" << std::endl;
    } else if (step.uses == "actions/create-super-table") {
        std::cout << "Action type: Create Super Table" << std::endl;
    } else if (step.uses == "actions/create-child-table") {
        std::cout << "Action type: Create Child Table" << std::endl;
    } else if (step.uses == "actions/insert-data") {
        std::cout << "Action type: Insert Data" << std::endl;
    } else if (step.uses == "actions/query-data") {
        std::cout << "Action type: Query Data" << std::endl;
    } else if (step.uses == "actions/subscribe-data") {
        std::cout << "Action type: Subscribe Data" << std::endl;
    } else {
        std::cerr << "Unknown action type: " << step.uses << std::endl;
        throw std::runtime_error("Unknown action type: " + step.uses);
    }

    std::cout << "Step completed: " << step.name << std::endl;
}
