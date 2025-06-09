#include "StepExecutionStrategy.h"
#include <iostream>


// 生产环境策略的实现

#include "ActionFactory.h"

void ProductionStepStrategy::execute(const Step& step) {
    try {
        // 打印调试信息
        std::cout << "Executing step: " << step.name << " (" << step.uses << ")" << std::endl;

        auto action = ActionFactory::instance().create_action(step.uses, step.action_config);
        action->execute();

        // 打印调试信息，表示步骤完成
        std::cout << "Step completed: " << step.name << std::endl;

    } catch (const std::exception& e) {
        // 捕获其他标准异常
        std::cerr << "Error executing step: " << step.name << " (" << step.uses << ")" << std::endl;
        std::cerr << "Reason: Exception - " << e.what() << std::endl;
        throw; // 重新抛出异常
    }
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
