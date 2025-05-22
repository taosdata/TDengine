#ifndef STEP_EXECUTION_STRATEGY_H
#define STEP_EXECUTION_STRATEGY_H

#include "Step.h"


// 抽象基类：步骤执行策略
class StepExecutionStrategy {
public:
    virtual ~StepExecutionStrategy() = default;

    // 执行步骤的接口
    virtual void execute(const Step& step) = 0;
};

// 生产环境策略
class ProductionStepStrategy : public StepExecutionStrategy {
public:
    void execute(const Step& step) override;
};

// 调试环境策略
class DebugStepStrategy : public StepExecutionStrategy {
public:
    void execute(const Step& step) override;
};

#endif // STEP_EXECUTION_STRATEGY_H