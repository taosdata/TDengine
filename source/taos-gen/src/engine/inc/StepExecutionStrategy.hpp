#pragma once

#include "Step.hpp"
#include "GlobalConfig.hpp"


// Abstract base class: step execution strategy
class StepExecutionStrategy {
public:
    StepExecutionStrategy(const GlobalConfig& global) : global_(global) {}

    virtual ~StepExecutionStrategy() = default;

    // Interface for executing a step
    virtual bool execute(const Step& step) = 0;

protected:
    const GlobalConfig& global_;
};

// Production environment strategy
class ProductionStepStrategy : public StepExecutionStrategy {
public:
    ProductionStepStrategy(const GlobalConfig& global) : StepExecutionStrategy(global) {}

    bool execute(const Step& step) override;
};

// Debug environment strategy
class DebugStepStrategy : public StepExecutionStrategy {
public:
    DebugStepStrategy(const GlobalConfig& global) : StepExecutionStrategy(global) {}

    bool execute(const Step& step) override;
};