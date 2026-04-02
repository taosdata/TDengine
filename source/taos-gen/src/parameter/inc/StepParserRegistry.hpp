#pragma once
#include "Job.hpp"
#include "Step.hpp"
#include <functional>
#include <string>
#include <unordered_map>

class ParameterContext;

class StepParserRegistry {
public:
    using StepParser = std::function<void(ParameterContext&, Job&, Step&)>;

    static StepParserRegistry& instance();

    static void register_parser(const std::string& action, StepParser parser);
    static bool apply(const std::string& action, ParameterContext& ctx, Job& job, Step& step);

private:
    std::unordered_map<std::string, StepParser> parsers_;
};
