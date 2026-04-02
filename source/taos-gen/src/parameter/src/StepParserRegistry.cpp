#include "StepParserRegistry.hpp"

StepParserRegistry& StepParserRegistry::instance() {
    static StepParserRegistry inst;
    return inst;
}

void StepParserRegistry::register_parser(const std::string& action, StepParser parser) {
    instance().parsers_[action] = std::move(parser);
}

bool StepParserRegistry::apply(const std::string& action, ParameterContext& ctx, Job& job, Step& step) {
    auto& m = instance().parsers_;
    auto it = m.find(action);

    if (it == m.end())
        return false;

    it->second(ctx, job, step);
    return true;
}
