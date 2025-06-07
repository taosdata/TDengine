#include "FunctionColumnGenerator.h"
#include <cmath>
#include <random>
#include <stdexcept>


static thread_local std::mt19937_64 random_engine(std::random_device{}());

ColumnType FunctionColumnGenerator::generate() const {
    if (!instance_.config().function_config) {
        throw std::runtime_error("Missing function config for function column");
    }
    const auto& func = *instance_.config().function_config;
    
    if (func.function == "sinusoid") {
        if (!func.period || !func.offset) {
            throw std::runtime_error("Missing period/offset for sinusoid function");
        }
        
        double value = func.base + 
                       func.multiple * std::sin(2 * M_PI * (counter_ + *func.offset) / *func.period);
        
        if (func.random > 0) {
            std::uniform_real_distribution<double> dist(-func.random, func.random);
            value += dist(random_engine);
        }
        
        counter_++;
        return value;
    }
    else if (func.function == "counter") {
        double value = func.base + func.multiple * counter_ + func.addend;
        counter_++;
        return value;
    }

    
    throw std::runtime_error("Unsupported function: " + func.function);
}

std::vector<ColumnType> FunctionColumnGenerator::generate(size_t count) const {
    std::vector<ColumnType> values;
    values.reserve(count);
    
    for (size_t i = 0; i < count; ++i) {
        values.push_back(generate());
    }
    
    return values;
}