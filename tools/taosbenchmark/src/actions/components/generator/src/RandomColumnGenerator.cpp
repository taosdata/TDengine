#include "RandomColumnGenerator.h"
#include <random>
#include <stdexcept>



static thread_local std::mt19937_64 random_engine(std::random_device{}());

ColumnType RandomColumnGenerator::generate() const {
    if (config_.type == "int") {
        if (!config_.min || !config_.max) {
            throw std::runtime_error("Missing min/max for int column");
        }
        std::uniform_int_distribution<int> dist(*config_.min, *config_.max - 1);
        return dist(random_engine);
    }
    else if (config_.type == "double") {
        if (!config_.min || !config_.max) {
            throw std::runtime_error("Missing min/max for double column");
        }
        std::uniform_real_distribution<double> dist(*config_.min, *config_.max);
        return dist(random_engine);
    }
    else if (config_.type == "bool") {
        std::bernoulli_distribution dist(0.5);
        return dist(random_engine);
    }
    else if (config_.type == "string") {
        if (!config_.corpus) {
            throw std::runtime_error("Missing corpus for string column");
        }
        const auto& corpus = *config_.corpus;
        std::uniform_int_distribution<size_t> dist(0, corpus.size() - 1);
        return std::string(1, corpus[dist(random_engine)]);
    }

    
    throw std::runtime_error("Unsupported column type: " + config_.type);
}

std::vector<ColumnType> RandomColumnGenerator::generate(size_t count) const {
    std::vector<ColumnType> values;
    values.reserve(count);
    
    for (size_t i = 0; i < count; ++i) {
        values.push_back(generate());
    }
    
    return values;
}