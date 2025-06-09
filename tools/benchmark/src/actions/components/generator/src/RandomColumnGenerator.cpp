#include "RandomColumnGenerator.h"
#include <random>
#include <stdexcept>



static thread_local std::mt19937_64 random_engine(std::random_device{}());

ColumnType RandomColumnGenerator::generate() const {
    if (instance_.config().type_tag == ColumnTypeTag::INT) {
        if (!instance_.config().min || !instance_.config().max) {
            throw std::runtime_error("Missing min/max for int column");
        }
        std::uniform_int_distribution<int> dist(*instance_.config().min, *instance_.config().max - 1);
        return dist(random_engine);
    }
    else if (instance_.config().type_tag == ColumnTypeTag::FLOAT) {
        if (!instance_.config().min || !instance_.config().max) {
            throw std::runtime_error("Missing min/max for float column");
        }
        std::uniform_real_distribution<float> dist(*instance_.config().min, *instance_.config().max);
        return dist(random_engine);
    }
    else if (instance_.config().type_tag == ColumnTypeTag::DOUBLE) {
        if (!instance_.config().min || !instance_.config().max) {
            throw std::runtime_error("Missing min/max for double column");
        }
        std::uniform_real_distribution<double> dist(*instance_.config().min, *instance_.config().max);
        return dist(random_engine);
    }
    else if (instance_.config().type_tag == ColumnTypeTag::BOOL) {
        std::bernoulli_distribution dist(0.5);
        return dist(random_engine);
    }
    else if (instance_.config().type_tag == ColumnTypeTag::VARCHAR || 
             instance_.config().type_tag == ColumnTypeTag::BINARY) {    
        if (!instance_.config().corpus) {
            throw std::runtime_error("Missing corpus for string column");
        }
        const auto& corpus = *instance_.config().corpus;
        std::uniform_int_distribution<size_t> dist(0, corpus.size() - 1);
        return std::string(1, corpus[dist(random_engine)]);
    }
    else if (instance_.config().type_tag == ColumnTypeTag::NCHAR) {
        if (!instance_.config().len) {
            throw std::runtime_error("Missing len for nchar column");
        }
        int len = *instance_.config().len;
        std::uniform_int_distribution<char16_t> dist(0x4E00, 0x9FA5); // Unicode range for Chinese characters
        std::u16string result;
        result.reserve(len);
        for (int i = 0; i < len; ++i) {
            result.push_back(dist(random_engine));
        }
        return result;
    }

    throw std::runtime_error("Unsupported column type: " + instance_.config().type);
}

std::vector<ColumnType> RandomColumnGenerator::generate(size_t count) const {
    std::vector<ColumnType> values;
    values.reserve(count);
    
    for (size_t i = 0; i < count; ++i) {
        values.push_back(generate());
    }
    
    return values;
}