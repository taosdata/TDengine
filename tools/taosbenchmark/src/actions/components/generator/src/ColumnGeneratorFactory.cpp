
#include "ColumnGeneratorFactory.h"
#include <stdexcept>
#include "RandomColumnGenerator.h"
#include "OrderColumnGenerator.h"
#include "FunctionColumnGenerator.h"


std::unique_ptr<ColumnGenerator> ColumnGeneratorFactory::create(const ColumnConfig& config) {
    if (!config.gen_type) return nullptr;
    
    const std::string& gen_type = *config.gen_type;
    
    if (gen_type == "random") {
        return std::make_unique<RandomColumnGenerator>(config);
    } 
    else if (gen_type == "order") {
        return std::make_unique<OrderColumnGenerator>(config);
    }
    else if (gen_type == "function") {
        return std::make_unique<FunctionColumnGenerator>(config);
    }
    
    throw std::runtime_error("Unsupported generator type: " + gen_type);
}