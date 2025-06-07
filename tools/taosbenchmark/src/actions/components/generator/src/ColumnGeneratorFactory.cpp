
#include "ColumnGeneratorFactory.h"
#include <stdexcept>
#include "RandomColumnGenerator.h"
#include "OrderColumnGenerator.h"
#include "FunctionColumnGenerator.h"


std::unique_ptr<ColumnGenerator> ColumnGeneratorFactory::create(const ColumnConfigInstance& instance) {
    if (!instance.config().gen_type) return nullptr;
    
    const std::string& gen_type = *instance.config().gen_type;
    
    if (gen_type == "random") {
        return std::make_unique<RandomColumnGenerator>(instance);
    } 
    else if (gen_type == "order") {
        return std::make_unique<OrderColumnGenerator>(instance);
    }
    else if (gen_type == "function") {
        return std::make_unique<FunctionColumnGenerator>(instance);
    }
    
    throw std::runtime_error("Unsupported generator type: " + gen_type);
}