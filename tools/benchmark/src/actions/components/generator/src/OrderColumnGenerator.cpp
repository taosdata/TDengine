#include "OrderColumnGenerator.h"
#include <stdexcept>


OrderColumnGenerator::OrderColumnGenerator(const ColumnConfigInstance& instance) 
    : ColumnGenerator(instance), current_(0)
{
    if (!instance.config().order_min) {
        throw std::runtime_error("Missing order_min for order column");
    }
    current_ = *instance.config().order_min;
    
    if (instance.config().order_max && *instance.config().order_max <= current_) {
        throw std::runtime_error("Invalid order range");
    }
}

ColumnType OrderColumnGenerator::generate() const {
    if (instance_.config().order_max && current_ >= *instance_.config().order_max) {
        current_ = *instance_.config().order_min;
    }
    
    int64_t value = current_;
    current_++;
    return value;
}

ColumnTypeVector OrderColumnGenerator::generate(size_t count) const {
    ColumnTypeVector values;
    values.reserve(count);

    for (size_t i = 0; i < count; ++i) {
        if (instance_.config().order_max && current_ > *instance_.config().order_max) {
            throw std::runtime_error("Order column value exceeded max");
        }
        
        values.push_back(current_);
        current_++;
    }

    return values;
}