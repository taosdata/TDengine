#include "OrderColumnGenerator.h"
#include <stdexcept>


OrderColumnGenerator::OrderColumnGenerator(const ColumnConfig& config) 
    : ColumnGenerator(config), current_(0)
{
    if (!config.order_min) {
        throw std::runtime_error("Missing order_min for order column");
    }
    current_ = *config.order_min;
    
    if (config.order_max && *config.order_max <= current_) {
        throw std::runtime_error("Invalid order range");
    }
}

ColumnType OrderColumnGenerator::generate() const {
    if (config_.order_max && current_ >= *config_.order_max) {
        current_ = *config_.order_min;
    }
    
    int64_t value = current_;
    current_++;
    return value;
}

std::vector<ColumnType> OrderColumnGenerator::generate(size_t count) const {
    std::vector<ColumnType> values;
    values.reserve(count);

    for (size_t i = 0; i < count; ++i) {
        if (config_.order_max && current_ > *config_.order_max) {
            throw std::runtime_error("Order column value exceeded max");
        }
        
        values.push_back(current_);
        current_++;
    }

    return values;
}