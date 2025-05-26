#pragma once
#include <stdexcept>
#include "IFormatter.h"

class IDatabaseFormatter : public IFormatter {
public:
    virtual ~IDatabaseFormatter() = default;
    
    FormatResult format(const CreateDatabaseConfig&, bool is_drop) const override {
        throw std::logic_error("Unsupported operation for database formatter");
    }
};
