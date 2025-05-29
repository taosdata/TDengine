#include "TableNameGenerator.h"
#include <sstream>


std::vector<std::string> TableNameGenerator::generate() const {
    std::vector<std::string> names;
    names.reserve(config_.count);
    
    for (int i = config_.from; i < config_.from + config_.count; ++i) {
        std::ostringstream oss;
        oss << config_.prefix << i;
        names.push_back(oss.str());
    }

    return names;
}