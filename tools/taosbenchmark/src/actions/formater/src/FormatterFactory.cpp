#include "FormatterFactory.h"
#include <stdexcept>


template<>
void FormatterFactory::register_formatter<CreateDatabaseConfig>(const std::string& format_type, FormatterCreator creator) {
    const std::string key = "database." + format_type;
    std::lock_guard<std::mutex> lock(mutex_);
    creators_[key] = std::move(creator);
}

template<>
std::unique_ptr<IFormatter> FormatterFactory::create_formatter<CreateDatabaseConfig>(const DataFormat& format) {
    const std::string key = "database." + format.format_type;
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = creators_.find(key);
    if (it != creators_.end()) {
        return it->second(format);
    }
    throw std::invalid_argument("Unsupported formatter type: " + format.format_type);
}
