#pragma once
#include <memory>
#include <mutex>
#include <unordered_map>
#include <functional>
#include "IFormatter.h"
#include "ActionConfigVariant.h"


class FormatterFactory {
public:
    using FormatterCreator = std::function<std::unique_ptr<IFormatter>(const DataFormat&)>;

    static FormatterFactory& instance() {
        static FormatterFactory factory;
        return factory;
    }

    template<typename ActionConfig>
    void register_formatter(const std::string& format_type, FormatterCreator creator);

    template<typename ActionConfig>
    std::unique_ptr<IFormatter> create_formatter(const DataFormat& format);

private:
    std::unordered_map<std::string, FormatterCreator> creators_;
    std::mutex mutex_;
};