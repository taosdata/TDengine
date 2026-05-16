#pragma once
#include <memory>
#include <mutex>
#include <unordered_map>
#include <functional>
#include <stdexcept>
#include "IFormatter.hpp"
#include "ActionConfigVariant.hpp"

template<typename T>
struct FormatterTraits;

template<>
struct FormatterTraits<CreateDatabaseConfig> {
    using Interface = IDatabaseFormatter;
    static constexpr const char* prefix = "database";
};

template<>
struct FormatterTraits<CreateSuperTableConfig> {
    using Interface = ISuperTableFormatter;
    static constexpr const char* prefix = "supertable";
};

template<>
struct FormatterTraits<CreateChildTableConfig> {
    using Interface = IChildTableFormatter;
    static constexpr const char* prefix = "childtable";
};

template<>
struct FormatterTraits<InsertDataConfig> {
    using Interface = IInsertDataFormatter;
    static constexpr const char* prefix = "insert";
};


class FormatterFactory {
public:
    template<typename T>
    using CreatorType = std::function<std::unique_ptr<typename FormatterTraits<T>::Interface>(const DataFormat&)>;
    using FormatterCreator = std::function<std::unique_ptr<IFormatter>(const DataFormat&)>;

    static FormatterFactory& instance() {
        static FormatterFactory factory;
        return factory;
    }


    template<typename ActionConfig>
    static void register_formatter(const std::string& format_type, CreatorType<ActionConfig> creator) {
        auto& inst = instance();

        const std::string key = std::string(FormatterTraits<ActionConfig>::prefix) + "." + format_type;

        auto adapter = [creator](const DataFormat& fmt) -> std::unique_ptr<IFormatter> {
            return creator(fmt);
        };

        std::lock_guard<std::mutex> lock(inst.mutex_);
        inst.creators_[key] = std::move(adapter);
    }


    template<typename ActionConfig>
    static std::unique_ptr<typename FormatterTraits<ActionConfig>::Interface> create_formatter(const DataFormat& format) {
        auto base_ptr = create_base_formatter<ActionConfig>(format);

        return std::unique_ptr<typename FormatterTraits<ActionConfig>::Interface>(
            static_cast<typename FormatterTraits<ActionConfig>::Interface*>(base_ptr.release())
        );
    }

private:
    template<typename ActionConfig>
    static std::unique_ptr<IFormatter> create_base_formatter(const DataFormat& format) {
        auto& inst = instance();
        const std::string key = std::string(FormatterTraits<ActionConfig>::prefix) + "." + format.format_type;

        std::lock_guard<std::mutex> lock(inst.mutex_);
        if (auto it = inst.creators_.find(key); it != inst.creators_.end()) {
            return it->second(format);
        }
        throw std::invalid_argument("Unsupported formatter type: " + key);
    }


    std::unordered_map<std::string, FormatterCreator> creators_;
    std::mutex mutex_;
};