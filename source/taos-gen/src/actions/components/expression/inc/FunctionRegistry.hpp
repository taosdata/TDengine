#pragma once
#include <lua.hpp>
#include <functional>
#include <vector>
#include <string>
#include <unordered_map>

class FunctionRegistry {
public:
    using RegisterFunction = std::function<void(lua_State*)>;

    // Singleton access
    static FunctionRegistry& instance() {
        static FunctionRegistry reg;
        return reg;
    }

    // Explicitly register a module
    void register_module(const std::string& name, RegisterFunction func) {
        modules_[name] = func;
    }

    // Get the registration function for a specific module
    RegisterFunction get_module(const std::string& name) {
        auto it = modules_.find(name);
        if (it != modules_.end()) {
            return it->second;
        }
        return nullptr;
    }

    // Register all modules to the Lua environment
    void register_all(lua_State* L) {
        for (auto& [name, func] : modules_) {
            (void)name;
            func(L);
        }
    }

private:
    std::unordered_map<std::string, RegisterFunction> modules_;
};