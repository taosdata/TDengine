#include "ExpressionEngine.hpp"
#include "RegistryFunctions.hpp"
#include <stdexcept>
#include <sstream>
#include <cassert>

// Explicitly register all modules
void register_all_custom_modules() {
    auto& registry = FunctionRegistry::instance();
    registry.register_module("MathFunctions", register_MathFunctions);
    registry.register_module("NetworkFunctions", register_NetworkFunctions);
    // Add more modules...
}

// --------------------------
// ThreadLocalContext implementation
// --------------------------
ExpressionEngine::ThreadLocalContext::ThreadLocalContext() {
    static std::once_flag once;
    std::call_once(once, register_all_custom_modules);

    // Create Lua VM
    lua_vm = luaL_newstate();
    if (!lua_vm) throw std::runtime_error("Failed to create Lua state");

    // Open base libraries
    luaL_openlibs(lua_vm);

    // Register all custom functions
    FunctionRegistry::instance().register_all(lua_vm);
}

ExpressionEngine::ThreadLocalContext::~ThreadLocalContext() {
    if (lua_vm) {
        // Release all cached function references
        for (auto& [_, ref] : template_cache) {
            (void)_;
            if (ref != LUA_NOREF) {
                luaL_unref(lua_vm, LUA_REGISTRYINDEX, ref);
            }
        }

        lua_close(lua_vm);
    }
}

// --------------------------
// Get thread-local context
// --------------------------
ExpressionEngine::ThreadLocalContext& ExpressionEngine::get_thread_context() {
    // Thread-local storage
    thread_local ThreadLocalContext context;
    return context;
}

// --------------------------
// Template management
// --------------------------
std::shared_ptr<ExpressionEngine::ExpressionTemplate>
ExpressionEngine::get_template(const std::string& expression, ThreadLocalContext& context) {
    // Find template in local context
    auto& cache = context.template_cache;
    auto it = cache.find(expression);

    if (it != cache.end()) {
        // Template already exists
        return std::make_shared<ExpressionTemplate>(ExpressionTemplate{
            expression,
            it->second
        });
    }

    // Compile new expression
    lua_State* L = context.lua_vm;
    std::string full_expr = "return " + expression;

    if (luaL_loadstring(L, full_expr.c_str())) {
        std::string err = lua_tostring(L, -1);
        lua_pop(L, 1);
        throw std::runtime_error("Compile error: " + err);
    }

    // Save function reference
    int ref = luaL_ref(L, LUA_REGISTRYINDEX);

    // Update cache
    cache[expression] = ref;

    // Create template object
    return std::make_shared<ExpressionTemplate>(ExpressionTemplate{
        expression,
        ref
    });
}

// --------------------------
// ExpressionEngine implementation
// --------------------------
ExpressionEngine::ExpressionEngine(const std::string& table_name, const std::string& expression) {
    // Get thread-local context
    auto& context = get_thread_context();

    // Get or create expression template
    auto template_ = get_template(expression, context);

    // Create execution state
    state_ = std::make_unique<ExpressionState>(template_, table_name);
}

ExpressionEngine::ExpressionEngine(const std::string& expression)
    : ExpressionEngine("", expression) {}

ExpressionEngine::Result ExpressionEngine::evaluate() {
    // if (!state_ || !state_->template_) {
    //     throw std::runtime_error("Expression engine not initialized");
    // }

    // Get thread-local context
    auto& context = get_thread_context();
    lua_State* L = context.lua_vm;

    // Update call count
    const int call_index = state_->call_index++;

    // Inject call count
    lua_pushinteger(L, call_index);
    lua_setglobal(L, "_i");

    // Set table name
    lua_pushstring(L, state_->table_name.c_str());
    lua_setglobal(L, "_table");

    // Set last value
    lua_pushnumber(L, state_->last_value);
    lua_setglobal(L, "_last");

    // Get precompiled function
    lua_rawgeti(L, LUA_REGISTRYINDEX, state_->template_->function_ref);

    // Execute function
    if (lua_pcall(L, 0, 1, 0)) {
        std::string err = lua_tostring(L, -1);
        lua_pop(L, 1);
        throw std::runtime_error("Runtime error: " + err);
    }

    // Handle return value
    Result result;
    if (lua_isnumber(L, -1)) {
        auto number = lua_tonumber(L, -1);
        state_->last_value = number;
        result = number;
    } else if (lua_isstring(L, -1)) {
        const char* str = lua_tostring(L, -1);
        result = std::string(str);
    } else if (lua_isboolean(L, -1)) {
        result = static_cast<bool>(lua_toboolean(L, -1));
    } else {
        lua_pop(L, 1);
        throw std::runtime_error("Invalid return type");
    }

    lua_pop(L, 1);
    return result;
}