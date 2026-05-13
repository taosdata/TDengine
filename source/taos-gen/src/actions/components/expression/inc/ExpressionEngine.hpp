#pragma once
#include "ColumnType.hpp"
#include <memory>
#include <string>
#include <variant>
#include <unordered_map>
#include <mutex>
#include <lua.hpp>
#include <thread>

class ExpressionEngine {
public:
    using Result = ColumnType;

    explicit ExpressionEngine(const std::string& expression);
    ExpressionEngine(const std::string& table_name, const std::string& expression);
    ~ExpressionEngine() = default;

    Result evaluate();

    // Disable copy and move
    ExpressionEngine(const ExpressionEngine&) = delete;
    ExpressionEngine& operator=(const ExpressionEngine&) = delete;

private:
    // Thread-local context
    struct ThreadLocalContext {
        lua_State* lua_vm = nullptr;
        std::unordered_map<std::string, int> template_cache;

        ThreadLocalContext();
        ~ThreadLocalContext();
    };

    // Expression template
    struct ExpressionTemplate {
        std::string expression;
        int function_ref = LUA_NOREF;
    };

    // Execution state
    struct ExpressionState {
        std::shared_ptr<ExpressionTemplate> template_;
        int call_index = 0;
        std::string table_name;
        double last_value = 0.0;

        ExpressionState(std::shared_ptr<ExpressionTemplate> tpl, const std::string& tbl_name)
            : template_(std::move(tpl)), call_index(0), table_name(tbl_name) {}
    };

    std::unique_ptr<ExpressionState> state_;

    // Get thread-local context
    static ThreadLocalContext& get_thread_context();

    // Get or create expression template
    std::shared_ptr<ExpressionTemplate> get_template(
        const std::string& expression,
        ThreadLocalContext& context
    );
};