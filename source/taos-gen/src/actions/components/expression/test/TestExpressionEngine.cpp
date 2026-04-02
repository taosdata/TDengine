#include "ExpressionEngine.hpp"
#include "RegistryFunctions.hpp"
#include <cassert>
#include <cmath>
#include <iostream>
#include <string>
#include <variant>

void test_evaluate_bool_expression() {
    // Lua expression returns true
    ExpressionEngine engine("true");
    auto result = engine.evaluate();
    assert(std::holds_alternative<bool>(result));
    assert(std::get<bool>(result) == true);

    // Lua expression returns false
    ExpressionEngine engine2("false");
    auto result2 = engine2.evaluate();
    assert(std::holds_alternative<bool>(result2));
    assert(std::get<bool>(result2) == false);

    // Lua expression returns result of comparison
    ExpressionEngine engine3("1 > 2");
    auto result3 = engine3.evaluate();
    assert(std::holds_alternative<bool>(result3));
    assert(std::get<bool>(result3) == false);

    std::cout << "test_evaluate_bool_expression passed.\n";
}

void test_evaluate_square_wave() {
    // Test Lua expression using registered square function
    ExpressionEngine engine("square(1, 10, 4, 0)"); // min=1, max=10, period=4, offset=0
    auto result = engine.evaluate();
    assert(std::holds_alternative<double>(result));
    double value = std::get<double>(result);
    (void)value;
    assert(value == 10.0);
    std::cout << "test_evaluate_square_wave passed.\n";
}

void test_evaluate_random_ipv4() {
    // Test Lua expression using registered rand_ipv4 function
    ExpressionEngine engine("rand_ipv4()");
    auto result = engine.evaluate();
    assert(std::holds_alternative<std::string>(result));
    std::string ip = std::get<std::string>(result);
    // Check IPv4 format: should contain three dots
    size_t dot1 = ip.find('.');
    size_t dot2 = ip.find('.', dot1 + 1);
    size_t dot3 = ip.find('.', dot2 + 1);
    (void)dot3;
    assert(dot1 != std::string::npos);
    assert(dot2 != std::string::npos);
    assert(dot3 != std::string::npos);
    std::cout << "test_evaluate_random_ipv4 passed.\n";
}

void test_evaluate_mixed_expression() {
    // Test mixed arithmetic expression
    ExpressionEngine engine("square(0, 1, 100, 25) * 2 + math.sin(_i/10)");
    auto result = engine.evaluate();
    assert(std::holds_alternative<double>(result));
    double value = std::get<double>(result);
    // square(0,1,100,25) == 1, _i==0, so 1*2 + sin(0.0)
    double expected = 2.0 + std::sin(0.0);
    (void)value;
    (void)expected;
    assert(std::abs(value - expected) < 1e-6);
    std::cout << "test_evaluate_mixed_expression passed.\n";
}

void test_evaluate_table_env() {
    // Test the availability of the _table environment variable
    std::string table_name = "my_table";
    ExpressionEngine engine(table_name, "_table");
    auto result = engine.evaluate();
    assert(std::holds_alternative<std::string>(result));
    assert(std::get<std::string>(result) == table_name);

    // When the table name is empty, _table should be an empty string
    ExpressionEngine engine2("", "_table");
    auto result2 = engine2.evaluate();
    assert(std::holds_alternative<std::string>(result2));
    assert(std::get<std::string>(result2).empty());

    std::cout << "test_evaluate_table_env passed.\n";
}

void test_evaluate_last_value_env() {
    // Test the _last environment variable for numeric expressions
    ExpressionEngine engine("(_last or 0) + 1");
    // First call: _last is 0 (default), so result should be 1
    auto result1 = engine.evaluate();
    assert(std::holds_alternative<double>(result1));
    assert(std::get<double>(result1) == 1.0);

    // Second call: _last is 1, so result should be 2
    auto result2 = engine.evaluate();
    assert(std::holds_alternative<double>(result2));
    assert(std::get<double>(result2) == 2.0);

    // Third call: _last is 2, so result should be 3
    auto result3 = engine.evaluate();
    assert(std::holds_alternative<double>(result3));
    assert(std::get<double>(result3) == 3.0);

    std::cout << "test_evaluate_last_value_env passed.\n";
}

int main() {
    test_evaluate_bool_expression();
    test_evaluate_square_wave();
    test_evaluate_random_ipv4();
    test_evaluate_mixed_expression();
    test_evaluate_table_env();
    test_evaluate_last_value_env();
    std::cout << "All ExpressionEngine tests passed.\n";
    return 0;
}