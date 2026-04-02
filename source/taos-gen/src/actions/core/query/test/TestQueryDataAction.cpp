#include "QueryDataAction.hpp"
#include <cassert>
#include <iostream>

QueryDataConfig create_test_query_config() {
    QueryDataConfig cfg;
    cfg.source.connection_info.host = "localhost";
    return cfg;
}

void test_query_execute() {
    GlobalConfig global;
    auto cfg = create_test_query_config();

    try {
        QueryDataAction action(global, cfg);
        action.execute();
        std::cout << "test_query_execute passed\n";
    } catch (...) {
        assert(false && "QueryDataAction::execute should not throw");
    }
}

int main() {
    test_query_execute();
    std::cout << "All QueryDataAction tests passed.\n";
    return 0;
}