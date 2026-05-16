#include "SubscribeDataAction.hpp"
#include <cassert>
#include <iostream>

SubscribeDataConfig create_test_subscribe_config() {
    SubscribeDataConfig cfg;
    cfg.control.subscribe_control.topics.push_back({ "topic1", "select * from st" });
    cfg.control.subscribe_control.topics.push_back({ "topic2", "select count(*) from st" });
    return cfg;
}

void test_subscribe_execute() {
    GlobalConfig global;
    auto cfg = create_test_subscribe_config();

    try {
        SubscribeDataAction action(global, cfg);
        action.execute();
        std::cout << "test_subscribe_execute passed\n";
    } catch (...) {
        assert(false && "SubscribeDataAction::execute should not throw");
    }
}

int main() {
    test_subscribe_execute();
    std::cout << "All SubscribeDataAction tests passed.\n";
    return 0;
}