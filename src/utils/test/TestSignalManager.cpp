#include "SignalManager.hpp"
#include <cassert>
#include <iostream>
#include <atomic>
#include <csignal>
#include <thread>
#include <chrono>

std::atomic<int> callback_count{0};
std::atomic<bool> final_called{false};

void test_normal_callback(int signum) {
    callback_count++;
    std::cout << "Normal callback called for signal: " << signum << std::endl;
}

void test_final_callback(int signum) {
    final_called = true;
    std::cout << "Final callback called for signal: " << signum << std::endl;
}

namespace {
#if defined(_WIN32)
constexpr int SIGNAL_BASIC = SIGINT;
constexpr int SIGNAL_ORDER = SIGINT; // Windows only has a few signals; reuse SIGINT
#else
constexpr int SIGNAL_BASIC = SIGUSR1;
constexpr int SIGNAL_ORDER = SIGUSR2;
#endif
}

void test_signal_manager_basic() {
    callback_count = 0;
    final_called = false;

    SignalManager::register_signal(SIGNAL_BASIC, test_normal_callback);
    SignalManager::register_signal(SIGNAL_BASIC, [](int){ callback_count++; });
    SignalManager::register_signal(SIGNAL_BASIC, test_final_callback, true);
    SignalManager::setup();

    std::raise(SIGNAL_BASIC);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    assert(callback_count == 2);
    assert(final_called == true);
    std::cout << "test_signal_manager_basic passed" << std::endl;
}

void test_signal_manager_order() {
    callback_count = 0;
    final_called = false;

    SignalManager::register_signal(SIGNAL_ORDER, [] (int) { callback_count += 10; });
    SignalManager::register_signal(SIGNAL_ORDER, [] (int) { callback_count += 100; });
    SignalManager::register_signal(SIGNAL_ORDER, [] (int) { final_called = true; }, true);
    SignalManager::setup();

    std::raise(SIGNAL_ORDER);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    assert(callback_count == 110);
    assert(final_called == true);
    std::cout << "test_signal_manager_order passed" << std::endl;
}

int main() {
    test_signal_manager_basic();
    test_signal_manager_order();

    std::cout << "All SignalManager tests passed!" << std::endl;
    return 0;
}
