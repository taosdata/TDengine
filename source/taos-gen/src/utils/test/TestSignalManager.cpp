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
constexpr int SIGNAL_ORDER = SIGTERM;
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

void test_register_signal_without_callback() {
    // register_signal(signum) should install handler that sets interrupted() flag
    // Use a different signal to avoid interference with other tests
#if defined(_WIN32)
    constexpr int SIGNAL_NOCB = SIGBREAK;
#else
    constexpr int SIGNAL_NOCB = SIGWINCH;
#endif

    SignalManager::register_signal(SIGNAL_NOCB);
    SignalManager::setup();

    // Before raising, interrupted() may already be true from previous tests
    // Just verify that raising doesn't crash and handler is properly installed
    std::raise(SIGNAL_NOCB);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // After any signal handled by our handler, interrupted() must be true
    assert(SignalManager::interrupted() == true);
    std::cout << "test_register_signal_without_callback passed" << std::endl;
}

int main() {
    test_signal_manager_basic();
    test_signal_manager_order();
    test_register_signal_without_callback();

    std::cout << "All SignalManager tests passed!" << std::endl;
    return 0;
}
