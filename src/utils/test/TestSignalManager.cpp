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

#if !defined(_WIN32)
void test_signal_manager_basic() {
    callback_count = 0;
    final_called = false;

    SignalManager::register_signal(SIGUSR1, test_normal_callback);
    SignalManager::register_signal(SIGUSR1, [](int){ callback_count++; });
    SignalManager::register_signal(SIGUSR1, test_final_callback, true);
    SignalManager::setup();

    std::raise(SIGUSR1);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    assert(callback_count == 2);
    assert(final_called == true);
    std::cout << "test_signal_manager_basic passed" << std::endl;
}

void test_signal_manager_order() {
    callback_count = 0;
    final_called = false;

    SignalManager::register_signal(SIGUSR2, [] (int) { callback_count += 10; });
    SignalManager::register_signal(SIGUSR2, [] (int) { callback_count += 100; });
    SignalManager::register_signal(SIGUSR2, [] (int) { final_called = true; }, true);
    SignalManager::setup();

    std::raise(SIGUSR2);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    assert(callback_count == 110);
    assert(final_called == true);
    std::cout << "test_signal_manager_order passed" << std::endl;
}
#else
// Windows does not support SIGUSR1/SIGUSR2, use SIGINT for basic testing
void test_signal_manager_basic() {
    std::cout << "test_signal_manager_basic skipped on Windows (no SIGUSR1/SIGUSR2)" << std::endl;
}

void test_signal_manager_order() {
    std::cout << "test_signal_manager_order skipped on Windows (no SIGUSR1/SIGUSR2)" << std::endl;
}
#endif

int main() {
    test_signal_manager_basic();
    test_signal_manager_order();

    std::cout << "All SignalManager tests passed!" << std::endl;
    return 0;
}
