#include "Latch.hpp"
#include <cassert>
#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>

void test_latch_basic_functionality() {
    const size_t num_threads = 5;
    Latch latch(num_threads);
    std::atomic<size_t> counter{0};

    std::vector<std::thread> threads;
    for (size_t i = 0; i < num_threads; ++i) {
        threads.emplace_back([&]() {
            // Simulate some work
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            counter++;
            latch.count_down();
        });
    }

    auto start_time = std::chrono::steady_clock::now();
    latch.wait();
    auto end_time = std::chrono::steady_clock::now();

    // The main thread should have waited until all threads finished.
    auto wait_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    assert(wait_duration.count() >= 10);
    assert(counter == num_threads);
    (void)wait_duration;
    (void)counter;
    (void)num_threads;

    for (auto& t : threads) {
        t.join();
    }

    std::cout << "test_latch_basic_functionality passed" << std::endl;
}

void test_latch_wait_with_stop_condition() {
    const size_t num_threads = 5;
    Latch latch(num_threads);
    std::atomic<bool> stop_flag{false};

    std::thread waiter([&]() {
        auto start_time = std::chrono::steady_clock::now();
        latch.wait([&] { return stop_flag.load(); });
        auto end_time = std::chrono::steady_clock::now();

        // This should unblock quickly after stop_flag is set.
        auto wait_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        assert(wait_duration.count() < 50);
        (void)wait_duration;
    });

    // Wait a bit to ensure the waiter thread is actually waiting.
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    // Set the stop condition and notify the waiter.
    stop_flag = true;
    latch.interrupt();

    waiter.join();

    std::cout << "test_latch_wait_with_stop_condition passed" << std::endl;
}

void test_latch_interrupt_functionality() {
    const size_t num_threads = 3;
    Latch latch(num_threads);
    std::atomic<bool> stop_flag{false};
    std::atomic<bool> waiter_finished{false};

    std::thread waiter([&]() {
        latch.wait([&] { return stop_flag.load(); });
        waiter_finished = true;
    });

    // Ensure waiter is waiting
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    assert(waiter_finished == false);

    // Set the stop flag and interrupt the wait
    stop_flag = true;
    latch.interrupt();

    waiter.join();
    assert(waiter_finished == true);

    std::cout << "test_latch_interrupt_functionality passed" << std::endl;
}

void test_latch_zero_count() {
    Latch latch(0);
    std::atomic<bool> passed{false};

    // wait() should return immediately for a zero-count latch.
    latch.wait();
    passed = true;

    assert(passed == true);
    std::cout << "test_latch_zero_count passed" << std::endl;
}

int main() {
    test_latch_basic_functionality();
    test_latch_wait_with_stop_condition();
    test_latch_interrupt_functionality();
    test_latch_zero_count();

    std::cout << "All Latch tests passed!" << std::endl;
    return 0;
}