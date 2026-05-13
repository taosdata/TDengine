#include "Barrier.hpp"
#include <cassert>
#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>

void test_barrier_basic_functionality() {
    const size_t num_threads = 5;
    Barrier phase_barrier(num_threads);
    Barrier check_barrier(num_threads);
    std::atomic<size_t> counter{0};

    std::vector<std::thread> threads;
    for (size_t i = 0; i < num_threads; ++i) {
        threads.emplace_back([&]() {
            // Phase 1: Calculation
            counter++;

            // Wait for all threads to finish calculation
            phase_barrier.arrive_and_wait();

            // Phase 2: Assertion (Safe Zone)
            assert(counter == num_threads);

            // Wait for all threads to finish assertion
            check_barrier.arrive_and_wait();
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    assert(counter == num_threads);
    std::cout << "test_barrier_basic_functionality passed" << std::endl;
}

void test_barrier_reusability() {
    const size_t num_threads = 3;
    Barrier phase_barrier(num_threads);
    Barrier check_barrier(num_threads);
    std::atomic<size_t> counter{0};

    std::vector<std::thread> threads;
    for (size_t i = 0; i < num_threads; ++i) {
        threads.emplace_back([&]() {
            // First use of the barrier
            counter++;
            phase_barrier.arrive_and_wait();

            assert(counter == num_threads);
            check_barrier.arrive_and_wait();

            // Second use of the barrier
            counter++;
            phase_barrier.arrive_and_wait();

            assert(counter == num_threads * 2);
            check_barrier.arrive_and_wait();
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    assert(counter == num_threads * 2);
    std::cout << "test_barrier_reusability passed" << std::endl;
}

void test_barrier_single_thread() {
    Barrier barrier(1);
    std::atomic<bool> passed{false};

    std::thread t([&]() {
        barrier.arrive_and_wait();
        passed = true;
    });

    t.join();

    assert(passed == true);
    std::cout << "test_barrier_single_thread passed" << std::endl;
}

void test_barrier_different_arrival_times() {
    const size_t num_threads = 4;
    Barrier barrier(num_threads);
    std::atomic<bool> all_passed{true};

    std::vector<std::thread> threads;
    for (size_t i = 0; i < num_threads; ++i) {
        threads.emplace_back([i, &barrier, &all_passed]() {
            // Each thread sleeps for a different amount of time
            std::this_thread::sleep_for(std::chrono::milliseconds(i * 10));

            auto start_time = std::chrono::steady_clock::now();
            barrier.arrive_and_wait();
            auto end_time = std::chrono::steady_clock::now();

            // The first thread to arrive should wait the longest
            if (i == 0) {
                auto wait_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
                if (wait_duration.count() < 25) { // (4-1)*10 - some buffer
                    all_passed = false;
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    assert(all_passed == true);
    std::cout << "test_barrier_different_arrival_times passed" << std::endl;
}

int main() {
    test_barrier_basic_functionality();
    test_barrier_reusability();
    test_barrier_single_thread();
    test_barrier_different_arrival_times();

    std::cout << "All Barrier tests passed!" << std::endl;
    return 0;
}