#include "ActionMetrics.hpp"
#include <cassert>
#include <iostream>
#include <thread>
#include <vector>

void test_single_thread_metrics() {
    ActionMetrics metrics;
    
    // Add some sample durations
    metrics.add_sample(10.5);  // min
    metrics.add_sample(15.2);
    metrics.add_sample(12.8);
    metrics.add_sample(20.9);  // max
    metrics.add_sample(14.7);

    std::vector<ActionMetrics> others;
    metrics.merge_from(others);
    metrics.calculate();

    // Get metrics summary
    std::string summary = metrics.get_summary();

    // Verify metrics values
    const auto& samples = metrics.get_samples();
    (void)samples;
    assert(samples.size() == 5);
    assert(std::abs(metrics.get_min() - 10.5) < 0.0001);
    assert(std::abs(metrics.get_max() - 20.9) < 0.0001);
    assert(std::abs(metrics.get_avg() - 14.82) < 0.0001);
    assert(std::abs(metrics.get_p90() - 18.62) < 0.0001);
    assert(std::abs(metrics.get_p95() - 19.76) < 0.0001);
    assert(std::abs(metrics.get_p99() - 20.672) < 0.0001);

    std::cout << "test_single_thread_metrics passed.\n";
}

void test_multi_thread_metrics() {
    std::vector<ActionMetrics> thread_metrics(3);
    std::vector<std::thread> threads;

    // Create three threads, each adding different samples
    for (int i = 0; i < 3; ++i) {
        threads.emplace_back([i, &thread_metrics]() {
            for (int j = 0; j < 5; ++j) {
                thread_metrics[i].add_sample(10.0 * i + j);
            }
        });
    }

    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }

    // Merge metrics from all threads
    ActionMetrics combined_metrics;
    combined_metrics.merge_from(thread_metrics);
    combined_metrics.calculate();

    // Verify combined metrics
    assert(combined_metrics.get_samples().size() == 15);
    assert(std::abs(combined_metrics.get_min() - 0.0) < 0.0001);
    assert(std::abs(combined_metrics.get_max() - 24.0) < 0.0001);

    std::cout << "test_multi_thread_metrics passed.\n";
}

void test_metrics_reset() {
    ActionMetrics metrics;
    
    metrics.add_sample(10.0);
    metrics.add_sample(20.0);
    
    assert(metrics.get_samples().size() == 2);
    
    metrics.reset();
    
    assert(metrics.get_samples().empty());
    assert(std::abs(metrics.get_min()) < 0.0001);
    assert(std::abs(metrics.get_max()) < 0.0001);
    assert(std::abs(metrics.get_avg()) < 0.0001);

    std::cout << "test_metrics_reset passed.\n";
}

void test_empty_metrics() {
    ActionMetrics metrics;
    metrics.calculate();
    
    assert(metrics.get_samples().empty());
    assert(std::abs(metrics.get_min()) < 0.0001);
    assert(std::abs(metrics.get_max()) < 0.0001);
    assert(std::abs(metrics.get_avg()) < 0.0001);
    assert(std::abs(metrics.get_p90()) < 0.0001);
    assert(std::abs(metrics.get_p95()) < 0.0001);
    assert(std::abs(metrics.get_p99()) < 0.0001);

    std::cout << "test_empty_metrics passed.\n";
}

int main() {
    test_single_thread_metrics();
    test_multi_thread_metrics();
    test_metrics_reset();
    test_empty_metrics();

    std::cout << "All tests passed.\n";
    return 0;
}