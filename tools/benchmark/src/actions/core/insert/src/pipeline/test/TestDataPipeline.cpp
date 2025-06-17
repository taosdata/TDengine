#include "DataPipeline.h"
#include <cassert>
#include <thread>
#include <vector>
#include <iostream>

void test_basic_operations() {
    DataPipeline<int> pipeline(2, 2);
    
    pipeline.push_data(0, 42);
    pipeline.push_data(1, 43);
    
    auto result1 = pipeline.fetch_data(0);
    auto result2 = pipeline.fetch_data(1);
    
    assert(result1.status == DataPipeline<int>::Status::Success);
    assert(result2.status == DataPipeline<int>::Status::Success);
    assert(result1.data.has_value());
    assert(result2.data.has_value());
    assert(result1.data.value() == 42 && "Consumer 0 should receive data from Producer 0");
    assert(result2.data.value() == 43 && "Consumer 1 should receive data from Producer 1");

    std::cout << "test_basic_operations passed.\n";
}

void test_producer_consumer_mapping() {
    // Test case 1: More producers than consumers (4 producers, 2 consumers)
    DataPipeline<int> pipeline1(4, 2);
    for (int i = 0; i < 4; i++) {
        pipeline1.push_data(i, i * 10);
    }
    
    {
        auto result1 = pipeline1.fetch_data(0);
        auto result2 = pipeline1.fetch_data(1);
        
        // Verify status
        assert(result1.status == DataPipeline<int>::Status::Success);
        assert(result2.status == DataPipeline<int>::Status::Success);
        
        // Verify data exists and values are correct
        assert(result1.data.has_value() && "Consumer 0 should receive data");
        assert(result2.data.has_value() && "Consumer 1 should receive data");
        
        // Consumer 0 should get data from its mapped producers (0, 1)
        // Consumer 1 should get data from its mapped producers (2, 3)
        assert(result1.data.value() == 0 && 
            "Consumer 0 should receive data from Producer 0");
        assert(result2.data.value() == 20 && 
            "Consumer 1 should receive data from Producer 2");
    }
    {
        auto result1 = pipeline1.fetch_data(0);
        auto result2 = pipeline1.fetch_data(1);
        
        // Verify status
        assert(result1.status == DataPipeline<int>::Status::Success);
        assert(result2.status == DataPipeline<int>::Status::Success);
        
        // Verify data exists and values are correct
        assert(result1.data.has_value() && "Consumer 0 should receive data");
        assert(result2.data.has_value() && "Consumer 1 should receive data");
        
        // Consumer 0 should get data from its mapped producers (0, 1)
        // Consumer 1 should get data from its mapped producers (2, 3)
        assert(result1.data.value() == 10 && 
            "Consumer 0 should receive data from Producer 1");
        assert(result2.data.value() == 30 && 
            "Consumer 1 should receive data from Producer 3");
    }

    // Test case 2: More consumers than producers (2 producers, 4 consumers)
    DataPipeline<int> pipeline2(2, 4);
    pipeline2.push_data(0, 100);
    pipeline2.push_data(1, 200);
    
    for (size_t i = 0; i < 4; i++) {
        auto result = pipeline2.fetch_data(i);
        if (result.status == DataPipeline<int>::Status::Success) {
            assert(result.data.has_value() && "Data should exist for Success status");
            int value = result.data.value();
            assert(((i == 0 && value == 100) || (i == 2 && value == 200)) && 
                   "Received value should be from either producer 0 or 2");
        } else {
            if (i == 1 || i == 3) {
                // Consumers 1 and 3 may not receive data if they are not enough data
                assert(result.status == DataPipeline<int>::Status::Timeout && 
                       "Consumers without data should return Timeout status");
            }
        }
    }
    
    // Test case 3: More consumers than producers with multiple data points
    DataPipeline<int> pipeline3(2, 4);
    pipeline3.push_data(0, 100);
    pipeline3.push_data(1, 200);
    pipeline3.push_data(0, 300);
    pipeline3.push_data(1, 400);
    
    for (size_t i = 0; i < 4; i++) {
        auto result = pipeline3.fetch_data(i);
        if (result.status == DataPipeline<int>::Status::Success) {
            assert(result.data.has_value() && "Data should exist for Success status");
            int value = result.data.value();
            assert(((i == 0 && value == 100) || (i == 2 && value == 200)
                 || (i == 1 && value == 300) || (i == 3 && value == 400)) && 
                   "Received value should be from either producer 0 or 2");
        } else {
            assert(false && "Consumers without data should not return");
        }
    }

    std::cout << "test_producer_consumer_mapping passed.\n";
}

void test_termination() {
    DataPipeline<int> pipeline(2, 2);
    
    pipeline.push_data(0, 42);
    pipeline.terminate();
    
    try {
        pipeline.push_data(0, 43);
        assert(false && "Should have thrown exception");
    } catch (const std::runtime_error&) {
        // Expected
    }
    
    auto result = pipeline.fetch_data(0);
    assert(result.status == DataPipeline<int>::Status::Terminated);
    
    std::cout << "test_termination passed.\n";
}

void test_concurrent_operations() {
    DataPipeline<int> pipeline(2, 2);
    std::atomic<bool> stop{false};
    
    // Producer threads
    std::vector<std::thread> producers;
    for (size_t i = 0; i < 2; i++) {
        producers.emplace_back([&pipeline, i, &stop]() {
            int count = 0;
            while (!stop && count < 100) {
                try {
                    pipeline.push_data(i, count++);
                } catch (const std::runtime_error&) {
                    break;
                }
            }
        });
    }
    
    // Consumer threads
    std::vector<std::thread> consumers;
    std::atomic<size_t> consumed{0};
    for (size_t i = 0; i < 2; i++) {
        consumers.emplace_back([&pipeline, i, &consumed, &stop]() {
            while (!stop) {
                auto result = pipeline.fetch_data(i);
                if (result.status == DataPipeline<int>::Status::Success) {
                    consumed++;
                }
            }
        });
    }
    
    // Let it run for a short while
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    stop = true;
    pipeline.terminate();
    
    for (auto& p : producers) p.join();
    for (auto& c : consumers) c.join();
    
    assert(consumed > 0);
    std::cout << "test_concurrent_operations passed.\n";
}

void test_queue_monitoring() {
    DataPipeline<int> pipeline(2, 2);
    
    assert(pipeline.total_queued() == 0);
    
    pipeline.push_data(0, 42);
    pipeline.push_data(1, 43);
    
    assert(pipeline.total_queued() == 2);
    
    auto result = pipeline.fetch_data(0);
    assert(result.status == DataPipeline<int>::Status::Success);
    assert(pipeline.total_queued() == 1);
    
    std::cout << "test_queue_monitoring passed.\n";
}

int main() {
    test_basic_operations();
    test_producer_consumer_mapping();
    test_termination();
    test_concurrent_operations();
    test_queue_monitoring();
    
    std::cout << "All DataPipeline tests passed.\n";
    return 0;
}