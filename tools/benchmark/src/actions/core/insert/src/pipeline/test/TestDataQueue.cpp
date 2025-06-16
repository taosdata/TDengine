#include <cassert>
#include <thread>
#include <chrono>
#include <iostream>
#include <vector>
#include "DataQueue.h"

void test_basic_operations() {
    DataQueue<int> queue(2);
    
    // Test push and pop
    queue.push(1);
    queue.push(2);
    
    auto result1 = queue.pop();
    assert(result1.status == PopStatus::Success);
    assert(*result1.data == 1);
    
    auto result2 = queue.pop();
    assert(result2.status == PopStatus::Success);
    assert(*result2.data == 2);
    
    std::cout << "test_basic_operations passed.\n";
}

void test_queue_capacity() {
    DataQueue<int> queue(2);
    
    assert(queue.try_push(1));
    assert(queue.try_push(2));
    assert(!queue.try_push(3));  // Should fail as queue is full
    
    auto result = queue.pop();
    assert(result.status == PopStatus::Success);
    assert(*result.data == 1);
    
    assert(queue.try_push(3));  // Should succeed now
    
    std::cout << "test_queue_capacity passed.\n";
}

void test_termination() {
    DataQueue<int> queue(2);
    
    queue.push(1);
    queue.terminate();
    
    // Should still be able to pop existing items
    auto result1 = queue.pop();
    assert(result1.status == PopStatus::Success);
    assert(*result1.data == 1);
    
    // Next pop should return Terminated
    auto result2 = queue.pop();
    assert(result2.status == PopStatus::Terminated);
    
    // Push should fail after termination
    try {
        queue.push(2);
        assert(false);  // Should not reach here
    } catch (const std::runtime_error&) {
        // Expected
    }
    
    std::cout << "test_termination passed.\n";
}

void test_concurrent_operations() {
    DataQueue<int> queue(100);
    
    std::thread producer([&queue]() {
        for (int i = 0; i < 50; i++) {
            queue.push(i);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });
    
    std::thread consumer([&queue]() {
        int count = 0;
        while (count < 50) {
            auto result = queue.pop();
            if (result.status == PopStatus::Success) {
                count++;
            }
        }
    });
    
    producer.join();
    consumer.join();
    
    assert(queue.empty());
    std::cout << "test_concurrent_operations passed.\n";
}

void test_timeout() {
    DataQueue<int> queue(1);
    
    auto result = queue.try_pop();
    assert(result.status == PopStatus::Timeout);
    
    std::cout << "test_timeout passed.\n";
}

void test_size_and_empty() {
    DataQueue<int> queue(3);
    
    assert(queue.empty());
    assert(queue.size() == 0);
    
    queue.push(1);
    assert(!queue.empty());
    assert(queue.size() == 1);
    
    queue.pop();
    assert(queue.empty());
    assert(queue.size() == 0);
    
    std::cout << "test_size_and_empty passed.\n";
}

int main() {
    test_basic_operations();
    test_queue_capacity();
    test_termination();
    test_concurrent_operations();
    test_timeout();
    test_size_and_empty();
    
    std::cout << "All DataQueue tests passed.\n";
    return 0;
}