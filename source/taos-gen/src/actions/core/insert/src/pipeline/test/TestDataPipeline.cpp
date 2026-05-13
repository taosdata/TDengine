#include "DataPipeline.hpp"
#include <cassert>
#include <thread>
#include <vector>
#include <iostream>
#include <atomic>
#include <chrono>

void test_independent_basic() {
    DataPipeline<int> pipeline(false, 2, 2, 4);

    pipeline.push_data(0, 100);
    pipeline.push_data(1, 200);

    auto r1 = pipeline.fetch_data(0);
    auto r2 = pipeline.fetch_data(1);

    assert(r1.status == DataPipeline<int>::Status::Success);
    assert(r2.status == DataPipeline<int>::Status::Success);
    assert(r1.data && *r1.data == 100);
    assert(r2.data && *r2.data == 200);
    (void)r1;
    (void)r2;

    std::cout << "test_independent_basic passed." << std::endl;
}

void test_shared_basic() {
    DataPipeline<int> pipeline(true, 2, 2, 4);

    pipeline.push_data(0, 123);
    pipeline.push_data(1, 456);

    auto r1 = pipeline.fetch_data(0);
    auto r2 = pipeline.fetch_data(1);

    assert(r1.status == DataPipeline<int>::Status::Success);
    assert(r2.status == DataPipeline<int>::Status::Success);
    assert(r1.data && *r1.data == 123);
    assert(r2.data && *r2.data == 456);
    (void)r1;
    (void)r2;

    std::cout << "test_shared_basic passed." << std::endl;
}

void test_independent_termination() {
    DataPipeline<int> pipeline(false, 2, 2, 2);

    pipeline.push_data(0, 1);
    pipeline.terminate();

    auto r1 = pipeline.fetch_data(0);
    assert(r1.status == DataPipeline<int>::Status::Success);
    assert(r1.data && *r1.data == 1);
    (void)r1;

    auto r2 = pipeline.fetch_data(0);
    assert(r2.status == DataPipeline<int>::Status::Terminated);
    (void)r2;

    try {
        pipeline.push_data(0, 2);
        assert(false && "Should throw after termination");
    } catch (const std::runtime_error&) {}

    std::cout << "test_independent_termination passed." << std::endl;
}

void test_shared_termination() {
    DataPipeline<int> pipeline(true, 2, 2, 2);

    pipeline.push_data(0, 1);
    pipeline.terminate();

    auto r1 = pipeline.fetch_data(0);
    assert(r1.status == DataPipeline<int>::Status::Success);
    assert(r1.data && *r1.data == 1);
    (void)r1;

    auto r2 = pipeline.fetch_data(0);
    assert(r2.status == DataPipeline<int>::Status::Terminated);
    (void)r2;

    try {
        pipeline.push_data(0, 2);
        assert(false && "Should throw after termination");
    } catch (const std::runtime_error&) {}

    std::cout << "test_shared_termination passed." << std::endl;
}

void test_independent_concurrent() {
    DataPipeline<int> pipeline(false, 2, 2, 100);
    std::atomic<int> produced{0};
    std::atomic<int> consumed{0};

    std::thread producer1([&]() {
        for (int i = 0; i < 50; ++i) {
            pipeline.push_data(0, i);
            produced++;
        }
    });
    std::thread producer2([&]() {
        for (int i = 50; i < 100; ++i) {
            pipeline.push_data(1, i);
            produced++;
        }
    });

    std::thread consumer1([&]() {
        while (consumed < 100) {
            auto r = pipeline.fetch_data(0);
            if (r.status == DataPipeline<int>::Status::Success) consumed++;
            else if (r.status == DataPipeline<int>::Status::Terminated) break;
        }
    });
    std::thread consumer2([&]() {
        while (consumed < 100) {
            auto r = pipeline.fetch_data(1);
            if (r.status == DataPipeline<int>::Status::Success) consumed++;
            else if (r.status == DataPipeline<int>::Status::Terminated) break;
        }
    });

    producer1.join();
    producer2.join();
    pipeline.terminate();
    consumer1.join();
    consumer2.join();

    assert(produced == 100);
    assert(consumed == 100);

    std::cout << "test_independent_concurrent passed." << std::endl;
}

void test_shared_concurrent() {
    DataPipeline<int> pipeline(true, 2, 2, 100);
    std::atomic<int> produced{0};
    std::atomic<int> consumed{0};

    std::thread producer1([&]() {
        for (int i = 0; i < 50; ++i) {
            pipeline.push_data(0, i);
            produced++;
        }
    });
    std::thread producer2([&]() {
        for (int i = 50; i < 100; ++i) {
            pipeline.push_data(1, i);
            produced++;
        }
    });

    std::thread consumer1([&]() {
        while (consumed < 100) {
            auto r = pipeline.fetch_data(0);
            if (r.status == DataPipeline<int>::Status::Success) {
                std::cout << "consumed: " << consumed << " Consumer 1 got: " << *r.data << std::endl;
                consumed++;
            }
            else if (r.status == DataPipeline<int>::Status::Terminated) {
                std::cout << "Consumer 1 terminated." << std::endl;
                break;
            }
        }
    });
    std::thread consumer2([&]() {
        while (consumed < 100) {
            auto r = pipeline.fetch_data(1);
            if (r.status == DataPipeline<int>::Status::Success) {
                std::cout << "consumed: " << consumed << " Consumer 2 got: " << *r.data << std::endl;
                consumed++;
            }
            else if (r.status == DataPipeline<int>::Status::Terminated) {
                std::cout << "Consumer 2 terminated." << std::endl;
                break;
            }
        }
    });

    producer1.join();
    producer2.join();
    pipeline.terminate();
    consumer1.join();
    consumer2.join();

    assert(produced == 100);
    std::cout << "Total consumed: " << consumed << std::endl;
    assert(consumed == 100);

    std::cout << "test_shared_concurrent passed." << std::endl;
}

void test_total_queued() {
    DataPipeline<int> pipeline(false, 2, 2, 4);

    assert(pipeline.total_queued() == 0);
    pipeline.push_data(0, 1);
    pipeline.push_data(1, 2);
    assert(pipeline.total_queued() == 2);

    pipeline.fetch_data(0);
    assert(pipeline.total_queued() == 1);

    pipeline.fetch_data(1);
    assert(pipeline.total_queued() == 0);

    std::cout << "test_total_queued passed." << std::endl;
}


void test_invalid_producer_consumer_id() {
    DataPipeline<int> pipeline(false, 2, 2, 4);
    try {
        pipeline.push_data(2, 1);
        assert(false && "Should throw for invalid producer_id");
    } catch (const std::out_of_range&) {}

    try {
        pipeline.fetch_data(2);
        assert(false && "Should throw for invalid consumer_id");
    } catch (const std::out_of_range&) {}

    std::cout << "test_invalid_producer_consumer_id passed." << std::endl;
}

void test_capacity_limit_independent() {
    DataPipeline<int> pipeline(false, 1, 1, 2);
    pipeline.push_data(0, 1);
    pipeline.push_data(0, 2);

    std::atomic<bool> pushed{false};
    std::thread t([&]() {
        pipeline.push_data(0, 3);
        pushed = true;
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    assert(!pushed);

    pipeline.fetch_data(0);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    assert(pushed);

    t.join();
    std::cout << "test_capacity_limit_independent passed." << std::endl;
}

void test_capacity_limit_shared() {
    DataPipeline<int> pipeline(true, 1, 1, 2);
    pipeline.push_data(0, 1);
    pipeline.push_data(0, 2);

    std::atomic<bool> pushed{false};
    std::thread t([&]() {
        pipeline.push_data(0, 3);
        pushed = true;
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    assert(pushed);

    t.join();
    std::cout << "test_capacity_limit_shared passed." << std::endl;
}

void test_timeout_fetch() {
    DataPipeline<int> pipeline(false, 1, 1, 2);
    auto r = pipeline.fetch_data(0);
    assert(r.status == DataPipeline<int>::Status::Timeout);
    (void)r;
    std::cout << "test_timeout_fetch passed." << std::endl;
}

void test_terminate_twice() {
    DataPipeline<int> pipeline(false, 1, 1, 2);
    pipeline.terminate();
    pipeline.terminate();
    auto r = pipeline.fetch_data(0);
    assert(r.status == DataPipeline<int>::Status::Terminated);
    (void)r;
    std::cout << "test_terminate_twice passed." << std::endl;
}

void test_terminate_no_data() {
    DataPipeline<int> pipeline(true, 1, 1, 2);
    pipeline.terminate();
    auto r = pipeline.fetch_data(0);
    assert(r.status == DataPipeline<int>::Status::Terminated);
    (void)r;
    std::cout << "test_terminate_no_data passed." << std::endl;
}

void test_empty_total_queued() {
    DataPipeline<int> pipeline(false, 2, 2, 4);
    assert(pipeline.total_queued() == 0);
    (void)pipeline;
    std::cout << "test_empty_total_queued passed." << std::endl;
}

void test_push_after_terminate() {
    DataPipeline<int> pipeline(true, 1, 1, 2);
    pipeline.terminate();
    try {
        pipeline.push_data(0, 1);
        assert(false && "Should throw after terminate");
    } catch (const std::runtime_error&) {}
    std::cout << "test_push_after_terminate passed." << std::endl;
}

void test_no_producer_consumer() {
    DataPipeline<int> pipeline(false, 0, 0, 2);
    assert(pipeline.total_queued() == 0);
    std::cout << "test_no_producer_consumer passed." << std::endl;
}

void test_shared_multiple_terminate() {
    DataPipeline<int> pipeline(true, 1, 1, 2);
    pipeline.push_data(0, 1);
    pipeline.terminate();
    pipeline.terminate();
    auto r = pipeline.fetch_data(0);
    assert(r.status == DataPipeline<int>::Status::Success);
    r = pipeline.fetch_data(0);
    assert(r.status == DataPipeline<int>::Status::Terminated);
    std::cout << "test_shared_multiple_terminate passed." << std::endl;
}

int main() {
    test_independent_basic();
    test_shared_basic();
    test_independent_termination();
    test_shared_termination();
    test_independent_concurrent();
    test_shared_concurrent();
    test_total_queued();

    test_invalid_producer_consumer_id();
    test_capacity_limit_independent();
    test_capacity_limit_shared();
    test_timeout_fetch();
    test_terminate_twice();
    test_terminate_no_data();
    test_empty_total_queued();
    test_push_after_terminate();
    test_no_producer_consumer();
    test_shared_multiple_terminate();

    std::cout << "All DataPipeline tests passed." << std::endl;
    return 0;
}