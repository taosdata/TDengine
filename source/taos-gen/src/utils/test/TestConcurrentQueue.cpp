#include "concurrentqueue.hpp"
#include <atomic>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <numeric>
#include <thread>
#include <vector>

using moodycamel::ConcurrentQueue;
using moodycamel::ProducerToken;
using moodycamel::ConsumerToken;

namespace {
struct CountingType {
    static std::atomic<int> dtorCount;
    int value;
    CountingType(int v = 0) : value(v) {}
    CountingType(const CountingType&) = default;
    CountingType(CountingType&&) noexcept = default;
    CountingType& operator=(const CountingType&) = default;
    CountingType& operator=(CountingType&&) noexcept = default;
    ~CountingType() { dtorCount.fetch_add(1, std::memory_order_relaxed); }
};

std::atomic<int> CountingType::dtorCount{0};

struct ThrowOnMoveAssign {
    static std::atomic<int> moveAssignCount;
    static std::atomic<int> throwAfterCount;
    int value;

    ThrowOnMoveAssign(int v = 0) : value(v) {}
    ThrowOnMoveAssign(const ThrowOnMoveAssign&) = default;
    ThrowOnMoveAssign(ThrowOnMoveAssign&&) noexcept = default;
    ThrowOnMoveAssign& operator=(const ThrowOnMoveAssign&) = default;

    ThrowOnMoveAssign& operator=(ThrowOnMoveAssign&& other)
    {
        int count = moveAssignCount.fetch_add(1, std::memory_order_relaxed) + 1;
        if (count >= throwAfterCount.load(std::memory_order_relaxed)) {
            throw std::runtime_error("move assignment throws");
        }
        value = other.value;
        return *this;
    }
};

std::atomic<int> ThrowOnMoveAssign::moveAssignCount{0};
std::atomic<int> ThrowOnMoveAssign::throwAfterCount{1};

struct NoImplicitTraits : public moodycamel::ConcurrentQueueDefaultTraits {
    static const size_t INITIAL_IMPLICIT_PRODUCER_HASH_SIZE = 0;
};
}

void test_details_helpers() {
    char buffer[64];
    char* aligned = moodycamel::details::align_for<std::max_align_t>(buffer + 1);
    auto addr = reinterpret_cast<std::uintptr_t>(aligned);
    (void)addr;
    assert(addr % alignof(std::max_align_t) == 0);

    assert(moodycamel::details::ceil_to_pow_2<unsigned>(1u) == 1u);
    assert(moodycamel::details::ceil_to_pow_2<unsigned>(3u) == 4u);
    assert(moodycamel::details::ceil_to_pow_2<unsigned>(16u) == 16u);

    using u32 = std::uint32_t;
    (void)u32{};
    assert(moodycamel::details::circular_less_than<u32>(1u, 2u));
    assert(!moodycamel::details::circular_less_than<u32>(2u, 1u));
    assert(moodycamel::details::circular_less_than<u32>(static_cast<u32>(0xFFFFFFF0u), 5u));

    std::cout << "test_details_helpers passed.\n";
}

void test_explicit_producer_dequeue() {
    ConcurrentQueue<int> queue(ConcurrentQueue<int>::BLOCK_SIZE * 2);
    ProducerToken producer(queue);
    ConsumerToken consumer(queue);

    assert(queue.enqueue(producer, 42));

    int value = 0;
    bool ok = queue.try_dequeue(consumer, value);
    (void)ok;
    assert(ok);
    assert(value == 42);

    std::cout << "test_explicit_producer_dequeue passed.\n";
}

void test_basic_enqueue_dequeue_single_thread() {
    ConcurrentQueue<int> queue(ConcurrentQueue<int>::BLOCK_SIZE * 2);

    (void)queue;
    assert(queue.enqueue(1));
    assert(queue.enqueue(2));
    assert(queue.try_enqueue(3));

    int value = 0;
    (void)value;
    assert(queue.try_dequeue(value));
    assert(value == 1);
    assert(queue.try_dequeue(value));
    assert(value == 2);
    assert(queue.try_dequeue(value));
    assert(value == 3);

    std::cout << "test_basic_enqueue_dequeue_single_thread passed.\n";
}

void test_try_dequeue_empty_queue() {
    ConcurrentQueue<int> queue(ConcurrentQueue<int>::BLOCK_SIZE);
    int value = 0;
    bool ok = queue.try_dequeue(value);
    (void)ok;
    assert(!ok);

    std::cout << "test_try_dequeue_empty_queue passed.\n";
}

void test_explicit_bulk_resize_and_dequeue() {
    ConcurrentQueue<int> queue(ConcurrentQueue<int>::BLOCK_SIZE * 4);
    ProducerToken producer(queue);

    const size_t blockSize = ConcurrentQueue<int>::BLOCK_SIZE;
    const size_t indexSize = ConcurrentQueue<int>::EXPLICIT_INITIAL_INDEX_SIZE;
    const size_t count = blockSize * (indexSize + 2);

    std::vector<int> items(count);
    std::iota(items.begin(), items.end(), 1);

    bool ok = queue.enqueue_bulk(producer, items.begin(), items.size());
    (void)ok;
    assert(ok);

    std::vector<int> out(count);
    size_t dequeued = queue.try_dequeue_bulk_from_producer(producer, out.begin(), out.size());
    (void)dequeued;
    assert(dequeued == count);

    std::cout << "test_explicit_bulk_resize_and_dequeue passed.\n";
}

void test_explicit_bulk_with_tokens() {
    ConcurrentQueue<int> queue(ConcurrentQueue<int>::BLOCK_SIZE * 2);
    ProducerToken producer(queue);
    ConsumerToken consumer(queue);

    std::vector<int> items = {10, 20, 30, 40};
    bool ok = queue.enqueue_bulk(producer, items.begin(), items.size());
    (void)ok;
    assert(ok);

    std::vector<int> out(items.size());
    size_t dequeued = queue.try_dequeue_bulk(consumer, out.begin(), out.size());
    (void)dequeued;
    assert(dequeued == items.size());
    assert(out == items);

    std::cout << "test_explicit_bulk_with_tokens passed.\n";
}

void test_explicit_destructor_cleans_unconsumed() {
    CountingType::dtorCount.store(0, std::memory_order_relaxed);
    {
        ConcurrentQueue<CountingType> queue(ConcurrentQueue<CountingType>::BLOCK_SIZE * 2);
        ProducerToken producer(queue);
        for (int i = 0; i < 10; ++i) {
            queue.enqueue(producer, CountingType{i});
        }
    }
    assert(CountingType::dtorCount.load(std::memory_order_relaxed) >= 10);

    std::cout << "test_explicit_destructor_cleans_unconsumed passed.\n";
}

void test_explicit_destructor_half_dequeued() {
    CountingType::dtorCount.store(0, std::memory_order_relaxed);
    {
        ConcurrentQueue<CountingType> queue(ConcurrentQueue<CountingType>::BLOCK_SIZE * 2);
        ProducerToken producer(queue);
        ConsumerToken consumer(queue);

        const int total = static_cast<int>(ConcurrentQueue<CountingType>::BLOCK_SIZE) + 3;
        for (int i = 0; i < total; ++i) {
            queue.enqueue(producer, CountingType{i});
        }

        CountingType out;
        for (int i = 0; i < 2; ++i) {
            bool ok = queue.try_dequeue(consumer, out);
            (void)ok;
            assert(ok);
        }
    }

    assert(CountingType::dtorCount.load(std::memory_order_relaxed) >= 5);
    std::cout << "test_explicit_destructor_half_dequeued passed.\n";
}

void test_explicit_dequeue_move_assign_throw() {
    ThrowOnMoveAssign::moveAssignCount.store(0, std::memory_order_relaxed);
    ThrowOnMoveAssign::throwAfterCount.store(1, std::memory_order_relaxed);

    ConcurrentQueue<ThrowOnMoveAssign> queue(ConcurrentQueue<ThrowOnMoveAssign>::BLOCK_SIZE * 2);
    ProducerToken producer(queue);
    ConsumerToken consumer(queue);

    queue.enqueue(producer, ThrowOnMoveAssign{7});

    ThrowOnMoveAssign out;
    bool threw = false;
    try {
        queue.try_dequeue(consumer, out);
    } catch (const std::runtime_error&) {
        threw = true;
    }
    (void)threw;
    assert(threw);

    // Element should have been destroyed/removed
    ThrowOnMoveAssign out2;
    bool ok = queue.try_dequeue(consumer, out2);
    (void)ok;
    assert(!ok);

    std::cout << "test_explicit_dequeue_move_assign_throw passed.\n";
}

void test_implicit_bulk_resize_and_dequeue() {
    ConcurrentQueue<int> queue(ConcurrentQueue<int>::BLOCK_SIZE * 4);

    const size_t blockSize = ConcurrentQueue<int>::BLOCK_SIZE;
    const size_t indexSize = ConcurrentQueue<int>::IMPLICIT_INITIAL_INDEX_SIZE;
    const size_t count = blockSize * (indexSize + 2);

    std::vector<int> items(count);
    std::iota(items.begin(), items.end(), 1);

    bool ok = queue.enqueue_bulk(items.begin(), items.size());
    (void)ok;
    assert(ok);

    std::vector<int> out(count);
    size_t dequeued = queue.try_dequeue_bulk(out.begin(), out.size());
    (void)dequeued;
    assert(dequeued == count);

    std::cout << "test_implicit_bulk_resize_and_dequeue passed.\n";
}

void test_implicit_dequeue_move_assign_throw() {
    ThrowOnMoveAssign::moveAssignCount.store(0, std::memory_order_relaxed);
    ThrowOnMoveAssign::throwAfterCount.store(1, std::memory_order_relaxed);

    ConcurrentQueue<ThrowOnMoveAssign> queue(ConcurrentQueue<ThrowOnMoveAssign>::BLOCK_SIZE * 2);
    queue.enqueue(ThrowOnMoveAssign{3});

    ThrowOnMoveAssign out;
    bool threw = false;
    try {
        queue.try_dequeue(out);
    } catch (const std::runtime_error&) {
        threw = true;
    }
    (void)threw;
    assert(threw);

    ThrowOnMoveAssign out2;
    bool ok = queue.try_dequeue(out2);
    (void)ok;
    assert(!ok);

    std::cout << "test_implicit_dequeue_move_assign_throw passed.\n";
}

void test_implicit_hash_resize_with_threads() {
    ConcurrentQueue<int> queue(ConcurrentQueue<int>::BLOCK_SIZE * 2);
    const size_t threadCount = ConcurrentQueue<int>::INITIAL_IMPLICIT_PRODUCER_HASH_SIZE + 8;

    std::vector<std::thread> threads;
    threads.reserve(threadCount);

    for (size_t i = 0; i < threadCount; ++i) {
        threads.emplace_back([&queue, i]() {
            queue.enqueue(static_cast<int>(i));
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    int value = 0;
    size_t drained = 0;
    while (queue.try_dequeue(value)) {
        ++drained;
    }

    (void)drained;
    assert(drained == threadCount);

    std::cout << "test_implicit_hash_resize_with_threads passed.\n";
}

void test_implicit_production_disabled() {
    moodycamel::ConcurrentQueue<int, NoImplicitTraits> queue(64);

    bool ok1 = queue.enqueue(1);
    bool ok2 = queue.try_enqueue(2);

    std::vector<int> items = {1, 2, 3};
    bool ok3 = queue.enqueue_bulk(items.begin(), items.size());
    bool ok4 = queue.try_enqueue_bulk(items.begin(), items.size());

    (void)ok1;
    (void)ok2;
    (void)ok3;
    (void)ok4;
    assert(!ok1);
    assert(!ok2);
    assert(!ok3);
    assert(!ok4);

    std::cout << "test_implicit_production_disabled passed.\n";
}

void test_queue_swap_basic() {
    ConcurrentQueue<int> q1(ConcurrentQueue<int>::BLOCK_SIZE * 2);
    ConcurrentQueue<int> q2(ConcurrentQueue<int>::BLOCK_SIZE * 2);

    q1.enqueue(1);
    q1.enqueue(2);
    q2.enqueue(100);

    q1.swap(q2);

    int value = 0;
    (void)value;
    assert(q2.try_dequeue(value));
    assert(value == 1);
    assert(q2.try_dequeue(value));
    assert(value == 2);

    assert(q1.try_dequeue(value));
    assert(value == 100);

    std::cout << "test_queue_swap_basic passed.\n";
}

int main() {
    test_details_helpers();
    test_basic_enqueue_dequeue_single_thread();
    test_try_dequeue_empty_queue();
    test_explicit_producer_dequeue();
    test_explicit_bulk_resize_and_dequeue();
    test_explicit_bulk_with_tokens();
    test_explicit_destructor_cleans_unconsumed();
    test_explicit_destructor_half_dequeued();
    test_explicit_dequeue_move_assign_throw();
    test_implicit_bulk_resize_and_dequeue();
    test_implicit_dequeue_move_assign_throw();
    test_implicit_hash_resize_with_threads();
    test_implicit_production_disabled();
    test_queue_swap_basic();

    std::cout << "All concurrentqueue tests passed.\n";
    return 0;
}
