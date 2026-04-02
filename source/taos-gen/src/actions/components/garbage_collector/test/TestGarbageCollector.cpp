#include "GarbageCollector.hpp"
#include <cassert>
#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <chrono>


struct Dummy {
    static std::atomic<int> destruct_count;
    int id;
    Dummy(int id_) : id(id_) {}
    Dummy(Dummy&& other) noexcept : id(other.id) {}
    Dummy& operator=(Dummy&& other) noexcept { id = other.id; return *this; }
    ~Dummy() { ++destruct_count; }
};
std::atomic<int> Dummy::destruct_count{0};

void test_gc_single_thread_dispose() {
    Dummy::destruct_count = 0;
    {
        GarbageCollector<Dummy> gc(1);
        gc.dispose(Dummy(1));
        gc.dispose(Dummy(2));
        gc.dispose(Dummy(3));
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    assert(Dummy::destruct_count == 3*2 && "All Dummy destructors should be called");
    std::cout << "test_gc_single_thread_dispose passed\n";
}

void test_gc_multi_thread_dispose() {
    Dummy::destruct_count = 0;
    {
        GarbageCollector<Dummy> gc(4);
        std::vector<std::thread> writers;
        for (int i = 0; i < 8; ++i) {
            writers.emplace_back([&gc, i] {
                gc.dispose(Dummy(i));
            });
        }
        for (auto& t : writers) t.join();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    assert(Dummy::destruct_count == 8*2 && "All Dummy destructors should be called in multi-threaded case");
    std::cout << "test_gc_multi_thread_dispose passed\n";
}

void test_gc_terminate() {
    Dummy::destruct_count = 0;
    GarbageCollector<Dummy> gc(2);
    gc.dispose(Dummy(1));
    gc.terminate();
    gc.dispose(Dummy(2)); // Should not be processed
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    assert(Dummy::destruct_count == 3 && "Only one Dummy should be destroyed after terminate");
    std::cout << "test_gc_terminate passed\n";
}

void test_gc_destruction_flushes() {
    Dummy::destruct_count = 0;
    {
        GarbageCollector<Dummy> gc(2);
        gc.dispose(Dummy(1));
        gc.dispose(Dummy(2));
        // Don't wait, let destructor flush
    }
    assert(Dummy::destruct_count == 2*2 && "Destructor should flush all remaining tasks");
    std::cout << "test_gc_destruction_flushes passed\n";
}

int main() {
    test_gc_single_thread_dispose();
    test_gc_multi_thread_dispose();
    test_gc_terminate();
    test_gc_destruction_flushes();
    std::cout << "All GarbageCollector tests passed!\n";
    return 0;
}