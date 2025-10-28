#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <assert.h>
#include "../include/ring_buffer.h"

#define TEST_BUFFER_SIZE 10
#define TEST_ITERATIONS 1000

// 测试数据结构
typedef struct {
    int id;
    char data[64];
} TestData;

// 生产者线程函数
void* producer_thread(void* arg) {
    SRingBuffer* ring_buffer = (SRingBuffer*)arg;
    
    for (int i = 0; i < TEST_ITERATIONS; i++) {
        TestData* data = (TestData*)malloc(sizeof(TestData));
        data->id = i;
        snprintf(data->data, sizeof(data->data), "Producer data %d", i);
        
        int ret = ring_buffer_enqueue_blocking(ring_buffer, data, 1000);
        if (ret != 0) {
            printf("Producer failed to enqueue item %d\n", i);
            free(data);
        } else {
            printf("Producer enqueued item %d\n", i);
        }
        
        usleep(1000); // 1ms delay
    }
    
    return NULL;
}

// 消费者线程函数
void* consumer_thread(void* arg) {
    SRingBuffer* ring_buffer = (SRingBuffer*)arg;
    
    for (int i = 0; i < TEST_ITERATIONS; i++) {
        void* item;
        int ret = ring_buffer_dequeue_blocking(ring_buffer, &item, 1000);
        if (ret != 0) {
            printf("Consumer failed to dequeue item %d\n", i);
        } else {
            TestData* data = (TestData*)item;
            printf("Consumer dequeued item %d: %s\n", data->id, data->data);
            free(data);
        }
        
        usleep(2000); // 2ms delay
    }
    
    return NULL;
}

// 测试基本功能
void test_basic_functionality() {
    printf("=== Testing Basic Functionality ===\n");
    
    SRingBuffer* ring_buffer = ring_buffer_init(5);
    assert(ring_buffer != NULL);
    
    // 测试空队列状态
    assert(ring_buffer_is_empty(ring_buffer));
    assert(!ring_buffer_is_full(ring_buffer));
    assert(ring_buffer_get_size(ring_buffer) == 0);
    assert(ring_buffer_get_capacity(ring_buffer) == 5);
    
    // 测试入队
    int* item1 = malloc(sizeof(int));
    *item1 = 42;
    assert(ring_buffer_enqueue(ring_buffer, item1) == 0);
    assert(!ring_buffer_is_empty(ring_buffer));
    assert(ring_buffer_get_size(ring_buffer) == 1);
    
    // 测试查看队首元素
    void* peek_item;
    assert(ring_buffer_peek(ring_buffer, &peek_item) == 0);
    assert(*(int*)peek_item == 42);
    assert(ring_buffer_get_size(ring_buffer) == 1); // 大小不变
    
    // 测试出队
    void* dequeue_item;
    assert(ring_buffer_dequeue(ring_buffer, &dequeue_item) == 0);
    assert(*(int*)dequeue_item == 42);
    assert(ring_buffer_is_empty(ring_buffer));
    free(dequeue_item);
    
    // 测试队列满的情况
    for (int i = 0; i < 5; i++) {
        int* item = malloc(sizeof(int));
        *item = i;
        assert(ring_buffer_enqueue(ring_buffer, item) == 0);
    }
    assert(ring_buffer_is_full(ring_buffer));
    assert(ring_buffer_get_size(ring_buffer) == 5);
    
    // 测试队列满时入队失败
    int* overflow_item = malloc(sizeof(int));
    *overflow_item = 999;
    assert(ring_buffer_enqueue(ring_buffer, overflow_item) != 0);
    free(overflow_item);
    
    // 清空队列
    ring_buffer_clear(ring_buffer, free);
    assert(ring_buffer_is_empty(ring_buffer));
    
    ring_buffer_destroy(ring_buffer);
    printf("Basic functionality test passed!\n");
}

// 测试多线程并发
void test_concurrent_access() {
    printf("=== Testing Concurrent Access ===\n");
    
    SRingBuffer* ring_buffer = ring_buffer_init(TEST_BUFFER_SIZE);
    assert(ring_buffer != NULL);
    
    pthread_t producer, consumer;
    
    // 创建生产者和消费者线程
    assert(pthread_create(&producer, NULL, producer_thread, ring_buffer) == 0);
    assert(pthread_create(&consumer, NULL, consumer_thread, ring_buffer) == 0);
    
    // 等待线程完成
    pthread_join(producer, NULL);
    pthread_join(consumer, NULL);
    
    // 验证队列为空
    assert(ring_buffer_is_empty(ring_buffer));
    
    // 获取统计信息
    uint64_t enqueue_count, dequeue_count, overflow_count;
    ring_buffer_get_stats(ring_buffer, &enqueue_count, &dequeue_count, &overflow_count);
    printf("Stats: enqueue=%lu, dequeue=%lu, overflow=%lu\n", 
           enqueue_count, dequeue_count, overflow_count);
    
    ring_buffer_destroy(ring_buffer);
    printf("Concurrent access test passed!\n");
}

// 测试超时功能
void test_timeout_functionality() {
    printf("=== Testing Timeout Functionality ===\n");
    
    SRingBuffer* ring_buffer = ring_buffer_init(1);
    assert(ring_buffer != NULL);
    
    // 填充队列
    int* item1 = malloc(sizeof(int));
    *item1 = 1;
    assert(ring_buffer_enqueue(ring_buffer, item1) == 0);
    
    // 测试入队超时
    int* item2 = malloc(sizeof(int));
    *item2 = 2;
    int ret = ring_buffer_enqueue_blocking(ring_buffer, item2, 100); // 100ms timeout
    assert(ret != 0); // 应该超时
    free(item2);
    
    // 测试出队超时
    void* dequeue_item;
    ret = ring_buffer_dequeue(ring_buffer, &dequeue_item);
    assert(ret == 0);
    if (dequeue_item == item1) {
        free(dequeue_item);
        item1 = NULL;
        dequeue_item = NULL;
    }
    
    ret = ring_buffer_dequeue_blocking(ring_buffer, &dequeue_item, 100); // 100ms timeout
    assert(ret != 0); // 应该超时
    
    ring_buffer_clear(ring_buffer, free);
    ring_buffer_destroy(ring_buffer);
    printf("Timeout functionality test passed!\n");
}

int main() {
    printf("Starting Ring Buffer Tests...\n");
    test_basic_functionality();
    test_concurrent_access();
    test_timeout_functionality();
    printf("All tests passed!\n");
    return 0;
} 