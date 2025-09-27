#include <unistd.h>

#include "../include/bitmap_engine.h"
#include "../include/bitmap_interface.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <time.h>
#include <pthread.h>

// 测试辅助函数
static void print_test_result(const char* test_name, bool passed) {
    printf("[%s] %s\n", passed ? "PASS" : "FAIL", test_name);
}

static int64_t get_current_timestamp() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + (int64_t)ts.tv_nsec;
}

// 测试1: 位图引擎基本初始化和销毁
static void test_bitmap_engine_basic() {
    printf("\n=== 测试1: 位图引擎基本初始化和销毁 ===\n");
    
    // 测试初始化
    SBitmapEngine* engine = bitmap_engine_init();
    assert(engine != NULL);
    print_test_result("位图引擎初始化", true);
    
    // 测试销毁
    bitmap_engine_destroy(engine);
    print_test_result("位图引擎销毁", true);
}

// 测试2: 位图操作功能
static void test_bitmap_operations() {
    printf("\n=== 测试2: 位图操作功能 ===\n");
    
    SBitmapEngine* engine = bitmap_engine_init();
    assert(engine != NULL);
    
    // 测试标记脏块
    uint64_t test_values[] = {1, 10, 100, 1000, 10000};
    int test_count = sizeof(test_values) / sizeof(test_values[0]);
    
    for (int i = 0; i < test_count; i++) {
        int32_t result = bitmap_engine_mark_dirty(engine, test_values[i], i * 1000, get_current_timestamp());
        assert(result == 0);
    }
    print_test_result("标记脏块", true);
    
    // 测试查询块状态
    for (int i = 0; i < test_count; i++) {
        EBlockState state;
        int32_t result = bitmap_engine_get_block_state(engine, test_values[i], &state);
        assert(result == 0);
        assert(state == BLOCK_STATE_DIRTY);
    }
    print_test_result("查询块状态", true);
    
    // 测试清除块状态
    int32_t result = bitmap_engine_clear_block(engine, 100);
    assert(result == 0);
    
    EBlockState state;
    result = bitmap_engine_get_block_state(engine, 100, &state);
    assert(result == ERR_BLOCK_NOT_FOUND);
    print_test_result("清除块状态", true);
    
    bitmap_engine_destroy(engine);
}

// 测试3: 状态转换测试
static void test_state_transitions() {
    printf("\n=== 测试3: 状态转换测试 ===\n");
    
    SBitmapEngine* engine = bitmap_engine_init();
    assert(engine != NULL);
    
    uint64_t block_id = 12345;
    int64_t timestamp = get_current_timestamp();
    
    // 测试 CLEAN -> DIRTY
    int32_t result = bitmap_engine_mark_dirty(engine, block_id, 1000, timestamp);
    assert(result == 0);
    
    EBlockState state;
    result = bitmap_engine_get_block_state(engine, block_id, &state);
    assert(result == 0);
    assert(state == BLOCK_STATE_DIRTY);
    print_test_result("CLEAN -> DIRTY 转换", true);
    
    // 测试 DIRTY -> NEW
    result = bitmap_engine_mark_new(engine, block_id, 2000, timestamp);
    printf("mark_new result: %d\n", result);
    if (result != 0) {
        EBlockState current_state;
        bitmap_engine_get_block_state(engine, block_id, &current_state);
        printf("Current state before mark_new: %d\n", current_state);
        printf("Error: %s\n", bitmap_engine_get_state_transition_error(current_state, BLOCK_STATE_NEW));
    }
    assert(result == 0);
    
    result = bitmap_engine_get_block_state(engine, block_id, &state);
    assert(result == 0);
    assert(state == BLOCK_STATE_NEW);
    print_test_result("DIRTY -> NEW 转换", true);
    
    // 测试 NEW -> DELETED
    result = bitmap_engine_mark_deleted(engine, block_id, 3000, timestamp);
    printf("mark_deleted result: %d\n", result);
    if (result != 0) {
        EBlockState current_state;
        bitmap_engine_get_block_state(engine, block_id, &current_state);
        printf("Current state before mark_deleted: %d\n", current_state);
        printf("Error: %s\n", bitmap_engine_get_state_transition_error(current_state, BLOCK_STATE_DELETED));
    }
    assert(result == 0);
    
    result = bitmap_engine_get_block_state(engine, block_id, &state);
    assert(result == 0);
    assert(state == BLOCK_STATE_DELETED);
    print_test_result("NEW -> DELETED 转换", true);
    
    bitmap_engine_destroy(engine);
}

// 测试4: 统计信息测试
static void test_statistics() {
    printf("\n=== 测试4: 统计信息测试 ===\n");
    
    SBitmapEngine* engine = bitmap_engine_init();
    assert(engine != NULL);
    
    // 添加一些测试数据
    for (int i = 0; i < 10; i++) {
        bitmap_engine_mark_dirty(engine, i, i * 1000, get_current_timestamp());
    }
    
    for (int i = 10; i < 20; i++) {
        bitmap_engine_mark_new(engine, i, i * 1000, get_current_timestamp());
    }
    
    for (int i = 20; i < 25; i++) {
        bitmap_engine_mark_deleted(engine, i, i * 1000, get_current_timestamp());
    }
    
    // 获取统计信息
    uint64_t total_blocks, dirty_count, new_count, deleted_count;
    bitmap_engine_get_stats(engine, &total_blocks, &dirty_count, &new_count, &deleted_count);
    
    printf("统计信息: 总块数=%lu, 脏块数=%lu, 新块数=%lu, 删除块数=%lu\n",
           total_blocks, dirty_count, new_count, deleted_count);
    
    assert(dirty_count == 10);
    assert(new_count == 10);
    assert(deleted_count == 5);
    print_test_result("统计信息测试", true);
    
    bitmap_engine_destroy(engine);
}

// 测试5: 性能基准测试
static void test_performance_benchmark() {
    printf("\n=== 测试5: 性能基准测试 ===\n");
    
    SBitmapEngine* engine = bitmap_engine_init();
    assert(engine != NULL);
    
    const int benchmark_count = 100000;
    int64_t start_time = get_current_timestamp();
    
    // 添加性能测试
    for (int i = 0; i < benchmark_count; i++) {
        bitmap_engine_mark_dirty(engine, i, i * 1000, get_current_timestamp());
    }
    
    int64_t add_time = get_current_timestamp() - start_time;
    printf("添加 %d 个脏块耗时: %ld ns (%.2f ops/ms)\n", 
           benchmark_count, add_time, (double)benchmark_count / (add_time / 1000000.0));
    
    // 查询性能测试
    start_time = get_current_timestamp();
    for (int i = 0; i < benchmark_count; i++) {
        EBlockState state;
        bitmap_engine_get_block_state(engine, i, &state);
    }
    
    int64_t query_time = get_current_timestamp() - start_time;
    printf("查询 %d 个块状态耗时: %ld ns (%.2f ops/ms)\n", 
           benchmark_count, query_time, (double)benchmark_count / (query_time / 1000000.0));
    
    print_test_result("性能基准测试", true);
    
    bitmap_engine_destroy(engine);
}

// 测试6: 并发安全测试
static void* concurrent_mark_thread(void* arg) {
    SBitmapEngine* engine = (SBitmapEngine*)arg;
    
    for (int i = 0; i < 1000; i++) {
        bitmap_engine_mark_dirty(engine, i, i * 1000, get_current_timestamp());
    }
    
    return NULL;
}

static void test_concurrent_safety() {
    printf("\n=== 测试6: 并发安全测试 ===\n");
    
    SBitmapEngine* engine = bitmap_engine_init();
    assert(engine != NULL);
    
    const int thread_count = 4;
    pthread_t threads[thread_count];
    
    // 创建多个线程并发标记
    for (int i = 0; i < thread_count; i++) {
        int result = pthread_create(&threads[i], NULL, concurrent_mark_thread, engine);
        assert(result == 0);
    }
    
    // 等待所有线程完成
    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
    }
    
    print_test_result("并发安全测试", true);
    
    bitmap_engine_destroy(engine);
}

// 测试7: 时间范围查询测试
static void test_time_range_query() {
    printf("\n=== 测试7: 时间范围查询测试 ===\n");
    
    SBitmapEngine* engine = bitmap_engine_init();
    assert(engine != NULL);
    
    int64_t base_time = get_current_timestamp();
    
    // 添加不同时间戳的块
    for (int i = 0; i < 100; i++) {
        bitmap_engine_mark_dirty(engine, i, i * 1000, base_time + i * 1000000);
    }
    
    // 查询时间范围内的块
    uint64_t block_ids[50];
    uint32_t count = bitmap_engine_get_dirty_blocks_by_time(engine, 
                                                           base_time + 10 * 1000000, 
                                                           base_time + 20 * 1000000,
                                                           block_ids, 50);
    
    printf("时间范围内找到 %u 个块\n", count);
    assert(count > 0);
    print_test_result("时间范围查询测试", true);
    
    bitmap_engine_destroy(engine);
}

// 测试8: WAL范围查询测试
static void test_wal_range_query() {
    printf("\n=== 测试8: WAL范围查询测试 ===\n");
    
    SBitmapEngine* engine = bitmap_engine_init();
    assert(engine != NULL);
    
    // 添加不同WAL偏移量的块
    for (int i = 0; i < 100; i++) {
        bitmap_engine_mark_dirty(engine, i, i * 1000, get_current_timestamp());
    }
    
    // 查询WAL范围内的块
    uint64_t block_ids[50];
    uint32_t count = bitmap_engine_get_dirty_blocks_by_wal(engine, 5000, 15000, block_ids, 50);
    
    printf("WAL范围内找到 %u 个块\n", count);
    assert(count > 0);
    print_test_result("WAL范围查询测试", true);
    
    bitmap_engine_destroy(engine);
}

int main() {
    printf("开始位图引擎核心功能测试...\n");
    
    test_bitmap_engine_basic();
    test_bitmap_operations();
    test_state_transitions();
    test_statistics();
    test_performance_benchmark();
    test_concurrent_safety();
    test_time_range_query();
    test_wal_range_query();
    
    printf("\n所有位图引擎核心功能测试完成！\n");
    return 0;
}
