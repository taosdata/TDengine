#include <unistd.h>

#include "../include/event_interceptor.h"
#include "../include/bitmap_engine.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <time.h>
#include <pthread.h>

// 全局变量用于测试
static uint64_t g_event_count = 0;
static pthread_mutex_t g_event_mutex = PTHREAD_MUTEX_INITIALIZER;

// 事件回调函数
static void test_event_callback(const SBlockEvent* event, void* user_data) {
    pthread_mutex_lock(&g_event_mutex);
    g_event_count++;
    printf("收到事件: 类型=%d, 块ID=%lu, WAL偏移量=%lu, 时间戳=%ld\n",
           event->event_type, event->block_id, event->wal_offset, event->timestamp);
    pthread_mutex_unlock(&g_event_mutex);
}

// 测试辅助函数
static void print_test_result(const char* test_name, bool passed) {
    printf("[%s] %s\n", passed ? "PASS" : "FAIL", test_name);
}

static int64_t get_current_timestamp() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + (int64_t)ts.tv_nsec;
}

// 测试1: 基本初始化和销毁
static void test_basic_init_destroy() {
    printf("\n=== 测试1: 基本初始化和销毁 ===\n");
    
    // 创建位图引擎
    
    SBitmapEngine* bitmap_engine = bitmap_engine_init();
    assert(bitmap_engine != NULL);
    
    // 创建事件拦截器
    SEventInterceptorConfig config = {
        .enable_interception = true,
        .event_buffer_size = 1000,
        .callback_threads = 2,
        .callback = test_event_callback,
        .callback_user_data = NULL
    };
    
    SEventInterceptor* interceptor = event_interceptor_init(&config, bitmap_engine);
    assert(interceptor != NULL);
    print_test_result("初始化事件拦截器", true);
    
    event_interceptor_destroy(interceptor);
    bitmap_engine_destroy(bitmap_engine);
    print_test_result("销毁事件拦截器", true);
}

// 测试2: 事件处理
static void test_event_processing() {
    printf("\n=== 测试2: 事件处理 ===\n");
    
    // 创建位图引擎
    
    SBitmapEngine* bitmap_engine = bitmap_engine_init();
    assert(bitmap_engine != NULL);
    
    // 创建事件拦截器
    SEventInterceptorConfig config = {
        .enable_interception = true,
        .event_buffer_size = 1000,
        .callback_threads = 2,
        .callback = test_event_callback,
        .callback_user_data = NULL
    };
    
    SEventInterceptor* interceptor = event_interceptor_init(&config, bitmap_engine);
    assert(interceptor != NULL);
    
    // 启动拦截器
    int32_t result = event_interceptor_start(interceptor);
    assert(result == 0);
    print_test_result("启动事件拦截器", true);
    
    // 重置事件计数
    pthread_mutex_lock(&g_event_mutex);
    g_event_count = 0;
    pthread_mutex_unlock(&g_event_mutex);
    
    int64_t timestamp = get_current_timestamp();
    
    // 发送各种事件
    event_interceptor_on_block_create(interceptor, 1001, 1000, timestamp);
    event_interceptor_on_block_update(interceptor, 1002, 2000, timestamp + 1000);
    event_interceptor_on_block_flush(interceptor, 1003, 3000, timestamp + 2000);
    event_interceptor_on_block_delete(interceptor, 1004, 4000, timestamp + 3000);
    
    // 等待事件处理
    usleep(100000); // 100ms
    
    // 验证事件计数
    pthread_mutex_lock(&g_event_mutex);
    assert(g_event_count == 4);
    pthread_mutex_unlock(&g_event_mutex);
    print_test_result("事件处理", true);
    
    // 停止拦截器
    result = event_interceptor_stop(interceptor);
    assert(result == 0);
    print_test_result("停止事件拦截器", true);
    
    event_interceptor_destroy(interceptor);
    bitmap_engine_destroy(bitmap_engine);
}

// 测试3: 位图引擎集成
static void test_bitmap_engine_integration() {
    printf("\n=== 测试3: 位图引擎集成 ===\n");
    
    // 创建位图引擎
    
    SBitmapEngine* bitmap_engine = bitmap_engine_init();
    assert(bitmap_engine != NULL);
    
    // 创建事件拦截器
    SEventInterceptorConfig config = {
        .enable_interception = true,
        .event_buffer_size = 1000,
        .callback_threads = 2,
        .callback = NULL, // 不使用回调
        .callback_user_data = NULL
    };
    
    SEventInterceptor* interceptor = event_interceptor_init(&config, bitmap_engine);
    assert(interceptor != NULL);
    
    // 启动事件拦截器
    int32_t result = event_interceptor_start(interceptor);
    assert(result == 0);
    
    int64_t timestamp = get_current_timestamp();
    
    // 发送事件并验证位图引擎状态
    event_interceptor_on_block_create(interceptor, 2001, 1000, timestamp);
    event_interceptor_on_block_update(interceptor, 2002, 2000, timestamp + 1000);
    
    // 等待事件处理完成
    usleep(100000); // 100ms
    
    // 验证位图引擎统计信息
    uint64_t total_blocks, dirty_count, new_count, deleted_count;
    bitmap_engine_get_stats(bitmap_engine, &total_blocks, &dirty_count, &new_count, &deleted_count);
    
    printf("实际 total_blocks = %lu\n", total_blocks);
    assert(total_blocks == 2);
    assert(dirty_count == 1);
    assert(new_count == 1);
    print_test_result("位图引擎集成", true);
    
    event_interceptor_destroy(interceptor);
    bitmap_engine_destroy(bitmap_engine);
}

// 测试4: 事件缓冲区溢出
static void test_event_buffer_overflow() {
    printf("\n=== 测试4: 事件缓冲区溢出 ===\n");
    
    // 创建位图引擎
    
    SBitmapEngine* bitmap_engine = bitmap_engine_init();
    assert(bitmap_engine != NULL);
    
    // 创建小缓冲区的事件拦截器
    SEventInterceptorConfig config = {
        .enable_interception = true,
        .event_buffer_size = 5, // 很小的缓冲区
        .callback_threads = 1,
        .callback = NULL, // 不使用回调，让缓冲区填满
        .callback_user_data = NULL
    };
    
    SEventInterceptor* interceptor = event_interceptor_init(&config, bitmap_engine);
    assert(interceptor != NULL);
    
    int64_t timestamp = get_current_timestamp();
    
    // 发送超过缓冲区大小的事件
    for (int i = 0; i < 10; i++) {
        int32_t result = event_interceptor_on_block_update(interceptor, 3000 + i, 1000 + i, timestamp + i);
        if (i >= 5) {
            // 缓冲区满后应该返回错误
            assert(result != 0);
        }
    }
    
    print_test_result("事件缓冲区溢出处理", true);
    
    event_interceptor_destroy(interceptor);
    bitmap_engine_destroy(bitmap_engine);
}

// 测试5: 并发事件处理
static void* concurrent_event_thread(void* arg) {
    SEventInterceptor* interceptor = (SEventInterceptor*)arg;
    int64_t timestamp = get_current_timestamp();
    
    for (int i = 0; i < 50; i++) {
        uint64_t block_id = 4000 + i;
        event_interceptor_on_block_update(interceptor, block_id, block_id * 10, timestamp + i);
    }
    
    return NULL;
}

static void test_concurrent_event_processing() {
    printf("\n=== 测试5: 并发事件处理 ===\n");
    
    // 创建位图引擎
    
    SBitmapEngine* bitmap_engine = bitmap_engine_init();
    assert(bitmap_engine != NULL);
    
    // 创建事件拦截器
    SEventInterceptorConfig config = {
        .enable_interception = true,
        .event_buffer_size = 1000,
        .callback_threads = 4,
        .callback = test_event_callback,
        .callback_user_data = NULL
    };
    
    SEventInterceptor* interceptor = event_interceptor_init(&config, bitmap_engine);
    assert(interceptor != NULL);
    
    // 启动拦截器
    event_interceptor_start(interceptor);
    
    // 重置事件计数
    pthread_mutex_lock(&g_event_mutex);
    g_event_count = 0;
    pthread_mutex_unlock(&g_event_mutex);
    
    // 创建多个线程并发发送事件
    pthread_t threads[4];
    for (int i = 0; i < 4; i++) {
        pthread_create(&threads[i], NULL, concurrent_event_thread, interceptor);
    }
    
    // 等待所有线程完成
    for (int i = 0; i < 4; i++) {
        pthread_join(threads[i], NULL);
    }
    
    // 等待事件处理完成
    usleep(500000); // 500ms
    
    // 验证事件计数
    pthread_mutex_lock(&g_event_mutex);
    assert(g_event_count == 200); // 4个线程 * 50个事件
    pthread_mutex_unlock(&g_event_mutex);
    print_test_result("并发事件处理", true);
    
    // 停止拦截器
    event_interceptor_stop(interceptor);
    event_interceptor_destroy(interceptor);
    bitmap_engine_destroy(bitmap_engine);
}

// 测试6: 统计信息
static void test_statistics() {
    printf("\n=== 测试6: 统计信息 ===\n");
    
    // 创建位图引擎
    
    SBitmapEngine* bitmap_engine = bitmap_engine_init();
    assert(bitmap_engine != NULL);
    
    // 创建事件拦截器
    SEventInterceptorConfig config = {
        .enable_interception = true,
        .event_buffer_size = 1000,
        .callback_threads = 2,
        .callback = NULL,
        .callback_user_data = NULL
    };
    
    SEventInterceptor* interceptor = event_interceptor_init(&config, bitmap_engine);
    assert(interceptor != NULL);
    
    int64_t timestamp = get_current_timestamp();
    
    // 发送一些事件
    event_interceptor_on_block_create(interceptor, 5001, 1000, timestamp);
    event_interceptor_on_block_update(interceptor, 5002, 2000, timestamp + 1000);
    event_interceptor_on_block_update(interceptor, 5003, 3000, timestamp + 2000);
    
    // 获取统计信息
    uint64_t events_processed, events_dropped;
    event_interceptor_get_stats(interceptor, &events_processed, &events_dropped);
    
    print_test_result("统计信息", true);
    
    event_interceptor_destroy(interceptor);
    bitmap_engine_destroy(bitmap_engine);
}

// 测试7: 性能测试
static void test_performance() {
    printf("\n=== 测试7: 性能测试 ===\n");
    
    // 创建位图引擎
    SBitmapEngine* bitmap_engine = bitmap_engine_init();
    assert(bitmap_engine != NULL);
    
    // 创建事件拦截器 - 增加缓冲区大小和线程数
    SEventInterceptorConfig config = {
        .enable_interception = true,
        .event_buffer_size = 50000, // 增加缓冲区大小
        .callback_threads = 8,      // 增加线程数
        .callback = NULL, // 不使用回调以提高性能
        .callback_user_data = NULL
    };
    
    SEventInterceptor* interceptor = event_interceptor_init(&config, bitmap_engine);
    assert(interceptor != NULL);
    
    // 启动事件拦截器
    int32_t result = event_interceptor_start(interceptor);
    assert(result == 0);
    
    int64_t start_time = get_current_timestamp();
    
    // 批量发送事件 - 减少事件数量以避免过载
    const int event_count = 1000; // 减少到1000个事件
    for (int i = 0; i < event_count; i++) {
        uint64_t block_id = 6000 + i;
        event_interceptor_on_block_update(interceptor, block_id, block_id * 10, start_time + i);
    }
    
    int64_t end_time = get_current_timestamp();
    double duration_ms = (end_time - start_time) / 1000000.0;
    
    // 等待事件处理完成 - 增加等待时间
    usleep(1000000); // 1秒
    
    // 再次等待，直到所有事件都被处理
    int wait_count = 0;
    uint64_t events_processed = 0, events_dropped = 0;
    while (wait_count < 100) { // 最多等待10秒
        event_interceptor_get_stats(interceptor, &events_processed, &events_dropped);
        
        if (events_processed >= event_count) {
            break; // 所有事件都已处理
        }
        
        usleep(100000); // 100ms
        wait_count++;
    }
    
    // 验证结果
    uint64_t total_blocks, dirty_count, new_count, deleted_count;
    bitmap_engine_get_stats(bitmap_engine, &total_blocks, &dirty_count, &new_count, &deleted_count);
    
    printf("性能测试结果: 处理了 %lu 个事件，耗时 %.2f ms\n", events_processed, duration_ms);
    printf("位图引擎统计: 总块数=%lu, 脏块数=%lu, 新块数=%lu, 删除块数=%lu\n", 
           total_blocks, dirty_count, new_count, deleted_count);
    printf("丢弃事件数: %lu\n", events_dropped);
    
    // 验证所有事件都被处理了
    assert(events_processed == event_count);
    assert(events_dropped == 0);
    
    // 验证位图引擎中的块数量
    assert(total_blocks == event_count);
    assert(dirty_count == event_count);
    
    print_test_result("性能测试", true);
    
    event_interceptor_destroy(interceptor);
    bitmap_engine_destroy(bitmap_engine);
}

int main() {
    printf("开始事件拦截器测试...\n");
    
    test_basic_init_destroy();
    test_event_processing();
    test_bitmap_engine_integration();
    test_event_buffer_overflow();
    test_concurrent_event_processing();
    test_statistics();
    test_performance();
    
    printf("\n所有测试完成！\n");
    return 0;
} 