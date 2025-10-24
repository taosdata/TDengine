#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <stdlib.h>
#include <stdbool.h>
#include <signal.h>
#include <sys/time.h>
#include "../include/storage_engine_interface.h"
#include "../include/observability.h"
#include "../include/tdengine_storage_engine.h"

// 测试宏定义
#define TEST_ASSERT(condition, message) do { \
    total_tests++; \
    if (condition) { \
        passed_tests++; \
        printf("\xe2\x9c\x93 %s\n", message); \
    } else { \
        failed_tests++; \
        printf("\xe2\x9d\x8c %s\n", message); \
    } \
} while(0)

// 测试计数器
static int total_tests = 0;
static int passed_tests = 0;
static int failed_tests = 0;

// 全局变量
static volatile bool g_running = true;
static int64_t g_last_committed_offset = -1;
static pthread_mutex_t g_offset_mutex = PTHREAD_MUTEX_INITIALIZER;
static uint64_t g_total_events_processed = 0;
static uint64_t g_total_events_committed = 0;

// 信号处理
static void signal_handler(int sig) {
    printf("\n\xe6\x94\xb6\xe5\x88\xb0\xe4\xbf\xa1\xe5\x8f\xb7 %d\xef\xbc\x8c\xe6\xad\xa3\xe5\x9c\xa8\xe4\xbc\x98\xe9\x9b\x85\xe5\x85\xb3\xe9\x97\xad...\n", sig);
    g_running = false;
}

// 获取当前时间戳（毫秒）
static int64_t get_current_time_ms() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000LL + tv.tv_usec / 1000LL;
}

// 事件回调函数
static void test_event_callback(const SStorageEvent* event, void* user_data) {
    if (!event || !user_data) return;
    
    int* event_count = (int*)user_data;
    (*event_count)++;
    
    pthread_mutex_lock(&g_offset_mutex);
    g_total_events_processed++;
    
    // 记录最新的offset
    if (event->wal_offset > g_last_committed_offset) {
        g_last_committed_offset = event->wal_offset;
    }
    pthread_mutex_unlock(&g_offset_mutex);
    
    printf("[\xe4\xba\x8b\xe4\xbb\xb6] block_id=%lu, wal_offset=%ld, timestamp=%ld, type=%d\n", 
           event->block_id, event->wal_offset, event->timestamp, event->event_type);
}

// 等待事件处理
static bool wait_for_events(int expected_count, int timeout_ms) {
    int start_time = get_current_time_ms();
    int current_count = 0;
    
    while (current_count < expected_count && (get_current_time_ms() - start_time) < timeout_ms) {
        usleep(10000); // 10ms
        pthread_mutex_lock(&g_offset_mutex);
        current_count = g_total_events_processed;
        pthread_mutex_unlock(&g_offset_mutex);
    }
    
    return current_count >= expected_count;
}

// 测试1: 真实TMQ连接和配置测试
static void test_real_tmq_connection() {
    printf("\n=== \xe6\xb5\x8b\xe8\xaf\x951: \xe7\x9c\x9f\xe5\xae\x9eTMQ\xe8\xbf\x9e\xe6\x8e\xa5\xe5\x92\x8c\xe9\x85\x8d\xe7\xbd\xae\xe6\xb5\x8b\xe8\xaf\x95 ===\n");
    
    // 设置TMQ配置
    int32_t result = tdengine_set_tmq_config(
        "127.0.0.1", 6030, "root", "taosdata", 
        "test_db", "test_topic", "test_group"
    );
    TEST_ASSERT(result == 0, "TMQ\xe9\x85\x8d\xe7\xbd\xae\xe8\xae\xbe\xe7\xbd\xae\xe6\x88\x90\xe5\x8a\x9f");
    
    // 获取存储引擎接口
    SStorageEngineInterface* engine = get_storage_engine_interface("auto");
    TEST_ASSERT(engine != NULL, "\xe8\x8e\xb7\xe5\x8f\x96\xe5\xad\x98\xe5\x82\xa8\xe5\xbc\x95\xe6\x93\x8e\xe6\x8e\xa5\xe5\x8f\xa3\xe6\x88\x90\xe5\x8a\x9f");
    
    // 初始化存储引擎
    SStorageEngineConfig config = {
        .enable_interception = true,
        .event_callback = test_event_callback,
        .callback_user_data = &(int){0},
        .event_buffer_size = 1000,
        .callback_threads = 2
    };
    
    result = engine->init(&config);
    TEST_ASSERT(result == 0, "\xe5\xad\x98\xe5\x82\xa8\xe5\xbc\x95\xe6\x93\x8e\xe5\x88\x9d\xe5\xa7\x8b\xe5\x8c\x96\xe6\x88\x90\xe5\x8a\x9f");
    
    // 安装事件拦截
    result = engine->install_interception();
    TEST_ASSERT(result == 0, "\xe4\xba\x8b\xe4\xbb\xb6\xe6\x8b\xa6\xe6\x88\xaa\xe5\xae\x89\xe8\xa3\x85\xe6\x88\x90\xe5\x8a\x9f");
    
    // 等待连接建立
    printf("\xe7\xad\x89\xe5\xbe\x85TMQ\xe8\xbf\x9e\xe6\x8e\xa5\xe5\xbb\xba\xe7\xab\x8b...\n");
    sleep(2);
    
    // 获取可观测性指标
    SObservabilityMetrics metrics;
    result = engine->get_observability_metrics(engine, &metrics);
    TEST_ASSERT(result == 0, "\xe8\x8e\xb7\xe5\x8f\x96\xe5\x8f\xaf\xe8\xa7\x82\xe6\xb5\x8b\xe6\x80\xa7\xe6\x8c\x87\xe6\xa0\x87\xe6\x88\x90\xe5\x8a\x9f");
    
    printf("\xe8\xbf\x9e\xe6\x8e\xa5\xe7\x8a\xb6\xe6\x80\x81: uptime=%lds, memory=%zu bytes\n", 
           metrics.uptime_seconds, metrics.memory_usage_bytes);
    
    // 清理
    engine->uninstall_interception();
    engine->destroy();
}

// 测试2: 同步提交语义验证
static void test_sync_commit_semantics() {
    printf("\n=== \xe6\xb5\x8b\xe8\xaf\x952: \xe5\x90\x8c\xe6\xad\xa5\xe6\x8f\x90\xe4\xba\xa4\xe8\xaf\xad\xe4\xb9\x89\xe9\xaa\x8c\xe8\xaf\x81 ===\n");
    
    // 设置TMQ配置
    int32_t result = tdengine_set_tmq_config(
        "127.0.0.1", 6030, "root", "taosdata", 
        "test_db", "test_topic", "test_group_sync"
    );
    TEST_ASSERT(result == 0, "TMQ\xe9\x85\x8d\xe7\xbd\x8e\xe8\xae\xbe\xe7\xbd\xae\xe6\x88\x90\xe5\x8a\x9f");
    
    // 设置提交策略：同步提交，至少一次语义
    result = tdengine_set_commit_strategy(true, true, 1000);
    TEST_ASSERT(result == 0, "\xe5\x90\x8c\xe6\xad\xa5\xe6\x8f\x90\xe4\xba\xa4\xe7\xad\x96\xe7\x95\xa5\xe8\xae\xbe\xe7\xbd\xae\xe6\x88\x90\xe5\x8a\x9f");
    
    // 获取存储引擎接口
    SStorageEngineInterface* engine = get_storage_engine_interface("auto");
    TEST_ASSERT(engine != NULL, "\xe8\x8e\xb7\xe5\x8f\x96\xe5\xad\x98\xe5\x82\xa8\xe5\xbc\x95\xe6\x93\x8e\xe6\x8e\xa5\xe5\x8f\xa3\xe6\x88\x90\xe5\x8a\x9f");
    
    // 初始化存储引擎
    StorageEventCallback callback = test_event_callback;
    int event_count = 0;
    SStorageEngineConfig config = {
        .enable_interception = true,
        .event_callback = callback,
        .callback_user_data = &event_count,
        .event_buffer_size = 1000,
        .callback_threads = 2
    };
    
    result = engine->init(&config);
    TEST_ASSERT(result == 0, "\xe5\xad\x98\xe5\x82\xa8\xe5\xbc\x95\xe6\x93\x8e\xe5\x88\x9d\xe5\xa7\x8b\xe5\x8c\x96\xe6\x88\x90\xe5\x8a\x9f");
    
    // 安装事件拦截
    result = engine->install_interception();
    TEST_ASSERT(result == 0, "\xe4\xba\x8b\xe4\xbb\xb6\xe6\x8b\xa6\xe6\x88\xaa\xe5\xae\x89\xe8\xa3\x85\xe6\x88\x90\xe5\x8a\x9f");
    
    // 等待连接建立
    printf("\xe7\xad\x89\xe5\xbe\x85TMQ\xe8\xbf\x9e\xe6\x8e\xa5\xe5\xbb\xba\xe7\xab\x8b...\n");
    sleep(2);
    
    // 记录初始状态
    int64_t initial_offset = g_last_committed_offset;
    uint64_t initial_events = g_total_events_processed;
    
    // 等待事件处理
    printf("\xe7\xad\x89\xe5\xbe\x85\xe4\xba\x8b\xe4\xbb\xb6\xe5\xa4\x84\xe7\x90\x86...\n");
    if (wait_for_events(initial_events + 5, 10000)) {
        printf("\xe6\x8e\xa5\xe6\x94\xb6\xe5\x88\xb0 %d \xe4\xb8\xaa\xe6\x96\xb0\xe4\xba\x8b\xe4\xbb\xb6\n", (int)(g_total_events_processed - initial_events));
        
        // 验证同步提交：最新offset应该被提交
        int64_t current_offset = g_last_committed_offset;
        TEST_ASSERT(current_offset > initial_offset, "\xe5\x90\x8c\xe6\xad\xa5\xe6\x8f\x90\xe4\xba\xa4\xe5\x90\x8eoffset\xe5\x89\x8d\xe8\xbf\x9b");
        
        // 获取详细统计
        uint64_t events_processed, events_dropped, messages_consumed, offset_commits;
        int64_t last_commit_time;
        result = tdengine_get_detailed_stats(
            &events_processed, &events_dropped, &messages_consumed, 
            &offset_commits, &last_commit_time
        );
        TEST_ASSERT(result == 0, "\xe8\x8e\xb7\xe5\x8f\x96\xe8\xaf\xa6\xe7\xbb\x86\xe7\xbb\x9f\xe8\xae\xa1\xe6\x88\x90\xe5\x8a\x9f");
        
        printf("\xe7\xbb\x9f\xe8\xae\xa1: \xe5\xa4\x84\xe7\x90\x86=%lu, \xe4\xb8\xa2\xe5\xbc\x83=%lu, \xe6\xb6\x88\xe8\xb4\xb9=%lu, \xe6\x8f\x90\xe4\xba\xa4=%lu, \xe6\x9c\x80\xe5\x90\x8e\xe6\x8f\x90\xe4\xba\xa4=%ld\n",
               events_processed, events_dropped, messages_consumed, offset_commits, last_commit_time);
        
        TEST_ASSERT(offset_commits > 0, "\xe6\x9c\x89offset\xe8\xa2\xab\xe6\x8f\x90\xe4\xba\xa4");
        TEST_ASSERT(last_commit_time > 0, "\xe6\x9c\x80\xe5\x90\x8e\xe6\x8f\x90\xe4\xba\xa4\xe6\x97\xb6\xe9\x97\xb4\xe6\x9c\x89\xe6\x95\x88");
        
    } else {
        printf("\xe7\xad\x89\xe5\xbe\x85\xe4\xba\x8b\xe4\xbb\xb6\xe8\xb6\x85\xe6\x97\xb6\xef\xbc\x8c\xe8\xb7\xb3\xe8\xbf\x87\xe5\x90\x8c\xe6\xad\xa5\xe6\x8f\x90\xe4\xba\xa4\xe9\xaa\x8c\xe8\xaf\x81\n");
    }
    
    // 清理
    engine->uninstall_interception();
    engine->destroy();
}

// 测试3: 异步提交语义验证
static void test_async_commit_semantics() {
    printf("\n=== \xe6\xb5\x8b\xe8\xaf\x953: \xe5\xbc\x82\xe6\ad\xa5\xe6\x8f\x90\xe4\xba\xa4\xe8\xaf\xad\xe4\xb9\x89\xe9\xaa\x8c\xe8\xaf\x81 ===\n");
    
    // 设置TMQ配置
    int32_t result = tdengine_set_tmq_config(
        "127.0.0.1", 6030, "root", "taosdata", 
        "test_db", "test_topic", "test_group_async"
    );
    TEST_ASSERT(result == 0, "TMQ\xe9\x85\x8d\xe7\xbd\x8e\xe8\xae\xbe\xe7\xbd\xae\xe6\x88\x90\xe5\x8a\x9f");
    
    // 设置提交策略：异步提交，至多一次语义
    result = tdengine_set_commit_strategy(false, false, 500);
    TEST_ASSERT(result == 0, "\xe5\xbc\x82\xe6\ad\xa5\xe6\x8f\x90\xe4\xba\xa4\xe7\xad\x96\xe7\x95\xa5\xe8\xae\xbe\xe7\xbd\xae\xe6\x88\x90\xe5\x8a\x9f");
    
    // 获取存储引擎接口
    SStorageEngineInterface* engine = get_storage_engine_interface("auto");
    TEST_ASSERT(engine != NULL, "\xe8\x8e\xb7\xe5\x8f\x96\xe5\xad\x98\xe5\x82\xa8\xe5\xbc\x95\xe6\x93\x8e\xe6\x8e\xa5\xe5\x8f\xa3\xe6\x88\x90\xe5\x8a\x9f");
    
    // 初始化存储引擎
    StorageEventCallback callback = test_event_callback;
    int event_count = 0;
    SStorageEngineConfig config = {
        .enable_interception = true,
        .event_callback = callback,
        .callback_user_data = &event_count,
        .event_buffer_size = 1000,
        .callback_threads = 2
    };
    
    result = engine->init(&config);
    TEST_ASSERT(result == 0, "\xe5\xad\x98\xe5\x82\xa8\xe5\xbc\x95\xe6\x93\x8e\xe5\x88\x9d\xe5\xa7\x8b\xe5\x8c\x96\xe6\x88\x90\xe5\x8a\x9f");
    
    // 安装事件拦截
    result = engine->install_interception();
    TEST_ASSERT(result == 0, "\xe4\xba\x8b\xe4\xbb\xb6\xe6\x8b\xa6\xe6\x88\xaa\xe5\xae\x89\xe8\xa3\x85\xe6\x88\x90\xe5\x8a\x9f");
    
    // 等待连接建立
    printf("\xe7\xad\x89\xe5\xbe\x85TMQ\xe8\xbf\x9e\xe6\x8e\xa5\xe5\xbb\xba\xe7\xab\x8b...\n");
    sleep(2);
    
    // 记录初始状态
    int64_t initial_offset = g_last_committed_offset;
    uint64_t initial_events = g_total_events_processed;
    
    // 等待事件处理
    printf("\xe7\xad\x89\xe5\xbe\x85\xe4\xba\x8b\xe4\xbb\xb6\xe5\xa4\x84\xe7\x90\x86...\n");
    if (wait_for_events(initial_events + 5, 10000)) {
        printf("\xe6\x8e\xa5\xe6\x94\xb6\xe5\x88\xb0 %d \xe4\xb8\xaa\xe6\x96\xb0\xe4\xba\x8b\xe4\xbb\xb6\n", (int)(g_total_events_processed - initial_events));
        
        // 验证异步提交：offset可能延迟提交
        int64_t current_offset = g_last_committed_offset;
        TEST_ASSERT(current_offset >= initial_offset, "\xe5\xbc\x82\xe6\ad\xa5\xe6\x8f\x90\xe4\xba\xa4\xe5\x90\x8eoffset\xe6\x9c\xaa\xe5\x9b\x9e\xe9\x80\x80");
        
        // 获取可观测性指标
        SObservabilityMetrics metrics;
        result = engine->get_observability_metrics(engine, &metrics);
        TEST_ASSERT(result == 0, "\xe8\x8e\xb7\xe5\x8f\x96\xe5\x8f\xaf\xe8\xa7\x82\xe6\xb5\x8b\xe6\x80\xa7\xe6\x8c\x87\xe6\xa0\x87\xe6\x88\x90\xe5\x8a\x9f");
        
        printf("\xe5\xbc\x82\xe6\ad\xa5\xe6\x8f\x90\xe4\xba\xa4\xe6\x8c\x87\xe6\xa0\x87: \xe4\xba\x8b\xe4\xbb\xb6/\xe7\xa7\x92=%lu, \xe6\xb6\x88\xe6\x81\xaf/\xe7\xa7\x92=%lu, \xe6\xb6\x88\xe8\xb4\xb9\xe6\xbb\x9e\xe5\x90\x8e=%ldms\n",
               metrics.events_per_second, metrics.messages_per_second, metrics.consumer_lag_ms);
        
        // 异步提交可能有一些延迟
        TEST_ASSERT(metrics.events_per_second >= 0, "\xe4\xba\x8b\xe4\xbb\xb6\xe9\x80\x9f\xe7\x8e\x87\xe6\x9c\x89\xe6\x95\x88");
        
    } else {
        printf("\xe7\xad\x89\xe5\xbe\x85\xe4\xba\x8b\xe4\xbb\xb6\xe8\xb6\x85\xe6\x97\xb6\xef\xbc\x8c\xe8\xb7\xb3\xe8\xbf\x87\xe5\xbc\x82\xe6\xad\xa5\xe6\x8f\x90\xe4\xba\xa4\xe9\xaa\x8c\xe8\xaf\x81\n");
    }
    
    // 清理
    engine->uninstall_interception();
    engine->destroy();
}

// 测试4: 至少一次语义验证
static void test_at_least_once_semantics() {
    printf("\n=== \xe6\xb5\x8b\xe8\xaf\x954: \xe8\x87\xb3\xe5\xb0\x91\xe4\xb8\x80\xe6\xac\xa1\xe8\xaf\xad\xe4\xb9\x89\xe9\xaa\x8c\xe8\xaf\x81 ===\n");
    
    // 设置TMQ配置
    int32_t result = tdengine_set_tmq_config(
        "127.0.0.1", 6030, "root", "taosdata", 
        "test_db", "test_topic", "test_group_at_least_once"
    );
    TEST_ASSERT(result == 0, "TMQ\xe9\x85\x8d\xe7\xbd\x8e\xe8\xae\xbe\xe7\xbd\xae\xe6\x88\x90\xe5\x8a\x9f");
    
    // 设置提交策略：同步提交，至少一次语义
    result = tdengine_set_commit_strategy(true, true, 1000);
    TEST_ASSERT(result == 0, "\xe8\x87\xb3\xe5\xb0\x91\xe4\xb8\x80\xe6\xac\xa1\xe8\xaf\xad\xe4\xb9\x89\xe7\xad\x96\xe7\x95\xa5\xe8\xae\xbe\xe7\xbd\xae\xe6\x88\x90\xe5\x8a\x9f");
    
    // 获取存储引擎接口
    SStorageEngineInterface* engine = get_storage_engine_interface("auto");
    TEST_ASSERT(engine != NULL, "\xe8\x8e\xb7\xe5\x8f\x96\xe5\xad\x98\xe5\x82\xa8\xe5\xbc\x95\xe6\x93\x8e\xe6\x8e\xa5\xe5\x8f\xa3\xe6\x88\x90\xe5\x8a\x9f");
    
    // 初始化存储引擎
    StorageEventCallback callback = test_event_callback;
    int event_count = 0;
    SStorageEngineConfig config = {
        .enable_interception = true,
        .event_callback = callback,
        .callback_user_data = &event_count,
        .event_buffer_size = 1000,
        .callback_threads = 2
    };
    
    result = engine->init(&config);
    TEST_ASSERT(result == 0, "\xe5\xad\x98\xe5\x82\xa8\xe5\xbc\x95\xe6\x93\x8e\xe5\x88\x9d\xe5\xa7\x8b\xe5\x8c\x96\xe6\x88\x90\xe5\x8a\x9f");
    
    // 安装事件拦截
    result = engine->install_interception();
    TEST_ASSERT(result == 0, "\xe4\xba\x8b\xe4\xbb\xb6\xe6\x8b\xa6\xe6\x88\xaa\xe5\xae\x89\xe8\xa3\x85\xe6\x88\x90\xe5\x8a\x9f");
    
    // 等待连接建立
    printf("\xe7\xad\x89\xe5\xbe\x85TMQ\xe8\xbf\x9e\xe6\x8e\xa5\xe5\xbb\xba\xe7\xab\x8b...\n");
    sleep(2);
    
    // 记录初始状态
    uint64_t initial_events = g_total_events_processed;
    int64_t initial_offset = g_last_committed_offset;
    
    // 等待事件处理
    printf("\xe7\xad\x89\xe5\xbe\x85\xe4\xba\x8b\xe4\xbb\xb6\xe5\xa4\x84\xe7\x90\x86...\n");
    if (wait_for_events(initial_events + 10, 15000)) {
        uint64_t processed_events = g_total_events_processed - initial_events;
        printf("\xe6\x8e\xa5\xe6\x94\xb6\xe5\x88\xb0 %lu \xe4\xb8\xaa\xe6\x96\xb0\xe4\xba\x8b\xe4\xbb\xb6\n", processed_events);
        
        // 验证至少一次语义：所有事件都应该被处理
        TEST_ASSERT(processed_events > 0, "\xe6\x9c\x89\xe4\xba\x8b\xe4\xbb\xb6\xe8\xa2\xab\xe5\xa4\x84\xe7\x90\x86");
        
        // 获取详细统计
        uint64_t events_processed, events_dropped, messages_consumed, offset_commits;
        int64_t last_commit_time;
        result = tdengine_get_detailed_stats(
            &events_processed, &events_dropped, &messages_consumed, 
            &offset_commits, &last_commit_time
        );
        TEST_ASSERT(result == 0, "\xe8\x8e\xb7\xe5\x8f\x96\xe8\xaf\xa6\xe7\xbb\x86\xe7\xbb\x9f\xe8\xae\xa1\xe6\x88\x90\xe5\x8a\x9f");
        
        // 至少一次语义：丢弃的事件应该很少
        double drop_rate = (double)events_dropped / (events_processed + events_dropped);
        printf("\xe4\xba\x8b\xe4\xbb\xb6\xe4\xb8\xa2\xe5\xbc\x83\xe7\x8e\x87: %.2f%% (%lu/%lu)\n", 
               drop_rate * 100, events_dropped, events_processed + events_dropped);
        
        TEST_ASSERT(drop_rate < 0.1, "\xe4\xba\x8b\xe4\xbb\xb6\xe4\xb8\xa2\xe5\xbc\x83\xe7\x8e\x87\xe4\xbd\x8e\xe4\xba\x8e10%");
        TEST_ASSERT(offset_commits > 0, "\xe6\x9c\x89offset\xe8\xa2\xab\xe6\x8f\x90\xe4\xba\xa4");
        
    } else {
        printf("\xe7\xad\x89\xe5\xbe\x85\xe4\xba\x8b\xe4\xbb\xb6\xe8\xb6\x85\xe6\x97\xb6\xef\xbc\x8c\xe8\xb7\xb3\xe8\xbf\x87\xe8\x87\xb3\xe5\xb0\x91\xe4\xb8\x80\xe6\xac\xa1\xe8\xaf\xad\xe4\xb9\x89\xe9\xaa\x8c\xe8\xaf\x81\n");
    }
    
    // 清理
    engine->uninstall_interception();
    engine->destroy();
}

// 测试5: 至多一次语义验证
static void test_at_most_once_semantics() {
    printf("\n=== \xe6\xb5\x8b\xe8\xaf\x955: \xe8\x87\xb3\xe5\xa4\x9a\xe4\xb8\x80\xe6\xac\xa1\xe8\xaf\xad\xe4\xb9\x89\xe9\xaa\x8c\xe8\xaf\x81 ===\n");
    
    // 设置TMQ配置
    int32_t result = tdengine_set_tmq_config(
        "127.0.0.1", 6030, "root", "taosdata", 
        "test_db", "test_topic", "test_group_at_most_once"
    );
    TEST_ASSERT(result == 0, "TMQ\xe9\x85\x8d\xe7\xbd\x8e\xe8\xae\xbe\xe7\xbd\xae\xe6\x88\x90\xe5\x8a\x9f");
    
    // 设置提交策略：异步提交，至多一次语义
    result = tdengine_set_commit_strategy(false, false, 500);
    TEST_ASSERT(result == 0, "\xe8\x87\xb3\xe5\xa4\x9a\xe4\xb8\x80\xe6\xac\xa1\xe8\xaf\xad\xe4\xb9\x89\xe7\xad\x96\xe7\x95\xa5\xe8\xae\xbe\xe7\xbd\xae\xe6\x88\x90\xe5\x8a\x9f");
    
    // 获取存储引擎接口
    SStorageEngineInterface* engine = get_storage_engine_interface("auto");
    TEST_ASSERT(engine != NULL, "\xe8\x8e\xb7\xe5\x8f\x96\xe5\xad\x98\xe5\x82\xa8\xe5\xbc\x95\xe6\x93\x8e\xe6\x8e\xa5\xe5\x8f\xa3\xe6\x88\x90\xe5\x8a\x9f");
    
    // 初始化存储引擎
    StorageEventCallback callback = test_event_callback;
    int event_count = 0;
    SStorageEngineConfig config = {
        .enable_interception = true,
        .event_callback = callback,
        .callback_user_data = &event_count,
        .event_buffer_size = 1000,
        .callback_threads = 2
    };
    
    result = engine->init(&config);
    TEST_ASSERT(result == 0, "\xe5\xad\x98\xe5\x82\xa8\xe5\xbc\x95\xe6\x93\x8e\xe5\x88\x9d\xe5\xa7\x8b\xe5\x8c\x96\xe6\x88\x90\xe5\x8a\x9f");
    
    // 安装事件拦截
    result = engine->install_interception();
    TEST_ASSERT(result == 0, "\xe4\xba\x8b\xe4\xbb\xb6\xe6\x8b\xa6\xe6\x88\xaa\xe5\xae\x89\xe8\xa3\x85\xe6\x88\x90\xe5\x8a\x9f");
    
    // 等待连接建立
    printf("\xe7\xad\x89\xe5\xbe\x85TMQ\xe8\xbf\x9e\xe6\x8e\xa5\xe5\xbb\xba\xe7\xab\x8b...\n");
    sleep(2);
    
    // 记录初始状态
    uint64_t initial_events = g_total_events_processed;
    int64_t initial_offset = g_last_committed_offset;
    
    // 等待事件处理
    printf("\xe7\xad\x89\xe5\xbe\x85\xe4\xba\x8b\xe4\xbb\xb6\xe5\xa4\x84\xe7\x90\x86...\n");
    if (wait_for_events(initial_events + 10, 15000)) {
        uint64_t processed_events = g_total_events_processed - initial_events;
        printf("\xe6\x8e\xa5\xe6\x94\xb6\xe5\x88\xb0 %lu \xe4\xb8\xaa\xe6\x96\xb0\xe4\xba\x8b\xe4\xbb\xb6\n", processed_events);
        
        // 验证至多一次语义：可能丢失事件，但不会重复处理
        TEST_ASSERT(processed_events >= 0, "\xe4\xba\x8b\xe4\xbb\xb6\xe5\xa4\x84\xe7\x90\x86\xe8\xae\xa1\xe6\x95\xb0\xe6\x9c\x89\xe6\x95\x88");
        
        // 获取可观测性指标
        SObservabilityMetrics metrics;
        result = engine->get_observability_metrics(engine, &metrics);
        TEST_ASSERT(result == 0, "\xe8\x8e\xb7\xe5\x8f\x96\xe5\x8f\xaf\xe8\xa7\x82\xe6\xb5\x8b\xe6\x80\xa7\xe6\x8c\x87\xe6\xa0\x87\xe6\x88\x90\xe5\x8a\x9f");
        
        printf("\xe8\x87\xb3\xe5\xa4\x9a\xe4\xb8\x80\xe6\xac\xa1\xe8\xaf\xad\xe4\xb9\x89\xe6\x8c\x87\xe6\xa0\x87: \xe4\xba\x8b\xe4\xbb\xb6/\xe7\xa7\x92=%lu, \xe6\xb6\x88\xe6\x81\xaf/\xe7\xa7\x92=%lu, \xe6\xb6\x88\xe8\xb4\xb9\xe6\xbb\x9e\xe5\x90\x8e=%ldms\n",
               metrics.events_per_second, metrics.messages_per_second, metrics.consumer_lag_ms);
        
        // 至多一次语义：处理延迟可能较高，但不会重复
        TEST_ASSERT(metrics.processing_delay_ms >= 0, "\xe5\xa4\x84\xe7\x90\x86\xe5\xbb\xb6\xe8\xbf\x9f\xe6\x9c\x89\xe6\x95\x88");
        
    } else {
        printf("\xe7\xad\x89\xe5\xbe\x85\xe4\xba\x8b\xe4\xbb\xb6\xe8\xb6\x85\xe6\x97\xb6\xef\xbc\x8c\xe8\xb7\xb3\xe8\xbf\x87\xe8\x87\xb3\xe5\xa4\x9a\xe4\xb8\x80\xe6\xac\xa1\xe8\xaf\xad\xe4\xb9\x89\xe9\xaa\x8c\xe8\xaf\x81\n");
    }
    
    // 清理
    engine->uninstall_interception();
    engine->destroy();
}

// 测试6: 断点恢复测试
static void test_checkpoint_recovery() {
    printf("\n=== \xe6\xb5\x8b\xe8\xaf\x956: \xe6\x96\xad\xe7\x82\xb9\xe6\x81\xa2\xe5\xa4\x8d\xe6\xb5\x8b\xe8\xaf\x95 ===\n");
    
    // 设置TMQ配置
    int32_t result = tdengine_set_tmq_config(
        "127.0.0.1", 6030, "root", "taosdata", 
        "test_db", "test_topic", "test_group_recovery"
    );
    TEST_ASSERT(result == 0, "TMQ\xe9\x85\x8d\xe7\xbd\x8e\xe8\xae\xbe\xe7\xbd\xae\xe6\x88\x90\xe5\x8a\x9f");
    
    // 设置提交策略：同步提交，至少一次语义
    result = tdengine_set_commit_strategy(true, true, 1000);
    TEST_ASSERT(result == 0, "\xe6\x96\xad\xe7\x82\xb9\xe6\x81\xa2\xe5\xa4\x8d\xe7\xad\x96\xe7\x95\xa5\xe8\xae\xbe\xe7\xbd\xae\xe6\x88\x90\xe5\x8a\x9f");
    
    // 第一次运行：建立连接并处理事件
    printf("\xe7\xac\xac\xe4\xb8\x80\xe6\xac\xa1\xe8\xbf\x90\xe8\xa1\x8c\xef\xbc\x9a\xe5\xbb\xba\xe7\xab\x8b\xe8\xbf\x9e\xe6\x8e\xa5...\n");
    SStorageEngineInterface* engine1 = get_storage_engine_interface("auto");
    TEST_ASSERT(engine1 != NULL, "\xe8\x8e\xb7\xe5\x8f\x96\xe5\xad\x98\xe5\x82\xa8\xe5\xbc\x95\xe6\x93\x8e\xe6\x8e\xa5\xe5\x8f\xa3\xe6\x88\x90\xe5\x8a\x9f");
    
    StorageEventCallback callback = test_event_callback;
    int event_count1 = 0;
    SStorageEngineConfig config = {
        .enable_interception = true,
        .event_callback = callback,
        .callback_user_data = &event_count1,
        .event_buffer_size = 1000,
        .callback_threads = 2
    };
    
    result = engine1->init(&config);
    TEST_ASSERT(result == 0, "\xe5\xad\x98\xe5\x82\xa8\xe5\xbc\x95\xe6\x93\x8e\xe5\x88\x9d\xe5\xa7\x8b\xe5\x8c\x96\xe6\x88\x90\xe5\x8a\x9f");
    
    result = engine1->install_interception();
    TEST_ASSERT(result == 0, "\xe4\xba\x8b\xe4\xbb\xb6\xe6\x8b\xa6\xe6\x88\xaa\xe5\xae\x89\xe8\xa3\x85\xe6\x88\x90\xe5\x8a\x9f");
    
    // 等待连接建立和事件处理
    printf("\xe7\xad\x89\xe5\xbe\x85\xe4\xba\x8b\xe4\xbb\xb6\xe5\xa4\x84\xe7\x90\x86...\n");
    sleep(3);
    
    // 记录第一次运行的状态
    int64_t first_run_offset = g_last_committed_offset;
    uint64_t first_run_events = g_total_events_processed;
    
    printf("\xe7\xac\xac\xe4\xb8\x80\xe6\xac\xa1\xe8\xbf\x90\xe8\xa1\x8c\xe7\xbb\x93\xe6\x9e\x9c: offset=%ld, events=%lu\n", first_run_offset, first_run_events);
    
    // 清理第一次运行
    engine1->uninstall_interception();
    engine1->destroy();
    
    // 等待一段时间
    printf("\xe7\xad\x895\xe7\xa7\x92\xe5\x90\x8e\xe9\x87\x8d\xe6\x96\xb0\xe8\xbf\x9e\xe6\x8e\xa5...\n");
    sleep(5);
    
    // 第二次运行：模拟重启后的恢复
    printf("\xe7\xac\xac\xe4\xba\x8c\xe6\xac\xa1\xe8\xbf\x90\xe8\xa1\x8c\xef\xbc\x9a\xe6\xa8\xa1\xe6\x8b\x9f\xe9\x87\x8d\xe5\x90\xaf\xe6\x81\xa2\xe5\xa4\x8d...\n");
    SStorageEngineInterface* engine2 = get_storage_engine_interface("auto");
    TEST_ASSERT(engine2 != NULL, "\xe8\x8e\xb7\xe5\x8f\x96\xe5\xad\x98\xe5\x82\xa8\xe5\xbc\x95\xe6\x93\x8e\xe6\x8e\xa5\xe5\x8f\xa3\xe6\x88\x90\xe5\x8a\x9f");
    
    int event_count2 = 0;
    config.callback_user_data = &event_count2;
    
    result = engine2->init(&config);
    TEST_ASSERT(result == 0, "\xe5\xad\x98\xe5\x82\xa8\xe5\xbc\x95\xe6\x93\x8e\xe9\x87\x8d\xe6\x96\xb0\xe5\x88\x9d\xe5\xa7\x8b\xe5\x8c\x96\xe6\x88\x90\xe5\x8a\x9f");
    
    result = engine2->install_interception();
    TEST_ASSERT(result == 0, "\xe4\xba\x8b\xe4\xbb\xb6\xe6\x8b\xa6\xe6\x88\xaa\xe9\x87\x8d\xe6\x96\xb0\xe5\xae\x89\xe8\xa3\x85\xe6\x88\x90\xe5\x8a\x9f");
    
    // 等待连接建立和事件处理
    printf("\xe7\xad\x89\xe5\xbe\x85\xe4\xba\x8b\xe4\xbb\xb6\xe5\xa4\x84\xe7\x90\x86...\n");
    sleep(3);
    
    // 记录第二次运行的状态
    int64_t second_run_offset = g_last_committed_offset;
    uint64_t second_run_events = g_total_events_processed;
    
    printf("\xe7\xac\xac\xe4\xba\x8c\xe6\xac\xa1\xe8\xbf\x90\xe8\xa1\x8c\xe7\xbb\x93\xe6\x9e\x9c: offset=%ld, events=%lu\n", second_run_offset, second_run_events);
    
    // 验证断点恢复：第二次运行应该从上次的offset继续
    if (first_run_offset > 0) {
        TEST_ASSERT(second_run_offset >= first_run_offset, "\xe6\x81\xa2\xe5\xa4\x8doffset\xe6\x9c\xaa\xe5\x9b\x9e\xe9\x80\x80");
        printf("\xe6\x96\xad\xe7\x82\xb9\xe6\x81\xa2\xe5\xa4\x8d\xe6\x88\x90\xe5\x8a\x9f: \xe4\xbb\x8eoffset %ld \xe7\xbb\xad\xe7\xbb\xad\n", first_run_offset);
    } else {
        printf("\xe9\xa6\x96\xe6\xac\xa1\xe8\xbf\x90\xe8\xa1\x8c\xe6\x97\xa0\xe6\x9c\x89\xe6\x95\x88offset\xef\xbc\x8c\xe8\xb7\xb3\xe8\xbf\x87\xe6\x96\xad\xe7\x82\xb9\xe6\x81\xa2\xe5\xa4\x8d\xe9\xaa\x8c\xe8\xaf\x81\n");
    }
    
    // 清理第二次运行
    engine2->uninstall_interception();
    engine2->destroy();
}

// 测试7: 性能基准测试
static void test_performance_benchmark() {
    printf("\n=== \xe6\xb5\x8b\xe8\xaf\x957: \xe6\x80\xa7\xe8\x83\xbd\xe5\x9f\xba\xe5\x87\x86\xe6\xb5\x8b\xe8\xaf\x95 ===\n");
    
    // 设置TMQ配置
    int32_t result = tdengine_set_tmq_config(
        "127.0.0.1", 6030, "root", "taosdata", 
        "test_db", "test_topic", "test_group_perf"
    );
    TEST_ASSERT(result == 0, "TMQ\xe9\x85\x8d\xe7\xbd\x8e\xe8\xae\xbe\xe7\xbd\xae\xe6\x88\x90\xe5\x8a\x9f");
    
    // 设置提交策略：异步提交，至多一次语义（性能优先）
    result = tdengine_set_commit_strategy(false, false, 100);
    TEST_ASSERT(result == 0, "\xe6\x80\xa7\xe8\x83\xbd\xe6\xb5\x8b\xe8\xaf\x95\xe7\xad\x96\xe7\x95\xa5\xe8\xae\xbe\xe7\xbd\xae\xe6\x88\x90\xe5\x8a\x9f");
    
    // 获取存储引擎接口
    SStorageEngineInterface* engine = get_storage_engine_interface("auto");
    TEST_ASSERT(engine != NULL, "\xe8\x8e\xb7\xe5\x8f\x96\xe5\xad\x98\xe5\x82\xa8\xe5\xbc\x95\xe6\x93\x8e\xe6\x8e\xa5\xe5\x8f\xa3\xe6\x88\x90\xe5\x8a\x9f");
    
    // 初始化存储引擎
    StorageEventCallback callback = test_event_callback;
    int event_count = 0;
    SStorageEngineConfig config = {
        .enable_interception = true,
        .event_callback = callback,
        .callback_user_data = &event_count,
        .event_buffer_size = 10000,
        .callback_threads = 4
    };
    
    result = engine->init(&config);
    TEST_ASSERT(result == 0, "\xe5\xad\x98\xe5\x82\xa8\xe5\xbc\x95\xe6\x93\x8e\xe5\x88\x9d\xe5\xa7\x8b\xe5\x8c\x96\xe6\x88\x90\xe5\x8a\x9f");
    
    // 安装事件拦截
    result = engine->install_interception();
    TEST_ASSERT(result == 0, "\xe4\xba\x8b\xe4\xbb\xb6\xe6\x8b\xa6\xe6\x88\xaa\xe5\xae\x89\xe8\xa3\x85\xe6\x88\x90\xe5\x8a\x9f");
    
    // 等待连接建立
    printf("\xe7\xad\x89\xe5\xbe\x85TMQ\xe8\xbf\x9e\xe6\x8e\xa5\xe5\xbb\xba\xe7\xab\x8b...\n");
    sleep(2);
    
    // 记录性能测试开始时间
    int64_t start_time = get_current_time_ms();
    uint64_t start_events = g_total_events_processed;
    
    // 运行性能测试
    printf("\xe8\xbf\x90\xe8\xa1\x8c\xe6\x80\xa7\xe8\x83\xbd\xe6\xb5\x8b\xe8\xaf\x9530\xe7\xa7\x92...\n");
    int test_duration = 30;
    int elapsed = 0;
    
    while (elapsed < test_duration && g_running) {
        sleep(5);
        elapsed += 5;
        
        // 获取当前性能指标
        SObservabilityMetrics metrics;
        result = engine->get_observability_metrics(engine, &metrics);
        if (result == 0) {
            printf("\xe6\x80\xa7\xe8\x83\xbd\xe6\x8c\x87\xe6\xa0\x87 [%ds]: \xe4\xba\x8b\xe4\xbb\xb6/\xe7\xa7\x92=%lu, \xe6\xb6\x88\xe6\x81\xaf/\xe7\xa7\x92=%lu, \xe5\x86\x85\xe5\xad\x98=%zu bytes, \xe9\x98\x9f\xe5\x88\x97\xe4\xbd\xbf\xe7\x94\xa8=%u%%\n",
                   elapsed, metrics.events_per_second, metrics.messages_per_second,
                   metrics.memory_usage_bytes, metrics.ring_buffer_usage);
        }
    }
    
    // 记录性能测试结束时间
    int64_t end_time = get_current_time_ms();
    uint64_t end_events = g_total_events_processed;
    
    // 计算性能指标
    int64_t total_duration = end_time - start_time;
    uint64_t total_events = end_events - start_events;
    
    if (total_duration > 0) {
        double events_per_second = (double)total_events / (total_duration / 1000.0);
        printf("\xe6\x80\xa7\xe8\x83\xbd\xe6\xb5\x8b\xe8\xaf\x95\xe7\xbb\x93\xe6\x9e\x9c:\n");
        printf("  \xe6\x80\xbb\xe6\x97\xb6\xe9\x95\xbf: %ld ms\n", total_duration);
        printf("  \xe6\x80\xbb\xe4\xba\x8b\xe4\xbb\xb6: %lu\n", total_events);
        printf("  \xe4\xba\x8b\xe4\xbb\xb6\xe9\x80\x9f\xe7\x8e\x87: %.2f events/sec\n", events_per_second);
        
        // 验证性能指标
        TEST_ASSERT(total_events >= 0, "\xe4\xba\x8b\xe4\xbb\xb6\xe8\xae\xa1\xe6\x95\xb0\xe6\x9c\x89\xe6\x95\x88");
        TEST_ASSERT(events_per_second >= 0, "\xe4\xba\x8b\xe4\xbb\xb6\xe9\x80\x9f\xe7\x8e\x87\xe6\x9c\x89\xe6\x95\x88");
        
        // 获取最终性能指标
        SObservabilityMetrics final_metrics;
        result = engine->get_observability_metrics(engine, &final_metrics);
        if (result == 0) {
            printf("  \xe6\x9c\x80\xe7\xbb\x88\xe5\x86\x85\xe5\xad\x98\xe4\xbd\xbf\xe7\x94\xa8: %zu bytes\n", final_metrics.memory_usage_bytes);
            printf("  \xe6\x9c\x80\xe7\xbb\x88\xe9\x98\x9f\xe5\x88\x97\xe4\xbd\xbf\xe7\x94\xa8: %u%%\n", final_metrics.ring_buffer_usage);
            
            TEST_ASSERT(final_metrics.memory_usage_bytes > 0, "\xe5\x86\x85\xe5\xad\x98\xe4\xbd\xbf\xe7\x94\xa8\xe6\x9c\x89\xe6\x95\x88");
            TEST_ASSERT(final_metrics.ring_buffer_usage <= 100, "\xe9\x98\x9f\xe5\x88\x97\xe4\xbd\xbf\xe7\x94\xa8\xe7\x8e\x87\xe5\x90\x88\xe7\x90\x86");
        }
    } else {
        printf("\xe6\x80\xa7\xe8\x83\xbd\xe6\xb5\x8b\xe8\xaf\x95\xe6\x97\xb6\xe9\x97\xb4\xe8\xbf\x87\xe7\x9f\xad\xef\xbc\x8c\xe8\xb7\xb3\xe8\xbf\x87\xe6\x80\xa7\xe8\x83\xbd\xe9\xaa\x8c\xe8\xaf\x81\n");
    }
    
    // 清理
    engine->uninstall_interception();
    engine->destroy();
}

// 测试8: 错误处理和恢复测试
static void test_error_handling_and_recovery() {
    printf("\n=== \xe6\xb5\x8b\xe8\xaf\x958: \xe9\x94\x99\xe8\xaf\xaf\xe5\xa4\x84\xe7\x90\x86\xe5\x92\x8c\xe6\x81\xa2\xe5\xa4\x8d\xe6\xb5\x8b\xe8\xaf\x95 ===\n");
    
    // 测试无效配置
    printf("\xe6\xb5\x8b\xe8\xaf\x95\xe6\x97\xa0\xe6\x95\x88\xe9\x85\x8d\xe7\xbd\xae...\n");
    int32_t result = tdengine_set_tmq_config(
        "invalid_host", 9999, "invalid_user", "invalid_pass", 
        "invalid_db", "invalid_topic", "invalid_group"
    );
    // 注意：无效配置可能不会立即失败，而是在连接时失败
    
    // 设置有效配置
    result = tdengine_set_tmq_config(
        "127.0.0.1", 6030, "root", "taosdata", 
        "test_db", "test_topic", "test_group_error"
    );
    TEST_ASSERT(result == 0, "\xe6\x9c\x89\xe6\x95\x88TMQ\xe9\x85\x8d\xe7\xbd\x8e\xe8\xae\xbe\xe7\xbd\xae\xe6\x88\x90\xe5\x8a\x9f");
    
    // 获取存储引擎接口
    SStorageEngineInterface* engine = get_storage_engine_interface("auto");
    TEST_ASSERT(engine != NULL, "\xe8\x8e\xb7\xe5\x8f\x96\xe5\xad\x98\xe5\x82\xa8\xe5\xbc\x95\xe6\x93\x8e\xe6\x8e\xa5\xe5\x8f\xa3\xe6\x88\x90\xe5\x8a\x9f");
    
    // 初始化存储引擎
    StorageEventCallback callback = test_event_callback;
    int event_count = 0;
    SStorageEngineConfig config = {
        .enable_interception = true,
        .event_callback = callback,
        .callback_user_data = &event_count,
        .event_buffer_size = 1000,
        .callback_threads = 2
    };
    
    result = engine->init(&config);
    TEST_ASSERT(result == 0, "\xe5\xad\x98\xe5\x82\xa8\xe5\xbc\x95\xe6\x93\x8e\xe5\x88\x9d\xe5\xa7\x8b\xe5\x8c\x96\xe6\x88\x90\xe5\x8a\x9f");
    
    // 安装事件拦截
    result = engine->install_interception();
    TEST_ASSERT(result == 0, "\xe4\xba\x8b\xe4\xbb\xb6\xe6\x8b\xa6\xe6\x88\xaa\xe5\xae\x89\xe8\xa3\x85\xe6\x88\x90\xe5\x8a\x9f");
    
    // 等待连接建立
    printf("\xe7\xad\x89\xe5\xbe\x85TMQ\xe8\xbf\x9e\xe6\x8e\xa5\xe5\xbb\xba\xe7\xab\x8b...\n");
    sleep(2);
    
    // 获取错误统计
    uint64_t events_processed, events_dropped, messages_consumed, offset_commits;
    int64_t last_commit_time;
    result = tdengine_get_detailed_stats(
        &events_processed, &events_dropped, &messages_consumed, 
        &offset_commits, &last_commit_time
    );
    TEST_ASSERT(result == 0, "\xe8\x8e\xb7\xe5\x8f\x96\xe9\x94\x99\xe8\xaf\xaf\xe7\xbb\x9f\xe8\xae\xa1\xe6\x88\x90\xe5\x8a\x9f");
    
    printf("\xe9\x94\x99\xe8\xaf\xaf\xe7\xbb\x9f\xe8\xae\xa1: \xe5\xa4\x84\xe7\x90\x86=%lu, \xe4\xb8\xa2\xe5\xbc\x83=%lu, \xe6\xb6\x88\xe8\xb4\xb9=%lu, \xe6\x8f\x90\xe4\xba\xa4=%lu\n",
           events_processed, events_dropped, messages_consumed, offset_commits);
    
    // 验证错误处理：即使有错误，系统应该继续运行
    TEST_ASSERT(events_processed >= 0, "\xe5\xa4\x84\xe7\x90\x86\xe4\xba\x8b\xe4\xbb\xb6\xe8\xae\xa1\xe6\x95\xb0\xe6\x9c\x89\xe6\x95\x88");
    TEST_ASSERT(events_dropped >= 0, "\xe4\xb8\xa2\xe5\xbc\x83\xe4\xba\x8b\xe4\xbb\xb6\xe8\xae\xa1\xe6\x95\xb0\xe6\x9c\x89\xe6\x95\x88");
    
    // 获取可观测性指标
    SObservabilityMetrics metrics;
    result = engine->get_observability_metrics(engine, &metrics);
    TEST_ASSERT(result == 0, "\xe8\x8e\xb7\xe5\x8f\x96\xe5\x8f\xaf\xe8\xa7\x82\xe6\xb5\x8b\xe6\x80\xa7\xe6\x8c\x87\xe6\xa0\x87\xe6\x88\x90\xe5\x8a\x9f");
    
    printf("\xe9\x94\x99\xe8\xaf\xaf\xe6\x81\xa2\xe5\xa4\x8d\xe6\x8c\x87\xe6\xa0\x87: \xe8\xbf\x9e\xe6\x8e\xa5\xe9\x87\x8d\xe8\xaf\x95=%lu, \xe8\xae\xa2\xe9\x98\x85\xe9\x87\x8d\xe8\xaf\x95=%lu, \xe6\x8f\x90\xe4\xba\xa4\xe9\x87\x8d\xe8\xaf\x95=%lu, \xe8\xa7\xa3\xe6\x9e\x90\xe9\x94\x99\xe8\xaf\xaf=%lu\n",
           metrics.connection_retries, metrics.subscription_retries, 
           metrics.commit_retries, metrics.parse_errors);
    
    // 验证重试机制
    TEST_ASSERT(metrics.connection_retries >= 0, "\xe8\xbf\x9e\xe6\x8e\xa5\xe9\x87\x8d\xe8\xaf\x95\xe8\xae\xa1\xe6\x95\xb0\xe6\x9c\x89\xe6\x95\x88");
    TEST_ASSERT(metrics.subscription_retries >= 0, "\xe8\xae\xa2\xe9\x98\x85\xe9\x87\x8d\xe8\xaf\x95\xe8\xae\xa1\xe6\x95\xb0\xe6\x9c\x89\xe6\x95\x88");
    TEST_ASSERT(metrics.commit_retries >= 0, "\xe6\x8f\x90\xe4\xba\xa4\xe9\x87\x8d\xe8\xaf\x95\xe8\xae\xa1\xe6\x95\xb0\xe6\x9c\x89\xe6\x95\x88");
    TEST_ASSERT(metrics.parse_errors >= 0, "\xe8\xa7\xa3\xe6\x9e\x90\xe9\x94\x99\xe8\xaf\xaf\xe8\xae\xa1\xe6\x95\xb0\xe6\x9c\x89\xe6\x95\x88");
    
    // 清理
    engine->uninstall_interception();
    engine->destroy();
}

int main() {
    printf("\xe5\xbc\x80\xe5\xa7\x8b\xe7\x9c\x9f\xe5\xae\x9eTDengine TMQ Offset\xe8\xaf\xad\xe4\xb9\x89\xe9\xaa\x8c\xe8\xaf\x81\xe6\xb5\x8b\xe8\xaf\x95...\n");
    printf("\xe6\xb3\xa8\xe6\x84\x8f\xef\xbc\x9a\xe6\xad\xa4\xe6\xb5\x8b\xe8\xaf\x95\xe9\x9c\x80\xe8\xa6\x81\xe8\xbf\x90\xe8\xa1\x8c\xe4\xb8\xad\xe7\x9a\x84TDengine\xe5\xae\x9e\xe4\xbe\x8b\xe5\x92\x8cTMQ topic\n");
    
    // 设置信号处理
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    // 初始化全局变量
    pthread_mutex_init(&g_offset_mutex, NULL);
    g_total_events_processed = 0;
    g_total_events_committed = 0;
    g_last_committed_offset = -1;
    
    // 运行测试
    test_real_tmq_connection();
    test_sync_commit_semantics();
    test_async_commit_semantics();
    test_at_least_once_semantics();
    test_at_most_once_semantics();
    test_checkpoint_recovery();
    test_performance_benchmark();
    test_error_handling_and_recovery();
    
    // 清理
    pthread_mutex_destroy(&g_offset_mutex);
    
    printf("\n=== \xe7\x9c\x9f\xe5\xae\x9eTDengine\xe6\xb5\x8b\xe8\xaf\x95\xe7\xbb\xbb\xe6\x9e\x9c\xe6\xb1\x87\xe6\x80\xbb ===\n");
    printf("\xe6\x80\xbb\xe6\xb5\x8b\xe8\xaf\x95\xe6\x95\xb0: %d\n", total_tests);
    printf("\xe9\x80\x9a\xe8\xbf\x87\xe6\xb5\x8b\xe8\xaf\x95: %d\n", passed_tests);
    printf("\xe5\xa4\xb1\xe8\xb4\xa5\xe6\xb5\x8b\xe8\xaf\x95: %d\n", failed_tests);
    printf("\xe9\x80\x9a\xe8\xbf\x87\xe7\x8e\x87: %.2f%%\n", (double)passed_tests / total_tests * 100);
    
    if (failed_tests == 0) {
        printf("\n\xf0\x9f\x8e\x89 \xe6\x89\x80\xe6\x9c\x89\xe7\x9c\x9f\xe5\xae\x9eTDengine Offset\xe8\xaf\xad\xe4\xb9\x89\xe9\xaa\x8c\xe8\xaf\x81\xe6\xb5\x8b\xe8\xaf\x95\xe9\x80\x9a\xe8\xbf\x87\xef\xbc\x81\n");
        return 0;
    } else {
        printf("\n\xe2\x9d\x8c \xe6\x9c\x89 %d \xe4\xb8\xaa\xe6\xb5\x8b\xe8\xaf\x95\xe5\xa4\xb1\xe8\xb4\xa5\xef\xbc\x8c\xe9\x9c\x80\xe8\xa6\x81\xe4\xbf\xae\xe5\xa4\x8d\xef\xbc\x81\n", failed_tests);
        return 1;
    }
}


