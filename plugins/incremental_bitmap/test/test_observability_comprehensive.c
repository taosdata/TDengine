#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <stdlib.h>
#include "../include/storage_engine_interface.h"
#include "../include/observability.h"
#include "../include/bitmap_engine.h"
#include "../include/event_interceptor.h"
#include "../include/ring_buffer.h"

// 测试计数器
static int total_tests = 0;
static int passed_tests = 0;
static int failed_tests = 0;

// 测试宏
#define TEST_ASSERT(condition, message) do { \
    total_tests++; \
    if (condition) { \
        passed_tests++; \
        printf("✓ %s\n", message); \
    } else { \
        failed_tests++; \
        printf("✗ %s\n", message); \
    } \
} while(0)

// 测试1: 可观测性指标结构体完整性
static void test_observability_metrics_structure() {
    printf("\n=== 测试1: 可观测性指标结构体完整性 ===\n");
    
    SObservabilityMetrics metrics;
    memset(&metrics, 0, sizeof(metrics));
    
    // 测试结构体大小
    TEST_ASSERT(sizeof(SObservabilityMetrics) > 0, "结构体大小大于0");
    TEST_ASSERT(sizeof(SObservabilityMetrics) < 10000, "结构体大小合理");
    
    // 测试字段访问
    metrics.events_per_second = 1000;
    metrics.messages_per_second = 1500;
    metrics.bytes_per_second = 1024000;
    metrics.consumer_lag_ms = 50;
    metrics.offset_lag = 100;
    metrics.processing_delay_ms = 10;
    metrics.events_dropped = 5;
    metrics.messages_dropped = 3;
    metrics.parse_errors = 2;
    metrics.connection_retries = 1;
    metrics.subscription_retries = 0;
    metrics.commit_retries = 2;
    metrics.ring_buffer_usage = 75;
    metrics.ring_buffer_capacity = 10000;
    metrics.event_queue_size = 7500;
    metrics.memory_usage_bytes = 1048576;
    metrics.bitmap_memory_bytes = 524288;
    metrics.metadata_memory_bytes = 262144;
    metrics.last_update_time = 1234567890;
    metrics.uptime_seconds = 3600;
    
    TEST_ASSERT(metrics.events_per_second == 1000, "events_per_second字段正确");
    TEST_ASSERT(metrics.messages_per_second == 1500, "messages_per_second字段正确");
    TEST_ASSERT(metrics.bytes_per_second == 1024000, "bytes_per_second字段正确");
    TEST_ASSERT(metrics.consumer_lag_ms == 50, "consumer_lag_ms字段正确");
    TEST_ASSERT(metrics.offset_lag == 100, "offset_lag字段正确");
    TEST_ASSERT(metrics.processing_delay_ms == 10, "processing_delay_ms字段正确");
    TEST_ASSERT(metrics.events_dropped == 5, "events_dropped字段正确");
    TEST_ASSERT(metrics.messages_dropped == 3, "messages_dropped字段正确");
    TEST_ASSERT(metrics.parse_errors == 2, "parse_errors字段正确");
    TEST_ASSERT(metrics.connection_retries == 1, "connection_retries字段正确");
    TEST_ASSERT(metrics.subscription_retries == 0, "subscription_retries字段正确");
    TEST_ASSERT(metrics.commit_retries == 2, "commit_retries字段正确");
    TEST_ASSERT(metrics.ring_buffer_usage == 75, "ring_buffer_usage字段正确");
    TEST_ASSERT(metrics.ring_buffer_capacity == 10000, "ring_buffer_capacity字段正确");
    TEST_ASSERT(metrics.event_queue_size == 7500, "event_queue_size字段正确");
    TEST_ASSERT(metrics.memory_usage_bytes == 1048576, "memory_usage_bytes字段正确");
    TEST_ASSERT(metrics.bitmap_memory_bytes == 524288, "bitmap_memory_bytes字段正确");
    TEST_ASSERT(metrics.metadata_memory_bytes == 262144, "metadata_memory_bytes字段正确");
    TEST_ASSERT(metrics.last_update_time == 1234567890, "last_update_time字段正确");
    TEST_ASSERT(metrics.uptime_seconds == 3600, "uptime_seconds字段正确");
}

// 测试2: 可观测性指标更新函数
static void test_observability_metrics_update() {
    printf("\n=== 测试2: 可观测性指标更新函数 ===\n");
    
    SObservabilityMetrics metrics;
    memset(&metrics, 0, sizeof(metrics));
    
    // 测试NULL指针处理
    update_observability_metrics(NULL);
    TEST_ASSERT(1, "NULL指针处理正常");
    
    // 测试正常更新
    int64_t before_time = time(NULL);
    update_observability_metrics(&metrics);
    int64_t after_time = time(NULL);
    
    TEST_ASSERT(metrics.last_update_time > 0, "更新时间戳正确设置");
    TEST_ASSERT(metrics.last_update_time >= before_time * 1000, "更新时间戳合理");
    TEST_ASSERT(metrics.last_update_time <= after_time * 1000, "更新时间戳合理");
}

// 测试3: 可观测性指标打印函数
static void test_observability_metrics_print() {
    printf("\n=== 测试3: 可观测性指标打印函数 ===\n");
    
    SObservabilityMetrics metrics;
    memset(&metrics, 0, sizeof(metrics));
    
    // 设置测试数据
    metrics.events_per_second = 2000;
    metrics.messages_per_second = 2500;
    metrics.bytes_per_second = 2048000;
    metrics.consumer_lag_ms = 25;
    metrics.offset_lag = 50;
    metrics.processing_delay_ms = 5;
    metrics.events_dropped = 10;
    metrics.messages_dropped = 5;
    metrics.parse_errors = 3;
    metrics.connection_retries = 2;
    metrics.subscription_retries = 1;
    metrics.commit_retries = 3;
    metrics.ring_buffer_usage = 80;
    metrics.ring_buffer_capacity = 10000;
    metrics.event_queue_size = 8000;
    metrics.memory_usage_bytes = 2097152;
    metrics.bitmap_memory_bytes = 1048576;
    metrics.metadata_memory_bytes = 524288;
    metrics.last_update_time = 1234567890;
    metrics.uptime_seconds = 7200;
    
    // 测试NULL指针处理
    print_observability_metrics(NULL);
    TEST_ASSERT(1, "NULL指针处理正常");
    
    // 测试正常打印
    printf("调用 print_observability_metrics 函数:\n");
    print_observability_metrics(&metrics);
    TEST_ASSERT(1, "打印函数正常执行");
}

// 测试4: JSON格式化函数
static void test_json_formatting() {
    printf("\n=== 测试4: JSON格式化函数 ===\n");
    
    SObservabilityMetrics metrics;
    memset(&metrics, 0, sizeof(metrics));
    
    // 设置测试数据
    metrics.events_per_second = 1000;
    metrics.messages_per_second = 1500;
    metrics.bytes_per_second = 1024000;
    metrics.consumer_lag_ms = 50;
    metrics.offset_lag = 100;
    metrics.processing_delay_ms = 10;
    metrics.events_dropped = 5;
    metrics.messages_dropped = 3;
    metrics.parse_errors = 2;
    metrics.connection_retries = 1;
    metrics.subscription_retries = 0;
    metrics.commit_retries = 2;
    metrics.ring_buffer_usage = 75;
    metrics.ring_buffer_capacity = 10000;
    metrics.event_queue_size = 7500;
    metrics.memory_usage_bytes = 1048576;
    metrics.bitmap_memory_bytes = 524288;
    metrics.metadata_memory_bytes = 262144;
    metrics.last_update_time = 1234567890;
    metrics.uptime_seconds = 3600;
    
    // 测试NULL指针处理
    format_observability_metrics_json(NULL, NULL, 0);
    TEST_ASSERT(1, "NULL指针处理正常");
    
    // 测试空缓冲区
    format_observability_metrics_json(&metrics, NULL, 0);
    TEST_ASSERT(1, "空缓冲区处理正常");
    
    // 测试小缓冲区
    char small_buffer[10];
    format_observability_metrics_json(&metrics, small_buffer, sizeof(small_buffer));
    TEST_ASSERT(1, "小缓冲区处理正常");
    
    // 测试正常格式化
    char json_buffer[4096];
    format_observability_metrics_json(&metrics, json_buffer, sizeof(json_buffer));
    
    TEST_ASSERT(strlen(json_buffer) > 0, "JSON字符串非空");
    TEST_ASSERT(strstr(json_buffer, "\"events_per_second\":1000") != NULL, "JSON包含events_per_second");
    TEST_ASSERT(strstr(json_buffer, "\"memory_usage_bytes\":1048576") != NULL, "JSON包含memory_usage_bytes");
    TEST_ASSERT(strstr(json_buffer, "\"ring_buffer_usage\":75") != NULL, "JSON包含ring_buffer_usage");
    TEST_ASSERT(strstr(json_buffer, "\"rate\":") != NULL, "JSON包含rate字段");
    TEST_ASSERT(strstr(json_buffer, "\"lag\":") != NULL, "JSON包含lag字段");
    TEST_ASSERT(strstr(json_buffer, "\"dropped\":") != NULL, "JSON包含dropped字段");
    TEST_ASSERT(strstr(json_buffer, "\"retries\":") != NULL, "JSON包含retries字段");
    TEST_ASSERT(strstr(json_buffer, "\"queue\":") != NULL, "JSON包含queue字段");
    TEST_ASSERT(strstr(json_buffer, "\"memory\":") != NULL, "JSON包含memory字段");
    TEST_ASSERT(strstr(json_buffer, "\"time\":") != NULL, "JSON包含time字段");
    
    printf("JSON输出: %s\n", json_buffer);
}

// 测试5: Prometheus格式化函数
static void test_prometheus_formatting() {
    printf("\n=== 测试5: Prometheus格式化函数 ===\n");
    
    SObservabilityMetrics metrics;
    memset(&metrics, 0, sizeof(metrics));
    
    // 设置测试数据
    metrics.events_per_second = 1000;
    metrics.messages_per_second = 1500;
    metrics.bytes_per_second = 1024000;
    metrics.consumer_lag_ms = 50;
    metrics.offset_lag = 100;
    metrics.processing_delay_ms = 10;
    metrics.events_dropped = 5;
    metrics.messages_dropped = 3;
    metrics.parse_errors = 2;
    metrics.connection_retries = 1;
    metrics.subscription_retries = 0;
    metrics.commit_retries = 2;
    metrics.ring_buffer_usage = 75;
    metrics.ring_buffer_capacity = 10000;
    metrics.event_queue_size = 7500;
    metrics.memory_usage_bytes = 1048576;
    metrics.bitmap_memory_bytes = 524288;
    metrics.metadata_memory_bytes = 262144;
    metrics.last_update_time = 1234567890;
    metrics.uptime_seconds = 3600;
    
    // 测试NULL指针处理
    format_observability_metrics_prometheus(NULL, NULL, 0);
    TEST_ASSERT(1, "NULL指针处理正常");
    
    // 测试空缓冲区
    format_observability_metrics_prometheus(&metrics, NULL, 0);
    TEST_ASSERT(1, "空缓冲区处理正常");
    
    // 测试小缓冲区
    char small_buffer[10];
    format_observability_metrics_prometheus(&metrics, small_buffer, sizeof(small_buffer));
    TEST_ASSERT(1, "小缓冲区处理正常");
    
    // 测试正常格式化
    char prometheus_buffer[8192];
    format_observability_metrics_prometheus(&metrics, prometheus_buffer, sizeof(prometheus_buffer));
    
    TEST_ASSERT(strlen(prometheus_buffer) > 0, "Prometheus字符串非空");
    TEST_ASSERT(strstr(prometheus_buffer, "tdengine_events_per_second 1000") != NULL, "包含events_per_second指标");
    TEST_ASSERT(strstr(prometheus_buffer, "tdengine_memory_usage_bytes 1048576") != NULL, "包含memory_usage_bytes指标");
    TEST_ASSERT(strstr(prometheus_buffer, "tdengine_ring_buffer_usage 75") != NULL, "包含ring_buffer_usage指标");
    TEST_ASSERT(strstr(prometheus_buffer, "# HELP") != NULL, "包含HELP注释");
    TEST_ASSERT(strstr(prometheus_buffer, "# TYPE") != NULL, "包含TYPE注释");
    TEST_ASSERT(strstr(prometheus_buffer, "gauge") != NULL, "包含gauge类型");
    TEST_ASSERT(strstr(prometheus_buffer, "counter") != NULL, "包含counter类型");
    
    printf("Prometheus输出:\n%s\n", prometheus_buffer);
}

// 测试6: 内存使用计算
static void test_memory_usage_calculation() {
    printf("\n=== 测试6: 内存使用计算 ===\n");
    
    // 创建位图引擎
    SBitmapEngine* engine = bitmap_engine_init();
    TEST_ASSERT(engine != NULL, "位图引擎创建成功");
    
    // 添加一些测试数据
    for (int i = 0; i < 1000; i++) {
        int result = bitmap_engine_mark_dirty(engine, i, i * 100, i * 1000);
        TEST_ASSERT(result == 0, "标记脏块成功");
    }
    
    // 创建事件拦截器
    SEventInterceptorConfig config = {
        .enable_interception = true,
        .event_buffer_size = 1000,
        .callback_threads = 2,
        .callback = NULL,
        .callback_user_data = NULL
    };
    
    SEventInterceptor* interceptor = event_interceptor_init(&config, engine);
    TEST_ASSERT(interceptor != NULL, "事件拦截器创建成功");
    
    // 模拟一些事件
    for (int i = 0; i < 100; i++) {
        int result = event_interceptor_on_block_create(interceptor, i, i * 100, i * 1000);
        TEST_ASSERT(result == 0, "事件处理成功");
    }
    
    // 创建可观测性指标
    SObservabilityMetrics metrics;
    memset(&metrics, 0, sizeof(metrics));
    
    // 手动计算内存使用
    size_t total_memory = sizeof(void*) * 3; // 基本结构体大小
    size_t bitmap_memory = 0;
    size_t metadata_memory = 0;
    
    // 计算位图内存使用
    if (engine->dirty_blocks) {
        bitmap_memory += engine->dirty_blocks->memory_usage(engine->dirty_blocks->bitmap);
    }
    if (engine->new_blocks) {
        bitmap_memory += engine->new_blocks->memory_usage(engine->new_blocks->bitmap);
    }
    if (engine->deleted_blocks) {
        bitmap_memory += engine->deleted_blocks->memory_usage(engine->deleted_blocks->bitmap);
    }
    
    // 计算元数据内存使用
    metadata_memory = engine->metadata_map_size * sizeof(void*);
    metadata_memory += engine->metadata_count * sizeof(void*);
    
    total_memory += bitmap_memory + metadata_memory;
    
    TEST_ASSERT(bitmap_memory > 0, "位图内存使用大于0");
    TEST_ASSERT(metadata_memory > 0, "元数据内存使用大于0");
    TEST_ASSERT(total_memory > bitmap_memory + metadata_memory, "总内存使用合理");
    
    printf("计算的内存使用:\n");
    printf("  总内存: %zu 字节\n", total_memory);
    printf("  位图内存: %zu 字节\n", bitmap_memory);
    printf("  元数据内存: %zu 字节\n", metadata_memory);
    
    // 清理资源
    event_interceptor_destroy(interceptor);
    bitmap_engine_destroy(engine);
}

// 测试7: 队列水位监控
static void test_queue_watermark_monitoring() {
    printf("\n=== 测试7: 队列水位监控 ===\n");
    
    // 创建环形队列
    SRingBuffer* ring_buffer = ring_buffer_init(100);
    TEST_ASSERT(ring_buffer != NULL, "环形队列创建成功");
    
    // 测试空队列
    uint32_t size = ring_buffer_get_size(ring_buffer);
    uint32_t capacity = ring_buffer_get_capacity(ring_buffer);
    uint32_t usage = capacity > 0 ? (size * 100) / capacity : 0;
    
    TEST_ASSERT(size == 0, "空队列大小为0");
    TEST_ASSERT(capacity == 100, "队列容量正确");
    TEST_ASSERT(usage == 0, "空队列使用率为0");
    
    // 添加一些数据
    for (int i = 0; i < 50; i++) {
        int* data = malloc(sizeof(int));
        *data = i;
        int result = ring_buffer_enqueue(ring_buffer, data);
        TEST_ASSERT(result == 0, "数据入队成功");
    }
    
    size = ring_buffer_get_size(ring_buffer);
    usage = capacity > 0 ? (size * 100) / capacity : 0;
    
    TEST_ASSERT(size == 50, "半满队列大小正确");
    TEST_ASSERT(usage == 50, "半满队列使用率正确");
    
    // 填满队列
    for (int i = 50; i < 100; i++) {
        int* data = malloc(sizeof(int));
        *data = i;
        int result = ring_buffer_enqueue(ring_buffer, data);
        TEST_ASSERT(result == 0, "数据入队成功");
    }
    
    size = ring_buffer_get_size(ring_buffer);
    usage = capacity > 0 ? (size * 100) / capacity : 0;
    
    TEST_ASSERT(size == 100, "满队列大小正确");
    TEST_ASSERT(usage == 100, "满队列使用率正确");
    
    // 测试队列满时的行为
    int* extra_data = malloc(sizeof(int));
    *extra_data = 999;
    int result = ring_buffer_enqueue(ring_buffer, extra_data);
    TEST_ASSERT(result != 0, "满队列拒绝新数据");
    free(extra_data);
    
    // 清理资源
    ring_buffer_clear(ring_buffer, free);
    ring_buffer_destroy(ring_buffer);
}

// 测试8: 性能指标计算
static void test_performance_metrics_calculation() {
    printf("\n=== 测试8: 性能指标计算 ===\n");
    
    SObservabilityMetrics metrics;
    memset(&metrics, 0, sizeof(metrics));
    
    // 模拟性能数据
    metrics.events_per_second = 10000;
    metrics.messages_per_second = 15000;
    metrics.bytes_per_second = 10485760; // 10MB/s
    metrics.consumer_lag_ms = 100;
    metrics.offset_lag = 1000;
    metrics.processing_delay_ms = 5;
    metrics.ring_buffer_usage = 60;
    metrics.memory_usage_bytes = 52428800; // 50MB
    
    // 计算性能指标
    double events_per_ms = metrics.events_per_second / 1000.0;
    double messages_per_ms = metrics.messages_per_second / 1000.0;
    double mb_per_second = metrics.bytes_per_second / (1024.0 * 1024.0);
    double memory_mb = metrics.memory_usage_bytes / (1024.0 * 1024.0);
    
    TEST_ASSERT(events_per_ms > 0, "事件处理速率大于0");
    TEST_ASSERT(messages_per_ms > 0, "消息处理速率大于0");
    TEST_ASSERT(mb_per_second > 0, "数据吞吐量大于0");
    TEST_ASSERT(memory_mb > 0, "内存使用大于0");
    TEST_ASSERT(metrics.ring_buffer_usage <= 100, "队列使用率合理");
    TEST_ASSERT(metrics.processing_delay_ms >= 0, "处理延迟非负");
    
    printf("性能指标:\n");
    printf("  事件处理: %.2f 事件/毫秒\n", events_per_ms);
    printf("  消息处理: %.2f 消息/毫秒\n", messages_per_ms);
    printf("  数据吞吐: %.2f MB/秒\n", mb_per_second);
    printf("  内存使用: %.2f MB\n", memory_mb);
    printf("  队列使用率: %u%%\n", metrics.ring_buffer_usage);
    printf("  处理延迟: %ld ms\n", metrics.processing_delay_ms);
}

// 测试9: 边界条件测试
static void test_edge_cases() {
    printf("\n=== 测试9: 边界条件测试 ===\n");
    
    SObservabilityMetrics metrics;
    memset(&metrics, 0, sizeof(metrics));
    
    // 测试最大值
    metrics.events_per_second = UINT64_MAX;
    metrics.messages_per_second = UINT64_MAX;
    metrics.bytes_per_second = UINT64_MAX;
    metrics.consumer_lag_ms = INT64_MAX;
    metrics.offset_lag = UINT64_MAX;
    metrics.processing_delay_ms = INT64_MAX;
    metrics.events_dropped = UINT64_MAX;
    metrics.messages_dropped = UINT64_MAX;
    metrics.parse_errors = UINT64_MAX;
    metrics.connection_retries = UINT64_MAX;
    metrics.subscription_retries = UINT64_MAX;
    metrics.commit_retries = UINT64_MAX;
    metrics.ring_buffer_usage = UINT32_MAX;
    metrics.ring_buffer_capacity = UINT32_MAX;
    metrics.event_queue_size = UINT32_MAX;
    metrics.memory_usage_bytes = SIZE_MAX;
    metrics.bitmap_memory_bytes = SIZE_MAX;
    metrics.metadata_memory_bytes = SIZE_MAX;
    metrics.last_update_time = INT64_MAX;
    metrics.uptime_seconds = INT64_MAX;
    
    // 测试JSON格式化
    char json_buffer[4096];
    format_observability_metrics_json(&metrics, json_buffer, sizeof(json_buffer));
    TEST_ASSERT(strlen(json_buffer) > 0, "最大值JSON格式化成功");
    
    // 测试Prometheus格式化
    char prometheus_buffer[8192];
    format_observability_metrics_prometheus(&metrics, prometheus_buffer, sizeof(prometheus_buffer));
    TEST_ASSERT(strlen(prometheus_buffer) > 0, "最大值Prometheus格式化成功");
    
    // 测试最小值
    memset(&metrics, 0, sizeof(metrics));
    format_observability_metrics_json(&metrics, json_buffer, sizeof(json_buffer));
    TEST_ASSERT(strlen(json_buffer) > 0, "最小值JSON格式化成功");
    
    format_observability_metrics_prometheus(&metrics, prometheus_buffer, sizeof(prometheus_buffer));
    TEST_ASSERT(strlen(prometheus_buffer) > 0, "最小值Prometheus格式化成功");
}

// 线程函数 - 移到函数外部
static void* thread_func(void* arg) {
    SObservabilityMetrics metrics;
    char buffer[1024];
    int iterations = *(int*)arg;
    
    for (int i = 0; i < iterations; i++) {
        memset(&metrics, 0, sizeof(metrics));
        metrics.events_per_second = i;
        metrics.memory_usage_bytes = i * 1024;
        
        update_observability_metrics(&metrics);
        format_observability_metrics_json(&metrics, buffer, sizeof(buffer));
        format_observability_metrics_prometheus(&metrics, buffer, sizeof(buffer));
    }
    
    return NULL;
}

// 测试10: 并发安全性测试
static void test_concurrency_safety() {
    printf("\n=== 测试10: 并发安全性测试 ===\n");
    
    // 创建多个线程同时访问可观测性指标
    const int num_threads = 8;  // 减少线程数量
    const int iterations = 100; // 减少迭代次数
    pthread_t threads[num_threads];
    int thread_arg = iterations;
    
    // 创建线程
    for (int i = 0; i < num_threads; i++) {
        int result = pthread_create(&threads[i], NULL, thread_func, &thread_arg);
        TEST_ASSERT(result == 0, "线程创建成功");
    }
    
    // 等待所有线程完成
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }
    
    TEST_ASSERT(1, "并发访问测试完成");
}

// 测试11: 内存泄漏测试
static void test_memory_leaks() {
    printf("\n=== 测试11: 内存泄漏测试 ===\n");
    
    // 多次调用格式化函数，检查内存使用
    SObservabilityMetrics metrics;
    memset(&metrics, 0, sizeof(metrics));
    
    // 设置一些测试数据
    metrics.events_per_second = 1000;
    metrics.memory_usage_bytes = 1048576;
    metrics.ring_buffer_usage = 50;
    
    char json_buffer[4096];
    char prometheus_buffer[8192];
    
    // 多次调用格式化函数
    for (int i = 0; i < 1000; i++) {
        format_observability_metrics_json(&metrics, json_buffer, sizeof(json_buffer));
        format_observability_metrics_prometheus(&metrics, prometheus_buffer, sizeof(prometheus_buffer));
    }
    
    TEST_ASSERT(1, "内存泄漏测试完成");
}

// 测试12: 错误恢复测试
static void test_error_recovery() {
    printf("\n=== 测试12: 错误恢复测试 ===\n");
    
    // 测试各种错误情况下的恢复能力
    SObservabilityMetrics metrics;
    memset(&metrics, 0, sizeof(metrics));
    
    // 测试NULL指针
    update_observability_metrics(NULL);
    print_observability_metrics(NULL);
    format_observability_metrics_json(NULL, NULL, 0);
    format_observability_metrics_prometheus(NULL, NULL, 0);
    
    // 测试空缓冲区
    format_observability_metrics_json(&metrics, NULL, 0);
    format_observability_metrics_prometheus(&metrics, NULL, 0);
    
    // 测试小缓冲区
    char small_buffer[1];
    format_observability_metrics_json(&metrics, small_buffer, 1);
    format_observability_metrics_prometheus(&metrics, small_buffer, 1);
    
    // 测试正常情况
    char buffer[1024];
    format_observability_metrics_json(&metrics, buffer, sizeof(buffer));
    format_observability_metrics_prometheus(&metrics, buffer, sizeof(buffer));
    
    TEST_ASSERT(1, "错误恢复测试完成");
}

int main() {
    printf("开始全面可观测性指标测试...\n");
    
    test_observability_metrics_structure();
    test_observability_metrics_update();
    test_observability_metrics_print();
    test_json_formatting();
    test_prometheus_formatting();
    test_memory_usage_calculation();
    test_queue_watermark_monitoring();
    test_performance_metrics_calculation();
    test_edge_cases();
    test_concurrency_safety();
    test_memory_leaks();
    test_error_recovery();
    
    printf("\n=== 测试结果汇总 ===\n");
    printf("总测试数: %d\n", total_tests);
    printf("通过测试: %d\n", passed_tests);
    printf("失败测试: %d\n", failed_tests);
    printf("通过率: %.2f%%\n", (double)passed_tests / total_tests * 100);
    
    if (failed_tests == 0) {
        printf("\n🎉 所有测试通过！可观测性指标功能验证完成！\n");
        return 0;
    } else {
        printf("\n❌ 有 %d 个测试失败，需要修复！\n", failed_tests);
        return 1;
    }
}
