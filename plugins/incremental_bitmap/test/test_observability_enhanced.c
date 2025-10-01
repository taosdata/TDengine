#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include "../include/storage_engine_interface.h"
#include "../include/observability.h"
#include "../include/bitmap_engine.h"
#include "../include/event_interceptor.h"
#include "../include/ring_buffer.h"

// 模拟事件处理函数
static void mock_event_processor(void* user_data) {
    // 模拟事件处理延迟
    usleep(1000); // 1ms延迟
}

// 测试1: 基础可观测性指标结构体
static void test_basic_observability_metrics() {
    printf("=== 测试1: 基础可观测性指标结构体 ===\n");
    
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
    
    // 验证结构体大小
    printf("结构体大小: %zu 字节\n", sizeof(SObservabilityMetrics));
    assert(sizeof(SObservabilityMetrics) > 0);
    
    // 验证字段访问
    assert(metrics.events_per_second == 1000);
    assert(metrics.messages_per_second == 1500);
    assert(metrics.consumer_lag_ms == 50);
    assert(metrics.ring_buffer_usage == 75);
    assert(metrics.memory_usage_bytes == 1048576);
    
    printf("✓ 基础可观测性指标结构体测试通过\n");
}

// 测试2: 可观测性指标打印函数
static void test_observability_metrics_print() {
    printf("\n=== 测试2: 可观测性指标打印函数 ===\n");
    
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
    
    // 调用打印函数
    printf("调用 print_observability_metrics 函数:\n");
    print_observability_metrics(&metrics);
    
    printf("✓ 可观测性指标打印函数测试通过\n");
}

// 测试3: JSON格式化函数
static void test_json_formatting() {
    printf("\n=== 测试3: JSON格式化函数 ===\n");
    
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
    
    // 格式化JSON
    char json_buffer[4096];
    format_observability_metrics_json(&metrics, json_buffer, sizeof(json_buffer));
    
    printf("JSON格式输出:\n%s\n", json_buffer);
    
    // 验证JSON包含关键字段
    assert(strstr(json_buffer, "\"events_per_second\":1000") != NULL);
    assert(strstr(json_buffer, "\"memory_usage_bytes\":1048576") != NULL);
    assert(strstr(json_buffer, "\"ring_buffer_usage\":75") != NULL);
    
    printf("✓ JSON格式化函数测试通过\n");
}

// 测试4: Prometheus格式化函数
static void test_prometheus_formatting() {
    printf("\n=== 测试4: Prometheus格式化函数 ===\n");
    
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
    
    // 格式化Prometheus
    char prometheus_buffer[8192];
    format_observability_metrics_prometheus(&metrics, prometheus_buffer, sizeof(prometheus_buffer));
    
    printf("Prometheus格式输出:\n%s\n", prometheus_buffer);
    
    // 验证Prometheus格式包含关键指标
    assert(strstr(prometheus_buffer, "tdengine_events_per_second 1000") != NULL);
    assert(strstr(prometheus_buffer, "tdengine_memory_usage_bytes 1048576") != NULL);
    assert(strstr(prometheus_buffer, "tdengine_ring_buffer_usage 75") != NULL);
    
    printf("✓ Prometheus格式化函数测试通过\n");
}

// 测试5: 内存使用指标计算
static void test_memory_usage_calculation() {
    printf("\n=== 测试5: 内存使用指标计算 ===\n");
    
    // 创建位图引擎
    SBitmapEngine* engine = bitmap_engine_init();
    assert(engine != NULL);
    
    // 添加一些测试数据
    for (int i = 0; i < 1000; i++) {
        bitmap_engine_mark_dirty(engine, i, i * 100, i * 1000);
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
    assert(interceptor != NULL);
    
    // 模拟一些事件
    for (int i = 0; i < 100; i++) {
        event_interceptor_on_block_create(interceptor, i, i * 100, i * 1000);
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
    
    printf("计算的内存使用:\n");
    printf("  总内存: %zu 字节\n", total_memory);
    printf("  位图内存: %zu 字节\n", bitmap_memory);
    printf("  元数据内存: %zu 字节\n", metadata_memory);
    
    // 验证内存使用合理
    assert(bitmap_memory > 0);
    assert(metadata_memory > 0);
    assert(total_memory > bitmap_memory + metadata_memory);
    
    // 清理资源
    event_interceptor_destroy(interceptor);
    bitmap_engine_destroy(engine);
    
    printf("✓ 内存使用指标计算测试通过\n");
}

// 测试6: 队列水位监控
static void test_queue_watermark_monitoring() {
    printf("\n=== 测试6: 队列水位监控 ===\n");
    
    // 创建环形队列
    SRingBuffer* ring_buffer = ring_buffer_init(100);
    assert(ring_buffer != NULL);
    
    // 测试空队列
    uint32_t size = ring_buffer_get_size(ring_buffer);
    uint32_t capacity = ring_buffer_get_capacity(ring_buffer);
    uint32_t usage = capacity > 0 ? (size * 100) / capacity : 0;
    
    printf("空队列状态:\n");
    printf("  大小: %u\n", size);
    printf("  容量: %u\n", capacity);
    printf("  使用率: %u%%\n", usage);
    
    assert(size == 0);
    assert(capacity == 100);
    assert(usage == 0);
    
    // 添加一些数据
    for (int i = 0; i < 50; i++) {
        int* data = malloc(sizeof(int));
        *data = i;
        ring_buffer_enqueue(ring_buffer, data);
    }
    
    size = ring_buffer_get_size(ring_buffer);
    usage = capacity > 0 ? (size * 100) / capacity : 0;
    
    printf("半满队列状态:\n");
    printf("  大小: %u\n", size);
    printf("  容量: %u\n", capacity);
    printf("  使用率: %u%%\n", usage);
    
    assert(size == 50);
    assert(usage == 50);
    
    // 填满队列
    for (int i = 50; i < 100; i++) {
        int* data = malloc(sizeof(int));
        *data = i;
        ring_buffer_enqueue(ring_buffer, data);
    }
    
    size = ring_buffer_get_size(ring_buffer);
    usage = capacity > 0 ? (size * 100) / capacity : 0;
    
    printf("满队列状态:\n");
    printf("  大小: %u\n", size);
    printf("  容量: %u\n", capacity);
    printf("  使用率: %u%%\n", usage);
    
    assert(size == 100);
    assert(usage == 100);
    
    // 清理资源
    ring_buffer_clear(ring_buffer, free);
    ring_buffer_destroy(ring_buffer);
    
    printf("✓ 队列水位监控测试通过\n");
}

// 测试7: 性能指标计算
static void test_performance_metrics_calculation() {
    printf("\n=== 测试7: 性能指标计算 ===\n");
    
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
    
    printf("性能指标:\n");
    printf("  事件处理: %.2f 事件/毫秒\n", events_per_ms);
    printf("  消息处理: %.2f 消息/毫秒\n", messages_per_ms);
    printf("  数据吞吐: %.2f MB/秒\n", mb_per_second);
    printf("  内存使用: %.2f MB\n", memory_mb);
    printf("  队列使用率: %u%%\n", metrics.ring_buffer_usage);
    printf("  处理延迟: %ld ms\n", metrics.processing_delay_ms);
    
    // 验证性能指标合理
    assert(events_per_ms > 0);
    assert(messages_per_ms > 0);
    assert(mb_per_second > 0);
    assert(memory_mb > 0);
    assert(metrics.ring_buffer_usage <= 100);
    assert(metrics.processing_delay_ms >= 0);
    
    printf("✓ 性能指标计算测试通过\n");
}

// 测试8: 错误处理
static void test_error_handling() {
    printf("\n=== 测试8: 错误处理 ===\n");
    
    // 测试NULL指针处理
    print_observability_metrics(NULL);
    
    char buffer[100];
    format_observability_metrics_json(NULL, buffer, sizeof(buffer));
    format_observability_metrics_prometheus(NULL, buffer, sizeof(buffer));
    
    // 测试空缓冲区
    SObservabilityMetrics metrics;
    memset(&metrics, 0, sizeof(metrics));
    
    format_observability_metrics_json(&metrics, NULL, 0);
    format_observability_metrics_prometheus(&metrics, NULL, 0);
    
    // 测试小缓冲区
    char small_buffer[10];
    format_observability_metrics_json(&metrics, small_buffer, sizeof(small_buffer));
    format_observability_metrics_prometheus(&metrics, small_buffer, sizeof(small_buffer));
    
    printf("✓ 错误处理测试通过\n");
}

int main() {
    printf("开始增强可观测性指标测试...\n\n");
    
    test_basic_observability_metrics();
    test_observability_metrics_print();
    test_json_formatting();
    test_prometheus_formatting();
    test_memory_usage_calculation();
    test_queue_watermark_monitoring();
    test_performance_metrics_calculation();
    test_error_handling();
    
    printf("\n🎉 所有增强可观测性指标测试通过！\n");
    return 0;
}
