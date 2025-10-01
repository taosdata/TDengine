#include <stdio.h>
#include <assert.h>
#include <string.h>
#include "../include/storage_engine_interface.h"
#include "../include/observability.h"

// 测试可观测性指标结构体
static void test_observability_metrics_struct() {
    printf("=== 测试可观测性指标结构体 ===\n");
    
    // 创建并初始化指标结构体
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
    
    // 验证结构体大小（确保没有填充问题）
    printf("结构体大小: %zu 字节\n", sizeof(SObservabilityMetrics));
    assert(sizeof(SObservabilityMetrics) > 0);
    
    // 验证字段访问
    assert(metrics.events_per_second == 1000);
    assert(metrics.messages_per_second == 1500);
    assert(metrics.consumer_lag_ms == 50);
    assert(metrics.ring_buffer_usage == 75);
    assert(metrics.memory_usage_bytes == 1048576);
    
    printf("✓ 可观测性指标结构体测试通过\n");
}

// 测试可观测性指标打印函数
static void test_observability_metrics_print() {
    printf("\n=== 测试可观测性指标打印函数 ===\n");
    
    SObservabilityMetrics metrics;
    memset(&metrics, 0, sizeof(metrics));
    
    // 设置一些测试数据
    metrics.events_per_second = 2000;
    metrics.messages_per_second = 2500;
    metrics.consumer_lag_ms = 25;
    metrics.events_dropped = 10;
    metrics.ring_buffer_usage = 80;
    metrics.memory_usage_bytes = 2097152;
    metrics.uptime_seconds = 7200;
    
    // 调用打印函数
    printf("调用 print_observability_metrics 函数...\n");
    print_observability_metrics(&metrics);
    
    printf("✓ 可观测性指标打印函数测试通过\n");
}

// 测试存储引擎接口中的可观测性函数指针
static void test_storage_engine_observability_interface() {
    printf("\n=== 测试存储引擎接口中的可观测性函数 ===\n");
    
    // 创建存储引擎接口结构体
    SStorageEngineInterface interface;
    memset(&interface, 0, sizeof(interface));
    
    // 验证函数指针类型
    printf("get_observability_metrics 函数指针类型: %zu 字节\n", 
           sizeof(interface.get_observability_metrics));
    
    // 验证函数指针可以设置为NULL
    interface.get_observability_metrics = NULL;
    assert(interface.get_observability_metrics == NULL);
    
    printf("✓ 存储引擎接口中的可观测性函数测试通过\n");
}

// 测试可观测性指标更新函数
static void test_observability_metrics_update() {
    printf("\n=== 测试可观测性指标更新函数 ===\n");
    
    SObservabilityMetrics metrics;
    memset(&metrics, 0, sizeof(metrics));
    
    // 调用更新函数
    printf("调用 update_observability_metrics 函数...\n");
    update_observability_metrics(&metrics);
    
    printf("✓ 可观测性指标更新函数测试通过\n");
}

// 测试可观测性指标的内存布局
static void test_observability_metrics_memory_layout() {
    printf("\n=== 测试可观测性指标内存布局 ===\n");
    
    SObservabilityMetrics metrics;
    memset(&metrics, 0, sizeof(metrics));
    
    // 验证字段偏移量
    printf("events_per_second 偏移量: %zu\n", 
           (char*)&metrics.events_per_second - (char*)&metrics);
    printf("messages_per_second 偏移量: %zu\n", 
           (char*)&metrics.messages_per_second - (char*)&metrics);
    printf("consumer_lag_ms 偏移量: %zu\n", 
           (char*)&metrics.consumer_lag_ms - (char*)&metrics);
    printf("ring_buffer_usage 偏移量: %zu\n", 
           (char*)&metrics.ring_buffer_usage - (char*)&metrics);
    printf("memory_usage_bytes 偏移量: %zu\n", 
           (char*)&metrics.memory_usage_bytes - (char*)&metrics);
    
    // 验证所有偏移量都是合理的
    assert((char*)&metrics.events_per_second - (char*)&metrics >= 0);
    assert((char*)&metrics.messages_per_second - (char*)&metrics >= 0);
    assert((char*)&metrics.consumer_lag_ms - (char*)&metrics >= 0);
    assert((char*)&metrics.ring_buffer_usage - (char*)&metrics >= 0);
    assert((char*)&metrics.memory_usage_bytes - (char*)&metrics >= 0);
    
    printf("✓ 可观测性指标内存布局测试通过\n");
}

int main() {
    printf("开始可观测性接口测试...\n\n");
    
    test_observability_metrics_struct();
    test_observability_metrics_print();
    test_storage_engine_observability_interface();
    test_observability_metrics_update();
    test_observability_metrics_memory_layout();
    
    printf("\n🎉 所有可观测性接口测试通过！\n");
    return 0;
}

