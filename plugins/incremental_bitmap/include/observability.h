#ifndef OBSERVABILITY_H
#define OBSERVABILITY_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// 可观测性指标结构体
typedef struct {
    // 速率指标
    uint64_t events_per_second;      // 事件处理速率
    uint64_t messages_per_second;    // 消息消费速率
    uint64_t bytes_per_second;       // 数据吞吐量
    
    // 滞后指标
    int64_t consumer_lag_ms;         // 消费者滞后时间(毫秒)
    uint64_t offset_lag;             // Offset滞后数量
    int64_t processing_delay_ms;     // 处理延迟(毫秒)
    
    // 丢弃指标
    uint64_t events_dropped;         // 丢弃事件数
    uint64_t messages_dropped;       // 丢弃消息数
    uint64_t parse_errors;           // 解析错误数
    
    // 重试指标
    uint64_t connection_retries;     // 连接重试次数
    uint64_t subscription_retries;   // 订阅重试次数
    uint64_t commit_retries;         // 提交重试次数
    
    // 队列水位
    uint32_t ring_buffer_usage;      // 环形队列使用率(%)
    uint32_t ring_buffer_capacity;   // 环形队列容量
    uint32_t event_queue_size;       // 事件队列大小
    
    // 内存指标
    size_t memory_usage_bytes;       // 内存使用量(字节)
    size_t bitmap_memory_bytes;      // 位图内存使用量
    size_t metadata_memory_bytes;    // 元数据内存使用量
    
    // 时间戳
    int64_t last_update_time;        // 最后更新时间
    int64_t uptime_seconds;          // 运行时间(秒)
} SObservabilityMetrics;

// 可观测性指标更新函数
void update_observability_metrics(SObservabilityMetrics* metrics);

// 可观测性指标打印函数
void print_observability_metrics(const SObservabilityMetrics* metrics);

// 格式化可观测性指标为JSON字符串
void format_observability_metrics_json(const SObservabilityMetrics* metrics, char* buffer, size_t buffer_size);

// 格式化可观测性指标为Prometheus格式
void format_observability_metrics_prometheus(const SObservabilityMetrics* metrics, char* buffer, size_t buffer_size);

#ifdef __cplusplus
}
#endif

#endif // OBSERVABILITY_H

