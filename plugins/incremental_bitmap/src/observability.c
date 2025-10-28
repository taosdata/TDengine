#include "observability.h"
#include <stdio.h>
#include <string.h>
#include <time.h>

// 可观测性指标更新函数
void update_observability_metrics(SObservabilityMetrics* metrics) {
    if (!metrics) {
        return;
    }
    
    // 更新最后更新时间
    metrics->last_update_time = time(NULL) * 1000; // 转换为毫秒
    
    // 这里可以添加更多的指标更新逻辑
    // 例如从系统获取内存使用情况等
}

// 可观测性指标打印函数
void print_observability_metrics(const SObservabilityMetrics* metrics) {
    if (!metrics) {
        printf("可观测性指标: NULL\n");
        return;
    }
    
    printf("\n=== TDengine 增量位图插件可观测性指标 ===\n");
    
    // 速率指标
    printf("📊 速率指标:\n");
    printf("  事件处理速率: %lu 事件/秒\n", metrics->events_per_second);
    printf("  消息消费速率: %lu 消息/秒\n", metrics->messages_per_second);
    printf("  数据吞吐量: %lu 字节/秒\n", metrics->bytes_per_second);
    
    // 滞后指标
    printf("\n⏱️  滞后指标:\n");
    printf("  消费者滞后时间: %ld ms\n", metrics->consumer_lag_ms);
    printf("  Offset滞后数量: %lu\n", metrics->offset_lag);
    printf("  处理延迟: %ld ms\n", metrics->processing_delay_ms);
    
    // 丢弃指标
    printf("\n❌ 丢弃指标:\n");
    printf("  丢弃事件数: %lu\n", metrics->events_dropped);
    printf("  丢弃消息数: %lu\n", metrics->messages_dropped);
    printf("  解析错误数: %lu\n", metrics->parse_errors);
    
    // 重试指标
    printf("\n🔄 重试指标:\n");
    printf("  连接重试次数: %lu\n", metrics->connection_retries);
    printf("  订阅重试次数: %lu\n", metrics->subscription_retries);
    printf("  提交重试次数: %lu\n", metrics->commit_retries);
    
    // 队列水位
    printf("\n📦 队列水位:\n");
    printf("  环形队列使用率: %u%%\n", metrics->ring_buffer_usage);
    printf("  环形队列容量: %u\n", metrics->ring_buffer_capacity);
    printf("  事件队列大小: %u\n", metrics->event_queue_size);
    
    // 内存指标
    printf("\n💾 内存指标:\n");
    printf("  总内存使用: %zu 字节 (%.2f MB)\n", 
           metrics->memory_usage_bytes, 
           metrics->memory_usage_bytes / (1024.0 * 1024.0));
    printf("  位图内存使用: %zu 字节 (%.2f MB)\n", 
           metrics->bitmap_memory_bytes, 
           metrics->bitmap_memory_bytes / (1024.0 * 1024.0));
    printf("  元数据内存使用: %zu 字节 (%.2f MB)\n", 
           metrics->metadata_memory_bytes, 
           metrics->metadata_memory_bytes / (1024.0 * 1024.0));
    
    // 时间戳
    printf("\n⏰ 时间信息:\n");
    printf("  最后更新时间: %ld\n", metrics->last_update_time);
    printf("  运行时间: %ld 秒 (%.2f 小时)\n", 
           metrics->uptime_seconds, 
           metrics->uptime_seconds / 3600.0);
    
    printf("\n==========================================\n");
}

// 格式化可观测性指标为JSON字符串
void format_observability_metrics_json(const SObservabilityMetrics* metrics, char* buffer, size_t buffer_size) {
    if (!metrics || !buffer || buffer_size == 0) {
        return;
    }
    
    snprintf(buffer, buffer_size,
        "{"
        "\"rate\":{"
            "\"events_per_second\":%lu,"
            "\"messages_per_second\":%lu,"
            "\"bytes_per_second\":%lu"
        "},"
        "\"lag\":{"
            "\"consumer_lag_ms\":%ld,"
            "\"offset_lag\":%lu,"
            "\"processing_delay_ms\":%ld"
        "},"
        "\"dropped\":{"
            "\"events_dropped\":%lu,"
            "\"messages_dropped\":%lu,"
            "\"parse_errors\":%lu"
        "},"
        "\"retries\":{"
            "\"connection_retries\":%lu,"
            "\"subscription_retries\":%lu,"
            "\"commit_retries\":%lu"
        "},"
        "\"queue\":{"
            "\"ring_buffer_usage\":%u,"
            "\"ring_buffer_capacity\":%u,"
            "\"event_queue_size\":%u"
        "},"
        "\"memory\":{"
            "\"memory_usage_bytes\":%zu,"
            "\"bitmap_memory_bytes\":%zu,"
            "\"metadata_memory_bytes\":%zu"
        "},"
        "\"time\":{"
            "\"last_update_time\":%ld,"
            "\"uptime_seconds\":%ld"
        "}"
        "}",
        metrics->events_per_second,
        metrics->messages_per_second,
        metrics->bytes_per_second,
        metrics->consumer_lag_ms,
        metrics->offset_lag,
        metrics->processing_delay_ms,
        metrics->events_dropped,
        metrics->messages_dropped,
        metrics->parse_errors,
        metrics->connection_retries,
        metrics->subscription_retries,
        metrics->commit_retries,
        metrics->ring_buffer_usage,
        metrics->ring_buffer_capacity,
        metrics->event_queue_size,
        metrics->memory_usage_bytes,
        metrics->bitmap_memory_bytes,
        metrics->metadata_memory_bytes,
        metrics->last_update_time,
        metrics->uptime_seconds
    );
}

// 格式化可观测性指标为Prometheus格式
void format_observability_metrics_prometheus(const SObservabilityMetrics* metrics, char* buffer, size_t buffer_size) {
    if (!metrics || !buffer || buffer_size == 0) {
        return;
    }
    
    size_t offset = 0;
    int written;
    
    // 速率指标
    written = snprintf(buffer + offset, buffer_size - offset,
        "# HELP tdengine_events_per_second Events processed per second\n"
        "# TYPE tdengine_events_per_second gauge\n"
        "tdengine_events_per_second %lu\n"
        "# HELP tdengine_messages_per_second Messages consumed per second\n"
        "# TYPE tdengine_messages_per_second gauge\n"
        "tdengine_messages_per_second %lu\n"
        "# HELP tdengine_bytes_per_second Data throughput in bytes per second\n"
        "# TYPE tdengine_bytes_per_second gauge\n"
        "tdengine_bytes_per_second %lu\n",
        metrics->events_per_second,
        metrics->messages_per_second,
        metrics->bytes_per_second
    );
    if (written < 0 || (size_t)written >= buffer_size - offset) return;
    offset += written;
    
    // 滞后指标
    written = snprintf(buffer + offset, buffer_size - offset,
        "# HELP tdengine_consumer_lag_ms Consumer lag in milliseconds\n"
        "# TYPE tdengine_consumer_lag_ms gauge\n"
        "tdengine_consumer_lag_ms %ld\n"
        "# HELP tdengine_offset_lag Offset lag count\n"
        "# TYPE tdengine_offset_lag gauge\n"
        "tdengine_offset_lag %lu\n"
        "# HELP tdengine_processing_delay_ms Processing delay in milliseconds\n"
        "# TYPE tdengine_processing_delay_ms gauge\n"
        "tdengine_processing_delay_ms %ld\n",
        metrics->consumer_lag_ms,
        metrics->offset_lag,
        metrics->processing_delay_ms
    );
    if (written < 0 || (size_t)written >= buffer_size - offset) return;
    offset += written;
    
    // 丢弃指标
    written = snprintf(buffer + offset, buffer_size - offset,
        "# HELP tdengine_events_dropped Total events dropped\n"
        "# TYPE tdengine_events_dropped counter\n"
        "tdengine_events_dropped %lu\n"
        "# HELP tdengine_messages_dropped Total messages dropped\n"
        "# TYPE tdengine_messages_dropped counter\n"
        "tdengine_messages_dropped %lu\n"
        "# HELP tdengine_parse_errors Total parse errors\n"
        "# TYPE tdengine_parse_errors counter\n"
        "tdengine_parse_errors %lu\n",
        metrics->events_dropped,
        metrics->messages_dropped,
        metrics->parse_errors
    );
    if (written < 0 || (size_t)written >= buffer_size - offset) return;
    offset += written;
    
    // 重试指标
    written = snprintf(buffer + offset, buffer_size - offset,
        "# HELP tdengine_connection_retries Total connection retries\n"
        "# TYPE tdengine_connection_retries counter\n"
        "tdengine_connection_retries %lu\n"
        "# HELP tdengine_subscription_retries Total subscription retries\n"
        "# TYPE tdengine_subscription_retries counter\n"
        "tdengine_subscription_retries %lu\n"
        "# HELP tdengine_commit_retries Total commit retries\n"
        "# TYPE tdengine_commit_retries counter\n"
        "tdengine_commit_retries %lu\n",
        metrics->connection_retries,
        metrics->subscription_retries,
        metrics->commit_retries
    );
    if (written < 0 || (size_t)written >= buffer_size - offset) return;
    offset += written;
    
    // 队列指标
    written = snprintf(buffer + offset, buffer_size - offset,
        "# HELP tdengine_ring_buffer_usage Ring buffer usage percentage\n"
        "# TYPE tdengine_ring_buffer_usage gauge\n"
        "tdengine_ring_buffer_usage %u\n"
        "# HELP tdengine_ring_buffer_capacity Ring buffer capacity\n"
        "# TYPE tdengine_ring_buffer_capacity gauge\n"
        "tdengine_ring_buffer_capacity %u\n"
        "# HELP tdengine_event_queue_size Event queue size\n"
        "# TYPE tdengine_event_queue_size gauge\n"
        "tdengine_event_queue_size %u\n",
        metrics->ring_buffer_usage,
        metrics->ring_buffer_capacity,
        metrics->event_queue_size
    );
    if (written < 0 || (size_t)written >= buffer_size - offset) return;
    offset += written;
    
    // 内存指标
    written = snprintf(buffer + offset, buffer_size - offset,
        "# HELP tdengine_memory_usage_bytes Total memory usage in bytes\n"
        "# TYPE tdengine_memory_usage_bytes gauge\n"
        "tdengine_memory_usage_bytes %zu\n"
        "# HELP tdengine_bitmap_memory_bytes Bitmap memory usage in bytes\n"
        "# TYPE tdengine_bitmap_memory_bytes gauge\n"
        "tdengine_bitmap_memory_bytes %zu\n"
        "# HELP tdengine_metadata_memory_bytes Metadata memory usage in bytes\n"
        "# TYPE tdengine_metadata_memory_bytes gauge\n"
        "tdengine_metadata_memory_bytes %zu\n",
        metrics->memory_usage_bytes,
        metrics->bitmap_memory_bytes,
        metrics->metadata_memory_bytes
    );
    if (written < 0 || (size_t)written >= buffer_size - offset) return;
    offset += written;
    
    // 时间指标
    written = snprintf(buffer + offset, buffer_size - offset,
        "# HELP tdengine_uptime_seconds Total uptime in seconds\n"
        "# TYPE tdengine_uptime_seconds gauge\n"
        "tdengine_uptime_seconds %ld\n",
        metrics->uptime_seconds
    );
    if (written < 0 || (size_t)written >= buffer_size - offset) return;
    offset += written;
}
