#ifndef EVENT_INTERCEPTOR_H
#define EVENT_INTERCEPTOR_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include "storage_engine_interface.h"

#ifdef __cplusplus
extern "C" {
#endif

// 前向声明
struct SBitmapEngine;
struct SStorageEngineInterface;

// 块事件类型
typedef enum {
    EVENT_BLOCK_CREATE = 0,
    EVENT_BLOCK_UPDATE,
    EVENT_BLOCK_FLUSH,
    EVENT_BLOCK_DELETE,
    EVENT_MAX
} EBlockEventType;

// 块事件结构
typedef struct {
    EBlockEventType event_type;
    uint64_t block_id;
    uint64_t wal_offset;
    int64_t timestamp;
    void* user_data;
} SBlockEvent;

// 事件回调函数类型
typedef void (*BlockEventCallback)(const SBlockEvent* event, void* user_data);

// 事件拦截器配置
typedef struct {
    bool enable_interception;
    BlockEventCallback callback;
    void* callback_user_data;
    uint32_t event_buffer_size;
    uint32_t callback_threads;
} SEventInterceptorConfig;

// 事件拦截器结构
typedef struct {
    SEventInterceptorConfig config;
    struct SBitmapEngine* bitmap_engine;
    SStorageEngineInterface* storage_interface;
    
    // 事件缓冲区
    void* event_buffer;  // SRingBuffer*
    
    // 线程管理
    pthread_t* callback_threads;
    uint32_t thread_count;
    bool stop_threads;
    
    // 同步机制
    pthread_mutex_t mutex;
    pthread_cond_t condition;
    
    // 统计信息
    uint64_t events_processed;
    uint64_t events_dropped;
    
    // 配置参数
    uint32_t buffer_size;
} SEventInterceptor;

// 事件拦截器管理函数
SEventInterceptor* event_interceptor_init(const SEventInterceptorConfig* config,
                                         struct SBitmapEngine* bitmap_engine);

void event_interceptor_destroy(SEventInterceptor* interceptor);

int32_t event_interceptor_start(SEventInterceptor* interceptor);

int32_t event_interceptor_stop(SEventInterceptor* interceptor);

// 事件处理函数
int32_t event_interceptor_on_block_create(SEventInterceptor* interceptor,
                                         uint64_t block_id, uint64_t wal_offset, int64_t timestamp);

int32_t event_interceptor_on_block_update(SEventInterceptor* interceptor,
                                         uint64_t block_id, uint64_t wal_offset, int64_t timestamp);

int32_t event_interceptor_on_block_flush(SEventInterceptor* interceptor,
                                        uint64_t block_id, uint64_t wal_offset, int64_t timestamp);

int32_t event_interceptor_on_block_delete(SEventInterceptor* interceptor,
                                         uint64_t block_id, uint64_t wal_offset, int64_t timestamp);

// 统计信息
void event_interceptor_get_stats(SEventInterceptor* interceptor,
                                uint64_t* events_processed, uint64_t* events_dropped);

// 存储引擎接口管理
int32_t event_interceptor_set_storage_interface(SEventInterceptor* interceptor,
                                               SStorageEngineInterface* interface);

int32_t event_interceptor_install_storage_interception(SEventInterceptor* interceptor);

int32_t event_interceptor_uninstall_storage_interception(SEventInterceptor* interceptor);

// 测试支持
int32_t event_interceptor_trigger_test_event(SEventInterceptor* interceptor,
                                            EBlockEventType event_type,
                                            uint64_t block_id, uint64_t wal_offset, int64_t timestamp);

#ifdef __cplusplus
}
#endif

#endif // EVENT_INTERCEPTOR_H 