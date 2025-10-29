#include "event_interceptor.h"
#include "storage_engine_interface.h"
#include "ring_buffer.h"
#include "bitmap_engine.h"
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>

// 回调线程参数
typedef struct {
    SEventInterceptor* interceptor;
    uint32_t thread_id;
    bool running;
} SCallbackThreadParam;

// 获取当前时间戳（纳秒）
static int64_t get_current_timestamp() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + (int64_t)ts.tv_nsec;
}

// 回调线程函数
static void* callback_thread_func(void* arg) {
    SEventInterceptor* interceptor = (SEventInterceptor*)arg;
    
    while (!interceptor->stop_threads) {
        // 从环形队列中获取事件
        void* event_ptr;
        int32_t result = ring_buffer_dequeue_blocking(interceptor->event_buffer, &event_ptr, 1000); // 1秒超时
        
        if (result == 0) {
            SBlockEvent* event = (SBlockEvent*)event_ptr;
            
            // 更新位图引擎
            switch (event->event_type) {
                case EVENT_BLOCK_CREATE:
                    bitmap_engine_mark_new(interceptor->bitmap_engine, event->block_id, event->wal_offset, event->timestamp);
                    break;
                case EVENT_BLOCK_UPDATE:
                    bitmap_engine_mark_dirty(interceptor->bitmap_engine, event->block_id, event->wal_offset, event->timestamp);
                    break;
                case EVENT_BLOCK_FLUSH:
                    bitmap_engine_clear_block(interceptor->bitmap_engine, event->block_id);
                    break;
                case EVENT_BLOCK_DELETE:
                    bitmap_engine_mark_deleted(interceptor->bitmap_engine, event->block_id, event->wal_offset, event->timestamp);
                    break;
                default:
                    break;
            }
            
            // 处理事件
            if (interceptor->config.callback) {
                interceptor->config.callback(event, interceptor->config.callback_user_data);
            }
            
            // 更新统计信息
            pthread_mutex_lock(&interceptor->mutex);
            interceptor->events_processed++;
            pthread_mutex_unlock(&interceptor->mutex);
            
            // 释放事件内存
            free(event);
        }
    }
    
    return NULL;
}

SEventInterceptor* event_interceptor_init(const SEventInterceptorConfig* config,
                                         SBitmapEngine* bitmap_engine) {
    if (!config || !bitmap_engine) {
        return NULL;
    }
    
    SEventInterceptor* interceptor = (SEventInterceptor*)malloc(sizeof(SEventInterceptor));
    if (!interceptor) {
        return NULL;
    }
    
    // 复制配置
    memcpy(&interceptor->config, config, sizeof(SEventInterceptorConfig));
    interceptor->bitmap_engine = bitmap_engine;
    interceptor->buffer_size = config->event_buffer_size;
    interceptor->thread_count = config->callback_threads;
    interceptor->stop_threads = false;
    interceptor->events_processed = 0;
    interceptor->events_dropped = 0;
    interceptor->storage_interface = NULL;
    
    // 初始化事件缓冲区（环形队列）
    interceptor->event_buffer = ring_buffer_init(interceptor->buffer_size);
    if (!interceptor->event_buffer) {
        free(interceptor);
        return NULL;
    }
    
    // 初始化互斥锁
    if (pthread_mutex_init(&interceptor->mutex, NULL) != 0) {
        ring_buffer_destroy(interceptor->event_buffer);
        free(interceptor);
        return NULL;
    }
    
    // 初始化条件变量
    if (pthread_cond_init(&interceptor->condition, NULL) != 0) {
        pthread_mutex_destroy(&interceptor->mutex);
        ring_buffer_destroy(interceptor->event_buffer);
        free(interceptor);
        return NULL;
    }
    
    // 分配回调线程数组
    interceptor->callback_threads = (pthread_t*)malloc(sizeof(pthread_t) * interceptor->thread_count);
    if (!interceptor->callback_threads) {
        pthread_cond_destroy(&interceptor->condition);
        pthread_mutex_destroy(&interceptor->mutex);
        ring_buffer_destroy(interceptor->event_buffer);
        free(interceptor);
        return NULL;
    }
    
    // 初始化线程ID为0
    for (uint32_t i = 0; i < interceptor->thread_count; i++) {
        interceptor->callback_threads[i] = 0;
    }
    
    return interceptor;
}

void event_interceptor_destroy(SEventInterceptor* interceptor) {
    if (!interceptor) {
        return;
    }
    
    // 停止所有线程（如果正在运行）
    if (!interceptor->stop_threads) {
        event_interceptor_stop(interceptor);
    }
    
    // 销毁存储引擎接口
    if (interceptor->storage_interface) {
        interceptor->storage_interface->destroy();
        interceptor->storage_interface = NULL;
    }
    
    // 销毁环形队列
    if (interceptor->event_buffer) {
        ring_buffer_destroy(interceptor->event_buffer);
    }
    
    // 销毁条件变量
    pthread_cond_destroy(&interceptor->condition);
    
    // 销毁互斥锁
    pthread_mutex_destroy(&interceptor->mutex);
    
    // 释放线程数组
    if (interceptor->callback_threads) {
        free(interceptor->callback_threads);
    }
    
    free(interceptor);
}

int32_t event_interceptor_start(SEventInterceptor* interceptor) {
    if (!interceptor) {
        return -1;
    }
    
    pthread_mutex_lock(&interceptor->mutex);
    
    if (interceptor->stop_threads) {
        // 已经启动
        pthread_mutex_unlock(&interceptor->mutex);
        return 0;
    }
    
    interceptor->stop_threads = false;
    
    // 创建回调线程
    for (uint32_t i = 0; i < interceptor->thread_count; i++) {
        if (pthread_create(&interceptor->callback_threads[i], NULL,
                          callback_thread_func, interceptor) != 0) {
            // 创建失败，停止已创建的线程
            for (uint32_t j = 0; j < i; j++) {
                pthread_join(interceptor->callback_threads[j], NULL);
            }
            pthread_mutex_unlock(&interceptor->mutex);
            return -1;
        }
    }
    
    pthread_mutex_unlock(&interceptor->mutex);
    return 0;
}

int32_t event_interceptor_stop(SEventInterceptor* interceptor) {
    if (!interceptor) {
        return -1;
    }
    
    pthread_mutex_lock(&interceptor->mutex);
    
    if (interceptor->stop_threads) {
        // 已经停止
        pthread_mutex_unlock(&interceptor->mutex);
        return 0;
    }
    
    interceptor->stop_threads = true;
    
    // 等待所有线程结束（只有在线程已经创建的情况下）
    for (uint32_t i = 0; i < interceptor->thread_count; i++) {
        if (interceptor->callback_threads[i] != 0) {
            pthread_join(interceptor->callback_threads[i], NULL);
        }
    }
    
    pthread_mutex_unlock(&interceptor->mutex);
    return 0;
}

int32_t event_interceptor_on_block_create(SEventInterceptor* interceptor,
                                         uint64_t block_id, uint64_t wal_offset, int64_t timestamp) {
    if (!interceptor || !interceptor->config.enable_interception) {
        return 0;
    }
    
    SBlockEvent* event = (SBlockEvent*)malloc(sizeof(SBlockEvent));
    if (!event) {
        return -1;
    }
    
    event->event_type = EVENT_BLOCK_CREATE;
    event->block_id = block_id;
    event->wal_offset = wal_offset;
    event->timestamp = timestamp;
    event->user_data = NULL;
    
    int32_t result = ring_buffer_enqueue(interceptor->event_buffer, event);
    if (result != 0) {
        pthread_mutex_lock(&interceptor->mutex);
        interceptor->events_dropped++;
        pthread_mutex_unlock(&interceptor->mutex);
        free(event);
    }
    
    return result;
}

int32_t event_interceptor_on_block_update(SEventInterceptor* interceptor,
                                         uint64_t block_id, uint64_t wal_offset, int64_t timestamp) {
    if (!interceptor || !interceptor->config.enable_interception) {
        return 0;
    }
    
    SBlockEvent* event = (SBlockEvent*)malloc(sizeof(SBlockEvent));
    if (!event) {
        return -1;
    }
    
    event->event_type = EVENT_BLOCK_UPDATE;
    event->block_id = block_id;
    event->wal_offset = wal_offset;
    event->timestamp = timestamp;
    event->user_data = NULL;
    
    int32_t result = ring_buffer_enqueue(interceptor->event_buffer, event);
    if (result != 0) {
        pthread_mutex_lock(&interceptor->mutex);
        interceptor->events_dropped++;
        pthread_mutex_unlock(&interceptor->mutex);
        free(event);
    }
    
    return result;
}

int32_t event_interceptor_on_block_flush(SEventInterceptor* interceptor,
                                        uint64_t block_id, uint64_t wal_offset, int64_t timestamp) {
    if (!interceptor || !interceptor->config.enable_interception) {
        return 0;
    }
    
    SBlockEvent* event = (SBlockEvent*)malloc(sizeof(SBlockEvent));
    if (!event) {
        return -1;
    }
    
    event->event_type = EVENT_BLOCK_FLUSH;
    event->block_id = block_id;
    event->wal_offset = wal_offset;
    event->timestamp = timestamp;
    event->user_data = NULL;
    
    int32_t result = ring_buffer_enqueue(interceptor->event_buffer, event);
    if (result != 0) {
        pthread_mutex_lock(&interceptor->mutex);
        interceptor->events_dropped++;
        pthread_mutex_unlock(&interceptor->mutex);
        free(event);
    }
    
    return result;
}

int32_t event_interceptor_on_block_delete(SEventInterceptor* interceptor,
                                         uint64_t block_id, uint64_t wal_offset, int64_t timestamp) {
    if (!interceptor || !interceptor->config.enable_interception) {
        return 0;
    }
    
    SBlockEvent* event = (SBlockEvent*)malloc(sizeof(SBlockEvent));
    if (!event) {
        return -1;
    }
    
    event->event_type = EVENT_BLOCK_DELETE;
    event->block_id = block_id;
    event->wal_offset = wal_offset;
    event->timestamp = timestamp;
    event->user_data = NULL;
    
    int32_t result = ring_buffer_enqueue(interceptor->event_buffer, event);
    if (result != 0) {
        pthread_mutex_lock(&interceptor->mutex);
        interceptor->events_dropped++;
        pthread_mutex_unlock(&interceptor->mutex);
        free(event);
    }
    
    return result;
}

void event_interceptor_get_stats(SEventInterceptor* interceptor,
                                uint64_t* events_processed, uint64_t* events_dropped) {
    if (!interceptor) {
        return;
    }
    
    pthread_mutex_lock(&interceptor->mutex);
    
    if (events_processed) {
        *events_processed = interceptor->events_processed;
    }
    if (events_dropped) {
        *events_dropped = interceptor->events_dropped;
    }
    
    pthread_mutex_unlock(&interceptor->mutex);
}

// 存储引擎接口管理
int32_t event_interceptor_set_storage_interface(SEventInterceptor* interceptor,
                                               SStorageEngineInterface* interface) {
    if (!interceptor || !interface) {
        return -1;
    }
    
    // 如果已有接口，先销毁
    if (interceptor->storage_interface) {
        interceptor->storage_interface->destroy();
    }
    
    interceptor->storage_interface = interface;
    return 0;
}

int32_t event_interceptor_install_storage_interception(SEventInterceptor* interceptor) {
    if (!interceptor || !interceptor->storage_interface) {
        return -1;
    }
    
    return interceptor->storage_interface->install_interception();
}

int32_t event_interceptor_uninstall_storage_interception(SEventInterceptor* interceptor) {
    if (!interceptor || !interceptor->storage_interface) {
        return -1;
    }
    
    return interceptor->storage_interface->uninstall_interception();
}

// 测试用的手动事件触发
int32_t event_interceptor_trigger_test_event(SEventInterceptor* interceptor,
                                            EBlockEventType event_type,
                                            uint64_t block_id, uint64_t wal_offset, int64_t timestamp) {
    switch (event_type) {
        case EVENT_BLOCK_CREATE:
            return event_interceptor_on_block_create(interceptor, block_id, wal_offset, timestamp);
        case EVENT_BLOCK_UPDATE:
            return event_interceptor_on_block_update(interceptor, block_id, wal_offset, timestamp);
        case EVENT_BLOCK_FLUSH:
            return event_interceptor_on_block_flush(interceptor, block_id, wal_offset, timestamp);
        case EVENT_BLOCK_DELETE:
            return event_interceptor_on_block_delete(interceptor, block_id, wal_offset, timestamp);
        default:
            return -1;
    }
} 