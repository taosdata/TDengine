#include "storage_engine_interface.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <pthread.h>

// Mock存储引擎状态
typedef struct {
    bool initialized;
    bool interception_installed;
    uint64_t events_processed;
    uint64_t events_dropped;
    StorageEventCallback event_callback;
    void* callback_user_data;
    pthread_mutex_t mutex;
} SMockStorageEngine;

static SMockStorageEngine g_mock_engine = {
    .initialized = false,
    .interception_installed = false,
    .events_processed = 0,
    .events_dropped = 0,
    .mutex = PTHREAD_MUTEX_INITIALIZER
};

// Mock存储引擎实现函数
static int32_t mock_init(const SStorageEngineConfig* config) {
    if (!config) return -1;
    
    pthread_mutex_lock(&g_mock_engine.mutex);
    g_mock_engine.initialized = true;
    g_mock_engine.events_processed = 0;
    g_mock_engine.events_dropped = 0;
    g_mock_engine.event_callback = config->event_callback;
    g_mock_engine.callback_user_data = config->callback_user_data;
    pthread_mutex_unlock(&g_mock_engine.mutex);
    
    printf("[Mock] 存储引擎初始化成功\n");
    return 0;
}

static void mock_destroy(void) {
    pthread_mutex_lock(&g_mock_engine.mutex);
    g_mock_engine.initialized = false;
    g_mock_engine.interception_installed = false;
    pthread_mutex_unlock(&g_mock_engine.mutex);
    
    printf("[Mock] 存储引擎销毁完成\n");
}

static int32_t mock_install_interception(void) {
    pthread_mutex_lock(&g_mock_engine.mutex);
    if (!g_mock_engine.initialized) {
        pthread_mutex_unlock(&g_mock_engine.mutex);
        return -1;
    }
    
    g_mock_engine.interception_installed = true;
    pthread_mutex_unlock(&g_mock_engine.mutex);
    
    printf("[Mock] 事件拦截安装成功\n");
    return 0;
}

static int32_t mock_uninstall_interception(void) {
    pthread_mutex_lock(&g_mock_engine.mutex);
    g_mock_engine.interception_installed = false;
    pthread_mutex_unlock(&g_mock_engine.mutex);
    
    printf("[Mock] 事件拦截卸载成功\n");
    return 0;
}

static int32_t mock_trigger_event(const SStorageEvent* event) {
    if (!event) {
        return -1;
    }
    
    pthread_mutex_lock(&g_mock_engine.mutex);
    if (!g_mock_engine.interception_installed) {
        pthread_mutex_unlock(&g_mock_engine.mutex);
        return -1;
    }
    
    g_mock_engine.events_processed++;
    StorageEventCallback cb = g_mock_engine.event_callback;
    void* user = g_mock_engine.callback_user_data;
    pthread_mutex_unlock(&g_mock_engine.mutex);
    
    printf("[Mock] 触发事件: 类型=%d, 块ID=%lu, WAL偏移量=%lu, 时间戳=%ld\n",
           event->event_type, event->block_id, event->wal_offset, event->timestamp);
    
    if (cb) {
        cb(event, user);
    }
    
    return 0;
}

static int32_t mock_get_stats(uint64_t* events_processed, uint64_t* events_dropped) {
    pthread_mutex_lock(&g_mock_engine.mutex);
    
    if (events_processed) {
        *events_processed = g_mock_engine.events_processed;
    }
    if (events_dropped) {
        *events_dropped = g_mock_engine.events_dropped;
    }
    
    pthread_mutex_unlock(&g_mock_engine.mutex);
    return 0;
}

static bool mock_is_supported(void) {
    return true; // Mock引擎总是支持的
}

static const char* mock_get_engine_name(void) {
    return "mock";
}

// Mock存储引擎接口
static SStorageEngineInterface g_mock_interface = {
    .init = mock_init,
    .destroy = mock_destroy,
    .install_interception = mock_install_interception,
    .uninstall_interception = mock_uninstall_interception,
    .trigger_event = mock_trigger_event,
    .get_stats = mock_get_stats,
    .is_supported = mock_is_supported,
    .get_engine_name = mock_get_engine_name
};

// Mock存储引擎工厂函数
SStorageEngineInterface* mock_storage_engine_create(void) {
    return &g_mock_interface;
}

// 便捷函数：注册Mock存储引擎
int32_t register_mock_storage_engine(void) {
    extern int32_t register_storage_engine_interface(const char* name, StorageEngineInterfaceFactory factory);
    return register_storage_engine_interface("mock", mock_storage_engine_create);
}

