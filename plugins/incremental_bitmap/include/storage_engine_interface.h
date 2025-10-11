#ifndef STORAGE_ENGINE_INTERFACE_H
#define STORAGE_ENGINE_INTERFACE_H

#include <stdint.h>
#include <stdbool.h>
#include "observability.h"

#ifdef __cplusplus
extern "C" {
#endif

// 存储引擎事件类型
typedef enum {
    STORAGE_EVENT_BLOCK_CREATE = 0,
    STORAGE_EVENT_BLOCK_UPDATE,
    STORAGE_EVENT_BLOCK_FLUSH,
    STORAGE_EVENT_BLOCK_DELETE,
    STORAGE_EVENT_MAX
} EStorageEventType;

// 存储引擎事件结构
typedef struct {
    EStorageEventType event_type;
    uint64_t block_id;
    uint64_t wal_offset;
    int64_t timestamp;
    void* user_data;
} SStorageEvent;

// 存储引擎事件回调函数类型
typedef void (*StorageEventCallback)(const SStorageEvent* event, void* user_data);

// 存储引擎接口配置
typedef struct {
    bool enable_interception;
    StorageEventCallback event_callback;
    void* callback_user_data;
    uint32_t event_buffer_size;
    uint32_t callback_threads;
} SStorageEngineConfig;

// 存储引擎接口函数指针
typedef struct {
    // 初始化存储引擎接口
    int32_t (*init)(const SStorageEngineConfig* config);
    
    // 销毁存储引擎接口
    void (*destroy)(void);
    
    // 安装事件拦截
    int32_t (*install_interception)(void);
    
    // 卸载事件拦截
    int32_t (*uninstall_interception)(void);
    
    // 手动触发事件（用于测试）
    int32_t (*trigger_event)(const SStorageEvent* event);
    
    // 获取统计信息
    int32_t (*get_stats)(uint64_t* events_processed, uint64_t* events_dropped);
    
    // 获取详细统计信息
    int32_t (*get_detailed_stats)(void* engine, void* stats);
    
    // 获取可观测性指标
    int32_t (*get_observability_metrics)(void* engine, SObservabilityMetrics* metrics);
    
    // 重置统计信息
    int32_t (*reset_stats)(void* engine);
    
    // 检查是否支持当前存储引擎
    bool (*is_supported)(void);
    
    // 获取存储引擎名称
    const char* (*get_engine_name)(void);
} SStorageEngineInterface;

// 存储引擎接口工厂函数
typedef SStorageEngineInterface* (*StorageEngineInterfaceFactory)(void);

// 注册存储引擎接口工厂
int32_t register_storage_engine_interface(const char* name, StorageEngineInterfaceFactory factory);

// 获取存储引擎接口
SStorageEngineInterface* get_storage_engine_interface(const char* name);

// 获取默认存储引擎接口
SStorageEngineInterface* get_default_storage_engine_interface(void);

// 列出所有可用的存储引擎接口
int32_t list_storage_engine_interfaces(char** names, uint32_t max_count, uint32_t* actual_count);

#ifdef __cplusplus
}
#endif

#endif // STORAGE_ENGINE_INTERFACE_H

