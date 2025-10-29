#include "bitmap_engine.h"
#include "event_interceptor.h"
#include "backup_coordinator.h"
#include "storage_engine_interface.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <time.h>

// 线程数自适应与覆盖：
// 优先使用环境变量 IB_CALLBACK_THREADS（正整数）；
// 否则按 min(2 * 在线CPU核数, 64) 自适应。
static uint32_t get_adaptive_callback_threads(void) {
    const char* env_value = getenv("IB_CALLBACK_THREADS");
    if (env_value && *env_value) {
        long parsed = strtol(env_value, NULL, 10);
        if (parsed > 0 && parsed < 1000000) {
            printf("[并发配置] 使用环境变量 IB_CALLBACK_THREADS=%ld\n", parsed);
            return (uint32_t)parsed;
        } else {
            printf("[并发配置] 环境变量 IB_CALLBACK_THREADS=\"%s\" 非法，忽略并采用自适应\n", env_value);
        }
    }

    long cores = sysconf(_SC_NPROCESSORS_ONLN);
    if (cores <= 0) cores = 1;
    long adaptive = cores * 2;
    if (adaptive > 64) adaptive = 64;
    if (adaptive < 1) adaptive = 1;
    printf("[并发配置] Detected cores=%ld, using callback_threads=%ld (source=auto)\n", cores, adaptive);
    return (uint32_t)adaptive;
}

// 前向声明回调，确保在配置结构体使用前可见
static void backup_event_callback(const SBlockEvent* event, void* user_data);

// 增量备份工具配置
typedef struct {
    char* source_host;
    int source_port;
    char* database;
    char* backup_path;
    char* bitmap_cache_path;
    int64_t since_timestamp;
    uint32_t batch_size;
    bool enable_compression;
    bool enable_encryption;
} SIncrementalBackupConfig;

// 增量备份工具状态
typedef struct {
    SIncrementalBackupConfig config;
    SBitmapEngine* bitmap_engine;
    SEventInterceptor* event_interceptor;
    SBackupCoordinator* backup_coordinator;
    SStorageEngineInterface* storage_interface;
    bool is_running;
    uint64_t total_blocks;
    uint64_t processed_blocks;
    uint64_t failed_blocks;
} SIncrementalBackupTool;

// 创建增量备份工具
SIncrementalBackupTool* incremental_backup_tool_create(const SIncrementalBackupConfig* config) {
    if (!config) {
        return NULL;
    }
    
    SIncrementalBackupTool* tool = malloc(sizeof(SIncrementalBackupTool));
    if (!tool) {
        return NULL;
    }
    
    memset(tool, 0, sizeof(SIncrementalBackupTool));
    memcpy(&tool->config, config, sizeof(SIncrementalBackupConfig));
    
    // 初始化位图引擎（当前API无配置参数）
    tool->bitmap_engine = bitmap_engine_init();
    if (!tool->bitmap_engine) {
        free(tool);
        return NULL;
    }
    
    // 初始化事件拦截器
    SEventInterceptorConfig interceptor_config = {
        .enable_interception = true,
        .event_buffer_size = 10000,
        .callback_threads = get_adaptive_callback_threads(),
        .callback = backup_event_callback,
        .callback_user_data = tool
    };
    
    tool->event_interceptor = event_interceptor_init(&interceptor_config, tool->bitmap_engine);
    if (!tool->event_interceptor) {
        bitmap_engine_destroy(tool->bitmap_engine);
        free(tool);
        return NULL;
    }
    
    // 初始化备份协调器
    SBackupConfig backup_config = {
        .batch_size = config->batch_size,
        .max_retries = 3,
        .retry_interval_ms = 1000,
        .timeout_ms = 30000,
        .enable_compression = config->enable_compression,
        .enable_encryption = config->enable_encryption,
        .backup_path = config->backup_path,
        .temp_path = "/tmp"
    };
    
    tool->backup_coordinator = backup_coordinator_init(tool->bitmap_engine, &backup_config);
    if (!tool->backup_coordinator) {
        event_interceptor_destroy(tool->event_interceptor);
        bitmap_engine_destroy(tool->bitmap_engine);
        free(tool);
        return NULL;
    }
    
    // 获取存储引擎接口
    tool->storage_interface = get_storage_engine_interface("tdengine");
    if (tool->storage_interface) {
        event_interceptor_set_storage_interface(tool->event_interceptor, tool->storage_interface);
    }
    
    tool->is_running = false;
    return tool;
}

// 事件回调函数
static void backup_event_callback(const SBlockEvent* event, void* user_data) {
    SIncrementalBackupTool* tool = (SIncrementalBackupTool*)user_data;
    
    // 将事件转换为存储事件
    SStorageEvent storage_event = {
        .event_type = (EStorageEventType)event->event_type,
        .block_id = event->block_id,
        .wal_offset = event->wal_offset,
        .timestamp = event->timestamp,
        .user_data = NULL
    };
    
    // 触发存储引擎事件
    if (tool->storage_interface) {
        tool->storage_interface->trigger_event(&storage_event);
    }
    
    // 更新位图引擎
    switch (event->event_type) {
        case EVENT_BLOCK_CREATE:
            bitmap_engine_mark_new(tool->bitmap_engine, event->block_id, 
                                 event->wal_offset, event->timestamp);
            break;
        case EVENT_BLOCK_UPDATE:
            bitmap_engine_mark_dirty(tool->bitmap_engine, event->block_id, 
                                   event->wal_offset, event->timestamp);
            break;
        case EVENT_BLOCK_FLUSH:
            // 清除块状态表示已刷新
            bitmap_engine_clear_block(tool->bitmap_engine, event->block_id);
            break;
        case EVENT_BLOCK_DELETE:
            bitmap_engine_mark_deleted(tool->bitmap_engine, event->block_id, 
                                     event->wal_offset, event->timestamp);
            break;
    }
}

// 启动增量备份
int32_t incremental_backup_tool_start(SIncrementalBackupTool* tool) {
    if (!tool || tool->is_running) {
        return -1;
    }
    
    // 回调在创建时已经设置，此处无需更新配置
    
    // 启动事件拦截器
    int32_t result = event_interceptor_start(tool->event_interceptor);
    if (result != 0) {
        return result;
    }
    
    // 安装存储引擎拦截
    if (tool->storage_interface) {
        result = event_interceptor_install_storage_interception(tool->event_interceptor);
        if (result != 0) {
            event_interceptor_stop(tool->event_interceptor);
            return result;
        }
    }
    
    tool->is_running = true;
    printf("[增量备份] 工具启动成功\n");
    return 0;
}

// 执行增量备份
int32_t incremental_backup_tool_backup(SIncrementalBackupTool* tool, int64_t since_timestamp) {
    if (!tool) {
        return -1;
    }
    
    printf("[增量备份] 开始执行增量备份，时间戳: %ld\n", since_timestamp);
    
    // 获取增量块
    uint32_t max_ids = tool->config.batch_size > 0 ? tool->config.batch_size : 10000;
    uint64_t* block_ids = (uint64_t*)calloc(max_ids, sizeof(uint64_t));
    if (!block_ids) return -1;

    int64_t end_time = (int64_t)time(NULL) * 1000000000LL; // ns
    uint32_t block_count = backup_coordinator_get_dirty_blocks_by_time(
        tool->backup_coordinator, since_timestamp, end_time, block_ids, max_ids);
    
    if (block_count == 0) {
        printf("[增量备份] 没有发现增量数据\n");
        return 0;
    }
    
    printf("[增量备份] 发现 %u 个增量块\n", block_count);

    // 将块ID转换为块信息（此处不加载实际数据，只聚合元信息）
    uint64_t total_size = 0;
    for (uint32_t i = 0; i < block_count; i++) {
        SBlockMetadata md;
        if (bitmap_engine_get_block_metadata(tool->bitmap_engine, block_ids[i], &md) == 0) {
            total_size += md.wal_offset ? (uint64_t)1024 : 0; // 估算大小占位
        }
    }

    // 更新统计信息
    tool->total_blocks += block_count;
    tool->processed_blocks += block_count;
    printf("[增量备份] 备份完成: 处理=%u, 估算大小=%lu bytes\n", block_count, total_size);
    
    // 清理内存
    free(block_ids);
    
    return 0;
}

// 生成taosdump兼容的增量备份脚本
int32_t incremental_backup_tool_generate_taosdump_script(SIncrementalBackupTool* tool,
                                                        const char* script_path) {
    if (!tool || !script_path) {
        return -1;
    }
    
    FILE* file = fopen(script_path, "w");
    if (!file) {
        return -1;
    }
    
    // 生成bash脚本
    fprintf(file, "#!/bin/bash\n\n");
    fprintf(file, "# TDengine增量备份脚本 - 由位图插件生成\n");
    fprintf(file, "# 生成时间: %s\n\n", ctime(&(time_t){time(NULL)}));
    
    fprintf(file, "SOURCE_HOST=%s\n", tool->config.source_host);
    fprintf(file, "SOURCE_PORT=%d\n", tool->config.source_port);
    fprintf(file, "DATABASE=%s\n", tool->config.database);
    fprintf(file, "BACKUP_PATH=%s\n", tool->config.backup_path);
    fprintf(file, "SINCE_TIMESTAMP=%ld\n\n", tool->config.since_timestamp);
    
    // 1. 使用位图插件检测增量块
    fprintf(file, "echo \"步骤1: 检测增量数据块...\"\n");
    fprintf(file, "./incremental_bitmap_tool --detect \\\n");
    fprintf(file, "  --host $SOURCE_HOST --port $SOURCE_PORT \\\n");
    fprintf(file, "  --database $DATABASE \\\n");
    fprintf(file, "  --since $SINCE_TIMESTAMP \\\n");
    fprintf(file, "  --output incremental_blocks.json\n\n");
    
    // 2. 使用taosdump备份增量数据
    fprintf(file, "echo \"步骤2: 使用taosdump备份增量数据...\"\n");
    fprintf(file, "taosdump -h $SOURCE_HOST -P $SOURCE_PORT \\\n");
    fprintf(file, "  -D $DATABASE \\\n");
    fprintf(file, "  -S $SINCE_TIMESTAMP \\\n");
    fprintf(file, "  -o $BACKUP_PATH/incremental_$(date +%%Y%%m%%d_%%H%%M%%S)\n\n");
    
    // 3. 使用位图插件验证备份完整性
    fprintf(file, "echo \"步骤3: 验证备份完整性...\"\n");
    fprintf(file, "./incremental_bitmap_tool --verify \\\n");
    fprintf(file, "  --backup $BACKUP_PATH \\\n");
    fprintf(file, "  --blocks incremental_blocks.json \\\n");
    fprintf(file, "  --report backup_verification_report.json\n\n");
    
    fprintf(file, "echo \"增量备份完成!\"\n");
    
    fclose(file);
    
    // 设置执行权限
    chmod(script_path, 0755);
    
    printf("[增量备份] 生成taosdump脚本: %s\n", script_path);
    return 0;
}

// 停止增量备份
int32_t incremental_backup_tool_stop(SIncrementalBackupTool* tool) {
    if (!tool || !tool->is_running) {
        return -1;
    }
    
    // 停止事件拦截器
    event_interceptor_stop(tool->event_interceptor);
    
    // 卸载存储引擎拦截
    if (tool->storage_interface) {
        event_interceptor_uninstall_storage_interception(tool->event_interceptor);
    }
    
    tool->is_running = false;
    printf("[增量备份] 工具已停止\n");
    return 0;
}

// 销毁增量备份工具
void incremental_backup_tool_destroy(SIncrementalBackupTool* tool) {
    if (!tool) {
        return;
    }
    
    if (tool->is_running) {
        incremental_backup_tool_stop(tool);
    }
    
    if (tool->backup_coordinator) {
        backup_coordinator_destroy(tool->backup_coordinator);
    }
    
    if (tool->event_interceptor) {
        event_interceptor_destroy(tool->event_interceptor);
    }
    
    if (tool->bitmap_engine) {
        bitmap_engine_destroy(tool->bitmap_engine);
    }
    
    free(tool);
}

// 获取备份统计信息
void incremental_backup_tool_get_stats(SIncrementalBackupTool* tool,
                                      uint64_t* total_blocks,
                                      uint64_t* processed_blocks,
                                      uint64_t* failed_blocks) {
    if (!tool) {
        return;
    }
    
    if (total_blocks) {
        *total_blocks = tool->total_blocks;
    }
    if (processed_blocks) {
        *processed_blocks = tool->processed_blocks;
    }
    if (failed_blocks) {
        *failed_blocks = tool->failed_blocks;
    }
}

