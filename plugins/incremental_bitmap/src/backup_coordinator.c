#include "backup_coordinator.h"
#include "bitmap_engine.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <sys/time.h>
#include <pthread.h>

// 获取当前时间戳（纳秒）
static int64_t get_current_timestamp() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + (int64_t)ts.tv_nsec;
}

// 深拷贝字符串
static char* deep_copy_string(const char* src) {
    if (src == NULL) {
        return NULL;
    }
    
    size_t len = strlen(src);
    char* dst = (char*)malloc(len + 1);
    if (dst == NULL) {
        return NULL;
    }
    
    strcpy(dst, src);
    return dst;
}

SBackupCoordinator* backup_coordinator_init(struct SBitmapEngine* bitmap_engine,
                                          const SBackupConfig* config) {
    if (!bitmap_engine || !config) {
        return NULL;
    }
    
    SBackupCoordinator* coordinator = (SBackupCoordinator*)malloc(sizeof(SBackupCoordinator));
    if (!coordinator) {
        return NULL;
    }
    
    // 初始化基本字段
    coordinator->bitmap_engine = bitmap_engine;
    coordinator->is_running = false;
    coordinator->user_data = NULL;
    
    // 复制配置
    coordinator->config.batch_size = config->batch_size;
    coordinator->config.max_retries = config->max_retries;
    coordinator->config.retry_interval_ms = config->retry_interval_ms;
    coordinator->config.timeout_ms = config->timeout_ms;
    coordinator->config.enable_compression = config->enable_compression;
    coordinator->config.enable_encryption = config->enable_encryption;
    coordinator->config.backup_path = deep_copy_string(config->backup_path);
    coordinator->config.temp_path = deep_copy_string(config->temp_path);
    
    // 初始化游标
    coordinator->cursor.type = BACKUP_CURSOR_TYPE_HYBRID;
    coordinator->cursor.time_cursor = 0;
    coordinator->cursor.wal_cursor = 0;
    coordinator->cursor.block_count = 0;
    coordinator->cursor.last_update_time = get_current_timestamp();
    
    // 初始化统计信息
    coordinator->stats.total_blocks = 0;
    coordinator->stats.processed_blocks = 0;
    coordinator->stats.failed_blocks = 0;
    coordinator->stats.total_size = 0;
    coordinator->stats.start_time = 0;
    coordinator->stats.end_time = 0;
    coordinator->stats.retry_count = 0;
    
    return coordinator;
}

void backup_coordinator_destroy(SBackupCoordinator* coordinator) {
    if (!coordinator) {
        return;
    }
    
    // 释放配置中的字符串
    if (coordinator->config.backup_path) {
        free(coordinator->config.backup_path);
    }
    if (coordinator->config.temp_path) {
        free(coordinator->config.temp_path);
    }
    
    free(coordinator);
}

int32_t backup_coordinator_start(SBackupCoordinator* coordinator) {
    if (!coordinator) {
        return -1;
    }
    
    if (coordinator->is_running) {
        return 0; // 已经运行
    }
    
    coordinator->is_running = true;
    coordinator->stats.start_time = get_current_timestamp();
    
    return 0;
}

int32_t backup_coordinator_stop(SBackupCoordinator* coordinator) {
    if (!coordinator) {
        return -1;
    }
    
    if (!coordinator->is_running) {
        return 0; // 已经停止
    }
    
    coordinator->is_running = false;
    coordinator->stats.end_time = get_current_timestamp();
    
    return 0;
}

uint32_t backup_coordinator_get_dirty_blocks(SBackupCoordinator* coordinator,
                                            uint64_t start_wal, uint64_t end_wal,
                                            uint64_t* block_ids, uint32_t max_count) {
    if (!coordinator || !block_ids || max_count == 0) {
        return 0;
    }
    
    return bitmap_engine_get_dirty_blocks_by_wal(coordinator->bitmap_engine,
                                                start_wal, end_wal, block_ids, max_count);
}

uint32_t backup_coordinator_get_dirty_blocks_by_time(SBackupCoordinator* coordinator,
                                                    int64_t start_time, int64_t end_time,
                                                    uint64_t* block_ids, uint32_t max_count) {
    if (!coordinator || !block_ids || max_count == 0) {
        return 0;
    }
    
    return bitmap_engine_get_dirty_blocks_by_time(coordinator->bitmap_engine,
                                                 start_time, end_time, block_ids, max_count);
}

uint32_t backup_coordinator_get_incremental_blocks(SBackupCoordinator* coordinator,
                                                  uint64_t start_wal, uint64_t end_wal,
                                                  SIncrementalBlock* blocks, uint32_t max_count) {
    // printf("DEBUG: backup_coordinator_get_incremental_blocks: ENTRY\n");
    
    if (!coordinator || !blocks || max_count == 0) {
        // printf("DEBUG: backup_coordinator_get_incremental_blocks: Invalid parameters\n");
        return 0;
    }
    
    // printf("DEBUG: backup_coordinator_get_incremental_blocks: start_wal=%lu, end_wal=%lu, max_count=%u\n", 
    //        start_wal, end_wal, max_count);
    
    // 获取脏块ID
    uint64_t* block_ids = (uint64_t*)malloc(sizeof(uint64_t) * max_count);
    if (block_ids == NULL) {
        // printf("DEBUG: Failed to allocate block_ids\n");
        return 0;
    }
    
    // printf("DEBUG: Allocated block_ids at %p\n", block_ids);
    
    uint32_t count = bitmap_engine_get_dirty_blocks_by_wal(coordinator->bitmap_engine,
                                                          start_wal, end_wal, block_ids, max_count);
    
    // printf("DEBUG: backup_coordinator_get_incremental_blocks: count=%u, max_count=%u\n", count, max_count);
    
    if (count == 0) {
    // printf("DEBUG: No dirty blocks found, freeing block_ids and returning 0\n");
        free(block_ids);
        return 0;
    }
    
    // 填充块信息
    // printf("DEBUG: Starting to fill block information\n");
    uint32_t valid_count = 0;
    
    for (uint32_t i = 0; i < count && valid_count < max_count; i++) {
    // printf("DEBUG: Processing block %u: block_id=%lu\n", i, block_ids[i]);
        
        // 检查数组边界
        if (valid_count >= max_count) {
    // printf("DEBUG: Array boundary check failed: valid_count=%u, max_count=%u\n", valid_count, max_count);
            break;
        }
        
        // 检查blocks数组是否有效
        if (blocks == NULL) {
    // printf("DEBUG: blocks array is NULL\n");
            break;
        }
        
        SBlockMetadata metadata;
    // printf("DEBUG: About to call bitmap_engine_get_block_metadata for block_id=%lu\n", block_ids[i]);
        
        int32_t result = bitmap_engine_get_block_metadata(coordinator->bitmap_engine, block_ids[i], &metadata);
    // printf("DEBUG: bitmap_engine_get_block_metadata returned %d for block_id=%lu\n", result, block_ids[i]);
        
        if (result == 0) {
    // printf("DEBUG: Successfully got metadata for block_id=%lu\n", block_ids[i]);
            blocks[valid_count].block_id = block_ids[i];
            blocks[valid_count].wal_offset = metadata.wal_offset;
            blocks[valid_count].timestamp = metadata.timestamp;
            blocks[valid_count].data_size = 0; // 实际数据大小需要从存储引擎获取
            blocks[valid_count].data = NULL;   // 实际数据需要从存储引擎获取
            valid_count++;
        } else {
    // printf("DEBUG: Failed to get metadata for block_id=%lu, skipping this block\n", block_ids[i]);
            // 跳过这个块，不添加到结果中
            continue;
        }
    // printf("DEBUG: Completed processing block %u, valid_count=%u\n", i, valid_count);
    }
    // printf("DEBUG: Finished filling block information, valid_count=%u\n", valid_count);
    
    // printf("DEBUG: About to free block_ids\n");
    free(block_ids);
    // printf("DEBUG: Freed block_ids\n");
    
    // printf("DEBUG: Returning valid_count=%u\n", valid_count);
    return valid_count;
}

uint64_t backup_coordinator_estimate_backup_size(SBackupCoordinator* coordinator,
                                                uint64_t start_wal, uint64_t end_wal) {
    if (!coordinator) {
        return 0;
    }
    
    // 获取块数量
    uint64_t temp_block_ids[1];
    uint32_t count = bitmap_engine_get_dirty_blocks_by_wal(coordinator->bitmap_engine,
                                                          start_wal, end_wal, temp_block_ids, 1);
    
    if (count == 0) {
        return 0;
    }
    
    // 估算每个块的平均大小（这里使用固定值，实际应该根据数据统计）
    const uint64_t avg_block_size = 4096; // 4KB per block
    
    // 获取总块数
    uint64_t total_blocks = 0;
    uint64_t offset = start_wal;
    while (offset <= end_wal) {
        uint32_t batch_count = bitmap_engine_get_dirty_blocks_by_wal(coordinator->bitmap_engine,
                                                                    offset, end_wal, temp_block_ids, 1);
        if (batch_count == 0) {
            break;
        }
        total_blocks += batch_count;
        offset += 1000; // 假设WAL偏移量间隔
    }
    
    return total_blocks * avg_block_size;
}

int32_t backup_coordinator_get_stats(SBackupCoordinator* coordinator, SBackupStats* stats) {
    if (!coordinator || !stats) {
        return -1;
    }
    
    *stats = coordinator->stats;
    return 0;
}

int32_t backup_coordinator_reset_stats(SBackupCoordinator* coordinator) {
    if (!coordinator) {
        return -1;
    }
    
    coordinator->stats.total_blocks = 0;
    coordinator->stats.processed_blocks = 0;
    coordinator->stats.failed_blocks = 0;
    coordinator->stats.total_size = 0;
    coordinator->stats.start_time = 0;
    coordinator->stats.end_time = 0;
    coordinator->stats.retry_count = 0;
    
    return 0;
}

int32_t backup_coordinator_get_cursor(SBackupCoordinator* coordinator, SBackupCursor* cursor) {
    if (!coordinator || !cursor) {
        return -1;
    }
    
    *cursor = coordinator->cursor;
    return 0;
}

int32_t backup_coordinator_set_cursor(SBackupCoordinator* coordinator, const SBackupCursor* cursor) {
    if (!coordinator || !cursor) {
        return -1;
    }
    
    coordinator->cursor = *cursor;
    return 0;
}

int32_t backup_coordinator_update_cursor(SBackupCoordinator* coordinator,
                                        uint64_t block_id, uint64_t wal_offset, int64_t timestamp) {
    if (!coordinator) {
        return -1;
    }
    
    coordinator->cursor.wal_cursor = wal_offset;
    coordinator->cursor.time_cursor = timestamp;
    coordinator->cursor.block_count++;
    coordinator->cursor.last_update_time = get_current_timestamp();
    
    return 0;
}

// 插件接口实现
static SBackupPluginInterface g_backup_plugin_interface = {0};
static SBackupCoordinator* g_backup_coordinator = NULL;

static int32_t plugin_init(const char* config) {
    // 创建位图引擎（使用默认配置）
    SBitmapEngine* bitmap_engine = bitmap_engine_init();
    if (!bitmap_engine) {
    // printf("DEBUG: plugin_init: Failed to create bitmap engine\n");
        return -1;
    }
    
    // 创建备份配置
    SBackupConfig backup_config = {
        .batch_size = 100,
        .max_retries = 3,
        .retry_interval_ms = 1000,
        .timeout_ms = 5000,
        .enable_compression = true,
        .enable_encryption = false,
        .backup_path = NULL,
        .temp_path = NULL
    };
    
    // 创建备份协调器
    g_backup_coordinator = backup_coordinator_init(bitmap_engine, &backup_config);
    if (!g_backup_coordinator) {
    // printf("DEBUG: plugin_init: Failed to create backup coordinator\n");
        bitmap_engine_destroy(bitmap_engine);
        return -1;
    }
    
    // printf("DEBUG: plugin_init: Successfully initialized plugin\n");
    return 0;
}

static void plugin_destroy(void) {
    if (g_backup_coordinator) {
        // 获取位图引擎引用
        SBitmapEngine* bitmap_engine = g_backup_coordinator->bitmap_engine;
        
        // 销毁备份协调器
        backup_coordinator_destroy(g_backup_coordinator);
        g_backup_coordinator = NULL;
        
        // 销毁位图引擎
        if (bitmap_engine) {
            bitmap_engine_destroy(bitmap_engine);
        }
        
    // printf("DEBUG: plugin_destroy: Successfully destroyed plugin\n");
    }
}

static uint32_t plugin_get_dirty_blocks(uint64_t start_wal, uint64_t end_wal,
                                       uint64_t* block_ids, uint32_t max_count) {
    if (!g_backup_coordinator) {
        return 0;
    }
    
    return backup_coordinator_get_dirty_blocks(g_backup_coordinator, start_wal, end_wal, block_ids, max_count);
}

static uint32_t plugin_get_incremental_blocks(uint64_t start_wal, uint64_t end_wal,
                                             SIncrementalBlock* blocks, uint32_t max_count) {
    if (!g_backup_coordinator) {
        return 0;
    }
    
    return backup_coordinator_get_incremental_blocks(g_backup_coordinator, start_wal, end_wal, blocks, max_count);
}

static uint64_t plugin_estimate_backup_size(uint64_t start_wal, uint64_t end_wal) {
    if (!g_backup_coordinator) {
        return 0;
    }
    
    return backup_coordinator_estimate_backup_size(g_backup_coordinator, start_wal, end_wal);
}

static int32_t plugin_get_stats(SBackupStats* stats) {
    if (!g_backup_coordinator) {
        return -1;
    }
    
    return backup_coordinator_get_stats(g_backup_coordinator, stats);
}

static int32_t plugin_reset_stats(void) {
    if (!g_backup_coordinator) {
        return -1;
    }
    
    return backup_coordinator_reset_stats(g_backup_coordinator);
}

// 初始化插件接口
static void init_plugin_interface(void) {
    static bool initialized = false;
    if (initialized) {
        return;
    }
    
    g_backup_plugin_interface.init = plugin_init;
    g_backup_plugin_interface.destroy = plugin_destroy;
    g_backup_plugin_interface.get_dirty_blocks = plugin_get_dirty_blocks;
    g_backup_plugin_interface.get_incremental_blocks = plugin_get_incremental_blocks;
    g_backup_plugin_interface.estimate_backup_size = plugin_estimate_backup_size;
    g_backup_plugin_interface.get_stats = plugin_get_stats;
    g_backup_plugin_interface.reset_stats = plugin_reset_stats;
    
    initialized = true;
}

SBackupPluginInterface* backup_plugin_get_interface(void) {
    init_plugin_interface();
    return &g_backup_plugin_interface;
}

// 便捷函数实现
uint32_t backup_plugin_get_dirty_blocks(uint64_t start_wal, uint64_t end_wal,
                                       uint64_t* block_ids, uint32_t max_count) {
    SBackupPluginInterface* interface = backup_plugin_get_interface();
    if (!interface || !interface->get_dirty_blocks) {
        return 0;
    }
    
    return interface->get_dirty_blocks(start_wal, end_wal, block_ids, max_count);
}

uint32_t backup_plugin_get_incremental_blocks(uint64_t start_wal, uint64_t end_wal,
                                             SIncrementalBlock* blocks, uint32_t max_count) {
    SBackupPluginInterface* interface = backup_plugin_get_interface();
    if (!interface || !interface->get_incremental_blocks) {
        return 0;
    }
    
    return interface->get_incremental_blocks(start_wal, end_wal, blocks, max_count);
}

uint64_t backup_plugin_estimate_backup_size(uint64_t start_wal, uint64_t end_wal) {
    SBackupPluginInterface* interface = backup_plugin_get_interface();
    if (!interface || !interface->estimate_backup_size) {
        return 0;
    }
    
    return interface->estimate_backup_size(start_wal, end_wal);
}

int32_t backup_plugin_get_stats(SBackupStats* stats) {
    SBackupPluginInterface* interface = backup_plugin_get_interface();
    if (!interface || !interface->get_stats) {
        return -1;
    }
    
    return interface->get_stats(stats);
}

int32_t backup_plugin_reset_stats(void) {
    SBackupPluginInterface* interface = backup_plugin_get_interface();
    if (!interface || !interface->reset_stats) {
        return -1;
    }
    
    return interface->reset_stats();
} 