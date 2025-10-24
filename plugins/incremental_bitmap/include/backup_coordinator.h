#ifndef BACKUP_COORDINATOR_H
#define BACKUP_COORDINATOR_H

#include <stdint.h>
#include <stdbool.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

// 前向声明
struct SBitmapEngine;

// 备份游标类型
typedef enum {
    BACKUP_CURSOR_TYPE_TIME = 0,    // 基于时间的游标
    BACKUP_CURSOR_TYPE_WAL = 1,     // 基于WAL偏移量的游标
    BACKUP_CURSOR_TYPE_HYBRID = 2   // 混合游标
} EBackupCursorType;

// 备份游标
typedef struct {
    EBackupCursorType type;
    int64_t time_cursor;           // 时间游标（纳秒）
    uint64_t wal_cursor;           // WAL偏移量游标
    uint64_t block_count;          // 已备份块数
    int64_t last_update_time;      // 最后更新时间
} SBackupCursor;

// 增量块信息
typedef struct {
    uint64_t block_id;             // 块ID
    uint64_t wal_offset;           // WAL偏移量
    int64_t timestamp;             // 时间戳
    uint32_t data_size;            // 数据大小
    void* data;                    // 块数据（可选）
} SIncrementalBlock;

// 备份配置
typedef struct {
    uint32_t batch_size;           // 批处理大小
    uint32_t max_retries;          // 最大重试次数
    uint32_t retry_interval_ms;    // 重试间隔（毫秒）
    uint32_t timeout_ms;           // 超时时间（毫秒）
    bool enable_compression;       // 是否启用压缩
    bool enable_encryption;        // 是否启用加密
    char* backup_path;             // 备份路径
    char* temp_path;               // 临时路径
} SBackupConfig;

// 备份统计信息
typedef struct {
    uint64_t total_blocks;         // 总块数
    uint64_t processed_blocks;     // 已处理块数
    uint64_t failed_blocks;        // 失败块数
    uint64_t total_size;           // 总大小（字节）
    int64_t start_time;            // 开始时间
    int64_t end_time;              // 结束时间
    uint32_t retry_count;          // 重试次数
} SBackupStats;

// 备份协调器实例
typedef struct {
    struct SBitmapEngine* bitmap_engine;
    SBackupCursor cursor;
    SBackupConfig config;
    SBackupStats stats;
    bool is_running;
    void* user_data;
} SBackupCoordinator;

// 备份协调器API

/**
 * 初始化备份协调器
 * @param bitmap_engine 位图引擎实例
 * @param config 备份配置
 * @return 协调器实例，失败返回NULL
 */
SBackupCoordinator* backup_coordinator_init(struct SBitmapEngine* bitmap_engine,
                                          const SBackupConfig* config);

/**
 * 销毁备份协调器
 * @param coordinator 协调器实例
 */
void backup_coordinator_destroy(SBackupCoordinator* coordinator);

/**
 * 启动备份协调器
 * @param coordinator 协调器实例
 * @return 0成功，非0失败
 */
int32_t backup_coordinator_start(SBackupCoordinator* coordinator);

/**
 * 停止备份协调器
 * @param coordinator 协调器实例
 * @return 0成功，非0失败
 */
int32_t backup_coordinator_stop(SBackupCoordinator* coordinator);

/**
 * 获取指定WAL偏移量范围内的脏块
 * @param coordinator 协调器实例
 * @param start_wal 起始WAL偏移量
 * @param end_wal 结束WAL偏移量
 * @param block_ids 块ID数组
 * @param max_count 最大块数
 * @return 实际获取的块数
 */
uint32_t backup_coordinator_get_dirty_blocks(SBackupCoordinator* coordinator,
                                            uint64_t start_wal, uint64_t end_wal,
                                            uint64_t* block_ids, uint32_t max_count);

/**
 * 获取指定时间范围内的脏块
 * @param coordinator 协调器实例
 * @param start_time 起始时间
 * @param end_time 结束时间
 * @param block_ids 块ID数组
 * @param max_count 最大块数
 * @return 实际获取的块数
 */
uint32_t backup_coordinator_get_dirty_blocks_by_time(SBackupCoordinator* coordinator,
                                                    int64_t start_time, int64_t end_time,
                                                    uint64_t* block_ids, uint32_t max_count);

/**
 * 获取增量块信息
 * @param coordinator 协调器实例
 * @param start_wal 起始WAL偏移量
 * @param end_wal 结束WAL偏移量
 * @param blocks 块信息数组
 * @param max_count 最大块数
 * @return 实际获取的块数
 */
uint32_t backup_coordinator_get_incremental_blocks(SBackupCoordinator* coordinator,
                                        uint64_t start_wal, uint64_t end_wal,
                                                  SIncrementalBlock* blocks, uint32_t max_count);

/**
 * 估算备份大小
 * @param coordinator 协调器实例
 * @param start_wal 起始WAL偏移量
 * @param end_wal 结束WAL偏移量
 * @return 估算的备份大小（字节）
 */
uint64_t backup_coordinator_estimate_backup_size(SBackupCoordinator* coordinator,
                                                uint64_t start_wal, uint64_t end_wal);

/**
 * 获取备份统计信息
 * @param coordinator 协调器实例
 * @param stats 统计信息结构
 * @return 0成功，非0失败
 */
int32_t backup_coordinator_get_stats(SBackupCoordinator* coordinator, SBackupStats* stats);

/**
 * 重置备份统计信息
 * @param coordinator 协调器实例
 * @return 0成功，非0失败
 */
int32_t backup_coordinator_reset_stats(SBackupCoordinator* coordinator);

/**
 * 获取备份游标
 * @param coordinator 协调器实例
 * @param cursor 游标结构
 * @return 0成功，非0失败
 */
int32_t backup_coordinator_get_cursor(SBackupCoordinator* coordinator, SBackupCursor* cursor);

/**
 * 设置备份游标
 * @param coordinator 协调器实例
 * @param cursor 游标结构
 * @return 0成功，非0失败
 */
int32_t backup_coordinator_set_cursor(SBackupCoordinator* coordinator, const SBackupCursor* cursor);

/**
 * 更新备份游标
 * @param coordinator 协调器实例
 * @param block_id 块ID
 * @param wal_offset WAL偏移量
 * @param timestamp 时间戳
 * @return 0成功，非0失败
 */
int32_t backup_coordinator_update_cursor(SBackupCoordinator* coordinator,
                                        uint64_t block_id, uint64_t wal_offset, int64_t timestamp);

// 插件接口函数（通用备份插件接口）
typedef struct {
    // 插件初始化
    int32_t (*init)(const char* config);
    
    // 插件销毁
    void (*destroy)(void);
    
    // 获取脏块
    uint32_t (*get_dirty_blocks)(uint64_t start_wal, uint64_t end_wal,
                                uint64_t* block_ids, uint32_t max_count);
    
    // 获取增量块
    uint32_t (*get_incremental_blocks)(uint64_t start_wal, uint64_t end_wal,
                                      SIncrementalBlock* blocks, uint32_t max_count);
    
    // 估算备份大小
    uint64_t (*estimate_backup_size)(uint64_t start_wal, uint64_t end_wal);
    
    // 获取统计信息
    int32_t (*get_stats)(SBackupStats* stats);
    
    // 重置统计信息
    int32_t (*reset_stats)(void);
} SBackupPluginInterface;

// 插件接口函数
extern SBackupPluginInterface* backup_plugin_get_interface(void);

// 便捷函数
uint32_t backup_plugin_get_dirty_blocks(uint64_t start_wal, uint64_t end_wal,
                                       uint64_t* block_ids, uint32_t max_count);

uint32_t backup_plugin_get_incremental_blocks(uint64_t start_wal, uint64_t end_wal,
                                             SIncrementalBlock* blocks, uint32_t max_count);

uint64_t backup_plugin_estimate_backup_size(uint64_t start_wal, uint64_t end_wal);

int32_t backup_plugin_get_stats(SBackupStats* stats);

int32_t backup_plugin_reset_stats(void);

#ifdef __cplusplus
}
#endif

#endif // BACKUP_COORDINATOR_H 