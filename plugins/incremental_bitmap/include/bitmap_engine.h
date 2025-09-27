// API版本号定义
#define BITMAP_ENGINE_API_VERSION 1

/*
 * Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef TDENGINE_BITMAP_ENGINE_H
#define TDENGINE_BITMAP_ENGINE_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include "bitmap_interface.h"
#include "skiplist.h"

#ifdef __cplusplus
extern "C" {
#endif

// 错误码定义
#define ERR_SUCCESS                   0       // 成功
#define ERR_INVALID_PARAM            -1       // 无效参数
#define ERR_INVALID_STATE_TRANS      -1001    // 无效状态转换
#define ERR_BLOCK_NOT_FOUND          -1002    // 块未找到

// 块状态枚举
typedef enum {
    BLOCK_STATE_CLEAN = 0,    // 未修改
    BLOCK_STATE_DIRTY = 1,    // 已修改
    BLOCK_STATE_NEW = 2,      // 新增
    BLOCK_STATE_DELETED = 3   // 已删除
} EBlockState;

// 块元数据
typedef struct {
    uint64_t block_id;        // 物理块ID
    uint64_t wal_offset;      // WAL偏移量
    int64_t timestamp;        // 纳秒级时间戳
    EBlockState state;        // 块状态
} SBlockMetadata;

// LRU节点结构
typedef struct SLruNode {
    uint64_t block_id;        // 块ID
    int64_t last_access;      // 最后访问时间
    struct SLruNode* prev;    // 前驱节点
    struct SLruNode* next;    // 后继节点
} SLruNode;

// 块元数据映射节点
typedef struct SBlockMetadataNode {
    uint64_t block_id;
    SBlockMetadata metadata;
    struct SBlockMetadataNode* next;
} SBlockMetadataNode;

// 时间索引节点
typedef struct STimeIndexNode {
    int64_t timestamp;
    SBitmapInterface* block_ids;
    struct STimeIndexNode* next;
} STimeIndexNode;

// WAL索引节点
typedef struct SWalIndexNode {
    uint64_t wal_offset;
    SBitmapInterface* block_ids;
    struct SWalIndexNode* next;
} SWalIndexNode;

// LRU映射节点
typedef struct SLruMapNode {
    uint64_t block_id;
    SLruNode* lru_node;
    struct SLruMapNode* next;
} SLruMapNode;





// 位图引擎实例
// API版本：BITMAP_ENGINE_API_VERSION
typedef struct SBitmapEngine {
    SBitmapInterface* dirty_blocks;    // 脏块位图
    SBitmapInterface* new_blocks;      // 新块位图
    SBitmapInterface* deleted_blocks;  // 删除块位图
    
    // 元数据映射（哈希表）
    SBlockMetadataNode** metadata_map;
    uint32_t metadata_map_size;
    uint32_t metadata_count;
    
    // 时空索引
    STimeIndexNode* time_index_head;
    SWalIndexNode* wal_index_head;
    

    
    // 统计信息
    uint64_t total_blocks;
    uint64_t dirty_count;
    uint64_t new_count;
    uint64_t deleted_count;
    
    // 线程安全
    pthread_mutex_t mutex;
    pthread_rwlock_t rwlock;
    
    // 跳表索引
    skiplist_t* time_index;
    skiplist_t* wal_index;
    

} SBitmapEngine;



// 位图引擎API

/**
 * 初始化位图引擎
 * @return 引擎实例，失败返回NULL
 */
SBitmapEngine* bitmap_engine_init(void);

/**
 * 销毁位图引擎
 * @param engine 引擎实例
 */
void bitmap_engine_destroy(SBitmapEngine* engine);

/**
 * 标记块为脏状态
 * @param engine 引擎实例
 * @param block_id 块ID
 * @param wal_offset WAL偏移量
 * @param timestamp 时间戳
 * @return 0成功，非0失败
 */
int32_t bitmap_engine_mark_dirty(SBitmapEngine* engine, uint64_t block_id, 
                                uint64_t wal_offset, int64_t timestamp);

/**
 * 标记块为新增状态
 * @param engine 引擎实例
 * @param block_id 块ID
 * @param wal_offset WAL偏移量
 * @param timestamp 时间戳
 * @return 0成功，非0失败
 */
int32_t bitmap_engine_mark_new(SBitmapEngine* engine, uint64_t block_id,
                              uint64_t wal_offset, int64_t timestamp);

/**
 * 标记块为删除状态
 * @param engine 引擎实例
 * @param block_id 块ID
 * @param wal_offset WAL偏移量
 * @param timestamp 时间戳
 * @return 0成功，非0失败
 */
int32_t bitmap_engine_mark_deleted(SBitmapEngine* engine, uint64_t block_id,
                                  uint64_t wal_offset, int64_t timestamp);

/**
 * 清除块状态
 * @param engine 引擎实例
 * @param block_id 块ID
 * @return 0成功，非0失败
 */
int32_t bitmap_engine_clear_block(SBitmapEngine* engine, uint64_t block_id);

/**
 * 获取时间范围内的脏块
 * @param engine 引擎实例
 * @param start_time 开始时间
 * @param end_time 结束时间
 * @param block_ids 输出块ID数组
 * @param max_count 最大返回数量
 * @return 实际返回的块数量
 */
uint32_t bitmap_engine_get_dirty_blocks_by_time(SBitmapEngine* engine,
                                               int64_t start_time, int64_t end_time,
                                               uint64_t* block_ids, uint32_t max_count);

/**
 * 获取WAL偏移量范围内的脏块
 * @param engine 引擎实例
 * @param start_offset 开始偏移量
 * @param end_offset 结束偏移量
 * @param block_ids 输出块ID数组
 * @param max_count 最大返回数量
 * @return 实际返回的块数量
 */
uint32_t bitmap_engine_get_dirty_blocks_by_wal(SBitmapEngine* engine,
                                              uint64_t start_offset, uint64_t end_offset,
                                              uint64_t* block_ids, uint32_t max_count);

/**
 * 获取块元数据
 * @param engine 引擎实例
 * @param block_id 块ID
 * @param metadata 输出元数据
 * @return 0成功，非0失败
 */
int32_t bitmap_engine_get_block_metadata(SBitmapEngine* engine, uint64_t block_id,
                                        SBlockMetadata* metadata);



/**
 * 获取统计信息
 * @param engine 引擎实例
 * @param total_blocks 总块数
 * @param dirty_count 脏块数
 * @param new_count 新块数
 * @param deleted_count 删除块数
 */
void bitmap_engine_get_stats(SBitmapEngine* engine, uint64_t* total_blocks,
                           uint64_t* dirty_count, uint64_t* new_count, uint64_t* deleted_count);



// 状态转换验证API

/**
 * 验证状态转换是否合法
 * @param current_state 当前状态
 * @param target_state 目标状态
 * @return 0合法，ERR_INVALID_STATE_TRANS非法
 */
int32_t bitmap_engine_validate_state_transition(EBlockState current_state, EBlockState target_state);

/**
 * 获取状态转换错误信息
 * @param current_state 当前状态
 * @param target_state 目标状态
 * @return 错误信息字符串
 */
const char* bitmap_engine_get_state_transition_error(EBlockState current_state, EBlockState target_state);

/**
 * 获取块当前状态
 * @param engine 引擎实例
 * @param block_id 块ID
 * @param state 输出状态
 * @return 0成功，ERR_BLOCK_NOT_FOUND块未找到
 */
int32_t bitmap_engine_get_block_state(SBitmapEngine* engine, uint64_t block_id, EBlockState* state);

#ifdef __cplusplus
}
#endif

#endif // TDENGINE_BITMAP_ENGINE_H 