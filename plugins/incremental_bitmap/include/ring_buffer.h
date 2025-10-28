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

#ifndef TDENGINE_RING_BUFFER_H
#define TDENGINE_RING_BUFFER_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

// 环形队列状态
typedef enum {
    RING_BUFFER_EMPTY = 0,    // 空
    RING_BUFFER_PARTIAL = 1,  // 部分填充
    RING_BUFFER_FULL = 2      // 满
} ERingBufferState;

// 环形队列
typedef struct {
    void** buffer;            // 数据缓冲区
    uint32_t capacity;        // 容量
    uint32_t size;            // 当前大小
    uint32_t head;            // 头部索引
    uint32_t tail;            // 尾部索引
    
    // 线程同步
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
    
    // 统计信息
    uint64_t enqueue_count;
    uint64_t dequeue_count;
    uint64_t overflow_count;
} SRingBuffer;

// 环形队列API

/**
 * 初始化环形队列
 * @param capacity 队列容量
 * @return 队列实例，失败返回NULL
 */
SRingBuffer* ring_buffer_init(uint32_t capacity);

/**
 * 销毁环形队列
 * @param ring_buffer 队列实例
 */
void ring_buffer_destroy(SRingBuffer* ring_buffer);

/**
 * 入队（非阻塞）
 * @param ring_buffer 队列实例
 * @param item 数据项
 * @return 0成功，非0失败
 */
int32_t ring_buffer_enqueue(SRingBuffer* ring_buffer, void* item);

/**
 * 入队（阻塞）
 * @param ring_buffer 队列实例
 * @param item 数据项
 * @param timeout_ms 超时时间(毫秒)，0表示无限等待
 * @return 0成功，非0失败
 */
int32_t ring_buffer_enqueue_blocking(SRingBuffer* ring_buffer, void* item, uint32_t timeout_ms);

/**
 * 出队（非阻塞）
 * @param ring_buffer 队列实例
 * @param item 输出数据项
 * @return 0成功，非0失败
 */
int32_t ring_buffer_dequeue(SRingBuffer* ring_buffer, void** item);

/**
 * 出队（阻塞）
 * @param ring_buffer 队列实例
 * @param item 输出数据项
 * @param timeout_ms 超时时间(毫秒)，0表示无限等待
 * @return 0成功，非0失败
 */
int32_t ring_buffer_dequeue_blocking(SRingBuffer* ring_buffer, void** item, uint32_t timeout_ms);

/**
 * 查看队首元素（不移除）
 * @param ring_buffer 队列实例
 * @param item 输出数据项
 * @return 0成功，非0失败
 */
int32_t ring_buffer_peek(SRingBuffer* ring_buffer, void** item);

/**
 * 清空队列
 * @param ring_buffer 队列实例
 * @param free_func 释放函数，NULL表示不释放
 */
void ring_buffer_clear(SRingBuffer* ring_buffer, void (*free_func)(void*));

/**
 * 获取队列状态
 * @param ring_buffer 队列实例
 * @return 队列状态
 */
ERingBufferState ring_buffer_get_state(SRingBuffer* ring_buffer);

/**
 * 获取队列大小
 * @param ring_buffer 队列实例
 * @return 当前大小
 */
uint32_t ring_buffer_get_size(SRingBuffer* ring_buffer);

/**
 * 获取队列容量
 * @param ring_buffer 队列实例
 * @return 容量
 */
uint32_t ring_buffer_get_capacity(SRingBuffer* ring_buffer);

/**
 * 检查队列是否为空
 * @param ring_buffer 队列实例
 * @return true为空，false非空
 */
bool ring_buffer_is_empty(SRingBuffer* ring_buffer);

/**
 * 检查队列是否已满
 * @param ring_buffer 队列实例
 * @return true已满，false未满
 */
bool ring_buffer_is_full(SRingBuffer* ring_buffer);

/**
 * 获取统计信息
 * @param ring_buffer 队列实例
 * @param enqueue_count 入队次数
 * @param dequeue_count 出队次数
 * @param overflow_count 溢出次数
 */
void ring_buffer_get_stats(SRingBuffer* ring_buffer, uint64_t* enqueue_count,
                          uint64_t* dequeue_count, uint64_t* overflow_count);

#ifdef __cplusplus
}
#endif

#endif // TDENGINE_RING_BUFFER_H 