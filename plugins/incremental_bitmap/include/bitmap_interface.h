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

#ifndef TDENGINE_BITMAP_INTERFACE_H
#define TDENGINE_BITMAP_INTERFACE_H

#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

// 位图抽象接口
typedef struct SBitmapInterface {
    void* bitmap;                    // 位图实现（具体实现由实现层提供）
    
    // 基本操作
    void (*add)(void* bitmap, uint64_t value);
    void (*remove)(void* bitmap, uint64_t value);
    bool (*contains)(void* bitmap, uint64_t value);
    uint32_t (*cardinality)(void* bitmap);
    void (*clear)(void* bitmap);
    
    // 集合操作
    void (*union_with)(void* bitmap, const void* other);
    void (*intersect_with)(void* bitmap, const void* other);
    void (*subtract)(void* bitmap, const void* other);
    
    // 迭代操作
    uint32_t (*to_array)(void* bitmap, uint64_t* array, uint32_t max_count);
    
    // 序列化操作
    size_t (*serialized_size)(void* bitmap);
    int32_t (*serialize)(void* bitmap, void* buffer, size_t buffer_size);
    int32_t (*deserialize)(void** bitmap, const void* buffer, size_t buffer_size);
    
    // 内存管理
    void (*destroy)(void* bitmap);
    size_t (*memory_usage)(void* bitmap);
    
    // 创建和复制
    void* (*create)(void);
    void* (*clone)(const void* bitmap);
} SBitmapInterface;

// 位图接口实现函数声明
extern SBitmapInterface* bitmap_interface_create(void);
extern void bitmap_interface_destroy(SBitmapInterface* interface);

#ifdef __cplusplus
}
#endif

#endif // TDENGINE_BITMAP_INTERFACE_H 