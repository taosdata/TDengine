#include "../include/bitmap_interface.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>

// 简单位图实现（用于开发阶段）
typedef struct SSimpleBitmap {
    uint64_t* values;        // 存储的值数组
    uint32_t count;          // 当前元素数量
    uint32_t capacity;       // 数组容量
    bool sorted;             // 是否已排序
} SSimpleBitmap;

// 内部函数声明
static void simple_bitmap_add(void* bitmap, uint64_t value);
static void simple_bitmap_remove(void* bitmap, uint64_t value);
static bool simple_bitmap_contains(void* bitmap, uint64_t value);
static uint32_t simple_bitmap_cardinality(void* bitmap);
static void simple_bitmap_clear(void* bitmap);
static void simple_bitmap_union_with(void* bitmap, const void* other);
static void simple_bitmap_intersect_with(void* bitmap, const void* other);
static void simple_bitmap_subtract(void* bitmap, const void* other);
static uint32_t simple_bitmap_to_array(void* bitmap, uint64_t* array, uint32_t max_count);
static size_t simple_bitmap_serialized_size(void* bitmap);
static int32_t simple_bitmap_serialize(void* bitmap, void* buffer, size_t buffer_size);
static int32_t simple_bitmap_deserialize(void** bitmap, const void* buffer, size_t buffer_size);
static void simple_bitmap_destroy(void* bitmap);
static size_t simple_bitmap_memory_usage(void* bitmap);
static void* simple_bitmap_create(void);
static void* simple_bitmap_clone(const void* bitmap);

// 辅助函数：二分查找
static int32_t binary_search(const uint64_t* array, uint32_t count, uint64_t value) {
    int32_t left = 0;
    int32_t right = count - 1;
    
    while (left <= right) {
        int32_t mid = left + (right - left) / 2;
        if (array[mid] == value) {
            return mid;
        } else if (array[mid] < value) {
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }
    return -1;
}

// 辅助函数：插入排序
static void insert_sort(uint64_t* array, uint32_t count) {
    for (uint32_t i = 1; i < count; i++) {
        uint64_t key = array[i];
        int32_t j = i - 1;
        
        while (j >= 0 && array[j] > key) {
            array[j + 1] = array[j];
            j--;
        }
        array[j + 1] = key;
    }
}

// 辅助函数：确保容量足够
static bool ensure_capacity(SSimpleBitmap* bitmap, uint32_t min_capacity) {
    if (bitmap->capacity >= min_capacity) {
        return true;
    }
    
    uint32_t new_capacity = bitmap->capacity * 2;
    if (new_capacity < min_capacity) {
        new_capacity = min_capacity;
    }
    
    // 添加安全检查
    if (bitmap->values == NULL) {
    // printf("DEBUG: ensure_capacity: bitmap->values is NULL\n");
        return false;
    }
    
    uint64_t* new_values = realloc(bitmap->values, new_capacity * sizeof(uint64_t));
    if (!new_values) {
    // printf("DEBUG: ensure_capacity: realloc failed\n");
        return false;
    }
    
    // 检查realloc是否移动了内存块
    if (new_values != bitmap->values) {
    // printf("DEBUG: ensure_capacity: memory moved from %p to %p\n", bitmap->values, new_values);
    }
    
    bitmap->values = new_values;
    bitmap->capacity = new_capacity;
    
    // 添加验证
    if (bitmap->values == NULL) {
    // printf("DEBUG: ensure_capacity: bitmap->values became NULL after update\n");
        return false;
    }
    
    return true;
}

// 基本操作实现
static void simple_bitmap_add(void* bitmap, uint64_t value) {
    SSimpleBitmap* sb = (SSimpleBitmap*)bitmap;
    
    // 添加安全检查
    if (sb == NULL) {
    // printf("DEBUG: simple_bitmap_add: sb is NULL\n");
        return;
    }
    
    if (sb->values == NULL) {
    // printf("DEBUG: simple_bitmap_add: sb->values is NULL\n");
        return;
    }
    
    // 检查是否已存在
    if (simple_bitmap_contains(bitmap, value)) {
        return;
    }
    
    // 确保容量足够
    if (!ensure_capacity(sb, sb->count + 1)) {
    // printf("DEBUG: simple_bitmap_add: ensure_capacity failed\n");
        return;
    }
    
    // 再次检查values指针
    if (sb->values == NULL) {
    // printf("DEBUG: simple_bitmap_add: sb->values became NULL after ensure_capacity\n");
        return;
    }
    
    // 添加到数组末尾
    sb->values[sb->count++] = value;
    sb->sorted = false;
}

static void simple_bitmap_remove(void* bitmap, uint64_t value) {
    SSimpleBitmap* sb = (SSimpleBitmap*)bitmap;
    
    // 如果未排序，先排序
    if (!sb->sorted) {
        insert_sort(sb->values, sb->count);
        sb->sorted = true;
    }
    
    // 查找并删除
    int32_t index = binary_search(sb->values, sb->count, value);
    if (index >= 0) {
        // 移动后面的元素
        for (uint32_t i = index; i < sb->count - 1; i++) {
            sb->values[i] = sb->values[i + 1];
        }
        sb->count--;
    }
}

static bool simple_bitmap_contains(void* bitmap, uint64_t value) {
    SSimpleBitmap* sb = (SSimpleBitmap*)bitmap;
    
    // 如果未排序，先排序
    if (!sb->sorted) {
        insert_sort(sb->values, sb->count);
        sb->sorted = true;
    }
    
    return binary_search(sb->values, sb->count, value) >= 0;
}

static uint32_t simple_bitmap_cardinality(void* bitmap) {
    SSimpleBitmap* sb = (SSimpleBitmap*)bitmap;
    return sb->count;
}

static void simple_bitmap_clear(void* bitmap) {
    SSimpleBitmap* sb = (SSimpleBitmap*)bitmap;
    sb->count = 0;
    sb->sorted = true;
}

// 集合操作实现
static void simple_bitmap_union_with(void* bitmap, const void* other) {
    SSimpleBitmap* sb = (SSimpleBitmap*)bitmap;
    const SSimpleBitmap* other_sb = (const SSimpleBitmap*)other;
    
    for (uint32_t i = 0; i < other_sb->count; i++) {
        simple_bitmap_add(bitmap, other_sb->values[i]);
    }
}

static void simple_bitmap_intersect_with(void* bitmap, const void* other) {
    SSimpleBitmap* sb = (SSimpleBitmap*)bitmap;
    const SSimpleBitmap* other_sb = (const SSimpleBitmap*)other;
    
    // 创建临时数组存储交集结果
    uint64_t* temp = malloc(sb->count * sizeof(uint64_t));
    if (!temp) return;
    
    uint32_t temp_count = 0;
    for (uint32_t i = 0; i < sb->count; i++) {
        if (simple_bitmap_contains((void*)other, sb->values[i])) {
            temp[temp_count++] = sb->values[i];
        }
    }
    
    // 更新原数组
    memcpy(sb->values, temp, temp_count * sizeof(uint64_t));
    sb->count = temp_count;
    sb->sorted = false;
    
    free(temp);
}

static void simple_bitmap_subtract(void* bitmap, const void* other) {
    SSimpleBitmap* sb = (SSimpleBitmap*)bitmap;
    const SSimpleBitmap* other_sb = (const SSimpleBitmap*)other;
    
    for (uint32_t i = 0; i < other_sb->count; i++) {
        simple_bitmap_remove(bitmap, other_sb->values[i]);
    }
}

// 迭代操作实现
static uint32_t simple_bitmap_to_array(void* bitmap, uint64_t* array, uint32_t max_count) {
    SSimpleBitmap* sb = (SSimpleBitmap*)bitmap;
    
    uint32_t copy_count = (sb->count < max_count) ? sb->count : max_count;
    memcpy(array, sb->values, copy_count * sizeof(uint64_t));
    
    return copy_count;
}

// 序列化操作实现
static size_t simple_bitmap_serialized_size(void* bitmap) {
    SSimpleBitmap* sb = (SSimpleBitmap*)bitmap;
    return sizeof(uint32_t) + sb->count * sizeof(uint64_t);
}

static int32_t simple_bitmap_serialize(void* bitmap, void* buffer, size_t buffer_size) {
    SSimpleBitmap* sb = (SSimpleBitmap*)bitmap;
    size_t required_size = simple_bitmap_serialized_size(bitmap);
    
    if (buffer_size < required_size) {
        return -1;
    }
    
    // 写入元素数量
    memcpy(buffer, &sb->count, sizeof(uint32_t));
    
    // 写入元素数组
    if (sb->count > 0) {
        memcpy((char*)buffer + sizeof(uint32_t), sb->values, sb->count * sizeof(uint64_t));
    }
    
    return 0;
}

static int32_t simple_bitmap_deserialize(void** bitmap, const void* buffer, size_t buffer_size) {
    if (buffer_size < sizeof(uint32_t)) {
        return -1;
    }
    
    // 读取元素数量
    uint32_t count;
    memcpy(&count, buffer, sizeof(uint32_t));
    
    size_t expected_size = sizeof(uint32_t) + count * sizeof(uint64_t);
    if (buffer_size < expected_size) {
        return -1;
    }
    
    // 创建新的位图
    SSimpleBitmap* sb = simple_bitmap_create();
    if (!sb) {
        return -1;
    }
    
    // 确保容量足够
    if (!ensure_capacity(sb, count)) {
        simple_bitmap_destroy(sb);
        return -1;
    }
    
    // 读取元素数组
    if (count > 0) {
        memcpy(sb->values, (char*)buffer + sizeof(uint32_t), count * sizeof(uint64_t));
    }
    
    sb->count = count;
    sb->sorted = false;
    
    *bitmap = sb;
    return 0;
}

// 内存管理实现
static void simple_bitmap_destroy(void* bitmap) {
    SSimpleBitmap* sb = (SSimpleBitmap*)bitmap;
    if (sb) {
        free(sb->values);
        free(sb);
    }
}

static size_t simple_bitmap_memory_usage(void* bitmap) {
    SSimpleBitmap* sb = (SSimpleBitmap*)bitmap;
    return sizeof(SSimpleBitmap) + sb->capacity * sizeof(uint64_t);
}

static void* simple_bitmap_create(void) {
    SSimpleBitmap* sb = malloc(sizeof(SSimpleBitmap));
    if (!sb) {
        return NULL;
    }
    
    sb->values = malloc(16 * sizeof(uint64_t)); // 初始容量16
    if (!sb->values) {
        free(sb);
        return NULL;
    }
    
    sb->count = 0;
    sb->capacity = 16;
    sb->sorted = true;
    
    return sb;
}

static void* simple_bitmap_clone(const void* bitmap) {
    const SSimpleBitmap* src = (const SSimpleBitmap*)bitmap;
    SSimpleBitmap* dst = simple_bitmap_create();
    if (!dst) {
        return NULL;
    }
    
    // 确保容量足够
    if (!ensure_capacity(dst, src->count)) {
        simple_bitmap_destroy(dst);
        return NULL;
    }
    
    // 复制数据
    memcpy(dst->values, src->values, src->count * sizeof(uint64_t));
    dst->count = src->count;
    dst->sorted = src->sorted;
    
    return dst;
}

// 创建位图接口
SBitmapInterface* bitmap_interface_create(void) {
    // 检查是否强制使用简单位图实现
    const char* use_simple = getenv("TDENGINE_USE_SIMPLE_BITMAP");
    if (use_simple && (strcmp(use_simple, "1") == 0 || strcmp(use_simple, "true") == 0)) {
        // 强制使用简单位图实现
        SBitmapInterface* interface = malloc(sizeof(SBitmapInterface));
        if (!interface) {
            return NULL;
        }
        
        // 创建位图实例
        interface->bitmap = simple_bitmap_create();
        if (!interface->bitmap) {
            free(interface);
            return NULL;
        }
        
        // 设置函数指针
        interface->add = simple_bitmap_add;
        interface->remove = simple_bitmap_remove;
        interface->contains = simple_bitmap_contains;
        interface->cardinality = simple_bitmap_cardinality;
        interface->clear = simple_bitmap_clear;
        interface->union_with = simple_bitmap_union_with;
        interface->intersect_with = simple_bitmap_intersect_with;
        interface->subtract = simple_bitmap_subtract;
        interface->to_array = simple_bitmap_to_array;
        interface->serialized_size = simple_bitmap_serialized_size;
        interface->serialize = simple_bitmap_serialize;
        interface->deserialize = simple_bitmap_deserialize;
        interface->destroy = simple_bitmap_destroy;
        interface->memory_usage = simple_bitmap_memory_usage;
        interface->create = simple_bitmap_create;
        interface->clone = simple_bitmap_clone;
        
        return interface;
    }
    
    // 默认使用 RoaringBitmap 实现
    extern SBitmapInterface* roaring_bitmap_interface_create(void);
    return roaring_bitmap_interface_create();
}

void bitmap_interface_destroy(SBitmapInterface* interface) {
    if (!interface) {
        return;
    }
    
    // printf("DEBUG: bitmap_interface_destroy: interface=%p, interface->bitmap=%p\n", 
    //        interface, interface->bitmap);
    
    // 验证位图对象的完整性
    if (interface->bitmap == (void*)0x7d6) {
        // printf("DEBUG: bitmap_interface_destroy: Corrupted bitmap detected, skipping destruction\n");
        // 清理接口结构体，但不销毁损坏的位图
        free(interface);
        return;
    }
    
    if (interface->bitmap) {
        // 验证位图对象是否仍然有效
        if (interface->bitmap < (void*)0x1000) {
            // printf("DEBUG: bitmap_interface_destroy: Invalid bitmap address, skipping destruction\n");
            free(interface);
            return;
        }
        
        // 安全地销毁位图
        interface->destroy(interface->bitmap);
    }
    
    free(interface);
} 