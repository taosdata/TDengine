#include "../include/bitmap_interface.h"
#include <roaring/roaring64.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>

// RoaringBitmap 适配器实现
typedef struct SRoaringBitmap {
    roaring64_bitmap_t* roaring_bitmap;
    pthread_mutex_t mutex;  // 添加互斥锁用于线程安全
} SRoaringBitmap;

// 内部函数声明 - 使用 tdengine_ 前缀避免与 CRoaring 冲突
static void tdengine_roaring_add(void* bitmap, uint64_t value);
static void tdengine_roaring_remove(void* bitmap, uint64_t value);
static bool tdengine_roaring_contains(void* bitmap, uint64_t value);
static uint32_t tdengine_roaring_cardinality(void* bitmap);
static void tdengine_roaring_clear(void* bitmap);
static void tdengine_roaring_union_with(void* bitmap, const void* other);
static void tdengine_roaring_intersect_with(void* bitmap, const void* other);
static void tdengine_roaring_subtract(void* bitmap, const void* other);
static uint32_t tdengine_roaring_to_array(void* bitmap, uint64_t* array, uint32_t max_count);
static size_t tdengine_roaring_serialized_size(void* bitmap);
static int32_t tdengine_roaring_serialize(void* bitmap, void* buffer, size_t buffer_size);
static int32_t tdengine_roaring_deserialize(void** bitmap, const void* buffer, size_t buffer_size);
static void tdengine_roaring_destroy(void* bitmap);
static size_t tdengine_roaring_memory_usage(void* bitmap);
static void* tdengine_roaring_create(void);
static void* tdengine_roaring_clone(const void* bitmap);

// 实现函数
static void tdengine_roaring_add(void* bitmap, uint64_t value) {
    SRoaringBitmap* rb = (SRoaringBitmap*)bitmap;
    if (rb && rb->roaring_bitmap) {
        pthread_mutex_lock(&rb->mutex);
        roaring64_bitmap_add(rb->roaring_bitmap, value);
        pthread_mutex_unlock(&rb->mutex);
    }
}

static void tdengine_roaring_remove(void* bitmap, uint64_t value) {
    SRoaringBitmap* rb = (SRoaringBitmap*)bitmap;
    if (rb && rb->roaring_bitmap) {
        pthread_mutex_lock(&rb->mutex);
        roaring64_bitmap_remove(rb->roaring_bitmap, value);
        pthread_mutex_unlock(&rb->mutex);
    }
}

static bool tdengine_roaring_contains(void* bitmap, uint64_t value) {
    SRoaringBitmap* rb = (SRoaringBitmap*)bitmap;
    if (rb && rb->roaring_bitmap) {
        pthread_mutex_lock(&rb->mutex);
        bool result = roaring64_bitmap_contains(rb->roaring_bitmap, value);
        pthread_mutex_unlock(&rb->mutex);
        return result;
    }
    return false;
}

static uint32_t tdengine_roaring_cardinality(void* bitmap) {
    SRoaringBitmap* rb = (SRoaringBitmap*)bitmap;
    if (rb && rb->roaring_bitmap) {
        pthread_mutex_lock(&rb->mutex);
        uint32_t result = (uint32_t)roaring64_bitmap_get_cardinality(rb->roaring_bitmap);
        pthread_mutex_unlock(&rb->mutex);
        return result;
    }
    return 0;
}

static void tdengine_roaring_clear(void* bitmap) {
    SRoaringBitmap* rb = (SRoaringBitmap*)bitmap;
    if (rb && rb->roaring_bitmap) {
        pthread_mutex_lock(&rb->mutex);
        roaring64_bitmap_clear(rb->roaring_bitmap);
        pthread_mutex_unlock(&rb->mutex);
    }
}

static void tdengine_roaring_union_with(void* bitmap, const void* other) {
    SRoaringBitmap* rb = (SRoaringBitmap*)bitmap;
    SRoaringBitmap* other_rb = (SRoaringBitmap*)other;
    
    // 严格的参数验证
    if (!rb || !rb->roaring_bitmap || !other_rb || !other_rb->roaring_bitmap) {
        printf("DEBUG: tdengine_roaring_union_with: Invalid parameters\n");
        return;
    }
    
    // 验证位图对象的完整性
    if (rb->roaring_bitmap == (void*)0x7d6 || other_rb->roaring_bitmap == (void*)0x7d6) {
        printf("DEBUG: tdengine_roaring_union_with: Corrupted bitmap detected\n");
        return;
    }
    
    pthread_mutex_lock(&rb->mutex);
    
    // 再次验证位图是否仍然有效
    if (!rb->roaring_bitmap || !other_rb->roaring_bitmap) {
        printf("DEBUG: tdengine_roaring_union_with: Bitmap became invalid during lock\n");
        pthread_mutex_unlock(&rb->mutex);
        return;
    }
    
    // 使用安全的并集操作
    roaring64_bitmap_or_inplace(rb->roaring_bitmap, other_rb->roaring_bitmap);
    
    pthread_mutex_unlock(&rb->mutex);
}

static void tdengine_roaring_intersect_with(void* bitmap, const void* other) {
    SRoaringBitmap* rb = (SRoaringBitmap*)bitmap;
    SRoaringBitmap* other_rb = (SRoaringBitmap*)other;
    if (rb && rb->roaring_bitmap && other_rb && other_rb->roaring_bitmap) {
        pthread_mutex_lock(&rb->mutex);
        roaring64_bitmap_and_inplace(rb->roaring_bitmap, other_rb->roaring_bitmap);
        pthread_mutex_unlock(&rb->mutex);
    }
}

static void tdengine_roaring_subtract(void* bitmap, const void* other) {
    SRoaringBitmap* rb = (SRoaringBitmap*)bitmap;
    SRoaringBitmap* other_rb = (SRoaringBitmap*)other;
    if (rb && rb->roaring_bitmap && other_rb && other_rb->roaring_bitmap) {
        pthread_mutex_lock(&rb->mutex);
        roaring64_bitmap_andnot_inplace(rb->roaring_bitmap, other_rb->roaring_bitmap);
        pthread_mutex_unlock(&rb->mutex);
    }
}

static uint32_t tdengine_roaring_to_array(void* bitmap, uint64_t* array, uint32_t max_count) {
    SRoaringBitmap* rb = (SRoaringBitmap*)bitmap;
    if (!rb || !rb->roaring_bitmap || !array) {
        return 0;
    }
    
    pthread_mutex_lock(&rb->mutex);
    uint64_t cardinality = roaring64_bitmap_get_cardinality(rb->roaring_bitmap);
    uint32_t count = (uint32_t)(cardinality < max_count ? cardinality : max_count);
    
    if (count > 0) {
        roaring64_bitmap_to_uint64_array(rb->roaring_bitmap, array);
    }
    pthread_mutex_unlock(&rb->mutex);
    
    return count;
}

static size_t tdengine_roaring_serialized_size(void* bitmap) {
    SRoaringBitmap* rb = (SRoaringBitmap*)bitmap;
    if (rb && rb->roaring_bitmap) {
        pthread_mutex_lock(&rb->mutex);
        size_t size = roaring64_bitmap_portable_size_in_bytes(rb->roaring_bitmap);
        pthread_mutex_unlock(&rb->mutex);
        return size;
    }
    return 0;
}

static int32_t tdengine_roaring_serialize(void* bitmap, void* buffer, size_t buffer_size) {
    SRoaringBitmap* rb = (SRoaringBitmap*)bitmap;
    if (!rb || !rb->roaring_bitmap || !buffer) {
        return -1;
    }
    
    pthread_mutex_lock(&rb->mutex);
    size_t serialized_size = roaring64_bitmap_portable_size_in_bytes(rb->roaring_bitmap);
    
    if (buffer_size < serialized_size) {
        pthread_mutex_unlock(&rb->mutex);
        return -1;
    }
    
    size_t bytes_written = roaring64_bitmap_portable_serialize(rb->roaring_bitmap, (char*)buffer);
    pthread_mutex_unlock(&rb->mutex);
    
    return (bytes_written == serialized_size) ? 0 : -1;
}

static int32_t tdengine_roaring_deserialize(void** bitmap, const void* buffer, size_t buffer_size) {
    if (!bitmap || !buffer) {
        return -1;
    }
    
    SRoaringBitmap* rb = malloc(sizeof(SRoaringBitmap));
    if (!rb) {
        return -1;
    }
    
    // 初始化互斥锁
    if (pthread_mutex_init(&rb->mutex, NULL) != 0) {
        free(rb);
        return -1;
    }
    
    pthread_mutex_lock(&rb->mutex);
    rb->roaring_bitmap = roaring64_bitmap_portable_deserialize_safe(buffer, buffer_size);
    pthread_mutex_unlock(&rb->mutex);
    
    if (!rb->roaring_bitmap) {
        pthread_mutex_destroy(&rb->mutex);
        free(rb);
        return -1;
    }
    
    *bitmap = rb;
    return 0;
}

static void tdengine_roaring_destroy(void* bitmap) {
    SRoaringBitmap* rb = (SRoaringBitmap*)bitmap;
    if (rb) {
        if (rb->roaring_bitmap) {
            roaring64_bitmap_free(rb->roaring_bitmap);
        }
        pthread_mutex_destroy(&rb->mutex);
        free(rb);
    }
}

static size_t tdengine_roaring_memory_usage(void* bitmap) {
    SRoaringBitmap* rb = (SRoaringBitmap*)bitmap;
    if (rb && rb->roaring_bitmap) {
        pthread_mutex_lock(&rb->mutex);
        roaring64_statistics_t stats;
        roaring64_bitmap_statistics(rb->roaring_bitmap, &stats);
        size_t memory_usage = stats.n_bytes_array_containers + 
                             stats.n_bytes_run_containers + 
                             stats.n_bytes_bitset_containers;
        pthread_mutex_unlock(&rb->mutex);
        return memory_usage;
    }
    return 0;
}

static void* tdengine_roaring_create(void) {
    SRoaringBitmap* rb = malloc(sizeof(SRoaringBitmap));
    if (!rb) {
        return NULL;
    }
    
    // 初始化互斥锁
    if (pthread_mutex_init(&rb->mutex, NULL) != 0) {
        free(rb);
        return NULL;
    }
    
    rb->roaring_bitmap = roaring64_bitmap_create();
    if (!rb->roaring_bitmap) {
        pthread_mutex_destroy(&rb->mutex);
        free(rb);
        return NULL;
    }
    
    return rb;
}

static void* tdengine_roaring_clone(const void* bitmap) {
    SRoaringBitmap* src_rb = (SRoaringBitmap*)bitmap;
    if (!src_rb || !src_rb->roaring_bitmap) {
        return NULL;
    }
    
    SRoaringBitmap* dst_rb = malloc(sizeof(SRoaringBitmap));
    if (!dst_rb) {
        return NULL;
    }
    
    // 初始化互斥锁
    if (pthread_mutex_init(&dst_rb->mutex, NULL) != 0) {
        free(dst_rb);
        return NULL;
    }
    
    pthread_mutex_lock(&src_rb->mutex);
    dst_rb->roaring_bitmap = roaring64_bitmap_copy(src_rb->roaring_bitmap);
    pthread_mutex_unlock(&src_rb->mutex);
    
    if (!dst_rb->roaring_bitmap) {
        pthread_mutex_destroy(&dst_rb->mutex);
        free(dst_rb);
        return NULL;
    }
    
    return dst_rb;
}

// 工厂函数
SBitmapInterface* roaring_bitmap_interface_create(void) {
    SBitmapInterface* interface = malloc(sizeof(SBitmapInterface));
    if (!interface) {
        return NULL;
    }
    
    // 创建位图实例
    interface->bitmap = tdengine_roaring_create();
    if (!interface->bitmap) {
        free(interface);
        return NULL;
    }
    
    // 设置函数指针
    interface->add = tdengine_roaring_add;
    interface->remove = tdengine_roaring_remove;
    interface->contains = tdengine_roaring_contains;
    interface->cardinality = tdengine_roaring_cardinality;
    interface->clear = tdengine_roaring_clear;
    interface->union_with = tdengine_roaring_union_with;
    interface->intersect_with = tdengine_roaring_intersect_with;
    interface->subtract = tdengine_roaring_subtract;
    interface->to_array = tdengine_roaring_to_array;
    interface->serialized_size = tdengine_roaring_serialized_size;
    interface->serialize = tdengine_roaring_serialize;
    interface->deserialize = tdengine_roaring_deserialize;
    interface->destroy = tdengine_roaring_destroy;
    interface->memory_usage = tdengine_roaring_memory_usage;
    interface->create = tdengine_roaring_create;
    interface->clone = tdengine_roaring_clone;
    
    return interface;
} 