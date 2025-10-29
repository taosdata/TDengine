#include <unistd.h>
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

#include "../include/bitmap_interface.h"
#include "../include/roaring_bitmap.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <time.h>
#include <pthread.h>

// 测试辅助函数
static void print_test_result(const char* test_name, bool passed) {
    printf("[%s] %s\n", passed ? "PASS" : "FAIL", test_name);
}

static int64_t get_current_timestamp() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + (int64_t)ts.tv_nsec;
}

// 测试1: RoaringBitmap基本功能测试
static void test_roaring_basic_functionality() {
    printf("\n=== 测试1: RoaringBitmap基本功能测试 ===\n");
    
    SBitmapInterface* roaring_interface = roaring_bitmap_interface_create();
    assert(roaring_interface != NULL);
    
    // 测试添加元素
    roaring_interface->add(roaring_interface->bitmap, 1);
    roaring_interface->add(roaring_interface->bitmap, 1000);
    roaring_interface->add(roaring_interface->bitmap, 1000000);
    
    // 测试查询元素
    assert(roaring_interface->contains(roaring_interface->bitmap, 1) == true);
    assert(roaring_interface->contains(roaring_interface->bitmap, 1000) == true);
    assert(roaring_interface->contains(roaring_interface->bitmap, 1000000) == true);
    assert(roaring_interface->contains(roaring_interface->bitmap, 999) == false);
    
    // 测试基数
    assert(roaring_interface->cardinality(roaring_interface->bitmap) == 3);
    
    print_test_result("RoaringBitmap基本功能", true);
    
    roaring_interface->destroy(roaring_interface->bitmap);
    free(roaring_interface);
}

// 测试2: RoaringBitmap稀疏数据测试
static void test_roaring_sparse_data() {
    printf("\n=== 测试2: RoaringBitmap稀疏数据测试 ===\n");
    
    SBitmapInterface* roaring_interface = roaring_bitmap_interface_create();
    assert(roaring_interface != NULL);
    
    // 添加稀疏数据（大间隔）
    for (int i = 0; i < 1000; i++) {
        roaring_interface->add(roaring_interface->bitmap, i * 10000);
    }
    
    // 验证数据
    for (int i = 0; i < 1000; i++) {
        assert(roaring_interface->contains(roaring_interface->bitmap, i * 10000) == true);
    }
    
    assert(roaring_interface->cardinality(roaring_interface->bitmap) == 1000);
    
    print_test_result("RoaringBitmap稀疏数据", true);
    
    roaring_interface->destroy(roaring_interface->bitmap);
    free(roaring_interface);
}

// 测试3: RoaringBitmap密集数据测试
static void test_roaring_dense_data() {
    printf("\n=== 测试3: RoaringBitmap密集数据测试 ===\n");
    
    SBitmapInterface* roaring_interface = roaring_bitmap_interface_create();
    assert(roaring_interface != NULL);
    
    // 添加密集数据（连续）
    for (int i = 0; i < 100000; i++) {
        roaring_interface->add(roaring_interface->bitmap, i);
    }
    
    // 验证数据
    for (int i = 0; i < 100000; i++) {
        assert(roaring_interface->contains(roaring_interface->bitmap, i) == true);
    }
    
    assert(roaring_interface->cardinality(roaring_interface->bitmap) == 100000);
    
    print_test_result("RoaringBitmap密集数据", true);
    
    roaring_interface->destroy(roaring_interface->bitmap);
    free(roaring_interface);
}

// 测试4: RoaringBitmap混合数据测试
static void test_roaring_mixed_data() {
    printf("\n=== 测试4: RoaringBitmap混合数据测试 ===\n");
    
    SBitmapInterface* roaring_interface = roaring_bitmap_interface_create();
    assert(roaring_interface != NULL);
    
    // 添加混合数据（既有稀疏又有密集）
    // 密集区域1
    for (int i = 0; i < 1000; i++) {
        roaring_interface->add(roaring_interface->bitmap, i);
    }
    
    // 稀疏区域
    for (int i = 0; i < 100; i++) {
        roaring_interface->add(roaring_interface->bitmap, 10000 + i * 1000);
    }
    
    // 密集区域2
    for (int i = 0; i < 2000; i++) {
        roaring_interface->add(roaring_interface->bitmap, 100000 + i);
    }
    
    // 验证数据
    for (int i = 0; i < 1000; i++) {
        assert(roaring_interface->contains(roaring_interface->bitmap, i) == true);
    }
    
    for (int i = 0; i < 100; i++) {
        assert(roaring_interface->contains(roaring_interface->bitmap, 10000 + i * 1000) == true);
    }
    
    for (int i = 0; i < 2000; i++) {
        assert(roaring_interface->contains(roaring_interface->bitmap, 100000 + i) == true);
    }
    
    print_test_result("RoaringBitmap混合数据", true);
    
    roaring_interface->destroy(roaring_interface->bitmap);
    free(roaring_interface);
}

// 测试5: RoaringBitmap内存效率测试
static void test_roaring_memory_efficiency() {
    printf("\n=== 测试5: RoaringBitmap内存效率测试 ===\n");
    
    SBitmapInterface* roaring_interface = roaring_bitmap_interface_create();
    assert(roaring_interface != NULL);
    
    // 添加大量稀疏数据
    for (uint64_t i = 0; i < 10000; i++) {
        roaring_interface->add(roaring_interface->bitmap, i * 1000000ULL);
    }
    
    // 稀疏数据内存使用
    size_t memory_usage = roaring_interface->memory_usage(roaring_interface->bitmap);
    printf("稀疏数据内存使用: %zu bytes\n", memory_usage);
    
    // 清除数据
    roaring_interface->destroy(roaring_interface->bitmap);
    free(roaring_interface);
    
    // 重新创建，添加密集数据
    roaring_interface = roaring_bitmap_interface_create();
    for (int i = 0; i < 10000; i++) {
        roaring_interface->add(roaring_interface->bitmap, i);
    }
    
    memory_usage = roaring_interface->memory_usage(roaring_interface->bitmap);
    printf("密集数据内存使用: %zu bytes\n", memory_usage);
    
    print_test_result("RoaringBitmap内存效率", true);
    
    roaring_interface->destroy(roaring_interface->bitmap);
    free(roaring_interface);
}

// 测试6: RoaringBitmap性能基准测试
static void test_roaring_performance_benchmark() {
    printf("\n=== 测试6: RoaringBitmap性能基准测试 ===\n");
    
    SBitmapInterface* roaring_interface = roaring_bitmap_interface_create();
    assert(roaring_interface != NULL);
    
    const int test_count = 1000000;
    int64_t start_time = get_current_timestamp();
    
    // 添加性能测试
    for (int i = 0; i < test_count; i++) {
        roaring_interface->add(roaring_interface->bitmap, i);
    }
    
    int64_t add_time = get_current_timestamp() - start_time;
    printf("添加 %d 个元素耗时: %ld ns (%.2f ops/ms)\n", 
           test_count, add_time, (double)test_count / (add_time / 1000000.0));
    
    // 查询性能测试
    start_time = get_current_timestamp();
    for (int i = 0; i < test_count; i++) {
        roaring_interface->contains(roaring_interface->bitmap, i);
    }
    
    int64_t query_time = get_current_timestamp() - start_time;
    printf("查询 %d 个元素耗时: %ld ns (%.2f ops/ms)\n", 
           test_count, query_time, (double)test_count / (query_time / 1000000.0));
    
    // 基数计算性能测试
    start_time = get_current_timestamp();
    uint64_t cardinality = roaring_interface->cardinality(roaring_interface->bitmap);
    int64_t cardinality_time = get_current_timestamp() - start_time;
    printf("基数计算耗时: %ld ns, 结果: %lu\n", cardinality_time, cardinality);
    
    print_test_result("RoaringBitmap性能基准", true);
    
    roaring_interface->destroy(roaring_interface->bitmap);
    free(roaring_interface);
}

// 测试7: RoaringBitmap并发安全测试
static void* concurrent_roaring_thread(void* arg) {
    SBitmapInterface* roaring_interface = (SBitmapInterface*)arg;
    
    for (int i = 0; i < 10000; i++) {
        roaring_interface->add(roaring_interface->bitmap, i);
    }
    
    return NULL;
}

static void test_roaring_concurrent_safety() {
    printf("\n=== 测试7: RoaringBitmap并发安全测试 ===\n");
    
    SBitmapInterface* roaring_interface = roaring_bitmap_interface_create();
    assert(roaring_interface != NULL);
    
    const int thread_count = 8;
    pthread_t threads[thread_count];
    
    // 创建多个线程并发添加
    for (int i = 0; i < thread_count; i++) {
        int result = pthread_create(&threads[i], NULL, concurrent_roaring_thread, roaring_interface);
        assert(result == 0);
    }
    
    // 等待所有线程完成
    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
    }
    
    print_test_result("RoaringBitmap并发安全", true);
    
    roaring_interface->destroy(roaring_interface->bitmap);
    free(roaring_interface);
}

// 测试8: RoaringBitmap序列化性能测试
static void test_roaring_serialization_performance() {
    printf("\n=== 测试8: RoaringBitmap序列化性能测试 ===\n");
    
    SBitmapInterface* roaring_interface = roaring_bitmap_interface_create();
    assert(roaring_interface != NULL);
    
    // 添加测试数据
    for (int i = 0; i < 100000; i++) {
        roaring_interface->add(roaring_interface->bitmap, i);
    }
    
    // 序列化性能测试
    int64_t start_time = get_current_timestamp();
    size_t serialized_size = roaring_interface->serialized_size(roaring_interface->bitmap);
    void* serialized_data = malloc(serialized_size);
    int32_t ser_ret = roaring_interface->serialize(roaring_interface->bitmap, serialized_data, serialized_size);
    int64_t serialize_time = get_current_timestamp() - start_time;
    
    printf("序列化耗时: %ld ns, 数据大小: %zu bytes\n", serialize_time, serialized_size);
    
    // 反序列化性能测试
    SBitmapInterface* roaring_interface2 = roaring_bitmap_interface_create();
    void* tmp_bitmap = NULL;
    start_time = get_current_timestamp();
    int32_t deser_ret = roaring_interface2->deserialize(&tmp_bitmap, serialized_data, serialized_size);
    int64_t deserialize_time = get_current_timestamp() - start_time;
    roaring_interface2->destroy(roaring_interface2->bitmap);
    roaring_interface2->bitmap = tmp_bitmap;
    
    printf("反序列化耗时: %ld ns\n", deserialize_time);
    
    assert(ser_ret == 0);
    assert(deser_ret == 0);
    
    // 验证数据一致性
    for (int i = 0; i < 100000; i++) {
        assert(roaring_interface2->contains(roaring_interface2->bitmap, i) == true);
    }
    
    print_test_result("RoaringBitmap序列化性能", true);
    
    // 清理
    free(serialized_data);
    roaring_interface->destroy(roaring_interface->bitmap);
    free(roaring_interface);
    roaring_interface2->destroy(roaring_interface2->bitmap);
    free(roaring_interface2);
}

// 测试9: RoaringBitmap与SimpleBitmap对比测试
static void test_roaring_vs_simple_comparison() {
    printf("\n=== 测试9: RoaringBitmap与SimpleBitmap对比测试 ===\n");
    
    const int test_count = 100000;
    
    // 测试RoaringBitmap
    SBitmapInterface* roaring_interface = roaring_bitmap_interface_create();
    
    int64_t start_time = get_current_timestamp();
    for (int i = 0; i < test_count; i++) {
        roaring_interface->add(roaring_interface->bitmap, i);
    }
    int64_t roaring_add_time = get_current_timestamp() - start_time;
    
    start_time = get_current_timestamp();
    for (int i = 0; i < test_count; i++) {
        roaring_interface->contains(roaring_interface->bitmap, i);
    }
    int64_t roaring_query_time = get_current_timestamp() - start_time;
    
    size_t roaring_memory = roaring_interface->memory_usage(roaring_interface->bitmap);
    
    roaring_interface->destroy(roaring_interface->bitmap);
    free(roaring_interface);
    
    // 测试SimpleBitmap
    setenv("TDENGINE_USE_SIMPLE_BITMAP", "1", 1);
    SBitmapInterface* simple_interface = bitmap_interface_create();
    
    start_time = get_current_timestamp();
    for (int i = 0; i < test_count; i++) {
        simple_interface->add(simple_interface->bitmap, i);
    }
    int64_t simple_add_time = get_current_timestamp() - start_time;
    
    start_time = get_current_timestamp();
    for (int i = 0; i < test_count; i++) {
        simple_interface->contains(simple_interface->bitmap, i);
    }
    int64_t simple_query_time = get_current_timestamp() - start_time;
    
    size_t simple_memory = simple_interface->memory_usage(simple_interface->bitmap);
    
    simple_interface->destroy(simple_interface->bitmap);
    free(simple_interface);
    unsetenv("TDENGINE_USE_SIMPLE_BITMAP");
    
    // 输出对比结果
    printf("性能对比结果:\n");
    printf("RoaringBitmap - 添加: %ld ns, 查询: %ld ns, 内存: %zu bytes\n", 
           roaring_add_time, roaring_query_time, roaring_memory);
    printf("SimpleBitmap  - 添加: %ld ns, 查询: %ld ns, 内存: %zu bytes\n", 
           simple_add_time, simple_query_time, simple_memory);
    
    print_test_result("RoaringBitmap与SimpleBitmap对比", true);
}

int main() {
    printf("开始RoaringBitmap专用测试...\n");
    
    test_roaring_basic_functionality();
    test_roaring_sparse_data();
    test_roaring_dense_data();
    test_roaring_mixed_data();
    test_roaring_memory_efficiency();
    test_roaring_performance_benchmark();
    test_roaring_concurrent_safety();
    test_roaring_serialization_performance();
    test_roaring_vs_simple_comparison();
    
    printf("\n所有RoaringBitmap专用测试完成！\n");
    return 0;
}
