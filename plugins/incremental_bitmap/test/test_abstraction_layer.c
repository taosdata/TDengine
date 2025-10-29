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

// 测试1: 工厂模式测试
static void test_factory_pattern() {
    printf("\n=== 测试1: 工厂模式测试 ===\n");
    
    // 测试默认实现（应该是RoaringBitmap）
    SBitmapInterface* default_interface = bitmap_interface_create();
    assert(default_interface != NULL);
    assert(default_interface->add != NULL);
    assert(default_interface->remove != NULL);
    assert(default_interface->contains != NULL);
    print_test_result("默认工厂创建", true);
    
    // 测试基本操作
    default_interface->add(default_interface->bitmap, 100);
    assert(default_interface->contains(default_interface->bitmap, 100) == true);
    assert(default_interface->cardinality(default_interface->bitmap) == 1);
    print_test_result("默认实现基本操作", true);
    
    // 清理
    default_interface->destroy(default_interface->bitmap);
    free(default_interface);
}

// 测试2: 环境变量控制测试
static void test_environment_control() {
    printf("\n=== 测试2: 环境变量控制测试 ===\n");
    
    // 设置环境变量强制使用SimpleBitmap
    setenv("TDENGINE_USE_SIMPLE_BITMAP", "1", 1);
    
    SBitmapInterface* simple_interface = bitmap_interface_create();
    assert(simple_interface != NULL);
    print_test_result("环境变量控制SimpleBitmap", true);
    
    // 测试基本操作
    simple_interface->add(simple_interface->bitmap, 200);
    assert(simple_interface->contains(simple_interface->bitmap, 200) == true);
    assert(simple_interface->cardinality(simple_interface->bitmap) == 1);
    print_test_result("SimpleBitmap基本操作", true);
    
    // 清理
    simple_interface->destroy(simple_interface->bitmap);
    free(simple_interface);
    
    // 恢复环境变量
    unsetenv("TDENGINE_USE_SIMPLE_BITMAP");
}

// 测试3: RoaringBitmap直接创建测试
static void test_roaring_bitmap_direct() {
    printf("\n=== 测试3: RoaringBitmap直接创建测试 ===\n");
    
    SBitmapInterface* roaring_interface = roaring_bitmap_interface_create();
    assert(roaring_interface != NULL);
    print_test_result("RoaringBitmap直接创建", true);
    
    // 测试基本操作
    roaring_interface->add(roaring_interface->bitmap, 300);
    assert(roaring_interface->contains(roaring_interface->bitmap, 300) == true);
    assert(roaring_interface->cardinality(roaring_interface->bitmap) == 1);
    print_test_result("RoaringBitmap基本操作", true);
    
    // 清理
    roaring_interface->destroy(roaring_interface->bitmap);
    free(roaring_interface);
}

// 测试4: 接口一致性测试
static void test_interface_consistency() {
    printf("\n=== 测试4: 接口一致性测试 ===\n");
    
    // 测试SimpleBitmap
    setenv("TDENGINE_USE_SIMPLE_BITMAP", "1", 1);
    SBitmapInterface* simple_interface = bitmap_interface_create();
    
    // 测试RoaringBitmap
    unsetenv("TDENGINE_USE_SIMPLE_BITMAP");
    SBitmapInterface* roaring_interface = bitmap_interface_create();
    
    // 验证两个接口都有相同的函数指针
    assert(simple_interface->add != NULL);
    assert(simple_interface->remove != NULL);
    assert(simple_interface->contains != NULL);
    assert(simple_interface->cardinality != NULL);
    assert(simple_interface->clone != NULL);
    assert(simple_interface->destroy != NULL);
    
    assert(roaring_interface->add != NULL);
    assert(roaring_interface->remove != NULL);
    assert(roaring_interface->contains != NULL);
    assert(roaring_interface->cardinality != NULL);
    assert(roaring_interface->clone != NULL);
    assert(roaring_interface->destroy != NULL);
    
    print_test_result("接口一致性验证", true);
    
    // 清理
    simple_interface->destroy(simple_interface->bitmap);
    free(simple_interface);
    roaring_interface->destroy(roaring_interface->bitmap);
    free(roaring_interface);
}

// 测试5: 性能对比测试
static void test_performance_comparison() {
    printf("\n=== 测试5: 性能对比测试 ===\n");
    
    const int test_count = 100000;
    
    // 测试SimpleBitmap性能
    setenv("TDENGINE_USE_SIMPLE_BITMAP", "1", 1);
    SBitmapInterface* simple_interface = bitmap_interface_create();
    
    clock_t start_time = clock();
    for (int i = 0; i < test_count; i++) {
        simple_interface->add(simple_interface->bitmap, i);
    }
    clock_t simple_add_time = clock() - start_time;
    
    start_time = clock();
    for (int i = 0; i < test_count; i++) {
        simple_interface->contains(simple_interface->bitmap, i);
    }
    clock_t simple_query_time = clock() - start_time;
    
    printf("SimpleBitmap - 添加: %.2f ms, 查询: %.2f ms\n", 
           (double)simple_add_time * 1000 / CLOCKS_PER_SEC,
           (double)simple_query_time * 1000 / CLOCKS_PER_SEC);
    
    simple_interface->destroy(simple_interface->bitmap);
    free(simple_interface);
    
    // 测试RoaringBitmap性能
    unsetenv("TDENGINE_USE_SIMPLE_BITMAP");
    SBitmapInterface* roaring_interface = bitmap_interface_create();
    
    start_time = clock();
    for (int i = 0; i < test_count; i++) {
        roaring_interface->add(roaring_interface->bitmap, i);
    }
    clock_t roaring_add_time = clock() - start_time;
    
    start_time = clock();
    for (int i = 0; i < test_count; i++) {
        roaring_interface->contains(roaring_interface->bitmap, i);
    }
    clock_t roaring_query_time = clock() - start_time;
    
    printf("RoaringBitmap - 添加: %.2f ms, 查询: %.2f ms\n", 
           (double)roaring_add_time * 1000 / CLOCKS_PER_SEC,
           (double)roaring_query_time * 1000 / CLOCKS_PER_SEC);
    
    roaring_interface->destroy(roaring_interface->bitmap);
    free(roaring_interface);
    
    print_test_result("性能对比测试", true);
}

// 测试6: 内存使用对比测试
static void test_memory_usage_comparison() {
    printf("\n=== 测试6: 内存使用对比测试 ===\n");
    
    const int test_count = 10000;
    
    // 测试SimpleBitmap内存使用
    setenv("TDENGINE_USE_SIMPLE_BITMAP", "1", 1);
    SBitmapInterface* simple_interface = bitmap_interface_create();
    
    for (int i = 0; i < test_count; i++) {
        simple_interface->add(simple_interface->bitmap, i);
    }
    
    size_t simple_memory = simple_interface->memory_usage(simple_interface->bitmap);
    printf("SimpleBitmap内存使用: %zu bytes\n", simple_memory);
    
    simple_interface->destroy(simple_interface->bitmap);
    free(simple_interface);
    
    // 测试RoaringBitmap内存使用
    unsetenv("TDENGINE_USE_SIMPLE_BITMAP");
    SBitmapInterface* roaring_interface = bitmap_interface_create();
    
    for (int i = 0; i < test_count; i++) {
        roaring_interface->add(roaring_interface->bitmap, i);
    }
    
    size_t roaring_memory = roaring_interface->memory_usage(roaring_interface->bitmap);
    printf("RoaringBitmap内存使用: %zu bytes\n", roaring_memory);
    
    roaring_interface->destroy(roaring_interface->bitmap);
    free(roaring_interface);
    
    print_test_result("内存使用对比测试", true);
}

// 测试7: 序列化/反序列化测试
static void test_serialization() {
    printf("\n=== 测试7: 序列化/反序列化测试 ===\n");
    
    // 测试SimpleBitmap序列化
    setenv("TDENGINE_USE_SIMPLE_BITMAP", "1", 1);
    SBitmapInterface* simple_interface = bitmap_interface_create();
    
    // 添加测试数据
    for (int i = 0; i < 100; i++) {
        simple_interface->add(simple_interface->bitmap, i);
    }
    
    // 序列化
    size_t simple_size = simple_interface->serialized_size(simple_interface->bitmap);
    void* simple_data = malloc(simple_size);
    int32_t ser_ret = simple_interface->serialize(simple_interface->bitmap, simple_data, simple_size);
    assert(ser_ret == 0);
    
    // 反序列化
    SBitmapInterface* simple_interface2 = bitmap_interface_create();
    void* tmp_bitmap = NULL;
    int32_t deser_ret = simple_interface2->deserialize(&tmp_bitmap, simple_data, simple_size);
    assert(deser_ret == 0);
    simple_interface2->destroy(simple_interface2->bitmap);
    simple_interface2->bitmap = tmp_bitmap;
    
    // 验证数据一致性
    for (int i = 0; i < 100; i++) {
        assert(simple_interface2->contains(simple_interface2->bitmap, i) == true);
    }
    
    print_test_result("SimpleBitmap序列化测试", true);
    
    // 清理
    free(simple_data);
    simple_interface->destroy(simple_interface->bitmap);
    free(simple_interface);
    simple_interface2->destroy(simple_interface2->bitmap);
    free(simple_interface2);
    
    // 测试RoaringBitmap序列化
    unsetenv("TDENGINE_USE_SIMPLE_BITMAP");
    SBitmapInterface* roaring_interface = bitmap_interface_create();
    
    // 添加测试数据
    for (int i = 0; i < 100; i++) {
        roaring_interface->add(roaring_interface->bitmap, i);
    }
    
    // 序列化
    size_t roaring_size = roaring_interface->serialized_size(roaring_interface->bitmap);
    void* roaring_data = malloc(roaring_size);
    ser_ret = roaring_interface->serialize(roaring_interface->bitmap, roaring_data, roaring_size);
    assert(ser_ret == 0);
    
    // 反序列化
    SBitmapInterface* roaring_interface2 = bitmap_interface_create();
    tmp_bitmap = NULL;
    deser_ret = roaring_interface2->deserialize(&tmp_bitmap, roaring_data, roaring_size);
    assert(deser_ret == 0);
    roaring_interface2->destroy(roaring_interface2->bitmap);
    roaring_interface2->bitmap = tmp_bitmap;
    
    // 验证数据一致性
    for (int i = 0; i < 100; i++) {
        assert(roaring_interface2->contains(roaring_interface2->bitmap, i) == true);
    }
    
    print_test_result("RoaringBitmap序列化测试", true);
    
    // 清理
    free(roaring_data);
    roaring_interface->destroy(roaring_interface->bitmap);
    free(roaring_interface);
    roaring_interface2->destroy(roaring_interface2->bitmap);
    free(roaring_interface2);
}

// 测试8: 克隆功能测试
static void test_clone_functionality() {
    printf("\n=== 测试8: 克隆功能测试 ===\n");
    
    // 测试SimpleBitmap克隆
    setenv("TDENGINE_USE_SIMPLE_BITMAP", "1", 1);
    SBitmapInterface* simple_interface = bitmap_interface_create();
    
    // 添加测试数据
    for (int i = 0; i < 50; i++) {
        simple_interface->add(simple_interface->bitmap, i);
    }
    
    // 克隆
    void* simple_clone_bitmap = simple_interface->clone(simple_interface->bitmap);
    SBitmapInterface* simple_clone = bitmap_interface_create();
    simple_clone->destroy(simple_clone->bitmap);
    simple_clone->bitmap = simple_clone_bitmap;
    
    // 验证克隆数据一致性
    for (int i = 0; i < 50; i++) {
        assert(simple_clone->contains(simple_clone->bitmap, i) == true);
    }
    
    print_test_result("SimpleBitmap克隆测试", true);
    
    // 清理
    simple_interface->destroy(simple_interface->bitmap);
    free(simple_interface);
    simple_clone->destroy(simple_clone->bitmap);
    free(simple_clone);
    
    // 测试RoaringBitmap克隆
    unsetenv("TDENGINE_USE_SIMPLE_BITMAP");
    SBitmapInterface* roaring_interface = bitmap_interface_create();
    
    // 添加测试数据
    for (int i = 0; i < 50; i++) {
        roaring_interface->add(roaring_interface->bitmap, i);
    }
    
    // 克隆
    void* roaring_clone_bitmap = roaring_interface->clone(roaring_interface->bitmap);
    SBitmapInterface* roaring_clone = bitmap_interface_create();
    roaring_clone->destroy(roaring_clone->bitmap);
    roaring_clone->bitmap = roaring_clone_bitmap;
    
    // 验证克隆数据一致性
    for (int i = 0; i < 50; i++) {
        assert(roaring_clone->contains(roaring_clone->bitmap, i) == true);
    }
    
    print_test_result("RoaringBitmap克隆测试", true);
    
    // 清理
    roaring_interface->destroy(roaring_interface->bitmap);
    free(roaring_interface);
    roaring_clone->destroy(roaring_clone->bitmap);
    free(roaring_clone);
}

int main() {
    printf("开始抽象层接口测试...\n");
    
    test_factory_pattern();
    test_environment_control();
    test_roaring_bitmap_direct();
    test_interface_consistency();
    test_performance_comparison();
    test_memory_usage_comparison();
    test_serialization();
    test_clone_functionality();
    
    printf("\n所有抽象层接口测试完成！\n");
    return 0;
}
