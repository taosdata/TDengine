#include "../include/skiplist.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>

// 测试结果统计
static int total_tests = 0;
static int passed_tests = 0;
static int failed_tests = 0;

// 测试用全局互斥锁：保护对非线程安全跳表的并发写
static pthread_mutex_t g_skiplist_mutex = PTHREAD_MUTEX_INITIALIZER;

// 测试宏
#define TEST_ASSERT(condition, message) do { \
    total_tests++; \
    if (condition) { \
        passed_tests++; \
        printf("✓ %s\n", message); \
    } else { \
        failed_tests++; \
        printf("✗ %s\n", message); \
    } \
} while(0)

// 简单的键值结构用于测试
typedef struct {
    uint64_t key;
    char* value;
} TestEntry;

// 释放节点值的回调（文件作用域）
static void free_entry_callback(uint64_t key, void* value, void* user_data) {
    (void)key;
    if (value) {
        TestEntry* e = (TestEntry*)value;
        free(e->value);
        free(e);
        if (user_data) {
            int* c = (int*)user_data;
            (*c)++;
        }
    }
}

// 范围查询回调函数
static void range_query_callback(uint64_t key, void* value, void* user_data) {
    int* count_ptr = (int*)user_data;
    (*count_ptr)++;
}

// 测试基本操作
void test_basic_operations() {
    printf("\n=== 测试跳表基本操作 ===\n");
    
    skiplist_t* skiplist = skiplist_create();
    TEST_ASSERT(skiplist != NULL, "创建跳表");
    
    // 测试空跳表
    TEST_ASSERT(skiplist->size == 0, "空跳表大小为0");
    TEST_ASSERT(skiplist->size == 0, "空跳表为空");
    
    // 测试插入元素
    TestEntry* entry1 = malloc(sizeof(TestEntry));
    entry1->key = 100;
    entry1->value = strdup("value100");
    
    skiplist_insert(skiplist, entry1->key, entry1);
    TEST_ASSERT(skiplist->size == 1, "插入后大小为1");
    TEST_ASSERT(skiplist->size > 0, "插入后不为空");
    
    // 测试查找元素
    TestEntry* found = (TestEntry*)skiplist_find(skiplist, entry1->key);
    TEST_ASSERT(found != NULL, "查找元素成功");
    TEST_ASSERT(found->key == 100, "查找结果键值正确");
    TEST_ASSERT(strcmp(found->value, "value100") == 0, "查找结果值正确");
    
    // 测试查找不存在的元素
    uint64_t not_found_key = 200;
    TestEntry* not_found = (TestEntry*)skiplist_find(skiplist, not_found_key);
    TEST_ASSERT(not_found == NULL, "查找不存在的元素返回NULL");
    
    // 测试插入多个元素
    TestEntry* entry2 = malloc(sizeof(TestEntry));
    entry2->key = 50;
    entry2->value = strdup("value50");
    
    TestEntry* entry3 = malloc(sizeof(TestEntry));
    entry3->key = 150;
    entry3->value = strdup("value150");
    
    skiplist_insert(skiplist, entry2->key, entry2);
    skiplist_insert(skiplist, entry3->key, entry3);
    
    TEST_ASSERT(skiplist->size == 3, "插入多个元素后大小正确");
    
    // 测试删除元素
    skiplist_remove(skiplist, entry2->key);
    TEST_ASSERT(skiplist->size == 2, "删除后大小正确");
    
    // 验证删除后无法找到
    TestEntry* should_not_find = (TestEntry*)skiplist_find(skiplist, entry2->key);
    TEST_ASSERT(should_not_find == NULL, "删除后无法找到元素");
    
    // 清理
    free(entry1->value);
    free(entry1);
    free(entry2->value);
    free(entry2);
    free(entry3->value);
    free(entry3);
    skiplist_destroy(skiplist);
}

// 测试范围查询
void test_range_queries() {
    printf("\n=== 测试范围查询 ===\n");
    
    skiplist_t* skiplist = skiplist_create();
    
    // 插入测试数据
    TestEntry* entries[10];
    for (int i = 0; i < 10; i++) {
        entries[i] = malloc(sizeof(TestEntry));
        entries[i]->key = i * 10;  // 0, 10, 20, ..., 90
        entries[i]->value = malloc(20);
        sprintf(entries[i]->value, "value%d", i * 10);
        skiplist_insert(skiplist, entries[i]->key, entries[i]);
    }
    
    TEST_ASSERT(skiplist->size == 10, "插入10个元素");
    
    // 测试范围查询 [20, 60]
    uint64_t start_key = 20;
    uint64_t end_key = 60;
    
    int count = 0;
    skiplist_range_query(skiplist, start_key, end_key, false, range_query_callback, &count);
    
    TEST_ASSERT(count == 5, "范围查询返回5个元素");  // 20, 30, 40, 50, 60
    
    // 测试空范围查询
    uint64_t empty_start = 100;
    uint64_t empty_end = 200;
    int empty_count = 0;
    skiplist_range_query(skiplist, empty_start, empty_end, false, range_query_callback, &empty_count);
    
    TEST_ASSERT(empty_count == 0, "空范围查询返回0个元素");
    
    // 清理
    for (int i = 0; i < 10; i++) {
        free(entries[i]->value);
        free(entries[i]);
    }
    skiplist_destroy(skiplist);
}

// 测试迭代器
void test_iterators() {
    printf("\n=== 测试迭代器 ===\n");
    
    skiplist_t* skiplist = skiplist_create();
    
    // 插入测试数据
    TestEntry* entries[5];
    for (int i = 0; i < 5; i++) {
        entries[i] = malloc(sizeof(TestEntry));
        entries[i]->key = i * 10;  // 0, 10, 20, 30, 40
        entries[i]->value = malloc(20);
        sprintf(entries[i]->value, "value%d", i * 10);
        skiplist_insert(skiplist, entries[i]->key, entries[i]);
    }
    
    // 测试正向迭代
    int forward_count = 0;
    skiplist_range_query(skiplist, 0, 100, false, range_query_callback, &forward_count);
    
    TEST_ASSERT(forward_count == 5, "正向迭代返回5个元素");
    
    // 测试反向迭代
    int reverse_count = 0;
    skiplist_range_query(skiplist, 0, 100, true, range_query_callback, &reverse_count);
    
    TEST_ASSERT(reverse_count == 5, "反向迭代返回5个元素");
    
    // 清理
    for (int i = 0; i < 5; i++) {
        free(entries[i]->value);
        free(entries[i]);
    }
    skiplist_destroy(skiplist);
}

// 测试内存管理
void test_memory_management() {
    printf("\n=== 测试内存管理 ===\n");
    
    skiplist_t* skiplist = skiplist_create();
    
    size_t initial_memory = skiplist_get_memory_usage(skiplist);
    TEST_ASSERT(initial_memory > 0, "初始内存使用量大于0");
    
    // 插入大量元素
    TestEntry* entries[100];
    for (int i = 0; i < 100; i++) {
        entries[i] = malloc(sizeof(TestEntry));
        entries[i]->key = i;
        entries[i]->value = malloc(20);
        sprintf(entries[i]->value, "value%d", i);
        skiplist_insert(skiplist, entries[i]->key, entries[i]);
    }
    
    size_t after_insert_memory = skiplist_get_memory_usage(skiplist);
    TEST_ASSERT(after_insert_memory > initial_memory, "插入后内存使用量增加");
    
    // 删除所有元素
    for (int i = 0; i < 100; i++) {
        skiplist_remove(skiplist, entries[i]->key);
        free(entries[i]->value);
        free(entries[i]);
    }
    
    TEST_ASSERT(skiplist->size == 0, "删除所有元素后为空");
    
    skiplist_destroy(skiplist);
}

// 并发插入参数
typedef struct {
    skiplist_t* skiplist;
    uint64_t base_key;
} ConcurrentArgs;

// 并发插入测试函数（使用不重叠键区间，避免覆盖导致的旧值泄漏）
void* test_concurrent_insert(void* arg) {
    ConcurrentArgs* args = (ConcurrentArgs*)arg;
    skiplist_t* skiplist = args->skiplist;
    uint64_t base = args->base_key;
    
    for (int i = 0; i < 100; i++) {
        TestEntry* entry = malloc(sizeof(TestEntry));
        entry->key = base + (uint64_t)i;
        entry->value = malloc(20);
        sprintf(entry->value, "value%lu", (unsigned long)entry->key);
        pthread_mutex_lock(&g_skiplist_mutex);
        skiplist_insert(skiplist, entry->key, entry);
        pthread_mutex_unlock(&g_skiplist_mutex);
    }
    
    return NULL;
}

// 测试并发性
void test_concurrency() {
    printf("\n=== 测试并发性 ===\n");
    
    skiplist_t* skiplist = skiplist_create();
    
    // 创建多个线程进行并发插入（分配不重叠键区间）
    pthread_t threads[4];
    ConcurrentArgs args[4];
    for (int i = 0; i < 4; i++) {
        args[i].skiplist = skiplist;
        args[i].base_key = (uint64_t)i * 1000ULL; // 0,1000,2000,3000
        pthread_create(&threads[i], NULL, test_concurrent_insert, &args[i]);
    }
    
    // 等待所有线程完成
    for (int i = 0; i < 4; i++) {
        pthread_join(threads[i], NULL);
    }
    
    // 验证所有元素都被插入（检查每个分区的首元素）
    for (int i = 0; i < 4; i++) {
        uint64_t key = (uint64_t)i * 1000ULL;
        TestEntry* found = (TestEntry*)skiplist_find(skiplist, key);
        TEST_ASSERT(found != NULL, "并发插入的元素存在");
    }

    // 释放所有节点的值，避免内存泄漏
    int free_count = 0;
    skiplist_range_query(skiplist, 0, UINT64_MAX, false, free_entry_callback, &free_count);
    
    skiplist_destroy(skiplist);
}

// 测试边界情况
void test_edge_cases() {
    printf("\n=== 测试边界情况 ===\n");
    
    skiplist_t* skiplist = skiplist_create();
    
    // 测试插入NULL值
    skiplist_insert(skiplist, 1, NULL);
    TEST_ASSERT(skiplist->size == 1, "插入NULL值成功");
    
    // 测试查找不存在的键
    TestEntry* null_find = (TestEntry*)skiplist_find(skiplist, 999);
    TEST_ASSERT(null_find == NULL, "查找不存在的键返回NULL");
    
    // 测试删除不存在的键
    uint64_t not_exist_key = 999;
    skiplist_remove(skiplist, not_exist_key);
    TEST_ASSERT(skiplist->size == 1, "删除不存在的键不影响大小");
    
    // 测试空跳表的范围查询
    int empty_count = 0;
    skiplist_range_query(skiplist, 0, 100, false, range_query_callback, &empty_count);
    
    TEST_ASSERT(empty_count == 1, "空跳表范围查询返回1个元素（NULL值）");
    
    skiplist_destroy(skiplist);
}

int main() {
    printf("开始跳表测试...\n");
    
    test_basic_operations();
    test_range_queries();
    test_iterators();
    test_memory_management();
    test_concurrency();
    test_edge_cases();
    
    printf("\n=== 测试结果 ===\n");
    
    return (failed_tests == 0) ? 0 : 1;
} 