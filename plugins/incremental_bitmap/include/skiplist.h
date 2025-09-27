#ifndef BITMAP_SKIPLIST_H
#define BITMAP_SKIPLIST_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

#define SKIPLIST_MAX_LEVEL 32
#define SKIPLIST_P 0.5

// 跳表节点
typedef struct skiplist_node {
    uint64_t key; // 可用于时间戳或WAL偏移量
    void* value; // 使用void*以支持任意类型
    struct skiplist_node* forward[SKIPLIST_MAX_LEVEL];
    struct skiplist_node* backward; // 支持双向遍历
    int level;
} skiplist_node_t;

// 跳表结构体
typedef struct skiplist {
    skiplist_node_t* header;
    skiplist_node_t* tail;
    int level;
    int size;
    pthread_rwlock_t rwlock; // 跳表内部并发保护
    // 内存池
    skiplist_node_t* free_nodes;
    int free_count;
    // value内存统计函数指针，由外部实现并传入
    uint64_t (*value_mem_usage)(void* value);
} skiplist_t;

// 跳表API
skiplist_t* skiplist_create();
void skiplist_destroy(skiplist_t* sl);

// 插入/查找/删除
void skiplist_insert(skiplist_t* sl, uint64_t key, void* value);
void* skiplist_find(skiplist_t* sl, uint64_t key);
void skiplist_remove(skiplist_t* sl, uint64_t key);

// 范围查询（升序/降序）
typedef void (*skiplist_range_cb)(uint64_t key, void* value, void* user_data);
void skiplist_range_query(skiplist_t* sl, uint64_t start, uint64_t end, bool reverse, skiplist_range_cb cb, void* user_data);

// 内存池复用
void skiplist_node_pool_clear(skiplist_t* sl);

// 内存使用统计
uint64_t skiplist_get_memory_usage(skiplist_t* sl);

// 线程安全（外部可加锁，也可内部加锁）
void skiplist_rdlock(skiplist_t* sl);
void skiplist_wrlock(skiplist_t* sl);
void skiplist_unlock(skiplist_t* sl);

#ifdef __cplusplus
}
#endif

#endif // BITMAP_SKIPLIST_H 