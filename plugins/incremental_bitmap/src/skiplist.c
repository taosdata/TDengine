#include "../include/skiplist.h"
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <assert.h>

static int random_level() {
    int level = 1;
    while (((double)rand() / RAND_MAX) < SKIPLIST_P && level < SKIPLIST_MAX_LEVEL)
        level++;
    return level;
}

// 内存池分配/回收
static skiplist_node_t* node_pool_alloc(skiplist_t* sl) {
    if (sl->free_nodes) {
        skiplist_node_t* node = sl->free_nodes;
        sl->free_nodes = node->forward[0];
        sl->free_count--;
        return node;
    }
    return (skiplist_node_t*)calloc(1, sizeof(skiplist_node_t));
}

static void node_pool_free(skiplist_t* sl, skiplist_node_t* node) {
    node->forward[0] = sl->free_nodes;
    sl->free_nodes = node;
    sl->free_count++;
}

void skiplist_node_pool_clear(skiplist_t* sl) {
    skiplist_node_t* node = sl->free_nodes;
    while (node) {
        skiplist_node_t* next = node->forward[0];
        free(node);
        node = next;
    }
    sl->free_nodes = NULL;
    sl->free_count = 0;
}

skiplist_t* skiplist_create() {
    skiplist_t* sl = (skiplist_t*)calloc(1, sizeof(skiplist_t));
    sl->level = 1;
    sl->size = 0;
    sl->header = (skiplist_node_t*)calloc(1, sizeof(skiplist_node_t));
    sl->tail = NULL;
    pthread_rwlock_init(&sl->rwlock, NULL);
    sl->free_nodes = NULL;
    sl->free_count = 0;
    return sl;
}

void skiplist_destroy(skiplist_t* sl) {
    if (!sl) return;
    skiplist_node_t* node = sl->header->forward[0];
    while (node) {
        skiplist_node_t* next = node->forward[0];
        // 注意：这里不释放value，由调用者负责
        free(node);
        node = next;
    }
    free(sl->header);
    skiplist_node_pool_clear(sl);
    pthread_rwlock_destroy(&sl->rwlock);
    free(sl);
}

void skiplist_insert(skiplist_t* sl, uint64_t key, void* value) {
    pthread_rwlock_wrlock(&sl->rwlock);
    skiplist_node_t* update[SKIPLIST_MAX_LEVEL];
    skiplist_node_t* x = sl->header;
    for (int i = sl->level - 1; i >= 0; i--) {
        while (x->forward[i] && x->forward[i]->key < key) {
            x = x->forward[i];
        }
        update[i] = x;
    }
    x = x->forward[0];
    if (x && x->key == key) {
        // 注意：这里不释放旧value，由调用者负责
        x->value = value;
        pthread_rwlock_unlock(&sl->rwlock);
        return;
    }
    int lvl = random_level();
    if (lvl > sl->level) {
        for (int i = sl->level; i < lvl; i++) {
            update[i] = sl->header;
        }
        sl->level = lvl;
    }
    x = node_pool_alloc(sl);
    x->key = key;
    x->value = value;
    x->level = lvl;
    for (int i = 0; i < lvl; i++) {
        x->forward[i] = update[i]->forward[i];
        update[i]->forward[i] = x;
    }
    // backward指针
    x->backward = (update[0] == sl->header) ? NULL : update[0];
    if (x->forward[0]) x->forward[0]->backward = x;
    if (!x->forward[0]) sl->tail = x;
    sl->size++;
    pthread_rwlock_unlock(&sl->rwlock);
}

void* skiplist_find(skiplist_t* sl, uint64_t key) {
    pthread_rwlock_rdlock(&sl->rwlock);
    skiplist_node_t* x = sl->header;
    for (int i = sl->level - 1; i >= 0; i--) {
        while (x->forward[i] && x->forward[i]->key < key) {
            x = x->forward[i];
        }
    }
    x = x->forward[0];
    void* result = NULL;
    if (x && x->key == key) {
        result = x->value;
    }
    pthread_rwlock_unlock(&sl->rwlock);
    return result;
}

void skiplist_remove(skiplist_t* sl, uint64_t key) {
    pthread_rwlock_wrlock(&sl->rwlock);
    skiplist_node_t* update[SKIPLIST_MAX_LEVEL];
    skiplist_node_t* x = sl->header;
    for (int i = sl->level - 1; i >= 0; i--) {
        while (x->forward[i] && x->forward[i]->key < key) {
            x = x->forward[i];
        }
        update[i] = x;
    }
    x = x->forward[0];
    if (x && x->key == key) {
        for (int i = 0; i < sl->level; i++) {
            if (update[i]->forward[i] != x) break;
            update[i]->forward[i] = x->forward[i];
        }
        if (x->forward[0]) x->forward[0]->backward = x->backward;
        if (sl->tail == x) sl->tail = x->backward;
        // 注意：这里不释放value，由调用者负责
        node_pool_free(sl, x);
        while (sl->level > 1 && sl->header->forward[sl->level - 1] == NULL) {
            sl->level--;
        }
        sl->size--;
    }
    pthread_rwlock_unlock(&sl->rwlock);
}

void skiplist_range_query(skiplist_t* sl, uint64_t start, uint64_t end, bool reverse, skiplist_range_cb cb, void* user_data) {
    pthread_rwlock_rdlock(&sl->rwlock);
    if (!reverse) {
        skiplist_node_t* x = sl->header;
        for (int i = sl->level - 1; i >= 0; i--) {
            while (x->forward[i] && x->forward[i]->key < start) {
                x = x->forward[i];
            }
        }
        x = x->forward[0];
        while (x && x->key <= end) {
            cb(x->key, x->value, user_data);
            x = x->forward[0];
        }
    } else {
        skiplist_node_t* x = sl->tail;
        while (x && x->key >= start) {
            if (x->key <= end) cb(x->key, x->value, user_data);
            if (x->key == start) break;
            x = x->backward;
        }
    }
    pthread_rwlock_unlock(&sl->rwlock);
}

void skiplist_rdlock(skiplist_t* sl) {
    pthread_rwlock_rdlock(&sl->rwlock);
}
void skiplist_wrlock(skiplist_t* sl) {
    pthread_rwlock_wrlock(&sl->rwlock);
}
void skiplist_unlock(skiplist_t* sl) {
    pthread_rwlock_unlock(&sl->rwlock);
}

// 内存使用统计
uint64_t skiplist_get_memory_usage(skiplist_t* sl) {
    if (!sl) return 0;
    uint64_t total_bytes = 0;
    total_bytes += sizeof(skiplist_t);
    total_bytes += sizeof(skiplist_node_t);
    skiplist_node_t* node = sl->header->forward[0];
    while (node) {
        total_bytes += sizeof(skiplist_node_t);
        // 位图内存大小（抽象接口）
        if (node->value && sl->value_mem_usage) {
            total_bytes += sl->value_mem_usage(node->value);
        }
        node = node->forward[0];
    }
    skiplist_node_t* free_node = sl->free_nodes;
    while (free_node) {
        total_bytes += sizeof(skiplist_node_t);
        free_node = free_node->forward[0];
    }
    return total_bytes;
} 