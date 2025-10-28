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

#include "ring_buffer.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <stdio.h> // Added for printf

SRingBuffer* ring_buffer_init(uint32_t capacity) {
    if (capacity == 0) {
        return NULL;
    }
    
    SRingBuffer* ring_buffer = (SRingBuffer*)malloc(sizeof(SRingBuffer));
    if (!ring_buffer) {
        return NULL;
    }
    
    ring_buffer->buffer = (void**)malloc(sizeof(void*) * capacity);
    if (!ring_buffer->buffer) {
        free(ring_buffer);
        return NULL;
    }
    
    ring_buffer->capacity = capacity;
    ring_buffer->size = 0;
    ring_buffer->head = 0;
    ring_buffer->tail = 0;
    ring_buffer->enqueue_count = 0;
    ring_buffer->dequeue_count = 0;
    ring_buffer->overflow_count = 0;
    
    // 初始化互斥锁和条件变量
    if (pthread_mutex_init(&ring_buffer->mutex, NULL) != 0) {
        free(ring_buffer->buffer);
        free(ring_buffer);
        return NULL;
    }
    
    if (pthread_cond_init(&ring_buffer->not_empty, NULL) != 0) {
        pthread_mutex_destroy(&ring_buffer->mutex);
        free(ring_buffer->buffer);
        free(ring_buffer);
        return NULL;
    }
    
    if (pthread_cond_init(&ring_buffer->not_full, NULL) != 0) {
        pthread_cond_destroy(&ring_buffer->not_empty);
        pthread_mutex_destroy(&ring_buffer->mutex);
        free(ring_buffer->buffer);
        free(ring_buffer);
        return NULL;
    }
    
    return ring_buffer;
}

void ring_buffer_destroy(SRingBuffer* ring_buffer) {
    if (!ring_buffer) {
        return;
    }
    
    pthread_cond_destroy(&ring_buffer->not_full);
    pthread_cond_destroy(&ring_buffer->not_empty);
    pthread_mutex_destroy(&ring_buffer->mutex);
    
    free(ring_buffer->buffer);
    free(ring_buffer);
}

int32_t ring_buffer_enqueue(SRingBuffer* ring_buffer, void* item) {
    if (!ring_buffer || !item) {
        return -1;
    }
    
    pthread_mutex_lock(&ring_buffer->mutex);
    
    if (ring_buffer->size >= ring_buffer->capacity) {
        ring_buffer->overflow_count++;
        pthread_mutex_unlock(&ring_buffer->mutex);
        return -1; // 队列已满
    }
    
    ring_buffer->buffer[ring_buffer->tail] = item;
    ring_buffer->tail = (ring_buffer->tail + 1) % ring_buffer->capacity;
    ring_buffer->size++;
    ring_buffer->enqueue_count++;
    
    // 通知等待的消费者
    pthread_cond_signal(&ring_buffer->not_empty);
    
    pthread_mutex_unlock(&ring_buffer->mutex);
    return 0;
}

int32_t ring_buffer_enqueue_blocking(SRingBuffer* ring_buffer, void* item, uint32_t timeout_ms) {
    if (!ring_buffer || !item) {
        return -1;
    }
    
    pthread_mutex_lock(&ring_buffer->mutex);
    int success = 0;
    // 等待队列有空间
    while (ring_buffer->size >= ring_buffer->capacity) {
        if (timeout_ms == 0) {
            // 无限等待
            pthread_cond_wait(&ring_buffer->not_full, &ring_buffer->mutex);
        } else {
            // 有限等待
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_nsec += (timeout_ms % 1000) * 1000000;
            ts.tv_sec += timeout_ms / 1000 + ts.tv_nsec / 1000000000;
            ts.tv_nsec %= 1000000000;
            int ret = pthread_cond_timedwait(&ring_buffer->not_full, &ring_buffer->mutex, &ts);
            if (ret == ETIMEDOUT) {
                ring_buffer->overflow_count++;
                pthread_mutex_unlock(&ring_buffer->mutex);
                return -1; // 超时
            }
        }
    }
    // 只有在有空间时才写入 item
    ring_buffer->buffer[ring_buffer->tail] = item;
    ring_buffer->tail = (ring_buffer->tail + 1) % ring_buffer->capacity;
    ring_buffer->size++;
    ring_buffer->enqueue_count++;
    // 通知等待的消费者
    pthread_cond_signal(&ring_buffer->not_empty);
    pthread_mutex_unlock(&ring_buffer->mutex);
    return 0;
}

int32_t ring_buffer_dequeue(SRingBuffer* ring_buffer, void** item) {
    if (!ring_buffer || !item) {
        return -1;
    }
    
    pthread_mutex_lock(&ring_buffer->mutex);
    
    if (ring_buffer->size == 0) {
        pthread_mutex_unlock(&ring_buffer->mutex);
        return -1; // 队列为空
    }
    
    *item = ring_buffer->buffer[ring_buffer->head];
    ring_buffer->buffer[ring_buffer->head] = NULL;
    ring_buffer->head = (ring_buffer->head + 1) % ring_buffer->capacity;
    ring_buffer->size--;
    ring_buffer->dequeue_count++;
    
    // 通知等待的生产者
    pthread_cond_signal(&ring_buffer->not_full);
    
    pthread_mutex_unlock(&ring_buffer->mutex);
    return 0;
}

int32_t ring_buffer_dequeue_blocking(SRingBuffer* ring_buffer, void** item, uint32_t timeout_ms) {
    if (!ring_buffer || !item) {
        return -1;
    }
    
    pthread_mutex_lock(&ring_buffer->mutex);
    
    // 等待队列有数据
    while (ring_buffer->size == 0) {
        if (timeout_ms == 0) {
            // 无限等待
            pthread_cond_wait(&ring_buffer->not_empty, &ring_buffer->mutex);
        } else {
            // 有限等待
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_nsec += (timeout_ms % 1000) * 1000000;
            ts.tv_sec += timeout_ms / 1000 + ts.tv_nsec / 1000000000;
            ts.tv_nsec %= 1000000000;
            
            int ret = pthread_cond_timedwait(&ring_buffer->not_empty, &ring_buffer->mutex, &ts);
            if (ret == ETIMEDOUT) {
                pthread_mutex_unlock(&ring_buffer->mutex);
                return -1; // 超时
            }
        }
    }
    
    *item = ring_buffer->buffer[ring_buffer->head];
    ring_buffer->buffer[ring_buffer->head] = NULL;
    ring_buffer->head = (ring_buffer->head + 1) % ring_buffer->capacity;
    ring_buffer->size--;
    ring_buffer->dequeue_count++;
    
    // 通知等待的生产者
    pthread_cond_signal(&ring_buffer->not_full);
    
    pthread_mutex_unlock(&ring_buffer->mutex);
    return 0;
}

int32_t ring_buffer_peek(SRingBuffer* ring_buffer, void** item) {
    if (!ring_buffer || !item) {
        return -1;
    }
    
    pthread_mutex_lock(&ring_buffer->mutex);
    
    if (ring_buffer->size == 0) {
        pthread_mutex_unlock(&ring_buffer->mutex);
        return -1; // 队列为空
    }
    
    *item = ring_buffer->buffer[ring_buffer->head];
    
    pthread_mutex_unlock(&ring_buffer->mutex);
    return 0;
}

void ring_buffer_clear(SRingBuffer* ring_buffer, void (*free_func)(void*)) {
    if (!ring_buffer) {
        return;
    }
    
    pthread_mutex_lock(&ring_buffer->mutex);
    
    if (free_func) {
        uint32_t i = ring_buffer->head;
        uint32_t count = ring_buffer->size;
        while (count > 0) {
            if (ring_buffer->buffer[i]) {
                free_func(ring_buffer->buffer[i]);
                ring_buffer->buffer[i] = NULL;
            }
            i = (i + 1) % ring_buffer->capacity;
            count--;
        }
    }
    
    ring_buffer->size = 0;
    ring_buffer->head = 0;
    ring_buffer->tail = 0;
    
    // 通知所有等待的线程
    pthread_cond_broadcast(&ring_buffer->not_full);
    
    pthread_mutex_unlock(&ring_buffer->mutex);
}

ERingBufferState ring_buffer_get_state(SRingBuffer* ring_buffer) {
    if (!ring_buffer) {
        return RING_BUFFER_EMPTY;
    }
    
    pthread_mutex_lock(&ring_buffer->mutex);
    ERingBufferState state;
    
    if (ring_buffer->size == 0) {
        state = RING_BUFFER_EMPTY;
    } else if (ring_buffer->size >= ring_buffer->capacity) {
        state = RING_BUFFER_FULL;
    } else {
        state = RING_BUFFER_PARTIAL;
    }
    
    pthread_mutex_unlock(&ring_buffer->mutex);
    return state;
}

uint32_t ring_buffer_get_size(SRingBuffer* ring_buffer) {
    if (!ring_buffer) {
        return 0;
    }
    
    pthread_mutex_lock(&ring_buffer->mutex);
    uint32_t size = ring_buffer->size;
    pthread_mutex_unlock(&ring_buffer->mutex);
    
    return size;
}

uint32_t ring_buffer_get_capacity(SRingBuffer* ring_buffer) {
    return ring_buffer ? ring_buffer->capacity : 0;
}

bool ring_buffer_is_empty(SRingBuffer* ring_buffer) {
    return ring_buffer_get_size(ring_buffer) == 0;
}

bool ring_buffer_is_full(SRingBuffer* ring_buffer) {
    if (!ring_buffer) {
        return true;
    }
    
    pthread_mutex_lock(&ring_buffer->mutex);
    bool is_full = (ring_buffer->size >= ring_buffer->capacity);
    pthread_mutex_unlock(&ring_buffer->mutex);
    
    return is_full;
}

void ring_buffer_get_stats(SRingBuffer* ring_buffer, uint64_t* enqueue_count,
                          uint64_t* dequeue_count, uint64_t* overflow_count) {
    if (!ring_buffer) {
        if (enqueue_count) *enqueue_count = 0;
        if (dequeue_count) *dequeue_count = 0;
        if (overflow_count) *overflow_count = 0;
        return;
    }
    
    pthread_mutex_lock(&ring_buffer->mutex);
    
    if (enqueue_count) *enqueue_count = ring_buffer->enqueue_count;
    if (dequeue_count) *dequeue_count = ring_buffer->dequeue_count;
    if (overflow_count) *overflow_count = ring_buffer->overflow_count;
    
    pthread_mutex_unlock(&ring_buffer->mutex);
} 