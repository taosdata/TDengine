/*
 * Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 */
    
#include "bckArgs.h"
#include "bckLog.h"
#include "bckDb.h"
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>

typedef struct {
    TAOS **pool;
    int *used;
    int size;
    int count;
    pthread_mutex_t mutex;
} ConnectionPool;

static ConnectionPool g_pool = {0};

int initConnectionPool(int poolSize) {
    if (poolSize <= 0) return -1;
    
    g_pool.pool = (TAOS **)calloc(poolSize, sizeof(TAOS *));
    g_pool.used = (int *)calloc(poolSize, sizeof(int));
    if (!g_pool.pool || !g_pool.used) {
        free(g_pool.pool);
        free(g_pool.used);
        return -1;
    }
    
    g_pool.size = poolSize;
    g_pool.count = 0;
    pthread_mutex_init(&g_pool.mutex, NULL);
    
    return 0;
}

void destroyConnectionPool() {
    pthread_mutex_lock(&g_pool.mutex);
    
    for (int i = 0; i < g_pool.count; i++) {
        if (g_pool.pool[i]) {
            taos_close(g_pool.pool[i]);
        }
    }
    
    free(g_pool.pool);
    free(g_pool.used);
    g_pool.pool = NULL;
    g_pool.used = NULL;
    g_pool.size = 0;
    g_pool.count = 0;
    
    pthread_mutex_unlock(&g_pool.mutex);
    pthread_mutex_destroy(&g_pool.mutex);
}

TAOS* getConnection() {
    pthread_mutex_lock(&g_pool.mutex);
    
    // find idle connection
    for (int i = 0; i < g_pool.count; i++) {
        if (!g_pool.used[i] && g_pool.pool[i]) {
            g_pool.used[i] = 1;
            pthread_mutex_unlock(&g_pool.mutex);
            return g_pool.pool[i];
        }
    }
    
    // pool not full
    if (g_pool.count < g_pool.size) {
        TAOS *conn = taos_connect(argHost(), argUser(), argPassword(), NULL, argPort());
        if (conn) {
            g_pool.pool[g_pool.count] = conn;
            g_pool.used[g_pool.count] = 1;
            g_pool.count++;
            pthread_mutex_unlock(&g_pool.mutex);
            return conn;
        }
    }
    
    pthread_mutex_unlock(&g_pool.mutex);
    return NULL;
}

TAOS* createConnection() {
    pthread_mutex_lock(&g_pool.mutex);
    
    TAOS *conn = taos_connect(argHost(), argUser(), argPassword(), NULL, argPort());
    if (!conn) {
        pthread_mutex_unlock(&g_pool.mutex);
        return NULL;
    }
    
    // pool not full
    if (g_pool.count < g_pool.size) {
        g_pool.pool[g_pool.count] = conn;
        g_pool.used[g_pool.count] = 1;
        g_pool.count++;
        pthread_mutex_unlock(&g_pool.mutex);
        return conn;
    }
    
    // replace idle connection
    for (int i = 0; i < g_pool.size; i++) {
        if (!g_pool.used[i]) {
            if (g_pool.pool[i]) {
                taos_close(g_pool.pool[i]);
            }
            g_pool.pool[i] = conn;
            g_pool.used[i] = 1;
            pthread_mutex_unlock(&g_pool.mutex);
            return conn;
        }
    }
    
    // replace first connection
    if (g_pool.pool[0]) {
        taos_close(g_pool.pool[0]);
    }
    g_pool.pool[0] = conn;
    g_pool.used[0] = 1;
    
    pthread_mutex_unlock(&g_pool.mutex);
    return conn;
}

void releaseConnection(TAOS* conn) {
    if (!conn) return;
    
    pthread_mutex_lock(&g_pool.mutex);
    
    for (int i = 0; i < g_pool.count; i++) {
        if (g_pool.pool[i] == conn) {
            g_pool.used[i] = 0;
            break;
        }
    }
    
    pthread_mutex_unlock(&g_pool.mutex);
}

