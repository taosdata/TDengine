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
#include <time.h>

typedef enum {
    CONN_EMPTY = 0,    // slot is empty
    CONN_IDLE,         // connection ready to use
    CONN_BUSY,         // connection in use
    CONN_CONNECTING    // connection being established
} ConnState;

typedef struct {
    TAOS **pool;
    ConnState *state;
    int size;
    int count;
    pthread_mutex_t mutex;
    pthread_cond_t  cond;
} ConnectionPool;

static ConnectionPool g_pool = {0};

int initConnectionPool(int poolSize) {
    if (poolSize <= 0) return -1;

    g_pool.pool = (TAOS **)taosMemoryCalloc(poolSize, sizeof(TAOS *));
    g_pool.state = (ConnState *)taosMemoryCalloc(poolSize, sizeof(ConnState));
    if (!g_pool.pool || !g_pool.state) {
        taosMemoryFree(g_pool.pool);
        taosMemoryFree(g_pool.state);
        return -1;
    }

    g_pool.size = poolSize;
    g_pool.count = 0;
    pthread_mutex_init(&g_pool.mutex, NULL);
    pthread_cond_init(&g_pool.cond, NULL);

    return 0;
}

void destroyConnectionPool() {
    pthread_mutex_lock(&g_pool.mutex);

    for (int i = 0; i < g_pool.count; i++) {
        if (g_pool.pool[i]) {
            taos_close(g_pool.pool[i]);
        }
    }

    taosMemoryFree(g_pool.pool);
    taosMemoryFree(g_pool.state);
    g_pool.pool = NULL;
    g_pool.state = NULL;
    g_pool.size = 0;
    g_pool.count = 0;

    pthread_mutex_unlock(&g_pool.mutex);
    pthread_cond_destroy(&g_pool.cond);
    pthread_mutex_destroy(&g_pool.mutex);
}

// Helper: reserve a slot and mark it CONNECTING (must hold lock)
// Returns slot index or -1 if pool is full
static int reserveSlot() {
    if (g_pool.count < g_pool.size) {
        int idx = g_pool.count++;
        g_pool.state[idx] = CONN_CONNECTING;
        g_pool.pool[idx] = NULL;
        return idx;
    }
    return -1;
}

// Helper: commit a successful connection (must hold lock)
static void commitSlot(int idx, TAOS *conn) {
    g_pool.pool[idx] = conn;
    g_pool.state[idx] = CONN_BUSY;
}

// Helper: rollback a failed connection attempt (must hold lock)
static void rollbackSlot(int idx) {
    for (int j = idx; j < g_pool.count - 1; j++) {
        g_pool.pool[j] = g_pool.pool[j + 1];
        g_pool.state[j] = g_pool.state[j + 1];
    }
    g_pool.count--;
}

// Exponential back-off parameters for reconnection when the pool is empty
// (i.e. the server is temporarily unreachable).
// Industry convention (similar to PostgreSQL libpq / MongoDB driver):
//   initial wait 1 s → doubles each attempt → capped at 30 s.
#define BCK_RECONNECT_INIT_MS   1000   // first wait: 1 s
#define BCK_RECONNECT_MAX_MS   30000   // ceiling:   30 s

TAOS* getConnection(int *code) {
    pthread_mutex_lock(&g_pool.mutex);

    // Back-off state for when the pool is empty (server down).
    // Reset to initial value each time getConnection() is entered so that
    // a successful call never carries stale back-off state to the next call.
    int reconnectWaitMs = BCK_RECONNECT_INIT_MS;

    while (1) {
        // check if interrupted
        if (g_interrupted) {
            pthread_mutex_unlock(&g_pool.mutex);
            *code = TSDB_CODE_BCK_USER_CANCEL;
            return NULL;
        }

        // First, look for an IDLE connection
        for (int i = 0; i < g_pool.count; i++) {
            if (g_pool.state[i] == CONN_IDLE && g_pool.pool[i]) {
                g_pool.state[i] = CONN_BUSY;
                pthread_mutex_unlock(&g_pool.mutex);
                *code = TSDB_CODE_SUCCESS;
                return g_pool.pool[i];
            }
        }

        // pool not full: try to create a new connection
        if (g_pool.count < g_pool.size) {
            // check interrupt before blocking in taos_connect
            if (g_interrupted) {
                pthread_mutex_unlock(&g_pool.mutex);
                *code = TSDB_CODE_BCK_USER_CANCEL;
                return NULL;
            }

            // Reserve a slot using helper
            int idx = reserveSlot();

            // Unlock while connecting (slow operation)
            pthread_mutex_unlock(&g_pool.mutex);

            TAOS *conn = taos_connect(argHost(), argUser(), argPassword(), NULL, argPort());

            // Re-lock to commit or rollback
            pthread_mutex_lock(&g_pool.mutex);

            if (conn) {
                // successfully created new connection — reset back-off
                reconnectWaitMs = BCK_RECONNECT_INIT_MS;
                commitSlot(idx, conn);
                pthread_mutex_unlock(&g_pool.mutex);
                *code = TSDB_CODE_SUCCESS;
                return conn;
            }

            // connection failed - rollback the slot
            rollbackSlot(idx);

            int    errCode = taos_errno(NULL);
            const char *errStr  = taos_errstr(NULL);
            if (g_pool.count == 0) {
                // No existing connections — server is unreachable.
                // Apply exponential back-off and retry instead of giving up
                // immediately, so transient restarts are handled transparently.
                logWarn("connect to %s:%d failed (0x%08X): %s — retry in %d ms",
                        argHost(), argPort(), errCode, errStr, reconnectWaitMs);

                // Unlock, sleep, re-lock, then retry the outer loop
                pthread_mutex_unlock(&g_pool.mutex);

                // Sleep in small slices so we can honour g_interrupted
                int slept = 0;
                int sliceMs = 200;
                while (slept < reconnectWaitMs) {
                    if (g_interrupted) break;
                    int thisSlice = (reconnectWaitMs - slept < sliceMs)
                                    ? (reconnectWaitMs - slept) : sliceMs;
                    taosMsleep(thisSlice);
                    slept += thisSlice;
                }

                pthread_mutex_lock(&g_pool.mutex);

                // Advance back-off (double, cap at max)
                reconnectWaitMs *= 2;
                if (reconnectWaitMs > BCK_RECONNECT_MAX_MS)
                    reconnectWaitMs = BCK_RECONNECT_MAX_MS;

                continue;  // retry from the top of the while(1) loop
            }
            // some connections already exist; just couldn't expand — wait for idle
            logWarn("failed to expand connection pool (0x%08X): %s, waiting for idle ...", errCode, errStr);
        }

        // all connections busy, wait for release
        {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_nsec += 500000000L;  // +500ms
            if (ts.tv_nsec >= 1000000000L) {
                ts.tv_sec  += 1;
                ts.tv_nsec -= 1000000000L;
            }
            pthread_cond_timedwait(&g_pool.cond, &g_pool.mutex, &ts);
        }
    }
}

TAOS* createConnection() {
    pthread_mutex_lock(&g_pool.mutex);

    // Try to reserve a slot
    int idx = reserveSlot();

    if (idx >= 0) {
        // Reserved a slot, unlock and connect
        pthread_mutex_unlock(&g_pool.mutex);

        TAOS *conn = taos_connect(argHost(), argUser(), argPassword(), NULL, argPort());

        pthread_mutex_lock(&g_pool.mutex);
        if (conn) {
            commitSlot(idx, conn);
            pthread_mutex_unlock(&g_pool.mutex);
            return conn;
        } else {
            rollbackSlot(idx);
            pthread_mutex_unlock(&g_pool.mutex);
            return NULL;
        }
    }

    // Pool is full, need to replace an existing connection
    pthread_mutex_unlock(&g_pool.mutex);

    TAOS *conn = taos_connect(argHost(), argUser(), argPassword(), NULL, argPort());
    if (!conn) {
        return NULL;
    }

    pthread_mutex_lock(&g_pool.mutex);

    // Replace idle connection
    for (int i = 0; i < g_pool.size; i++) {
        if (g_pool.state[i] == CONN_IDLE) {
            if (g_pool.pool[i]) {
                taos_close(g_pool.pool[i]);
            }
            g_pool.pool[i] = conn;
            g_pool.state[i] = CONN_BUSY;
            pthread_mutex_unlock(&g_pool.mutex);
            return conn;
        }
    }

    // Replace first connection
    if (g_pool.pool[0]) {
        taos_close(g_pool.pool[0]);
    }
    g_pool.pool[0] = conn;
    g_pool.state[0] = CONN_BUSY;

    pthread_mutex_unlock(&g_pool.mutex);
    return conn;
}

void releaseConnection(TAOS* conn) {
    if (!conn) return;

    pthread_mutex_lock(&g_pool.mutex);

    for (int i = 0; i < g_pool.count; i++) {
        if (g_pool.pool[i] == conn) {
            g_pool.state[i] = CONN_IDLE;
            pthread_cond_signal(&g_pool.cond);
            break;
        }
    }

    pthread_mutex_unlock(&g_pool.mutex);
}

// Mark a connection as broken: close it and evict it from the pool so the next
// getConnection() call creates a fresh one instead of handing out this stale handle.
void releaseConnectionBad(TAOS* conn) {
    if (!conn) return;

    pthread_mutex_lock(&g_pool.mutex);

    for (int i = 0; i < g_pool.count; i++) {
        if (g_pool.pool[i] == conn) {
            taos_close(conn);
            // shift remaining entries down to fill the gap
            for (int j = i; j < g_pool.count - 1; j++) {
                g_pool.pool[j] = g_pool.pool[j + 1];
                g_pool.state[j] = g_pool.state[j + 1];
            }
            g_pool.pool[g_pool.count - 1] = NULL;
            g_pool.state[g_pool.count - 1] = CONN_EMPTY;
            g_pool.count--;
            pthread_cond_signal(&g_pool.cond);
            break;
        }
    }

    pthread_mutex_unlock(&g_pool.mutex);
}

