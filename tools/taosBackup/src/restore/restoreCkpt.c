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

#include "restoreCkpt.h"
#include "storageTaos.h"
#include "storageParquet.h"
#include "parquetBlock.h"
#include "colCompress.h"
#include "blockReader.h"
#include "bckPool.h"
#include "bckDb.h"
#include "bckSchemaChange.h"
#include "decimal.h"
#include "ttypes.h"
#include "osString.h"
#include "bckProgress.h"
#include "bckLog.h"
#include "bckUtil.h"
#include "bckArgs.h"
#include "osFile.h"
#include "osMemPool.h"

#ifndef MAX_PATH_LEN
#define MAX_PATH_LEN 512
#endif

//
// -------------------------------------- RESTORE CHECKPOINT -----------------------------------------
//

// Hash table for O(1) checkpoint lookup
typedef struct CkptHashEntry {
    char *filePath;
    struct CkptHashEntry *next;
} CkptHashEntry;

typedef struct {
    CkptHashEntry **buckets;
    int capacity;
    int count;
} CkptHashTable;

static pthread_mutex_t g_ckptMutex = PTHREAD_MUTEX_INITIALIZER;
static char  g_ckptPath[MAX_PATH_LEN] = "";
static CkptHashTable g_ckptHash = {0};
static TdFilePtr g_ckptFp = NULL;  // keep checkpoint file open
static bool  g_allowWriteCP = true;  // false when output dir is read-only

// Thread-local checkpoint buffer
typedef struct {
    char buf[16384];  // 16KB buffer
    int used;
} CkptBuffer;

static __thread CkptBuffer t_ckptBuf = {0};

static uint32_t hashString(const char *str) {
    uint32_t hash = 5381;
    int c;
    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c;
    }
    return hash;
}

static int nextPowerOf2(int n) {
    if (n <= 0) return 1;
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    return n + 1;
}

static void initCkptHashTable(int estimatedFiles) {
    int capacity = nextPowerOf2(estimatedFiles * 2);
    if (capacity < 256) capacity = 256;

    g_ckptHash.buckets = (CkptHashEntry **)taosMemoryCalloc(capacity, sizeof(CkptHashEntry *));
    if (!g_ckptHash.buckets) {
        logError("failed to allocate checkpoint hash table");
        return;
    }
    g_ckptHash.capacity = capacity;
    g_ckptHash.count = 0;
}

void insertCkptHash(const char *filePath) {
    if (!g_ckptHash.buckets) return;
    uint32_t hash = hashString(filePath);
    int idx = hash % g_ckptHash.capacity;

    CkptHashEntry *entry = (CkptHashEntry *)taosMemoryMalloc(sizeof(CkptHashEntry) + strlen(filePath) + 1);
    if (!entry) return;
    entry->filePath = (char *)(entry + 1);
    strcpy(entry->filePath, filePath);
    entry->next = g_ckptHash.buckets[idx];
    g_ckptHash.buckets[idx] = entry;
    g_ckptHash.count++;
}

static bool lookupCkptHash(const char *filePath) {
    if (!g_ckptHash.buckets) return false;
    uint32_t hash = hashString(filePath);
    int idx = hash % g_ckptHash.capacity;

    for (CkptHashEntry *e = g_ckptHash.buckets[idx]; e; e = e->next) {
        if (strcmp(e->filePath, filePath) == 0) return true;
    }
    return false;
}

static void freeCkptHashTable() {
    if (!g_ckptHash.buckets) return;
    for (int i = 0; i < g_ckptHash.capacity; i++) {
        CkptHashEntry *e = g_ckptHash.buckets[i];
        while (e) {
            CkptHashEntry *next = e->next;
            taosMemoryFree(e);
            e = next;
        }
    }
    taosMemoryFree(g_ckptHash.buckets);
    g_ckptHash.buckets = NULL;
    g_ckptHash.capacity = 0;
    g_ckptHash.count = 0;
}

void loadRestoreCheckpoint(const char *dbName) {
    snprintf(g_ckptPath, sizeof(g_ckptPath), "%s/%s/restore_checkpoint.txt", argOutPath(), dbName);
    g_allowWriteCP = true;

    // free previous
    freeCkptHashTable();
    if (g_ckptFp) {
        taosCloseFile(&g_ckptFp);
        g_ckptFp = NULL;
    }

    // Open checkpoint file for append (keep it open for the entire restore)
    g_ckptFp = taosOpenFile(g_ckptPath, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_APPEND);
    if (!g_ckptFp) {
        logInfo("restore checkpoint disabled: cannot write to %s (read-only directory), restoring without checkpoint", g_ckptPath);
        g_allowWriteCP = false;
        return;
    }

    // Without -C the caller never calls isRestoreDone(), so there is no need
    // to load previous entries into memory.
    if (!argCheckpoint()) return;

    // Read existing checkpoint file to populate hash table
    TdFilePtr rfp = taosOpenFile(g_ckptPath, TD_FILE_READ);
    if (!rfp) {
        // File doesn't exist yet or can't be read - start fresh
        initCkptHashTable(1024);
        return;
    }

    int64_t fileSize = 0;
    if (taosFStatFile(rfp, &fileSize, NULL) != 0 || fileSize <= 0) {
        taosCloseFile(&rfp);
        initCkptHashTable(1024);
        return;
    }

    char *buf = (char *)taosMemoryMalloc(fileSize + 1);
    if (!buf) {
        taosCloseFile(&rfp);
        initCkptHashTable(1024);
        return;
    }

    int64_t readLen = taosReadFile(rfp, buf, fileSize);
    taosCloseFile(&rfp);
    if (readLen <= 0) {
        taosMemoryFree(buf);
        initCkptHashTable(1024);
        return;
    }
    buf[readLen] = '\0';

    // Count lines for hash table sizing
    int lineCount = 0;
    for (int64_t i = 0; i < readLen; i++) {
        if (buf[i] == '\n') lineCount++;
    }

    initCkptHashTable(lineCount);

    // Parse lines and insert into hash table
    char *line = buf;
    while (*line) {
        char *eol = strchr(line, '\n');
        int len;
        if (eol) {
            len = (int)(eol - line);
            if (len > 0 && line[len-1] == '\r') len--;
        } else {
            len = strlen(line);
        }
        if (len > 0) {
            char path[MAX_PATH_LEN];
            if (len < MAX_PATH_LEN) {
                memcpy(path, line, len);
                path[len] = '\0';
                insertCkptHash(path);
            }
        }
        if (!eol) break;
        line = eol + 1;
    }

    taosMemoryFree(buf);
    logInfo("loaded restore checkpoint: %d files already done", g_ckptHash.count);
}

bool isRestoreDone(const char *filePath) {
    return lookupCkptHash(filePath);
}

// Flush thread-local checkpoint buffer to file
void flushCkptBuffer() {
    if (t_ckptBuf.used == 0) return;
    if (!g_allowWriteCP || !g_ckptFp) return;

    pthread_mutex_lock(&g_ckptMutex);
    if (g_ckptFp) {
        taosWriteFile(g_ckptFp, t_ckptBuf.buf, t_ckptBuf.used);
        taosFsyncFile(g_ckptFp);  // ensure data is written
    }
    pthread_mutex_unlock(&g_ckptMutex);

    t_ckptBuf.used = 0;
}

void markRestoreDone(const char *filePath) {
    if (!g_allowWriteCP) return;

    char line[MAX_PATH_LEN + 2];
    int len = snprintf(line, sizeof(line), "%s\n", filePath);

    // If buffer would overflow, flush first
    if (t_ckptBuf.used + len > sizeof(t_ckptBuf.buf)) {
        flushCkptBuffer();
    }

    // Append to thread-local buffer
    if (t_ckptBuf.used + len <= sizeof(t_ckptBuf.buf)) {
        memcpy(t_ckptBuf.buf + t_ckptBuf.used, line, len);
        t_ckptBuf.used += len;
    }
}

void freeRestoreCheckpoint() {
    // Flush any remaining buffered checkpoint data
    flushCkptBuffer();

    // Close the global checkpoint file
    if (g_ckptFp) {
        taosCloseFile(&g_ckptFp);
        g_ckptFp = NULL;
    }

    // Free hash table
    freeCkptHashTable();
}

// Delete the checkpoint file after a fully successful restore so that the
// next restore run always starts fresh instead of skipping everything.
void deleteRestoreCheckpoint() {
    if (g_allowWriteCP && g_ckptPath[0] != '\0') {
        int ret = taosRemoveFile(g_ckptPath);
        logDebug("deleted restore checkpoint: %s ret:%d", g_ckptPath, ret);
    }
}

