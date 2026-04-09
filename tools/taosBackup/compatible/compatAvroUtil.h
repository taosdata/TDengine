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

#ifndef COMPAT_AVRO_UTIL_H_
#define COMPAT_AVRO_UTIL_H_

#include "compatAvroSchema.h"
#include <pthread.h>
#include <taos.h>

// ---- HashMap ----
#define AVRO_HASH_BUCKETS 1024

typedef struct AvroHashEntry {
    char                 *key;
    void                 *value;
    struct AvroHashEntry *next;
} AvroHashEntry;

typedef struct {
    AvroHashEntry  *buckets[AVRO_HASH_BUCKETS];
    pthread_mutex_t lock;
} AvroHashMap;

AvroHashMap *avroHashMapCreate(void);
bool         avroHashMapInsert(AvroHashMap *map, const char *key, void *value);
void        *avroHashMapFind(AvroHashMap *map, const char *key);
void         avroHashMapDestroy(AvroHashMap *map);

// ---- Schema change tracking ----

typedef struct {
    AvroTableDes *tableDes;
    char         *strTags;       // "(`tag1`,`tag2`,...)" or ""
    char         *strCols;       // "(`col1`,`col2`,...)" or ""
    bool          schemaChanged;
} AvroStbChange;

typedef struct {
    int16_t      version;
    AvroHashMap  stbMap;
    const char  *dbPath;
} AvroDBChange;

AvroDBChange  *avroCreateDbChange(const char *dbPath);
void           avroFreeDbChange(AvroDBChange *dc);
void           avroFreeStbChange(AvroStbChange *sc);
int            avroAddStbChanged(AvroDBChange *dc, const char *dbName,
                                 TAOS *taos, AvroRecordSchema *rs,
                                 AvroStbChange **ppStbChange);
AvroStbChange *avroFindStbChange(AvroDBChange *dc, const char *stbName);
bool           avroIdxInBindCols(int16_t idx, AvroTableDes *td);
bool           avroIdxInBindTags(int16_t idx, AvroTableDes *td);

// ---- File scanning ----

// Scan a data directory for files matching the given extension.
// ext: e.g. "avro-tbtags", "avro-ntb", "avro"
// isVirtual: if true, only return files whose first record is a VTABLE.
// Returns NULL-terminated array of filenames (relative to dirPath). Caller must free.
char **avroScanFiles(const char *dirPath, const char *ext,
                     bool isVirtual, int64_t *outCount);

// Count files matching extension in a directory.
int64_t avroCountFiles(const char *dirPath, const char *ext);

// Check if the first AVRO record in a file contains "CREATE VTABLE".
bool avroIsVirtualTable(const char *filePath);

// Free a file list returned by avroScanFiles.
void avroFreeFileList(char **list);

// Read the "stbname" file from a data directory to get the STB name.
// Returns a strdup'd name or NULL.
char *avroReadFolderStbName(const char *dirPath);

// Get the AVRO table description from server via DESCRIBE.
int avroGetTableDes(TAOS *taos, const char *dbName, const char *tableName,
                    AvroTableDes *tableDes, bool colOnly);

// Check if a directory is a normal-table folder (contains avro-ntb files
// but no avro-tbtags files, or ends with special marker).
bool avroIsNormalTableFolder(const char *dbPath);

#endif  // COMPAT_AVRO_UTIL_H_
