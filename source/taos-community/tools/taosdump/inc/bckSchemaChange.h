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

#ifndef INC_BCKSCHEMACHANGE_H_
#define INC_BCKSCHEMACHANGE_H_

#include "bck.h"
#include "colCompress.h"

//
// Schema change detection for super tables during restore.
// When a super table's columns have changed between backup and server,
// only the columns that exist in both are written (partial column write).
//

//
// ---------------- define ----------------
//
#define SCHEMA_CHANGE_MAP_BUCKETS 256

//
// ---------------- struct ----------------
//

// Column mapping: maps backup column index to server column index
typedef struct {
    char    name[65];    // column name
    int8_t  type;        // column type
    int32_t bytes;       // column bytes
    int16_t backupIdx;   // index in backup schema  
    bool    existsOnServer; // whether this column still exists on server
} ColMapping;

// Schema change info for one super table
typedef struct StbChange {
    char    stbName[TSDB_TABLE_NAME_LEN];   // super table name
    bool    schemaChanged;                   // true if any column changed
    int     backupColCount;                  // total columns in backup (data only, no tags)
    int     matchColCount;                   // columns that match server schema
    ColMapping *colMappings;                 // array of matchColCount mappings for matched cols
    int    *bindIdxMap;                      // O(1) lookup: bindIdxMap[backupIdx] = bind position, -1 if skipped
    char   *partColsStr;                     // partial column list string like "(`ts`,`col1`,`col3`)"
} StbChange;

// Hash map entry for StbChange cache
typedef struct StbChangeEntry {
    char   *key;                      // stb name
    StbChange *value;
    struct StbChangeEntry *next;
} StbChangeEntry;

// Hash map for all super table changes in a database
typedef struct StbChangeMap {
    StbChangeEntry *buckets[SCHEMA_CHANGE_MAP_BUCKETS];
    TdThreadMutex lock;
} StbChangeMap;


//
// ---------------- interface ----------------
//

// Initialize a schema change map (call once per database restore)
void stbChangeMapInit(StbChangeMap *map);

// Destroy and free all entries in the map
void stbChangeMapDestroy(StbChangeMap *map);

// Detect schema changes for a super table:
// - Reads backup .csv schema file
// - Queries server for current DESCRIBE
// - Computes matching columns
// - Caches result in the map
// Returns 0 on success, -1 on error
int addStbChanged(StbChangeMap *map, TAOS *conn, const char *dbName, const char *stbName);

// Find cached StbChange for a super table (returns NULL if not found or no change)
StbChange* findStbChange(StbChangeMap *map, const char *stbName);

// Free a single StbChange
void freeStbChange(StbChange *sc);

// Check if a backup column index should be included in the bind
// Returns true if the column at backupIdx is in the matched column set
bool isColInPartialWrite(StbChange *sc, int backupIdx);

// Get the bind position for a backup column index in partial write mode
// O(1) via pre-built lookup table. Returns -1 if column should be skipped.
int getPartialWriteBindIdx(StbChange *sc, int backupIdx);

// Build a per-table schema change for a normal table.
// Uses the file's embedded FieldInfo[] as the backup schema and queries
// the server's current DESCRIBE for the table.  Returns NULL if schemas
// match (no partial-write needed) or on error.  Caller must freeStbChange().
StbChange* buildNtbSchemaChange(TAOS *conn, const char *dbName,
                                const char *tbName,
                                FieldInfo *fis, int numFields);

#endif  // INC_BCKSCHEMACHANGE_H_
