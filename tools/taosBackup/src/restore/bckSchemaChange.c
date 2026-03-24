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

#include "bckSchemaChange.h"
#include "bckPool.h"

//
// --------------------------------- HASH MAP -----------------------------------------
//

static uint32_t bkdrHash(const char *str) {
    uint32_t seed = 131;
    uint32_t hash = 0;
    while (*str) {
        hash = hash * seed + (*str++);
    }
    return hash;
}

void stbChangeMapInit(StbChangeMap *map) {
    memset(map->buckets, 0, sizeof(map->buckets));
    pthread_mutex_init(&map->lock, NULL);
}

static bool stbChangeMapInsert(StbChangeMap *map, const char *key, StbChange *value) {
    pthread_mutex_lock(&map->lock);
    uint32_t hash = bkdrHash(key) % SCHEMA_CHANGE_MAP_BUCKETS;
    StbChangeEntry *entry = (StbChangeEntry *)taosMemoryMalloc(sizeof(StbChangeEntry));
    if (entry == NULL) {
        pthread_mutex_unlock(&map->lock);
        return false;
    }

    entry->key         = taosStrdup(key);
    entry->value       = value;
    entry->next        = map->buckets[hash];
    map->buckets[hash] = entry;

    pthread_mutex_unlock(&map->lock);
    return true;
}

// NOTE: Thread-safety assumption — all inserts to the map are done before
// worker threads start. Worker threads only call find (read-only), so no
// lock is needed here. If future refactoring allows concurrent insert+find,
// change to rwlock.
static StbChange* stbChangeMapFind(StbChangeMap *map, const char *key) {
    uint32_t hash = bkdrHash(key) % SCHEMA_CHANGE_MAP_BUCKETS;
    StbChangeEntry *entry = map->buckets[hash];
    while (entry != NULL) {
        if (strcmp(entry->key, key) == 0) {
            return entry->value;
        }
        entry = entry->next;
    }
    return NULL;
}

void stbChangeMapDestroy(StbChangeMap *map) {
    for (int i = 0; i < SCHEMA_CHANGE_MAP_BUCKETS; i++) {
        StbChangeEntry *entry = map->buckets[i];
        while (entry != NULL) {
            StbChangeEntry *next = entry->next;
            if (entry->value) {
                freeStbChange(entry->value);
            }
            taosMemoryFree(entry->key);
            taosMemoryFree(entry);
            entry = next;
        }
        map->buckets[i] = NULL;
    }
    pthread_mutex_destroy(&map->lock);
}


//
// --------------------------------- SCHEMA PARSING -----------------------------------------
//

// Column info parsed from backup CSV
typedef struct {
    char    field[65];
    char    type[32];
    int32_t length;
    char    note[32];    // "TAG" or empty
    int8_t  tdType;      // converted TDengine type
} CsvColInfo;

// Convert type string to TDengine type code
static int8_t typeStrToTdType(const char *typeStr) {
    if (strcasecmp(typeStr, "BOOL") == 0)              return TSDB_DATA_TYPE_BOOL;
    if (strcasecmp(typeStr, "TINYINT") == 0)           return TSDB_DATA_TYPE_TINYINT;
    if (strcasecmp(typeStr, "SMALLINT") == 0)          return TSDB_DATA_TYPE_SMALLINT;
    if (strcasecmp(typeStr, "INT") == 0)               return TSDB_DATA_TYPE_INT;
    if (strcasecmp(typeStr, "BIGINT") == 0)            return TSDB_DATA_TYPE_BIGINT;
    if (strcasecmp(typeStr, "FLOAT") == 0)             return TSDB_DATA_TYPE_FLOAT;
    if (strcasecmp(typeStr, "DOUBLE") == 0)            return TSDB_DATA_TYPE_DOUBLE;
    if (strcasecmp(typeStr, "BINARY") == 0)            return TSDB_DATA_TYPE_BINARY;
    if (strcasecmp(typeStr, "VARCHAR") == 0)           return TSDB_DATA_TYPE_VARCHAR;
    if (strcasecmp(typeStr, "TIMESTAMP") == 0)         return TSDB_DATA_TYPE_TIMESTAMP;
    if (strcasecmp(typeStr, "NCHAR") == 0)             return TSDB_DATA_TYPE_NCHAR;
    if (strcasecmp(typeStr, "TINYINT UNSIGNED") == 0)  return TSDB_DATA_TYPE_UTINYINT;
    if (strcasecmp(typeStr, "SMALLINT UNSIGNED") == 0) return TSDB_DATA_TYPE_USMALLINT;
    if (strcasecmp(typeStr, "INT UNSIGNED") == 0)      return TSDB_DATA_TYPE_UINT;
    if (strcasecmp(typeStr, "BIGINT UNSIGNED") == 0)   return TSDB_DATA_TYPE_UBIGINT;
    if (strcasecmp(typeStr, "JSON") == 0)              return TSDB_DATA_TYPE_JSON;
    if (strcasecmp(typeStr, "VARBINARY") == 0)         return TSDB_DATA_TYPE_VARBINARY;
    if (strcasecmp(typeStr, "GEOMETRY") == 0)          return TSDB_DATA_TYPE_GEOMETRY;
    if (strcasecmp(typeStr, "BLOB") == 0)              return TSDB_DATA_TYPE_BLOB;
    if (strcasecmp(typeStr, "MEDIUMBLOB") == 0)        return TSDB_DATA_TYPE_MEDIUMBLOB;
    /* DECIMAL(p,s) — DESCRIBE returns "DECIMAL(p, s)"; precision <= 18 → DECIMAL64 */
    if (strncasecmp(typeStr, "DECIMAL", 7) == 0) {
        int precision = 38;  /* default to 128-bit if no parens */
        const char *paren = strchr(typeStr, '(');
        if (paren) sscanf(paren + 1, " %d", &precision);
        return (precision <= 18) ? TSDB_DATA_TYPE_DECIMAL64 : TSDB_DATA_TYPE_DECIMAL;
    }
    return -1;
}

//
// Parse the backup CSV schema file for a super table
// CSV format: field,type,length,note
// Returns array of CsvColInfo, count in *outCount. Caller frees.
//
static CsvColInfo* parseBackupCsv(const char *csvPath, int *outCount) {
    *outCount = 0;

    TdFilePtr fp = taosOpenFile(csvPath, TD_FILE_READ);
    if (fp == NULL) {
        logError("open csv file failed: %s", csvPath);
        return NULL;
    }

    // read entire file
    int64_t fileSize = 0;
    if (taosFStatFile(fp, &fileSize, NULL) != 0 || fileSize <= 0) {
        fileSize = 1024 * 1024;
    }

    char *buf = (char *)taosMemoryMalloc(fileSize + 1);
    if (buf == NULL) {
        taosCloseFile(&fp);
        return NULL;
    }

    int64_t readLen = taosReadFile(fp, buf, fileSize);
    taosCloseFile(&fp);
    if (readLen <= 0) {
        taosMemoryFree(buf);
        return NULL;
    }
    buf[readLen] = '\0';

    // count lines (excluding header)
    int lineCount = 0;
    char *p = buf;
    while (*p) {
        if (*p == '\n') lineCount++;
        p++;
    }
    if (lineCount <= 0) {
        taosMemoryFree(buf);
        return NULL;
    }

    // +1 to handle files where the last line has no trailing newline
    CsvColInfo *cols = (CsvColInfo *)taosMemoryCalloc(lineCount + 1, sizeof(CsvColInfo));
    if (cols == NULL) {
        taosMemoryFree(buf);
        return NULL;
    }

    // parse lines
    char *line = buf;
    int idx = 0;
    bool headerSkipped = false;

    while (line && *line) {
        char *eol = strchr(line, '\n');
        if (eol) *eol = '\0';

        // trim trailing \r
        int len = strlen(line);
        if (len > 0 && line[len - 1] == '\r') line[--len] = '\0';

        // skip empty lines
        if (len == 0) {
            if (eol) { line = eol + 1; continue; }
            break;
        }

        // skip header line
        if (!headerSkipped) {
            headerSkipped = true;
            if (eol) { line = eol + 1; continue; }
            break;
        }

        // parse CSV: field,type,length,note
        CsvColInfo *col = &cols[idx];
        char *tok = line;
        char *comma;

        // field
        comma = strchr(tok, ',');
        if (!comma) goto next_line;
        *comma = '\0';
        strncpy(col->field, tok, sizeof(col->field) - 1);
        tok = comma + 1;

        // type
        comma = strchr(tok, ',');
        if (!comma) goto next_line;
        *comma = '\0';
        strncpy(col->type, tok, sizeof(col->type) - 1);
        tok = comma + 1;

        // length
        comma = strchr(tok, ',');
        if (!comma) goto next_line;
        *comma = '\0';
        col->length = atoi(tok);
        tok = comma + 1;

        // note (rest of line)
        strncpy(col->note, tok, sizeof(col->note) - 1);

        // convert type
        col->tdType = typeStrToTdType(col->type);

        idx++;

next_line:
        if (eol) {
            line = eol + 1;
        } else {
            break;
        }
    }

    taosMemoryFree(buf);
    *outCount = idx;
    return cols;
}


//
// Query server for current super table schema via DESCRIBE
// Returns array of CsvColInfo (reusing same struct), count in *outCount. Caller frees.
//
static CsvColInfo* queryServerSchema(TAOS *conn, const char *dbName, const char *stbName, int *outCount) {
    *outCount = 0;

    char sql[512];
    snprintf(sql, sizeof(sql), "DESCRIBE `%s`.`%s`", dbName, stbName);

    TAOS_RES *res = taos_query(conn, sql);
    int code = taos_errno(res);
    if (code != 0) {
        logWarn("DESCRIBE %s.%s failed (0x%08X): %s", dbName, stbName, code, taos_errstr(res));
        if (res) taos_free_result(res);
        return NULL;
    }

    // allocate initial capacity
    int capacity = 64;
    CsvColInfo *cols = (CsvColInfo *)taosMemoryCalloc(capacity, sizeof(CsvColInfo));
    if (cols == NULL) {
        taos_free_result(res);
        return NULL;
    }

    TAOS_ROW row;
    int idx = 0;
    while ((row = taos_fetch_row(res))) {
        int32_t *lens = taos_fetch_lengths(res);
        if (!row[0] || !row[1]) continue;

        if (idx >= capacity) {
            capacity *= 2;
            CsvColInfo *tmp = (CsvColInfo *)taosMemoryRealloc(cols, capacity * sizeof(CsvColInfo));
            if (!tmp) {
                taosMemoryFree(cols);
                taos_free_result(res);
                return NULL;
            }
            cols = tmp;
        }

        CsvColInfo *col = &cols[idx];
        memset(col, 0, sizeof(CsvColInfo));

        // field name
        int flen = lens[0] < 64 ? lens[0] : 63;
        memcpy(col->field, (char *)row[0], flen);
        col->field[flen] = '\0';

        // type
        int tlen = lens[1] < 31 ? lens[1] : 30;
        memcpy(col->type, (char *)row[1], tlen);
        col->type[tlen] = '\0';

        // length
        if (row[2]) {
            col->length = *(int32_t *)row[2];
        }

        // note
        if (row[3] && lens[3] > 0) {
            int nlen = lens[3] < 31 ? lens[3] : 30;
            memcpy(col->note, (char *)row[3], nlen);
            col->note[nlen] = '\0';
        }

        col->tdType = typeStrToTdType(col->type);
        idx++;
    }

    taos_free_result(res);
    *outCount = idx;
    return cols;
}


//
// --------------------------------- CORE LOGIC -----------------------------------------
//

// Generate partial column list string like "(`ts`,`col1`,`col3`)"
static char* genPartColsStr(ColMapping *mappings, int count) {
    if (count == 0) return NULL;

    int size = 2 + count * (65 + 4);  // parentheses + names + backticks + commas
    char *str = (char *)taosMemoryCalloc(1, size);
    if (str == NULL) return NULL;

    int pos = 0;
    for (int i = 0; i < count; i++) {
        pos += snprintf(str + pos, size - pos,
                        i == 0 ? "(`%s`" : ",`%s`",
                        mappings[i].name);
    }
    strcat(str, ")");
    return str;
}


//
// Build a per-table StbChange for a NORMAL TABLE using the .dat file's
// embedded FieldInfo[] as the backup schema.
// Algorithm mirrors taosdump's localCrossServer: intersect backup columns
// with server columns by name+type.  Returns NULL when schemas match
// (no partial write needed) or on error.  Caller must freeStbChange().
//
StbChange* buildNtbSchemaChange(TAOS *conn, const char *dbName,
                                const char *tbName,
                                FieldInfo *fis, int numFields) {
    if (!conn || !dbName || !tbName || !fis || numFields <= 0) return NULL;

    // Query current server schema for this normal table
    const char *targetDb = argRenameDb(dbName);
    int serverColCount = 0;
    CsvColInfo *serverCols = queryServerSchema(conn, targetDb, tbName, &serverColCount);
    if (serverCols == NULL || serverColCount == 0) {
        logWarn("ntb %s.%s: cannot query server schema, skip schema change detection",
                targetDb, tbName);
        if (serverCols) taosMemoryFree(serverCols);
        return NULL;
    }

    // For normal tables all DESCRIBE rows are data columns (no TAGs)
    int serverDataCols = serverColCount;

    // Intersect backup fields with server fields by name + type
    ColMapping *mappings = (ColMapping *)taosMemoryCalloc(numFields, sizeof(ColMapping));
    if (!mappings) { taosMemoryFree(serverCols); return NULL; }

    int  matchCount   = 0;
    bool schemaChanged = false;

    for (int bi = 0; bi < numFields; bi++) {
        bool found = false;
        for (int si = 0; si < serverDataCols; si++) {
            if (strcmp(fis[bi].name, serverCols[si].field) == 0 &&
                (int8_t)fis[bi].type == serverCols[si].tdType) {
                ColMapping *m = &mappings[matchCount];
                strncpy(m->name, fis[bi].name, sizeof(m->name) - 1);
                m->type          = fis[bi].type;
                m->bytes         = fis[bi].bytes;
                m->backupIdx     = bi;
                m->existsOnServer = true;
                matchCount++;
                found = true;
                break;
            }
        }
        if (!found) {
            schemaChanged = true;
            logInfo("ntb %s.%s: backup column '%s' (type=%d) not found on server",
                    dbName, tbName, fis[bi].name, (int)fis[bi].type);
        }
    }
    if (serverDataCols != numFields) schemaChanged = true;

    taosMemoryFree(serverCols);

    if (matchCount == 0) {
        logError("ntb %s.%s: no matching columns between backup and server!", dbName, tbName);
        taosMemoryFree(mappings);
        return NULL;
    }

    if (!schemaChanged) {
        // schemas are identical — no partial-write needed
        taosMemoryFree(mappings);
        return NULL;
    }

    // Build StbChange
    StbChange *sc = (StbChange *)taosMemoryCalloc(1, sizeof(StbChange));
    if (!sc) { taosMemoryFree(mappings); return NULL; }

    strncpy(sc->stbName, tbName, TSDB_TABLE_NAME_LEN - 1);
    sc->schemaChanged  = true;
    sc->backupColCount = numFields;
    sc->matchColCount  = matchCount;
    sc->colMappings    = mappings;

    sc->bindIdxMap = (int *)taosMemoryMalloc(numFields * sizeof(int));
    if (!sc->bindIdxMap) { freeStbChange(sc); return NULL; }
    for (int i = 0; i < numFields; i++) sc->bindIdxMap[i] = -1;
    for (int i = 0; i < matchCount; i++) sc->bindIdxMap[mappings[i].backupIdx] = i;

    sc->partColsStr = genPartColsStr(mappings, matchCount);
    logInfo("ntb %s.%s: schema changed! backup cols=%d, server cols=%d, matched=%d, partCols=%s",
            dbName, tbName, numFields, serverDataCols, matchCount,
            sc->partColsStr ? sc->partColsStr : "NULL");
    return sc;
}


void freeStbChange(StbChange *sc) {
    if (sc == NULL) return;
    if (sc->colMappings) {
        taosMemoryFree(sc->colMappings);
        sc->colMappings = NULL;
    }
    if (sc->bindIdxMap) {
        taosMemoryFree(sc->bindIdxMap);
        sc->bindIdxMap = NULL;
    }
    if (sc->partColsStr) {
        taosMemoryFree(sc->partColsStr);
        sc->partColsStr = NULL;
    }
    taosMemoryFree(sc);
}


StbChange* findStbChange(StbChangeMap *map, const char *stbName) {
    if (map == NULL || stbName == NULL || stbName[0] == '\0') {
        return NULL;
    }
    return stbChangeMapFind(map, stbName);
}


int addStbChanged(StbChangeMap *map, TAOS *conn, const char *dbName, const char *stbName) {
    // check if already computed
    if (stbChangeMapFind(map, stbName) != NULL) {
        logDebug("stb %s.%s already in change map, skip", dbName, stbName);
        return 0;
    }

    //
    // 1. Read backup schema from CSV file
    //
    char csvFile[MAX_PATH_LEN] = {0};
    obtainFileName(BACK_FILE_STBCSV, dbName, stbName, NULL, 0, 0, BINARY_TAOS, csvFile, sizeof(csvFile));

    int backupColCount = 0;
    CsvColInfo *backupCols = parseBackupCsv(csvFile, &backupColCount);
    if (backupCols == NULL || backupColCount == 0) {
        logWarn("no backup schema found for %s.%s, skip schema change detection", dbName, stbName);
        if (backupCols) taosMemoryFree(backupCols);
        return 0;
    }

    // separate columns and tags in backup schema
    int backupDataCols = 0;
    for (int i = 0; i < backupColCount; i++) {
        if (backupCols[i].note[0] == '\0' || strncasecmp(backupCols[i].note, "TAG", 3) != 0) {
            backupDataCols++;
        } else {
            break;  // TAG columns come after data columns in DESCRIBE output
        }
    }

    //
    // 2. Query server for current schema
    //
    const char *targetDb = argRenameDb(dbName);
    int serverColCount = 0;
    CsvColInfo *serverCols = queryServerSchema(conn, targetDb, stbName, &serverColCount);
    if (serverCols == NULL || serverColCount == 0) {
        logWarn("cannot query server schema for %s.%s, skip schema change detection", targetDb, stbName);
        taosMemoryFree(backupCols);
        if (serverCols) taosMemoryFree(serverCols);
        return 0;
    }

    // separate columns and tags on server
    int serverDataCols = 0;
    for (int i = 0; i < serverColCount; i++) {
        if (serverCols[i].note[0] == '\0' || strncasecmp(serverCols[i].note, "TAG", 3) != 0) {
            serverDataCols++;
        } else {
            break;
        }
    }

    //
    // 3. Compare: find matching data columns (name + type match)
    //
    ColMapping *mappings = (ColMapping *)taosMemoryCalloc(backupDataCols, sizeof(ColMapping));
    if (mappings == NULL) {
        taosMemoryFree(backupCols);
        taosMemoryFree(serverCols);
        return -1;
    }

    int matchCount = 0;
    bool schemaChanged = false;

    for (int bi = 0; bi < backupDataCols; bi++) {
        bool found = false;
        for (int si = 0; si < serverDataCols; si++) {
            if (strcmp(backupCols[bi].field, serverCols[si].field) == 0 &&
                backupCols[bi].tdType == serverCols[si].tdType) {
                // match found
                ColMapping *m = &mappings[matchCount];
                strncpy(m->name, backupCols[bi].field, sizeof(m->name) - 1);
                m->type = backupCols[bi].tdType;
                m->bytes = backupCols[bi].length;
                m->backupIdx = bi;
                m->existsOnServer = true;
                matchCount++;
                found = true;
                break;
            }
        }
        if (!found) {
            schemaChanged = true;
            logInfo("stb %s.%s: backup column '%s' (type=%s) not found on server",
                    dbName, stbName, backupCols[bi].field, backupCols[bi].type);
        }
    }

    // Also flag as changed if server has more columns than backup
    if (serverDataCols != backupDataCols) {
        schemaChanged = true;
    }

    // Check: must have at least 1 matching column (timestamp)
    if (matchCount == 0) {
        logError("stb %s.%s: no matching columns between backup and server!", dbName, stbName);
        taosMemoryFree(mappings);
        taosMemoryFree(backupCols);
        taosMemoryFree(serverCols);
        return -1;
    }

    //
    // 4. Build StbChange
    //
    StbChange *sc = (StbChange *)taosMemoryCalloc(1, sizeof(StbChange));
    if (sc == NULL) {
        taosMemoryFree(mappings);
        taosMemoryFree(backupCols);
        taosMemoryFree(serverCols);
        return -1;
    }

    strncpy(sc->stbName, stbName, TSDB_TABLE_NAME_LEN - 1);
    sc->schemaChanged  = schemaChanged;
    sc->backupColCount = backupDataCols;
    sc->matchColCount  = matchCount;
    sc->colMappings    = mappings;

    // Build O(1) lookup table: bindIdxMap[backupIdx] -> bind position or -1
    sc->bindIdxMap = (int *)taosMemoryMalloc(backupDataCols * sizeof(int));
    if (sc->bindIdxMap == NULL) {
        taosMemoryFree(mappings);
        taosMemoryFree(sc);
        taosMemoryFree(backupCols);
        taosMemoryFree(serverCols);
        return -1;
    }
    for (int i = 0; i < backupDataCols; i++) {
        sc->bindIdxMap[i] = -1;  // default: skip
    }
    for (int i = 0; i < matchCount; i++) {
        sc->bindIdxMap[mappings[i].backupIdx] = i;
    }

    if (schemaChanged) {
        sc->partColsStr = genPartColsStr(mappings, matchCount);
        logInfo("stb %s.%s: schema changed! backup cols=%d, server cols=%d, matched=%d, partCols=%s",
                dbName, stbName, backupDataCols, serverDataCols, matchCount,
                sc->partColsStr ? sc->partColsStr : "NULL");
    } else {
        sc->partColsStr = NULL;
        logInfo("stb %s.%s: schema unchanged. cols=%d", dbName, stbName, backupDataCols);
    }

    //
    // 5. Insert to map
    //
    if (!stbChangeMapInsert(map, stbName, sc)) {
        logError("insert stb change to map failed: %s.%s", dbName, stbName);
        freeStbChange(sc);
        taosMemoryFree(backupCols);
        taosMemoryFree(serverCols);
        return -1;
    }

    taosMemoryFree(backupCols);
    taosMemoryFree(serverCols);
    return 0;
}


bool isColInPartialWrite(StbChange *sc, int backupIdx) {
    if (sc == NULL || !sc->schemaChanged) {
        return true;  // no change, all columns included
    }
    if (backupIdx < 0 || backupIdx >= sc->backupColCount) {
        return false;
    }
    return sc->bindIdxMap[backupIdx] >= 0;
}


int getPartialWriteBindIdx(StbChange *sc, int backupIdx) {
    if (sc == NULL || !sc->schemaChanged) {
        return backupIdx;  // no change, 1:1 mapping
    }
    if (backupIdx < 0 || backupIdx >= sc->backupColCount) {
        return -1;
    }
    return sc->bindIdxMap[backupIdx];
}
