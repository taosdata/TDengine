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

#include "compatAvroUtil.h"
#include <avro.h>
// Temporarily undef TDengine atomic overrides for jansson.h inline functions
#pragma push_macro("__atomic_add_fetch")
#pragma push_macro("__atomic_sub_fetch")
#undef __atomic_add_fetch
#undef __atomic_sub_fetch
#include <jansson.h>
#pragma pop_macro("__atomic_sub_fetch")
#pragma pop_macro("__atomic_add_fetch")
#include "bck.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taoserror.h>
#include <osDir.h>

#ifndef TSDB_MAX_ALLOWED_SQL_LEN
#define TSDB_MAX_ALLOWED_SQL_LEN (4*1024*1024u)
#endif

// ==================== HashMap ====================

static uint32_t bkdrHash(const char *str) {
    uint32_t seed = 131;
    uint32_t hash = 0;
    while (*str) {
        hash = hash * seed + (uint32_t)(*str++);
    }
    return hash % AVRO_HASH_BUCKETS;
}

AvroHashMap *avroHashMapCreate(void) {
    AvroHashMap *map = (AvroHashMap *)taosMemoryCalloc(1, sizeof(AvroHashMap));
    if (!map) return NULL;
    pthread_mutex_init(&map->lock, NULL);
    return map;
}

bool avroHashMapInsert(AvroHashMap *map, const char *key, void *value) {
    if (!map || !key) return false;
    pthread_mutex_lock(&map->lock);

    uint32_t idx = bkdrHash(key);
    // Check if key already exists
    AvroHashEntry *entry = map->buckets[idx];
    while (entry) {
        if (strcmp(entry->key, key) == 0) {
            entry->value = value;
            pthread_mutex_unlock(&map->lock);
            return true;
        }
        entry = entry->next;
    }

    // Insert new
    AvroHashEntry *newEntry = (AvroHashEntry *)taosMemoryCalloc(1, sizeof(AvroHashEntry));
    if (!newEntry) {
        pthread_mutex_unlock(&map->lock);
        return false;
    }
    newEntry->key   = taosStrdup(key);
    newEntry->value = value;
    newEntry->next  = map->buckets[idx];
    map->buckets[idx] = newEntry;

    pthread_mutex_unlock(&map->lock);
    return true;
}

void *avroHashMapFind(AvroHashMap *map, const char *key) {
    if (!map || !key) return NULL;
    pthread_mutex_lock(&map->lock);

    uint32_t idx = bkdrHash(key);
    AvroHashEntry *entry = map->buckets[idx];
    while (entry) {
        if (strcmp(entry->key, key) == 0) {
            void *val = entry->value;
            pthread_mutex_unlock(&map->lock);
            return val;
        }
        entry = entry->next;
    }

    pthread_mutex_unlock(&map->lock);
    return NULL;
}

void avroHashMapDestroy(AvroHashMap *map) {
    if (!map) return;
    for (int i = 0; i < AVRO_HASH_BUCKETS; i++) {
        AvroHashEntry *entry = map->buckets[i];
        while (entry) {
            AvroHashEntry *next = entry->next;
            taosMemoryFree(entry->key);
            // value freed by caller (avroFreeDbChange)
            taosMemoryFree(entry);
            entry = next;
        }
    }
    pthread_mutex_destroy(&map->lock);
}

// ==================== DBChange / StbChange ====================

AvroDBChange *avroCreateDbChange(const char *dbPath) {
    AvroDBChange *dc = (AvroDBChange *)taosMemoryCalloc(1, sizeof(AvroDBChange));
    if (!dc) return NULL;
    dc->dbPath = dbPath;
    pthread_mutex_init(&dc->stbMap.lock, NULL);
    return dc;
}

void avroFreeStbChange(AvroStbChange *sc) {
    if (!sc) return;
    if (sc->tableDes) {
        avroFreeTableDes(sc->tableDes, true);
        sc->tableDes = NULL;
    }
    if (sc->strTags) { taosMemoryFree(sc->strTags); sc->strTags = NULL; }
    if (sc->strCols) { taosMemoryFree(sc->strCols); sc->strCols = NULL; }
    taosMemoryFree(sc);
}

void avroFreeDbChange(AvroDBChange *dc) {
    if (!dc) return;
    // Free all StbChange values in the hashmap
    for (int i = 0; i < AVRO_HASH_BUCKETS; i++) {
        AvroHashEntry *entry = dc->stbMap.buckets[i];
        while (entry) {
            AvroHashEntry *next = entry->next;
            if (entry->value) {
                avroFreeStbChange((AvroStbChange *)entry->value);
            }
            taosMemoryFree(entry->key);
            taosMemoryFree(entry);
            entry = next;
        }
        dc->stbMap.buckets[i] = NULL;
    }
    pthread_mutex_destroy(&dc->stbMap.lock);
    taosMemoryFree(dc);
}

AvroStbChange *avroFindStbChange(AvroDBChange *dc, const char *stbName) {
    if (!dc || !stbName) return NULL;
    return (AvroStbChange *)avroHashMapFind(&dc->stbMap, stbName);
}

// ---- getTableDes: query server for DESCRIBE table ----

int avroGetTableDes(TAOS *taos, const char *dbName, const char *tableName,
                    AvroTableDes *tableDes, bool colOnly) {
    if (!taos || !dbName || !tableName || !tableDes) return -1;

    char sql[512];
    snprintf(sql, sizeof(sql), "DESCRIBE `%s`.`%s`", dbName, tableName);

    TAOS_RES *res = taos_query(taos, sql);
    int code = taos_errno(res);
    if (code != 0) {
        logError("avro: DESCRIBE %s.%s failed: %s", dbName, tableName, taos_errstr(res));
        taos_free_result(res);
        return -1;
    }

    TAOS_ROW    row;
    TAOS_FIELD *fields = taos_fetch_fields(res);
    int         numFields = taos_field_count(res);
    int         index = 0;

    snprintf(tableDes->name, AVRO_TABLE_NAME_LEN, "%s", tableName);
    tableDes->columns = 0;
    tableDes->tags    = 0;

    while ((row = taos_fetch_row(res)) != NULL) {
        // field[0]: Field, field[1]: Type, field[2]: Length, field[3]: Note
        if (numFields < 4) break;

        int *lengths = taos_fetch_lengths(res);

        // Field name
        if (row[0] && lengths[0] > 0) {
            int copyLen = lengths[0] < AVRO_COL_NAME_LEN - 1 ? lengths[0] : AVRO_COL_NAME_LEN - 1;
            memcpy(tableDes->cols[index].field, row[0], copyLen);
            tableDes->cols[index].field[copyLen] = '\0';
        }

        // Type string → type int
        if (row[1]) {
            char typeBuf[32] = {0};
            int tlen = lengths[1] < 31 ? lengths[1] : 31;
            memcpy(typeBuf, row[1], tlen);
            typeBuf[tlen] = '\0';

            // Map common type strings
            if      (strcasecmp(typeBuf, "TIMESTAMP")  == 0) tableDes->cols[index].type = TSDB_DATA_TYPE_TIMESTAMP;
            else if (strcasecmp(typeBuf, "BOOL")       == 0) tableDes->cols[index].type = TSDB_DATA_TYPE_BOOL;
            else if (strcasecmp(typeBuf, "TINYINT")    == 0) tableDes->cols[index].type = TSDB_DATA_TYPE_TINYINT;
            else if (strcasecmp(typeBuf, "SMALLINT")   == 0) tableDes->cols[index].type = TSDB_DATA_TYPE_SMALLINT;
            else if (strcasecmp(typeBuf, "INT")        == 0) tableDes->cols[index].type = TSDB_DATA_TYPE_INT;
            else if (strcasecmp(typeBuf, "BIGINT")     == 0) tableDes->cols[index].type = TSDB_DATA_TYPE_BIGINT;
            else if (strcasecmp(typeBuf, "FLOAT")      == 0) tableDes->cols[index].type = TSDB_DATA_TYPE_FLOAT;
            else if (strcasecmp(typeBuf, "DOUBLE")     == 0) tableDes->cols[index].type = TSDB_DATA_TYPE_DOUBLE;
            else if (strncasecmp(typeBuf, "BINARY",  6) == 0) tableDes->cols[index].type = TSDB_DATA_TYPE_BINARY;
            else if (strncasecmp(typeBuf, "VARCHAR", 7) == 0) tableDes->cols[index].type = TSDB_DATA_TYPE_BINARY;
            else if (strncasecmp(typeBuf, "NCHAR",   5) == 0) tableDes->cols[index].type = TSDB_DATA_TYPE_NCHAR;
            else if (strcasecmp(typeBuf, "JSON")       == 0) tableDes->cols[index].type = TSDB_DATA_TYPE_JSON;
            else if (strncasecmp(typeBuf, "VARBINARY", 9) == 0) tableDes->cols[index].type = TSDB_DATA_TYPE_VARBINARY;
            else if (strncasecmp(typeBuf, "GEOMETRY", 8) == 0) tableDes->cols[index].type = TSDB_DATA_TYPE_GEOMETRY;
            else if (strcasecmp(typeBuf, "TINYINT UNSIGNED")  == 0) tableDes->cols[index].type = TSDB_DATA_TYPE_UTINYINT;
            else if (strcasecmp(typeBuf, "SMALLINT UNSIGNED") == 0) tableDes->cols[index].type = TSDB_DATA_TYPE_USMALLINT;
            else if (strcasecmp(typeBuf, "INT UNSIGNED")      == 0) tableDes->cols[index].type = TSDB_DATA_TYPE_UINT;
            else if (strcasecmp(typeBuf, "BIGINT UNSIGNED")   == 0) tableDes->cols[index].type = TSDB_DATA_TYPE_UBIGINT;
            else tableDes->cols[index].type = TSDB_DATA_TYPE_BINARY;
        }

        // Length
        if (row[2]) {
            tableDes->cols[index].length = *((int *)row[2]);
        }

        // Note: "TAG" or empty
        if (row[3] && lengths[3] > 0) {
            int nlen = lengths[3] < AVRO_COL_NOTE_LEN - 1 ? lengths[3] : AVRO_COL_NOTE_LEN - 1;
            memcpy(tableDes->cols[index].note, row[3], nlen);
            tableDes->cols[index].note[nlen] = '\0';
        }

        tableDes->cols[index].idx = (int16_t)index;
        index++;
    }
    taos_free_result(res);

    // Count columns vs tags based on note field
    tableDes->columns = 0;
    tableDes->tags    = 0;
    for (int i = 0; i < index; i++) {
        if (strcasecmp(tableDes->cols[i].note, "TAG") == 0) {
            tableDes->tags++;
        } else {
            tableDes->columns++;
        }
    }

    if (colOnly) {
        tableDes->tags = 0;
    }

    return 0;
}

// ---- Schema comparison for AddStbChanged ----

// Check if local schema matches server schema
static bool schemaNoChanged(AvroTableDes *local, AvroTableDes *server, bool colOnly) {
    if (!local || !server) return false;

    int localCount  = colOnly ? local->columns  : (local->columns + local->tags);
    int serverCount = colOnly ? server->columns : (server->columns + server->tags);

    if (localCount != serverCount) return false;

    for (int i = 0; i < localCount; i++) {
        if (strcasecmp(local->cols[i].field, server->cols[i].field) != 0)
            return false;
        if (local->cols[i].type != server->cols[i].type)
            return false;
    }
    return true;
}

// Build partial column/tag string like "(col1,col2,...)"
static char *buildPartialStr(AvroTableDes *serverDes, AvroTableDes *localDes,
                              int start, int count, bool isTags) {
    // Build list of columns that exist in both local and server
    char *buf = (char *)taosMemoryCalloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    if (!buf) return NULL;

    int pos = 0;
    buf[pos++] = '(';

    int matched = 0;
    for (int si = start; si < start + count; si++) {
        // Check if this server column exists in local
        bool found = false;
        int localStart = isTags ? localDes->columns : 0;
        int localEnd   = isTags ? (localDes->columns + localDes->tags) : localDes->columns;
        for (int li = localStart; li < localEnd; li++) {
            if (strcasecmp(serverDes->cols[si].field, localDes->cols[li].field) == 0) {
                found = true;
                break;
            }
        }
        if (found) {
            if (matched > 0) buf[pos++] = ',';
            pos += snprintf(buf + pos, TSDB_MAX_ALLOWED_SQL_LEN - pos, "`%s`",
                           serverDes->cols[si].field);
            matched++;
        }
    }
    buf[pos++] = ')';
    buf[pos]   = '\0';

    if (matched == 0) {
        taosMemoryFree(buf);
        return NULL;
    }
    return buf;
}

int avroAddStbChanged(AvroDBChange *dc, const char *dbName,
                      TAOS *taos, AvroRecordSchema *rs,
                      AvroStbChange **ppStbChange) {
    if (!dc || !rs) return -1;
    if (ppStbChange) *ppStbChange = NULL;

    // Determine the STB name
    const char *stbName = rs->stbName[0] ? rs->stbName : rs->name;
    if (!stbName[0]) return 0;  // no STB name available

    // Check if already tracked
    AvroStbChange *existing = avroFindStbChange(dc, stbName);
    if (existing) {
        if (ppStbChange) *ppStbChange = existing;
        return 0;
    }

    // No .m metadata → version 0 / old format
    if (rs->version == 0 && !rs->tableDes) {
        if (ppStbChange) *ppStbChange = NULL;
        return 0;
    }

    // Get server-side schema via DESCRIBE
    AvroTableDes *serverDes = avroAllocTableDes(TSDB_MAX_COLUMNS);
    if (!serverDes) return -1;

    if (avroGetTableDes(taos, dbName, stbName, serverDes, false) < 0) {
        avroFreeTableDes(serverDes, true);
        // Table might not exist on server yet — not necessarily an error
        if (ppStbChange) *ppStbChange = NULL;
        return 0;
    }

    AvroStbChange *sc = (AvroStbChange *)taosMemoryCalloc(1, sizeof(AvroStbChange));
    if (!sc) {
        avroFreeTableDes(serverDes, true);
        return -1;
    }

    sc->tableDes = serverDes;

    // Compare local (from .m file) vs server
    if (rs->tableDes) {
        bool colsSame = schemaNoChanged(rs->tableDes, serverDes, true);
        bool tagsSame = true;
        if (rs->tableDes->tags > 0 && serverDes->tags > 0) {
            // Compare tags portion
            AvroTableDes *localCopy = rs->tableDes;
            if (localCopy->tags != serverDes->tags) {
                tagsSame = false;
            } else {
                for (int i = 0; i < localCopy->tags; i++) {
                    int li = localCopy->columns + i;
                    int si = serverDes->columns + i;
                    if (strcasecmp(localCopy->cols[li].field, serverDes->cols[si].field) != 0) {
                        tagsSame = false;
                        break;
                    }
                }
            }
        }

        if (!colsSame || !tagsSame) {
            sc->schemaChanged = true;
            logInfo("avro: schema changed for %s (cols=%s, tags=%s)",
                    stbName, colsSame ? "same" : "diff", tagsSame ? "same" : "diff");

            if (!colsSame) {
                sc->strCols = buildPartialStr(serverDes, rs->tableDes,
                                               0, serverDes->columns, false);
            }
            if (!tagsSame) {
                sc->strTags = buildPartialStr(serverDes, rs->tableDes,
                                               serverDes->columns, serverDes->tags, true);
            }
        }
    }

    // Insert into hashmap
    avroHashMapInsert(&dc->stbMap, stbName, sc);

    if (ppStbChange) *ppStbChange = sc;
    return 0;
}

bool avroIdxInBindCols(int16_t idx, AvroTableDes *td) {
    if (!td || idx < 0 || idx >= td->columns) return false;
    return (td->cols[idx].idx >= 0);
}

bool avroIdxInBindTags(int16_t idx, AvroTableDes *td) {
    if (!td) return false;
    int tagStart = td->columns;
    int tagIdx   = tagStart + idx;
    if (tagIdx < 0 || tagIdx >= td->columns + td->tags) return false;
    return (td->cols[tagIdx].idx >= 0);
}

// ==================== File Scanning ====================

// Check if filename has the given extension (e.g. "avro-tbtags", "avro-ntb", "avro")
static bool hasExtension(const char *filename, const char *ext) {
    if (!filename || !ext) return false;
    size_t fnLen  = strlen(filename);
    size_t extLen = strlen(ext);
    if (fnLen <= extLen + 1) return false;
    // Check ".ext" at end of filename
    if (filename[fnLen - extLen - 1] != '.') return false;
    return (strcmp(filename + fnLen - extLen, ext) == 0);
}

bool avroIsVirtualTable(const char *filePath) {
    avro_file_reader_t reader;
    if (avro_file_reader(filePath, &reader)) {
        return false;
    }

    avro_schema_t schema = avro_file_reader_get_writer_schema(reader);
    avro_value_iface_t *iface = avro_generic_class_from_schema(schema);
    avro_value_t value;
    avro_generic_value_new(iface, &value);

    bool isVirtual = false;
    if (!avro_file_reader_read_value(reader, &value)) {
        avro_value_t sqlField, sqlBranch;
        if (avro_value_get_by_name(&value, "sql", &sqlField, NULL) == 0) {
            avro_value_get_current_branch(&sqlField, &sqlBranch);
            if (avro_value_get_null(&sqlBranch) != 0) {
                char *buf = NULL;
                size_t size = 0;
                avro_value_get_string(&sqlBranch, (const char **)&buf, &size);
                if (buf && size > 0) {
                    const char *vtablePrefix = "CREATE VTABLE";
                    isVirtual = (strncasecmp(buf, vtablePrefix, strlen(vtablePrefix)) == 0);
                }
            }
        }
    }

    avro_value_decref(&value);
    avro_value_iface_decref(iface);
    avro_file_reader_close(reader);
    return isVirtual;
}

int64_t avroCountFiles(const char *dirPath, const char *ext) {
    TdDirPtr dir = taosOpenDir(dirPath);
    if (!dir) return 0;

    int64_t count = 0;
    TdDirEntryPtr entry;
    while ((entry = taosReadDir(dir)) != NULL) {
        char *name = taosGetDirEntryName(entry);
        if (hasExtension(name, ext)) {
            // Exclude .m metadata files
            size_t nameLen = strlen(name);
            if (nameLen > 2 && strcmp(name + nameLen - 2, ".m") == 0) continue;
            count++;
        }
    }
    taosCloseDir(&dir);
    return count;
}

char **avroScanFiles(const char *dirPath, const char *ext,
                     bool isVirtual, int64_t *outCount) {
    *outCount = 0;

    TdDirPtr dir = taosOpenDir(dirPath);
    if (!dir) return NULL;

    // First pass: count
    int64_t capacity = 128;
    char **list = (char **)taosMemoryCalloc((size_t)capacity, sizeof(char *));
    if (!list) {
        taosCloseDir(&dir);
        return NULL;
    }

    TdDirEntryPtr entry;
    while ((entry = taosReadDir(dir)) != NULL) {
        char *name = taosGetDirEntryName(entry);
        if (!hasExtension(name, ext)) continue;

        // Skip .m metadata files
        size_t nameLen = strlen(name);
        if (nameLen > 2 && strcmp(name + nameLen - 2, ".m") == 0) continue;

        // For tbtags and ntb: check virtual table status
        if (strcmp(ext, "avro-tbtags") == 0 || strcmp(ext, "avro-ntb") == 0) {
            char fullPath[512];
            snprintf(fullPath, sizeof(fullPath), "%s/%s", dirPath, name);
            bool fileIsVirtual = avroIsVirtualTable(fullPath);
            if (fileIsVirtual != isVirtual) continue;
        }

        // Add to list
        if (*outCount >= capacity) {
            capacity *= 2;
            char **tmp = (char **)taosMemoryRealloc(list, (size_t)capacity * sizeof(char *));
            if (!tmp) {
                avroFreeFileList(list);
                taosCloseDir(&dir);
                *outCount = 0;
                return NULL;
            }
            list = tmp;
        }
        list[*outCount] = taosStrdup(name);
        (*outCount)++;
    }
    taosCloseDir(&dir);

    // NULL-terminate
    if (*outCount >= capacity) {
        char **tmp = (char **)taosMemoryRealloc(list, (size_t)(*outCount + 1) * sizeof(char *));
        if (tmp) list = tmp;
    }
    list[*outCount] = NULL;

    return list;
}

void avroFreeFileList(char **list) {
    if (!list) return;
    for (int i = 0; list[i] != NULL; i++) {
        taosMemoryFree(list[i]);
    }
    taosMemoryFree(list);
}

char *avroReadFolderStbName(const char *dirPath) {
    char filePath[512];
    snprintf(filePath, sizeof(filePath), "%s/stbname", dirPath);

    TdFilePtr pFile = taosOpenFile(filePath, TD_FILE_READ);
    if (!pFile) return NULL;

    char buf[AVRO_TABLE_NAME_LEN] = {0};
    int64_t rd = taosReadFile(pFile, buf, sizeof(buf) - 1);
    taosCloseFile(&pFile);
    if (rd <= 0) return NULL;
    buf[rd] = '\0';

    // Trim trailing newline
    size_t len = strlen(buf);
    while (len > 0 && (buf[len - 1] == '\n' || buf[len - 1] == '\r')) {
        buf[--len] = '\0';
    }

    if (len == 0) return NULL;
    return taosStrdup(buf);
}

bool avroIsNormalTableFolder(const char *dbPath) {
    // A normal table folder typically has "avro-ntb" files but no "avro-tbtags"
    int64_t ntbCount    = avroCountFiles(dbPath, "avro-ntb");
    int64_t tbtagsCount = avroCountFiles(dbPath, "avro-tbtags");
    return (ntbCount > 0 && tbtagsCount == 0);
}
