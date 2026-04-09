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

#include "compatAvroSchema.h"
#include "compatAvroUtil.h"
#include "compatAvro.h"
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
#include <inttypes.h>

#ifndef TSDB_MAX_ALLOWED_SQL_LEN
#define TSDB_MAX_ALLOWED_SQL_LEN (4*1024*1024u)
#endif

// ==================== Tag extraction helpers ====================
// Each extracts a tag value from an AVRO record field and appends SQL text.

static int32_t extractTagTinyInt(AvroFieldInfo *fi, avro_value_t *val,
                                 char *sql, int32_t pos) {
    if (fi->nullable) {
        avro_value_t branch;
        avro_value_get_current_branch(val, &branch);
        if (avro_value_get_null(&branch) == 0) {
            return pos + sprintf(sql + pos, "NULL,");
        }
        int32_t n = 0;
        avro_value_get_int(&branch, &n);
        return pos + sprintf(sql + pos, "%d,", n);
    }
    int32_t n = 0;
    avro_value_get_int(val, &n);
    if ((int8_t)n == (int8_t)TSDB_DATA_TINYINT_NULL)
        return pos + sprintf(sql + pos, "NULL,");
    return pos + sprintf(sql + pos, "%d,", n);
}

static int32_t extractTagSmallInt(AvroFieldInfo *fi, avro_value_t *val,
                                  char *sql, int32_t pos) {
    if (fi->nullable) {
        avro_value_t branch;
        avro_value_get_current_branch(val, &branch);
        if (avro_value_get_null(&branch) == 0)
            return pos + sprintf(sql + pos, "NULL,");
        int32_t n = 0;
        avro_value_get_int(&branch, &n);
        return pos + sprintf(sql + pos, "%d,", n);
    }
    int32_t n = 0;
    avro_value_get_int(val, &n);
    if ((int16_t)n == (int16_t)TSDB_DATA_SMALLINT_NULL)
        return pos + sprintf(sql + pos, "NULL,");
    return pos + sprintf(sql + pos, "%d,", n);
}

static int32_t extractTagInt(AvroFieldInfo *fi, avro_value_t *val,
                             char *sql, int32_t pos) {
    if (fi->nullable) {
        avro_value_t branch;
        avro_value_get_current_branch(val, &branch);
        if (avro_value_get_null(&branch) == 0)
            return pos + sprintf(sql + pos, "NULL,");
        int32_t n = 0;
        avro_value_get_int(&branch, &n);
        return pos + sprintf(sql + pos, "%d,", n);
    }
    int32_t n = 0;
    avro_value_get_int(val, &n);
    if (n == (int32_t)TSDB_DATA_INT_NULL)
        return pos + sprintf(sql + pos, "NULL,");
    return pos + sprintf(sql + pos, "%d,", n);
}

static int32_t extractTagBigInt(AvroFieldInfo *fi, avro_value_t *val,
                                char *sql, int32_t pos) {
    if (fi->nullable) {
        avro_value_t branch;
        avro_value_get_current_branch(val, &branch);
        if (avro_value_get_null(&branch) == 0)
            return pos + sprintf(sql + pos, "NULL,");
        int64_t n = 0;
        avro_value_get_long(&branch, &n);
        return pos + sprintf(sql + pos, "%" PRId64 ",", n);
    }
    int64_t n = 0;
    avro_value_get_long(val, &n);
    if (n == (int64_t)TSDB_DATA_BIGINT_NULL)
        return pos + sprintf(sql + pos, "NULL,");
    return pos + sprintf(sql + pos, "%" PRId64 ",", n);
}

static int32_t extractTagFloat(AvroFieldInfo *fi, avro_value_t *val,
                               char *sql, int32_t pos) {
    if (fi->nullable) {
        avro_value_t branch;
        avro_value_get_current_branch(val, &branch);
        if (avro_value_get_null(&branch) == 0)
            return pos + sprintf(sql + pos, "NULL,");
        float f = 0;
        avro_value_get_float(&branch, &f);
        return pos + sprintf(sql + pos, "%f,", f);
    }
    float f = 0;
    avro_value_get_float(val, &f);
    if (f == TSDB_DATA_FLOAT_NULL)
        return pos + sprintf(sql + pos, "NULL,");
    return pos + sprintf(sql + pos, "%f,", f);
}

static int32_t extractTagDouble(AvroFieldInfo *fi, avro_value_t *val,
                                char *sql, int32_t pos) {
    if (fi->nullable) {
        avro_value_t branch;
        avro_value_get_current_branch(val, &branch);
        if (avro_value_get_null(&branch) == 0)
            return pos + sprintf(sql + pos, "NULL,");
        double d = 0;
        avro_value_get_double(&branch, &d);
        return pos + sprintf(sql + pos, "%f,", d);
    }
    double d = 0;
    avro_value_get_double(val, &d);
    if (d == TSDB_DATA_DOUBLE_NULL)
        return pos + sprintf(sql + pos, "NULL,");
    return pos + sprintf(sql + pos, "%f,", d);
}

static int32_t extractTagTimestamp(AvroFieldInfo *fi, avro_value_t *val,
                                  char *sql, int32_t pos) {
    if (fi->nullable) {
        avro_value_t branch;
        avro_value_get_current_branch(val, &branch);
        if (avro_value_get_null(&branch) == 0)
            return pos + sprintf(sql + pos, "NULL,");
        int64_t n = 0;
        avro_value_get_long(&branch, &n);
        return pos + sprintf(sql + pos, "%" PRId64 ",", n);
    }
    int64_t n = 0;
    avro_value_get_long(val, &n);
    if (n == (int64_t)TSDB_DATA_BIGINT_NULL)
        return pos + sprintf(sql + pos, "NULL,");
    return pos + sprintf(sql + pos, "%" PRId64 ",", n);
}

static int32_t extractTagBool(avro_value_t *val, char *sql, int32_t pos) {
    avro_value_t branch;
    avro_value_get_current_branch(val, &branch);
    if (avro_value_get_null(&branch) == 0)
        return pos + sprintf(sql + pos, "NULL,");
    int32_t bl = 0;
    avro_value_get_boolean(&branch, &bl);
    return pos + sprintf(sql + pos, "%d,", bl ? 1 : 0);
}

// Append a possibly-quoted string value to SQL
static int32_t appendTagStr(char *buf, const char *val) {
    if (!val) return sprintf(buf, "NULL,");
    int32_t len = (int32_t)strlen(val);
    bool hasSingle = false, hasDouble = false;
    for (int i = 0; i < len; i++) {
        if (val[i] == '\'') hasSingle = true;
        if (val[i] == '"')  hasDouble = true;
    }
    if (!hasSingle) return sprintf(buf, "'%s',", val);
    if (!hasDouble) return sprintf(buf, "\"%s\",", val);
    // Escape single quotes by doubling
    int p = 0;
    buf[p++] = '\'';
    for (int i = 0; i < len; i++) {
        buf[p++] = val[i];
        if (val[i] == '\'') buf[p++] = '\'';
    }
    buf[p++] = '\'';
    buf[p++] = ',';
    buf[p]   = '\0';
    return p;
}

static int32_t extractTagBinary(AvroFieldInfo *fi, avro_value_t *val,
                                char *sql, int32_t pos) {
    avro_value_t branch;
    avro_value_get_current_branch(val, &branch);
    char *buf = NULL;
    size_t sz;
    avro_value_get_string(&branch, (const char **)&buf, &sz);
    if (!buf)
        return pos + sprintf(sql + pos, "NULL,");
    return pos + appendTagStr(sql + pos, buf);
}

static int32_t extractTagNChar(AvroFieldInfo *fi, avro_value_t *val,
                               char *sql, int32_t pos) {
    void *buf = NULL;
    size_t sz = 0;
    avro_value_t branch;
    avro_value_get_current_branch(val, &branch);
    avro_value_get_bytes(&branch, (const void **)&buf, &sz);
    if (!buf)
        return pos + sprintf(sql + pos, "NULL,");
    return pos + appendTagStr(sql + pos, (char *)buf);
}

// Unsigned types: encoded as AVRO arrays
static int32_t extractTagUnsigned(AvroFieldInfo *fi, avro_value_t *val,
                                  char *sql, int32_t pos, int bytes) {
    if (fi->nullable) {
        avro_value_t branch;
        avro_value_get_current_branch(val, &branch);
        if (avro_value_get_null(&branch) == 0)
            return pos + sprintf(sql + pos, "NULL,");

        if (bytes <= 4) {
            // utinyint/usmallint/uint: int array
            uint64_t sum = 0;
            size_t arrSz = 0;
            avro_value_get_size(&branch, &arrSz);
            for (size_t j = 0; j < arrSz; j++) {
                avro_value_t item;
                avro_value_get_by_index(&branch, j, &item, NULL);
                int32_t n = 0;
                avro_value_get_int(&item, &n);
                sum += (uint32_t)n;
            }
            if (bytes == 1)      return pos + sprintf(sql + pos, "%u,", (uint32_t)(uint8_t)sum);
            else if (bytes == 2) return pos + sprintf(sql + pos, "%u,", (uint32_t)(uint16_t)sum);
            else                 return pos + sprintf(sql + pos, "%u,", (uint32_t)sum);
        } else {
            // ubigint: long array
            uint64_t sum = 0;
            size_t arrSz = 0;
            avro_value_get_size(&branch, &arrSz);
            for (size_t j = 0; j < arrSz; j++) {
                avro_value_t item;
                avro_value_get_by_index(&branch, j, &item, NULL);
                int64_t n = 0;
                avro_value_get_long(&item, &n);
                sum += (uint64_t)n;
            }
            return pos + sprintf(sql + pos, "%" PRIu64 ",", sum);
        }
    }

    // Non-nullable unsigned
    if (bytes <= 4) {
        uint64_t sum = 0;
        size_t arrSz = 0;
        avro_value_get_size(val, &arrSz);
        for (size_t j = 0; j < arrSz; j++) {
            avro_value_t item;
            avro_value_get_by_index(val, j, &item, NULL);
            int32_t n = 0;
            avro_value_get_int(&item, &n);
            sum += (uint32_t)n;
        }
        if (bytes == 1) {
            if ((uint8_t)sum == TSDB_DATA_UTINYINT_NULL)
                return pos + sprintf(sql + pos, "NULL,");
            return pos + sprintf(sql + pos, "%u,", (uint32_t)(uint8_t)sum);
        } else if (bytes == 2) {
            if ((uint16_t)sum == TSDB_DATA_USMALLINT_NULL)
                return pos + sprintf(sql + pos, "NULL,");
            return pos + sprintf(sql + pos, "%u,", (uint32_t)(uint16_t)sum);
        } else {
            if ((uint32_t)sum == TSDB_DATA_UINT_NULL)
                return pos + sprintf(sql + pos, "NULL,");
            return pos + sprintf(sql + pos, "%u,", (uint32_t)sum);
        }
    } else {
        uint64_t sum = 0;
        size_t arrSz = 0;
        avro_value_get_size(val, &arrSz);
        for (size_t j = 0; j < arrSz; j++) {
            avro_value_t item;
            avro_value_get_by_index(val, j, &item, NULL);
            int64_t n = 0;
            avro_value_get_long(&item, &n);
            sum += (uint64_t)n;
        }
        if (sum == TSDB_DATA_UBIGINT_NULL)
            return pos + sprintf(sql + pos, "NULL,");
        return pos + sprintf(sql + pos, "%" PRIu64 ",", sum);
    }
}

// ==================== avroRestoreTbTags ====================

int64_t avroRestoreTbTags(AvroRestoreCtx *ctx, const char *dirPath,
                          const char *fileName, AvroDBChange *pDbChange,
                          bool isVirtual) {
    char avroFile[MAX_PATH_LEN];
    snprintf(avroFile, sizeof(avroFile), "%s/%s", dirPath, fileName);

    avro_file_reader_t reader;
    avro_schema_t schema;
    AvroRecordSchema *rs = avroGetSchemaFromFile(AVRO_TYPE_TBTAGS, avroFile,
                                                  &schema, &reader);
    if (!rs) return -1;

    // Track schema changes
    AvroStbChange *stbChange = NULL;
    int code = avroAddStbChanged(pDbChange, ctx->targetDb, ctx->conn,
                                  rs, &stbChange);
    if (code) {
        avroFreeRecordSchema(rs);
        avro_file_reader_close(reader);
        return code;
    }

    const char *partTags = "";
    if (stbChange && stbChange->strTags) partTags = stbChange->strTags;

    char *sqlstr = (char *)taosMemoryCalloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    if (!sqlstr) {
        avroFreeRecordSchema(rs);
        avro_file_reader_close(reader);
        return -1;
    }

    // Table des: prefer stbChange's version, else use rs->tableDes, else query server
    AvroTableDes *tableDes = stbChange ? stbChange->tableDes : NULL;
    AvroTableDes *mallocDes = NULL;
    if (!tableDes) {
        if (rs->tableDes) {
            tableDes = rs->tableDes;
        } else {
            mallocDes = avroAllocTableDes(TSDB_MAX_COLUMNS);
            if (!mallocDes) {
                taosMemoryFree(sqlstr);
                avroFreeRecordSchema(rs);
                avro_file_reader_close(reader);
                return -1;
            }
            tableDes = mallocDes;
        }
    }

    avro_value_iface_t *iface = avro_generic_class_from_schema(schema);
    avro_value_t value;
    avro_generic_value_new(iface, &value);

    int64_t success = 0, failed = 0;
    int tagAdjExt = ctx->looseMode ? 0 : 1;

    while (!avro_file_reader_read_value(reader, &value)) {
        // Check for embedded SQL (virtual tables)
        avro_value_t sqlVal, sqlBranch;
        if (avro_value_get_by_name(&value, "sql", &sqlVal, NULL) == 0) {
            avro_value_get_current_branch(&sqlVal, &sqlBranch);
            if (avro_value_get_null(&sqlBranch) != 0) {
                char *buf = NULL;
                size_t sz = 0;
                avro_value_get_string(&sqlBranch, (const char **)&buf, &sz);
                if (buf && sz > 0) {
                    // This is a virtual table SQL
                    if (!isVirtual) continue;  // skip in physical pass

                    // Apply rename if needed
                    char *renamed = avroAfterRenameSql(ctx, buf);
                    const char *execSql = renamed ? renamed : buf;

                    TAOS_RES *res = taos_query(ctx->conn, execSql);
                    if (taos_errno(res) != 0) {
                        logWarn("avro tbtags: query failed: %s, reason: %s",
                                execSql, taos_errstr(res));
                        failed++;
                    } else {
                        success++;
                    }
                    taos_free_result(res);
                    if (renamed) taosMemoryFree(renamed);
                    continue;
                }
            } else {
                // sql is NULL → this is a physical table record
                if (isVirtual) continue;
            }
        } else {
            if (isVirtual) continue;
        }

        // Build CREATE TABLE ... USING ... TAGS(...) SQL
        char *stbName = NULL;
        char *tbName  = NULL;
        int32_t sqlPos = 0;
        int n = 0;

        for (int i = 0; i < rs->numFields - tagAdjExt; i++) {
            avro_value_t fieldVal;

            if (i == 0) {
                // Extract stbName
                if (!ctx->looseMode) {
                    avro_value_t stbVal, stbBranch;
                    avro_value_get_by_name(&value, "stbname", &stbVal, NULL);
                    avro_value_get_current_branch(&stbVal, &stbBranch);
                    size_t sz;
                    avro_value_get_string(&stbBranch, (const char **)&stbName, &sz);
                } else {
                    stbName = (char *)taosMemoryCalloc(1, AVRO_TABLE_NAME_LEN);
                    // Parse stb name from filename: "stb.tb.avro-tbtags"
                    char *dup = taosStrdup(fileName);
                    char *run = dup;
                    char *tok = strsep(&run, ".");
                    if (tok) snprintf(stbName, AVRO_TABLE_NAME_LEN, "%s", tok);
                    taosMemoryFree(dup);
                }

                // Ensure we have a valid tableDes for this STB
                if (mallocDes && (strlen(tableDes->name) == 0
                        || strcmp(tableDes->name, stbName) != 0)) {
                    if (avroGetTableDes(ctx->conn, ctx->targetDb, stbName, mallocDes, false) < 0) {
                        if (mallocDes) avroFreeTableDes(mallocDes, true);
                        taosMemoryFree(sqlstr);
                        avroFreeRecordSchema(rs);
                        avro_value_decref(&value);
                        avro_value_iface_decref(iface);
                        avro_file_reader_close(reader);
                        return -1;
                    }
                }

                // Extract tbName
                avro_value_t tbVal, tbBranch;
                avro_value_get_by_name(&value, "tbname", &tbVal, NULL);
                avro_value_get_current_branch(&tbVal, &tbBranch);
                size_t tbSz;
                avro_value_get_string(&tbBranch, (const char **)&tbName, &tbSz);

                sqlPos = snprintf(sqlstr, TSDB_MAX_ALLOWED_SQL_LEN,
                    "CREATE TABLE IF NOT EXISTS `%s`.`%s` USING `%s`.`%s`%s TAGS(",
                    ctx->targetDb, tbName, ctx->targetDb, stbName, partTags);
            } else {
                AvroFieldInfo *fi = &rs->fields[i + tagAdjExt];
                if (fi->name[0] && strcasecmp(fi->name, "sql") == 0) continue;

                // Schema evolution: skip tags not on server
                int16_t idx = (int16_t)(i - 1);
                if (stbChange && stbChange->schemaChanged) {
                    if (!avroIdxInBindTags(idx, tableDes)) continue;
                }

                if (avro_value_get_by_name(&value, fi->name, &fieldVal, NULL) != 0) {
                    logError("avro tbtags: failed to get field: %s", fi->name);
                    continue;
                }

                int colType = tableDes->cols[tableDes->columns + n].type;
                switch (colType) {
                    case TSDB_DATA_TYPE_BOOL:
                        sqlPos = extractTagBool(&fieldVal, sqlstr, sqlPos);
                        break;
                    case TSDB_DATA_TYPE_TINYINT:
                        sqlPos = extractTagTinyInt(fi, &fieldVal, sqlstr, sqlPos);
                        break;
                    case TSDB_DATA_TYPE_SMALLINT:
                        sqlPos = extractTagSmallInt(fi, &fieldVal, sqlstr, sqlPos);
                        break;
                    case TSDB_DATA_TYPE_INT:
                        sqlPos = extractTagInt(fi, &fieldVal, sqlstr, sqlPos);
                        break;
                    case TSDB_DATA_TYPE_BIGINT:
                        sqlPos = extractTagBigInt(fi, &fieldVal, sqlstr, sqlPos);
                        break;
                    case TSDB_DATA_TYPE_FLOAT:
                        sqlPos = extractTagFloat(fi, &fieldVal, sqlstr, sqlPos);
                        break;
                    case TSDB_DATA_TYPE_DOUBLE:
                        sqlPos = extractTagDouble(fi, &fieldVal, sqlstr, sqlPos);
                        break;
                    case TSDB_DATA_TYPE_BINARY:
                    case TSDB_DATA_TYPE_DECIMAL:
                    case TSDB_DATA_TYPE_DECIMAL64:
                        sqlPos = extractTagBinary(fi, &fieldVal, sqlstr, sqlPos);
                        break;
                    case TSDB_DATA_TYPE_NCHAR:
                    case TSDB_DATA_TYPE_JSON:
                    case TSDB_DATA_TYPE_VARBINARY:
                    case TSDB_DATA_TYPE_GEOMETRY:
                    case TSDB_DATA_TYPE_BLOB:
                        sqlPos = extractTagNChar(fi, &fieldVal, sqlstr, sqlPos);
                        break;
                    case TSDB_DATA_TYPE_TIMESTAMP:
                        sqlPos = extractTagTimestamp(fi, &fieldVal, sqlstr, sqlPos);
                        break;
                    case TSDB_DATA_TYPE_UTINYINT:
                        sqlPos = extractTagUnsigned(fi, &fieldVal, sqlstr, sqlPos, 1);
                        break;
                    case TSDB_DATA_TYPE_USMALLINT:
                        sqlPos = extractTagUnsigned(fi, &fieldVal, sqlstr, sqlPos, 2);
                        break;
                    case TSDB_DATA_TYPE_UINT:
                        sqlPos = extractTagUnsigned(fi, &fieldVal, sqlstr, sqlPos, 4);
                        break;
                    case TSDB_DATA_TYPE_UBIGINT:
                        sqlPos = extractTagUnsigned(fi, &fieldVal, sqlstr, sqlPos, 8);
                        break;
                    default:
                        logError("avro tbtags: unknown tag type: %d", colType);
                        break;
                }
                n++;
            }

            if (sqlPos > TSDB_MAX_ALLOWED_SQL_LEN - 128) {
                logError("avro tbtags: SQL too long (%d)", sqlPos);
                break;
            }
        }

        // Close TAGS(...) — replace trailing comma with )
        sprintf(sqlstr + sqlPos - 1, ")");

        if (ctx->looseMode && stbName) {
            taosMemoryFree(stbName);
            stbName = NULL;
        }

        // Execute
        TAOS_RES *res = taos_query(ctx->conn, sqlstr);
        if (taos_errno(res) != 0) {
            logWarn("avro tbtags: create child table failed: %s, reason: %s",
                    sqlstr, taos_errstr(res));
            failed++;
        } else {
            success++;
        }
        taos_free_result(res);
    }

    avro_value_decref(&value);
    avro_value_iface_decref(iface);
    if (mallocDes) avroFreeTableDes(mallocDes, true);
    taosMemoryFree(sqlstr);
    avroFreeRecordSchema(rs);
    avro_file_reader_close(reader);

    return (failed > 0) ? -1 : success;
}

// ==================== avroRestoreNtb ====================

int64_t avroRestoreNtb(AvroRestoreCtx *ctx, const char *dirPath,
                       const char *fileName, AvroDBChange *pDbChange,
                       bool isVirtual) {
    char avroFile[MAX_PATH_LEN];
    snprintf(avroFile, sizeof(avroFile), "%s/%s", dirPath, fileName);

    avro_file_reader_t reader;
    avro_schema_t schema;
    AvroRecordSchema *rs = avroGetSchemaFromFile(AVRO_TYPE_NTB, avroFile,
                                                  &schema, &reader);
    if (!rs) return -1;

    // Register in schema tracking
    avroAddStbChanged(pDbChange, ctx->targetDb, ctx->conn, rs, NULL);

    avro_value_iface_t *iface = avro_generic_class_from_schema(schema);
    avro_value_t value;
    avro_generic_value_new(iface, &value);

    int64_t success = 0, failed = 0;

    while (!avro_file_reader_read_value(reader, &value)) {
        avro_value_t sqlVal, sqlBranch;
        avro_value_get_by_name(&value, "sql", &sqlVal, NULL);

        char *buf = NULL;
        size_t sz = 0;
        avro_value_get_current_branch(&sqlVal, &sqlBranch);
        avro_value_get_string(&sqlBranch, (const char **)&buf, &sz);

        if (!buf || sz == 0) continue;

        // Check virtual table
        const char *vtPrefix = "CREATE VTABLE";
        bool isVTable = (strncasecmp(buf, vtPrefix, strlen(vtPrefix)) == 0);
        if (isVTable != isVirtual) continue;

        // Apply rename
        char *renamed = avroAfterRenameSql(ctx, buf);
        const char *execSql = renamed ? renamed : buf;

        TAOS_RES *res = taos_query(ctx->conn, execSql);
        if (taos_errno(res) != 0) {
            logError("avro ntb: create table failed: %s, reason: %s",
                     execSql, taos_errstr(res));
            failed++;
        } else {
            success++;
        }
        taos_free_result(res);
        if (renamed) taosMemoryFree(renamed);
    }

    avro_value_decref(&value);
    avro_value_iface_decref(iface);
    avroFreeRecordSchema(rs);
    avro_file_reader_close(reader);

    return (failed > 0) ? -(int64_t)failed : success;
}
