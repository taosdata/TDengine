/*
 * Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#include "compatAvroSchema.h"
#include "compatAvroUtil.h"
#include "compatAvro.h"
#include "avroSqlBuf.h"
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
#include "bckProgress.h"

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

// Append a tag value as a quoted SQL literal (see avroSqlBuf.h): returns the new
// position, or -1 when the value does not fit in `cap` bytes.
static inline int32_t appendTagStr(char *sql, int32_t pos, int32_t cap,
                                   const char *val, size_t len) {
    return avroSqlAppendTagValue(sql, cap, pos, val, len);
}

static int32_t extractTagBinary(AvroFieldInfo *fi, avro_value_t *val,
                                char *sql, int32_t pos, int32_t cap) {
    avro_value_t branch;
    avro_value_get_current_branch(val, &branch);
    if (avro_value_get_null(&branch) == 0)
        return pos + sprintf(sql + pos, "NULL,");
    char *buf = NULL;
    size_t sz = 0;
    avro_value_get_string(&branch, (const char **)&buf, &sz);
    if (!buf)
        return pos + sprintf(sql + pos, "NULL,");
    // `sz` comes from the file and the buffer is not necessarily NUL-terminated.
    return appendTagStr(sql, pos, cap, buf, strnlen(buf, sz));
}

static int32_t extractTagNChar(AvroFieldInfo *fi, avro_value_t *val,
                               char *sql, int32_t pos, int32_t cap) {
    avro_value_t branch;
    avro_value_get_current_branch(val, &branch);
    if (avro_value_get_null(&branch) == 0)
        return pos + sprintf(sql + pos, "NULL,");
    void *buf = NULL;
    size_t sz = 0;
    avro_value_get_bytes(&branch, (const void **)&buf, &sz);
    if (!buf)
        return pos + sprintf(sql + pos, "NULL,");
    // `sz` comes from the file and the buffer is not necessarily NUL-terminated.
    return appendTagStr(sql, pos, cap, (char *)buf, strnlen((char *)buf, sz));
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

// Helper: extract a single tag value by column type and append to SQL.
// Returns the new SQL position, or -1 when the value does not fit in `cap`.
static int32_t extractTagByType(AvroFieldInfo *fi, avro_value_t *fieldVal,
                                char *sqlstr, int32_t sqlPos, int colType,
                                int32_t cap) {
    // A previous value may already have failed to fit (sqlPos == -1); the
    // numeric branches below write with sprintf(), so bail out before any
    // append is attempted at an invalid position.
    if (sqlPos < 0 || sqlPos >= cap) return -1;

    switch (colType) {
        case TSDB_DATA_TYPE_BOOL:
            return extractTagBool(fieldVal, sqlstr, sqlPos);
        case TSDB_DATA_TYPE_TINYINT:
            return extractTagTinyInt(fi, fieldVal, sqlstr, sqlPos);
        case TSDB_DATA_TYPE_SMALLINT:
            return extractTagSmallInt(fi, fieldVal, sqlstr, sqlPos);
        case TSDB_DATA_TYPE_INT:
            return extractTagInt(fi, fieldVal, sqlstr, sqlPos);
        case TSDB_DATA_TYPE_BIGINT:
            return extractTagBigInt(fi, fieldVal, sqlstr, sqlPos);
        case TSDB_DATA_TYPE_FLOAT:
            return extractTagFloat(fi, fieldVal, sqlstr, sqlPos);
        case TSDB_DATA_TYPE_DOUBLE:
            return extractTagDouble(fi, fieldVal, sqlstr, sqlPos);
        case TSDB_DATA_TYPE_BINARY:
        case TSDB_DATA_TYPE_DECIMAL:
        case TSDB_DATA_TYPE_DECIMAL64:
            return extractTagBinary(fi, fieldVal, sqlstr, sqlPos, cap);
        case TSDB_DATA_TYPE_NCHAR:
        case TSDB_DATA_TYPE_JSON:
        case TSDB_DATA_TYPE_VARBINARY:
        case TSDB_DATA_TYPE_GEOMETRY:
        case TSDB_DATA_TYPE_BLOB:
        case TSDB_DATA_TYPE_MEDIUMBLOB:
            return extractTagNChar(fi, fieldVal, sqlstr, sqlPos, cap);
        case TSDB_DATA_TYPE_TIMESTAMP:
            return extractTagTimestamp(fi, fieldVal, sqlstr, sqlPos);
        case TSDB_DATA_TYPE_UTINYINT:
            return extractTagUnsigned(fi, fieldVal, sqlstr, sqlPos, 1);
        case TSDB_DATA_TYPE_USMALLINT:
            return extractTagUnsigned(fi, fieldVal, sqlstr, sqlPos, 2);
        case TSDB_DATA_TYPE_UINT:
            return extractTagUnsigned(fi, fieldVal, sqlstr, sqlPos, 4);
        case TSDB_DATA_TYPE_UBIGINT:
            return extractTagUnsigned(fi, fieldVal, sqlstr, sqlPos, 8);
        default:
            logError("avro tbtags: unknown tag type: %d", colType);
            return sqlPos + sprintf(sqlstr + sqlPos, "NULL,");
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
        avro_schema_decref(schema);
        avro_file_reader_close(reader);
        return code;
    }

    const char *partTags = "";
    if (stbChange && stbChange->strTags) partTags = stbChange->strTags;

    char *sqlstr = (char *)taosMemoryCalloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    if (!sqlstr) {
        avroFreeRecordSchema(rs);
        avro_schema_decref(schema);
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
                avro_schema_decref(schema);
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
        atomic_add_fetch_64(&g_progress.ctbDoneCur, 1);

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

                    logDebug("avro tbtags: execute SQL: %s", execSql);
                    TAOS_RES *res = taos_query(ctx->conn, execSql);
                    if (taos_errno(res) != 0) {
                        logWarn("avro tbtags: query failed(0x%08X): %s, reason: %s",
                                taos_errno(res), execSql, taos_errstr(res));
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
                    // Parse stb name from filename: "db.stb.seq.avro-tbtags"
                    char *dup = taosStrdup(fileName);
                    char *run = dup;
                    strsep(&run, ".");  // skip db name
                    char *tok = strsep(&run, ".");
                    if (tok) snprintf(stbName, AVRO_TABLE_NAME_LEN, "%s", tok);
                    taosMemoryFree(dup);
                }

                // Ensure we have a valid tableDes for this STB
                if (mallocDes && (strlen(tableDes->name) == 0
                        || strcmp(tableDes->name, stbName) != 0)) {
                    if (avroGetTableDes(ctx->conn, ctx->targetDb, stbName, mallocDes,
                                        TSDB_MAX_COLUMNS, false) < 0) {
                        if (ctx->looseMode && stbName) taosMemoryFree(stbName);
                        if (mallocDes) avroFreeTableDes(mallocDes, true);
                        taosMemoryFree(sqlstr);
                        avroFreeRecordSchema(rs);
                        avro_value_decref(&value);
                        avro_value_iface_decref(iface);
                        avro_schema_decref(schema);
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

                int hdrLen = snprintf(sqlstr, TSDB_MAX_ALLOWED_SQL_LEN,
                    "CREATE TABLE IF NOT EXISTS `%s`.`%s` USING `%s`.`%s`%s TAGS(",
                    ctx->targetDb, tbName, ctx->targetDb, stbName, partTags);
                // snprintf() returns the length that *would* have been written,
                // so a truncated header must be rejected before anything else is
                // appended at that position.
                if (hdrLen < 0
                        || hdrLen >= (int32_t)TSDB_MAX_ALLOWED_SQL_LEN - AVRO_SQL_TAIL_RESERVE) {
                    logError("avro tbtags: table name too long for SQL buffer: `%s`.`%s`",
                             ctx->targetDb, tbName);
                    sqlPos = -1;
                    break;
                }
                sqlPos = hdrLen;
            } else {
                AvroFieldInfo *fi = &rs->fields[i + tagAdjExt];
                if (fi->name[0] && strcasecmp(fi->name, "sql") == 0) continue;

                // Schema evolution: skip tags not on server
                int16_t idx = (int16_t)(i - 1);
                if (stbChange && stbChange->schemaChanged && stbChange->tagBindMap) {
                    // Skip: tag values will be emitted in server order below
                    continue;
                }

                if (avro_value_get_by_name(&value, fi->name, &fieldVal, NULL) != 0) {
                    logError("avro tbtags: failed to get field: %s", fi->name);
                    continue;
                }

                int colType = tableDes->cols[tableDes->columns + n].type;
                int32_t newPos = extractTagByType(fi, &fieldVal, sqlstr, sqlPos, colType,
                                                  TSDB_MAX_ALLOWED_SQL_LEN);
                if (newPos < 0) {
                    logError("avro tbtags: tag `%s` too long for SQL buffer (%d bytes used), "
                             "skipping table", fi->name, sqlPos);
                    sqlPos = -1;
                    break;
                }
                sqlPos = newPos;
                n++;
            }

            if (sqlPos > TSDB_MAX_ALLOWED_SQL_LEN - 128) {
                logError("avro tbtags: SQL too long (%d)", sqlPos);
                break;
            }
        }

        // When tags have schema changes, emit values in server order.  A
        // negative position means the first pass already failed, so nothing
        // more may be appended.
        if (sqlPos >= 0 && stbChange && stbChange->schemaChanged && stbChange->tagBindMap
                && stbChange->serverTagMap) {
            for (int tn = 0; tn < stbChange->matchedTags; tn++) {
                int serverTagIdx = stbChange->serverTagMap[tn];
                int serverColIdx = tableDes->columns + serverTagIdx;
                const char *tagName = tableDes->cols[serverColIdx].field;
                int colType = tableDes->cols[serverColIdx].type;

                // Find the matching AVRO field by name
                AvroFieldInfo *fi = NULL;
                for (int j = 0; j < rs->numFields; j++) {
                    if (strcasecmp(rs->fields[j].name, tagName) == 0) {
                        fi = &rs->fields[j];
                        break;
                    }
                }

                avro_value_t fieldVal;
                if (fi && avro_value_get_by_name(&value, fi->name, &fieldVal, NULL) == 0) {
                    int32_t newPos = extractTagByType(fi, &fieldVal, sqlstr, sqlPos, colType,
                                                      TSDB_MAX_ALLOWED_SQL_LEN);
                    if (newPos < 0) {
                        logError("avro tbtags: tag `%s` too long for SQL buffer (%d bytes used), "
                                 "skipping table", tagName, sqlPos);
                        sqlPos = -1;
                        break;
                    }
                    sqlPos = newPos;
                } else {
                    if (sqlPos > (int32_t)TSDB_MAX_ALLOWED_SQL_LEN - AVRO_SQL_TAIL_RESERVE - 8) {
                        logError("avro tbtags: SQL buffer exhausted before tag `%s`", tagName);
                        sqlPos = -1;
                        break;
                    }
                    sqlPos += sprintf(sqlstr + sqlPos, "NULL,");
                }
            }
        }

        // A negative or oversized position means a value did not fit (or the
        // header was truncated): reject the record instead of writing outside
        // sqlstr.
        if (sqlPos < 2 || sqlPos > (int32_t)TSDB_MAX_ALLOWED_SQL_LEN - AVRO_SQL_TAIL_RESERVE) {
            logError("avro tbtags: SQL statement too long or truncated (%d bytes), skipping table",
                     sqlPos);
            if (ctx->looseMode && stbName) {
                taosMemoryFree(stbName);
                stbName = NULL;
            }
            failed++;
            continue;
        }

        // Close TAGS(...) — replace trailing comma with )
        sqlstr[sqlPos - 1] = ')';
        sqlstr[sqlPos] = '\0';

        if (ctx->looseMode && stbName) {
            taosMemoryFree(stbName);
            stbName = NULL;
        }

        // Execute
        logDebug("avro tbtags: execute SQL: %s", sqlstr);
        TAOS_RES *res = taos_query(ctx->conn, sqlstr);
        if (taos_errno(res) != 0) {
            logWarn("avro tbtags: create child table failed(0x%08X): %s, reason: %s",
                    taos_errno(res), sqlstr, taos_errstr(res));
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
    avro_schema_decref(schema);
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

    avro_value_iface_t *iface = avro_generic_class_from_schema(schema);
    avro_value_t value;
    avro_generic_value_new(iface, &value);

    int64_t success = 0, failed = 0;

    while (!avro_file_reader_read_value(reader, &value)) {
        atomic_add_fetch_64(&g_progress.ctbDoneCur, 1);

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

        logDebug("avro ntb: execute SQL: %s", execSql);
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

    if (success > 0) {
        int code = avroAddStbChanged(pDbChange, ctx->targetDb, ctx->conn, rs, NULL);
        if (code != 0) {
            // Tables are already created; the data phase falls back to an
            // on-the-fly DESCRIBE when no stbChange is tracked, so this must
            // not count as a failed restore.
            logWarn("avro ntb: schema pre-check failed for %s (code=0x%08X), "
                    "schema will be checked again during data restore",
                    rs->name, code);
        }
    }

    avro_value_decref(&value);
    avro_value_iface_decref(iface);
    avroFreeRecordSchema(rs);
    avro_schema_decref(schema);
    avro_file_reader_close(reader);

    return (failed > 0) ? -(int64_t)failed : success;
}
