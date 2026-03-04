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
    
#include "restoreMeta.h"
#include "storageTaos.h"
#include "colCompress.h"
#include "blockReader.h"
#include "bckPool.h"
#include "bckDb.h"
#include "parquetBlock.h"
#include "bckArgs.h"
#include "ttypes.h"
#include "osString.h"

//
// -------------------------------------- UTIL -----------------------------------------
//

// execute a SQL string on the given connection
static int execSql(TAOS *conn, const char *sql) {
    TAOS_RES *res = taos_query(conn, sql);
    int code = taos_errno(res);
    if (code != TSDB_CODE_SUCCESS) {
        logError("exec sql failed(%s): %s", taos_errstr(res), sql);
    }
    if (res) taos_free_result(res);
    return code;
}

// read entire file content into a malloc'd buffer (caller frees)
static char* readFileContent(const char *filePath, int *outLen) {
    TdFilePtr fp = taosOpenFile(filePath, TD_FILE_READ);
    if (fp == NULL) {
        logError("open file failed: %s", filePath);
        return NULL;
    }

    // get file size
    int64_t fileSize = 0;
    if (taosFStatFile(fp, &fileSize, NULL) != 0 || fileSize <= 0) {
        // try read up to 1MB
        fileSize = 1024 * 1024;
    }

    char *buf = (char *)taosMemoryMalloc(fileSize + 1);
    if (buf == NULL) {
        logError("malloc failed for file content: %s", filePath);
        taosCloseFile(&fp);
        return NULL;
    }

    int64_t readLen = taosReadFile(fp, buf, fileSize);
    taosCloseFile(&fp);

    if (readLen <= 0) {
        logError("read file content failed: %s", filePath);
        taosMemoryFree(buf);
        return NULL;
    }

    buf[readLen] = '\0';
    if (outLen) *outLen = (int)readLen;
    return buf;
}


//
// -------------------------------------- META: DB SQL -----------------------------------------
//

// restore create database SQL
static int restoreDbSql(const char *dbName) {
    int code = TSDB_CODE_FAILED;
    const char *targetDb = argRenameDb(dbName);
    
    char sqlFile[MAX_PATH_LEN] = {0};
    obtainFileName(BACK_FILE_DBSQL, dbName, NULL, NULL, 0, 0, BINARY_TAOS, sqlFile, sizeof(sqlFile));

    if (!taosCheckExistFile(sqlFile)) {
        logWarn("db.sql not found, skip: %s", sqlFile);
        return TSDB_CODE_SUCCESS;
    }

    char *content = readFileContent(sqlFile, NULL);
    if (content == NULL) {
        logError("read db.sql failed: %s", sqlFile);
        return TSDB_CODE_BCK_READ_FILE_FAILED;
    }

    // trim trailing newline/space
    int len = strlen(content);
    while (len > 0 && (content[len-1] == '\n' || content[len-1] == '\r' || content[len-1] == ' ')) {
        content[--len] = '\0';
    }

    // if rename is configured, replace old db name with new in SQL
    // SQL format: CREATE DATABASE `oldDb` ...
    char *execContent = content;
    char *renamedSql = NULL;
    if (strcmp(targetDb, dbName) != 0) {
        // build search pattern: `oldDb`
        char oldPat[TSDB_DB_NAME_LEN + 4];
        char newPat[TSDB_DB_NAME_LEN + 4];
        snprintf(oldPat, sizeof(oldPat), "`%s`", dbName);
        snprintf(newPat, sizeof(newPat), "`%s`", targetDb);
        char *pos = strstr(content, oldPat);
        if (pos) {
            int oldLen = strlen(oldPat);
            int newLen = strlen(newPat);
            int totalLen = len - oldLen + newLen + 1;
            renamedSql = (char *)taosMemoryMalloc(totalLen);
            if (renamedSql) {
                int prefixLen = (int)(pos - content);
                memcpy(renamedSql, content, prefixLen);
                memcpy(renamedSql + prefixLen, newPat, newLen);
                memcpy(renamedSql + prefixLen + newLen, pos + oldLen, len - prefixLen - oldLen);
                renamedSql[totalLen - 1] = '\0';
                execContent = renamedSql;
            }
        }
        logInfo("rename database: %s -> %s", dbName, targetDb);
    }

    logInfo("restore db sql: %s", execContent);

    TAOS *conn = getConnection();
    if (conn == NULL) {
        taosMemoryFree(content);
        if (renamedSql) taosMemoryFree(renamedSql);
        return TSDB_CODE_BCK_CONN_POOL_EXHAUSTED;
    }

    // execute - if database exists, this may fail, which is fine (APPEND mode)
    TAOS_RES *res = taos_query(conn, execContent);
    code = taos_errno(res);
    if (res) taos_free_result(res);

    if (code != TSDB_CODE_SUCCESS) {
        // try USE database instead - database may already exist
        logWarn("create database may already exist (code=0x%08X), trying USE %s", code, targetDb);
        char useSql[256];
        snprintf(useSql, sizeof(useSql), "USE `%s`", targetDb);
        code = execSql(conn, useSql);
    }

    releaseConnection(conn);
    taosMemoryFree(content);
    if (renamedSql) taosMemoryFree(renamedSql);
    return code;
}


//
// -------------------------------------- META: STB SQL -----------------------------------------
//

// restore create stable SQL  
static int restoreStbSql(const char *dbName) {
    int code = TSDB_CODE_FAILED;
    
    char sqlFile[MAX_PATH_LEN] = {0};
    obtainFileName(BACK_FILE_STBSQL, dbName, NULL, NULL, 0, 0, BINARY_TAOS, sqlFile, sizeof(sqlFile));

    if (!taosCheckExistFile(sqlFile)) {
        logWarn("stb.sql not found, skip: %s", sqlFile);
        return TSDB_CODE_SUCCESS;
    }

    char *content = readFileContent(sqlFile, NULL);
    if (content == NULL) {
        logError("read stb.sql failed: %s", sqlFile);
        return TSDB_CODE_BCK_READ_FILE_FAILED;
    }

    TAOS *conn = getConnection();
    if (conn == NULL) {
        taosMemoryFree(content);
        return TSDB_CODE_BCK_CONN_POOL_EXHAUSTED;
    }

    // stb.sql may contain multiple CREATE STABLE statements separated by newlines
    // each line is a CREATE STABLE SQL from "SHOW CREATE TABLE"
    char *line = content;
    char *next = NULL;
    code = TSDB_CODE_SUCCESS;

    while (line && *line) {
        if (g_interrupted) {
            code = TSDB_CODE_BCK_USER_CANCEL;
            break;
        }

        // find next line
        next = strchr(line, '\n');
        if (next) {
            *next = '\0';
            next++;
        }

        // trim
        while (*line == ' ' || *line == '\r') line++;
        int lineLen = strlen(line);
        while (lineLen > 0 && (line[lineLen-1] == ' ' || line[lineLen-1] == '\r')) {
            line[--lineLen] = '\0';
        }

        if (lineLen > 0) {
            // Add database prefix: "CREATE STABLE `tbl`" -> "CREATE STABLE `db`.`tbl`"
            char *fullSql = (char *)taosMemoryMalloc(TSDB_MAX_SQL_LEN);
            if (fullSql == NULL) {
                code = TSDB_CODE_BCK_MALLOC_FAILED;
                break;
            }

            // Find the STABLE keyword and the backtick-quoted table name after it
            char *stablePos = strstr(line, "STABLE");
            if (stablePos) {
                char *nameStart = stablePos + strlen("STABLE");
                while (*nameStart == ' ') nameStart++;
                // nameStart now points to "`tableName`"
                int prefixLen = nameStart - line;
                const char *targetDb = argRenameDb(dbName);
                snprintf(fullSql, TSDB_MAX_SQL_LEN, "%.*s`%s`.%s",
                         prefixLen, line, targetDb, nameStart);
            } else {
                snprintf(fullSql, TSDB_MAX_SQL_LEN, "%s", line);
            }

            logInfo("restore stb sql: %s", fullSql);
            // execute - if stable already exists, it's ok
            TAOS_RES *res = taos_query(conn, fullSql);
            int rc = taos_errno(res);
            if (res) taos_free_result(res);
            if (rc == TSDB_CODE_SUCCESS) {
                // ok
            } else if (rc == TSDB_CODE_MND_STB_ALREADY_EXIST) {
                logWarn("stable already exists (0x%08X): %s", rc, fullSql);
                // fine in APPEND mode
            } else {
                logError("create stable failed (0x%08X): %s", rc, fullSql);
                code = rc;
            }
            taosMemoryFree(fullSql);
        }

        line = next;
    }

    releaseConnection(conn);
    taosMemoryFree(content);
    return code;
}


//
// -------------------------------------- META: NTB SQL -----------------------------------------
//

// restore normal table create SQL
static int restoreNtbSql(const char *dbName) {
    int code = TSDB_CODE_SUCCESS;

    char sqlFile[MAX_PATH_LEN] = {0};
    obtainFileName(BACK_FILE_NTBSQL, dbName, NULL, NULL, 0, 0, BINARY_TAOS, sqlFile, sizeof(sqlFile));

    if (!taosCheckExistFile(sqlFile)) {
        logInfo("ntb.sql not found, skip normal tables: %s", sqlFile);
        return TSDB_CODE_SUCCESS;
    }

    char *content = readFileContent(sqlFile, NULL);
    if (content == NULL) {
        logError("read ntb.sql failed: %s", sqlFile);
        return TSDB_CODE_BCK_READ_FILE_FAILED;
    }

    TAOS *conn = getConnection();
    if (conn == NULL) {
        taosMemoryFree(content);
        return TSDB_CODE_BCK_CONN_POOL_EXHAUSTED;
    }

    // ntb.sql contains CREATE TABLE statements, one per line
    char *line = content;
    char *next = NULL;
    int ntbCount = 0;

    while (line && *line) {
        if (g_interrupted) {
            code = TSDB_CODE_BCK_USER_CANCEL;
            break;
        }

        next = strchr(line, '\n');
        if (next) {
            *next = '\0';
            next++;
        }

        // trim
        while (*line == ' ' || *line == '\r') line++;
        int lineLen = strlen(line);
        while (lineLen > 0 && (line[lineLen-1] == ' ' || line[lineLen-1] == '\r')) {
            line[--lineLen] = '\0';
        }

        if (lineLen > 0) {
            char *fullSql = (char *)taosMemoryMalloc(TSDB_MAX_SQL_LEN);
            if (fullSql == NULL) {
                code = TSDB_CODE_BCK_MALLOC_FAILED;
                break;
            }

            // Add database prefix: "CREATE TABLE `tbl`" -> "CREATE TABLE `db`.`tbl`"
            char *tablePos = strstr(line, "TABLE");
            if (tablePos) {
                char *nameStart = tablePos + strlen("TABLE");
                while (*nameStart == ' ') nameStart++;
                int prefixLen = nameStart - line;
                const char *targetDb = argRenameDb(dbName);
                snprintf(fullSql, TSDB_MAX_SQL_LEN, "%.*s`%s`.%s",
                         prefixLen, line, targetDb, nameStart);
            } else {
                snprintf(fullSql, TSDB_MAX_SQL_LEN, "%s", line);
            }

            TAOS_RES *res = taos_query(conn, fullSql);
            int rc = taos_errno(res);
            if (res) taos_free_result(res);

            if (rc == TSDB_CODE_SUCCESS) {
                ntbCount++;
                atomic_add_fetch_64(&g_stats.ntbTotal, 1);
            } else if (rc == TSDB_CODE_TDB_TABLE_ALREADY_EXIST || rc == TSDB_CODE_MND_STB_ALREADY_EXIST) {
                logWarn("normal table already exists, skip: %s", fullSql);
                ntbCount++;
                atomic_add_fetch_64(&g_stats.ntbTotal, 1);
            } else {
                logError("create normal table failed(0x%08X): %s", rc, fullSql);
                code = rc;
            }
            taosMemoryFree(fullSql);
        }

        line = next;
    }

    logInfo("restored %d normal table(s) for db: %s", ntbCount, dbName);
    releaseConnection(conn);
    taosMemoryFree(content);
    return code;
}



//
// Context for tag block callback - builds CREATE TABLE SQL from raw block data
//
typedef struct {
    TAOS       *conn;
    const char *dbName;
    const char *stbName;
    FieldInfo  *fieldInfos;
    int         numFields;
    int64_t     successCnt;
    int64_t     failedCnt;
} TagRestoreCtx;

//
// Build value string for a single column from raw block data
// Returns pointer past the end of written data in buf
//
static char* appendValueFromBlock(char *buf, int bufRemain,
                                  BlockReader *reader, 
                                  FieldInfo *fieldInfo,
                                  int colIdx, int rowIdx,
                                  int blockRows) {
    // We need to read the column data for the given row
    // The raw block layout: oriBlockHeader + schema + col_lengths + col_data...
    // We already have the reader positioned at the start

    // For simplicity, we'll use a different approach:
    // Parse the raw block directly using the known layout
    // This function is called per-row, per-column from the block callback
    
    // placeholder - actual implementation below in the block callback
    return buf;
}

//
// Callback for processing each decompressed tag block
// Builds "CREATE TABLE IF NOT EXISTS `db`.`childTb` USING `db`.`stb` TAGS(...)" SQL
//
static int tagBlockCallback(void *userData, 
                            FieldInfo *fieldInfos, 
                            int numFields,
                            void *blockData, 
                            int32_t blockLen, 
                            int32_t blockRows) {
    TagRestoreCtx *ctx = (TagRestoreCtx *)userData;
    
    // Parse block using BlockReader
    BlockReader reader;
    int32_t code = initBlockReader(&reader, blockData);
    if (code != TSDB_CODE_SUCCESS) {
        logError("init block reader failed for tags: %d", code);
        return code;
    }

    // The tag file schema: first column is "tbname", rest are tag columns
    // We need to read all column data pointers first
    int numCols = reader.oriHeader->numOfCols;
    
    // Collect column data pointers and lengths
    void  **colDataPtrs = (void **)taosMemoryCalloc(numCols, sizeof(void *));
    int32_t *colDataLens = (int32_t *)taosMemoryCalloc(numCols, sizeof(int32_t));
    if (!colDataPtrs || !colDataLens) {
        taosMemoryFree(colDataPtrs);
        taosMemoryFree(colDataLens);
        return TSDB_CODE_BCK_MALLOC_FAILED;
    }

    for (int c = 0; c < numCols; c++) {
        code = getColumnData(&reader, fieldInfos[c].type, &colDataPtrs[c], &colDataLens[c]);
        if (code != TSDB_CODE_SUCCESS) {
            logError("get column data failed: col=%d", c);
            taosMemoryFree(colDataPtrs);
            taosMemoryFree(colDataLens);
            return code;
        }
    }

    // Process each row
    for (int row = 0; row < blockRows; row++) {
        if (g_interrupted) {
            taosMemoryFree(colDataPtrs);
            taosMemoryFree(colDataLens);
            return TSDB_CODE_BCK_USER_CANCEL;
        }

        // Column 0 = tbname (VARCHAR type)
        // Read tbname from variable-length column
        char tbName[TSDB_TABLE_NAME_LEN] = {0};

        // Variable column layout: offsets[rows] + data
        // offsets is int32_t array, offset=-1 means NULL
        int32_t *offsets = (int32_t *)colDataPtrs[0];
        int32_t offset = offsets[row];
        if (offset < 0) {
            logWarn("tbname is NULL at row %d, skip", row);
            ctx->failedCnt++;
            continue;
        }
        
        // data starts after offsets array
        char *varData = (char *)colDataPtrs[0] + blockRows * sizeof(int32_t);
        // TDengine variable data format: 2-byte length prefix + data  
        uint16_t varLen = *(uint16_t *)(varData + offset);
        if (varLen >= TSDB_TABLE_NAME_LEN) varLen = TSDB_TABLE_NAME_LEN - 1;
        memcpy(tbName, varData + offset + sizeof(uint16_t), varLen);
        tbName[varLen] = '\0';

        // Build SQL: CREATE TABLE IF NOT EXISTS `db`.`tb` USING `db`.`stb` TAGS(v1, v2, ...)
        char *sql = (char *)taosMemoryMalloc(TSDB_MAX_SQL_LEN);
        if (sql == NULL) {
            ctx->failedCnt++;
            continue;
        }

        int pos = snprintf(sql, TSDB_MAX_SQL_LEN,
                           "CREATE TABLE IF NOT EXISTS `%s`.`%s` USING `%s`.`%s` TAGS(",
                           argRenameDb(ctx->dbName), tbName, argRenameDb(ctx->dbName), ctx->stbName);

        // Append tag values (columns 1..numCols-1)
        for (int c = 1; c < numCols; c++) {
            if (c > 1) {
                pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, ",");
            }

            int8_t type = fieldInfos[c].type;

            if (IS_VAR_DATA_TYPE(type)) {
                // Variable type: offsets[rows] + data
                int32_t *tagOffsets = (int32_t *)colDataPtrs[c];
                int32_t tagOffset = tagOffsets[row];
                if (tagOffset < 0) {
                    pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, "NULL");
                } else {
                    char *tagVarData = (char *)colDataPtrs[c] + blockRows * sizeof(int32_t);
                    uint16_t tagLen = *(uint16_t *)(tagVarData + tagOffset);
                    char *tagVal = tagVarData + tagOffset + sizeof(uint16_t);
                    
                    if (type == TSDB_DATA_TYPE_NCHAR) {
                        // NCHAR is stored as UCS-4 (UTF-32 LE); must convert to UTF-8 before
                        // embedding in SQL, otherwise null bytes in UCS-4 encoding truncate
                        // the C string and produce a malformed CREATE TABLE statement.
                        char *utf8Buf = (char *)taosMemoryMalloc(tagLen + 4);
                        if (utf8Buf != NULL) {
                            int32_t utf8Len = taosUcs4ToMbs((TdUcs4 *)tagVal, (int32_t)tagLen, utf8Buf, NULL);
                            if (utf8Len < 0) utf8Len = 0;
                            pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, "'");
                            for (int32_t k = 0; k < utf8Len && pos < TSDB_MAX_SQL_LEN - 4; k++) {
                                if (utf8Buf[k] == '\'') {
                                    sql[pos++] = '\\';
                                    sql[pos++] = '\'';
                                } else {
                                    sql[pos++] = utf8Buf[k];
                                }
                            }
                            pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, "'");
                            taosMemoryFree(utf8Buf);
                        } else {
                            pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, "NULL");
                        }
                    } else if (type == TSDB_DATA_TYPE_BINARY ||
                               type == TSDB_DATA_TYPE_VARCHAR || type == TSDB_DATA_TYPE_JSON) {
                        // escape single quotes
                        pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, "'");
                        for (uint16_t k = 0; k < tagLen && pos < TSDB_MAX_SQL_LEN - 4; k++) {
                            if (tagVal[k] == '\'') {
                                sql[pos++] = '\\';
                                sql[pos++] = '\'';
                            } else {
                                sql[pos++] = tagVal[k];
                            }
                        }
                        pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, "'");
                    } else {
                        // VARBINARY/GEOMETRY - hex encode
                        pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, "'");
                        for (uint16_t k = 0; k < tagLen && pos < TSDB_MAX_SQL_LEN - 4; k++) {
                            pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, "%02X", (unsigned char)tagVal[k]);
                        }
                        pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, "'");
                    }
                }
            } else {
                // Fixed type: bitmap[bitmapLen] + data[rows * bytes]
                int32_t bitmapLen = (blockRows + 7) / 8;
                char *bitmap = (char *)colDataPtrs[c];
                char *fixData = (char *)colDataPtrs[c] + bitmapLen;

                // check null bitmap (TDengine convention: bit7=row0, bit6=row1, ...)
                int byteIdx = row >> 3;
                int bitIdx  = 7 - (row & 7);
                bool isNull = (bitmap[byteIdx] & (1u << bitIdx)) != 0;
                
                if (isNull) {
                    pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, "NULL");
                } else {
                    int32_t bytes = fieldInfos[c].bytes;
                    char *val = fixData + row * bytes;
                    
                    switch (type) {
                        case TSDB_DATA_TYPE_BOOL:
                            pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, "%s", 
                                          (*(int8_t *)val) ? "true" : "false");
                            break;
                        case TSDB_DATA_TYPE_TINYINT:
                            pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, "%d", *(int8_t *)val);
                            break;
                        case TSDB_DATA_TYPE_SMALLINT:
                            pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, "%d", *(int16_t *)val);
                            break;
                        case TSDB_DATA_TYPE_INT:
                            pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, "%d", *(int32_t *)val);
                            break;
                        case TSDB_DATA_TYPE_BIGINT:
                            pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, "%" PRId64, *(int64_t *)val);
                            break;
                        case TSDB_DATA_TYPE_UTINYINT:
                            pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, "%u", *(uint8_t *)val);
                            break;
                        case TSDB_DATA_TYPE_USMALLINT:
                            pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, "%u", *(uint16_t *)val);
                            break;
                        case TSDB_DATA_TYPE_UINT:
                            pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, "%u", *(uint32_t *)val);
                            break;
                        case TSDB_DATA_TYPE_UBIGINT:
                            pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, "%" PRIu64, *(uint64_t *)val);
                            break;
                        case TSDB_DATA_TYPE_FLOAT:
                            pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, "%f", *(float *)val);
                            break;
                        case TSDB_DATA_TYPE_DOUBLE:
                            pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, "%f", *(double *)val);
                            break;
                        case TSDB_DATA_TYPE_TIMESTAMP:
                            pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, "%" PRId64, *(int64_t *)val);
                            break;
                        default:
                            pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, "NULL");
                            break;
                    }
                }
            }
        }

        pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, ")");

        logDebug("restore tag sql: %s", sql);

        // execute
        TAOS_RES *res = taos_query(ctx->conn, sql);
        int rc = taos_errno(res);
        if (res) taos_free_result(res);
        
        if (rc == TSDB_CODE_SUCCESS) {
            ctx->successCnt++;
            atomic_add_fetch_64(&g_stats.childTablesTotal, 1);
        } else if (rc == TSDB_CODE_TDB_TABLE_ALREADY_EXIST) {
            // table already exists - that's fine in APPEND mode
            logWarn("create child table may already exist (0x%08X): %s", rc, tbName);
            ctx->successCnt++;
            atomic_add_fetch_64(&g_stats.childTablesTotal, 1);
        } else {
            // real error - table creation failed
            logError("create child table failed (0x%08X): %s, sql: %s", rc, tbName, sql);
            ctx->failedCnt++;
        }

        taosMemoryFree(sql);
    }

    taosMemoryFree(colDataPtrs);
    taosMemoryFree(colDataLens);
    return TSDB_CODE_SUCCESS;
}

/* -----------------------------------------------------------------------
 * Parquet-based tag restore
 * ----------------------------------------------------------------------- */
typedef struct {
    TAOS       *conn;
    const char *dbName;
    const char *stbName;
    int64_t     successCnt;
    int64_t     failedCnt;
} ParquetTagCtx;

static int parquetTagCallback(void *userData,
                              TAOS_FIELD *fields, int numFields,
                              TAOS_MULTI_BIND *bindArray, int32_t numRows) {
    ParquetTagCtx *ctx = (ParquetTagCtx *)userData;
    if (numFields < 1) return TSDB_CODE_BCK_NO_FIELDS;

    for (int row = 0; row < numRows; row++) {
        if (g_interrupted) return TSDB_CODE_BCK_USER_CANCEL;

        /* Column 0  = tbname (VARCHAR / BINARY) */
        TAOS_MULTI_BIND *tbBind = &bindArray[0];
        if (tbBind->is_null && tbBind->is_null[row]) {
            logWarn("parquet tag: tbname NULL at row %d, skip", row);
            ctx->failedCnt++;
            continue;
        }
        int32_t tbLen = tbBind->length ? (int32_t)tbBind->length[row]
                                       : (int32_t)tbBind->buffer_length;
        char tbName[TSDB_TABLE_NAME_LEN] = {0};
        if (tbLen >= TSDB_TABLE_NAME_LEN) tbLen = TSDB_TABLE_NAME_LEN - 1;
        memcpy(tbName, (char *)tbBind->buffer + (size_t)row * tbBind->buffer_length, tbLen);
        tbName[tbLen] = '\0';

        char *sql = (char *)taosMemoryMalloc(TSDB_MAX_SQL_LEN);
        if (!sql) { ctx->failedCnt++; continue; }

        int pos = snprintf(sql, TSDB_MAX_SQL_LEN,
                           "CREATE TABLE IF NOT EXISTS `%s`.`%s` USING `%s`.`%s` TAGS(",
                           argRenameDb(ctx->dbName), tbName,
                           argRenameDb(ctx->dbName), ctx->stbName);

        /* Columns 1..numFields-1 are tag values */
        for (int c = 1; c < numFields && pos < TSDB_MAX_SQL_LEN - 64; c++) {
            if (c > 1) pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, ",");

            TAOS_MULTI_BIND *b   = &bindArray[c];
            bool             isNull = (b->is_null && b->is_null[row]);
            int8_t           type   = (int8_t)fields[c].type;

            if (isNull) {
                pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, "NULL");
                continue;
            }

            if (IS_VAR_DATA_TYPE(type)) {
                int32_t vLen = b->length ? (int32_t)b->length[row]
                                         : (int32_t)b->buffer_length;
                char   *vPtr = (char *)b->buffer + (size_t)row * b->buffer_length;
                pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, "'");
                for (int32_t k = 0; k < vLen && pos < TSDB_MAX_SQL_LEN - 4; k++) {
                    if (vPtr[k] == '\'') sql[pos++] = '\\';
                    if (pos < TSDB_MAX_SQL_LEN - 2) sql[pos++] = vPtr[k];
                }
                pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, "'");
            } else {
                char *vp = (char *)b->buffer + (size_t)row * b->buffer_length;
                switch (type) {
                    case TSDB_DATA_TYPE_BOOL:
                        pos += snprintf(sql+pos, TSDB_MAX_SQL_LEN-pos, "%s",
                                        (*(int8_t*)vp) ? "true" : "false"); break;
                    case TSDB_DATA_TYPE_TINYINT:
                        pos += snprintf(sql+pos, TSDB_MAX_SQL_LEN-pos, "%d",  *(int8_t *)vp); break;
                    case TSDB_DATA_TYPE_SMALLINT:
                        pos += snprintf(sql+pos, TSDB_MAX_SQL_LEN-pos, "%d",  *(int16_t*)vp); break;
                    case TSDB_DATA_TYPE_INT:
                        pos += snprintf(sql+pos, TSDB_MAX_SQL_LEN-pos, "%d",  *(int32_t*)vp); break;
                    case TSDB_DATA_TYPE_BIGINT:
                        pos += snprintf(sql+pos, TSDB_MAX_SQL_LEN-pos, "%" PRId64, *(int64_t*)vp); break;
                    case TSDB_DATA_TYPE_UTINYINT:
                        pos += snprintf(sql+pos, TSDB_MAX_SQL_LEN-pos, "%u",  *(uint8_t *)vp); break;
                    case TSDB_DATA_TYPE_USMALLINT:
                        pos += snprintf(sql+pos, TSDB_MAX_SQL_LEN-pos, "%u",  *(uint16_t*)vp); break;
                    case TSDB_DATA_TYPE_UINT:
                        pos += snprintf(sql+pos, TSDB_MAX_SQL_LEN-pos, "%u",  *(uint32_t*)vp); break;
                    case TSDB_DATA_TYPE_UBIGINT:
                        pos += snprintf(sql+pos, TSDB_MAX_SQL_LEN-pos, "%" PRIu64, *(uint64_t*)vp); break;
                    case TSDB_DATA_TYPE_FLOAT:
                        pos += snprintf(sql+pos, TSDB_MAX_SQL_LEN-pos, "%g",
                                        (double)*(float *)vp); break;
                    case TSDB_DATA_TYPE_DOUBLE:
                        pos += snprintf(sql+pos, TSDB_MAX_SQL_LEN-pos, "%g",  *(double*)vp); break;
                    case TSDB_DATA_TYPE_TIMESTAMP:
                        pos += snprintf(sql+pos, TSDB_MAX_SQL_LEN-pos, "%" PRId64, *(int64_t*)vp); break;
                    default:
                        pos += snprintf(sql+pos, TSDB_MAX_SQL_LEN-pos, "NULL"); break;
                }
            }
        }
        snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, ")");

        TAOS_RES *res = taos_query(ctx->conn, sql);
        int       rc  = taos_errno(res);
        if (res) taos_free_result(res);
        taosMemoryFree(sql);

        if (rc == TSDB_CODE_SUCCESS || rc == TSDB_CODE_TDB_TABLE_ALREADY_EXIST) {
            ctx->successCnt++;
            atomic_add_fetch_64(&g_stats.childTablesTotal, 1);
        } else {
            logError("create child table failed (0x%08X): %s.%s",
                     rc, argRenameDb(ctx->dbName), tbName);
            ctx->failedCnt++;
        }
    }
    return TSDB_CODE_SUCCESS;
}

//
// restore tag thread function
//
static void* restoreTagThread(void *arg) {
    RestoreTagThread *thread = (RestoreTagThread *)arg;
    thread->code = TSDB_CODE_SUCCESS;

    logInfo("restore tag thread %d start, file: %s", thread->index, thread->tagFile);

    /* Detect file format from extension */
    const char *ext       = strrchr(thread->tagFile, '.');
    bool        isParquet = (ext && strcmp(ext, ".par") == 0);

    if (isParquet) {
        /* ---- Parquet tag restore path ---- */
        int code = TSDB_CODE_SUCCESS;
        ParquetReader *pr = parquetReaderOpen(thread->tagFile, &code);
        if (pr == NULL) {
            logError("open parquet tag file failed(%d): %s", code, thread->tagFile);
            thread->code = code;
            return NULL;
        }

        ParquetTagCtx ctx;
        memset(&ctx, 0, sizeof(ctx));
        ctx.conn    = thread->conn;
        ctx.dbName  = thread->dbInfo->dbName;
        ctx.stbName = thread->stbInfo->stbName;

        code = parquetReaderReadAll(pr, parquetTagCallback, &ctx);
        parquetReaderClose(pr);
        if (code != TSDB_CODE_SUCCESS)
            logError("read parquet tag file failed(%d): %s", code, thread->tagFile);

        logInfo("restore tag thread %d done. success: %" PRId64 " failed: %" PRId64,
                thread->index, ctx.successCnt, ctx.failedCnt);
        thread->code = code;
        return NULL;
    }

    /* ---- Binary .dat tag restore path ---- */
    int code = 0;
    TaosFile *taosFile = openTaosFileForRead(thread->tagFile, &code);
    if (taosFile == NULL) {
        logError("open tag file failed(%d): %s", code, thread->tagFile);
        thread->code = code;
        return NULL;
    }

    if (taosFile->header.numRows == 0) {
        logInfo("tag file has no rows: %s", thread->tagFile);
        closeTaosFileRead(taosFile);
        return NULL;
    }

    // prepare context
    TagRestoreCtx ctx;
    ctx.conn       = thread->conn;
    ctx.dbName     = thread->dbInfo->dbName;
    ctx.stbName    = thread->stbInfo->stbName;
    ctx.fieldInfos = (FieldInfo *)taosFile->header.schema;
    ctx.numFields  = taosFile->header.numFields;
    ctx.successCnt = 0;
    ctx.failedCnt  = 0;

    // read blocks and process
    code = readTaosFileBlocks(taosFile, tagBlockCallback, &ctx);
    if (code != TSDB_CODE_SUCCESS) {
        logError("read tag blocks failed(%d): %s", code, thread->tagFile);
    }
    thread->code = code;

    logInfo("restore tag thread %d done. success: %" PRId64 " failed: %" PRId64,
            thread->index, ctx.successCnt, ctx.failedCnt);

    closeTaosFileRead(taosFile);
    return NULL;
}


//
// Find all tag files for a given stb under {outPath}/{dbName}/tags/
// Looks for .dat (binary) or .par (parquet) depending on actual format used.
// Pattern: {stbName}_data{N}.{ext}
//
static char** findTagFiles(const char *dbName, const char *stbName, int *count) {
    *count = 0;
    
    // Tag dir path is format-independent (just a directory)
    char tagDir[MAX_PATH_LEN] = {0};
    obtainFileName(BACK_DIR_TAG, dbName, NULL, NULL, 0, 0, BINARY_TAOS, tagDir, sizeof(tagDir));

    TdDirPtr dir = taosOpenDir(tagDir);
    if (dir == NULL) {
        logWarn("open tag dir failed: %s", tagDir);
        return NULL;
    }

    int capacity = 16;
    char **files = (char **)taosMemoryCalloc(capacity + 1, sizeof(char *));
    if (files == NULL) {
        taosCloseDir(&dir);
        return NULL;
    }

    // prefix: {stbName}_data
    char prefix[TSDB_TABLE_NAME_LEN + 16];
    snprintf(prefix, sizeof(prefix), "%s_data", stbName);
    int prefixLen = strlen(prefix);

    TdDirEntryPtr entry;
    while ((entry = taosReadDir(dir)) != NULL) {
        char *entryName = taosGetDirEntryName(entry);
        if (entryName[0] == '.') continue;
        
        // match prefix and either .dat (binary) or .par (parquet) extension
        if (strncmp(entryName, prefix, prefixLen) != 0) continue;
        bool isDat = (strstr(entryName, ".dat") != NULL);
        bool isPar = (strstr(entryName, ".par") != NULL);
        if (!isDat && !isPar) continue;

        if (*count >= capacity) {
            capacity *= 2;
            char **tmp = (char **)taosMemoryRealloc(files, (capacity + 1) * sizeof(char *));
            if (!tmp) {
                freeArrayPtr(files);
                taosCloseDir(&dir);
                return NULL;
            }
            files = tmp;
        }

        char fullPath[MAX_PATH_LEN];
        snprintf(fullPath, sizeof(fullPath), "%s%s", tagDir, entryName);
        files[*count] = taosStrdup(fullPath);
        (*count)++;
    }
    files[*count] = NULL;

    taosCloseDir(&dir);
    return files;
}


//
// restore child table tags for one super table (parallel threads)
//
static int restoreStbTags(DBInfo *dbInfo, StbInfo *stbInfo) {
    int code = TSDB_CODE_SUCCESS;
    const char *dbName = dbInfo->dbName;
    const char *stbName = stbInfo->stbName;

    // find tag files
    int fileCnt = 0;
    char **tagFiles = findTagFiles(dbName, stbName, &fileCnt);
    if (tagFiles == NULL || fileCnt == 0) {
        logInfo("no tag files found for %s.%s", dbName, stbName);
        if (tagFiles) freeArrayPtr(tagFiles);
        return TSDB_CODE_SUCCESS;
    }

    logInfo("found %d tag files for %s.%s", fileCnt, dbName, stbName);

    // determine thread count
    int threadCnt = argTagThread();
    if (fileCnt < threadCnt) {
        threadCnt = fileCnt;
    }

    // allocate threads - one file per thread (round-robin if more files than threads)
    // For simplicity, each thread processes one file (most cases: 1 file per thread index)
    RestoreTagThread *threads = (RestoreTagThread *)taosMemoryCalloc(fileCnt, sizeof(RestoreTagThread));
    if (threads == NULL) {
        freeArrayPtr(tagFiles);
        return TSDB_CODE_BCK_MALLOC_FAILED;
    }

    // Create threads - one per file
    int actualThreads = fileCnt;
    for (int i = 0; i < actualThreads; i++) {
        threads[i].dbInfo  = dbInfo;
        threads[i].stbInfo = stbInfo;
        threads[i].index   = i + 1;
        threads[i].conn    = getConnection();
        if (!threads[i].conn) {
            for (int j = 0; j < i; j++) {
                releaseConnection(threads[j].conn);
            }
            taosMemoryFree(threads);
            freeArrayPtr(tagFiles);
            return g_interrupted ? TSDB_CODE_BCK_USER_CANCEL : TSDB_CODE_BCK_CONN_POOL_EXHAUSTED;
        }
        snprintf(threads[i].tagFile, MAX_PATH_LEN, "%s", tagFiles[i]);

        if (pthread_create(&threads[i].pid, NULL, restoreTagThread, (void *)&threads[i]) != 0) {
            logError("create restore tag thread failed(%s) for stb: %s.%s", 
                     strerror(errno), dbName, stbName);
            // release connections for already-created threads
            for (int j = 0; j <= i; j++) {
                releaseConnection(threads[j].conn);
            }
            taosMemoryFree(threads);
            freeArrayPtr(tagFiles);
            return TSDB_CODE_BCK_CREATE_THREAD_FAILED;
        }
    }

    // wait threads
    for (int i = 0; i < actualThreads; i++) {
        pthread_join(threads[i].pid, NULL);
        releaseConnection(threads[i].conn);
        if (code == TSDB_CODE_SUCCESS && threads[i].code != TSDB_CODE_SUCCESS) {
            code = threads[i].code;
        }
    }

    taosMemoryFree(threads);
    freeArrayPtr(tagFiles);
    return code;
}


//
// Get super table names from backup: parse stb.sql or scan data directories
// Read from the schema CSV files in the backup directory
//
static char** getBackupStbNames(const char *dbName, int *code) {
    *code = TSDB_CODE_SUCCESS;

    char *outPath = argOutPath();
    char dbDir[MAX_PATH_LEN];
    snprintf(dbDir, sizeof(dbDir), "%s/%s", outPath, dbName);

    TdDirPtr dir = taosOpenDir(dbDir);
    if (dir == NULL) {
        logError("open backup db dir failed: %s", dbDir);
        *code = TSDB_CODE_BCK_OPEN_DIR_FAILED;
        return NULL;
    }

    int capacity = 16;
    int count = 0;
    char **names = (char **)taosMemoryCalloc(capacity + 1, sizeof(char *));
    if (!names) {
        taosCloseDir(&dir);
        *code = TSDB_CODE_BCK_MALLOC_FAILED;
        return NULL;
    }

    // Look for {stbName}.csv files (schema files)
    TdDirEntryPtr entry;
    while ((entry = taosReadDir(dir)) != NULL) {
        char *entryName = taosGetDirEntryName(entry);
        if (entryName[0] == '.') continue;
        
        // check for .csv extension
        int nameLen = strlen(entryName);
        if (nameLen > 4 && strcmp(entryName + nameLen - 4, ".csv") == 0) {
            if (count >= capacity) {
                capacity *= 2;
                char **tmp = (char **)taosMemoryRealloc(names, (capacity + 1) * sizeof(char *));
                if (!tmp) {
                    freeArrayPtr(names);
                    taosCloseDir(&dir);
                    *code = TSDB_CODE_BCK_MALLOC_FAILED;
                    return NULL;
                }
                names = tmp;
            }
            // extract stb name (remove .csv)
            char stbName[TSDB_TABLE_NAME_LEN] = {0};
            int stbNameLen = nameLen - 4;
            if (stbNameLen >= TSDB_TABLE_NAME_LEN) stbNameLen = TSDB_TABLE_NAME_LEN - 1;
            memcpy(stbName, entryName, stbNameLen);
            stbName[stbNameLen] = '\0';
            names[count++] = taosStrdup(stbName);
        }
    }
    names[count] = NULL;

    taosCloseDir(&dir);
    return names;
}


//
// -------------------------------------- MAIN -----------------------------------------
//

//
// restore database meta: db sql + stb sql + tags
//
int restoreDatabaseMeta(const char *dbName) {
    int code = TSDB_CODE_FAILED;

    //
    // 1. Restore database SQL
    //
    code = restoreDbSql(dbName);
    if (code != TSDB_CODE_SUCCESS) {
        logError("restore db sql failed(%d): %s", code, dbName);
        return code;
    }

    //
    // 2. Restore super table SQL
    //
    code = restoreStbSql(dbName);
    if (code != TSDB_CODE_SUCCESS) {
        logError("restore stb sql failed(%d): %s", code, dbName);
        return code;
    }

    //
    // 3. Restore child table tags (create child tables)
    //
    int stbCode = TSDB_CODE_SUCCESS;
    char **stbNames = getBackupStbNames(dbName, &stbCode);
    if (stbNames == NULL) {
        if (stbCode != TSDB_CODE_SUCCESS) return stbCode;
        return TSDB_CODE_SUCCESS; // no stables
    }

    DBInfo dbInfo;
    dbInfo.dbName = dbName;

    for (int i = 0; stbNames[i] != NULL; i++) {
        if (g_interrupted) {
            code = TSDB_CODE_BCK_USER_CANCEL;
            break;
        }

        logInfo("restore tags for super table: %s.%s", dbName, stbNames[i]);
        atomic_add_fetch_64(&g_stats.stbTotal, 1);
        StbInfo stbInfo;
        memset(&stbInfo, 0, sizeof(StbInfo));
        stbInfo.dbInfo = &dbInfo;
        stbInfo.stbName = stbNames[i];

        code = restoreStbTags(&dbInfo, &stbInfo);
        if (code != TSDB_CODE_SUCCESS) {
            logError("restore stb tags failed(%d): %s.%s", code, dbName, stbNames[i]);
            freeArrayPtr(stbNames);
            return code;
        }
    }

    freeArrayPtr(stbNames);

    if (code != TSDB_CODE_SUCCESS) {
        return code;
    }

    //
    // 4. Restore normal table SQL
    //
    code = restoreNtbSql(dbName);
    if (code != TSDB_CODE_SUCCESS) {
        logError("restore ntb sql failed(%d): %s", code, dbName);
        return code;
    }

    return TSDB_CODE_SUCCESS;
}
