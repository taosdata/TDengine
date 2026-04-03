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
#include "bckProgress.h"

//
// -------------------------------------- UTIL -----------------------------------------
//

// execute a SQL string on the given connection
static int execSql(TAOS *conn, const char *sql) {
    TAOS_RES *res = taos_query(conn, sql);
    int code = taos_errno(res);
    if (code != TSDB_CODE_SUCCESS) {
        logError("exec sql failed(0x%08X %s): %s", code, taos_errstr(res), sql);
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
    int statRet = taosFStatFile(fp, &fileSize, NULL);
    if (statRet == 0 && fileSize == 0) {
        // empty file — caller should treat this as "nothing to process"
        taosCloseFile(&fp);
        char *empty = (char *)taosMemoryMalloc(1);
        if (empty) empty[0] = '\0';
        if (outLen) *outLen = 0;
        return empty;
    }
    if (statRet != 0 || fileSize < 0) {
        // fstat failed — fall back to unbounded read (up to 1 MB)
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

    logDebug("restore db sql: %s", execContent);

    TAOS *conn = getConnection(&code);
    if (conn == NULL) {
        taosMemoryFree(content);
        if (renamedSql) taosMemoryFree(renamedSql);
        return code;
    }

    // execute - if database exists, this may fail, which is fine (APPEND mode)
    TAOS_RES *res = taos_query(conn, execContent);
    code = taos_errno(res);

    if (code != TSDB_CODE_SUCCESS) {
        // try USE database instead - database may already exist
        logDebug("database may already exist(0x%08X %s), so need not create. `%s`: %s", code, taos_errstr(res), targetDb, execContent);
    }
    if (res) taos_free_result(res);

    if (code != TSDB_CODE_SUCCESS) {
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

    TAOS *conn = getConnection(&code);
    if (conn == NULL) {
        taosMemoryFree(content);
        return code;
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

            logDebug("restore stb sql: %s", fullSql);
            // execute - if stable already exists, it's ok
            TAOS_RES *res = taos_query(conn, fullSql);
            int rc = taos_errno(res);
            if (rc == TSDB_CODE_SUCCESS) {
                // ok
            } else if (rc == TSDB_CODE_MND_STB_ALREADY_EXIST) {
                logDebug("stable already exists(0x%08X %s): %s", rc, taos_errstr(res), fullSql);
                // fine in APPEND mode
            } else {
                logError("create stable failed(0x%08X %s): %s", rc, taos_errstr(res), fullSql);
                code = rc;
            }
            if (res) taos_free_result(res);
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
        logDebug("ntb.sql not found, skip normal tables: %s", sqlFile);
        return TSDB_CODE_SUCCESS;
    }

    char *content = readFileContent(sqlFile, NULL);
    if (content == NULL) {
        logError("read ntb.sql failed: %s", sqlFile);
        return TSDB_CODE_BCK_READ_FILE_FAILED;
    }

    TAOS *conn = getConnection(&code);
    if (conn == NULL) {
        taosMemoryFree(content);
        return code;
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

            if (rc == TSDB_CODE_SUCCESS) {
                ntbCount++;
                atomic_add_fetch_64(&g_stats.ntbTotal, 1);
            } else if (rc == TSDB_CODE_TDB_TABLE_ALREADY_EXIST || rc == TSDB_CODE_MND_STB_ALREADY_EXIST) {
                logWarn("normal table already exists(0x%08X %s): %s", rc, taos_errstr(res), fullSql);
                ntbCount++;
                atomic_add_fetch_64(&g_stats.ntbTotal, 1);
            } else {
                logError("create normal table failed(0x%08X %s): %s", rc, taos_errstr(res), fullSql);
                code = rc;
            }
            if (res) taos_free_result(res);
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
// -------------------------------------- META: VTB SQL -----------------------------------------
//

// Replace all occurrences of srcPat with dstPat in buf (in-place, buf must have TSDB_MAX_SQL_LEN capacity)
static void replaceAllInSql(char *buf, const char *srcPat, const char *dstPat) {
    int srcLen = (int)strlen(srcPat);
    int dstLen = (int)strlen(dstPat);
    if (srcLen == 0 || strcmp(srcPat, dstPat) == 0) return;

    char *tmp = (char *)taosMemoryMalloc(TSDB_MAX_SQL_LEN);
    if (!tmp) return;

    int inPos = 0, outPos = 0;
    int totalLen = (int)strlen(buf);
    while (inPos < totalLen) {
        if (inPos <= totalLen - srcLen && memcmp(buf + inPos, srcPat, srcLen) == 0) {
            if (outPos + dstLen < TSDB_MAX_SQL_LEN - 1) {
                memcpy(tmp + outPos, dstPat, dstLen);
                outPos += dstLen;
            }
            inPos += srcLen;
        } else {
            if (outPos < TSDB_MAX_SQL_LEN - 1) tmp[outPos++] = buf[inPos];
            inPos++;
        }
    }
    tmp[outPos] = '\0';
    snprintf(buf, TSDB_MAX_SQL_LEN, "%s", tmp);
    taosMemoryFree(tmp);
}

//
// ---- Virtual child table columnar tag restore (vtags/ binary files) ----
//

#define BCK_MAX_VTAG 128

// Skeleton entry for one virtual child table in vtb.sql.
// renamedSql holds the full CREATE VTABLE skeleton *without* the " TAGS (...)" suffix.
typedef struct {
    char backupTbName[TSDB_TABLE_NAME_LEN];    // bare child table name, no db/backticks
    char backupVstbName[TSDB_TABLE_NAME_LEN];  // bare vstb name
    char renamedSql[TSDB_MAX_SQL_LEN];         // skeleton DDL with targetDb applied, no TAGS
} VtbChildSkeleton;

// Context for vtbTagBlockCallback
typedef struct {
    TAOS             *conn;
    const char       *dbName;        // backup db name
    const char       *vstbName;      // vstb name (same in src and dst; only DB is renamed)
    VtbChildSkeleton *items;         // all child skeletons for this vstb
    int               itemCount;
    // server tag ordering and types from DESCRIBE vstb on the target server
    int     serverTagCount;
    char    serverTagNames[BCK_MAX_VTAG][65];
    int8_t  serverTagTypes[BCK_MAX_VTAG];
    // current child accumulator (rows are sorted by table_name in the binary file)
    char    curTbname[TSDB_TABLE_NAME_LEN];
    bool    hasAccum;
    char    tagValues[BCK_MAX_VTAG][512];
    bool    tagSet[BCK_MAX_VTAG];
    // stats
    int64_t successCnt;
    int64_t failedCnt;
} VtbRestoreCtx;

//
// Extract the bare (unquoted, no db prefix) identifier from a position in a SQL string.
// Handles "`db`.`name`", "`name`", and plain unquoted identifiers.
// Only the IMMEDIATE (db`.`name) qualification is recognized; deeper dot-separated
// paths inside FROM clauses are ignored.
// Returns number of chars written to out (0 on failure).
//
static int extractBareIdent(const char *p, char *out, int outSz) {
    while (*p == ' ') p++;
    if (*p != '`') {
        // Unquoted: read until whitespace or '(' or ','
        const char *end = p;
        while (*end && *end != ' ' && *end != '(' && *end != ',') end++;
        int len = (int)(end - p);
        if (len >= outSz) len = outSz - 1;
        memcpy(out, p, len);
        out[len] = '\0';
        return len;
    }

    // Backtick-quoted: extract first segment between backticks
    p++;  // skip opening backtick
    const char *seg1End = strchr(p, '`');
    if (!seg1End) return 0;

    // Check immediately after the closing backtick for ".`" (db-qualified form)
    const char *after = seg1End + 1;
    if (after[0] == '.' && after[1] == '`') {
        // Qualified form: `db`.`name`  — skip db part, extract name part
        p = after + 2;  // skip ".`", now at start of name content
        const char *seg2End = strchr(p, '`');
        if (!seg2End) return 0;
        int len = (int)(seg2End - p);
        if (len >= outSz) len = outSz - 1;
        memcpy(out, p, len);
        out[len] = '\0';
        return len;
    }

    // Unqualified form: `name` — use content of first segment
    int len = (int)(seg1End - p);
    if (len >= outSz) len = outSz - 1;
    memcpy(out, p, len);
    out[len] = '\0';
    return len;
}

//
// Read a variable-length (VARCHAR or NCHAR) value from a binary block column into outBuf.
// For NCHAR, UCS4 is converted to UTF-8. Returns false if NULL.
//
static bool readBlockVarColStr(int8_t colType, void *colDataPtr,
                               int row, int blockRows,
                               char *outBuf, int outBufSz) {
    int32_t *offsets = (int32_t *)colDataPtr;
    int32_t  off     = offsets[row];
    if (off < 0) { outBuf[0] = '\0'; return false; }
    char    *varData = (char *)colDataPtr + blockRows * sizeof(int32_t);
    uint16_t varLen  = *(uint16_t *)(varData + off);
    char    *payload = varData + off + sizeof(uint16_t);
    if (colType == TSDB_DATA_TYPE_NCHAR) {
        int32_t utf8Len = taosUcs4ToMbs((TdUcs4 *)payload, (int32_t)varLen, outBuf, NULL);
        if (utf8Len < 0) utf8Len = 0;
        if (utf8Len >= outBufSz) utf8Len = outBufSz - 1;
        outBuf[utf8Len] = '\0';
    } else {
        int n = (varLen < outBufSz - 1) ? varLen : outBufSz - 1;
        memcpy(outBuf, payload, n);
        outBuf[n] = '\0';
    }
    return true;
}

//
// Append a tag value to a SQL buffer with proper quoting based on tag type.
// String/JSON types are single-quoted; numeric/bool types are unquoted.
// Returns number of chars written.
//
static int appendVtabTagValue(char *buf, int bufRemain, int8_t tagType, const char *val) {
    if (!val || val[0] == '\0') {
        return snprintf(buf, bufRemain, "NULL");
    }
    bool needsQuote = (tagType == TSDB_DATA_TYPE_VARCHAR ||
                       tagType == TSDB_DATA_TYPE_BINARY  ||
                       tagType == TSDB_DATA_TYPE_NCHAR   ||
                       tagType == TSDB_DATA_TYPE_JSON);
    if (!needsQuote) {
        return snprintf(buf, bufRemain, "%s", val);
    }
    int pos = 0;
    if (pos < bufRemain - 1) buf[pos++] = '\'';
    for (int i = 0; val[i] && pos < bufRemain - 2; i++) {
        if (val[i] == '\'') buf[pos++] = '\'';   // double single-quote escape
        buf[pos++] = val[i];
    }
    if (pos < bufRemain - 1) buf[pos++] = '\'';
    if (pos < bufRemain)     buf[pos]   = '\0';
    return pos;
}

//
// Emit a CREATE VTABLE statement for the current accumulated child table.
//
static void emitVtbChild(VtbRestoreCtx *ctx) {
    if (!ctx->hasAccum || ctx->curTbname[0] == '\0') return;

    // Find the matching skeleton by the unquoted child table name
    VtbChildSkeleton *sk = NULL;
    for (int i = 0; i < ctx->itemCount; i++) {
        if (strcasecmp(ctx->items[i].backupTbName, ctx->curTbname) == 0) {
            sk = &ctx->items[i];
            break;
        }
    }
    if (!sk) {
        logWarn("no skeleton for vtable child '%s' (vstb %s) — skip",
                ctx->curTbname, ctx->vstbName);
        ctx->failedCnt++;
        return;
    }

    // Build " TAGS (v1, v2, ...)" suffix
    char tagsSuffix[4096];
    int  pos = snprintf(tagsSuffix, sizeof(tagsSuffix), " TAGS (");
    for (int t = 0; t < ctx->serverTagCount; t++) {
        if (t > 0 && pos < (int)sizeof(tagsSuffix) - 2) {
            tagsSuffix[pos++] = ',';
            tagsSuffix[pos++] = ' ';
        }
        const char *v = ctx->tagSet[t] ? ctx->tagValues[t] : NULL;
        pos += appendVtabTagValue(tagsSuffix + pos, (int)sizeof(tagsSuffix) - pos,
                                  ctx->serverTagTypes[t], v);
    }
    if (pos < (int)sizeof(tagsSuffix) - 2) {
        tagsSuffix[pos++] = ')';
        tagsSuffix[pos]   = '\0';
    }

    // Final SQL: renamedSkeleton + TAGS suffix
    char *fullSql = (char *)taosMemoryMalloc(TSDB_MAX_SQL_LEN + 128);
    if (!fullSql) { ctx->failedCnt++; return; }
    snprintf(fullSql, TSDB_MAX_SQL_LEN + 128, "%s%s", sk->renamedSql, tagsSuffix);

    logDebug("restore vtable child sql: %s", fullSql);
    TAOS_RES *res = taos_query(ctx->conn, fullSql);
    int rc = taos_errno(res);

    if (rc == TSDB_CODE_SUCCESS) {
        ctx->successCnt++;
        atomic_add_fetch_64(&g_stats.childTablesTotal, 1);
        atomic_add_fetch_64(&g_progress.ctbDoneCur, 1);
    } else if (rc == TSDB_CODE_TDB_TABLE_ALREADY_EXIST || rc == TSDB_CODE_MND_STB_ALREADY_EXIST) {
        logWarn("virtual child table already exists(0x%08X %s): %s", rc, taos_errstr(res), ctx->curTbname);
        ctx->successCnt++;
        atomic_add_fetch_64(&g_stats.childTablesTotal, 1);
        atomic_add_fetch_64(&g_progress.ctbDoneCur, 1);
    } else {
        logError("create virtual child table failed(0x%08X %s): %s",
                 rc, taos_errstr(res), fullSql);
        ctx->failedCnt++;
    }
    if (res) taos_free_result(res);
    taosMemoryFree(fullSql);
}

//
// Block callback for reading ins_tags vtag binary files.
// Binary layout: col0=table_name, col1=tag_name, col2=tag_value (all variable-length).
// Rows are sorted by table_name (ORDER BY in backup query) so changes in table_name
// signal a new child table, allowing us to flush the previous accumulation.
//
static int vtbTagBlockCallback(void *userData,
                               FieldInfo *fieldInfos,
                               int numFields,
                               void *blockData,
                               int32_t blockLen,
                               int32_t blockRows) {
    VtbRestoreCtx *ctx = (VtbRestoreCtx *)userData;

    BlockReader reader;
    int32_t code = initBlockReader(&reader, blockData);
    if (code != TSDB_CODE_SUCCESS) {
        logError("vtbTagBlockCallback: initBlockReader failed: %d", code);
        return code;
    }

    int numCols = reader.oriHeader->numOfCols;
    if (numCols < 3 || numFields < 3) {
        logError("vtbTagBlockCallback: expected >=3 cols, got numCols=%d numFields=%d",
                 numCols, numFields);
        return TSDB_CODE_BCK_INVALID_FILE;
    }

    void   **colData = (void **)taosMemoryCalloc(numCols, sizeof(void *));
    int32_t *colLen  = (int32_t *)taosMemoryCalloc(numCols, sizeof(int32_t));
    if (!colData || !colLen) {
        taosMemoryFree(colData);
        taosMemoryFree(colLen);
        return TSDB_CODE_BCK_MALLOC_FAILED;
    }

    for (int c = 0; c < numCols; c++) {
        int8_t t = (c < numFields) ? fieldInfos[c].type : TSDB_DATA_TYPE_NCHAR;
        code = getColumnData(&reader, t, &colData[c], &colLen[c]);
        if (code != TSDB_CODE_SUCCESS) {
            logError("vtbTagBlockCallback: getColumnData col=%d failed: %d", c, code);
            taosMemoryFree(colData);
            taosMemoryFree(colLen);
            return code;
        }
    }

    char tbName[TSDB_TABLE_NAME_LEN];
    char tagName[65];
    char tagVal[512];

    for (int row = 0; row < blockRows; row++) {
        if (g_interrupted) {
            taosMemoryFree(colData);
            taosMemoryFree(colLen);
            return TSDB_CODE_BCK_USER_CANCEL;
        }

        readBlockVarColStr(fieldInfos[0].type, colData[0], row, blockRows, tbName,  sizeof(tbName));
        readBlockVarColStr(fieldInfos[1].type, colData[1], row, blockRows, tagName, sizeof(tagName));
        readBlockVarColStr(fieldInfos[2].type, colData[2], row, blockRows, tagVal,  sizeof(tagVal));

        if (tbName[0] == '\0') continue;

        // Table name changed: emit the previous accumulated child
        if (ctx->hasAccum && strcmp(tbName, ctx->curTbname) != 0) {
            emitVtbChild(ctx);
            memset(ctx->tagValues, 0, sizeof(ctx->tagValues));
            memset(ctx->tagSet,    0, sizeof(ctx->tagSet));
        }

        snprintf(ctx->curTbname, sizeof(ctx->curTbname), "%s", tbName);
        ctx->hasAccum = true;

        // Index tag value by server tag order
        for (int t = 0; t < ctx->serverTagCount; t++) {
            if (strcasecmp(ctx->serverTagNames[t], tagName) == 0) {
                snprintf(ctx->tagValues[t], sizeof(ctx->tagValues[t]), "%s", tagVal);
                ctx->tagSet[t] = true;
                break;
            }
        }
    }

    taosMemoryFree(colData);
    taosMemoryFree(colLen);
    return TSDB_CODE_SUCCESS;
}

//
// Query server vstb tag names and types via DESCRIBE.
// Returns tag count (>=0) or -1 on error.
//
static int queryVstbTagInfo(TAOS *conn, const char *dbName, const char *vstbName,
                            char names[][65], int8_t *types, int maxN) {
    char sql[TSDB_TABLE_FNAME_LEN + 32];
    snprintf(sql, sizeof(sql), "DESCRIBE `%s`.`%s`", dbName, vstbName);
    TAOS_RES *res = taos_query(conn, sql);
    if (taos_errno(res) != TSDB_CODE_SUCCESS) {
        logError("DESCRIBE %s.%s failed(0x%08X %s): %s", dbName, vstbName, taos_errno(res), taos_errstr(res), sql);
        taos_free_result(res);
        return -1;
    }

    int numResultCols = taos_field_count(res);
    int tagCnt = 0;
    TAOS_ROW row;
    while ((row = taos_fetch_row(res)) != NULL && tagCnt < maxN) {
        int *lens = taos_fetch_lengths(res);
        if (numResultCols < 4 || !row[3] || !lens) continue;
        if (lens[3] != 3 || strncasecmp((char *)row[3], "TAG", 3) != 0) continue;
        if (!row[0] || !row[1]) continue;

        int nameLen = lens[0] < 64 ? lens[0] : 64;
        memcpy(names[tagCnt], (char *)row[0], nameLen);
        names[tagCnt][nameLen] = '\0';

        // Determine data type for quoting by parsing the type string
        char typeStr[32] = {0};
        int  typeLen = lens[1] < 31 ? lens[1] : 31;
        memcpy(typeStr, (char *)row[1], typeLen);
        typeStr[typeLen] = '\0';

        if (strncasecmp(typeStr, "NCHAR", 5) == 0) {
            types[tagCnt] = TSDB_DATA_TYPE_NCHAR;
        } else if (strncasecmp(typeStr, "VARCHAR", 7) == 0 ||
                   strncasecmp(typeStr, "BINARY",  6) == 0) {
            types[tagCnt] = TSDB_DATA_TYPE_VARCHAR;
        } else if (strncasecmp(typeStr, "JSON",    4) == 0) {
            types[tagCnt] = TSDB_DATA_TYPE_JSON;
        } else {
            // Numeric or bool: treated as unquoted (use INT as a generic marker)
            types[tagCnt] = TSDB_DATA_TYPE_INT;
        }
        tagCnt++;
    }
    taos_free_result(res);
    return tagCnt;
}

//
// Find vtag binary files for a vstb under {outPath}/{dbName}/vtags/.
// Matches {vstbName}_data*.{dat|par}.  Returns NULL-terminated array (caller freeArrayPtr()).
//
static char** findVtagFiles(const char *dbName, const char *vstbName, int *count) {
    *count = 0;
    char vtagDir[MAX_PATH_LEN] = {0};
    obtainFileName(BACK_DIR_VTAG, dbName, NULL, NULL, 0, 0, BINARY_TAOS, vtagDir, sizeof(vtagDir));

    TdDirPtr dir = taosOpenDir(vtagDir);
    if (!dir) {
        logWarn("vtag dir not found: %s", vtagDir);
        return NULL;
    }

    char prefix[TSDB_TABLE_NAME_LEN + 16];
    snprintf(prefix, sizeof(prefix), "%s_data", vstbName);
    int prefixLen = (int)strlen(prefix);

    int   capacity = 8;
    char **files = (char **)taosMemoryCalloc(capacity + 1, sizeof(char *));
    if (!files) { taosCloseDir(&dir); return NULL; }

    TdDirEntryPtr entry;
    while ((entry = taosReadDir(dir)) != NULL) {
        char *name = taosGetDirEntryName(entry);
        if (name[0] == '.') continue;
        if (strncmp(name, prefix, prefixLen) != 0) continue;
        bool isDat = strstr(name, ".dat") != NULL;
        bool isPar = strstr(name, ".par") != NULL;
        if (!isDat && !isPar) continue;

        if (*count >= capacity) {
            capacity *= 2;
            char **tmp = (char **)taosMemoryRealloc(files, (capacity + 1) * sizeof(char *));
            if (!tmp) { freeArrayPtr(files); taosCloseDir(&dir); return NULL; }
            files = tmp;
        }
        char fullPath[MAX_PATH_LEN];
        snprintf(fullPath, sizeof(fullPath), "%s%s", vtagDir, name);
        files[(*count)++] = taosStrdup(fullPath);
    }
    if (*count >= 0) files[*count] = NULL;
    taosCloseDir(&dir);
    return files;
}

//
// Restore virtual child table TAGS for one virtual super table.
// Reads vtags binary files and executes CREATE VTABLE ... USING vstb TAGS(...) per child.
//
static int restoreVstbChildTags(const char       *dbName,
                                const char       *vstbName,
                                VtbChildSkeleton *items,
                                int               itemCount) {
    if (itemCount == 0) return TSDB_CODE_SUCCESS;

    int    fileCnt  = 0;
    char **vtagFiles = findVtagFiles(dbName, vstbName, &fileCnt);
    if (!vtagFiles || fileCnt == 0) {
        logWarn("no vtag files for vstb %s.%s; virtual child tag restore skipped",
                dbName, vstbName);
        if (vtagFiles) freeArrayPtr(vtagFiles);
        return TSDB_CODE_SUCCESS;
    }

    int connCode = TSDB_CODE_FAILED;
    TAOS *conn = getConnection(&connCode);
    if (!conn) { freeArrayPtr(vtagFiles); return connCode; }

    const char *targetDb = argRenameDb(dbName);
    int code = TSDB_CODE_SUCCESS;

    for (int f = 0; f < fileCnt && code == TSDB_CODE_SUCCESS; f++) {
        if (g_interrupted) { code = TSDB_CODE_BCK_USER_CANCEL; break; }

        int openCode = 0;
        TaosFile *taosFile = openTaosFileForRead(vtagFiles[f], &openCode);
        if (!taosFile || openCode != TSDB_CODE_SUCCESS) {
            logError("open vtag file failed(%d): %s", openCode, vtagFiles[f]);
            code = TSDB_CODE_BCK_READ_FILE_FAILED;
            break;
        }

        VtbRestoreCtx ctx;
        memset(&ctx, 0, sizeof(ctx));
        ctx.conn      = conn;
        ctx.dbName    = dbName;
        ctx.vstbName  = vstbName;
        ctx.items     = items;
        ctx.itemCount = itemCount;

        int n = queryVstbTagInfo(conn, targetDb, vstbName,
                                 ctx.serverTagNames, ctx.serverTagTypes, BCK_MAX_VTAG);
        if (n > 0) {
            ctx.serverTagCount = n;
            logInfo("vstb %s.%s has %d TAG(s) on target server", targetDb, vstbName, n);
        } else {
            logWarn("DESCRIBE %s.%s failed; child tag restore may be incomplete",
                    targetDb, vstbName);
        }

        code = readTaosFileBlocks(taosFile, vtbTagBlockCallback, &ctx);
        if (code != TSDB_CODE_SUCCESS) {
            logError("readTaosFileBlocks failed(%d): %s", code, vtagFiles[f]);
        }

        // Flush the last accumulated child table (readTaosFileBlocks has no finalize hook)
        if (ctx.hasAccum) {
            emitVtbChild(&ctx);
        }

        logInfo("vtag restore %s: success=%" PRId64 " failed=%" PRId64,
                vtagFiles[f], ctx.successCnt, ctx.failedCnt);
        if (ctx.failedCnt > 0 && code == TSDB_CODE_SUCCESS) {
            code = TSDB_CODE_BCK_EXEC_SQL_FAILED;
        }

        closeTaosFileRead(taosFile);
    }

    releaseConnection(conn);
    freeArrayPtr(vtagFiles);
    return code;
}

// restore virtual table DDL (vtb.sql)
// vtb.sql lines are:
//   - CREATE VTABLE `db`.`name` (...) [virtual normal table, no USING]
//   - CREATE VTABLE `db`.`name` (...) USING `db`.`vstb` (`t1`) [child skeleton, no TAGS]
//   - CREATE VTABLE `db`.`name` (...) USING `db`.`vstb` (`t1`) TAGS (v) [old full format]
// Normal vtables and old-format children are executed immediately.
// New-format child skeletons are batched and restored via vtags binary files.
static int restoreVtbSql(const char *dbName) {
    int code = TSDB_CODE_SUCCESS;

    char sqlFile[MAX_PATH_LEN] = {0};
    obtainFileName(BACK_FILE_VTBSQL, dbName, NULL, NULL, 0, 0, BINARY_TAOS, sqlFile, sizeof(sqlFile));

    if (!taosCheckExistFile(sqlFile)) {
        logDebug("vtb.sql not found, skip virtual tables: %s", sqlFile);
        return TSDB_CODE_SUCCESS;
    }

    char *content = readFileContent(sqlFile, NULL);
    if (content == NULL) {
        logError("read vtb.sql failed: %s", sqlFile);
        return TSDB_CODE_BCK_READ_FILE_FAILED;
    }

    TAOS *conn = getConnection(&code);
    if (conn == NULL) {
        taosMemoryFree(content);
        return code;
    }

    const char *targetDb = argRenameDb(dbName);

    // Storage for virtual child table skeletons (USING present, TAGS absent in vtb.sql)
    int               skeletonCap   = 16;
    int               skeletonCount = 0;
    VtbChildSkeleton *skeletons     = (VtbChildSkeleton *)taosMemoryCalloc(
                                          skeletonCap, sizeof(VtbChildSkeleton));
    if (!skeletons) {
        releaseConnection(conn);
        taosMemoryFree(content);
        return TSDB_CODE_BCK_MALLOC_FAILED;
    }

    char *line = content;
    char *next = NULL;
    int vtbCount = 0;

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
            char *vtablePos = strstr(line, "VTABLE");
            if (!vtablePos) { line = next; continue; }

            // Determine line type: has USING? has TAGS?
            char *usingPos = strstr(vtablePos, " USING ");
            char *tagsPos  = NULL;
            if (usingPos) {
                tagsPos = strstr(usingPos, " TAGS (");
                if (!tagsPos) tagsPos = strstr(usingPos, " TAGS(");
            }
            bool isChildSkeleton = (usingPos != NULL && tagsPos == NULL);

            // Build the renamed SQL (inject `targetDb`. prefix)
            char *nameStart = vtablePos + strlen("VTABLE");
            while (*nameStart == ' ') nameStart++;
            int prefixLen = (int)(nameStart - line);

            char *fullSql = (char *)taosMemoryMalloc(TSDB_MAX_SQL_LEN);
            if (fullSql == NULL) {
                code = TSDB_CODE_BCK_MALLOC_FAILED;
                break;
            }

            if (usingPos) {
                // Extract vtable name span
                const char *p = nameStart;
                int vtbLen;
                if (*p == '`') {
                    const char *close = strchr(p + 1, '`');
                    vtbLen = close ? (int)(close - p + 1) : (int)(usingPos - p);
                } else {
                    vtbLen = (int)(usingPos - p);
                }
                const char *middleStart = nameStart + vtbLen;
                int          middleLen  = (int)(usingPos - middleStart);
                char *stbNameStart = usingPos + strlen(" USING ");
                while (*stbNameStart == ' ') stbNameStart++;

                snprintf(fullSql, TSDB_MAX_SQL_LEN, "%.*s`%s`.%.*s%.*s USING `%s`.%s",
                         prefixLen, line,
                         targetDb,
                         vtbLen, nameStart,
                         middleLen, middleStart,
                         targetDb,
                         stbNameStart);
            } else {
                snprintf(fullSql, TSDB_MAX_SQL_LEN, "%.*s`%s`.%s",
                         prefixLen, line, targetDb, nameStart);
            }

            // Apply db rename in FROM/USING column references
            if (strcmp(dbName, targetDb) != 0) {
                char srcPat[TSDB_DB_NAME_LEN + 5];
                char dstPat[TSDB_DB_NAME_LEN + 5];
                snprintf(srcPat, sizeof(srcPat), "`%s`.", dbName);
                snprintf(dstPat, sizeof(dstPat), "`%s`.", targetDb);
                replaceAllInSql(fullSql, srcPat, dstPat);
            }

            if (isChildSkeleton) {
                // New-format skeleton: store for batch restore via vtags binary
                char bareTbName[TSDB_TABLE_NAME_LEN]   = {0};
                char bareVstbName[TSDB_TABLE_NAME_LEN] = {0};
                extractBareIdent(nameStart, bareTbName, sizeof(bareTbName));
                extractBareIdent(usingPos + strlen(" USING "), bareVstbName, sizeof(bareVstbName));

                if (skeletonCount >= skeletonCap) {
                    skeletonCap *= 2;
                    VtbChildSkeleton *tmp = (VtbChildSkeleton *)taosMemoryRealloc(
                                               skeletons, skeletonCap * sizeof(VtbChildSkeleton));
                    if (!tmp) {
                        taosMemoryFree(fullSql);
                        code = TSDB_CODE_BCK_MALLOC_FAILED;
                        break;
                    }
                    skeletons = tmp;
                }
                VtbChildSkeleton *sk = &skeletons[skeletonCount++];
                snprintf(sk->backupTbName,   sizeof(sk->backupTbName),   "%s", bareTbName);
                snprintf(sk->backupVstbName, sizeof(sk->backupVstbName), "%s", bareVstbName);
                snprintf(sk->renamedSql,     sizeof(sk->renamedSql),     "%s", fullSql);
            } else {
                // Execute immediately: virtual normal tables + old-format child DDLs with TAGS
                logDebug("restore vtable sql: %s", fullSql);
                TAOS_RES *res = taos_query(conn, fullSql);
                int rc = taos_errno(res);

                if (rc == TSDB_CODE_SUCCESS) {
                    vtbCount++;
                    atomic_add_fetch_64(&g_stats.ntbTotal, 1);
                } else if (rc == TSDB_CODE_TDB_TABLE_ALREADY_EXIST ||
                           rc == TSDB_CODE_MND_STB_ALREADY_EXIST) {
                    logWarn("virtual table already exists(0x%08X %s): %s", rc, taos_errstr(res), fullSql);
                    vtbCount++;
                    atomic_add_fetch_64(&g_stats.ntbTotal, 1);
                } else {
                    logError("create virtual table failed(0x%08X %s): %s", rc, taos_errstr(res), fullSql);
                    code = rc;
                }
                if (res) taos_free_result(res);
            }
            taosMemoryFree(fullSql);
        }

        line = next;
    }

    releaseConnection(conn);

    // Restore virtual child tables via vtags binary files (new-format backups)
    if (code == TSDB_CODE_SUCCESS && skeletonCount > 0) {
        // Collect unique vstb names across all skeletons
        char vstbsSeen[64][TSDB_TABLE_NAME_LEN];
        int  vstbSeenCnt = 0;
        memset(vstbsSeen, 0, sizeof(vstbsSeen));

        for (int i = 0; i < skeletonCount; i++) {
            const char *vn = skeletons[i].backupVstbName;
            bool found = false;
            for (int j = 0; j < vstbSeenCnt; j++) {
                if (strcasecmp(vstbsSeen[j], vn) == 0) { found = true; break; }
            }
            if (!found && vstbSeenCnt < 64) {
                snprintf(vstbsSeen[vstbSeenCnt], TSDB_TABLE_NAME_LEN, "%s", vn);
                vstbSeenCnt++;
            }
        }

        // Per-vstb: collect its children and restore from vtags binary
        for (int v = 0; v < vstbSeenCnt && code == TSDB_CODE_SUCCESS; v++) {
            if (g_interrupted) { code = TSDB_CODE_BCK_USER_CANCEL; break; }

            const char *vstbName = vstbsSeen[v];

            // Filter skeletons belonging to this vstb
            VtbChildSkeleton *vstbItems = (VtbChildSkeleton *)taosMemoryCalloc(
                                              skeletonCount, sizeof(VtbChildSkeleton));
            if (!vstbItems) { code = TSDB_CODE_BCK_MALLOC_FAILED; break; }
            int vstbItemCnt = 0;
            for (int i = 0; i < skeletonCount; i++) {
                if (strcasecmp(skeletons[i].backupVstbName, vstbName) == 0) {
                    vstbItems[vstbItemCnt++] = skeletons[i];
                }
            }

            logInfo("restoring %d virtual child table(s) under vstb: %s.%s",
                    vstbItemCnt, dbName, vstbName);
            code = restoreVstbChildTags(dbName, vstbName, vstbItems, vstbItemCnt);
            vtbCount += vstbItemCnt;
            taosMemoryFree(vstbItems);
        }
    }

    logInfo("restored %d virtual table(s) for db: %s", vtbCount, dbName);
    taosMemoryFree(skeletons);
    taosMemoryFree(content);
    return code;
}


//
// Context for tag block callback - builds CREATE TABLE SQL from raw block data
//
#define BCK_MAX_TAG_COLS 128

// Batch size for CREATE TABLE multi-table syntax:
//   CREATE TABLE IF NOT EXISTS t1 USING stb TAGS(...) t2 USING stb TAGS(...) ...
// One RPC per batch instead of one per CTB.  Each row needs at most TSDB_MAX_SQL_LEN
// bytes, so allocate TAG_BATCH_SQL_BYTES to hold up to TAG_BATCH_SIZE rows.
#define TAG_BATCH_SIZE      64
#define TAG_BATCH_SQL_BYTES (TAG_BATCH_SIZE * TSDB_MAX_SQL_LEN)

typedef struct {
    TAOS       *conn;
    const char *dbName;
    const char *stbName;
    FieldInfo  *fieldInfos;
    int         numFields;
    int64_t     successCnt;
    int64_t     failedCnt;
    // Server stable's tags in their declared order:
    int         serverTagCount;
    char        serverTagNames[BCK_MAX_TAG_COLS][65];
    // For each server tag: index into backup fieldInfos (1-based, so add 1).
    // -1 means the server has this tag but backup does not (emit NULL).
    int         tagMapping[BCK_MAX_TAG_COLS];
    // Reusable SQL buffer for batch CREATE TABLE (avoids per-row malloc)
    char       *sqlBuf;   // TAG_BATCH_SQL_BYTES, allocated once, reused across callbacks
} TagRestoreCtx;

//
// Query the server stable's tag names in declared order.
// Fills serverTagNames[] and tagMapping[] in ctx by matching against backup fieldInfos.
// Returns the number of server tags found, or -1 on error.
//
static int buildServerTagMapping(TAOS *conn, const char *dbName, const char *stbName,
                                  TagRestoreCtx *ctx) {
    char sql[TSDB_TABLE_FNAME_LEN + 32];
    snprintf(sql, sizeof(sql), "DESCRIBE `%s`.`%s`", dbName, stbName);
    TAOS_RES *res = taos_query(conn, sql);
    if (taos_errno(res) != TSDB_CODE_SUCCESS) {
        logError("DESCRIBE failed(0x%08X %s): %s", taos_errno(res), taos_errstr(res), sql);
        taos_free_result(res);
        return -1;
    }
    int numResultCols = taos_field_count(res);
    int tagCnt = 0;
    TAOS_ROW row;
    // TDengine C API: VARCHAR row[i] is NOT null-terminated; must use taos_fetch_lengths().
    while ((row = taos_fetch_row(res)) != NULL && tagCnt < BCK_MAX_TAG_COLS) {
        int *lens = taos_fetch_lengths(res);
        // DESCRIBE: col0=Field, col1=Type, col2=Length, col3=Note, ...
        if (numResultCols < 4 || row[3] == NULL || lens == NULL) continue;
        // Check note column == "TAG" (length-safe comparison)
        if (lens[3] != 3 || strncasecmp((char *)row[3], "TAG", 3) != 0) continue;
        if (row[0] == NULL) continue;

        // Copy field name (not null-terminated in API response)
        int nameLen = lens[0] < 64 ? lens[0] : 64;
        memcpy(ctx->serverTagNames[tagCnt], (char *)row[0], nameLen);
        ctx->serverTagNames[tagCnt][nameLen] = '\0';
        const char *tagName = ctx->serverTagNames[tagCnt];

        // Find this tag name in backup fieldInfos (skip col 0 which is tbname).
        ctx->tagMapping[tagCnt] = -1;  // default: not in backup -> emit NULL
        for (int c = 1; c < ctx->numFields; c++) {
            if (strcasecmp(ctx->fieldInfos[c].name, tagName) == 0) {
                ctx->tagMapping[tagCnt] = c;
                break;
            }
        }
        tagCnt++;
    }
    taos_free_result(res);
    return tagCnt;
}

//
// Query server stable tag names into a flat array (no backup matching needed).
// Used for the Parquet path where backup field names come from the callback.
// Returns tag count on success, or -1 on failure.
//
static int queryServerTagNames(TAOS *conn, const char *dbName, const char *stbName,
                               char names[][65], int maxNames) {
    char sql[TSDB_TABLE_FNAME_LEN + 32];
    snprintf(sql, sizeof(sql), "DESCRIBE `%s`.`%s`", dbName, stbName);
    TAOS_RES *res = taos_query(conn, sql);
    if (taos_errno(res) != TSDB_CODE_SUCCESS) {
        taos_free_result(res);
        return -1;
    }
    int numResultCols = taos_field_count(res);
    int tagCnt = 0;
    TAOS_ROW row;
    // TDengine C API: VARCHAR row[i] is NOT null-terminated; must use taos_fetch_lengths().
    while ((row = taos_fetch_row(res)) != NULL && tagCnt < maxNames) {
        int *lens = taos_fetch_lengths(res);
        if (numResultCols < 4 || row[3] == NULL || lens == NULL) continue;
        if (lens[3] != 3 || strncasecmp((char *)row[3], "TAG", 3) != 0) continue;
        if (row[0] == NULL) continue;
        int nameLen = lens[0] < 64 ? lens[0] : 64;
        memcpy(names[tagCnt], (char *)row[0], nameLen);
        names[tagCnt][nameLen] = '\0';
        tagCnt++;
    }
    taos_free_result(res);
    return tagCnt;
}

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
// Minimal WKB (little-endian, no SRID) → WKT converter for POINT/LINESTRING/POLYGON.
// Returns number of characters written (like snprintf), or -1 on error.
//
static int bckWkbToWkt(const unsigned char *wkb, int wkbLen, char *out, int outLen) {
    if (!wkb || wkbLen < 5 || !out || outLen < 16) return -1;
    if (wkb[0] != 0x01) return -1;   // only little-endian supported

    uint32_t geomType;
    memcpy(&geomType, wkb + 1, 4);

    const unsigned char *p = wkb + 5;
    int rem = wkbLen - 5;
    int pos = 0;

    switch (geomType) {
        case 1: { /* POINT */
            if (rem < 16) return -1;
            double x, y;
            memcpy(&x, p,     8);
            memcpy(&y, p + 8, 8);
            pos += snprintf(out + pos, outLen - pos, "POINT(%.*g %.*g)",
                            17, x, 17, y);
            break;
        }
        case 2: { /* LINESTRING */
            if (rem < 4) return -1;
            uint32_t n;
            memcpy(&n, p, 4);
            p += 4; rem -= 4;
            if (rem < (int)(n * 16)) return -1;
            pos += snprintf(out + pos, outLen - pos, "LINESTRING(");
            for (uint32_t i = 0; i < n; i++) {
                double x, y;
                memcpy(&x, p,     8);
                memcpy(&y, p + 8, 8);
                p   += 16;
                rem -= 16;
                if (i > 0 && pos < outLen - 2) out[pos++] = ',';
                pos += snprintf(out + pos, outLen - pos, "%.*g %.*g",
                                17, x, 17, y);
            }
            if (pos < outLen - 1) out[pos++] = ')';
            break;
        }
        case 3: { /* POLYGON */
            if (rem < 4) return -1;
            uint32_t numRings;
            memcpy(&numRings, p, 4);
            p += 4; rem -= 4;
            pos += snprintf(out + pos, outLen - pos, "POLYGON(");
            for (uint32_t r = 0; r < numRings; r++) {
                if (rem < 4) return -1;
                uint32_t n;
                memcpy(&n, p, 4);
                p += 4; rem -= 4;
                if (rem < (int)(n * 16)) return -1;
                if (r > 0 && pos < outLen - 2) out[pos++] = ',';
                if (pos < outLen - 1) out[pos++] = '(';
                for (uint32_t i = 0; i < n; i++) {
                    double x, y;
                    memcpy(&x, p,     8);
                    memcpy(&y, p + 8, 8);
                    p   += 16;
                    rem -= 16;
                    if (i > 0 && pos < outLen - 2) out[pos++] = ',';
                    pos += snprintf(out + pos, outLen - pos, "%.*g %.*g",
                                    17, x, 17, y);
                }
                if (pos < outLen - 1) out[pos++] = ')';
            }
            if (pos < outLen - 1) out[pos++] = ')';
            if (pos < outLen)     out[pos]   = '\0';
            break;
        }
        default:
            return -1;  /* unsupported geometry type */
    }

    if (pos < outLen) out[pos] = '\0';
    return pos;
}

// Append one tag value (for a single row/column) into sql at *pos.
static void appendTagVal(char *sql, int *pos, int maxLen,
                         int8_t type, int row, int c,
                         void **colDataPtrs, int blockRows,
                         FieldInfo *fieldInfos) {
    if (c < 0) {
        *pos += snprintf(sql + *pos, maxLen - *pos, "NULL");
        return;
    }
    if (IS_VAR_DATA_TYPE(type)) {
        int32_t *_to = (int32_t *)colDataPtrs[c];
        int32_t  _off = _to[row];
        if (_off < 0) {
            *pos += snprintf(sql + *pos, maxLen - *pos, "NULL");
        } else {
            char    *_vd  = (char *)colDataPtrs[c] + blockRows * sizeof(int32_t);
            uint16_t _vl  = *(uint16_t *)(_vd + _off);
            char    *_val = _vd + _off + sizeof(uint16_t);
            if (type == TSDB_DATA_TYPE_NCHAR) {
                char *_utf8 = (char *)taosMemoryMalloc((_vl) * 4 + 4);
                if (!_utf8) {
                    *pos += snprintf(sql + *pos, maxLen - *pos, "NULL");
                } else {
                    int32_t _ul = taosUcs4ToMbs((TdUcs4 *)_val, (int32_t)_vl, _utf8, NULL);
                    if (_ul < 0) _ul = 0;
                    *pos += snprintf(sql + *pos, maxLen - *pos, "'");
                    for (int32_t _k = 0; _k < _ul && *pos < maxLen - 4; _k++) {
                        if (_utf8[_k] == '\'') { sql[(*pos)++] = '\\'; sql[(*pos)++] = '\''; }
                        else { sql[(*pos)++] = _utf8[_k]; }
                    }
                    *pos += snprintf(sql + *pos, maxLen - *pos, "'");
                    taosMemoryFree(_utf8);
                }
            } else if (type == TSDB_DATA_TYPE_GEOMETRY) {
                char _wkt[4096];
                int _wl = bckWkbToWkt((const unsigned char *)_val, (int)_vl, _wkt, (int)sizeof(_wkt));
                if (_wl > 0) *pos += snprintf(sql + *pos, maxLen - *pos, "'%s'", _wkt);
                else         *pos += snprintf(sql + *pos, maxLen - *pos, "NULL");
            } else {
                *pos += snprintf(sql + *pos, maxLen - *pos, "'");
                for (uint16_t _k = 0; _k < _vl && *pos < maxLen - 4; _k++) {
                    unsigned char _b = (unsigned char)_val[_k];
                    if      (_b == '\'') { sql[(*pos)++] = '\\'; sql[(*pos)++] = '\''; }
                    else if (_b == '\\') { sql[(*pos)++] = '\\'; sql[(*pos)++] = '\\'; }
                    else                 { sql[(*pos)++] = (char)_b; }
                }
                *pos += snprintf(sql + *pos, maxLen - *pos, "'");
            }
        }
    } else {
        int32_t _bml  = (blockRows + 7) / 8;
        char   *_bmp  = (char *)colDataPtrs[c];
        char   *_fd   = (char *)colDataPtrs[c] + _bml;
        bool    _null = (_bmp[row >> 3] & (1u << (7 - (row & 7)))) != 0;
        if (_null) {
            *pos += snprintf(sql + *pos, maxLen - *pos, "NULL");
        } else {
            int32_t _bytes = fieldInfos[c].bytes;
            char   *_v     = _fd + row * _bytes;
            switch (type) {
                case TSDB_DATA_TYPE_BOOL:      *pos += snprintf(sql+*pos, maxLen-*pos, "%s", (*(int8_t*)_v)?"true":"false"); break;
                case TSDB_DATA_TYPE_TINYINT:   *pos += snprintf(sql+*pos, maxLen-*pos, "%d",   *(int8_t *)_v); break;
                case TSDB_DATA_TYPE_SMALLINT:  *pos += snprintf(sql+*pos, maxLen-*pos, "%d",   *(int16_t*)_v); break;
                case TSDB_DATA_TYPE_INT:       *pos += snprintf(sql+*pos, maxLen-*pos, "%d",   *(int32_t*)_v); break;
                case TSDB_DATA_TYPE_BIGINT:    *pos += snprintf(sql+*pos, maxLen-*pos, "%"PRId64, *(int64_t*)_v); break;
                case TSDB_DATA_TYPE_UTINYINT:  *pos += snprintf(sql+*pos, maxLen-*pos, "%u",   *(uint8_t *)_v); break;
                case TSDB_DATA_TYPE_USMALLINT: *pos += snprintf(sql+*pos, maxLen-*pos, "%u",   *(uint16_t*)_v); break;
                case TSDB_DATA_TYPE_UINT:      *pos += snprintf(sql+*pos, maxLen-*pos, "%u",   *(uint32_t*)_v); break;
                case TSDB_DATA_TYPE_UBIGINT:   *pos += snprintf(sql+*pos, maxLen-*pos, "%"PRIu64, *(uint64_t*)_v); break;
                case TSDB_DATA_TYPE_FLOAT:     *pos += snprintf(sql+*pos, maxLen-*pos, "%f",   *(float *)_v); break;
                case TSDB_DATA_TYPE_DOUBLE:    *pos += snprintf(sql+*pos, maxLen-*pos, "%f",   *(double*)_v); break;
                case TSDB_DATA_TYPE_TIMESTAMP: *pos += snprintf(sql+*pos, maxLen-*pos, "%"PRId64, *(int64_t*)_v); break;
                default: *pos += snprintf(sql+*pos, maxLen-*pos, "NULL"); break;
            }
        }
    }
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

    // Allocate reusable SQL buffer once (on first call for this ctx)
    if (!ctx->sqlBuf) {
        ctx->sqlBuf = (char *)taosMemoryMalloc(TAG_BATCH_SQL_BYTES);
        if (!ctx->sqlBuf) {
            taosMemoryFree(colDataPtrs);
            taosMemoryFree(colDataLens);
            return TSDB_CODE_BCK_MALLOC_FAILED;
        }
    }

    const char *rdbName  = argRenameDb(ctx->dbName);
    int useMapping = (ctx->serverTagCount > 0);
    int loopCount  = useMapping ? ctx->serverTagCount : (numCols - 1);

    // Batch rows: accumulate up to TAG_BATCH_SIZE child-table clauses into one SQL,
    // then fire a single taos_query instead of one query per row.
    int row = 0;
    while (row < blockRows) {
        if (g_interrupted) {
            taosMemoryFree(colDataPtrs);
            taosMemoryFree(colDataLens);
            return TSDB_CODE_BCK_USER_CANCEL;
        }

        // Build one batch SQL: CREATE TABLE IF NOT EXISTS t1 USING stb TAGS(...) t2 ...
        int   pos      = 0;
        int   batchCnt = 0;
        int   batchStart = row;

        while (row < blockRows && batchCnt < TAG_BATCH_SIZE) {
            // Read tbname
            int32_t *offsets = (int32_t *)colDataPtrs[0];
            int32_t  offset  = offsets[row];
            if (offset < 0) {
                logWarn("tbname is NULL at row %d, skip", row);
                ctx->failedCnt++;
                row++;
                continue;
            }
            char *varData = (char *)colDataPtrs[0] + blockRows * sizeof(int32_t);
            uint16_t varLen = *(uint16_t *)(varData + offset);
            if (varLen >= TSDB_TABLE_NAME_LEN) varLen = TSDB_TABLE_NAME_LEN - 1;
            char tbName[TSDB_TABLE_NAME_LEN] = {0};
            memcpy(tbName, varData + offset + sizeof(uint16_t), varLen);
            tbName[varLen] = '\0';

            // Append "CREATE TABLE IF NOT EXISTS `db`.`tb` USING `db`.`stb` TAGS("
            pos += snprintf(ctx->sqlBuf + pos, TAG_BATCH_SQL_BYTES - pos,
                            "CREATE TABLE IF NOT EXISTS `%s`.`%s` USING `%s`.`%s` TAGS(",
                            rdbName, tbName, rdbName, ctx->stbName);

            for (int s = 0; s < loopCount; s++) {
                if (s > 0) pos += snprintf(ctx->sqlBuf + pos, TAG_BATCH_SQL_BYTES - pos, ",");
                int c = useMapping ? ctx->tagMapping[s] : (s + 1);
                int8_t type = (c >= 0) ? fieldInfos[c].type : TSDB_DATA_TYPE_NULL;
                appendTagVal(ctx->sqlBuf, &pos, TAG_BATCH_SQL_BYTES, type, row, c, colDataPtrs, blockRows, fieldInfos);
            }
            pos += snprintf(ctx->sqlBuf + pos, TAG_BATCH_SQL_BYTES - pos, ") ");

            batchCnt++;
            row++;
        }

        if (batchCnt == 0) continue;

        logDebug("restore tag batch sql (%d tables): %.120s...", batchCnt, ctx->sqlBuf);

        TAOS_RES *res  = taos_query(ctx->conn, ctx->sqlBuf);
        int       rc   = taos_errno(res);
        if (res) taos_free_result(res);

        if (rc == TSDB_CODE_SUCCESS || rc == TSDB_CODE_TDB_TABLE_ALREADY_EXIST) {
            ctx->successCnt += batchCnt;
            atomic_add_fetch_64(&g_stats.childTablesTotal, batchCnt);
            atomic_add_fetch_64(&g_progress.ctbDoneCur, batchCnt);
        } else {
            // Batch failed: fall back to one-by-one to identify and skip bad rows
            logDebug("tag batch failed (0x%08X), retrying %d rows one-by-one", rc, batchCnt);
            for (int r = batchStart; r < row; r++) {
                int32_t *off2 = (int32_t *)colDataPtrs[0];
                if (off2[r] < 0) continue;
                char *vd2  = (char *)colDataPtrs[0] + blockRows * sizeof(int32_t);
                uint16_t vl2 = *(uint16_t *)(vd2 + off2[r]);
                if (vl2 >= TSDB_TABLE_NAME_LEN) vl2 = TSDB_TABLE_NAME_LEN - 1;
                char tb2[TSDB_TABLE_NAME_LEN] = {0};
                memcpy(tb2, vd2 + off2[r] + sizeof(uint16_t), vl2);
                tb2[vl2] = '\0';

                int p2 = snprintf(ctx->sqlBuf, TAG_BATCH_SQL_BYTES,
                                  "CREATE TABLE IF NOT EXISTS `%s`.`%s` USING `%s`.`%s` TAGS(",
                                  rdbName, tb2, rdbName, ctx->stbName);
                for (int s = 0; s < loopCount; s++) {
                    if (s > 0) p2 += snprintf(ctx->sqlBuf + p2, TAG_BATCH_SQL_BYTES - p2, ",");
                    int c2 = useMapping ? ctx->tagMapping[s] : (s + 1);
                    int8_t t2 = (c2 >= 0) ? fieldInfos[c2].type : TSDB_DATA_TYPE_NULL;
                    appendTagVal(ctx->sqlBuf, &p2, TAG_BATCH_SQL_BYTES, t2, r, c2, colDataPtrs, blockRows, fieldInfos);
                }
                p2 += snprintf(ctx->sqlBuf + p2, TAG_BATCH_SQL_BYTES - p2, ")");

                TAOS_RES *r2 = taos_query(ctx->conn, ctx->sqlBuf);
                int rc2 = taos_errno(r2);
                if (r2) taos_free_result(r2);
                if (rc2 == TSDB_CODE_SUCCESS || rc2 == TSDB_CODE_TDB_TABLE_ALREADY_EXIST) {
                    ctx->successCnt++;
                    atomic_add_fetch_64(&g_stats.childTablesTotal, 1);
                    atomic_add_fetch_64(&g_progress.ctbDoneCur, 1);
                } else {
                    logError("create child table failed(0x%08X): %s.%s", rc2, rdbName, tb2);
                    ctx->failedCnt++;
                }
            }
        }
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
    int         serverTagCount;  // number of server tags
    char        serverTagNames[BCK_MAX_TAG_COLS][65];  // server tag names in declared order
} ParquetTagCtx;

static int parquetTagCallback(void *userData,
                              TAOS_FIELD *fields, int numFields,
                              TAOS_MULTI_BIND *bindArray, int32_t numRows) {
    ParquetTagCtx *ctx = (ParquetTagCtx *)userData;
    if (numFields < 1) return TSDB_CODE_BCK_NO_FIELDS;

    // Build a per-callback mapping: for each server tag name, find its backup field index.
    // fields[0] is tbname; fields[1..numFields-1] are backup tags.
    // serverTagCount==0 means DESCRIBE failed; use backup tag order as fallback.
    int  sTagMapBuf[BCK_MAX_TAG_COLS];
    int  useMapping = (ctx->serverTagCount > 0);
    int  loopCount  = useMapping ? ctx->serverTagCount : (numFields - 1);
    if (useMapping) {
        for (int s = 0; s < ctx->serverTagCount; s++) {
            sTagMapBuf[s] = -1;  // default: not in backup
            for (int c = 1; c < numFields; c++) {
                if (strcasecmp(fields[c].name, ctx->serverTagNames[s]) == 0) {
                    sTagMapBuf[s] = c;
                    break;
                }
            }
        }
    }

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

        /* Emit TAGS in server's declared order (name-based mapping) */
        for (int s = 0; s < loopCount && pos < TSDB_MAX_SQL_LEN - 64; s++) {
            if (s > 0) pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, ",");

            // c = backup bind index; -1 → server has tag not in backup → NULL
            int c = useMapping ? sTagMapBuf[s] : (s + 1);
            if (c < 0) {
                pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, "NULL");
                continue;
            }

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
                if (type == TSDB_DATA_TYPE_GEOMETRY) {
                    /* GEOMETRY is stored as WKB; SQL requires WKT notation.    */
                    char wkt[4096];
                    int  wktLen = bckWkbToWkt((const unsigned char *)vPtr, vLen, wkt, (int)sizeof(wkt));
                    if (wktLen > 0) {
                        pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, "'%s'", wkt);
                    } else {
                        pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, "NULL");
                    }
                } else {
                    pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, "'");
                    for (int32_t k = 0; k < vLen && pos < TSDB_MAX_SQL_LEN - 4; k++) {
                        if (vPtr[k] == '\'') sql[pos++] = '\\';
                        if (pos < TSDB_MAX_SQL_LEN - 2) sql[pos++] = vPtr[k];
                    }
                    pos += snprintf(sql + pos, TSDB_MAX_SQL_LEN - pos, "'");
                }
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
            atomic_add_fetch_64(&g_progress.ctbDoneCur, 1);
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

    logDebug("restore tag thread %d start, file: %s", thread->index, thread->tagFile);
    const char *_dbName4log  = thread->dbInfo  ? thread->dbInfo->dbName  : "?";
    const char *_stb4log     = thread->stbInfo ? thread->stbInfo->stbName : "?";

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
        {
            int n = queryServerTagNames(ctx.conn, argRenameDb(ctx.dbName), ctx.stbName,
                                        ctx.serverTagNames, BCK_MAX_TAG_COLS);
            if (n > 0) {
                ctx.serverTagCount = n;
                logDebug("parquet stb %s.%s: server has %d tags; using name mapping",
                        argRenameDb(ctx.dbName), ctx.stbName, n);
            } else {
                logWarn("parquet stb %s.%s: DESCRIBE failed, using backup tag order",
                        argRenameDb(ctx.dbName), ctx.stbName);
            }
        }

        code = parquetReaderReadAll(pr, parquetTagCallback, &ctx);
        parquetReaderClose(pr);
        if (code != TSDB_CODE_SUCCESS)
            logError("read parquet tag file failed(%d): %s", code, thread->tagFile);

        logDebug("restore tag thread %d done. success: %" PRId64 " failed: %" PRId64,
                thread->index, ctx.successCnt, ctx.failedCnt);
        logInfo("tag thread %d finished for %s.%s", thread->index, _dbName4log, _stb4log);
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
    memset(&ctx, 0, sizeof(ctx));
    ctx.conn       = thread->conn;
    ctx.dbName     = thread->dbInfo->dbName;
    ctx.stbName    = thread->stbInfo->stbName;
    ctx.fieldInfos = (FieldInfo *)taosFile->header.schema;
    ctx.numFields  = taosFile->header.numFields;
    ctx.successCnt = 0;
    ctx.failedCnt  = 0;
    // Build name-based mapping: server tag order -> backup column index.
    // serverTagCount == 0 on failure; callback falls back to backup order.
    {
        int n = buildServerTagMapping(ctx.conn, argRenameDb(ctx.dbName), ctx.stbName, &ctx);
        if (n > 0) {
            ctx.serverTagCount = n;
            logDebug("stb %s.%s: server has %d tags, backup has %d; using name mapping",
                    argRenameDb(ctx.dbName), ctx.stbName, n, ctx.numFields - 1);
        } else {
            logWarn("stb %s.%s: DESCRIBE failed, using backup tag order",
                    argRenameDb(ctx.dbName), ctx.stbName);
        }
    }

    // read blocks and process
    code = readTaosFileBlocks(taosFile, tagBlockCallback, &ctx);
    if (code != TSDB_CODE_SUCCESS) {
        logError("read tag blocks failed(%d): %s", code, thread->tagFile);
    }
    thread->code = code;

    logDebug("restore tag thread %d done. success: %" PRId64 " failed: %" PRId64,
            thread->index, ctx.successCnt, ctx.failedCnt);
    logInfo("tag thread %d finished for %s.%s", thread->index, _dbName4log, _stb4log);

    if (ctx.sqlBuf) { taosMemoryFree(ctx.sqlBuf); ctx.sqlBuf = NULL; }
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

    logDebug("found %d tag files for %s.%s", fileCnt, dbName, stbName);

    // Sum row counts from tag file headers to set ctbTotalCur for progress display.
    // .dat: read numRows from binary header.  .par: read from Parquet footer metadata.
    {
        int64_t totalCtbs = 0;
        for (int i = 0; i < fileCnt; i++) {
            const char *ext = strrchr(tagFiles[i], '.');
            if (ext && strcmp(ext, ".dat") == 0) {
                int peekCode = 0;
                TaosFile *pf = openTaosFileForRead(tagFiles[i], &peekCode);
                if (pf) {
                    totalCtbs += pf->header.numRows;
                    closeTaosFileRead(pf);
                }
            } else if (ext && strcmp(ext, ".par") == 0) {
                int64_t n = parquetGetNumRowsQuick(tagFiles[i]);
                if (n > 0) totalCtbs += n;
            }
        }
        g_progress.ctbTotalCur = totalCtbs;
        atomic_add_fetch_64(&g_progress.ctbTotalAll, totalCtbs);
        atomic_store_64(&g_progress.ctbDoneCur, 0);
    }

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
        logInfo("tag thread %d started for %s.%s", i + 1, dbName, stbName);
        threads[i].conn    = getConnection(&code);
        if (!threads[i].conn) {
            // Join already-started threads before freeing shared state
            for (int j = 0; j < i; j++) {
                taosThreadJoin(threads[j].pid, NULL);
                releaseConnection(threads[j].conn);
            }
            taosMemoryFree(threads);
            freeArrayPtr(tagFiles);
            return code;
        }
        snprintf(threads[i].tagFile, MAX_PATH_LEN, "%s", tagFiles[i]);

        if (taosThreadCreate(&threads[i].pid, NULL, restoreTagThread, (void *)&threads[i]) != 0) {
            logError("create restore tag thread failed(%s) for stb: %s.%s", 
                     strerror(errno), dbName, stbName);
            // Join already-started threads before freeing shared state
            for (int j = 0; j < i; j++) {
                taosThreadJoin(threads[j].pid, NULL);
                releaseConnection(threads[j].conn);
            }
            // Release connection for thread i (never started)
            releaseConnection(threads[i].conn);
            taosMemoryFree(threads);
            freeArrayPtr(tagFiles);
            return TSDB_CODE_BCK_CREATE_THREAD_FAILED;
        }
    }

    // wait threads
    for (int i = 0; i < actualThreads; i++) {
        taosThreadJoin(threads[i].pid, NULL);
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

    // count STBs for meta progress
    int stbCount = 0;
    for (int k = 0; stbNames[k] != NULL; k++) stbCount++;

    // set up META phase progress
    g_progress.phase    = PROGRESS_PHASE_META;
    g_progress.stbTotal = stbCount;
    g_progress.stbIndex = 0;
    g_progress.stbName[0] = '\0';
    g_progress.ctbTotalCur = 0;
    atomic_store_64(&g_progress.ctbDoneCur,  0);
    atomic_store_64(&g_progress.ctbDoneAll,  0);
    atomic_store_64(&g_progress.ctbTotalAll, 0);
    g_progress.startMs = taosGetTimestampMs();

    for (int i = 0; stbNames[i] != NULL; i++) {
        if (g_interrupted) {
            code = TSDB_CODE_BCK_USER_CANCEL;
            break;
        }

        // update progress for this STB
        g_progress.stbIndex = i + 1;
        snprintf(g_progress.stbName, PROGRESS_STB_NAME_LEN, "%s", stbNames[i]);
        g_progress.ctbTotalCur = 0;
        atomic_store_64(&g_progress.ctbDoneCur, 0);

        atomic_add_fetch_64(&g_stats.stbTotal, 1);
        logInfo("[%lld/%lld] db: %s  [%d/%d] stb: %s  meta start",
                (long long)g_progress.dbIndex, (long long)g_progress.dbTotal,
                dbName, i + 1, stbCount, stbNames[i]);
        StbInfo stbInfo;
        memset(&stbInfo, 0, sizeof(StbInfo));
        stbInfo.dbInfo = &dbInfo;
        stbInfo.stbName = stbNames[i];

        code = restoreStbTags(&dbInfo, &stbInfo);

        // accumulate completed CTBs
        atomic_add_fetch_64(&g_progress.ctbDoneAll, g_progress.ctbDoneCur);
        atomic_store_64(&g_progress.ctbDoneCur, 0);

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

    //
    // 5. Restore virtual table DDL (vtb.sql)
    //    Must run after physical tables so that virtual columns can reference them
    //
    code = restoreVtbSql(dbName);
    if (code != TSDB_CODE_SUCCESS) {
        logError("restore vtb sql failed(%d): %s", code, dbName);
        return code;
    }

    return TSDB_CODE_SUCCESS;
}
