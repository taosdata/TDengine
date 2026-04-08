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
    
#include "backupMeta.h"
#include "bckPool.h"
#include "bckDb.h"
#include "bckArgs.h"
#include "bckProgress.h"


//
// ----------------------------------- FUNCTION --------------------------------------
//




//
// -------------------------------------- META -----------------------------------------
//
int backCreateDbSql(const char *dbName) {
    int code = TSDB_CODE_FAILED;
    
    // path
    char sqlFile[MAX_PATH_LEN] = {0};
    obtainFileName(BACK_FILE_DBSQL, dbName, NULL, NULL, 0, 0, BINARY_TAOS, sqlFile, sizeof(sqlFile));

    // sql
    char sql[512] = {0};
    snprintf(sql, sizeof(sql), "show create database `%s`;", dbName);
    code = queryWriteTxt(sql, 1, sqlFile);

    return code;
}

int backCreateStbSql(const char *dbName, const char *stbName) {
    // path: stb.sql (APPEND mode; caller pre-truncates before the loop)
    char sqlFile[MAX_PATH_LEN] = {0};
    obtainFileName(BACK_FILE_STBSQL, dbName, NULL, NULL, 0, 0, BINARY_TAOS, sqlFile, sizeof(sqlFile));

    char sql[512] = {0};
    snprintf(sql, sizeof(sql), "SHOW CREATE TABLE `%s`.`%s`;", dbName, stbName);

    int connCode = TSDB_CODE_FAILED;
    TAOS *conn = getConnection(&connCode);
    if (!conn) return connCode;

    TAOS_RES *res = taos_query(conn, sql);
    int code = taos_errno(res);
    if (!res || code) {
        logError("show create table failed(0x%08X %s): %s", code, taos_errstr(res), sql);
        if (res) taos_free_result(res);
        releaseConnection(conn);
        return code;
    }

    TAOS_ROW row = taos_fetch_row(res);
    if (!row || !row[1]) {
        taos_free_result(res);
        releaseConnection(conn);
        return TSDB_CODE_BCK_NO_FIELDS;
    }

    int32_t *lengths = taos_fetch_lengths(res);
    int      ddlLen  = lengths[1];
    char    *ddl     = (char *)row[1];

    // Open stb.sql in APPEND mode so multiple STBs accumulate.
    // The caller is responsible for truncating the file before the STB loop starts.
    TdFilePtr fp = taosOpenFile(sqlFile, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_APPEND);
    if (!fp) {
        logError("open stb.sql for append failed: %s", sqlFile);
        taos_free_result(res);
        releaseConnection(conn);
        return TSDB_CODE_BCK_CREATE_FILE_FAILED;
    }

    // SHOW CREATE TABLE for virtual super tables can embed '\n' characters
    // (e.g., between TAGS columns, or before "VIRTUAL 1").
    // Replace all embedded newlines/CR with spaces so that each STB occupies
    // exactly ONE line in stb.sql.  Write the entire cleaned DDL in one call
    // (much faster than byte-by-byte writing).
    int writeCode = TSDB_CODE_SUCCESS;
    char *buf = (char *)taosMemoryMalloc(ddlLen + 2);
    if (buf) {
        int n = 0;
        for (int i = 0; i < ddlLen; i++) {
            buf[n++] = (ddl[i] == '\n' || ddl[i] == '\r') ? ' ' : ddl[i];
        }
        buf[n++] = '\n';
        if (taosWriteFile(fp, buf, n) != n) {
            logError("write stb DDL to file failed: %s", sqlFile);
            writeCode = TSDB_CODE_BCK_WRITE_FILE_FAILED;
        }
        taosMemoryFree(buf);
    } else {
        logError("malloc stb DDL buffer failed for: %s.%s", dbName, stbName);
        writeCode = TSDB_CODE_BCK_MALLOC_FAILED;
    }

    taosCloseFile(&fp);
    taos_free_result(res);
    releaseConnection(conn);
    return writeCode;
}

int backStbSchema(const char *dbName, const char *stbName, char ** selectTags) {
    int code = TSDB_CODE_FAILED;
    
    // path
    char csvFile[MAX_PATH_LEN] = {0};    
    obtainFileName(BACK_FILE_STBCSV, dbName, stbName, NULL, 0, 0, BINARY_TAOS, csvFile, sizeof(csvFile));
    
    // csv
    char sql[512] = {0};
    snprintf(sql, sizeof(sql), "describe `%s`.`%s`;", dbName, stbName);
    code = queryWriteCsv(sql, csvFile, selectTags);

    return code;
}

//
// back tag data thread
//
static void* backTagThread(void *arg) {
    TagThread *thread = (TagThread *)arg;
    thread->code = TSDB_CODE_SUCCESS;
    logInfo("tag thread %d started for %s.%s (offset=%d, limit=%d)",
            thread->index, thread->dbInfo->dbName, thread->stbInfo->stbName,
            thread->offset, thread->limit);

    char *sql = (char *)taosMemoryCalloc(TSDB_MAX_SQL_LEN, sizeof(char));
    if (sql == NULL) {
        logError("allocate sql buffer failed");
        thread->code = TSDB_CODE_BCK_MALLOC_FAILED;
        return NULL;
    }
    // build WHERE clause for specific tables if requested
    // If the STB name itself is in specTables the user wants the whole STB,
    // so do NOT filter by child table name.
    char whereClause[4096] = "";
    if (argSpecTables() && !argStbNameInSpecTables(thread->stbInfo->stbName)) {
        char inClause[3800] = "";
        argBuildInClause("tbname", inClause, sizeof(inClause));
        snprintf(whereClause, sizeof(whereClause), "WHERE %s ", inClause);
    }
    snprintf(sql, TSDB_MAX_SQL_LEN, 
             "SELECT DISTINCT tbname,%s FROM `%s`.`%s` "
             "%sORDER BY tbname "
             "LIMIT %d OFFSET %d;",
             thread->stbInfo->selectTags,
             thread->dbInfo->dbName,
             thread->stbInfo->stbName,
             whereClause,
             thread->limit,
             thread->offset);
    logDebug("backing up tags for thread %d: %s", thread->index, sql);         
    
    // sql result to file
    char fileName[MAX_PATH_LEN] = {0};
    StorageFormat format = argStorageFormat();

    int code = obtainFileName(BACK_FILE_TAG, thread->dbInfo->dbName, thread->stbInfo->stbName, NULL, thread->index, 0, format, fileName, sizeof(fileName));
    if (code != TSDB_CODE_SUCCESS) {
        logError("generate backup file name failed(%d): %s.%s", code, thread->dbInfo->dbName, thread->stbInfo->stbName);
        thread->code = code;
        freePtr(sql);
        return NULL;
    }

    code = queryWriteBinary(thread->conn, sql, format, fileName, NULL, &g_progress.ctbDoneCur);
    if (code != TSDB_CODE_SUCCESS) {
        logError("query write binary failed. sql=%s, format=%d file=%s", sql, format, fileName);
    }
    thread->code = code;

    freePtr(sql);
    logInfo("tag thread %d finished for %s.%s",
            thread->index, thread->dbInfo->dbName, thread->stbInfo->stbName);
    return NULL;
}

//
// split backup tag task
//
TagThread * splitTaskTag(DBInfo *dbInfo, StbInfo *stbInfo, int *code, int *outCount) {
    int threadCnt = *outCount;
    const char* dbName = dbInfo->dbName;
    const char* stbName = stbInfo->stbName;

    char sql[8192] = {0};
    char specFilter[4096] = "";
    if (argSpecTables() && !argStbNameInSpecTables(stbName)) {
        // user specified individual CTBs, not the whole STB
        char inClause[3800] = "";
        argBuildInClause("table_name", inClause, sizeof(inClause));
        snprintf(specFilter, sizeof(specFilter), " AND %s", inClause);
    }
    snprintf(sql, sizeof(sql), "select count(*) from information_schema.ins_tables where db_name='%s' and stable_name='%s'%s;", dbName, stbName, specFilter);
    int tableCnt = 0;
    
    // query table count
    *code = queryValueInt(sql, 0, &tableCnt);
    if (*code != TSDB_CODE_SUCCESS) {
        logError("query table count failed(%d): %s", *code, sql);
        return NULL;
    }
    // zero
    if (tableCnt == 0) {
        logDebug("no child table found for super table: %s.%s", dbName, stbName);
        *code = TSDB_CODE_SUCCESS;
        *outCount = 0;
        return NULL;
    }

    if (tableCnt < threadCnt) {
        threadCnt = tableCnt;
    }

    // tag thread
    TagThread * threads = (TagThread *)taosMemoryCalloc(threadCnt, sizeof(TagThread));
    if (threads == NULL) {
        *code = TSDB_CODE_FAILED;
        return NULL;
    }

    int remain = tableCnt % threadCnt;
    int base = tableCnt / threadCnt;
    int offset = 0;

    for(int i = 0; i < threadCnt; i++) {
        threads[i].dbInfo  = dbInfo;
        threads[i].stbInfo = stbInfo;
        threads[i].index   = i + 1;
        threads[i].limit   = base;
        if (remain > 0) {
            remain--;
            threads[i].limit += 1;
        }
        threads[i].offset  = offset;
        threads[i].conn    = getConnection(code);
        if (!threads[i].conn) {
            for (int j = 0; j < i; j++) {
                releaseConnection(threads[j].conn);
            }
            taosMemoryFree(threads);
            return NULL;
        }
        offset += threads[i].limit;
    }

    // succ
    *outCount = threadCnt;
    *code = TSDB_CODE_SUCCESS;
    return threads;
}

//
// tag create threads
//
int backChildTableTags(DBInfo *dbInfo, StbInfo *stbInfo) {
    int code = TSDB_CODE_FAILED;
    int count = argTagThread();

    // splite tags with vgroups
    TagThread *threads = splitTaskTag(dbInfo, stbInfo, &code, &count);
    if (threads == NULL) {
        return code;
    }

    // calculate total child tables
    int totalChildTables = 0;
    for (int i = 0; i < count; i++) {
        totalChildTables += threads[i].limit;
    }
    // set per-STB CTB total for progress display
    g_progress.ctbTotalCur = totalChildTables;
    atomic_store_64(&g_progress.ctbDoneCur, 0);

    // create threads
    for (int i = 0; i < count; i++) {
        if(taosThreadCreate(&threads[i].pid, NULL, backTagThread, (void *)&threads[i]) != 0) {
            logError("create backup thread failed(%s) for stb:%s", strerror(errno), stbInfo->stbName);
            // Join already-started threads before freeing shared state
            for (int j = 0; j < i; j++) {
                taosThreadJoin(threads[j].pid, NULL);
                releaseConnection(threads[j].conn);
            }
            // Release connections for threads that were never started
            for (int j = i; j < count; j++) {
                releaseConnection(threads[j].conn);
            }
            freePtr(threads);
            return TSDB_CODE_BCK_CREATE_THREAD_FAILED;
        }
    }

    // wait threads
    for (int i = 0; i < count; i++) {
        taosThreadJoin(threads[i].pid, NULL);
        releaseConnection(threads[i].conn);
        if (code == TSDB_CODE_SUCCESS && threads[i].code != TSDB_CODE_SUCCESS) {
            code = threads[i].code;
        }
    }

    // free
    freePtr(threads);
    return code;
}

//
// Backup virtual child table tags for one virtual super table.
// Queries ins_tags (table_name, tag_name, tag_value) and stores result
// as a binary/parquet file in vtags/{vstbname}_data1.{dat|par}.
// This columnar approach handles an arbitrary number of TAG columns efficiently.
//
static int backVstbChildTags(DBInfo *dbInfo, StbInfo *stbInfo) {
    const char *dbName  = dbInfo->dbName;
    const char *stbName = stbInfo->stbName;

    // Check whether there are any virtual child tables for this vstb
    char cntSql[512] = {0};
    snprintf(cntSql, sizeof(cntSql),
             "SELECT count(*) FROM information_schema.ins_tables "
             "WHERE db_name='%s' AND stable_name='%s' AND type='VIRTUAL_CHILD_TABLE';",
             dbName, stbName);
    int32_t childCnt = 0;
    int code = queryValueInt(cntSql, 0, &childCnt);
    if (code != TSDB_CODE_SUCCESS || childCnt == 0) {
        logInfo("no virtual child tables for vstb: %s.%s", dbName, stbName);
        return TSDB_CODE_SUCCESS;
    }
    logInfo("backup %d virtual child table tag(s) for vstb: %s.%s", childCnt, dbName, stbName);

    // Create vtags/ directory
    char vttagDir[MAX_PATH_LEN] = {0};
    StorageFormat format = argStorageFormat();
    obtainFileName(BACK_DIR_VTAG, dbName, NULL, NULL, 0, 0, format, vttagDir, sizeof(vttagDir));
    if (taosMkDir(vttagDir) != 0) {
        // EEXIST is fine; any other error is fatal
        if (errno != EEXIST) {
            logError("create vtags dir failed: %s (errno=%d)", vttagDir, errno);
            return TSDB_CODE_BCK_CREATE_FILE_FAILED;
        }
    }

    // ins_tags gives us (table_name, tag_name, tag_value) for all child tables.
    // Order by table_name so rows for the same child are contiguous (aids restore).
    char sql[512] = {0};
    snprintf(sql, sizeof(sql),
             "SELECT table_name, tag_name, tag_value "
             "FROM information_schema.ins_tags "
             "WHERE db_name='%s' AND stable_name='%s' "
             "ORDER BY table_name, tag_name;",
             dbName, stbName);

    char vttagFile[MAX_PATH_LEN] = {0};
    obtainFileName(BACK_FILE_VTAG, dbName, stbName, NULL, 1, 0, format, vttagFile, sizeof(vttagFile));

    TAOS *conn = getConnection(&code);
    if (!conn) return code;

    code = queryWriteBinary(conn, sql, format, vttagFile, NULL, NULL);
    releaseConnection(conn);

    if (code != TSDB_CODE_SUCCESS) {
        logError("backup vtable tags failed(%d): %s.%s", code, dbName, stbName);
    }
    return code;
}

//
// normal tables create sql
//
int backNormalTablesSql(const char *dbName) {
    int code = TSDB_CODE_SUCCESS;

    // get normal table names
    char **ntbNames = getDBNormalTableNames(dbName, &code);
    if (ntbNames == NULL) {
        if (code != TSDB_CODE_SUCCESS) return code;
        return TSDB_CODE_SUCCESS; // no normal tables
    }

    // count
    int ntbCount = 0;
    for (int i = 0; ntbNames[i] != NULL; i++) ntbCount++;
    if (ntbCount == 0) {
        freeArrayPtr(ntbNames);
        return TSDB_CODE_SUCCESS;
    }

    logInfo("backup %d normal table(s) sql for db: %s", ntbCount, dbName);
    atomic_add_fetch_64(&g_stats.ntbTotal, ntbCount);

    // open ntb.sql file
    char ntbSqlFile[MAX_PATH_LEN] = {0};
    obtainFileName(BACK_FILE_NTBSQL, dbName, NULL, NULL, 0, 0, BINARY_TAOS, ntbSqlFile, sizeof(ntbSqlFile));

    TdFilePtr fp = taosOpenFile(ntbSqlFile, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
    if (!fp) {
        logError("open ntb.sql failed: %s", ntbSqlFile);
        freeArrayPtr(ntbNames);
        return TSDB_CODE_BCK_CREATE_FILE_FAILED;
    }

    // for each normal table: show create table -> write to file
    TAOS *conn = getConnection(&code);
    if (!conn) {
        taosCloseFile(&fp);
        freeArrayPtr(ntbNames);
        return code;
    }

    code = TSDB_CODE_SUCCESS;
    char sql[512];
    for (int i = 0; ntbNames[i] != NULL; i++) {
        if(g_interrupted) {
            code = TSDB_CODE_BCK_USER_CANCEL;
            break;
        }

        snprintf(sql, sizeof(sql), "show create table `%s`.`%s`;", dbName, ntbNames[i]);
        TAOS_RES *res = taos_query(conn, sql);
        code = taos_errno(res);
        if (!res || code != TSDB_CODE_SUCCESS) {
            logError("show create table failed(0x%08X %s): %s", code, taos_errstr(res), sql);
            if (res) taos_free_result(res);
            break;
        }

        TAOS_ROW row = taos_fetch_row(res);
        if (row && row[1]) {
            int32_t *lens = taos_fetch_lengths(res);
            if (taosWriteFile(fp, row[1], lens[1]) != lens[1] ||
                taosWriteFile(fp, "\n", 1) != 1) {
                logError("write ntb sql to file failed: %s", ntbSqlFile);
                code = TSDB_CODE_BCK_WRITE_FILE_FAILED;
                taos_free_result(res);
                break;
            }
        }
        taos_free_result(res);
    }

    releaseConnection(conn);
    taosCloseFile(&fp);
    freeArrayPtr(ntbNames);
    return code;
}

//
// check if a super table is virtual (its DDL contains VIRTUAL columns)
//
bool isVirtualSuperTable(const char *dbName, const char *stbName) {
    char sql[512];
    snprintf(sql, sizeof(sql), "SHOW CREATE TABLE `%s`.`%s`", dbName, stbName);
    int connCode = TSDB_CODE_FAILED;
    TAOS *conn = getConnection(&connCode);
    if (!conn) return false;
    TAOS_RES *res = taos_query(conn, sql);
    bool isVirtual = false;
    if (res && taos_errno(res) == TSDB_CODE_SUCCESS) {
        TAOS_ROW row = taos_fetch_row(res);
        if (row && row[1]) {
            isVirtual = (strstr((char *)row[1], "VIRTUAL 1") != NULL);
        }
    }
    if (res) taos_free_result(res);
    releaseConnection(conn);
    return isVirtual;
}

//
// virtual tables create DDL sql (vtb.sql)
// backs up: virtual normal tables + virtual child tables (DDL via SHOW CREATE VTABLE)
//
int backVirtualTablesSql(const char *dbName) {
    int code = TSDB_CODE_SUCCESS;

    // get virtual table names (VIRTUAL_NORMAL_TABLE + VIRTUAL_CHILD_TABLE)
    char **vtbNames = getDBVirtualTableNames(dbName, &code);
    if (vtbNames == NULL) {
        if (code != TSDB_CODE_SUCCESS) return code;
        return TSDB_CODE_SUCCESS; // no virtual tables
    }

    int vtbCount = 0;
    for (int i = 0; vtbNames[i] != NULL; i++) vtbCount++;
    if (vtbCount == 0) {
        freeArrayPtr(vtbNames);
        return TSDB_CODE_SUCCESS;
    }

    logInfo("backup %d virtual table DDL(s) for db: %s", vtbCount, dbName);

    // open vtb.sql file
    char vtbSqlFile[MAX_PATH_LEN] = {0};
    obtainFileName(BACK_FILE_VTBSQL, dbName, NULL, NULL, 0, 0, BINARY_TAOS, vtbSqlFile, sizeof(vtbSqlFile));

    TdFilePtr fp = taosOpenFile(vtbSqlFile, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
    if (!fp) {
        logError("open vtb.sql failed: %s", vtbSqlFile);
        freeArrayPtr(vtbNames);
        return TSDB_CODE_BCK_CREATE_FILE_FAILED;
    }

    TAOS *conn = getConnection(&code);
    if (!conn) {
        taosCloseFile(&fp);
        freeArrayPtr(vtbNames);
        return code;
    }

    code = TSDB_CODE_SUCCESS;
    char sql[512];
    for (int i = 0; vtbNames[i] != NULL; i++) {
        if (g_interrupted) {
            code = TSDB_CODE_BCK_USER_CANCEL;
            break;
        }

        snprintf(sql, sizeof(sql), "SHOW CREATE VTABLE `%s`.`%s`;", dbName, vtbNames[i]);
        TAOS_RES *res = taos_query(conn, sql);
        code = taos_errno(res);
        if (!res || code != TSDB_CODE_SUCCESS) {
            logError("show create vtable failed(0x%08X %s): %s", code, taos_errstr(res), sql);
            if (res) taos_free_result(res);
            break;
        }

        TAOS_ROW row = taos_fetch_row(res);
        if (row && row[1]) {
            int32_t *lens = taos_fetch_lengths(res);
            char    *ddl  = (char *)row[1];
            // Virtual CHILD table DDL ends with: ... USING `vstb` (`tag1`) TAGS (val1, val2)
            // Store only the skeleton (without " TAGS (...)") in vtb.sql.
            // Tag values are stored separately in vtags/ binary files (columnar storage).
            // Virtual NORMAL tables have no USING/TAGS clause — write the full DDL as-is.
            char *tagsPos = strstr(ddl, " TAGS (");
            if (!tagsPos) tagsPos = strstr(ddl, " TAGS(");
            // child table: write up to " TAGS ("; virtual normal: write full DDL
            int64_t ddlWriteLen = tagsPos ? (int64_t)(tagsPos - ddl) : (int64_t)lens[1];
            if (taosWriteFile(fp, ddl, ddlWriteLen) != ddlWriteLen ||
                taosWriteFile(fp, "\n", 1) != 1) {
                logError("write vtb sql to file failed: %s", vtbSqlFile);
                code = TSDB_CODE_BCK_WRITE_FILE_FAILED;
                taos_free_result(res);
                break;
            }
        }
        taos_free_result(res);
    }

    releaseConnection(conn);
    taosCloseFile(&fp);
    freeArrayPtr(vtbNames);
    return code;
}

//
// backup database meta
//
int backDatabaseMeta(DBInfo *dbInfo) {
    int code = TSDB_CODE_FAILED;
    const char *dbName = dbInfo->dbName;

    //
    // database sql
    //
    code = backCreateDbSql(dbName);
    if (code != TSDB_CODE_SUCCESS) {
        return code;
    }

    //
    // super tables meta
    //
    char ** stbNames = getDBSuperTableNames(dbName, &code);
    if (stbNames == NULL && code != TSDB_CODE_SUCCESS) {
        return code;
    }

    // Truncate stb.sql before writing any STBs so append mode starts clean
    {
        char stbSqlFile[MAX_PATH_LEN] = {0};
        obtainFileName(BACK_FILE_STBSQL, dbName, NULL, NULL, 0, 0, BINARY_TAOS, stbSqlFile, sizeof(stbSqlFile));
        TdFilePtr truncFp = taosOpenFile(stbSqlFile, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
        if (truncFp) taosCloseFile(&truncFp);
    }
    // Count super tables for META phase progress
    {
        int stbCount = 0;
        for (int k = 0; stbNames != NULL && stbNames[k] != NULL; k++) stbCount++;
        g_progress.stbTotal       = stbCount;
        g_progress.stbIndex       = 0;
        g_progress.stbName[0]     = '\0';
        g_progress.ctbTotalCur    = 0;
        g_progress.ctbTotalAll    = 0;
        atomic_store_64(&g_progress.ctbDoneCur, 0);
        atomic_store_64(&g_progress.ctbDoneAll, 0);
        g_progress.phase          = PROGRESS_PHASE_META;
    }
    int stbEffectiveIdx = 0;
    for (int i = 0; stbNames != NULL && stbNames[i] != NULL; i++) {
        if (g_interrupted) {
            code = TSDB_CODE_BCK_USER_CANCEL;
            break;
        }
        // If specific tables are requested, only include stbs that either
        // (a) are directly named in specTables, or
        // (b) have child tables that match specTables.
        if (argSpecTables()) {
            bool include = false;
            char **specTbs = argSpecTables();
            // (a) direct match: stb name in specTables
            for (int j = 0; !include && specTbs[j] != NULL; j++) {
                if (strcmp(stbNames[i], specTbs[j]) == 0) include = true;
            }
            // (b) child table match: any spec table is a child of this stb
            if (!include) {
                char inClause[3800] = "";
                argBuildInClause("table_name", inClause, sizeof(inClause));
                char countSql[TSDB_MAX_SQL_LEN];
                snprintf(countSql, sizeof(countSql),
                         "SELECT count(*) FROM information_schema.ins_tables "
                         "WHERE db_name='%s' AND stable_name='%s' AND %s;",
                         dbName, stbNames[i], inClause);
                int32_t matchCount = 0;
                queryValueInt(countSql, 0, &matchCount);
                include = (matchCount > 0);
            }
            if (!include) continue;  /* skip this super table */
        }
        // Update META phase progress for this STB
        stbEffectiveIdx++;
        atomic_add_fetch_64(&g_stats.stbTotal, 1);
        g_progress.stbIndex = stbEffectiveIdx;
        snprintf(g_progress.stbName, sizeof(g_progress.stbName), "%s", stbNames[i]);
        g_progress.ctbTotalCur = 0;
        atomic_store_64(&g_progress.ctbDoneCur, 0);
        logInfo("[%lld/%lld] db: %s  [%lld/%lld] stb: %s  meta start",
                (long long)g_progress.dbIndex, (long long)g_progress.dbTotal, dbName,
                (long long)g_progress.stbIndex, (long long)g_progress.stbTotal, stbNames[i]);
        StbInfo stbInfo;
        memset(&stbInfo, 0, sizeof(StbInfo));
        stbInfo.dbInfo = dbInfo;
        stbInfo.stbName = stbNames[i];

        // sql
        code = backCreateStbSql(dbName, stbNames[i]);
        if (code != TSDB_CODE_SUCCESS) {
            freeArrayPtr(stbNames);
            return code;
        }
        // schema
        char * selectTags = NULL; 
        code = backStbSchema(dbName, stbNames[i], &selectTags);
        if (code != TSDB_CODE_SUCCESS) {
            freeArrayPtr(stbNames);
            freePtr(selectTags);
            return code;
        }
        // tags: virtual super tables use columnar vtags binary (ins_tags); physical use legacy SelectDistinct
        stbInfo.selectTags = selectTags;
        if (!isVirtualSuperTable(dbName, stbNames[i])) {
            code = backChildTableTags(dbInfo, &stbInfo);
            if (code != TSDB_CODE_SUCCESS) {
                freeArrayPtr(stbNames);
                freePtr(selectTags);
                return code;
            }
        } else {
            code = backVstbChildTags(dbInfo, &stbInfo);
            if (code != TSDB_CODE_SUCCESS) {
                freeArrayPtr(stbNames);
                freePtr(selectTags);
                return code;
            }
        }
        // Accumulate child table progress across all STBs
        atomic_add_fetch_64(&g_progress.ctbDoneAll, g_progress.ctbDoneCur);
        // free
        freePtr(selectTags);
    }    

    freeArrayPtr(stbNames);

    if (code != TSDB_CODE_SUCCESS) {
        return code;
    }

    //
    // normal tables sql
    //
    code = backNormalTablesSql(dbName);
    if (code != TSDB_CODE_SUCCESS) {
        return code;
    }

    //
    // virtual tables DDL sql (vtb.sql): virtual normal tables + virtual child tables
    //
    code = backVirtualTablesSql(dbName);
    if (code != TSDB_CODE_SUCCESS) {
        return code;
    }

    return code;
}