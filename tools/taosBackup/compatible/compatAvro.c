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
#include "bckProgress.h"


#ifndef TSDB_MAX_ALLOWED_SQL_LEN
#define TSDB_MAX_ALLOWED_SQL_LEN (4*1024*1024u)
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <osDir.h>

// ==================== Format detection ====================

bool isAvroBackupDir(const char *dbPath) {
    if (!dbPath) return false;
    char dbsSqlPath[MAX_PATH_LEN];
    snprintf(dbsSqlPath, sizeof(dbsSqlPath), "%s/dbs.sql", dbPath);
    return taosCheckExistFile(dbsSqlPath);
}

// ==================== SQL rename ====================

// Replace old database names with renamed target in SQL statements.
// Handles: CREATE DATABASE/STABLE/TABLE/VTABLE IF NOT EXISTS `dbname`...
// Returns malloc'd string on replacement, NULL if no change needed.
char *avroAfterRenameSql(AvroRestoreCtx *ctx, const char *sql) {
    if (!ctx || !sql) return NULL;

    // Use argRenameDb to check if db name in SQL needs renaming.
    // For simplicity, we do a direct name replacement approach.
    // This matches taosdump's afterRenameSql logic.

    const char *prefixes[] = {
        "CREATE DATABASE IF NOT EXISTS ",
        "CREATE STABLE IF NOT EXISTS ",
        "CREATE TABLE IF NOT EXISTS ",
        "CREATE VTABLE IF NOT EXISTS ",
        "USE ",
        NULL
    };

    for (int i = 0; prefixes[i]; i++) {
        size_t prefixLen = strlen(prefixes[i]);
        if (strncasecmp(sql, prefixes[i], prefixLen) == 0) {
            // Extract database name after prefix
            const char *nameStart = sql + prefixLen;
            // Skip backtick if present
            if (*nameStart == '`') nameStart++;

            char oldDbName[AVRO_DB_NAME_LEN] = {0};
            int j = 0;
            while (nameStart[j] && nameStart[j] != '`' && nameStart[j] != '.'
                   && nameStart[j] != ' ' && nameStart[j] != ';'
                   && j < AVRO_DB_NAME_LEN - 1) {
                oldDbName[j] = nameStart[j];
                j++;
            }
            oldDbName[j] = '\0';

            if (strlen(oldDbName) == 0) return NULL;

            const char *newDbName = argRenameDb(oldDbName);
            if (strcmp(oldDbName, newDbName) == 0) return NULL;

            // Build new SQL with replaced name
            size_t newSqlLen = strlen(sql) + strlen(newDbName) + 64;
            char *newSql = (char *)taosMemoryCalloc(1, newSqlLen);
            if (!newSql) return NULL;

            // Copy prefix
            memcpy(newSql, sql, prefixLen);
            int pos = (int)prefixLen;

            // Handle backtick
            bool hadBacktick = (sql[prefixLen] == '`');
            if (hadBacktick) {
                pos += sprintf(newSql + pos, "`%s`", newDbName);
                // Skip old name + backticks
                const char *rest = sql + prefixLen + 1 + j;
                if (*rest == '`') rest++;
                strcat(newSql + pos, rest);
            } else {
                pos += sprintf(newSql + pos, "%s", newDbName);
                const char *rest = sql + prefixLen + j;
                strcat(newSql + pos, rest);
            }

            return newSql;
        }
    }

    return NULL;
}

// ==================== dbs.sql execution ====================

// Parse and execute dbs.sql file
static int avroRestoreDbSql(AvroRestoreCtx *ctx) {
    char dbsSqlPath[MAX_PATH_LEN];
    snprintf(dbsSqlPath, sizeof(dbsSqlPath), "%s/dbs.sql", ctx->dbPath);

    TdFilePtr pFile = taosOpenFile(dbsSqlPath, TD_FILE_READ);
    if (!pFile) {
        logError("avro: cannot open dbs.sql: %s", dbsSqlPath);
        return -1;
    }

    // Get file size and read entire file
    int64_t fileSize = 0;
    if (taosFStatFile(pFile, &fileSize, NULL) != 0 || fileSize <= 0) {
        logError("avro: cannot stat dbs.sql: %s", dbsSqlPath);
        taosCloseFile(&pFile);
        return -1;
    }

    char *fileBuf = (char *)taosMemoryCalloc(1, fileSize + 1);
    if (!fileBuf) { taosCloseFile(&pFile); return -1; }

    int64_t readBytes = taosReadFile(pFile, fileBuf, fileSize);
    taosCloseFile(&pFile);
    if (readBytes <= 0) { taosMemoryFree(fileBuf); return -1; }
    fileBuf[readBytes] = '\0';

    int64_t execCount = 0;
    char *saveptr = NULL;
    char *line = strtok_r(fileBuf, "\n", &saveptr);

    while (line) {
        // Trim trailing \r
        size_t len = strlen(line);
        while (len > 0 && line[len-1] == '\r')
            line[--len] = '\0';

        if (len == 0) { line = strtok_r(NULL, "\n", &saveptr); continue; }

        // Parse metadata lines starting with #!
        if (line[0] == '#' && line[1] == '!') {
            if (strncmp(line, "#!loose_mode: ", 14) == 0) {
                ctx->looseMode = (strcasecmp(line + 14, "true") == 0);
                logInfo("avro: loose_mode = %s", ctx->looseMode ? "true" : "false");
            } else if (strncmp(line, "#!escape_char: ", 15) == 0) {
                ctx->escapeChar = (strcasecmp(line + 15, "true") == 0);
            } else if (strncmp(line, "#!charset: ", 11) == 0) {
                snprintf(ctx->charset, sizeof(ctx->charset), "%s", line + 11);
            } else if (strncmp(line, "#!server_ver: ", 14) == 0) {
                ctx->serverMajorVer = atoi(line + 14);
            }
            line = strtok_r(NULL, "\n", &saveptr);
            continue;
        }

        // Skip comments
        if (line[0] == '-' && line[1] == '-') { line = strtok_r(NULL, "\n", &saveptr); continue; }
        if (line[0] == '#') { line = strtok_r(NULL, "\n", &saveptr); continue; }

        // Apply rename
        char *renamed = avroAfterRenameSql(ctx, line);
        const char *execSql = renamed ? renamed : line;

        TAOS_RES *res = taos_query(ctx->conn, execSql);
        int code = taos_errno(res);
        if (code != 0) {
            // Some errors are acceptable (e.g. database already exists)
            logWarn("avro dbs.sql: query warning: %s, reason: %s", execSql, taos_errstr(res));
        } else {
            execCount++;
            // Track super table creation for stats
            if (strncasecmp(execSql, "CREATE STABLE ", 14) == 0) {
                atomic_add_fetch_64(&g_stats.stbTotal, 1);
            }
        }
        taos_free_result(res);
        if (renamed) taosMemoryFree(renamed);

        line = strtok_r(NULL, "\n", &saveptr);
    }

    taosMemoryFree(fileBuf);
    logInfo("avro: executed %"PRId64" SQL statements from dbs.sql", execCount);
    return 0;
}

// ==================== Scan and restore meta ====================

static int avroScanAndRestoreMeta(AvroRestoreCtx *ctx, const char *dataDir,
                                  const char *ext, bool isVirtual) {
    int64_t fileCount = 0;
    char **files = avroScanFiles(dataDir, ext, isVirtual, &fileCount);
    if (!files || fileCount == 0) {
        avroFreeFileList(files);
        return 0;
    }

    logInfo("avro: found %"PRId64" %s files in %s (virtual=%d)",
            fileCount, ext, dataDir, isVirtual);

    int errCount = 0;
    for (int64_t i = 0; i < fileCount; i++) {
        if (g_interrupted) break;

        int64_t res;
        if (strcmp(ext, "avro-tbtags") == 0) {
            res = avroRestoreTbTags(ctx, dataDir, files[i], ctx->pDbChange, isVirtual);
            if (res > 0) atomic_add_fetch_64(&g_stats.childTablesTotal, res);
        } else {
            res = avroRestoreNtb(ctx, dataDir, files[i], ctx->pDbChange, isVirtual);
            if (res > 0) atomic_add_fetch_64(&g_stats.ntbTotal, res);
        }
        if (res < 0) {
            logWarn("avro: failed to restore %s/%s", dataDir, files[i]);
            errCount++;
        }
    }

    avroFreeFileList(files);
    return errCount > 0 ? -1 : 0;
}

// ==================== Data import thread ====================

typedef struct {
    AvroRestoreCtx *ctx;
    const char     *dataDir;
    char          **files;
    int             fileCnt;
    AvroDBChange   *pDbChange;
    int             threadIdx;
    int64_t         totalRows;
    int             code;
} AvroDataThread;

static void *avroDataThreadFunc(void *arg) {
    AvroDataThread *thread = (AvroDataThread *)arg;
    thread->code = 0;
    thread->totalRows = 0;

    int connCode = 0;
    TAOS *conn = getConnection(&connCode);
    if (!conn) {
        logError("avro data thread %d: getConnection failed", thread->threadIdx);
        thread->code = -1;
        return NULL;
    }

    // Select database
    if (taos_select_db(conn, thread->ctx->targetDb) != 0) {
        logError("avro data thread %d: select_db failed: %s",
                 thread->threadIdx, taos_errstr(NULL));
        releaseConnection(conn);
        thread->code = -1;
        return NULL;
    }

    // Detect stbChange from folder's stbname file
    char *folderStb = avroReadFolderStbName(thread->dataDir);
    AvroStbChange *stbChange = NULL;
    if (folderStb) {
        stbChange = avroFindStbChange(thread->pDbChange, folderStb);
        taosMemoryFree(folderStb);
    }

    for (int i = 0; i < thread->fileCnt; i++) {
        if (g_interrupted) break;

        atomic_add_fetch_64(&g_stats.dataFilesTotal, 1);

        int64_t rows = avroRestoreDataImpl(thread->ctx, conn,
                                            thread->dataDir, thread->files[i],
                                            thread->pDbChange, stbChange);
        if (rows < 0) {
            logWarn("avro data thread %d: failed on %s (rows=%"PRId64")",
                    thread->threadIdx, thread->files[i], rows);
            atomic_add_fetch_64(&g_stats.dataFilesFailed, 1);
            thread->code = -1;
        } else {
            thread->totalRows += rows;
            atomic_add_fetch_64(&g_stats.totalRows, rows);
            // Accumulate file size for stats
            char filePath[MAX_PATH_LEN];
            snprintf(filePath, sizeof(filePath), "%s/%s",
                     thread->dataDir, thread->files[i]);
            int64_t fsz = 0;
            taosStatFile(filePath, &fsz, NULL, NULL);
            if (fsz > 0) atomic_add_fetch_64(&g_stats.dataFilesSizeBytes, fsz);
        }
        // Update progress display (one file = one unit)
        atomic_add_fetch_64(&g_progress.ctbDoneCur, 1);
    }

    releaseConnection(conn);
    return NULL;
}

static int avroScanAndRestoreData(AvroRestoreCtx *ctx, const char *dataDir,
                                  bool isVirtual) {
    int64_t fileCount = 0;
    char **files = avroScanFiles(dataDir, "avro", isVirtual, &fileCount);
    if (!files || fileCount == 0) {
        avroFreeFileList(files);
        return 0;
    }

    logInfo("avro: found %"PRId64" data files in %s", fileCount, dataDir);

    int nThreads = ctx->dataThreads;
    if (nThreads <= 0) nThreads = 1;
    if (nThreads > (int)fileCount) nThreads = (int)fileCount;

    AvroDataThread *threads = (AvroDataThread *)taosMemoryCalloc(nThreads, sizeof(AvroDataThread));
    if (!threads) {
        avroFreeFileList(files);
        return -1;
    }

    // Distribute files across threads
    int filesPerThread = (int)(fileCount / nThreads);
    int remainder = (int)(fileCount % nThreads);
    int offset = 0;

    pthread_t *tids = (pthread_t *)taosMemoryCalloc(nThreads, sizeof(pthread_t));

    for (int t = 0; t < nThreads; t++) {
        threads[t].ctx       = ctx;
        threads[t].dataDir   = dataDir;
        threads[t].pDbChange = ctx->pDbChange;
        threads[t].threadIdx = t;
        threads[t].files     = files + offset;
        threads[t].fileCnt   = filesPerThread + (t < remainder ? 1 : 0);
        offset += threads[t].fileCnt;

        pthread_create(&tids[t], NULL, avroDataThreadFunc, &threads[t]);
    }

    int retCode = 0;
    int64_t totalRows = 0;
    for (int t = 0; t < nThreads; t++) {
        pthread_join(tids[t], NULL);
        totalRows += threads[t].totalRows;
        if (threads[t].code != 0) retCode = -1;
    }

    logInfo("avro: data restore completed: %"PRId64" rows (%d threads)", totalRows, nThreads);

    taosMemoryFree(tids);
    taosMemoryFree(threads);
    avroFreeFileList(files);
    return retCode;
}

// ==================== Scan data directories ====================

// Scan for data*/ subdirectories under dbPath
static char **scanDataDirs(const char *dbPath, int *outCount) {
    *outCount = 0;

    TdDirPtr dir = taosOpenDir(dbPath);
    if (!dir) return NULL;

    int capacity = 32;
    char **dirs = (char **)taosMemoryCalloc(capacity + 1, sizeof(char *));

    TdDirEntryPtr entry;
    while ((entry = taosReadDir(dir)) != NULL) {
        char *name = taosGetDirEntryName(entry);
        if (strncmp(name, "data", 4) != 0) continue;

        // Verify it's a directory
        char fullPath[MAX_PATH_LEN];
        snprintf(fullPath, sizeof(fullPath), "%s/%s", dbPath, name);

        if (*outCount >= capacity) {
            capacity *= 2;
            dirs = (char **)taosMemoryRealloc(dirs, (capacity + 1) * sizeof(char *));
        }
        dirs[*outCount] = taosStrdup(fullPath);
        (*outCount)++;
    }
    dirs[*outCount] = NULL;

    taosCloseDir(&dir);
    return dirs;
}

// ==================== Main entry ====================

// Parse #!dumpdb from dbs.sql to extract real database name.
// Returns taosStrdup'd name or NULL if not found.
static char *avroExtractDbNameFromSql(const char *dbPath) {
    char sqlPath[MAX_PATH_LEN];
    snprintf(sqlPath, sizeof(sqlPath), "%s/dbs.sql", dbPath);

    TdFilePtr pFile = taosOpenFile(sqlPath, TD_FILE_READ);
    if (!pFile) return NULL;

    int64_t fileSize = 0;
    if (taosFStatFile(pFile, &fileSize, NULL) != 0 || fileSize <= 0) {
        taosCloseFile(&pFile);
        return NULL;
    }

    char *buf = (char *)taosMemoryCalloc(1, fileSize + 1);
    if (!buf) { taosCloseFile(&pFile); return NULL; }

    int64_t rd = taosReadFile(pFile, buf, fileSize);
    taosCloseFile(&pFile);
    if (rd <= 0) { taosMemoryFree(buf); return NULL; }
    buf[rd] = '\0';

    // Look for "#!dumpdb: <dbname>:" or "CREATE DATABASE IF NOT EXISTS <dbname>"
    char *result = NULL;
    char *saveptr = NULL;
    char *line = strtok_r(buf, "\n", &saveptr);
    while (line) {
        // Try #!dumpdb: <dbname>:
        if (strncmp(line, "#!dumpdb: ", 10) == 0) {
            char *nameStart = line + 10;
            // Trim trailing ':' and spaces
            char tmp[AVRO_DB_NAME_LEN] = {0};
            int j = 0;
            while (nameStart[j] && nameStart[j] != ':' && nameStart[j] != ' '
                   && nameStart[j] != '\r' && j < AVRO_DB_NAME_LEN - 1) {
                tmp[j] = nameStart[j];
                j++;
            }
            tmp[j] = '\0';
            if (j > 0) { result = taosStrdup(tmp); break; }
        }
        // Try CREATE DATABASE IF NOT EXISTS <dbname>
        const char *prefix = "CREATE DATABASE IF NOT EXISTS ";
        size_t prefixLen = strlen(prefix);
        if (strncasecmp(line, prefix, prefixLen) == 0) {
            const char *ns = line + prefixLen;
            if (*ns == '`') ns++;
            char tmp[AVRO_DB_NAME_LEN] = {0};
            int j = 0;
            while (ns[j] && ns[j] != '`' && ns[j] != ' ' && ns[j] != ';'
                   && j < AVRO_DB_NAME_LEN - 1) {
                tmp[j] = ns[j];
                j++;
            }
            tmp[j] = '\0';
            if (j > 0) { result = taosStrdup(tmp); break; }
        }
        line = strtok_r(NULL, "\n", &saveptr);
    }
    taosMemoryFree(buf);
    return result;
}

int restoreAvroDatabase(const char *dbPath) {
    logInfo("avro: restoring from %s", dbPath);

    // Determine database name:
    // 1. Try extracting from dbs.sql (#!dumpdb: or CREATE DATABASE)
    // 2. Fall back to directory name (strip "taosdump." prefix)
    char *extractedName = avroExtractDbNameFromSql(dbPath);
    const char *dbName = NULL;
    if (extractedName) {
        dbName = extractedName;
    } else {
        const char *dirName = strrchr(dbPath, '/');
        dirName = dirName ? dirName + 1 : dbPath;
        if (strncmp(dirName, "taosdump.", 9) == 0) {
            // Try parent dbs.sql for real name
            char parentPath[MAX_PATH_LEN];
            snprintf(parentPath, sizeof(parentPath), "%s", dbPath);
            char *slash = strrchr(parentPath, '/');
            if (slash) {
                *slash = '\0';
                extractedName = avroExtractDbNameFromSql(parentPath);
                if (extractedName) dbName = extractedName;
            }
        }
        if (!dbName) {
            const char *dn = strrchr(dbPath, '/');
            dn = dn ? dn + 1 : dbPath;
            dbName = dn;
        }
    }

    // Apply rename mapping
    const char *targetDb = argRenameDb(dbName);
    logInfo("avro: database: %s -> %s", dbName, targetDb);

    // Update progress display with resolved DB name
    snprintf(g_progress.dbName, sizeof(g_progress.dbName), "%s", targetDb);

    // Initialize context
    AvroRestoreCtx ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.dbPath      = dbPath;
    ctx.targetDb    = targetDb;
    ctx.dataBatch   = argDataBatch();
    ctx.dataThreads = argDataThread();

    // Get a connection for meta operations
    int connCode = 0;
    ctx.conn = getConnection(&connCode);
    if (!ctx.conn) {
        logError("avro: getConnection failed");
        if (extractedName) taosMemoryFree(extractedName);
        return -1;
    }

    // Create schema change tracker
    ctx.pDbChange = avroCreateDbChange(dbPath);
    if (!ctx.pDbChange) {
        releaseConnection(ctx.conn);
        if (extractedName) taosMemoryFree(extractedName);
        return -1;
    }

    int ret = 0;

    // Step 1: Execute dbs.sql (CREATE DATABASE, USE, CREATE STABLE, ...)
    ret = avroRestoreDbSql(&ctx);
    if (ret != 0) {
        logError("avro: dbs.sql execution failed");
        goto cleanup;
    }

    // Step 2: Scan data directories
    int dataDirCount = 0;
    char **dataDirs = scanDataDirs(dbPath, &dataDirCount);

    if (!dataDirs || dataDirCount == 0) {
        logWarn("avro: no data directories found in %s, trying top-level", dbPath);
        // Some single-db exports may have files directly under dbPath
        dataDirs = (char **)taosMemoryCalloc(2, sizeof(char *));
        dataDirs[0] = taosStrdup(dbPath);
        dataDirCount = 1;
    }

    // Step 3: Restore meta at dbPath level (tbtags and ntb files live at DB root)
    g_progress.phase    = PROGRESS_PHASE_META;
    g_progress.isRestore = 1;
    g_progress.startMs  = taosGetTimestampMs();
    g_progress.stbTotal = 1;  // AVRO backup has one STB per data dir
    g_progress.stbIndex = 1;
    g_progress.stbName[0] = '\0';

    if (!g_interrupted && ret == 0) {
        ret = avroScanAndRestoreMeta(&ctx, dbPath, "avro-tbtags", false);
    }
    if (!g_interrupted && ret == 0) {
        ret = avroScanAndRestoreMeta(&ctx, dbPath, "avro-ntb", false);
    }

    // Step 4: Physical data (from data*/ subdirectories)
    // Count total data files for progress display
    int64_t totalDataFiles = 0;
    for (int d = 0; d < dataDirCount; d++) {
        totalDataFiles += avroCountFiles(dataDirs[d], "avro");
    }
    // Read STB name from the first data directory for progress display
    if (dataDirCount > 0) {
        char *folderStb = avroReadFolderStbName(dataDirs[0]);
        if (folderStb) {
            snprintf(g_progress.stbName, PROGRESS_STB_NAME_LEN, "%s", folderStb);
            taosMemoryFree(folderStb);
        }
    }

    g_progress.phase = PROGRESS_PHASE_DATA;
    g_progress.ctbTotalCur  = totalDataFiles;
    g_progress.ctbTotalAll  = totalDataFiles;
    atomic_store_64(&g_progress.ctbDoneCur, 0);
    atomic_store_64(&g_progress.ctbDoneAll, 0);

    for (int d = 0; d < dataDirCount && ret == 0; d++) {
        if (g_interrupted) { ret = TSDB_CODE_BCK_USER_CANCEL; break; }
        ret = avroScanAndRestoreData(&ctx, dataDirs[d], false);
    }

    // Step 5: Virtual tables meta at dbPath level (second pass)
    // Virtual table SQL (e.g. CREATE VTABLE ... USING stb TAGS(...)) may reference
    // tables without a fully-qualified db prefix, so we must set the default database
    // on the connection before executing them.
    if (!g_interrupted && ret == 0) {
        taos_select_db(ctx.conn, ctx.targetDb);
    }
    if (!g_interrupted && ret == 0) {
        avroScanAndRestoreMeta(&ctx, dbPath, "avro-tbtags", true);
    }
    if (!g_interrupted && ret == 0) {
        avroScanAndRestoreMeta(&ctx, dbPath, "avro-ntb", true);
    }

    // Step 6: Virtual data — skip for now; virtual tables don't export data files
    // To support future virtual data files, data directories would need a virtual marker.

    // Free data dirs
    if (dataDirs) {
        for (int d = 0; d < dataDirCount; d++) taosMemoryFree(dataDirs[d]);
        taosMemoryFree(dataDirs);
    }

cleanup:
    if (ctx.pDbChange) avroFreeDbChange(ctx.pDbChange);
    if (ctx.conn) releaseConnection(ctx.conn);

    if (ret == 0) {
        logInfo("avro: restore of %s completed successfully", dbPath);
    } else {
        logError("avro: restore of %s failed (code=%d)", dbPath, ret);
    }

    if (extractedName) taosMemoryFree(extractedName);
    return ret;
}
