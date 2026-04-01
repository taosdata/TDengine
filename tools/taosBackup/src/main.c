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
    
#include "bck.h"
#include "backup.h"
#include "restore.h"
#include "bckArgs.h"
#include "bckProgress.h"

// global interrupt flag
volatile sig_atomic_t g_interrupted = 0;

// global statistics
BckStats g_stats = {0};

static void signalHandler(int32_t signum, void *sigInfo, void *context) {
    g_interrupted = 1;
    const char *msg = "\nReceived interrupt signal, stopping gracefully...\n";
    // write() is async-signal-safe, printf is not
    write(STDOUT_FILENO, msg, sizeof(msg)-1);
}

//
// print startup summary
//
static void printStartSummary(enum ActionType action) {
    bool wsMode = (argDriver() == CONN_MODE_WEBSOCKET) ||
                  (argDriver() == CONN_MODE_INVALID && argIsDsn());
    printf("\n");
    printf("===========================================================================\n");
    printf("  taosBackup - %s\n", action == ACTION_BACKUP ? "BACKUP" : "RESTORE");
    printf("===========================================================================\n");
    printf("  Connect Mode : %s\n", wsMode ? "WebSocket" : "Native");
    printf("  Config Dir   : %s\n", argConfigDir());
    if (argIsDsn()) {
        printf("  DSN          : %s\n", argDsn());
    }
    printf("  Server       : %s:%d\n", argHost(), argPort());
    printf("  User         : %s\n", argUser());
    printf("  Output Path  : %s\n", argOutPath());
    {
        char **dbs = argBackDB();
        if (dbs && dbs[0]) {
            printf("  Databases    :");
            for (int i = 0; dbs[i]; i++) {
                printf(" %s", dbs[i]);
            }
            printf("\n");
        } else {
            printf("  Databases    : ALL %s\n", action == ACTION_BACKUP ? "(system databases excluded)" : "");
        }
    }
    printf("  Data Threads : %d\n", argDataThread());
    printf("  Tag Threads  : %d\n", argTagThread());
    if (action == ACTION_BACKUP) {
        printf("  Format       : %s\n", argStorageFormat() == BINARY_TAOS ? "binary" : "parquet");
        printf("  Schema Only  : %s\n", argSchemaOnly() ? "yes" : "no");
        {
            const char *ts = argStartTime();
            const char *te = argEndTime();
            if (ts && te) {
                printf("  Time Range   : %s ~ %s\n", ts, te);
            } else if (ts) {
                printf("  Time Range   : %s ~\n", ts);
            } else if (te) {
                printf("  Time Range   : ~ %s\n", te);
            } else {
                printf("  Time Range   : ALL\n");
            }
        }
        {
            char **specTbs = argSpecTables();
            if (specTbs) {
                printf("  Tables       :");
                for (int i = 0; specTbs[i]; i++) {
                    printf(" %s", specTbs[i]);
                }
                printf("\n");
            }
        }
        printf("  Check Point  : %s\n", argCheckpoint() ? "yes" : "no");
    }
    if (action == ACTION_RESTORE) {
        printf("  Check Point  : %s\n", argCheckpoint() ? "yes" : "no");
        const char *rl = argRenameList();
        if (rl) printf("  Rename DB    : %s\n", rl);
    }
    printf("===========================================================================\n");
    printf("\n");
}

//
// print end summary
//
static void printEndSummary(enum ActionType action, int code, double elapsed) {
    printf("\n");
    printf("===========================================================================\n");
    if (code == TSDB_CODE_SUCCESS) {
        printf("  Result       : SUCCESS\n");
    } else if (code == TSDB_CODE_BCK_USER_CANCEL) {
        printf("  Result       : CANCELLED BY USER (code: 0x%08X)\n", code);
    } else {
        printf("  Result       : FAILED (code: 0x%08X)\n", code);
    }
    printf("---------------------------------------------------------------------------\n");
    printf("  Databases    : total=%" PRId64 ", success=%" PRId64 ", failed=%" PRId64 "\n",
           g_stats.dbTotal, g_stats.dbSuccess, g_stats.dbFailed);
    printf("  Super Tables : %" PRId64 "\n", g_stats.stbTotal);
    if (action == ACTION_RESTORE) {
        int64_t restored = g_stats.dataFilesTotal - g_stats.dataFilesFailed - g_stats.dataFilesSkipped;
        printf("  Child Tables : %" PRId64 " (data restored)\n", restored);
    } else {
        printf("  Child Tables : %" PRId64 " (data exported)\n", g_stats.childTablesTotal);
    }
    printf("  Total Rows   : %" PRId64 "\n", g_stats.totalRows);
    printf("  Normal Tables: %" PRId64 "\n", g_stats.ntbTotal);
    if (action == ACTION_BACKUP) {
        printf("  Data Files   : total=%" PRId64 ", skipped(resume)=%" PRId64 ", failed=%" PRId64 "\n",
               g_stats.dataFilesTotal, g_stats.dataFilesSkipped, g_stats.dataFilesFailed);
    } else {
        printf("  Data Files   : total=%" PRId64 ", skipped(checkpoint)=%" PRId64 ", failed=%" PRId64 "\n",
               g_stats.dataFilesTotal, g_stats.dataFilesSkipped, g_stats.dataFilesFailed);
    }
    // File Size:
    // - backup : scan the whole output directory (files written)
    // - restore: use accumulated byte count of files actually processed
    int64_t displayBytes = 0;
    if (action == ACTION_RESTORE) {
        displayBytes = g_stats.dataFilesSizeBytes;
    } else {
        const char *outPath = argOutPath();
        if (outPath && taosDirExist(outPath)) {
            taosGetDirSize(outPath, &displayBytes);
        }
    }
    double sizeMB = (double)displayBytes / (1024.0 * 1024.0);
    if (sizeMB >= 1024.0) {
        printf("  File Size    : %.2f GB\n", sizeMB / 1024.0);
    } else {
        printf("  File Size    : %.2f MB\n", sizeMB);
    }
    if (elapsed >= 3600.0) {
        int hours = (int)(elapsed / 3600);
        int mins  = (int)((elapsed - hours * 3600) / 60);
        double secs = elapsed - hours * 3600 - mins * 60;
        printf("  Elapsed Time : %d hours %d mins %.2f seconds\n", hours, mins, secs);
    } else if (elapsed >= 60.0) {
        int mins  = (int)(elapsed / 60);
        double secs = elapsed - mins * 60;
        printf("  Elapsed Time : %d mins %.2f seconds\n", mins, secs);
    } else {
        printf("  Elapsed Time : %.2f seconds\n", elapsed);
    }
    printf("===========================================================================\n");
    printf("\n");
}

int main(int argc, char *argv[]) {
    printVersion(false);
    // register signal handlers for graceful shutdown
    taosSetSignal(SIGINT,  signalHandler);
    taosSetSignal(SIGTERM, signalHandler);

    int code = TSDB_CODE_SUCCESS;

    //
    // init 
    //
    
    // arguments
    if (argsInit(argc, argv) != 0) {
        logError("init args failed");
        return -1;
    }
    // Determine and apply connection driver before any connection is opened.
    // Priority: explicit -Z > auto-from-DSN > default (native).
    {
        int8_t drv = argDriver();
        bool useWs = (drv == CONN_MODE_WEBSOCKET) ||
                     (drv == CONN_MODE_INVALID && argIsDsn());
        const char *drvName = useWs ? "websocket" : "native";
        int rc = taos_options(TSDB_OPTION_DRIVER, drvName);
        if (rc != 0) {
            logError("failed to set driver '%s': %s", drvName, taos_errstr(NULL));
            argsDestroy();
            return -1;
        }
    }
    // Apply config directory before any taos_connect() call
    taos_options(TSDB_OPTION_CONFIGDIR, argConfigDir());
    // conn pool
    // conn pool: data threads each need 2 conns (one pre-assigned, one for queries),
    // tag threads need 1 each, plus a few for main thread operations
    int poolSize = argDataThread() * 2 + argTagThread() + 4;
    if (initConnectionPool(poolSize) != 0) {
        logError("initialize connection pool failed");
        argsDestroy();
        return -1;
    }

    // reset stats
    memset(&g_stats, 0, sizeof(g_stats));

    //
    // action
    //
    enum ActionType action = argAction();

    printStartSummary(action);

    // record start time
    int64_t startMs = taosGetTimestampMs();

    // start progress display thread
    if (action == ACTION_BACKUP || action == ACTION_RESTORE) {
        memset(&g_progress, 0, sizeof(g_progress));
        g_progress.startMs   = startMs;
        g_progress.isRestore = (action == ACTION_RESTORE) ? 1 : 0;
        progressStart();
    }

    switch (action) {
        case ACTION_BACKUP:
            code = backupMain();
            break;
        case ACTION_RESTORE:
            code = restoreMain();
            break;
        default:
            logError("unknown action");
            code = TSDB_CODE_INVALID_PARA;
            break;
    }

    // stop progress display thread
    if (action == ACTION_BACKUP || action == ACTION_RESTORE) {
        progressStop();
    }

    // calc elapsed time
    int64_t endMs = taosGetTimestampMs();
    double elapsed = (double)(endMs - startMs) / 1000.0;

    // if the user interrupted and the code doesn't already reflect that, override it
    if (g_interrupted && code != TSDB_CODE_BCK_USER_CANCEL) {
        code = TSDB_CODE_BCK_USER_CANCEL;
    }

    printEndSummary(action, code, elapsed);

    //
    // destroy 
    //
    argsDestroy();
    destroyConnectionPool();
    return code;
}