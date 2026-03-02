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

// global interrupt flag
volatile sig_atomic_t g_interrupted = 0;

// global statistics
BckStats g_stats = {0};

static void signalHandler(int sig) {
    g_interrupted = 1;
    const char *msg = "\nReceived interrupt signal, stopping gracefully...\n";
    // write() is async-signal-safe, printf is not
    write(STDOUT_FILENO, msg, strlen(msg));
}

//
// print startup summary
//
static void printStartSummary(enum ActionType action, int poolSize) {
    printf("\n");
    printf("===========================================================================\n");
    printf("  taosBackup - %s\n", action == ACTION_BACKUP ? "BACKUP" : "RESTORE");
    printf("===========================================================================\n");
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
            printf("  Databases    : (auto-discover all)\n");
        }
    }
    printf("  Data Threads : %d\n", argDataThread());
    printf("  Tag Threads  : %d\n", argTagThread());
    printf("  Pool Size    : %d\n", poolSize);
    if (action == ACTION_BACKUP) {
        printf("  Schema Only  : %s\n", argSchemaOnly() ? "yes" : "no");
        char *tf = argTimeFilter();
        printf("  Time Filter  : %s\n", (tf && strlen(tf) > 0) ? tf : "(none)");
    }
    if (action == ACTION_RESTORE) {
        const char *rl = argRenameList();
        if (rl) printf("  Rename       : %s\n", rl);
    }
    printf("  Retry Count  : %d\n", argRetryCount());
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
    printf("  Child Tables : %" PRId64 " (data exported)\n", g_stats.childTablesTotal);
    printf("  Total Rows   : %" PRId64 "\n", g_stats.totalRows);
    printf("  Normal Tables: %" PRId64 "\n", g_stats.ntbTotal);
    if (action == ACTION_BACKUP) {
        printf("  Data Files   : total=%" PRId64 ", skipped(resume)=%" PRId64 ", failed=%" PRId64 "\n",
               g_stats.dataFilesTotal, g_stats.dataFilesSkipped, g_stats.dataFilesFailed);
    } else {
        printf("  Data Files   : total=%" PRId64 ", skipped(checkpoint)=%" PRId64 ", failed=%" PRId64 "\n",
               g_stats.dataFilesTotal, g_stats.dataFilesSkipped, g_stats.dataFilesFailed);
    }
    // calculate output directory size
    int64_t dirSize = 0;
    const char *outPath = argOutPath();
    if (outPath && taosDirExist(outPath)) {
        taosGetDirSize(outPath, &dirSize);
    }
    double sizeMB = (double)dirSize / (1024.0 * 1024.0);
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
    // register signal handlers for graceful shutdown
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = signalHandler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    printf("taosBackup tool v1.0\n");
    int code = TSDB_CODE_SUCCESS;

    //
    // init 
    //
    
    // arguments
    if (argsInit(argc, argv) != 0) {
        printf("init args failed\n");
        return -1;
    }
    // conn pool
    // conn pool: data threads each need 2 conns (one pre-assigned, one for queries),
    // tag threads need 1 each, plus a few for main thread operations
    int poolSize = argDataThread() * 2 + argTagThread() + 4;
    if (initConnectionPool(poolSize) != 0) {
        printf("initialize connection pool failed\n");
        argsDestroy();
        return -1;
    }

    // reset stats
    memset(&g_stats, 0, sizeof(g_stats));

    //
    // action
    //
    enum ActionType action = argAction();

    printStartSummary(action, poolSize);

    // record start time
    int64_t startMs = taosGetTimestampMs();

    switch (action) {
        case ACTION_BACKUP:
            logInfo("perform backup action");
            code = backupMain();
            break;
        case ACTION_RESTORE:
            logInfo("perform restore action");
            code = restoreMain();
            break;
        default:
            printf("unknown action\n");
            code = TSDB_CODE_INVALID_PARA;
            break;
    }

    // calc elapsed time
    int64_t endMs = taosGetTimestampMs();
    double elapsed = (double)(endMs - startMs) / 1000.0;

    printEndSummary(action, code, elapsed);

    //
    // destroy 
    //
    argsDestroy();
    destroyConnectionPool();
    return code;
}