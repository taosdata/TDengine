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

#ifdef WINDOWS
// Windows uses a console control handler instead of POSIX signals.
// The handler is called from a separate thread by the OS.
static BOOL signalHandler(DWORD fdwCtrlType) {
    if (fdwCtrlType == CTRL_C_EVENT || fdwCtrlType == CTRL_BREAK_EVENT ||
        fdwCtrlType == CTRL_CLOSE_EVENT) {
        g_interrupted = 1;
        return TRUE;
    }
    return FALSE;
}
#else
static void signalHandler(int32_t signum, void *sigInfo, void *context) {
    g_interrupted = 1;
    const char *msg = "\nReceived interrupt signal, stopping gracefully...\n";
    // write() is async-signal-safe, printf is not
    (void)write(STDOUT_FILENO, msg, strlen(msg));
}
#endif

// Format epoch ms as "YYYY-MM-DD HH:MM:SS" into buf (must be >= 20 bytes).
static void formatDateTime(int64_t ms, char *buf, int len) {
    time_t t = (time_t)(ms / 1000);
    struct tm tm_info;
    taosLocalTime(&t, &tm_info, NULL, 0, NULL);
    taosStrfTime(buf, len, "%Y-%m-%d %H:%M:%S", &tm_info);
}

// Query the connected server's engine version via SQL, since taosdump has no
// direct API for the *remote* engine version (taos_get_client_info() reports
// the linked client library, not the server). Best-effort: returns false and
// leaves outVersion untouched on any failure - the caller must not treat this
// as fatal.
static bool fetchServerVersion(char *outVersion, int len) {
    int code = TSDB_CODE_FAILED;
    TAOS *conn = getConnection(&code);
    if (!conn) return false;

    bool ok = false;
    TAOS_RES *res = taos_query(conn, "SELECT SERVER_VERSION();");
    if (res && taos_errno(res) == TSDB_CODE_SUCCESS) {
        TAOS_ROW row = taos_fetch_row(res);
        if (row && row[0]) {
            int32_t *lens = taos_fetch_lengths(res);
            int copyLen = lens[0] < len - 1 ? lens[0] : len - 1;
            memcpy(outVersion, row[0], copyLen);
            outVersion[copyLen] = '\0';
            ok = true;
        }
    }
    if (res) taos_free_result(res);
    releaseConnection(conn);
    return ok;
}

//
// print startup summary
//
static void printStartSummary(enum ActionType action, const char *serverVersion, int64_t startMs) {
    bool wsMode = (argDriver() == CONN_MODE_WEBSOCKET) ||
                  (argDriver() == CONN_MODE_INVALID && argIsDsn());
    logTee("\n");
    logTee("===========================================================================\n");
    logTee("  taosdump - %s\n", action == ACTION_BACKUP ? "BACKUP" : "RESTORE");
    logTee("===========================================================================\n");
    logTee("  Connect Mode : %s\n", wsMode ? "WebSocket" : "Native");
    logTee("  Config Dir   : %s\n", argConfigDir());
    if (argIsDsn()) {
        logTee("  DSN          : %s\n", argDsn());
    }
    logTee("  Server       : %s:%d\n", argHost() ? argHost() : "(firstEp from cfg)", argPort());
    logTee("  Server Ver.  : %s\n", (serverVersion && serverVersion[0]) ? serverVersion : "(unknown)");
    logTee("  User         : %s\n", argUser());
    logTee("  Output Path  : %s\n", argOutPath());
    {
        char dtBuf[32];
        formatDateTime(startMs, dtBuf, sizeof(dtBuf));
        logTee("  Start Time   : %s\n", dtBuf);
    }
    {
        char **dbs = argBackDB();
        if (dbs && dbs[0]) {
            logTee("  Databases    :");
            for (int i = 0; dbs[i]; i++) {
                logTee(" %s", dbs[i]);
            }
            logTee("\n");
        } else {
            logTee("  Databases    : ALL %s\n", action == ACTION_BACKUP ? "(system databases excluded)" : "");
        }
    }
    logTee("  Data Threads : %d\n", argDataThread());
    logTee("  Tag Threads  : %d\n", argTagThread());
    if (action == ACTION_BACKUP) {
        logTee("  Format       : %s\n", argStorageFormat() == BINARY_TAOS ? "binary" : "parquet");
        logTee("  Schema Only  : %s\n", argSchemaOnly() ? "yes" : "no");
        {
            const char *ts = argStartTime();
            const char *te = argEndTime();
            if (ts && te) {
                logTee("  Time Range   : %s ~ %s\n", ts, te);
            } else if (ts) {
                logTee("  Time Range   : %s ~\n", ts);
            } else if (te) {
                logTee("  Time Range   : ~ %s\n", te);
            } else {
                logTee("  Time Range   : ALL\n");
            }
        }
        {
            char **specTbs = argSpecTables();
            if (specTbs) {
                logTee("  Tables       :");
                for (int i = 0; specTbs[i]; i++) {
                    logTee(" %s", specTbs[i]);
                }
                logTee("\n");
            }
        }
        logTee("  Check Point  : %s\n", argCheckpoint() ? "yes" : "no");
    }
    if (action == ACTION_RESTORE) {
        logTee("  Check Point  : %s\n", argCheckpoint() ? "yes" : "no");
        const char *rl = argRenameList();
        if (rl) logTee("  Rename DB    : %s\n", rl);
    }
    logTee("===========================================================================\n");
    logTee("\n");
}

//
// print end summary
//
static void printEndSummary(enum ActionType action, int code, double elapsed, int64_t endMs) {
    logTee("\n");
    logTee("===========================================================================\n");
    if (code == TSDB_CODE_SUCCESS) {
        logTee("  Result       : SUCCESS\n");
    } else if (code == TSDB_CODE_BCK_USER_CANCEL) {
        logTee("  Result       : CANCELLED BY USER (code: 0x%08X)\n", code);
    } else {
        logTee("  Result       : FAILED (code: 0x%08X)\n", code);
    }
    logTee("---------------------------------------------------------------------------\n");
    logTee("  Databases    : total=%" PRId64 ", success=%" PRId64 ", failed=%" PRId64 "\n",
           g_stats.dbTotal, g_stats.dbSuccess, g_stats.dbFailed);
    logTee("  Super Tables : %" PRId64 "\n", g_stats.stbTotal);
    if (action == ACTION_RESTORE) {
        int64_t restored = g_stats.dataFilesTotal - g_stats.dataFilesFailed - g_stats.dataFilesSkipped;
        logTee("  Child Tables : %" PRId64 " (data restored)\n", restored);
    } else {
        logTee("  Child Tables : %" PRId64 " (data exported)\n", g_stats.childTablesTotal);
    }
    logTee("  Total Rows   : %" PRId64 "\n", g_stats.totalRows);
    logTee("  Normal Tables: %" PRId64 "\n", g_stats.ntbTotal);
    if (action == ACTION_BACKUP) {
        logTee("  Data Files   : total=%" PRId64 ", skipped(resume)=%" PRId64 ", failed=%" PRId64 "\n",
               g_stats.dataFilesTotal, g_stats.dataFilesSkipped, g_stats.dataFilesFailed);
    } else {
        logTee("  Data Files   : total=%" PRId64 ", skipped(checkpoint)=%" PRId64 ", failed=%" PRId64 "\n",
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
        logTee("  File Size    : %.2f GB\n", sizeMB / 1024.0);
    } else {
        logTee("  File Size    : %.2f MB\n", sizeMB);
    }
    int elapsedSecs = (int)elapsed;
    if (elapsedSecs < 1) {
        elapsedSecs = 1;
    }
    if (elapsedSecs >= 3600) {
        int hours = elapsedSecs / 3600;
        int mins  = (elapsedSecs % 3600) / 60;
        int secs  = elapsedSecs % 60;
        logTee("  Elapsed Time : %d hours %d mins %d seconds\n", hours, mins, secs);
    } else if (elapsedSecs >= 60) {
        int mins = elapsedSecs / 60;
        int secs = elapsedSecs % 60;
        logTee("  Elapsed Time : %d mins %d seconds\n", mins, secs);
    } else {
        logTee("  Elapsed Time : %d seconds\n", elapsedSecs);
    }
    {
        char dtBuf[32];
        formatDateTime(endMs, dtBuf, sizeof(dtBuf));
        logTee("  End Time     : %s\n", dtBuf);
    }
    logTee("===========================================================================\n");
    logTee("\n");
}

int main(int argc, char *argv[]) {
    // register signal handlers for graceful shutdown
#ifdef WINDOWS
    // On Windows, use SetConsoleCtrlHandler for Ctrl-C / Ctrl-Break / close
    SetConsoleCtrlHandler((PHANDLER_ROUTINE)signalHandler, TRUE);
#else
    taosSetSignal(SIGINT,  signalHandler);
    taosSetSignal(SIGTERM, signalHandler);
#endif

    int code = TSDB_CODE_SUCCESS;

    //
    // init 
    //
    
    // arguments (-V/--version exits here before startup banner)
    if (argsInit(argc, argv) != 0) {
        logError("init args failed");
        return -1;
    }

    // Open the on-disk mirror log as early as possible so it captures
    // everything from the version banner onward.
    //   backup  -> <outPath>/backup.log,  truncated on each run
    //   restore -> <cwd>/restore.log,     appended across runs
    enum ActionType action = argAction();
    if (action == ACTION_BACKUP) {
        taosMulMkDir(argOutPath());
        char logPath[MAX_PATH_LEN];
        snprintf(logPath, sizeof(logPath), "%s/backup.log", argOutPath());
        logFileOpen(logPath, true);
    } else if (action == ACTION_RESTORE) {
        char cwd[MAX_PATH_LEN];
        taosGetCwd(cwd, sizeof(cwd));
        char logPath[MAX_PATH_LEN];
        snprintf(logPath, sizeof(logPath), "%s/restore.log", cwd);
        logFileOpen(logPath, false);
    }

    printVersion(false);
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
    // Apply config directory only when user explicitly passed -c
    const char *cfgPath = argConfigDir();
    if (cfgPath && cfgPath[0] != '\0') {
        taos_options(TSDB_OPTION_CONFIGDIR, cfgPath);
    }
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

    // record start time
    int64_t startMs = taosGetTimestampMs();

    // Fetch the connected server's engine version for the summary banner.
    // Best-effort: an empty string is displayed as "(unknown)" if this fails,
    // it never blocks the backup/restore itself.
    char serverVersion[64] = {0};
    fetchServerVersion(serverVersion, sizeof(serverVersion));

    printStartSummary(action, serverVersion, startMs);

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

    printEndSummary(action, code, elapsed, endMs);

    //
    // destroy
    //
    logFileClose();
    argsDestroy();
    destroyConnectionPool();
    return code;
}