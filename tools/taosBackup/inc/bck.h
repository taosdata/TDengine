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

#ifndef INC_BCK_H_
#define INC_BCK_H_
// TDengine: os.h (pulled in by tdef.h) already includes all common system
// headers (<stdio.h>, <stdlib.h>, <string.h>, <errno.h>, <signal.h>, <time.h>,
// <stdarg.h>, <inttypes.h>, <stdbool.h>, <stdint.h>, <unistd.h>, etc.) and
// osThread.h — no need to repeat them here.
#include <taos.h>
#include <tdef.h>
#include <taoserror.h>

// Windows: items not covered by os.h/osDef.h
// (strncasecmp/strcasecmp are already mapped by osDef.h → _strnicmp/_stricmp)
#ifdef WINDOWS
#ifndef flockfile
#define flockfile(f)   _lock_file(f)
#define funlockfile(f) _unlock_file(f)
#endif
#ifndef STDOUT_FILENO
#define STDOUT_FILENO 1
#define STDERR_FILENO 2
#endif
#endif

// global interrupt flag (set by SIGINT/SIGTERM handler)
extern volatile sig_atomic_t g_interrupted;

// global statistics counters
typedef struct {
    volatile int64_t  dbTotal;          // total databases to process
    volatile int64_t  dbSuccess;        // databases completed successfully
    volatile int64_t  dbFailed;         // databases failed
    volatile int64_t  stbTotal;         // super tables processed
    volatile int64_t  ntbTotal;         // normal tables processed
    volatile int64_t  dataFilesTotal;   // data files processed (backup written / restore written)
    volatile int64_t  dataFilesSkipped; // data files skipped (resume/checkpoint)
    volatile int64_t  dataFilesFailed;  // data files failed
    volatile int64_t  childTablesTotal; // child tables with data exported
    volatile int64_t  totalRows;        // total rows backed up / restored
    volatile int64_t  dataFilesSizeBytes; // cumulative size of successfully processed data files (restore only)
} BckStats;

extern BckStats g_stats;
// bck
#include "bckTypes.h"
#include "bckUtil.h"
#include "bckArgs.h"
#include "bckLog.h"
#include "bckError.h"
#include "bckFile.h"
#include "bckDb.h"
#include "bckPool.h"

// ---------------- define ----------------
#define MAX_PATH_LEN 512
#define FOLDER_MAXFILE 100000

#define NORMAL_TABLE_DIR "_ntb"

// ---------------- struct ----------------

// db
typedef struct {
    const char *dbName;
} DBInfo;

// stb
typedef struct {
    DBInfo     *dbInfo;
    const char *stbName;
    const char *selectTags;
} StbInfo;

#endif  // INC_BCK_H_
