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

#ifndef INC_BCK_PROGRESS_H_
#define INC_BCK_PROGRESS_H_

#include "tdef.h"

// Maximum name lengths held in g_progress
#define PROGRESS_DB_NAME_LEN  64
#define PROGRESS_STB_NAME_LEN 256

// phase values for g_progress.phase
#define PROGRESS_PHASE_IDLE 0   // not started
#define PROGRESS_PHASE_META 1   // backing up / restoring metadata (schemas + tags)
#define PROGRESS_PHASE_DATA 2   // backing up / restoring data files

// Progress state shared between backup threads and the progress display thread.
// All fields are read/written atomically (volatile int64_t + atomic helpers).
// stbName is written only by the main thread (holding no locks), and only read
// by the display thread — a memcpy of PROGRESS_STB_NAME_LEN bytes is safe
// because the display reads approximate snapshots.
typedef struct {
    volatile int64_t dbIndex;       // current database index (1-based)
    volatile int64_t dbTotal;       // total databases to process
    char             dbName[PROGRESS_DB_NAME_LEN];   // current database name
    volatile int64_t stbIndex;      // current STB index within DB (1-based)
    volatile int64_t stbTotal;      // total STBs in current DB
    char             stbName[PROGRESS_STB_NAME_LEN]; // current STB name
    volatile int64_t ctbDoneCur;    // units completed in current STB (atomic)
    volatile int64_t ctbTotalCur;   // total units in current STB
    volatile int64_t ctbDoneAll;    // cumulative units done (finished STBs)
    volatile int64_t ctbTotalAll;   // total units across all STBs
    volatile int64_t startMs;       // operation start time (ms since epoch)
    volatile int64_t isRestore;     // 0 = backup (unit=CTB), 1 = restore (unit=file)
    volatile int64_t phase;         // PROGRESS_PHASE_* : IDLE / META / DATA
} BckProgress;

extern BckProgress g_progress;

// Set to 1 by the progress thread when stdout is a tty.
// logInfo() checks this to clear the rolling line before printing.
extern volatile int g_tty_progress;

// Start the background progress display thread.
// Call once before backupMain() / restoreMain().
void progressStart(void);

// Signal the progress thread to stop and wait for it to finish.
// Call once after backupMain() / restoreMain() returns.
void progressStop(void);

#endif  // INC_BCK_PROGRESS_H_
