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

#include <stdint.h>

// Maximum STB name length held in g_progress
#define PROGRESS_STB_NAME_LEN 256

// Progress state shared between backup threads and the progress display thread.
// All fields are read/written atomically (volatile int64_t + atomic helpers).
// stbName is written only by the main thread (holding no locks), and only read
// by the display thread — a memcpy of PROGRESS_STB_NAME_LEN bytes is safe
// because the display reads approximate snapshots.
typedef struct {
    volatile int64_t dbIndex;       // current database index (1-based)
    volatile int64_t dbTotal;       // total databases to back up
    volatile int64_t stbIndex;      // current STB index within DB (1-based)
    volatile int64_t stbTotal;      // total STBs in current DB
    char             stbName[PROGRESS_STB_NAME_LEN]; // current STB name
    volatile int64_t ctbDoneCur;    // CTBs completed in current STB (atomic)
    volatile int64_t ctbTotalCur;   // total CTBs in current STB
    volatile int64_t ctbDoneAll;    // cumulative CTBs done (finished STBs + doneCur)
    volatile int64_t ctbTotalAll;   // total CTBs across all STBs (pre-scanned)
    volatile int64_t startMs;       // backup start time (ms since epoch)
} BckProgress;

extern BckProgress g_progress;

// Set to 1 by the progress thread when stdout is a tty.
// logInfo() checks this to clear the rolling line before printing.
extern volatile int g_tty_progress;

// Start the background progress display thread.
// Call once before backupMain().
void progressStart(void);

// Signal the progress thread to stop and wait for it to finish.
// Call once after backupMain() returns.
void progressStop(void);

#endif  // INC_BCK_PROGRESS_H_
