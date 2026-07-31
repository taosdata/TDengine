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

#ifndef INC_BCKLOG_H_
#define INC_BCKLOG_H_

#ifdef __cplusplus
extern "C" {
#endif

//
// ---------------- define ----------------
//

void logError(const char *format, ...);
void logInfo(const char *format, ...);
void logWarn(const char *format, ...);
void logDebug(const char *format, ...);

// Open the on-disk mirror log (backup.log / restore.log).
// truncate=true: overwrite any existing file (backup); false: append (restore).
// On failure this only logs a warning to the console and leaves file-mirroring
// disabled for the rest of the run - it never fails the caller.
void logFileOpen(const char *path, bool truncate);

// Close the on-disk mirror log, if open. Safe to call even if never opened.
void logFileClose(void);

// printf() to stdout exactly as before, and additionally mirror the same
// text to the on-disk log file (if open). Used for the version/summary
// banners that currently use raw printf().
void logTee(const char *format, ...);

#ifdef __cplusplus
}
#endif

#endif  // INC_BCKLOG_H_
