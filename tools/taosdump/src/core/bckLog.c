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
    
#include "bckArgs.h"
#include "bckLog.h"
#include "bckProgress.h"
#include "osFile.h"
#include "osThread.h"

// Windows: provide flockfile/funlockfile via MSVC CRT _lock_file/_unlock_file
#ifdef WINDOWS
#ifndef flockfile
#define flockfile(f)   _lock_file(f)
#define funlockfile(f) _unlock_file(f)
#endif
#endif

// Thread-safe log: prepend [HH:MM:SS] timestamp, then write atomically.
// flockfile/funlockfile ensures the entire write+flush is atomic across threads.

// On-disk mirror log (backup.log / restore.log). NULL when disabled (not
// requested, or open failed - in which case logging silently continues to
// the console only, per the "log failures must never break the run" rule).
static TdFilePtr     g_logFp = NULL;
static TdThreadMutex g_logFileMutex;

// Fill ts[10] with "HH:MM:SS\0"
static void logTimestamp(char *ts) {
    time_t t;
    taosTime(&t);
    struct tm tm_info;
    taosLocalTime(&t, &tm_info, NULL, 0, NULL);
    taosStrfTime(ts, 10, "%H:%M:%S", &tm_info);
}

// Write raw bytes to the mirror log file, if open. Best-effort: write
// failures are ignored here (nothing sane to do - the console output, which
// already succeeded, remains the source of truth).
static void logFileWrite(const char *buf, int len) {
    if (!g_logFp || len <= 0) return;
    taosThreadMutexLock(&g_logFileMutex);
    if (g_logFp) {
        taosWriteFile(g_logFp, buf, len);
    }
    taosThreadMutexUnlock(&g_logFileMutex);
}

void logFileOpen(const char *path, bool truncate) {
    int flags = TD_FILE_WRITE | TD_FILE_CREATE | (truncate ? TD_FILE_TRUNC : TD_FILE_APPEND);
    TdFilePtr fp = taosOpenFile(path, flags);
    if (!fp) {
        logWarn("open log file failed, continuing without file log: %s", path);
        return;
    }
    taosThreadMutexInit(&g_logFileMutex, NULL);
    g_logFp = fp;
}

void logFileClose(void) {
    if (!g_logFp) return;
    TdFilePtr fp = g_logFp;
    g_logFp = NULL;
    taosCloseFile(&fp);
    taosThreadMutexDestroy(&g_logFileMutex);
}

// Log content (SQL text in particular) can run into the megabytes, so none
// of the loggers below may use a fixed-size stack buffer - measure the
// formatted length first, then allocate exactly enough room. `args` is
// consumed by the length probe; callers still own their own va_start/va_end
// pair. addNewline appends '\n' after the message - callers whose format
// string already ends in '\n' (the printf-style banners routed through
// logTee) must pass false to avoid a doubled blank line.
static char *formatLogLine(const char *prefix, const char *format, va_list args, bool addNewline, int *outLen) {
    va_list argsCopy;
    va_copy(argsCopy, args);
    int msgLen = vsnprintf(NULL, 0, format, args);
    if (msgLen < 0) {
        va_end(argsCopy);
        return NULL;
    }

    int   prefixLen = (int)strlen(prefix);
    int   total     = prefixLen + msgLen + (addNewline ? 1 : 0);
    char *buf       = taosMemoryMalloc(total + 1);
    if (!buf) {
        va_end(argsCopy);
        return NULL;
    }
    memcpy(buf, prefix, prefixLen);
    vsnprintf(buf + prefixLen, msgLen + 1, format, argsCopy);
    va_end(argsCopy);
    if (addNewline) buf[total - 1] = '\n';
    buf[total] = '\0';
    if (outLen) *outLen = total;
    return buf;
}

void logTee(const char *format, ...) {
    va_list args;
    va_start(args, format);
    int   total;
    char *buf = formatLogLine("", format, args, false, &total);
    va_end(args);
    if (!buf) return;
    fwrite(buf, 1, total, stdout);
    logFileWrite(buf, total);
    taosMemoryFree(buf);
}

void logError(const char *format, ...) {
    char ts[10];
    logTimestamp(ts);
    char prefix[32];
    snprintf(prefix, sizeof(prefix), "[%s] ERROR: ", ts);

    va_list args;
    va_start(args, format);
    int   total;
    char *buf = formatLogLine(prefix, format, args, true, &total);
    va_end(args);
    if (!buf) return;

    flockfile(stderr);
    fwrite(buf, 1, total, stderr);
    fflush(stderr);
    funlockfile(stderr);
    logFileWrite(buf, total);
    taosMemoryFree(buf);
}

void logInfo(const char *format, ...) {
    char ts[10];
    logTimestamp(ts);
    char prefix[32];
    snprintf(prefix, sizeof(prefix), "[%s] ", ts);

    va_list args;
    va_start(args, format);
    int   total;
    char *buf = formatLogLine(prefix, format, args, true, &total);
    va_end(args);
    if (!buf) return;

    flockfile(stdout);
    // clear the progress rolling line so this message starts on a clean line
#ifdef WINDOWS
    if (g_tty_progress) fwrite("\r", 1, 1, stdout);
#else
    if (g_tty_progress) fwrite("\r\033[K", 1, 4, stdout);
#endif
    fwrite(buf, 1, total, stdout);
    fflush(stdout);
    funlockfile(stdout);
    logFileWrite(buf, total);
    taosMemoryFree(buf);
}

void logWarn(const char *format, ...) {
    char ts[10];
    logTimestamp(ts);
    char prefix[32];
    snprintf(prefix, sizeof(prefix), "[%s] WARN: ", ts);

    va_list args;
    va_start(args, format);
    int   total;
    char *buf = formatLogLine(prefix, format, args, true, &total);
    va_end(args);
    if (!buf) return;

    flockfile(stderr);
    fwrite(buf, 1, total, stderr);
    fflush(stderr);
    funlockfile(stderr);
    logFileWrite(buf, total);
    taosMemoryFree(buf);
}

void logDebug(const char *format, ...) {
    if (!argDebug()) return;
    char ts[10];
    logTimestamp(ts);
    char prefix[32];
    snprintf(prefix, sizeof(prefix), "[%s] DEBUG: ", ts);

    va_list args;
    va_start(args, format);
    int   total;
    char *buf = formatLogLine(prefix, format, args, true, &total);
    va_end(args);
    if (!buf) return;

    flockfile(stdout);
    // clear the progress rolling line so this message starts on a clean line
#ifdef WINDOWS
    if (g_tty_progress) fwrite("\r", 1, 1, stdout);
#else
    if (g_tty_progress) fwrite("\r\033[K", 1, 4, stdout);
#endif
    fwrite(buf, 1, total, stdout);
    fflush(stdout);
    funlockfile(stdout);
    logFileWrite(buf, total);
    taosMemoryFree(buf);
}  // logDebug