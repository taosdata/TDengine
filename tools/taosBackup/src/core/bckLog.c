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
#include <stdarg.h>
#include <stdio.h>
#include <time.h>

// Thread-safe log: prepend [HH:MM:SS] timestamp, then write atomically.
// flockfile/funlockfile ensures the entire write+flush is atomic across threads.

#define LOG_BUF_SIZE 4096

// Fill ts[10] with "HH:MM:SS\0"
static void logTimestamp(char *ts) {
    time_t t = time(NULL);
    struct tm tm_info;
    localtime_r(&t, &tm_info);
    strftime(ts, 10, "%H:%M:%S", &tm_info);
}

void logError(const char *format, ...) {
    char ts[10];
    logTimestamp(ts);
    char buf[LOG_BUF_SIZE];
    int prefix = snprintf(buf, sizeof(buf), "[%s] ERROR: ", ts);
    va_list args;
    va_start(args, format);
    int msgLen = vsnprintf(buf + prefix, sizeof(buf) - prefix - 2, format, args);
    va_end(args);
    if (msgLen < 0) return;
    int total = prefix + msgLen;
    if (total > (int)sizeof(buf) - 2) total = (int)sizeof(buf) - 2;
    buf[total++] = '\n';
    buf[total] = '\0';
    flockfile(stderr);
    fwrite(buf, 1, total, stderr);
    fflush(stderr);
    funlockfile(stderr);
}

void logInfo(const char *format, ...) {
    char ts[10];
    logTimestamp(ts);
    char buf[LOG_BUF_SIZE];
    int prefix = snprintf(buf, sizeof(buf), "[%s] ", ts);
    va_list args;
    va_start(args, format);
    int msgLen = vsnprintf(buf + prefix, sizeof(buf) - prefix - 2, format, args);
    va_end(args);
    if (msgLen < 0) return;
    int total = prefix + msgLen;
    if (total > (int)sizeof(buf) - 2) total = (int)sizeof(buf) - 2;
    buf[total++] = '\n';
    buf[total] = '\0';
    flockfile(stdout);
    // clear the progress rolling line so this message starts on a clean line
    if (g_tty_progress) fwrite("\r\033[K", 1, 4, stdout);
    fwrite(buf, 1, total, stdout);
    fflush(stdout);
    funlockfile(stdout);
}

void logWarn(const char *format, ...) {
    char ts[10];
    logTimestamp(ts);
    char buf[LOG_BUF_SIZE];
    int prefix = snprintf(buf, sizeof(buf), "[%s] WARN: ", ts);
    va_list args;
    va_start(args, format);
    int msgLen = vsnprintf(buf + prefix, sizeof(buf) - prefix - 2, format, args);
    va_end(args);
    if (msgLen < 0) return;
    int total = prefix + msgLen;
    if (total > (int)sizeof(buf) - 2) total = (int)sizeof(buf) - 2;
    buf[total++] = '\n';
    buf[total] = '\0';
    flockfile(stderr);
    fwrite(buf, 1, total, stderr);
    fflush(stderr);
    funlockfile(stderr);
}

void logDebug(const char *format, ...) {
    if (!argDebug()) return;
    char ts[10];
    logTimestamp(ts);
    char buf[LOG_BUF_SIZE];
    int prefix = snprintf(buf, sizeof(buf), "[%s] DEBUG: ", ts);
    va_list args;
    va_start(args, format);
    int msgLen = vsnprintf(buf + prefix, sizeof(buf) - prefix - 2, format, args);
    va_end(args);
    if (msgLen < 0) return;
    int total = prefix + msgLen;
    if (total > (int)sizeof(buf) - 2) total = (int)sizeof(buf) - 2;
    buf[total++] = '\n';
    buf[total] = '\0';
    flockfile(stdout);
    // clear the progress rolling line so this message starts on a clean line
    if (g_tty_progress) fwrite("\r\033[K", 1, 4, stdout);
    fwrite(buf, 1, total, stdout);
    fflush(stdout);
    funlockfile(stdout);
}  // logDebug