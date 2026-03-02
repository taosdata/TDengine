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
#include <stdarg.h>
#include <stdio.h>

// Thread-safe log: format message + newline into buffer, then atomic stdio output.
// flockfile/funlockfile ensures the entire write+flush is atomic across threads.

#define LOG_BUF_SIZE 4096

void logError(const char *format, ...) {
    char buf[LOG_BUF_SIZE];
    va_list args;
    va_start(args, format);
    int len = vsnprintf(buf, sizeof(buf) - 2, format, args);
    va_end(args);
    if (len < 0) return;
    if (len > (int)sizeof(buf) - 2) len = (int)sizeof(buf) - 2;
    buf[len++] = '\n';
    buf[len] = '\0';
    flockfile(stderr);
    fwrite(buf, 1, len, stderr);
    fflush(stderr);
    funlockfile(stderr);
}

void logInfo(const char *format, ...) {
    char buf[LOG_BUF_SIZE];
    va_list args;
    va_start(args, format);
    int len = vsnprintf(buf, sizeof(buf) - 2, format, args);
    va_end(args);
    if (len < 0) return;
    if (len > (int)sizeof(buf) - 2) len = (int)sizeof(buf) - 2;
    buf[len++] = '\n';
    buf[len] = '\0';
    flockfile(stdout);
    fwrite(buf, 1, len, stdout);
    fflush(stdout);
    funlockfile(stdout);
}

void logWarn(const char *format, ...) {
    char buf[LOG_BUF_SIZE];
    va_list args;
    va_start(args, format);
    int len = vsnprintf(buf, sizeof(buf) - 2, format, args);
    va_end(args);
    if (len < 0) return;
    if (len > (int)sizeof(buf) - 2) len = (int)sizeof(buf) - 2;
    buf[len++] = '\n';
    buf[len] = '\0';
    flockfile(stderr);
    fwrite(buf, 1, len, stderr);
    fflush(stderr);
    funlockfile(stderr);
}

void logDebug(const char *format, ...) {
    if (!argDebug()) return;
    
    char buf[LOG_BUF_SIZE];
    va_list args;
    va_start(args, format);
    int len = vsnprintf(buf, sizeof(buf) - 2, format, args);
    va_end(args);
    if (len < 0) return;
    if (len > (int)sizeof(buf) - 2) len = (int)sizeof(buf) - 2;
    buf[len++] = '\n';
    buf[len] = '\0';
    flockfile(stdout);
    fwrite(buf, 1, len, stdout);
    fflush(stdout);
    funlockfile(stdout);
} 