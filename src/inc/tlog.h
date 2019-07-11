/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef TDENGINE_TLOG_H
#define TDENGINE_TLOG_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "tglobalcfg.h"

#define DEBUG_ERROR 1
#define DEBUG_WARN  2
#define DEBUG_TRACE 4
#define DEBUG_DUMP  8

#define DEBUG_FILE   0x80
#define DEBUG_SCREEN 0x40

extern int uDebugFlag;

extern void (*taosLogFp)(int level, const char *const format, ...);

extern void (*taosLogSqlFp)(char *sql);

extern void (*taosLogAcctFp)(char *acctId, int64_t currentPointsPerSecond, int64_t maxPointsPerSecond,
                             int64_t totalTimeSeries, int64_t maxTimeSeries, int64_t totalStorage, int64_t maxStorage,
                             int64_t totalQueryTime, int64_t maxQueryTime, int64_t totalInbound, int64_t maxInbound,
                             int64_t totalOutbound, int64_t maxOutbound, int64_t totalDbs, int64_t maxDbs,
                             int64_t totalUsers, int64_t maxUsers, int64_t totalStreams, int64_t maxStreams,
                             int64_t totalConns, int64_t maxConns, int8_t accessState);

int taosInitLog(char *logName, int numOfLogLines, int maxFiles);

void taosCloseLogger();

void taosDumpData(unsigned char *msg, int len);

int taosOpenLogFile(char *fn);

void tprintf(const char *const flags, int dflag, const char *const format, ...);

void taosPrintLongString(const char *const flags, int dflag, const char *const format, ...);

int taosOpenLogFileWithMaxLines(char *fn, int maxLines, int maxFileNum);

void taosCloseLog();

void taosResetLogFile();

#define taosLogError(...)         \
  if (taosLogFp) {                \
    (*taosLogFp)(2, __VA_ARGS__); \
  }
#define taosLogWarn(...)          \
  if (taosLogFp) {                \
    (*taosLogFp)(1, __VA_ARGS__); \
  }
#define taosLogPrint(...)         \
  if (taosLogFp) {                \
    (*taosLogFp)(0, __VA_ARGS__); \
  }

// utility log function
#define pError(...)                          \
  if (uDebugFlag & DEBUG_ERROR) {            \
    tprintf("ERROR UTL ", 255, __VA_ARGS__); \
  }
#define pWarn(...)                                  \
  if (uDebugFlag & DEBUG_WARN) {                    \
    tprintf("WARN  UTL ", uDebugFlag, __VA_ARGS__); \
  }
#define pTrace(...)                           \
  if (uDebugFlag & DEBUG_TRACE) {             \
    tprintf("UTL ", uDebugFlag, __VA_ARGS__); \
  }
#define pDump(x, y)              \
  if (uDebugFlag & DEBUG_DUMP) { \
    taosDumpData(x, y);          \
  }

#define pPrint(...) \
  { tprintf("UTL ", tscEmbedded ? 255 : uDebugFlag, __VA_ARGS__); }

// client log function
extern int cdebugFlag;

#define tscError(...)                               \
  if (cdebugFlag & DEBUG_ERROR) {                   \
    tprintf("ERROR TSC ", cdebugFlag, __VA_ARGS__); \
  }
#define tscWarn(...)                                \
  if (cdebugFlag & DEBUG_WARN) {                    \
    tprintf("WARN  TSC ", cdebugFlag, __VA_ARGS__); \
  }
#define tscTrace(...)                         \
  if (cdebugFlag & DEBUG_TRACE) {             \
    tprintf("TSC ", cdebugFlag, __VA_ARGS__); \
  }
#define tscPrint(...) \
  { tprintf("TSC ", 255, __VA_ARGS__); }

#define jniError(...)                                 \
  if (jnidebugFlag & DEBUG_ERROR) {                   \
    tprintf("ERROR JNI ", jnidebugFlag, __VA_ARGS__); \
  }
#define jniWarn(...)                                  \
  if (jnidebugFlag & DEBUG_WARN) {                    \
    tprintf("WARN  JNI ", jnidebugFlag, __VA_ARGS__); \
  }
#define jniTrace(...)                           \
  if (jnidebugFlag & DEBUG_TRACE) {             \
    tprintf("JNI ", jnidebugFlag, __VA_ARGS__); \
  }
#define jniPrint(...) \
  { tprintf("JNI ", 255, __VA_ARGS__); }

// rpc log function
extern int taosDebugFlag;
#define tError(...)                                    \
  if (taosDebugFlag & DEBUG_ERROR) {                   \
    tprintf("ERROR RPC ", taosDebugFlag, __VA_ARGS__); \
  }
#define tWarn(...)                                     \
  if (taosDebugFlag & DEBUG_WARN) {                    \
    tprintf("WARN  RPC ", taosDebugFlag, __VA_ARGS__); \
  }
#define tTrace(...)                              \
  if (taosDebugFlag & DEBUG_TRACE) {             \
    tprintf("RPC ", taosDebugFlag, __VA_ARGS__); \
  }
#define tPrint(...) \
  { tprintf("RPC ", 255, __VA_ARGS__); }
#define tDump(x, y)                      \
  if (taosDebugFlag & DEBUG_DUMP) {      \
    taosDumpData((unsigned char *)x, y); \
  }

// dnode log function
#define dError(...)                          \
  if (ddebugFlag & DEBUG_ERROR) {            \
    tprintf("ERROR DND ", 255, __VA_ARGS__); \
  }
#define dWarn(...)                                  \
  if (ddebugFlag & DEBUG_WARN) {                    \
    tprintf("WARN  DND ", ddebugFlag, __VA_ARGS__); \
  }
#define dTrace(...)                           \
  if (ddebugFlag & DEBUG_TRACE) {             \
    tprintf("DND ", ddebugFlag, __VA_ARGS__); \
  }
#define dPrint(...) \
  { tprintf("DND ", 255, __VA_ARGS__); }

#define dLError(...) taosLogError(__VA_ARGS__) dError(__VA_ARGS__)
#define dLWarn(...) taosLogWarn(__VA_ARGS__) dWarn(__VA_ARGS__)
#define dLPrint(...) taosLogPrint(__VA_ARGS__) dPrint(__VA_ARGS__)

#define qTrace(...)                               \
  if (qdebugFlag & DEBUG_TRACE) {                 \
    tprintf("DND QRY ", qdebugFlag, __VA_ARGS__); \
  }

// mnode log function
#define mError(...)                          \
  if (mdebugFlag & DEBUG_ERROR) {            \
    tprintf("ERROR MND ", 255, __VA_ARGS__); \
  }
#define mWarn(...)                                  \
  if (mdebugFlag & DEBUG_WARN) {                    \
    tprintf("WARN  MND ", mdebugFlag, __VA_ARGS__); \
  }
#define mTrace(...)                           \
  if (mdebugFlag & DEBUG_TRACE) {             \
    tprintf("MND ", mdebugFlag, __VA_ARGS__); \
  }
#define mPrint(...) \
  { tprintf("MND ", 255, __VA_ARGS__); }

#define mLError(...) taosLogError(__VA_ARGS__) mError(__VA_ARGS__)
#define mLWarn(...) taosLogWarn(__VA_ARGS__) mWarn(__VA_ARGS__)
#define mLPrint(...) taosLogPrint(__VA_ARGS__) mPrint(__VA_ARGS__)

#ifdef __cplusplus
}
#endif

#endif
