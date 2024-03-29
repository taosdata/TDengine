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

#ifndef _TD_UTIL_LOG_H_
#define _TD_UTIL_LOG_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  DEBUG_FATAL = 1,
  DEBUG_ERROR = 1,
  DEBUG_WARN = 2,
  DEBUG_INFO = 2,
  DEBUG_DEBUG = 4,
  DEBUG_TRACE = 8,
  DEBUG_DUMP = 16,
  DEBUG_SCREEN = 64,
  DEBUG_FILE = 128
} ELogLevel;

typedef void (*LogFp)(int64_t ts, ELogLevel level, const char *content);

extern bool    tsLogEmbedded;
extern bool    tsAsyncLog;
extern bool    tsAssert;
extern int32_t tsNumOfLogLines;
extern int32_t tsLogKeepDays;
extern LogFp   tsLogFp;
extern int64_t tsNumOfErrorLogs;
extern int64_t tsNumOfInfoLogs;
extern int64_t tsNumOfDebugLogs;
extern int64_t tsNumOfTraceLogs;
extern int32_t dDebugFlag;
extern int32_t vDebugFlag;
extern int32_t mDebugFlag;
extern int32_t cDebugFlag;
extern int32_t jniDebugFlag;
extern int32_t tmrDebugFlag;
extern int32_t uDebugFlag;
extern int32_t rpcDebugFlag;
extern int32_t qDebugFlag;
extern int32_t stDebugFlag;
extern int32_t wDebugFlag;
extern int32_t sDebugFlag;
extern int32_t tsdbDebugFlag;
extern int32_t tqDebugFlag;
extern int32_t fsDebugFlag;
extern int32_t metaDebugFlag;
extern int32_t udfDebugFlag;
extern int32_t smaDebugFlag;
extern int32_t idxDebugFlag;
extern int32_t tdbDebugFlag;
extern int32_t sndDebugFlag;
extern int32_t simDebugFlag;

int32_t taosInitLog(const char *logName, int32_t maxFiles);
void    taosCloseLog();
void    taosResetLog();
void    taosDumpData(uint8_t *msg, int32_t len);

void taosPrintLog(const char *flags, ELogLevel level, int32_t dflag, const char *format, ...)
#ifdef __GNUC__
    __attribute__((format(printf, 4, 5)))
#endif
    ;

void taosPrintLongString(const char *flags, ELogLevel level, int32_t dflag, const char *format, ...)
#ifdef __GNUC__
    __attribute__((format(printf, 4, 5)))
#endif
    ;

void taosPrintSlowLog(const char *format, ...)
#ifdef __GNUC__
      __attribute__((format(printf, 1, 2)))
#endif
      ;

bool taosAssertDebug(bool condition, const char *file, int32_t line, const char *format, ...);
bool taosAssertRelease(bool condition);

// Disable all asserts that may compromise the performance.
#if defined DISABLE_ASSERT
#define ASSERT(condition)
#define ASSERTS(condition, ...) (0)
#else
#define ASSERTS(condition, ...) ((condition) ? false : taosAssertDebug(condition, __FILE__, __LINE__, __VA_ARGS__))
#ifdef NDEBUG
#define ASSERT(condition) taosAssertRelease(condition)
#else
#define ASSERT(condition) ASSERTS(condition, "assert info not provided")
#endif
#endif

void    taosLogCrashInfo(char *nodeType, char *pMsg, int64_t msgLen, int signum, void *sigInfo);
void    taosReadCrashInfo(char *filepath, char **pMsg, int64_t *pMsgLen, TdFilePtr *pFd);
void    taosReleaseCrashLogFile(TdFilePtr pFile, bool truncateFile);

// clang-format off
#define uFatal(...) { if (uDebugFlag & DEBUG_FATAL) { taosPrintLog("UTL FATAL", DEBUG_FATAL, tsLogEmbedded ? 255 : uDebugFlag, __VA_ARGS__); }}
#define uError(...) { if (uDebugFlag & DEBUG_ERROR) { taosPrintLog("UTL ERROR ", DEBUG_ERROR, tsLogEmbedded ? 255 : uDebugFlag, __VA_ARGS__); }}
#define uWarn(...)  { if (uDebugFlag & DEBUG_WARN)  { taosPrintLog("UTL WARN ", DEBUG_WARN, tsLogEmbedded ? 255 : uDebugFlag, __VA_ARGS__); }}
#define uInfo(...)  { if (uDebugFlag & DEBUG_INFO)  { taosPrintLog("UTL ", DEBUG_INFO, tsLogEmbedded ? 255 : uDebugFlag, __VA_ARGS__); }}
#define uDebug(...) { if (uDebugFlag & DEBUG_DEBUG) { taosPrintLog("UTL ", DEBUG_DEBUG, uDebugFlag, __VA_ARGS__); }}
#define uTrace(...) { if (uDebugFlag & DEBUG_TRACE) { taosPrintLog("UTL ", DEBUG_TRACE, uDebugFlag, __VA_ARGS__); }}
#define uDebugL(...) { if (uDebugFlag & DEBUG_DEBUG) { taosPrintLongString("UTL ", DEBUG_DEBUG, uDebugFlag, __VA_ARGS__); }}
#define uInfoL(...) { if (uDebugFlag & DEBUG_INFO) { taosPrintLongString("UTL ", DEBUG_INFO, uDebugFlag, __VA_ARGS__); }}

#define pError(...) { taosPrintLog("APP ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }
#define pPrint(...) { taosPrintLog("APP ", DEBUG_INFO, 255, __VA_ARGS__); }
// clang-format on
// #define BUF_PAGE_DEBUG
#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_LOG_H_*/
