/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software
 * Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef INC_BENCHLOG_H_
#define INC_BENCHLOG_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stdint.h>

//
// suport thread safe log module
//

enum LOG_LOCK {
    LOG_STDOUT,  // 0
    LOG_STDERR,  // 1
    LOG_RESULT,  // 2 g_arguments->fpOfInsertResult file lock
    LOG_COUNT    // 3
};

// init log
bool initLog();
// lock
void lockLog(int8_t idx);
// unlock
void unlockLog(int8_t idx);
// exit log
void exitLog();

#define debugPrint(fmt, ...)                                                \
    do {                                                                    \
        if (g_arguments->debug_print) {                                     \
            struct tm      Tm, *ptm;                                        \
            struct timeval timeSecs;                                        \
            time_t         curTime;                                         \
            toolsGetTimeOfDay(&timeSecs);                                   \
            curTime = timeSecs.tv_sec;                                      \
            ptm = toolsLocalTime(&curTime, &Tm);                            \
            lockLog(LOG_STDOUT);                                            \
            fprintf(stdout, "[%02d/%02d %02d:%02d:%02d.%06d] ",             \
                    ptm->tm_mon + 1,                                        \
                    ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec,   \
                    (int32_t)timeSecs.tv_usec);                             \
            fprintf(stdout, "DEBG: ");                                      \
            fprintf(stdout, "%s(%d) ", __FILE__, __LINE__);                 \
            fprintf(stdout, "" fmt, ##__VA_ARGS__);                         \
            unlockLog(LOG_STDOUT);                                          \
        }                                                                   \
    } while (0)

#define debugPrintWithLen(fmt, len, ...)                                    \
    do {                                                                    \
        if (g_arguments->debug_print) {                                     \
            struct tm      Tm, *ptm;                                        \
            struct timeval timeSecs;                                        \
            time_t         curTime;                                         \
            toolsGetTimeOfDay(&timeSecs);                                   \
            curTime = timeSecs.tv_sec;                                      \
            ptm = toolsLocalTime(&curTime, &Tm);                            \
            lockLog(LOG_STDOUT);                                            \
            fnprintf(stdout, len, "[%02d/%02d %02d:%02d:%02d.%06d] ",       \
                    ptm->tm_mon + 1,                                        \
                    ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec,   \
                    (int32_t)timeSecs.tv_usec);                             \
            fprintf(stdout, "DEBG: ");                                      \
            fprintf(stdout, "%s(%d) ", __FILE__, __LINE__);                 \
            fprintf(stdout, "" fmt, ##__VA_ARGS__);                         \
            unlockLog(LOG_STDOUT);                                          \
        }                                                                   \
    } while (0)

#define debugPrintJsonNoTime(json)                                          \
    do {                                                                    \
        if (g_arguments->debug_print) {                                     \
            char *out = tools_cJSON_PrintUnformatted(json);                 \
            lockLog(LOG_STDOUT);                                            \
            fprintf(stdout, "JSON: %s\n", out);                             \
            unlockLog(LOG_STDOUT);                                          \
            free(out);                                                      \
        }                                                                   \
    } while (0)

#define debugPrintNoTimestamp(fmt, ...)                                     \
    do {                                                                    \
        if (g_arguments->debug_print) {                                     \
            lockLog(LOG_STDOUT);                                            \
            fprintf(stdout, "" fmt, ##__VA_ARGS__);                         \
            unlockLog(LOG_STDOUT);                                          \
        }                                                                   \
    } while (0)

#define infoPrintNoTimestamp(fmt, ...)                                      \
    do {                                                                    \
        lockLog(LOG_STDOUT);                                                \
        fprintf(stdout, "" fmt, ##__VA_ARGS__);                             \
        unlockLog(LOG_STDOUT);                                              \
    } while (0)

#define infoPrintNoTimestampToFile(fmt, ...)                                \
    do {                                                                    \
        lockLog(LOG_RESULT);                                                \
        fprintf(g_arguments->fpOfInsertResult, "" fmt, ##__VA_ARGS__);      \
        unlockLog(LOG_RESULT);                                              \
    } while (0)

#define infoPrint(fmt, ...)                                                 \
    do {                                                                    \
        struct tm      Tm, *ptm;                                            \
        struct timeval timeSecs;                                            \
        time_t         curTime;                                             \
        toolsGetTimeOfDay(&timeSecs);                                       \
        curTime = timeSecs.tv_sec;                                          \
        ptm = toolsLocalTime(&curTime, &Tm);                                \
        lockLog(LOG_STDOUT);                                                \
        fprintf(stdout, "[%02d/%02d %02d:%02d:%02d.%06d] ",                 \
                ptm->tm_mon + 1,                                            \
                ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec,       \
                (int32_t)timeSecs.tv_usec);                                 \
        fprintf(stdout, "INFO: " fmt, ##__VA_ARGS__);                       \
        unlockLog(LOG_STDOUT);                                              \
    } while (0)

#define infoPrintToFile(fmt, ...)                                        \
    do {                                                                 \
        struct tm      Tm, *ptm;                                         \
        struct timeval timeSecs;                                         \
        time_t         curTime;                                          \
        toolsGetTimeOfDay(&timeSecs);                                    \
        curTime = timeSecs.tv_sec;                                       \
        ptm = toolsLocalTime(&curTime, &Tm);                             \
        lockLog(LOG_RESULT);                                             \
        fprintf(g_arguments->fpOfInsertResult,"[%02d/%02d %02d:%02d:%02d.%06d] ", ptm->tm_mon + 1, \
                ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec,    \
                (int32_t)timeSecs.tv_usec);                              \
        fprintf(g_arguments->fpOfInsertResult, "INFO: " fmt, ##__VA_ARGS__);\
        unlockLog(LOG_RESULT);                                            \
    } while (0)

#define perfPrint(fmt, ...)                                                 \
    do {                                                                    \
        if (g_arguments->performance_print) {                               \
            struct tm      Tm, *ptm;                                        \
            struct timeval timeSecs;                                        \
            time_t         curTime;                                         \
            toolsGetTimeOfDay(&timeSecs);                                   \
            curTime = timeSecs.tv_sec;                                      \
            ptm = toolsLocalTime(&curTime, &Tm);                            \
            lockLog(LOG_STDERR);                                            \
            fprintf(stderr, "[%02d/%02d %02d:%02d:%02d.%06d] ",             \
                    ptm->tm_mon + 1,                                        \
                    ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec,   \
                    (int32_t)timeSecs.tv_usec);                             \
            fprintf(stderr, "PERF: " fmt, ##__VA_ARGS__);                   \
            unlockLog(LOG_STDERR);                                          \
            if (g_arguments->fpOfInsertResult && !g_arguments->terminate) { \
                lockLog(LOG_RESULT);                                        \
                fprintf(g_arguments->fpOfInsertResult,                      \
                        "[%02d/%02d %02d:%02d:%02d.%06d] ",                 \
                        ptm->tm_mon + 1,                                    \
                        ptm->tm_mday, ptm->tm_hour, ptm->tm_min,            \
                        ptm->tm_sec,                                        \
                        (int32_t)timeSecs.tv_usec);                         \
                fprintf(g_arguments->fpOfInsertResult, "PERF: ");           \
                fprintf(g_arguments->fpOfInsertResult,                      \
                        "" fmt, ##__VA_ARGS__);                             \
                unlockLog(LOG_RESULT);                                      \
            }                                                               \
        }                                                                   \
    } while (0)

#define errorPrint(fmt, ...)                                                \
    do {                                                                    \
        struct tm      Tm, *ptm;                                            \
        struct timeval timeSecs;                                            \
        time_t         curTime;                                             \
        toolsGetTimeOfDay(&timeSecs);                                       \
        curTime = timeSecs.tv_sec;                                          \
        ptm = toolsLocalTime(&curTime, &Tm);                                \
        lockLog(LOG_STDERR);                                                \
        fprintf(stderr, "[%02d/%02d %02d:%02d:%02d.%06d] ",                 \
                ptm->tm_mon + 1,                                            \
                ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec,       \
                (int32_t)timeSecs.tv_usec);                                 \
        fprintf(stderr, "\033[31m");                                        \
        fprintf(stderr, "ERROR: ");                                         \
        if (g_arguments->debug_print) {                                     \
            fprintf(stderr, "%s(%d) ", __FILE__, __LINE__);                 \
        }                                                                   \
        fprintf(stderr, "" fmt, ##__VA_ARGS__);                             \
        fprintf(stderr, "\033[0m");                                         \
        unlockLog(LOG_STDERR);                                              \
        if (g_arguments->fpOfInsertResult && !g_arguments->terminate) {     \
            lockLog(LOG_RESULT);                                            \
            fprintf(g_arguments->fpOfInsertResult,                          \
                    "[%02d/%02d %02d:%02d:%02d.%06d] ", ptm->tm_mon + 1,    \
                ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec,       \
                (int32_t)timeSecs.tv_usec);                                 \
            fprintf(g_arguments->fpOfInsertResult, "ERROR: ");              \
            fprintf(g_arguments->fpOfInsertResult, "" fmt, ##__VA_ARGS__);  \
            unlockLog(LOG_RESULT);                                          \
        }                                                                   \
    } while (0)

#define warnPrint(fmt, ...)                                                 \
    do {                                                                    \
        struct tm      Tm, *ptm;                                            \
        struct timeval timeSecs;                                            \
        time_t         curTime;                                             \
        toolsGetTimeOfDay(&timeSecs);                                       \
        curTime = timeSecs.tv_sec;                                          \
        ptm = toolsLocalTime(&curTime, &Tm);                                \
        lockLog(LOG_STDERR);                                                \
        fprintf(stderr, "[%02d/%02d %02d:%02d:%02d.%06d] ",                 \
                ptm->tm_mon + 1,                                            \
                ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec,       \
                (int32_t)timeSecs.tv_usec);                                 \
        fprintf(stderr, "\033[33m");                                        \
        fprintf(stderr, "WARN: ");                                          \
        if (g_arguments->debug_print) {                                     \
            fprintf(stderr, "%s(%d) ", __FILE__, __LINE__);                 \
        }                                                                   \
        fprintf(stderr, "" fmt, ##__VA_ARGS__);                             \
        fprintf(stderr, "\033[0m");                                         \
        unlockLog(LOG_STDERR);                                              \
        if (g_arguments->fpOfInsertResult && !g_arguments->terminate) {     \
            lockLog(LOG_RESULT);                                            \
            fprintf(g_arguments->fpOfInsertResult,                          \
                    "[%02d/%02d %02d:%02d:%02d.%06d] ", ptm->tm_mon + 1,    \
                ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec,       \
                (int32_t)timeSecs.tv_usec);                                 \
            fprintf(g_arguments->fpOfInsertResult, "WARN: ");               \
            fprintf(g_arguments->fpOfInsertResult, "" fmt, ##__VA_ARGS__);  \
            unlockLog(LOG_RESULT);                                          \
        }                                                                   \
    } while (0)

#define succPrint(fmt, ...)                                                 \
    do {                                                                    \
        struct tm      Tm, *ptm;                                            \
        struct timeval timeSecs;                                            \
        time_t         curTime;                                             \
        toolsGetTimeOfDay(&timeSecs);                                       \
        curTime = timeSecs.tv_sec;                                          \
        ptm = toolsLocalTime(&curTime, &Tm);                                \
        lockLog(LOG_STDERR);                                                \
        fprintf(stderr, "[%02d/%02d %02d:%02d:%02d.%06d] ",                 \
                ptm->tm_mon + 1,                                            \
                ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec,       \
                (int32_t)timeSecs.tv_usec);                                 \
        fprintf(stderr, "\033[32m");                                        \
        fprintf(stderr, "SUCC: ");                                          \
        if (g_arguments->debug_print) {                                     \
            fprintf(stderr, "%s(%d) ", __FILE__, __LINE__);                 \
        }                                                                   \
        fprintf(stderr, "" fmt, ##__VA_ARGS__);                             \
        fprintf(stderr, "\033[0m");                                         \
        unlockLog(LOG_STDERR);                                              \
        if (g_arguments->fpOfInsertResult && !g_arguments->terminate) {     \
            lockLog(LOG_RESULT);                                            \
            fprintf(g_arguments->fpOfInsertResult,                          \
                    "[%02d/%02d %02d:%02d:%02d.%06d] ", ptm->tm_mon + 1,    \
                ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec,       \
                (int32_t)timeSecs.tv_usec);                                 \
            fprintf(g_arguments->fpOfInsertResult, "SUCC: ");               \
            fprintf(g_arguments->fpOfInsertResult, "" fmt, ##__VA_ARGS__);  \
            unlockLog(LOG_RESULT);                                          \
        }                                                                   \
    } while (0)

#ifdef __cplusplus
}
#endif

#endif   // INC_BENCH_H_
