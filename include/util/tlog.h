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

#ifndef _TD_UTIL_LOG_H
#define _TD_UTIL_LOG_H

#ifdef __cplusplus
extern "C" {
#endif


// log
extern int8_t  tsAsyncLog;
extern int32_t tsNumOfLogLines;
extern int32_t tsLogKeepDays;
extern int32_t dDebugFlag;
extern int32_t vDebugFlag;
extern int32_t mDebugFlag;
extern int32_t cDebugFlag;
extern int32_t jniDebugFlag;
extern int32_t tmrDebugFlag;
extern int32_t sdbDebugFlag;
extern int32_t httpDebugFlag;
extern int32_t mqttDebugFlag;
extern int32_t monDebugFlag;
extern int32_t uDebugFlag;
extern int32_t rpcDebugFlag;
extern int32_t odbcDebugFlag;
extern int32_t qDebugFlag;
extern int32_t wDebugFlag;
extern int32_t sDebugFlag;
extern int32_t tsdbDebugFlag;
extern int32_t cqDebugFlag;
extern int32_t debugFlag;

#define DEBUG_FATAL 1U
#define DEBUG_ERROR DEBUG_FATAL
#define DEBUG_WARN 2U
#define DEBUG_INFO DEBUG_WARN
#define DEBUG_DEBUG 4U
#define DEBUG_TRACE 8U
#define DEBUG_DUMP 16U

#define DEBUG_SCREEN 64U
#define DEBUG_FILE 128U

int32_t taosInitLog(char *logName, int32_t numOfLogLines, int32_t maxFiles);
void    taosCloseLog();
void    taosResetLog();

void taosPrintLog(const char *flags, int32_t dflag, const char *format, ...)
#ifdef __GNUC__
    __attribute__((format(printf, 3, 4)))
#endif
    ;

void taosPrintLongString(const char *flags, int32_t dflag, const char *format, ...)
#ifdef __GNUC__
    __attribute__((format(printf, 3, 4)))
#endif
    ;

void taosDumpData(unsigned char *msg, int32_t len);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_LOG_H*/
