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

#define DEBUG_FATAL 1U
#define DEBUG_ERROR DEBUG_FATAL
#define DEBUG_WARN  2U
#define DEBUG_INFO  DEBUG_WARN
#define DEBUG_DEBUG 4U
#define DEBUG_TRACE 8U
#define DEBUG_DUMP  16U

#define DEBUG_SCREEN 64U
#define DEBUG_FILE   128U

int32_t taosInitLog(char *logName, int32_t numOfLogLines, int32_t maxFiles);
void    taosCloseLog();
void    taosResetLog();

void    taosPrintLog(const char *flags, int32_t dflag, const char *format, ...)
#ifdef __GNUC__
 __attribute__((format(printf, 3, 4)))
#endif
;

void    taosPrintLongString(const char * flags, int32_t dflag, const char *format, ...)
#ifdef __GNUC__
 __attribute__((format(printf, 3, 4)))
#endif
;

void    taosDumpData(unsigned char *msg, int32_t len);

#ifdef __cplusplus
}
#endif

#endif
