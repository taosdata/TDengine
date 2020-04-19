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

#define DEBUG_ERROR 1U
#define DEBUG_WARN  2U
#define DEBUG_TRACE 4U
#define DEBUG_DUMP  8U

#define DEBUG_FILE   0x80
#define DEBUG_SCREEN 0x40

int32_t taosInitLog(char *logName, int32_t numOfLogLines, int32_t maxFiles);
void    taosCloseLog();
void    taosResetLog();

void    taosPrintLog(const char *const flags, int32_t dflag, const char *const format, ...);
void    taosPrintLongString(const char *const flags, int32_t dflag, const char *const format, ...);
void    taosDumpData(unsigned char *msg, int32_t len);

#ifdef __cplusplus
}
#endif

#endif
