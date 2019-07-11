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

#ifndef TDENGINE_HTTP_H
#define TDENGINE_HTTP_H

#include "tglobalcfg.h"
#include "tlog.h"

#define httpError(...)                       \
  if (httpDebugFlag & DEBUG_ERROR) {         \
    tprintf("ERROR HTP ", 255, __VA_ARGS__); \
  }
#define httpWarn(...)                                  \
  if (httpDebugFlag & DEBUG_WARN) {                    \
    tprintf("WARN  HTP ", httpDebugFlag, __VA_ARGS__); \
  }
#define httpTrace(...)                           \
  if (httpDebugFlag & DEBUG_TRACE) {             \
    tprintf("HTP ", httpDebugFlag, __VA_ARGS__); \
  }
#define httpDump(...)                                        \
  if (httpDebugFlag & DEBUG_TRACE) {                         \
    taosPrintLongString("HTP ", httpDebugFlag, __VA_ARGS__); \
  }
#define httpPrint(...) \
  { tprintf("HTP ", 255, __VA_ARGS__); }

#define httpLError(...) taosLogError(__VA_ARGS__) httpError(__VA_ARGS__)
#define httpLWarn(...) taosLogWarn(__VA_ARGS__) httpWarn(__VA_ARGS__)
#define httpLPrint(...) taosLogPrint(__VA_ARGS__) httpPrint(__VA_ARGS__)

#endif
