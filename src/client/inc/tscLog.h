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

#ifndef TDENGINE_TSC_LOG_H
#define TDENGINE_TSC_LOG_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tlog.h"

extern int32_t cdebugFlag;

#define tscError(...)                        \
  if (cdebugFlag & DEBUG_ERROR) {            \
    taosPrintLog("ERROR TSC ", 255, __VA_ARGS__); \
  }
#define tscWarn(...)                                \
  if (cdebugFlag & DEBUG_WARN) {                    \
    taosPrintLog("WARN  TSC ", cdebugFlag, __VA_ARGS__); \
  }
#define tscTrace(...)                         \
  if (cdebugFlag & DEBUG_TRACE) {             \
    taosPrintLog("TSC ", cdebugFlag, __VA_ARGS__); \
  }
#define tscPrint(...) \
  { taosPrintLog("TSC ", 255, __VA_ARGS__); }
#define tscDump(...)                                      \
  if (cdebugFlag & DEBUG_TRACE) {                         \
    taosPrintLongString("TSC ", cdebugFlag, __VA_ARGS__); \
  }

#ifdef __cplusplus
}
#endif

#endif
