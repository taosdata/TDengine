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

#ifndef __MONITOR_H__
#define __MONITOR_H__

#include "tglobalcfg.h"
#include "tlog.h"

#define monitorError(...)                    \
  if (monitorDebugFlag & DEBUG_ERROR) {      \
    tprintf("ERROR MON ", 255, __VA_ARGS__); \
  }
#define monitorWarn(...)                                  \
  if (monitorDebugFlag & DEBUG_WARN) {                    \
    tprintf("WARN  MON ", monitorDebugFlag, __VA_ARGS__); \
  }
#define monitorTrace(...)                           \
  if (monitorDebugFlag & DEBUG_TRACE) {             \
    tprintf("MON ", monitorDebugFlag, __VA_ARGS__); \
  }
#define monitorPrint(...) \
  { tprintf("MON ", 255, __VA_ARGS__); }

#define monitorLError(...) taosLogError(__VA_ARGS__) monitorError(__VA_ARGS__)
#define monitorLWarn(...) taosLogWarn(__VA_ARGS__) monitorWarn(__VA_ARGS__)
#define monitorLPrint(...) taosLogPrint(__VA_ARGS__) monitorPrint(__VA_ARGS__)

#endif