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

#ifndef TDENGINE_VNODE_LOG_H
#define TDENGINE_VNODE_LOG_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tlog.h"

extern int32_t ddebugFlag;

#define dError(...)                          \
  if (ddebugFlag & DEBUG_ERROR) {            \
    taosPrintLog("ERROR DND ", 255, __VA_ARGS__); \
  }
#define dWarn(...)                                  \
  if (ddebugFlag & DEBUG_WARN) {                    \
    taosPrintLog("WARN  DND ", ddebugFlag, __VA_ARGS__); \
  }
#define dTrace(...)                           \
  if (ddebugFlag & DEBUG_TRACE) {             \
    taosPrintLog("DND ", ddebugFlag, __VA_ARGS__); \
  }
#define dPrint(...) \
  { taosPrintLog("DND ", 255, __VA_ARGS__); }

#ifdef __cplusplus
}
#endif

#endif
