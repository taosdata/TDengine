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

#ifndef _TD_VNODE_INT_H_
#define _TD_VNODE_INT_H_

#include "vnode.h"

#include "meta.h"
// #include "sync.h"
#include "tlog.h"
#include "tq.h"
#include "tsdb.h"
#include "wal.h"

#ifdef __cplusplus
extern "C" {
#endif

extern int32_t vDebugFlag;

#define vFatal(...) { if (vDebugFlag & DEBUG_FATAL) { taosPrintLog("VND FATAL ", 255, __VA_ARGS__); }}
#define vError(...) { if (vDebugFlag & DEBUG_ERROR) { taosPrintLog("VND ERROR ", 255, __VA_ARGS__); }}
#define vWarn(...)  { if (vDebugFlag & DEBUG_WARN)  { taosPrintLog("VND WARN ", 255, __VA_ARGS__); }}
#define vInfo(...)  { if (vDebugFlag & DEBUG_INFO)  { taosPrintLog("VND ", 255, __VA_ARGS__); }}
#define vDebug(...) { if (vDebugFlag & DEBUG_DEBUG) { taosPrintLog("VND ", vDebugFlag, __VA_ARGS__); }}
#define vTrace(...) { if (vDebugFlag & DEBUG_TRACE) { taosPrintLog("VND ", vDebugFlag, __VA_ARGS__); }}

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_INT_H_*/
