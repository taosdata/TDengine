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

#ifndef _TD_TQ_INT_H_
#define _TD_TQ_INT_H_

#include "tq.h"
#include "tlog.h"
#include "trpc.h"
#ifdef __cplusplus
extern "C" {
#endif

extern int32_t tqDebugFlag;

#define tqFatal(...) { if (tqDebugFlag & DEBUG_FATAL) { taosPrintLog("TQ  FATAL ", 255, __VA_ARGS__); }}
#define tqError(...) { if (tqDebugFlag & DEBUG_ERROR) { taosPrintLog("TQ  ERROR ", 255, __VA_ARGS__); }}
#define tqWarn(...)  { if (tqDebugFlag & DEBUG_WARN)  { taosPrintLog("TQ  WARN ", 255, __VA_ARGS__); }}
#define tqInfo(...)  { if (tqDebugFlag & DEBUG_INFO)  { taosPrintLog("TQ  ", 255, __VA_ARGS__); }}
#define tqDebug(...) { if (tqDebugFlag & DEBUG_DEBUG) { taosPrintLog("TQ  ", tqDebugFlag, __VA_ARGS__); }}
#define tqTrace(...) { if (tqDebugFlag & DEBUG_TRACE) { taosPrintLog("TQ  ", tqDebugFlag, __VA_ARGS__); }}

// create persistent storage for meta info such as consuming offset
// return value > 0: cgId
// return value <= 0: error code
// int tqCreateTCGroup(STQ*, const char* topic, int cgId, tqBufferHandle** handle);
// create ring buffer in memory and load consuming offset
// int tqOpenTCGroup(STQ*, const char* topic, int cgId);
// destroy ring buffer and persist consuming offset
// int tqCloseTCGroup(STQ*, const char* topic, int cgId);
// delete persistent storage for meta info
// int tqDropTCGroup(STQ*, const char* topic, int cgId);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TQ_INT_H_*/
