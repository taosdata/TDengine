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

#ifndef TDENGINE_VNODE_INT_H
#define TDENGINE_VNODE_INT_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tlog.h"
#include "tsync.h"
#include "twal.h"
#include "tcq.h"

extern int32_t vDebugFlag;

#define vFatal(...) { if (vDebugFlag & DEBUG_FATAL) { taosPrintLog("VND FATAL ", 255, __VA_ARGS__); }}
#define vError(...) { if (vDebugFlag & DEBUG_ERROR) { taosPrintLog("VND ERROR ", 255, __VA_ARGS__); }}
#define vWarn(...)  { if (vDebugFlag & DEBUG_WARN)  { taosPrintLog("VND WARN ", 255, __VA_ARGS__); }}
#define vInfo(...)  { if (vDebugFlag & DEBUG_INFO)  { taosPrintLog("VND ", 255, __VA_ARGS__); }}
#define vDebug(...) { if (vDebugFlag & DEBUG_DEBUG) { taosPrintLog("VND ", vDebugFlag, __VA_ARGS__); }}
#define vTrace(...) { if (vDebugFlag & DEBUG_TRACE) { taosPrintLog("VND ", vDebugFlag, __VA_ARGS__); }}

typedef struct {
  int32_t      vgId;      // global vnode group ID
  int32_t      refCount;  // reference count
  int32_t      queuedWMsg;
  int32_t      queuedRMsg;
  int32_t      flowctrlLevel;
  int8_t       status; 
  int8_t       role;   
  int8_t       accessState;
  int8_t       isFull;
  uint64_t     version;   // current version 
  uint64_t     fversion;  // version on saved data file
  void        *wqueue;
  void        *rqueue;
  void        *wal;
  void        *tsdb;
  int64_t      sync;
  void        *events;
  void        *cq;  // continuous query
  int32_t      cfgVersion;
  STsdbCfg     tsdbCfg;
  SSyncCfg     syncCfg;
  SWalCfg      walCfg;
  void        *qMgmt;
  char        *rootDir;
  tsem_t       sem;
  int8_t       dropped;
  char         db[TSDB_ACCT_LEN + TSDB_DB_NAME_LEN];
} SVnodeObj;

void vnodeInitWriteFp(void);
void vnodeInitReadFp(void);

#ifdef __cplusplus
}
#endif

#endif
