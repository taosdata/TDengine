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

#define vError(fmt, ...) { if (vDebugFlag & DEBUG_ERROR) { TLOG("ERROR VND ", 255, fmt, ##__VA_ARGS__); }}
#define vWarn(fmt, ...)  { if (vDebugFlag & DEBUG_WARN)  { TLOG("WARN VND ", vDebugFlag, fmt, ##__VA_ARGS__); }}
#define vTrace(fmt, ...) { if (vDebugFlag & DEBUG_TRACE) { TLOG("VND ", vDebugFlag, fmt, ##__VA_ARGS__); }}
#define vPrint(fmt, ...) { TLOG("VND ", 255, fmt, ##__VA_ARGS__); }

typedef struct {
  int32_t      vgId;      // global vnode group ID
  int32_t      refCount;  // reference count
  int          status; 
  int8_t       role;   
  int64_t      version;   // current version 
  int64_t      fversion;  // version on saved data file
  void        *wqueue;
  void        *rqueue;
  void        *wal;
  void        *tsdb;
  void        *sync;
  void        *events;
  void        *cq;  // continuous query
  int32_t      cfgVersion;
  STsdbCfg     tsdbCfg;
  SSyncCfg     syncCfg;
  SWalCfg      walCfg;
  char        *rootDir;
  char         db[TSDB_DB_NAME_LEN];
} SVnodeObj;

int  vnodeWriteToQueue(void *param, void *pHead, int type);
void vnodeInitWriteFp(void);
void vnodeInitReadFp(void);

#ifdef __cplusplus
}
#endif

#endif
