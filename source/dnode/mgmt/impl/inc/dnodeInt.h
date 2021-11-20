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

#ifndef _TD_DNODE_INT_H_
#define _TD_DNODE_INT_H_

#ifdef __cplusplus
extern "C" {
#endif
#include "os.h"
#include "taosmsg.h"
#include "tglobal.h"
#include "thash.h"
#include "tlog.h"
#include "trpc.h"
#include "tthread.h"
#include "ttime.h"

extern int32_t dDebugFlag;

#define dFatal(...) { if (dDebugFlag & DEBUG_FATAL) { taosPrintLog("SRV FATAL ", 255, __VA_ARGS__); }}
#define dError(...) { if (dDebugFlag & DEBUG_ERROR) { taosPrintLog("SRV ERROR ", 255, __VA_ARGS__); }}
#define dWarn(...)  { if (dDebugFlag & DEBUG_WARN)  { taosPrintLog("SRV WARN ", 255, __VA_ARGS__); }}
#define dInfo(...)  { if (dDebugFlag & DEBUG_INFO)  { taosPrintLog("SRV ", 255, __VA_ARGS__); }}
#define dDebug(...) { if (dDebugFlag & DEBUG_DEBUG) { taosPrintLog("SRV ", dDebugFlag, __VA_ARGS__); }}
#define dTrace(...) { if (dDebugFlag & DEBUG_TRACE) { taosPrintLog("SRV ", dDebugFlag, __VA_ARGS__); }}

typedef enum { DN_STAT_INIT, DN_STAT_RUNNING, DN_STAT_STOPPED } EStat;
typedef void (*MsgFp)(SRpcMsg *pMsg, SEpSet *pEpSet);

typedef struct {
  char *dnode;
  char *mnode;
  char *vnodes;
} SDnodeDir;

typedef struct {
  int32_t         dnodeId;
  int64_t         clusterId;
  SDnodeEps      *dnodeEps;
  SHashObj       *dnodeHash;
  SEpSet          mnodeEpSetForShell;
  SEpSet          mnodeEpSetForPeer;
  char           *file;
  uint32_t        rebootTime;
  int8_t          dropped;
  int8_t          threadStop;
  pthread_t      *threadId;
  pthread_mutex_t mutex;
} SDnodeDnode;

typedef struct {
} SDnodeMnode;

typedef struct {
} SDnodeVnodes;

typedef struct {
  void *peerRpc;
  void *shellRpc;
  void *clientRpc;
} SDnodeTrans;

typedef struct SDnode {
  EStat        stat;
  SDnodeDir    dir;
  SDnodeDnode  dnode;
  SDnodeVnodes vnodes;
  SDnodeMnode  mnode;
  SDnodeTrans  trans;
  SStartupMsg  startup;
} SDnode;

EStat dnodeGetStat(SDnode *pDnode);
void  dnodeSetStat(SDnode *pDnode, EStat stat);
char *dnodeStatStr(EStat stat);

void dnodeReportStartup(SDnode *pDnode, char *name, char *desc);
void dnodeGetStartup(SDnode *pDnode, SStartupMsg *pStartup);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DNODE_INT_H_*/