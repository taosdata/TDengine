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

#ifndef _TD_DND_DEF_H_
#define _TD_DND_DEF_H_

#include "dndLog.h"

#include "cJSON.h"
#include "tcache.h"
#include "tcrc32c.h"
#include "tdatablock.h"
#include "tglobal.h"
#include "thash.h"
#include "tlockfree.h"
#include "tlog.h"
#include "tmsg.h"
#include "tmsgcb.h"
#include "tprocess.h"
#include "tqueue.h"
#include "trpc.h"
#include "tthread.h"
#include "ttime.h"
#include "tworker.h"

#include "dnode.h"
#include "monitor.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum { NODE_BEGIN, VNODE, QNODE, SNODE, MNODE, BNODE, NODE_END } EDndNodeType;
typedef enum { DND_STAT_INIT, DND_STAT_RUNNING, DND_STAT_STOPPED } EDndRunStatus;
typedef enum { DND_ENV_INIT, DND_ENV_READY, DND_ENV_CLEANUP } EDndEnvStatus;
typedef enum { DND_PROC_SINGLE, DND_PROC_CHILD, DND_PROC_PARENT } EDndProcType;

typedef int32_t (*NodeMsgFp)(struct SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
typedef int32_t (*OpenNodeFp)(struct SMgmtWrapper *pWrapper);
typedef void (*CloseNodeFp)(struct SMgmtWrapper *pWrapper);
typedef int32_t (*StartNodeFp)(struct SMgmtWrapper *pWrapper);
typedef void (*StopNodeFp)(struct SMgmtWrapper *pWrapper);
typedef int32_t (*CreateNodeFp)(struct SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
typedef int32_t (*DropNodeFp)(struct SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
typedef int32_t (*RequireNodeFp)(struct SMgmtWrapper *pWrapper, bool *required);

typedef struct {
  SMgmtWrapper *pQndWrapper;
  SMgmtWrapper *pMndWrapper;
  SMgmtWrapper *pNdWrapper;
} SMsgHandle;

typedef struct {
  OpenNodeFp    openFp;
  CloseNodeFp   closeFp;
  StartNodeFp   startFp;
  StopNodeFp    stopFp;
  CreateNodeFp  createFp;
  DropNodeFp    dropFp;
  RequireNodeFp requiredFp;
} SMgmtFp;

typedef struct SMgmtWrapper {
  SDnode *pDnode;
  struct {
    const char  *name;
    char        *path;
    int32_t      refCount;
    SRWLatch     latch;
    EDndNodeType ntype;
    bool         deployed;
    bool         required;
    SMgmtFp      fp;
    void        *pMgmt;
  };
  struct {
    EDndProcType procType;
    int32_t      procId;
    SProcObj    *procObj;
    SShm         procShm;
  };
  struct {
    int8_t    msgVgIds[TDMT_MAX];  // Handle the case where the same message type is distributed to qnode or vnode
    NodeMsgFp msgFps[TDMT_MAX];
  };
} SMgmtWrapper;

typedef struct {
  void      *serverRpc;
  void      *clientRpc;
  SMsgHandle msgHandles[TDMT_MAX];
} SDnodeTrans;

typedef struct {
  int32_t       dnodeId;
  int64_t       clusterId;
  int64_t       dnodeVer;
  int64_t       updateTime;
  int64_t       rebootTime;
  bool          dropped;
  int8_t        statusSent;
  SEpSet        mnodeEpSet;
  SHashObj     *dnodeHash;
  SArray       *dnodeEps;
  TdThread     *threadId;
  SRWLatch      latch;
  SSingleWorker mgmtWorker;
  SSingleWorker monitorWorker;
  SMsgCb        msgCb;
  SDnode       *pDnode;
  const char   *path;
  TdFilePtr     lockfile;
  struct {
    char     *localEp;
    char     *localFqdn;
    char     *firstEp;
    char     *secondEp;
    char     *dataDir;
    SDiskCfg *disks;
    int32_t   numOfDisks;
    int32_t   supportVnodes;
    uint16_t  serverPort;
  };
} SDnodeData;

typedef struct SDnode {
  EDndProcType  ptype;
  EDndNodeType  ntype;
  EDndRunStatus status;
  EDndEvent     event;
  SStartupReq   startup;
  SDnodeTrans   trans;
  SDnodeData    data;
  SMgmtWrapper  wrappers[NODE_END];
} SDnode;

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_DEF_H_*/