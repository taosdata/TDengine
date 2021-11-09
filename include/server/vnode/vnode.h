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

#ifndef _TD_VNODE_H_
#define _TD_VNODE_H_

#include "os.h"
#include "trequest.h"

#include "meta.h"
#include "tq.h"
#include "tsdb.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------ TYPES EXPOSED ------------------------ */
typedef struct SVnode        SVnode;
typedef struct SVnodeOptions SVnodeOptions;

/* ------------------------ SVnode ------------------------ */
SVnode *vnodeOpen(const char *path, const SVnodeOptions *pVnodeOptions);
void    vnodeClose(SVnode *pVnode);
void    vnodeDestroy(const char *path);
int     vnodeProcessWriteReqs(SVnode *pVnode, SReqBatch *pReqBatch);
int     vnodeApplyWriteRequest(SVnode *pVnode, const SRequest *pRequest);
int     vnodeProcessReadReq(SVnode *pVnode, SRequest *pReq);
int     vnodeProcessSyncReq(SVnode *pVnode, SRequest *pReq);

/* ------------------------ SVnodeOptions ------------------------ */
void vnodeOptionsInit(SVnodeOptions *);
void vnodeOptionsClear(SVnodeOptions *);

/* ------------------------ STRUCT DEFINITIONS ------------------------ */
struct SVnodeOptions {
  size_t       wsize;
  STsdbOptions tsdbOptions;
  SMetaOptions metaOptions;
  // STqOptions   tqOptions; // TODO
};

/* ------------------------ FOR COMPILE ------------------------ */

#if 1

#include "taosmsg.h"
#include "trpc.h"

typedef struct {
  char     db[TSDB_FULL_DB_NAME_LEN];
  int32_t  cacheBlockSize;  // MB
  int32_t  totalBlocks;
  int32_t  daysPerFile;
  int32_t  daysToKeep0;
  int32_t  daysToKeep1;
  int32_t  daysToKeep2;
  int32_t  minRowsPerFileBlock;
  int32_t  maxRowsPerFileBlock;
  int8_t   precision;  // time resolution
  int8_t   compression;
  int8_t   cacheLastRow;
  int8_t   update;
  int8_t   quorum;
  int8_t   replica;
  int8_t   selfIndex;
  int8_t   walLevel;
  int32_t  fsyncPeriod;  // millisecond
  SReplica replicas[TSDB_MAX_REPLICA];
} SVnodeCfg;

typedef enum {
  VN_MSG_TYPE_WRITE = 1,
  VN_MSG_TYPE_APPLY,
  VN_MSG_TYPE_SYNC,
  VN_MSG_TYPE_QUERY,
  VN_MSG_TYPE_FETCH
} EVnMsgType;

typedef struct {
  int32_t curNum;
  int32_t allocNum;
  SRpcMsg rpcMsg[];
} SVnodeMsg;

typedef struct {
  void (*SendMsgToDnode)(SEpSet *pEpSet, SRpcMsg *pMsg);
  void (*SendMsgToMnode)(SRpcMsg *pMsg);
  int32_t (*PutMsgIntoApplyQueue)(int32_t vgId, SVnodeMsg *pMsg);
} SVnodePara;

int32_t vnodeInit(SVnodePara);
void    vnodeCleanup();

int32_t vnodeAlter(SVnode *pVnode, const SVnodeCfg *pCfg);
SVnode *vnodeCreate(int32_t vgId, const char *path, const SVnodeCfg *pCfg);
void    vnodeDrop(SVnode *pVnode);
int32_t vnodeCompact(SVnode *pVnode);
int32_t vnodeSync(SVnode *pVnode);

int32_t vnodeGetLoad(SVnode *pVnode, SVnodeLoad *pLoad);

SVnodeMsg *vnodeInitMsg(int32_t msgNum);
int32_t    vnodeAppendMsg(SVnodeMsg *pMsg, SRpcMsg *pRpcMsg);
void       vnodeCleanupMsg(SVnodeMsg *pMsg);
void       vnodeProcessMsg(SVnode *pVnode, SVnodeMsg *pMsg, EVnMsgType msgType);

#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_H_*/
