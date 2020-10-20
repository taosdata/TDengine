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

#ifndef TDENGINE_DNODE_H
#define TDENGINE_DNODE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "hash.h"
#include "taoserror.h"
#include "trpc.h"

typedef struct {
  int32_t queryReqNum;
  int32_t submitReqNum;
  int32_t httpReqNum;
} SDnodeStatisInfo;

typedef enum {
  TSDB_DNODE_RUN_STATUS_INITIALIZE,
  TSDB_DNODE_RUN_STATUS_RUNING,
  TSDB_DNODE_RUN_STATUS_STOPPED
} SDnodeRunStatus;

SDnodeRunStatus dnodeGetRunStatus();
SDnodeStatisInfo dnodeGetStatisInfo();

bool    dnodeIsFirstDeploy();
char *  dnodeGetMnodeMasterEp();
void    dnodeGetMnodeEpSetForPeer(void *epSet);
void    dnodeGetMnodeEpSetForShell(void *epSet);
void *  dnodeGetMnodeInfos();
int32_t dnodeGetDnodeId();
bool    dnodeStartMnode(void *pModes);

void  dnodeAddClientRspHandle(uint8_t msgType, void (*fp)(SRpcMsg *rpcMsg));
void  dnodeSendMsgToDnode(SRpcEpSet *epSet, SRpcMsg *rpcMsg);
void  dnodeSendMsgToMnodeRecv(SRpcMsg *rpcMsg, SRpcMsg *rpcRsp);
void  dnodeSendMsgToDnodeRecv(SRpcMsg *rpcMsg, SRpcMsg *rpcRsp, SRpcEpSet *epSet);
void *dnodeSendCfgTableToRecv(int32_t vgId, int32_t sid);

void *dnodeAllocateVnodeWqueue(void *pVnode);
void  dnodeFreeVnodeWqueue(void *queue);
void *dnodeAllocateVnodeRqueue(void *pVnode);
void  dnodeFreeVnodeRqueue(void *rqueue);
void  dnodeSendRpcVnodeWriteRsp(void *pVnode, void *param, int32_t code);

int32_t dnodeAllocateMnodePqueue();
void    dnodeFreeMnodePqueue();
int32_t dnodeAllocateMnodeRqueue();
void    dnodeFreeMnodeRqueue();
int32_t dnodeAllocateMnodeWqueue();
void    dnodeFreeMnodeWqueue();
void    dnodeSendRpcMnodeWriteRsp(void *pMsg, int32_t code);
void    dnodeReprocessMnodeWriteMsg(void *pMsg);
void    dnodeDelayReprocessMnodeWriteMsg(void *pMsg);

void    dnodeSendStatusMsgToMnode();

// DNODE TIER
#define DNODE_MAX_TIERS 3
#define DNODE_MAX_DISKS_PER_TIER 16

typedef struct {
  int level;
  int did;
} SDiskID;

typedef struct {
  uint64_t size;
  uint64_t free;
  uint64_t nfiles;
} SDiskMeta;

typedef struct {
  char      dir[TSDB_FILENAME_LEN];
  SDiskMeta dmeta;
} SDisk;

typedef struct {
  int   level;
  int   nDisks;
  SDisk disks[DNODE_MAX_DISKS_PER_TIER];
} STier;

typedef struct SDnodeTier {
  pthread_rwlock_t rwlock;
  int              nTiers;
  STier            tiers[DNODE_MAX_TIERS];
  SHashObj *       map;
} SDnodeTier;

#define DNODE_PRIMARY_DISK(pDnodeTier) (&(pDnodeTier)->tiers[0].disks[0])

static FORCE_INLINE int dnodeRLockTiers(SDnodeTier *pDnodeTier) {
  int code = pthread_rwlock_rdlock(&(pDnodeTier->rwlock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

static FORCE_INLINE int dnodeWLockTiers(SDnodeTier *pDnodeTier) {
  int code = pthread_rwlock_wrlock(&(pDnodeTier->rwlock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

static FORCE_INLINE int dnodeUnLockTiers(SDnodeTier *pDnodeTier) {
  int code = pthread_rwlock_unlock(&(pDnodeTier->rwlock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

static FORCE_INLINE SDisk *dnodeGetDisk(SDnodeTier *pDnodeTier, int level, int did) {
  if (level < 0 || level >= pDnodeTier->nTiers) return NULL;

  if (did < 0 || did >= pDnodeTier->tiers[level].nDisks) return NULL;

  return &(pDnodeTier->tiers[level].disks[did]);
}

SDnodeTier *dnodeNewTier();
void *      dnodeCloseTier(SDnodeTier *pDnodeTier);
int         dnodeAddDisk(SDnodeTier *pDnodeTier, char *dir, int level);
int         dnodeUpdateTiersInfo(SDnodeTier *pDnodeTier);
int         dnodeCheckTiers(SDnodeTier *pDnodeTier);
SDisk *     dnodeAssignDisk(SDnodeTier *pDnodeTier, int level);
SDisk *     dnodeGetDiskByName(SDnodeTier *pDnodeTier, char *dirName);


#ifdef __cplusplus
}
#endif

#endif
