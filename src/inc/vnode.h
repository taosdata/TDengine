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

#ifndef TDENGINE_VNODE_H
#define TDENGINE_VNODE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "twal.h"

typedef enum _VN_STATUS {
  TAOS_VN_STATUS_INIT,
  TAOS_VN_STATUS_READY,
  TAOS_VN_STATUS_CLOSING,
  TAOS_VN_STATUS_UPDATING,
  TAOS_VN_STATUS_RESET,
} EVnStatus;

typedef struct {
  int32_t len;
  void *  rsp;
  void *  qhandle;  // used by query and retrieve msg
} SRspRet;

typedef struct {
  SRspRet  rspRet;
  void    *pCont;
  int32_t  contLen;
  SRpcMsg  rpcMsg;
} SReadMsg;

typedef struct {
  int32_t   code;
  int32_t   processedCount;
  void *    rpcHandle;
  void *    rpcAhandle;
  SRspRet   rspRet;
  char      reserveForSync[16];
  SWalHead  pHead[];
} SVWriteMsg;

extern char *vnodeStatus[];

int32_t vnodeCreate(SCreateVnodeMsg *pVnodeCfg);
int32_t vnodeDrop(int32_t vgId);
int32_t vnodeOpen(int32_t vgId, char *rootDir);
int32_t vnodeAlter(void *pVnode, SCreateVnodeMsg *pVnodeCfg);
int32_t vnodeClose(int32_t vgId);

void*   vnodeAcquire(int32_t vgId);        // add refcount
void*   vnodeAcquireRqueue(int32_t vgId);  // add refCount, get read queue 
void*   vnodeAcquireWqueue(int32_t vgId);  // add recCount, get write queue
void    vnodeRelease(void *pVnode);        // dec refCount
void*   vnodeGetWal(void *pVnode);


int32_t vnodeWriteToQueue(void *vparam, void *wparam, int32_t qtype, void *pMsg);
int32_t vnodeProcessWrite(void *param, int32_t qtype, SVWriteMsg *pWrite);
int32_t vnodeCheckWrite(void *pVnode);
int32_t vnodeGetVnodeList(int32_t vnodeList[], int32_t *numOfVnodes);
void    vnodeBuildStatusMsg(void *param);
void    vnodeConfirmForward(void *param, uint64_t version, int32_t code);
void    vnodeSetAccess(SVgroupAccess *pAccess, int32_t numOfVnodes);

int32_t vnodeInitResources();
void    vnodeCleanupResources();

int32_t vnodeProcessRead(void *pVnode, SReadMsg *pReadMsg);
int32_t vnodeCheckRead(void *pVnode);

#ifdef __cplusplus
}
#endif

#endif
