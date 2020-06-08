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

typedef enum _VN_STATUS {
  TAOS_VN_STATUS_INIT,
  TAOS_VN_STATUS_UPDATING,
  TAOS_VN_STATUS_READY,
  TAOS_VN_STATUS_CLOSING,
  TAOS_VN_STATUS_DELETING,
} EVnStatus;

typedef struct {
  int   len;
  int   code;
  void *rsp;
  void *qhandle; //used by query and retrieve msg
} SRspRet;

int32_t vnodeCreate(SMDCreateVnodeMsg *pVnodeCfg);
int32_t vnodeDrop(int32_t vgId);
int32_t vnodeOpen(int32_t vgId, char *rootDir);
int32_t vnodeStartStream(int32_t vgId);
int32_t vnodeAlter(void *pVnode, SMDCreateVnodeMsg *pVnodeCfg);
int32_t vnodeClose(int32_t vgId);

void    vnodeRelease(void *pVnode);
void*   vnodeAccquireVnode(int32_t vgId); // add refcount 
void*   vnodeGetVnode(int32_t vgId);      // keep refcount unchanged

void*   vnodeGetRqueue(void *);
void*   vnodeGetWqueue(int32_t vgId);
void*   vnodeGetWal(void *pVnode);

int32_t vnodeProcessWrite(void *pVnode, int qtype, void *pHead, void *item);
void    vnodeBuildStatusMsg(void * param);

int32_t vnodeProcessRead(void *pVnode, int msgType, void *pCont, int32_t contLen, SRspRet *ret);

#ifdef __cplusplus
}
#endif

#endif
