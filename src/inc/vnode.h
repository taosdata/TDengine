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

typedef struct {
  int   len;
  void *rsp;
} SRspRet;

int32_t vnodeCreate(SMDCreateVnodeMsg *pVnodeCfg);
int32_t vnodeDrop(int32_t vgId);
int32_t vnodeOpen(int32_t vnode, char *rootDir);
int32_t vnodeClose(void *pVnode);

void    vnodeRelease(void *pVnode);
void*   vnodeGetVnode(int32_t vgId);

void*   vnodeGetRqueue(void *);
void*   vnodeGetWqueue(int32_t vgId);
void*   vnodeGetWal(void *pVnode);
void*   vnodeGetTsdb(void *pVnode);

int32_t vnodeProcessWrite(void *pVnode, int qtype, SWalHead *pHead, void *item);

#ifdef __cplusplus
}
#endif

#endif
