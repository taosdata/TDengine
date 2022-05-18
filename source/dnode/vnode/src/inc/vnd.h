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

#ifndef _TD_VND_H_
#define _TD_VND_H_

#include "sync.h"
#include "syncTools.h"
#include "vnodeInt.h"

#ifdef __cplusplus
extern "C" {
#endif

// vnodeDebug ====================
// clang-format off
#define vFatal(...) do { if (vDebugFlag & DEBUG_FATAL) { taosPrintLog("VND FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }}     while(0)
#define vError(...) do { if (vDebugFlag & DEBUG_ERROR) { taosPrintLog("VND ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }}     while(0)
#define vWarn(...)  do { if (vDebugFlag & DEBUG_WARN)  { taosPrintLog("VND WARN ", DEBUG_WARN, 255, __VA_ARGS__); }}       while(0)
#define vInfo(...)  do { if (vDebugFlag & DEBUG_INFO)  { taosPrintLog("VND ", DEBUG_INFO, 255, __VA_ARGS__); }}            while(0)
#define vDebug(...) do { if (vDebugFlag & DEBUG_DEBUG) { taosPrintLog("VND ", DEBUG_DEBUG, vDebugFlag, __VA_ARGS__); }}    while(0)
#define vTrace(...) do { if (vDebugFlag & DEBUG_TRACE) { taosPrintLog("VND ", DEBUG_TRACE, vDebugFlag, __VA_ARGS__); }}    while(0)
// clang-format on

// vnodeCfg ====================
extern const SVnodeCfg vnodeCfgDefault;

int vnodeCheckCfg(const SVnodeCfg*);
int vnodeEncodeConfig(const void* pObj, SJson* pJson);
int vnodeDecodeConfig(const SJson* pJson, void* pObj);

// vnodeModule ====================
int vnodeScheduleTask(int (*execute)(void*), void* arg);

// vnodeBufPool ====================
typedef struct SVBufPoolNode SVBufPoolNode;
struct SVBufPoolNode {
  SVBufPoolNode*  prev;
  SVBufPoolNode** pnext;
  int64_t         size;
  uint8_t         data[];
};

struct SVBufPool {
  SVBufPool*     next;
  int64_t        nRef;
  int64_t        size;
  uint8_t*       ptr;
  SVBufPoolNode* pTail;
  SVBufPoolNode  node;
};

int  vnodeOpenBufPool(SVnode* pVnode, int64_t size);
int  vnodeCloseBufPool(SVnode* pVnode);
void vnodeBufPoolReset(SVBufPool* pPool);

// vnodeQuery ====================
int  vnodeQueryOpen(SVnode* pVnode);
void vnodeQueryClose(SVnode* pVnode);
int  vnodeGetTableMeta(SVnode* pVnode, SRpcMsg* pMsg);

// vnodeCommit ====================
int vnodeBegin(SVnode* pVnode);
int vnodeShouldCommit(SVnode* pVnode);
int vnodeCommit(SVnode* pVnode);
int vnodeSaveInfo(const char* dir, const SVnodeInfo* pCfg);
int vnodeCommitInfo(const char* dir, const SVnodeInfo* pInfo);
int vnodeLoadInfo(const char* dir, SVnodeInfo* pInfo);
int vnodeSyncCommit(SVnode* pVnode);
int vnodeAsyncCommit(SVnode* pVnode);

// vnodeCommit ====================
int32_t   vnodeSyncOpen(SVnode* pVnode, char* path);
int32_t   vnodeSyncStart(SVnode* pVnode);
void      vnodeSyncClose(SVnode* pVnode);
void      vnodeSyncSetQ(SVnode* pVnode, void* qHandle);
void      vnodeSyncSetRpc(SVnode* pVnode, void* rpcHandle);
int32_t   vnodeSyncEqMsg(void* qHandle, SRpcMsg* pMsg);
int32_t   vnodeSendMsg(void* rpcHandle, const SEpSet* pEpSet, SRpcMsg* pMsg);
void      vnodeSyncCommitCb(struct SSyncFSM* pFsm, const SRpcMsg* pMsg, SFsmCbMeta cbMeta);
void      vnodeSyncPreCommitCb(struct SSyncFSM* pFsm, const SRpcMsg* pMsg, SFsmCbMeta cbMeta);
void      vnodeSyncRollBackCb(struct SSyncFSM* pFsm, const SRpcMsg* pMsg, SFsmCbMeta cbMeta);
int32_t   vnodeSyncGetSnapshotCb(struct SSyncFSM* pFsm, SSnapshot* pSnapshot);
SSyncFSM* syncVnodeMakeFsm();

#ifdef __cplusplus
}
#endif

#endif /*_TD_VND_H_*/