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
#if 1
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

int   vnodeOpenBufPool(SVnode* pVnode, int64_t size);
int   vnodeCloseBufPool(SVnode* pVnode);
void  vnodeBufPoolReset(SVBufPool* pPool);
void* vnodeBufPoolMalloc(SVBufPool* pPool, int size);
void  vnodeBufPoolFree(SVBufPool* pPool, void* p);
#else
// SVBufPool
int   vnodeOpenBufPool(SVnode* pVnode);
void  vnodeCloseBufPool(SVnode* pVnode);
int   vnodeBufPoolSwitch(SVnode* pVnode);
int   vnodeBufPoolRecycle(SVnode* pVnode);
void* vnodeMalloc(SVnode* pVnode, uint64_t size);
bool  vnodeBufPoolIsFull(SVnode* pVnode);

SMemAllocatorFactory* vBufPoolGetMAF(SVnode* pVnode);

// SVMemAllocator
typedef struct SVArenaNode {
  TD_SLIST_NODE(SVArenaNode);
  uint64_t size;  // current node size
  void*    ptr;
  char     data[];
} SVArenaNode;

typedef struct SVMemAllocator {
  T_REF_DECLARE()
  TD_DLIST_NODE(SVMemAllocator);
  uint64_t     capacity;
  uint64_t     ssize;
  uint64_t     lsize;
  SVArenaNode* pNode;
  TD_SLIST(SVArenaNode) nlist;
} SVMemAllocator;

SVMemAllocator* vmaCreate(uint64_t capacity, uint64_t ssize, uint64_t lsize);
void            vmaDestroy(SVMemAllocator* pVMA);
void            vmaReset(SVMemAllocator* pVMA);
void*           vmaMalloc(SVMemAllocator* pVMA, uint64_t size);
void            vmaFree(SVMemAllocator* pVMA, void* ptr);
bool            vmaIsFull(SVMemAllocator* pVMA);
#endif

// vnodeQuery ====================
int  vnodeQueryOpen(SVnode* pVnode);
void vnodeQueryClose(SVnode* pVnode);
int  vnodeGetTableMeta(SVnode* pVnode, SRpcMsg* pMsg);

// vnodeCommit ====================
int vnodeBegin(SVnode* pVnode);
int vnodeSaveInfo(const char* dir, const SVnodeInfo* pCfg);
int vnodeCommitInfo(const char* dir, const SVnodeInfo* pInfo);
int vnodeLoadInfo(const char* dir, SVnodeInfo* pInfo);
int vnodeSyncCommit(SVnode* pVnode);
int vnodeAsyncCommit(SVnode* pVnode);

#ifdef __cplusplus
}
#endif

#endif /*_TD_VND_H_*/