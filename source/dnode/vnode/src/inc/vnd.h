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
#include "ttrace.h"
#include "vnodeInt.h"

#ifdef __cplusplus
extern "C" {
#endif

// clang-format off
#define vFatal(...) do { if (vDebugFlag & DEBUG_FATAL) { taosPrintLog("VND FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }}     while(0)
#define vError(...) do { if (vDebugFlag & DEBUG_ERROR) { taosPrintLog("VND ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }}     while(0)
#define vWarn(...)  do { if (vDebugFlag & DEBUG_WARN)  { taosPrintLog("VND WARN ", DEBUG_WARN, 255, __VA_ARGS__); }}       while(0)
#define vInfo(...)  do { if (vDebugFlag & DEBUG_INFO)  { taosPrintLog("VND ", DEBUG_INFO, 255, __VA_ARGS__); }}            while(0)
#define vDebug(...) do { if (vDebugFlag & DEBUG_DEBUG) { taosPrintLog("VND ", DEBUG_DEBUG, vDebugFlag, __VA_ARGS__); }}    while(0)
#define vTrace(...) do { if (vDebugFlag & DEBUG_TRACE) { taosPrintLog("VND ", DEBUG_TRACE, vDebugFlag, __VA_ARGS__); }}    while(0)

#define vGTrace(param, ...) do { if (vDebugFlag & DEBUG_TRACE) { char buf[40] = {0}; TRACE_TO_STR(trace, buf); vTrace(param ", gtid:%s", __VA_ARGS__, buf);}} while(0)
#define vGFatal(param, ...) do { if (vDebugFlag & DEBUG_FATAL) { char buf[40] = {0}; TRACE_TO_STR(trace, buf); vFatal(param ", gtid:%s", __VA_ARGS__, buf);}} while(0)
#define vGError(param, ...) do { if (vDebugFlag & DEBUG_ERROR) { char buf[40] = {0}; TRACE_TO_STR(trace, buf); vError(param ", gtid:%s", __VA_ARGS__, buf);}} while(0)
#define vGWarn(param, ...)  do { if (vDebugFlag & DEBUG_WARN)  { char buf[40] = {0}; TRACE_TO_STR(trace, buf); vWarn(param ", gtid:%s", __VA_ARGS__, buf);}} while(0)
#define vGInfo(param, ...)  do { if (vDebugFlag & DEBUG_INFO)  { char buf[40] = {0}; TRACE_TO_STR(trace, buf); vInfo(param ", gtid:%s", __VA_ARGS__, buf);}} while(0)
#define vGDebug(param, ...) do { if (vDebugFlag & DEBUG_DEBUG) { char buf[40] = {0}; TRACE_TO_STR(trace, buf); vDebug(param ", gtid:%s", __VA_ARGS__, buf);}}    while(0)

// clang-format on

// vnodeCfg.c
extern const SVnodeCfg vnodeCfgDefault;

int32_t vnodeCheckCfg(const SVnodeCfg*);
int32_t vnodeEncodeConfig(const void* pObj, SJson* pJson);
int32_t vnodeDecodeConfig(const SJson* pJson, void* pObj);

// vnodeAsync.c
typedef struct SVAsync SVAsync;

typedef enum {
  EVA_PRIORITY_HIGH = 0,
  EVA_PRIORITY_NORMAL,
  EVA_PRIORITY_LOW,
} EVAPriority;

#define VNODE_ASYNC_VALID_CHANNEL_ID(channelId) ((channelId) > 0)
#define VNODE_ASYNC_VALID_TASK_ID(taskId)       ((taskId) > 0)

int32_t vnodeAsyncInit(SVAsync** async, char* label);
int32_t vnodeAsyncDestroy(SVAsync** async);
int32_t vnodeAChannelInit(SVAsync* async, int64_t* channelId);
int32_t vnodeAChannelDestroy(SVAsync* async, int64_t channelId, bool waitRunning);
int32_t vnodeAsync(SVAsync* async, EVAPriority priority, int32_t (*execute)(void*), void (*complete)(void*), void* arg,
                   int64_t* taskId);
int32_t vnodeAsyncC(SVAsync* async, int64_t channelId, EVAPriority priority, int32_t (*execute)(void*),
                    void (*complete)(void*), void* arg, int64_t* taskId);
int32_t vnodeAWait(SVAsync* async, int64_t taskId);
int32_t vnodeACancel(SVAsync* async, int64_t taskId);
int32_t vnodeAsyncSetWorkers(SVAsync* async, int32_t numWorkers);

// vnodeModule.c
extern SVAsync* vnodeAsyncHandle[2];

// vnodeBufPool.c
typedef struct SVBufPoolNode SVBufPoolNode;
struct SVBufPoolNode {
  SVBufPoolNode*  prev;
  SVBufPoolNode** pnext;
  int64_t         size;
  uint8_t         data[];
};

struct SVBufPool {
  SVBufPool* freeNext;
  SVBufPool* recycleNext;
  SVBufPool* recyclePrev;

  // query handle list
  TdThreadMutex mutex;
  int32_t       nQuery;
  SQueryNode    qList;

  SVnode*           pVnode;
  int32_t           id;
  volatile int32_t  nRef;
  TdThreadSpinlock* lock;
  int64_t           size;
  uint8_t*          ptr;
  SVBufPoolNode*    pTail;
  SVBufPoolNode     node;
};

int32_t vnodeOpenBufPool(SVnode* pVnode);
int32_t vnodeCloseBufPool(SVnode* pVnode);
void    vnodeBufPoolReset(SVBufPool* pPool);
void    vnodeBufPoolAddToFreeList(SVBufPool* pPool);
int32_t vnodeBufPoolRecycle(SVBufPool* pPool);

// vnodeOpen.c
int32_t vnodeGetPrimaryDir(const char* relPath, int32_t diskPrimary, STfs* pTfs, char* buf, size_t bufLen);

// vnodeQuery.c
int32_t vnodeQueryOpen(SVnode* pVnode);
void    vnodeQueryPreClose(SVnode* pVnode);
void    vnodeQueryClose(SVnode* pVnode);
int32_t vnodeGetTableMeta(SVnode* pVnode, SRpcMsg* pMsg, bool direct);
int     vnodeGetTableCfg(SVnode* pVnode, SRpcMsg* pMsg, bool direct);
int32_t vnodeGetBatchMeta(SVnode* pVnode, SRpcMsg* pMsg);

// vnodeCommit.c
int32_t vnodeBegin(SVnode* pVnode);
int32_t vnodeShouldCommit(SVnode* pVnode, bool atExit);
void    vnodeRollback(SVnode* pVnode);
int32_t vnodeSaveInfo(const char* dir, const SVnodeInfo* pCfg);
int32_t vnodeCommitInfo(const char* dir);
int32_t vnodeLoadInfo(const char* dir, SVnodeInfo* pInfo);
int32_t vnodeSyncCommit(SVnode* pVnode);
int32_t vnodeAsyncCommit(SVnode* pVnode);
bool    vnodeShouldRollback(SVnode* pVnode);

// vnodeSync.c
int32_t vnodeSyncOpen(SVnode* pVnode, char* path, int32_t vnodeVersion);
int32_t vnodeSyncStart(SVnode* pVnode);
void    vnodeSyncPreClose(SVnode* pVnode);
void    vnodeSyncPostClose(SVnode* pVnode);
void    vnodeSyncClose(SVnode* pVnode);
void    vnodeRedirectRpcMsg(SVnode* pVnode, SRpcMsg* pMsg, int32_t code);
bool    vnodeIsLeader(SVnode* pVnode);
bool    vnodeIsRoleLeader(SVnode* pVnode);

#ifdef __cplusplus
}
#endif

#endif /*_TD_VND_H_*/
