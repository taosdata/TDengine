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
#define vFatal(...) do { if (vDebugFlag & DEBUG_FATAL) { taosPrintLog("VND FATAL ", DEBUG_FATAL, 255,        __VA_ARGS__); }} while(0)
#define vError(...) do { if (vDebugFlag & DEBUG_ERROR) { taosPrintLog("VND ERROR ", DEBUG_ERROR, 255,        __VA_ARGS__); }} while(0)
#define vWarn(...)  do { if (vDebugFlag & DEBUG_WARN)  { taosPrintLog("VND WARN  ", DEBUG_WARN,  255,        __VA_ARGS__); }} while(0)
#define vInfo(...)  do { if (vDebugFlag & DEBUG_INFO)  { taosPrintLog("VND INFO  ", DEBUG_INFO,  255,        __VA_ARGS__); }} while(0)
#define vDebug(...) do { if (vDebugFlag & DEBUG_DEBUG) { taosPrintLog("VND DEBUG ", DEBUG_DEBUG, vDebugFlag, __VA_ARGS__); }} while(0)
#define vTrace(...) do { if (vDebugFlag & DEBUG_TRACE) { taosPrintLog("VND TRACE ", DEBUG_TRACE, vDebugFlag, __VA_ARGS__); }} while(0)

#define vGTrace(trace, param, ...) do { if (vDebugFlag & DEBUG_TRACE) { vTrace(param ", QID:0x%" PRIx64 ":0x%" PRIx64, __VA_ARGS__, (trace) ? (trace)->rootId : 0, (trace) ? (trace)->msgId : 0);}} while(0)
#define vGFatal(trace, param, ...) do { if (vDebugFlag & DEBUG_FATAL) { vFatal(param ", QID:0x%" PRIx64 ":0x%" PRIx64, __VA_ARGS__, (trace) ? (trace)->rootId : 0, (trace) ? (trace)->msgId : 0);}} while(0)
#define vGError(trace, param, ...) do { if (vDebugFlag & DEBUG_ERROR) { vError(param ", QID:0x%" PRIx64 ":0x%" PRIx64, __VA_ARGS__, (trace) ? (trace)->rootId : 0, (trace) ? (trace)->msgId : 0);}} while(0)
#define vGWarn(trace, param, ...)  do { if (vDebugFlag & DEBUG_WARN)  { vWarn(param  ", QID:0x%" PRIx64 ":0x%" PRIx64, __VA_ARGS__, (trace) ? (trace)->rootId : 0, (trace) ? (trace)->msgId : 0);}} while(0)
#define vGInfo(trace, param, ...)  do { if (vDebugFlag & DEBUG_INFO)  { vInfo(param  ", QID:0x%" PRIx64 ":0x%" PRIx64, __VA_ARGS__, (trace) ? (trace)->rootId : 0, (trace) ? (trace)->msgId : 0);}} while(0)
#define vGDebug(trace, param, ...) do { if (vDebugFlag & DEBUG_DEBUG) { vDebug(param ", QID:0x%" PRIx64 ":0x%" PRIx64, __VA_ARGS__, (trace) ? (trace)->rootId : 0, (trace) ? (trace)->msgId : 0);}} while(0)

// clang-format on

// vnodeCfg.c
extern const SVnodeCfg vnodeCfgDefault;

int32_t vnodeCheckCfg(const SVnodeCfg*);
int32_t vnodeEncodeConfig(const void* pObj, SJson* pJson);
int32_t vnodeDecodeConfig(const SJson* pJson, void* pObj);

// vnodeAsync.c
typedef enum {
  EVA_PRIORITY_HIGH = 0,
  EVA_PRIORITY_NORMAL,
  EVA_PRIORITY_LOW,
} EVAPriority;

typedef enum {
  EVA_TASK_COMMIT = 1,
  EVA_TASK_MERGE,
  EVA_TASK_COMPACT,
  EVA_TASK_RETENTION,
} EVATaskT;

#define COMMIT_TASK_ASYNC    1
#define MERGE_TASK_ASYNC     2
#define COMPACT_TASK_ASYNC   3
#define RETENTION_TASK_ASYNC 4
#define SCAN_TASK_ASYNC      5

int32_t vnodeAsyncOpen();
void    vnodeAsyncClose();
int32_t vnodeAChannelInit(int64_t async, SVAChannelID* channelID);
int32_t vnodeAChannelDestroy(SVAChannelID* channelID, bool waitRunning);
int32_t vnodeAsync(int64_t async, EVAPriority priority, int32_t (*execute)(void*), void (*complete)(void*), void* arg,
                   SVATaskID* taskID);
int32_t vnodeAsyncC(SVAChannelID* channelID, EVAPriority priority, int32_t (*execute)(void*), void (*complete)(void*),
                    void* arg, SVATaskID* taskID);
void    vnodeAWait(SVATaskID* taskID);
int32_t vnodeACancel(SVATaskID* taskID);
int32_t vnodeAsyncSetWorkers(int64_t async, int32_t numWorkers);
bool    vnodeATaskValid(SVATaskID* taskID);

const char* vnodeGetATaskName(EVATaskT task);

// vnodeBufPool.c
typedef struct SVBufPoolNode SVBufPoolNode;
struct SVBufPoolNode {
  SVBufPoolNode*  prev;
  SVBufPoolNode** pnext;
  int64_t         size;
  uint8_t*        data;
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
  int64_t           size;
  uint8_t*          ptr;
  SVBufPoolNode*    pTail;
  SVBufPoolNode     node;
};

int32_t vnodeOpenBufPool(SVnode* pVnode);
void    vnodeCloseBufPool(SVnode* pVnode);
void    vnodeBufPoolReset(SVBufPool* pPool);
void    vnodeBufPoolAddToFreeList(SVBufPool* pPool);
int32_t vnodeBufPoolRecycle(SVBufPool* pPool);

// vnodeOpen.c
void vnodeGetPrimaryDir(const char* relPath, int32_t diskPrimary, STfs* pTfs, char* buf, size_t bufLen);
void vnodeGetPrimaryPath(SVnode* pVnode, bool mount, char* buf, size_t bufLen);

// vnodeQuery.c
int32_t vnodeQueryOpen(SVnode* pVnode);
void    vnodeQueryPreClose(SVnode* pVnode);
void    vnodeQueryClose(SVnode* pVnode);
int32_t vnodeGetTableMeta(SVnode* pVnode, SRpcMsg* pMsg, bool direct);
int     vnodeGetTableCfg(SVnode* pVnode, SRpcMsg* pMsg, bool direct);
int32_t vnodeGetBatchMeta(SVnode* pVnode, SRpcMsg* pMsg);
int32_t vnodeGetVSubtablesMeta(SVnode *pVnode, SRpcMsg *pMsg);
int32_t vnodeGetVStbRefDbs(SVnode *pVnode, SRpcMsg *pMsg);

// vnodeCommit.c
int32_t vnodeBegin(SVnode* pVnode);
int32_t vnodeShouldCommit(SVnode* pVnode, bool atExit);
void    vnodeRollback(SVnode* pVnode);
int32_t vnodeSaveInfo(const char* dir, const SVnodeInfo* pCfg);
int32_t vnodeCommitInfo(const char* dir);
int32_t vnodeLoadInfo(const char* dir, SVnodeInfo* pInfo);
int32_t vnodeSyncCommit(SVnode* pVnode);
int32_t vnodeAsyncCommit(SVnode* pVnode);
int32_t vnodeAsyncCommitEx(SVnode* pVnode, bool forceTrim);
bool    vnodeShouldRollback(SVnode* pVnode);

// vnodeSync.c
int64_t vnodeClusterId(SVnode* pVnode);
int32_t vnodeNodeId(SVnode* pVnode);
int32_t vnodeSyncOpen(SVnode* pVnode, char* path, int32_t vnodeVersion);
int32_t vnodeSyncStart(SVnode* pVnode);
void    vnodeSyncPreClose(SVnode* pVnode);
void    vnodeSyncPostClose(SVnode* pVnode);
void    vnodeSyncClose(SVnode* pVnode);
void    vnodeRedirectRpcMsg(SVnode* pVnode, SRpcMsg* pMsg, int32_t code);
bool    vnodeIsLeader(SVnode* pVnode);
bool    vnodeIsRoleLeader(SVnode* pVnode);
int32_t    vnodeSetElectBaseline(SVnode* pVnode, int32_t ms);

#ifdef __cplusplus
}
#endif

#endif /*_TD_VND_H_*/
