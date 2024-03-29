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

#ifndef _TD_QWORKER_H_
#define _TD_QWORKER_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "executor.h"
#include "tmsgcb.h"
#include "trpc.h"

enum {
  NODE_TYPE_VNODE = 1,
  NODE_TYPE_QNODE,
  NODE_TYPE_SNODE,
  NODE_TYPE_MNODE,
  NODE_TYPE_CLIENT,
};

typedef struct SQWorkerCfg {
  uint32_t maxSchedulerNum;
  uint32_t maxTaskNum;
  uint32_t maxSchTaskNum;
} SQWorkerCfg;

typedef struct {
  uint64_t cacheDataSize;

  uint64_t queryProcessed;
  uint64_t cqueryProcessed;
  uint64_t fetchProcessed;
  uint64_t dropProcessed;
  uint64_t notifyProcessed;
  uint64_t hbProcessed;
  uint64_t deleteProcessed;

  uint64_t numOfQueryInQueue;
  uint64_t numOfFetchInQueue;
  uint64_t timeInQueryQueue;
  uint64_t timeInFetchQueue;

  uint64_t numOfErrors;
} SQWorkerStat;

typedef struct SQWMsgInfo {
  int8_t taskType;
  int8_t explain;
  int8_t needFetch;
} SQWMsgInfo;

typedef struct SQWMsg {
  void          *node;
  int32_t        code;
  int32_t        msgType;
  void          *msg;
  int32_t        msgLen;
  SQWMsgInfo     msgInfo;
  SRpcHandleInfo connInfo;
} SQWMsg;

int32_t qWorkerInit(int8_t nodeType, int32_t nodeId, void **qWorkerMgmt, const SMsgCb *pMsgCb);

int32_t qWorkerAbortPreprocessQueryMsg(void *qWorkerMgmt, SRpcMsg *pMsg);

int32_t qWorkerPreprocessQueryMsg(void *qWorkerMgmt, SRpcMsg *pMsg, bool chkGrant);

int32_t qWorkerProcessQueryMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg, int64_t ts);

int32_t qWorkerProcessCQueryMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg, int64_t ts);

int32_t qWorkerProcessFetchMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg, int64_t ts);

int32_t qWorkerProcessRspMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg, int64_t ts);

int32_t qWorkerProcessCancelMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg, int64_t ts);

int32_t qWorkerProcessDropMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg, int64_t ts);

int32_t qWorkerProcessNotifyMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg, int64_t ts);

int32_t qWorkerProcessHbMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg, int64_t ts);

int32_t qWorkerProcessDeleteMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg, SDeleteRes *pRes);

void qWorkerStopAllTasks(void *qWorkerMgmt);

void qWorkerDestroy(void **qWorkerMgmt);

int32_t qWorkerGetStat(SReadHandle *handle, void *qWorkerMgmt, SQWorkerStat *pStat);

int32_t qWorkerProcessLocalQuery(void *pMgmt, uint64_t sId, uint64_t qId, uint64_t tId, int64_t rId, int32_t eId,
                                 SQWMsg *qwMsg, SArray *explainRes);

int32_t qWorkerProcessLocalFetch(void *pMgmt, uint64_t sId, uint64_t qId, uint64_t tId, int64_t rId, int32_t eId,
                                 void **pRsp, SArray *explainRes);

int32_t qWorkerDbgEnableDebug(char *option);

#ifdef __cplusplus
}
#endif

#endif /*_TD_QWORKER_H_*/
