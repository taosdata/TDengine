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

#include "tmsgcb.h"
#include "trpc.h"


enum {
  NODE_TYPE_VNODE = 1,
  NODE_TYPE_QNODE,
  NODE_TYPE_SNODE,
};



typedef struct SQWorkerCfg {
  uint32_t maxSchedulerNum;
  uint32_t maxTaskNum;
  uint32_t maxSchTaskNum;
} SQWorkerCfg;

typedef struct {
  uint64_t numOfStartTask;
  uint64_t numOfStopTask;
  uint64_t numOfRecvedFetch;
  uint64_t numOfSentHb;
  uint64_t numOfSentFetch;
  uint64_t numOfTaskInQueue;
  uint64_t numOfFetchInQueue;
  uint64_t numOfErrors;
} SQWorkerStat;

int32_t qWorkerInit(int8_t nodeType, int32_t nodeId, SQWorkerCfg *cfg, void **qWorkerMgmt, const SMsgCb *pMsgCb);

int32_t qWorkerProcessQueryMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg);

int32_t qWorkerProcessCQueryMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg);

int32_t qWorkerProcessDataSinkMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg);

int32_t qWorkerProcessReadyMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg);

int32_t qWorkerProcessStatusMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg);

int32_t qWorkerProcessFetchMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg);

int32_t qWorkerProcessFetchRsp(void *node, void *qWorkerMgmt, SRpcMsg *pMsg);

int32_t qWorkerProcessCancelMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg);

int32_t qWorkerProcessDropMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg);

int32_t qWorkerProcessHbMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg);

int32_t qWorkerProcessShowMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg);

int32_t qWorkerProcessShowFetchMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg);

void qWorkerDestroy(void **qWorkerMgmt);

#ifdef __cplusplus
}
#endif

#endif /*_TD_QWORKER_H_*/
