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

#ifndef _TD_QWORKER_MSG_H_
#define _TD_QWORKER_MSG_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "qworkerInt.h"
#include "dataSinkMgt.h"

int32_t qwProcessQuery(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, SQWMsg *qwMsg, int8_t taskType);
int32_t qwProcessCQuery(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, SQWMsg *qwMsg);
int32_t qwProcessReady(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, SQWMsg *qwMsg);
int32_t qwProcessFetch(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, SQWMsg *qwMsg);
int32_t qwProcessDrop(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, SQWMsg *qwMsg);

int32_t qwBuildAndSendDropRsp(void *connection, int32_t code);
int32_t qwBuildAndSendCancelRsp(SRpcMsg *pMsg, int32_t code);
int32_t qwBuildAndSendFetchRsp(void *connection, SRetrieveTableRsp *pRsp, int32_t dataLength, int32_t code);
void qwBuildFetchRsp(void *msg, SOutputData *input, int32_t len, bool qComplete);
int32_t qwBuildAndSendCQueryMsg(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, void *connection);
int32_t qwBuildAndSendSchSinkMsg(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, void *connection);
int32_t qwBuildAndSendReadyRsp(void *connection, int32_t code);
int32_t qwBuildAndSendQueryRsp(void *connection, int32_t code);
void qwFreeFetchRsp(void *msg);
int32_t qwMallocFetchRsp(int32_t length, SRetrieveTableRsp **rsp);



#ifdef __cplusplus
}
#endif

#endif /*_TD_QWORKER_INT_H_*/
