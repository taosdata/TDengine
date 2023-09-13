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

#include "dataSinkMgt.h"
#include "qwInt.h"

int32_t qwAbortPrerocessQuery(QW_FPARAMS_DEF);
int32_t qwPreprocessQuery(QW_FPARAMS_DEF, SQWMsg *qwMsg);
int32_t qwProcessQuery(QW_FPARAMS_DEF, SQWMsg *qwMsg, char *sql);
int32_t qwProcessCQuery(QW_FPARAMS_DEF, SQWMsg *qwMsg);
int32_t qwProcessReady(QW_FPARAMS_DEF, SQWMsg *qwMsg);
int32_t qwProcessFetch(QW_FPARAMS_DEF, SQWMsg *qwMsg);
int32_t qwProcessDrop(QW_FPARAMS_DEF, SQWMsg *qwMsg);
int32_t qwProcessNotify(QW_FPARAMS_DEF, SQWMsg *qwMsg);
int32_t qwProcessHb(SQWorker *mgmt, SQWMsg *qwMsg, SSchedulerHbReq *req);
int32_t qwProcessDelete(QW_FPARAMS_DEF, SQWMsg *qwMsg, SDeleteRes *pRes);

int32_t qwBuildAndSendDropRsp(SRpcHandleInfo *pConn, int32_t code);
int32_t qwBuildAndSendCancelRsp(SRpcHandleInfo *pConn, int32_t code);
int32_t qwBuildAndSendFetchRsp(int32_t rspType, SRpcHandleInfo *pConn, SRetrieveTableRsp *pRsp, int32_t dataLength,
                               int32_t code);
void    qwBuildFetchRsp(void *msg, SOutputData *input, int32_t len, bool qComplete);
int32_t qwBuildAndSendCQueryMsg(QW_FPARAMS_DEF, SRpcHandleInfo *pConn);
int32_t qwBuildAndSendQueryRsp(int32_t rspType, SRpcHandleInfo *pConn, int32_t code, SQWTaskCtx *ctx);
int32_t qwBuildAndSendExplainRsp(SRpcHandleInfo *pConn, SArray *pExecList);
int32_t qwBuildAndSendErrorRsp(int32_t rspType, SRpcHandleInfo *pConn, int32_t code);
void    qwFreeFetchRsp(void *msg);
int32_t qwMallocFetchRsp(int8_t rpcMalloc, int32_t length, SRetrieveTableRsp **rsp);
int32_t qwBuildAndSendHbRsp(SRpcHandleInfo *pConn, SSchedulerHbRsp *rsp, int32_t code);
int32_t qwRegisterQueryBrokenLinkArg(QW_FPARAMS_DEF, SRpcHandleInfo *pConn);
int32_t qwRegisterHbBrokenLinkArg(SQWorker *mgmt, uint64_t sId, SRpcHandleInfo *pConn);
int32_t qwBuildAndSendDropMsg(QW_FPARAMS_DEF, SRpcHandleInfo *pConn);

#ifdef __cplusplus
}
#endif

#endif /*_TD_QWORKER_INT_H_*/
