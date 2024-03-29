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

#ifndef TDENGINE_TQ_COMMON_H
#define TDENGINE_TQ_COMMON_H

// message process
int32_t tqStreamTaskStartAsync(SStreamMeta* pMeta, SMsgCb* cb, bool restart);
int32_t tqStreamStartOneTaskAsync(SStreamMeta* pMeta, SMsgCb* cb, int64_t streamId, int32_t taskId);
int32_t tqStreamTaskProcessUpdateReq(SStreamMeta* pMeta, SMsgCb* cb, SRpcMsg* pMsg, bool restored);
int32_t tqStreamTaskProcessDispatchReq(SStreamMeta* pMeta, SRpcMsg* pMsg);
int32_t tqStreamTaskProcessDispatchRsp(SStreamMeta* pMeta, SRpcMsg* pMsg);
int32_t tqStreamTaskProcessRetrieveReq(SStreamMeta* pMeta, SRpcMsg* pMsg);
int32_t tqStreamTaskProcessCheckReq(SStreamMeta* pMeta, SRpcMsg* pMsg);
int32_t tqStreamTaskProcessCheckRsp(SStreamMeta* pMeta, SRpcMsg* pMsg, bool isLeader);
int32_t tqStreamTaskProcessCheckpointReadyMsg(SStreamMeta* pMeta, SRpcMsg* pMsg);
int32_t tqStreamProcessStreamHbRsp(SStreamMeta* pMeta, SRpcMsg* pMsg);
int32_t tqStreamProcessReqCheckpointRsp(SStreamMeta* pMeta, SRpcMsg* pMsg);
int32_t tqStreamProcessCheckpointReadyRsp(SStreamMeta* pMeta, SRpcMsg* pMsg);
int32_t tqStreamTaskProcessDeployReq(SStreamMeta* pMeta, SMsgCb* cb, int64_t sversion, char* msg, int32_t msgLen,
                                     bool isLeader, bool restored);
int32_t tqStreamTaskProcessDropReq(SStreamMeta* pMeta, char* msg, int32_t msgLen);
int32_t tqStreamTaskProcessRunReq(SStreamMeta* pMeta, SRpcMsg* pMsg, bool isLeader);
int32_t tqStreamTaskResetStatus(SStreamMeta* pMeta);
int32_t tqStartTaskCompleteCallback(SStreamMeta* pMeta);
int32_t tqStreamTasksGetTotalNum(SStreamMeta* pMeta);
int32_t tqStreamTaskProcessTaskResetReq(SStreamMeta* pMeta, SRpcMsg* pMsg);
int32_t tqStreamTaskProcessTaskPauseReq(SStreamMeta* pMeta, char* pMsg);
int32_t tqStreamTaskProcessTaskResumeReq(void* handle, int64_t sversion, char* pMsg, bool fromVnode);

#endif  // TDENGINE_TQ_COMMON_H
