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
#ifndef TDENGINE_QUERY_H
#define TDENGINE_QUERY_H

#ifdef __cplusplus
extern "C" {
#endif

typedef void* qinfo_t;

/**
 * create the qinfo object according to QueryTableMsg
 * @param tsdb
 * @param pQueryTableMsg
 * @param qinfo
 * @return
 */
int32_t qCreateQueryInfo(void* tsdb, int32_t vgId, SQueryTableMsg* pQueryTableMsg, qinfo_t* qinfo, uint64_t *qId);


/**
 * the main query execution function, including query on both table and multitables,
 * which are decided according to the tag or table name query conditions
 *
 * @param qinfo
 * @return
 */
bool qTableQuery(qinfo_t qinfo);

/**
 * Retrieve the produced results information, if current query is not paused or completed,
 * this function will be blocked to wait for the query execution completed or paused,
 * in which case enough results have been produced already.
 *
 * @param qinfo
 * @return
 */
int32_t qRetrieveQueryResultInfo(qinfo_t qinfo, bool* buildRes, void* pRspContext);

/**
 *
 * Retrieve the actual results to fill the response message payload.
 * Note that this function must be executed after qRetrieveQueryResultInfo is invoked.
 *
 * @param qinfo  qinfo object
 * @param pRsp    response message
 * @param contLen payload length
 * @return
 */
int32_t qDumpRetrieveResult(qinfo_t qinfo, SRetrieveTableRsp** pRsp, int32_t* contLen, bool* continueExec);

/**
 *
 * @param qinfo
 * @return
 */
void* qGetResultRetrieveMsg(qinfo_t qinfo);

/**
 * kill current ongoing query and free query handle automatically
 * @param qinfo  qhandle
 * @return
 */
int32_t qKillQuery(qinfo_t qinfo);

int32_t qQueryCompleted(qinfo_t qinfo);

/**
 * destroy query info structure
 * @param qHandle
 */
void qDestroyQueryInfo(qinfo_t qHandle);

void* qOpenQueryMgmt(int32_t vgId);
void  qQueryMgmtNotifyClosed(void* pExecutor);
void  qQueryMgmtReOpen(void *pExecutor);
void  qCleanupQueryMgmt(void* pExecutor);
void** qRegisterQInfo(void* pMgmt, uint64_t qId, uint64_t qInfo);
void** qAcquireQInfo(void* pMgmt, uint64_t key);
void** qReleaseQInfo(void* pMgmt, void* pQInfo, bool freeHandle);
bool checkQIdEqual(void *qHandle, uint64_t qId);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_QUERY_H
