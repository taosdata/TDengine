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

#ifndef _TD_EXECUTOR_H_
#define _TD_EXECUTOR_H_

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
int32_t qCreateQueryInfo(void* tsdb, int32_t vgId, SQueryTableMsg* pQueryTableMsg, qinfo_t* qinfo, uint64_t qId);


/**
 * the main query execution function, including query on both table and multiple tables,
 * which are decided according to the tag or table name query conditions
 *
 * @param qinfo
 * @return
 */
bool qTableQuery(qinfo_t qinfo, uint64_t *qId);

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
 * return the transporter context (RPC)
 * @param qinfo
 * @return
 */
void* qGetResultRetrieveMsg(qinfo_t qinfo);

/**
 * kill the ongoing query and free the query handle and corresponding resources automatically
 * @param qinfo  qhandle
 * @return
 */
int32_t qKillQuery(qinfo_t qinfo);

/**
 * return whether query is completed or not
 * @param qinfo
 * @return
 */
int32_t qIsQueryCompleted(qinfo_t qinfo);

/**
 * destroy query info structure
 * @param qHandle
 */
void qDestroyQueryInfo(qinfo_t qHandle);

/**
 * Get the queried table uid
 * @param qHandle
 * @return
 */
int64_t qGetQueriedTableUid(qinfo_t qHandle);

/**
 * Extract the qualified table id list, and than pass them to the TSDB driver to load the required table data blocks.
 *
 * @param iter  the table iterator to traverse all tables belongs to a super table, or an invert index
 * @return
 */
int32_t qGetQualifiedTableIdList(void* pTableList, const char* tagCond, int32_t tagCondLen, SArray* pTableIdList);

/**
 * Create the table group according to the group by tags info
 * @param pTableIdList
 * @param skey
 * @param groupInfo
 * @param groupByIndex
 * @param numOfIndex
 * @return
 */
int32_t qCreateTableGroupByGroupExpr(SArray* pTableIdList, TSKEY skey, STableGroupInfo groupInfo, SColIndex* groupByIndex, int32_t numOfIndex);

/**
 * Update the table id list of a given query.
 * @param uid   child table uid
 * @param type  operation type: ADD|DROP
 * @return
 */
int32_t qUpdateQueriedTableIdList(qinfo_t qinfo, int64_t uid, int32_t type);

//================================================================================================
// query handle management
/**
 * Query handle mgmt object
 * @param vgId
 * @return
 */
void* qOpenQueryMgmt(int32_t vgId);

/**
 * broadcast the close information and wait for all query stop.
 * @param pExecutor
 */
void  qQueryMgmtNotifyClosed(void* pExecutor);

/**
 * Re-open the query handle management module when opening the vnode again.
 * @param pExecutor
 */
void  qQueryMgmtReOpen(void *pExecutor);

/**
 * Close query mgmt and clean up resources.
 * @param pExecutor
 */
void  qCleanupQueryMgmt(void* pExecutor);

/**
 * Add the query into the query mgmt object
 * @param pMgmt
 * @param qId
 * @param qInfo
 * @return
 */
void** qRegisterQInfo(void* pMgmt, uint64_t qId, void *qInfo);

/**
 * acquire the query handle according to the key from query mgmt object.
 * @param pMgmt
 * @param key
 * @return
 */
void** qAcquireQInfo(void* pMgmt, uint64_t key);

/**
 * release the query handle and decrease the reference count in cache
 * @param pMgmt
 * @param pQInfo
 * @param freeHandle
 * @return
 */
void** qReleaseQInfo(void* pMgmt, void* pQInfo);

/**
 * De-register the query handle from the management module and free it immediately.
 * @param pMgmt
 * @param pQInfo
 * @return
 */
void** qDeregisterQInfo(void* pMgmt, void* pQInfo);

#ifdef __cplusplus
}
#endif

#endif /*_TD_EXECUTOR_H_*/