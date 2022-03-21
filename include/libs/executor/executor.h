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

#include "tcommon.h"

typedef void* qTaskInfo_t;
typedef void* DataSinkHandle;
struct SRpcMsg;
struct SSubplan;

typedef struct SReadHandle {
  void* reader;
  void* meta;
} SReadHandle;

#define STREAM_DATA_TYPE_SUBMIT_BLOCK 0x1
#define STREAM_DATA_TYPE_SSDAT_BLOCK  0x2

 /**
  * Create the exec task for streaming mode
  * @param pMsg
  * @param streamReadHandle
  * @return
  */
qTaskInfo_t qCreateStreamExecTaskInfo(void *msg, void* streamReadHandle);

/**
 * Set the input data block for the stream scan.
 * @param tinfo
 * @param input
 * @param type
 * @return
 */
int32_t qSetStreamInput(qTaskInfo_t tinfo, const void* input, int32_t type);

/**
 * Update the table id list, add or remove.
 *
 * @param tinfo
 * @param id
 * @param isAdd
 * @return
 */
int32_t qUpdateQualifiedTableId(qTaskInfo_t tinfo, SArray* tableIdList, bool isAdd);

 /**
  * Create the exec task object according to task json
  * @param readHandle
  * @param vgId
  * @param pTaskInfoMsg
  * @param pTaskInfo
  * @param qId
  * @return
  */
int32_t qCreateExecTask(SReadHandle* readHandle, int32_t vgId, uint64_t taskId, struct SSubplan* pPlan, qTaskInfo_t* pTaskInfo, DataSinkHandle* handle);

/**
 * The main task execution function, including query on both table and multiple tables,
 * which are decided according to the tag or table name query conditions
 *
 * @param tinfo
 * @param handle
 * @return
 */
int32_t qExecTask(qTaskInfo_t tinfo, SSDataBlock** pRes, uint64_t *useconds);

/**
 * Retrieve the produced results information, if current query is not paused or completed,
 * this function will be blocked to wait for the query execution completed or paused,
 * in which case enough results have been produced already.
 *
 * @param tinfo
 * @return
 */
int32_t qRetrieveQueryResultInfo(qTaskInfo_t tinfo, bool* buildRes, void* pRspContext);

/**
 * kill the ongoing query and free the query handle and corresponding resources automatically
 * @param tinfo  qhandle
 * @return
 */
int32_t qKillTask(qTaskInfo_t tinfo);

/**
 * kill the ongoing query asynchronously
 * @param tinfo  qhandle
 * @return
 */
int32_t qAsyncKillTask(qTaskInfo_t tinfo);

/**
 * return whether query is completed or not
 * @param tinfo
 * @return
 */
int32_t qIsTaskCompleted(qTaskInfo_t tinfo);

/**
 * destroy query info structure
 * @param qHandle
 */
void qDestroyTask(qTaskInfo_t tinfo);

/**
 * Get the queried table uid
 * @param qHandle
 * @return
 */
int64_t qGetQueriedTableUid(qTaskInfo_t tinfo);

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
//int32_t qCreateTableGroupByGroupExpr(SArray* pTableIdList, TSKEY skey, STableGroupInfo groupInfo, SColIndex* groupByIndex, int32_t numOfIndex);

/**
 * Update the table id list of a given query.
 * @param uid   child table uid
 * @param type  operation type: ADD|DROP
 * @return
 */
int32_t qUpdateQueriedTableIdList(qTaskInfo_t tinfo, int64_t uid, int32_t type);

//================================================================================================
// query handle management
/**
 * Query handle mgmt object
 * @param vgId
 * @return
 */
void* qOpenTaskMgmt(int32_t vgId);

/**
 * broadcast the close information and wait for all query stop.
 * @param pExecutor
 */
void  qTaskMgmtNotifyClosing(void* pExecutor);

/**
 * Re-open the query handle management module when opening the vnode again.
 * @param pExecutor
 */
void  qQueryMgmtReOpen(void *pExecutor);

/**
 * Close query mgmt and clean up resources.
 * @param pExecutor
 */
void  qCleanupTaskMgmt(void* pExecutor);

/**
 * Add the query into the query mgmt object
 * @param pMgmt
 * @param qId
 * @param qInfo
 * @return
 */
void** qRegisterTask(void* pMgmt, uint64_t qId, void *qInfo);

/**
 * acquire the query handle according to the key from query mgmt object.
 * @param pMgmt
 * @param key
 * @return
 */
void** qAcquireTask(void* pMgmt, uint64_t key);

/**
 * release the query handle and decrease the reference count in cache
 * @param pMgmt
 * @param pQInfo
 * @param freeHandle
 * @return
 */
void** qReleaseTask(void* pMgmt, void* pQInfo, bool freeHandle);

/**
 * De-register the query handle from the management module and free it immediately.
 * @param pMgmt
 * @param pQInfo
 * @return
 */
void** qDeregisterQInfo(void* pMgmt, void* pQInfo);

void qProcessFetchRsp(void* parent, struct SRpcMsg* pMsg, struct SEpSet* pEpSet);

#ifdef __cplusplus
}
#endif

#endif /*_TD_EXECUTOR_H_*/
