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

#include "query.h"
#include "tcommon.h"
#include "tmsgcb.h"

typedef void* qTaskInfo_t;
typedef void* DataSinkHandle;
struct SRpcMsg;
struct SSubplan;

typedef struct SReadHandle {
  void*   reader;
  void*   meta;
  void*   config;
  void*   vnode;
  void*   mnd;
  SMsgCb* pMsgCb;
} SReadHandle;

#define STREAM_DATA_TYPE_SUBMIT_BLOCK 0x1
#define STREAM_DATA_TYPE_SSDATA_BLOCK 0x2

typedef enum {
  OPTR_EXEC_MODEL_BATCH = 0x1,
  OPTR_EXEC_MODEL_STREAM = 0x2,
} EOPTR_EXEC_MODEL;

/**
 * Create the exec task for streaming mode
 * @param pMsg
 * @param streamReadHandle
 * @return
 */
qTaskInfo_t qCreateStreamExecTaskInfo(void* msg, void* streamReadHandle);

/**
 * Set the input data block for the stream scan.
 * @param tinfo
 * @param input
 * @param type
 * @return
 */
int32_t qSetStreamInput(qTaskInfo_t tinfo, const void* input, int32_t type, bool assignUid);

/**
 * Set multiple input data blocks for the stream scan.
 * @param tinfo
 * @param pBlocks
 * @param numOfInputBlock
 * @param type
 * @return
 */
int32_t qSetMultiStreamInput(qTaskInfo_t tinfo, const void* pBlocks, size_t numOfBlocks, int32_t type, bool assignUid);

/**
 * Update the table id list, add or remove.
 *
 * @param tinfo
 * @param id
 * @param isAdd
 * @return
 */
int32_t qUpdateQualifiedTableId(qTaskInfo_t tinfo, const SArray* tableIdList, bool isAdd);

/**
 * Create the exec task object according to task json
 * @param readHandle
 * @param vgId
 * @param pTaskInfoMsg
 * @param pTaskInfo
 * @param qId
 * @return
 */
int32_t qCreateExecTask(SReadHandle* readHandle, int32_t vgId, uint64_t taskId, struct SSubplan* pPlan,
                        qTaskInfo_t* pTaskInfo, DataSinkHandle* handle, EOPTR_EXEC_MODEL model);

/**
 *
 * @param tinfo
 * @param sversion
 * @param tversion
 * @return
 */
int32_t qGetQueriedTableSchemaVersion(qTaskInfo_t tinfo, char* dbName, char* tableName, int32_t* sversion, int32_t* tversion);

/**
 * The main task execution function, including query on both table and multiple tables,
 * which are decided according to the tag or table name query conditions
 *
 * @param tinfo
 * @param handle
 * @return
 */
int32_t qExecTask(qTaskInfo_t tinfo, SSDataBlock** pRes, uint64_t* useconds);

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
// int32_t qCreateTableGroupByGroupExpr(SArray* pTableIdList, TSKEY skey, STableGroupInfo groupInfo, SColIndex*
// groupByIndex, int32_t numOfIndex);

/**
 * Update the table id list of a given query.
 * @param uid   child table uid
 * @param type  operation type: ADD|DROP
 * @return
 */
int32_t qUpdateQueriedTableIdList(qTaskInfo_t tinfo, int64_t uid, int32_t type);

void qProcessFetchRsp(void* parent, struct SRpcMsg* pMsg, struct SEpSet* pEpSet);

int32_t qGetExplainExecInfo(qTaskInfo_t tinfo, int32_t* resNum, SExplainExecInfo** pRes);

#ifdef __cplusplus
}
#endif

#endif /*_TD_EXECUTOR_H_*/
