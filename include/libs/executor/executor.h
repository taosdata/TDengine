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
#include "storageapi.h"

typedef void* qTaskInfo_t;
typedef void* DataSinkHandle;

struct SRpcMsg;
struct SSubplan;

typedef int32_t (*localFetchFp)(void*, uint64_t, uint64_t, uint64_t, int64_t, int32_t, void**, SArray*);

typedef struct {
  void*        handle;
  bool         localExec;
  localFetchFp fp;
  SArray*      explainRes;
} SLocalFetch;

typedef struct {
  void*       tqReader;  // todo remove it
  void*       vnode;
  void*       mnd;
  SMsgCb*     pMsgCb;
  int64_t     version;
  uint64_t    checkpointId;
  bool        initTableReader;
  bool        initTqReader;
  bool        skipRollup;
  int32_t     numOfVgroups;
  void*       sContext;  // SSnapContext*
  void*       pStateBackend;
  int8_t      fillHistory;
  STimeWindow winRange;

  struct SStorageAPI api;
} SReadHandle;

// in queue mode, data streams are seperated by msg
typedef enum {
  OPTR_EXEC_MODEL_BATCH = 0x1,
  OPTR_EXEC_MODEL_STREAM = 0x2,
  OPTR_EXEC_MODEL_QUEUE = 0x3,
} EOPTR_EXEC_MODEL;

/**
 * Create the exec task for stream mode
 * @param pMsg
 * @param SReadHandle
 * @param vgId
 * @return
 */
qTaskInfo_t qCreateStreamExecTaskInfo(void* msg, SReadHandle* readers, int32_t vgId, int32_t taskId);

/**
 * Create the exec task for queue mode
 * @param pMsg
 * @param SReadHandle
 * @return
 */
qTaskInfo_t qCreateQueueExecTaskInfo(void* msg, SReadHandle* pReaderHandle, int32_t vgId, int32_t* numOfCols,
                                     uint64_t id);

int32_t qGetTableList(int64_t suid, void* pVnode, void* node, SArray **tableList, void* pTaskInfo);

/**
 * set the task Id, usually used by message queue process
 * @param tinfo
 * @param taskId
 * @param queryId
 */
void qSetTaskId(qTaskInfo_t tinfo, uint64_t taskId, uint64_t queryId);

int32_t qSetStreamOpOpen(qTaskInfo_t tinfo);

/**
 * Set multiple input data blocks for the stream scan.
 * @param tinfo
 * @param pBlocks
 * @param numOfInputBlock
 * @param type
 * @return
 */
int32_t qSetMultiStreamInput(qTaskInfo_t tinfo, const void* pBlocks, size_t numOfBlocks, int32_t type);

/**
 * Set block for sma
 * @param tinfo
 * @param pBlocks
 * @param numOfInputBlock
 * @param type
 * @return
 */
int32_t qSetSMAInput(qTaskInfo_t tinfo, const void* pBlocks, size_t numOfBlocks, int32_t type);

/**
 * Update the table id list, add or remove.
 *
 * @param tinfo
 * @param id
 * @param isAdd
 * @return
 */
int32_t qUpdateTableListForStreamScanner(qTaskInfo_t tinfo, const SArray* tableIdList, bool isAdd);

bool   qIsDynamicExecTask(qTaskInfo_t tinfo);

void   qUpdateOperatorParam(qTaskInfo_t tinfo, void* pParam);

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
                        qTaskInfo_t* pTaskInfo, DataSinkHandle* handle, char* sql, EOPTR_EXEC_MODEL model);

/**
 *
 * @param tinfo
 * @param sversion
 * @param tversion
 * @return
 */
int32_t qGetQueryTableSchemaVersion(qTaskInfo_t tinfo, char* dbName, char* tableName, int32_t* sversion,
                                    int32_t* tversion, int32_t idx);

/**
 * The main task execution function, including query on both table and multiple tables,
 * which are decided according to the tag or table name query conditions
 *
 * @param tinfo
 * @param handle
 * @return
 */
int32_t qExecTaskOpt(qTaskInfo_t tinfo, SArray* pResList, uint64_t* useconds, bool* hasMore, SLocalFetch* pLocal);

int32_t qExecTask(qTaskInfo_t tinfo, SSDataBlock** pBlock, uint64_t* useconds);

void qCleanExecTaskBlockBuf(qTaskInfo_t tinfo);

/**
 * kill the ongoing query asynchronously
 * @param tinfo  qhandle
 * @return
 */
int32_t qAsyncKillTask(qTaskInfo_t tinfo, int32_t rspCode);

int32_t qKillTask(qTaskInfo_t tinfo, int32_t rspCode);

bool qTaskIsExecuting(qTaskInfo_t qinfo);

/**
 * destroy query info structure
 * @param qHandle
 */
void qDestroyTask(qTaskInfo_t tinfo);

void qProcessRspMsg(void* parent, struct SRpcMsg* pMsg, struct SEpSet* pEpSet);

int32_t qGetExplainExecInfo(qTaskInfo_t tinfo, SArray* pExecInfoList);

void getNextTimeWindow(const SInterval* pInterval, STimeWindow* tw, int32_t order);
void getInitialStartTimeWindow(SInterval* pInterval, TSKEY ts, STimeWindow* w, bool ascQuery);
STimeWindow getAlignQueryTimeWindow(const SInterval* pInterval, int64_t key);

SArray* qGetQueriedTableListInfo(qTaskInfo_t tinfo);

int32_t qStreamPrepareScan(qTaskInfo_t tinfo, STqOffsetVal* pOffset, int8_t subType);

void qStreamSetOpen(qTaskInfo_t tinfo);

void qStreamSetSourceExcluded(qTaskInfo_t tinfo, int8_t sourceExcluded);

void qStreamExtractOffset(qTaskInfo_t tinfo, STqOffsetVal* pOffset);

SMqMetaRsp* qStreamExtractMetaMsg(qTaskInfo_t tinfo);

const SSchemaWrapper* qExtractSchemaFromTask(qTaskInfo_t tinfo);

const char* qExtractTbnameFromTask(qTaskInfo_t tinfo);

void* qExtractReaderFromStreamScanner(void* scanner);

int32_t qExtractStreamScanner(qTaskInfo_t tinfo, void** scanner);

int32_t qSetStreamOperatorOptionForScanHistory(qTaskInfo_t tinfo);
int32_t qStreamSourceScanParamForHistoryScanStep1(qTaskInfo_t tinfo, SVersionRange *pVerRange, STimeWindow* pWindow);
int32_t qStreamSourceScanParamForHistoryScanStep2(qTaskInfo_t tinfo, SVersionRange *pVerRange, STimeWindow* pWindow);
int32_t qStreamRecoverFinish(qTaskInfo_t tinfo);
bool    qStreamScanhistoryFinished(qTaskInfo_t tinfo);
int32_t qStreamInfoResetTimewindowFilter(qTaskInfo_t tinfo);
void    resetTaskInfo(qTaskInfo_t tinfo);

int32_t qStreamOperatorReleaseState(qTaskInfo_t tInfo);
int32_t qStreamOperatorReloadState(qTaskInfo_t tInfo);

#ifdef __cplusplus
}
#endif

#endif /*_TD_EXECUTOR_H_*/
