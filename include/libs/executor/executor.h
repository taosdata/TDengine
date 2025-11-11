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

#include <stdint.h>
#include "tarray.h"
#ifdef __cplusplus
extern "C" {
#endif

#include "query.h"
#include "storageapi.h"
#include "tcommon.h"
#include "tmsgcb.h"
#include "storageapi.h"
#include "functionMgt.h"

typedef void* qTaskInfo_t;
typedef void* DataSinkHandle;

struct SRpcMsg;
struct SSubplan;

typedef int32_t (*localFetchFp)(void*, uint64_t, uint64_t, uint64_t, uint64_t, int64_t, int32_t, void**, SArray*);

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
  void*       pOtherBackend;
  int8_t      fillHistory;
  STimeWindow winRange;
  bool        winRangeValid;

  struct SStorageAPI api;
  void*              pWorkerCb;
  bool               localExec;
  int64_t            uid;
  void*              streamRtInfo;
  bool               cacheSttStatis;
} SReadHandle;

typedef struct SStreamInserterParam {
  SArray*   pFields;     // SArray<SFieldWithOptions>
  SArray*   pTagFields;  // SArray<SFieldWithOptions>
  int64_t   suid;
  int32_t   sver;
  char*     tbname;
  char*     stbname;
  int8_t    tbType;
  char*     dbFName;
  void*     pSinkHandle;
} SStreamInserterParam;

typedef struct SStreamVtableDeployInfo {
  int64_t  uid;
  int32_t  rversion;
  SArray*  addVgIds;
  SArray*  addedVgInfo;  // deploy response,SArray<SStreamTaskAddr>
} SStreamVtableDeployInfo;

typedef struct {
  SStreamRuntimeFuncInfo funcInfo;
  int32_t                 execId;
  bool                    resetFlag;
  const SArray*           pForceOutputCols;
  SStreamInserterParam    inserterParams;
  SStreamVtableDeployInfo vtableDeployInfo;
  int8_t*                 vtableDeployGot;
} SStreamRuntimeInfo;

#define GET_STM_RTINFO(_t) (((_t)->pStreamRuntimeInfo) ? (&(_t)->pStreamRuntimeInfo->funcInfo) : NULL)

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
int32_t qCreateStreamExecTaskInfo(qTaskInfo_t* pInfo, void* msg, SReadHandle* readers, SStreamInserterParam* pInsertParams, int32_t vgId, int32_t taskId);
int32_t qResetTableScan(qTaskInfo_t pInfo, SReadHandle* handle);
bool    qNeedReset(qTaskInfo_t pInfo);

/**
 * Create the exec task for queue mode
 * @param pMsg
 * @param SReadHandle
 * @return
 */
qTaskInfo_t qCreateQueueExecTaskInfo(void* msg, SReadHandle* pReaderHandle, int32_t vgId, int32_t* numOfCols,
                                     uint64_t id);

int32_t qGetColumnsFromNodeList(void* data, bool isList, SArray** pColList);
SSDataBlock* createDataBlockFromDescNode(void* pNode);
void    printDataBlock(SSDataBlock* pBlock, const char* flag, const char* taskIdStr, int64_t qId);

int32_t qGetTableList(int64_t suid, void* pVnode, void* node, SArray** tableList, void* pTaskInfo);

/**
 * set the task Id, usually used by message queue process
 * @param tinfo
 * @param taskId
 * @param queryId
 */
int32_t qSetTaskId(qTaskInfo_t tinfo, uint64_t taskId, uint64_t queryId);
bool    qTaskIsDone(qTaskInfo_t tinfo);
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

bool qIsDynamicExecTask(qTaskInfo_t tinfo);

void qDestroyOperatorParam(SOperatorParam* pParam);

void qUpdateOperatorParam(qTaskInfo_t tinfo, void* pParam);

/**
 * Create the exec task object according to task json
 * @param readHandle
 * @param vgId
 * @param pTaskInfoMsg
 * @param pTaskInfo
 * @param qId
 * @return
 */
int32_t qCreateExecTask(SReadHandle* readHandle, int32_t vgId, uint64_t taskId, struct SSubplan* pSubplan,
                        qTaskInfo_t* pTaskInfo, DataSinkHandle* handle, int8_t compressResult, char* sql,
                        EOPTR_EXEC_MODEL model);

/**
 *
 * @param tinfo
 * @param sversion
 * @param tversion
 * @param rversion
 * @return
 */
int32_t qGetQueryTableSchemaVersion(qTaskInfo_t tinfo, char* dbName, int32_t dbNameBuffLen, char* tableName,
                                    int32_t tbaleNameBuffLen, int32_t* sversion, int32_t* tversion, int32_t* rversion,
                                    int32_t idx, bool* tbGet);

/**
 * The main task execution function, including query on both table and multiple tables,
 * which are decided according to the tag or table name query conditions
 *
 * @param tinfo
 * @param handle
 * @return
 */
int32_t qExecTaskOpt(qTaskInfo_t tinfo, SArray* pResList, uint64_t* useconds, bool* hasMore, SLocalFetch* pLocal,
                     bool processOneBlock);

int32_t qExecTask(qTaskInfo_t tinfo, SSDataBlock** pBlock, uint64_t* useconds);

int32_t qExecutorInit(void);
void    qResetTaskCode(qTaskInfo_t tinfo);

void qCleanExecTaskBlockBuf(qTaskInfo_t tinfo);

/**
 * kill the ongoing query asynchronously
 * @param tinfo  qhandle
 * @return
 */
int32_t qAsyncKillTask(qTaskInfo_t tinfo, int32_t rspCode);

int32_t qKillTask(qTaskInfo_t tinfo, int32_t rspCode, int64_t waitDuration);

bool qTaskIsExecuting(qTaskInfo_t qinfo);

/**
 * destroy query info structure
 * @param qHandle
 */
void qDestroyTask(qTaskInfo_t tinfo);

void qProcessRspMsg(void* parent, struct SRpcMsg* pMsg, struct SEpSet* pEpSet);

int32_t qGetExplainExecInfo(qTaskInfo_t tinfo, SArray* pExecInfoList);

TSKEY       getNextTimeWindowStart(const SInterval* pInterval, TSKEY start, int32_t order);
void        getNextTimeWindow(const SInterval* pInterval, STimeWindow* tw, int32_t order);
void        getInitialStartTimeWindow(SInterval* pInterval, TSKEY ts, STimeWindow* w, bool ascQuery);
STimeWindow getAlignQueryTimeWindow(const SInterval* pInterval, int64_t key);

SArray* qGetQueriedTableListInfo(qTaskInfo_t tinfo);

int32_t qStreamPrepareScan(qTaskInfo_t tinfo, STqOffsetVal* pOffset, int8_t subType);

void qStreamSetOpen(qTaskInfo_t tinfo);

void qStreamSetSourceExcluded(qTaskInfo_t tinfo, int8_t sourceExcluded);

int32_t qStreamExtractOffset(qTaskInfo_t tinfo, STqOffsetVal* pOffset);

SMqBatchMetaRsp* qStreamExtractMetaMsg(qTaskInfo_t tinfo);

const SSchemaWrapper* qExtractSchemaFromTask(qTaskInfo_t tinfo);

const char* qExtractTbnameFromTask(qTaskInfo_t tinfo);

void* qExtractReaderFromTmqScanner(void* scanner);
void  qExtractTmqScanner(qTaskInfo_t tinfo, void** scanner);

int32_t  qStreamOperatorReleaseState(qTaskInfo_t tInfo);
int32_t  qStreamOperatorReloadState(qTaskInfo_t tInfo);
int32_t  streamCollectExprsForReplace(qTaskInfo_t tInfo, SArray* pExprs);
int32_t  streamClearStatesForOperators(qTaskInfo_t tInfo);
int32_t  streamExecuteTask(qTaskInfo_t tInfo, SSDataBlock** ppBlock, uint64_t* ts, bool* finished);
void     streamDestroyExecTask(qTaskInfo_t tInfo);
int32_t  qStreamCreateTableListForReader(void* pVnode, uint64_t suid, uint64_t uid, int8_t tableType,
                                         SNodeList* pGroupTags, bool groupSort, SNode* pTagCond, SNode* pTagIndexCond,
                                         SStorageAPI* storageAPI, void** pTableListInfo, SHashObj* groupIdMap);

int32_t  qStreamFilterTableListForReader(void* pVnode, SArray* uidList,
                                        SNodeList* pGroupTags, SNode* pTagCond, SNode* pTagIndexCond,
                                        SStorageAPI* storageAPI, SHashObj* groupIdMap, uint64_t suid, SArray** tableList);

SArray*  qStreamGetTableListArray(void* pTableListInfo);
void     qStreamDestroyTableList(void* pTableListInfo);
int32_t  qStreamFilter(SSDataBlock* pBlock, void* pFilterInfo, SColumnInfoData** pRet);

int32_t createExprInfo(SNodeList* pNodeList, SNodeList* pGroupKeys, SExprInfo** pExprInfo, int32_t* numOfExprs);
void    destroyExprInfo(SExprInfo* pExpr, int32_t numOfExprs);

int32_t setTbNameColData(const SSDataBlock* pBlock, SColumnInfoData* pColInfoData, int32_t functionId,
  const char* name);
int32_t setVgIdColData(const SSDataBlock* pBlock, SColumnInfoData* pColInfoData, int32_t functionId, int32_t vgId);
int32_t setVgVerColData(const SSDataBlock* pBlock, SColumnInfoData* pColInfoData, int32_t functionId, int64_t vgVer);


int32_t streamCalcOutputTbName(SNode *pExpr, char *tbname, SStreamRuntimeFuncInfo *pPartColVals);

typedef void (*getMnodeEpset_f)(void *pDnode, SEpSet *pEpset);
typedef int32_t (*getDnodeId_f)(void *pData);
typedef SEpSet* (*getSynEpset_f)(int32_t leaderId);

typedef struct SGlobalExecInfo {
  void*           dnode;
  int32_t         dnodeId;
  int32_t         snodeId;
  getMnodeEpset_f getMnode;
  getDnodeId_f    getDnodeId;

} SGlobalExecInfo;

extern SGlobalExecInfo gExecInfo;

void    gExecInfoInit(void* pDnode, getDnodeId_f getDnodeId, getMnodeEpset_f getMnode);
int32_t getCurrentMnodeEpset(SEpSet* pEpSet);
int32_t cloneStreamInserterParam(SStreamInserterParam** pDst, SStreamInserterParam* pSrc);
void    destroyStreamInserterParam(SStreamInserterParam* pParam);
int32_t streamForceOutput(qTaskInfo_t tInfo, SSDataBlock** pRes, int32_t winIdx);
int32_t streamCalcOneScalarExpr(SNode* pExpr, SScalarParam* pDst, const SStreamRuntimeFuncInfo* pExtraParams);
int32_t streamCalcOneScalarExprInRange(SNode* pExpr, SScalarParam* pDst, int32_t rowStartIdx, int32_t rowEndIdx,  const SStreamRuntimeFuncInfo* pExtraParams);
void    cleanupQueryTableDataCond(SQueryTableDataCond* pCond);

int32_t dropStreamTable(SMsgCb* pMsgCb, void* pOutput, SSTriggerDropRequest* pReq);
int32_t dropStreamTableByTbName(SMsgCb* pMsgCb, void* pOutput, SSTriggerDropRequest* pReq, char* tbName);

#ifdef __cplusplus
}
#endif

#endif /*_TD_EXECUTOR_H_*/
