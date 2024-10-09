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

#ifndef TDENGINE_OPERATOR_H
#define TDENGINE_OPERATOR_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SOperatorCostInfo {
  double openCost;
  double totalCost;
} SOperatorCostInfo;

struct SOperatorInfo;

typedef int32_t (*__optr_open_fn_t)(struct SOperatorInfo* pOptr);
typedef int32_t (*__optr_fn_t)(struct SOperatorInfo* pOptr, SSDataBlock** pResBlock);
typedef void (*__optr_close_fn_t)(void* param);
typedef int32_t (*__optr_explain_fn_t)(struct SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len);
typedef int32_t (*__optr_reqBuf_fn_t)(struct SOperatorInfo* pOptr);
typedef int32_t (*__optr_get_ext_fn_t)(struct SOperatorInfo* pOptr, SOperatorParam* param, SSDataBlock** pRes);
typedef int32_t (*__optr_notify_fn_t)(struct SOperatorInfo* pOptr, SOperatorParam* param);
typedef void (*__optr_state_fn_t)(struct SOperatorInfo* pOptr);

typedef struct SOperatorFpSet {
  __optr_open_fn_t    _openFn;  // DO NOT invoke this function directly
  __optr_fn_t         getNextFn;
  __optr_fn_t         cleanupFn;  // call this function to release the allocated resources ASAP
  __optr_close_fn_t   closeFn;
  __optr_reqBuf_fn_t  reqBufFn;  // total used buffer for blocking operator
  __optr_explain_fn_t getExplainFn;
  __optr_get_ext_fn_t getNextExtFn;
  __optr_notify_fn_t  notifyFn;
  __optr_state_fn_t   releaseStreamStateFn;
  __optr_state_fn_t   reloadStreamStateFn;
} SOperatorFpSet;

enum {
  OP_NOT_OPENED = 0x0,
  OP_OPENED = 0x1,
  OP_RES_TO_RETURN = 0x5,
  OP_EXEC_DONE = 0x9,
};

typedef struct SOperatorInfo {
  uint16_t               operatorType;
  int16_t                resultDataBlockId;
  bool                   blocking;  // block operator or not
  bool                   transparent;
  bool                   dynamicTask;
  uint8_t                status;    // denote if current operator is completed
  char*                  name;      // name, for debug purpose
  void*                  info;      // extension attribution
  SExprSupp              exprSupp;
  SExecTaskInfo*         pTaskInfo;
  SOperatorCostInfo      cost;
  SResultInfo            resultInfo;
  SOperatorParam*        pOperatorGetParam;
  SOperatorParam*        pOperatorNotifyParam;
  SOperatorParam**       pDownstreamGetParams;
  SOperatorParam**       pDownstreamNotifyParams;
  struct SOperatorInfo** pDownstream;      // downstram pointer list
  int32_t                numOfDownstream;  // number of downstream. The value is always ONE expect for join operator
  int32_t                numOfRealDownstream;
  SOperatorFpSet         fpSet;
} SOperatorInfo;

// operator creater functions
// clang-format off
int32_t createExchangeOperatorInfo(void* pTransporter, SExchangePhysiNode* pExNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createTableScanOperatorInfo(STableScanPhysiNode* pTableScanNode, SReadHandle* pHandle, STableListInfo* pTableList, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createTableMergeScanOperatorInfo(STableScanPhysiNode* pTableScanNode, SReadHandle* readHandle, STableListInfo* pTableListInfo, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createTagScanOperatorInfo(SReadHandle* pReadHandle, STagScanPhysiNode* pPhyNode, STableListInfo* pTableListInfo, SNode* pTagCond, SNode*pTagIndexCond, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createSysTableScanOperatorInfo(void* readHandle, SSystemTableScanPhysiNode* pScanPhyNode, const char* pUser, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createTableCountScanOperatorInfo(SReadHandle* handle, STableCountScanPhysiNode* pNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createAggregateOperatorInfo(SOperatorInfo* downstream, SAggPhysiNode* pNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createIndefinitOutputOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createProjectOperatorInfo(SOperatorInfo* downstream, SProjectPhysiNode* pProjPhyNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createSortOperatorInfo(SOperatorInfo* downstream, SSortPhysiNode* pSortNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createMultiwayMergeOperatorInfo(SOperatorInfo** dowStreams, size_t numStreams, SMergePhysiNode* pMergePhysiNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createCacherowsScanOperator(SLastRowScanPhysiNode* pTableScanNode, SReadHandle* readHandle, STableListInfo* pTableListInfo, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createIntervalOperatorInfo(SOperatorInfo* downstream, SIntervalPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createMergeIntervalOperatorInfo(SOperatorInfo* downstream, SMergeIntervalPhysiNode* pIntervalPhyNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createMergeAlignedIntervalOperatorInfo(SOperatorInfo* downstream, SMergeAlignedIntervalPhysiNode* pNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createStreamFinalIntervalOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo, int32_t numOfChild, SReadHandle* pHandle, SOperatorInfo** pInfo);

int32_t createSessionAggOperatorInfo(SOperatorInfo* downstream, SSessionWinodwPhysiNode* pSessionNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createGroupOperatorInfo(SOperatorInfo* downstream, SAggPhysiNode* pAggNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createDataBlockInfoScanOperator(SReadHandle* readHandle, SBlockDistScanPhysiNode* pBlockScanNode, STableListInfo* pTableListInfo, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createStreamScanOperatorInfo(SReadHandle* pHandle, STableScanPhysiNode* pTableScanNode, SNode* pTagCond, STableListInfo* pTableListInfo, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createRawScanOperatorInfo(SReadHandle* pHandle, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createFillOperatorInfo(SOperatorInfo* downstream, SFillPhysiNode* pPhyFillNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createStatewindowOperatorInfo(SOperatorInfo* downstream, SStateWinodwPhysiNode* pStateNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createPartitionOperatorInfo(SOperatorInfo* downstream, SPartitionPhysiNode* pPartNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createStreamPartitionOperatorInfo(SOperatorInfo* downstream, SStreamPartitionPhysiNode* pPartNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createTimeSliceOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createForecastOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createMergeJoinOperatorInfo(SOperatorInfo** pDownstream, int32_t numOfDownstream, SSortMergeJoinPhysiNode* pJoinNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createHashJoinOperatorInfo(SOperatorInfo** pDownstream, int32_t numOfDownstream, SHashJoinPhysiNode* pJoinNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createStreamSessionAggOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo, SReadHandle* pHandle, SOperatorInfo** pInfo);

int32_t createStreamFinalSessionAggOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo, int32_t numOfChild, SReadHandle* pHandle, SOperatorInfo** pInfo);

int32_t createStreamIntervalOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo, SReadHandle* pHandle, SOperatorInfo** pInfo);

int32_t createStreamStateAggOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo, SReadHandle* pHandle, SOperatorInfo** pInfo);

int32_t createStreamFillOperatorInfo(SOperatorInfo* downstream, SStreamFillPhysiNode* pPhyFillNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createStreamEventAggOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo, SReadHandle* pHandle, SOperatorInfo** pInfo);

int32_t createStreamCountAggOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo, SReadHandle* pHandle, SOperatorInfo** pInfo);

int32_t createGroupSortOperatorInfo(SOperatorInfo* downstream, SGroupSortPhysiNode* pSortPhyNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createEventwindowOperatorInfo(SOperatorInfo* downstream, SPhysiNode* physiNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createCountwindowOperatorInfo(SOperatorInfo* downstream, SPhysiNode* physiNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createGroupCacheOperatorInfo(SOperatorInfo** pDownstream, int32_t numOfDownstream, SGroupCachePhysiNode* pPhyciNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createAnomalywindowOperatorInfo(SOperatorInfo* downstream, SPhysiNode* physiNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createDynQueryCtrlOperatorInfo(SOperatorInfo** pDownstream, int32_t numOfDownstream, SDynQueryCtrlPhysiNode* pPhyciNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pInfo);

int32_t createStreamTimeSliceOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo, SReadHandle* pHandle, SOperatorInfo** ppOptInfo);

// clang-format on

SOperatorFpSet createOperatorFpSet(__optr_open_fn_t openFn, __optr_fn_t nextFn, __optr_fn_t cleanup,
                                   __optr_close_fn_t closeFn, __optr_reqBuf_fn_t reqBufFn,
                                   __optr_explain_fn_t explain, __optr_get_ext_fn_t nextExtFn, __optr_notify_fn_t notifyFn);
void           setOperatorStreamStateFn(SOperatorInfo* pOperator, __optr_state_fn_t relaseFn, __optr_state_fn_t reloadFn);
int32_t        optrDummyOpenFn(SOperatorInfo* pOperator);
int32_t        appendDownstream(SOperatorInfo* p, SOperatorInfo** pDownstream, int32_t num);
void           setOperatorCompleted(SOperatorInfo* pOperator);
void           setOperatorInfo(SOperatorInfo* pOperator, const char* name, int32_t type, bool blocking, int32_t status,
                               void* pInfo, SExecTaskInfo* pTaskInfo);
int32_t        optrDefaultBufFn(SOperatorInfo* pOperator);
int32_t optrDefaultGetNextExtFn(struct SOperatorInfo* pOperator, SOperatorParam* pParam, SSDataBlock** pRes);
int32_t        optrDefaultNotifyFn(struct SOperatorInfo* pOperator, SOperatorParam* pParam);
SSDataBlock*   getNextBlockFromDownstream(struct SOperatorInfo* pOperator, int32_t idx);
SSDataBlock*   getNextBlockFromDownstreamRemain(struct SOperatorInfo* pOperator, int32_t idx);
int16_t        getOperatorResultBlockId(struct SOperatorInfo* pOperator, int32_t idx);

int32_t        createOperator(SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo, SReadHandle* pHandle, SNode* pTagCond,
                              SNode* pTagIndexCond, const char* pUser, const char* dbname, SOperatorInfo** pOptrInfo);
void           destroyOperator(SOperatorInfo* pOperator);
void           destroyOperatorAndDownstreams(SOperatorInfo* pOperator, SOperatorInfo** stream, int32_t num);

int32_t        extractOperatorInTree(SOperatorInfo* pOperator, int32_t type, const char* id, SOperatorInfo** pOptrInfo);
int32_t        getTableScanInfo(SOperatorInfo* pOperator, int32_t* order, int32_t* scanFlag, bool inheritUsOrder);
int32_t        stopTableScanOperator(SOperatorInfo* pOperator, const char* pIdStr, SStorageAPI* pAPI);
int32_t        getOperatorExplainExecInfo(struct SOperatorInfo* operatorInfo, SArray* pExecInfoList);
void *         getOperatorParam(int32_t opType, SOperatorParam* param, int32_t idx);

void doKeepTuple(SWindowRowsSup* pRowSup, int64_t ts, uint64_t groupId);
void doKeepNewWindowStartInfo(SWindowRowsSup* pRowSup, const int64_t* tsList, int32_t rowIndex, uint64_t groupId);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_OPERATOR_H
