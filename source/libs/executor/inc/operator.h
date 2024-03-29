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

typedef int32_t (*__optr_encode_fn_t)(struct SOperatorInfo* pOperator, char** result, int32_t* length);
typedef int32_t (*__optr_decode_fn_t)(struct SOperatorInfo* pOperator, char* result);

typedef int32_t (*__optr_open_fn_t)(struct SOperatorInfo* pOptr);
typedef SSDataBlock* (*__optr_fn_t)(struct SOperatorInfo* pOptr);
typedef void (*__optr_close_fn_t)(void* param);
typedef int32_t (*__optr_explain_fn_t)(struct SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len);
typedef int32_t (*__optr_reqBuf_fn_t)(struct SOperatorInfo* pOptr);
typedef SSDataBlock* (*__optr_get_ext_fn_t)(struct SOperatorInfo* pOptr, SOperatorParam* param);
typedef int32_t (*__optr_notify_fn_t)(struct SOperatorInfo* pOptr, SOperatorParam* param);
typedef void (*__optr_state_fn_t)(struct SOperatorInfo* pOptr);

typedef struct SOperatorFpSet {
  __optr_open_fn_t    _openFn;  // DO NOT invoke this function directly
  __optr_fn_t         getNextFn;
  __optr_fn_t         cleanupFn;  // call this function to release the allocated resources ASAP
  __optr_close_fn_t   closeFn;
  __optr_reqBuf_fn_t  reqBufFn;  // total used buffer for blocking operator
  __optr_encode_fn_t  encodeResultRow;
  __optr_decode_fn_t  decodeResultRow;
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
SOperatorInfo* createExchangeOperatorInfo(void* pTransporter, SExchangePhysiNode* pExNode, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createTableScanOperatorInfo(STableScanPhysiNode* pTableScanNode, SReadHandle* pHandle, STableListInfo* pTableList, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createTableMergeScanOperatorInfo(STableScanPhysiNode* pTableScanNode, SReadHandle* readHandle, STableListInfo* pTableListInfo, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createTagScanOperatorInfo(SReadHandle* pReadHandle, STagScanPhysiNode* pPhyNode, STableListInfo* pTableListInfo, SNode* pTagCond, SNode*pTagIndexCond, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createSysTableScanOperatorInfo(void* readHandle, SSystemTableScanPhysiNode* pScanPhyNode, const char* pUser, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createTableCountScanOperatorInfo(SReadHandle* handle, STableCountScanPhysiNode* pNode, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createAggregateOperatorInfo(SOperatorInfo* downstream, SAggPhysiNode* pNode, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createIndefinitOutputOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pNode, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createProjectOperatorInfo(SOperatorInfo* downstream, SProjectPhysiNode* pProjPhyNode, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createSortOperatorInfo(SOperatorInfo* downstream, SSortPhysiNode* pSortNode, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createMultiwayMergeOperatorInfo(SOperatorInfo** dowStreams, size_t numStreams, SMergePhysiNode* pMergePhysiNode, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createCacherowsScanOperator(SLastRowScanPhysiNode* pTableScanNode, SReadHandle* readHandle, STableListInfo* pTableListInfo, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createIntervalOperatorInfo(SOperatorInfo* downstream, SIntervalPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createMergeIntervalOperatorInfo(SOperatorInfo* downstream, SMergeIntervalPhysiNode* pIntervalPhyNode, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createMergeAlignedIntervalOperatorInfo(SOperatorInfo* downstream, SMergeAlignedIntervalPhysiNode* pNode, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createStreamFinalIntervalOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo, int32_t numOfChild, SReadHandle* pHandle);

SOperatorInfo* createSessionAggOperatorInfo(SOperatorInfo* downstream, SSessionWinodwPhysiNode* pSessionNode, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createGroupOperatorInfo(SOperatorInfo* downstream, SAggPhysiNode* pAggNode, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createDataBlockInfoScanOperator(SReadHandle* readHandle, SBlockDistScanPhysiNode* pBlockScanNode, STableListInfo* pTableListInfo, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createStreamScanOperatorInfo(SReadHandle* pHandle, STableScanPhysiNode* pTableScanNode, SNode* pTagCond, STableListInfo* pTableListInfo, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createRawScanOperatorInfo(SReadHandle* pHandle, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createFillOperatorInfo(SOperatorInfo* downstream, SFillPhysiNode* pPhyFillNode, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createStatewindowOperatorInfo(SOperatorInfo* downstream, SStateWinodwPhysiNode* pStateNode, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createPartitionOperatorInfo(SOperatorInfo* downstream, SPartitionPhysiNode* pPartNode, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createStreamPartitionOperatorInfo(SOperatorInfo* downstream, SStreamPartitionPhysiNode* pPartNode, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createTimeSliceOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pNode, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createMergeJoinOperatorInfo(SOperatorInfo** pDownstream, int32_t numOfDownstream, SSortMergeJoinPhysiNode* pJoinNode, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createHashJoinOperatorInfo(SOperatorInfo** pDownstream, int32_t numOfDownstream,         SHashJoinPhysiNode* pJoinNode, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createStreamSessionAggOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo, SReadHandle* pHandle);

SOperatorInfo* createStreamFinalSessionAggOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo, int32_t numOfChild, SReadHandle* pHandle);

SOperatorInfo* createStreamIntervalOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo, SReadHandle* pHandle);

SOperatorInfo* createStreamStateAggOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo, SReadHandle* pHandle);

SOperatorInfo* createStreamFillOperatorInfo(SOperatorInfo* downstream, SStreamFillPhysiNode* pPhyFillNode, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createStreamEventAggOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo, SReadHandle* pHandle);

SOperatorInfo* createStreamCountAggOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo, SReadHandle* pHandle);

SOperatorInfo* createGroupSortOperatorInfo(SOperatorInfo* downstream, SGroupSortPhysiNode* pSortPhyNode, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createEventwindowOperatorInfo(SOperatorInfo* downstream, SPhysiNode* physiNode, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createCountwindowOperatorInfo(SOperatorInfo* downstream, SPhysiNode* physiNode, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createGroupCacheOperatorInfo(SOperatorInfo** pDownstream, int32_t numOfDownstream, SGroupCachePhysiNode* pPhyciNode, SExecTaskInfo* pTaskInfo);

SOperatorInfo* createDynQueryCtrlOperatorInfo(SOperatorInfo** pDownstream, int32_t numOfDownstream,           SDynQueryCtrlPhysiNode* pPhyciNode, SExecTaskInfo* pTaskInfo);

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
SSDataBlock*   optrDefaultGetNextExtFn(struct SOperatorInfo* pOperator, SOperatorParam* pParam);
int32_t        optrDefaultNotifyFn(struct SOperatorInfo* pOperator, SOperatorParam* pParam);
SSDataBlock*   getNextBlockFromDownstream(struct SOperatorInfo* pOperator, int32_t idx);
SSDataBlock*   getNextBlockFromDownstreamRemain(struct SOperatorInfo* pOperator, int32_t idx);
int16_t        getOperatorResultBlockId(struct SOperatorInfo* pOperator, int32_t idx);

SOperatorInfo* createOperator(SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo, SReadHandle* pHandle, SNode* pTagCond,
                              SNode* pTagIndexCond, const char* pUser, const char* dbname);
void           destroyOperator(SOperatorInfo* pOperator);

SOperatorInfo* extractOperatorInTree(SOperatorInfo* pOperator, int32_t type, const char* id);
int32_t        getTableScanInfo(SOperatorInfo* pOperator, int32_t* order, int32_t* scanFlag, bool inheritUsOrder);
int32_t        stopTableScanOperator(SOperatorInfo* pOperator, const char* pIdStr, SStorageAPI* pAPI);
int32_t        getOperatorExplainExecInfo(struct SOperatorInfo* operatorInfo, SArray* pExecInfoList);
void *         getOperatorParam(int32_t opType, SOperatorParam* param, int32_t idx);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_OPERATOR_H
