#include "streamReader.h"
#include "osMemPool.h"
#include "streamInt.h"
#include "executor.h"

void destroyOptions(SStreamTriggerReaderTaskInnerOptions* options) {
  if (options == NULL) return;
  if (options->isSchema) {
    taosArrayDestroy(options->schemas);
  }
}

void releaseStreamTask(void* p) {
  if (p == NULL) return;
  SStreamReaderTaskInner* pTask = *((SStreamReaderTaskInner**)p);
  if (pTask == NULL) return;
  blockDataDestroy(pTask->pResBlock);
  blockDataDestroy(pTask->pResBlockDst);
  qStreamDestroyTableList(pTask->pTableList);
  pTask->api.tsdReader.tsdReaderClose(pTask->pReader);
  filterFreeInfo(pTask->pFilterInfo);
  destroyOptions(&pTask->options);
  cleanupQueryTableDataCond(&pTask->cond);
  
  taosMemoryFree(pTask);
}

int32_t createDataBlockForStream(SArray* schemas, SSDataBlock** pBlockRet) {
  int32_t      code = 0;
  int32_t      lino = 0;
  int32_t      numOfCols = taosArrayGetSize(schemas);
  SSDataBlock* pBlock = NULL;
  STREAM_CHECK_RET_GOTO(createDataBlock(&pBlock));

  for (int32_t i = 0; i < numOfCols; ++i) {
    SSchema* pSchema = taosArrayGet(schemas, i);
    STREAM_CHECK_NULL_GOTO(pSchema, terrno);
    SColumnInfoData idata = createColumnInfoData(pSchema->type, pSchema->bytes, pSchema->colId);

    STREAM_CHECK_RET_GOTO(blockDataAppendColInfo(pBlock, &idata));
  }
  STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(pBlock, STREAM_RETURN_ROWS_NUM));

end:
  STREAM_PRINT_LOG_END(code, lino)
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    pBlock = NULL;
  }
  *pBlockRet = pBlock;
  return code;
}

int32_t createDataBlockForTs(SSDataBlock** pBlockRet) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SSDataBlock* pBlock = NULL;
  STREAM_CHECK_RET_GOTO(createDataBlock(&pBlock));
  SColumnInfoData idata = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES, PRIMARYKEY_TIMESTAMP_COL_ID);
  STREAM_CHECK_RET_GOTO(blockDataAppendColInfo(pBlock, &idata));
  STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(pBlock, STREAM_RETURN_ROWS_NUM));

end:
  STREAM_PRINT_LOG_END(code, lino)
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    pBlock = NULL;
  }
  *pBlockRet = pBlock;
  return code;
}

int32_t qStreamInitQueryTableDataCond(SQueryTableDataCond* pCond, int32_t order, void* schemas, bool isSchema,
                                      STimeWindow twindows, uint64_t suid, int64_t ver) {
  int32_t code = 0;
  int32_t lino = 0;

  memset(pCond, 0, sizeof(*pCond));

  pCond->order = order;
  pCond->numOfCols = isSchema ? taosArrayGetSize((SArray*)schemas) : LIST_LENGTH((SNodeList*)schemas);

  pCond->colList = taosMemoryCalloc(pCond->numOfCols, sizeof(SColumnInfo));
  STREAM_CHECK_NULL_GOTO(pCond->colList, terrno);
  pCond->pSlotList = taosMemoryMalloc(sizeof(int32_t) * pCond->numOfCols);
  STREAM_CHECK_NULL_GOTO(pCond->pSlotList, terrno);

  pCond->twindows = twindows;
  pCond->suid = suid;
  pCond->type = TIMEWINDOW_RANGE_CONTAINED;
  pCond->startVersion = -1;
  pCond->endVersion = ver;
  //  pCond->skipRollup = readHandle->skipRollup;

  pCond->notLoadData = false;

  for (int32_t i = 0; i < pCond->numOfCols; ++i) {
    SColumnInfo* pColInfo = &pCond->colList[i];
    if (isSchema) {
      SSchema* pSchema = taosArrayGet((SArray*)schemas, i);
      pCond->colList[i].type = pSchema->type;
      pCond->colList[i].bytes = pSchema->bytes;
      pCond->colList[i].colId = pSchema->colId;
      pCond->colList[i].pk = pSchema->flags & COL_IS_KEY;

      pCond->pSlotList[i] = i;
    } else {
      STargetNode* pNode = (STargetNode*)nodesListGetNode((SNodeList*)schemas, i);
      STREAM_CHECK_NULL_GOTO(pNode, TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);

      SColumnNode* pColNode = (SColumnNode*)pNode->pExpr;
      STREAM_CHECK_NULL_GOTO(pColNode, TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);

      pCond->colList[i].type = pColNode->node.resType.type;
      pCond->colList[i].bytes = pColNode->node.resType.bytes;
      pCond->colList[i].colId = pColNode->colId;
      pCond->colList[i].pk = pColNode->isPk;

      pCond->pSlotList[i] = pNode->slotId;
    }
  }

end:
  STREAM_PRINT_LOG_END(code, lino);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pCond->colList);
    taosMemoryFree(pCond->pSlotList);
    pCond->colList = NULL;
    pCond->pSlotList = NULL;
  }
  return code;
}

int32_t createStreamTask(void* pVnode, SStreamTriggerReaderTaskInnerOptions* options, SStreamReaderTaskInner** ppTask,
                         SSDataBlock* pResBlock, SHashObj* groupIdMap, SStorageAPI* api) {
  int32_t                 code = 0;
  int32_t                 lino = 0;
  SStreamReaderTaskInner* pTask = taosMemoryCalloc(1, sizeof(SStreamReaderTaskInner));
  SNodeList*              groupNew = NULL;
  STREAM_CHECK_NULL_GOTO(pTask, terrno);
  pTask->api = *api;
  pTask->options = *options;
  options->schemas = NULL;
  if (pResBlock != NULL) {
    STREAM_CHECK_RET_GOTO(createOneDataBlock(pResBlock, false, &pTask->pResBlock));
  } else {
    STREAM_CHECK_RET_GOTO(createDataBlockForStream(pTask->options.schemas, &pTask->pResBlock));
  }
  if (options->initReader) {
    int32_t        pNum = 0;
    STableKeyInfo* pList = NULL;
    if (options->uidList != NULL) {
      for (int32_t i = 0; i < taosArrayGetSize(options->uidList); ++i) {
        int64_t* uid = taosArrayGet(options->uidList, i);
        STREAM_CHECK_NULL_GOTO(uid, terrno);
        STableKeyInfo data = {uid[1], 0};
        STREAM_CHECK_RET_GOTO(qStreamSetTableList(&pTask->pTableList, &data));
      }
      STREAM_CHECK_RET_GOTO(qStreamGetTableList(pTask->pTableList, -1, &pList, &pNum))
    } else {
      STREAM_CHECK_RET_GOTO(filterInitFromNode(options->pConditions, &pTask->pFilterInfo, 0, NULL));
      STREAM_CHECK_RET_GOTO(nodesCloneList(options->partitionCols, &groupNew));
      STREAM_CHECK_RET_GOTO(qStreamCreateTableListForReader(
          pVnode, options->suid, options->uid, options->tableType, groupNew, options->groupSort,
          options->pTagCond, options->pTagIndexCond, api, &pTask->pTableList, groupIdMap));

      if (options->gid != 0) {
        int32_t index = qStreamGetGroupIndex(pTask->pTableList, options->gid);
        STREAM_CHECK_CONDITION_GOTO(index < 0, TSDB_CODE_STREAM_NO_DATA);
        pTask->currentGroupIndex = index;
      }
      if (options->scanMode == STREAM_SCAN_GROUP_ONE_BY_ONE) {
        STREAM_CHECK_RET_GOTO(qStreamGetTableList(pTask->pTableList, pTask->currentGroupIndex, &pList, &pNum))
      } else if (options->scanMode == STREAM_SCAN_ALL) {
        STREAM_CHECK_RET_GOTO(qStreamGetTableList(pTask->pTableList, -1, &pList, &pNum))
      }
    }

    cleanupQueryTableDataCond(&pTask->cond);
    STREAM_CHECK_RET_GOTO(qStreamInitQueryTableDataCond(&pTask->cond, options->order, pTask->options.schemas, options->isSchema,
                                                        options->twindows, options->suid, options->ver));
    STREAM_CHECK_RET_GOTO(pTask->api.tsdReader.tsdReaderOpen(pVnode, &pTask->cond, pList, pNum, pTask->pResBlock,
                                                           (void**)&pTask->pReader, pTask->idStr, NULL));
  }
  
  *ppTask = pTask;
  pTask = NULL;

end:
  nodesDestroyList(groupNew);
  STREAM_PRINT_LOG_END(code, lino);
  releaseStreamTask(&pTask);
  destroyOptions(options);
  return code;
}

static void destroyCondition(SNode* pCond) {
  if (pCond == NULL) return;
  nodesDestroyNode(pCond);
}

static void releaseStreamReaderInfo(void* p) {
  if (p == NULL) return;
  SStreamTriggerReaderInfo* pInfo = (SStreamTriggerReaderInfo*)p;
  if (pInfo == NULL) return;
  taosHashCleanup(pInfo->streamTaskMap);
  taosHashCleanup(pInfo->groupIdMap);
  pInfo->streamTaskMap = NULL;

  nodesDestroyNode((SNode*)(pInfo->triggerAst));
  nodesDestroyNode((SNode*)(pInfo->calcAst));
  
  nodesDestroyList(pInfo->partitionCols);
  blockDataDestroy(pInfo->triggerResBlock);
  blockDataDestroy(pInfo->calcResBlock);
  blockDataDestroy(pInfo->tsBlock);
  blockDataDestroy(pInfo->calcResBlockTmp);
  destroyExprInfo(pInfo->pExprInfo, pInfo->numOfExpr);
  taosMemoryFreeClear(pInfo->pExprInfo);
  taosArrayDestroy(pInfo->uidList);
  taosArrayDestroy(pInfo->uidListIndex);
  taosHashCleanup(pInfo->uidHash);
  taosMemoryFree(pInfo);
}

static void releaseStreamReaderCalcInfo(void* p) {
  if (p == NULL) return;
  SStreamTriggerReaderCalcInfo* pInfo = (SStreamTriggerReaderCalcInfo*)p;
  if (pInfo == NULL) return;
  nodesDestroyNode((SNode*)(pInfo->calcAst));
  taosMemoryFreeClear(pInfo->calcScanPlan);
  qDestroyTask(pInfo->pTaskInfo);
  pInfo->pTaskInfo = NULL;
  nodesDestroyNode((SNode*)pInfo->tsConditions);
  filterFreeInfo(pInfo->pFilterInfo);

  tDestroyStRtFuncInfo(&pInfo->rtInfo.funcInfo);
  taosMemoryFree(pInfo);
}

int32_t qStreamBuildSchema(SArray* schemas, int8_t type, int32_t bytes, col_id_t colId) {
  SSchema* pSchema = taosArrayReserve(schemas, 1);
  if (pSchema == NULL) {
    return terrno;
  }
  pSchema->type = type;
  pSchema->bytes = bytes;
  pSchema->colId = colId;
  return 0;
}

static void releaseGroupIdMap(void* p) {
  if (p == NULL) return;
  SArray* gInfo = *((SArray**)p);
  if (gInfo == NULL) return;
  taosArrayDestroyEx(gInfo, tDestroySStreamGroupValue);
}

static int32_t setColIdForCalcResBlock(SNodeList* colList, SArray* pDataBlock){
  int32_t  code = 0;
  int32_t  lino = 0;
  SNode*  nodeItem = NULL;
  FOREACH(nodeItem, colList) {
    SNode*           pNode = ((STargetNode*)nodeItem)->pExpr;
    int32_t          slotId = ((STargetNode*)nodeItem)->slotId;
    SColumnInfoData* pColData = taosArrayGet(pDataBlock, slotId);
    STREAM_CHECK_NULL_GOTO(pColData, terrno);

    if (nodeType(pNode) == QUERY_NODE_FUNCTION){
      SFunctionNode* pFuncNode = (SFunctionNode*)pNode;
      STREAM_CHECK_CONDITION_GOTO(pFuncNode->funcType != FUNCTION_TYPE_TBNAME, TSDB_CODE_INVALID_PARA);
      pColData->info.colId = -1;
    } else if (nodeType(pNode) == QUERY_NODE_COLUMN) {
      SColumnNode*     valueNode = (SColumnNode*)(pNode);
      pColData->info.colId = valueNode->colId;
    } else {
      code = TSDB_CODE_INVALID_PARA;
      goto end;
    }
  }
end:
  STREAM_PRINT_LOG_END(code, lino);
  return code;
}

static SStreamTriggerReaderInfo* createStreamReaderInfo(void* pTask, const SStreamReaderDeployMsg* pMsg) {
  int32_t    code = 0;
  int32_t    lino = 0;

  SStreamTriggerReaderInfo* sStreamReaderInfo = taosMemoryCalloc(1, sizeof(SStreamTriggerReaderInfo));
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);

  sStreamReaderInfo->pTask = pTask;
  sStreamReaderInfo->tableType = pMsg->msg.trigger.triggerTblType;
  if (pMsg->msg.trigger.triggerTblType == TD_SUPER_TABLE) {
    sStreamReaderInfo->suid = pMsg->msg.trigger.triggerTblUid;
  } else {
    sStreamReaderInfo->uid = pMsg->msg.trigger.triggerTblUid;
  }

  ST_TASK_DLOG("pMsg->msg.trigger.deleteReCalc: %d", pMsg->msg.trigger.deleteReCalc);
  sStreamReaderInfo->deleteReCalc = pMsg->msg.trigger.deleteReCalc;
  sStreamReaderInfo->deleteOutTbl = pMsg->msg.trigger.deleteOutTbl;
  // process triggerScanPlan
  STREAM_CHECK_RET_GOTO(
      nodesStringToNode(pMsg->msg.trigger.triggerScanPlan, (SNode**)(&sStreamReaderInfo->triggerAst)));
  if (sStreamReaderInfo->triggerAst != NULL) {
    STREAM_CHECK_CONDITION_GOTO(
        QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN != nodeType(sStreamReaderInfo->triggerAst->pNode) &&
            QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN != nodeType(sStreamReaderInfo->triggerAst->pNode),
        TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);
    sStreamReaderInfo->pTagCond = sStreamReaderInfo->triggerAst->pTagCond;
    sStreamReaderInfo->pTagIndexCond = sStreamReaderInfo->triggerAst->pTagIndexCond;
    sStreamReaderInfo->pConditions = sStreamReaderInfo->triggerAst->pNode->pConditions;
    STREAM_CHECK_RET_GOTO(nodesStringToList(pMsg->msg.trigger.partitionCols, &sStreamReaderInfo->partitionCols));
    // sStreamReaderInfo->partitionCols = ((STableScanPhysiNode*)(sStreamReaderInfo->triggerAst->pNode))->pGroupTags;
    sStreamReaderInfo->twindows = ((STableScanPhysiNode*)(sStreamReaderInfo->triggerAst->pNode))->scanRange;
    sStreamReaderInfo->triggerCols = ((STableScanPhysiNode*)(sStreamReaderInfo->triggerAst->pNode))->scan.pScanCols;
    STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->triggerCols, TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);
    SDataBlockDescNode* pDescNode =
        ((STableScanPhysiNode*)(sStreamReaderInfo->triggerAst->pNode))->scan.node.pOutputDataBlockDesc;
    sStreamReaderInfo->triggerResBlock = createDataBlockFromDescNode(pDescNode);
    STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->triggerResBlock, TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);
    // STREAM_CHECK_RET_GOTO(buildSTSchemaForScanData(&sStreamReaderInfo->triggerSchema, sStreamReaderInfo->triggerCols));
    sStreamReaderInfo->triggerPseudoCols = ((STableScanPhysiNode*)(sStreamReaderInfo->triggerAst->pNode))->scan.pScanPseudoCols;
    if (sStreamReaderInfo->triggerPseudoCols != NULL) {
      STREAM_CHECK_RET_GOTO(
          createExprInfo(sStreamReaderInfo->triggerPseudoCols, NULL, &sStreamReaderInfo->pExprInfo, &sStreamReaderInfo->numOfExpr));
    }
    setColIdForCalcResBlock(sStreamReaderInfo->triggerPseudoCols, sStreamReaderInfo->triggerResBlock->pDataBlock);
    setColIdForCalcResBlock(sStreamReaderInfo->triggerCols, sStreamReaderInfo->triggerResBlock->pDataBlock);
  }

  // process calcCacheScanPlan
  STREAM_CHECK_RET_GOTO(nodesStringToNode(pMsg->msg.trigger.calcCacheScanPlan, (SNode**)(&sStreamReaderInfo->calcAst)));
  if (sStreamReaderInfo->calcAst != NULL) {
    STREAM_CHECK_CONDITION_GOTO(
        QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN != nodeType(sStreamReaderInfo->calcAst->pNode) &&
            QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN != nodeType(sStreamReaderInfo->calcAst->pNode),
        TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);
    
    SDataBlockDescNode* pDescNode =
        ((STableScanPhysiNode*)(sStreamReaderInfo->calcAst->pNode))->scan.node.pOutputDataBlockDesc;
    sStreamReaderInfo->calcResBlock = createDataBlockFromDescNode(pDescNode);
    STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->calcResBlock, TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);
    

    SNodeList* pseudoCols = ((STableScanPhysiNode*)(sStreamReaderInfo->calcAst->pNode))->scan.pScanPseudoCols;
    SNodeList* pScanCols = ((STableScanPhysiNode*)(sStreamReaderInfo->calcAst->pNode))->scan.pScanCols;
    setColIdForCalcResBlock(pseudoCols, sStreamReaderInfo->calcResBlock->pDataBlock);
    setColIdForCalcResBlock(pScanCols, sStreamReaderInfo->calcResBlock->pDataBlock);
    STREAM_CHECK_RET_GOTO(createOneDataBlock(sStreamReaderInfo->calcResBlock, false, &sStreamReaderInfo->calcResBlockTmp));
  }

  STREAM_CHECK_RET_GOTO(createDataBlockForTs(&sStreamReaderInfo->tsBlock));
  sStreamReaderInfo->groupIdMap =
      taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->groupIdMap, terrno);
  taosHashSetFreeFp(sStreamReaderInfo->groupIdMap, releaseGroupIdMap);

  sStreamReaderInfo->streamTaskMap =
      taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->streamTaskMap, terrno);
  taosHashSetFreeFp(sStreamReaderInfo->streamTaskMap, releaseStreamTask);

end:
  STREAM_PRINT_LOG_END(code, lino);

  if (code != 0) {
    releaseStreamReaderInfo(sStreamReaderInfo);
    sStreamReaderInfo = NULL;
  }
  return sStreamReaderInfo;
}

static SStreamTriggerReaderCalcInfo* createStreamReaderCalcInfo(void* pTask, const SStreamReaderDeployMsg* pMsg) {
  int32_t    code = 0;
  int32_t    lino = 0;
  SNodeList* triggerCols = NULL;

  SStreamTriggerReaderCalcInfo* sStreamReaderCalcInfo = taosMemoryCalloc(1, sizeof(SStreamTriggerReaderCalcInfo));
  STREAM_CHECK_NULL_GOTO(sStreamReaderCalcInfo, terrno);

  sStreamReaderCalcInfo->pTask = pTask;
  STREAM_CHECK_RET_GOTO(nodesStringToNode(pMsg->msg.calc.calcScanPlan, (SNode**)(&sStreamReaderCalcInfo->calcAst)));
  STREAM_CHECK_NULL_GOTO(sStreamReaderCalcInfo->calcAst, TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);
  if (QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN == nodeType(sStreamReaderCalcInfo->calcAst->pNode) ||
      QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN == nodeType(sStreamReaderCalcInfo->calcAst->pNode)){
    SNodeList* pScanCols = ((STableScanPhysiNode*)(sStreamReaderCalcInfo->calcAst->pNode))->scan.pScanCols;
    SNode*     nodeItem = NULL;
    FOREACH(nodeItem, pScanCols) {
      SColumnNode* valueNode = (SColumnNode*)((STargetNode*)nodeItem)->pExpr;
      if (valueNode->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
        sStreamReaderCalcInfo->pTargetNodeTs = (STargetNode*)nodeItem;
      }
    }
  }

  sStreamReaderCalcInfo->calcScanPlan = taosStrdup(pMsg->msg.calc.calcScanPlan);
  STREAM_CHECK_NULL_GOTO(sStreamReaderCalcInfo->calcScanPlan, terrno);
  sStreamReaderCalcInfo->pTaskInfo = NULL;

end:
  STREAM_PRINT_LOG_END(code, lino);

  if (code != 0) {
    releaseStreamReaderCalcInfo(sStreamReaderCalcInfo);
    sStreamReaderCalcInfo = NULL;
  }
  return sStreamReaderCalcInfo;
}

int32_t stReaderTaskDeploy(SStreamReaderTask* pTask, const SStreamReaderDeployMsg* pMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  STREAM_CHECK_NULL_GOTO(pTask, TSDB_CODE_INVALID_PARA);
  STREAM_CHECK_NULL_GOTO(pMsg, TSDB_CODE_INVALID_PARA);

  pTask->triggerReader = pMsg->triggerReader;
  if (pMsg->triggerReader == 1) {
    ST_TASK_DLOGL("triggerScanPlan:%s", (char*)(pMsg->msg.trigger.triggerScanPlan));
    ST_TASK_DLOGL("calcCacheScanPlan:%s", (char*)(pMsg->msg.trigger.calcCacheScanPlan));
    pTask->info = createStreamReaderInfo(pTask, pMsg);
    STREAM_CHECK_NULL_GOTO(pTask->info, terrno);
  } else {
    ST_TASK_DLOGL("calcScanPlan:%s", (char*)(pMsg->msg.calc.calcScanPlan));
    pTask->info = taosArrayInit(pMsg->msg.calc.execReplica, POINTER_BYTES);
    STREAM_CHECK_NULL_GOTO(pTask->info, terrno);
    for (int32_t i = 0; i < pMsg->msg.calc.execReplica; ++i) {
      SStreamTriggerReaderCalcInfo* pCalcInfo = createStreamReaderCalcInfo(pTask, pMsg);
      STREAM_CHECK_NULL_GOTO(pCalcInfo, terrno);
      STREAM_CHECK_NULL_GOTO(taosArrayPush(pTask->info, &pCalcInfo), terrno);
    }
  }
  ST_TASK_DLOG("stReaderTaskDeploy: stream %" PRIx64 " task %" PRIx64 " vgId:%d pTask:%p, info:%p", pTask->task.streamId,
         pTask->task.taskId, pTask->task.nodeId, pTask, pTask->info);

  pTask->task.status = STREAM_STATUS_INIT;

end:

  STREAM_PRINT_LOG_END(code, lino);

  if (code) {
    pTask->task.status = STREAM_STATUS_FAILED;
  }

  return code;
}

int32_t stReaderTaskUndeployImpl(SStreamReaderTask** ppTask, const SStreamUndeployTaskMsg* pMsg, taskUndeplyCallback cb) {
  int32_t code = 0;
  int32_t lino = 0;
  STREAM_CHECK_NULL_GOTO(ppTask, TSDB_CODE_INVALID_PARA);
  STREAM_CHECK_NULL_GOTO(pMsg, TSDB_CODE_INVALID_PARA);
  if ((*ppTask)->triggerReader == 1) {
    releaseStreamReaderInfo((*ppTask)->info);
  } else {
    taosArrayDestroyP((*ppTask)->info, releaseStreamReaderCalcInfo);
  }
  (*ppTask)->info = NULL;

end:
  STREAM_PRINT_LOG_END(code, lino);
  (*cb)(ppTask);

  return code;
}


int32_t stReaderTaskUndeploy(SStreamReaderTask** ppTask, bool force) {
  int32_t            code = TSDB_CODE_SUCCESS;
  SStreamReaderTask *pTask = *ppTask;
  
  if (!force && taosWTryForceLockLatch(&pTask->task.entryLock)) {
    ST_TASK_DLOG("ignore undeploy reader task since working, entryLock:%x", pTask->task.entryLock);
    return code;
  }

  return stReaderTaskUndeployImpl(ppTask, &pTask->task.undeployMsg, pTask->task.undeployCb);
}
// int32_t stReaderTaskExecute(SStreamReaderTask* pTask, SStreamMsg* pMsg);
// void qStreamSetGroupId(void* pTableListInfo, SSDataBlock* pBlock) {
//   pBlock->ino.id.groupId = tableListGetTableGroupId(pTableListInfo, pBlock->info.id.uid);
// }

void* qStreamGetReaderInfo(int64_t streamId, int64_t taskId, void** taskAddr) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SStreamTask* pTask = NULL;
  STREAM_CHECK_RET_GOTO(streamAcquireTask(streamId, taskId, &pTask, taskAddr));

  pTask->status = STREAM_STATUS_RUNNING;

end:
  STREAM_PRINT_LOG_END(code, lino);
  if (code == TSDB_CODE_SUCCESS) {
    ST_TASK_DLOG("qStreamGetReaderInfo, pTask:%p, info:%p", pTask, ((SStreamReaderTask*)pTask)->info);
    return ((SStreamReaderTask*)pTask)->info;
  }
  terrno = code;
  return NULL;
}


int32_t streamBuildFetchRsp(SArray* pResList, bool hasNext, void** data, size_t* size, int8_t precision) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf = NULL;

  int32_t blockNum = 0;
  size_t  dataEncodeBufSize = sizeof(SRetrieveTableRsp);
  for(size_t i = 0; i < taosArrayGetSize(pResList); i++){
    SSDataBlock* pBlock = taosArrayGetP(pResList, i);
    if (pBlock == NULL || pBlock->info.rows == 0) continue;
    int32_t blockSize = blockGetEncodeSize(pBlock);
    dataEncodeBufSize += (INT_BYTES * 2 + blockSize);
    blockNum++;
  }
  buf = rpcMallocCont(dataEncodeBufSize);
  STREAM_CHECK_NULL_GOTO(buf, terrno);

  SRetrieveTableRsp* pRetrieve = (SRetrieveTableRsp*)buf;
  pRetrieve->version = 0;
  pRetrieve->precision = precision;
  pRetrieve->compressed = 0;
  pRetrieve->completed = hasNext ? 0 : 1;
  pRetrieve->numOfRows = 0;
  pRetrieve->numOfBlocks = htonl(blockNum);

  char* dataBuf = (char*)(pRetrieve->data);
  for(size_t i = 0; i < taosArrayGetSize(pResList); i++){
    SSDataBlock* pBlock = taosArrayGetP(pResList, i);
    if (pBlock == NULL || pBlock->info.rows == 0) continue;
    int32_t blockSize = blockGetEncodeSize(pBlock);
    *((int32_t*)(dataBuf)) = blockSize;
    *((int32_t*)(dataBuf + INT_BYTES)) = blockSize;
    pRetrieve->numOfRows += pBlock->info.rows;
    int32_t actualLen =
        blockEncode(pBlock, dataBuf + INT_BYTES * 2, blockSize, taosArrayGetSize(pBlock->pDataBlock));
    STREAM_CHECK_CONDITION_GOTO(actualLen < 0, terrno);
    dataBuf += (INT_BYTES * 2 + actualLen);
  }
  stDebug("stream fetch get result blockNum:%d, rows:%" PRId64, blockNum, pRetrieve->numOfRows);

  pRetrieve->numOfRows = htobe64(pRetrieve->numOfRows);
  
  *data = buf;
  *size = dataEncodeBufSize;
  buf = NULL;

end:
  rpcFreeCont(buf);
  return code;
}


