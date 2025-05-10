#include "streamReader.h"
#include "osMemPool.h"
#include "streamInt.h"

void releaseStreamTask(void* p) {
  if (p == NULL) return;
  SStreamReaderTaskInner* pTask = *((SStreamReaderTaskInner**)p);
  if (pTask == NULL) return;
  taosHashCleanup(pTask->pIgnoreTables);
  blockDataDestroy(pTask->pResBlock);
  blockDataDestroy(pTask->pResBlockDst);
  qStreamDestroyTableList(pTask->pTableList);
  pTask->api.tsdReader.tsdReaderClose(pTask->pReader);
  filterFreeInfo(pTask->pFilterInfo);
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
  PRINT_LOG_END(code, lino)
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    pBlock = NULL;
  }
  *pBlockRet = pBlock;
  return code;
}

int32_t qStreamInitQueryTableDataCond(SQueryTableDataCond* pCond, int32_t order, void* schemas, bool isSchema,
                                      STimeWindow twindows, uint64_t suid) {
  int32_t code = 0;
  int32_t lino = 0;
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
  pCond->endVersion = -1;
  //  pCond->skipRollup = readHandle->skipRollup;

  pCond->notLoadData = false;

  for (int32_t i = 0; i < pCond->numOfCols; ++i) {
    SColumnInfo* pColInfo = &pCond->colList[i];
    if (isSchema) {
      SSchema* pSchema = taosArrayGet((SArray*)schemas, i);
      pCond->colList[i].type = pSchema[i].type;
      pCond->colList[i].bytes = pSchema[i].bytes;
      pCond->colList[i].colId = pSchema[i].colId;
      pCond->colList[i].pk = pSchema[i].flags & COL_IS_KEY;

      pCond->pSlotList[i] = i;
    } else {
      STargetNode* pNode = (STargetNode*)nodesListGetNode((SNodeList*)schemas, i);
      STREAM_CHECK_NULL_GOTO(pNode, TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);

      SColumnNode* pColNode = (SColumnNode*)pNode->pExpr;
      STREAM_CHECK_NULL_GOTO(pColNode, TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);

      if (pColNode->colType == COLUMN_TYPE_TAG) {
        continue;
      }

      pCond->colList[i].type = pColNode->node.resType.type;
      pCond->colList[i].bytes = pColNode->node.resType.bytes;
      pCond->colList[i].colId = pColNode->colId;
      pCond->colList[i].pk = pColNode->isPk;

      pCond->pSlotList[i] = pNode->slotId;
    }
  }

end:
  PRINT_LOG_END(code, lino);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pCond->colList);
    taosMemoryFree(pCond->pSlotList);
    pCond->colList = NULL;
    pCond->pSlotList = NULL;
  }
  return code;
}

int32_t createStreamTask(void* pVnode, SStreamTriggerReaderTaskInnerOptions* options, SStreamReaderTaskInner** ppTask,
                         SSDataBlock* pResBlock) {
  int32_t                 code = 0;
  int32_t                 lino = 0;
  SStreamReaderTaskInner* pTask = taosMemoryCalloc(1, sizeof(SStreamReaderTaskInner));
  STREAM_CHECK_NULL_GOTO(pTask, terrno);
  initStorageAPI(&pTask->api);
  pTask->pIgnoreTables = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  STREAM_CHECK_NULL_GOTO(pTask->pIgnoreTables, terrno);

  pTask->options = *options;
  if (pResBlock != NULL) {
    STREAM_CHECK_RET_GOTO(copyDataBlock(pTask->pResBlock, pResBlock));
  } else {
    STREAM_CHECK_RET_GOTO(createDataBlockForStream(options->schemas, &pTask->pResBlock));
  }
  STREAM_CHECK_RET_GOTO(filterInitFromNode(options->pConditions, &pTask->pFilterInfo, 0, NULL));
  STREAM_CHECK_RET_GOTO(qStreamCreateTableListForReader(pVnode, options->suid, options->uid, options->tableType,
                                                        options->partitionCols, options->groupSort, options->pTagCond,
                                                        options->pTagIndexCond, &pTask->api, &pTask->pTableList));
  if (options->gid != 0) {
    int32_t index = qStreamGetGroupIndex(pTask->pTableList, options->gid);
    STREAM_CHECK_CONDITION_GOTO(index < 0, TSDB_CODE_INVALID_PARA);
    pTask->currentGroupIndex = index;
  }

  int32_t        pNum = 0;
  STableKeyInfo* pList = NULL;
  if (options->scanMode == STREAM_SCAN_GROUP_ONE_BY_ONE) {
    STREAM_CHECK_RET_GOTO(qStreamGetTableList(pTask->pTableList, pTask->currentGroupIndex, &pList, &pNum))
  } else if (options->scanMode == STREAM_SCAN_ALL) {
    STREAM_CHECK_RET_GOTO(qStreamGetTableList(pTask->pTableList, -1, &pList, &pNum))
  }

  SQueryTableDataCond pCond = {0};
  STREAM_CHECK_RET_GOTO(qStreamInitQueryTableDataCond(&pCond, options->order, options->schemas, options->isSchema,
                                                      options->twindows, options->suid));
  STREAM_CHECK_RET_GOTO(pTask->api.tsdReader.tsdReaderOpen(
      pVnode, &pCond, pList, pNum, pTask->pResBlock, (void**)&pTask->pReader, pTask->idStr, &pTask->pIgnoreTables));
  *ppTask = pTask;
  pTask = NULL;

end:
  PRINT_LOG_END(code, lino);
  releaseStreamTask(pTask);
  return code;
}

static void releaseStreamInfo(void* p) {
  if (p == NULL) return;
  SStreamTriggerReaderInfo* pInfo = (SStreamTriggerReaderInfo*)p;
  if (pInfo == NULL) return;
  taosHashCleanup(pInfo->streamTaskMap);
  pInfo->streamTaskMap = NULL;
  nodesDestroyNode((SNode*)(pInfo->triggerAst));
  nodesDestroyNode((SNode*)(pInfo->calcAst));
  nodesDestroyList(pInfo->triggerCols);
  nodesDestroyList(pInfo->calcCols);
  blockDataDestroy(pInfo->triggerResBlock);
  blockDataDestroy(pInfo->calcResBlock);
  taosMemoryFree(pInfo->triggerSchema);
  taosMemoryFree(pInfo->calcSchema);
  taosMemoryFree(pInfo);
}

static int32_t compareSlotId(SNode* pNode1, SNode* pNode2) {
  SColumnNode* pC1 = (SColumnNode*)pNode1;
  SColumnNode* pC2 = (SColumnNode*)pNode2;
  if (pC1->slotId < pC2->slotId)
    return -1;
  else if (pC1->slotId > pC2->slotId)
    return 1;
  else {
    return 0;
  }
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

static int32_t buildSTSchemaAndSetColId(STSchema** sSchema, SNodeList* list, SSDataBlock* pResBlock) {
  int32_t  code = 0;
  int32_t  lino = 0;
  int32_t  nCols = LIST_LENGTH(list);
  SSchema* pSchema = (SSchema*)taosMemoryCalloc(nCols, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(pSchema, terrno);
  SNode*  nodeItem = NULL;
  int32_t i = 0;
  FOREACH(nodeItem, list) {
    SColumnNode*     valueNode = (SColumnNode*)((STargetNode*)nodeItem)->pExpr;
    int32_t          slotId = ((STargetNode*)nodeItem)->slotId;
    SColumnInfoData* pColData = taosArrayGet(pResBlock->pDataBlock, slotId);
    STREAM_CHECK_NULL_GOTO(pColData, terrno);
    pColData->info.colId = valueNode->colId;
    pSchema[i].type = valueNode->node.resType.type;
    pSchema[i].colId = valueNode->colId;
    pSchema[i].bytes = valueNode->node.resType.bytes;
    i++;
  }

  *sSchema = tBuildTSchema(pSchema, nCols, 0);
  STREAM_CHECK_NULL_GOTO(*sSchema, terrno);

end:
  PRINT_LOG_END(code, lino);

  taosMemoryFree(pSchema);
  return code;
}

static SStreamTriggerReaderInfo* createStreamReaderInfo(const SStreamReaderDeployMsg* pMsg) {
  int32_t    code = 0;
  int32_t    lino = 0;
  SNodeList* triggerCols = NULL;

  SStreamTriggerReaderInfo* sStreamReaderInfo = taosMemoryCalloc(1, sizeof(SStreamTriggerReaderInfo));
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);

  sStreamReaderInfo->tableType = pMsg->msg.trigger.triggerTblType;
  if (pMsg->msg.trigger.triggerTblType == TD_SUPER_TABLE) {
    sStreamReaderInfo->suid = pMsg->msg.trigger.triggerTblUid;
  } else {
    sStreamReaderInfo->uid = pMsg->msg.trigger.triggerTblUid;
  }

  sStreamReaderInfo->twindows.skey = INT64_MIN;
  sStreamReaderInfo->twindows.ekey = INT64_MAX;
  sStreamReaderInfo->pTagCond = NULL;
  sStreamReaderInfo->pTagIndexCond = NULL;
  sStreamReaderInfo->pConditions = NULL;
  STREAM_CHECK_RET_GOTO(nodesStringToList(pMsg->msg.trigger.partitionCols, &sStreamReaderInfo->partitionCols));
  sStreamReaderInfo->deleteReCalc = pMsg->msg.trigger.deleteReCalc;
  sStreamReaderInfo->deleteOutTbl = pMsg->msg.trigger.deleteOutTbl;
  // pMsg->msg.trigger.calcCacheScanPlan;
  // STREAM_CHECK_RET_GOTO(nodesStringToList(pMsg->msg.trigger.triggerCols, &triggerCols));
  // sStreamReaderInfo->triggerCols = taosArrayInit(LIST_LENGTH(triggerCols), sizeof(SSchema));
  // STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->triggerCols, terrno);
  // nodesSortList(&triggerCols, compareSlotId);
  // SNode* nodeItem = NULL;
  // FOREACH(nodeItem, triggerCols) {
  //   SColumnNode *valueNode = (SColumnNode *)nodeItem;
  //   STREAM_CHECK_RET_GOTO(qStreamBuildSchema(sStreamReaderInfo->triggerCols, valueNode->node.resType.type,
  //                     valueNode->node.resType.bytes, valueNode->colId));
  // }

  // process triggerScanPlan
  STREAM_CHECK_RET_GOTO(
      nodesStringToNode(pMsg->msg.trigger.triggerScanPlan, (SNode**)(&sStreamReaderInfo->triggerAst)));
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->triggerAst, TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);
  STREAM_CHECK_CONDITION_GOTO(QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN != nodeType(sStreamReaderInfo->triggerAst->pNode),
                              TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);
  sStreamReaderInfo->pTagCond = sStreamReaderInfo->triggerAst->pTagCond;
  sStreamReaderInfo->pTagIndexCond = sStreamReaderInfo->triggerAst->pTagIndexCond;
  sStreamReaderInfo->pConditions = sStreamReaderInfo->triggerAst->pNode->pConditions;
  sStreamReaderInfo->partitionCols = ((STableScanPhysiNode*)(sStreamReaderInfo->triggerAst->pNode))->pGroupTags;

  sStreamReaderInfo->triggerCols = ((STableScanPhysiNode*)(sStreamReaderInfo->triggerAst->pNode))->scan.pScanCols;
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->triggerCols, TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);
  SDataBlockDescNode* pDescNode =
      ((STableScanPhysiNode*)(sStreamReaderInfo->triggerAst->pNode))->scan.node.pOutputDataBlockDesc;
  sStreamReaderInfo->triggerResBlock = createDataBlockFromDescNode(pDescNode);
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->triggerResBlock, TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);
  STREAM_CHECK_RET_GOTO(buildSTSchemaAndSetColId(&sStreamReaderInfo->triggerSchema, sStreamReaderInfo->triggerCols,
                                                 sStreamReaderInfo->triggerResBlock));

  // process calcCacheScanPlan
  STREAM_CHECK_RET_GOTO(nodesStringToNode(pMsg->msg.trigger.calcCacheScanPlan, (SNode**)(&sStreamReaderInfo->calcAst)));
  if (sStreamReaderInfo->calcAst != NULL) {
    STREAM_CHECK_CONDITION_GOTO(QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN != nodeType(sStreamReaderInfo->calcAst->pNode),
                                TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);
    sStreamReaderInfo->calcCols = ((STableScanPhysiNode*)(sStreamReaderInfo->calcAst->pNode))->scan.pScanCols;
    STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->calcCols, TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);
    // sStreamReaderInfo->calcCols = taosArrayInit(LIST_LENGTH(calcCols), sizeof(SSchema));
    // STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->calcCols, terrno);
    // SNode* nodeItem = NULL;
    // FOREACH(nodeItem, calcCols) {
    //   SColumnNode *valueNode = (SColumnNode *)nodeItem;
    //   STREAM_CHECK_RET_GOTO(qStreamBuildSchema(sStreamReaderInfo->calcCols, valueNode->node.resType.type,
    //                     valueNode->node.resType.bytes, valueNode->colId));
    // }
    SDataBlockDescNode* pDescNode =
        ((STableScanPhysiNode*)(sStreamReaderInfo->calcAst->pNode))->scan.node.pOutputDataBlockDesc;
    sStreamReaderInfo->calcResBlock = createDataBlockFromDescNode(pDescNode);
    STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->calcResBlock, TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);

    STREAM_CHECK_RET_GOTO(buildSTSchemaAndSetColId(&sStreamReaderInfo->calcSchema, sStreamReaderInfo->calcCols,
                                                   sStreamReaderInfo->calcResBlock));
  }

  sStreamReaderInfo->streamTaskMap =
      taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->streamTaskMap, terrno);
  taosHashSetFreeFp(sStreamReaderInfo->streamTaskMap, releaseStreamTask);

end:
  PRINT_LOG_END(code, lino);

  if (code != 0) {
    releaseStreamInfo(sStreamReaderInfo);
    sStreamReaderInfo = NULL;
  }
  nodesDestroyList(triggerCols);
  return sStreamReaderInfo;
}

int32_t stReaderTaskDeploy(SStreamReaderTask* pTask, const SStreamReaderDeployMsg* pMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  STREAM_CHECK_NULL_GOTO(pTask, TSDB_CODE_INVALID_PARA);
  STREAM_CHECK_NULL_GOTO(pMsg, TSDB_CODE_INVALID_PARA);

  pTask->triggerReader = pMsg->triggerReader;
  if (pMsg->triggerReader == 1) {
    stDebug("triggerScanPlan:%s", (char*)(pMsg->msg.trigger.triggerScanPlan));
    stDebug("calcCacheScanPlan:%s", (char*)(pMsg->msg.trigger.calcCacheScanPlan));
    pTask->info.triggerReaderInfo = createStreamReaderInfo(pMsg);
    stInfo("stream %" PRIx64 " task %" PRIx64 "  in stReaderTaskDeploy, pTask:%p, info:%p", pTask->task.streamId, pTask->task.taskId, pTask, pTask->info.triggerReaderInfo);

    STREAM_CHECK_NULL_GOTO(pTask->info.triggerReaderInfo, terrno);
  } else {
    stDebug("calcScanPlan:%s", (char*)(pMsg->msg.calc.calcScanPlan));
    // int32_t vgId = pTask->task.nodeId;
    // int64_t streamId = pTask->task.streamId;
    // int32_t taskId = pTask->task.taskId;

    // ST_TASK_DLOG("vgId:%d start to build stream reader calc task", vgId);
    pTask->info.calcReaderInfo.calcScanPlan = taosStrdup(pMsg->msg.calc.calcScanPlan);
    STREAM_CHECK_NULL_GOTO(pTask->info.calcReaderInfo.calcScanPlan, terrno);
    pTask->info.calcReaderInfo.pTaskInfo = NULL;
  }

  pTask->task.status = STREAM_STATUS_INIT;

end:

  PRINT_LOG_END(code, lino);

  if (code) {
    pTask->task.status = STREAM_STATUS_FAILED;
  }

  return code;
}

int32_t stReaderTaskUndeploy(SStreamReaderTask** ppTask, const SStreamUndeployTaskMsg* pMsg, taskUndeplyCallback cb) {
  int32_t code = 0;
  int32_t lino = 0;
  STREAM_CHECK_NULL_GOTO(ppTask, TSDB_CODE_INVALID_PARA);
  STREAM_CHECK_NULL_GOTO(pMsg, TSDB_CODE_INVALID_PARA);
  if ((*ppTask)->triggerReader == 1) {
    releaseStreamInfo((*ppTask)->info.triggerReaderInfo);
    (*ppTask)->info.triggerReaderInfo = NULL;
  } else {
    taosMemoryFreeClear((*ppTask)->info.calcReaderInfo.calcScanPlan);
    qDestroyTask((*ppTask)->info.calcReaderInfo.pTaskInfo);
    (*ppTask)->info.calcReaderInfo.pTaskInfo = NULL;
  }

end:
  PRINT_LOG_END(code, lino);
  (*cb)(ppTask);

  return code;
}
// int32_t stReaderTaskExecute(SStreamReaderTask* pTask, SStreamMsg* pMsg);
// void qStreamSetGroupId(void* pTableListInfo, SSDataBlock* pBlock) {
//   pBlock->ino.id.groupId = tableListGetTableGroupId(pTableListInfo, pBlock->info.id.uid);
// }

void* qStreamGetReaderInfo(int64_t streamId, int64_t taskId) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SStreamTask* pTask = NULL;
  STREAM_CHECK_RET_GOTO(streamGetTask(streamId, taskId, &pTask));

end:
  PRINT_LOG_END(code, lino);
  if (code == TSDB_CODE_SUCCESS) {
    stInfo("stream %" PRIx64 " task %" PRIx64 "  in qStreamGetReaderInfo, pTask:%p, info:%p", streamId, taskId, pTask, ((SStreamReaderTask*)pTask)->info.triggerReaderInfo);
    return ((SStreamReaderTask*)pTask)->info.triggerReaderInfo;
  }
  terrno = code;
  return NULL;
}
