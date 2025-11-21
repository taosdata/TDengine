#include "streamReader.h"
#include <stdint.h>
#include <tdef.h>
#include "osMemPool.h"
#include "osMemory.h"
#include "streamInt.h"
#include "executor.h"
#include "tarray.h"
#include "tdatablock.h"
#include "tdef.h"
#include "thash.h"
#include "tsimplehash.h"
#include "tcommon.h"

void qStreamDestroyTableInfo(StreamTableListInfo* pTableListInfo) { 
  if (pTableListInfo == NULL) return;
  taosArrayDestroyP(pTableListInfo->pTableList, taosMemFree);
  taosHashCancelIterate(pTableListInfo->gIdMap, pTableListInfo->pIter);
  taosHashCleanup(pTableListInfo->gIdMap);
  taosHashCleanup(pTableListInfo->uIdMap);
}

static int32_t removeList(SHashObj* idMap, SStreamTableKeyInfo* table, uint64_t key){
  int32_t code = 0;
  int32_t lino = 0;
  SStreamTableList* list = taosHashGet(idMap, &key, LONG_BYTES);
  if (list == NULL) {
    stError("stream reader remove table list failed, groupId not exist, key:%"PRIu64, key);
    code = TSDB_CODE_NOT_FOUND;
    goto end;
  } 
  if (list->head == table && list->tail == table) {
    // only one element
    list->head = NULL;
    list->tail = NULL;
    list->size = 0;
    code = taosHashRemove(idMap, &key, LONG_BYTES);
    if (code != 0) {
      stError("stream reader remove table list failed, remove groupId failed, key:%"PRIu64, key);
      goto end;
    }
  } else if (list->head == table) {
    // first element
    list->head = table->next;
    list->head->prev = NULL;
    list->size -= 1;
  } else if (list->tail == table) {
    // last element
    list->tail = table->prev;
    list->tail->next = NULL;
    list->size -= 1;
  } else {
    // middle element
    table->prev->next = table->next;
    table->next->prev = table->prev;
    list->size -= 1;
  }
end:
  return code;
}

static int32_t addList(SHashObj* idMap, SStreamTableKeyInfo* table, uint64_t key){
  int32_t code = 0;
  int32_t lino = 0;

  SStreamTableList* list = taosHashGet(idMap, &key, LONG_BYTES);
  if (list == NULL) {
    SStreamTableList tmp  = {.head = table, .tail = table, .size = 1};
    STREAM_CHECK_RET_GOTO(taosHashPut(idMap, &key, LONG_BYTES, &tmp, sizeof(SStreamTableList)));
  } else {
    list->tail->next = table;
    table->prev = list->tail;
    list->tail = table;
    list->size += 1;
  }

end:
  return code;
}

int32_t initStreamTableListInfo(StreamTableListInfo* pTableListInfo){
  int32_t                   code = 0;
  int32_t                   lino = 0;
  if (pTableListInfo->pTableList == NULL) {
    pTableListInfo->pTableList = taosArrayInit(4, POINTER_BYTES);
    STREAM_CHECK_NULL_GOTO(pTableListInfo->pTableList, terrno);
  }
  if (pTableListInfo->gIdMap == NULL) {
    pTableListInfo->gIdMap = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
    STREAM_CHECK_NULL_GOTO(pTableListInfo->gIdMap, terrno);
  }
  if (pTableListInfo->uIdMap == NULL) {
    pTableListInfo->uIdMap = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
    STREAM_CHECK_NULL_GOTO(pTableListInfo->uIdMap, terrno);
  }

end:
  return code;
}

int32_t  qStreamSetTableList(StreamTableListInfo* pTableListInfo, int64_t uid, uint64_t gid){
  int32_t code = 0;
  int32_t lino = 0;

  stDebug("stream reader set table list, uid:%"PRIu64", gid:%"PRIu64, uid, gid);
  STREAM_CHECK_RET_GOTO(initStreamTableListInfo(pTableListInfo));
  SStreamTableKeyInfo* keyInfo = taosMemoryCalloc(1, sizeof(SStreamTableKeyInfo));
  STREAM_CHECK_NULL_GOTO(keyInfo, terrno);
  *keyInfo = (SStreamTableKeyInfo){.uid = uid, .groupId = gid, .markedDeleted = false, .prev = NULL, .next = NULL};
  if (taosArrayPush(pTableListInfo->pTableList, &keyInfo) == NULL) {
    taosMemoryFreeClear(keyInfo);
    code = terrno;
    goto end;
  }

  STREAM_CHECK_RET_GOTO(addList(pTableListInfo->gIdMap, keyInfo, gid));

  SStreamTableMapElement element = {.table = keyInfo, .index = taosArrayGetSize(pTableListInfo->pTableList) - 1};
  STREAM_CHECK_RET_GOTO(taosHashPut(pTableListInfo->uIdMap, &uid, LONG_BYTES, &element, sizeof(element)));

end:
  return code;
}

int32_t  qStreamRemoveTableList(StreamTableListInfo* pTableListInfo, int64_t uid){
  int32_t code = 0;
  int32_t lino = 0;

  STREAM_CHECK_NULL_GOTO(pTableListInfo->pTableList, terrno);
  STREAM_CHECK_NULL_GOTO(pTableListInfo->gIdMap, terrno);
  STREAM_CHECK_NULL_GOTO(pTableListInfo->uIdMap, terrno);
  SStreamTableMapElement* info = taosHashGet(pTableListInfo->uIdMap, &uid, LONG_BYTES);
  if (info == NULL) {
    goto end;
  }

  STREAM_CHECK_RET_GOTO(removeList(pTableListInfo->gIdMap, info->table, info->table->groupId));
  
  SStreamTableKeyInfo* tmp = taosArrayGetP(pTableListInfo->pTableList, info->index);
  if (tmp != NULL) {
    tmp->markedDeleted = true;
  }
  code = taosHashRemove(pTableListInfo->uIdMap, &uid, LONG_BYTES);
  
end:
  return code;
}

static void* copyTableInfo(void* p) {
  SStreamTableKeyInfo* src = (SStreamTableKeyInfo*)p;
  SStreamTableKeyInfo* dst = taosMemoryMalloc(sizeof(SStreamTableKeyInfo));
  if (dst != NULL) {
    *dst = *src;
    dst->prev = NULL;
    dst->next = NULL;
  }
  return dst;
} 

int32_t  qStreamCopyTableInfo(SStreamTriggerReaderInfo* sStreamReaderInfo, StreamTableListInfo* dst){
  int32_t code = 0;
  int32_t lino = 0;
  taosRLockLatch(&sStreamReaderInfo->lock);
  StreamTableListInfo* src = sStreamReaderInfo->isVtableStream ? &sStreamReaderInfo->vSetTableList : &sStreamReaderInfo->tableList;
  int32_t totalSize = taosArrayGetSize(src->pTableList);
  for (int32_t i = 0; i < totalSize; ++i) {
    SStreamTableKeyInfo* info = taosArrayGetP(src->pTableList, i);
    if (info == NULL) {
      continue;
    }
    SStreamTableMapElement* element = taosHashGet(src->uIdMap, &info->uid, LONG_BYTES);
    if (info->markedDeleted || element == NULL) {
      continue;
    }
    STREAM_CHECK_RET_GOTO(qStreamSetTableList(dst, info->uid, info->groupId));
  }
end:
   taosRUnLockLatch(&sStreamReaderInfo->lock);
  return code;
}

SArray* qStreamGetTableArrayList(SStreamTriggerReaderInfo* sStreamReaderInfo) { 
  taosRLockLatch(&sStreamReaderInfo->lock);
  SArray* pTableList = taosArrayDup(sStreamReaderInfo->tableList.pTableList, copyTableInfo);
  taosRUnLockLatch(&sStreamReaderInfo->lock);
  return pTableList;
}

int32_t  qStreamGetTableListNum(SStreamTriggerReaderInfo* sStreamReaderInfo){
  taosRLockLatch(&sStreamReaderInfo->lock);
  StreamTableListInfo* tmp = sStreamReaderInfo->isVtableStream ? &sStreamReaderInfo->vSetTableList : &sStreamReaderInfo->tableList;
  int32_t num = taosArrayGetSize(tmp->pTableList);
  taosRUnLockLatch(&sStreamReaderInfo->lock);
  return num;
}

int32_t  qStreamGetTableListGroupNum(SStreamTriggerReaderInfo* sStreamReaderInfo){
  taosRLockLatch(&sStreamReaderInfo->lock);
  StreamTableListInfo* tmp = sStreamReaderInfo->isVtableStream ? &sStreamReaderInfo->vSetTableList : &sStreamReaderInfo->tableList;
  int32_t num = taosHashGetSize(tmp->gIdMap);
  taosRUnLockLatch(&sStreamReaderInfo->lock);
  return num;
}

static uint64_t qStreamGetGroupId(StreamTableListInfo* tmp, int64_t uid){
  uint64_t groupId = -1;
  SStreamTableMapElement* info = taosHashGet(tmp->uIdMap, &uid, LONG_BYTES);
  if (info != NULL) {
    groupId = info->table->groupId;
  }
  return groupId;
}

uint64_t qStreamGetGroupIdFromOrigin(SStreamTriggerReaderInfo* sStreamReaderInfo, int64_t uid){
  StreamTableListInfo* tmp = &sStreamReaderInfo->tableList;
  uint64_t groupId = qStreamGetGroupId(tmp, uid);
  return groupId;
}

uint64_t qStreamGetGroupIdFromSet(SStreamTriggerReaderInfo* sStreamReaderInfo, int64_t uid){
  uint64_t groupId = uid;
  taosRLockLatch(&sStreamReaderInfo->lock);
  if (!sStreamReaderInfo->isVtableStream){
    groupId = qStreamGetGroupId(&sStreamReaderInfo->tableList, uid);
  }
  taosRUnLockLatch(&sStreamReaderInfo->lock);
  return groupId;
}

static int32_t buildTableListFromList(STableKeyInfo** pKeyInfo, int32_t* size, SStreamTableList* list){
  *size = list->size;
  *pKeyInfo = taosMemoryCalloc(*size, sizeof(STableKeyInfo));
  if (*pKeyInfo == NULL) {
    return terrno;
  }
  SStreamTableKeyInfo* iter = list->head;
  STableKeyInfo* kInfo = *pKeyInfo;
  while (iter != NULL) {
    stDebug("stream reader get table list, uid:%"PRIu64", gid:%"PRIu64, iter->uid, iter->groupId);
    kInfo->uid = iter->uid;
    kInfo->groupId = iter->groupId;
    iter = iter->next;
    kInfo++;
  }
  return 0;
}

static int32_t buildTableListFromArray(STableKeyInfo** pKeyInfo, int32_t* size, SArray* pTableList){
  int32_t totalSize = taosArrayGetSize(pTableList);
  *size = totalSize;
  *pKeyInfo = taosMemoryCalloc(*size, sizeof(STableKeyInfo));
  if (*pKeyInfo == NULL) {
    return terrno;
  }
  STableKeyInfo* kInfo = *pKeyInfo;
  for (int32_t i = 0; i < totalSize; ++i) {
    SStreamTableKeyInfo* info = taosArrayGetP(pTableList, i);
    if (info == NULL || info->markedDeleted) {
      continue;
    }
    kInfo->uid = info->uid;
    kInfo->groupId = info->groupId;
    kInfo++;
  }
  return 0;
}

int32_t qStreamGetTableList(SStreamTriggerReaderInfo* sStreamReaderInfo, uint64_t gid, STableKeyInfo** pKeyInfo, int32_t* size) {
  int32_t      code = 0;
  int32_t      lino = 0;
  if (pKeyInfo == NULL || size == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  void* pTask = sStreamReaderInfo->pTask;
  *size = 0;
  *pKeyInfo = NULL;
  taosRLockLatch(&sStreamReaderInfo->lock);
  StreamTableListInfo* tmp = sStreamReaderInfo->isVtableStream ? &sStreamReaderInfo->vSetTableList : &sStreamReaderInfo->tableList;
  if (gid == 0) {   // return all tables
    STREAM_CHECK_RET_GOTO(buildTableListFromArray(pKeyInfo, size, tmp->pTableList));
    goto end;
  }
  SStreamTableList* list = taosHashGet(tmp->gIdMap, &gid, LONG_BYTES);
  if (list == NULL) {
    ST_TASK_DLOG("%s not found gid:%"PRId64, __func__, gid);
    goto end;
  }

  STREAM_CHECK_RET_GOTO(buildTableListFromList(pKeyInfo, size, list));
end:
  taosRUnLockLatch(&sStreamReaderInfo->lock);
  return code;
}

int32_t qStreamIterTableList(StreamTableListInfo* tableInfo, STableKeyInfo** pKeyInfo, int32_t* size, int64_t* suid) {
  int32_t      code = 0;
  int32_t      lino = 0;
  if (pKeyInfo == NULL || size == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  *size = 0;
  *pKeyInfo = NULL;
  tableInfo->pIter = taosHashIterate(tableInfo->gIdMap, tableInfo->pIter);
  STREAM_CHECK_NULL_GOTO(tableInfo->pIter, code);

  int64_t* key = (int64_t*)taosHashGetKey(tableInfo->pIter, NULL);
  *suid = *key;
  stDebug("stream reader iter table list, suid:%"PRId64, *suid);
  SStreamTableList* list = (SStreamTableList*)(tableInfo->pIter);
  STREAM_CHECK_RET_GOTO(buildTableListFromList(pKeyInfo, size, list));
end:
  return code;
}

int32_t qBuildVTableList(SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t      code = 0;
  int32_t      lino = 0;
  int32_t iter = 0;
  void* pTask = sStreamReaderInfo->pTask;
  void*   px = tSimpleHashIterate(sStreamReaderInfo->uidHashTrigger, NULL, &iter);
  while (px != NULL) {
    int64_t* id = tSimpleHashGetKey(px, NULL);
    STREAM_CHECK_RET_GOTO(qStreamSetTableList(&sStreamReaderInfo->vSetTableList, *(id+1), *id));
    px = tSimpleHashIterate(sStreamReaderInfo->uidHashTrigger, px, &iter);
    ST_TASK_DLOG("%s build tablelist for vtable, suid:%"PRId64" uid:%"PRId64, __func__, *id, *(id+1));
  }
  
end:
  return code;
}

void releaseStreamTask(void* p) {
  if (p == NULL) return;
  SStreamReaderTaskInner* pTask = *((SStreamReaderTaskInner**)p);
  if (pTask == NULL) return;
  blockDataDestroy(pTask->pResBlock);
  blockDataDestroy(pTask->pResBlockDst);
  pTask->storageApi->tsdReader.tsdReaderClose(pTask->pReader);
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
  // STREAM_PRINT_LOG_END(code, lino)
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
                                      STimeWindow twindows, uint64_t suid, int64_t ver, int32_t** pSlotList) {
  int32_t code = 0;
  int32_t lino = 0;

  memset(pCond, 0, sizeof(*pCond));

  pCond->order = order;
  pCond->numOfCols = isSchema ? taosArrayGetSize((SArray*)schemas) : LIST_LENGTH((SNodeList*)schemas);
  pCond->pSlotList = pSlotList != NULL ? *pSlotList : taosMemoryMalloc(sizeof(int32_t) * pCond->numOfCols);
  STREAM_CHECK_NULL_GOTO(pCond->pSlotList, terrno);

  pCond->colList = taosMemoryCalloc(pCond->numOfCols, sizeof(SColumnInfo));
  STREAM_CHECK_NULL_GOTO(pCond->colList, terrno);
  

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

      if (pSlotList == NULL ) pCond->pSlotList[i] = i;
    } else {
      STargetNode* pNode = (STargetNode*)nodesListGetNode((SNodeList*)schemas, i);
      STREAM_CHECK_NULL_GOTO(pNode, TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);

      SColumnNode* pColNode = (SColumnNode*)pNode->pExpr;
      STREAM_CHECK_NULL_GOTO(pColNode, TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);

      pCond->colList[i].type = pColNode->node.resType.type;
      pCond->colList[i].bytes = pColNode->node.resType.bytes;
      pCond->colList[i].colId = pColNode->colId;
      pCond->colList[i].pk = pColNode->isPk;

      if (pSlotList == NULL)  pCond->pSlotList[i] = pNode->slotId;
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
  if (pSlotList != NULL) *pSlotList = NULL;
  return code;
}

int32_t createStreamTask(void* pVnode, SStreamOptions* options, SStreamReaderTaskInner** ppTask,
                         SSDataBlock* pResBlock, STableKeyInfo* pList, int32_t pNum, SStorageAPI* storageApi) {
  int32_t                 code = 0;
  int32_t                 lino = 0;
  SStreamReaderTaskInner* pTaskInner = taosMemoryCalloc(1, sizeof(SStreamReaderTaskInner));

  STREAM_CHECK_NULL_GOTO(pTaskInner, terrno);
  pTaskInner->options = options;
  pTaskInner->storageApi = storageApi;
  if (pResBlock != NULL) {
    STREAM_CHECK_RET_GOTO(createOneDataBlock(pResBlock, false, &pTaskInner->pResBlock));
  } else {
    STREAM_CHECK_RET_GOTO(createDataBlockForStream(pTaskInner->options->schemas, &pTaskInner->pResBlock));
  }

  cleanupQueryTableDataCond(&pTaskInner->cond);
  STREAM_CHECK_RET_GOTO(qStreamInitQueryTableDataCond(&pTaskInner->cond, options->order, options->schemas, options->isSchema,
                                                    options->twindows, options->suid, options->ver, options->pSlotList));
  STREAM_CHECK_RET_GOTO(pTaskInner->storageApi->tsdReader.tsdReaderOpen(pVnode, &pTaskInner->cond, pList, pNum, pTaskInner->pResBlock,
                                                          (void**)&pTaskInner->pReader, pTaskInner->idStr, NULL));
  *ppTask = pTaskInner;
  pTaskInner = NULL;

end:
  releaseStreamTask(&pTaskInner);
  return code;
}

int32_t createStreamTaskForTs(SStreamOptions* options, SStreamReaderTaskInner** ppTask, SStorageAPI* storageApi) {
  SStreamReaderTaskInner* pTaskInner = taosMemoryCalloc(1, sizeof(SStreamReaderTaskInner));
  if (pTaskInner == NULL) 
    return terrno;
  
  pTaskInner->options = options;
  pTaskInner->storageApi = storageApi;
  *ppTask = pTaskInner;
  return 0;
}

static void destroyCondition(SNode* pCond) {
  if (pCond == NULL) return;
  nodesDestroyNode(pCond);
}

static void destroyBlock(void* data) {
  if (data == NULL) return;
  blockDataDestroy(*(SSDataBlock**)data);
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
  taosArrayDestroy(pInfo->tsSchemas);
  destroyExprInfo(pInfo->pExprInfoTriggerTag, pInfo->numOfExprTriggerTag);
  taosMemoryFreeClear(pInfo->pExprInfoTriggerTag);
  destroyExprInfo(pInfo->pExprInfoCalcTag, pInfo->numOfExprCalcTag);
  taosMemoryFreeClear(pInfo->pExprInfoCalcTag);
  tSimpleHashCleanup(pInfo->uidHashTrigger);
  tSimpleHashCleanup(pInfo->uidHashCalc);
  qStreamDestroyTableInfo(&pInfo->tableList);
  qStreamDestroyTableInfo(&pInfo->vSetTableList);
  filterFreeInfo(pInfo->pFilterInfo);
  pInfo->pFilterInfo = NULL;
  blockDataDestroy(pInfo->triggerBlock);
  pInfo->triggerBlock = NULL;
  blockDataDestroy(pInfo->calcBlock);
  pInfo->calcBlock = NULL;
  blockDataDestroy(pInfo->metaBlock);
  pInfo->metaBlock = NULL;
  taosHashCleanup(pInfo->triggerTableSchemaMapVTable);
  taosMemoryFreeClear(pInfo->triggerTableSchema);
  taosHashCleanup(pInfo->pTableMetaCacheTrigger);
  taosHashCleanup(pInfo->pTableMetaCacheCalc);
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
  taosArrayDestroy(pInfo->tmpRtFuncInfo.pStreamPesudoFuncVals);
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

static void freeTagCache(void* pData){
  if (pData == NULL) return;
  SArray* tagCache = *(SArray**)pData;
  taosArrayDestroyP(tagCache, taosMemFree);
}

static void freeSchema(void* pData){
  if (pData == NULL) return;
  STSchema* schema = *(STSchema**)pData;
  taosMemoryFree(schema);
}

static bool groupbyTbname(SNodeList* pGroupList) {
  bool   bytbname = false;
  SNode* pNode = NULL;
  FOREACH(pNode, pGroupList) {
    if (pNode->type == QUERY_NODE_FUNCTION) {
      bytbname = (strcmp(((struct SFunctionNode*)pNode)->functionName, "tbname") == 0);
      break;
    }
  }
  return bytbname;
}

static SStreamTriggerReaderInfo* createStreamReaderInfo(void* pTask, const SStreamReaderDeployMsg* pMsg) {
  int32_t    code = 0;
  int32_t    lino = 0;

  SStreamTriggerReaderInfo* sStreamReaderInfo = taosMemoryCalloc(1, sizeof(SStreamTriggerReaderInfo));
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);

  sStreamReaderInfo->lock = 0;
  sStreamReaderInfo->pTask = pTask;
  sStreamReaderInfo->tableType = pMsg->msg.trigger.triggerTblType;
  sStreamReaderInfo->isVtableStream = pMsg->msg.trigger.isTriggerTblVirt;

  if (pMsg->msg.trigger.triggerTblType == TD_SUPER_TABLE) {
    sStreamReaderInfo->suid = pMsg->msg.trigger.triggerTblUid;
    sStreamReaderInfo->uid = pMsg->msg.trigger.triggerTblUid;
  } else {
    sStreamReaderInfo->suid = 0;
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
    STREAM_CHECK_RET_GOTO(filterInitFromNode(sStreamReaderInfo->pConditions, &sStreamReaderInfo->pFilterInfo, 0, NULL));
    STREAM_CHECK_RET_GOTO(nodesStringToList(pMsg->msg.trigger.partitionCols, &sStreamReaderInfo->partitionCols));
    sStreamReaderInfo->twindows = ((STableScanPhysiNode*)(sStreamReaderInfo->triggerAst->pNode))->scanRange;
    sStreamReaderInfo->triggerCols = ((STableScanPhysiNode*)(sStreamReaderInfo->triggerAst->pNode))->scan.pScanCols;
    STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->triggerCols, TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);
    SDataBlockDescNode* pDescNode =
        ((STableScanPhysiNode*)(sStreamReaderInfo->triggerAst->pNode))->scan.node.pOutputDataBlockDesc;
    sStreamReaderInfo->triggerResBlock = createDataBlockFromDescNode(pDescNode);
    STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->triggerResBlock, TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);

    // SColumnInfoData idata = createColumnInfoData(TSDB_DATA_TYPE_BIGINT, LONG_BYTES, -1); // uid
    // STREAM_CHECK_RET_GOTO(blockDataAppendColInfo(sStreamReaderInfo->triggerResBlockNew, &idata));
    // idata = createColumnInfoData(TSDB_DATA_TYPE_UBIGINT, LONG_BYTES, -1); // gid
    // STREAM_CHECK_RET_GOTO(blockDataAppendColInfo(sStreamReaderInfo->triggerResBlockNew, &idata));

    // STREAM_CHECK_RET_GOTO(buildSTSchemaForScanData(&sStreamReaderInfo->triggerSchema, sStreamReaderInfo->triggerCols));
    sStreamReaderInfo->triggerPseudoCols = ((STableScanPhysiNode*)(sStreamReaderInfo->triggerAst->pNode))->scan.pScanPseudoCols;
    if (sStreamReaderInfo->triggerPseudoCols != NULL) {
      STREAM_CHECK_RET_GOTO(
          createExprInfo(sStreamReaderInfo->triggerPseudoCols, NULL, &sStreamReaderInfo->pExprInfoTriggerTag, &sStreamReaderInfo->numOfExprTriggerTag));
    }
    STREAM_CHECK_RET_GOTO(setColIdForCalcResBlock(sStreamReaderInfo->triggerPseudoCols, sStreamReaderInfo->triggerResBlock->pDataBlock));
    STREAM_CHECK_RET_GOTO(setColIdForCalcResBlock(sStreamReaderInfo->triggerCols, sStreamReaderInfo->triggerResBlock->pDataBlock));
    sStreamReaderInfo->groupByTbname = groupbyTbname(sStreamReaderInfo->partitionCols);
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
    if (pseudoCols != NULL) {
      STREAM_CHECK_RET_GOTO(
          createExprInfo(pseudoCols, NULL, &sStreamReaderInfo->pExprInfoCalcTag, &sStreamReaderInfo->numOfExprCalcTag));
    }
    SNodeList* pScanCols = ((STableScanPhysiNode*)(sStreamReaderInfo->calcAst->pNode))->scan.pScanCols;
    STREAM_CHECK_RET_GOTO(setColIdForCalcResBlock(pseudoCols, sStreamReaderInfo->calcResBlock->pDataBlock));
    STREAM_CHECK_RET_GOTO(setColIdForCalcResBlock(pScanCols, sStreamReaderInfo->calcResBlock->pDataBlock));
    STREAM_CHECK_RET_GOTO(createOneDataBlock(sStreamReaderInfo->calcResBlock, false, &sStreamReaderInfo->calcBlock));
    SColumnInfoData idata = createColumnInfoData(TSDB_DATA_TYPE_BIGINT, LONG_BYTES, INT16_MIN); // ver
    STREAM_CHECK_RET_GOTO(blockDataAppendColInfo(sStreamReaderInfo->calcBlock, &idata));

    sStreamReaderInfo->pTableMetaCacheCalc = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
    STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->pTableMetaCacheCalc, terrno);
    taosHashSetFreeFp(sStreamReaderInfo->pTableMetaCacheCalc, freeTagCache);
  }

  sStreamReaderInfo->tsSchemas = taosArrayInit(4, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->tsSchemas, terrno)
  STREAM_CHECK_RET_GOTO(
      qStreamBuildSchema(sStreamReaderInfo->tsSchemas, TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES, PRIMARYKEY_TIMESTAMP_COL_ID))  // first ts
  STREAM_CHECK_RET_GOTO(createDataBlockForTs(&sStreamReaderInfo->tsBlock));
  sStreamReaderInfo->groupIdMap =
      taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->groupIdMap, terrno);
  taosHashSetFreeFp(sStreamReaderInfo->groupIdMap, releaseGroupIdMap);

  sStreamReaderInfo->streamTaskMap =
      taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->streamTaskMap, terrno);
  taosHashSetFreeFp(sStreamReaderInfo->streamTaskMap, releaseStreamTask);

  sStreamReaderInfo->pTableMetaCacheTrigger = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->pTableMetaCacheTrigger, terrno);
  taosHashSetFreeFp(sStreamReaderInfo->pTableMetaCacheTrigger, freeTagCache);

  sStreamReaderInfo->triggerTableSchemaMapVTable = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->triggerTableSchemaMapVTable, terrno);
  taosHashSetFreeFp(sStreamReaderInfo->triggerTableSchemaMapVTable, freeSchema);

  STREAM_CHECK_RET_GOTO(createOneDataBlock(sStreamReaderInfo->triggerResBlock, false, &sStreamReaderInfo->triggerBlock));
  SColumnInfoData idata = createColumnInfoData(TSDB_DATA_TYPE_BIGINT, LONG_BYTES, INT16_MIN); // ver
  STREAM_CHECK_RET_GOTO(blockDataAppendColInfo(sStreamReaderInfo->triggerBlock, &idata));

end:
  STREAM_PRINT_LOG_END(code, lino);

  if (code != 0) {
    releaseStreamReaderInfo(sStreamReaderInfo);
    sStreamReaderInfo = NULL;
  }
  return sStreamReaderInfo;
}

static EDealRes checkPlaceHolderColumn(SNode* pNode, void* pContext) {
  if (QUERY_NODE_FUNCTION != nodeType((pNode))) {
    return DEAL_RES_CONTINUE;
  }
  SFunctionNode* pFuncNode = (SFunctionNode*)(pNode);
  if (fmIsStreamPesudoColVal(pFuncNode->funcId)) {
    *(bool*)pContext = true;
  }

  return DEAL_RES_CONTINUE;
}

static SStreamTriggerReaderCalcInfo* createStreamReaderCalcInfo(void* pTask, const SStreamReaderDeployMsg* pMsg, SNode* pPlan, bool keepPlan) {
  int32_t    code = 0;
  int32_t    lino = 0;
  SNodeList* triggerCols = NULL;

  SStreamTriggerReaderCalcInfo* sStreamReaderCalcInfo = taosMemoryCalloc(1, sizeof(SStreamTriggerReaderCalcInfo));
  STREAM_CHECK_NULL_GOTO(sStreamReaderCalcInfo, terrno);

  sStreamReaderCalcInfo->pTask = pTask;
  if (keepPlan) {
    sStreamReaderCalcInfo->calcAst = (SSubplan*)pPlan;
  } else {
    STREAM_CHECK_RET_GOTO(nodesCloneNode(pPlan, (SNode**)&sStreamReaderCalcInfo->calcAst));
  }
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
  
  bool hasPlaceHolderColumn = false;
  nodesWalkExpr(((SSubplan*)pPlan)->pTagCond, checkPlaceHolderColumn, (void*)&hasPlaceHolderColumn);
  sStreamReaderCalcInfo->hasPlaceHolder = hasPlaceHolderColumn;
  sStreamReaderCalcInfo->calcScanPlan = taosStrdup(pMsg->msg.calc.calcScanPlan);
  STREAM_CHECK_NULL_GOTO(sStreamReaderCalcInfo->calcScanPlan, terrno);
  sStreamReaderCalcInfo->pTaskInfo = NULL;

  sStreamReaderCalcInfo->tmpRtFuncInfo.pStreamPesudoFuncVals = taosArrayInit_s(sizeof(SSTriggerCalcParam), 1);
  STREAM_CHECK_NULL_GOTO(sStreamReaderCalcInfo->tmpRtFuncInfo.pStreamPesudoFuncVals, terrno);

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
    SNode* pPlan = NULL;
    ST_TASK_DLOGL("calcScanPlan:%s", (char*)(pMsg->msg.calc.calcScanPlan));
    pTask->info = taosArrayInit(pMsg->msg.calc.execReplica, POINTER_BYTES);
    STREAM_CHECK_NULL_GOTO(pTask->info, terrno);
    STREAM_CHECK_RET_GOTO(nodesStringToNode(pMsg->msg.calc.calcScanPlan, &pPlan));
    
    for (int32_t i = 0; i < pMsg->msg.calc.execReplica; ++i) {
      SStreamTriggerReaderCalcInfo* pCalcInfo = createStreamReaderCalcInfo(pTask, pMsg, pPlan, 0 == i);
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

