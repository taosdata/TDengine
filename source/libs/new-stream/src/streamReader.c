#include "streamReader.h"
#include "streamInt.h"

SHashObj* streamInfoMap = NULL;

void releaseStreamTask(void* p) {
  if (p == NULL) return;
  SStreamReaderTaskInner* pTask = (SStreamReaderTaskInner*)p;
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

int32_t qStreamInitQueryTableDataCond(SQueryTableDataCond* pCond, int32_t order, SArray* schemas, STimeWindow twindows,
                                      uint64_t suid) {
  pCond->order = order;
  pCond->numOfCols = taosArrayGetSize(schemas);

  pCond->colList = taosMemoryCalloc(pCond->numOfCols, sizeof(SColumnInfo));
  if (!pCond->colList) {
    return terrno;
  }
  pCond->pSlotList = taosMemoryMalloc(sizeof(int32_t) * pCond->numOfCols);
  if (pCond->pSlotList == NULL) {
    taosMemoryFreeClear(pCond->colList);
    return terrno;
  }

  pCond->twindows = twindows;
  pCond->suid = suid;
  pCond->type = TIMEWINDOW_RANGE_CONTAINED;
  pCond->startVersion = -1;
  pCond->endVersion = -1;
  //  pCond->skipRollup = readHandle->skipRollup;

  pCond->notLoadData = false;

  for (int32_t i = 0; i < pCond->numOfCols; ++i) {
    SColumnInfo* pColInfo = &pCond->colList[i];
    SSchema*     pSchema = taosArrayGet(schemas, i);
    pColInfo->type = pSchema[i].type;
    pColInfo->bytes = pSchema[i].bytes;
    pColInfo->colId = pSchema[i].colId;
    pColInfo->pk = pSchema[i].flags & COL_IS_KEY;

    pCond->pSlotList[i] = i;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t createStreamTask(void* pVnode, SStreamReaderTaskInnerOptions* options, SStreamReaderTaskInner** ppTask) {
  int32_t                 code = 0;
  int32_t                 lino = 0;
  SStreamReaderTaskInner* pTask = taosMemoryCalloc(1, sizeof(SStreamReaderTaskInner));
  STREAM_CHECK_NULL_GOTO(pTask, terrno);
  initStorageAPI(&pTask->api);
  pTask->pIgnoreTables = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  STREAM_CHECK_NULL_GOTO(pTask->pIgnoreTables, terrno);

  pTask->options = *options;
  STREAM_CHECK_RET_GOTO(createDataBlockForStream(options->schemas, &pTask->pResBlock));
  STREAM_CHECK_RET_GOTO(filterInitFromNode(options->pConditions, &pTask->pFilterInfo, 0));
  STREAM_CHECK_RET_GOTO(qStreamCreateTableListForReader(pVnode, options->suid, options->uid, options->tableType,
                                                        options->pGroupTags, options->groupSort, options->pTagCond,
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
  STREAM_CHECK_RET_GOTO(
    qStreamInitQueryTableDataCond(&pCond, options->order, options->schemas, options->twindows, options->suid));
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
  SStreamInfoObj* pInfo = (SStreamInfoObj*)p;
  taosHashCleanup(pInfo->streamTaskMap);
  taosMemoryFree(pInfo);
}

static SStreamInfoObj* createStreamInfo(const SStreamReaderDeployMsg* pMsg) {
  int32_t         code = 0;
  int32_t         lino = 0;
  SStreamInfoObj* sStreamInfo = taosMemoryCalloc(1, sizeof(SStreamInfoObj));
  STREAM_CHECK_NULL_GOTO(sStreamInfo, terrno);

  sStreamInfo->suid = 4647125232520705121;
  sStreamInfo->uid = 0;
  sStreamInfo->tableType = TD_SUPER_TABLE;
  sStreamInfo->twindows.skey = INT64_MIN;
  sStreamInfo->twindows.ekey = INT64_MAX;
  sStreamInfo->pTagCond = NULL;
  sStreamInfo->pTagIndexCond = NULL;
  sStreamInfo->pConditions = NULL;

  sStreamInfo->streamTaskMap = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  STREAM_CHECK_NULL_GOTO(sStreamInfo->streamTaskMap, terrno);
  taosHashSetFreeFp(sStreamInfo->streamTaskMap, releaseStreamTask);

end:
  if (code != 0) {
    releaseStreamInfo(sStreamInfo);
    sStreamInfo = NULL;
  }
  return sStreamInfo;
}

int32_t stReaderStreamDeploy(SStreamReaderTask* pTask, const SStreamReaderDeployMsg* pMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  STREAM_CHECK_NULL_GOTO(pTask, TSDB_CODE_INVALID_PARA);
  STREAM_CHECK_NULL_GOTO(pMsg, TSDB_CODE_INVALID_PARA);

  if (streamInfoMap == NULL) {
    streamInfoMap = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
    STREAM_CHECK_NULL_GOTO(streamInfoMap, terrno);
    taosHashSetFreeFp(streamInfoMap, releaseStreamInfo);
  }

  SStreamInfoObj* info = createStreamInfo(pMsg);
  STREAM_CHECK_NULL_GOTO(info, terrno);
  STREAM_CHECK_RET_GOTO(
      taosHashPut(streamInfoMap, &pTask->task.streamId, sizeof(pTask->task.streamId), &info, POINTER_BYTES));
end:
  if (code != 0) {
    taosHashCleanup(streamInfoMap);
  }
  return code;
}

int32_t stReaderTaskUndeploy(SStreamReaderTask* pTask, const SStreamUndeployTaskMsg* pMsg, taskUndeplyCallback cb) {
  int32_t code = 0;
  int32_t lino = 0;
  STREAM_CHECK_NULL_GOTO(pTask, TSDB_CODE_INVALID_PARA);
  STREAM_CHECK_NULL_GOTO(pMsg, TSDB_CODE_INVALID_PARA);
  STREAM_CHECK_RET_GOTO(taosHashRemove(streamInfoMap, &pTask->task.streamId, sizeof(pTask->task.streamId)));
end:
  PRINT_LOG_END(code, lino);
  return code;
}
// int32_t stReaderTaskExecute(SStreamReaderTask* pTask, SStreamMsg* pMsg);
// void qStreamSetGroupId(void* pTableListInfo, SSDataBlock* pBlock) {
//   pBlock->ino.id.groupId = tableListGetTableGroupId(pTableListInfo, pBlock->info.id.uid);
// }

SStreamReaderTaskInner* qStreamGetStreamInnerTask(int64_t streamId, int64_t sessionId, void* key, int32_t keySize) {
  int32_t                 code = 0;
  int32_t                 lino = 0;
  SStreamReaderTaskInner* pTask = NULL;
  SStreamInfoObj**        info = taosHashGet(streamInfoMap, &streamId, sizeof(streamId));
  STREAM_CHECK_NULL_GOTO(info, terrno);
  STREAM_CHECK_NULL_GOTO(*info, TSDB_CODE_INVALID_PARA);

  SStreamReaderTaskInner** ppTask = taosHashGet((*info)->streamTaskMap, key, keySize);
  STREAM_CHECK_NULL_GOTO(ppTask, terrno);
  pTask = *ppTask;

end:
  PRINT_LOG_END(code, lino);
  return pTask;
}

int32_t qStreamPutStreamInnerTask(int64_t streamId, int64_t sessionId, void* key, int32_t keySize, void* data,
                                  int32_t dataSize) {
  int32_t                 code = 0;
  int32_t                 lino = 0;
  SStreamReaderTaskInner* pTask = NULL;
  SStreamInfoObj**        info = taosHashGet(streamInfoMap, &streamId, sizeof(streamId));
  STREAM_CHECK_NULL_GOTO(info, terrno);
  STREAM_CHECK_NULL_GOTO(*info, TSDB_CODE_INVALID_PARA);

  code = taosHashPut((*info)->streamTaskMap, key, keySize, data, dataSize);

end:
  PRINT_LOG_END(code, lino);
  return code;
}

int32_t qStreamRemoveStreamInnerTask(int64_t streamId, int64_t sessionId, void* key, int32_t keySize) {
  int32_t                 code = 0;
  int32_t                 lino = 0;
  SStreamReaderTaskInner* pTask = NULL;
  SStreamInfoObj**        info = taosHashGet(streamInfoMap, &streamId, sizeof(streamId));
  STREAM_CHECK_NULL_GOTO(info, terrno);
  STREAM_CHECK_NULL_GOTO(*info, TSDB_CODE_INVALID_PARA);

  code = taosHashRemove((*info)->streamTaskMap, key, keySize);

end:
  PRINT_LOG_END(code, lino);
  return code;
}
