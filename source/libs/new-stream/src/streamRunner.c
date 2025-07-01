#include "streamRunner.h"
#include "dataSink.h"
#include "dataSinkMgt.h"
#include "executor.h"
#include "plannodes.h"
#include "scalar.h"
#include "streamInt.h"
#include "tarray.h"
#include "tdatablock.h"

static int32_t streamBuildTask(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pTaskExec);

static int32_t stRunnerInitTaskExecMgr(SStreamRunnerTask* pTask, const SStreamRunnerDeployMsg* pMsg) {
  SStreamRunnerTaskExecMgr*  pMgr = &pTask->execMgr;
  SStreamRunnerTaskExecution exec = {.pExecutor = NULL, .pPlan = pTask->pPlan};
  // decode plan into queryPlan
  int32_t code = 0;
  code = taosThreadMutexInit(&pMgr->lock, 0);
  if (code != 0) {
    ST_TASK_ELOG("failed to init stream runner task mgr mutex, code:%s", tstrerror(code));
    return code;
  }
  pMgr->pFreeExecs = tdListNew(sizeof(SStreamRunnerTaskExecution));
  if (!pMgr->pFreeExecs) return terrno;

  for (int32_t i = 0; i < pTask->parallelExecutionNun && code == 0; ++i) {
    exec.runtimeInfo.execId = i + pTask->task.deployId * pTask->parallelExecutionNun;
    if (pMsg->outTblType == TSDB_NORMAL_TABLE) {
      strncpy(exec.tbname, pMsg->outTblName, TSDB_TABLE_NAME_LEN);
    }
    code = tdListAppend(pMgr->pFreeExecs, &exec);
    if (code != 0) {
      ST_TASK_ELOG("failed to append task exec mgr:%s", tstrerror(code));
    }
  }
  if (code != 0) return code;

  pMgr->pRunningExecs = tdListNew(sizeof(SStreamRunnerTaskExecution));
  if (!pMgr->pRunningExecs) return terrno;
  return 0;
}

static void stRunnerDestroyTaskExecution(void* pExec) {
  SStreamRunnerTaskExecution* pExecution = pExec;
  pExecution->pPlan = 0;
  streamDestroyExecTask(pExecution->pExecutor);
}

static int32_t stRunnerTaskExecMgrAcquireExec(SStreamRunnerTask* pTask, int32_t execId,
                                              SStreamRunnerTaskExecution** ppExec) {
  SStreamRunnerTaskExecMgr* pMgr = &pTask->execMgr;
  int32_t                   code = 0;
  taosThreadMutexLock(&pMgr->lock);
  if (execId == -1) {
    if (pMgr->pFreeExecs->dl_neles_ > 0) {
      SListNode* pNode = tdListPopHead(pMgr->pFreeExecs);
      tdListAppendNode(pTask->execMgr.pRunningExecs, pNode);
      *ppExec = (SStreamRunnerTaskExecution*)pNode->data;
    } else {
      code = TSDB_CODE_STREAM_TASK_IVLD_STATUS;
      ST_TASK_ELOG("too many exec tasks scheduled: %s", tstrerror(code));
    }
  } else {
    SListNode* pNode = tdListGetHead(pMgr->pFreeExecs);
    while (pNode) {
      SStreamRunnerTaskExecution* pExec = (SStreamRunnerTaskExecution*)pNode->data;
      if (pExec->runtimeInfo.execId == execId) {
        tdListPopNode(pMgr->pFreeExecs, pNode);
        tdListAppendNode(pMgr->pRunningExecs, pNode);
        *ppExec = pExec;
        break;
      }
      pNode = pNode->dl_next_;
    }
    if (!*ppExec) {
      code = TSDB_CODE_STREAM_TASK_IVLD_STATUS;
      ST_TASK_ELOG("failed to get task exec, invalid execId:%d", execId);
    }
  }
  taosThreadMutexUnlock(&pMgr->lock);
  if (*ppExec) ST_TASK_DLOG("get exec task with nodeId: %d", (*ppExec)->runtimeInfo.execId);
  return code;
}

static void stRunnerTaskExecMgrReleaseExec(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec) {
  SStreamRunnerTaskExecMgr* pMgr = &pTask->execMgr;
  taosThreadMutexLock(&pMgr->lock);
  SListNode* pNode = listNode(pExec);
  pNode = tdListPopNode(pMgr->pRunningExecs, pNode);
  tdListAppendNode(pMgr->pFreeExecs, pNode);
  taosThreadMutexUnlock(&pMgr->lock);
}

static void stSetRunnerOutputInfo(SStreamRunnerTask* pTask, SStreamRunnerDeployMsg* pMsg) {
  strncpy(pTask->output.outDbFName, pMsg->outDBFName, TSDB_DB_FNAME_LEN);
  TSWAP(pTask->output.outCols, pMsg->outCols);
  pTask->output.outTblType = pMsg->outTblType;
  pTask->output.outStbUid = pMsg->outStbUid;
  TSWAP(pTask->output.outTags, pMsg->outTags);
  if (pMsg->outTblType == TSDB_SUPER_TABLE) strncpy(pTask->output.outSTbName, pMsg->outTblName, TSDB_TABLE_NAME_LEN);
}

int32_t stRunnerTaskDeploy(SStreamRunnerTask* pTask, SStreamRunnerDeployMsg* pMsg) {
  ST_TASK_DLOGL("deploy runner task for %s.%s, runner plan:%s", pMsg->outDBFName, pMsg->outTblName,
                (char*)(pMsg->pPlan));
  TSWAP(pTask->pPlan, pMsg->pPlan);
  TSWAP(pTask->notification.pNotifyAddrUrls, pMsg->pNotifyAddrUrls);
  TSWAP(pTask->forceOutCols, pMsg->forceOutCols);
  pTask->parallelExecutionNun = pMsg->execReplica;
  pTask->output.outStbVersion = pMsg->outStbSversion;
  pTask->topTask = pMsg->topPlan;
  pTask->notification.calcNotifyOnly = pMsg->calcNotifyOnly;
  pTask->notification.notifyErrorHandle = pMsg->notifyErrorHandle;

  int32_t code = nodesStringToList(pMsg->tagValueExpr, &pTask->output.pTagValExprs);
  ST_TASK_DLOG("pTagValExprs: %s", (char*)pMsg->tagValueExpr);
  if (code != 0) {
    ST_TASK_ELOG("failed to convert tag value expr to node err: %s expr: %s", strerror(code),
                 (char*)pMsg->tagValueExpr);
    pTask->task.status = STREAM_STATUS_FAILED;
    return code;
  }
  stSetRunnerOutputInfo(pTask, pMsg);
  code = stRunnerInitTaskExecMgr(pTask, pMsg);
  if (code != 0) {
    ST_TASK_ELOG("failed to init task exec mgr code:%s", tstrerror(code));
    pTask->task.status = STREAM_STATUS_FAILED;
    return code;
  }
  code = nodesStringToNode(pMsg->subTblNameExpr, (SNode**)&pTask->pSubTableExpr);
  if (code != 0) {
    ST_TASK_ELOG("failed to deserialize sub table expr: %s", tstrerror(code));
    pTask->task.status = STREAM_STATUS_FAILED;
    return code;
  }

  pTask->task.status = STREAM_STATUS_INIT;
  return 0;
}

int32_t stRunnerTaskUndeployImpl(SStreamRunnerTask** ppTask, const SStreamUndeployTaskMsg* pMsg, taskUndeplyCallback cb) {
  SStreamRunnerTask* pTask = *ppTask;
  SStreamRunnerTaskExecMgr* pMgr = &pTask->execMgr;
  tdListFreeP(pMgr->pRunningExecs, stRunnerDestroyTaskExecution);
  tdListFreeP(pMgr->pFreeExecs, stRunnerDestroyTaskExecution);
  taosThreadMutexDestroy(&pMgr->lock);
  NODES_DESTORY_NODE(pTask->pSubTableExpr);
  NODES_DESTORY_LIST(pTask->output.pTagValExprs);
  taosMemoryFreeClear(pTask->pPlan);
  taosArrayDestroyEx(pTask->forceOutCols, destroySStreamOutCols);
  taosArrayDestroyP(pTask->notification.pNotifyAddrUrls, taosMemFree);

  cb(ppTask);
  
  return 0;
}

int32_t stRunnerTaskUndeploy(SStreamRunnerTask** ppTask, bool force) {
  int32_t             code = TSDB_CODE_SUCCESS;
  SStreamRunnerTask *pTask = *ppTask;
  
  if (!force && taosWTryForceLockLatch(&pTask->task.entryLock)) {
    ST_TASK_DLOG("ignore undeploy runner task since working, entryLock:%x", pTask->task.entryLock);
    return code;
  }

  return stRunnerTaskUndeployImpl(ppTask, &pTask->task.undeployMsg, pTask->task.undeployCb);
}


static int32_t streamResetTaskExec(SStreamRunnerTaskExecution* pExec, bool ignoreTbName) {
  int32_t code = 0;
  if (!ignoreTbName) pExec->tbname[0] = '\0';
  stDebug("streamResetTaskExec:%p, ignoreTbName:%d tbname: %s", pExec, ignoreTbName, pExec->tbname);
  code = streamClearStatesForOperators(pExec->pExecutor);
  return code;
}

static int32_t stMakeSValueFromColInfoData(SStreamRunnerTask* pTask, SStreamGroupValue* pVal,
                                           const SColumnInfoData* pCol) {
  int32_t code = 0;
  pVal->data.type = pCol->info.type;
  char* p = colDataGetData(pCol, 0);
  pVal->isNull = colDataIsNull(pCol, 1, 0, NULL);
  if (!pVal->isNull) {
    size_t len = 0;
    if (IS_VAR_DATA_TYPE(pVal->data.type)) {
      len = varDataLen(p);
      pVal->data.pData = taosMemoryCalloc(1, len);
      if (!pVal->data.pData) {
        code = terrno;
        ST_TASK_ELOG("failed to make svalue from col info data: %s", strerror(code));
        return code;
      }
      memcpy(pVal->data.pData, varDataVal(p), len);
      pVal->data.nData = len;
    } else if (pVal->data.type == TSDB_DATA_TYPE_DECIMAL) {
      pVal->data.pData = taosMemoryCalloc(1, tDataTypes[TSDB_DATA_TYPE_DECIMAL].bytes);
      if (!pVal->data.pData) {
        code = terrno;
        ST_TASK_ELOG("failed to make svalue from col info data: %s", strerror(code));
        return code;
      }
      memcpy(pVal->data.pData, p, pCol->info.bytes);
      pVal->data.nData = pCol->info.bytes;
    } else {
      valueSetDatum(&pVal->data, pVal->data.type, p, pCol->info.bytes);
    }
  }
  return code;
}

static void stRunnerFreeTagInfo(void* p) {
  SStreamTagInfo* pTagInfo = p;
  if (pTagInfo->val.data.type == TSDB_DATA_TYPE_DECIMAL || IS_VAR_DATA_TYPE(pTagInfo->val.data.type))
    taosMemoryFreeClear(pTagInfo->val.data.pData);
}

static int32_t stRunnerCalcSubTbTagVal(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec,
                                       SArray** ppTagVals) {
  int32_t code = 0;
  SNode*  pNode = NULL;
  *ppTagVals = NULL;
  int32_t tagIdx = 0;
  FOREACH(pNode, pTask->output.pTagValExprs) {
    SScalarParam dst = {0};
    if (!*ppTagVals) *ppTagVals = taosArrayInit(1, sizeof(SStreamTagInfo));
    if (!*ppTagVals) {
      ST_TASK_ELOG("failed to init  stream tag info array: %s", strerror(code));
      code = terrno;
      break;
    }
    const SFieldWithOptions* pTagField = taosArrayGet(pTask->output.outTags, tagIdx);
    tagIdx++;
    SColumnInfoData* pCol = taosMemoryCalloc(1, sizeof(SColumnInfoData));
    if (!pCol) {
      code = terrno;
      break;
    }
    SDataType pType = ((SExprNode*)pNode)->resType;
    pCol->info.type = pType.type;
    pCol->info.bytes = pType.bytes;
    pCol->info.precision = pType.precision;
    pCol->info.scale = pType.scale;
    colInfoDataEnsureCapacity(pCol, 1, true);

    dst.colAlloced = true;
    dst.numOfRows = 1;
    dst.columnData = pCol;
    code = streamCalcOneScalarExpr(pNode, &dst, &pExec->runtimeInfo.funcInfo);
    if (code != 0) {
      sclFreeParam(&dst);
      break;
    }
    SStreamTagInfo tagInfo = {0};
    tstrncpy(tagInfo.tagName, pTagField->name, TSDB_COL_NAME_LEN);
    code = stMakeSValueFromColInfoData(pTask, &tagInfo.val, dst.columnData);
    sclFreeParam(&dst);
    if (NULL == taosArrayPush(*ppTagVals, &tagInfo)) {
      if (IS_VAR_DATA_TYPE(tagInfo.val.data.type) || tagInfo.val.data.type == TSDB_DATA_TYPE_DECIMAL)
        taosMemoryFreeClear(tagInfo.val.data.pData);
      code = terrno;
      break;
    }
    if (code != 0) break;
  }

  return code;
}

static int32_t stRunnerInitTbTagVal(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec, SArray** ppTagVals) {
  int32_t code = 0;
  if (pTask->output.outTblType == TSDB_SUPER_TABLE) {
    code = stRunnerCalcSubTbTagVal(pTask, pExec, ppTagVals);
  }
  return code;
}

static int32_t stRunnerOutputBlock(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec, SSDataBlock* pBlock,
                                   bool createTb) {
  int32_t code = 0;
  if (pTask->notification.calcNotifyOnly) return 0;
  bool needCalcTbName = pExec->tbname[0] == '\0';
  if (pBlock && pBlock->info.rows > 0) {
    if (needCalcTbName) {
      code = streamCalcOutputTbName(pTask->pSubTableExpr, pExec->tbname, &pExec->runtimeInfo.funcInfo);
      stDebug("stRunnerOutputBlock tbname: %s", pExec->tbname);
    }
    if (code != 0) {
      ST_TASK_ELOG("failed to calc output tbname: %s", tstrerror(code));
    } else {
      SArray* pTagVals = NULL;
      if (createTb) code = stRunnerInitTbTagVal(pTask, pExec, &pTagVals);
      if (code == 0) {
        SStreamDataInserterInfo d = {.tbName = pExec->tbname,
                                     .streamId = pTask->task.streamId,
                                     .groupId = pExec->runtimeInfo.funcInfo.groupId,
                                     .isAutoCreateTable = createTb,
                                     .pTagVals = pTagVals};
        SInputData              input = {.pData = pBlock, .pStreamDataInserterInfo = &d};
        bool                    cont = false;
        code = dsPutDataBlock(pExec->pSinkHandle, &input, &cont);
        ST_TASK_DLOG("runner output block to sink code:%d, rows: %" PRId64 ", tbname: %s, createTb: %d, gid: %" PRId64,
                     code, pBlock->info.rows, pExec->tbname, createTb, pExec->runtimeInfo.funcInfo.groupId);
        printDataBlock(pBlock, "output block to sink", "runner");
      } else {
        ST_TASK_ELOG("failed to init tag vals for output block: %s", tstrerror(code));
      }
      taosArrayDestroyEx(pTagVals, stRunnerFreeTagInfo);
    }
  }
  return code;
}

static int32_t streamDoNotification(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec,
                                    const SSDataBlock* pBlock) {
  int32_t code = 0;
  int32_t lino = 0;
  if (!pBlock || pBlock->info.rows <= 0) return code;
  char* pContent = NULL;
  code = streamBuildBlockResultNotifyContent(pBlock, &pContent, pTask->output.outCols);
  if (code == 0) {
    ST_TASK_DLOG("start to send notify:%s", pContent);
    SSTriggerCalcParam* pTriggerCalcParams =
        taosArrayGet(pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals, pExec->runtimeInfo.funcInfo.curOutIdx);
    pTriggerCalcParams->resultNotifyContent = pContent;

    code = streamSendNotifyContent(&pTask->task, pExec->runtimeInfo.funcInfo.triggerType,
                                   pExec->runtimeInfo.funcInfo.groupId, pTask->notification.pNotifyAddrUrls,
                                   pTask->notification.notifyErrorHandle, pTriggerCalcParams, 1);
  }
  return code;
}

static int32_t stRunnerHandleResultBlock(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec,
                                         SSDataBlock* pBlock, bool* pCreateTb) {
  int32_t code = stRunnerOutputBlock(pTask, pExec, pBlock, *pCreateTb);
  //*pCreateTb = false;
  if (code == 0) {
    return streamDoNotification(pTask, pExec, pBlock);
  }
  return code;
}

static int32_t stRunnerMergeBlockHandleOverflow(const SSDataBlock* pSrc, SSDataBlock* pDst, int32_t start,
                                                int32_t rowsToCopy, SSDataBlock** ppExtraBlock) {
  *ppExtraBlock = NULL;
  int32_t code = 0;
  if (pDst->info.rows + rowsToCopy > 4096) {
    int32_t rowsToCopy2 = 4096 - pDst->info.rows;
    if (rowsToCopy2 > 0) {
      code = blockDataMergeNRows(pDst, pSrc, start, rowsToCopy2);
      if (code != 0) return code;
      start += rowsToCopy2;
      rowsToCopy -= rowsToCopy2;
    }
  }
  if (rowsToCopy > 0) {
    code = createOneDataBlock(pSrc, false, ppExtraBlock);
    if (code == 0) {
      code = blockDataMergeNRows(*ppExtraBlock, pSrc, start, rowsToCopy);
      if (code != 0) {
        blockDataDestroy(*ppExtraBlock);
        *ppExtraBlock = NULL;
      }
    }
  }
  return code;
}

static int32_t stRunnerForceOutput(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec,
                                   const SSDataBlock* pBlock, SSDataBlock** ppForceOutBlock, int32_t* pWinIdx) {
  int32_t          code = 0;
  SArray*          pTriggerCalcParams = pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals;
  int32_t          curWinIdx = *pWinIdx;
  int32_t          rowsInput = pBlock ? pBlock->info.rows : 0;
  SColumnInfoData* pTsCol = rowsInput > 0 ? taosArrayGet(pBlock->pDataBlock, 0) : NULL;
  if (*pWinIdx >= taosArrayGetSize(pTriggerCalcParams)) return 0;
  SSTriggerCalcParam* pTriggerCalcParam = taosArrayGet(pTriggerCalcParams, *pWinIdx);
  int32_t             totalWinNum = taosArrayGetSize(pTriggerCalcParams);
  STimeWindow         curWin = {.skey = pTriggerCalcParam->wstart, .ekey = pTriggerCalcParam->wend};
  int32_t             rowIdx = 0;
  int32_t             rowsToCopy = 0;
  SSDataBlock*        pSecondBlock = NULL;
  bool                allRowsConsumed = false;

  for (; curWinIdx < totalWinNum && code == 0;) {
    int64_t ts = INT64_MAX;
    if (rowIdx < rowsInput) {
      ts = *(int64_t*)colDataGetNumData(pTsCol, rowIdx);
      assert(ts >= curWin.skey);
    }
    if (ts < curWin.ekey) {
      // cur window already has data
      rowIdx++;
      rowsToCopy++;
      if (rowIdx >= rowsInput) {
        allRowsConsumed = true;
      }
      continue;
    } else if (ts >= curWin.ekey) {
      if (rowsToCopy > 0) {
        // copy rows of prev windows
        if (!*ppForceOutBlock) {
          code = createOneDataBlock(pBlock, false, ppForceOutBlock);
        }
        if (code == 0) code = blockDataMergeNRows(*ppForceOutBlock, pBlock, rowIdx - rowsToCopy, rowsToCopy);
        if (code != 0) break;
        rowsToCopy = 0;
        if (allRowsConsumed) break;
      }
      curWinIdx++;
      assert(curWinIdx < taosArrayGetSize(pTriggerCalcParams));
      pTriggerCalcParam = taosArrayGet(pTriggerCalcParams, curWinIdx);
      curWin.skey = pTriggerCalcParam->wstart;
      curWin.ekey = pTriggerCalcParam->wend;
      if (ts >= curWin.ekey) {
        // cur win has no data
        code = streamForceOutput(pExec->pExecutor, ppForceOutBlock, curWinIdx);
      }
    }
  }
  *pWinIdx = curWinIdx;
  pExec->runtimeInfo.funcInfo.curOutIdx = curWinIdx;
  return code;
}

static int32_t stRunnerTopTaskHandleOutputBlockAgg(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec,
                                                   SSDataBlock* pBlock, SSDataBlock** ppForceOutBlock,
                                                   int32_t* pNextOutIdx, bool* createTable) {
  int32_t      code = 0;
  SSDataBlock* pOutputBlock = pBlock;
  if (taosArrayGetSize(pExec->runtimeInfo.pForceOutputCols) > 0) {
    if (*ppForceOutBlock) blockDataCleanup(*ppForceOutBlock);
    code = stRunnerForceOutput(pTask, pExec, pBlock, ppForceOutBlock, pNextOutIdx);
    pOutputBlock = *ppForceOutBlock;
  }
  if (code == 0) {
    if (pOutputBlock && pOutputBlock->info.rows > 0) {
      code = stRunnerHandleResultBlock(pTask, pExec, pOutputBlock, createTable);
    }
  }
  return code;
}

static int32_t stRunnerTopTaskHandleOutputBlockProj(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec,
                                                    SSDataBlock* pBlock, SSDataBlock** ppForceOutBlock,
                                                    int32_t* pNextOutIdx, bool* createTable) {
  int32_t code = 0;
  if (*ppForceOutBlock) blockDataCleanup(*ppForceOutBlock);
  if (*pNextOutIdx < taosArrayGetSize(pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals)) {
    if (*pNextOutIdx == pExec->runtimeInfo.funcInfo.curOutIdx && !pBlock) {
      // got no data from current window
      code = streamForceOutput(pExec->pExecutor, ppForceOutBlock, *pNextOutIdx);
      (*pNextOutIdx)++;
    } else if (*pNextOutIdx < pExec->runtimeInfo.funcInfo.curOutIdx && code == 0) {
      // got data from later windows, force output cur window
      while (*pNextOutIdx < pExec->runtimeInfo.funcInfo.curOutIdx && code == 0) {
        code = streamForceOutput(pExec->pExecutor, ppForceOutBlock, *pNextOutIdx);
        // won't overflow, total rows should smaller than 4096
        (*pNextOutIdx)++;
      }
    }
  }
  if (code == 0 && (*ppForceOutBlock) && (*ppForceOutBlock)->info.rows > 0) {
    code = stRunnerHandleResultBlock(pTask, pExec, *ppForceOutBlock, createTable);
  }
  if (code == 0) {
    *pNextOutIdx = pExec->runtimeInfo.funcInfo.curOutIdx + 1;
    code = stRunnerHandleResultBlock(pTask, pExec, pBlock, createTable);
  }
  return code;
}

int32_t stRunnerTaskExecute(SStreamRunnerTask* pTask, SSTriggerCalcRequest* pReq) {
  int32_t                     code = 0;
  int32_t                     lino = 0;
  SSDataBlock*                pForceOutBlock = NULL;
  SStreamRunnerTaskExecution* pExec = NULL;
  ST_TASK_DLOG("[runner calc]start, gid:%" PRId64 ", topTask: %d", pReq->gid, pTask->topTask);

  code = stRunnerTaskExecMgrAcquireExec(pTask, pReq->execId, &pExec);
  if (code != 0) {
    ST_TASK_ELOG("failed to get task exec for stream code:%s", tstrerror(code));
    return code;
  }
  pTask->task.status = STREAM_STATUS_RUNNING;
  pTask->task.sessionId = pReq->sessionId;
  TSWAP(pExec->runtimeInfo.funcInfo.pStreamPartColVals, pReq->groupColVals);
  TSWAP(pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals, pReq->params);
  pExec->runtimeInfo.funcInfo.groupId = pReq->gid;
  pExec->runtimeInfo.pForceOutputCols = pTask->forceOutCols;
  pExec->runtimeInfo.funcInfo.sessionId = pReq->sessionId;
  pExec->runtimeInfo.funcInfo.triggerType = pReq->triggerType;

  int32_t winNum = taosArrayGetSize(pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals);
  STREAM_CHECK_CONDITION_GOTO(winNum > STREAM_TRIGGER_MAX_WIN_NUM_PER_REQUEST, TSDB_CODE_STREAM_TASK_IVLD_STATUS);

  if (!pExec->pExecutor) {
    STREAM_CHECK_RET_GOTO(streamBuildTask(pTask, pExec));
  } else if (pReq->brandNew) {
    STREAM_CHECK_RET_GOTO(streamResetTaskExec(pExec, pTask->output.outTblType == TSDB_NORMAL_TABLE));
  }

  pExec->runtimeInfo.funcInfo.curIdx = pReq->curWinIdx;
  pExec->runtimeInfo.funcInfo.curOutIdx = pReq->curWinIdx;
  bool    createTable = pReq->createTable;
  int32_t nextOutIdx = pExec->runtimeInfo.funcInfo.curOutIdx;
  while (pExec->runtimeInfo.funcInfo.curOutIdx < winNum && code == 0) {
    bool         finished = false;
    SSDataBlock* pBlock = NULL;
    uint64_t     ts = 0;
    STREAM_CHECK_RET_GOTO(streamExecuteTask(pExec->pExecutor, &pBlock, &ts, &finished));
    printDataBlock(pBlock, __func__, "streamExecuteTask block");
    if (pTask->topTask) {
      if (pExec->runtimeInfo.funcInfo.withExternalWindow) {
        ST_TASK_DLOG("[runner calc] external window: %d, curIdx: %d, curOutIdx: %d, nextOutIdx: %d",
                     pExec->runtimeInfo.funcInfo.withExternalWindow, pExec->runtimeInfo.funcInfo.curIdx,
                     pExec->runtimeInfo.funcInfo.curOutIdx, nextOutIdx);
        if (pExec->runtimeInfo.funcInfo.extWinProjMode) {
          code = stRunnerTopTaskHandleOutputBlockProj(pTask, pExec, pBlock, &pForceOutBlock, &nextOutIdx, &createTable);
        } else {
          code = stRunnerTopTaskHandleOutputBlockAgg(pTask, pExec, pBlock, &pForceOutBlock, &nextOutIdx, &createTable);
        }
      } else {
        // no external window, only one window to calc, force output and output block
        if (!pBlock || pBlock->info.rows == 0) {
          if (nextOutIdx <= pExec->runtimeInfo.funcInfo.curOutIdx) {
            if (pForceOutBlock) blockDataCleanup(pForceOutBlock);
            code = streamForceOutput(pExec->pExecutor, &pForceOutBlock, nextOutIdx);
            if (code == 0) {
              code = stRunnerHandleResultBlock(pTask, pExec, pForceOutBlock, &createTable);
            }
            ++nextOutIdx;
          }
          ST_TASK_DLOG("[runner calc]gid:%" PRId64 " result has no data, status:%d", pReq->gid, pTask->task.status);
        } else {
          ST_TASK_DLOG("[runner calc]gid:%" PRId64
                       " non external window, %d, curIdx: %d, curOutIdx: %d, nextOutIdx: %d",
                       pReq->gid, pExec->runtimeInfo.funcInfo.withExternalWindow, pExec->runtimeInfo.funcInfo.curIdx,
                       pExec->runtimeInfo.funcInfo.curOutIdx, nextOutIdx);
          code = stRunnerHandleResultBlock(pTask, pExec, pBlock, &createTable);
          nextOutIdx = pExec->runtimeInfo.funcInfo.curOutIdx + 1;
        }
        if (finished) {
          ++pExec->runtimeInfo.funcInfo.curIdx;
          ++pExec->runtimeInfo.funcInfo.curOutIdx;
          ST_TASK_DLOG("[runner calc]gid:%" PRId64 " finished, %d, curIdx: %d, curOutIdx: %d, nextOutIdx: %d",
                       pReq->gid, pExec->runtimeInfo.funcInfo.withExternalWindow, pExec->runtimeInfo.funcInfo.curIdx,
                       pExec->runtimeInfo.funcInfo.curOutIdx, nextOutIdx);
        }
      }
    } else {
      if (pBlock) {
        STREAM_CHECK_RET_GOTO(createOneDataBlock(pBlock, true, (SSDataBlock**)&pReq->pOutBlock));
      }
      break;
    }
    if (finished) {
      streamResetTaskExec(pExec, true);
      if (pExec->runtimeInfo.funcInfo.withExternalWindow) break;
    }
  }

end:
  stRunnerTaskExecMgrReleaseExec(pTask, pExec);
  if (pForceOutBlock != NULL) blockDataDestroy(pForceOutBlock);
  if (code) {
    ST_TASK_ELOG("[runner calc]faild gid:%" PRId64 ", lino:%d code:%s", pReq->gid, lino, tstrerror(code));
    pTask->task.status = STREAM_STATUS_FAILED;
  } else {
    ST_TASK_DLOG("[runner calc]success, gid:%" PRId64 ",, status:%d", pReq->gid, pTask->task.status);
  }
  return code;
}

static int32_t streamBuildTask(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec) {
  int32_t vgId = pTask->task.nodeId;
  int64_t st = taosGetTimestampMs();
  int64_t streamId = pTask->task.streamId;
  int32_t taskId = pTask->task.taskId;
  int32_t code = 0;

  ST_TASK_DLOG("vgId:%d start to build stream task", vgId);
  SReadHandle handle = {0};
  handle.streamRtInfo = &pExec->runtimeInfo;
  handle.pMsgCb = pTask->pMsgCb;
  handle.pWorkerCb = pTask->pWorkerCb;
  if (pTask->topTask) {
    SStreamInserterParam params = {.dbFName = pTask->output.outDbFName,
                                   .tbname = pExec->tbname,
                                   .pFields = pTask->output.outCols,
                                   .pTagFields = pTask->output.outTags,
                                   .suid = pTask->output.outStbUid,
                                   .tbType = pTask->output.outTblType,
                                   .sver = pTask->output.outStbVersion,
                                   .stbname = pTask->output.outSTbName,
                                   .pSinkHandle = NULL};
    code = qCreateStreamExecTaskInfo(&pExec->pExecutor, (void*)pExec->pPlan, &handle, &params, vgId, taskId);
    pExec->pSinkHandle = params.pSinkHandle;
  } else {
    code = qCreateStreamExecTaskInfo(&pExec->pExecutor, (void*)pExec->pPlan, &handle, NULL, vgId, taskId);
  }
  if (code) {
    ST_TASK_ELOG("failed to build task, code:%s", tstrerror(code));
    return code;
  }

  code = qSetTaskId(pExec->pExecutor, taskId, streamId);
  if (code) {
    return code;
  }

  if (code) {
    ST_TASK_ELOG("failed to set stream notify info, code:%s", tstrerror(code));
    return code;
  }

  double el = (taosGetTimestampMs() - st) / 1000.0;
  ST_TASK_DLOG("expand stream task completed, elapsed time:%.2fsec", el);

  return code;
}

int32_t stRunnerFetchDataFromCache(SStreamCacheReadInfo* pInfo, bool* finished) {
  int32_t code = 0, lino = 0;
  void**  ppIter = NULL;
  int64_t streamId = pInfo->taskInfo.streamId;
  TAOS_CHECK_EXIT(readStreamDataCache(pInfo->taskInfo.streamId, pInfo->taskInfo.taskId, pInfo->taskInfo.sessionId,
                                     pInfo->gid, pInfo->start, pInfo->end, &ppIter));
  if (*ppIter != NULL) {
    TAOS_CHECK_EXIT(getNextStreamDataCache(ppIter, &pInfo->pBlock));
  }
  
  *finished = (*ppIter == NULL) ? true : false;

_exit:

  if (code) {
    stsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
  return code;
}
