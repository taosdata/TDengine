#include "streamRunner.h"
#include "dataSink.h"
#include "dataSinkMgt.h"
#include "executor.h"
#include "osMemory.h"
#include "plannodes.h"
#include "scalar.h"
#include "stream.h"
#include "streamInt.h"
#include "taoserror.h"
#include "tarray.h"
#include "tdatablock.h"

static int32_t streamBuildTask(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pTaskExec);

static int32_t stRunnerInitTaskExecMgr(SStreamRunnerTask* pTask, const SStreamRunnerDeployMsg* pMsg) {
  SStreamRunnerTaskExecMgr*  pMgr = &pTask->execMgr;
  SStreamRunnerTaskExecution exec = {.pExecutor = NULL, .pPlan = pTask->pPlan};
  // decode plan into queryPlan
  int32_t code = 0, lino = 0;
  code = taosThreadMutexInit(&pMgr->lock, 0);
  if (code != 0) {
    ST_TASK_ELOG("failed to init stream runner task mgr mutex, code:%s", tstrerror(code));
    return code;
  }

  pMgr->lockInited = true;
  
  code = taosThreadMutexLock(&pMgr->lock);
  if(code != 0) {
    ST_TASK_ELOG("failed to lock stream runner task mgr mutex, code:%s", tstrerror(code));
    return code;
  }
  pMgr->pFreeExecs = tdListNew(sizeof(SStreamRunnerTaskExecution));
  TSDB_CHECK_NULL(pMgr->pFreeExecs, code, lino, _exit, terrno);

  for (int32_t i = 0; i < pTask->parallelExecutionNun && code == 0; ++i) {
    exec.runtimeInfo.execId = i + pTask->task.deployId * pTask->parallelExecutionNun;
    if (pMsg->outTblType == TSDB_NORMAL_TABLE) {
      strncpy(exec.tbname, pMsg->outTblName, TSDB_TABLE_NAME_LEN);
    }
    ST_TASK_DLOG("init task exec mgr with execId:%d, topTask:%d", exec.runtimeInfo.execId, pTask->topTask);
    code = tdListAppend(pMgr->pFreeExecs, &exec);
    if (code != 0) {
      ST_TASK_ELOG("failed to append task exec mgr:%s", tstrerror(code));
      TAOS_CHECK_EXIT(code);
    }
  }

  pMgr->pRunningExecs = tdListNew(sizeof(SStreamRunnerTaskExecution));
  if (!pMgr->pRunningExecs) return terrno;

_exit:

  taosThreadMutexUnlock(&pMgr->lock);
  
  return code;
}

static void stRunnerDestroyRuntimeInfo(SStreamRuntimeInfo* pRuntime) {
  TAOS_UNUSED(tDestroyStRtFuncInfo(&pRuntime->funcInfo));
}

static void stRunnerDestroyTaskExecution(void* pExec) {
  SStreamRunnerTaskExecution* pExecution = pExec;
  pExecution->pPlan = NULL;
  streamDestroyExecTask(pExecution->pExecutor);  
  dsDestroyDataSinker(pExecution->pSinkHandle);
  stRunnerDestroyRuntimeInfo(&pExecution->runtimeInfo);
}

static int32_t stRunnerTaskExecMgrAcquireExec(SStreamRunnerTask* pTask, int32_t execId,
                                              SStreamRunnerTaskExecution** ppExec) {
  SStreamRunnerTaskExecMgr* pMgr = &pTask->execMgr;
  int32_t                   code = 0;
  code = taosThreadMutexLock(&pMgr->lock);
  if (code != 0) {
    ST_TASK_ELOG("failed to lock stream runner task exec mgr mutex, code:%s", tstrerror(code));
    return code;
  }
  ST_TASK_DLOG("get task exec with execId:%d", execId);
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
        pNode = tdListPopNode(pMgr->pFreeExecs, pNode);
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
  TAOS_UNUSED(taosThreadMutexUnlock(&pMgr->lock));
  if (*ppExec) ST_TASK_DLOG("get exec task with execId: %d", (*ppExec)->runtimeInfo.execId);
  return code;
}

static void stRunnerTaskExecMgrReleaseExec(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec) {
  SStreamRunnerTaskExecMgr* pMgr = &pTask->execMgr;
  int32_t code = (taosThreadMutexLock(&pMgr->lock));
  if (code != 0) {
    ST_TASK_ELOG("failed to lock stream runner task exec mgr mutex, code:%s", tstrerror(code));
    return;
  }
  SListNode* pNode = listNode(pExec);
  pNode = tdListPopNode(pMgr->pRunningExecs, pNode);
  tdListAppendNode(pMgr->pFreeExecs, pNode);
  TAOS_UNUSED(taosThreadMutexUnlock(&pMgr->lock));
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
  int32_t code = 0;
  
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
  pTask->streamName = taosStrdup(pMsg->streamName);

  code = stRunnerInitTaskExecMgr(pTask, pMsg);
  if (code != 0) {
    ST_TASK_ELOG("failed to init task exec mgr code:%s", tstrerror(code));
    pTask->task.status = STREAM_STATUS_FAILED;
    return code;
  }


  code = nodesStringToList(pMsg->tagValueExpr, &pTask->output.pTagValExprs);
  ST_TASK_DLOG("pTagValExprs: %s", (char*)pMsg->tagValueExpr);
  if (code != 0) {
    ST_TASK_ELOG("failed to convert tag value expr to node err: %s expr: %s", strerror(code),
                 (char*)pMsg->tagValueExpr);
    pTask->task.status = STREAM_STATUS_FAILED;
    return code;
  }
  stSetRunnerOutputInfo(pTask, pMsg);
  ST_TASK_DLOG("subTblNameExpr: %s", (char*)pMsg->subTblNameExpr);
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
  TAOS_UNUSED(taosThreadMutexDestroy(&pMgr->lock));
  NODES_DESTORY_NODE(pTask->pSubTableExpr);
  NODES_DESTORY_LIST(pTask->output.pTagValExprs);
  taosArrayDestroy(pTask->output.outCols);
  taosArrayDestroy(pTask->output.outTags);
  taosMemoryFreeClear(pTask->pPlan);
  taosArrayDestroyEx(pTask->forceOutCols, destroySStreamOutCols);
  taosArrayDestroyP(pTask->notification.pNotifyAddrUrls, taosMemFree);
  taosMemoryFreeClear(pTask->streamName);

  cb(ppTask);
  
  return 0;
}

void stRunnerKillAllExecs(SStreamRunnerTask *pTask) {
  SStreamRunnerTaskExecMgr* pMgr = &pTask->execMgr;
  int32_t                   code = 0;

  if (!pMgr->lockInited) {
    return;
  }

  code = taosThreadMutexLock(&pMgr->lock);
  if (code != 0) {
    ST_TASK_ELOG("failed to lock stream runner task exec mgr mutex, code:%s", tstrerror(code));
    return;
  }
  if (NULL == pMgr->pRunningExecs) {
    TAOS_UNUSED(taosThreadMutexUnlock(&pMgr->lock));
    return;
  }
  ST_TASK_DLOG("start to kill running execs, num:%d", listNEles(pMgr->pRunningExecs));
  SListNode* pNode = tdListGetHead(pMgr->pRunningExecs);
  while (pNode) {
    SStreamRunnerTaskExecution* pExec = (SStreamRunnerTaskExecution*)pNode->data;
    TAOS_UNUSED(qAsyncKillTask(pExec->pExecutor, TSDB_CODE_STREAM_EXEC_CANCELLED));
    pNode = pNode->dl_next_;
  }
  ST_TASK_DLOG("all runner execs killed, num: %d", listNEles(pMgr->pRunningExecs));
  TAOS_UNUSED(taosThreadMutexUnlock(&pMgr->lock));
}

int32_t stRunnerTaskUndeploy(SStreamRunnerTask** ppTask, bool force) {
  int32_t             code = TSDB_CODE_SUCCESS;
  SStreamRunnerTask *pTask = *ppTask;
  int64_t            streamId = pTask->task.streamId;
  int64_t            taskId = pTask->task.taskId;

  stRunnerKillAllExecs(pTask);
  
  if (!force && taosWTryForceLockLatch(&pTask->task.entryLock)) {
    stsDebug("ignore undeploy runner task %" PRIx64 " since working", taskId);
    return code;
  }

  ST_TASK_DLOG("runner task start undeploy impl, entryLock:%x", pTask->task.entryLock);

  return stRunnerTaskUndeployImpl(ppTask, &pTask->task.undeployMsg, pTask->task.undeployCb);
}

bool stRunnerTaskWaitQuit(SStreamRunnerTask* pTask) { return taosHasRWWFlag(&pTask->task.entryLock); }

static int32_t streamResetTaskExec(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec, bool ignoreTbName) {
  int32_t code = 0;
  if (!ignoreTbName) pExec->tbname[0] = '\0';
  ST_TASK_DLOG("streamResetTaskExec:%p, execId:%d exec finished, ignoreTbName:%d tbname: %s", pExec, pExec->runtimeInfo.execId, ignoreTbName, pExec->tbname);
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
  int32_t lino = 0;
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
    code = colInfoDataEnsureCapacity(pCol, 1, true);
    if (code != 0) {
      ST_TASK_ELOG("failed to ensure capacity for col info data: %s", strerror(code));
      taosMemoryFreeClear(pCol);
      break;
    }

    dst.colAlloced = true;
    dst.numOfRows = 1;
    dst.columnData = pCol;
    if (pNode->type == QUERY_NODE_VALUE) {
      void* p = nodesGetValueFromNode((SValueNode*)pNode);
      code = colDataSetVal(pCol, 0, p, ((SValueNode*)pNode)->isNull);
    } else {
      code = streamCalcOneScalarExpr(pNode, &dst, &pExec->runtimeInfo.funcInfo);
    }
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
                                   bool* createTb) {
  int32_t code = 0;
  if (stRunnerTaskWaitQuit(pTask)) {
    ST_TASK_ILOG("[runner calc]quit, skip output. status:%d", pTask->task.status);
    return TSDB_CODE_SUCCESS;
  }
  if (pTask->notification.calcNotifyOnly) return 0;
  bool needCalcTbName = pExec->tbname[0] == '\0';
  if (pBlock && pBlock->info.rows > 0) {
    if (*createTb && needCalcTbName) {
      code = streamCalcOutputTbName(pTask->pSubTableExpr, pExec->tbname, &pExec->runtimeInfo.funcInfo);
      stDebug("stRunnerOutputBlock tbname: %s", pExec->tbname);
    }
    if (code != 0) {
      ST_TASK_ELOG("failed to calc output tbname: %s", tstrerror(code));
    } else {
      SArray* pTagVals = NULL;
      if (*createTb) code = stRunnerInitTbTagVal(pTask, pExec, &pTagVals);
      if (code == 0) {
        SStreamDataInserterInfo d = {.tbName = pExec->tbname,
                                     .streamId = pTask->task.streamId,
                                     .groupId = pExec->runtimeInfo.funcInfo.groupId,
                                     .isAutoCreateTable = *createTb,
                                     .pTagVals = pTagVals};
        SInputData              input = {.pData = pBlock, .pStreamDataInserterInfo = &d};
        bool                    cont = false;
        code = dsPutDataBlock(pExec->pSinkHandle, &input, &cont);
        ST_TASK_DLOG("runner output block to sink code:0x%0x, rows: %" PRId64 ", tbname: %s, createTb: %d, gid: %" PRId64,
                     code, pBlock->info.rows, pExec->tbname, *createTb, pExec->runtimeInfo.funcInfo.groupId);
        printDataBlock(pBlock, "output block to sink", "runner");
        if(code == TSDB_CODE_SUCCESS) *createTb = false;  // if output block success, then no need to create table
      } else {
        ST_TASK_ELOG("failed to init tag vals for output block: %s", tstrerror(code));
      }
      taosArrayDestroyEx(pTagVals, stRunnerFreeTagInfo);
    }
  }
  return code;
}

static int32_t streamPrepareNotification(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec,
                                         const SSDataBlock* pBlock, const int32_t curWinIdx, const int32_t startRow,
                                         const int32_t endRow) {
  int32_t code = 0;
  if (!pBlock || pBlock->info.rows <= 0) return code;
  char* pContent = NULL;
  code = streamBuildBlockResultNotifyContent(pBlock, &pContent, pTask->output.outCols, startRow, endRow);
  if (code == 0) {
    ST_TASK_DLOG("start to send notify:%s", pContent);
    SSTriggerCalcParam* pTriggerCalcParams = taosArrayGet(pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals, curWinIdx);
    if (pTriggerCalcParams == NULL) {
      ST_TASK_ELOG("%s failed to get trigger calc params for win index:%d, size:%d", __FUNCTION__, curWinIdx,
                   (int32_t)pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals->size);
      taosMemoryFreeClear(pContent);
      code = TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
      goto _exit;
    }
    pTriggerCalcParams->resultNotifyContent = pContent;
  }
_exit:
  if (code != 0) {
    ST_TASK_ELOG("failed to prepare notification for task:%" PRIx64 ", code:%s", pTask->task.streamId, tstrerror(code));
    if (pContent) taosMemoryFreeClear(pContent);
    if (pTask->notification.notifyErrorHandle == 0) {
      code = TSDB_CODE_SUCCESS;
    }
  }
  return code;
}

static void clearNotifyContent(SStreamRunnerTaskExecution* pExec) {
  int32_t nParam = TARRAY_SIZE(pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals);
  for (int i = 0; i < nParam; ++i) {
    SSTriggerCalcParam* pTriggerCalcParams = taosArrayGet(pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals, i);
    if (pTriggerCalcParams != NULL && pTriggerCalcParams->resultNotifyContent != NULL) {
      taosMemoryFreeClear(pTriggerCalcParams->resultNotifyContent);
    }
  }
}

static int32_t streamDoNotification(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec, int32_t startWinIdx,
                                    int32_t endWinIdx, const char* tbname) {
  int32_t code = 0;
  int32_t lino = 0;

  int32_t              nParam = endWinIdx - startWinIdx + 1;
  SSTriggerCalcParam** params = taosMemCalloc(nParam, sizeof(SSTriggerCalcParam*));
  if (!params) {
    ST_TASK_ELOG("failed to init stream pesudo func vals array, size:%d", nParam);
    TAOS_CHECK_EXIT(terrno);
  }

  for (int i = startWinIdx; i <= endWinIdx; ++i) {
    SSTriggerCalcParam* pTriggerCalcParams = taosArrayGet(pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals, i);
    if (pTriggerCalcParams == NULL) {
      ST_TASK_ELOG("%s failed to get trigger calc params for index:%d, size:%d", __FUNCTION__, i,
                   (int32_t)pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals->size);
      TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
    }
    params[i - startWinIdx] = pTriggerCalcParams;
  }

  code = streamSendNotifyContent(&pTask->task, pTask->streamName, tbname, pExec->runtimeInfo.funcInfo.triggerType,
                                 pExec->runtimeInfo.funcInfo.groupId, pTask->notification.pNotifyAddrUrls,
                                 pTask->notification.notifyErrorHandle, *params, nParam);

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("failed to send notification for task:%" PRIx64 ", code:%s", pTask->task.streamId, tstrerror(code));
  } else {
    ST_TASK_DLOG("send notification for task:%" PRIx64 ", win count:%d", pTask->task.streamId, nParam);
  }
  if (pTask->notification.notifyErrorHandle == 0) {
    code = TSDB_CODE_SUCCESS;  // if notify error handle is 0, then ignore the error
  }
  clearNotifyContent(pExec);
  taosMemoryFreeClear(params);
  return code;
}

static int32_t streamDoNotification1For1(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec,
                                         const SSDataBlock* pBlock, const char* tbname) {
  int32_t code = 0;
  int32_t lino = 0;

  if (!pBlock || pBlock->info.rows <= 0) return code;
  char* pContent = NULL;
  code = streamBuildBlockResultNotifyContent(pBlock, &pContent, pTask->output.outCols, 0, pBlock->info.rows - 1);
  if (code == 0) {
    ST_TASK_DLOG("start to send notify:%s", pContent);
    int32_t index = pExec->runtimeInfo.funcInfo.curOutIdx;
    if (pExec->runtimeInfo.funcInfo.curOutIdx >= pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals->size) {
      pExec->runtimeInfo.funcInfo.curOutIdx = pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals->size - 1;
    }
    SSTriggerCalcParam* pTriggerCalcParams =
        taosArrayGet(pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals, pExec->runtimeInfo.funcInfo.curOutIdx);
    if (pTriggerCalcParams == NULL) {
      ST_TASK_ELOG("%s failed to get trigger calc params for index:%d, size:%d", __FUNCTION__, index,
                   (int32_t)pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals->size);
      taosMemoryFreeClear(pContent);
      return TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
    }
    pTriggerCalcParams->resultNotifyContent = pContent;

    code = streamSendNotifyContent(&pTask->task, pTask->streamName, tbname, pExec->runtimeInfo.funcInfo.triggerType,
                                   pExec->runtimeInfo.funcInfo.groupId, pTask->notification.pNotifyAddrUrls,
                                   pTask->notification.notifyErrorHandle, pTriggerCalcParams, 1);
    taosMemoryFreeClear(pTriggerCalcParams->resultNotifyContent);
  }
  return code;
}

static int32_t stRunnerHandleSingleWinResultBlock(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec,
                                                  SSDataBlock* pBlock, bool* pCreateTb) {
  int32_t code = stRunnerOutputBlock(pTask, pExec, pBlock, pCreateTb);
  if (code == 0) {
    code = streamDoNotification1For1(pTask, pExec, pBlock, pExec->tbname);
    if (code != TSDB_CODE_SUCCESS) {
      ST_TASK_ELOG("failed to send notification for block, code:%s", tstrerror(code));
    }
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

static int32_t stRunnerForceOutputHelp(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec,
                                       const SSDataBlock* pBlock, SSDataBlock** ppForceOutBlock, int32_t* pWinIdx,
                                       bool onlyForPrepareNotify) {
  int32_t          code = 0, lino = 0;
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

  if (!onlyForPrepareNotify && !*ppForceOutBlock) {
    TAOS_CHECK_EXIT(createOneDataBlock(pBlock, false, ppForceOutBlock));
    TAOS_CHECK_EXIT(blockDataEnsureCapacity(*ppForceOutBlock, totalWinNum));
  }

  for (; curWinIdx < totalWinNum && code == 0;) {
    int64_t ts = INT64_MAX;
    if (rowIdx < rowsInput) {
      ts = *(int64_t*)colDataGetNumData(pTsCol, rowIdx);
      if (ts < curWin.skey) {
        ST_TASK_ILOG("ts:%" PRId64 " is less than current window start key:%" PRId64
                     ", skip this row, curWinIdx:%d, totalWinNum:%d",
                     ts, curWin.skey, curWinIdx, totalWinNum);
        rowIdx++;
        continue;
      }
    }
    if (ts < curWin.ekey) {
      // cur window already has data
      rowIdx++;
      rowsToCopy++;
      continue;
    } else {
      if (pExec->runtimeInfo.funcInfo.triggerType != STREAM_TRIGGER_SLIDING && ts == curWin.ekey) {
        rowIdx++;
        rowsToCopy++;
      }
      if (rowsToCopy > 0) {
        // copy rows of prev windows
        if (!onlyForPrepareNotify) {
          TAOS_CHECK_EXIT(blockDataMergeNRows(*ppForceOutBlock, pBlock, rowIdx - rowsToCopy, rowsToCopy));
        }
        // prepare notify
        TAOS_CHECK_GOTO(streamPrepareNotification(pTask, pExec, pBlock, curWinIdx, rowIdx - rowsToCopy, rowIdx - 1), &lino, _exit);
      } else {
        if (!onlyForPrepareNotify) {
          TAOS_CHECK_GOTO(streamForceOutput(pExec->pExecutor, ppForceOutBlock, curWinIdx), &lino, _exit);

          TAOS_CHECK_GOTO(
              streamPrepareNotification(pTask, pExec, *ppForceOutBlock, curWinIdx, (*ppForceOutBlock)->info.rows - 1,
                                        (*ppForceOutBlock)->info.rows - 1),
              &lino, _exit);
        }
      }
      rowsToCopy = 0;  // reset for next window

      curWinIdx++;
      if (curWinIdx >= totalWinNum) {
        // no more windows to process
        break;
      }
      if (rowsInput != 0 && rowIdx >= rowsInput) {
        // no more rows in this block to process
        // if current window is an empty window, we still need to force output all the windows. or break to wait next block.
        break;
      }

      pTriggerCalcParam = taosArrayGet(pTriggerCalcParams, curWinIdx);
      curWin.skey = pTriggerCalcParam->wstart;
      curWin.ekey = pTriggerCalcParam->wend;
    }
  }
  *pWinIdx = curWinIdx;
  pExec->runtimeInfo.funcInfo.curOutIdx = curWinIdx;

_exit:

  if (code != 0) {
    ST_TASK_ELOG("%s failed to force output for stream task at line %d, code:%s", __FUNCTION__, lino, tstrerror(code));
    if (!onlyForPrepareNotify && *ppForceOutBlock) {
      blockDataDestroy(*ppForceOutBlock);
      *ppForceOutBlock = NULL;
    }
    return code;
  }
  return code;
}

static int32_t stRunnerForceOutput(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec,
                                   const SSDataBlock* pBlock, SSDataBlock** ppForceOutBlock, int32_t* pWinIdx) {
  return stRunnerForceOutputHelp(pTask, pExec, pBlock, ppForceOutBlock, pWinIdx, false);
}

static int32_t stRunnerPrepareMulWinNotification(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec,
                                                 const SSDataBlock* pBlock, int32_t* pWinIdx) {
  return stRunnerForceOutputHelp(pTask, pExec, pBlock, NULL, pWinIdx, true);
}

void stRunnerUpdateNextOutIdx(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec, SSDataBlock* pBlock, int32_t* nextIdx) {
  SArray* pTriggerCalcParams = pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals;
  int32_t winNum = taosArrayGetSize(pTriggerCalcParams);
  if (NULL == pBlock || pBlock->info.rows <= 0) {
    *nextIdx = winNum;
    return;
  }
  
  SSTriggerCalcParam* pTriggerCalcParam = NULL;
  SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, 0);
  int64_t lastTs = *(int64_t*)colDataGetNumData(pTsCol, pBlock->info.rows - 1);
  for (int32_t i = *nextIdx + pBlock->info.rows - 1; i < winNum; ++i) {
    pTriggerCalcParam = taosArrayGet(pTriggerCalcParams, i);
    if (lastTs == pTriggerCalcParam->wstart) {
      *nextIdx = i;
      return;
    }
  }

  ST_TASK_ELOG("%s failed to get output ts %" PRId64 " window, start:%d, total:%d", __FUNCTION__, lastTs, *nextIdx, winNum);
  
  *nextIdx = winNum;
}

static int32_t stRunnerTopTaskHandleOutputBlockAgg(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec,
                                                   SSDataBlock* pBlock, SSDataBlock** ppForceOutBlock,
                                                   int32_t* pNextOutIdx, bool* pCreateTable) {
  int32_t code = 0;
  int32_t startWinIdx = *pNextOutIdx;
  int32_t endWinIdx = 0;

  SSDataBlock* pOutputBlock = pBlock;
  if (taosArrayGetSize(pExec->runtimeInfo.pForceOutputCols) > 0) {
    if (*ppForceOutBlock) {
      blockDataCleanup(*ppForceOutBlock);
      *ppForceOutBlock = NULL;
    }
    code = stRunnerForceOutput(pTask, pExec, pBlock, ppForceOutBlock, pNextOutIdx);
    pOutputBlock = *ppForceOutBlock;
  } else if (taosArrayGetSize(pTask->notification.pNotifyAddrUrls) > 0) {
    // prepare notification for current block when no force output cols
    code = stRunnerPrepareMulWinNotification(pTask, pExec, pOutputBlock, pNextOutIdx);
  } else {
    stRunnerUpdateNextOutIdx(pTask, pExec, pBlock, pNextOutIdx);
  }
  
  if (code == 0) {
    if (pOutputBlock && pOutputBlock->info.rows > 0) {
      code = stRunnerOutputBlock(pTask, pExec, pOutputBlock, pCreateTable);
      if (code != TSDB_CODE_SUCCESS) {
        ST_TASK_ELOG("failed  to output block, code:%s", tstrerror(code));
      }
    }
  }
  if (code == 0 && taosArrayGetSize(pTask->notification.pNotifyAddrUrls) > 0) {
    endWinIdx = *pNextOutIdx - 1;
    if (endWinIdx >= startWinIdx) {
      code = streamDoNotification(pTask, pExec, startWinIdx, endWinIdx, pExec->tbname);
      if (code != TSDB_CODE_SUCCESS) {
        ST_TASK_ELOG("failed to send notification for block, code:%s", tstrerror(code));
      }
    }
  }
  return code;
}

static int32_t stRunnerTopTaskHandleOutputBlockProj(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec,
                                                    SSDataBlock* pBlock, SSDataBlock** ppForceOutBlock,
                                                    int32_t* pNextOutIdx, bool* createTable) {
  int32_t code = 0;
  int     lino = 0;
  int32_t startWinIdx = *pNextOutIdx;
  int32_t endWinIdx = 0;
  if (*ppForceOutBlock) blockDataCleanup(*ppForceOutBlock);
  if (*pNextOutIdx < taosArrayGetSize(pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals)) {
    if (*pNextOutIdx == pExec->runtimeInfo.funcInfo.curOutIdx && !pBlock) {
      // got no data from current window
      code = streamForceOutput(pExec->pExecutor, ppForceOutBlock, *pNextOutIdx);
      TAOS_CHECK_GOTO(streamPrepareNotification(pTask, pExec, *ppForceOutBlock, *pNextOutIdx, 0, 0), &lino, _exit);
      (*pNextOutIdx)++;
    } else if (*pNextOutIdx < pExec->runtimeInfo.funcInfo.curOutIdx && code == 0) {
      // got data from later windows, force output cur window
      while (*pNextOutIdx < pExec->runtimeInfo.funcInfo.curOutIdx && code == 0) {
        TAOS_CHECK_GOTO(streamForceOutput(pExec->pExecutor, ppForceOutBlock, *pNextOutIdx), &lino, _exit);
        TAOS_CHECK_GOTO(streamPrepareNotification(pTask, pExec, *ppForceOutBlock, *pNextOutIdx, 0, 0), &lino, _exit);
        // won't overflow, total rows should smaller than 4096
        (*pNextOutIdx)++;
      }
    }
  }

  if (code == 0 && (*ppForceOutBlock) && (*ppForceOutBlock)->info.rows > 0) {
    code = stRunnerOutputBlock(pTask, pExec, *ppForceOutBlock, createTable);
  }

  if (code == 0 && pBlock) {  // && *pNextOutIdx < taosArrayGetSize(pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals)
    TAOS_CHECK_GOTO(streamPrepareNotification(pTask, pExec, pBlock, pExec->runtimeInfo.funcInfo.curOutIdx, 0,
                                              pBlock ? pBlock->info.rows - 1 : 0),
                    &lino, _exit);
    code = stRunnerOutputBlock(pTask, pExec, pBlock, createTable);
    *pNextOutIdx = pExec->runtimeInfo.funcInfo.curOutIdx + 1;
  }
  if (code == 0) {
    endWinIdx = *pNextOutIdx - 1;
    if (endWinIdx >= startWinIdx) {
      TAOS_CHECK_GOTO(streamDoNotification(pTask, pExec, startWinIdx, endWinIdx, pExec->tbname), &lino, _exit);
    }
  }
_exit:
  if (code != 0) {
    ST_TASK_ELOG("failed to handle output block, code:%s, lino:%d", tstrerror(code), lino);
    if (*ppForceOutBlock) {
      blockDataDestroy(*ppForceOutBlock);
      *ppForceOutBlock = NULL;
    }
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
  STREAM_CHECK_CONDITION_GOTO(winNum > STREAM_CALC_REQ_MAX_WIN_NUM, TSDB_CODE_STREAM_TASK_IVLD_STATUS);

  if (!pExec->pExecutor) {
    STREAM_CHECK_RET_GOTO(streamBuildTask(pTask, pExec));
  } else if (pReq->brandNew) {
    STREAM_CHECK_RET_GOTO(streamResetTaskExec(pTask, pExec, pTask->output.outTblType == TSDB_NORMAL_TABLE));
  }

  pExec->runtimeInfo.funcInfo.curIdx = pReq->curWinIdx;
  pExec->runtimeInfo.funcInfo.curOutIdx = pReq->curWinIdx;
  bool    createTable = (pReq->createTable != 0);
  int32_t nextOutIdx = pExec->runtimeInfo.funcInfo.curOutIdx;
  while (pExec->runtimeInfo.funcInfo.curOutIdx < winNum && code == 0) {
    if (stRunnerTaskWaitQuit(pTask)) {
      ST_TASK_ILOG("[runner calc]quit, skip calc. gid:%" PRId64 ", status:%d", pReq->gid, pTask->task.status);
      break;
    }
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
              code = stRunnerHandleSingleWinResultBlock(pTask, pExec, pForceOutBlock, &createTable);
            }
            ++nextOutIdx;
          }
          ST_TASK_DLOG("[runner calc]gid:%" PRId64 " result has no data, status:%d", pReq->gid, pTask->task.status);
        } else {
          ST_TASK_DLOG("[runner calc]gid:%" PRId64
                       " non external window, %d, curIdx: %d, curOutIdx: %d, nextOutIdx: %d",
                       pReq->gid, pExec->runtimeInfo.funcInfo.withExternalWindow, pExec->runtimeInfo.funcInfo.curIdx,
                       pExec->runtimeInfo.funcInfo.curOutIdx, nextOutIdx);
          code = stRunnerHandleSingleWinResultBlock(pTask, pExec, pBlock, &createTable);
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
      code = streamResetTaskExec(pTask, pExec, true);
      if (code != 0) {
        ST_TASK_ELOG("failed to reset task exec, code:%s", tstrerror(code));
        break;
      }
      if (pExec->runtimeInfo.funcInfo.withExternalWindow) break;
    }
  }

end:
  
  ST_TASK_DLOG("execId %d stop to run", pExec->runtimeInfo.execId);
  
  stRunnerTaskExecMgrReleaseExec(pTask, pExec);
  if (pForceOutBlock != NULL) blockDataDestroy(pForceOutBlock);
  if (code) {
    ST_TASK_ELOG("[runner calc]faild gid:%" PRId64 ", lino:%d code:%s", pReq->gid, lino, tstrerror(code));
    pTask->task.status = STREAM_STATUS_FAILED;
  } else {
    ST_TASK_DLOG("[runner calc]success, gid:%" PRId64 ",, status:%d", pReq->gid, pTask->task.status);
  }
  if (createTable) {
    ST_TASK_ILOG("[runner calc]success, gid:%" PRId64 ", but has no data, skip create table.", pReq->gid);
    return TSDB_CODE_MND_STREAM_TABLE_NOT_CREATE;
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
  handle.pMsgCb = &pTask->msgCb;
  //handle.pMsgCb = pTask->pMsgCb;
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
