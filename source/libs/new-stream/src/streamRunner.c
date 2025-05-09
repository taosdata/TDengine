#include "streamRunner.h"
#include "dataSink.h"
#include "dataSinkMgt.h"
#include "executor.h"
#include "plannodes.h"
#include "tdatablock.h"
#include "streamInt.h"

static int32_t streamBuildTask(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pTaskExec);

static int32_t stRunnerInitTaskExecMgr(SStreamRunnerTask* pTask, const SStreamRunnerDeployMsg* pMsg) {
  SStreamRunnerTaskExecMgr*  pMgr = &pTask->execMgr;
  SStreamRunnerTaskExecution exec = {.pExecutor = NULL, .pPlan = pTask->pPlan};
  // decode plan into queryPlan
  int32_t                    code = 0;
  code = taosThreadMutexInit(&pMgr->lock, 0);
  if (code != 0) {
    ST_TASK_ELOG("failed to init stream runner task mgr mutex, code:%s", tstrerror(code));
    return code;
  }
  pMgr->pFreeExecs = tdListNew(sizeof(SStreamRunnerTaskExecution));
  if (!pMgr->pFreeExecs) return terrno;

  for (int32_t i = 0; i < pTask->parallelExecutionNun && code == 0; ++i) {
    exec.runtimeInfo.execId = i;
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

static void stRunnerDestroyTaskExecMgr(SStreamRunnerTask* pTask) {
  SStreamRunnerTaskExecMgr* pMgr = &pTask->execMgr;
  taosThreadMutexLock(&pMgr->lock);
  pMgr->exit = true;
  if (pMgr->pFreeExecs->dl_neles_ > 0) {
    tdListFreeP(pMgr->pFreeExecs, stRunnerDestroyTaskExecution);
  }
  taosThreadMutexUnlock(&pMgr->lock);
}

static int32_t stRunnerTaskExecMgrAcquireExec(SStreamRunnerTask* pTask, int32_t execId, SStreamRunnerTaskExecution** ppExec) {
  SStreamRunnerTaskExecMgr* pMgr = &pTask->execMgr;
  int32_t                   code = 0;
  taosThreadMutexLock(&pMgr->lock);
  if (pMgr->exit) {
    code = TSDB_CODE_STREAM_TASK_NOT_EXIST;
    ST_TASK_WLOG("task has been undeployed: %s", tstrerror(code));
  } else {
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
  }
  taosThreadMutexUnlock(&pMgr->lock);
  return code;
}

static void stRunnerTaskExecMgrReleaseExec(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec) {
  SStreamRunnerTaskExecMgr* pMgr = &pTask->execMgr;
  taosThreadMutexLock(&pMgr->lock);
  if (pMgr->exit) {
    if (pMgr->pRunningExecs->dl_neles_ == 1) {
      tdListFreeP(pMgr->pRunningExecs, stRunnerDestroyTaskExecution);
      // TODO destroy Mgr
    } else {
      SListNode* pNode = listNode(pExec);
      pNode = tdListPopNode(pMgr->pRunningExecs, pNode);
      stRunnerDestroyTaskExecution(pNode->data);
      taosMemoryFreeClear(pNode);
    }
  } else {
    SListNode* pNode = listNode(pExec);
    pNode = tdListPopNode(pMgr->pRunningExecs, pNode);
    tdListAppendNode(pMgr->pFreeExecs, pNode);
  }
  taosThreadMutexUnlock(&pMgr->lock);
}

void init_rt_info(SStreamRuntimeFuncInfo* pRtInfo) {
  pRtInfo->groupId = 1231231;
}

void test_scalar_calc(SStreamRunnerTask* pTask) {
  char tbname[128]= {0};
  SStreamRuntimeFuncInfo rtInfo = {0};
  init_rt_info(&rtInfo);
  int32_t code = streamCalcOutputTbName(pTask->pSubTableExpr, tbname, &rtInfo);

  SSTriggerCalcRequest req = {0};
  req.streamId = pTask->task.streamId;
  req.triggerTaskId = pTask->task.taskId;
  req.sessionId = 0;
  req.execId = -1;
  req.gid = 123123;
  req.brandNew = true;
  //code = stRunnerTaskExecute(pTask, &req);
}

static void stSetRunnerOutputInfo(SStreamRunnerTask* pTask, const SStreamRunnerDeployMsg* pMsg) {
  strncpy(pTask->output.outDbFName, pMsg->outDBFName, TSDB_DB_FNAME_LEN);
  pTask->output.outCols = pMsg->outCols;
  pTask->output.outTblType = pMsg->outTblType;
  pTask->output.outStbUid = pMsg->outStbUid;
  pTask->output.outTags = pMsg->outTags;
}

int32_t stRunnerTaskDeploy(SStreamRunnerTask* pTask, const SStreamRunnerDeployMsg* pMsg) {
  ST_TASK_ILOG("deploy runner task for %s.%s", pMsg->outDBFName, pMsg->outTblName);
  pTask->pPlan = pMsg->pPlan;  // TODO wjm do we need to deep copy this char*
  pTask->forceOutCols = pMsg->forceOutCols;
  pTask->parallelExecutionNun = pMsg->execReplica;
  pTask->output.outStbVersion = pMsg->outStbSversion;
  pTask->topTask = pMsg->topPlan;
  stSetRunnerOutputInfo(pTask, pMsg);
  int32_t code = stRunnerInitTaskExecMgr(pTask, pMsg);
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
  test_scalar_calc(pTask);

  return 0;
}

int32_t stRunnerTaskUndeploy(SStreamRunnerTask** ppTask, const SStreamUndeployTaskMsg* pMsg, taskUndeplyCallback cb) {
  if ((*ppTask)->execMgr.exit) return 0;
  nodesDestroyNode((*ppTask)->pSubTableExpr);
  (*ppTask)->pSubTableExpr = NULL;
  stRunnerDestroyTaskExecMgr(*ppTask);

  (*cb)(ppTask);

  return 0;
}

static int32_t streamResetTaskExec(SStreamRunnerTaskExecution* pExec, bool ignoreTbName) {
  int32_t code = 0;
  if (!ignoreTbName) pExec->tbname[0] = '\0';
  code = streamClearStatesForOperators(pExec->pExecutor);
  return code;
}

static int32_t stRunnerOutputBlock(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec, SSDataBlock* pBlock, bool createTb) {
  int32_t code = 0;
  if (pTask->notification.calcNotifyOnly) return 0;
  bool needCalcTbName = pExec->tbname[0] == '\0';
  if (pBlock && pBlock->info.rows > 0) {
    if (needCalcTbName)
      code = streamCalcOutputTbName(pTask->pSubTableExpr, pExec->tbname, &pExec->runtimeInfo.funcInfo);
    if (code != 0) {
      ST_TASK_ELOG("failed to calc output tbname: %s", tstrerror(code));
    } else {
      SStreamDataInserterInfo d = {.tbName = pExec->tbname, .isAutoCreateTable = createTb};
      SInputData              input = {.pData = pBlock, .pStreamDataInserterInfo = &d};
      bool                    cont = false;
      code = dsPutDataBlock(pExec->pSinkHandle, &input, &cont);
    }
  }
  return code;
}

int32_t stRunnerTaskExecute(SStreamRunnerTask* pTask, SSTriggerCalcRequest* pReq) {
  SStreamRunnerTaskExecution* pExec = NULL;

  int32_t code = stRunnerTaskExecMgrAcquireExec(pTask, pReq->execId, &pExec);
  if (code != 0) {
    ST_TASK_ELOG("failed to get task exec for stream code:%s", tstrerror(code));
    return code;
  }

  pTask->task.sessionId = pReq->sessionId;
  pExec->runtimeInfo.funcInfo.pStreamPartColVals = pReq->groupColVals;
  pExec->runtimeInfo.funcInfo.groupId = pReq->gid;
  pExec->runtimeInfo.pForceOutputCols = pTask->forceOutCols;
  pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals = pReq->params;
  pExec->runtimeInfo.funcInfo.groupId = pReq->gid;
  pExec->runtimeInfo.funcInfo.sessionId = pReq->sessionId;

  int32_t calcNum = taosArrayGetSize(pReq->params);
  if (!pExec->pExecutor) {
    code = streamBuildTask(pTask, pExec);
  } else {
    if (pReq->brandNew) // TODO wjm
      streamResetTaskExec(pExec, pTask->output.outTblType == TSDB_NORMAL_TABLE);
  }

  streamSetTaskRuntimeInfo(pExec->pExecutor, &pExec->runtimeInfo);

  pExec->runtimeInfo.funcInfo.curIdx = 0;
  for (; pExec->runtimeInfo.funcInfo.curIdx++ < calcNum; ++pExec->runtimeInfo.funcInfo.curIdx) {

    SSDataBlock* pBlock = NULL;
    uint64_t     ts = 0;
    if (code == 0) {
      code = streamExecuteTask(pExec->pExecutor, &pBlock, &ts);
    }
    if (code != 0) {
      ST_TASK_ELOG("failed to exec task code: %s", tstrerror(code));
    } else {
      if (pTask->topTask) {
        code = stRunnerOutputBlock(pTask, pExec, pBlock, pExec->runtimeInfo.funcInfo.curIdx == 0 ? pReq->createTable : false);
      } else {
        if (pBlock) {
          code = createOneDataBlock(pBlock, true, &pTask->output.pBlock);
        } else {
          blockDataCleanup(pTask->output.pBlock);
          pTask->output.pBlock = NULL;
        }
      }
    }
    streamResetTaskExec(pExec, true);
  }

  // free the block data?
  stRunnerTaskExecMgrReleaseExec(pTask, pExec);
  return code;
}

static int32_t streamBuildTask(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec) {
  int32_t vgId = pTask->task.nodeId;
  int64_t st = taosGetTimestampMs();
  int64_t streamId = pTask->task.streamId;
  int32_t taskId = pTask->task.taskId;
  int32_t code = 0;

  ST_TASK_DLOG("vgId:%d start to build stream task", vgId);
  SReadHandle handle = {.pMsgCb = pTask->pMsgCb};
  if (pTask->topTask) {
    SStreamInserterParam params = {.dbFName = pTask->output.outDbFName,
      .tbname =  pExec->tbname,
      .pFields = pTask->output.outCols,
      .pTagFields = pTask->output.outTags,
      .suid = pTask->output.outStbUid,
      .tbType = pTask->output.outTblType,
      .sver = pTask->output.outStbVersion,
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

int32_t stRunnerFetchDataFromCache(SStreamCacheReadInfo* pInfo) {
  void** ppIter;
  int32_t code = readStreamDataCache(pInfo->taskInfo.streamId, pInfo->taskInfo.taskId, pInfo->taskInfo.sessionId,
                                     pInfo->gid, pInfo->start, pInfo->end, &ppIter);
  if (code == 0) {
    code = getNextStreamDataCache(ppIter, &pInfo->pBlock);
  }
  return code;
}
