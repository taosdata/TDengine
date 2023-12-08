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

#include "mndScheduler.h"
#include "tmisce.h"
#include "mndMnode.h"
#include "mndDb.h"
#include "mndSnode.h"
#include "mndVgroup.h"
#include "parser.h"
#include "tcompare.h"
#include "tname.h"
#include "tuuid.h"

#define SINK_NODE_LEVEL (0)
extern bool tsDeployOnSnode;

int32_t mndConvertRsmaTask(char** pDst, int32_t* pDstLen, const char* ast, int64_t uid, int8_t triggerType,
                           int64_t watermark, int64_t deleteMark) {
  SNode*      pAst = NULL;
  SQueryPlan* pPlan = NULL;
  terrno = TSDB_CODE_SUCCESS;

  if (nodesStringToNode(ast, &pAst) < 0) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    goto END;
  }

  if (qSetSTableIdForRsma(pAst, uid) < 0) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    goto END;
  }

  SPlanContext cxt = {
      .pAstRoot = pAst,
      .topicQuery = false,
      .streamQuery = true,
      .rSmaQuery = true,
      .triggerType = triggerType,
      .watermark = watermark,
      .deleteMark = deleteMark,
  };

  if (qCreateQueryPlan(&cxt, &pPlan, NULL) < 0) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    goto END;
  }

  int32_t levelNum = LIST_LENGTH(pPlan->pSubplans);
  if (levelNum != 1) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    goto END;
  }
  SNodeListNode* inner = (SNodeListNode*)nodesListGetNode(pPlan->pSubplans, 0);

  int32_t opNum = LIST_LENGTH(inner->pNodeList);
  if (opNum != 1) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    goto END;
  }

  SSubplan* plan = (SSubplan*)nodesListGetNode(inner->pNodeList, 0);
  if (qSubPlanToString(plan, pDst, pDstLen) < 0) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    goto END;
  }

END:
  if (pAst) nodesDestroyNode(pAst);
  if (pPlan) nodesDestroyNode((SNode*)pPlan);
  return terrno;
}

int32_t mndSetSinkTaskInfo(SStreamObj* pStream, SStreamTask* pTask) {
  STaskOutputInfo* pInfo = &pTask->outputInfo;

  if (pStream->smaId != 0) {
    pInfo->type = TASK_OUTPUT__SMA;
    pInfo->smaSink.smaId = pStream->smaId;
  } else {
    pInfo->type = TASK_OUTPUT__TABLE;
    pInfo->tbSink.stbUid = pStream->targetStbUid;
    memcpy(pInfo->tbSink.stbFullName, pStream->targetSTbName, TSDB_TABLE_FNAME_LEN);
    pInfo->tbSink.pSchemaWrapper = tCloneSSchemaWrapper(&pStream->outputSchema);
    if (pInfo->tbSink.pSchemaWrapper == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return 0;
}

int32_t mndAddDispatcherForInternalTask(SMnode* pMnode, SStreamObj* pStream, SArray* pSinkNodeList,
                                        SStreamTask* pTask) {
  bool isShuffle = false;

  if (pStream->fixedSinkVgId == 0) {
    SDbObj* pDb = mndAcquireDb(pMnode, pStream->targetDb);
    if (pDb != NULL && pDb->cfg.numOfVgroups > 1) {
      isShuffle = true;
      pTask->outputInfo.type = TASK_OUTPUT__SHUFFLE_DISPATCH;
      pTask->msgInfo.msgType = TDMT_STREAM_TASK_DISPATCH;
      if (mndExtractDbInfo(pMnode, pDb, &pTask->outputInfo.shuffleDispatcher.dbInfo, NULL) < 0) {
        return -1;
      }
    }

    sdbRelease(pMnode->pSdb, pDb);
  }

  int32_t numOfSinkNodes = taosArrayGetSize(pSinkNodeList);

  if (isShuffle) {
    memcpy(pTask->outputInfo.shuffleDispatcher.stbFullName, pStream->targetSTbName, TSDB_TABLE_FNAME_LEN);
    SArray* pVgs = pTask->outputInfo.shuffleDispatcher.dbInfo.pVgroupInfos;

    int32_t numOfVgroups = taosArrayGetSize(pVgs);
    for (int32_t i = 0; i < numOfVgroups; i++) {
      SVgroupInfo* pVgInfo = taosArrayGet(pVgs, i);

      for (int32_t j = 0; j < numOfSinkNodes; j++) {
        SStreamTask* pSinkTask = taosArrayGetP(pSinkNodeList, j);
        if (pSinkTask->info.nodeId == pVgInfo->vgId) {
          pVgInfo->taskId = pSinkTask->id.taskId;
          break;
        }
      }
    }
  } else {
    SStreamTask* pOneSinkTask = taosArrayGetP(pSinkNodeList, 0);
    streamTaskSetFixedDownstreamInfo(pTask, pOneSinkTask);
  }

  return 0;
}

int32_t mndAssignStreamTaskToVgroup(SMnode* pMnode, SStreamTask* pTask, SSubplan* plan, const SVgObj* pVgroup) {
  int32_t msgLen;

  pTask->info.nodeId = pVgroup->vgId;
  pTask->info.epSet = mndGetVgroupEpset(pMnode, pVgroup);

  plan->execNode.nodeId = pTask->info.nodeId;
  plan->execNode.epSet = pTask->info.epSet;
  return qSubPlanToString(plan, &pTask->exec.qmsg, &msgLen);
}

SSnodeObj* mndSchedFetchOneSnode(SMnode* pMnode) {
  SSnodeObj* pObj = NULL;
  void*      pIter = NULL;
  // TODO random fetch
  pIter = sdbFetch(pMnode->pSdb, SDB_SNODE, pIter, (void**)&pObj);
  sdbCancelFetch(pMnode->pSdb, pIter);
  return pObj;
}

int32_t mndAssignStreamTaskToSnode(SMnode* pMnode, SStreamTask* pTask, SSubplan* plan, const SSnodeObj* pSnode) {
  int32_t msgLen;

  pTask->info.nodeId = SNODE_HANDLE;
  pTask->info.epSet = mndAcquireEpFromSnode(pMnode, pSnode);

  plan->execNode.nodeId = SNODE_HANDLE;
  plan->execNode.epSet = pTask->info.epSet;
  mDebug("s-task:0x%x set the agg task to snode:%d", pTask->id.taskId, SNODE_HANDLE);

  return qSubPlanToString(plan, &pTask->exec.qmsg, &msgLen);
}

// todo random choose a node to do compute
SVgObj* mndSchedFetchOneVg(SMnode* pMnode, int64_t dbUid) {
  void*   pIter = NULL;
  SVgObj* pVgroup = NULL;
  while (1) {
    pIter = sdbFetch(pMnode->pSdb, SDB_VGROUP, pIter, (void**)&pVgroup);
    if (pIter == NULL) break;
    if (pVgroup->dbUid != dbUid) {
      sdbRelease(pMnode->pSdb, pVgroup);
      continue;
    }
    sdbCancelFetch(pMnode->pSdb, pIter);
    return pVgroup;
  }
  return pVgroup;
}

static int32_t doAddSinkTask(SStreamObj* pStream, SMnode* pMnode, int32_t vgId, SVgObj* pVgroup,
                      SEpSet* pEpset, bool isFillhistory) {
  int64_t uid = (isFillhistory) ? pStream->hTaskUid : pStream->uid;
  SArray** pTaskList = (isFillhistory) ? taosArrayGetLast(pStream->pHTasksList) : taosArrayGetLast(pStream->tasks);

  SStreamTask* pTask = tNewStreamTask(uid, TASK_LEVEL__SINK, isFillhistory, 0, *pTaskList, pStream->conf.fillHistory);
  if (pTask == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  epsetAssign(&(pTask)->info.mnodeEpset, pEpset);

  pTask->info.nodeId = vgId;
  pTask->info.epSet = mndGetVgroupEpset(pMnode, pVgroup);
  return mndSetSinkTaskInfo(pStream, pTask);
}

// create sink node for each vgroup.
static int32_t doAddShuffleSinkTask(SMnode* pMnode, SStreamObj* pStream, SEpSet* pEpset) {
  SSdb* pSdb = pMnode->pSdb;
  void* pIter = NULL;

  while (1) {
    SVgObj* pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void**)&pVgroup);
    if (pIter == NULL) {
      break;
    }

    if (!mndVgroupInDb(pVgroup, pStream->targetDbUid)) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    int32_t code = doAddSinkTask(pStream, pMnode, pVgroup->vgId, pVgroup, pEpset, false);
    if(code != 0){
      sdbRelease(pSdb, pVgroup);
      return code;
    }
    if(pStream->conf.fillHistory){
      code = doAddSinkTask(pStream, pMnode, pVgroup->vgId, pVgroup, pEpset, true);
      if(code != 0){
        sdbRelease(pSdb, pVgroup);
        return code;
      }
    }
    sdbRelease(pSdb, pVgroup);
  }

  return TDB_CODE_SUCCESS;
}

static SStreamTask* buildSourceTask(SStreamObj* pStream, SEpSet* pEpset,
                                    int64_t firstWindowSkey, bool isFillhistory) {
  uint64_t uid = (isFillhistory) ? pStream->hTaskUid : pStream->uid;
  SArray** pTaskList = (isFillhistory) ? taosArrayGetLast(pStream->pHTasksList) : taosArrayGetLast(pStream->tasks);

  SStreamTask* pTask = tNewStreamTask(uid, TASK_LEVEL__SOURCE,
                                      isFillhistory, pStream->conf.triggerParam,
                                      *pTaskList, pStream->conf.fillHistory);
  if (pTask == NULL) {
    return NULL;
  }

  epsetAssign(&pTask->info.mnodeEpset, pEpset);
  STimeWindow* pWindow = &pTask->dataRange.window;

  pWindow->skey = INT64_MIN;
  pWindow->ekey = firstWindowSkey - 1;
  mDebug("add source task 0x%x window:%" PRId64 " - %" PRId64, pTask->id.taskId, pWindow->skey, pWindow->ekey);

  return pTask;
}

static SArray* addNewTaskList(SArray* pTasksList) {
  SArray* pTaskList = taosArrayInit(0, POINTER_BYTES);
  taosArrayPush(pTasksList, &pTaskList);
  return pTaskList;
}

// set the history task id
static void setHTasksId(SArray* pTaskList, const SArray* pHTaskList) {
  for (int32_t i = 0; i < taosArrayGetSize(pTaskList); ++i) {
    SStreamTask** pStreamTask = taosArrayGet(pTaskList, i);
    SStreamTask** pHTask = taosArrayGet(pHTaskList, i);

    (*pStreamTask)->hTaskInfo.id.taskId = (*pHTask)->id.taskId;
    (*pStreamTask)->hTaskInfo.id.streamId = (*pHTask)->id.streamId;

    (*pHTask)->streamTaskId.taskId = (*pStreamTask)->id.taskId;
    (*pHTask)->streamTaskId.streamId = (*pStreamTask)->id.streamId;

    mDebug("s-task:0x%" PRIx64 "-0x%x related history task:0x%" PRIx64 "-0x%x, level:%d", (*pStreamTask)->id.streamId,
           (*pStreamTask)->id.taskId, (*pHTask)->id.streamId, (*pHTask)->id.taskId, (*pHTask)->info.taskLevel);
  }
}

static int32_t doAddSourceTask(SMnode* pMnode, SSubplan* plan, SStreamObj* pStream,
                               SEpSet* pEpset, int64_t nextWindowSkey,
                               SVgObj* pVgroup, bool isFillhistory ){
  // new stream task
  SStreamTask* pTask = buildSourceTask(pStream, pEpset, nextWindowSkey, isFillhistory);
  if(pTask == NULL){
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }
  int32_t code = mndAssignStreamTaskToVgroup(pMnode, pTask, plan, pVgroup);
  if(code != 0){
    terrno = code;
    return terrno;
  }
  return TDB_CODE_SUCCESS;
}

static SSubplan* getScanSubPlan(const SQueryPlan* pPlan){
  int32_t numOfPlanLevel = LIST_LENGTH(pPlan->pSubplans);
  SNodeListNode* inner = (SNodeListNode*)nodesListGetNode(pPlan->pSubplans, numOfPlanLevel - 1);
  if (LIST_LENGTH(inner->pNodeList) != 1) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    return NULL;
  }

  SSubplan* plan = (SSubplan*)nodesListGetNode(inner->pNodeList, 0);
  if (plan->subplanType != SUBPLAN_TYPE_SCAN) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    return NULL;
  }
  return plan;
}

static SSubplan* getAggSubPlan(const SQueryPlan* pPlan, int index){
  SNodeListNode* inner = (SNodeListNode*)nodesListGetNode(pPlan->pSubplans, index);
  if (LIST_LENGTH(inner->pNodeList) != 1) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    return NULL;
  }

  SSubplan* plan = (SSubplan*)nodesListGetNode(inner->pNodeList, 0);
  if (plan->subplanType != SUBPLAN_TYPE_MERGE) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    return NULL;
  }
  return plan;
}

static int32_t addSourceTask(SMnode* pMnode, SSubplan* plan, SStreamObj* pStream,
                                               SEpSet* pEpset, int64_t nextWindowSkey) {
  // create exec stream task, since only one level, the exec task is also the source task
  SArray* pTaskList = addNewTaskList(pStream->tasks);
  SArray* pHTaskList = NULL;
  if (pStream->conf.fillHistory) {
    pHTaskList = addNewTaskList(pStream->pHTasksList);
  }

  void* pIter = NULL;
  SSdb* pSdb = pMnode->pSdb;
  while (1) {
    SVgObj* pVgroup;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void**)&pVgroup);
    if (pIter == NULL) {
      break;
    }

    if (!mndVgroupInDb(pVgroup, pStream->sourceDbUid)) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    int code = doAddSourceTask(pMnode, plan, pStream, pEpset, nextWindowSkey, pVgroup, false);
    if(code != 0){
      sdbRelease(pSdb, pVgroup);
      return code;
    }

    if (pStream->conf.fillHistory) {
      code = doAddSourceTask(pMnode, plan, pStream, pEpset, nextWindowSkey, pVgroup, true);
      if(code != 0){
        sdbRelease(pSdb, pVgroup);
        return code;
      }
    }

    sdbRelease(pSdb, pVgroup);
  }

  if (pStream->conf.fillHistory) {
    setHTasksId(pTaskList, pHTaskList);
  }

  return TSDB_CODE_SUCCESS;
}

static SStreamTask* buildAggTask(SStreamObj* pStream, SEpSet* pEpset, bool isFillhistory) {
  uint64_t uid = (isFillhistory) ? pStream->hTaskUid : pStream->uid;
  SArray** pTaskList = (isFillhistory) ? taosArrayGetLast(pStream->pHTasksList) : taosArrayGetLast(pStream->tasks);

  SStreamTask* pAggTask = tNewStreamTask(uid, TASK_LEVEL__AGG, isFillhistory, pStream->conf.triggerParam, *pTaskList, pStream->conf.fillHistory);
  if (pAggTask == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  epsetAssign(&pAggTask->info.mnodeEpset, pEpset);
  return pAggTask;
}

static int32_t doAddAggTask(SStreamObj* pStream, SMnode* pMnode, SSubplan* plan, SEpSet* pEpset){
  SStreamTask* pTask = buildAggTask(pStream, pEpset, false);
  if (pTask == NULL) {
    return terrno;
  }

  SVgObj*    pVgroup = NULL;
  SSnodeObj* pSnode = NULL;
  int32_t    code = 0;
  if (tsDeployOnSnode) {
    pSnode = mndSchedFetchOneSnode(pMnode);
    if (pSnode == NULL) {
      pVgroup = mndSchedFetchOneVg(pMnode, pStream->sourceDbUid);
    }
  } else {
    pVgroup = mndSchedFetchOneVg(pMnode, pStream->sourceDbUid);
  }

  if (pSnode != NULL) {
    code = mndAssignStreamTaskToSnode(pMnode, pTask, plan, pSnode);
  } else {
    code = mndAssignStreamTaskToVgroup(pMnode, pTask, plan, pVgroup);
  }
  if(code != 0){
    goto END;
  }

  if (pStream->conf.fillHistory) {
    pTask = buildAggTask(pStream, pEpset, true);
    if (pTask == NULL) {
      code = terrno;
      goto END;
    }

    if (pSnode != NULL) {
      code = mndAssignStreamTaskToSnode(pMnode, pTask, plan, pSnode);
    } else {
      code = mndAssignStreamTaskToVgroup(pMnode, pTask, plan, pVgroup);
    }
    if(code != 0){
      goto END;
    }

    SArray** pAggTaskList = taosArrayGetLast(pStream->tasks);
    SArray** pHAggTaskList = taosArrayGetLast(pStream->pHTasksList);
    setHTasksId(*pAggTaskList, *pHAggTaskList);
  }

  END:
  if (pSnode != NULL) {
    sdbRelease(pMnode->pSdb, pSnode);
  } else {
    sdbRelease(pMnode->pSdb, pVgroup);
  }
  return code;
}

static int32_t addAggTask(SStreamObj* pStream, SMnode* pMnode, SSubplan* plan, SEpSet* pEpset) {
  addNewTaskList(pStream->tasks);
  if (pStream->conf.fillHistory) {
    addNewTaskList(pStream->pHTasksList);
  }
  return doAddAggTask(pStream, pMnode, plan, pEpset);
}

static int32_t addSinkTask(SMnode* pMnode, SStreamObj* pStream, SEpSet* pEpset){
  SArray* pSinkTaskList = addNewTaskList(pStream->tasks);

  SArray* pHSinkTaskList = NULL;
  if (pStream->conf.fillHistory) {
    pHSinkTaskList = addNewTaskList(pStream->pHTasksList);
  }

  int32_t code = 0;
  if (pStream->fixedSinkVgId == 0) {
    code = doAddShuffleSinkTask(pMnode, pStream, pEpset);
    if (code != 0) {
      return code;
    }
  } else {
    code = doAddSinkTask(pStream, pMnode, pStream->fixedSinkVgId, &pStream->fixedSinkVg, pEpset, false);
    if (code != 0) {
      return code;
    }
    if(pStream->conf.fillHistory){
      code = doAddSinkTask(pStream, pMnode, pStream->fixedSinkVgId, &pStream->fixedSinkVg, pEpset, true);
      if (code != 0) {
        return code;
      }
    }
  }

  if (pStream->conf.fillHistory) {
    setHTasksId(pSinkTaskList, pHSinkTaskList);
  }
  return TDB_CODE_SUCCESS;
}

static void bindTaskToSinkTask(SStreamObj* pStream, SMnode* pMnode, SArray* pSinkTaskList, SStreamTask* task){
  mndAddDispatcherForInternalTask(pMnode, pStream, pSinkTaskList, task);
  for(int32_t k = 0; k < taosArrayGetSize(pSinkTaskList); k++) {
    SStreamTask* pSinkTask = taosArrayGetP(pSinkTaskList, k);
    streamTaskSetUpstreamInfo(pSinkTask, task);
  }
}

static void bindAggSink(SStreamObj* pStream, SMnode* pMnode, SArray* tasks) {
  SArray* pSinkTaskList = taosArrayGetP(tasks, SINK_NODE_LEVEL);
  SArray** pAggTaskList = taosArrayGetLast(tasks);

  for(int i = 0; i < taosArrayGetSize(*pAggTaskList); i++){
    SStreamTask* pAggTask = taosArrayGetP(*pAggTaskList, i);
    bindTaskToSinkTask(pStream, pMnode, pSinkTaskList, pAggTask);
  }
}

static void bindSourceSink(SStreamObj* pStream, SMnode* pMnode, SArray* tasks, bool hasExtraSink) {
  SArray* pSinkTaskList = taosArrayGetP(tasks, SINK_NODE_LEVEL);
  SArray* pSourceTaskList = taosArrayGetP(tasks, hasExtraSink ? SINK_NODE_LEVEL + 1 : SINK_NODE_LEVEL);

  for(int i = 0; i < taosArrayGetSize(pSourceTaskList); i++){
    SStreamTask* pSourceTask = taosArrayGetP(pSourceTaskList, i);
    if (hasExtraSink) {
      bindTaskToSinkTask(pStream, pMnode, pSinkTaskList, pSourceTask);
    } else {
      mndSetSinkTaskInfo(pStream, pSourceTask);
    }
  }
}

static void bindTwoLevel(SArray* tasks, int32_t begin, int32_t end) {
  size_t size = taosArrayGetSize(tasks);
  ASSERT(size >= 2);
  SArray* pDownTaskList = taosArrayGetP(tasks, size - 1);
  SArray* pUpTaskList = taosArrayGetP(tasks, size - 2);

  SStreamTask** pDownTask = taosArrayGetLast(pDownTaskList);
  for(int i = begin; i < end; i++){
    SStreamTask* pUpTask = taosArrayGetP(pUpTaskList, i);
    if(pUpTask == NULL) { // out of range
      break;
    }
    streamTaskSetFixedDownstreamInfo(pUpTask, *pDownTask);
    streamTaskSetUpstreamInfo(*pDownTask, pUpTask);
  }
}

static int32_t doScheduleStream(SStreamObj* pStream, SMnode* pMnode, SQueryPlan* pPlan, int64_t nextWindowSkey, SEpSet* pEpset) {
  SSdb*   pSdb = pMnode->pSdb;
  int32_t numOfPlanLevel = LIST_LENGTH(pPlan->pSubplans);
  bool    hasExtraSink = false;
  bool    externalTargetDB = strcmp(pStream->sourceDb, pStream->targetDb) != 0;
  SDbObj* pDbObj = mndAcquireDb(pMnode, pStream->targetDb);
  if (pDbObj == NULL) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    return -1;
  }

  bool multiTarget = (pDbObj->cfg.numOfVgroups > 1);
  sdbRelease(pSdb, pDbObj);

  pStream->tasks = taosArrayInit(numOfPlanLevel + 1, POINTER_BYTES);
  pStream->pHTasksList = taosArrayInit(numOfPlanLevel + 1, POINTER_BYTES);

  if (numOfPlanLevel > 1 || externalTargetDB || multiTarget || pStream->fixedSinkVgId) {
    // add extra sink
    hasExtraSink = true;
    int32_t code = addSinkTask(pMnode, pStream, pEpset);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  pStream->totalLevel = numOfPlanLevel + hasExtraSink;

  SSubplan* plan = getScanSubPlan(pPlan);   // source plan
  if (plan == NULL) {
    return terrno;
  }
  int32_t code = addSourceTask(pMnode, plan, pStream, pEpset, nextWindowSkey);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (numOfPlanLevel == 1) {
    bindSourceSink(pStream, pMnode, pStream->tasks, hasExtraSink);
    if (pStream->conf.fillHistory) {
      bindSourceSink(pStream, pMnode, pStream->pHTasksList, hasExtraSink);
    }
    return TDB_CODE_SUCCESS;
  }

  if(numOfPlanLevel == 3){
    plan = getAggSubPlan(pPlan, 1);  // middle agg plan
    if (plan == NULL) {
      return terrno;
    }
    do{
      SArray** list = taosArrayGetLast(pStream->tasks);
      float size = (float)taosArrayGetSize(*list);
      size_t cnt = (int)(size/tsStreamAggCnt + 0.5);
      if(cnt <= 1) break;

      addNewTaskList(pStream->tasks);
      if (pStream->conf.fillHistory) {
        addNewTaskList(pStream->pHTasksList);
      }

      for(int j = 0; j < cnt; j++){
        code = doAddAggTask(pStream, pMnode, plan, pEpset);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }

        bindTwoLevel(pStream->tasks, j*tsStreamAggCnt, (j+1)*tsStreamAggCnt);
        if (pStream->conf.fillHistory) {
          bindTwoLevel(pStream->pHTasksList, j*tsStreamAggCnt, (j+1)*tsStreamAggCnt);
        }
      }
    }while(1);
  }

  plan = getAggSubPlan(pPlan, 0);
  if (plan == NULL) {
    return terrno;
  }

  SArray** list = taosArrayGetLast(pStream->tasks);
  size_t size = taosArrayGetSize(*list);
  code = addAggTask(pStream, pMnode, plan, pEpset);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  bindTwoLevel(pStream->tasks, 0, size);
  if (pStream->conf.fillHistory) {
    bindTwoLevel(pStream->pHTasksList, 0, size);
  }

  bindAggSink(pStream, pMnode, pStream->tasks);
  if (pStream->conf.fillHistory) {
    bindAggSink(pStream, pMnode, pStream->pHTasksList);
  }
  return TDB_CODE_SUCCESS;
}

int32_t mndScheduleStream(SMnode* pMnode, SStreamObj* pStream, int64_t nextWindowSkey) {
  SQueryPlan* pPlan = qStringToQueryPlan(pStream->physicalPlan);
  if (pPlan == NULL) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    return -1;
  }

  SEpSet mnodeEpset = {0};
  mndGetMnodeEpSet(pMnode, &mnodeEpset);

  int32_t code = doScheduleStream(pStream, pMnode, pPlan, nextWindowSkey, &mnodeEpset);
  qDestroyQueryPlan(pPlan);

  return code;
}

int32_t mndSchedInitSubEp(SMnode* pMnode, const SMqTopicObj* pTopic, SMqSubscribeObj* pSub) {
  SSdb*       pSdb = pMnode->pSdb;
  SVgObj*     pVgroup = NULL;
  SQueryPlan* pPlan = NULL;
  SSubplan*   pSubplan = NULL;

  if (pTopic->subType == TOPIC_SUB_TYPE__COLUMN) {
    pPlan = qStringToQueryPlan(pTopic->physicalPlan);
    if (pPlan == NULL) {
      terrno = TSDB_CODE_QRY_INVALID_INPUT;
      return -1;
    }
  } else if (pTopic->subType == TOPIC_SUB_TYPE__TABLE && pTopic->ast != NULL) {
    SNode* pAst = NULL;
    if (nodesStringToNode(pTopic->ast, &pAst) != 0) {
      mError("topic:%s, failed to create since %s", pTopic->name, terrstr());
      return -1;
    }

    SPlanContext cxt = {.pAstRoot = pAst, .topicQuery = true};
    if (qCreateQueryPlan(&cxt, &pPlan, NULL) != 0) {
      mError("failed to create topic:%s since %s", pTopic->name, terrstr());
      nodesDestroyNode(pAst);
      return -1;
    }
    nodesDestroyNode(pAst);
  }

  if (pPlan) {
    int32_t levelNum = LIST_LENGTH(pPlan->pSubplans);
    if (levelNum != 1) {
      qDestroyQueryPlan(pPlan);
      terrno = TSDB_CODE_MND_INVALID_TOPIC_QUERY;
      return -1;
    }

    SNodeListNode* pNodeListNode = (SNodeListNode*)nodesListGetNode(pPlan->pSubplans, 0);

    int32_t opNum = LIST_LENGTH(pNodeListNode->pNodeList);
    if (opNum != 1) {
      qDestroyQueryPlan(pPlan);
      terrno = TSDB_CODE_MND_INVALID_TOPIC_QUERY;
      return -1;
    }

    pSubplan = (SSubplan*)nodesListGetNode(pNodeListNode->pNodeList, 0);
  }

  void* pIter = NULL;
  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void**)&pVgroup);
    if (pIter == NULL) {
      break;
    }

    if (!mndVgroupInDb(pVgroup, pTopic->dbUid)) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    pSub->vgNum++;

    SMqVgEp* pVgEp = taosMemoryMalloc(sizeof(SMqVgEp));
    pVgEp->epSet = mndGetVgroupEpset(pMnode, pVgroup);
    pVgEp->vgId = pVgroup->vgId;
    taosArrayPush(pSub->unassignedVgs, &pVgEp);

    mInfo("init subscription %s for topic:%s assign vgId:%d", pSub->key, pTopic->name, pVgEp->vgId);

    sdbRelease(pSdb, pVgroup);
  }

  if (pSubplan) {
    int32_t msgLen;

    if (qSubPlanToString(pSubplan, &pSub->qmsg, &msgLen) < 0) {
      qDestroyQueryPlan(pPlan);
      terrno = TSDB_CODE_QRY_INVALID_INPUT;
      return -1;
    }
  } else {
    pSub->qmsg = taosStrdup("");
  }

  qDestroyQueryPlan(pPlan);
  return 0;
}
