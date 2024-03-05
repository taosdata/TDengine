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
#include "mndDb.h"
#include "mndMnode.h"
#include "mndSnode.h"
#include "mndVgroup.h"
#include "parser.h"
#include "tcompare.h"
#include "tmisce.h"
#include "tname.h"
#include "tuuid.h"

#define SINK_NODE_LEVEL (0)
extern bool tsDeployOnSnode;

static bool hasCountWindowNode(SPhysiNode* pNode) {
  if (nodeType(pNode) == QUERY_NODE_PHYSICAL_PLAN_STREAM_COUNT) {
    return true;
  } else {
    size_t size = LIST_LENGTH(pNode->pChildren);

    for (int32_t i = 0; i < size; ++i) {
      SPhysiNode* pChild = (SPhysiNode*)nodesListGetNode(pNode->pChildren, i);
      if (hasCountWindowNode(pChild)) {
        return true;
      }
    }

    return false;
  }
}

static bool isCountWindowStreamTask(SSubplan* pPlan) {
  return hasCountWindowNode((SPhysiNode*)pPlan->pNode);
}

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

  mDebug("mndSetSinkTaskInfo to sma or table, taskId:%s", pTask->id.idStr);

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

// random choose a node to do compute
SVgObj* mndSchedFetchOneVg(SMnode* pMnode, SStreamObj* pStream) {
  SDbObj* pDbObj = mndAcquireDb(pMnode, pStream->sourceDb);
  if (pDbObj == NULL) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    return NULL;
  }

  if (pStream->indexForMultiAggBalance == -1) {
    taosSeedRand(taosSafeRand());
    pStream->indexForMultiAggBalance = taosRand() % pDbObj->cfg.numOfVgroups;
  }

  int32_t index = 0;
  void*   pIter = NULL;
  SVgObj* pVgroup = NULL;
  while (1) {
    pIter = sdbFetch(pMnode->pSdb, SDB_VGROUP, pIter, (void**)&pVgroup);
    if (pIter == NULL) break;
    if (pVgroup->dbUid != pStream->sourceDbUid) {
      sdbRelease(pMnode->pSdb, pVgroup);
      continue;
    }
    if (index++ == pStream->indexForMultiAggBalance) {
      pStream->indexForMultiAggBalance++;
      pStream->indexForMultiAggBalance %= pDbObj->cfg.numOfVgroups;
      sdbCancelFetch(pMnode->pSdb, pIter);
      break;
    }
    sdbRelease(pMnode->pSdb, pVgroup);
  }
  sdbRelease(pMnode->pSdb, pDbObj);

  return pVgroup;
}

static int32_t doAddSinkTask(SStreamObj* pStream, SMnode* pMnode, SVgObj* pVgroup, SEpSet* pEpset, bool isFillhistory) {
  int64_t  uid = (isFillhistory) ? pStream->hTaskUid : pStream->uid;
  SArray** pTaskList = (isFillhistory) ? taosArrayGetLast(pStream->pHTasksList) : taosArrayGetLast(pStream->tasks);

  SStreamTask* pTask = tNewStreamTask(uid, TASK_LEVEL__SINK, pEpset, isFillhistory, 0, *pTaskList,
                                      pStream->conf.fillHistory, pStream->subTableWithoutMd5);
  if (pTask == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  mDebug("doAddSinkTask taskId:%s, vgId:%d, isFillHistory:%d", pTask->id.idStr, pVgroup->vgId, isFillhistory);

  pTask->info.nodeId = pVgroup->vgId;
  pTask->info.epSet = mndGetVgroupEpset(pMnode, pVgroup);
  return mndSetSinkTaskInfo(pStream, pTask);
}

static int32_t doAddSinkTaskToVg(SMnode* pMnode, SStreamObj* pStream, SEpSet* pEpset, SVgObj* vgObj) {
  int32_t code = doAddSinkTask(pStream, pMnode, vgObj, pEpset, false);
  if (code != 0) {
    return code;
  }
  if (pStream->conf.fillHistory) {
    code = doAddSinkTask(pStream, pMnode, vgObj, pEpset, true);
    if (code != 0) {
      return code;
    }
  }
  return TDB_CODE_SUCCESS;
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

    int32_t code = doAddSinkTaskToVg(pMnode, pStream, pEpset, pVgroup);
    if (code != 0) {
      sdbRelease(pSdb, pVgroup);
      return code;
    }

    sdbRelease(pSdb, pVgroup);
  }

  return TDB_CODE_SUCCESS;
}

static int64_t getVgroupLastVer(const SArray* pList, int32_t vgId) {
  int32_t size = (int32_t) taosArrayGetSize(pList);
  for (int32_t i = 0; i < size; ++i) {
    SVgroupVer* pVer = taosArrayGet(pList, i);
    if (pVer->vgId == vgId) {
      return pVer->ver;
    }
  }

  mDebug("no data in vgId:%d for extract last version, set to be 0, total existed vgs:%d", vgId, size);
  return 1;
}

static void streamTaskSetDataRange(SStreamTask* pTask, int64_t skey, SArray* pVerList, int32_t vgId) {
  int64_t latestVer = getVgroupLastVer(pVerList, vgId);
  if (latestVer < 0) {
    latestVer = 0;
  }

  // set the correct ts, which is the last key of queried table.
  SDataRange*  pRange = &pTask->dataRange;
  STimeWindow* pWindow = &pRange->window;

  if (pTask->info.fillHistory) {
    pWindow->skey = INT64_MIN;
    pWindow->ekey = skey - 1;

    pRange->range.minVer = 0;
    pRange->range.maxVer = latestVer;
    mDebug("add fill-history source task 0x%x timeWindow:%" PRId64 "-%" PRId64 " verRange:%" PRId64 "-%" PRId64,
           pTask->id.taskId, pWindow->skey, pWindow->ekey, pRange->range.minVer, pRange->range.maxVer);
  } else {
    pWindow->skey = skey;
    pWindow->ekey = INT64_MAX;

    pRange->range.minVer = latestVer + 1;
    pRange->range.maxVer = INT64_MAX;

    mDebug("add source task 0x%x timeWindow:%" PRId64 "-%" PRId64 " verRange:%" PRId64 "-%" PRId64, pTask->id.taskId,
           pWindow->skey, pWindow->ekey, pRange->range.minVer, pRange->range.maxVer);
  }
}

static void haltInitialTaskStatus(SStreamTask* pTask, SSubplan* pPlan, bool isFillhistoryTask) {
  bool hasCountWindowNode = isCountWindowStreamTask(pPlan);

  if (hasCountWindowNode && (!isFillhistoryTask)) {
    SStreamStatus* pStatus = &pTask->status;
    mDebug("s-task:0x%x status set %s from %s for count window agg task with fill-history option set",
           pTask->id.taskId, streamTaskGetStatusStr(TASK_STATUS__HALT), streamTaskGetStatusStr(pStatus->taskStatus));
    pStatus->taskStatus = TASK_STATUS__HALT;
  }
}

static SStreamTask* buildSourceTask(SStreamObj* pStream, SEpSet* pEpset, bool isFillhistory, bool useTriggerParam) {
  uint64_t uid = (isFillhistory) ? pStream->hTaskUid : pStream->uid;
  SArray** pTaskList = (isFillhistory) ? taosArrayGetLast(pStream->pHTasksList) : taosArrayGetLast(pStream->tasks);

  SStreamTask* pTask =
      tNewStreamTask(uid, TASK_LEVEL__SOURCE, pEpset, isFillhistory, useTriggerParam ? pStream->conf.triggerParam : 0,
                     *pTaskList, pStream->conf.fillHistory, pStream->subTableWithoutMd5);
  if (pTask == NULL) {
    return NULL;
  }

  return pTask;
}

static void addNewTaskList(SStreamObj* pStream) {
  SArray* pTaskList = taosArrayInit(0, POINTER_BYTES);
  taosArrayPush(pStream->tasks, &pTaskList);
  if (pStream->conf.fillHistory) {
    pTaskList = taosArrayInit(0, POINTER_BYTES);
    taosArrayPush(pStream->pHTasksList, &pTaskList);
  }
}

// set the history task id
static void setHTasksId(SStreamObj* pStream) {
  SArray* pTaskList = *(SArray**)taosArrayGetLast(pStream->tasks);
  SArray* pHTaskList = *(SArray**)taosArrayGetLast(pStream->pHTasksList);

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

static int32_t doAddSourceTask(SMnode* pMnode, SSubplan* plan, SStreamObj* pStream, SEpSet* pEpset, int64_t skey,
                               SArray* pVerList, SVgObj* pVgroup, bool isFillhistory, bool useTriggerParam) {
  SStreamTask* pTask = buildSourceTask(pStream, pEpset, isFillhistory, useTriggerParam);
  if (pTask == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  mDebug("doAddSourceTask taskId:%s, vgId:%d, isFillHistory:%d", pTask->id.idStr, pVgroup->vgId, isFillhistory);

  if (pStream->conf.fillHistory) {
    haltInitialTaskStatus(pTask, plan, isFillhistory);
  }

  streamTaskSetDataRange(pTask, skey, pVerList, pVgroup->vgId);

  int32_t code = mndAssignStreamTaskToVgroup(pMnode, pTask, plan, pVgroup);
  if (code != 0) {
    terrno = code;
    return terrno;
  }

  return TDB_CODE_SUCCESS;
}

static SSubplan* getScanSubPlan(const SQueryPlan* pPlan) {
  int32_t        numOfPlanLevel = LIST_LENGTH(pPlan->pSubplans);
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

static SSubplan* getAggSubPlan(const SQueryPlan* pPlan, int index) {
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

static int32_t addSourceTask(SMnode* pMnode, SSubplan* plan, SStreamObj* pStream, SEpSet* pEpset,
                             int64_t nextWindowSkey, SArray* pVerList, bool useTriggerParam) {
  addNewTaskList(pStream);

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

    int code =
        doAddSourceTask(pMnode, plan, pStream, pEpset, nextWindowSkey, pVerList, pVgroup, false, useTriggerParam);
    if (code != 0) {
      mError("create stream task, code:%s", tstrerror(code));

      // todo drop the added source tasks.
      sdbRelease(pSdb, pVgroup);
      return code;
    }

    if (pStream->conf.fillHistory) {
      code = doAddSourceTask(pMnode, plan, pStream, pEpset, nextWindowSkey, pVerList, pVgroup, true, useTriggerParam);
      if (code != 0) {
        sdbRelease(pSdb, pVgroup);
        return code;
      }
    }

    sdbRelease(pSdb, pVgroup);
  }

  if (pStream->conf.fillHistory) {
    setHTasksId(pStream);
  }

  return TSDB_CODE_SUCCESS;
}

static SStreamTask* buildAggTask(SStreamObj* pStream, SEpSet* pEpset, bool isFillhistory, bool useTriggerParam) {
  uint64_t uid = (isFillhistory) ? pStream->hTaskUid : pStream->uid;
  SArray** pTaskList = (isFillhistory) ? taosArrayGetLast(pStream->pHTasksList) : taosArrayGetLast(pStream->tasks);

  SStreamTask* pAggTask =
      tNewStreamTask(uid, TASK_LEVEL__AGG, pEpset, isFillhistory, useTriggerParam ? pStream->conf.triggerParam : 0,
                     *pTaskList, pStream->conf.fillHistory, pStream->subTableWithoutMd5);
  if (pAggTask == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  return pAggTask;
}

static int32_t doAddAggTask(SStreamObj* pStream, SMnode* pMnode, SSubplan* plan, SEpSet* pEpset, SVgObj* pVgroup,
                            SSnodeObj* pSnode, bool isFillhistory, bool useTriggerParam) {
  int32_t      code = 0;
  SStreamTask* pTask = buildAggTask(pStream, pEpset, isFillhistory, useTriggerParam);
  if (pTask == NULL) {
    return terrno;
  }
  if (pSnode != NULL) {
    code = mndAssignStreamTaskToSnode(pMnode, pTask, plan, pSnode);
    mDebug("doAddAggTask taskId:%s, snode id:%d, isFillHistory:%d", pTask->id.idStr, pSnode->id, isFillhistory);

  } else {
    code = mndAssignStreamTaskToVgroup(pMnode, pTask, plan, pVgroup);
    mDebug("doAddAggTask taskId:%s, vgId:%d, isFillHistory:%d", pTask->id.idStr, pVgroup->vgId, isFillhistory);
  }
  return code;
}

static int32_t addAggTask(SStreamObj* pStream, SMnode* pMnode, SSubplan* plan, SEpSet* pEpset, bool useTriggerParam) {
  SVgObj*    pVgroup = NULL;
  SSnodeObj* pSnode = NULL;
  int32_t    code = 0;
  if (tsDeployOnSnode) {
    pSnode = mndSchedFetchOneSnode(pMnode);
    if (pSnode == NULL) {
      pVgroup = mndSchedFetchOneVg(pMnode, pStream);
    }
  } else {
    pVgroup = mndSchedFetchOneVg(pMnode, pStream);
  }

  code = doAddAggTask(pStream, pMnode, plan, pEpset, pVgroup, pSnode, false, useTriggerParam);
  if (code != 0) {
    goto END;
  }

  if (pStream->conf.fillHistory) {
    code = doAddAggTask(pStream, pMnode, plan, pEpset, pVgroup, pSnode, true, useTriggerParam);
    if (code != 0) {
      goto END;
    }

    setHTasksId(pStream);
  }

END:
  if (pSnode != NULL) {
    sdbRelease(pMnode->pSdb, pSnode);
  } else {
    sdbRelease(pMnode->pSdb, pVgroup);
  }
  return code;
}

static int32_t addSinkTask(SMnode* pMnode, SStreamObj* pStream, SEpSet* pEpset) {
  int32_t code = 0;
  addNewTaskList(pStream);

  if (pStream->fixedSinkVgId == 0) {
    code = doAddShuffleSinkTask(pMnode, pStream, pEpset);
    if (code != 0) {
      return code;
    }
  } else {
    code = doAddSinkTaskToVg(pMnode, pStream, pEpset, &pStream->fixedSinkVg);
    if (code != 0) {
      return code;
    }
  }

  if (pStream->conf.fillHistory) {
    setHTasksId(pStream);
  }
  return TDB_CODE_SUCCESS;
}

static void bindTaskToSinkTask(SStreamObj* pStream, SMnode* pMnode, SArray* pSinkTaskList, SStreamTask* task) {
  mndAddDispatcherForInternalTask(pMnode, pStream, pSinkTaskList, task);
  for (int32_t k = 0; k < taosArrayGetSize(pSinkTaskList); k++) {
    SStreamTask* pSinkTask = taosArrayGetP(pSinkTaskList, k);
    streamTaskSetUpstreamInfo(pSinkTask, task);
  }
  mDebug("bindTaskToSinkTask taskId:%s to sink task list", task->id.idStr);
}

static void bindAggSink(SStreamObj* pStream, SMnode* pMnode, SArray* tasks) {
  SArray*  pSinkTaskList = taosArrayGetP(tasks, SINK_NODE_LEVEL);
  SArray** pAggTaskList = taosArrayGetLast(tasks);

  for (int i = 0; i < taosArrayGetSize(*pAggTaskList); i++) {
    SStreamTask* pAggTask = taosArrayGetP(*pAggTaskList, i);
    bindTaskToSinkTask(pStream, pMnode, pSinkTaskList, pAggTask);
    mDebug("bindAggSink taskId:%s to sink task list", pAggTask->id.idStr);
  }
}

static void bindSourceSink(SStreamObj* pStream, SMnode* pMnode, SArray* tasks, bool hasExtraSink) {
  SArray* pSinkTaskList = taosArrayGetP(tasks, SINK_NODE_LEVEL);
  SArray* pSourceTaskList = taosArrayGetP(tasks, hasExtraSink ? SINK_NODE_LEVEL + 1 : SINK_NODE_LEVEL);

  for (int i = 0; i < taosArrayGetSize(pSourceTaskList); i++) {
    SStreamTask* pSourceTask = taosArrayGetP(pSourceTaskList, i);
    mDebug("bindSourceSink taskId:%s to sink task list", pSourceTask->id.idStr);

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
  end = end > taosArrayGetSize(pUpTaskList) ? taosArrayGetSize(pUpTaskList) : end;
  for (int i = begin; i < end; i++) {
    SStreamTask* pUpTask = taosArrayGetP(pUpTaskList, i);
    pUpTask->info.selfChildId = i - begin;
    streamTaskSetFixedDownstreamInfo(pUpTask, *pDownTask);
    streamTaskSetUpstreamInfo(*pDownTask, pUpTask);
  }
  mDebug("bindTwoLevel task list(%d-%d) to taskId:%s", begin, end - 1, (*(pDownTask))->id.idStr);
}

static int32_t doScheduleStream(SStreamObj* pStream, SMnode* pMnode, SQueryPlan* pPlan, SEpSet* pEpset, int64_t skey,
                                SArray* pVerList) {
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

  mDebug("doScheduleStream numOfPlanLevel:%d, exDb:%d, multiTarget:%d, fix vgId:%d, physicalPlan:%s", numOfPlanLevel,
         externalTargetDB, multiTarget, pStream->fixedSinkVgId, pStream->physicalPlan);
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

  SSubplan* plan = getScanSubPlan(pPlan);  // source plan
  if (plan == NULL) {
    return terrno;
  }
  int32_t code = addSourceTask(pMnode, plan, pStream, pEpset, skey, pVerList, numOfPlanLevel == 1);
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

  if (numOfPlanLevel == 3) {
    plan = getAggSubPlan(pPlan, 1);  // middle agg plan
    if (plan == NULL) {
      return terrno;
    }
    do {
      SArray** list = taosArrayGetLast(pStream->tasks);
      float    size = (float)taosArrayGetSize(*list);
      size_t   cnt = (size_t)ceil(size / tsStreamAggCnt);
      if (cnt <= 1) break;

      mDebug("doScheduleStream add middle agg, size:%d, cnt:%d", (int)size, (int)cnt);
      addNewTaskList(pStream);

      for (int j = 0; j < cnt; j++) {
        code = addAggTask(pStream, pMnode, plan, pEpset, false);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }

        bindTwoLevel(pStream->tasks, j * tsStreamAggCnt, (j + 1) * tsStreamAggCnt);
        if (pStream->conf.fillHistory) {
          bindTwoLevel(pStream->pHTasksList, j * tsStreamAggCnt, (j + 1) * tsStreamAggCnt);
        }
      }
    } while (1);
  }

  plan = getAggSubPlan(pPlan, 0);
  if (plan == NULL) {
    return terrno;
  }

  mDebug("doScheduleStream add final agg");
  SArray** list = taosArrayGetLast(pStream->tasks);
  size_t   size = taosArrayGetSize(*list);
  addNewTaskList(pStream);
  code = addAggTask(pStream, pMnode, plan, pEpset, true);
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

int32_t mndScheduleStream(SMnode* pMnode, SStreamObj* pStream, int64_t skey, SArray* pVgVerList) {
  SQueryPlan* pPlan = qStringToQueryPlan(pStream->physicalPlan);
  if (pPlan == NULL) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    return -1;
  }

  SEpSet mnodeEpset = {0};
  mndGetMnodeEpSet(pMnode, &mnodeEpset);

  int32_t code = doScheduleStream(pStream, pMnode, pPlan, &mnodeEpset, skey, pVgVerList);
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
