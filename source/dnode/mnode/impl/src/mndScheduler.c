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
  int32_t     code = 0;
  SNode*      pAst = NULL;
  SQueryPlan* pPlan = NULL;

  if (nodesStringToNode(ast, &pAst) < 0) {
    code = TSDB_CODE_QRY_INVALID_INPUT;
    goto END;
  }

  if (qSetSTableIdForRsma(pAst, uid) < 0) {
    code = TSDB_CODE_QRY_INVALID_INPUT;
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
    code = TSDB_CODE_QRY_INVALID_INPUT;
    goto END;
  }

  int32_t levelNum = LIST_LENGTH(pPlan->pSubplans);
  if (levelNum != 1) {
    code = TSDB_CODE_QRY_INVALID_INPUT;
    goto END;
  }
  SNodeListNode* inner = (SNodeListNode*)nodesListGetNode(pPlan->pSubplans, 0);

  int32_t opNum = LIST_LENGTH(inner->pNodeList);
  if (opNum != 1) {
    code = TSDB_CODE_QRY_INVALID_INPUT;
    goto END;
  }

  SSubplan* plan = (SSubplan*)nodesListGetNode(inner->pNodeList, 0);
  if (qSubPlanToString(plan, pDst, pDstLen) < 0) {
    code = TSDB_CODE_QRY_INVALID_INPUT;
    goto END;
  }

END:
  if (pAst) nodesDestroyNode(pAst);
  if (pPlan) nodesDestroyNode((SNode*)pPlan);
  TAOS_RETURN(code);
}
#ifdef USE_STREAM
int32_t mndSetSinkTaskInfo(SStreamObj* pStream, SStreamTask* pTask) {
  STaskOutputInfo* pInfo = &pTask->outputInfo;

  mDebug("mndSetSinkTaskInfo to sma or table, taskId:%s", pTask->id.idStr);

  if (pStream->smaId != 0 && pStream->subTableWithoutMd5 != 1) {
    pInfo->type = TASK_OUTPUT__SMA;
    pInfo->smaSink.smaId = pStream->smaId;
  } else {
    pInfo->type = TASK_OUTPUT__TABLE;
    pInfo->tbSink.stbUid = pStream->targetStbUid;
    (void)memcpy(pInfo->tbSink.stbFullName, pStream->targetSTbName, TSDB_TABLE_FNAME_LEN);
    pInfo->tbSink.pSchemaWrapper = tCloneSSchemaWrapper(&pStream->outputSchema);
    if (pInfo->tbSink.pSchemaWrapper == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return 0;
}

int32_t mndAddDispatcherForInternalTask(SMnode* pMnode, SStreamObj* pStream, SArray* pSinkNodeList,
                                        SStreamTask* pTask) {
  int32_t code = 0;
  bool isShuffle = false;

  if (pStream->fixedSinkVgId == 0) {
    SDbObj* pDb = mndAcquireDb(pMnode, pStream->targetDb);
    if (pDb != NULL && pDb->cfg.numOfVgroups > 1) {
      isShuffle = true;
      pTask->outputInfo.type = TASK_OUTPUT__SHUFFLE_DISPATCH;
      pTask->msgInfo.msgType = TDMT_STREAM_TASK_DISPATCH;
      TAOS_CHECK_RETURN(mndExtractDbInfo(pMnode, pDb, &pTask->outputInfo.shuffleDispatcher.dbInfo, NULL));
    }

    sdbRelease(pMnode->pSdb, pDb);
  }

  int32_t numOfSinkNodes = taosArrayGetSize(pSinkNodeList);

  if (isShuffle) {
    (void)memcpy(pTask->outputInfo.shuffleDispatcher.stbFullName, pStream->targetSTbName, TSDB_TABLE_FNAME_LEN);
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

  TAOS_RETURN(code);
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
    if (pVgroup->mountVgId) {
      sdbRelease(pMnode->pSdb, pVgroup);
      continue;
    }
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

static void streamGetUidTaskList(SStreamObj* pStream, EStreamTaskType type, uint64_t* pUid, SArray*** pTaskList) {
  if (type == STREAM_NORMAL_TASK) {
    *pUid = pStream->uid;
    *pTaskList = taosArrayGetLast(pStream->pTaskList);
  } else if (type == STREAM_HISTORY_TASK || type == STREAM_RECALCUL_TASK) {
    *pUid = pStream->hTaskUid;
    *pTaskList = taosArrayGetLast(pStream->pHTaskList);
  }
}

static int32_t doAddSinkTask(SStreamObj* pStream, SMnode* pMnode, SVgObj* pVgroup, SEpSet* pEpset, EStreamTaskType type) {
  uint64_t uid = 0;
  SArray** pTaskList = NULL;
  streamGetUidTaskList(pStream, type, &uid, &pTaskList);

  SStreamTask* pTask = NULL;
  int32_t code = tNewStreamTask(uid, TASK_LEVEL__SINK, pEpset, type, pStream->conf.trigger, 0, *pTaskList, pStream->conf.fillHistory,
                                pStream->subTableWithoutMd5, 1, &pTask);
  if (code != 0) {
    return code;
  }

  mDebug("doAddSinkTask taskId:%s, %p vgId:%d, isFillHistory:%d", pTask->id.idStr, pTask, pVgroup->vgId, (type == STREAM_HISTORY_TASK));

  pTask->info.nodeId = pVgroup->vgId;
  pTask->info.epSet = mndGetVgroupEpset(pMnode, pVgroup);
  return mndSetSinkTaskInfo(pStream, pTask);
}

bool needHistoryTask(SStreamObj* pStream) {
  return (pStream->conf.fillHistory) || (pStream->conf.trigger == STREAM_TRIGGER_CONTINUOUS_WINDOW_CLOSE);
}

static int32_t doAddSinkTaskToVg(SMnode* pMnode, SStreamObj* pStream, SEpSet* pEpset, SVgObj* vgObj) {
  int32_t code = doAddSinkTask(pStream, pMnode, vgObj, pEpset, STREAM_NORMAL_TASK);
  if (code != 0) {
    return code;
  }

  if (needHistoryTask(pStream)) {
    EStreamTaskType type = (pStream->conf.trigger == STREAM_TRIGGER_CONTINUOUS_WINDOW_CLOSE) ? STREAM_RECALCUL_TASK
                                                                                             : STREAM_HISTORY_TASK;
    code = doAddSinkTask(pStream, pMnode, vgObj, pEpset, type);
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
    if (pVgroup->mountVgId) {
      sdbRelease(pSdb, pVgroup);
      continue;
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

  if (pTask->info.fillHistory == STREAM_HISTORY_TASK) {
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

static int32_t buildSourceTask(SStreamObj* pStream, SEpSet* pEpset, EStreamTaskType type, bool useTriggerParam,
                               int8_t hasAggTasks, SStreamTask** pTask, SArray* pSourceTaskList) {
  uint64_t uid = 0;
  SArray** pTaskList = NULL;
  if (pSourceTaskList) {
    uid = pStream->uid;
    pTaskList = &pSourceTaskList;
  } else {
    streamGetUidTaskList(pStream, type, &uid, &pTaskList);
  }

  int32_t trigger = 0;
  if (type == STREAM_RECALCUL_TASK) {
    trigger = STREAM_TRIGGER_WINDOW_CLOSE;
  } else {
    trigger = pStream->conf.trigger;
  }

  int32_t triggerParam = useTriggerParam ? pStream->conf.triggerParam : 0;
  int32_t code =
      tNewStreamTask(uid, TASK_LEVEL__SOURCE, pEpset, type, trigger, triggerParam,
                     *pTaskList, pStream->conf.fillHistory, pStream->subTableWithoutMd5,
                     hasAggTasks, pTask);

  return code;
}

static int32_t addNewTaskList(SStreamObj* pStream) {
  SArray* pTaskList = taosArrayInit(0, POINTER_BYTES);
  if (pTaskList == NULL) {
    mError("failed init task list, code:%s", tstrerror(terrno));
    return terrno;
  }

  if (taosArrayPush(pStream->pTaskList, &pTaskList) == NULL) {
    mError("failed to put into array, code:%s", tstrerror(terrno));
    return terrno;
  }

  if (needHistoryTask(pStream)) {
    pTaskList = taosArrayInit(0, POINTER_BYTES);
    if (pTaskList == NULL) {
      mError("failed init history task list, code:%s", tstrerror(terrno));
      return terrno;
    }

    if (taosArrayPush(pStream->pHTaskList, &pTaskList) == NULL) {
      mError("failed to put into array, code:%s", tstrerror(terrno));
      return terrno;
    }
  }

  return TSDB_CODE_SUCCESS;
}

// set the history task id
static void setHTasksId(SStreamObj* pStream) {
  SArray* pTaskList = *(SArray**)taosArrayGetLast(pStream->pTaskList);
  SArray* pHTaskList = *(SArray**)taosArrayGetLast(pStream->pHTaskList);

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

static int32_t addSourceTaskVTableOutput(SStreamTask* pTask, SSHashObj* pVgTasks, SSHashObj* pVtables) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t taskNum = tSimpleHashGetSize(pVgTasks);
  int32_t tbNum = tSimpleHashGetSize(pVtables);

  SSHashObj *pTaskMap = tSimpleHashInit(taskNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  TSDB_CHECK_NULL(pTaskMap, code, lino, _end, terrno);

  pTask->outputInfo.type = TASK_OUTPUT__VTABLE_MAP;
  pTask->msgInfo.msgType = TDMT_STREAM_TASK_DISPATCH;
  STaskDispatcherVtableMap *pDispatcher = &pTask->outputInfo.vtableMapDispatcher;
  pDispatcher->taskInfos = taosArrayInit(taskNum, sizeof(STaskDispatcherFixed));
  TSDB_CHECK_NULL(pDispatcher->taskInfos, code, lino, _end, terrno);
  pDispatcher->vtableMap = tSimpleHashInit(tbNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  TSDB_CHECK_NULL(pDispatcher->vtableMap, code, lino, _end, terrno);

  int32_t               iter = 0, vgId = 0;
  uint64_t              uid = 0;
  void*                 p = NULL;
  while (NULL != (p = tSimpleHashIterate(pVtables, p, &iter))) {
    char* vgUid = tSimpleHashGetKey(p, NULL);
    vgId = *(int32_t*)vgUid;
    uid = *(uint64_t*)((int32_t*)vgUid + 1);
    
    void *px = tSimpleHashGet(pVgTasks, &vgId, sizeof(vgId));
    if (NULL == px) {
      mError("tSimpleHashGet vgId %d not found", vgId);
      return code;
    }
    SStreamTask* pMergeTask = *(SStreamTask**)px;
    if (pMergeTask == NULL) {
      mError("tSimpleHashGet pMergeTask %d not found", vgId);
      return code;
    }

    px = tSimpleHashGet(pTaskMap, &pMergeTask->id.taskId, sizeof(pMergeTask->id.taskId));
    int32_t idx = 0;
    if (px == NULL) {
      STaskDispatcherFixed addr = {
          .taskId = pMergeTask->id.taskId, .nodeId = pMergeTask->info.nodeId, .epSet = pMergeTask->info.epSet};
      px = taosArrayPush(pDispatcher->taskInfos, &addr);
      TSDB_CHECK_NULL(px, code, lino, _end, terrno);
      idx = taosArrayGetSize(pDispatcher->taskInfos) - 1;
      code = tSimpleHashPut(pTaskMap, &pMergeTask->id.taskId, sizeof(pMergeTask->id.taskId), &idx, sizeof(idx));
      if (code) {
        mError("tSimpleHashPut uid to task idx failed, error:%d", code);
        return code;
      }
    } else {
      idx = *(int32_t*)px;
    }

    code = tSimpleHashPut(pDispatcher->vtableMap, &uid, sizeof(int64_t), &idx, sizeof(int32_t));
    if (code) {
      mError("tSimpleHashPut uid to STaskDispatcherFixed failed, error:%d", code);
      return code;
    }

    code = streamTaskSetUpstreamInfo(pMergeTask, pTask);
    if (code != TSDB_CODE_SUCCESS) {
      mError("failed to set upstream info of merge task, error:%d", code);
      return code;
    }

    mDebug("source task[%s,vg:%d] add vtable output map, vuid %" PRIu64 " => [%d, vg:%d]", pTask->id.idStr,
           pTask->info.nodeId, uid, pMergeTask->id.taskId, pMergeTask->info.nodeId);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    mError("source task[%s,vg:%d] add vtable output map failed, lino:%d, error:%s", pTask->id.idStr, pTask->info.nodeId,
           lino, tstrerror(code));
  }
  if (pTaskMap != NULL) {
    tSimpleHashCleanup(pTaskMap);
  }
  return code;
}

static int32_t doAddSourceTask(SMnode* pMnode, SSubplan* plan, SStreamObj* pStream, SEpSet* pEpset, int64_t skey,
                               SArray* pVerList, SVgObj* pVgroup, EStreamTaskType type, bool useTriggerParam,
                               int8_t hasAggTasks, SSHashObj* pVgTasks, SArray* pSourceTaskList) {
  SStreamTask* pTask = NULL;
  int32_t code = buildSourceTask(pStream, pEpset, type, useTriggerParam, hasAggTasks, &pTask, pSourceTaskList);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  mDebug("doAddSourceTask taskId:%s, %p vgId:%d, historyTask:%d", pTask->id.idStr, pTask, pVgroup->vgId, (type == STREAM_HISTORY_TASK));

  if (needHistoryTask(pStream)) {
    haltInitialTaskStatus(pTask, plan, (type == STREAM_HISTORY_TASK));
  }

  streamTaskSetDataRange(pTask, skey, pVerList, pVgroup->vgId);

  code = mndAssignStreamTaskToVgroup(pMnode, pTask, plan, pVgroup);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  mTrace("souce task plan:%s", pTask->exec.qmsg);

  if (pVgTasks) {
    code = addSourceTaskVTableOutput(pTask, pVgTasks, plan->pVTables);
  }

  return code;
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

static int32_t doAddMergeTask(SMnode* pMnode, SSubplan* plan, SStreamObj* pStream, SEpSet* pEpset, SVgObj* pVgroup,
                              bool isHistoryTask, bool useTriggerParam, int8_t hasAggTasks, SArray* pVtables) {
  SStreamTask* pTask = NULL;
  SArray** pTaskList = taosArrayGetLast(pStream->pTaskList);

  int32_t code = tNewStreamTask(pStream->uid, TASK_LEVEL__MERGE, pEpset, isHistoryTask, pStream->conf.trigger,
                                useTriggerParam ? pStream->conf.triggerParam : 0, *pTaskList, pStream->conf.fillHistory,
                                pStream->subTableWithoutMd5, hasAggTasks, &pTask);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  int32_t vtbNum = taosArrayGetSize(pVtables);
  pTask->pVTables = taosArrayInit(vtbNum, sizeof(SVCTableMergeInfo));
  if (NULL == pTask->pVTables) {
    code = terrno;
    mError("taosArrayInit %d SVCTableMergeInfo failed, error:%d", vtbNum, terrno);
    return code;
  }

  SVCTableMergeInfo tbInfo;
  for (int32_t i = 0; i < vtbNum; ++i) {
    SVCTableRefCols** pTb = taosArrayGet(pVtables, i);
    tbInfo.uid = (*pTb)->uid;
    tbInfo.numOfSrcTbls = (*pTb)->numOfSrcTbls;
    if (NULL == taosArrayPush(pTask->pVTables, &tbInfo)) {
      code = terrno;
      mError("taosArrayPush SVCTableMergeInfo failed, error:%d", terrno);
      return code;
    }

    mDebug("merge task[%s, vg:%d] add vtable info: vuid %" PRIu64 ", numOfSrcTbls:%d", 
        pTask->id.idStr, pVgroup->vgId, tbInfo.uid, tbInfo.numOfSrcTbls);
  }

  code = mndAssignStreamTaskToVgroup(pMnode, pTask, plan, pVgroup);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  return TDB_CODE_SUCCESS;
}

static SSubplan* getVTbScanSubPlan(const SQueryPlan* pPlan) {
  int32_t        numOfPlanLevel = LIST_LENGTH(pPlan->pSubplans);
  SNodeListNode* inner = (SNodeListNode*)nodesListGetNode(pPlan->pSubplans, numOfPlanLevel - 2);
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

static int32_t addVTableMergeTask(SMnode* pMnode, SSubplan* plan, SStreamObj* pStream, SEpSet* pEpset,
                                  bool useTriggerParam, bool hasAggTasks, SCMCreateStreamReq* pCreate) {
  SVgObj* pVgroup = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t vgNum = taosArrayGetSize(pCreate->pVSubTables);
  
  for (int32_t i = 0; i < vgNum; ++i) {
    SVSubTablesRsp* pVg = (SVSubTablesRsp*)taosArrayGet(pCreate->pVSubTables, i);
    pVgroup = mndAcquireVgroup(pMnode, pVg->vgId);
    if (NULL == pVgroup) {
      mWarn("vnode %d in pVSubTables not found", pVg->vgId);
      continue;
    }

    code = doAddMergeTask(pMnode, plan, pStream, pEpset, pVgroup, false, useTriggerParam, hasAggTasks, pVg->pTables);
    if (code != 0) {
      mError("failed to create stream task, code:%s", tstrerror(code));

      mndReleaseVgroup(pMnode, pVgroup);
      return code;
    }

    mndReleaseVgroup(pMnode, pVgroup);
  }

  return code;
}

static int32_t buildMergeTaskHash(SArray* pMergeTaskList, SSHashObj** ppVgTasks) {
  int32_t code = 0;
  int32_t taskNum = taosArrayGetSize(pMergeTaskList);

  *ppVgTasks = tSimpleHashInit(taskNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  if (NULL == *ppVgTasks) {
    code = terrno;
    mError("tSimpleHashInit %d failed", taskNum);
    return code;
  }
  
  for (int32_t i = 0; i < taskNum; ++i) {
    SStreamTask* pTask = taosArrayGetP(pMergeTaskList, i);

    code = tSimpleHashPut(*ppVgTasks, &pTask->info.nodeId, sizeof(pTask->info.nodeId), &pTask, POINTER_BYTES);
    if (code) {
      mError("tSimpleHashPut %d STaskDispatcherFixed failed", i);
      return code;
    }
  }

  return code;
}

static int32_t addVTableSourceTask(SMnode* pMnode, SSubplan* plan, SStreamObj* pStream, SEpSet* pEpset,
                                   int64_t nextWindowSkey, SArray* pVerList, bool useTriggerParam, bool hasAggTasks,
                                   SCMCreateStreamReq* pCreate, SSHashObj* pVTableMap, SArray* pSourceTaskList,
                                   SArray* pMergeTaskList) {
  int32_t code = 0;
  SSHashObj* pVgTasks = NULL;
  int32_t vgId = 0;
  int32_t iter = 0;
  SVgObj* pVgroup = NULL;
  void* p = NULL;

  code = buildMergeTaskHash(pMergeTaskList, &pVgTasks);
  if (code) {
    tSimpleHashCleanup(pVgTasks);
    return code;
  }
  
  while (NULL != (p = tSimpleHashIterate(pVTableMap, p, &iter))) {
    char* pDbVg = tSimpleHashGetKey(p, NULL);
    char* pVgStr = strrchr(pDbVg, '.');
    if (NULL == pVgStr) {
      mError("Invalid DbVg string: %s", pDbVg);
      tSimpleHashCleanup(pVgTasks);
      return TSDB_CODE_MND_INTERNAL_ERROR;
    }

    (void)taosStr2int32(pVgStr + 1, &vgId);
    
    pVgroup = mndAcquireVgroup(pMnode, vgId);
    if (NULL == pVgroup) {
      mWarn("vnode %d not found", vgId);
      continue;
    }

    plan->pVTables = *(SSHashObj**)p;
    code = doAddSourceTask(pMnode, plan, pStream, pEpset, nextWindowSkey, pVerList, pVgroup, false, useTriggerParam,
                           hasAggTasks, pVgTasks, pSourceTaskList);
    plan->pVTables = NULL;
    if (code != 0) {
      mError("failed to create stream task, code:%s", tstrerror(code));

      mndReleaseVgroup(pMnode, pVgroup);
      tSimpleHashCleanup(pVgTasks);
      return code;
    }

    mndReleaseVgroup(pMnode, pVgroup);
  }

  tSimpleHashCleanup(pVgTasks);

  return TSDB_CODE_SUCCESS;
}

static int32_t addSourceTask(SMnode* pMnode, SSubplan* plan, SStreamObj* pStream, SEpSet* pEpset,
                             int64_t nextWindowSkey, SArray* pVerList, bool useTriggerParam, bool hasAggTasks) {
  void*   pIter = NULL;
  SSdb*   pSdb = pMnode->pSdb;
  int32_t code = addNewTaskList(pStream);
  if (code) {
    return code;
  }

  while (1) {
    SVgObj* pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void**)&pVgroup);
    if (pIter == NULL) {
      break;
    }
    if (pVgroup->mountVgId) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    if (!mndVgroupInDb(pVgroup, pStream->sourceDbUid)) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    code = doAddSourceTask(pMnode, plan, pStream, pEpset, nextWindowSkey, pVerList, pVgroup, STREAM_NORMAL_TASK,
                           useTriggerParam, hasAggTasks, NULL, NULL);
    if (code != 0) {
      mError("failed to create stream task, code:%s", tstrerror(code));

      mndReleaseVgroup(pMnode, pVgroup);
      return code;
    }

    if (needHistoryTask(pStream)) {
      EStreamTaskType type = 0;
      if (pStream->conf.trigger == STREAM_TRIGGER_CONTINUOUS_WINDOW_CLOSE && (pStream->conf.fillHistory == 0)) {
        type = STREAM_RECALCUL_TASK; // only the recalculating task
      } else {
        type = STREAM_HISTORY_TASK; // set the fill-history option
      }

      code = doAddSourceTask(pMnode, plan, pStream, pEpset, nextWindowSkey, pVerList, pVgroup, type,
                             useTriggerParam, hasAggTasks, NULL, NULL);
      if (code != 0) {
        sdbRelease(pSdb, pVgroup);
        return code;
      }
    }

    sdbRelease(pSdb, pVgroup);
  }

  if (needHistoryTask(pStream)) {
    setHTasksId(pStream);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t buildAggTask(SStreamObj* pStream, SEpSet* pEpset, EStreamTaskType type, bool useTriggerParam,
                            SStreamTask** pAggTask) {
  *pAggTask = NULL;

  uint64_t uid = 0;
  SArray** pTaskList = NULL;
  streamGetUidTaskList(pStream, type, &uid, &pTaskList);

  int64_t triggerParam = useTriggerParam? pStream->conf.triggerParam:0;
  int32_t code = tNewStreamTask(uid, TASK_LEVEL__AGG, pEpset, type, pStream->conf.trigger,
                                triggerParam, *pTaskList, pStream->conf.fillHistory,
                                pStream->subTableWithoutMd5, 1, pAggTask);
  return code;
}

static int32_t doAddAggTask(SStreamObj* pStream, SMnode* pMnode, SSubplan* plan, SEpSet* pEpset, SVgObj* pVgroup,
                            SSnodeObj* pSnode, EStreamTaskType type, bool useTriggerParam) {
  int32_t      code = 0;
  SStreamTask* pTask = NULL;
  const char*  id = NULL;

  code = buildAggTask(pStream, pEpset, type, useTriggerParam, &pTask);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  id = pTask->id.idStr;
  if (pSnode != NULL) {
    code = mndAssignStreamTaskToSnode(pMnode, pTask, plan, pSnode);
    mDebug("doAddAggTask taskId:%s, %p snode id:%d, isFillHistory:%d", id, pTask, pSnode->id, (type == STREAM_HISTORY_TASK));
  } else {
    code = mndAssignStreamTaskToVgroup(pMnode, pTask, plan, pVgroup);
    mDebug("doAddAggTask taskId:%s, %p vgId:%d, isFillHistory:%d", id, pTask, pVgroup->vgId, (type == STREAM_HISTORY_TASK));
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

  code = doAddAggTask(pStream, pMnode, plan, pEpset, pVgroup, pSnode, STREAM_NORMAL_TASK, useTriggerParam);
  if (code != 0) {
    goto END;
  }

  if (needHistoryTask(pStream)) {
    EStreamTaskType type = 0;
    if (pStream->conf.trigger == STREAM_TRIGGER_CONTINUOUS_WINDOW_CLOSE && (pStream->conf.fillHistory == 0)) {
      type = STREAM_RECALCUL_TASK;  // only the recalculating task
    } else {
      type = STREAM_HISTORY_TASK;  // set the fill-history option
    }
    code = doAddAggTask(pStream, pMnode, plan, pEpset, pVgroup, pSnode, type, useTriggerParam);
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
  int32_t code = addNewTaskList(pStream);
  if (code) {
    return code;
  }

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

  if (needHistoryTask(pStream)) {
    setHTasksId(pStream);
  }

  return TDB_CODE_SUCCESS;
}

static void bindTaskToSinkTask(SStreamObj* pStream, SMnode* pMnode, SArray* pSinkTaskList, SStreamTask* task) {
  int32_t code = 0;
  if ((code = mndAddDispatcherForInternalTask(pMnode, pStream, pSinkTaskList, task)) != 0) {
    mError("failed bind task to sink task since %s", tstrerror(code));
  }
  for (int32_t k = 0; k < taosArrayGetSize(pSinkTaskList); k++) {
    SStreamTask* pSinkTask = taosArrayGetP(pSinkTaskList, k);
    if ((code = streamTaskSetUpstreamInfo(pSinkTask, task)) != 0) {
      mError("failed bind task to sink task since %s", tstrerror(code));
    }
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
  int32_t code = 0;
  SArray* pSinkTaskList = taosArrayGetP(tasks, SINK_NODE_LEVEL);
  SArray* pSourceTaskList = taosArrayGetP(tasks, hasExtraSink ? SINK_NODE_LEVEL + 1 : SINK_NODE_LEVEL);

  for (int i = 0; i < taosArrayGetSize(pSourceTaskList); i++) {
    SStreamTask* pSourceTask = taosArrayGetP(pSourceTaskList, i);
    mDebug("bindSourceSink taskId:%s to sink task list", pSourceTask->id.idStr);

    if (hasExtraSink) {
      bindTaskToSinkTask(pStream, pMnode, pSinkTaskList, pSourceTask);
    } else {
      if ((code = mndSetSinkTaskInfo(pStream, pSourceTask)) != 0) {
        mError("failed bind task to sink task since %s", tstrerror(code));
      }
    }
  }
}

static void bindVtableMergeSink(SStreamObj* pStream, SMnode* pMnode, SArray* tasks, bool hasExtraSink) {
  int32_t code = 0;
  SArray* pSinkTaskList = taosArrayGetP(tasks, SINK_NODE_LEVEL);
  SArray* pMergeTaskList = taosArrayGetP(tasks, hasExtraSink ? SINK_NODE_LEVEL + 2 : SINK_NODE_LEVEL + 1);

  for (int i = 0; i < taosArrayGetSize(pMergeTaskList); i++) {
    SStreamTask* pMergeTask = taosArrayGetP(pMergeTaskList, i);
    mDebug("bindVtableMergeSink taskId:%s to sink task list", pMergeTask->id.idStr);

    if (hasExtraSink) {
      bindTaskToSinkTask(pStream, pMnode, pSinkTaskList, pMergeTask);
    } else {
      if ((code = mndSetSinkTaskInfo(pStream, pMergeTask)) != 0) {
        mError("failed bind task to sink task since %s", tstrerror(code));
      }
    }
  }
}


static void bindTwoLevel(SArray* tasks, int32_t begin, int32_t end) {
  int32_t code = 0;
  size_t size = taosArrayGetSize(tasks);
  if (size < 2) {
    mError("task list size is less than 2");
    return;
  }
  SArray* pDownTaskList = taosArrayGetP(tasks, size - 1);
  SArray* pUpTaskList = taosArrayGetP(tasks, size - 2);

  SStreamTask** pDownTask = taosArrayGetLast(pDownTaskList);
  end = end > taosArrayGetSize(pUpTaskList) ? taosArrayGetSize(pUpTaskList) : end;
  for (int i = begin; i < end; i++) {
    SStreamTask* pUpTask = taosArrayGetP(pUpTaskList, i);
    pUpTask->info.selfChildId = i - begin;
    streamTaskSetFixedDownstreamInfo(pUpTask, *pDownTask);
    if ((code = streamTaskSetUpstreamInfo(*pDownTask, pUpTask)) != 0) {
      mError("failed bind task to sink task since %s", tstrerror(code));
    }
  }
  mDebug("bindTwoLevel task list(%d-%d) to taskId:%s", begin, end - 1, (*(pDownTask))->id.idStr);
}

int32_t tableHashValueComp(void const* lp, void const* rp) {
  uint32_t*    key = (uint32_t*)lp;
  SVgroupInfo* pVg = (SVgroupInfo*)rp;

  if (*key < pVg->hashBegin) {
    return -1;
  } else if (*key > pVg->hashEnd) {
    return 1;
  }

  return 0;
}


int dbVgInfoComp(const void* lp, const void* rp) {
  SVGroupHashInfo* pLeft = (SVGroupHashInfo*)lp;
  SVGroupHashInfo* pRight = (SVGroupHashInfo*)rp;
  if (pLeft->hashBegin < pRight->hashBegin) {
    return -1;
  } else if (pLeft->hashBegin > pRight->hashBegin) {
    return 1;
  }

  return 0;
}

int32_t getTableVgId(SDBVgHashInfo* dbInfo, int32_t acctId, char* dbFName, int32_t* vgId, char *tbName) {
  int32_t code = 0;
  int32_t lino = 0;
  SVgroupInfo* vgInfo = NULL;
  char         tbFullName[TSDB_TABLE_FNAME_LEN];
  (void)snprintf(tbFullName, sizeof(tbFullName), "%s.%s", dbFName, tbName);
  uint32_t hashValue = taosGetTbHashVal(tbFullName, (uint32_t)strlen(tbFullName), dbInfo->hashMethod,
                                        dbInfo->hashPrefix, dbInfo->hashSuffix);

  if (!dbInfo->vgSorted) {
    taosArraySort(dbInfo->vgArray, dbVgInfoComp);
    dbInfo->vgSorted = true;
  }

  vgInfo = taosArraySearch(dbInfo->vgArray, &hashValue, tableHashValueComp, TD_EQ);
  if (NULL == vgInfo) {
    qError("no hash range found for hash value [%u], dbFName:%s, numOfVgId:%d", hashValue, dbFName,
             (int32_t)taosArrayGetSize(dbInfo->vgArray));
    return TSDB_CODE_INVALID_PARA;
  }

  *vgId = vgInfo->vgId;

_return:

  return code;
}


static void destroyVSubtableVtb(SSHashObj *pVtable) {
  int32_t iter = 0;
  void* p = NULL;
  while (NULL != (p = tSimpleHashIterate(pVtable, p, &iter))) {
    taosArrayDestroy(*(SArray**)p);
  }

  tSimpleHashCleanup(pVtable);
}

static void destroyVSubtableVgHash(SSHashObj *pVg) {
  int32_t iter = 0;
  SSHashObj** pVtable = NULL;
  void* p = NULL;
  while (NULL != (p = tSimpleHashIterate(pVg, p, &iter))) {
    pVtable = (SSHashObj**)p;
    destroyVSubtableVtb(*pVtable);
  }

  tSimpleHashCleanup(pVg);
}

static void destroyDbVgroupsHash(SSHashObj *pDbVgs) {
  int32_t iter = 0;
  SDBVgHashInfo* pVg = NULL;
  void* p = NULL;
  while (NULL != (p = tSimpleHashIterate(pDbVgs, p, &iter))) {
    pVg = (SDBVgHashInfo*)p;
    taosArrayDestroy(pVg->vgArray);
  }
  
  tSimpleHashCleanup(pDbVgs);
}

static int32_t buildDBVgroupsMap(SMnode* pMnode, SSHashObj* pDbVgroup) {
  void*   pIter = NULL;
  SSdb*   pSdb = pMnode->pSdb;
  int32_t code = TSDB_CODE_SUCCESS;
  char    key[TSDB_DB_NAME_LEN + 32];
  SArray* pTarget = NULL;
  SArray* pNew = NULL;
  SDbObj* pDb = NULL;
  SDBVgHashInfo dbInfo = {0}, *pDbInfo = NULL;

  while (1) {
    SVgObj* pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void**)&pVgroup);
    if (pIter == NULL) {
      break;
    }
    if (pVgroup->mountVgId) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    pDbInfo = (SDBVgHashInfo*)tSimpleHashGet(pDbVgroup, pVgroup->dbName, strlen(pVgroup->dbName) + 1);
    if (NULL == pDbInfo) {
      pNew = taosArrayInit(20, sizeof(SVGroupHashInfo));
      if (NULL == pNew) {
        code = terrno;
        mError("taosArrayInit SVGroupHashInfo failed, code:%s", tstrerror(terrno));
        sdbRelease(pSdb, pVgroup);
        return code;
      }      

      pDb = mndAcquireDb(pMnode, pVgroup->dbName);
      if (pDb == NULL) {
        code = terrno;
        mError("mndAcquireDb %s failed, code:%s", pVgroup->dbName, tstrerror(terrno));
        sdbRelease(pSdb, pVgroup);
        return code;
      }

      dbInfo.vgSorted = false;
      dbInfo.hashMethod = pDb->cfg.hashMethod;
      dbInfo.hashPrefix = pDb->cfg.hashPrefix;
      dbInfo.hashSuffix = pDb->cfg.hashSuffix;
      dbInfo.vgArray = pNew;
      
      mndReleaseDb(pMnode, pDb);

      pTarget = pNew;
    } else {
      pTarget = pDbInfo->vgArray;
    }

    SVGroupHashInfo vgInfo = {.vgId = pVgroup->vgId, .hashBegin = pVgroup->hashBegin, .hashEnd = pVgroup->hashEnd};
    if (NULL == taosArrayPush(pTarget, &vgInfo)) {
      code = terrno;
      mError("taosArrayPush SVGroupHashInfo failed, code:%s", tstrerror(terrno));
      taosArrayDestroy(pNew);
      sdbRelease(pSdb, pVgroup);
      return code;
    }

    if (NULL == pDbInfo) {
      code = tSimpleHashPut(pDbVgroup, pVgroup->dbName, strlen(pVgroup->dbName) + 1, &dbInfo, sizeof(dbInfo));
      if (code != 0) {
        mError("tSimpleHashPut SDBVgHashInfo failed, code:%s", tstrerror(code));
        taosArrayDestroy(pNew);
        sdbRelease(pSdb, pVgroup);
        return code;
      }
      
      pNew = NULL;
    }

    sdbRelease(pSdb, pVgroup);
  }

  return code;
}

static int32_t addVTableToVnode(SSHashObj* pVg, int32_t vvgId, uint64_t vuid, SRefColInfo* pCol, SStreamVBuildCtx* pCtx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SSHashObj* pNewVtable = NULL;
  SArray* pNewOtable = NULL, *pTarOtable = NULL;
  SColIdName col;
  char vId[sizeof(int32_t) + sizeof(uint64_t)];
  *(int32_t*)vId = vvgId;
  *(uint64_t*)((int32_t*)vId + 1) = vuid;

  pCtx->lastUid = vuid;

  SSHashObj** pVtable = (SSHashObj**)tSimpleHashGet(pVg, vId, sizeof(vId));
  if (NULL == pVtable) {
    pNewVtable = (SSHashObj*)tSimpleHashInit(0, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
    TSDB_CHECK_NULL(pNewVtable, code, lino, _return, terrno);
    pNewOtable = taosArrayInit(4, sizeof(SColIdName));
    TSDB_CHECK_NULL(pNewOtable, code, lino, _return, terrno);
    tSimpleHashSetFreeFp(pNewVtable, tFreeStreamVtbOtbInfo);
    col.colId = pCol->colId;
    col.colName = taosStrdup(pCol->refColName);
    TSDB_CHECK_NULL(col.colName, code, lino, _return, terrno);
    TSDB_CHECK_NULL(taosArrayPush(pNewOtable, &col), code, lino, _return, terrno);
    TSDB_CHECK_CODE(tSimpleHashPut(pNewVtable, pCol->refTableName, strlen(pCol->refTableName) + 1, &pNewOtable, POINTER_BYTES), lino, _return);
    TSDB_CHECK_CODE(tSimpleHashPut(pVg, vId, sizeof(vId), &pNewVtable, POINTER_BYTES), lino, _return);

    pCtx->lastVtable = pNewVtable;
    pCtx->lastOtable = pNewOtable;

    return code;
  }
  
  SArray** pOtable = tSimpleHashGet(*pVtable, pCol->refTableName, strlen(pCol->refTableName) + 1);
  if (NULL == pOtable) {
    pNewOtable = taosArrayInit(4, sizeof(SColIdName));
    TSDB_CHECK_NULL(pNewOtable, code, lino, _return, terrno);
    pTarOtable = pNewOtable;
  } else {
    pTarOtable = *pOtable;
  }
  
  col.colId = pCol->colId;
  col.colName = taosStrdup(pCol->refColName);
  TSDB_CHECK_NULL(col.colName, code, lino, _return, terrno);  
  TSDB_CHECK_NULL(taosArrayPush(pTarOtable, &col), code, lino, _return, terrno);
  if (NULL == pOtable) {
    TSDB_CHECK_CODE(tSimpleHashPut(*pVtable, pCol->refTableName, strlen(pCol->refTableName) + 1, &pNewOtable, POINTER_BYTES), lino, _return);
  }

  pCtx->lastVtable = *pVtable;
  pCtx->lastOtable = pTarOtable;

_return:

  if (code) {
    mError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}

static int32_t addVgroupToRes(char* fDBName, int32_t vvgId, uint64_t vuid, SRefColInfo* pCol, SDBVgHashInfo* pDb, SSHashObj* pRes, SStreamVBuildCtx* pCtx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t vgId = 0;
  char dbVgId[TSDB_DB_NAME_LEN + 32];
  SSHashObj *pTarVg = NULL, *pNewVg = NULL;
  
  TSDB_CHECK_CODE(getTableVgId(pDb, 1, fDBName, &vgId, pCol->refTableName), lino, _return);

  snprintf(dbVgId, sizeof(dbVgId), "%s.%d", pCol->refDbName, vgId);

  SSHashObj** pVg = (SSHashObj**)tSimpleHashGet(pRes, dbVgId, strlen(dbVgId) + 1);
  if (NULL == pVg) {
    pNewVg = tSimpleHashInit(10, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    TSDB_CHECK_NULL(pNewVg, code, lino, _return, terrno);
    tSimpleHashSetFreeFp(pNewVg, tFreeStreamVtbVtbInfo);
    pTarVg = pNewVg;
  } else {
    pTarVg = *pVg;
  }

  TSDB_CHECK_CODE(addVTableToVnode(pTarVg, vvgId, vuid, pCol, pCtx), lino, _return);

  if (NULL == pVg) {
    TSDB_CHECK_CODE(tSimpleHashPut(pRes, dbVgId, strlen(dbVgId) + 1, &pNewVg, POINTER_BYTES), lino, _return);
    pNewVg = NULL;
  }

  pCtx->lastVg = pTarVg;

_return:

  if (code) {
    mError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  destroyVSubtableVgHash(pNewVg);

  return code;
}

static int32_t addRefColToMap(int32_t vvgId, uint64_t vuid, SRefColInfo* pCol, SSHashObj* pDbVgroups, SSHashObj* pRes, SStreamVBuildCtx* pCtx) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  bool isLastVtable = vuid == pCtx->lastUid;
  SSHashObj* currOtable = NULL;
  SColIdName col;
  char fDBName[TSDB_DB_FNAME_LEN];
  
  if (pCtx->lastCol && pCtx->lastCol->refDbName[0] == pCol->refDbName[0] && pCtx->lastCol->refTableName[0] == pCol->refTableName[0] &&
     0 == strcmp(pCtx->lastCol->refDbName, pCol->refDbName) && 0 == strcmp(pCtx->lastCol->refTableName, pCol->refTableName)) {
    if (isLastVtable) {
      col.colId = pCol->colId;
      col.colName = taosStrdup(pCol->refColName);
      TSDB_CHECK_NULL(col.colName, code, lino, _return, terrno);
      TSDB_CHECK_NULL(taosArrayPush(pCtx->lastOtable, &col), code, lino, _return, terrno);
      return code;
    }

    TSDB_CHECK_CODE(addVTableToVnode(pCtx->lastVg, vvgId, vuid, pCol, pCtx), lino, _return);
    return code;
  }

  snprintf(fDBName, sizeof(fDBName), "1.%s", pCol->refDbName);
  SDBVgHashInfo* pDb = (SDBVgHashInfo*)tSimpleHashGet(pDbVgroups, fDBName, strlen(fDBName) + 1);
  if (NULL == pDb) {
    mError("refDb %s does not exist", pCol->refDbName);
    code = TSDB_CODE_MND_DB_NOT_EXIST;
    goto _return;
  }

  TSDB_CHECK_CODE(addVgroupToRes(fDBName, vvgId, vuid, pCol, pDb, pRes, pCtx), lino, _return);

  pCtx->lastCol = pCol;

_return:

  if (code) {
    mError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}

static int32_t buildVSubtableMap(SMnode* pMnode, SArray* pVSubTables, SSHashObj** ppRes) {
  int32_t code = 0;
  int32_t lino = 0;

  SSHashObj* pDbVgroups = tSimpleHashInit(20, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
  if (NULL == pDbVgroups) {
    mError("tSimpleHashInit failed, error:%s", tstrerror(terrno));
    return terrno;
  }
  
  TAOS_CHECK_EXIT(buildDBVgroupsMap(pMnode, pDbVgroups));

  *ppRes = tSimpleHashInit(100, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
  if (NULL == *ppRes) {
    code = terrno;
    mError("tSimpleHashInit failed, error:%s", tstrerror(terrno));
    goto _exit;
  }
  tSimpleHashSetFreeFp(*ppRes, tFreeStreamVtbDbVgInfo);

  SStreamVBuildCtx ctx = {0};
  int32_t vgNum = taosArrayGetSize(pVSubTables);
  for (int32_t i = 0; i < vgNum; ++i) {
    SVSubTablesRsp* pVgTbs = taosArrayGet(pVSubTables, i);
    int32_t tbNum = taosArrayGetSize(pVgTbs->pTables);
    for (int32_t n = 0; n < tbNum; ++n) {
      SVCTableRefCols* pTb = (SVCTableRefCols*)taosArrayGetP(pVgTbs->pTables, n);
      for (int32_t m = 0; m < pTb->numOfColRefs; ++m) {
        SRefColInfo* pCol = pTb->refCols + m;
        TAOS_CHECK_EXIT(addRefColToMap(pVgTbs->vgId, pTb->uid, pCol, pDbVgroups, *ppRes, &ctx));
      }
    }
  }

_exit:

  if (code) {
    mError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  destroyDbVgroupsHash(pDbVgroups);

  return code;
}

static int32_t doScheduleStream(SStreamObj* pStream, SMnode* pMnode, SQueryPlan* pPlan, SEpSet* pEpset, SCMCreateStreamReq* pCreate) {
  int32_t code = 0;
  bool    isVTableStream = (NULL != pCreate->pVSubTables);
  int64_t skey = pCreate->lastTs;
  SArray* pVerList = pCreate->pVgroupVerList;
  SSdb*   pSdb = pMnode->pSdb;
  int32_t numOfPlanLevel = LIST_LENGTH(pPlan->pSubplans);
  bool    hasExtraSink = false;
  bool    externalTargetDB = strcmp(pStream->sourceDb, pStream->targetDb) != 0;
  SSubplan* plan = NULL;
  SDbObj* pDbObj = mndAcquireDb(pMnode, pStream->targetDb);

  if (pDbObj == NULL) {
    code = TSDB_CODE_QRY_INVALID_INPUT;
    TAOS_RETURN(code);
  }

  bool multiTarget = (pDbObj->cfg.numOfVgroups > 1);
  sdbRelease(pSdb, pDbObj);

  mDebug("doScheduleStream numOfPlanLevel:%d, exDb:%d, multiTarget:%d, fix vgId:%d, physicalPlan:%s", numOfPlanLevel,
         externalTargetDB, multiTarget, pStream->fixedSinkVgId, pStream->physicalPlan);

  pStream->pTaskList = taosArrayInit(numOfPlanLevel + 1, POINTER_BYTES);
  pStream->pHTaskList = taosArrayInit(numOfPlanLevel + 1, POINTER_BYTES);
  if (pStream->pTaskList == NULL || pStream->pHTaskList == NULL) {
    mError("failed to create stream obj, code:%s", tstrerror(terrno));
    return terrno;
  }

  if (pCreate->pVSubTables) {
    code = buildVSubtableMap(pMnode, pCreate->pVSubTables, &pStream->pVTableMap);
    if (TSDB_CODE_SUCCESS != code) {
      mError("failed to buildVSubtableMap, code:%s", tstrerror(terrno));
      return code;
    }
  }

  if ((numOfPlanLevel > 1 && !isVTableStream) || (numOfPlanLevel > 2 && isVTableStream) || externalTargetDB ||
      multiTarget || pStream->fixedSinkVgId) {
    // add extra sink
    hasExtraSink = true;
    code = addSinkTask(pMnode, pStream, pEpset);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  pStream->totalLevel = numOfPlanLevel + hasExtraSink;

  int8_t hasAggTasks = (numOfPlanLevel > 1) ? 1 : 0;  // task level is greater than 1, which means agg existing
  if (pStream->pVTableMap) {
    code = addNewTaskList(pStream);
    if (code) {
      return code;
    }

    plan = getVTbScanSubPlan(pPlan);
    if (plan == NULL) {
      mError("fail to get vtable scan plan");
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      TAOS_RETURN(code);
    }

    SArray** pSourceTaskList = taosArrayGetLast(pStream->pTaskList);

    code = addNewTaskList(pStream);
    if (code) {
      return code;
    }
    code = addVTableMergeTask(pMnode, plan, pStream, pEpset, (numOfPlanLevel == 1), hasAggTasks, pCreate);
    if (code) {
      return code;
    }

    plan = getScanSubPlan(pPlan);  // source plan
    if (plan == NULL) {
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      TAOS_RETURN(code);
    }

    SArray** pMergeTaskList = taosArrayGetLast(pStream->pTaskList);
    code = addVTableSourceTask(pMnode, plan, pStream, pEpset, skey, pVerList, (numOfPlanLevel == 1), hasAggTasks,
                               pCreate, pStream->pVTableMap, *pSourceTaskList, *pMergeTaskList);
  } else {
    plan = getScanSubPlan(pPlan);  // source plan
    if (plan == NULL) {
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      TAOS_RETURN(code);
    }

    code = addSourceTask(pMnode, plan, pStream, pEpset, skey, pVerList, (numOfPlanLevel == 1), hasAggTasks);
  }
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if ((numOfPlanLevel == 1 && !isVTableStream)) {
    bindSourceSink(pStream, pMnode, pStream->pTaskList, hasExtraSink);
    if (needHistoryTask(pStream)) {
      bindSourceSink(pStream, pMnode, pStream->pHTaskList, hasExtraSink);
    }
    return TDB_CODE_SUCCESS;
  }

  if (numOfPlanLevel == 2 && isVTableStream) {
    bindVtableMergeSink(pStream, pMnode, pStream->pTaskList, hasExtraSink);
    return TDB_CODE_SUCCESS;
  }

  if ((numOfPlanLevel == 3 && !isVTableStream) || (numOfPlanLevel == 4 && isVTableStream)) {
    int32_t idx = isVTableStream ? 2 : 1;
    plan = getAggSubPlan(pPlan, idx);  // middle agg plan
    if (plan == NULL) {
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      TAOS_RETURN(code);
    }

    do {
      SArray** list = taosArrayGetLast(pStream->pTaskList);
      float    size = (float)taosArrayGetSize(*list);
      size_t   cnt = (size_t)ceil(size / tsStreamAggCnt);
      if (cnt <= 1) break;

      mDebug("doScheduleStream add middle agg, size:%d, cnt:%d", (int)size, (int)cnt);
      code = addNewTaskList(pStream);
      if (code) {
        return code;
      }

      for (int j = 0; j < cnt; j++) {
        code = addAggTask(pStream, pMnode, plan, pEpset, false);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }

        bindTwoLevel(pStream->pTaskList, j * tsStreamAggCnt, (j + 1) * tsStreamAggCnt);
        if (needHistoryTask(pStream)) {
          bindTwoLevel(pStream->pHTaskList, j * tsStreamAggCnt, (j + 1) * tsStreamAggCnt);
        }
      }
    } while (1);
  }

  plan = getAggSubPlan(pPlan, 0);
  if (plan == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  mDebug("doScheduleStream add final agg");
  SArray** list = taosArrayGetLast(pStream->pTaskList);
  size_t   size = taosArrayGetSize(*list);

  code = addNewTaskList(pStream);
  if (code) {
    return code;
  }

  code = addAggTask(pStream, pMnode, plan, pEpset, true);
  if (code != TSDB_CODE_SUCCESS) {
    TAOS_RETURN(code);
  }
  bindTwoLevel(pStream->pTaskList, 0, size);
  if (needHistoryTask(pStream)) {
    bindTwoLevel(pStream->pHTaskList, 0, size);
  }

  bindAggSink(pStream, pMnode, pStream->pTaskList);
  if (needHistoryTask(pStream)) {
    bindAggSink(pStream, pMnode, pStream->pHTaskList);
  }
  TAOS_RETURN(code);
}

int32_t mndScheduleStream(SMnode* pMnode, SStreamObj* pStream, SCMCreateStreamReq* pCreate) {
  int32_t     code = 0;
  pStream->pPlan = qStringToQueryPlan(pStream->physicalPlan);
  if (pStream->pPlan == NULL) {
    code = TSDB_CODE_QRY_INVALID_INPUT;
    TAOS_RETURN(code);
  }

  SEpSet mnodeEpset = {0};
  mndGetMnodeEpSet(pMnode, &mnodeEpset);

  code = doScheduleStream(pStream, pMnode, pStream->pPlan, &mnodeEpset, pCreate);

  TAOS_RETURN(code);
}
#endif

#ifdef USE_TOPIC
int32_t mndSchedInitSubEp(SMnode* pMnode, const SMqTopicObj* pTopic, SMqSubscribeObj* pSub) {
  int32_t     code = 0;
  SSdb*       pSdb = pMnode->pSdb;
  SVgObj*     pVgroup = NULL;
  SQueryPlan* pPlan = NULL;
  SSubplan*   pSubplan = NULL;

  if (pTopic->subType == TOPIC_SUB_TYPE__COLUMN) {
    pPlan = qStringToQueryPlan(pTopic->physicalPlan);
    if (pPlan == NULL) {
      return TSDB_CODE_QRY_INVALID_INPUT;
    }
  } else if (pTopic->subType == TOPIC_SUB_TYPE__TABLE && pTopic->ast != NULL) {
    SNode* pAst = NULL;
    code = nodesStringToNode(pTopic->ast, &pAst);
    if (code != 0) {
      mError("topic:%s, failed to create since %s", pTopic->name, terrstr());
      return code;
    }

    SPlanContext cxt = {.pAstRoot = pAst, .topicQuery = true};
    code = qCreateQueryPlan(&cxt, &pPlan, NULL);
    if (code != 0) {
      mError("failed to create topic:%s since %s", pTopic->name, terrstr());
      nodesDestroyNode(pAst);
      return code;
    }
    nodesDestroyNode(pAst);
  }

  if (pPlan) {
    int32_t levelNum = LIST_LENGTH(pPlan->pSubplans);
    if (levelNum != 1) {
      code = TSDB_CODE_MND_INVALID_TOPIC_QUERY;
      goto END;
    }

    SNodeListNode* pNodeListNode = (SNodeListNode*)nodesListGetNode(pPlan->pSubplans, 0);
    if (pNodeListNode == NULL){
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto END;
    }
    int32_t opNum = LIST_LENGTH(pNodeListNode->pNodeList);
    if (opNum != 1) {
      code = TSDB_CODE_MND_INVALID_TOPIC_QUERY;
      goto END;
    }

    pSubplan = (SSubplan*)nodesListGetNode(pNodeListNode->pNodeList, 0);
  }

  void* pIter = NULL;
  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void**)&pVgroup);
    if (pIter == NULL) {
      break;
    }
    if (pVgroup->mountVgId) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    if (!mndVgroupInDb(pVgroup, pTopic->dbUid)) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    pSub->vgNum++;

    SMqVgEp* pVgEp = taosMemoryMalloc(sizeof(SMqVgEp));
    if (pVgEp == NULL){
      code = terrno;
      goto END;
    }
    pVgEp->epSet = mndGetVgroupEpset(pMnode, pVgroup);
    pVgEp->vgId = pVgroup->vgId;
    if (taosArrayPush(pSub->unassignedVgs, &pVgEp) == NULL){
      code = terrno;
      taosMemoryFree(pVgEp);
      goto END;
    }
    mInfo("init subscription %s for topic:%s assign vgId:%d", pSub->key, pTopic->name, pVgEp->vgId);
    sdbRelease(pSdb, pVgroup);
  }

  if (pSubplan) {
    int32_t msgLen;

    if (qSubPlanToString(pSubplan, &pSub->qmsg, &msgLen) < 0) {
      code = TSDB_CODE_QRY_INVALID_INPUT;
      goto END;
    }
  } else {
    pSub->qmsg = taosStrdup("");
  }

END:
  qDestroyQueryPlan(pPlan);
  return code;
}
#endif
