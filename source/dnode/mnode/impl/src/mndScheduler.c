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

static int32_t mndAddSinkTaskToStream(SStreamObj* pStream, SArray* pTaskList, SMnode* pMnode, int32_t vgId,
                                      SVgObj* pVgroup, SEpSet* pEpset, int32_t fillHistory);

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
  if (pStream->smaId != 0) {
    pTask->outputInfo.type = TASK_OUTPUT__SMA;
    pTask->smaSink.smaId = pStream->smaId;
  } else {
    pTask->outputInfo.type = TASK_OUTPUT__TABLE;
    pTask->tbSink.stbUid = pStream->targetStbUid;
    memcpy(pTask->tbSink.stbFullName, pStream->targetSTbName, TSDB_TABLE_FNAME_LEN);
    pTask->tbSink.pSchemaWrapper = tCloneSSchemaWrapper(&pStream->outputSchema);
    if (pTask->tbSink.pSchemaWrapper == NULL) {
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
      if (mndExtractDbInfo(pMnode, pDb, &pTask->shuffleDispatcher.dbInfo, NULL) < 0) {
        return -1;
      }
    }

    sdbRelease(pMnode->pSdb, pDb);
  }

  int32_t numOfSinkNodes = taosArrayGetSize(pSinkNodeList);

  if (isShuffle) {
    memcpy(pTask->shuffleDispatcher.stbFullName, pStream->targetSTbName, TSDB_TABLE_FNAME_LEN);
    SArray* pVgs = pTask->shuffleDispatcher.dbInfo.pVgroupInfos;

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
  if (qSubPlanToString(plan, &pTask->exec.qmsg, &msgLen) < 0) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    return -1;
  }

  return 0;
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

  if (qSubPlanToString(plan, &pTask->exec.qmsg, &msgLen) < 0) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    return -1;
  }
  return 0;
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

// create sink node for each vgroup.
int32_t mndAddShuffleSinkTasksToStream(SMnode* pMnode, SArray* pTaskList, SStreamObj* pStream, SEpSet* pEpset,
                                       int32_t fillHistory) {
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

    mndAddSinkTaskToStream(pStream, pTaskList, pMnode, pVgroup->vgId, pVgroup, pEpset, fillHistory);
    sdbRelease(pSdb, pVgroup);
  }

  return 0;
}

int32_t mndAddSinkTaskToStream(SStreamObj* pStream, SArray* pTaskList, SMnode* pMnode, int32_t vgId, SVgObj* pVgroup,
                               SEpSet* pEpset, int32_t fillHistory) {
  int64_t uid = (fillHistory == 0)? pStream->uid:pStream->hTaskUid;
  SStreamTask* pTask = tNewStreamTask(uid, TASK_LEVEL__SINK, fillHistory, 0, pTaskList);
  if (pTask == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  epsetAssign(&(pTask)->info.mnodeEpset, pEpset);

  pTask->info.nodeId = vgId;
  pTask->info.epSet = mndGetVgroupEpset(pMnode, pVgroup);
  mndSetSinkTaskInfo(pStream, pTask);
  return 0;
}

static int32_t addSourceStreamTask(SMnode* pMnode, SVgObj* pVgroup, SArray* pTaskList, SArray* pSinkTaskList,
                                   SStreamObj* pStream, SSubplan* plan, uint64_t uid, SEpSet* pEpset,
                                   int8_t fillHistory, bool hasExtraSink, int64_t firstWindowSkey) {
  SStreamTask* pTask = tNewStreamTask(uid, TASK_LEVEL__SOURCE, fillHistory, pStream->conf.triggerParam, pTaskList);
  if (pTask == NULL) {
    return terrno;
  }

  epsetAssign(&pTask->info.mnodeEpset, pEpset);

  // todo set the correct ts, which should be last key of queried table.
  STimeWindow* pWindow = &pTask->dataRange.window;

  pWindow->skey = INT64_MIN;
  pWindow->ekey = firstWindowSkey - 1;
  mDebug("add source task 0x%x window:%" PRId64 " - %" PRId64, pTask->id.taskId, pWindow->skey, pWindow->ekey);

  // sink or dispatch
  if (hasExtraSink) {
    mndAddDispatcherForInternalTask(pMnode, pStream, pSinkTaskList, pTask);
  } else {
    mndSetSinkTaskInfo(pStream, pTask);
  }

  if (mndAssignStreamTaskToVgroup(pMnode, pTask, plan, pVgroup) < 0) {
    return terrno;
  }

  for(int32_t i = 0; i < taosArrayGetSize(pSinkTaskList); ++i) {
    SStreamTask* pSinkTask = taosArrayGetP(pSinkTaskList, i);
    streamTaskSetUpstreamInfo(pSinkTask, pTask);
  }

  return TSDB_CODE_SUCCESS;
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

    (*pStreamTask)->historyTaskId.taskId = (*pHTask)->id.taskId;
    (*pStreamTask)->historyTaskId.streamId = (*pHTask)->id.streamId;

    (*pHTask)->streamTaskId.taskId = (*pStreamTask)->id.taskId;
    (*pHTask)->streamTaskId.streamId = (*pStreamTask)->id.streamId;

    mDebug("s-task:0x%" PRIx64 "-0x%x related history task:0x%" PRIx64 "-0x%x, level:%d", (*pStreamTask)->id.streamId,
           (*pStreamTask)->id.taskId, (*pHTask)->id.streamId, (*pHTask)->id.taskId, (*pHTask)->info.taskLevel);
  }
}

static int32_t addSourceTasksForOneLevelStream(SMnode* pMnode, const SQueryPlan* pPlan, SStreamObj* pStream,
                                               SEpSet* pEpset, bool hasExtraSink, int64_t nextWindowSkey) {
  // create exec stream task, since only one level, the exec task is also the source task
  SArray* pTaskList = addNewTaskList(pStream->tasks);
  SSdb*   pSdb = pMnode->pSdb;

  SArray* pHTaskList = NULL;
  if (pStream->conf.fillHistory) {
    pHTaskList = addNewTaskList(pStream->pHTasksList);
  }

  SNodeListNode* inner = (SNodeListNode*)nodesListGetNode(pPlan->pSubplans, 0);
  if (LIST_LENGTH(inner->pNodeList) != 1) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    return -1;
  }

  SSubplan* plan = (SSubplan*)nodesListGetNode(inner->pNodeList, 0);
  if (plan->subplanType != SUBPLAN_TYPE_SCAN) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    return -1;
  }

  void* pIter = NULL;
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

    // new stream task
    SArray** pSinkTaskList = taosArrayGet(pStream->tasks, SINK_NODE_LEVEL);
    int32_t  code = addSourceStreamTask(pMnode, pVgroup, pTaskList, *pSinkTaskList, pStream, plan, pStream->uid, pEpset,
                                        0, hasExtraSink, nextWindowSkey);
    if (code != TSDB_CODE_SUCCESS) {
      sdbRelease(pSdb, pVgroup);
      return -1;
    }

    if (pStream->conf.fillHistory) {
      SArray** pHSinkTaskList = taosArrayGet(pStream->pHTasksList, SINK_NODE_LEVEL);
      code = addSourceStreamTask(pMnode, pVgroup, pHTaskList, *pHSinkTaskList, pStream, plan, pStream->hTaskUid,
                                 pEpset, 1, hasExtraSink, nextWindowSkey);
    }

    sdbRelease(pSdb, pVgroup);
    if (code != TSDB_CODE_SUCCESS) {
      return -1;
    }
  }

  if (pStream->conf.fillHistory) {
    setHTasksId(pTaskList, pHTaskList);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t doAddSourceTask(SArray* pTaskList, int8_t fillHistory, int64_t uid, SStreamTask* pDownstreamTask,
                               SMnode* pMnode, SSubplan* pPlan, SVgObj* pVgroup, SEpSet* pEpset,
                               int64_t nextWindowSkey) {
  SStreamTask* pTask = tNewStreamTask(uid, TASK_LEVEL__SOURCE, fillHistory, 0, pTaskList);
  if (pTask == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  epsetAssign(&(pTask)->info.mnodeEpset, pEpset);

  // todo set the correct ts, which should be last key of queried table.
  STimeWindow* pWindow = &pTask->dataRange.window;
  pWindow->skey = INT64_MIN;
  pWindow->ekey = nextWindowSkey - 1;

  mDebug("s-task:0x%x level:%d set time window:%" PRId64 " - %" PRId64, pTask->id.taskId, pTask->info.taskLevel,
         pWindow->skey, pWindow->ekey);

  // all the source tasks dispatch result to a single agg node.
  streamTaskSetFixedDownstreamInfo(pTask, pDownstreamTask);
  if (mndAssignStreamTaskToVgroup(pMnode, pTask, pPlan, pVgroup) < 0) {
    return -1;
  }

  return streamTaskSetUpstreamInfo(pDownstreamTask, pTask);
}

static int32_t doAddAggTask(uint64_t uid, SArray* pTaskList, SArray* pSinkNodeList, SMnode* pMnode, SStreamObj* pStream,
                            SEpSet* pEpset, int32_t fillHistory, SStreamTask** pAggTask) {
  *pAggTask = tNewStreamTask(uid, TASK_LEVEL__AGG, fillHistory, pStream->conf.triggerParam, pTaskList);
  if (*pAggTask == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  epsetAssign(&(*pAggTask)->info.mnodeEpset, pEpset);

  // dispatch
  if (mndAddDispatcherForInternalTask(pMnode, pStream, pSinkNodeList, *pAggTask) < 0) {
    return -1;
  }

  return 0;
}

static int32_t addAggTask(SStreamObj* pStream, SMnode* pMnode, SQueryPlan* pPlan, SEpSet* pEpset,
                          SStreamTask** pAggTask, SStreamTask** pHAggTask) {
  SArray* pAggTaskList = addNewTaskList(pStream->tasks);
  SSdb*   pSdb = pMnode->pSdb;

  SNodeListNode* pInnerNode = (SNodeListNode*)nodesListGetNode(pPlan->pSubplans, 0);
  SSubplan*      plan = (SSubplan*)nodesListGetNode(pInnerNode->pNodeList, 0);
  if (plan->subplanType != SUBPLAN_TYPE_MERGE) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    return -1;
  }

  *pAggTask = NULL;
  SArray* pSinkNodeList = taosArrayGetP(pStream->tasks, SINK_NODE_LEVEL);

  int32_t code = doAddAggTask(pStream->uid, pAggTaskList, pSinkNodeList, pMnode, pStream, pEpset, 0, pAggTask);
  if (code != TSDB_CODE_SUCCESS) {
    return -1;
  }

  SVgObj*    pVgroup = NULL;
  SSnodeObj* pSnode = NULL;

  if (tsDeployOnSnode) {
    pSnode = mndSchedFetchOneSnode(pMnode);
    if (pSnode == NULL) {
      pVgroup = mndSchedFetchOneVg(pMnode, pStream->sourceDbUid);
    }
  } else {
    pVgroup = mndSchedFetchOneVg(pMnode, pStream->sourceDbUid);
  }

  if (pSnode != NULL) {
    code = mndAssignStreamTaskToSnode(pMnode, *pAggTask, plan, pSnode);
  } else {
    code = mndAssignStreamTaskToVgroup(pMnode, *pAggTask, plan, pVgroup);
  }

  if (pStream->conf.fillHistory) {
    SArray* pHAggTaskList = addNewTaskList(pStream->pHTasksList);
    SArray* pHSinkNodeList = taosArrayGetP(pStream->pHTasksList, SINK_NODE_LEVEL);

    *pHAggTask = NULL;
    code = doAddAggTask(pStream->hTaskUid, pHAggTaskList, pHSinkNodeList, pMnode, pStream, pEpset, pStream->conf.fillHistory,
                        pHAggTask);
    if (code != TSDB_CODE_SUCCESS) {
      if (pSnode != NULL) {
        sdbRelease(pSdb, pSnode);
      } else {
        sdbRelease(pSdb, pVgroup);
      }
      return code;
    }

    if (pSnode != NULL) {
      code = mndAssignStreamTaskToSnode(pMnode, *pHAggTask, plan, pSnode);
    } else {
      code = mndAssignStreamTaskToVgroup(pMnode, *pHAggTask, plan, pVgroup);
    }

    setHTasksId(pAggTaskList, pHAggTaskList);
  }

  if (pSnode != NULL) {
    sdbRelease(pSdb, pSnode);
  } else {
    sdbRelease(pSdb, pVgroup);
  }

  return code;
}

static int32_t addSourceTasksForMultiLevelStream(SMnode* pMnode, SQueryPlan* pPlan, SStreamObj* pStream,
                                                 SStreamTask* pDownstreamTask, SStreamTask* pHDownstreamTask,
                                                 SEpSet* pEpset, int64_t nextWindowSkey) {
  SArray* pSourceTaskList = addNewTaskList(pStream->tasks);

  SArray* pHSourceTaskList = NULL;
  if (pStream->conf.fillHistory) {
    pHSourceTaskList = addNewTaskList(pStream->pHTasksList);
  }

  SSdb*          pSdb = pMnode->pSdb;
  SNodeListNode* inner = (SNodeListNode*)nodesListGetNode(pPlan->pSubplans, 1);
  SSubplan*      plan = (SSubplan*)nodesListGetNode(inner->pNodeList, 0);
  if (plan->subplanType != SUBPLAN_TYPE_SCAN) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    return -1;
  }

  void* pIter = NULL;
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

    int32_t code =
        doAddSourceTask(pSourceTaskList, 0, pStream->uid, pDownstreamTask, pMnode, plan, pVgroup, pEpset, nextWindowSkey);
    if (code != TSDB_CODE_SUCCESS) {
      sdbRelease(pSdb, pVgroup);
      terrno = code;
      return -1;
    }

    if (pStream->conf.fillHistory) {
      code = doAddSourceTask(pHSourceTaskList, 1, pStream->hTaskUid, pHDownstreamTask, pMnode, plan, pVgroup,
                             pEpset, nextWindowSkey);
      if (code != TSDB_CODE_SUCCESS) {
        sdbRelease(pSdb, pVgroup);
        return code;
      }
    }

    sdbRelease(pSdb, pVgroup);
  }

  if (pStream->conf.fillHistory) {
    setHTasksId(pSourceTaskList, pHSourceTaskList);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t addSinkTasks(SArray* pTasksList, SMnode* pMnode, SStreamObj* pStream, SArray** pCreatedTaskList,
                            SEpSet* pEpset, int32_t fillHistory) {
  SArray* pSinkTaskList = addNewTaskList(pTasksList);
  if (pStream->fixedSinkVgId == 0) {
    if (mndAddShuffleSinkTasksToStream(pMnode, pSinkTaskList, pStream, pEpset, fillHistory) < 0) {
      // TODO free
      return -1;
    }
  } else {
    if (mndAddSinkTaskToStream(pStream, pSinkTaskList, pMnode, pStream->fixedSinkVgId, &pStream->fixedSinkVg,
                               pEpset, fillHistory) < 0) {
      // TODO free
      return -1;
    }
  }

  *pCreatedTaskList = pSinkTaskList;
  return TSDB_CODE_SUCCESS;
}

static void setSinkTaskUpstreamInfo(SArray* pTasksList, const SStreamTask* pUpstreamTask) {
  SArray* pSinkTaskList = taosArrayGetP(pTasksList, SINK_NODE_LEVEL);
  for(int32_t i = 0; i < taosArrayGetSize(pSinkTaskList); ++i) {
    SStreamTask* pSinkTask = taosArrayGetP(pSinkTaskList, i);
    streamTaskSetUpstreamInfo(pSinkTask, pUpstreamTask);
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

  if (numOfPlanLevel == 2 || externalTargetDB || multiTarget || pStream->fixedSinkVgId) {
    // add extra sink
    hasExtraSink = true;

    SArray* pSinkTaskList = NULL;
    int32_t code = addSinkTasks(pStream->tasks, pMnode, pStream, &pSinkTaskList, pEpset, 0);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    // check for fill history
    if (pStream->conf.fillHistory) {
      SArray* pHSinkTaskList = NULL;
      code = addSinkTasks(pStream->pHTasksList, pMnode, pStream, &pHSinkTaskList, pEpset, 1);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      setHTasksId(pSinkTaskList, pHSinkTaskList);
    }
  }

  pStream->totalLevel = numOfPlanLevel + hasExtraSink;

  if (numOfPlanLevel > 1) {
    SStreamTask* pAggTask = NULL;
    SStreamTask* pHAggTask = NULL;

    int32_t code = addAggTask(pStream, pMnode, pPlan, pEpset, &pAggTask, &pHAggTask);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    setSinkTaskUpstreamInfo(pStream->tasks, pAggTask);
    setSinkTaskUpstreamInfo(pStream->pHTasksList, pHAggTask);

    // source level
    return addSourceTasksForMultiLevelStream(pMnode, pPlan, pStream, pAggTask, pHAggTask, pEpset, nextWindowSkey);
  } else if (numOfPlanLevel == 1) {
    return addSourceTasksForOneLevelStream(pMnode, pPlan, pStream, pEpset, hasExtraSink, nextWindowSkey);
  }

  return 0;
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

    mDebug("init subscription %s for topic:%s assign vgId:%d", pSub->key, pTopic->name, pVgEp->vgId);

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
