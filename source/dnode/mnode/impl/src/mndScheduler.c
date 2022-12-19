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
#include "mndConsumer.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndMnode.h"
#include "mndShow.h"
#include "mndSnode.h"
#include "mndStb.h"
#include "mndStream.h"
#include "mndSubscribe.h"
#include "mndTopic.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"
#include "parser.h"
#include "tcompare.h"
#include "tname.h"
#include "tuuid.h"

extern bool tsDeployOnSnode;

static int32_t mndAddTaskToTaskSet(SArray* pArray, SStreamTask* pTask) {
  int32_t childId = taosArrayGetSize(pArray);
  pTask->selfChildId = childId;
  taosArrayPush(pArray, &pTask);
  return 0;
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

int32_t mndAddSinkToTask(SMnode* pMnode, SStreamObj* pStream, SStreamTask* pTask) {
  if (pStream->smaId != 0) {
    pTask->outputType = TASK_OUTPUT__SMA;
    pTask->smaSink.smaId = pStream->smaId;
  } else {
    pTask->outputType = TASK_OUTPUT__TABLE;
    pTask->tbSink.stbUid = pStream->targetStbUid;
    memcpy(pTask->tbSink.stbFullName, pStream->targetSTbName, TSDB_TABLE_FNAME_LEN);
    pTask->tbSink.pSchemaWrapper = tCloneSSchemaWrapper(&pStream->outputSchema);
  }
  return 0;
}

int32_t mndAddDispatcherToInnerTask(SMnode* pMnode, SStreamObj* pStream, SStreamTask* pTask) {
  bool isShuffle = false;

  if (pStream->fixedSinkVgId == 0) {
    SDbObj* pDb = mndAcquireDb(pMnode, pStream->targetDb);
    ASSERT(pDb);
    if (pDb->cfg.numOfVgroups > 1) {
      isShuffle = true;
      pTask->outputType = TASK_OUTPUT__SHUFFLE_DISPATCH;
      pTask->dispatchMsgType = TDMT_STREAM_TASK_DISPATCH;
      if (mndExtractDbInfo(pMnode, pDb, &pTask->shuffleDispatcher.dbInfo, NULL) < 0) {
        ASSERT(0);
        return -1;
      }
    }

    sdbRelease(pMnode->pSdb, pDb);
  }

  if (isShuffle) {
    memcpy(pTask->shuffleDispatcher.stbFullName, pStream->targetSTbName, TSDB_TABLE_FNAME_LEN);
    SArray* pVgs = pTask->shuffleDispatcher.dbInfo.pVgroupInfos;
    int32_t sz = taosArrayGetSize(pVgs);
    SArray* sinkLv = taosArrayGetP(pStream->tasks, 0);
    int32_t sinkLvSize = taosArrayGetSize(sinkLv);
    for (int32_t i = 0; i < sz; i++) {
      SVgroupInfo* pVgInfo = taosArrayGet(pVgs, i);
      for (int32_t j = 0; j < sinkLvSize; j++) {
        SStreamTask* pLastLevelTask = taosArrayGetP(sinkLv, j);
        if (pLastLevelTask->nodeId == pVgInfo->vgId) {
          ASSERT(pVgInfo->vgId > 0);
          pVgInfo->taskId = pLastLevelTask->taskId;
          ASSERT(pVgInfo->taskId != 0);
          break;
        }
      }
    }
  } else {
    pTask->outputType = TASK_OUTPUT__FIXED_DISPATCH;
    pTask->dispatchMsgType = TDMT_STREAM_TASK_DISPATCH;
    SArray* pArray = taosArrayGetP(pStream->tasks, 0);
    // one sink only
    ASSERT(taosArrayGetSize(pArray) == 1);
    SStreamTask* lastLevelTask = taosArrayGetP(pArray, 0);
    pTask->fixedEpDispatcher.taskId = lastLevelTask->taskId;
    pTask->fixedEpDispatcher.nodeId = lastLevelTask->nodeId;
    pTask->fixedEpDispatcher.epSet = lastLevelTask->epSet;
  }
  return 0;
}

int32_t mndAssignTaskToVg(SMnode* pMnode, SStreamTask* pTask, SSubplan* plan, const SVgObj* pVgroup) {
  int32_t msgLen;
  pTask->nodeId = pVgroup->vgId;
  pTask->epSet = mndGetVgroupEpset(pMnode, pVgroup);

  plan->execNode.nodeId = pVgroup->vgId;
  plan->execNode.epSet = pTask->epSet;

  if (qSubPlanToString(plan, &pTask->exec.qmsg, &msgLen) < 0) {
    ASSERT(0);
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
  return pObj;
}

int32_t mndAssignTaskToSnode(SMnode* pMnode, SStreamTask* pTask, SSubplan* plan, const SSnodeObj* pSnode) {
  int32_t msgLen;

  pTask->nodeId = SNODE_HANDLE;
  pTask->epSet = mndAcquireEpFromSnode(pMnode, pSnode);

  plan->execNode.nodeId = SNODE_HANDLE;
  plan->execNode.epSet = pTask->epSet;

  if (qSubPlanToString(plan, &pTask->exec.qmsg, &msgLen) < 0) {
    ASSERT(0);
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    return -1;
  }
  return 0;
}

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
    return pVgroup;
  }
  return pVgroup;
}

int32_t mndAddShuffleSinkTasksToStream(SMnode* pMnode, SStreamObj* pStream) {
  SSdb*   pSdb = pMnode->pSdb;
  void*   pIter = NULL;
  SArray* tasks = taosArrayGetP(pStream->tasks, 0);

  ASSERT(taosArrayGetSize(pStream->tasks) == 1);

  while (1) {
    SVgObj* pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void**)&pVgroup);
    if (pIter == NULL) break;
    if (!mndVgroupInDb(pVgroup, pStream->targetDbUid)) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    SStreamTask* pTask = tNewSStreamTask(pStream->uid);
    if (pTask == NULL) {
      sdbRelease(pSdb, pVgroup);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
    pTask->fillHistory = pStream->fillHistory;
    mndAddTaskToTaskSet(tasks, pTask);

    pTask->nodeId = pVgroup->vgId;
    pTask->epSet = mndGetVgroupEpset(pMnode, pVgroup);

    // type
    pTask->taskLevel = TASK_LEVEL__SINK;

    // sink
    if (pStream->smaId != 0) {
      pTask->outputType = TASK_OUTPUT__SMA;
      pTask->smaSink.smaId = pStream->smaId;
    } else {
      pTask->outputType = TASK_OUTPUT__TABLE;
      pTask->tbSink.stbUid = pStream->targetStbUid;
      memcpy(pTask->tbSink.stbFullName, pStream->targetSTbName, TSDB_TABLE_FNAME_LEN);
      pTask->tbSink.pSchemaWrapper = tCloneSSchemaWrapper(&pStream->outputSchema);
      ASSERT(pTask->tbSink.pSchemaWrapper);
    }
    sdbRelease(pSdb, pVgroup);
  }
  return 0;
}

int32_t mndAddFixedSinkTaskToStream(SMnode* pMnode, SStreamObj* pStream) {
  ASSERT(pStream->fixedSinkVgId != 0);
  SArray*      tasks = taosArrayGetP(pStream->tasks, 0);
  SStreamTask* pTask = tNewSStreamTask(pStream->uid);
  if (pTask == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  pTask->fillHistory = pStream->fillHistory;
  mndAddTaskToTaskSet(tasks, pTask);

  ASSERT(pStream->fixedSinkVg.vgId == pStream->fixedSinkVgId);

  pTask->nodeId = pStream->fixedSinkVgId;
#if 0
  SVgObj* pVgroup = mndAcquireVgroup(pMnode, pStream->fixedSinkVgId);
  if (pVgroup == NULL) {
    return -1;
  }
  pTask->epSet = mndGetVgroupEpset(pMnode, pVgroup);
#endif
  pTask->epSet = mndGetVgroupEpset(pMnode, &pStream->fixedSinkVg);

  pTask->taskLevel = TASK_LEVEL__SINK;

  // sink
  if (pStream->smaId != 0) {
    pTask->outputType = TASK_OUTPUT__SMA;
    pTask->smaSink.smaId = pStream->smaId;
  } else {
    pTask->outputType = TASK_OUTPUT__TABLE;
    pTask->tbSink.stbUid = pStream->targetStbUid;
    memcpy(pTask->tbSink.stbFullName, pStream->targetSTbName, TSDB_TABLE_FNAME_LEN);
    pTask->tbSink.pSchemaWrapper = tCloneSSchemaWrapper(&pStream->outputSchema);
  }

  return 0;
}

int32_t mndScheduleStream(SMnode* pMnode, SStreamObj* pStream) {
  SSdb*       pSdb = pMnode->pSdb;
  SQueryPlan* pPlan = qStringToQueryPlan(pStream->physicalPlan);
  if (pPlan == NULL) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    return -1;
  }
  int32_t planTotLevel = LIST_LENGTH(pPlan->pSubplans);
  ASSERT(planTotLevel <= 2);
  pStream->tasks = taosArrayInit(planTotLevel, sizeof(void*));

  bool    hasExtraSink = false;
  bool    externalTargetDB = strcmp(pStream->sourceDb, pStream->targetDb) != 0;
  SDbObj* pDbObj = mndAcquireDb(pMnode, pStream->targetDb);
  ASSERT(pDbObj != NULL);

  bool multiTarget = pDbObj->cfg.numOfVgroups > 1;
  sdbRelease(pSdb, pDbObj);

  if (planTotLevel == 2 || externalTargetDB || multiTarget || pStream->fixedSinkVgId) {
    /*if (true) {*/
    SArray* taskOneLevel = taosArrayInit(0, sizeof(void*));
    taosArrayPush(pStream->tasks, &taskOneLevel);
    // add extra sink
    hasExtraSink = true;
    if (pStream->fixedSinkVgId == 0) {
      if (mndAddShuffleSinkTasksToStream(pMnode, pStream) < 0) {
        // TODO free
        return -1;
      }
    } else {
      if (mndAddFixedSinkTaskToStream(pMnode, pStream) < 0) {
        // TODO free
        return -1;
      }
    }
  }
  pStream->totalLevel = planTotLevel + hasExtraSink;

  if (planTotLevel > 1) {
    SStreamTask* pInnerTask;
    // inner level
    {
      SArray* taskInnerLevel = taosArrayInit(0, sizeof(void*));
      taosArrayPush(pStream->tasks, &taskInnerLevel);

      SNodeListNode* inner = (SNodeListNode*)nodesListGetNode(pPlan->pSubplans, 0);
      SSubplan*      plan = (SSubplan*)nodesListGetNode(inner->pNodeList, 0);
      ASSERT(plan->subplanType == SUBPLAN_TYPE_MERGE);

      pInnerTask = tNewSStreamTask(pStream->uid);
      if (pInnerTask == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        qDestroyQueryPlan(pPlan);
        return -1;
      }
      pInnerTask->fillHistory = pStream->fillHistory;
      mndAddTaskToTaskSet(taskInnerLevel, pInnerTask);

      pInnerTask->childEpInfo = taosArrayInit(0, sizeof(void*));

      pInnerTask->taskLevel = TASK_LEVEL__AGG;

      // trigger
      pInnerTask->triggerParam = pStream->triggerParam;

      // dispatch
      if (mndAddDispatcherToInnerTask(pMnode, pStream, pInnerTask) < 0) {
        qDestroyQueryPlan(pPlan);
        return -1;
      }

      if (tsDeployOnSnode) {
        SSnodeObj* pSnode = mndSchedFetchOneSnode(pMnode);
        if (pSnode == NULL) {
          SVgObj* pVgroup = mndSchedFetchOneVg(pMnode, pStream->sourceDbUid);
          if (mndAssignTaskToVg(pMnode, pInnerTask, plan, pVgroup) < 0) {
            sdbRelease(pSdb, pVgroup);
            qDestroyQueryPlan(pPlan);
            return -1;
          }
          sdbRelease(pSdb, pVgroup);
        } else {
          if (mndAssignTaskToSnode(pMnode, pInnerTask, plan, pSnode) < 0) {
            sdbRelease(pSdb, pSnode);
            qDestroyQueryPlan(pPlan);
            return -1;
          }
        }
      } else {
        SVgObj* pVgroup = mndSchedFetchOneVg(pMnode, pStream->sourceDbUid);
        if (mndAssignTaskToVg(pMnode, pInnerTask, plan, pVgroup) < 0) {
          sdbRelease(pSdb, pVgroup);
          qDestroyQueryPlan(pPlan);
          return -1;
        }
        sdbRelease(pSdb, pVgroup);
      }
    }

    // source level
    SArray* taskSourceLevel = taosArrayInit(0, sizeof(void*));
    taosArrayPush(pStream->tasks, &taskSourceLevel);

    SNodeListNode* inner = (SNodeListNode*)nodesListGetNode(pPlan->pSubplans, 1);
    SSubplan*      plan = (SSubplan*)nodesListGetNode(inner->pNodeList, 0);
    ASSERT(plan->subplanType == SUBPLAN_TYPE_SCAN);

    void* pIter = NULL;
    while (1) {
      SVgObj* pVgroup;
      pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void**)&pVgroup);
      if (pIter == NULL) break;
      if (!mndVgroupInDb(pVgroup, pStream->sourceDbUid)) {
        sdbRelease(pSdb, pVgroup);
        continue;
      }

      SStreamTask* pTask = tNewSStreamTask(pStream->uid);
      if (pTask == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        sdbRelease(pSdb, pVgroup);
        qDestroyQueryPlan(pPlan);
        return -1;
      }
      pTask->fillHistory = pStream->fillHistory;
      mndAddTaskToTaskSet(taskSourceLevel, pTask);

      pTask->triggerParam = 0;

      // source
      pTask->taskLevel = TASK_LEVEL__SOURCE;

      // add fixed vg dispatch
      pTask->dispatchMsgType = TDMT_STREAM_TASK_DISPATCH;
      pTask->outputType = TASK_OUTPUT__FIXED_DISPATCH;

      pTask->fixedEpDispatcher.taskId = pInnerTask->taskId;
      pTask->fixedEpDispatcher.nodeId = pInnerTask->nodeId;
      pTask->fixedEpDispatcher.epSet = pInnerTask->epSet;

      if (mndAssignTaskToVg(pMnode, pTask, plan, pVgroup) < 0) {
        sdbRelease(pSdb, pVgroup);
        qDestroyQueryPlan(pPlan);
        return -1;
      }

      SStreamChildEpInfo* pEpInfo = taosMemoryMalloc(sizeof(SStreamChildEpInfo));
      if (pEpInfo == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        sdbRelease(pSdb, pVgroup);
        qDestroyQueryPlan(pPlan);
        return -1;
      }
      pEpInfo->childId = pTask->selfChildId;
      pEpInfo->epSet = pTask->epSet;
      pEpInfo->nodeId = pTask->nodeId;
      pEpInfo->taskId = pTask->taskId;
      taosArrayPush(pInnerTask->childEpInfo, &pEpInfo);
      sdbRelease(pSdb, pVgroup);
    }
  }

  if (planTotLevel == 1) {
    SArray* taskOneLevel = taosArrayInit(0, sizeof(void*));
    taosArrayPush(pStream->tasks, &taskOneLevel);

    SNodeListNode* inner = (SNodeListNode*)nodesListGetNode(pPlan->pSubplans, 0);
    ASSERT(LIST_LENGTH(inner->pNodeList) == 1);
    SSubplan* plan = (SSubplan*)nodesListGetNode(inner->pNodeList, 0);
    ASSERT(plan->subplanType == SUBPLAN_TYPE_SCAN);

    void* pIter = NULL;
    while (1) {
      SVgObj* pVgroup;
      pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void**)&pVgroup);
      if (pIter == NULL) break;
      if (!mndVgroupInDb(pVgroup, pStream->sourceDbUid)) {
        sdbRelease(pSdb, pVgroup);
        continue;
      }

      SStreamTask* pTask = tNewSStreamTask(pStream->uid);
      if (pTask == NULL) {
        sdbRelease(pSdb, pVgroup);
        qDestroyQueryPlan(pPlan);
        return -1;
      }
      pTask->fillHistory = pStream->fillHistory;
      mndAddTaskToTaskSet(taskOneLevel, pTask);

      // source
      pTask->taskLevel = TASK_LEVEL__SOURCE;

      // trigger
      pTask->triggerParam = pStream->triggerParam;

      // sink or dispatch
      if (hasExtraSink) {
        mndAddDispatcherToInnerTask(pMnode, pStream, pTask);
      } else {
        mndAddSinkToTask(pMnode, pStream, pTask);
      }

      if (mndAssignTaskToVg(pMnode, pTask, plan, pVgroup) < 0) {
        sdbRelease(pSdb, pVgroup);
        qDestroyQueryPlan(pPlan);
        return -1;
      }
      sdbRelease(pSdb, pVgroup);
    }
  }
  qDestroyQueryPlan(pPlan);
  return 0;
}

int32_t mndSchedInitSubEp(SMnode* pMnode, const SMqTopicObj* pTopic, SMqSubscribeObj* pSub) {
  SSdb*       pSdb = pMnode->pSdb;
  SVgObj*     pVgroup = NULL;
  SQueryPlan* pPlan = NULL;
  SSubplan*   plan = NULL;

  if (pTopic->subType == TOPIC_SUB_TYPE__COLUMN) {
    pPlan = qStringToQueryPlan(pTopic->physicalPlan);
    if (pPlan == NULL) {
      terrno = TSDB_CODE_QRY_INVALID_INPUT;
      return -1;
    }

    int32_t levelNum = LIST_LENGTH(pPlan->pSubplans);
    if (levelNum != 1) {
      qDestroyQueryPlan(pPlan);
      terrno = TSDB_CODE_MND_INVALID_TOPIC_QUERY;
      return -1;
    }

    SNodeListNode* inner = (SNodeListNode*)nodesListGetNode(pPlan->pSubplans, 0);

    int32_t opNum = LIST_LENGTH(inner->pNodeList);
    if (opNum != 1) {
      qDestroyQueryPlan(pPlan);
      terrno = TSDB_CODE_MND_INVALID_TOPIC_QUERY;
      return -1;
    }
    plan = (SSubplan*)nodesListGetNode(inner->pNodeList, 0);
  }

  ASSERT(pSub->unassignedVgs);
  ASSERT(taosHashGetSize(pSub->consumerHash) == 0);

  void* pIter = NULL;
  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void**)&pVgroup);
    if (pIter == NULL) break;
    if (!mndVgroupInDb(pVgroup, pTopic->dbUid)) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    pSub->vgNum++;

    SMqVgEp* pVgEp = taosMemoryMalloc(sizeof(SMqVgEp));
    pVgEp->epSet = mndGetVgroupEpset(pMnode, pVgroup);
    pVgEp->vgId = pVgroup->vgId;
    taosArrayPush(pSub->unassignedVgs, &pVgEp);

    mDebug("init subscription %s, assign vg: %d", pSub->key, pVgEp->vgId);

    if (pTopic->subType == TOPIC_SUB_TYPE__COLUMN) {
      int32_t msgLen;

      plan->execNode.epSet = pVgEp->epSet;
      plan->execNode.nodeId = pVgEp->vgId;

      if (qSubPlanToString(plan, &pVgEp->qmsg, &msgLen) < 0) {
        sdbRelease(pSdb, pVgroup);
        qDestroyQueryPlan(pPlan);
        terrno = TSDB_CODE_QRY_INVALID_INPUT;
        return -1;
      }
    } else {
      pVgEp->qmsg = strdup("");
    }

    sdbRelease(pSdb, pVgroup);
  }

  ASSERT(pSub->unassignedVgs->size > 0);

  ASSERT(taosHashGetSize(pSub->consumerHash) == 0);

  qDestroyQueryPlan(pPlan);

  return 0;
}
