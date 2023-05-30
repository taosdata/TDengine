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
#include "mndSnode.h"
#include "mndVgroup.h"
#include "parser.h"
#include "tcompare.h"
#include "tname.h"
#include "tuuid.h"

#define SINK_NODE_LEVEL (0)
extern bool tsDeployOnSnode;

static int32_t mndAddSinkTaskToStream(SStreamObj* pStream, SArray* pTaskList, SMnode* pMnode, int32_t vgId, SVgObj* pVgroup);
static void setFixedDownstreamEpInfo(SStreamTask* pDstTask, const SStreamTask* pTask);

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
    pTask->outputType = TASK_OUTPUT__SMA;
    pTask->smaSink.smaId = pStream->smaId;
  } else {
    pTask->outputType = TASK_OUTPUT__TABLE;
    pTask->tbSink.stbUid = pStream->targetStbUid;
    memcpy(pTask->tbSink.stbFullName, pStream->targetSTbName, TSDB_TABLE_FNAME_LEN);
    pTask->tbSink.pSchemaWrapper = tCloneSSchemaWrapper(&pStream->outputSchema);
    if (pTask->tbSink.pSchemaWrapper == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return 0;
}

int32_t mndAddDispatcherForInnerTask(SMnode* pMnode, SStreamObj* pStream, SArray* pSinkNodeList, SStreamTask* pTask) {
  bool isShuffle = false;

  if (pStream->fixedSinkVgId == 0) {
    SDbObj* pDb = mndAcquireDb(pMnode, pStream->targetDb);
    if (pDb != NULL && pDb->cfg.numOfVgroups > 1) {

      isShuffle = true;
      pTask->outputType = TASK_OUTPUT__SHUFFLE_DISPATCH;
      pTask->dispatchMsgType = TDMT_STREAM_TASK_DISPATCH;
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
        if (pSinkTask->nodeId == pVgInfo->vgId) {
          pVgInfo->taskId = pSinkTask->id.taskId;
          break;
        }
      }
    }
  } else {
    SStreamTask* pOneSinkTask = taosArrayGetP(pSinkNodeList, 0);
    setFixedDownstreamEpInfo(pTask, pOneSinkTask);
  }

  return 0;
}

int32_t mndAssignStreamTaskToVgroup(SMnode* pMnode, SStreamTask* pTask, SSubplan* plan, const SVgObj* pVgroup) {
  int32_t msgLen;

  pTask->nodeId = pVgroup->vgId;
  pTask->epSet = mndGetVgroupEpset(pMnode, pVgroup);

  plan->execNode.nodeId = pTask->nodeId;
  plan->execNode.epSet = pTask->epSet;
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
  return pObj;
}

int32_t mndAssignTaskToSnode(SMnode* pMnode, SStreamTask* pTask, SSubplan* plan, const SSnodeObj* pSnode) {
  int32_t msgLen;

  pTask->nodeId = SNODE_HANDLE;
  pTask->epSet = mndAcquireEpFromSnode(pMnode, pSnode);

  plan->execNode.nodeId = SNODE_HANDLE;
  plan->execNode.epSet = pTask->epSet;

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
    return pVgroup;
  }
  return pVgroup;
}

// create sink node for each vgroup.
int32_t mndAddShuffleSinkTasksToStream(SMnode* pMnode, SArray* pTaskList, SStreamObj* pStream) {
  SSdb*   pSdb = pMnode->pSdb;
  void*   pIter = NULL;

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

    mndAddSinkTaskToStream(pStream, pTaskList, pMnode, pVgroup->vgId, pVgroup);
    sdbRelease(pSdb, pVgroup);
  }

  return 0;
}

int32_t mndAddSinkTaskToStream(SStreamObj* pStream, SArray* pTaskList, SMnode* pMnode, int32_t vgId, SVgObj* pVgroup) {
  SStreamTask* pTask = tNewStreamTask(pStream->uid, TASK_LEVEL__SINK, pStream->conf.fillHistory, 0, pTaskList);
  if (pTask == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pTask->nodeId = vgId;
  pTask->epSet = mndGetVgroupEpset(pMnode, pVgroup);
  mndSetSinkTaskInfo(pStream, pTask);
  return 0;
}

static int32_t mndScheduleFillHistoryStreamTask(SMnode* pMnode, SStreamObj* pStream) {
  return 0;
}

static int32_t addSourceStreamTask(SMnode* pMnode, SVgObj* pVgroup, SArray* pTaskList, SArray* pSinkTaskList, SStreamObj* pStream,
                                   SSubplan* plan, uint64_t uid, int8_t taskLevel, int8_t fillHistory,
                                   bool hasExtraSink) {
  SStreamTask* pTask = tNewStreamTask(uid, taskLevel, fillHistory, pStream->conf.triggerParam, pTaskList);
  if (pTask == NULL) {
    return terrno;
  }

  // sink or dispatch
  if (hasExtraSink) {

    mndAddDispatcherForInnerTask(pMnode, pStream, pSinkTaskList, pTask);
  } else {
    mndSetSinkTaskInfo(pStream, pTask);
  }

  if (mndAssignStreamTaskToVgroup(pMnode, pTask, plan, pVgroup) < 0) {
    return terrno;
  }

  return TSDB_CODE_SUCCESS;
}

static SStreamChildEpInfo* createStreamTaskEpInfo(SStreamTask* pTask) {
  SStreamChildEpInfo* pEpInfo = taosMemoryMalloc(sizeof(SStreamChildEpInfo));
  if (pEpInfo == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pEpInfo->childId = pTask->selfChildId;
  pEpInfo->epSet = pTask->epSet;
  pEpInfo->nodeId = pTask->nodeId;
  pEpInfo->taskId = pTask->id.taskId;

  return pEpInfo;
}

void setFixedDownstreamEpInfo(SStreamTask* pDstTask, const SStreamTask* pTask) {
  STaskDispatcherFixedEp* pDispatcher = &pDstTask->fixedEpDispatcher;
  pDispatcher->taskId = pTask->id.taskId;
  pDispatcher->nodeId = pTask->nodeId;
  pDispatcher->epSet = pTask->epSet;

  pDstTask->outputType = TASK_OUTPUT__FIXED_DISPATCH;
  pDstTask->dispatchMsgType = TDMT_STREAM_TASK_DISPATCH;
}

int32_t appendToDownstream(SStreamTask* pTask, SStreamTask* pDownstream) {
  SStreamChildEpInfo* pEpInfo = createStreamTaskEpInfo(pTask);
  if (pEpInfo == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if(pDownstream->childEpInfo == NULL) {
    pDownstream->childEpInfo = taosArrayInit(4, POINTER_BYTES);
  }
  
  taosArrayPush(pDownstream->childEpInfo, &pEpInfo);
  return TSDB_CODE_SUCCESS;
}

static int32_t doScheduleStream(uint64_t uid, SArray* pTasksList, SStreamObj* pStream, SMnode* pMnode, SQueryPlan* pPlan) {
  SSdb* pSdb = pMnode->pSdb;
  int32_t planTotLevel = LIST_LENGTH(pPlan->pSubplans);

  bool    hasExtraSink = false;
  bool    externalTargetDB = strcmp(pStream->sourceDb, pStream->targetDb) != 0;
  SDbObj* pDbObj = mndAcquireDb(pMnode, pStream->targetDb);
  if (pDbObj == NULL) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    return -1;
  }

  bool multiTarget = (pDbObj->cfg.numOfVgroups > 1);
  sdbRelease(pSdb, pDbObj);

  if (planTotLevel == 2 || externalTargetDB || multiTarget || pStream->fixedSinkVgId) {
    SArray* taskOneLevel = taosArrayInit(0, POINTER_BYTES);
    taosArrayPush(pTasksList, &taskOneLevel);

    // add extra sink
    hasExtraSink = true;
    if (pStream->fixedSinkVgId == 0) {
      if (mndAddShuffleSinkTasksToStream(pMnode, taskOneLevel, pStream) < 0) {
        // TODO free
        return -1;
      }
    } else {
      if (mndAddSinkTaskToStream(pStream, taskOneLevel, pMnode, pStream->fixedSinkVgId, &pStream->fixedSinkVg) < 0) {
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
      SArray* taskInnerLevel = taosArrayInit(0, POINTER_BYTES);
      taosArrayPush(pTasksList, &taskInnerLevel);

      SNodeListNode* inner = (SNodeListNode*)nodesListGetNode(pPlan->pSubplans, 0);
      SSubplan*      plan = (SSubplan*)nodesListGetNode(inner->pNodeList, 0);
      if (plan->subplanType != SUBPLAN_TYPE_MERGE) {
        terrno = TSDB_CODE_QRY_INVALID_INPUT;
        return -1;
      }

      pInnerTask = tNewStreamTask(uid, TASK_LEVEL__AGG, pStream->conf.fillHistory, pStream->conf.triggerParam, taskInnerLevel);
      if (pInnerTask == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        return -1;
      }

      // dispatch
      SArray* pSinkNodeList = taosArrayGet(pTasksList, SINK_NODE_LEVEL);
      if (mndAddDispatcherForInnerTask(pMnode, pStream, pSinkNodeList, pInnerTask) < 0) {
        return -1;
      }

      if (tsDeployOnSnode) {
        SSnodeObj* pSnode = mndSchedFetchOneSnode(pMnode);
        if (pSnode == NULL) {
          SVgObj* pVgroup = mndSchedFetchOneVg(pMnode, pStream->sourceDbUid);
          if (mndAssignStreamTaskToVgroup(pMnode, pInnerTask, plan, pVgroup) < 0) {
            sdbRelease(pSdb, pVgroup);
            return -1;
          }
          sdbRelease(pSdb, pVgroup);
        } else {
          if (mndAssignTaskToSnode(pMnode, pInnerTask, plan, pSnode) < 0) {
            sdbRelease(pSdb, pSnode);
            return -1;
          }
        }
      } else {
        SVgObj* pVgroup = mndSchedFetchOneVg(pMnode, pStream->sourceDbUid);
        if (mndAssignStreamTaskToVgroup(pMnode, pInnerTask, plan, pVgroup) < 0) {
          sdbRelease(pSdb, pVgroup);
          return -1;
        }

        sdbRelease(pSdb, pVgroup);
      }
    }

    // source level
    SArray* taskSourceLevel = taosArrayInit(0, POINTER_BYTES);
    taosArrayPush(pTasksList, &taskSourceLevel);

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

      SStreamTask* pTask = tNewStreamTask(uid, TASK_LEVEL__SOURCE, pStream->conf.fillHistory, 0, taskSourceLevel);
      if (pTask == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        sdbRelease(pSdb, pVgroup);
        return -1;
      }

      // all the source tasks dispatch result to a single agg node.
      setFixedDownstreamEpInfo(pTask, pInnerTask);

      if (mndAssignStreamTaskToVgroup(pMnode, pTask, plan, pVgroup) < 0) {
        sdbRelease(pSdb, pVgroup);
        return -1;
      }

      int32_t code = appendToDownstream(pTask, pInnerTask);
      sdbRelease(pSdb, pVgroup);

      if (code != TSDB_CODE_SUCCESS) {
        terrno = code;
        return -1;
      }
    }
  } else if (planTotLevel == 1) {
    // create exec stream task, since only one level, the exec task is also the source task
    SArray* pTaskList = taosArrayInit(0, POINTER_BYTES);
    taosArrayPush(pTasksList, &pTaskList);

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
      SArray* pSinkNodeTaskList = taosArrayGet(pTasksList, SINK_NODE_LEVEL);
      int32_t code = addSourceStreamTask(pMnode, pVgroup, pTaskList, pSinkNodeTaskList, pStream, plan, uid, TASK_LEVEL__SOURCE, pStream->conf.fillHistory, hasExtraSink);
      sdbRelease(pSdb, pVgroup);

      if (code != TSDB_CODE_SUCCESS) {
        return -1;
      }
    }
  }

  return 0;
}

int32_t mndScheduleStream(SMnode* pMnode, SStreamObj* pStream) {
  SQueryPlan* pPlan = qStringToQueryPlan(pStream->physicalPlan);
  if (pPlan == NULL) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    return -1;
  }

  int32_t code = doScheduleStream(pStream->uid, pStream->tasks, pStream, pMnode, pPlan);

  if (code == TSDB_CODE_SUCCESS) {
    code = doScheduleStream(pStream->batchTaskUid, pStream->pBatchTask, pStream, pMnode, pPlan);
  }

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

    if (pTopic->subType == TOPIC_SUB_TYPE__COLUMN) {
      int32_t msgLen;

      pSubplan->execNode.epSet = pVgEp->epSet;
      pSubplan->execNode.nodeId = pVgEp->vgId;

      if (qSubPlanToString(pSubplan, &pVgEp->qmsg, &msgLen) < 0) {
        sdbRelease(pSdb, pVgroup);
        qDestroyQueryPlan(pPlan);
        terrno = TSDB_CODE_QRY_INVALID_INPUT;
        return -1;
      }
    } else {
      pVgEp->qmsg = taosStrdup("");
    }

    sdbRelease(pSdb, pVgroup);
  }

  qDestroyQueryPlan(pPlan);
  return 0;
}
