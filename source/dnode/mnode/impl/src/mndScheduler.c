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
#include "mndOffset.h"
#include "mndShow.h"
#include "mndSnode.h"
#include "mndStb.h"
#include "mndStream.h"
#include "mndSubscribe.h"
#include "mndTopic.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"
#include "tcompare.h"
#include "tname.h"
#include "tuuid.h"

extern bool tsStreamSchedV;

int32_t mndPersistTaskDeployReq(STrans* pTrans, SStreamTask* pTask, const SEpSet* pEpSet, tmsg_t type, int32_t nodeId) {
  SCoder encoder;
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, NULL, 0, TD_ENCODER);
  tEncodeSStreamTask(&encoder, pTask);
  int32_t size = encoder.pos;
  int32_t tlen = sizeof(SMsgHead) + size;
  tCoderClear(&encoder);
  void* buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  ((SMsgHead*)buf)->vgId = htonl(nodeId);
  void* abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, abuf, size, TD_ENCODER);
  tEncodeSStreamTask(&encoder, pTask);
  tCoderClear(&encoder);

  STransAction action = {0};
  memcpy(&action.epSet, pEpSet, sizeof(SEpSet));
  action.pCont = buf;
  action.contLen = tlen;
  action.msgType = type;
  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(buf);
    return -1;
  }
  return 0;
}

int32_t mndAssignTaskToVg(SMnode* pMnode, STrans* pTrans, SStreamTask* pTask, SSubplan* plan, const SVgObj* pVgroup) {
  int32_t msgLen;
  pTask->nodeId = pVgroup->vgId;
  pTask->epSet = mndGetVgroupEpset(pMnode, pVgroup);

  plan->execNode.nodeId = pVgroup->vgId;
  plan->execNode.epSet = pTask->epSet;

  if (qSubPlanToString(plan, &pTask->exec.qmsg, &msgLen) < 0) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    return -1;
  }
  mndPersistTaskDeployReq(pTrans, pTask, &plan->execNode.epSet, TDMT_VND_TASK_DEPLOY, pVgroup->vgId);
  return 0;
}

SSnodeObj* mndSchedFetchSnode(SMnode* pMnode) {
  SSnodeObj* pObj = NULL;
  pObj = sdbFetch(pMnode->pSdb, SDB_SNODE, NULL, (void**)&pObj);
  return pObj;
}

int32_t mndAssignTaskToSnode(SMnode* pMnode, STrans* pTrans, SStreamTask* pTask, SSubplan* plan,
                             const SSnodeObj* pSnode) {
  int32_t msgLen;

  pTask->nodeId = 0;
  pTask->epSet = mndAcquireEpFromSnode(pMnode, pSnode);

  plan->execNode.nodeId = 0;
  plan->execNode.epSet = pTask->epSet;

  if (qSubPlanToString(plan, &pTask->exec.qmsg, &msgLen) < 0) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    return -1;
  }
  mndPersistTaskDeployReq(pTrans, pTask, &plan->execNode.epSet, TDMT_SND_TASK_DEPLOY, 0);
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

int32_t mndAddShuffledSinkToStream(SMnode* pMnode, STrans* pTrans, SStreamObj* pStream) {
  SSdb*   pSdb = pMnode->pSdb;
  void*   pIter = NULL;
  SArray* tasks = taosArrayGetP(pStream->tasks, 0);

  ASSERT(taosArrayGetSize(pStream->tasks) == 1);

  while (1) {
    SVgObj* pVgroup;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void**)&pVgroup);
    if (pIter == NULL) break;
    if (pVgroup->dbUid != pStream->dbUid) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }
    SStreamTask* pTask = tNewSStreamTask(pStream->uid);
    if (pTask == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
    taosArrayPush(tasks, &pTask);

    pTask->nodeId = pVgroup->vgId;
    pTask->epSet = mndGetVgroupEpset(pMnode, pVgroup);

    // source
    pTask->sourceType = TASK_SOURCE__MERGE;

    // exec
    pTask->execType = TASK_EXEC__NONE;

    // sink
    if (pStream->createdBy == STREAM_CREATED_BY__SMA) {
      pTask->sinkType = TASK_SINK__SMA;
      pTask->smaSink.smaId = pStream->smaId;
    } else {
      pTask->sinkType = TASK_SINK__TABLE;
    }

    // dispatch
    pTask->dispatchType = TASK_DISPATCH__NONE;

    mndPersistTaskDeployReq(pTrans, pTask, &pTask->epSet, TDMT_VND_TASK_DEPLOY, pVgroup->vgId);
  }
  return 0;
}

int32_t mndAddFixedSinkToStream(SMnode* pMnode, STrans* pTrans, SStreamObj* pStream) {
  ASSERT(pStream->fixedSinkVgId != 0);
  SArray*      tasks = taosArrayGetP(pStream->tasks, 0);
  SStreamTask* pTask = tNewSStreamTask(pStream->uid);
  if (pTask == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  taosArrayPush(tasks, &pTask);

  pTask->nodeId = pStream->fixedSinkVgId;
  SVgObj* pVgroup = mndAcquireVgroup(pMnode, pStream->fixedSinkVgId);
  if (pVgroup == NULL) {
    return -1;
  }
  pTask->epSet = mndGetVgroupEpset(pMnode, pVgroup);
  // source
  pTask->sourceType = TASK_SOURCE__MERGE;

  // exec
  pTask->execType = TASK_EXEC__NONE;

  // sink
  if (pStream->createdBy == STREAM_CREATED_BY__SMA) {
    pTask->sinkType = TASK_SINK__SMA;
    pTask->smaSink.smaId = pStream->smaId;
  } else {
    pTask->sinkType = TASK_SINK__TABLE;
  }
  //
  // dispatch
  pTask->dispatchType = TASK_DISPATCH__NONE;

  mndPersistTaskDeployReq(pTrans, pTask, &pTask->epSet, TDMT_VND_TASK_DEPLOY, pVgroup->vgId);

  return 0;
}

int32_t mndScheduleStream(SMnode* pMnode, STrans* pTrans, SStreamObj* pStream) {
  SSdb*       pSdb = pMnode->pSdb;
  SQueryPlan* pPlan = qStringToQueryPlan(pStream->physicalPlan);
  if (pPlan == NULL) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    return -1;
  }
  ASSERT(pStream->vgNum == 0);

  int32_t totLevel = LIST_LENGTH(pPlan->pSubplans);
  ASSERT(totLevel <= 2);
  pStream->tasks = taosArrayInit(totLevel, sizeof(void*));

  bool hasExtraSink = false;
  if (totLevel == 2) {
    SArray* taskOneLevel = taosArrayInit(0, sizeof(void*));
    taosArrayPush(pStream->tasks, &taskOneLevel);
    // add extra sink
    hasExtraSink = true;
    if (pStream->fixedSinkVgId == 0) {
      mndAddShuffledSinkToStream(pMnode, pTrans, pStream);
    } else {
      mndAddFixedSinkToStream(pMnode, pTrans, pStream);
    }
  }

  for (int32_t level = 0; level < totLevel; level++) {
    SArray*        taskOneLevel = taosArrayInit(0, sizeof(void*));
    SNodeListNode* inner = nodesListGetNode(pPlan->pSubplans, level);
    ASSERT(LIST_LENGTH(inner->pNodeList) == 1);

    SSubplan* plan = nodesListGetNode(inner->pNodeList, 0);

    // if (level == totLevel - 1 /* or no snode */) {
    if (level == totLevel - 1) {
      // last level, source, must assign to vnode
      // must be scan type
      ASSERT(plan->subplanType == SUBPLAN_TYPE_SCAN);

      // replicate task to each vnode
      void* pIter = NULL;
      while (1) {
        SVgObj* pVgroup;
        pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void**)&pVgroup);
        if (pIter == NULL) break;
        if (pVgroup->dbUid != pStream->dbUid) {
          sdbRelease(pSdb, pVgroup);
          continue;
        }
        SStreamTask* pTask = tNewSStreamTask(pStream->uid);
        // source part
        pTask->sourceType = TASK_SOURCE__SCAN;

        // sink part
        if (level == 0) {
          // only for inplace
          pTask->sinkType = TASK_SINK__SHOW;
          pTask->showSink.reserved = 0;
          if (!hasExtraSink) {
#if 1
            if (pStream->createdBy == STREAM_CREATED_BY__SMA) {
              pTask->sinkType = TASK_SINK__SMA;
              pTask->smaSink.smaId = pStream->smaId;
            } else {
              pTask->sinkType = TASK_SINK__TABLE;
            }
#endif
          }
        } else {
          pTask->sinkType = TASK_SINK__NONE;
        }

        // dispatch part
        if (level == 0) {
          pTask->dispatchType = TASK_DISPATCH__NONE;
        } else {
          // add fixed ep dispatcher
          int32_t lastLevel = level - 1;
          ASSERT(lastLevel == 0);
          if (hasExtraSink) lastLevel++;
          SArray* pArray = taosArrayGetP(pStream->tasks, lastLevel);
          // one merge only
          ASSERT(taosArrayGetSize(pArray) == 1);
          SStreamTask* lastLevelTask = taosArrayGetP(pArray, 0);
          pTask->dispatchMsgType = TDMT_VND_TASK_MERGE_EXEC;
          pTask->dispatchType = TASK_DISPATCH__FIXED;

          pTask->fixedEpDispatcher.taskId = lastLevelTask->taskId;
          pTask->fixedEpDispatcher.nodeId = lastLevelTask->nodeId;
          pTask->fixedEpDispatcher.epSet = lastLevelTask->epSet;
        }

        // exec part
        pTask->execType = TASK_EXEC__PIPE;
        pTask->exec.parallelizable = 1;
        if (mndAssignTaskToVg(pMnode, pTrans, pTask, plan, pVgroup) < 0) {
          sdbRelease(pSdb, pVgroup);
          qDestroyQueryPlan(pPlan);
          return -1;
        }
        sdbRelease(pSdb, pVgroup);
        taosArrayPush(taskOneLevel, &pTask);
      }
    } else {
      // merge plan

      // TODO if has snode, assign to snode

      // else, assign to vnode
      ASSERT(plan->subplanType == SUBPLAN_TYPE_MERGE);
      SStreamTask* pTask = tNewSStreamTask(pStream->uid);

      // source part, currently only support multi source
      pTask->sourceType = TASK_SOURCE__PIPE;

      // sink part
      pTask->sinkType = TASK_SINK__SHOW;
      /*pTask->sinkType = TASK_SINK__NONE;*/

      // dispatch part
      ASSERT(hasExtraSink);
      /*pTask->dispatchType = TASK_DISPATCH__NONE;*/
#if 1

      if (hasExtraSink) {
        // add dispatcher
        if (pStream->fixedSinkVgId == 0) {
          pTask->dispatchType = TASK_DISPATCH__SHUFFLE;

          pTask->dispatchMsgType = TDMT_VND_TASK_WRITE_EXEC;
          SDbObj* pDb = mndAcquireDb(pMnode, pStream->db);
          ASSERT(pDb);
          if (mndExtractDbInfo(pMnode, pDb, &pTask->shuffleDispatcher.dbInfo, NULL) < 0) {
            sdbRelease(pSdb, pDb);
            qDestroyQueryPlan(pPlan);
            return -1;
          }
          sdbRelease(pSdb, pDb);

          // put taskId to useDbRsp
          // TODO: optimize
          SArray* pVgs = pTask->shuffleDispatcher.dbInfo.pVgroupInfos;
          int32_t sz = taosArrayGetSize(pVgs);
          SArray* sinkLv = taosArrayGetP(pStream->tasks, 0);
          int32_t sinkLvSize = taosArrayGetSize(sinkLv);
          for (int32_t i = 0; i < sz; i++) {
            SVgroupInfo* pVgInfo = taosArrayGet(pVgs, i);
            for (int32_t j = 0; j < sinkLvSize; j++) {
              SStreamTask* pLastLevelTask = taosArrayGetP(sinkLv, j);
              /*printf("vgid %d node id %d\n", pVgInfo->vgId, pTask->nodeId);*/
              if (pLastLevelTask->nodeId == pVgInfo->vgId) {
                pVgInfo->taskId = pLastLevelTask->taskId;
                /*printf("taskid %d set to %d\n", pVgInfo->taskId, pTask->taskId);*/
                break;
              }
            }
          }
        } else {
          pTask->dispatchType = TASK_DISPATCH__FIXED;
          pTask->dispatchMsgType = TDMT_VND_TASK_WRITE_EXEC;
          SArray* pArray = taosArrayGetP(pStream->tasks, 0);
          // one sink only
          ASSERT(taosArrayGetSize(pArray) == 1);
          SStreamTask* lastLevelTask = taosArrayGetP(pArray, 0);
          pTask->fixedEpDispatcher.taskId = lastLevelTask->taskId;
          pTask->fixedEpDispatcher.nodeId = lastLevelTask->nodeId;
          pTask->fixedEpDispatcher.epSet = lastLevelTask->epSet;
        }
      }
#endif

      // exec part
      pTask->execType = TASK_EXEC__MERGE;
      pTask->exec.parallelizable = 0;
      SVgObj* pVgroup = mndSchedFetchOneVg(pMnode, pStream->dbUid);
      ASSERT(pVgroup);
      if (mndAssignTaskToVg(pMnode, pTrans, pTask, plan, pVgroup) < 0) {
        sdbRelease(pSdb, pVgroup);
        qDestroyQueryPlan(pPlan);
        return -1;
      }
      sdbRelease(pSdb, pVgroup);
      taosArrayPush(taskOneLevel, &pTask);
    }

    taosArrayPush(pStream->tasks, &taskOneLevel);
  }

  if (totLevel == 2) {
    void* pIter = NULL;
    while (1) {
      SVgObj* pVgroup;
      pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void**)&pVgroup);
      if (pIter == NULL) break;
      if (pVgroup->dbUid != pStream->dbUid) {
        sdbRelease(pSdb, pVgroup);
        continue;
      }
      SStreamTask* pTask = tNewSStreamTask(pStream->uid);

      // source part
      pTask->sourceType = TASK_SOURCE__MERGE;

      // sink part
      pTask->sinkType = TASK_SINK__SHOW;

      // dispatch part
      pTask->dispatchType = TASK_DISPATCH__NONE;

      // exec part
      pTask->execType = TASK_EXEC__NONE;
      pTask->exec.parallelizable = 0;
    }
  }

  // free memory
  qDestroyQueryPlan(pPlan);

  return 0;
}

int32_t mndSchedInitSubEp(SMnode* pMnode, const SMqTopicObj* pTopic, SMqSubscribeObj* pSub) {
  SSdb*       pSdb = pMnode->pSdb;
  SVgObj*     pVgroup = NULL;
  SQueryPlan* pPlan = qStringToQueryPlan(pTopic->physicalPlan);
  if (pPlan == NULL) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    return -1;
  }

  ASSERT(pSub->vgNum == 0);

  int32_t levelNum = LIST_LENGTH(pPlan->pSubplans);
  if (levelNum != 1) {
    qDestroyQueryPlan(pPlan);
    terrno = TSDB_CODE_MND_UNSUPPORTED_TOPIC;
    return -1;
  }

  SNodeListNode* inner = nodesListGetNode(pPlan->pSubplans, 0);

  int32_t opNum = LIST_LENGTH(inner->pNodeList);
  if (opNum != 1) {
    qDestroyQueryPlan(pPlan);
    terrno = TSDB_CODE_MND_UNSUPPORTED_TOPIC;
    return -1;
  }
  SSubplan* plan = nodesListGetNode(inner->pNodeList, 0);

  void* pIter = NULL;
  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void**)&pVgroup);
    if (pIter == NULL) break;
    if (pVgroup->dbUid != pTopic->dbUid) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    pSub->vgNum++;
    plan->execNode.nodeId = pVgroup->vgId;
    plan->execNode.epSet = mndGetVgroupEpset(pMnode, pVgroup);

    SMqConsumerEp consumerEp = {0};
    consumerEp.status = 0;
    consumerEp.consumerId = -1;
    consumerEp.epSet = plan->execNode.epSet;
    consumerEp.vgId = plan->execNode.nodeId;
    int32_t msgLen;
    if (qSubPlanToString(plan, &consumerEp.qmsg, &msgLen) < 0) {
      sdbRelease(pSdb, pVgroup);
      qDestroyQueryPlan(pPlan);
      terrno = TSDB_CODE_QRY_INVALID_INPUT;
      return -1;
    }
    taosArrayPush(pSub->unassignedVg, &consumerEp);
  }

  qDestroyQueryPlan(pPlan);

  return 0;
}
