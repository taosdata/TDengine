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
  plan->execNode.nodeId = pVgroup->vgId;
  plan->execNode.epSet = mndGetVgroupEpset(pMnode, pVgroup);

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
  plan->execNode.nodeId = pSnode->id;
  plan->execNode.epSet = mndAcquireEpFromSnode(pMnode, pSnode);

  if (qSubPlanToString(plan, &pTask->exec.qmsg, &msgLen) < 0) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    return -1;
  }
  mndPersistTaskDeployReq(pTrans, pTask, &plan->execNode.epSet, TDMT_SND_TASK_DEPLOY, 0);
  return 0;
}

int32_t mndScheduleStream(SMnode* pMnode, STrans* pTrans, SStreamObj* pStream) {
  SSdb*       pSdb = pMnode->pSdb;
  SVgObj*     pVgroup = NULL;
  SQueryPlan* pPlan = qStringToQueryPlan(pStream->physicalPlan);
  if (pPlan == NULL) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    return -1;
  }
  ASSERT(pStream->vgNum == 0);

  int32_t totLevel = LIST_LENGTH(pPlan->pSubplans);
  pStream->tasks = taosArrayInit(totLevel, sizeof(SArray));
  int32_t lastUsedVgId = 0;

  // gather vnodes
  // gather snodes
  // iterate plan, expand source to vnodes and assign ep to each task
  // iterate tasks, assign sink type and sink ep to each task

  for (int32_t revLevel = totLevel - 1; revLevel >= 0; revLevel--) {
    int32_t        level = totLevel - 1 - revLevel;
    SArray*        taskOneLevel = taosArrayInit(0, sizeof(SStreamTask));
    SNodeListNode* inner = nodesListGetNode(pPlan->pSubplans, revLevel);
    int32_t        opNum = LIST_LENGTH(inner->pNodeList);
    ASSERT(opNum == 1);

    SSubplan* plan = nodesListGetNode(inner->pNodeList, 0);
    if (level == 0) {
      ASSERT(plan->subplanType == SUBPLAN_TYPE_SCAN);
      void* pIter = NULL;
      while (1) {
        pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void**)&pVgroup);
        if (pIter == NULL) break;
        if (pVgroup->dbUid != pStream->dbUid) {
          sdbRelease(pSdb, pVgroup);
          continue;
        }

        lastUsedVgId = pVgroup->vgId;
        pStream->vgNum++;

        SStreamTask* pTask = tNewSStreamTask(pStream->uid);
        /*pTask->level = level;*/
        // TODO
        /*pTask->sourceType = STREAM_SOURCE__SUPER;*/
        /*pTask->sinkType = level == totLevel - 1 ? 1 : 0;*/
        pTask->exec.parallelizable = 1;
        if (mndAssignTaskToVg(pMnode, pTrans, pTask, plan, pVgroup) < 0) {
          sdbRelease(pSdb, pVgroup);
          qDestroyQueryPlan(pPlan);
          return -1;
        }
        taosArrayPush(taskOneLevel, pTask);
      }
    } else {
      SStreamTask* pTask = tNewSStreamTask(pStream->uid);
      /*pTask->level = level;*/
      /*pTask->sourceType = STREAM_SOURCE__NONE;*/
      /*pTask->sinkType = level == totLevel - 1 ? 1 : 0;*/
      pTask->exec.parallelizable = plan->subplanType == SUBPLAN_TYPE_SCAN;

      SSnodeObj* pSnode = mndSchedFetchSnode(pMnode);
      if (pSnode == NULL || tsStreamSchedV) {
        ASSERT(lastUsedVgId != 0);
        SVgObj* pVg = mndAcquireVgroup(pMnode, lastUsedVgId);
        if (mndAssignTaskToVg(pMnode, pTrans, pTask, plan, pVg) < 0) {
          sdbRelease(pSdb, pVg);
          qDestroyQueryPlan(pPlan);
          return -1;
        }
        sdbRelease(pSdb, pVg);
      } else {
        if (mndAssignTaskToSnode(pMnode, pTrans, pTask, plan, pSnode) < 0) {
          sdbRelease(pSdb, pSnode);
          qDestroyQueryPlan(pPlan);
          return -1;
        }
      }
      sdbRelease(pMnode->pSdb, pSnode);

      taosArrayPush(taskOneLevel, pTask);
    }
    taosArrayPush(pStream->tasks, taskOneLevel);
  }
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
