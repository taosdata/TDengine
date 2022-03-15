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

int32_t mndScheduleStream(SMnode* pMnode, SStreamObj* pStream) {
  SSdb*       pSdb = pMnode->pSdb;
  SVgObj*     pVgroup = NULL;
  SQueryPlan* pPlan = qStringToQueryPlan(pStream->physicalPlan);
  if (pPlan == NULL) {
    terrno = TSDB_CODE_QRY_INVALID_INPUT;
    return -1;
  }
  ASSERT(pStream->vgNum == 0);

  int32_t levelNum = LIST_LENGTH(pPlan->pSubplans);
  pStream->tasks = taosArrayInit(levelNum, sizeof(SArray));

  for (int32_t i = 0; i < levelNum; i++) {
    SArray*        taskOneLevel = taosArrayInit(0, sizeof(SStreamTaskMeta));
    SNodeListNode* inner = nodesListGetNode(pPlan->pSubplans, i);
    int32_t        opNum = LIST_LENGTH(inner->pNodeList);
    ASSERT(opNum == 1);

    SSubplan* plan = nodesListGetNode(inner->pNodeList, 0);
    if (i == 0) {
      ASSERT(plan->type == SUBPLAN_TYPE_SCAN);
      void* pIter = NULL;
      while (1) {
        pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void**)&pVgroup);
        if (pIter == NULL) break;
        if (pVgroup->dbUid != pStream->dbUid) {
          sdbRelease(pSdb, pVgroup);
          continue;
        }

        pStream->vgNum++;
        plan->execNode.nodeId = pVgroup->vgId;
        plan->execNode.epSet = mndGetVgroupEpset(pMnode, pVgroup);
        SStreamTaskMeta task = {
            .taskId = tGenIdPI32(),
            .level = i,
            .plan = plan,
        };
        // send to vnode
        taosArrayPush(taskOneLevel, &task);
      }
    } else if (plan->subplanType == SUBPLAN_TYPE_SCAN) {
      // duplicatable

      int32_t parallel = 0;
      // if no snode, parallel set to fetch thread num in vnode

      // if has snode, set to shared thread num in snode
      parallel = SND_SHARED_THREAD_NUM;

      for (int32_t j = 0; j < parallel; j++) {
        SStreamTaskMeta task = {
            .taskId = tGenIdPI32(),
            .level = i,
            .plan = plan,
        };
        taosArrayPush(taskOneLevel, &task);
      }
    } else {
      // not duplicatable
      SStreamTaskMeta task = {
          .taskId = tGenIdPI32(),
          .level = i,
          .plan = plan,
      };
      taosArrayPush(taskOneLevel, &task);
    }
    taosArrayPush(pStream->tasks, taskOneLevel);
  }
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
