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
#include "mndSubscribe.h"
#include "mndTopic.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"
#include "tcompare.h"
#include "tname.h"

int32_t mndSchedInitSubEp(SMnode* pMnode, const SMqTopicObj* pTopic, SMqSubscribeObj* pSub) {
  SSdb*      pSdb = pMnode->pSdb;
  SVgObj*    pVgroup = NULL;
  SQueryDag* pDag = qStringToDag(pTopic->physicalPlan);
  SArray*    pAray = NULL;
  SArray*    unassignedVg = pSub->unassignedVg;

  ASSERT(pSub->vgNum == 0);

  int32_t levelNum = taosArrayGetSize(pDag->pSubplans);
  if (levelNum != 1) {
    return -1;
  }

  SArray* inner = taosArrayGet(pDag->pSubplans, 0);

  int32_t opNum = taosArrayGetSize(inner);
  if (opNum != 1) {
    return -1;
  }
  SSubplan* plan = taosArrayGetP(inner, 0);

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
    plan->execNode.epset = mndGetVgroupEpset(pMnode, pVgroup);

    SMqConsumerEp consumerEp = {0};
    consumerEp.status = 0;
    consumerEp.consumerId = -1;
    consumerEp.epSet = plan->execNode.epset;
    consumerEp.vgId = plan->execNode.nodeId;
    int32_t msgLen;
    int32_t code = qSubPlanToString(plan, &consumerEp.qmsg, &msgLen);
    taosArrayPush(unassignedVg, &consumerEp);
  }

  return 0;
}
