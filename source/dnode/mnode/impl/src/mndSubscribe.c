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

#define _DEFAULT_SOURCE
#include "mndSubscribe.h"
#include "mndConsumer.h"
#include "mndScheduler.h"
#include "mndShow.h"
#include "mndTopic.h"
#include "mndTrans.h"
#include "mndVgroup.h"
#include "tcompare.h"
#include "tname.h"

#define MND_SUBSCRIBE_VER_NUMBER   3
#define MND_SUBSCRIBE_RESERVE_SIZE 64

//#define MND_CONSUMER_LOST_HB_CNT          6

static int32_t mqRebInExecCnt = 0;

static SSdbRaw *mndSubActionEncode(SMqSubscribeObj *);
static SSdbRow *mndSubActionDecode(SSdbRaw *pRaw);
static int32_t  mndSubActionInsert(SSdb *pSdb, SMqSubscribeObj *);
static int32_t  mndSubActionDelete(SSdb *pSdb, SMqSubscribeObj *);
static int32_t  mndSubActionUpdate(SSdb *pSdb, SMqSubscribeObj *pOldSub, SMqSubscribeObj *pNewSub);
static int32_t  mndProcessRebalanceReq(SRpcMsg *pMsg);
static int32_t  mndProcessDropCgroupReq(SRpcMsg *pMsg);
static int32_t  mndRetrieveSubscribe(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextSubscribe(SMnode *pMnode, void *pIter);
static int32_t  mndCheckConsumer(SRpcMsg *pMsg, SHashObj *hash);

static int32_t mndSetSubCommitLogs(STrans *pTrans, SMqSubscribeObj *pSub) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndSubActionEncode(pSub);
  MND_TMQ_NULL_CHECK(pCommitRaw);
  code = mndTransAppendCommitlog(pTrans, pCommitRaw);
  if (code != 0) {
    sdbFreeRaw(pCommitRaw);
    goto END;
  }
  code = sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);

END:
  return code;
}

int32_t mndInitSubscribe(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_SUBSCRIBE,
      .keyType = SDB_KEY_BINARY,
      .encodeFp = (SdbEncodeFp)mndSubActionEncode,
      .decodeFp = (SdbDecodeFp)mndSubActionDecode,
      .insertFp = (SdbInsertFp)mndSubActionInsert,
      .updateFp = (SdbUpdateFp)mndSubActionUpdate,
      .deleteFp = (SdbDeleteFp)mndSubActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_VND_TMQ_SUBSCRIBE_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_TMQ_DELETE_SUB_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_TMQ_TIMER, mndProcessRebalanceReq);
  mndSetMsgHandle(pMnode, TDMT_MND_TMQ_DROP_CGROUP, mndProcessDropCgroupReq);
  mndSetMsgHandle(pMnode, TDMT_MND_TMQ_DROP_CGROUP_RSP, mndTransProcessRsp);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_SUBSCRIPTIONS, mndRetrieveSubscribe);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_TOPICS, mndCancelGetNextSubscribe);

  return sdbSetTable(pMnode->pSdb, table);
}

static int32_t mndCreateSubscription(SMnode *pMnode, const SMqTopicObj *pTopic, const char *subKey, SMqSubscribeObj** pSub) {
  int32_t code = 0;
  MND_TMQ_RETURN_CHECK(tNewSubscribeObj(subKey, pSub));
  (*pSub)->dbUid = pTopic->dbUid;
  (*pSub)->stbUid = pTopic->stbUid;
  (*pSub)->subType = pTopic->subType;
  (*pSub)->withMeta = pTopic->withMeta;

  MND_TMQ_RETURN_CHECK(mndSchedInitSubEp(pMnode, pTopic, *pSub));
  return code;

END:
  tDeleteSubscribeObj(*pSub);
  taosMemoryFree(*pSub);
  return code;
}

static int32_t mndBuildSubChangeReq(void **pBuf, int32_t *pLen, SMqSubscribeObj *pSub, const SMqRebOutputVg *pRebVg,
                                    SSubplan *pPlan) {
  SMqRebVgReq req = {0};
  int32_t     code = 0;
  SEncoder encoder = {0};

  req.oldConsumerId = pRebVg->oldConsumerId;
  req.newConsumerId = pRebVg->newConsumerId;
  req.vgId = pRebVg->pVgEp->vgId;
  if (pPlan) {
    pPlan->execNode.epSet = pRebVg->pVgEp->epSet;
    pPlan->execNode.nodeId = pRebVg->pVgEp->vgId;
    int32_t msgLen = 0;
    MND_TMQ_RETURN_CHECK(qSubPlanToString(pPlan, &req.qmsg, &msgLen));
  } else {
    req.qmsg = taosStrdup("");
  }
  req.subType = pSub->subType;
  req.withMeta = pSub->withMeta;
  req.suid = pSub->stbUid;
  tstrncpy(req.subKey, pSub->key, TSDB_SUBSCRIBE_KEY_LEN);

  int32_t tlen = 0;
  tEncodeSize(tEncodeSMqRebVgReq, &req, tlen, code);
  if (code < 0) {
    goto END;
  }

  tlen += sizeof(SMsgHead);
  void *buf = taosMemoryMalloc(tlen);
  MND_TMQ_NULL_CHECK(buf);
  SMsgHead *pMsgHead = (SMsgHead *)buf;
  pMsgHead->contLen = htonl(tlen);
  pMsgHead->vgId = htonl(pRebVg->pVgEp->vgId);

  tEncoderInit(&encoder, POINTER_SHIFT(buf, sizeof(SMsgHead)), tlen);
  MND_TMQ_RETURN_CHECK(tEncodeSMqRebVgReq(&encoder, &req));
  *pBuf = buf;
  *pLen = tlen;

END:
  tEncoderClear(&encoder);
  taosMemoryFree(req.qmsg);
  return code;
}

static int32_t mndPersistSubChangeVgReq(SMnode *pMnode, STrans *pTrans, SMqSubscribeObj *pSub,
                                        const SMqRebOutputVg *pRebVg, SSubplan *pPlan) {
  int32_t code = 0;
  void   *buf  = NULL;

  if (pRebVg->oldConsumerId == pRebVg->newConsumerId) {
    if (pRebVg->oldConsumerId == -1) return 0;  // drop stream, no consumer, while split vnode,all consumerId is -1
    code = TSDB_CODE_MND_INVALID_SUB_OPTION;
    goto END;
  }

  int32_t tlen = 0;
  MND_TMQ_RETURN_CHECK(mndBuildSubChangeReq(&buf, &tlen, pSub, pRebVg, pPlan));
  int32_t vgId = pRebVg->pVgEp->vgId;
  SVgObj *pVgObj = mndAcquireVgroup(pMnode, vgId);
  if (pVgObj == NULL) {
    code = TSDB_CODE_MND_VGROUP_NOT_EXIST;
    goto END;
  }

  STransAction action = {0};
  action.epSet = mndGetVgroupEpset(pMnode, pVgObj);
  action.pCont = buf;
  action.contLen = tlen;
  action.msgType = TDMT_VND_TMQ_SUBSCRIBE;

  mndReleaseVgroup(pMnode, pVgObj);
  MND_TMQ_RETURN_CHECK(mndTransAppendRedoAction(pTrans, &action));
  return code;

END:
  taosMemoryFree(buf);
  return code;
}

static void mndSplitSubscribeKey(const char *key, char *topic, char *cgroup, bool fullName) {
  int32_t i = 0;
  while (key[i] != TMQ_SEPARATOR_CHAR) {
    i++;
  }
  (void)memcpy(cgroup, key, i);
  cgroup[i] = 0;
  if (fullName) {
    (void)strcpy(topic, &key[i + 1]);
  } else {
    while (key[i] != '.') {
      i++;
    }
    (void)strcpy(topic, &key[i + 1]);
  }
}

static int32_t mndGetOrCreateRebSub(SHashObj *pHash, const char *key, SMqRebInfo **pReb) {
  int32_t code = 0;
  SMqRebInfo* pRebInfo = taosHashGet(pHash, key, strlen(key) + 1);
  if (pRebInfo == NULL) {
    pRebInfo = tNewSMqRebSubscribe(key);
    if (pRebInfo == NULL) {
      code = terrno;
      goto END;
    }
    code = taosHashPut(pHash, key, strlen(key) + 1, pRebInfo, sizeof(SMqRebInfo));
    taosMemoryFreeClear(pRebInfo);
    if (code != 0) {
      goto END;
    }
    pRebInfo = taosHashGet(pHash, key, strlen(key) + 1);
    MND_TMQ_NULL_CHECK(pRebInfo);
  }
  if (pReb){
    *pReb = pRebInfo;
  }

END:
  return code;
}

static int32_t pushVgDataToHash(SArray *vgs, SHashObj *pHash, int64_t consumerId, char *key) {
  int32_t         code = 0;
  SMqVgEp       **pVgEp = (SMqVgEp **)taosArrayPop(vgs);
  MND_TMQ_NULL_CHECK(pVgEp);
  SMqRebOutputVg outputVg = {consumerId, -1, *pVgEp};
  MND_TMQ_RETURN_CHECK(taosHashPut(pHash, &(*pVgEp)->vgId, sizeof(int32_t), &outputVg, sizeof(SMqRebOutputVg)));
  mInfo("[rebalance] sub:%s mq rebalance remove vgId:%d from consumer:0x%" PRIx64, key, (*pVgEp)->vgId, consumerId);
END:
  return code;
}

static int32_t processRemovedConsumers(SMqRebOutputObj *pOutput, SHashObj *pHash, const SMqRebInputObj *pInput) {
  int32_t code = 0;
  int32_t numOfRemoved = taosArrayGetSize(pInput->pRebInfo->removedConsumers);
  int32_t actualRemoved = 0;
  for (int32_t i = 0; i < numOfRemoved; i++) {
    int64_t*      consumerId = (int64_t *)taosArrayGet(pInput->pRebInfo->removedConsumers, i);
    MND_TMQ_NULL_CHECK(consumerId);
    SMqConsumerEp *pConsumerEp = taosHashGet(pOutput->pSub->consumerHash, consumerId, sizeof(int64_t));
    if (pConsumerEp == NULL) {
      continue;
    }

    int32_t consumerVgNum = taosArrayGetSize(pConsumerEp->vgs);
    for (int32_t j = 0; j < consumerVgNum; j++) {
      MND_TMQ_RETURN_CHECK(pushVgDataToHash(pConsumerEp->vgs, pHash, *consumerId, pOutput->pSub->key));
    }

    (void)taosArrayDestroy(pConsumerEp->vgs);
    MND_TMQ_RETURN_CHECK(taosHashRemove(pOutput->pSub->consumerHash, consumerId, sizeof(int64_t)));
    MND_TMQ_NULL_CHECK(taosArrayPush(pOutput->removedConsumers, consumerId));
    actualRemoved++;
  }

  if (numOfRemoved != actualRemoved) {
    mError("[rebalance] sub:%s mq rebalance removedNum:%d not matched with actual:%d", pOutput->pSub->key, numOfRemoved,
           actualRemoved);
  } else {
    mInfo("[rebalance] sub:%s removed %d consumers", pOutput->pSub->key, numOfRemoved);
  }
END:
  return code;
}

static int32_t processNewConsumers(SMqRebOutputObj *pOutput, const SMqRebInputObj *pInput) {
  int32_t code = 0;
  int32_t numOfNewConsumers = taosArrayGetSize(pInput->pRebInfo->newConsumers);

  for (int32_t i = 0; i < numOfNewConsumers; i++) {
    int64_t* consumerId = (int64_t *)taosArrayGet(pInput->pRebInfo->newConsumers, i);
    MND_TMQ_NULL_CHECK(consumerId);
    SMqConsumerEp newConsumerEp = {0};
    newConsumerEp.consumerId = *consumerId;
    newConsumerEp.vgs = taosArrayInit(0, sizeof(void *));
    MND_TMQ_NULL_CHECK(newConsumerEp.vgs);
    MND_TMQ_RETURN_CHECK(taosHashPut(pOutput->pSub->consumerHash, consumerId, sizeof(int64_t), &newConsumerEp, sizeof(SMqConsumerEp)));
    MND_TMQ_NULL_CHECK(taosArrayPush(pOutput->newConsumers, consumerId));
    mInfo("[rebalance] sub:%s mq rebalance add new consumer:0x%" PRIx64, pOutput->pSub->key, *consumerId);
  }
END:
  return code;
}

static int32_t processUnassignedVgroups(SMqRebOutputObj *pOutput, SHashObj *pHash) {
  int32_t code = 0;
  int32_t numOfVgroups = taosArrayGetSize(pOutput->pSub->unassignedVgs);
  for (int32_t i = 0; i < numOfVgroups; i++) {
    MND_TMQ_RETURN_CHECK(pushVgDataToHash(pOutput->pSub->unassignedVgs, pHash, -1, pOutput->pSub->key));
  }
END:
  return code;
}

static int32_t processModifiedConsumers(SMqRebOutputObj *pOutput, SHashObj *pHash, int32_t minVgCnt,
                                     int32_t remainderVgCnt) {
  int32_t code = 0;
  int32_t cnt = 0;
  void   *pIter = NULL;

  while (1) {
    pIter = taosHashIterate(pOutput->pSub->consumerHash, pIter);
    if (pIter == NULL) {
      break;
    }

    SMqConsumerEp *pConsumerEp = (SMqConsumerEp *)pIter;
    int32_t        consumerVgNum = taosArrayGetSize(pConsumerEp->vgs);

    MND_TMQ_NULL_CHECK(taosArrayPush(pOutput->modifyConsumers, &pConsumerEp->consumerId));
    if (consumerVgNum > minVgCnt) {
      if (cnt < remainderVgCnt) {
        while (taosArrayGetSize(pConsumerEp->vgs) > minVgCnt + 1) {  // pop until equal minVg + 1
          MND_TMQ_RETURN_CHECK(pushVgDataToHash(pConsumerEp->vgs, pHash, pConsumerEp->consumerId, pOutput->pSub->key));
        }
        cnt++;
      } else {
        while (taosArrayGetSize(pConsumerEp->vgs) > minVgCnt) {
          MND_TMQ_RETURN_CHECK(pushVgDataToHash(pConsumerEp->vgs, pHash, pConsumerEp->consumerId, pOutput->pSub->key));
        }
      }
    }
  }
END:
  return code;
}

static int32_t processRemoveAddVgs(SMnode *pMnode, SMqRebOutputObj *pOutput) {
  int32_t code = 0;
  int32_t totalVgNum = 0;
  SVgObj *pVgroup = NULL;
  SMqVgEp *pVgEp = NULL;
  void   *pIter = NULL;
  SArray *newVgs = taosArrayInit(0, POINTER_BYTES);
  MND_TMQ_NULL_CHECK(newVgs);
  while (1) {
    pIter = sdbFetch(pMnode->pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) {
      break;
    }

    if (!mndVgroupInDb(pVgroup, pOutput->pSub->dbUid)) {
      sdbRelease(pMnode->pSdb, pVgroup);
      continue;
    }

    totalVgNum++;
    pVgEp = taosMemoryMalloc(sizeof(SMqVgEp));
    MND_TMQ_NULL_CHECK(pVgEp);
    pVgEp->epSet = mndGetVgroupEpset(pMnode, pVgroup);
    pVgEp->vgId = pVgroup->vgId;
    MND_TMQ_NULL_CHECK(taosArrayPush(newVgs, &pVgEp));
    pVgEp = NULL;
    sdbRelease(pMnode->pSdb, pVgroup);
  }

  pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pOutput->pSub->consumerHash, pIter);
    if (pIter == NULL) break;
    SMqConsumerEp *pConsumerEp = (SMqConsumerEp *)pIter;
    int32_t j = 0;
    while (j < taosArrayGetSize(pConsumerEp->vgs)) {
      SMqVgEp *pVgEpTmp = taosArrayGetP(pConsumerEp->vgs, j);
      MND_TMQ_NULL_CHECK(pVgEpTmp);
      bool     find = false;
      for (int32_t k = 0; k < taosArrayGetSize(newVgs); k++) {
        SMqVgEp *pnewVgEp = taosArrayGetP(newVgs, k);
        MND_TMQ_NULL_CHECK(pnewVgEp);
        if (pVgEpTmp->vgId == pnewVgEp->vgId) {
          tDeleteSMqVgEp(pnewVgEp);
          taosArrayRemove(newVgs, k);
          find = true;
          break;
        }
      }
      if (!find) {
        mInfo("[rebalance] processRemoveAddVgs old vgId:%d", pVgEpTmp->vgId);
        tDeleteSMqVgEp(pVgEpTmp);
        taosArrayRemove(pConsumerEp->vgs, j);
        continue;
      }
      j++;
    }
  }

  if (taosArrayGetSize(pOutput->pSub->unassignedVgs) == 0 && taosArrayGetSize(newVgs) != 0) {
    MND_TMQ_NULL_CHECK(taosArrayAddAll(pOutput->pSub->unassignedVgs, newVgs));
    mInfo("[rebalance] processRemoveAddVgs add new vg num:%d", (int)taosArrayGetSize(newVgs));
    taosArrayDestroy(newVgs);
  } else {
    taosArrayDestroyP(newVgs, (FDelete)tDeleteSMqVgEp);
  }
  return totalVgNum;

END:
  sdbRelease(pMnode->pSdb, pVgroup);
  taosMemoryFree(pVgEp);
  taosArrayDestroyP(newVgs, (FDelete)tDeleteSMqVgEp);
  return code;
}

static int32_t processSubOffsetRows(SMnode *pMnode, const SMqRebInputObj *pInput, SMqRebOutputObj *pOutput) {
  SMqSubscribeObj *pSub = NULL;
  int32_t          code = mndAcquireSubscribeByKey(pMnode, pInput->pRebInfo->key, &pSub);  // put all offset rows
  if( code != 0){
    return 0;
  }
  taosRLockLatch(&pSub->lock);
  if (pOutput->pSub->offsetRows == NULL) {
    pOutput->pSub->offsetRows = taosArrayInit(4, sizeof(OffsetRows));
    if(pOutput->pSub->offsetRows == NULL) {
      taosRUnLockLatch(&pSub->lock);
      code = terrno;
      goto END;
    }
  }
  void *pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pSub->consumerHash, pIter);
    if (pIter == NULL) break;
    SMqConsumerEp *pConsumerEp = (SMqConsumerEp *)pIter;
    SMqConsumerEp *pConsumerEpNew = taosHashGet(pOutput->pSub->consumerHash, &pConsumerEp->consumerId, sizeof(int64_t));

    for (int j = 0; j < taosArrayGetSize(pConsumerEp->offsetRows); j++) {
      OffsetRows *d1 = taosArrayGet(pConsumerEp->offsetRows, j);
      MND_TMQ_NULL_CHECK(d1);
      bool        jump = false;
      for (int i = 0; pConsumerEpNew && i < taosArrayGetSize(pConsumerEpNew->vgs); i++) {
        SMqVgEp *pVgEp = taosArrayGetP(pConsumerEpNew->vgs, i);
        MND_TMQ_NULL_CHECK(pVgEp);
        if (pVgEp->vgId == d1->vgId) {
          jump = true;
          mInfo("pSub->offsetRows jump, because consumer id:0x%" PRIx64 " and vgId:%d not change",
                pConsumerEp->consumerId, pVgEp->vgId);
          break;
        }
      }
      if (jump) continue;
      bool find = false;
      for (int i = 0; i < taosArrayGetSize(pOutput->pSub->offsetRows); i++) {
        OffsetRows *d2 = taosArrayGet(pOutput->pSub->offsetRows, i);
        MND_TMQ_NULL_CHECK(d2);
        if (d1->vgId == d2->vgId) {
          d2->rows += d1->rows;
          d2->offset = d1->offset;
          d2->ever = d1->ever;
          find = true;
          mInfo("pSub->offsetRows add vgId:%d, after:%" PRId64 ", before:%" PRId64, d2->vgId, d2->rows, d1->rows);
          break;
        }
      }
      if (!find) {
        MND_TMQ_NULL_CHECK(taosArrayPush(pOutput->pSub->offsetRows, d1));
      }
    }
  }
  taosRUnLockLatch(&pSub->lock);
  mndReleaseSubscribe(pMnode, pSub);

END:
  return code;
}

static void printRebalanceLog(SMqRebOutputObj *pOutput) {
  mInfo("sub:%s mq rebalance calculation completed, re-balanced vg", pOutput->pSub->key);
  for (int32_t i = 0; i < taosArrayGetSize(pOutput->rebVgs); i++) {
    SMqRebOutputVg *pOutputRebVg = taosArrayGet(pOutput->rebVgs, i);
    if (pOutputRebVg == NULL) continue;
    mInfo("sub:%s mq rebalance vgId:%d, moved from consumer:0x%" PRIx64 ", to consumer:0x%" PRIx64, pOutput->pSub->key,
          pOutputRebVg->pVgEp->vgId, pOutputRebVg->oldConsumerId, pOutputRebVg->newConsumerId);
  }

  void *pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pOutput->pSub->consumerHash, pIter);
    if (pIter == NULL) break;
    SMqConsumerEp *pConsumerEp = (SMqConsumerEp *)pIter;
    int32_t        sz = taosArrayGetSize(pConsumerEp->vgs);
    mInfo("sub:%s mq rebalance final cfg: consumer:0x%" PRIx64 " has %d vg", pOutput->pSub->key,
          pConsumerEp->consumerId, sz);
    for (int32_t i = 0; i < sz; i++) {
      SMqVgEp *pVgEp = taosArrayGetP(pConsumerEp->vgs, i);
      if (pVgEp == NULL) continue;
      mInfo("sub:%s mq rebalance final cfg: vg %d to consumer:0x%" PRIx64, pOutput->pSub->key, pVgEp->vgId,
            pConsumerEp->consumerId);
    }
  }
}

static void calcVgroupsCnt(const SMqRebInputObj *pInput, int32_t totalVgNum, const char *pSubKey, int32_t *minVgCnt,
                           int32_t *remainderVgCnt) {
  int32_t numOfRemoved = taosArrayGetSize(pInput->pRebInfo->removedConsumers);
  int32_t numOfAdded = taosArrayGetSize(pInput->pRebInfo->newConsumers);
  int32_t numOfFinal = pInput->oldConsumerNum + numOfAdded - numOfRemoved;

  // calc num
  if (numOfFinal != 0) {
    *minVgCnt = totalVgNum / numOfFinal;
    *remainderVgCnt = totalVgNum % numOfFinal;
  } else {
    mInfo("[rebalance] sub:%s no consumer subscribe this topic", pSubKey);
  }
  mInfo(
      "[rebalance] sub:%s mq rebalance %d vgroups, existed consumers:%d, added:%d, removed:%d, minVg:%d remainderVg:%d",
      pSubKey, totalVgNum, pInput->oldConsumerNum, numOfAdded, numOfRemoved, *minVgCnt, *remainderVgCnt);
}

static int32_t assignVgroups(SMqRebOutputObj *pOutput, SHashObj *pHash, int32_t minVgCnt) {
  SMqRebOutputVg *pRebVg = NULL;
  void           *pAssignIter = NULL;
  void           *pIter = NULL;
  int32_t         code = 0;

  while (1) {
    pIter = taosHashIterate(pOutput->pSub->consumerHash, pIter);
    if (pIter == NULL) {
      break;
    }
    SMqConsumerEp *pConsumerEp = (SMqConsumerEp *)pIter;
    while (taosArrayGetSize(pConsumerEp->vgs) < minVgCnt) {
      pAssignIter = taosHashIterate(pHash, pAssignIter);
      if (pAssignIter == NULL) {
        mError("[rebalance] sub:%s assign iter is NULL, never should reach here", pOutput->pSub->key);
        break;
      }

      pRebVg = (SMqRebOutputVg *)pAssignIter;
      pRebVg->newConsumerId = pConsumerEp->consumerId;
      MND_TMQ_NULL_CHECK(taosArrayPush(pConsumerEp->vgs, &pRebVg->pVgEp));
      mInfo("[rebalance] mq rebalance: add vgId:%d to consumer:0x%" PRIx64 " for average", pRebVg->pVgEp->vgId,
            pConsumerEp->consumerId);
    }
  }

  while (1) {
    pIter = taosHashIterate(pOutput->pSub->consumerHash, pIter);
    if (pIter == NULL) {
      break;
    }
    SMqConsumerEp *pConsumerEp = (SMqConsumerEp *)pIter;
    if (taosArrayGetSize(pConsumerEp->vgs) == minVgCnt) {
      pAssignIter = taosHashIterate(pHash, pAssignIter);
      if (pAssignIter == NULL) {
        mInfo("[rebalance] sub:%s assign iter is used up", pOutput->pSub->key);
        break;
      }

      pRebVg = (SMqRebOutputVg *)pAssignIter;
      pRebVg->newConsumerId = pConsumerEp->consumerId;
      MND_TMQ_NULL_CHECK(taosArrayPush(pConsumerEp->vgs, &pRebVg->pVgEp));
      mInfo("[rebalance] mq rebalance: add vgId:%d to consumer:0x%" PRIx64 " for average + 1", pRebVg->pVgEp->vgId,
            pConsumerEp->consumerId);
    }
  }

  taosHashCancelIterate(pOutput->pSub->consumerHash, pIter);
  if (pAssignIter != NULL) {
    mError("[rebalance]sub:%s assign iter is not NULL, never should reach here", pOutput->pSub->key);
    code = TSDB_CODE_PAR_INTERNAL_ERROR;
    goto END;
  }
  while (1) {
    pAssignIter = taosHashIterate(pHash, pAssignIter);
    if (pAssignIter == NULL) {
      break;
    }

    SMqRebOutputVg *pRebOutput = (SMqRebOutputVg *)pAssignIter;
    MND_TMQ_NULL_CHECK(taosArrayPush(pOutput->rebVgs, pRebOutput));
    if (taosHashGetSize(pOutput->pSub->consumerHash) == 0) {            // if all consumer is removed
      MND_TMQ_NULL_CHECK(taosArrayPush(pOutput->pSub->unassignedVgs, &pRebOutput->pVgEp));  // put all vg into unassigned
    }
  }

END:
  return code;
}

static int32_t mndDoRebalance(SMnode *pMnode, const SMqRebInputObj *pInput, SMqRebOutputObj *pOutput) {
  int32_t     totalVgNum = processRemoveAddVgs(pMnode, pOutput);
  if (totalVgNum < 0){
    return totalVgNum;
  }
  const char *pSubKey = pOutput->pSub->key;
  int32_t     minVgCnt = 0;
  int32_t     remainderVgCnt = 0;
  int32_t     code = 0;
  SHashObj   *pHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  MND_TMQ_NULL_CHECK(pHash);
  MND_TMQ_RETURN_CHECK(processRemovedConsumers(pOutput, pHash, pInput));
  MND_TMQ_RETURN_CHECK(processUnassignedVgroups(pOutput, pHash));
  calcVgroupsCnt(pInput, totalVgNum, pSubKey, &minVgCnt, &remainderVgCnt);
  MND_TMQ_RETURN_CHECK(processModifiedConsumers(pOutput, pHash, minVgCnt, remainderVgCnt));
  MND_TMQ_RETURN_CHECK(processNewConsumers(pOutput, pInput));
  MND_TMQ_RETURN_CHECK(assignVgroups(pOutput, pHash, minVgCnt));
  MND_TMQ_RETURN_CHECK(processSubOffsetRows(pMnode, pInput, pOutput));
  printRebalanceLog(pOutput);
  taosHashCleanup(pHash);

END:
  return code;
}

static int32_t presistConsumerByType(STrans *pTrans, SArray *consumers, int8_t type, char *cgroup, char *topic) {
  int32_t         code = 0;
  SMqConsumerObj *pConsumerNew = NULL;
  int32_t         consumerNum = taosArrayGetSize(consumers);
  for (int32_t i = 0; i < consumerNum; i++) {
    int64_t* consumerId = (int64_t *)taosArrayGet(consumers, i);
    MND_TMQ_NULL_CHECK(consumerId);
    MND_TMQ_RETURN_CHECK(tNewSMqConsumerObj(*consumerId, cgroup, type, topic, NULL, &pConsumerNew));
    MND_TMQ_RETURN_CHECK(mndSetConsumerCommitLogs(pTrans, pConsumerNew));
    tDeleteSMqConsumerObj(pConsumerNew);
  }
  pConsumerNew = NULL;

END:
  tDeleteSMqConsumerObj(pConsumerNew);
  return code;
}

static int32_t mndPresistConsumer(STrans *pTrans, const SMqRebOutputObj *pOutput, char *cgroup, char *topic) {
  int32_t code = 0;
  MND_TMQ_RETURN_CHECK(presistConsumerByType(pTrans, pOutput->modifyConsumers, CONSUMER_UPDATE_REB, cgroup, NULL));
  MND_TMQ_RETURN_CHECK(presistConsumerByType(pTrans, pOutput->newConsumers, CONSUMER_ADD_REB, cgroup, topic));
  MND_TMQ_RETURN_CHECK(presistConsumerByType(pTrans, pOutput->removedConsumers, CONSUMER_REMOVE_REB, cgroup, topic));
END:
  return code;
}

static int32_t mndPersistRebResult(SMnode *pMnode, SRpcMsg *pMsg, const SMqRebOutputObj *pOutput) {
  struct SSubplan *pPlan = NULL;
  int32_t          code = 0;
  STrans          *pTrans = NULL;

  if (strcmp(pOutput->pSub->qmsg, "") != 0) {
    MND_TMQ_RETURN_CHECK(qStringToSubplan(pOutput->pSub->qmsg, &pPlan));
  }

  char topic[TSDB_TOPIC_FNAME_LEN] = {0};
  char cgroup[TSDB_CGROUP_LEN] = {0};
  mndSplitSubscribeKey(pOutput->pSub->key, topic, cgroup, true);

  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB_INSIDE, pMsg, "tmq-reb");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto END;
  }

  mndTransSetDbName(pTrans, pOutput->pSub->dbName, pOutput->pSub->key);
  MND_TMQ_RETURN_CHECK(mndTransCheckConflict(pMnode, pTrans));

  // 1. redo action: action to all vg
  const SArray *rebVgs = pOutput->rebVgs;
  int32_t       vgNum = taosArrayGetSize(rebVgs);
  for (int32_t i = 0; i < vgNum; i++) {
    SMqRebOutputVg *pRebVg = taosArrayGet(rebVgs, i);
    MND_TMQ_NULL_CHECK(pRebVg);
    MND_TMQ_RETURN_CHECK(mndPersistSubChangeVgReq(pMnode, pTrans, pOutput->pSub, pRebVg, pPlan));
  }

  // 2. commit log: subscribe and vg assignment
  MND_TMQ_RETURN_CHECK(mndSetSubCommitLogs(pTrans, pOutput->pSub));

  // 3. commit log: consumer to update status and epoch
  MND_TMQ_RETURN_CHECK(mndPresistConsumer(pTrans, pOutput, cgroup, topic));

  // 4. set cb
  mndTransSetCb(pTrans, TRANS_START_FUNC_MQ_REB, TRANS_STOP_FUNC_MQ_REB, NULL, 0);

  // 5. execution
  MND_TMQ_RETURN_CHECK(mndTransPrepare(pMnode, pTrans));

END:
  nodesDestroyNode((SNode *)pPlan);
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static void freeRebalanceItem(void *param) {
  SMqRebInfo *pInfo = param;
  (void)taosArrayDestroy(pInfo->newConsumers);
  (void)taosArrayDestroy(pInfo->removedConsumers);
}

// type = 0 remove  type = 1 add
static int32_t buildRebInfo(SHashObj *rebSubHash, SArray *topicList, int8_t type, char *group, int64_t consumerId) {
  int32_t code = 0;
  int32_t topicNum = taosArrayGetSize(topicList);
  for (int32_t i = 0; i < topicNum; i++) {
    char *removedTopic = taosArrayGetP(topicList, i);
    MND_TMQ_NULL_CHECK(removedTopic);
    char  key[TSDB_SUBSCRIBE_KEY_LEN] = {0};
    (void)snprintf(key, TSDB_SUBSCRIBE_KEY_LEN, "%s%s%s", group, TMQ_SEPARATOR, removedTopic);
    SMqRebInfo *pRebSub = NULL;
    MND_TMQ_RETURN_CHECK(mndGetOrCreateRebSub(rebSubHash, key, &pRebSub));
    if (type == 0)
      MND_TMQ_NULL_CHECK(taosArrayPush(pRebSub->removedConsumers, &consumerId));
    else if (type == 1)
      MND_TMQ_NULL_CHECK(taosArrayPush(pRebSub->newConsumers, &consumerId));
  }

END:
  return code;
}

static void checkForVgroupSplit(SMnode *pMnode, SMqConsumerObj *pConsumer, SHashObj *rebSubHash) {
  int32_t newTopicNum = taosArrayGetSize(pConsumer->currentTopics);
  for (int32_t i = 0; i < newTopicNum; i++) {
    char            *topic = taosArrayGetP(pConsumer->currentTopics, i);
    if (topic == NULL){
      continue;
    }
    SMqSubscribeObj *pSub = NULL;
    char  key[TSDB_SUBSCRIBE_KEY_LEN] = {0};
    (void)snprintf(key, TSDB_SUBSCRIBE_KEY_LEN, "%s%s%s", pConsumer->cgroup, TMQ_SEPARATOR, topic);
    int32_t code = mndAcquireSubscribeByKey(pMnode, key, &pSub);
    if (code != 0) {
      continue;
    }
    taosRLockLatch(&pSub->lock);

    // iterate all vg assigned to the consumer of that topic
    SMqConsumerEp *pConsumerEp = taosHashGet(pSub->consumerHash, &pConsumer->consumerId, sizeof(int64_t));
    if (pConsumerEp == NULL){
      taosRUnLockLatch(&pSub->lock);
      mndReleaseSubscribe(pMnode, pSub);
      continue;
    }
    int32_t vgNum = taosArrayGetSize(pConsumerEp->vgs);
    for (int32_t j = 0; j < vgNum; j++) {
      SMqVgEp *pVgEp = taosArrayGetP(pConsumerEp->vgs, j);
      if (pVgEp == NULL) {
        continue;
      }
      SVgObj  *pVgroup = mndAcquireVgroup(pMnode, pVgEp->vgId);
      if (!pVgroup) {
        (void)mndGetOrCreateRebSub(rebSubHash, key, NULL);
        mInfo("vnode splitted, vgId:%d rebalance will be triggered", pVgEp->vgId);
      }
      mndReleaseVgroup(pMnode, pVgroup);
    }
    taosRUnLockLatch(&pSub->lock);
    mndReleaseSubscribe(pMnode, pSub);
  }
}

static int32_t mndCheckConsumer(SRpcMsg *pMsg, SHashObj *rebSubHash) {
  SMnode         *pMnode = pMsg->info.node;
  SSdb           *pSdb = pMnode->pSdb;
  SMqConsumerObj *pConsumer = NULL;
  void           *pIter = NULL;
  int32_t         code = 0;

  // iterate all consumers, find all modification
  while (1) {
    pIter = sdbFetch(pSdb, SDB_CONSUMER, pIter, (void **)&pConsumer);
    if (pIter == NULL) {
      break;
    }

    int32_t hbStatus = atomic_add_fetch_32(&pConsumer->hbStatus, 1);
    int32_t pollStatus = atomic_add_fetch_32(&pConsumer->pollStatus, 1);
    int32_t status = atomic_load_32(&pConsumer->status);

    mDebug("[rebalance] check for consumer:0x%" PRIx64 " status:%d(%s), sub-time:%" PRId64 ", createTime:%" PRId64
           ", hbstatus:%d, pollStatus:%d",
           pConsumer->consumerId, status, mndConsumerStatusName(status), pConsumer->subscribeTime,
           pConsumer->createTime, hbStatus, pollStatus);

    if (status == MQ_CONSUMER_STATUS_READY) {
      if (taosArrayGetSize(pConsumer->currentTopics) == 0) {  // unsubscribe or close
        MND_TMQ_RETURN_CHECK(mndSendConsumerMsg(pMnode, pConsumer->consumerId, TDMT_MND_TMQ_LOST_CONSUMER_CLEAR, &pMsg->info));
      } else if (hbStatus * tsMqRebalanceInterval * 1000 >= pConsumer->sessionTimeoutMs ||
                 pollStatus * tsMqRebalanceInterval * 1000 >= pConsumer->maxPollIntervalMs) {
        taosRLockLatch(&pConsumer->lock);
        MND_TMQ_RETURN_CHECK(buildRebInfo(rebSubHash, pConsumer->currentTopics, 0, pConsumer->cgroup, pConsumer->consumerId));
        taosRUnLockLatch(&pConsumer->lock);
      } else {
        checkForVgroupSplit(pMnode, pConsumer, rebSubHash);
      }
    } else if (status == MQ_CONSUMER_STATUS_REBALANCE) {
      taosRLockLatch(&pConsumer->lock);
      MND_TMQ_RETURN_CHECK(buildRebInfo(rebSubHash, pConsumer->rebNewTopics, 1, pConsumer->cgroup, pConsumer->consumerId));
      MND_TMQ_RETURN_CHECK(buildRebInfo(rebSubHash, pConsumer->rebRemovedTopics, 0, pConsumer->cgroup, pConsumer->consumerId));
      taosRUnLockLatch(&pConsumer->lock);
    } else {
      MND_TMQ_RETURN_CHECK(mndSendConsumerMsg(pMnode, pConsumer->consumerId, TDMT_MND_TMQ_LOST_CONSUMER_CLEAR, &pMsg->info));
    }

    mndReleaseConsumer(pMnode, pConsumer);
  }
END:
  return code;
}

bool mndRebTryStart() {
  int32_t old = atomic_val_compare_exchange_32(&mqRebInExecCnt, 0, 1);
  if (old > 0) mInfo("[rebalance] counter old val:%d", old) return old == 0;
}

void mndRebCntInc() {
  int32_t val = atomic_add_fetch_32(&mqRebInExecCnt, 1);
  if (val > 0) mInfo("[rebalance] cnt inc, value:%d", val)
}

void mndRebCntDec() {
  int32_t val = atomic_sub_fetch_32(&mqRebInExecCnt, 1);
  if (val > 0) mInfo("[rebalance] cnt sub, value:%d", val)
}

static void clearRebOutput(SMqRebOutputObj *rebOutput) {
  (void)taosArrayDestroy(rebOutput->newConsumers);
  (void)taosArrayDestroy(rebOutput->modifyConsumers);
  (void)taosArrayDestroy(rebOutput->removedConsumers);
  (void)taosArrayDestroy(rebOutput->rebVgs);
  tDeleteSubscribeObj(rebOutput->pSub);
  taosMemoryFree(rebOutput->pSub);
}

static int32_t initRebOutput(SMqRebOutputObj *rebOutput) {
  int32_t code = 0;
  rebOutput->newConsumers = taosArrayInit(0, sizeof(int64_t));
  MND_TMQ_NULL_CHECK(rebOutput->newConsumers);
  rebOutput->removedConsumers = taosArrayInit(0, sizeof(int64_t));
  MND_TMQ_NULL_CHECK(rebOutput->removedConsumers);
  rebOutput->modifyConsumers = taosArrayInit(0, sizeof(int64_t));
  MND_TMQ_NULL_CHECK(rebOutput->modifyConsumers);
  rebOutput->rebVgs = taosArrayInit(0, sizeof(SMqRebOutputVg));
  MND_TMQ_NULL_CHECK(rebOutput->rebVgs);
  return code;

END:
  clearRebOutput(rebOutput);
  return code;
}

// This function only works when there are dirty consumers
static int32_t checkConsumer(SMnode *pMnode, SMqSubscribeObj *pSub) {
  int32_t code = 0;
  void   *pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pSub->consumerHash, pIter);
    if (pIter == NULL) {
      break;
    }

    SMqConsumerEp  *pConsumerEp = (SMqConsumerEp *)pIter;
    SMqConsumerObj *pConsumer = NULL;
    code = mndAcquireConsumer(pMnode, pConsumerEp->consumerId, &pConsumer);
    if (code == 0) {
      mndReleaseConsumer(pMnode, pConsumer);
      continue;
    }
    mError("consumer:0x%" PRIx64 " not exists in sdb for exception", pConsumerEp->consumerId);
    MND_TMQ_NULL_CHECK(taosArrayAddAll(pSub->unassignedVgs, pConsumerEp->vgs));

    (void)taosArrayDestroy(pConsumerEp->vgs);
    MND_TMQ_RETURN_CHECK(taosHashRemove(pSub->consumerHash, &pConsumerEp->consumerId, sizeof(int64_t)));
  }
END:
  return code;
}

static int32_t buildRebOutput(SMnode *pMnode, SMqRebInputObj *rebInput, SMqRebOutputObj *rebOutput) {
  const char      *key = rebInput->pRebInfo->key;
  SMqSubscribeObj *pSub = NULL;
  int32_t          code = mndAcquireSubscribeByKey(pMnode, key, &pSub);

  if (code != 0) {
    // split sub key and extract topic
    char topic[TSDB_TOPIC_FNAME_LEN] = {0};
    char cgroup[TSDB_CGROUP_LEN] = {0};
    mndSplitSubscribeKey(key, topic, cgroup, true);
    SMqTopicObj *pTopic = NULL;
    MND_TMQ_RETURN_CHECK(mndAcquireTopic(pMnode, topic, &pTopic));
    taosRLockLatch(&pTopic->lock);

    rebInput->oldConsumerNum = 0;
    code = mndCreateSubscription(pMnode, pTopic, key, &rebOutput->pSub);
    if (code != 0) {
      mError("[rebalance] mq rebalance %s failed create sub since %s, ignore", key, tstrerror(code));
      taosRUnLockLatch(&pTopic->lock);
      mndReleaseTopic(pMnode, pTopic);
      return code;
    }

    (void)memcpy(rebOutput->pSub->dbName, pTopic->db, TSDB_DB_FNAME_LEN);
    taosRUnLockLatch(&pTopic->lock);
    mndReleaseTopic(pMnode, pTopic);

    mInfo("[rebalance] sub topic:%s has no consumers sub yet", key);
  } else {
    taosRLockLatch(&pSub->lock);
    code = tCloneSubscribeObj(pSub, &rebOutput->pSub);
    if(code != 0){
      taosRUnLockLatch(&pSub->lock);
      goto END;
    }
    code = checkConsumer(pMnode, rebOutput->pSub);
    if(code != 0){
      taosRUnLockLatch(&pSub->lock);
      goto END;
    }
    rebInput->oldConsumerNum = taosHashGetSize(rebOutput->pSub->consumerHash);
    taosRUnLockLatch(&pSub->lock);

    mInfo("[rebalance] sub topic:%s has %d consumers sub till now", key, rebInput->oldConsumerNum);
    mndReleaseSubscribe(pMnode, pSub);
  }

END:
  return code;
}

static int32_t mndProcessRebalanceReq(SRpcMsg *pMsg) {
  int     code = 0;
  void   *pIter = NULL;
  SMnode *pMnode = pMsg->info.node;
  mDebug("[rebalance] start to process mq timer");
  if (!mndRebTryStart()) {
    mInfo("[rebalance] mq rebalance already in progress, do nothing");
    return code;
  }

  SHashObj *rebSubHash = taosHashInit(64, MurmurHash3_32, true, HASH_NO_LOCK);
  MND_TMQ_NULL_CHECK(rebSubHash);

  taosHashSetFreeFp(rebSubHash, freeRebalanceItem);

  MND_TMQ_RETURN_CHECK(mndCheckConsumer(pMsg, rebSubHash));
  if (taosHashGetSize(rebSubHash) > 0) {
    mInfo("[rebalance] mq rebalance start, total required re-balanced trans:%d", taosHashGetSize(rebSubHash))
  }

  while (1) {
    pIter = taosHashIterate(rebSubHash, pIter);
    if (pIter == NULL) {
      break;
    }

    SMqRebInputObj  rebInput = {0};
    SMqRebOutputObj rebOutput = {0};
    MND_TMQ_RETURN_CHECK(initRebOutput(&rebOutput));
    rebInput.pRebInfo = (SMqRebInfo *)pIter;
    code = buildRebOutput(pMnode, &rebInput, &rebOutput);
    if (code != 0) {
      mError("mq rebalance buildRebOutput, msg:%s", tstrerror(code))
    }

    if (code == 0){
      code = mndDoRebalance(pMnode, &rebInput, &rebOutput);
      if (code != 0) {
        mError("mq rebalance do rebalance error, msg:%s", tstrerror(code))
      }
    }

    if (code == 0){
      code = mndPersistRebResult(pMnode, pMsg, &rebOutput);
      if (code != 0) {
        mError("mq rebalance persist output error, possibly vnode splitted or dropped,msg:%s", tstrerror(code))
      }
    }

    clearRebOutput(&rebOutput);
  }

  if (taosHashGetSize(rebSubHash) > 0) {
    mInfo("[rebalance] mq rebalance completed successfully, wait trans finish")
  }

END:
  taosHashCancelIterate(rebSubHash, pIter);
  taosHashCleanup(rebSubHash);
  mndRebCntDec();

  TAOS_RETURN(code);
}

static int32_t sendDeleteSubToVnode(SMnode *pMnode, SMqSubscribeObj *pSub, STrans *pTrans) {
  void   *pIter = NULL;
  SVgObj *pVgObj = NULL;
  int32_t code = 0;
  while (1) {
    pIter = sdbFetch(pMnode->pSdb, SDB_VGROUP, pIter, (void **)&pVgObj);
    if (pIter == NULL) {
      break;
    }

    if (!mndVgroupInDb(pVgObj, pSub->dbUid)) {
      sdbRelease(pMnode->pSdb, pVgObj);
      continue;
    }
    SMqVDeleteReq *pReq = taosMemoryCalloc(1, sizeof(SMqVDeleteReq));
    MND_TMQ_NULL_CHECK(pReq);
    pReq->head.vgId = htonl(pVgObj->vgId);
    pReq->vgId = pVgObj->vgId;
    pReq->consumerId = -1;
    (void)memcpy(pReq->subKey, pSub->key, TSDB_SUBSCRIBE_KEY_LEN);

    STransAction action = {0};
    action.epSet = mndGetVgroupEpset(pMnode, pVgObj);
    action.pCont = pReq;
    action.contLen = sizeof(SMqVDeleteReq);
    action.msgType = TDMT_VND_TMQ_DELETE_SUB;
    action.acceptableCode = TSDB_CODE_MND_VGROUP_NOT_EXIST;

    sdbRelease(pMnode->pSdb, pVgObj);
    MND_TMQ_RETURN_CHECK(mndTransAppendRedoAction(pTrans, &action));
  }

END:
  sdbRelease(pMnode->pSdb, pVgObj);
  sdbCancelFetch(pMnode->pSdb, pIter);
  return code;
}

static int32_t mndCheckConsumerByGroup(SMnode *pMnode, STrans *pTrans, char *cgroup, char *topic) {
  void           *pIter = NULL;
  SMqConsumerObj *pConsumer = NULL;
  int             code = 0;
  while (1) {
    pIter = sdbFetch(pMnode->pSdb, SDB_CONSUMER, pIter, (void **)&pConsumer);
    if (pIter == NULL) {
      break;
    }

    if (strcmp(cgroup, pConsumer->cgroup) != 0) {
      sdbRelease(pMnode->pSdb, pConsumer);
      continue;
    }

    bool found = checkTopic(pConsumer->assignedTopics, topic);
    if (found){
      mError("topic:%s, failed to drop since subscribed by consumer:0x%" PRIx64 ", in consumer group %s",
             topic, pConsumer->consumerId, pConsumer->cgroup);
      code = TSDB_CODE_MND_CGROUP_USED;
      goto END;
    }

    sdbRelease(pMnode->pSdb, pConsumer);
  }

END:
  sdbRelease(pMnode->pSdb, pConsumer);
  sdbCancelFetch(pMnode->pSdb, pIter);
  return code;
}

static int32_t mndProcessDropCgroupReq(SRpcMsg *pMsg) {
  SMnode         *pMnode = pMsg->info.node;
  SMDropCgroupReq dropReq = {0};
  STrans         *pTrans = NULL;
  int32_t         code = TSDB_CODE_ACTION_IN_PROGRESS;
  SMqSubscribeObj *pSub = NULL;

  MND_TMQ_RETURN_CHECK(tDeserializeSMDropCgroupReq(pMsg->pCont, pMsg->contLen, &dropReq));
  char  key[TSDB_SUBSCRIBE_KEY_LEN] = {0};
  (void)snprintf(key, TSDB_SUBSCRIBE_KEY_LEN, "%s%s%s", dropReq.cgroup, TMQ_SEPARATOR, dropReq.topic);
  code = mndAcquireSubscribeByKey(pMnode, key, &pSub);
  if (code != 0) {
    if (dropReq.igNotExists) {
      mInfo("cgroup:%s on topic:%s, not exist, ignore not exist is set", dropReq.cgroup, dropReq.topic);
      return 0;
    } else {
      code = TSDB_CODE_MND_SUBSCRIBE_NOT_EXIST;
      mError("topic:%s, cgroup:%s, failed to drop since %s", dropReq.topic, dropReq.cgroup, tstrerror(code));
      return code;
    }
  }

  taosWLockLatch(&pSub->lock);
  if (taosHashGetSize(pSub->consumerHash) != 0) {
    code = TSDB_CODE_MND_CGROUP_USED;
    mError("cgroup:%s on topic:%s, failed to drop since %s", dropReq.cgroup, dropReq.topic, tstrerror(code));
    goto END;
  }

  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB, pMsg, "drop-cgroup");
  MND_TMQ_NULL_CHECK(pTrans);
  mInfo("trans:%d, used to drop cgroup:%s on topic %s", pTrans->id, dropReq.cgroup, dropReq.topic);
  mndTransSetDbName(pTrans, pSub->dbName, NULL);
  MND_TMQ_RETURN_CHECK(mndTransCheckConflict(pMnode, pTrans));
  MND_TMQ_RETURN_CHECK(sendDeleteSubToVnode(pMnode, pSub, pTrans));
  MND_TMQ_RETURN_CHECK(mndCheckConsumerByGroup(pMnode, pTrans, dropReq.cgroup, dropReq.topic));
  MND_TMQ_RETURN_CHECK(mndSetDropSubCommitLogs(pMnode, pTrans, pSub));
  MND_TMQ_RETURN_CHECK(mndTransPrepare(pMnode, pTrans));

END:
  taosWUnLockLatch(&pSub->lock);
  mndReleaseSubscribe(pMnode, pSub);
  mndTransDrop(pTrans);

  if (code != 0) {
    mError("cgroup %s on topic:%s, failed to drop", dropReq.cgroup, dropReq.topic);
    TAOS_RETURN(code);
  }
  TAOS_RETURN(TSDB_CODE_ACTION_IN_PROGRESS);
}

void mndCleanupSubscribe(SMnode *pMnode) {}

static SSdbRaw *mndSubActionEncode(SMqSubscribeObj *pSub) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  void   *buf = NULL;
  int32_t tlen = tEncodeSubscribeObj(NULL, pSub);
  if (tlen <= 0) goto SUB_ENCODE_OVER;
  int32_t size = sizeof(int32_t) + tlen + MND_SUBSCRIBE_RESERVE_SIZE;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_SUBSCRIBE, MND_SUBSCRIBE_VER_NUMBER, size);
  if (pRaw == NULL) goto SUB_ENCODE_OVER;

  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) goto SUB_ENCODE_OVER;

  void *abuf = buf;
  if (tEncodeSubscribeObj(&abuf, pSub) < 0){
    goto SUB_ENCODE_OVER;
  }

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, tlen, SUB_ENCODE_OVER);
  SDB_SET_BINARY(pRaw, dataPos, buf, tlen, SUB_ENCODE_OVER);
  SDB_SET_RESERVE(pRaw, dataPos, MND_SUBSCRIBE_RESERVE_SIZE, SUB_ENCODE_OVER);
  SDB_SET_DATALEN(pRaw, dataPos, SUB_ENCODE_OVER);

  terrno = TSDB_CODE_SUCCESS;

SUB_ENCODE_OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("subscribe:%s, failed to encode to raw:%p since %s", pSub->key, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("subscribe:%s, encode to raw:%p, row:%p", pSub->key, pRaw, pSub);
  return pRaw;
}

static SSdbRow *mndSubActionDecode(SSdbRaw *pRaw) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  SSdbRow         *pRow = NULL;
  SMqSubscribeObj *pSub = NULL;
  void            *buf = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto SUB_DECODE_OVER;

  if (sver > MND_SUBSCRIBE_VER_NUMBER || sver < 1) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto SUB_DECODE_OVER;
  }

  pRow = sdbAllocRow(sizeof(SMqSubscribeObj));
  if (pRow == NULL) goto SUB_DECODE_OVER;

  pSub = sdbGetRowObj(pRow);
  if (pSub == NULL) goto SUB_DECODE_OVER;

  int32_t dataPos = 0;
  int32_t tlen;
  SDB_GET_INT32(pRaw, dataPos, &tlen, SUB_DECODE_OVER);
  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) goto SUB_DECODE_OVER;
  SDB_GET_BINARY(pRaw, dataPos, buf, tlen, SUB_DECODE_OVER);
  SDB_GET_RESERVE(pRaw, dataPos, MND_SUBSCRIBE_RESERVE_SIZE, SUB_DECODE_OVER);

  if (tDecodeSubscribeObj(buf, pSub, sver) == NULL) {
    goto SUB_DECODE_OVER;
  }

  // update epset saved in mnode
  if (pSub->unassignedVgs != NULL) {
    int32_t size = (int32_t)taosArrayGetSize(pSub->unassignedVgs);
    for (int32_t i = 0; i < size; ++i) {
      SMqVgEp *pMqVgEp = (SMqVgEp *)taosArrayGetP(pSub->unassignedVgs, i);
      tmsgUpdateDnodeEpSet(&pMqVgEp->epSet);
    }
  }
  if (pSub->consumerHash != NULL) {
    void *pIter = taosHashIterate(pSub->consumerHash, NULL);
    while (pIter) {
      SMqConsumerEp *pConsumerEp = pIter;
      int32_t        size = (int32_t)taosArrayGetSize(pConsumerEp->vgs);
      for (int32_t i = 0; i < size; ++i) {
        SMqVgEp *pMqVgEp = (SMqVgEp *)taosArrayGetP(pConsumerEp->vgs, i);
        tmsgUpdateDnodeEpSet(&pMqVgEp->epSet);
      }
      pIter = taosHashIterate(pSub->consumerHash, pIter);
    }
  }

  terrno = TSDB_CODE_SUCCESS;

SUB_DECODE_OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("subscribe:%s, failed to decode from raw:%p since %s", pSub == NULL ? "null" : pSub->key, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("subscribe:%s, decode from raw:%p, row:%p", pSub->key, pRaw, pSub);
  return pRow;
}

static int32_t mndSubActionInsert(SSdb *pSdb, SMqSubscribeObj *pSub) {
  mTrace("subscribe:%s, perform insert action", pSub->key);
  return 0;
}

static int32_t mndSubActionDelete(SSdb *pSdb, SMqSubscribeObj *pSub) {
  mTrace("subscribe:%s, perform delete action", pSub->key);
  tDeleteSubscribeObj(pSub);
  return 0;
}

static int32_t mndSubActionUpdate(SSdb *pSdb, SMqSubscribeObj *pOldSub, SMqSubscribeObj *pNewSub) {
  mTrace("subscribe:%s, perform update action", pOldSub->key);
  taosWLockLatch(&pOldSub->lock);

  SHashObj *tmp = pOldSub->consumerHash;
  pOldSub->consumerHash = pNewSub->consumerHash;
  pNewSub->consumerHash = tmp;

  SArray *tmp1 = pOldSub->unassignedVgs;
  pOldSub->unassignedVgs = pNewSub->unassignedVgs;
  pNewSub->unassignedVgs = tmp1;

  SArray *tmp2 = pOldSub->offsetRows;
  pOldSub->offsetRows = pNewSub->offsetRows;
  pNewSub->offsetRows = tmp2;

  taosWUnLockLatch(&pOldSub->lock);
  return 0;
}

int32_t mndAcquireSubscribeByKey(SMnode *pMnode, const char *key, SMqSubscribeObj** pSub) {
  SSdb            *pSdb = pMnode->pSdb;
  *pSub = sdbAcquire(pSdb, SDB_SUBSCRIBE, key);
  if (*pSub == NULL) {
    return TSDB_CODE_MND_SUBSCRIBE_NOT_EXIST;
  }
  return 0;
}

int32_t mndGetGroupNumByTopic(SMnode *pMnode, const char *topicName) {
  int32_t num = 0;
  SSdb   *pSdb = pMnode->pSdb;

  void            *pIter = NULL;
  SMqSubscribeObj *pSub = NULL;
  while (1) {
    pIter = sdbFetch(pSdb, SDB_SUBSCRIBE, pIter, (void **)&pSub);
    if (pIter == NULL) break;

    char topic[TSDB_TOPIC_FNAME_LEN] = {0};
    char cgroup[TSDB_CGROUP_LEN] = {0};
    mndSplitSubscribeKey(pSub->key, topic, cgroup, true);
    if (strcmp(topic, topicName) != 0) {
      sdbRelease(pSdb, pSub);
      continue;
    }

    num++;
    sdbRelease(pSdb, pSub);
  }

  return num;
}

void mndReleaseSubscribe(SMnode *pMnode, SMqSubscribeObj *pSub) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pSub);
}

int32_t mndSetDropSubCommitLogs(SMnode *pMnode, STrans *pTrans, SMqSubscribeObj *pSub) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndSubActionEncode(pSub);
  MND_TMQ_NULL_CHECK(pCommitRaw);
  code = mndTransAppendCommitlog(pTrans, pCommitRaw);
  if (code != 0){
    sdbFreeRaw(pCommitRaw);
    goto END;
  }
  code = sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED);
END:
  return code;
}

int32_t mndDropSubByTopic(SMnode *pMnode, STrans *pTrans, const char *topicName) {
  SSdb            *pSdb = pMnode->pSdb;
  int32_t          code = 0;
  void            *pIter = NULL;
  SMqSubscribeObj *pSub = NULL;
  while (1) {
    sdbRelease(pSdb, pSub);
    pIter = sdbFetch(pSdb, SDB_SUBSCRIBE, pIter, (void **)&pSub);
    if (pIter == NULL) break;

    char topic[TSDB_TOPIC_FNAME_LEN] = {0};
    char cgroup[TSDB_CGROUP_LEN] = {0};
    mndSplitSubscribeKey(pSub->key, topic, cgroup, true);
    if (strcmp(topic, topicName) != 0) {
      continue;
    }

    // iter all vnode to delete handle
    if (taosHashGetSize(pSub->consumerHash) != 0) {
      code = TSDB_CODE_MND_IN_REBALANCE;
      goto END;
    }

    MND_TMQ_RETURN_CHECK(sendDeleteSubToVnode(pMnode, pSub, pTrans));
    MND_TMQ_RETURN_CHECK(mndSetDropSubCommitLogs(pMnode, pTrans, pSub));
  }

END:
  sdbRelease(pSdb, pSub);
  sdbCancelFetch(pSdb, pIter);

  TAOS_RETURN(code);
}

static int32_t buildResult(SSDataBlock *pBlock, int32_t *numOfRows, int64_t consumerId, const char* user, const char* fqdn,
                           const char *topic, const char *cgroup, SArray *vgs, SArray *offsetRows) {
  int32_t code = 0;
  int32_t sz = taosArrayGetSize(vgs);
  for (int32_t j = 0; j < sz; j++) {
    SMqVgEp *pVgEp = taosArrayGetP(vgs, j);
    MND_TMQ_NULL_CHECK(pVgEp);

    SColumnInfoData *pColInfo = NULL;
    int32_t          cols = 0;

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    MND_TMQ_NULL_CHECK(pColInfo);
    MND_TMQ_RETURN_CHECK(colDataSetVal(pColInfo, *numOfRows, (const char *)topic, false));

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    MND_TMQ_NULL_CHECK(pColInfo);
    MND_TMQ_RETURN_CHECK(colDataSetVal(pColInfo, *numOfRows, (const char *)cgroup, false));

    // vg id
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    MND_TMQ_NULL_CHECK(pColInfo);
    MND_TMQ_RETURN_CHECK(colDataSetVal(pColInfo, *numOfRows, (const char *)&pVgEp->vgId, false));

    // consumer id
    char consumerIdHex[TSDB_CONSUMER_ID_LEN] = {0};
    (void)sprintf(varDataVal(consumerIdHex), "0x%" PRIx64, consumerId);
    varDataSetLen(consumerIdHex, strlen(varDataVal(consumerIdHex)));

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    MND_TMQ_NULL_CHECK(pColInfo);
    MND_TMQ_RETURN_CHECK(colDataSetVal(pColInfo, *numOfRows, (const char *)consumerIdHex, consumerId == -1));

    char userStr[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
    if (user) STR_TO_VARSTR(userStr, user);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    MND_TMQ_NULL_CHECK(pColInfo);
    MND_TMQ_RETURN_CHECK(colDataSetVal(pColInfo, *numOfRows, userStr, user == NULL));

    char fqdnStr[TSDB_FQDN_LEN + VARSTR_HEADER_SIZE] = {0};
    if (fqdn) STR_TO_VARSTR(fqdnStr, fqdn);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    MND_TMQ_NULL_CHECK(pColInfo);
    MND_TMQ_RETURN_CHECK(colDataSetVal(pColInfo, *numOfRows, fqdnStr, fqdn == NULL));

    mInfo("mnd show subscriptions: topic %s, consumer:0x%" PRIx64 " cgroup %s vgid %d", varDataVal(topic), consumerId,
          varDataVal(cgroup), pVgEp->vgId);

    // offset
    OffsetRows *data = NULL;
    for (int i = 0; i < taosArrayGetSize(offsetRows); i++) {
      OffsetRows *tmp = taosArrayGet(offsetRows, i);
      MND_TMQ_NULL_CHECK(tmp);
      if (tmp->vgId != pVgEp->vgId) {
        mInfo("mnd show subscriptions: do not find vgId:%d, %d in offsetRows", tmp->vgId, pVgEp->vgId);
        continue;
      }
      data = tmp;
    }
    if (data) {
      // vg id
      char buf[TSDB_OFFSET_LEN * 2 + VARSTR_HEADER_SIZE] = {0};
      (void)tFormatOffset(varDataVal(buf), TSDB_OFFSET_LEN, &data->offset);
      (void)sprintf(varDataVal(buf) + strlen(varDataVal(buf)), "/%" PRId64, data->ever);
      varDataSetLen(buf, strlen(varDataVal(buf)));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      MND_TMQ_NULL_CHECK(pColInfo);
      MND_TMQ_RETURN_CHECK(colDataSetVal(pColInfo, *numOfRows, (const char *)buf, false));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      MND_TMQ_NULL_CHECK(pColInfo);
      MND_TMQ_RETURN_CHECK(colDataSetVal(pColInfo, *numOfRows, (const char *)&data->rows, false));
    } else {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      MND_TMQ_NULL_CHECK(pColInfo);
      colDataSetNULL(pColInfo, *numOfRows);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      MND_TMQ_NULL_CHECK(pColInfo);
      colDataSetNULL(pColInfo, *numOfRows);
      mInfo("mnd show subscriptions: do not find vgId:%d in offsetRows", pVgEp->vgId);
    }
    (*numOfRows)++;
  }
  return 0;
END:
  return code;
}

int32_t mndRetrieveSubscribe(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rowsCapacity) {
  SMnode          *pMnode = pReq->info.node;
  SSdb            *pSdb = pMnode->pSdb;
  int32_t          numOfRows = 0;
  SMqSubscribeObj *pSub = NULL;
  int32_t          code = 0;

  mInfo("mnd show subscriptions begin");

  while (numOfRows < rowsCapacity) {
    pShow->pIter = sdbFetch(pSdb, SDB_SUBSCRIBE, pShow->pIter, (void **)&pSub);
    if (pShow->pIter == NULL) {
      break;
    }

    taosRLockLatch(&pSub->lock);

    if (numOfRows + pSub->vgNum > rowsCapacity) {
      MND_TMQ_RETURN_CHECK(blockDataEnsureCapacity(pBlock, numOfRows + pSub->vgNum))  ;
    }

    // topic and cgroup
    char topic[TSDB_TOPIC_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
    char cgroup[TSDB_CGROUP_LEN + VARSTR_HEADER_SIZE] = {0};
    mndSplitSubscribeKey(pSub->key, varDataVal(topic), varDataVal(cgroup), false);
    varDataSetLen(topic, strlen(varDataVal(topic)));
    varDataSetLen(cgroup, strlen(varDataVal(cgroup)));

    SMqConsumerEp *pConsumerEp = NULL;
    void          *pIter = NULL;

    while (1) {
      pIter = taosHashIterate(pSub->consumerHash, pIter);
      if (pIter == NULL) break;
      pConsumerEp = (SMqConsumerEp *)pIter;

      char          *user = NULL;
      char          *fqdn = NULL;
      SMqConsumerObj *pConsumer = sdbAcquire(pSdb, SDB_CONSUMER, &pConsumerEp->consumerId);
      if (pConsumer != NULL) {
        user = pConsumer->user;
        fqdn = pConsumer->fqdn;
        sdbRelease(pSdb, pConsumer);
      }
      MND_TMQ_RETURN_CHECK(buildResult(pBlock, &numOfRows, pConsumerEp->consumerId, user, fqdn, topic, cgroup, pConsumerEp->vgs,
                  pConsumerEp->offsetRows));
    }

    MND_TMQ_RETURN_CHECK(buildResult(pBlock, &numOfRows, -1, NULL, NULL, topic, cgroup, pSub->unassignedVgs, pSub->offsetRows));

    pBlock->info.rows = numOfRows;

    taosRUnLockLatch(&pSub->lock);
    sdbRelease(pSdb, pSub);
  }

  mInfo("mnd end show subscriptions");

  pShow->numOfRows += numOfRows;
  return numOfRows;

END:
  return code;
}

void mndCancelGetNextSubscribe(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_SUBSCRIBE);
}
