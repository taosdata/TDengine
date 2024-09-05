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

#define MND_CONSUMER_LOST_HB_CNT          6
#define MND_CONSUMER_LOST_CLEAR_THRESHOLD 43200

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
static void     mndCheckConsumer(SRpcMsg *pMsg, SHashObj *hash);

static int32_t mndSetSubCommitLogs(STrans *pTrans, SMqSubscribeObj *pSub) {
  SSdbRaw *pCommitRaw = mndSubActionEncode(pSub);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY) != 0) return -1;
  return 0;
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

static SMqSubscribeObj *mndCreateSubscription(SMnode *pMnode, const SMqTopicObj *pTopic, const char *subKey) {
  SMqSubscribeObj *pSub = tNewSubscribeObj(subKey);
  if (pSub == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pSub->dbUid = pTopic->dbUid;
  pSub->stbUid = pTopic->stbUid;
  pSub->subType = pTopic->subType;
  pSub->withMeta = pTopic->withMeta;

  if (mndSchedInitSubEp(pMnode, pTopic, pSub) < 0) {
    tDeleteSubscribeObj(pSub);
    taosMemoryFree(pSub);
    return NULL;
  }

  return pSub;
}

static int32_t mndBuildSubChangeReq(void **pBuf, int32_t *pLen, SMqSubscribeObj *pSub, const SMqRebOutputVg *pRebVg,
                                    SSubplan *pPlan) {
  SMqRebVgReq req = {0};
  req.oldConsumerId = pRebVg->oldConsumerId;
  req.newConsumerId = pRebVg->newConsumerId;
  req.vgId = pRebVg->pVgEp->vgId;
  if (pPlan) {
    pPlan->execNode.epSet = pRebVg->pVgEp->epSet;
    pPlan->execNode.nodeId = pRebVg->pVgEp->vgId;
    int32_t msgLen;
    if (qSubPlanToString(pPlan, &req.qmsg, &msgLen) < 0) {
      terrno = TSDB_CODE_QRY_INVALID_INPUT;
      return -1;
    }
  } else {
    req.qmsg = taosStrdup("");
  }
  req.subType = pSub->subType;
  req.withMeta = pSub->withMeta;
  req.suid = pSub->stbUid;
  tstrncpy(req.subKey, pSub->key, TSDB_SUBSCRIBE_KEY_LEN);

  int32_t tlen = 0;
  int32_t ret = 0;
  tEncodeSize(tEncodeSMqRebVgReq, &req, tlen, ret);
  if (ret < 0) {
    taosMemoryFree(req.qmsg);
    return -1;
  }

  tlen += sizeof(SMsgHead);
  void *buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    taosMemoryFree(req.qmsg);
    return -1;
  }

  SMsgHead *pMsgHead = (SMsgHead *)buf;

  pMsgHead->contLen = htonl(tlen);
  pMsgHead->vgId = htonl(pRebVg->pVgEp->vgId);

  SEncoder encoder = {0};
  tEncoderInit(&encoder, POINTER_SHIFT(buf, sizeof(SMsgHead)), tlen);
  if (tEncodeSMqRebVgReq(&encoder, &req) < 0) {
    taosMemoryFreeClear(buf);
    tEncoderClear(&encoder);
    taosMemoryFree(req.qmsg);
    return -1;
  }
  tEncoderClear(&encoder);
  *pBuf = buf;
  *pLen = tlen;

  taosMemoryFree(req.qmsg);
  return 0;
}

static int32_t mndPersistSubChangeVgReq(SMnode *pMnode, STrans *pTrans, SMqSubscribeObj *pSub,
                                        const SMqRebOutputVg *pRebVg, SSubplan *pPlan) {
  if (pRebVg->oldConsumerId == pRebVg->newConsumerId) {
    if (pRebVg->oldConsumerId == -1) return 0;  // drop stream, no consumer, while split vnode,all consumerId is -1
    terrno = TSDB_CODE_MND_INVALID_SUB_OPTION;
    return -1;
  }

  void   *buf;
  int32_t tlen;
  if (mndBuildSubChangeReq(&buf, &tlen, pSub, pRebVg, pPlan) < 0) {
    return -1;
  }

  int32_t vgId = pRebVg->pVgEp->vgId;
  SVgObj *pVgObj = mndAcquireVgroup(pMnode, vgId);
  if (pVgObj == NULL) {
    taosMemoryFree(buf);
    terrno = TSDB_CODE_MND_VGROUP_NOT_EXIST;
    return -1;
  }

  STransAction action = {0};
  action.epSet = mndGetVgroupEpset(pMnode, pVgObj);
  action.pCont = buf;
  action.contLen = tlen;
  action.msgType = TDMT_VND_TMQ_SUBSCRIBE;

  mndReleaseVgroup(pMnode, pVgObj);
  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(buf);
    return -1;
  }
  return 0;
}

static int32_t mndSplitSubscribeKey(const char *key, char *topic, char *cgroup, bool fullName) {
  int32_t i = 0;
  while (key[i] != TMQ_SEPARATOR) {
    i++;
  }
  memcpy(cgroup, key, i);
  cgroup[i] = 0;
  if (fullName) {
    strcpy(topic, &key[i + 1]);
  } else {
    while (key[i] != '.') {
      i++;
    }
    strcpy(topic, &key[i + 1]);
  }
  return 0;
}

static SMqRebInfo *mndGetOrCreateRebSub(SHashObj *pHash, const char *key) {
  SMqRebInfo *pRebInfo = taosHashGet(pHash, key, strlen(key) + 1);
  if (pRebInfo == NULL) {
    pRebInfo = tNewSMqRebSubscribe(key);
    if (pRebInfo == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }
    taosHashPut(pHash, key, strlen(key) + 1, pRebInfo, sizeof(SMqRebInfo));
    taosMemoryFree(pRebInfo);
    pRebInfo = taosHashGet(pHash, key, strlen(key) + 1);
  }
  return pRebInfo;
}

static void pushVgDataToHash(SArray *vgs, SHashObj *pHash, int64_t consumerId, char *key) {
  SMqVgEp       *pVgEp = *(SMqVgEp **)taosArrayPop(vgs);
  SMqRebOutputVg outputVg = {consumerId, -1, pVgEp};
  taosHashPut(pHash, &pVgEp->vgId, sizeof(int32_t), &outputVg, sizeof(SMqRebOutputVg));
  mInfo("[rebalance] sub:%s mq rebalance remove vgId:%d from consumer:0x%" PRIx64, key, pVgEp->vgId, consumerId);
}

static void processRemovedConsumers(SMqRebOutputObj *pOutput, SHashObj *pHash, const SMqRebInputObj *pInput) {
  int32_t numOfRemoved = taosArrayGetSize(pInput->pRebInfo->removedConsumers);
  int32_t actualRemoved = 0;
  for (int32_t i = 0; i < numOfRemoved; i++) {
    uint64_t       consumerId = *(uint64_t *)taosArrayGet(pInput->pRebInfo->removedConsumers, i);
    SMqConsumerEp *pConsumerEp = taosHashGet(pOutput->pSub->consumerHash, &consumerId, sizeof(int64_t));
    if (pConsumerEp == NULL) {
      continue;
    }

    int32_t consumerVgNum = taosArrayGetSize(pConsumerEp->vgs);
    for (int32_t j = 0; j < consumerVgNum; j++) {
      pushVgDataToHash(pConsumerEp->vgs, pHash, consumerId, pOutput->pSub->key);
    }

    taosArrayDestroy(pConsumerEp->vgs);
    taosHashRemove(pOutput->pSub->consumerHash, &consumerId, sizeof(int64_t));
    taosArrayPush(pOutput->removedConsumers, &consumerId);
    actualRemoved++;
  }

  if (numOfRemoved != actualRemoved) {
    mError("[rebalance] sub:%s mq rebalance removedNum:%d not matched with actual:%d", pOutput->pSub->key, numOfRemoved,
           actualRemoved);
  } else {
    mInfo("[rebalance] sub:%s removed %d consumers", pOutput->pSub->key, numOfRemoved);
  }
}

static void processNewConsumers(SMqRebOutputObj *pOutput, const SMqRebInputObj *pInput) {
  int32_t numOfNewConsumers = taosArrayGetSize(pInput->pRebInfo->newConsumers);

  for (int32_t i = 0; i < numOfNewConsumers; i++) {
    int64_t consumerId = *(int64_t *)taosArrayGet(pInput->pRebInfo->newConsumers, i);

    SMqConsumerEp newConsumerEp = {0};
    newConsumerEp.consumerId = consumerId;
    newConsumerEp.vgs = taosArrayInit(0, sizeof(void *));

    taosHashPut(pOutput->pSub->consumerHash, &consumerId, sizeof(int64_t), &newConsumerEp, sizeof(SMqConsumerEp));
    taosArrayPush(pOutput->newConsumers, &consumerId);
    mInfo("[rebalance] sub:%s mq rebalance add new consumer:0x%" PRIx64, pOutput->pSub->key, consumerId);
  }
}

static void processUnassignedVgroups(SMqRebOutputObj *pOutput, SHashObj *pHash) {
  int32_t numOfVgroups = taosArrayGetSize(pOutput->pSub->unassignedVgs);
  for (int32_t i = 0; i < numOfVgroups; i++) {
    pushVgDataToHash(pOutput->pSub->unassignedVgs, pHash, -1, pOutput->pSub->key);
  }
}

// static void putNoTransferToOutput(SMqRebOutputObj *pOutput, SMqConsumerEp *pConsumerEp){
//   for(int i = 0; i < taosArrayGetSize(pConsumerEp->vgs); i++){
//     SMqVgEp       *pVgEp = (SMqVgEp *)taosArrayGetP(pConsumerEp->vgs, i);
//     SMqRebOutputVg outputVg = {
//         .oldConsumerId = pConsumerEp->consumerId,
//         .newConsumerId = pConsumerEp->consumerId,
//         .pVgEp = pVgEp,
//     };
//     taosArrayPush(pOutput->rebVgs, &outputVg);
//   }
// }

static void processModifiedConsumers(SMqRebOutputObj *pOutput, SHashObj *pHash, int32_t minVgCnt,
                                     int32_t remainderVgCnt) {
  int32_t cnt = 0;
  void   *pIter = NULL;

  while (1) {
    pIter = taosHashIterate(pOutput->pSub->consumerHash, pIter);
    if (pIter == NULL) {
      break;
    }

    SMqConsumerEp *pConsumerEp = (SMqConsumerEp *)pIter;
    int32_t        consumerVgNum = taosArrayGetSize(pConsumerEp->vgs);

    taosArrayPush(pOutput->modifyConsumers, &pConsumerEp->consumerId);
    if (consumerVgNum > minVgCnt) {
      if (cnt < remainderVgCnt) {
        while (taosArrayGetSize(pConsumerEp->vgs) > minVgCnt + 1) {  // pop until equal minVg + 1
          pushVgDataToHash(pConsumerEp->vgs, pHash, pConsumerEp->consumerId, pOutput->pSub->key);
        }
        cnt++;
      } else {
        while (taosArrayGetSize(pConsumerEp->vgs) > minVgCnt) {
          pushVgDataToHash(pConsumerEp->vgs, pHash, pConsumerEp->consumerId, pOutput->pSub->key);
        }
      }
    }
    //    putNoTransferToOutput(pOutput, pConsumerEp);
  }
}

static int32_t processRemoveAddVgs(SMnode *pMnode, SMqRebOutputObj *pOutput) {
  int32_t totalVgNum = 0;
  SVgObj *pVgroup = NULL;
  void   *pIter = NULL;
  SArray *newVgs = taosArrayInit(0, POINTER_BYTES);
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
    SMqVgEp *pVgEp = taosMemoryMalloc(sizeof(SMqVgEp));
    pVgEp->epSet = mndGetVgroupEpset(pMnode, pVgroup);
    pVgEp->vgId = pVgroup->vgId;
    taosArrayPush(newVgs, &pVgEp);
    sdbRelease(pMnode->pSdb, pVgroup);
  }

  pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pOutput->pSub->consumerHash, pIter);
    if (pIter == NULL) break;
    SMqConsumerEp *pConsumerEp = (SMqConsumerEp *)pIter;

    int32_t j = 0;
    while (j < taosArrayGetSize(pConsumerEp->vgs)) {
      SMqVgEp *pVgEp = taosArrayGetP(pConsumerEp->vgs, j);
      bool     find = false;
      for (int32_t k = 0; k < taosArrayGetSize(newVgs); k++) {
        SMqVgEp *pnewVgEp = taosArrayGetP(newVgs, k);
        if (pVgEp->vgId == pnewVgEp->vgId) {
          tDeleteSMqVgEp(pnewVgEp);
          taosArrayRemove(newVgs, k);
          find = true;
          break;
        }
      }
      if (!find) {
        mInfo("[rebalance] processRemoveAddVgs old vgId:%d", pVgEp->vgId);
        tDeleteSMqVgEp(pVgEp);
        taosArrayRemove(pConsumerEp->vgs, j);
        continue;
      }
      j++;
    }
  }

  if (taosArrayGetSize(pOutput->pSub->unassignedVgs) == 0 && taosArrayGetSize(newVgs) != 0) {
    taosArrayAddAll(pOutput->pSub->unassignedVgs, newVgs);
    mInfo("[rebalance] processRemoveAddVgs add new vg num:%d", (int)taosArrayGetSize(newVgs));
    taosArrayDestroy(newVgs);
  } else {
    taosArrayDestroyP(newVgs, (FDelete)tDeleteSMqVgEp);
  }
  return totalVgNum;
}

static void processSubOffsetRows(SMnode *pMnode, const SMqRebInputObj *pInput, SMqRebOutputObj *pOutput) {
  SMqSubscribeObj *pSub = mndAcquireSubscribeByKey(pMnode, pInput->pRebInfo->key);  // put all offset rows
  if (pSub == NULL) {
    return;
  }
  taosRLockLatch(&pSub->lock);
  if (pOutput->pSub->offsetRows == NULL) {
    pOutput->pSub->offsetRows = taosArrayInit(4, sizeof(OffsetRows));
  }
  void *pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pSub->consumerHash, pIter);
    if (pIter == NULL) break;
    SMqConsumerEp *pConsumerEp = (SMqConsumerEp *)pIter;
    SMqConsumerEp *pConsumerEpNew = taosHashGet(pOutput->pSub->consumerHash, &pConsumerEp->consumerId, sizeof(int64_t));

    for (int j = 0; j < taosArrayGetSize(pConsumerEp->offsetRows); j++) {
      OffsetRows *d1 = taosArrayGet(pConsumerEp->offsetRows, j);
      bool        jump = false;
      for (int i = 0; pConsumerEpNew && i < taosArrayGetSize(pConsumerEpNew->vgs); i++) {
        SMqVgEp *pVgEp = taosArrayGetP(pConsumerEpNew->vgs, i);
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
        taosArrayPush(pOutput->pSub->offsetRows, d1);
      }
    }
  }
  taosRUnLockLatch(&pSub->lock);
  mndReleaseSubscribe(pMnode, pSub);
}

static void printRebalanceLog(SMqRebOutputObj *pOutput) {
  mInfo("sub:%s mq rebalance calculation completed, re-balanced vg", pOutput->pSub->key);
  for (int32_t i = 0; i < taosArrayGetSize(pOutput->rebVgs); i++) {
    SMqRebOutputVg *pOutputRebVg = taosArrayGet(pOutput->rebVgs, i);
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

static void assignVgroups(SMqRebOutputObj *pOutput, SHashObj *pHash, int32_t minVgCnt) {
  SMqRebOutputVg *pRebVg = NULL;
  void           *pAssignIter = NULL;
  void           *pIter = NULL;

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
      taosArrayPush(pConsumerEp->vgs, &pRebVg->pVgEp);
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
      taosArrayPush(pConsumerEp->vgs, &pRebVg->pVgEp);
      mInfo("[rebalance] mq rebalance: add vgId:%d to consumer:0x%" PRIx64 " for average + 1", pRebVg->pVgEp->vgId,
            pConsumerEp->consumerId);
    }
  }

  taosHashCancelIterate(pOutput->pSub->consumerHash, pIter);
  if (pAssignIter != NULL) {
    mError("[rebalance]sub:%s assign iter is not NULL, never should reach here", pOutput->pSub->key);
  }
  while (1) {
    pAssignIter = taosHashIterate(pHash, pAssignIter);
    if (pAssignIter == NULL) {
      break;
    }

    SMqRebOutputVg *pRebOutput = (SMqRebOutputVg *)pAssignIter;
    taosArrayPush(pOutput->rebVgs, pRebOutput);
    if (taosHashGetSize(pOutput->pSub->consumerHash) == 0) {            // if all consumer is removed
      taosArrayPush(pOutput->pSub->unassignedVgs, &pRebOutput->pVgEp);  // put all vg into unassigned
    }
  }
}

static void mndDoRebalance(SMnode *pMnode, const SMqRebInputObj *pInput, SMqRebOutputObj *pOutput) {
  int32_t     totalVgNum = processRemoveAddVgs(pMnode, pOutput);
  const char *pSubKey = pOutput->pSub->key;
  int32_t     minVgCnt = 0;
  int32_t     remainderVgCnt = 0;

  SHashObj *pHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);

  processRemovedConsumers(pOutput, pHash, pInput);
  processUnassignedVgroups(pOutput, pHash);
  calcVgroupsCnt(pInput, totalVgNum, pSubKey, &minVgCnt, &remainderVgCnt);
  processModifiedConsumers(pOutput, pHash, minVgCnt, remainderVgCnt);
  processNewConsumers(pOutput, pInput);
  assignVgroups(pOutput, pHash, minVgCnt);
  processSubOffsetRows(pMnode, pInput, pOutput);
  printRebalanceLog(pOutput);
  taosHashCleanup(pHash);
}

static int32_t presistConsumerByType(STrans *pTrans, SArray *consumers, int8_t type, char *cgroup, char *topic) {
  int32_t         code = 0;
  SMqConsumerObj *pConsumerNew = NULL;
  int32_t         consumerNum = taosArrayGetSize(consumers);
  for (int32_t i = 0; i < consumerNum; i++) {
    int64_t consumerId = *(int64_t *)taosArrayGet(consumers, i);
    pConsumerNew = tNewSMqConsumerObj(consumerId, cgroup, type, topic, NULL);
    if (pConsumerNew == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto END;
    }

    code = mndSetConsumerCommitLogs(pTrans, pConsumerNew);
    if (code != 0) {
      goto END;
    }

    tDeleteSMqConsumerObj(pConsumerNew);
  }
  pConsumerNew = NULL;

END:
  tDeleteSMqConsumerObj(pConsumerNew);
  return code;
}

static int32_t mndPresistConsumer(STrans *pTrans, const SMqRebOutputObj *pOutput, char *cgroup, char *topic) {
  int32_t code = presistConsumerByType(pTrans, pOutput->modifyConsumers, CONSUMER_UPDATE_REB, cgroup, NULL);
  if (code != 0) {
    return code;
  }

  code = presistConsumerByType(pTrans, pOutput->newConsumers, CONSUMER_ADD_REB, cgroup, topic);
  if (code != 0) {
    return code;
  }

  return presistConsumerByType(pTrans, pOutput->removedConsumers, CONSUMER_REMOVE_REB, cgroup, topic);
}

static int32_t mndPersistRebResult(SMnode *pMnode, SRpcMsg *pMsg, const SMqRebOutputObj *pOutput) {
  struct SSubplan *pPlan = NULL;
  int32_t          code = 0;
  STrans          *pTrans = NULL;

  if (strcmp(pOutput->pSub->qmsg, "") != 0) {
    code = qStringToSubplan(pOutput->pSub->qmsg, &pPlan);
    if (code != 0) {
      terrno = code;
      goto END;
    }
  }

  char topic[TSDB_TOPIC_FNAME_LEN] = {0};
  char cgroup[TSDB_CGROUP_LEN] = {0};
  mndSplitSubscribeKey(pOutput->pSub->key, topic, cgroup, true);

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_DB_INSIDE, pMsg, "tmq-reb");
  if (pTrans == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto END;
  }

  mndTransSetDbName(pTrans, pOutput->pSub->dbName, cgroup);
  code = mndTransCheckConflict(pMnode, pTrans);
  if (code != 0) {
    goto END;
  }

  // 1. redo action: action to all vg
  const SArray *rebVgs = pOutput->rebVgs;
  int32_t       vgNum = taosArrayGetSize(rebVgs);
  for (int32_t i = 0; i < vgNum; i++) {
    SMqRebOutputVg *pRebVg = taosArrayGet(rebVgs, i);
    code = mndPersistSubChangeVgReq(pMnode, pTrans, pOutput->pSub, pRebVg, pPlan);
    if (code != 0) {
      goto END;
    }
  }

  // 2. commit log: subscribe and vg assignment
  code = mndSetSubCommitLogs(pTrans, pOutput->pSub);
  if (code != 0) {
    goto END;
  }

  // 3. commit log: consumer to update status and epoch
  code = mndPresistConsumer(pTrans, pOutput, cgroup, topic);
  if (code != 0) {
    goto END;
  }

  // 4. set cb
  mndTransSetCb(pTrans, TRANS_START_FUNC_MQ_REB, TRANS_STOP_FUNC_MQ_REB, NULL, 0);

  // 5. execution
  code = mndTransPrepare(pMnode, pTrans);

END:
  nodesDestroyNode((SNode *)pPlan);
  mndTransDrop(pTrans);
  return code;
}

static void freeRebalanceItem(void *param) {
  SMqRebInfo *pInfo = param;
  taosArrayDestroy(pInfo->newConsumers);
  taosArrayDestroy(pInfo->removedConsumers);
}

// type = 0 remove  type = 1 add
static void buildRebInfo(SHashObj *rebSubHash, SArray *topicList, int8_t type, char *group, int64_t consumerId) {
  int32_t topicNum = taosArrayGetSize(topicList);
  for (int32_t i = 0; i < topicNum; i++) {
    char  key[TSDB_SUBSCRIBE_KEY_LEN];
    char *removedTopic = taosArrayGetP(topicList, i);
    mndMakeSubscribeKey(key, group, removedTopic);
    SMqRebInfo *pRebSub = mndGetOrCreateRebSub(rebSubHash, key);
    if (type == 0)
      taosArrayPush(pRebSub->removedConsumers, &consumerId);
    else if (type == 1)
      taosArrayPush(pRebSub->newConsumers, &consumerId);
  }
}

static void checkForVgroupSplit(SMnode *pMnode, SMqConsumerObj *pConsumer, SHashObj *rebSubHash) {
  int32_t newTopicNum = taosArrayGetSize(pConsumer->currentTopics);
  for (int32_t i = 0; i < newTopicNum; i++) {
    char            *topic = taosArrayGetP(pConsumer->currentTopics, i);
    SMqSubscribeObj *pSub = mndAcquireSubscribe(pMnode, pConsumer->cgroup, topic);
    if (pSub == NULL) {
      continue;
    }
    taosRLockLatch(&pSub->lock);

    // iterate all vg assigned to the consumer of that topic
    SMqConsumerEp *pConsumerEp = taosHashGet(pSub->consumerHash, &pConsumer->consumerId, sizeof(int64_t));
    int32_t        vgNum = taosArrayGetSize(pConsumerEp->vgs);

    for (int32_t j = 0; j < vgNum; j++) {
      SMqVgEp *pVgEp = taosArrayGetP(pConsumerEp->vgs, j);
      SVgObj  *pVgroup = mndAcquireVgroup(pMnode, pVgEp->vgId);
      if (!pVgroup) {
        char key[TSDB_SUBSCRIBE_KEY_LEN];
        mndMakeSubscribeKey(key, pConsumer->cgroup, topic);
        mndGetOrCreateRebSub(rebSubHash, key);
        mInfo("vnode splitted, vgId:%d rebalance will be triggered", pVgEp->vgId);
      }
      mndReleaseVgroup(pMnode, pVgroup);
    }
    taosRUnLockLatch(&pSub->lock);
    mndReleaseSubscribe(pMnode, pSub);
  }
}

static void mndCheckConsumer(SRpcMsg *pMsg, SHashObj *rebSubHash) {
  SMnode         *pMnode = pMsg->info.node;
  SSdb           *pSdb = pMnode->pSdb;
  SMqConsumerObj *pConsumer = NULL;
  void           *pIter = NULL;

  // iterate all consumers, find all modification
  while (1) {
    pIter = sdbFetch(pSdb, SDB_CONSUMER, pIter, (void **)&pConsumer);
    if (pIter == NULL) {
      break;
    }

    int32_t hbStatus = atomic_add_fetch_32(&pConsumer->hbStatus, 1);
    int32_t status = atomic_load_32(&pConsumer->status);

    mDebug("[rebalance] check for consumer:0x%" PRIx64 " status:%d(%s), sub-time:%" PRId64 ", createTime:%" PRId64
           ", hbstatus:%d",
           pConsumer->consumerId, status, mndConsumerStatusName(status), pConsumer->subscribeTime,
           pConsumer->createTime, hbStatus);

    if (status == MQ_CONSUMER_STATUS_READY) {
      if (taosArrayGetSize(pConsumer->assignedTopics) == 0) {  // unsubscribe or close
        mndSendConsumerMsg(pMnode, pConsumer->consumerId, TDMT_MND_TMQ_LOST_CONSUMER_CLEAR, &pMsg->info);
      } else if (hbStatus > MND_CONSUMER_LOST_HB_CNT) {
        taosRLockLatch(&pConsumer->lock);
        buildRebInfo(rebSubHash, pConsumer->currentTopics, 0, pConsumer->cgroup, pConsumer->consumerId);
        taosRUnLockLatch(&pConsumer->lock);
      } else {
        checkForVgroupSplit(pMnode, pConsumer, rebSubHash);
      }
    } else if (status == MQ_CONSUMER_STATUS_LOST) {
      if (hbStatus > MND_CONSUMER_LOST_CLEAR_THRESHOLD) {  // clear consumer if lost a day
        mndSendConsumerMsg(pMnode, pConsumer->consumerId, TDMT_MND_TMQ_LOST_CONSUMER_CLEAR, &pMsg->info);
      }
    } else {
      taosRLockLatch(&pConsumer->lock);
      buildRebInfo(rebSubHash, pConsumer->rebNewTopics, 1, pConsumer->cgroup, pConsumer->consumerId);
      buildRebInfo(rebSubHash, pConsumer->rebRemovedTopics, 0, pConsumer->cgroup, pConsumer->consumerId);
      taosRUnLockLatch(&pConsumer->lock);
    }

    mndReleaseConsumer(pMnode, pConsumer);
  }
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
  taosArrayDestroy(rebOutput->newConsumers);
  taosArrayDestroy(rebOutput->modifyConsumers);
  taosArrayDestroy(rebOutput->removedConsumers);
  taosArrayDestroy(rebOutput->rebVgs);
  tDeleteSubscribeObj(rebOutput->pSub);
  taosMemoryFree(rebOutput->pSub);
}

static int32_t initRebOutput(SMqRebOutputObj *rebOutput) {
  rebOutput->newConsumers = taosArrayInit(0, sizeof(int64_t));
  rebOutput->removedConsumers = taosArrayInit(0, sizeof(int64_t));
  rebOutput->modifyConsumers = taosArrayInit(0, sizeof(int64_t));
  rebOutput->rebVgs = taosArrayInit(0, sizeof(SMqRebOutputVg));

  if (rebOutput->newConsumers == NULL || rebOutput->removedConsumers == NULL || rebOutput->modifyConsumers == NULL ||
      rebOutput->rebVgs == NULL) {
    clearRebOutput(rebOutput);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return 0;
}

// This function only works when there are dirty consumers
static void checkConsumer(SMnode *pMnode, SMqSubscribeObj *pSub) {
  void *pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pSub->consumerHash, pIter);
    if (pIter == NULL) {
      break;
    }

    SMqConsumerEp  *pConsumerEp = (SMqConsumerEp *)pIter;
    SMqConsumerObj *pConsumer = mndAcquireConsumer(pMnode, pConsumerEp->consumerId);
    if (pConsumer != NULL) {
      mndReleaseConsumer(pMnode, pConsumer);
      continue;
    }
    mError("consumer:0x%" PRIx64 " not exists in sdb for exception", pConsumerEp->consumerId);
    taosArrayAddAll(pSub->unassignedVgs, pConsumerEp->vgs);

    taosArrayDestroy(pConsumerEp->vgs);
    taosHashRemove(pSub->consumerHash, &pConsumerEp->consumerId, sizeof(int64_t));
  }
}

static int32_t buildRebOutput(SMnode *pMnode, SMqRebInputObj *rebInput, SMqRebOutputObj *rebOutput){
  const char *key = rebInput->pRebInfo->key;
  SMqSubscribeObj *pSub = mndAcquireSubscribeByKey(pMnode, key);

  if (pSub == NULL) {
    // split sub key and extract topic
    char topic[TSDB_TOPIC_FNAME_LEN];
    char cgroup[TSDB_CGROUP_LEN];
    mndSplitSubscribeKey(key, topic, cgroup, true);

    SMqTopicObj *pTopic = mndAcquireTopic(pMnode, topic);
    if (pTopic == NULL) {
      mError("[rebalance] mq rebalance %s ignored since topic %s doesn't exist", key, topic);
      return -1;
    }

    taosRLockLatch(&pTopic->lock);

    rebInput->oldConsumerNum = 0;
    rebOutput->pSub = mndCreateSubscription(pMnode, pTopic, key);

    if (rebOutput->pSub == NULL) {
      mError("[rebalance] mq rebalance %s failed create sub since %s, ignore", key, terrstr());
      taosRUnLockLatch(&pTopic->lock);
      mndReleaseTopic(pMnode, pTopic);
      return -1;
    }

    memcpy(rebOutput->pSub->dbName, pTopic->db, TSDB_DB_FNAME_LEN);
    taosRUnLockLatch(&pTopic->lock);
    mndReleaseTopic(pMnode, pTopic);

    mInfo("[rebalance] sub topic:%s has no consumers sub yet", key);
  } else {
    taosRLockLatch(&pSub->lock);
    rebOutput->pSub = tCloneSubscribeObj(pSub);
    checkConsumer(pMnode, rebOutput->pSub);
    rebInput->oldConsumerNum = taosHashGetSize(rebOutput->pSub->consumerHash);
    taosRUnLockLatch(&pSub->lock);

    mInfo("[rebalance] sub topic:%s has %d consumers sub till now", key, rebInput->oldConsumerNum);
    mndReleaseSubscribe(pMnode, pSub);
  }
  return 0;
}

static int32_t mndProcessRebalanceReq(SRpcMsg *pMsg) {
  int     code = 0;
  void   *pIter = NULL;
  SMnode *pMnode = pMsg->info.node;
  mDebug("[rebalance] start to process mq timer")

      if (!mndRebTryStart()) {
    mInfo("[rebalance] mq rebalance already in progress, do nothing") return code;
  }

  SHashObj *rebSubHash = taosHashInit(64, MurmurHash3_32, true, HASH_NO_LOCK);
  if (rebSubHash == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto END;
  }
  taosHashSetFreeFp(rebSubHash, freeRebalanceItem);

  mndCheckConsumer(pMsg, rebSubHash);
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
    code = initRebOutput(&rebOutput);
    if (code != 0) {
      goto END;
    }

    rebInput.pRebInfo = (SMqRebInfo *)pIter;

    if (buildRebOutput(pMnode, &rebInput, &rebOutput) != 0) {
      continue;
    }

    mndDoRebalance(pMnode, &rebInput, &rebOutput);

    if (mndPersistRebResult(pMnode, pMsg, &rebOutput) != 0) {
      mError("mq rebalance persist output error, possibly vnode splitted or dropped,msg:%s", terrstr())
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

  return code;
}

static int32_t sendDeleteSubToVnode(SMnode *pMnode, SMqSubscribeObj *pSub, STrans *pTrans) {
  void   *pIter = NULL;
  SVgObj *pVgObj = NULL;
  int32_t ret = 0;
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
    if (pReq == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      ret = -1;
      goto END;
    }
    pReq->head.vgId = htonl(pVgObj->vgId);
    pReq->vgId = pVgObj->vgId;
    pReq->consumerId = -1;
    memcpy(pReq->subKey, pSub->key, TSDB_SUBSCRIBE_KEY_LEN);

    STransAction action = {0};
    action.epSet = mndGetVgroupEpset(pMnode, pVgObj);
    ;
    action.pCont = pReq;
    action.contLen = sizeof(SMqVDeleteReq);
    action.msgType = TDMT_VND_TMQ_DELETE_SUB;
    action.acceptableCode = TSDB_CODE_MND_VGROUP_NOT_EXIST;

    sdbRelease(pMnode->pSdb, pVgObj);
    if (mndTransAppendRedoAction(pTrans, &action) != 0) {
      ret = -1;
      goto END;
    }
  }
END:
  sdbRelease(pMnode->pSdb, pVgObj);
  sdbCancelFetch(pMnode->pSdb, pIter);
  return ret;
}

static int32_t mndDropConsumerByGroup(SMnode *pMnode, STrans *pTrans, char *cgroup, char *topic) {
  void           *pIter = NULL;
  SMqConsumerObj *pConsumer = NULL;
  int             ret = 0;
  while (1) {
    pIter = sdbFetch(pMnode->pSdb, SDB_CONSUMER, pIter, (void **)&pConsumer);
    if (pIter == NULL) {
      break;
    }

    // drop consumer in lost status, other consumers not in lost status already deleted by rebalance
    if (pConsumer->status != MQ_CONSUMER_STATUS_LOST || strcmp(cgroup, pConsumer->cgroup) != 0) {
      sdbRelease(pMnode->pSdb, pConsumer);
      continue;
    }
    int32_t sz = taosArrayGetSize(pConsumer->assignedTopics);
    for (int32_t i = 0; i < sz; i++) {
      char *name = taosArrayGetP(pConsumer->assignedTopics, i);
      if (strcmp(topic, name) == 0) {
        int32_t code = mndSetConsumerDropLogs(pTrans, pConsumer);
        if (code != 0) {
          ret = code;
          goto END;
        }
      }
    }

    sdbRelease(pMnode->pSdb, pConsumer);
  }

END:
  sdbRelease(pMnode->pSdb, pConsumer);
  sdbCancelFetch(pMnode->pSdb, pIter);
  return ret;
}

static int32_t mndProcessDropCgroupReq(SRpcMsg *pMsg) {
  SMnode         *pMnode = pMsg->info.node;
  SMDropCgroupReq dropReq = {0};
  STrans         *pTrans = NULL;
  int32_t         code = TSDB_CODE_ACTION_IN_PROGRESS;

  if (tDeserializeSMDropCgroupReq(pMsg->pCont, pMsg->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  SMqSubscribeObj *pSub = mndAcquireSubscribe(pMnode, dropReq.cgroup, dropReq.topic);
  if (pSub == NULL) {
    if (dropReq.igNotExists) {
      mInfo("cgroup:%s on topic:%s, not exist, ignore not exist is set", dropReq.cgroup, dropReq.topic);
      return 0;
    } else {
      terrno = TSDB_CODE_MND_SUBSCRIBE_NOT_EXIST;
      mError("topic:%s, cgroup:%s, failed to drop since %s", dropReq.topic, dropReq.cgroup, terrstr());
      return -1;
    }
  }

  taosWLockLatch(&pSub->lock);
  if (taosHashGetSize(pSub->consumerHash) != 0) {
    terrno = TSDB_CODE_MND_CGROUP_USED;
    mError("cgroup:%s on topic:%s, failed to drop since %s", dropReq.cgroup, dropReq.topic, terrstr());
    code = -1;
    goto end;
  }

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_DB_INSIDE, pMsg, "drop-cgroup");
  if (pTrans == NULL) {
    mError("cgroup: %s on topic:%s, failed to drop since %s", dropReq.cgroup, dropReq.topic, terrstr());
    code = -1;
    goto end;
  }

  mInfo("trans:%d, used to drop cgroup:%s on topic %s", pTrans->id, dropReq.cgroup, dropReq.topic);
  mndTransSetDbName(pTrans, pSub->dbName, dropReq.cgroup);
  code = mndTransCheckConflict(pMnode, pTrans);
  if (code != 0) {
    goto end;
  }

  code = mndDropConsumerByGroup(pMnode, pTrans, dropReq.cgroup, dropReq.topic);
  if (code != 0) {
    goto end;
  }

  code = sendDeleteSubToVnode(pMnode, pSub, pTrans);
  if (code != 0) {
    goto end;
  }

  code = mndSetDropSubCommitLogs(pMnode, pTrans, pSub);
  if (code != 0) {
    mError("cgroup %s on topic:%s, failed to drop since %s", dropReq.cgroup, dropReq.topic, terrstr());
    goto end;
  }

  code = mndTransPrepare(pMnode, pTrans);
  if (code != 0) {
    goto end;
  }

end:
  taosWUnLockLatch(&pSub->lock);
  mndReleaseSubscribe(pMnode, pSub);
  mndTransDrop(pTrans);

  if (code != 0) {
    mError("cgroup %s on topic:%s, failed to drop", dropReq.cgroup, dropReq.topic);
    return code;
  }
  return TSDB_CODE_ACTION_IN_PROGRESS;
}

void mndCleanupSubscribe(SMnode *pMnode) {}

static SSdbRaw *mndSubActionEncode(SMqSubscribeObj *pSub) {
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
  tEncodeSubscribeObj(&abuf, pSub);

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

void mndMakeSubscribeKey(char *key, const char *cgroup, const char *topicName) {
  int32_t tlen = strlen(cgroup);
  memcpy(key, cgroup, tlen);
  key[tlen] = TMQ_SEPARATOR;
  strcpy(key + tlen + 1, topicName);
}

SMqSubscribeObj *mndAcquireSubscribe(SMnode *pMnode, const char *cgroup, const char *topicName) {
  SSdb *pSdb = pMnode->pSdb;
  char  key[TSDB_SUBSCRIBE_KEY_LEN];
  mndMakeSubscribeKey(key, cgroup, topicName);
  SMqSubscribeObj *pSub = sdbAcquire(pSdb, SDB_SUBSCRIBE, key);
  if (pSub == NULL) {
    terrno = TSDB_CODE_MND_SUBSCRIBE_NOT_EXIST;
  }
  return pSub;
}

SMqSubscribeObj *mndAcquireSubscribeByKey(SMnode *pMnode, const char *key) {
  SSdb            *pSdb = pMnode->pSdb;
  SMqSubscribeObj *pSub = sdbAcquire(pSdb, SDB_SUBSCRIBE, key);
  if (pSub == NULL) {
    terrno = TSDB_CODE_MND_SUBSCRIBE_NOT_EXIST;
  }
  return pSub;
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

static int32_t mndSetDropSubRedoLogs(SMnode *pMnode, STrans *pTrans, SMqSubscribeObj *pSub) {
  SSdbRaw *pRedoRaw = mndSubActionEncode(pSub);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPED) != 0) return -1;
  return 0;
}

int32_t mndSetDropSubCommitLogs(SMnode *pMnode, STrans *pTrans, SMqSubscribeObj *pSub) {
  SSdbRaw *pCommitRaw = mndSubActionEncode(pSub);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED) != 0) return -1;
  return 0;
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

    code = sendDeleteSubToVnode(pMnode, pSub, pTrans);
    if (code != 0) {
      goto END;
    }

    code = mndSetDropSubCommitLogs(pMnode, pTrans, pSub);
    if (code != 0) {
      goto END;
    }
  }

END:
  sdbRelease(pSdb, pSub);
  sdbCancelFetch(pSdb, pIter);

  return code;
}

static int32_t buildResult(SSDataBlock *pBlock, int32_t* numOfRows, int64_t consumerId, const char* user, const char* fqdn,
                           const char *topic, const char* cgroup, SArray* vgs, SArray *offsetRows){
  int32_t sz = taosArrayGetSize(vgs);
  for (int32_t j = 0; j < sz; j++) {
    SMqVgEp *pVgEp = taosArrayGetP(vgs, j);

    SColumnInfoData *pColInfo;
    int32_t          cols = 0;

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, *numOfRows, (const char *)topic, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, *numOfRows, (const char *)cgroup, false);

    // vg id
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, *numOfRows, (const char *)&pVgEp->vgId, false);

    // consumer id
    char consumerIdHex[32] = {0};
    sprintf(varDataVal(consumerIdHex), "0x%" PRIx64, consumerId);
    varDataSetLen(consumerIdHex, strlen(varDataVal(consumerIdHex)));

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, *numOfRows, (const char *)consumerIdHex, consumerId == -1);

    char userStr[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
    if (user) STR_TO_VARSTR(userStr, user);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, *numOfRows, userStr, user == NULL);

    char fqdnStr[TSDB_FQDN_LEN + VARSTR_HEADER_SIZE] = {0};
    if (fqdn) STR_TO_VARSTR(fqdnStr, fqdn);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, *numOfRows, fqdnStr, fqdn == NULL);

    mInfo("mnd show subscriptions: topic %s, consumer:0x%" PRIx64 " cgroup %s vgid %d", varDataVal(topic), consumerId,
          varDataVal(cgroup), pVgEp->vgId);

    // offset
    OffsetRows *data = NULL;
    for (int i = 0; i < taosArrayGetSize(offsetRows); i++) {
      OffsetRows *tmp = taosArrayGet(offsetRows, i);
      if (tmp->vgId != pVgEp->vgId) {
        mError("mnd show subscriptions: do not find vgId:%d, %d in offsetRows", tmp->vgId, pVgEp->vgId);
        continue;
      }
      data = tmp;
    }
    if (data) {
      // vg id
      char buf[TSDB_OFFSET_LEN * 2 + VARSTR_HEADER_SIZE] = {0};
      tFormatOffset(varDataVal(buf), TSDB_OFFSET_LEN, &data->offset);
      sprintf(varDataVal(buf) + strlen(varDataVal(buf)), "/%" PRId64, data->ever);
      varDataSetLen(buf, strlen(varDataVal(buf)));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, *numOfRows, (const char *)buf, false);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, *numOfRows, (const char *)&data->rows, false);
    } else {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetNULL(pColInfo, *numOfRows);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetNULL(pColInfo, *numOfRows);
      mError("mnd show subscriptions: do not find vgId:%d in offsetRows", pVgEp->vgId);
    }
    (*numOfRows)++;
  }
  return 0;
}

int32_t mndRetrieveSubscribe(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rowsCapacity) {
  SMnode          *pMnode = pReq->info.node;
  SSdb            *pSdb = pMnode->pSdb;
  int32_t          numOfRows = 0;
  SMqSubscribeObj *pSub = NULL;

  mInfo("mnd show subscriptions begin");

  while (numOfRows < rowsCapacity) {
    pShow->pIter = sdbFetch(pSdb, SDB_SUBSCRIBE, pShow->pIter, (void **)&pSub);
    if (pShow->pIter == NULL) {
      break;
    }

    taosRLockLatch(&pSub->lock);

    if (numOfRows + pSub->vgNum > rowsCapacity) {
      blockDataEnsureCapacity(pBlock, numOfRows + pSub->vgNum);
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

      buildResult(pBlock, &numOfRows, pConsumerEp->consumerId, user, fqdn, topic, cgroup, pConsumerEp->vgs, pConsumerEp->offsetRows);
    }

    buildResult(pBlock, &numOfRows, -1, NULL, NULL, topic, cgroup, pSub->unassignedVgs, pSub->offsetRows);

    pBlock->info.rows = numOfRows;

    taosRUnLockLatch(&pSub->lock);
    sdbRelease(pSdb, pSub);
  }

  mInfo("mnd end show subscriptions");

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

void mndCancelGetNextSubscribe(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}
