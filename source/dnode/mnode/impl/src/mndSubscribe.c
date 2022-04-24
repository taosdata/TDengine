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
#include "mndDb.h"
#include "mndDnode.h"
#include "mndMnode.h"
#include "mndOffset.h"
#include "mndScheduler.h"
#include "mndShow.h"
#include "mndStb.h"
#include "mndTopic.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"
#include "tcompare.h"
#include "tname.h"

#define MND_SUBSCRIBE_VER_NUMBER   1
#define MND_SUBSCRIBE_RESERVE_SIZE 64

#define MND_SUBSCRIBE_REBALANCE_CNT 3

enum {
  MQ_SUBSCRIBE_STATUS__ACTIVE = 1,
  MQ_SUBSCRIBE_STATUS__DELETED,
};

static SSdbRaw *mndSubActionEncode(SMqSubscribeObj *);
static SSdbRow *mndSubActionDecode(SSdbRaw *pRaw);
static int32_t  mndSubActionInsert(SSdb *pSdb, SMqSubscribeObj *);
static int32_t  mndSubActionDelete(SSdb *pSdb, SMqSubscribeObj *);
static int32_t  mndSubActionUpdate(SSdb *pSdb, SMqSubscribeObj *pOldSub, SMqSubscribeObj *pNewSub);

static int32_t mndProcessRebalanceReq(SNodeMsg *pMsg);
static int32_t mndProcessSubscribeInternalRsp(SNodeMsg *pMsg);

static int32_t mndSetSubRedoLogs(SMnode *pMnode, STrans *pTrans, SMqSubscribeObj *pSub) {
  SSdbRaw *pRedoRaw = mndSubActionEncode(pSub);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_READY) != 0) return -1;
  return 0;
}

static int32_t mndSetSubCommitLogs(SMnode *pMnode, STrans *pTrans, SMqSubscribeObj *pSub) {
  SSdbRaw *pCommitRaw = mndSubActionEncode(pSub);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY) != 0) return -1;
  return 0;
}

int32_t mndInitSubscribe(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_SUBSCRIBE,
                     .keyType = SDB_KEY_BINARY,
                     .encodeFp = (SdbEncodeFp)mndSubActionEncode,
                     .decodeFp = (SdbDecodeFp)mndSubActionDecode,
                     .insertFp = (SdbInsertFp)mndSubActionInsert,
                     .updateFp = (SdbUpdateFp)mndSubActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndSubActionDelete};

  mndSetMsgHandle(pMnode, TDMT_VND_MQ_VG_CHANGE_RSP, mndProcessSubscribeInternalRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_MQ_DO_REBALANCE, mndProcessRebalanceReq);
  return sdbSetTable(pMnode->pSdb, table);
}

static SMqSubscribeObj *mndCreateSub(SMnode *pMnode, const SMqTopicObj *pTopic, const char *subKey) {
  SMqSubscribeObj *pSub = tNewSubscribeObj(subKey);
  if (pSub == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  pSub->subType = pTopic->subType;
  pSub->withTbName = pTopic->withTbName;
  pSub->withSchema = pTopic->withSchema;
  pSub->withTag = pTopic->withTag;
  pSub->withTagSchema = pTopic->withTagSchema;

  ASSERT(taosHashGetSize(pSub->consumerHash) == 1);

  if (mndSchedInitSubEp(pMnode, pTopic, pSub) < 0) {
    tDeleteSubscribeObj(pSub);
    taosMemoryFree(pSub);
    return NULL;
  }

  ASSERT(taosHashGetSize(pSub->consumerHash) == 1);

  return pSub;
}

static int32_t mndBuildSubChangeReq(void **pBuf, int32_t *pLen, const SMqSubscribeObj *pSub,
                                    const SMqRebOutputVg *pRebVg) {
  SMqRebVgReq req = {0};
  req.oldConsumerId = pRebVg->oldConsumerId;
  req.newConsumerId = pRebVg->newConsumerId;
  req.vgId = pRebVg->pVgEp->vgId;
  req.qmsg = pRebVg->pVgEp->qmsg;
  req.subType = pSub->subType;
  req.withTbName = pSub->withTbName;
  req.withSchema = pSub->withSchema;
  req.withTag = pSub->withTag;
  req.withTagSchema = pSub->withTagSchema;
  strncpy(req.subKey, pSub->key, TSDB_SUBSCRIBE_KEY_LEN);

  int32_t tlen = sizeof(SMsgHead) + tEncodeSMqRebVgReq(NULL, &req);
  void   *buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  SMsgHead *pMsgHead = (SMsgHead *)buf;

  pMsgHead->contLen = htonl(tlen);
  pMsgHead->vgId = htonl(pRebVg->pVgEp->vgId);

  void *abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
  tEncodeSMqRebVgReq(&abuf, &req);
  *pBuf = buf;
  *pLen = tlen;

  return 0;
}

static int32_t mndPersistSubChangeVgReq(SMnode *pMnode, STrans *pTrans, const SMqSubscribeObj *pSub,
                                        const SMqRebOutputVg *pRebVg) {
  ASSERT(pRebVg->oldConsumerId != pRebVg->newConsumerId);

  void   *buf;
  int32_t tlen;
  if (mndBuildSubChangeReq(&buf, &tlen, pSub, pRebVg) < 0) {
    return -1;
  }

  int32_t vgId = pRebVg->pVgEp->vgId;
  SVgObj *pVgObj = mndAcquireVgroup(pMnode, vgId);

  STransAction action = {0};
  action.epSet = mndGetVgroupEpset(pMnode, pVgObj);
  action.pCont = buf;
  action.contLen = tlen;
  action.msgType = TDMT_VND_MQ_VG_CHANGE;

  mndReleaseVgroup(pMnode, pVgObj);
  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(buf);
    return -1;
  }
  return 0;
}

static int32_t mndSplitSubscribeKey(const char *key, char *topic, char *cgroup) {
  int32_t i = 0;
  while (key[i] != TMQ_SEPARATOR) {
    i++;
  }
  memcpy(cgroup, key, i);
  cgroup[i] = 0;
  strcpy(topic, &key[i + 1]);
  return 0;
}

static SMqRebSubscribe *mndGetOrCreateRebSub(SHashObj *pHash, const char *key) {
  SMqRebSubscribe *pRebSub = taosHashGet(pHash, key, strlen(key) + 1);
  if (pRebSub == NULL) {
    pRebSub = tNewSMqRebSubscribe(key);
    if (pRebSub == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }
    taosHashPut(pHash, key, strlen(key) + 1, pRebSub, sizeof(SMqRebSubscribe));
  }
  return pRebSub;
}

static int32_t mndDoRebalance(SMnode *pMnode, const SMqRebInputObj *pInput, SMqRebOutputObj *pOutput) {
  if (pInput->pTopic != NULL) {
    // create subscribe
    pOutput->pSub = mndCreateSub(pMnode, pInput->pTopic, pInput->pRebInfo->key);
    ASSERT(taosHashGetSize(pOutput->pSub->consumerHash) == 1);
  } else {
    pOutput->pSub = tCloneSubscribeObj(pInput->pOldSub);
  }
  int32_t totalVgNum = pOutput->pSub->vgNum;

  mInfo("mq rebalance subscription: %s, vgNum: %d", pOutput->pSub->key, pOutput->pSub->vgNum);

  // 1. build temporary hash(vgId -> SMqRebOutputVg) to store modified vg
  SHashObj *pHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);

  ASSERT(taosHashGetSize(pOutput->pSub->consumerHash) > 0);
  // 2. check and get actual removed consumers, put their vg into hash
  int32_t removedNum = taosArrayGetSize(pInput->pRebInfo->removedConsumers);
  int32_t actualRemoved = 0;
  for (int32_t i = 0; i < removedNum; i++) {
    int64_t consumerId = *(int64_t *)taosArrayGet(pInput->pRebInfo->removedConsumers, i);
    ASSERT(consumerId > 0);
    SMqConsumerEpInSub *pEpInSub = taosHashGet(pOutput->pSub->consumerHash, &consumerId, sizeof(int64_t));
    ASSERT(pEpInSub);
    if (pEpInSub) {
      ASSERT(consumerId == pEpInSub->consumerId);
      actualRemoved++;
      int32_t consumerVgNum = taosArrayGetSize(pEpInSub->vgs);
      for (int32_t j = 0; j < consumerVgNum; j++) {
        SMqVgEp       *pVgEp = taosArrayGetP(pEpInSub->vgs, j);
        SMqRebOutputVg outputVg = {
            .oldConsumerId = consumerId,
            .newConsumerId = -1,
            .pVgEp = pVgEp,
        };
        taosHashPut(pHash, &pVgEp->vgId, sizeof(int32_t), &outputVg, sizeof(SMqRebOutputVg));
      }
      taosHashRemove(pOutput->pSub->consumerHash, &consumerId, sizeof(int64_t));
      // put into removed
      taosArrayPush(pOutput->removedConsumers, &consumerId);
    }
  }
  ASSERT(removedNum == actualRemoved);
  ASSERT(taosHashGetSize(pOutput->pSub->consumerHash) > 0);

  // if previously no consumer, there are vgs not assigned
  {
    int64_t             unexistKey = -1;
    SMqConsumerEpInSub *pEpInSub = taosHashGet(pOutput->pSub->consumerHash, &unexistKey, sizeof(int64_t));
    ASSERT(pEpInSub);
    int32_t consumerVgNum = taosArrayGetSize(pEpInSub->vgs);
    for (int32_t i = 0; i < consumerVgNum; i++) {
      SMqVgEp       *pVgEp = *(SMqVgEp **)taosArrayPop(pEpInSub->vgs);
      SMqRebOutputVg rebOutput = {
          .oldConsumerId = -1,
          .newConsumerId = -1,
          .pVgEp = pVgEp,
      };
      taosHashPut(pHash, &pVgEp->vgId, sizeof(int32_t), &rebOutput, sizeof(SMqRebOutputVg));
    }
  }

  // 3. calc vg number of each consumer
  int32_t oldSz = 0;
  if (pInput->pOldSub) {
    oldSz = taosHashGetSize(pInput->pOldSub->consumerHash) - 1;
  }
  int32_t afterRebConsumerNum =
      oldSz + taosArrayGetSize(pInput->pRebInfo->newConsumers) - taosArrayGetSize(pInput->pRebInfo->removedConsumers);
  int32_t minVgCnt = 0;
  int32_t imbConsumerNum = 0;
  // calc num
  if (afterRebConsumerNum) {
    minVgCnt = totalVgNum / afterRebConsumerNum;
    imbConsumerNum = totalVgNum % afterRebConsumerNum;
  }

  // 4. first scan: remove consumer more than wanted, put to remove hash
  int32_t imbCnt = 0;
  void   *pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pOutput->pSub->consumerHash, pIter);
    if (pIter == NULL) break;
    SMqConsumerEpInSub *pEpInSub = (SMqConsumerEpInSub *)pIter;
    if (pEpInSub->consumerId == -1) continue;
    ASSERT(pEpInSub->consumerId > 0);
    int32_t consumerVgNum = taosArrayGetSize(pEpInSub->vgs);
    // all old consumers still existing are touched
    // TODO optimize: touch only consumer whose vgs changed
    taosArrayPush(pOutput->touchedConsumers, &pEpInSub->consumerId);
    if (consumerVgNum > minVgCnt) {
      if (imbCnt < imbConsumerNum) {
        if (consumerVgNum == minVgCnt + 1) {
          continue;
        } else {
          // pop until equal minVg + 1
          while (taosArrayGetSize(pEpInSub->vgs) > minVgCnt + 1) {
            SMqVgEp       *pVgEp = *(SMqVgEp **)taosArrayPop(pEpInSub->vgs);
            SMqRebOutputVg outputVg = {
                .oldConsumerId = pEpInSub->consumerId,
                .newConsumerId = -1,
                .pVgEp = pVgEp,
            };
            taosHashPut(pHash, &pVgEp->vgId, sizeof(int32_t), &outputVg, sizeof(SMqRebOutputVg));
          }
          imbCnt++;
        }
      } else {
        // pop until equal minVg
        while (taosArrayGetSize(pEpInSub->vgs) > minVgCnt) {
          SMqVgEp       *pVgEp = *(SMqVgEp **)taosArrayPop(pEpInSub->vgs);
          SMqRebOutputVg outputVg = {
              .oldConsumerId = pEpInSub->consumerId,
              .newConsumerId = -1,
              .pVgEp = pVgEp,
          };
          taosHashPut(pHash, &pVgEp->vgId, sizeof(int32_t), &outputVg, sizeof(SMqRebOutputVg));
        }
      }
    }
  }

  // 5. add new consumer into sub
  {
    int32_t consumerNum = taosArrayGetSize(pInput->pRebInfo->newConsumers);
    for (int32_t i = 0; i < consumerNum; i++) {
      int64_t consumerId = *(int64_t *)taosArrayGet(pInput->pRebInfo->newConsumers, i);
      ASSERT(consumerId > 0);
      SMqConsumerEpInSub newConsumerEp;
      newConsumerEp.consumerId = consumerId;
      newConsumerEp.vgs = taosArrayInit(0, sizeof(void *));
      taosHashPut(pOutput->pSub->consumerHash, &consumerId, sizeof(int64_t), &newConsumerEp,
                  sizeof(SMqConsumerEpInSub));
      /*SMqConsumerEpInSub *pTestNew = taosHashGet(pOutput->pSub->consumerHash, &consumerId, sizeof(int64_t));*/
      /*ASSERT(pTestNew->consumerId == consumerId);*/
      /*ASSERT(pTestNew->vgs == newConsumerEp.vgs);*/
      taosArrayPush(pOutput->newConsumers, &consumerId);
    }
  }

  // 6. second scan: find consumer do not have enough vg, extract from temporary hash and assign to new consumer.
  // All related vg should be put into rebVgs
  SMqRebOutputVg *pRebVg = NULL;
  void           *pRemovedIter = NULL;
  pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pOutput->pSub->consumerHash, pIter);
    if (pIter == NULL) break;
    SMqConsumerEpInSub *pEpInSub = (SMqConsumerEpInSub *)pIter;
    if (pEpInSub->consumerId == -1) continue;
    ASSERT(pEpInSub->consumerId > 0);

    // push until equal minVg
    while (taosArrayGetSize(pEpInSub->vgs) < minVgCnt) {
      // iter hash and find one vg
      pRemovedIter = taosHashIterate(pHash, pRemovedIter);
      ASSERT(pRemovedIter);
      pRebVg = (SMqRebOutputVg *)pRemovedIter;
      // push
      taosArrayPush(pEpInSub->vgs, &pRebVg->pVgEp);
      pRebVg->newConsumerId = pEpInSub->consumerId;
      taosArrayPush(pOutput->rebVgs, pRebVg);
    }
  }

  // 7. handle unassigned vg
  if (taosHashGetSize(pOutput->pSub->consumerHash) != 1) {
    // if has consumer, assign all left vg
    while (1) {
      pRemovedIter = taosHashIterate(pHash, pRemovedIter);
      if (pRemovedIter == NULL) break;
      pIter = taosHashIterate(pOutput->pSub->consumerHash, pIter);
      ASSERT(pIter);
      pRebVg = (SMqRebOutputVg *)pRemovedIter;
      SMqConsumerEpInSub *pEpInSub = (SMqConsumerEpInSub *)pIter;
      if (pEpInSub->consumerId == -1) continue;
      ASSERT(pEpInSub->consumerId > 0);
      taosArrayPush(pEpInSub->vgs, &pRebVg->pVgEp);
      pRebVg->newConsumerId = pEpInSub->consumerId;
      taosArrayPush(pOutput->rebVgs, pRebVg);
    }
  } else {
    // if all consumer is removed, put all vg into unassigned
    int64_t             unexistKey = -1;
    SMqConsumerEpInSub *pEpInSub = taosHashGet(pOutput->pSub->consumerHash, &unexistKey, sizeof(int64_t));
    ASSERT(pEpInSub);
    ASSERT(pEpInSub->consumerId == -1);

    pIter = NULL;
    SMqRebOutputVg *pRebOutput = NULL;
    while (1) {
      pIter = taosHashIterate(pHash, pIter);
      if (pIter == NULL) break;
      pRebOutput = (SMqRebOutputVg *)pIter;
      ASSERT(pRebOutput->newConsumerId == -1);
      taosArrayPush(pEpInSub->vgs, &pRebOutput->pVgEp);
      taosArrayPush(pOutput->rebVgs, pRebOutput);
    }
  }

  // 8. generate logs

  // 9. clear
  taosHashCleanup(pHash);

  return 0;
}

static int32_t mndPersistRebResult(SMnode *pMnode, SNodeMsg *pMsg, const SMqRebOutputObj *pOutput) {
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_TYPE_REBALANCE, &pMsg->rpcMsg);
  if (pTrans == NULL) {
    return -1;
  }
  // make txn:
  // 1. redo action: action to all vg
  const SArray *rebVgs = pOutput->rebVgs;
  int32_t       vgNum = taosArrayGetSize(rebVgs);
  for (int32_t i = 0; i < vgNum; i++) {
    SMqRebOutputVg *pRebVg = taosArrayGet(rebVgs, i);
    if (mndPersistSubChangeVgReq(pMnode, pTrans, pOutput->pSub, pRebVg) < 0) {
      goto REB_FAIL;
    }
  }

  // 2. redo log: subscribe and vg assignment
  // subscribe
  if (mndSetSubRedoLogs(pMnode, pTrans, pOutput->pSub) != 0) {
    goto REB_FAIL;
  }

  // 3. commit log: consumer to update status and epoch
  // 3.1 set touched consumer
  int32_t consumerNum = taosArrayGetSize(pOutput->touchedConsumers);
  for (int32_t i = 0; i < consumerNum; i++) {
    int64_t         consumerId = *(int64_t *)taosArrayGet(pOutput->touchedConsumers, i);
    SMqConsumerObj *pConsumerOld = mndAcquireConsumer(pMnode, consumerId);
    SMqConsumerObj *pConsumerNew = tNewSMqConsumerObj(pConsumerOld->consumerId, pConsumerOld->cgroup);
    pConsumerNew->updateType = CONSUMER_UPDATE__TOUCH;
    mndReleaseConsumer(pMnode, pConsumerOld);
    if (mndSetConsumerCommitLogs(pMnode, pTrans, pConsumerNew) != 0) {
      goto REB_FAIL;
    }
  }
  // 3.2 set new consumer
  consumerNum = taosArrayGetSize(pOutput->newConsumers);
  for (int32_t i = 0; i < consumerNum; i++) {
    int64_t consumerId = *(int64_t *)taosArrayGet(pOutput->newConsumers, i);
    ASSERT(consumerId > 0);
    SMqConsumerObj *pConsumerOld = mndAcquireConsumer(pMnode, consumerId);
    SMqConsumerObj *pConsumerNew = tNewSMqConsumerObj(pConsumerOld->consumerId, pConsumerOld->cgroup);
    pConsumerNew->updateType = CONSUMER_UPDATE__ADD;
    char *topic = taosMemoryCalloc(1, TSDB_TOPIC_FNAME_LEN);
    char  cgroup[TSDB_CGROUP_LEN];
    mndSplitSubscribeKey(pOutput->pSub->key, topic, cgroup);
    taosArrayPush(pConsumerNew->rebNewTopics, &topic);
    mndReleaseConsumer(pMnode, pConsumerOld);
    if (mndSetConsumerCommitLogs(pMnode, pTrans, pConsumerNew) != 0) {
      goto REB_FAIL;
    }
  }

  // 3.3 set removed consumer
  consumerNum = taosArrayGetSize(pOutput->removedConsumers);
  for (int32_t i = 0; i < consumerNum; i++) {
    int64_t consumerId = *(int64_t *)taosArrayGet(pOutput->removedConsumers, i);
    ASSERT(consumerId > 0);
    SMqConsumerObj *pConsumerOld = mndAcquireConsumer(pMnode, consumerId);
    SMqConsumerObj *pConsumerNew = tNewSMqConsumerObj(pConsumerOld->consumerId, pConsumerOld->cgroup);
    pConsumerNew->updateType = CONSUMER_UPDATE__REMOVE;
    char *topic = taosMemoryCalloc(1, TSDB_TOPIC_FNAME_LEN);
    char  cgroup[TSDB_CGROUP_LEN];
    mndSplitSubscribeKey(pOutput->pSub->key, topic, cgroup);
    taosArrayPush(pConsumerNew->rebRemovedTopics, &topic);
    mndReleaseConsumer(pMnode, pConsumerOld);
    if (mndSetConsumerCommitLogs(pMnode, pTrans, pConsumerNew) != 0) {
      goto REB_FAIL;
    }
  }
  // 4. commit log: modification log
  if (mndTransPrepare(pMnode, pTrans) != 0) goto REB_FAIL;

  mndTransDrop(pTrans);
  return 0;

REB_FAIL:
  mndTransDrop(pTrans);
  return -1;
}

static int32_t mndProcessRebalanceReq(SNodeMsg *pMsg) {
  SMnode            *pMnode = pMsg->pNode;
  SMqDoRebalanceMsg *pReq = pMsg->rpcMsg.pCont;
  void              *pIter = NULL;

  mInfo("mq rebalance start");

  while (1) {
    pIter = taosHashIterate(pReq->rebSubHash, pIter);
    if (pIter == NULL) break;
    SMqRebInputObj rebInput = {0};

    SMqRebOutputObj rebOutput = {0};
    rebOutput.newConsumers = taosArrayInit(0, sizeof(void *));
    rebOutput.removedConsumers = taosArrayInit(0, sizeof(void *));
    rebOutput.touchedConsumers = taosArrayInit(0, sizeof(void *));
    rebOutput.rebVgs = taosArrayInit(0, sizeof(SMqRebOutputVg));

    SMqRebSubscribe *pRebSub = (SMqRebSubscribe *)pIter;
    SMqSubscribeObj *pSub = mndAcquireSubscribeByKey(pMnode, pRebSub->key);

    if (pSub == NULL) {
      // split sub key and extract topic
      char topic[TSDB_TOPIC_FNAME_LEN];
      char cgroup[TSDB_CGROUP_LEN];
      mndSplitSubscribeKey(pRebSub->key, topic, cgroup);
      SMqTopicObj *pTopic = mndAcquireTopic(pMnode, topic);
      ASSERT(pTopic);
      taosRLockLatch(&pTopic->lock);
      rebInput.pTopic = pTopic;
    }

    rebInput.pRebInfo = pRebSub;
    rebInput.pOldSub = pSub;

    // TODO replace assert with error check
    ASSERT(mndDoRebalance(pMnode, &rebInput, &rebOutput) == 0);
    // if add more consumer to balanced subscribe,
    // possibly no vg is changed
    /*ASSERT(taosArrayGetSize(rebOutput.rebVgs) != 0);*/
    ASSERT(mndPersistRebResult(pMnode, pMsg, &rebOutput) == 0);

    if (rebInput.pTopic) {
      SMqTopicObj *pTopic = (SMqTopicObj *)rebInput.pTopic;
      taosRUnLockLatch(&pTopic->lock);
      mndReleaseTopic(pMnode, pTopic);
    } else {
      mndReleaseSubscribe(pMnode, pSub);
    }
  }

  // reset flag
  atomic_store_8(pReq->mqInReb, 0);
  mInfo("mq rebalance completed successfully");
  taosHashCleanup(pReq->rebSubHash);

  return 0;
}

void mndCleanupSubscribe(SMnode *pMnode) {}

static SSdbRaw *mndSubActionEncode(SMqSubscribeObj *pSub) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  void   *buf = NULL;
  int32_t tlen = tEncodeSubscribeObj(NULL, pSub);
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
  void *buf = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto SUB_DECODE_OVER;

  if (sver != MND_SUBSCRIBE_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto SUB_DECODE_OVER;
  }

  int32_t  size = sizeof(SMqSubscribeObj);
  SSdbRow *pRow = sdbAllocRow(size);
  if (pRow == NULL) goto SUB_DECODE_OVER;

  SMqSubscribeObj *pSub = sdbGetRowObj(pRow);
  if (pSub == NULL) goto SUB_DECODE_OVER;

  int32_t dataPos = 0;
  int32_t tlen;
  SDB_GET_INT32(pRaw, dataPos, &tlen, SUB_DECODE_OVER);
  buf = taosMemoryMalloc(tlen + 1);
  if (buf == NULL) goto SUB_DECODE_OVER;
  SDB_GET_BINARY(pRaw, dataPos, buf, tlen, SUB_DECODE_OVER);
  SDB_GET_RESERVE(pRaw, dataPos, MND_SUBSCRIBE_RESERVE_SIZE, SUB_DECODE_OVER);

  if (tDecodeSubscribeObj(buf, pSub) == NULL) {
    goto SUB_DECODE_OVER;
  }

  terrno = TSDB_CODE_SUCCESS;

SUB_DECODE_OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("subscribe:%s, failed to decode from raw:%p since %s", pSub->key, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

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

  taosWUnLockLatch(&pOldSub->lock);
  return 0;
}

int32_t mndMakeSubscribeKey(char *key, const char *cgroup, const char *topicName) {
  int32_t tlen = strlen(cgroup);
  memcpy(key, cgroup, tlen);
  key[tlen] = TMQ_SEPARATOR;
  strcpy(key + tlen + 1, topicName);
  return 0;
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

void mndReleaseSubscribe(SMnode *pMnode, SMqSubscribeObj *pSub) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pSub);
}

#if 0
static int32_t mndProcessSubscribeReq(SNodeMsg *pMsg) {
  SMnode         *pMnode = pMsg->pNode;
  char           *msgStr = pMsg->rpcMsg.pCont;
  SCMSubscribeReq subscribe;
  tDeserializeSCMSubscribeReq(msgStr, &subscribe);
  int64_t consumerId = subscribe.consumerId;
  char   *cgroup = subscribe.consumerGroup;

  SArray *newSub = subscribe.topicNames;
  int32_t newTopicNum = subscribe.topicNum;

  taosArraySortString(newSub, taosArrayCompareString);

  SArray *oldSub = NULL;
  int32_t oldTopicNum = 0;
  bool    createConsumer = false;
  // create consumer if not exist
  SMqConsumerObj *pConsumer = mndAcquireConsumer(pMnode, consumerId);
  if (pConsumer == NULL) {
    // create consumer
    pConsumer = mndCreateConsumer(consumerId, cgroup);
    createConsumer = true;
  } else {
    pConsumer->epoch++;
    oldSub = pConsumer->currentTopics;
  }
  pConsumer->currentTopics = newSub;

  if (oldSub != NULL) {
    oldTopicNum = taosArrayGetSize(oldSub);
  }

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_TYPE_SUBSCRIBE, &pMsg->rpcMsg);
  if (pTrans == NULL) {
    // TODO: free memory
    return -1;
  }

  int32_t i = 0, j = 0;
  while (i < newTopicNum || j < oldTopicNum) {
    char *newTopicName = NULL;
    char *oldTopicName = NULL;
    if (i >= newTopicNum) {
      // encode unset topic msg to all vnodes related to that topic
      oldTopicName = taosArrayGetP(oldSub, j);
      j++;
    } else if (j >= oldTopicNum) {
      newTopicName = taosArrayGetP(newSub, i);
      i++;
    } else {
      newTopicName = taosArrayGetP(newSub, i);
      oldTopicName = taosArrayGetP(oldSub, j);

      int32_t comp = compareLenPrefixedStr(newTopicName, oldTopicName);
      if (comp == 0) {
        // do nothing
        oldTopicName = newTopicName = NULL;
        i++;
        j++;
        continue;
      } else if (comp < 0) {
        oldTopicName = NULL;
        i++;
      } else {
        newTopicName = NULL;
        j++;
      }
    }

    if (oldTopicName != NULL) {
      ASSERT(newTopicName == NULL);

      // cancel subscribe of old topic
      SMqSubscribeObj *pSub = mndAcquireSubscribe(pMnode, cgroup, oldTopicName);
      ASSERT(pSub);
      int32_t csz = taosArrayGetSize(pSub->consumers);
      for (int32_t ci = 0; ci < csz; ci++) {
        SMqSubConsumer *pSubConsumer = taosArrayGet(pSub->consumers, ci);
        if (pSubConsumer->consumerId == consumerId) {
          int32_t vgsz = taosArrayGetSize(pSubConsumer->vgInfo);
          for (int32_t vgi = 0; vgi < vgsz; vgi++) {
            SMqConsumerEp *pConsumerEp = taosArrayGet(pSubConsumer->vgInfo, vgi);
            mndPersistCancelConnReq(pMnode, pTrans, pConsumerEp, oldTopicName);
            taosArrayPush(pSub->unassignedVg, pConsumerEp);
          }
          taosArrayRemove(pSub->consumers, ci);
          break;
        }
      }
      char *oldTopicNameDup = strdup(oldTopicName);
      taosArrayPush(pConsumer->recentRemovedTopics, &oldTopicNameDup);
      atomic_store_32(&pConsumer->status, MQ_CONSUMER_STATUS__MODIFY);
      /*pSub->status = MQ_SUBSCRIBE_STATUS__DELETED;*/
    } else if (newTopicName != NULL) {
      ASSERT(oldTopicName == NULL);

      SMqTopicObj *pTopic = mndAcquireTopic(pMnode, newTopicName);
      if (pTopic == NULL) {
        mError("topic being subscribed not exist: %s", newTopicName);
        continue;
      }

      SMqSubscribeObj *pSub = mndAcquireSubscribe(pMnode, cgroup, newTopicName);
      bool             createSub = false;
      if (pSub == NULL) {
        mDebug("create new subscription by consumer %" PRId64 ", group: %s, topic %s", consumerId, cgroup,
               newTopicName);
        pSub = mndCreateSubscription(pMnode, pTopic, cgroup);
        createSub = true;

        mndCreateOffset(pTrans, cgroup, newTopicName, pSub->unassignedVg);
      }

      SMqSubConsumer mqSubConsumer;
      mqSubConsumer.consumerId = consumerId;
      mqSubConsumer.vgInfo = taosArrayInit(0, sizeof(SMqConsumerEp));
      taosArrayPush(pSub->consumers, &mqSubConsumer);

      // if have un assigned vg, assign one to the consumer
      if (taosArrayGetSize(pSub->unassignedVg) > 0) {
        SMqConsumerEp *pConsumerEp = taosArrayPop(pSub->unassignedVg);
        pConsumerEp->oldConsumerId = pConsumerEp->consumerId;
        pConsumerEp->consumerId = consumerId;
        taosArrayPush(mqSubConsumer.vgInfo, pConsumerEp);
        if (pConsumerEp->oldConsumerId == -1) {
          mInfo("mq set conn: assign vgroup %d of topic %s to consumer %" PRId64 "", pConsumerEp->vgId, newTopicName,
                pConsumerEp->consumerId);
          mndPersistMqSetConnReq(pMnode, pTrans, pTopic, cgroup, pConsumerEp);
        } else {
          mndPersistRebalanceMsg(pMnode, pTrans, pConsumerEp, newTopicName);
        }
        // to trigger rebalance at once, do not set status active
        /*atomic_store_32(&pConsumer->status, MQ_CONSUMER_STATUS__ACTIVE);*/
      }

      SSdbRaw *pRaw = mndSubActionEncode(pSub);
      sdbSetRawStatus(pRaw, SDB_STATUS_READY);
      mndTransAppendRedolog(pTrans, pRaw);

      if (!createSub) mndReleaseSubscribe(pMnode, pSub);
      mndReleaseTopic(pMnode, pTopic);
    }
  }

  /*if (oldSub) taosArrayDestroyEx(oldSub, (void (*)(void *))taosMemoryFree);*/

  // persist consumerObj
  SSdbRaw *pConsumerRaw = mndConsumerActionEncode(pConsumer);
  sdbSetRawStatus(pConsumerRaw, SDB_STATUS_READY);
  mndTransAppendRedolog(pTrans, pConsumerRaw);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("mq-subscribe-trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    if (!createConsumer) mndReleaseConsumer(pMnode, pConsumer);
    return -1;
  }

  mndTransDrop(pTrans);
  if (!createConsumer) mndReleaseConsumer(pMnode, pConsumer);
  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}
#endif

static int32_t mndProcessSubscribeInternalRsp(SNodeMsg *pRsp) {
  mndTransProcessRsp(pRsp);
  return 0;
}

static void mndCancelGetNextConsumer(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}
