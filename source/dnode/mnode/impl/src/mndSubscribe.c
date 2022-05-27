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

static SSdbRaw *mndSubActionEncode(SMqSubscribeObj *);
static SSdbRow *mndSubActionDecode(SSdbRaw *pRaw);
static int32_t  mndSubActionInsert(SSdb *pSdb, SMqSubscribeObj *);
static int32_t  mndSubActionDelete(SSdb *pSdb, SMqSubscribeObj *);
static int32_t  mndSubActionUpdate(SSdb *pSdb, SMqSubscribeObj *pOldSub, SMqSubscribeObj *pNewSub);

static int32_t mndProcessRebalanceReq(SRpcMsg *pMsg);
static int32_t mndProcessDropCgroupReq(SRpcMsg *pMsg);
static int32_t mndProcessSubscribeInternalRsp(SRpcMsg *pMsg);

static int32_t mndRetrieveSubscribe(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextSubscribe(SMnode *pMnode, void *pIter);

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
  mndSetMsgHandle(pMnode, TDMT_VND_MQ_VG_DELETE_RSP, mndProcessSubscribeInternalRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_MQ_DO_REBALANCE, mndProcessRebalanceReq);
  mndSetMsgHandle(pMnode, TDMT_MND_MQ_DO_REBALANCE, mndProcessRebalanceReq);
  mndSetMsgHandle(pMnode, TDMT_MND_MQ_DROP_CGROUP, mndProcessDropCgroupReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_SUBSCRIPTIONS, mndRetrieveSubscribe);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_TOPICS, mndCancelGetNextSubscribe);

  return sdbSetTable(pMnode->pSdb, table);
}

static SMqSubscribeObj *mndCreateSub(SMnode *pMnode, const SMqTopicObj *pTopic, const char *subKey) {
  SMqSubscribeObj *pSub = tNewSubscribeObj(subKey);
  if (pSub == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  pSub->dbUid = pTopic->dbUid;
  pSub->subType = pTopic->subType;
  pSub->withTbName = pTopic->withTbName;
  pSub->withSchema = pTopic->withSchema;
  pSub->withTag = pTopic->withTag;

  ASSERT(pSub->unassignedVgs->size == 0);
  ASSERT(taosHashGetSize(pSub->consumerHash) == 0);

  if (mndSchedInitSubEp(pMnode, pTopic, pSub) < 0) {
    tDeleteSubscribeObj(pSub);
    taosMemoryFree(pSub);
    return NULL;
  }

  ASSERT(pSub->unassignedVgs->size > 0);
  ASSERT(taosHashGetSize(pSub->consumerHash) == 0);

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
  if (pVgObj == NULL) {
    taosMemoryFree(buf);
    return -1;
  }

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
  SMqRebInfo *pRebSub = taosHashGet(pHash, key, strlen(key) + 1);
  if (pRebSub == NULL) {
    pRebSub = tNewSMqRebSubscribe(key);
    if (pRebSub == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }
    taosHashPut(pHash, key, strlen(key) + 1, pRebSub, sizeof(SMqRebInfo));
  }
  return pRebSub;
}

static int32_t mndDoRebalance(SMnode *pMnode, const SMqRebInputObj *pInput, SMqRebOutputObj *pOutput) {
  int32_t totalVgNum = pOutput->pSub->vgNum;

  mInfo("mq rebalance: subscription: %s, vgNum: %d", pOutput->pSub->key, pOutput->pSub->vgNum);

  // 1. build temporary hash(vgId -> SMqRebOutputVg) to store modified vg
  SHashObj *pHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);

  // 2. check and get actual removed consumers, put their vg into hash
  int32_t removedNum = taosArrayGetSize(pInput->pRebInfo->removedConsumers);
  int32_t actualRemoved = 0;
  for (int32_t i = 0; i < removedNum; i++) {
    int64_t consumerId = *(int64_t *)taosArrayGet(pInput->pRebInfo->removedConsumers, i);
    ASSERT(consumerId > 0);
    SMqConsumerEp *pConsumerEp = taosHashGet(pOutput->pSub->consumerHash, &consumerId, sizeof(int64_t));
    ASSERT(pConsumerEp);
    if (pConsumerEp) {
      ASSERT(consumerId == pConsumerEp->consumerId);
      actualRemoved++;
      int32_t consumerVgNum = taosArrayGetSize(pConsumerEp->vgs);
      for (int32_t j = 0; j < consumerVgNum; j++) {
        SMqVgEp       *pVgEp = taosArrayGetP(pConsumerEp->vgs, j);
        SMqRebOutputVg outputVg = {
            .oldConsumerId = consumerId,
            .newConsumerId = -1,
            .pVgEp = pVgEp,
        };
        taosHashPut(pHash, &pVgEp->vgId, sizeof(int32_t), &outputVg, sizeof(SMqRebOutputVg));
        mInfo("mq rebalance: remove vg %d from consumer %ld", pVgEp->vgId, consumerId);
      }
      taosHashRemove(pOutput->pSub->consumerHash, &consumerId, sizeof(int64_t));
      // put into removed
      taosArrayPush(pOutput->removedConsumers, &consumerId);
    }
  }
  ASSERT(removedNum == actualRemoved);

  // if previously no consumer, there are vgs not assigned
  {
    int32_t consumerVgNum = taosArrayGetSize(pOutput->pSub->unassignedVgs);
    for (int32_t i = 0; i < consumerVgNum; i++) {
      SMqVgEp       *pVgEp = *(SMqVgEp **)taosArrayPop(pOutput->pSub->unassignedVgs);
      SMqRebOutputVg rebOutput = {
          .oldConsumerId = -1,
          .newConsumerId = -1,
          .pVgEp = pVgEp,
      };
      taosHashPut(pHash, &pVgEp->vgId, sizeof(int32_t), &rebOutput, sizeof(SMqRebOutputVg));
      mInfo("mq rebalance: remove vg %d from unassigned", pVgEp->vgId);
    }
  }

  // 3. calc vg number of each consumer
  int32_t afterRebConsumerNum = pInput->oldConsumerNum + taosArrayGetSize(pInput->pRebInfo->newConsumers) -
                                taosArrayGetSize(pInput->pRebInfo->removedConsumers);
  int32_t minVgCnt = 0;
  int32_t imbConsumerNum = 0;
  // calc num
  if (afterRebConsumerNum) {
    minVgCnt = totalVgNum / afterRebConsumerNum;
    imbConsumerNum = totalVgNum % afterRebConsumerNum;
  }
  mInfo("mq rebalance: %d consumer after rebalance, at least %d vg each, %d consumer has more vg", afterRebConsumerNum,
        minVgCnt, imbConsumerNum);

  // 4. first scan: remove consumer more than wanted, put to remove hash
  int32_t imbCnt = 0;
  void   *pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pOutput->pSub->consumerHash, pIter);
    if (pIter == NULL) break;
    SMqConsumerEp *pConsumerEp = (SMqConsumerEp *)pIter;
    ASSERT(pConsumerEp->consumerId > 0);
    int32_t consumerVgNum = taosArrayGetSize(pConsumerEp->vgs);
    // all old consumers still existing are touched
    // TODO optimize: touch only consumer whose vgs changed
    taosArrayPush(pOutput->touchedConsumers, &pConsumerEp->consumerId);
    if (consumerVgNum > minVgCnt) {
      if (imbCnt < imbConsumerNum) {
        if (consumerVgNum == minVgCnt + 1) {
          continue;
        } else {
          // pop until equal minVg + 1
          while (taosArrayGetSize(pConsumerEp->vgs) > minVgCnt + 1) {
            SMqVgEp       *pVgEp = *(SMqVgEp **)taosArrayPop(pConsumerEp->vgs);
            SMqRebOutputVg outputVg = {
                .oldConsumerId = pConsumerEp->consumerId,
                .newConsumerId = -1,
                .pVgEp = pVgEp,
            };
            taosHashPut(pHash, &pVgEp->vgId, sizeof(int32_t), &outputVg, sizeof(SMqRebOutputVg));
            mInfo("mq rebalance: remove vg %d from consumer %ld (first scan)", pVgEp->vgId, pConsumerEp->consumerId);
          }
          imbCnt++;
        }
      } else {
        // pop until equal minVg
        while (taosArrayGetSize(pConsumerEp->vgs) > minVgCnt) {
          SMqVgEp       *pVgEp = *(SMqVgEp **)taosArrayPop(pConsumerEp->vgs);
          SMqRebOutputVg outputVg = {
              .oldConsumerId = pConsumerEp->consumerId,
              .newConsumerId = -1,
              .pVgEp = pVgEp,
          };
          taosHashPut(pHash, &pVgEp->vgId, sizeof(int32_t), &outputVg, sizeof(SMqRebOutputVg));
          mInfo("mq rebalance: remove vg %d from consumer %ld (first scan)", pVgEp->vgId, pConsumerEp->consumerId);
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
      SMqConsumerEp newConsumerEp;
      newConsumerEp.consumerId = consumerId;
      newConsumerEp.vgs = taosArrayInit(0, sizeof(void *));
      taosHashPut(pOutput->pSub->consumerHash, &consumerId, sizeof(int64_t), &newConsumerEp, sizeof(SMqConsumerEp));
      taosArrayPush(pOutput->newConsumers, &consumerId);
      mInfo("mq rebalance: add new consumer %ld", consumerId);
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
    SMqConsumerEp *pConsumerEp = (SMqConsumerEp *)pIter;
    ASSERT(pConsumerEp->consumerId > 0);

    // push until equal minVg
    while (taosArrayGetSize(pConsumerEp->vgs) < minVgCnt) {
      // iter hash and find one vg
      pRemovedIter = taosHashIterate(pHash, pRemovedIter);
      ASSERT(pRemovedIter);
      pRebVg = (SMqRebOutputVg *)pRemovedIter;
      // push
      taosArrayPush(pConsumerEp->vgs, &pRebVg->pVgEp);
      pRebVg->newConsumerId = pConsumerEp->consumerId;
      taosArrayPush(pOutput->rebVgs, pRebVg);
      mInfo("mq rebalance: add vg %d to consumer %ld (second scan)", pRebVg->pVgEp->vgId, pConsumerEp->consumerId);
    }
  }

  // 7. handle unassigned vg
  if (taosHashGetSize(pOutput->pSub->consumerHash) != 0) {
    // if has consumer, assign all left vg
    while (1) {
      pRemovedIter = taosHashIterate(pHash, pRemovedIter);
      if (pRemovedIter == NULL) break;
      pIter = taosHashIterate(pOutput->pSub->consumerHash, pIter);
      ASSERT(pIter);
      pRebVg = (SMqRebOutputVg *)pRemovedIter;
      SMqConsumerEp *pConsumerEp = (SMqConsumerEp *)pIter;
      ASSERT(pConsumerEp->consumerId > 0);
      taosArrayPush(pConsumerEp->vgs, &pRebVg->pVgEp);
      pRebVg->newConsumerId = pConsumerEp->consumerId;
      taosArrayPush(pOutput->rebVgs, pRebVg);
      mInfo("mq rebalance: add vg %d to consumer %ld (second scan)", pRebVg->pVgEp->vgId, pConsumerEp->consumerId);
    }
  } else {
    // if all consumer is removed, put all vg into unassigned
    pIter = NULL;
    SMqRebOutputVg *pRebOutput = NULL;
    while (1) {
      pIter = taosHashIterate(pHash, pIter);
      if (pIter == NULL) break;
      pRebOutput = (SMqRebOutputVg *)pIter;
      ASSERT(pRebOutput->newConsumerId == -1);
      taosArrayPush(pOutput->pSub->unassignedVgs, &pRebOutput->pVgEp);
      taosArrayPush(pOutput->rebVgs, pRebOutput);
      mInfo("mq rebalance: unassign vg %d (second scan)", pRebOutput->pVgEp->vgId);
    }
  }

  // 8. TODO generate logs
  mInfo("rebalance calculation completed, rebalanced vg:");
  for (int32_t i = 0; i < taosArrayGetSize(pOutput->rebVgs); i++) {
    SMqRebOutputVg *pOutputRebVg = taosArrayGet(pOutput->rebVgs, i);
    mInfo("vg: %d moved from consumer %ld to consumer %ld", pOutputRebVg->pVgEp->vgId, pOutputRebVg->oldConsumerId,
          pOutputRebVg->newConsumerId);
  }

  // 9. clear
  taosHashCleanup(pHash);

  return 0;
}

static int32_t mndPersistRebResult(SMnode *pMnode, SRpcMsg *pMsg, const SMqRebOutputObj *pOutput) {
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_TYPE_REBALANCE, pMsg);
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
  if (mndSetSubCommitLogs(pMnode, pTrans, pOutput->pSub) != 0) {
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
    mndSplitSubscribeKey(pOutput->pSub->key, topic, cgroup, true);
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
    mndSplitSubscribeKey(pOutput->pSub->key, topic, cgroup, true);
    taosArrayPush(pConsumerNew->rebRemovedTopics, &topic);
    mndReleaseConsumer(pMnode, pConsumerOld);
    if (mndSetConsumerCommitLogs(pMnode, pTrans, pConsumerNew) != 0) {
      goto REB_FAIL;
    }
  }
  if (consumerNum) {
    char topic[TSDB_TOPIC_FNAME_LEN];
    char cgroup[TSDB_CGROUP_LEN];
    mndSplitSubscribeKey(pOutput->pSub->key, topic, cgroup, true);
    SMqTopicObj *pTopic = mndAcquireTopic(pMnode, topic);
    if (pTopic) {
      // TODO make topic complete
      SMqTopicObj topicObj = {0};
      memcpy(&topicObj, pTopic, sizeof(SMqTopicObj));
      topicObj.refConsumerCnt = pTopic->refConsumerCnt - consumerNum;
      // TODO is that correct?
      pTopic->refConsumerCnt = topicObj.refConsumerCnt;
      mInfo("subscribe topic %s unref %d consumer cgroup %s, refcnt %d", pTopic->name, consumerNum, cgroup,
            topicObj.refConsumerCnt);
      if (mndSetTopicCommitLogs(pMnode, pTrans, &topicObj) != 0) goto REB_FAIL;
    }
  }

  // 4. TODO commit log: modification log

  // 5. set cb
  mndTransSetCb(pTrans, MQ_REB_TRANS_START_FUNC, MQ_REB_TRANS_STOP_FUNC, NULL, 0);

  // 6. execution
  if (mndTransPrepare(pMnode, pTrans) != 0) goto REB_FAIL;

  mndTransDrop(pTrans);
  return 0;

REB_FAIL:
  mndTransDrop(pTrans);
  return -1;
}

static int32_t mndProcessRebalanceReq(SRpcMsg *pMsg) {
  SMnode            *pMnode = pMsg->info.node;
  SMqDoRebalanceMsg *pReq = pMsg->pCont;
  void              *pIter = NULL;

  mInfo("mq rebalance start");

  while (1) {
    pIter = taosHashIterate(pReq->rebSubHash, pIter);
    if (pIter == NULL) break;
    SMqRebInputObj rebInput = {0};

    SMqRebOutputObj rebOutput = {0};
    rebOutput.newConsumers = taosArrayInit(0, sizeof(int64_t));
    rebOutput.removedConsumers = taosArrayInit(0, sizeof(int64_t));
    rebOutput.touchedConsumers = taosArrayInit(0, sizeof(int64_t));
    rebOutput.rebVgs = taosArrayInit(0, sizeof(SMqRebOutputVg));

    SMqRebInfo      *pRebInfo = (SMqRebInfo *)pIter;
    SMqSubscribeObj *pSub = mndAcquireSubscribeByKey(pMnode, pRebInfo->key);

    rebInput.pRebInfo = pRebInfo;

    if (pSub == NULL) {
      // split sub key and extract topic
      char topic[TSDB_TOPIC_FNAME_LEN];
      char cgroup[TSDB_CGROUP_LEN];
      mndSplitSubscribeKey(pRebInfo->key, topic, cgroup, true);
      SMqTopicObj *pTopic = mndAcquireTopic(pMnode, topic);
      ASSERT(pTopic);
      taosRLockLatch(&pTopic->lock);

      rebOutput.pSub = mndCreateSub(pMnode, pTopic, pRebInfo->key);
      ASSERT(taosHashGetSize(rebOutput.pSub->consumerHash) == 0);

      taosRUnLockLatch(&pTopic->lock);
      mndReleaseTopic(pMnode, pTopic);

      rebInput.oldConsumerNum = 0;
    } else {
      taosRLockLatch(&pSub->lock);
      rebInput.oldConsumerNum = taosHashGetSize(pSub->consumerHash);
      rebOutput.pSub = tCloneSubscribeObj(pSub);
      taosRUnLockLatch(&pSub->lock);
      mndReleaseSubscribe(pMnode, pSub);
    }

    // TODO replace assert with error check
    ASSERT(mndDoRebalance(pMnode, &rebInput, &rebOutput) == 0);

    // if add more consumer to balanced subscribe,
    // possibly no vg is changed
    /*ASSERT(taosArrayGetSize(rebOutput.rebVgs) != 0);*/

    // TODO replace assert with error check
    if (mndPersistRebResult(pMnode, pMsg, &rebOutput) < 0) {
      mError("persist rebalance output error, possibly vnode splitted or dropped");
    }
    taosArrayDestroy(pRebInfo->lostConsumers);
    taosArrayDestroy(pRebInfo->newConsumers);
    taosArrayDestroy(pRebInfo->removedConsumers);

    taosArrayDestroy(rebOutput.newConsumers);
    taosArrayDestroy(rebOutput.touchedConsumers);
    taosArrayDestroy(rebOutput.removedConsumers);
    taosArrayDestroy(rebOutput.rebVgs);
    tDeleteSubscribeObj(rebOutput.pSub);
    taosMemoryFree(rebOutput.pSub);
  }

  // reset flag
  mInfo("mq rebalance completed successfully");
  taosHashCleanup(pReq->rebSubHash);
  mndRebEnd();

  return 0;
}

static int32_t mndProcessDropCgroupReq(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;
  /*SSdb          *pSdb = pMnode->pSdb;*/
  SMDropCgroupReq dropReq = {0};

  if (tDeserializeSMDropCgroupReq(pReq->pCont, pReq->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  SMqSubscribeObj *pSub = mndAcquireSubscribe(pMnode, dropReq.cgroup, dropReq.topic);
  if (pSub == NULL) {
    if (dropReq.igNotExists) {
      mDebug("cgroup:%s on topic:%s, not exist, ignore not exist is set", dropReq.cgroup, dropReq.topic);
      return 0;
    } else {
      terrno = TSDB_CODE_MND_SUBSCRIBE_NOT_EXIST;
      mError("topic:%s, cgroup:%s, failed to drop since %s", dropReq.topic, dropReq.cgroup, terrstr());
      return -1;
    }
  }

  if (taosHashGetSize(pSub->consumerHash) == 0) {
    terrno = TSDB_CODE_MND_CGROUP_USED;
    mError("cgroup:%s on topic:%s, failed to drop since %s", dropReq.cgroup, dropReq.topic, terrstr());
    return -1;
  }

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_TYPE_DROP_CGROUP, pReq);
  if (pTrans == NULL) {
    mError("cgroup: %s on topic:%s, failed to drop since %s", dropReq.cgroup, dropReq.topic, terrstr());
    return -1;
  }

  mDebug("trans:%d, used to drop cgroup:%s on topic %s", pTrans->id, dropReq.cgroup, dropReq.topic);

  if (mndDropOffsetBySubKey(pMnode, pTrans, pSub->key) < 0) {
    ASSERT(0);
    return -1;
  }

  if (mndSetDropSubCommitLogs(pMnode, pTrans, pSub) < 0) {
    mError("cgroup %s on topic:%s, failed to drop since %s", dropReq.cgroup, dropReq.topic, terrstr());
    return -1;
  }

  mndReleaseSubscribe(pMnode, pSub);

  return TSDB_CODE_ACTION_IN_PROGRESS;
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
  buf = taosMemoryMalloc(tlen);
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

  SArray *tmp1 = pOldSub->unassignedVgs;
  pOldSub->unassignedVgs = pNewSub->unassignedVgs;
  pNewSub->unassignedVgs = tmp1;

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

static int32_t mndProcessSubscribeInternalRsp(SRpcMsg *pRsp) {
  mndTransProcessRsp(pRsp);
  return 0;
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

int32_t mndDropSubByDB(SMnode *pMnode, STrans *pTrans, SDbObj *pDb) {
  int32_t code = -1;
  SSdb   *pSdb = pMnode->pSdb;

  void            *pIter = NULL;
  SMqSubscribeObj *pSub = NULL;
  while (1) {
    pIter = sdbFetch(pSdb, SDB_SUBSCRIBE, pIter, (void **)&pSub);
    if (pIter == NULL) break;

    if (pSub->dbUid != pDb->uid) {
      sdbRelease(pSdb, pSub);
      continue;
    }

    if (mndSetDropSubCommitLogs(pMnode, pTrans, pSub) < 0) {
      sdbRelease(pSdb, pSub);
      goto END;
    }
  }

  code = 0;
END:
  return code;
}

int32_t mndDropSubByTopic(SMnode *pMnode, STrans *pTrans, const char *topicName) {
  int32_t code = -1;
  SSdb   *pSdb = pMnode->pSdb;

  void            *pIter = NULL;
  SMqSubscribeObj *pSub = NULL;
  while (1) {
    pIter = sdbFetch(pSdb, SDB_SUBSCRIBE, pIter, (void **)&pSub);
    if (pIter == NULL) break;

    char topic[TSDB_TOPIC_FNAME_LEN];
    char cgroup[TSDB_CGROUP_LEN];
    mndSplitSubscribeKey(pSub->key, topic, cgroup, true);
    if (strcmp(topic, topicName) != 0) {
      sdbRelease(pSdb, pSub);
      continue;
    }

    // iter all vnode to delete handle
    ASSERT(taosHashGetSize(pSub->consumerHash) == 0);
    int32_t sz = taosArrayGetSize(pSub->unassignedVgs);
    for (int32_t i = 0; i < sz; i++) {
      SMqVgEp       *pVgEp = taosArrayGetP(pSub->unassignedVgs, i);
      SMqVDeleteReq *pReq = taosMemoryCalloc(1, sizeof(SMqVDeleteReq));
      pReq->head.vgId = htonl(pVgEp->vgId);
      pReq->vgId = pVgEp->vgId;
      pReq->consumerId = -1;
      memcpy(pReq->subKey, pSub->key, TSDB_SUBSCRIBE_KEY_LEN);
      STransAction action = {0};
      action.epSet = pVgEp->epSet;
      action.pCont = pReq;
      action.contLen = sizeof(SMqVDeleteReq);
      action.msgType = TDMT_VND_MQ_VG_DELETE;
      if (mndTransAppendRedoAction(pTrans, &action) != 0) {
        taosMemoryFree(pReq);
        return -1;
      }
    }

    if (mndSetDropSubRedoLogs(pMnode, pTrans, pSub) < 0) {
      sdbRelease(pSdb, pSub);
      goto END;
    }
  }

  code = 0;
END:
  return code;
}

static int32_t mndRetrieveSubscribe(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rowsCapacity) {
  SMnode          *pMnode = pReq->info.node;
  SSdb            *pSdb = pMnode->pSdb;
  int32_t          numOfRows = 0;
  SMqSubscribeObj *pSub = NULL;

  while (numOfRows < rowsCapacity) {
    pShow->pIter = sdbFetch(pSdb, SDB_SUBSCRIBE, pShow->pIter, (void **)&pSub);
    if (pShow->pIter == NULL) break;

    taosRLockLatch(&pSub->lock);

    if (numOfRows + pSub->vgNum > rowsCapacity) {
      blockDataEnsureCapacity(pBlock, numOfRows + pSub->vgNum);
    }

    SMqConsumerEp *pConsumerEp = NULL;
    void          *pIter = NULL;
    while (1) {
      pIter = taosHashIterate(pSub->consumerHash, pIter);
      if (pIter == NULL) break;
      pConsumerEp = (SMqConsumerEp *)pIter;

      int32_t sz = taosArrayGetSize(pConsumerEp->vgs);
      for (int32_t j = 0; j < sz; j++) {
        SMqVgEp *pVgEp = taosArrayGetP(pConsumerEp->vgs, j);

        SColumnInfoData *pColInfo;
        int32_t          cols = 0;

        // topic and cgroup
        char topic[TSDB_TOPIC_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
        char cgroup[TSDB_CGROUP_LEN + VARSTR_HEADER_SIZE] = {0};
        mndSplitSubscribeKey(pSub->key, varDataVal(topic), varDataVal(cgroup), false);
        varDataSetLen(topic, strlen(varDataVal(topic)));
        varDataSetLen(cgroup, strlen(varDataVal(cgroup)));

        pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
        colDataAppend(pColInfo, numOfRows, (const char *)topic, false);

        pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
        colDataAppend(pColInfo, numOfRows, (const char *)cgroup, false);

        // vg id
        pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
        colDataAppend(pColInfo, numOfRows, (const char *)&pVgEp->vgId, false);

        // consumer id
        pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
        colDataAppend(pColInfo, numOfRows, (const char *)&pConsumerEp->consumerId, false);

        // offset
#if 0
      // subscribe time
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)&pSub->subscribeTime, false);

      // rebalance time
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)&pSub->rebalanceTime, pConsumer->rebalanceTime == 0);
#endif

        numOfRows++;
      }
    }

    // do not show for cleared subscription
#if 0
    int32_t sz = taosArrayGetSize(pSub->unassignedVgs);
    for (int32_t i = 0; i < sz; i++) {
      SMqVgEp *pVgEp = taosArrayGetP(pSub->unassignedVgs, i);

      SColumnInfoData *pColInfo;
      int32_t          cols = 0;

      // topic and cgroup
      char topic[TSDB_TOPIC_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
      char cgroup[TSDB_CGROUP_LEN + VARSTR_HEADER_SIZE] = {0};
      mndSplitSubscribeKey(pSub->key, varDataVal(topic), varDataVal(cgroup), false);
      varDataSetLen(topic, strlen(varDataVal(topic)));
      varDataSetLen(cgroup, strlen(varDataVal(cgroup)));

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)topic, false);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)cgroup, false);

      // vg id
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)&pVgEp->vgId, false);

      // consumer id
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, NULL, true);

      // offset
#if 0
      // subscribe time
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)&pSub->subscribeTime, false);

      // rebalance time
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)&pSub->rebalanceTime, pConsumer->rebalanceTime == 0);
#endif

      numOfRows++;
    }

#endif
    taosRUnLockLatch(&pSub->lock);
    sdbRelease(pSdb, pSub);
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextSubscribe(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}
