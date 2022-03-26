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

static int32_t mndMakeSubscribeKey(char *key, const char *cgroup, const char *topicName);

static SSdbRaw *mndSubActionEncode(SMqSubscribeObj *);
static SSdbRow *mndSubActionDecode(SSdbRaw *pRaw);
static int32_t  mndSubActionInsert(SSdb *pSdb, SMqSubscribeObj *);
static int32_t  mndSubActionDelete(SSdb *pSdb, SMqSubscribeObj *);
static int32_t  mndSubActionUpdate(SSdb *pSdb, SMqSubscribeObj *pOldSub, SMqSubscribeObj *pNewSub);

static int32_t mndProcessSubscribeReq(SNodeMsg *pMsg);
static int32_t mndProcessSubscribeRsp(SNodeMsg *pMsg);
static int32_t mndProcessSubscribeInternalReq(SNodeMsg *pMsg);
static int32_t mndProcessSubscribeInternalRsp(SNodeMsg *pMsg);
static int32_t mndProcessMqTimerMsg(SNodeMsg *pMsg);
static int32_t mndProcessGetSubEpReq(SNodeMsg *pMsg);
static int32_t mndProcessDoRebalanceMsg(SNodeMsg *pMsg);
static int32_t mndProcessResetOffsetReq(SNodeMsg *pMsg);

static int32_t mndPersistMqSetConnReq(SMnode *pMnode, STrans *pTrans, const SMqTopicObj *pTopic, const char *cgroup,
                                      const SMqConsumerEp *pConsumerEp);

static int32_t mndPersistRebalanceMsg(SMnode *pMnode, STrans *pTrans, const SMqConsumerEp *pConsumerEp);

static int32_t mndInitUnassignedVg(SMnode *pMnode, const SMqTopicObj *pTopic, SMqSubscribeObj *pSub);

int32_t mndInitSubscribe(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_SUBSCRIBE,
                     .keyType = SDB_KEY_BINARY,
                     .encodeFp = (SdbEncodeFp)mndSubActionEncode,
                     .decodeFp = (SdbDecodeFp)mndSubActionDecode,
                     .insertFp = (SdbInsertFp)mndSubActionInsert,
                     .updateFp = (SdbUpdateFp)mndSubActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndSubActionDelete};

  mndSetMsgHandle(pMnode, TDMT_MND_SUBSCRIBE, mndProcessSubscribeReq);
  mndSetMsgHandle(pMnode, TDMT_VND_MQ_SET_CONN_RSP, mndProcessSubscribeInternalRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_MQ_REB_RSP, mndProcessSubscribeInternalRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_MQ_TIMER, mndProcessMqTimerMsg);
  mndSetMsgHandle(pMnode, TDMT_MND_GET_SUB_EP, mndProcessGetSubEpReq);
  mndSetMsgHandle(pMnode, TDMT_MND_MQ_DO_REBALANCE, mndProcessDoRebalanceMsg);
  return sdbSetTable(pMnode->pSdb, table);
}

static SMqSubscribeObj *mndCreateSubscription(SMnode *pMnode, const SMqTopicObj *pTopic, const char *cgroup) {
  SMqSubscribeObj *pSub = tNewSubscribeObj();
  if (pSub == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  char key[TSDB_SUBSCRIBE_KEY_LEN];
  mndMakeSubscribeKey(key, cgroup, pTopic->name);
  strcpy(pSub->key, key);

  if (mndSchedInitSubEp(pMnode, pTopic, pSub) < 0) {
    tDeleteSMqSubscribeObj(pSub);
    taosMemoryFree(pSub);
    return NULL;
  }

#if 0
  if (mndInitUnassignedVg(pMnode, pTopic, pSub) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tDeleteSMqSubscribeObj(pSub);
    taosMemoryFree(pSub);
    return NULL;
  }
#endif
  // TODO: disable alter subscribed table
  return pSub;
}

static int32_t mndBuildRebalanceMsg(void **pBuf, int32_t *pLen, const SMqConsumerEp *pConsumerEp) {
  SMqMVRebReq req = {
      .vgId = pConsumerEp->vgId,
      .oldConsumerId = pConsumerEp->oldConsumerId,
      .newConsumerId = pConsumerEp->consumerId,
  };

  int32_t tlen = tEncodeSMqMVRebReq(NULL, &req);
  void   *buf = taosMemoryMalloc(sizeof(SMsgHead) + tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  SMsgHead *pMsgHead = (SMsgHead *)buf;

  pMsgHead->contLen = htonl(sizeof(SMsgHead) + tlen);
  pMsgHead->vgId = htonl(pConsumerEp->vgId);

  void *abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
  tEncodeSMqMVRebReq(&abuf, &req);

  *pBuf = buf;
  *pLen = tlen;

  return 0;
}

static int32_t mndPersistRebalanceMsg(SMnode *pMnode, STrans *pTrans, const SMqConsumerEp *pConsumerEp) {
  ASSERT(pConsumerEp->oldConsumerId != -1);

  void   *buf;
  int32_t tlen;
  if (mndBuildRebalanceMsg(&buf, &tlen, pConsumerEp) < 0) {
    return -1;
  }

  int32_t vgId = pConsumerEp->vgId;
  SVgObj *pVgObj = mndAcquireVgroup(pMnode, vgId);

  STransAction action = {0};
  action.epSet = mndGetVgroupEpset(pMnode, pVgObj);
  action.pCont = buf;
  action.contLen = sizeof(SMsgHead) + tlen;
  action.msgType = TDMT_VND_MQ_REB;

  mndReleaseVgroup(pMnode, pVgObj);
  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(buf);
    return -1;
  }

  return 0;
}

static int32_t mndBuildCancelConnReq(void **pBuf, int32_t *pLen, const SMqConsumerEp *pConsumerEp) {
  SMqSetCVgReq req = {0};
  req.consumerId = pConsumerEp->consumerId;

  int32_t tlen = tEncodeSMqSetCVgReq(NULL, &req);
  void   *buf = taosMemoryMalloc(sizeof(SMsgHead) + tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  SMsgHead *pMsgHead = (SMsgHead *)buf;

  pMsgHead->contLen = htonl(sizeof(SMsgHead) + tlen);
  pMsgHead->vgId = htonl(pConsumerEp->vgId);
  void *abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
  tEncodeSMqSetCVgReq(&abuf, &req);
  *pBuf = buf;
  *pLen = tlen;
  return 0;
}

static int32_t mndPersistCancelConnReq(SMnode *pMnode, STrans *pTrans, const SMqConsumerEp *pConsumerEp) {
  void   *buf;
  int32_t tlen;
  if (mndBuildCancelConnReq(&buf, &tlen, pConsumerEp) < 0) {
    return -1;
  }

  int32_t vgId = pConsumerEp->vgId;
  SVgObj *pVgObj = mndAcquireVgroup(pMnode, vgId);

  STransAction action = {0};
  action.epSet = mndGetVgroupEpset(pMnode, pVgObj);
  action.pCont = buf;
  action.contLen = sizeof(SMsgHead) + tlen;
  action.msgType = TDMT_VND_MQ_SET_CONN;

  mndReleaseVgroup(pMnode, pVgObj);
  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(buf);
    return -1;
  }

  return 0;
}

#if 0
static int32_t mndProcessResetOffsetReq(SNodeMsg *pMsg) {
  SMnode             *pMnode = pMsg->pNode;
  uint8_t            *str = pMsg->rpcMsg.pCont;
  SMqCMResetOffsetReq req;

  SCoder decoder;
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, str, pMsg->rpcMsg.contLen, TD_DECODER);
  tDecodeSMqCMResetOffsetReq(&decoder, &req);

  SHashObj *pHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (pHash == NULL) {
    return -1;
  }

  for (int32_t i = 0; i < req.num; i++) {
    SMqOffset    *pOffset = &req.offsets[i];
    SMqVgOffsets *pVgOffset = taosHashGet(pHash, &pOffset->vgId, sizeof(int32_t));
    if (pVgOffset == NULL) {
      pVgOffset = taosMemoryMalloc(sizeof(SMqVgOffsets));
      if (pVgOffset == NULL) {
        return -1;
      }
      pVgOffset->offsets = taosArrayInit(0, sizeof(void *));
      taosArrayPush(pVgOffset->offsets, &pOffset);
    }
    taosHashPut(pHash, &pOffset->vgId, sizeof(int32_t), &pVgOffset, sizeof(void *));
  }

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, &pMsg->rpcMsg);
  if (pTrans == NULL) {
    mError("mq-reset-offset: failed since %s", terrstr());
    return -1;
  }

  return 0;
}
#endif

static int32_t mndProcessGetSubEpReq(SNodeMsg *pMsg) {
  SMnode           *pMnode = pMsg->pNode;
  SMqCMGetSubEpReq *pReq = (SMqCMGetSubEpReq *)pMsg->rpcMsg.pCont;
  SMqCMGetSubEpRsp  rsp = {0};
  int64_t           consumerId = be64toh(pReq->consumerId);
  int32_t           epoch = ntohl(pReq->epoch);

  SMqConsumerObj *pConsumer = mndAcquireConsumer(pMsg->pNode, consumerId);
  if (pConsumer == NULL) {
    terrno = TSDB_CODE_MND_CONSUMER_NOT_EXIST;
    return -1;
  }
  ASSERT(strcmp(pReq->cgroup, pConsumer->cgroup) == 0);

  // TODO
  int32_t hbStatus = atomic_load_32(&pConsumer->hbStatus);
  mTrace("try to get sub ep, old val: %d", hbStatus);
  atomic_store_32(&pConsumer->hbStatus, 0);
  /*SSdbRaw *pConsumerRaw = mndConsumerActionEncode(pConsumer);*/
  /*sdbSetRawStatus(pConsumerRaw, SDB_STATUS_READY);*/
  /*sdbWrite(pMnode->pSdb, pConsumerRaw);*/

  strcpy(rsp.cgroup, pReq->cgroup);
  if (epoch != pConsumer->epoch) {
    mInfo("send new assignment to consumer, consumer epoch %d, server epoch %d", epoch, pConsumer->epoch);
    SArray *pTopics = pConsumer->currentTopics;
    int32_t sz = taosArrayGetSize(pTopics);
    rsp.topics = taosArrayInit(sz, sizeof(SMqSubTopicEp));
    for (int32_t i = 0; i < sz; i++) {
      char            *topicName = taosArrayGetP(pTopics, i);
      SMqSubscribeObj *pSub = mndAcquireSubscribe(pMnode, pConsumer->cgroup, topicName);
      ASSERT(pSub);
      int32_t csz = taosArrayGetSize(pSub->consumers);
      // TODO: change to bsearch
      for (int32_t j = 0; j < csz; j++) {
        SMqSubConsumer *pSubConsumer = taosArrayGet(pSub->consumers, j);
        if (consumerId == pSubConsumer->consumerId) {
          int32_t       vgsz = taosArrayGetSize(pSubConsumer->vgInfo);
          SMqSubTopicEp topicEp;
          strcpy(topicEp.topic, topicName);
          topicEp.vgs = taosArrayInit(vgsz, sizeof(SMqSubVgEp));
          for (int32_t k = 0; k < vgsz; k++) {
            char           offsetKey[TSDB_PARTITION_KEY_LEN];
            SMqConsumerEp *pConsumerEp = taosArrayGet(pSubConsumer->vgInfo, k);
            SMqSubVgEp     vgEp = {
                    .epSet = pConsumerEp->epSet,
                    .vgId = pConsumerEp->vgId,
                    .offset = -1,
            };
            mndMakePartitionKey(offsetKey, pConsumer->cgroup, topicName, pConsumerEp->vgId);
            SMqOffsetObj *pOffsetObj = mndAcquireOffset(pMnode, offsetKey);
            if (pOffsetObj != NULL) {
              vgEp.offset = pOffsetObj->offset;
              mndReleaseOffset(pMnode, pOffsetObj);
            }
            taosArrayPush(topicEp.vgs, &vgEp);
          }
          taosArrayPush(rsp.topics, &topicEp);
          break;
        }
      }
      mndReleaseSubscribe(pMnode, pSub);
    }
  }
  int32_t tlen = sizeof(SMqRspHead) + tEncodeSMqCMGetSubEpRsp(NULL, &rsp);
  void   *buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  ((SMqRspHead *)buf)->mqMsgType = TMQ_MSG_TYPE__EP_RSP;
  ((SMqRspHead *)buf)->epoch = pConsumer->epoch;
  ((SMqRspHead *)buf)->consumerId = pConsumer->consumerId;

  void *abuf = POINTER_SHIFT(buf, sizeof(SMqRspHead));
  tEncodeSMqCMGetSubEpRsp(&abuf, &rsp);
  tDeleteSMqCMGetSubEpRsp(&rsp);
  mndReleaseConsumer(pMnode, pConsumer);
  pMsg->pRsp = buf;
  pMsg->rspLen = tlen;
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

static int32_t mndProcessMqTimerMsg(SNodeMsg *pMsg) {
  SMnode            *pMnode = pMsg->pNode;
  SSdb              *pSdb = pMnode->pSdb;
  SMqConsumerObj    *pConsumer;
  void              *pIter = NULL;
  SMqDoRebalanceMsg *pRebMsg = rpcMallocCont(sizeof(SMqDoRebalanceMsg));
  pRebMsg->rebSubHash = taosHashInit(64, MurmurHash3_32, true, HASH_NO_LOCK);

  while (1) {
    pIter = sdbFetch(pSdb, SDB_CONSUMER, pIter, (void **)&pConsumer);
    if (pIter == NULL) break;
    int32_t hbStatus = atomic_add_fetch_32(&pConsumer->hbStatus, 1);
    if (hbStatus > MND_SUBSCRIBE_REBALANCE_CNT) {
      int32_t old =
          atomic_val_compare_exchange_32(&pConsumer->status, MQ_CONSUMER_STATUS__ACTIVE, MQ_CONSUMER_STATUS__LOST);
      if (old == MQ_CONSUMER_STATUS__ACTIVE) {
        // get all topics of that topic
        int32_t sz = taosArrayGetSize(pConsumer->currentTopics);
        for (int32_t i = 0; i < sz; i++) {
          char *topic = taosArrayGetP(pConsumer->currentTopics, i);
          char  key[TSDB_SUBSCRIBE_KEY_LEN];
          mndMakeSubscribeKey(key, pConsumer->cgroup, topic);
          SMqRebSubscribe *pRebSub = mndGetOrCreateRebSub(pRebMsg->rebSubHash, key);
          taosArrayPush(pRebSub->lostConsumers, &pConsumer->consumerId);
        }
      }
    }
    int32_t status = atomic_load_32(&pConsumer->status);
    if (status == MQ_CONSUMER_STATUS__INIT || status == MQ_CONSUMER_STATUS__MODIFY) {
      SArray *rebSubs;
      if (status == MQ_CONSUMER_STATUS__INIT) {
        rebSubs = pConsumer->currentTopics;
      } else {
        rebSubs = pConsumer->recentRemovedTopics;
      }
      int32_t sz = taosArrayGetSize(rebSubs);
      for (int32_t i = 0; i < sz; i++) {
        char *topic = taosArrayGetP(rebSubs, i);
        char  key[TSDB_SUBSCRIBE_KEY_LEN];
        mndMakeSubscribeKey(key, pConsumer->cgroup, topic);
        SMqRebSubscribe *pRebSub = mndGetOrCreateRebSub(pRebMsg->rebSubHash, key);
        if (status == MQ_CONSUMER_STATUS__INIT) {
          taosArrayPush(pRebSub->newConsumers, &pConsumer->consumerId);
        } else if (status == MQ_CONSUMER_STATUS__MODIFY) {
          taosArrayPush(pRebSub->removedConsumers, &pConsumer->consumerId);
        }
      }
      if (status == MQ_CONSUMER_STATUS__MODIFY) {
        int32_t removeSz = taosArrayGetSize(pConsumer->recentRemovedTopics);
        for (int32_t i = 0; i < removeSz; i++) {
          char *topicName = taosArrayGet(pConsumer->recentRemovedTopics, i);
          taosMemoryFree(topicName);
        }
        taosArrayClear(pConsumer->recentRemovedTopics);
      }
    }
  }
  if (taosHashGetSize(pRebMsg->rebSubHash) != 0) {
    mInfo("mq rebalance will be triggered");
    SRpcMsg rpcMsg = {
        .msgType = TDMT_MND_MQ_DO_REBALANCE,
        .pCont = pRebMsg,
        .contLen = sizeof(SMqDoRebalanceMsg),
    };
    tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg);
  } else {
    taosHashCleanup(pRebMsg->rebSubHash);
    rpcFreeCont(pRebMsg);
  }
  return 0;
}

static int32_t mndProcessDoRebalanceMsg(SNodeMsg *pMsg) {
  SMnode            *pMnode = pMsg->pNode;
  SMqDoRebalanceMsg *pReq = pMsg->rpcMsg.pCont;
  STrans            *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_TYPE_REBALANCE, &pMsg->rpcMsg);
  void              *pIter = NULL;

  mInfo("mq rebalance start");

  while (1) {
    pIter = taosHashIterate(pReq->rebSubHash, pIter);
    if (pIter == NULL) break;
    SMqRebSubscribe *pRebSub = (SMqRebSubscribe *)pIter;
    SMqSubscribeObj *pSub = mndAcquireSubscribeByKey(pMnode, pRebSub->key);
    taosMemoryFreeClear(pRebSub->key);

    mInfo("mq rebalance subscription: %s", pSub->key);

    // remove lost consumer
    for (int32_t i = 0; i < taosArrayGetSize(pRebSub->lostConsumers); i++) {
      int64_t lostConsumerId = *(int64_t *)taosArrayGet(pRebSub->lostConsumers, i);

      mInfo("mq remove lost consumer %" PRId64 "", lostConsumerId);

      for (int32_t j = 0; j < taosArrayGetSize(pSub->consumers); j++) {
        SMqSubConsumer *pSubConsumer = taosArrayGet(pSub->consumers, j);
        if (pSubConsumer->consumerId == lostConsumerId) {
          taosArrayAddAll(pSub->unassignedVg, pSubConsumer->vgInfo);
          taosArrayPush(pSub->lostConsumers, pSubConsumer);
          taosArrayRemove(pSub->consumers, j);
          break;
        }
      }
    }

    // calculate rebalance
    int32_t consumerNum = taosArrayGetSize(pSub->consumers);
    if (consumerNum != 0) {
      int32_t vgNum = pSub->vgNum;
      int32_t vgEachConsumer = vgNum / consumerNum;
      int32_t imbalanceVg = vgNum % consumerNum;
      int32_t imbalanceSolved = 0;

      // iterate all consumers, set unassignedVgStash
      for (int32_t i = 0; i < consumerNum; i++) {
        SMqSubConsumer *pSubConsumer = taosArrayGet(pSub->consumers, i);
        int32_t         vgThisConsumerBeforeRb = taosArrayGetSize(pSubConsumer->vgInfo);
        int32_t         vgThisConsumerAfterRb;
        if (i < imbalanceVg)
          vgThisConsumerAfterRb = vgEachConsumer + 1;
        else
          vgThisConsumerAfterRb = vgEachConsumer;

        mInfo("mq consumer:%" PRId64 ", connectted vgroup number change from %d to %d", pSubConsumer->consumerId,
              vgThisConsumerBeforeRb, vgThisConsumerAfterRb);

        while (taosArrayGetSize(pSubConsumer->vgInfo) > vgThisConsumerAfterRb) {
          SMqConsumerEp *pConsumerEp = taosArrayPop(pSubConsumer->vgInfo);
          ASSERT(pConsumerEp != NULL);
          ASSERT(pConsumerEp->consumerId == pSubConsumer->consumerId);
          taosArrayPush(pSub->unassignedVg, pConsumerEp);
        }

        SMqConsumerObj *pRebConsumer = mndAcquireConsumer(pMnode, pSubConsumer->consumerId);
        int32_t         status = atomic_load_32(&pRebConsumer->status);
        if (vgThisConsumerAfterRb != vgThisConsumerBeforeRb ||
            (vgThisConsumerAfterRb != 0 && status != MQ_CONSUMER_STATUS__ACTIVE) ||
            (vgThisConsumerAfterRb == 0 && status != MQ_CONSUMER_STATUS__LOST)) {
          if (vgThisConsumerAfterRb != vgThisConsumerBeforeRb) {
            pRebConsumer->epoch++;
          }
          if (vgThisConsumerAfterRb != 0) {
            atomic_store_32(&pRebConsumer->status, MQ_CONSUMER_STATUS__ACTIVE);
          } else {
            atomic_store_32(&pRebConsumer->status, MQ_CONSUMER_STATUS__IDLE);
          }

          mInfo("mq consumer:%" PRId64 ", status change from %d to %d", pRebConsumer->consumerId, status,
                pRebConsumer->status);

          SSdbRaw *pConsumerRaw = mndConsumerActionEncode(pRebConsumer);
          sdbSetRawStatus(pConsumerRaw, SDB_STATUS_READY);
          mndTransAppendRedolog(pTrans, pConsumerRaw);
        }
        mndReleaseConsumer(pMnode, pRebConsumer);
      }

      // assign to vgroup
      if (taosArrayGetSize(pSub->unassignedVg) != 0) {
        for (int32_t i = 0; i < consumerNum; i++) {
          SMqSubConsumer *pSubConsumer = taosArrayGet(pSub->consumers, i);
          int32_t         vgThisConsumerBeforeRb = taosArrayGetSize(pSubConsumer->vgInfo);
          int32_t         vgThisConsumerAfterRb;
          if (i < imbalanceVg)
            vgThisConsumerAfterRb = vgEachConsumer + 1;
          else
            vgThisConsumerAfterRb = vgEachConsumer;

          while (taosArrayGetSize(pSubConsumer->vgInfo) < vgThisConsumerAfterRb) {
            SMqConsumerEp *pConsumerEp = taosArrayPop(pSub->unassignedVg);
            ASSERT(pConsumerEp != NULL);

            pConsumerEp->oldConsumerId = pConsumerEp->consumerId;
            pConsumerEp->consumerId = pSubConsumer->consumerId;
            taosArrayPush(pSubConsumer->vgInfo, pConsumerEp);

            if (pConsumerEp->oldConsumerId == -1) {
              char topic[TSDB_TOPIC_FNAME_LEN];
              char cgroup[TSDB_CGROUP_LEN];
              mndSplitSubscribeKey(pSub->key, topic, cgroup);
              SMqTopicObj *pTopic = mndAcquireTopic(pMnode, topic);

              mInfo("mq set conn: assign vgroup %d of topic %s to consumer %" PRId64 " cgroup: %s", pConsumerEp->vgId,
                    topic, pConsumerEp->consumerId, cgroup);

              mndPersistMqSetConnReq(pMnode, pTrans, pTopic, cgroup, pConsumerEp);
              mndReleaseTopic(pMnode, pTopic);
            } else {
              mInfo("mq rebalance: assign vgroup %d, from consumer %" PRId64 " to consumer %" PRId64 "",
                    pConsumerEp->vgId, pConsumerEp->oldConsumerId, pConsumerEp->consumerId);

              mndPersistRebalanceMsg(pMnode, pTrans, pConsumerEp);
            }
          }
        }
      }
      ASSERT(taosArrayGetSize(pSub->unassignedVg) == 0);

      // TODO: log rebalance statistics
      SSdbRaw *pSubRaw = mndSubActionEncode(pSub);
      sdbSetRawStatus(pSubRaw, SDB_STATUS_READY);
      mndTransAppendRedolog(pTrans, pSubRaw);
    }
    mndReleaseSubscribe(pMnode, pSub);
  }
  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("mq-rebalance-trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    taosHashCleanup(pReq->rebSubHash);
    mndTransDrop(pTrans);
    return -1;
  }

  taosHashCleanup(pReq->rebSubHash);
  mndTransDrop(pTrans);
  return 0;
}

#if 0
      for (int32_t j = 0; j < consumerNum; j++) {
        bool            changed = false;
        bool            unfished = false;

        bool            canUseLeft = imbalanceSolved < imbalanceVg;
        bool            mustUseLeft = canUseLeft && (imbalanceVg - imbalanceSolved >= consumerNum - j);
        ASSERT(imbalanceVg - imbalanceSolved <= consumerNum - j);

        int32_t maxVg = vgEachConsumer + canUseLeft;
        int32_t minVg = vgEachConsumer + mustUseLeft;

        SMqSubConsumer *pSubConsumer = taosArrayGet(pSub->consumers, j);
        int32_t         vgThisConsumerBeforeRb = taosArrayGetSize(pSubConsumer->vgInfo);
        int32_t         vgThisConsumerAfterRb;
        if (vgThisConsumerBeforeRb > maxVg) {
          vgThisConsumerAfterRb = maxVg; 
          imbalanceSolved++;
          changed = true;
        } else if (vgThisConsumerBeforeRb < minVg) {
          vgThisConsumerAfterRb = minVg;
          if (mustUseLeft) imbalanceSolved++;
          changed = true;
        } else {
          vgThisConsumerAfterRb = vgThisConsumerBeforeRb;
        }

        if (vgThisConsumerBeforeRb > vgThisConsumerAfterRb) {
          while (taosArrayGetSize(pSubConsumer->vgInfo) > vgThisConsumerAfterRb) {
            // put into unassigned
            SMqConsumerEp *pConsumerEp = taosArrayPop(pSubConsumer->vgInfo);
            ASSERT(pConsumerEp != NULL);
            ASSERT(pConsumerEp->consumerId == pSubConsumer->consumerId);
            taosArrayPush(unassignedVgStash, pConsumerEp);
          }

        } else if (vgThisConsumerBeforeRb < vgThisConsumerAfterRb) {
          // assign from unassigned
          while (taosArrayGetSize(pSubConsumer->vgInfo) < vgThisConsumerAfterRb) {
            // if no unassgined, save j
            if (taosArrayGetSize(unassignedVgStash) == 0) {
              taosArrayPush(unassignedConsumerIdx, &j);
              unfished = true;
              break;
            }
            // assign vg to consumer
            SMqConsumerEp *pConsumerEp = taosArrayPop(unassignedVgStash);
            ASSERT(pConsumerEp != NULL);
            pConsumerEp->oldConsumerId = pConsumerEp->consumerId;
            pConsumerEp->consumerId = pSubConsumer->consumerId;
            taosArrayPush(pSubConsumer->vgInfo, pConsumerEp);
            // build msg and persist into trans
          }
        }

        if (changed && !unfished) {
          SMqConsumerObj *pRebConsumer = mndAcquireConsumer(pMnode, pSubConsumer->consumerId);
          pRebConsumer->epoch++;
          if (vgThisConsumerAfterRb != 0) {
            atomic_store_32(&pRebConsumer->status, MQ_CONSUMER_STATUS__ACTIVE);
          } else {
            atomic_store_32(&pRebConsumer->status, MQ_CONSUMER_STATUS__IDLE);
          }
          SSdbRaw *pConsumerRaw = mndConsumerActionEncode(pRebConsumer);
          sdbSetRawStatus(pConsumerRaw, SDB_STATUS_READY);
          mndTransAppendRedolog(pTrans, pConsumerRaw);
          mndReleaseConsumer(pMnode, pRebConsumer);
          // TODO: save history
        }
      }

      for (int32_t j = 0; j < taosArrayGetSize(unassignedConsumerIdx); j++) {
        bool            canUseLeft = imbalanceSolved < imbalanceVg;
        int32_t         consumerIdx = *(int32_t *)taosArrayGet(unassignedConsumerIdx, j);
        SMqSubConsumer *pSubConsumer = taosArrayGet(pSub->consumers, consumerIdx);
        if (canUseLeft) imbalanceSolved++;
        // must use
        int32_t vgThisConsumerAfterRb = taosArrayGetSize(pSubConsumer->vgInfo) + canUseLeft;
        while (taosArrayGetSize(pSubConsumer->vgInfo) < vgEachConsumer + canUseLeft) {
          // assign vg to consumer
          SMqConsumerEp *pConsumerEp = taosArrayPop(unassignedVgStash);
          ASSERT(pConsumerEp != NULL);
          pConsumerEp->oldConsumerId = pConsumerEp->consumerId;
          pConsumerEp->consumerId = pSubConsumer->consumerId;
          taosArrayPush(pSubConsumer->vgInfo, pConsumerEp);
          // build msg and persist into trans
        }
        SMqConsumerObj *pRebConsumer = mndAcquireConsumer(pMnode, pSubConsumer->consumerId);
        pRebConsumer->epoch++;
        atomic_store_32(&pRebConsumer->status, MQ_CONSUMER_STATUS__ACTIVE);
        SSdbRaw *pConsumerRaw = mndConsumerActionEncode(pRebConsumer);
        sdbSetRawStatus(pConsumerRaw, SDB_STATUS_READY);
        mndTransAppendRedolog(pTrans, pConsumerRaw);
        mndReleaseConsumer(pMnode, pRebConsumer);
        // TODO: save history
      }
#endif

#if 0
    //update consumer status for the subscribption
    for (int32_t i = 0; i < taosArrayGetSize(pSub->assigned); i++) {
      SMqConsumerEp *pCEp = taosArrayGet(pSub->assigned, i);
      int64_t        consumerId = pCEp->consumerId;
      if (pCEp->status != -1) {
        int32_t consumerHbStatus = atomic_fetch_add_32(&pCEp->consumerHbStatus, 1);
        if (consumerHbStatus < MND_SUBSCRIBE_REBALANCE_CNT) {
          continue;
        }
        // put consumer into lostConsumer
        SMqConsumerEp* lostConsumer = taosArrayPush(pSub->lostConsumer, pCEp);
        lostConsumer->qmsg = NULL;
        // put vg into unassigned
        taosArrayPush(pSub->unassignedVg, pCEp);
        // remove from assigned
        // TODO: swap with last one, reduce size and reset i
        taosArrayRemove(pSub->assigned, i);
        // remove from available consumer
        for (int32_t j = 0; j < taosArrayGetSize(pSub->availConsumer); j++) {
          if (*(int64_t *)taosArrayGet(pSub->availConsumer, i) == pCEp->consumerId) {
            taosArrayRemove(pSub->availConsumer, j);
            break;
          }
          // TODO: acquire consumer, set status to unavail
        }
#if 0
        SMqConsumerObj* pConsumer = mndAcquireConsumer(pMnode, consumerId);
        pConsumer->epoch++;
        printf("current epoch %ld size %ld", pConsumer->epoch, pConsumer->topics->size);
        SSdbRaw* pRaw = mndConsumerActionEncode(pConsumer);
        sdbSetRawStatus(pRaw, SDB_STATUS_READY);
        sdbWriteNotFree(pMnode->pSdb, pRaw);
        mndReleaseConsumer(pMnode, pConsumer);
#endif
      }
    }
    // no available consumer, skip rebalance
    if (taosArrayGetSize(pSub->availConsumer) == 0) {
      continue;
    }
    taosArrayGet(pSub->availConsumer, 0);
    // rebalance condition1 : have unassigned vg
    // assign vg to a consumer, trying to find the least assigned one
    if ((sz = taosArrayGetSize(pSub->unassignedVg)) > 0) {
      char *topic = NULL;
      char *cgroup = NULL;
      mndSplitSubscribeKey(pSub->key, &topic, &cgroup);

      SMqTopicObj *pTopic = mndAcquireTopic(pMnode, topic);
      STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, &pMsg->rpcMsg);
      for (int32_t i = 0; i < sz; i++) {
        int64_t        consumerId = *(int64_t *)taosArrayGet(pSub->availConsumer, pSub->nextConsumerIdx);
        pSub->nextConsumerIdx = (pSub->nextConsumerIdx + 1) % taosArrayGetSize(pSub->availConsumer);

        SMqConsumerEp *pCEp = taosArrayPop(pSub->unassignedVg);
        pCEp->oldConsumerId = pCEp->consumerId;
        pCEp->consumerId = consumerId;
        taosArrayPush(pSub->assigned, pCEp);

        SMqConsumerObj *pConsumer = mndAcquireConsumer(pMnode, consumerId);
        pConsumer->epoch++;
        SSdbRaw* pConsumerRaw = mndConsumerActionEncode(pConsumer);
        sdbSetRawStatus(pConsumerRaw, SDB_STATUS_READY);
        sdbWrite(pMnode->pSdb, pConsumerRaw);
        mndReleaseConsumer(pMnode, pConsumer);

        void* msg;
        int32_t msgLen;
        mndBuildRebalanceMsg(&msg, &msgLen, pTopic, pCEp, cgroup, topic);

        // persist msg
        STransAction action = {0};
        action.epSet = pCEp->epSet;
        action.pCont = msg;
        action.contLen = sizeof(SMsgHead) + msgLen;
        action.msgType = TDMT_VND_MQ_SET_CONN;
        mndTransAppendRedoAction(pTrans, &action);

        // persist data
        SSdbRaw *pRaw = mndSubActionEncode(pSub);
        sdbSetRawStatus(pRaw, SDB_STATUS_READY);
        mndTransAppendRedolog(pTrans, pRaw);
      }

      if (mndTransPrepare(pMnode, pTrans) != 0) {
        mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
      }
      mndReleaseTopic(pMnode, pTopic);
      mndTransDrop(pTrans);
      taosMemoryFreeClear(topic);
      taosMemoryFreeClear(cgroup);
    }
    // rebalance condition2 : imbalance assignment
  }
  return 0;
}
#endif

#if 0
static int32_t mndInitUnassignedVg(SMnode *pMnode, const SMqTopicObj *pTopic, SMqSubscribeObj *pSub) {
  SSdb      *pSdb = pMnode->pSdb;
  SVgObj    *pVgroup = NULL;
  SQueryPlan *pPlan = qStringToQueryPlan(pTopic->physicalPlan);
  SArray    *pArray = NULL;
  SNodeListNode    *inner = (SNodeListNode*)nodesListGetNode(pPlan->pSubplans, 0);
  SSubplan  *plan = (SSubplan*)nodesListGetNode(inner->pNodeList, 0);
  SArray    *unassignedVg = pSub->unassignedVg;

  void *pIter = NULL;
  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;
    if (pVgroup->dbUid != pTopic->dbUid) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    pSub->vgNum++;
    plan->execNode.nodeId = pVgroup->vgId;
    plan->execNode.epset = mndGetVgroupEpset(pMnode, pVgroup);

    if (schedulerConvertDagToTaskList(pPlan, &pArray) < 0) {
      terrno = TSDB_CODE_MND_UNSUPPORTED_TOPIC;
      mError("unsupport topic: %s, sql: %s", pTopic->name, pTopic->sql);
      return -1;
    }

    SMqConsumerEp consumerEp = {0};
    consumerEp.status = 0;
    consumerEp.consumerId = -1;
    STaskInfo *pTaskInfo = taosArrayGet(pArray, 0);
    consumerEp.epSet = pTaskInfo->addr.epset;
    consumerEp.vgId = pTaskInfo->addr.nodeId;

    ASSERT(consumerEp.vgId == pVgroup->vgId);
    consumerEp.qmsg = strdup(pTaskInfo->msg->msg);
    taosArrayPush(unassignedVg, &consumerEp);
    // TODO: free taskInfo
    taosArrayDestroy(pArray);
  }

  /*qDestroyQueryDag(pDag);*/
  return 0;
}
#endif

static int32_t mndPersistMqSetConnReq(SMnode *pMnode, STrans *pTrans, const SMqTopicObj *pTopic, const char *cgroup,
                                      const SMqConsumerEp *pConsumerEp) {
  ASSERT(pConsumerEp->oldConsumerId == -1);
  int32_t vgId = pConsumerEp->vgId;

  SMqSetCVgReq req = {
      .vgId = vgId,
      .consumerId = pConsumerEp->consumerId,
      .sql = pTopic->sql,
      .logicalPlan = pTopic->logicalPlan,
      .physicalPlan = pTopic->physicalPlan,
      .qmsg = pConsumerEp->qmsg,
  };

  strcpy(req.cgroup, cgroup);
  strcpy(req.topicName, pTopic->name);
  int32_t tlen = tEncodeSMqSetCVgReq(NULL, &req);
  void   *buf = taosMemoryMalloc(sizeof(SMsgHead) + tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  SMsgHead *pMsgHead = (SMsgHead *)buf;

  pMsgHead->contLen = htonl(sizeof(SMsgHead) + tlen);
  pMsgHead->vgId = htonl(vgId);

  void *abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
  tEncodeSMqSetCVgReq(&abuf, &req);

  SVgObj *pVgObj = mndAcquireVgroup(pMnode, vgId);

  STransAction action = {0};
  action.epSet = mndGetVgroupEpset(pMnode, pVgObj);
  action.pCont = buf;
  action.contLen = sizeof(SMsgHead) + tlen;
  action.msgType = TDMT_VND_MQ_SET_CONN;

  mndReleaseVgroup(pMnode, pVgObj);
  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(buf);
    return -1;
  }
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
  tDeleteSMqSubscribeObj(pSub);
  return 0;
}

static int32_t mndSubActionUpdate(SSdb *pSdb, SMqSubscribeObj *pOldSub, SMqSubscribeObj *pNewSub) {
  mTrace("subscribe:%s, perform update action", pOldSub->key);
  return 0;
}

static int32_t mndMakeSubscribeKey(char *key, const char *cgroup, const char *topicName) {
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
            mndPersistCancelConnReq(pMnode, pTrans, pConsumerEp);
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
          mndPersistRebalanceMsg(pMnode, pTrans, pConsumerEp);
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

  if (oldSub) taosArrayDestroyEx(oldSub, (void (*)(void*))taosMemoryFree);

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

static int32_t mndProcessSubscribeInternalRsp(SNodeMsg *pRsp) {
  mndTransProcessRsp(pRsp);
  return 0;
}

static void mndCancelGetNextConsumer(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}
