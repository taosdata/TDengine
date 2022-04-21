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

/*static int32_t mndProcessSubscribeReq(SNodeMsg *pMsg);*/
/*static int32_t mndProcessSubscribeRsp(SNodeMsg *pMsg);*/
static int32_t mndProcessSubscribeInternalReq(SNodeMsg *pMsg);
static int32_t mndProcessSubscribeInternalRsp(SNodeMsg *pMsg);
/*static int32_t mndProcessMqTimerMsg(SNodeMsg *pMsg);*/
/*static int32_t mndProcessGetSubEpReq(SNodeMsg *pMsg);*/
/*static int32_t mndProcessDoRebalanceMsg(SNodeMsg *pMsg);*/
/*static int32_t mndProcessResetOffsetReq(SNodeMsg *pMsg);*/

static int32_t mndProcessRebalanceReq(SNodeMsg *pMsg);

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

/*static int32_t mndPersistMqSetConnReq(SMnode *pMnode, STrans *pTrans, const SMqTopicObj *pTopic, const char *cgroup,*/
/*const SMqConsumerEp *pConsumerEp);*/

/*static int32_t mndPersistRebalanceMsg(SMnode *pMnode, STrans *pTrans, const SMqConsumerEp *pConsumerEp,*/
/*const char *topicName);*/

/*static int32_t mndPersistCancelConnReq(SMnode *pMnode, STrans *pTrans, const SMqConsumerEp *pConsumerEp,*/
/*const char *oldTopicName);*/

int32_t mndInitSubscribe(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_SUBSCRIBE,
                     .keyType = SDB_KEY_BINARY,
                     .encodeFp = (SdbEncodeFp)mndSubActionEncode,
                     .decodeFp = (SdbDecodeFp)mndSubActionDecode,
                     .insertFp = (SdbInsertFp)mndSubActionInsert,
                     .updateFp = (SdbUpdateFp)mndSubActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndSubActionDelete};

  /*mndSetMsgHandle(pMnode, TDMT_MND_SUBSCRIBE, mndProcessSubscribeReq);*/
  /*mndSetMsgHandle(pMnode, TDMT_VND_MQ_SET_CONN_RSP, mndProcessSubscribeInternalRsp);*/
  /*mndSetMsgHandle(pMnode, TDMT_VND_MQ_REB_RSP, mndProcessSubscribeInternalRsp);*/
  /*mndSetMsgHandle(pMnode, TDMT_VND_MQ_CANCEL_CONN_RSP, mndProcessSubscribeInternalRsp);*/
  /*mndSetMsgHandle(pMnode, TDMT_MND_MQ_TIMER, mndProcessMqTimerMsg);*/
  /*mndSetMsgHandle(pMnode, TDMT_MND_GET_SUB_EP, mndProcessGetSubEpReq);*/
  /*mndSetMsgHandle(pMnode, TDMT_MND_MQ_DO_REBALANCE, mndProcessDoRebalanceMsg);*/
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
  ASSERT(taosHashGetSize(pSub->consumerHash) == 1);

  if (mndSchedInitSubEp(pMnode, pTopic, pSub) < 0) {
    tDeleteSubscribeObj(pSub);
    taosMemoryFree(pSub);
    return NULL;
  }

  ASSERT(taosHashGetSize(pSub->consumerHash) == 1);

  return pSub;
}

#if 0
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

  // TODO: disable alter subscribed table
  return pSub;
}

static int32_t mndBuildRebalanceMsg(void **pBuf, int32_t *pLen, const SMqConsumerEp *pConsumerEp,
                                    const char *topicName) {
  SMqMVRebReq req = {
      .vgId = pConsumerEp->vgId,
      .oldConsumerId = pConsumerEp->oldConsumerId,
      .newConsumerId = pConsumerEp->consumerId,
  };
  req.topic = strdup(topicName);

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
  taosMemoryFree(req.topic);

  *pBuf = buf;
  *pLen = tlen;

  return 0;
}

static int32_t mndPersistRebalanceMsg(SMnode *pMnode, STrans *pTrans, const SMqConsumerEp *pConsumerEp,
                                      const char *topicName) {
  ASSERT(pConsumerEp->oldConsumerId != -1);

  void   *buf;
  int32_t tlen;
  if (mndBuildRebalanceMsg(&buf, &tlen, pConsumerEp, topicName) < 0) {
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

static int32_t mndBuildCancelConnReq(void **pBuf, int32_t *pLen, const SMqConsumerEp *pConsumerEp,
                                     const char *oldTopicName) {
  SMqCancelConnReq req = {0};
  req.consumerId = pConsumerEp->consumerId;
  req.vgId = pConsumerEp->vgId;
  req.epoch = pConsumerEp->epoch;
  strcpy(req.topicName, oldTopicName);

  int32_t tlen = tEncodeSMqCancelConnReq(NULL, &req);
  void   *buf = taosMemoryMalloc(sizeof(SMsgHead) + tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  SMsgHead *pMsgHead = (SMsgHead *)buf;

  pMsgHead->contLen = htonl(sizeof(SMsgHead) + tlen);
  pMsgHead->vgId = htonl(pConsumerEp->vgId);
  void *abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
  tEncodeSMqCancelConnReq(&abuf, &req);
  *pBuf = buf;
  *pLen = tlen;
  return 0;
}

static int32_t mndPersistCancelConnReq(SMnode *pMnode, STrans *pTrans, const SMqConsumerEp *pConsumerEp,
                                       const char *oldTopicName) {
  void   *buf;
  int32_t tlen;
  if (mndBuildCancelConnReq(&buf, &tlen, pConsumerEp, oldTopicName) < 0) {
    return -1;
  }

  int32_t vgId = pConsumerEp->vgId;
  SVgObj *pVgObj = mndAcquireVgroup(pMnode, vgId);

  STransAction action = {0};
  action.epSet = mndGetVgroupEpset(pMnode, pVgObj);
  action.pCont = buf;
  action.contLen = sizeof(SMsgHead) + tlen;
  action.msgType = TDMT_VND_MQ_CANCEL_CONN;

  mndReleaseVgroup(pMnode, pVgObj);
  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(buf);
    return -1;
  }

  return 0;
}
#endif

static int32_t mndBuildSubChangeReq(void **pBuf, int32_t *pLen, const char *subKey, const SMqRebOutputVg *pRebVg) {
  SMqRebVgReq req = {0};
  req.oldConsumerId = pRebVg->oldConsumerId;
  req.newConsumerId = pRebVg->newConsumerId;
  req.vgId = pRebVg->pVgEp->vgId;
  req.qmsg = pRebVg->pVgEp->qmsg;
  strncpy(req.subKey, subKey, TSDB_SUBSCRIBE_KEY_LEN);

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

static int32_t mndPersistSubChangeVgReq(SMnode *pMnode, STrans *pTrans, const char *subKey,
                                        const SMqRebOutputVg *pRebVg) {
  ASSERT(pRebVg->oldConsumerId != pRebVg->newConsumerId);

  void   *buf;
  int32_t tlen;
  if (mndBuildSubChangeReq(&buf, &tlen, subKey, pRebVg) < 0) {
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

#if 0
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
  // TODO add lock
  ASSERT(strcmp(pReq->cgroup, pConsumer->cgroup) == 0);
  int32_t serverEpoch = pConsumer->epoch;

  // TODO
  int32_t hbStatus = atomic_load_32(&pConsumer->hbStatus);
  mDebug("consumer %ld epoch(%d) try to get sub ep, server epoch %d, old val: %d", consumerId, epoch, serverEpoch,
         hbStatus);
  atomic_store_32(&pConsumer->hbStatus, 0);
  /*SSdbRaw *pConsumerRaw = mndConsumerActionEncode(pConsumer);*/
  /*sdbSetRawStatus(pConsumerRaw, SDB_STATUS_READY);*/
  /*sdbWrite(pMnode->pSdb, pConsumerRaw);*/

  strcpy(rsp.cgroup, pReq->cgroup);
  if (epoch != serverEpoch) {
    mInfo("send new assignment to consumer %ld, consumer epoch %d, server epoch %d", pConsumer->consumerId, epoch,
          serverEpoch);
    mDebug("consumer %ld try r lock", consumerId);
    taosRLockLatch(&pConsumer->lock);
    mDebug("consumer %ld r locked", consumerId);
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
          int32_t vgsz = taosArrayGetSize(pSubConsumer->vgInfo);
          mInfo("topic %s has %d vg", topicName, serverEpoch);

          SMqSubTopicEp topicEp;
          strcpy(topicEp.topic, topicName);

          SMqTopicObj *pTopic = mndAcquireTopic(pMnode, topicName);
          ASSERT(pTopic != NULL);
          topicEp.schema = pTopic->schema;
          mndReleaseTopic(pMnode, pTopic);

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
    taosRUnLockLatch(&pConsumer->lock);
    mDebug("consumer %ld r unlock", consumerId);
  }
  int32_t tlen = sizeof(SMqRspHead) + tEncodeSMqCMGetSubEpRsp(NULL, &rsp);
  void   *buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  ((SMqRspHead *)buf)->mqMsgType = TMQ_MSG_TYPE__EP_RSP;
  ((SMqRspHead *)buf)->epoch = serverEpoch;
  ((SMqRspHead *)buf)->consumerId = pConsumer->consumerId;

  void *abuf = POINTER_SHIFT(buf, sizeof(SMqRspHead));
  tEncodeSMqCMGetSubEpRsp(&abuf, &rsp);
  tDeleteSMqCMGetSubEpRsp(&rsp);
  mndReleaseConsumer(pMnode, pConsumer);
  pMsg->pRsp = buf;
  pMsg->rspLen = tlen;
  return 0;
}
#endif

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

#if 0
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
          char *topicName = taosArrayGetP(pConsumer->recentRemovedTopics, i);
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
#endif

#if 0
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

    mInfo("mq rebalance subscription: %s, vgNum: %d, unassignedVg: %d", pSub->key, pSub->vgNum,
          (int32_t)taosArrayGetSize(pSub->unassignedVg));

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
          mDebug("mq rebalance: vg %d push to unassignedVg", pConsumerEp->vgId);
        }

        SMqConsumerObj *pRebConsumer = mndAcquireConsumer(pMnode, pSubConsumer->consumerId);
        mDebug("consumer %ld try w lock", pRebConsumer->consumerId);
        taosWLockLatch(&pRebConsumer->lock);
        mDebug("consumer %ld w locked", pRebConsumer->consumerId);
        int32_t status = atomic_load_32(&pRebConsumer->status);
        if (vgThisConsumerAfterRb != vgThisConsumerBeforeRb ||
            (vgThisConsumerAfterRb != 0 && status != MQ_CONSUMER_STATUS__ACTIVE) ||
            (vgThisConsumerAfterRb == 0 && status != MQ_CONSUMER_STATUS__LOST)) {
          /*if (vgThisConsumerAfterRb != vgThisConsumerBeforeRb) {*/
          /*pRebConsumer->epoch++;*/
          /*}*/
          if (vgThisConsumerAfterRb != 0) {
            atomic_store_32(&pRebConsumer->status, MQ_CONSUMER_STATUS__ACTIVE);
          } else {
            atomic_store_32(&pRebConsumer->status, MQ_CONSUMER_STATUS__IDLE);
          }

          mInfo("mq consumer:%" PRId64 ", status change from %d to %d", pRebConsumer->consumerId, status,
                pRebConsumer->status);

          SSdbRaw *pConsumerRaw = mndConsumerActionEncode(pRebConsumer);
          sdbSetRawStatus(pConsumerRaw, SDB_STATUS_READY);
          mndTransAppendCommitlog(pTrans, pConsumerRaw);
        }
        taosWUnLockLatch(&pRebConsumer->lock);
        mDebug("consumer %ld w unlock", pRebConsumer->consumerId);
        mndReleaseConsumer(pMnode, pRebConsumer);
      }

      // assign to vgroup
      if (taosArrayGetSize(pSub->unassignedVg) != 0) {
        for (int32_t i = 0; i < consumerNum; i++) {
          SMqSubConsumer *pSubConsumer = taosArrayGet(pSub->consumers, i);
          int32_t         vgThisConsumerAfterRb;
          if (i < imbalanceVg)
            vgThisConsumerAfterRb = vgEachConsumer + 1;
          else
            vgThisConsumerAfterRb = vgEachConsumer;

          while (taosArrayGetSize(pSubConsumer->vgInfo) < vgThisConsumerAfterRb) {
            SMqConsumerEp *pConsumerEp = taosArrayPop(pSub->unassignedVg);
            mDebug("mq rebalance: vg %d pop from unassignedVg", pConsumerEp->vgId);
            ASSERT(pConsumerEp != NULL);

            pConsumerEp->oldConsumerId = pConsumerEp->consumerId;
            pConsumerEp->consumerId = pSubConsumer->consumerId;
            // TODO
            pConsumerEp->epoch = 0;
            taosArrayPush(pSubConsumer->vgInfo, pConsumerEp);

            char topic[TSDB_TOPIC_FNAME_LEN];
            char cgroup[TSDB_CGROUP_LEN];
            mndSplitSubscribeKey(pSub->key, topic, cgroup);
            if (pConsumerEp->oldConsumerId == -1) {
              SMqTopicObj *pTopic = mndAcquireTopic(pMnode, topic);

              mInfo("mq set conn: assign vgroup %d of topic %s to consumer %" PRId64 " cgroup: %s", pConsumerEp->vgId,
                    topic, pConsumerEp->consumerId, cgroup);

              mndPersistMqSetConnReq(pMnode, pTrans, pTopic, cgroup, pConsumerEp);
              mndReleaseTopic(pMnode, pTopic);
            } else {
              mInfo("mq rebalance: assign vgroup %d, from consumer %" PRId64 " to consumer %" PRId64 "",
                    pConsumerEp->vgId, pConsumerEp->oldConsumerId, pConsumerEp->consumerId);

              mndPersistRebalanceMsg(pMnode, pTrans, pConsumerEp, topic);
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
#endif

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

#if 0
    /*int32_t consumerVgNum = taosArrayGetSize(pEpInSub->vgs);*/
    if (imbCnt < imbConsumerNum) {
      imbCnt++;
      // push until equal minVg + 1
      while (taosArrayGetSize(pEpInSub->vgs) < minVgCnt + 1) {
        // iter hash and find one vg
        pRemovedIter = taosHashIterate(pHash, pRemovedIter);
        ASSERT(pRemovedIter);
        pRebVg = (SMqRebOutputVg *)pRemovedIter;
        // push
        taosArrayPush(pEpInSub->vgs, &pRebVg->pVgEp);
        pRebVg->newConsumerId = pEpInSub->consumerId;
        taosArrayPush(pOutput->rebVgs, pRebVg);
      }
    } else {
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
#endif
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
    if (mndPersistSubChangeVgReq(pMnode, pTrans, pOutput->pSub->key, pRebVg) < 0) {
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

static int32_t mndPersistMqSetConnReq(SMnode *pMnode, STrans *pTrans, const SMqTopicObj *pTopic, const char *cgroup,
                                      const SMqConsumerEp *pConsumerEp) {
  ASSERT(pConsumerEp->oldConsumerId == -1);
  int32_t vgId = pConsumerEp->vgId;

  SMqSetCVgReq req = {
      .vgId = vgId,
      .consumerId = pConsumerEp->consumerId,
      .sql = pTopic->sql,
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
