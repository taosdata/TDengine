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
#include "mndConsumer.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndSubscribe.h"
#include "mndTopic.h"
#include "mndTrans.h"
#include "tcompare.h"
#include "tname.h"

#define MND_CONSUMER_VER_NUMBER   1
#define MND_CONSUMER_RESERVE_SIZE 64

#define MND_CONSUMER_LOST_HB_CNT          6
#define MND_CONSUMER_LOST_CLEAR_THRESHOLD 43200

static int32_t mqRebInExecCnt = 0;

static const char *mndConsumerStatusName(int status);

static int32_t mndConsumerActionInsert(SSdb *pSdb, SMqConsumerObj *pConsumer);
static int32_t mndConsumerActionDelete(SSdb *pSdb, SMqConsumerObj *pConsumer);
static int32_t mndConsumerActionUpdate(SSdb *pSdb, SMqConsumerObj *pOldConsumer, SMqConsumerObj *pNewConsumer);
static int32_t mndProcessConsumerMetaMsg(SRpcMsg *pMsg);
static int32_t mndRetrieveConsumer(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextConsumer(SMnode *pMnode, void *pIter);

static int32_t mndProcessSubscribeReq(SRpcMsg *pMsg);
static int32_t mndProcessAskEpReq(SRpcMsg *pMsg);
static int32_t mndProcessMqHbReq(SRpcMsg *pMsg);
static int32_t mndProcessMqTimerMsg(SRpcMsg *pMsg);
static int32_t mndProcessConsumerLostMsg(SRpcMsg *pMsg);
static int32_t mndProcessConsumerClearMsg(SRpcMsg *pMsg);
static int32_t mndProcessConsumerRecoverMsg(SRpcMsg *pMsg);

int32_t mndInitConsumer(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_CONSUMER,
      .keyType = SDB_KEY_INT64,
      .encodeFp = (SdbEncodeFp)mndConsumerActionEncode,
      .decodeFp = (SdbDecodeFp)mndConsumerActionDecode,
      .insertFp = (SdbInsertFp)mndConsumerActionInsert,
      .updateFp = (SdbUpdateFp)mndConsumerActionUpdate,
      .deleteFp = (SdbDeleteFp)mndConsumerActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_TMQ_SUBSCRIBE, mndProcessSubscribeReq);
  mndSetMsgHandle(pMnode, TDMT_MND_TMQ_HB, mndProcessMqHbReq);
  mndSetMsgHandle(pMnode, TDMT_MND_TMQ_ASK_EP, mndProcessAskEpReq);
  mndSetMsgHandle(pMnode, TDMT_MND_TMQ_TIMER, mndProcessMqTimerMsg);
  mndSetMsgHandle(pMnode, TDMT_MND_TMQ_CONSUMER_LOST, mndProcessConsumerLostMsg);
  mndSetMsgHandle(pMnode, TDMT_MND_TMQ_CONSUMER_RECOVER, mndProcessConsumerRecoverMsg);
  mndSetMsgHandle(pMnode, TDMT_MND_TMQ_LOST_CONSUMER_CLEAR, mndProcessConsumerClearMsg);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_CONSUMERS, mndRetrieveConsumer);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_CONSUMERS, mndCancelGetNextConsumer);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupConsumer(SMnode *pMnode) {}

bool mndRebTryStart() {
  int32_t old = atomic_val_compare_exchange_32(&mqRebInExecCnt, 0, 1);
  mDebug("tq timer, rebalance counter old val:%d", old);
  return old == 0;
}

void mndRebEnd() { mndRebCntDec(); }

void mndRebCntInc() {
  int32_t val = atomic_add_fetch_32(&mqRebInExecCnt, 1);
  mInfo("rebalance trans start, rebalance counter:%d", val);
}

void mndRebCntDec() {
  while (1) {
    int32_t val = atomic_load_32(&mqRebInExecCnt);
    if (val <= 0) {
      mError("rebalance trans end, rebalance counter:%d should not be less equalled than 0, ignore counter desc", val);
      break;
    }

    int32_t newVal = val - 1;
    int32_t oldVal = atomic_val_compare_exchange_32(&mqRebInExecCnt, val, newVal);
    if (oldVal == val) {
      mDebug("rebalance trans end, rebalance counter:%d", newVal);
      break;
    }
  }
}

static int32_t mndProcessConsumerLostMsg(SRpcMsg *pMsg) {
  SMnode             *pMnode = pMsg->info.node;
  SMqConsumerLostMsg *pLostMsg = pMsg->pCont;
  SMqConsumerObj     *pConsumer = mndAcquireConsumer(pMnode, pLostMsg->consumerId);
  if (pConsumer == NULL) {
    return 0;
  }

  mInfo("process consumer lost msg, consumer:0x%" PRIx64 " status:%d(%s)", pLostMsg->consumerId, pConsumer->status,
        mndConsumerStatusName(pConsumer->status));

  if (pConsumer->status != MQ_CONSUMER_STATUS__READY) {
    mndReleaseConsumer(pMnode, pConsumer);
    return -1;
  }

  SMqConsumerObj *pConsumerNew = tNewSMqConsumerObj(pConsumer->consumerId, pConsumer->cgroup);
  pConsumerNew->updateType = CONSUMER_UPDATE__LOST;

  mndReleaseConsumer(pMnode, pConsumer);

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pMsg, "lost-csm");
  if (pTrans == NULL) {
    goto FAIL;
  }

  if (mndSetConsumerCommitLogs(pMnode, pTrans, pConsumerNew) != 0) {
    goto FAIL;
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    goto FAIL;
  }

  tDeleteSMqConsumerObj(pConsumerNew);
  taosMemoryFree(pConsumerNew);
  mndTransDrop(pTrans);
  return 0;
FAIL:
  tDeleteSMqConsumerObj(pConsumerNew);
  taosMemoryFree(pConsumerNew);
  mndTransDrop(pTrans);
  return -1;
}

static int32_t mndProcessConsumerRecoverMsg(SRpcMsg *pMsg) {
  SMnode                *pMnode = pMsg->info.node;
  SMqConsumerRecoverMsg *pRecoverMsg = pMsg->pCont;
  SMqConsumerObj        *pConsumer = mndAcquireConsumer(pMnode, pRecoverMsg->consumerId);
  if (pConsumer == NULL) {
    mError("cannot find consumer %" PRId64 " when processing consumer recover msg", pRecoverMsg->consumerId);
    return -1;
  }

  mInfo("receive consumer recover msg, consumer:0x%" PRIx64 " status:%d(%s)", pRecoverMsg->consumerId,
        pConsumer->status, mndConsumerStatusName(pConsumer->status));

  if (pConsumer->status != MQ_CONSUMER_STATUS__LOST_REBD) {
    mndReleaseConsumer(pMnode, pConsumer);
    terrno = TSDB_CODE_MND_CONSUMER_NOT_READY;
    return -1;
  }

  SMqConsumerObj *pConsumerNew = tNewSMqConsumerObj(pConsumer->consumerId, pConsumer->cgroup);
  pConsumerNew->updateType = CONSUMER_UPDATE__RECOVER;

  mndReleaseConsumer(pMnode, pConsumer);

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pMsg, "recover-csm");
  if (pTrans == NULL) {
    goto FAIL;
  }

  if (mndSetConsumerCommitLogs(pMnode, pTrans, pConsumerNew) != 0) goto FAIL;
  if (mndTransPrepare(pMnode, pTrans) != 0) goto FAIL;

  tDeleteSMqConsumerObj(pConsumerNew);
  taosMemoryFree(pConsumerNew);
  mndTransDrop(pTrans);
  return 0;
FAIL:
  tDeleteSMqConsumerObj(pConsumerNew);
  taosMemoryFree(pConsumerNew);
  mndTransDrop(pTrans);
  return -1;
}

// todo check the clear process
static int32_t mndProcessConsumerClearMsg(SRpcMsg *pMsg) {
  SMnode              *pMnode = pMsg->info.node;
  SMqConsumerClearMsg *pClearMsg = pMsg->pCont;

  SMqConsumerObj *pConsumer = mndAcquireConsumer(pMnode, pClearMsg->consumerId);
  if (pConsumer == NULL) {
    mError("consumer:0x%"PRIx64" failed to be found to clear it", pClearMsg->consumerId);
    return 0;
  }

  mInfo("consumer:0x%" PRIx64 " needs to be cleared, status %s", pClearMsg->consumerId,
        mndConsumerStatusName(pConsumer->status));

  if (pConsumer->status != MQ_CONSUMER_STATUS__LOST_REBD) {
    mndReleaseConsumer(pMnode, pConsumer);
    return -1;
  }

  SMqConsumerObj *pConsumerNew = tNewSMqConsumerObj(pConsumer->consumerId, pConsumer->cgroup);
  pConsumerNew->updateType = CONSUMER_UPDATE__LOST;

  mndReleaseConsumer(pMnode, pConsumer);

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pMsg, "clear-csm");
  if (pTrans == NULL) goto FAIL;

  // this is the drop action, not the update action
  if (mndSetConsumerDropLogs(pMnode, pTrans, pConsumerNew) != 0) goto FAIL;
  if (mndTransPrepare(pMnode, pTrans) != 0) goto FAIL;

  tDeleteSMqConsumerObj(pConsumerNew);
  taosMemoryFree(pConsumerNew);
  mndTransDrop(pTrans);
  return 0;

FAIL:
  tDeleteSMqConsumerObj(pConsumerNew);
  taosMemoryFree(pConsumerNew);
  mndTransDrop(pTrans);
  return -1;
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

static void freeRebalanceItem(void *param) {
  SMqRebInfo *pInfo = param;
  taosArrayDestroy(pInfo->newConsumers);
  taosArrayDestroy(pInfo->removedConsumers);
}

static int32_t mndProcessMqTimerMsg(SRpcMsg *pMsg) {
  SMnode         *pMnode = pMsg->info.node;
  SSdb           *pSdb = pMnode->pSdb;
  SMqConsumerObj *pConsumer;
  void           *pIter = NULL;

  mDebug("start to process mq timer");

  // rebalance cannot be parallel
  if (!mndRebTryStart()) {
    mDebug("mq rebalance already in progress, do nothing");
    return 0;
  }

  SMqDoRebalanceMsg *pRebMsg = rpcMallocCont(sizeof(SMqDoRebalanceMsg));
  if (pRebMsg == NULL) {
    mError("failed to create the rebalance msg, size:%d, quit mq timer", (int32_t)sizeof(SMqDoRebalanceMsg));
    mndRebEnd();
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pRebMsg->rebSubHash = taosHashInit(64, MurmurHash3_32, true, HASH_NO_LOCK);
  if (pRebMsg->rebSubHash == NULL) {
    mError("failed to create rebalance hashmap");
    rpcFreeCont(pRebMsg);
    mndRebEnd();
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  taosHashSetFreeFp(pRebMsg->rebSubHash, freeRebalanceItem);

  // iterate all consumers, find all modification
  while (1) {
    pIter = sdbFetch(pSdb, SDB_CONSUMER, pIter, (void **)&pConsumer);
    if (pIter == NULL) {
      break;
    }

    int32_t hbStatus = atomic_add_fetch_32(&pConsumer->hbStatus, 1);
    int32_t status = atomic_load_32(&pConsumer->status);

    mDebug("check for consumer:0x%" PRIx64 " status:%d(%s), sub-time:%" PRId64 ", uptime:%" PRId64 ", hbstatus:%d",
           pConsumer->consumerId, status, mndConsumerStatusName(status), pConsumer->subscribeTime, pConsumer->upTime,
           hbStatus);

    if (status == MQ_CONSUMER_STATUS__READY) {
      if (hbStatus > MND_CONSUMER_LOST_HB_CNT) {
        SMqConsumerLostMsg *pLostMsg = rpcMallocCont(sizeof(SMqConsumerLostMsg));
        if (pLostMsg == NULL) {
          mError("consumer:0x%"PRIx64" failed to transfer consumer status to lost due to out of memory. alloc size:%d",
              pConsumer->consumerId, (int32_t)sizeof(SMqConsumerLostMsg));
          continue;
        }

        pLostMsg->consumerId = pConsumer->consumerId;
        SRpcMsg rpcMsg = {
            .msgType = TDMT_MND_TMQ_CONSUMER_LOST, .pCont = pLostMsg, .contLen = sizeof(SMqConsumerLostMsg)};

        mDebug("consumer:0x%"PRIx64" hb not received beyond threshold %d, set to lost", pConsumer->consumerId,
            MND_CONSUMER_LOST_HB_CNT);
        tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg);
      }
    } else if (status == MQ_CONSUMER_STATUS__LOST_REBD) {
      // if the client is lost longer than one day, clear it. Otherwise, do nothing about the lost consumers.
      if (hbStatus > MND_CONSUMER_LOST_CLEAR_THRESHOLD) {
        SMqConsumerClearMsg *pClearMsg = rpcMallocCont(sizeof(SMqConsumerClearMsg));
        if (pClearMsg == NULL) {
          mError("consumer:0x%"PRIx64" failed to clear consumer due to out of memory. alloc size:%d",
                 pConsumer->consumerId, (int32_t)sizeof(SMqConsumerClearMsg));
          continue;
        }

        pClearMsg->consumerId = pConsumer->consumerId;
        SRpcMsg rpcMsg = {
            .msgType = TDMT_MND_TMQ_LOST_CONSUMER_CLEAR, .pCont = pClearMsg, .contLen = sizeof(SMqConsumerClearMsg)};

        mDebug("consumer:0x%" PRIx64 " lost beyond threshold %d, clear it", pConsumer->consumerId,
               MND_CONSUMER_LOST_CLEAR_THRESHOLD);
        tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg);
      }
    } else if (status == MQ_CONSUMER_STATUS__LOST) {
      taosRLockLatch(&pConsumer->lock);
      int32_t topicNum = taosArrayGetSize(pConsumer->currentTopics);
      for (int32_t i = 0; i < topicNum; i++) {
        char  key[TSDB_SUBSCRIBE_KEY_LEN];
        char *removedTopic = taosArrayGetP(pConsumer->currentTopics, i);
        mndMakeSubscribeKey(key, pConsumer->cgroup, removedTopic);
        SMqRebInfo *pRebSub = mndGetOrCreateRebSub(pRebMsg->rebSubHash, key);
        taosArrayPush(pRebSub->removedConsumers, &pConsumer->consumerId);
      }
      taosRUnLockLatch(&pConsumer->lock);
    } else {  // MQ_CONSUMER_STATUS_REBALANCE
      taosRLockLatch(&pConsumer->lock);

      int32_t newTopicNum = taosArrayGetSize(pConsumer->rebNewTopics);
      for (int32_t i = 0; i < newTopicNum; i++) {
        char  key[TSDB_SUBSCRIBE_KEY_LEN];
        char *newTopic = taosArrayGetP(pConsumer->rebNewTopics, i);
        mndMakeSubscribeKey(key, pConsumer->cgroup, newTopic);
        SMqRebInfo *pRebSub = mndGetOrCreateRebSub(pRebMsg->rebSubHash, key);
        taosArrayPush(pRebSub->newConsumers, &pConsumer->consumerId);
      }

      int32_t removedTopicNum = taosArrayGetSize(pConsumer->rebRemovedTopics);
      for (int32_t i = 0; i < removedTopicNum; i++) {
        char  key[TSDB_SUBSCRIBE_KEY_LEN];
        char *removedTopic = taosArrayGetP(pConsumer->rebRemovedTopics, i);
        mndMakeSubscribeKey(key, pConsumer->cgroup, removedTopic);
        SMqRebInfo *pRebSub = mndGetOrCreateRebSub(pRebMsg->rebSubHash, key);
        taosArrayPush(pRebSub->removedConsumers, &pConsumer->consumerId);
      }
      taosRUnLockLatch(&pConsumer->lock);
    }

    mndReleaseConsumer(pMnode, pConsumer);
  }

  if (taosHashGetSize(pRebMsg->rebSubHash) != 0) {
    mInfo("mq rebalance will be triggered");
    SRpcMsg rpcMsg = {
        .msgType = TDMT_MND_TMQ_DO_REBALANCE,
        .pCont = pRebMsg,
        .contLen = sizeof(SMqDoRebalanceMsg),
    };
    tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg);
  } else {
    taosHashCleanup(pRebMsg->rebSubHash);
    rpcFreeCont(pRebMsg);
    mDebug("mq timer finished, no need to re-balance");
    mndRebEnd();
  }
  return 0;
}

static int32_t mndProcessMqHbReq(SRpcMsg *pMsg) {
  SMnode  *pMnode = pMsg->info.node;
  SMqHbReq req = {0};

  if (tDeserializeSMqHbReq(pMsg->pCont, pMsg->contLen, &req) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  int64_t         consumerId = req.consumerId;
  SMqConsumerObj *pConsumer = mndAcquireConsumer(pMnode, consumerId);
  if (pConsumer == NULL) {
    mError("consumer:0x%" PRIx64 " not exist", consumerId);
    terrno = TSDB_CODE_MND_CONSUMER_NOT_EXIST;
    return -1;
  }

  atomic_store_32(&pConsumer->hbStatus, 0);

  int32_t status = atomic_load_32(&pConsumer->status);

  if (status == MQ_CONSUMER_STATUS__LOST_REBD) {
    mInfo("try to recover consumer:0x%" PRIx64 "", consumerId);
    SMqConsumerRecoverMsg *pRecoverMsg = rpcMallocCont(sizeof(SMqConsumerRecoverMsg));

    pRecoverMsg->consumerId = consumerId;
    SRpcMsg pRpcMsg = {
        .msgType = TDMT_MND_TMQ_CONSUMER_RECOVER,
        .pCont = pRecoverMsg,
        .contLen = sizeof(SMqConsumerRecoverMsg),
    };
    tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &pRpcMsg);
  }

  mndReleaseConsumer(pMnode, pConsumer);

  return 0;
}

static int32_t mndProcessAskEpReq(SRpcMsg *pMsg) {
  SMnode     *pMnode = pMsg->info.node;
  SMqAskEpReq req = {0};
  SMqAskEpRsp rsp = {0};

  if (tDeserializeSMqAskEpReq(pMsg->pCont, pMsg->contLen, &req) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  int64_t consumerId = req.consumerId;
  int32_t epoch = req.epoch;

  SMqConsumerObj *pConsumer = mndAcquireConsumer(pMnode, consumerId);
  if (pConsumer == NULL) {
    mError("consumer:0x%" PRIx64 " group:%s not exists in sdb", consumerId, req.cgroup);
    terrno = TSDB_CODE_MND_CONSUMER_NOT_EXIST;
    return -1;
  }

  int32_t ret = strncmp(req.cgroup, pConsumer->cgroup, tListLen(pConsumer->cgroup));
  if (ret != 0) {
    mError("consumer:0x%" PRIx64 " group:%s not consistent with data in sdb, saved cgroup:%s", consumerId, req.cgroup,
           pConsumer->cgroup);
    terrno = TSDB_CODE_MND_CONSUMER_NOT_EXIST;
    return -1;
  }

  atomic_store_32(&pConsumer->hbStatus, 0);

  // 1. check consumer status
  int32_t status = atomic_load_32(&pConsumer->status);

  if (status == MQ_CONSUMER_STATUS__LOST_REBD) {
    mInfo("try to recover consumer:0x%" PRIx64, consumerId);
    SMqConsumerRecoverMsg *pRecoverMsg = rpcMallocCont(sizeof(SMqConsumerRecoverMsg));

    pRecoverMsg->consumerId = consumerId;
    SRpcMsg pRpcMsg = {
        .msgType = TDMT_MND_TMQ_CONSUMER_RECOVER,
        .pCont = pRecoverMsg,
        .contLen = sizeof(SMqConsumerRecoverMsg),
    };

    tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &pRpcMsg);
  }

  if (status != MQ_CONSUMER_STATUS__READY) {
    mInfo("consumer:0x%" PRIx64 " not ready, status: %s", consumerId, mndConsumerStatusName(status));
    terrno = TSDB_CODE_MND_CONSUMER_NOT_READY;
    return -1;
  }

  int32_t serverEpoch = atomic_load_32(&pConsumer->epoch);

  // 2. check epoch, only send ep info when epochs do not match
  if (epoch != serverEpoch) {
    taosRLockLatch(&pConsumer->lock);
    mInfo("process ask ep, consumer:0x%" PRIx64 "(epoch %d) update with server epoch %d", consumerId, epoch,
          serverEpoch);
    int32_t numOfTopics = taosArrayGetSize(pConsumer->currentTopics);

    rsp.topics = taosArrayInit(numOfTopics, sizeof(SMqSubTopicEp));
    if (rsp.topics == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      taosRUnLockLatch(&pConsumer->lock);
      goto FAIL;
    }

    // handle all topics subscribed by this consumer
    for (int32_t i = 0; i < numOfTopics; i++) {
      char            *topic = taosArrayGetP(pConsumer->currentTopics, i);
      SMqSubscribeObj *pSub = mndAcquireSubscribe(pMnode, pConsumer->cgroup, topic);
      // txn guarantees pSub is created

      taosRLockLatch(&pSub->lock);

      SMqSubTopicEp topicEp = {0};
      strcpy(topicEp.topic, topic);

      // 2.1 fetch topic schema
      SMqTopicObj *pTopic = mndAcquireTopic(pMnode, topic);
      taosRLockLatch(&pTopic->lock);
      tstrncpy(topicEp.db, pTopic->db, TSDB_DB_FNAME_LEN);
      topicEp.schema.nCols = pTopic->schema.nCols;
      if (topicEp.schema.nCols) {
        topicEp.schema.pSchema = taosMemoryCalloc(topicEp.schema.nCols, sizeof(SSchema));
        memcpy(topicEp.schema.pSchema, pTopic->schema.pSchema, topicEp.schema.nCols * sizeof(SSchema));
      }
      taosRUnLockLatch(&pTopic->lock);
      mndReleaseTopic(pMnode, pTopic);

      // 2.2 iterate all vg assigned to the consumer of that topic
      SMqConsumerEp *pConsumerEp = taosHashGet(pSub->consumerHash, &consumerId, sizeof(int64_t));
      int32_t        vgNum = taosArrayGetSize(pConsumerEp->vgs);

      // this customer assigned vgroups
      topicEp.vgs = taosArrayInit(vgNum, sizeof(SMqSubVgEp));
      if (topicEp.vgs == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        taosRUnLockLatch(&pConsumer->lock);
        taosRUnLockLatch(&pSub->lock);
        mndReleaseSubscribe(pMnode, pSub);
        goto FAIL;
      }

      for (int32_t j = 0; j < vgNum; j++) {
        SMqVgEp *pVgEp = taosArrayGetP(pConsumerEp->vgs, j);
        char     offsetKey[TSDB_PARTITION_KEY_LEN];
        mndMakePartitionKey(offsetKey, pConsumer->cgroup, topic, pVgEp->vgId);
        // 2.2.1 build vg ep
        SMqSubVgEp vgEp = {
            .epSet = pVgEp->epSet,
            .vgId = pVgEp->vgId,
            .offset = -1,
        };

        taosArrayPush(topicEp.vgs, &vgEp);
      }
      taosArrayPush(rsp.topics, &topicEp);

      taosRUnLockLatch(&pSub->lock);
      mndReleaseSubscribe(pMnode, pSub);
    }
    taosRUnLockLatch(&pConsumer->lock);
  }

  // encode rsp
  int32_t tlen = sizeof(SMqRspHead) + tEncodeSMqAskEpRsp(NULL, &rsp);
  void   *buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  SMqRspHead* pHead = buf;

  pHead->mqMsgType = TMQ_MSG_TYPE__EP_RSP;
  pHead->epoch = serverEpoch;
  pHead->consumerId = pConsumer->consumerId;
  pHead->walsver = 0;
  pHead->walever = 0;


  void *abuf = POINTER_SHIFT(buf, sizeof(SMqRspHead));
  tEncodeSMqAskEpRsp(&abuf, &rsp);

  // release consumer and free memory
  tDeleteSMqAskEpRsp(&rsp);
  mndReleaseConsumer(pMnode, pConsumer);

  // send rsp
  pMsg->info.rsp = buf;
  pMsg->info.rspLen = tlen;
  return 0;

FAIL:
  tDeleteSMqAskEpRsp(&rsp);
  mndReleaseConsumer(pMnode, pConsumer);
  return -1;
}

int32_t mndSetConsumerDropLogs(SMnode *pMnode, STrans *pTrans, SMqConsumerObj *pConsumer) {
  SSdbRaw *pCommitRaw = mndConsumerActionEncode(pConsumer);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED) != 0) return -1;
  return 0;
}

int32_t mndSetConsumerCommitLogs(SMnode *pMnode, STrans *pTrans, SMqConsumerObj *pConsumer) {
  SSdbRaw *pCommitRaw = mndConsumerActionEncode(pConsumer);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY) != 0) return -1;
  return 0;
}

static int32_t validateTopics(const SArray *pTopicList, SMnode *pMnode, const char *pUser) {
  int32_t numOfTopics = taosArrayGetSize(pTopicList);

  for (int32_t i = 0; i < numOfTopics; i++) {
    char        *pOneTopic = taosArrayGetP(pTopicList, i);
    SMqTopicObj *pTopic = mndAcquireTopic(pMnode, pOneTopic);
    if (pTopic == NULL) {  // terrno has been set by callee function
      return -1;
    }

    if (mndCheckTopicPrivilege(pMnode, pUser, MND_OPER_SUBSCRIBE, pTopic) != 0) {
      mndReleaseTopic(pMnode, pTopic);
      return -1;
    }

    mndReleaseTopic(pMnode, pTopic);
  }

  return 0;
}

static void *topicNameDup(void *p) { return taosStrdup((char *)p); }

static void freeItem(void *param) {
  void *pItem = *(void **)param;
  if (pItem != NULL) {
    taosMemoryFree(pItem);
  }
}

int32_t mndProcessSubscribeReq(SRpcMsg *pMsg) {
  SMnode *pMnode = pMsg->info.node;
  char   *msgStr = pMsg->pCont;

  SCMSubscribeReq subscribe = {0};
  tDeserializeSCMSubscribeReq(msgStr, &subscribe);

  uint64_t        consumerId = subscribe.consumerId;
  char           *cgroup = subscribe.cgroup;
  SMqConsumerObj *pExistedConsumer = NULL;
  SMqConsumerObj *pConsumerNew = NULL;

  int32_t code = -1;
  SArray *pTopicList = subscribe.topicNames;
  taosArraySort(pTopicList, taosArrayCompareString);
  taosArrayRemoveDuplicate(pTopicList, taosArrayCompareString, freeItem);

  int32_t newTopicNum = taosArrayGetSize(pTopicList);

  // check topic existence
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pMsg, "subscribe");
  if (pTrans == NULL) {
    goto _over;
  }

  code = validateTopics(pTopicList, pMnode, pMsg->info.conn.user);
  if (code != TSDB_CODE_SUCCESS) {
    goto _over;
  }

  pExistedConsumer = mndAcquireConsumer(pMnode, consumerId);
  if (pExistedConsumer == NULL) {
    mInfo("receive subscribe request from new consumer:0x%" PRIx64 " cgroup:%s, numOfTopics:%d", consumerId,
          subscribe.cgroup, (int32_t)taosArrayGetSize(pTopicList));

    pConsumerNew = tNewSMqConsumerObj(consumerId, cgroup);
    tstrncpy(pConsumerNew->clientId, subscribe.clientId, tListLen(pConsumerNew->clientId));

    // set the update type
    pConsumerNew->updateType = CONSUMER_UPDATE__REBALANCE;
    taosArrayDestroy(pConsumerNew->assignedTopics);
    pConsumerNew->assignedTopics = taosArrayDup(pTopicList, topicNameDup);

    // all subscribed topics should re-balance.
    taosArrayDestroy(pConsumerNew->rebNewTopics);
    pConsumerNew->rebNewTopics = pTopicList;
    subscribe.topicNames = NULL;

    if (mndSetConsumerCommitLogs(pMnode, pTrans, pConsumerNew) != 0) goto _over;
    if (mndTransPrepare(pMnode, pTrans) != 0) goto _over;

  } else {
    int32_t status = atomic_load_32(&pExistedConsumer->status);

    mInfo("receive subscribe request from existed consumer:0x%" PRIx64
          " cgroup:%s, current status:%d(%s), subscribe topic num: %d",
          consumerId, subscribe.cgroup, status, mndConsumerStatusName(status), newTopicNum);

    if (status != MQ_CONSUMER_STATUS__READY) {
      terrno = TSDB_CODE_MND_CONSUMER_NOT_READY;
      goto _over;
    }

    pConsumerNew = tNewSMqConsumerObj(consumerId, cgroup);
    if (pConsumerNew == NULL) {
      goto _over;
    }

    // set the update type
    pConsumerNew->updateType = CONSUMER_UPDATE__REBALANCE;
    taosArrayDestroy(pConsumerNew->assignedTopics);
    pConsumerNew->assignedTopics = taosArrayDup(pTopicList, topicNameDup);

    int32_t oldTopicNum = (pExistedConsumer->currentTopics) ? taosArrayGetSize(pExistedConsumer->currentTopics) : 0;

    int32_t i = 0, j = 0;
    while (i < oldTopicNum || j < newTopicNum) {
      if (i >= oldTopicNum) {
        char *newTopicCopy = taosStrdup(taosArrayGetP(pTopicList, j));
        taosArrayPush(pConsumerNew->rebNewTopics, &newTopicCopy);
        j++;
        continue;
      } else if (j >= newTopicNum) {
        char *oldTopicCopy = taosStrdup(taosArrayGetP(pExistedConsumer->currentTopics, i));
        taosArrayPush(pConsumerNew->rebRemovedTopics, &oldTopicCopy);
        i++;
        continue;
      } else {
        char *oldTopic = taosArrayGetP(pExistedConsumer->currentTopics, i);
        char *newTopic = taosArrayGetP(pTopicList, j);
        int   comp = strcmp(oldTopic, newTopic);
        if (comp == 0) {
          i++;
          j++;
          continue;
        } else if (comp < 0) {
          char *oldTopicCopy = taosStrdup(oldTopic);
          taosArrayPush(pConsumerNew->rebRemovedTopics, &oldTopicCopy);
          i++;
          continue;
        } else {
          char *newTopicCopy = taosStrdup(newTopic);
          taosArrayPush(pConsumerNew->rebNewTopics, &newTopicCopy);
          j++;
          continue;
        }
      }
    }

    // no topics need to be rebalanced
    if (taosArrayGetSize(pConsumerNew->rebNewTopics) == 0 && taosArrayGetSize(pConsumerNew->rebRemovedTopics) == 0) {
      goto _over;
    }

    if (mndSetConsumerCommitLogs(pMnode, pTrans, pConsumerNew) != 0) goto _over;
    if (mndTransPrepare(pMnode, pTrans) != 0) goto _over;
  }

  code = TSDB_CODE_ACTION_IN_PROGRESS;

_over:
  mndTransDrop(pTrans);

  if (pExistedConsumer) {
    /*taosRUnLockLatch(&pExistedConsumer->lock);*/
    mndReleaseConsumer(pMnode, pExistedConsumer);
  }

  if (pConsumerNew) {
    tDeleteSMqConsumerObj(pConsumerNew);
    taosMemoryFree(pConsumerNew);
  }

  // TODO: replace with destroy subscribe msg
  taosArrayDestroyP(subscribe.topicNames, (FDelete)taosMemoryFree);
  return code;
}

SSdbRaw *mndConsumerActionEncode(SMqConsumerObj *pConsumer) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  void   *buf = NULL;
  int32_t tlen = tEncodeSMqConsumerObj(NULL, pConsumer);
  int32_t size = sizeof(int32_t) + tlen + MND_CONSUMER_RESERVE_SIZE;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_CONSUMER, MND_CONSUMER_VER_NUMBER, size);
  if (pRaw == NULL) goto CM_ENCODE_OVER;

  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) goto CM_ENCODE_OVER;

  void *abuf = buf;
  tEncodeSMqConsumerObj(&abuf, pConsumer);

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, tlen, CM_ENCODE_OVER);
  SDB_SET_BINARY(pRaw, dataPos, buf, tlen, CM_ENCODE_OVER);
  SDB_SET_RESERVE(pRaw, dataPos, MND_CONSUMER_RESERVE_SIZE, CM_ENCODE_OVER);
  SDB_SET_DATALEN(pRaw, dataPos, CM_ENCODE_OVER);

  terrno = TSDB_CODE_SUCCESS;

CM_ENCODE_OVER:
  taosMemoryFreeClear(buf);
  if (terrno != 0) {
    mError("consumer:0x%" PRIx64 " failed to encode to raw:%p since %s", pConsumer->consumerId, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("consumer:0x%" PRIx64 ", encode to raw:%p, row:%p", pConsumer->consumerId, pRaw, pConsumer);
  return pRaw;
}

SSdbRow *mndConsumerActionDecode(SSdbRaw *pRaw) {
  SSdbRow        *pRow = NULL;
  SMqConsumerObj *pConsumer = NULL;
  void           *buf = NULL;

  terrno = 0;
  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    goto CM_DECODE_OVER;
  }

  if (sver != MND_CONSUMER_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto CM_DECODE_OVER;
  }

  pRow = sdbAllocRow(sizeof(SMqConsumerObj));
  if (pRow == NULL) {
    goto CM_DECODE_OVER;
  }

  pConsumer = sdbGetRowObj(pRow);
  if (pConsumer == NULL) {
    goto CM_DECODE_OVER;
  }

  int32_t dataPos = 0;
  int32_t len;
  SDB_GET_INT32(pRaw, dataPos, &len, CM_DECODE_OVER);
  buf = taosMemoryMalloc(len);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto CM_DECODE_OVER;
  }

  SDB_GET_BINARY(pRaw, dataPos, buf, len, CM_DECODE_OVER);
  SDB_GET_RESERVE(pRaw, dataPos, MND_CONSUMER_RESERVE_SIZE, CM_DECODE_OVER);

  if (tDecodeSMqConsumerObj(buf, pConsumer) == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;  // TODO set correct error code
    goto CM_DECODE_OVER;
  }

  tmsgUpdateDnodeEpSet(&pConsumer->ep);

CM_DECODE_OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("consumer:0x%" PRIx64 " failed to decode from raw:%p since %s",
           pConsumer == NULL ? 0 : pConsumer->consumerId, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
  }

  return pRow;
}

static int32_t mndConsumerActionInsert(SSdb *pSdb, SMqConsumerObj *pConsumer) {
  mDebug("consumer:0x%" PRIx64 " cgroup:%s status:%d(%s) epoch:%d load from sdb, perform insert action",
         pConsumer->consumerId, pConsumer->cgroup, pConsumer->status, mndConsumerStatusName(pConsumer->status),
         pConsumer->epoch);
  pConsumer->subscribeTime = pConsumer->upTime;
  return 0;
}

static int32_t mndConsumerActionDelete(SSdb *pSdb, SMqConsumerObj *pConsumer) {
  mDebug("consumer:0x%" PRIx64 " perform delete action, status:(%d)%s", pConsumer->consumerId, pConsumer->status,
         mndConsumerStatusName(pConsumer->status));
  tDeleteSMqConsumerObj(pConsumer);
  return 0;
}

static void updateConsumerStatus(SMqConsumerObj *pConsumer) {
  int32_t status = pConsumer->status;

  if (taosArrayGetSize(pConsumer->rebNewTopics) == 0 && taosArrayGetSize(pConsumer->rebRemovedTopics) == 0) {
    if (status == MQ_CONSUMER_STATUS_REBALANCE) {
      pConsumer->status = MQ_CONSUMER_STATUS__READY;
    } else if (status == MQ_CONSUMER_STATUS__LOST) {
      ASSERT(taosArrayGetSize(pConsumer->currentTopics) == 0);
      pConsumer->status = MQ_CONSUMER_STATUS__LOST_REBD;
    }
  }
}

// remove from new topic
static void removeFromNewTopicList(SMqConsumerObj *pConsumer, const char *pTopic) {
  int32_t size = taosArrayGetSize(pConsumer->rebNewTopics);
  for (int32_t i = 0; i < size; i++) {
    char *p = taosArrayGetP(pConsumer->rebNewTopics, i);
    if (strcmp(pTopic, p) == 0) {
      taosArrayRemove(pConsumer->rebNewTopics, i);
      taosMemoryFree(p);

      mDebug("consumer:0x%" PRIx64 " remove new topic:%s in the topic list, remain newTopics:%d", pConsumer->consumerId,
             pTopic, (int)taosArrayGetSize(pConsumer->rebNewTopics));
      break;
    }
  }
}

// remove from removed topic
static void removeFromRemoveTopicList(SMqConsumerObj *pConsumer, const char *pTopic) {
  int32_t size = taosArrayGetSize(pConsumer->rebRemovedTopics);
  for (int32_t i = 0; i < size; i++) {
    char *p = taosArrayGetP(pConsumer->rebRemovedTopics, i);
    if (strcmp(pTopic, p) == 0) {
      taosArrayRemove(pConsumer->rebRemovedTopics, i);
      taosMemoryFree(p);

      mDebug("consumer:0x%" PRIx64 " remove topic:%s in the removed topic list, remain removedTopics:%d",
             pConsumer->consumerId, pTopic, (int)taosArrayGetSize(pConsumer->rebRemovedTopics));
      break;
    }
  }
}

static void removeFromCurrentTopicList(SMqConsumerObj *pConsumer, const char *pTopic) {
  int32_t sz = taosArrayGetSize(pConsumer->currentTopics);
  for (int32_t i = 0; i < sz; i++) {
    char *topic = taosArrayGetP(pConsumer->currentTopics, i);
    if (strcmp(pTopic, topic) == 0) {
      taosArrayRemove(pConsumer->currentTopics, i);
      taosMemoryFree(topic);

      mDebug("consumer:0x%" PRIx64 " remove topic:%s in the current topic list, remain currentTopics:%d",
             pConsumer->consumerId, pTopic, (int)taosArrayGetSize(pConsumer->currentTopics));
      break;
    }
  }
}

static bool existInCurrentTopicList(const SMqConsumerObj* pConsumer, const char* pTopic) {
  bool    existing = false;
  int32_t size = taosArrayGetSize(pConsumer->currentTopics);
  for (int32_t i = 0; i < size; i++) {
    char *topic = taosArrayGetP(pConsumer->currentTopics, i);

    if (strcmp(topic, pTopic) == 0) {
      existing = true;
      break;
    }
  }

  return existing;
}

static int32_t mndConsumerActionUpdate(SSdb *pSdb, SMqConsumerObj *pOldConsumer, SMqConsumerObj *pNewConsumer) {
  mDebug("consumer:0x%" PRIx64 " perform update action, update type:%d, subscribe-time:%" PRId64 ", uptime:%" PRId64,
         pOldConsumer->consumerId, pNewConsumer->updateType, pOldConsumer->subscribeTime, pOldConsumer->upTime);

  taosWLockLatch(&pOldConsumer->lock);

  if (pNewConsumer->updateType == CONSUMER_UPDATE__REBALANCE) {
    TSWAP(pOldConsumer->rebNewTopics, pNewConsumer->rebNewTopics);
    TSWAP(pOldConsumer->rebRemovedTopics, pNewConsumer->rebRemovedTopics);
    TSWAP(pOldConsumer->assignedTopics, pNewConsumer->assignedTopics);

    pOldConsumer->subscribeTime = pNewConsumer->upTime;
    pOldConsumer->status = MQ_CONSUMER_STATUS_REBALANCE;
  } else if (pNewConsumer->updateType == CONSUMER_UPDATE__LOST) {
    int32_t sz = taosArrayGetSize(pOldConsumer->currentTopics);
    for (int32_t i = 0; i < sz; i++) {
      char *topic = taosStrdup(taosArrayGetP(pOldConsumer->currentTopics, i));
      taosArrayPush(pOldConsumer->rebRemovedTopics, &topic);
    }

    pOldConsumer->rebalanceTime = pNewConsumer->upTime;

    int32_t prevStatus = pOldConsumer->status;
    pOldConsumer->status = MQ_CONSUMER_STATUS__LOST;
    mDebug("consumer:0x%" PRIx64 " state %s -> %s, reb-time:%" PRId64 ", reb-removed-topics:%d",
           pOldConsumer->consumerId, mndConsumerStatusName(prevStatus), mndConsumerStatusName(pOldConsumer->status),
           pOldConsumer->rebalanceTime, (int)taosArrayGetSize(pOldConsumer->rebRemovedTopics));
  } else if (pNewConsumer->updateType == CONSUMER_UPDATE__RECOVER) {
    int32_t sz = taosArrayGetSize(pOldConsumer->assignedTopics);
    for (int32_t i = 0; i < sz; i++) {
      char *topic = taosStrdup(taosArrayGetP(pOldConsumer->assignedTopics, i));
      taosArrayPush(pOldConsumer->rebNewTopics, &topic);
    }

    pOldConsumer->rebalanceTime = pNewConsumer->upTime;
    pOldConsumer->status = MQ_CONSUMER_STATUS_REBALANCE;
  } else if (pNewConsumer->updateType == CONSUMER_UPDATE__TOUCH) {
    atomic_add_fetch_32(&pOldConsumer->epoch, 1);

    pOldConsumer->rebalanceTime = pNewConsumer->upTime;

  } else if (pNewConsumer->updateType == CONSUMER_UPDATE__ADD) {
    char *pNewTopic = taosStrdup(taosArrayGetP(pNewConsumer->rebNewTopics, 0));

    // check if exist in current topic
    removeFromNewTopicList(pOldConsumer, pNewTopic);

    // add to current topic
    bool existing = existInCurrentTopicList(pOldConsumer, pNewTopic);
    if (existing) {
      taosMemoryFree(pNewTopic);
    } else {  // added into current topic list
      taosArrayPush(pOldConsumer->currentTopics, &pNewTopic);
      taosArraySort(pOldConsumer->currentTopics, taosArrayCompareString);
    }

    // set status
    int32_t status = pOldConsumer->status;
    updateConsumerStatus(pOldConsumer);

    // the re-balance is triggered when the new consumer is launched.
    pOldConsumer->rebalanceTime = pNewConsumer->upTime;

    atomic_add_fetch_32(&pOldConsumer->epoch, 1);
    mDebug("consumer:0x%" PRIx64 " state (%d)%s -> (%d)%s, new epoch:%d, reb-time:%" PRId64
           ", current topics:%d, newTopics:%d, removeTopics:%d",
           pOldConsumer->consumerId, status, mndConsumerStatusName(status), pOldConsumer->status,
           mndConsumerStatusName(pOldConsumer->status), pOldConsumer->epoch, pOldConsumer->rebalanceTime,
           (int)taosArrayGetSize(pOldConsumer->currentTopics), (int)taosArrayGetSize(pOldConsumer->rebNewTopics),
           (int)taosArrayGetSize(pOldConsumer->rebRemovedTopics));

  } else if (pNewConsumer->updateType == CONSUMER_UPDATE__REMOVE) {
    char *removedTopic = taosArrayGetP(pNewConsumer->rebRemovedTopics, 0);

    // remove from removed topic
    removeFromRemoveTopicList(pOldConsumer, removedTopic);

    // remove from current topic
    removeFromCurrentTopicList(pOldConsumer, removedTopic);

    // set status
    int32_t status = pOldConsumer->status;
    updateConsumerStatus(pOldConsumer);

    pOldConsumer->rebalanceTime = pNewConsumer->upTime;
    atomic_add_fetch_32(&pOldConsumer->epoch, 1);

    mDebug("consumer:0x%" PRIx64 " state (%d)%s -> (%d)%s, new epoch:%d, reb-time:%" PRId64
           ", current topics:%d, newTopics:%d, removeTopics:%d",
           pOldConsumer->consumerId, status, mndConsumerStatusName(status), pOldConsumer->status,
           mndConsumerStatusName(pOldConsumer->status), pOldConsumer->epoch, pOldConsumer->rebalanceTime,
           (int)taosArrayGetSize(pOldConsumer->currentTopics), (int)taosArrayGetSize(pOldConsumer->rebNewTopics),
           (int)taosArrayGetSize(pOldConsumer->rebRemovedTopics));
  }

  taosWUnLockLatch(&pOldConsumer->lock);
  return 0;
}

SMqConsumerObj *mndAcquireConsumer(SMnode *pMnode, int64_t consumerId) {
  SSdb           *pSdb = pMnode->pSdb;
  SMqConsumerObj *pConsumer = sdbAcquire(pSdb, SDB_CONSUMER, &consumerId);
  if (pConsumer == NULL) {
    terrno = TSDB_CODE_MND_CONSUMER_NOT_EXIST;
  }
  return pConsumer;
}

void mndReleaseConsumer(SMnode *pMnode, SMqConsumerObj *pConsumer) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pConsumer);
}

static int32_t mndRetrieveConsumer(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rowsCapacity) {
  SMnode         *pMnode = pReq->info.node;
  SSdb           *pSdb = pMnode->pSdb;
  int32_t         numOfRows = 0;
  SMqConsumerObj *pConsumer = NULL;

  while (numOfRows < rowsCapacity) {
    pShow->pIter = sdbFetch(pSdb, SDB_CONSUMER, pShow->pIter, (void **)&pConsumer);
    if (pShow->pIter == NULL) {
      break;
    }

    if (taosArrayGetSize(pConsumer->assignedTopics) == 0) {
      mDebug("showing consumer:0x%" PRIx64 " no assigned topic, skip", pConsumer->consumerId);
      sdbRelease(pSdb, pConsumer);
      continue;
    }

    taosRLockLatch(&pConsumer->lock);
    mDebug("showing consumer:0x%" PRIx64, pConsumer->consumerId);

    int32_t topicSz = taosArrayGetSize(pConsumer->assignedTopics);
    bool    hasTopic = true;
    if (topicSz == 0) {
      hasTopic = false;
      topicSz = 1;
    }

    if (numOfRows + topicSz > rowsCapacity) {
      blockDataEnsureCapacity(pBlock, numOfRows + topicSz);
    }

    for (int32_t i = 0; i < topicSz; i++) {
      SColumnInfoData *pColInfo;
      int32_t          cols = 0;

      // consumer id
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)&pConsumer->consumerId, false);

      // consumer group
      char cgroup[TSDB_CGROUP_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_TO_VARSTR(cgroup, pConsumer->cgroup);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)cgroup, false);

      // client id
      char clientId[256 + VARSTR_HEADER_SIZE] = {0};
      STR_TO_VARSTR(clientId, pConsumer->clientId);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)clientId, false);

      // status
      char        status[20 + VARSTR_HEADER_SIZE] = {0};
      const char *pStatusName = mndConsumerStatusName(pConsumer->status);
      STR_TO_VARSTR(status, pStatusName);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)status, false);

      // one subscribed topic
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      if (hasTopic) {
        char        topic[TSDB_TOPIC_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
        const char *topicName = mndTopicGetShowName(taosArrayGetP(pConsumer->assignedTopics, i));
        STR_TO_VARSTR(topic, topicName);
        colDataSetVal(pColInfo, numOfRows, (const char *)topic, false);
      } else {
        colDataSetVal(pColInfo, numOfRows, NULL, true);
      }

      // end point
      /*pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);*/
      /*colDataSetVal(pColInfo, numOfRows, (const char *)&pConsumer->ep, true);*/

      // up time
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)&pConsumer->upTime, false);

      // subscribe time
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)&pConsumer->subscribeTime, false);

      // rebalance time
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)&pConsumer->rebalanceTime, pConsumer->rebalanceTime == 0);

      numOfRows++;
    }

    taosRUnLockLatch(&pConsumer->lock);
    sdbRelease(pSdb, pConsumer);

    pBlock->info.rows = numOfRows;
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextConsumer(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}

static const char *mndConsumerStatusName(int status) {
  switch (status) {
    case MQ_CONSUMER_STATUS__READY:
      return "ready";
    case MQ_CONSUMER_STATUS__LOST:
    case MQ_CONSUMER_STATUS__LOST_REBD:
      return "lost";
    case MQ_CONSUMER_STATUS_REBALANCE:
      return "rebalancing";
    default:
      return "unknown";
  }
}
