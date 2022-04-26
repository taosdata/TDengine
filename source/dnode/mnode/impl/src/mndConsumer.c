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
#include "mndAuth.h"
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

#define MND_CONSUMER_VER_NUMBER   1
#define MND_CONSUMER_RESERVE_SIZE 64

#define MND_CONSUMER_LOST_HB_CNT 3

static int8_t mqInRebFlag = 0;

static int32_t mndConsumerActionInsert(SSdb *pSdb, SMqConsumerObj *pConsumer);
static int32_t mndConsumerActionDelete(SSdb *pSdb, SMqConsumerObj *pConsumer);
static int32_t mndConsumerActionUpdate(SSdb *pSdb, SMqConsumerObj *pConsumer, SMqConsumerObj *pNewConsumer);
static int32_t mndProcessConsumerMetaMsg(SNodeMsg *pMsg);
static int32_t mndRetrieveConsumer(SNodeMsg *pMsg, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextConsumer(SMnode *pMnode, void *pIter);

static int32_t mndProcessSubscribeReq(SNodeMsg *pMsg);
static int32_t mndProcessAskEpReq(SNodeMsg *pMsg);
static int32_t mndProcessMqTimerMsg(SNodeMsg *pMsg);
static int32_t mndProcessConsumerLostMsg(SNodeMsg *pMsg);

int32_t mndInitConsumer(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_CONSUMER,
                     .keyType = SDB_KEY_INT64,
                     .encodeFp = (SdbEncodeFp)mndConsumerActionEncode,
                     .decodeFp = (SdbDecodeFp)mndConsumerActionDecode,
                     .insertFp = (SdbInsertFp)mndConsumerActionInsert,
                     .updateFp = (SdbUpdateFp)mndConsumerActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndConsumerActionDelete};

  mndSetMsgHandle(pMnode, TDMT_MND_SUBSCRIBE, mndProcessSubscribeReq);
  mndSetMsgHandle(pMnode, TDMT_MND_MQ_ASK_EP, mndProcessAskEpReq);
  mndSetMsgHandle(pMnode, TDMT_MND_MQ_TIMER, mndProcessMqTimerMsg);
  mndSetMsgHandle(pMnode, TDMT_MND_MQ_CONSUMER_LOST, mndProcessConsumerLostMsg);
  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupConsumer(SMnode *pMnode) {}

static int32_t mndProcessConsumerLostMsg(SNodeMsg *pMsg) {
  SMnode             *pMnode = pMsg->pNode;
  SMqConsumerLostMsg *pLostMsg = pMsg->rpcMsg.pCont;
  SMqConsumerObj     *pConsumer = mndAcquireConsumer(pMnode, pLostMsg->consumerId);
  ASSERT(pConsumer);

  SMqConsumerObj *pConsumerNew = tNewSMqConsumerObj(pConsumer->consumerId, pConsumer->cgroup);
  pConsumerNew->updateType = CONSUMER_UPDATE__LOST;

  mndReleaseConsumer(pMnode, pConsumer);

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_TYPE_CONSUMER_LOST, &pMsg->rpcMsg);
  if (pTrans == NULL) goto FAIL;
  if (mndSetConsumerCommitLogs(pMnode, pTrans, pConsumerNew) != 0) goto FAIL;
  if (mndTransPrepare(pMnode, pTrans) != 0) goto FAIL;

  mndTransDrop(pTrans);
  return 0;
FAIL:
  tDeleteSMqConsumerObj(pConsumerNew);
  mndTransDrop(pTrans);
  return -1;
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
  SMnode         *pMnode = pMsg->pNode;
  SSdb           *pSdb = pMnode->pSdb;
  SMqConsumerObj *pConsumer;
  void           *pIter = NULL;

  // rebalance cannot be parallel
  int8_t old = atomic_val_compare_exchange_8(&mqInRebFlag, 0, 1);
  if (old != 0) {
    mInfo("mq rebalance already in progress, do nothing");
    return 0;
  }

  SMqDoRebalanceMsg *pRebMsg = rpcMallocCont(sizeof(SMqDoRebalanceMsg));
  pRebMsg->rebSubHash = taosHashInit(64, MurmurHash3_32, true, HASH_NO_LOCK);
  // TODO set cleanfp
  pRebMsg->mqInReb = &mqInRebFlag;

  // iterate all consumers, find all modification
  while (1) {
    pIter = sdbFetch(pSdb, SDB_CONSUMER, pIter, (void **)&pConsumer);
    if (pIter == NULL) break;

    int32_t hbStatus = atomic_add_fetch_32(&pConsumer->hbStatus, 1);
    int32_t status = atomic_load_32(&pConsumer->status);
    if (status == MQ_CONSUMER_STATUS__READY && hbStatus > MND_CONSUMER_LOST_HB_CNT) {
      SMqConsumerLostMsg *pLostMsg = rpcMallocCont(sizeof(SMqConsumerLostMsg));

      pLostMsg->consumerId = pConsumer->consumerId;
      SRpcMsg *pRpcMsg = taosMemoryCalloc(1, sizeof(SRpcMsg));
      pRpcMsg->msgType = TDMT_MND_MQ_CONSUMER_LOST;
      pRpcMsg->pCont = pLostMsg;
      pRpcMsg->contLen = sizeof(SMqConsumerLostMsg);
      tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, pRpcMsg);
    }
    if (status == MQ_CONSUMER_STATUS__LOST_REBD || status == MQ_CONSUMER_STATUS__READY) {
      // do nothing
    } else if (status == MQ_CONSUMER_STATUS__LOST) {
      taosRLockLatch(&pConsumer->lock);
      int32_t topicNum = taosArrayGetSize(pConsumer->currentTopics);
      for (int32_t i = 0; i < topicNum; i++) {
        char  key[TSDB_SUBSCRIBE_KEY_LEN];
        char *removedTopic = taosArrayGetP(pConsumer->currentTopics, i);
        mndMakeSubscribeKey(key, pConsumer->cgroup, removedTopic);
        SMqRebSubscribe *pRebSub = mndGetOrCreateRebSub(pRebMsg->rebSubHash, key);
        taosArrayPush(pRebSub->removedConsumers, &pConsumer->consumerId);
      }
      taosRUnLockLatch(&pConsumer->lock);
    } else if (status == MQ_CONSUMER_STATUS__MODIFY) {
      taosRLockLatch(&pConsumer->lock);
      int32_t newTopicNum = taosArrayGetSize(pConsumer->rebNewTopics);
      for (int32_t i = 0; i < newTopicNum; i++) {
        char  key[TSDB_SUBSCRIBE_KEY_LEN];
        char *newTopic = taosArrayGetP(pConsumer->rebNewTopics, i);
        mndMakeSubscribeKey(key, pConsumer->cgroup, newTopic);
        SMqRebSubscribe *pRebSub = mndGetOrCreateRebSub(pRebMsg->rebSubHash, key);
        taosArrayPush(pRebSub->newConsumers, &pConsumer->consumerId);
      }

      int32_t removedTopicNum = taosArrayGetSize(pConsumer->rebRemovedTopics);
      for (int32_t i = 0; i < removedTopicNum; i++) {
        char  key[TSDB_SUBSCRIBE_KEY_LEN];
        char *removedTopic = taosArrayGetP(pConsumer->rebRemovedTopics, i);
        mndMakeSubscribeKey(key, pConsumer->cgroup, removedTopic);
        SMqRebSubscribe *pRebSub = mndGetOrCreateRebSub(pRebMsg->rebSubHash, key);
        taosArrayPush(pRebSub->removedConsumers, &pConsumer->consumerId);
      }
      taosRUnLockLatch(&pConsumer->lock);
    } else {
      // do nothing
    }

    mndReleaseConsumer(pMnode, pConsumer);
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
    mTrace("mq rebalance finished, no modification");
    atomic_store_8(&mqInRebFlag, 0);
  }
  return 0;
}

static int32_t mndProcessAskEpReq(SNodeMsg *pMsg) {
  SMnode      *pMnode = pMsg->pNode;
  SMqAskEpReq *pReq = (SMqAskEpReq *)pMsg->rpcMsg.pCont;
  SMqAskEpRsp  rsp = {0};
  int64_t      consumerId = be64toh(pReq->consumerId);
  int32_t      epoch = ntohl(pReq->epoch);

  SMqConsumerObj *pConsumer = mndAcquireConsumer(pMsg->pNode, consumerId);
  if (pConsumer == NULL) {
    terrno = TSDB_CODE_MND_CONSUMER_NOT_EXIST;
    return -1;
  }

  ASSERT(strcmp(pReq->cgroup, pConsumer->cgroup) == 0);
  /*int32_t hbStatus = atomic_load_32(&pConsumer->hbStatus);*/
  atomic_store_32(&pConsumer->hbStatus, 0);

  // 1. check consumer status
  int32_t status = atomic_load_32(&pConsumer->status);

  if (status == MQ_CONSUMER_STATUS__LOST) {
    // TODO: recover consumer
  }

  if (status != MQ_CONSUMER_STATUS__READY) {
    terrno = TSDB_CODE_MND_CONSUMER_NOT_READY;
    return -1;
  }

  int32_t serverEpoch = atomic_load_32(&pConsumer->epoch);

  // 2. check epoch, only send ep info when epoches do not match
  if (epoch != serverEpoch) {
    taosRLockLatch(&pConsumer->lock);
    mInfo("process ask ep, consumer %ld(epoch %d), server epoch %d", consumerId, epoch, serverEpoch);
    int32_t numOfTopics = taosArrayGetSize(pConsumer->currentTopics);

    rsp.topics = taosArrayInit(numOfTopics, sizeof(SMqSubTopicEp));
    if (rsp.topics == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      taosRUnLockLatch(&pConsumer->lock);
      goto FAIL;
    }

    // handle all topic subscribed by the consumer
    for (int32_t i = 0; i < numOfTopics; i++) {
      char            *topic = taosArrayGetP(pConsumer->currentTopics, i);
      SMqSubscribeObj *pSub = mndAcquireSubscribe(pMnode, pConsumer->cgroup, topic);

      // txn guarantees pSub is created
      ASSERT(pSub);
      taosRLockLatch(&pSub->lock);

      SMqSubTopicEp topicEp = {0};
      strcpy(topicEp.topic, topic);

      // 2.1 fetch topic schema
      SMqTopicObj *pTopic = mndAcquireTopic(pMnode, topic);
      ASSERT(pTopic);
      taosRLockLatch(&pTopic->lock);
      topicEp.schema.nCols = pTopic->schema.nCols;
      topicEp.schema.pSchema = taosMemoryCalloc(topicEp.schema.nCols, sizeof(SSchema));
      memcpy(topicEp.schema.pSchema, pTopic->schema.pSchema, topicEp.schema.nCols * sizeof(SSchema));
      taosRUnLockLatch(&pTopic->lock);
      mndReleaseTopic(pMnode, pTopic);

      // 2.2 iterate all vg assigned to the consumer of that topic
      SMqConsumerEpInSub *pEpInSub = taosHashGet(pSub->consumerHash, &consumerId, sizeof(int64_t));
      int32_t             vgNum = taosArrayGetSize(pEpInSub->vgs);

      topicEp.vgs = taosArrayInit(vgNum, sizeof(SMqSubVgEp));
      if (topicEp.vgs == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        taosRUnLockLatch(&pConsumer->lock);
        goto FAIL;
      }

      for (int32_t j = 0; j < vgNum; j++) {
        SMqVgEp *pVgEp = taosArrayGetP(pEpInSub->vgs, j);
        char     offsetKey[TSDB_PARTITION_KEY_LEN];
        mndMakePartitionKey(offsetKey, pConsumer->cgroup, topic, pVgEp->vgId);
        // 2.2.1 build vg ep
        SMqSubVgEp vgEp = {
            .epSet = pVgEp->epSet,
            .vgId = pVgEp->vgId,
            .offset = -1,
        };

        // 2.2.2 fetch vg offset
        SMqOffsetObj *pOffsetObj = mndAcquireOffset(pMnode, offsetKey);
        if (pOffsetObj != NULL) {
          vgEp.offset = atomic_load_64(&pOffsetObj->offset);
          mndReleaseOffset(pMnode, pOffsetObj);
        }
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
  ((SMqRspHead *)buf)->mqMsgType = TMQ_MSG_TYPE__EP_RSP;
  ((SMqRspHead *)buf)->epoch = serverEpoch;
  ((SMqRspHead *)buf)->consumerId = pConsumer->consumerId;

  void *abuf = POINTER_SHIFT(buf, sizeof(SMqRspHead));
  tEncodeSMqAskEpRsp(&abuf, &rsp);

  // release consumer and free memory
  tDeleteSMqAskEpRsp(&rsp);
  mndReleaseConsumer(pMnode, pConsumer);

  // send rsp
  pMsg->pRsp = buf;
  pMsg->rspLen = tlen;
  return 0;
FAIL:
  tDeleteSMqAskEpRsp(&rsp);
  mndReleaseConsumer(pMnode, pConsumer);
  return -1;
}

int32_t mndSetConsumerCommitLogs(SMnode *pMnode, STrans *pTrans, SMqConsumerObj *pConsumer) {
  SSdbRaw *pCommitRaw = mndConsumerActionEncode(pConsumer);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY) != 0) return -1;
  return 0;
}

static int32_t mndProcessSubscribeReq(SNodeMsg *pMsg) {
  SMnode         *pMnode = pMsg->pNode;
  char           *msgStr = pMsg->rpcMsg.pCont;
  SCMSubscribeReq subscribe = {0};
  tDeserializeSCMSubscribeReq(msgStr, &subscribe);
  int64_t         consumerId = subscribe.consumerId;
  char           *cgroup = subscribe.cgroup;
  SMqConsumerObj *pConsumerOld = NULL;
  SMqConsumerObj *pConsumerNew = NULL;

  int32_t code = -1;
  SArray *newSub = subscribe.topicNames;
  taosArraySortString(newSub, taosArrayCompareString);

  int32_t newTopicNum = taosArrayGetSize(newSub);
  // check topic existance
  for (int32_t i = 0; i < newTopicNum; i++) {
    char        *topic = taosArrayGetP(newSub, i);
    SMqTopicObj *pTopic = mndAcquireTopic(pMnode, topic);
    if (pTopic == NULL) {
      terrno = TSDB_CODE_MND_TOPIC_NOT_EXIST;
      goto SUBSCRIBE_OVER;
    }
    // TODO lock topic to prevent drop
    mndReleaseTopic(pMnode, pTopic);
  }

  pConsumerOld = mndAcquireConsumer(pMnode, consumerId);
  if (pConsumerOld == NULL) {
    pConsumerNew = tNewSMqConsumerObj(consumerId, cgroup);
    pConsumerNew->updateType = CONSUMER_UPDATE__MODIFY;
    /*pConsumerNew->waitingRebTopics = newSub;*/
    pConsumerNew->rebNewTopics = newSub;
    subscribe.topicNames = NULL;

    STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_TYPE_SUBSCRIBE, &pMsg->rpcMsg);
    if (pTrans == NULL) goto SUBSCRIBE_OVER;
    if (mndSetConsumerCommitLogs(pMnode, pTrans, pConsumerNew) != 0) goto SUBSCRIBE_OVER;
    if (mndTransPrepare(pMnode, pTrans) != 0) goto SUBSCRIBE_OVER;

  } else {
    /*taosRLockLatch(&pConsumerOld->lock);*/
    int32_t status = atomic_load_32(&pConsumerOld->status);
    if (status != MQ_CONSUMER_STATUS__READY) {
      terrno = TSDB_CODE_MND_CONSUMER_NOT_READY;
      goto SUBSCRIBE_OVER;
    }

    pConsumerNew = tNewSMqConsumerObj(consumerId, cgroup);
    if (pConsumerNew == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto SUBSCRIBE_OVER;
    }
    pConsumerNew->updateType = CONSUMER_UPDATE__MODIFY;
    /*pConsumerOld->waitingRebTopics = newSub;*/

    int32_t oldTopicNum = 0;
    if (pConsumerOld->currentTopics) {
      oldTopicNum = taosArrayGetSize(pConsumerOld->currentTopics);
    }

    int32_t i = 0, j = 0;
    while (i < oldTopicNum || j < newTopicNum) {
      if (i >= oldTopicNum) {
        char *newTopicCopy = strdup(taosArrayGetP(newSub, j));
        taosArrayPush(pConsumerNew->rebNewTopics, &newTopicCopy);
        j++;
        continue;
      } else if (j >= newTopicNum) {
        char *oldTopicCopy = strdup(taosArrayGetP(pConsumerOld->currentTopics, i));
        taosArrayPush(pConsumerNew->rebRemovedTopics, &oldTopicCopy);
        i++;
        continue;
      } else {
        char *oldTopic = taosArrayGetP(pConsumerOld->currentTopics, i);
        char *newTopic = taosArrayGetP(newSub, j);
        int   comp = compareLenPrefixedStr(oldTopic, newTopic);
        if (comp == 0) {
          i++;
          j++;
          continue;
        } else if (comp < 0) {
          char *oldTopicCopy = strdup(oldTopic);
          taosArrayPush(pConsumerNew->rebRemovedTopics, &oldTopicCopy);
          i++;
          continue;
        } else {
          char *newTopicCopy = strdup(newTopic);
          taosArrayPush(pConsumerNew->rebNewTopics, &newTopicCopy);
          j++;
          continue;
        }
      }
    }

    STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_TYPE_SUBSCRIBE, &pMsg->rpcMsg);
    if (pTrans == NULL) goto SUBSCRIBE_OVER;
    if (mndSetConsumerCommitLogs(pMnode, pTrans, pConsumerNew) != 0) goto SUBSCRIBE_OVER;
    if (mndTransPrepare(pMnode, pTrans) != 0) goto SUBSCRIBE_OVER;
  }

  code = TSDB_CODE_MND_ACTION_IN_PROGRESS;

SUBSCRIBE_OVER:
  if (pConsumerOld) {
    /*taosRUnLockLatch(&pConsumerOld->lock);*/
    mndReleaseConsumer(pMnode, pConsumerOld);
  }
  if (pConsumerNew) {
    tDeleteSMqConsumerObj(pConsumerNew);
  }
  // TODO: replace with destroy subscribe msg
  if (subscribe.topicNames) taosArrayDestroyP(subscribe.topicNames, (FDelete)taosMemoryFree);
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
    mError("consumer:%" PRId64 ", failed to encode to raw:%p since %s", pConsumer->consumerId, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("consumer:%" PRId64 ", encode to raw:%p, row:%p", pConsumer->consumerId, pRaw, pConsumer);
  return pRaw;
}

SSdbRow *mndConsumerActionDecode(SSdbRaw *pRaw) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  void *buf = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto CM_DECODE_OVER;

  if (sver != MND_CONSUMER_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto CM_DECODE_OVER;
  }

  SSdbRow *pRow = sdbAllocRow(sizeof(SMqConsumerObj));
  if (pRow == NULL) goto CM_DECODE_OVER;

  SMqConsumerObj *pConsumer = sdbGetRowObj(pRow);
  if (pConsumer == NULL) goto CM_DECODE_OVER;

  int32_t dataPos = 0;
  int32_t len;
  SDB_GET_INT32(pRaw, dataPos, &len, CM_DECODE_OVER);
  buf = taosMemoryMalloc(len);
  if (buf == NULL) goto CM_DECODE_OVER;
  SDB_GET_BINARY(pRaw, dataPos, buf, len, CM_DECODE_OVER);
  SDB_GET_RESERVE(pRaw, dataPos, MND_CONSUMER_RESERVE_SIZE, CM_DECODE_OVER);

  if (tDecodeSMqConsumerObj(buf, pConsumer) == NULL) {
    goto CM_DECODE_OVER;
  }

  terrno = TSDB_CODE_SUCCESS;

CM_DECODE_OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("consumer:%" PRId64 ", failed to decode from raw:%p since %s", pConsumer->consumerId, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  return pRow;
}

static int32_t mndConsumerActionInsert(SSdb *pSdb, SMqConsumerObj *pConsumer) {
  mTrace("consumer:%" PRId64 ", perform insert action", pConsumer->consumerId);
  return 0;
}

static int32_t mndConsumerActionDelete(SSdb *pSdb, SMqConsumerObj *pConsumer) {
  mTrace("consumer:%" PRId64 ", perform delete action", pConsumer->consumerId);
  tDeleteSMqConsumerObj(pConsumer);
  return 0;
}

static int32_t mndConsumerActionUpdate(SSdb *pSdb, SMqConsumerObj *pOldConsumer, SMqConsumerObj *pNewConsumer) {
  mTrace("consumer:%" PRId64 ", perform update action", pOldConsumer->consumerId);

  taosWLockLatch(&pOldConsumer->lock);

  if (pNewConsumer->updateType == CONSUMER_UPDATE__MODIFY) {
    ASSERT(taosArrayGetSize(pOldConsumer->rebNewTopics) == 0);
    ASSERT(taosArrayGetSize(pOldConsumer->rebRemovedTopics) == 0);
    SArray *tmp = pOldConsumer->rebNewTopics;
    pOldConsumer->rebNewTopics = pNewConsumer->rebNewTopics;
    pNewConsumer->rebNewTopics = tmp;

    tmp = pOldConsumer->rebRemovedTopics;
    pOldConsumer->rebRemovedTopics = pNewConsumer->rebRemovedTopics;
    pNewConsumer->rebRemovedTopics = tmp;

    pOldConsumer->status = MQ_CONSUMER_STATUS__MODIFY;
  } else if (pNewConsumer->updateType == CONSUMER_UPDATE__LOST) {
    int32_t sz = taosArrayGetSize(pOldConsumer->currentTopics);
    pOldConsumer->rebRemovedTopics = taosArrayInit(sz, sizeof(void *));
    for (int32_t i = 0; i < sz; i++) {
      char *topic = strdup(taosArrayGetP(pOldConsumer->currentTopics, i));
      taosArrayPush(pNewConsumer->rebRemovedTopics, &topic);
    }
    pOldConsumer->status = MQ_CONSUMER_STATUS__LOST;
  } else if (pNewConsumer->updateType == CONSUMER_UPDATE__TOUCH) {
    atomic_add_fetch_32(&pOldConsumer->epoch, 1);
  } else if (pNewConsumer->updateType == CONSUMER_UPDATE__ADD) {
    ASSERT(taosArrayGetSize(pNewConsumer->rebNewTopics) == 1);
    ASSERT(taosArrayGetSize(pNewConsumer->rebRemovedTopics) == 0);

    char *addedTopic = strdup(taosArrayGetP(pNewConsumer->rebNewTopics, 0));
    // not exist in current topic
#if 1
    for (int32_t i = 0; i < taosArrayGetSize(pOldConsumer->currentTopics); i++) {
      char *topic = taosArrayGetP(pOldConsumer->currentTopics, i);
      ASSERT(strcmp(topic, addedTopic) != 0);
    }
#endif

    // remove from new topic
    for (int32_t i = 0; i < taosArrayGetSize(pOldConsumer->rebNewTopics); i++) {
      char *topic = taosArrayGetP(pOldConsumer->rebNewTopics, i);
      if (strcmp(addedTopic, topic) == 0) {
        taosArrayRemove(pOldConsumer->rebNewTopics, i);
        taosMemoryFree(topic);
        break;
      }
    }

    // add to current topic
    taosArrayPush(pOldConsumer->currentTopics, &addedTopic);
    taosArraySortString(pOldConsumer->currentTopics, taosArrayCompareString);
    // set status
    if (taosArrayGetSize(pOldConsumer->rebNewTopics) == 0 && taosArrayGetSize(pOldConsumer->rebRemovedTopics) == 0) {
      if (pOldConsumer->status == MQ_CONSUMER_STATUS__MODIFY ||
          pOldConsumer->status == MQ_CONSUMER_STATUS__MODIFY_IN_REB) {
        pOldConsumer->status = MQ_CONSUMER_STATUS__READY;
      } else if (pOldConsumer->status == MQ_CONSUMER_STATUS__LOST_IN_REB ||
                 pOldConsumer->status == MQ_CONSUMER_STATUS__LOST) {
        pOldConsumer->status = MQ_CONSUMER_STATUS__LOST_REBD;
      }
    } else {
      if (pOldConsumer->status == MQ_CONSUMER_STATUS__MODIFY ||
          pOldConsumer->status == MQ_CONSUMER_STATUS__MODIFY_IN_REB) {
        pOldConsumer->status = MQ_CONSUMER_STATUS__MODIFY_IN_REB;
      } else if (pOldConsumer->status == MQ_CONSUMER_STATUS__LOST ||
                 pOldConsumer->status == MQ_CONSUMER_STATUS__LOST_IN_REB) {
        pOldConsumer->status = MQ_CONSUMER_STATUS__LOST_IN_REB;
      }
    }
    atomic_add_fetch_32(&pOldConsumer->epoch, 1);
  } else if (pNewConsumer->updateType == CONSUMER_UPDATE__REMOVE) {
    ASSERT(taosArrayGetSize(pNewConsumer->rebNewTopics) == 0);
    ASSERT(taosArrayGetSize(pNewConsumer->rebRemovedTopics) == 1);
    char *removedTopic = taosArrayGetP(pNewConsumer->rebRemovedTopics, 0);

    // not exist in new topic
#if 1
    for (int32_t i = 0; i < taosArrayGetSize(pOldConsumer->rebNewTopics); i++) {
      char *topic = taosArrayGetP(pOldConsumer->rebNewTopics, i);
      ASSERT(strcmp(topic, removedTopic) != 0);
    }
#endif

    // remove from removed topic
    for (int32_t i = 0; i < taosArrayGetSize(pOldConsumer->rebRemovedTopics); i++) {
      char *topic = taosArrayGetP(pOldConsumer->rebRemovedTopics, i);
      if (strcmp(removedTopic, topic) == 0) {
        taosArrayRemove(pOldConsumer->rebRemovedTopics, i);
        taosMemoryFree(topic);
        break;
      }
    }

    // remove from current topic
    int32_t i = 0;
    int32_t sz = taosArrayGetSize(pOldConsumer->currentTopics);
    for (i = 0; i < sz; i++) {
      char *topic = taosArrayGetP(pOldConsumer->currentTopics, i);
      if (strcmp(removedTopic, topic) == 0) {
        taosArrayRemove(pOldConsumer->currentTopics, i);
        taosMemoryFree(topic);
        break;
      }
    }
    // must find the topic
    ASSERT(i < sz);

    // set status
    if (taosArrayGetSize(pOldConsumer->rebNewTopics) == 0 && taosArrayGetSize(pOldConsumer->rebRemovedTopics) == 0) {
      if (pOldConsumer->status == MQ_CONSUMER_STATUS__MODIFY ||
          pOldConsumer->status == MQ_CONSUMER_STATUS__MODIFY_IN_REB) {
        pOldConsumer->status = MQ_CONSUMER_STATUS__READY;
      } else if (pOldConsumer->status == MQ_CONSUMER_STATUS__LOST_IN_REB ||
                 pOldConsumer->status == MQ_CONSUMER_STATUS__LOST) {
        pOldConsumer->status = MQ_CONSUMER_STATUS__LOST_REBD;
      }
    } else {
      if (pOldConsumer->status == MQ_CONSUMER_STATUS__MODIFY ||
          pOldConsumer->status == MQ_CONSUMER_STATUS__MODIFY_IN_REB) {
        pOldConsumer->status = MQ_CONSUMER_STATUS__MODIFY_IN_REB;
      } else if (pOldConsumer->status == MQ_CONSUMER_STATUS__LOST ||
                 pOldConsumer->status == MQ_CONSUMER_STATUS__LOST_IN_REB) {
        pOldConsumer->status = MQ_CONSUMER_STATUS__LOST_IN_REB;
      }
    }
    atomic_add_fetch_32(&pOldConsumer->epoch, 1);
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
