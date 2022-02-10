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
#include "mndShow.h"
#include "mndStb.h"
#include "mndTopic.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"
#include "tcompare.h"
#include "tname.h"

#define MND_SUBSCRIBE_VER_NUMBER 1
#define MND_SUBSCRIBE_RESERVE_SIZE 64

#define MND_SUBSCRIBE_REBALANCE_CNT 3

enum {
  MQ_CONSUMER_STATUS__INIT = 1,
  MQ_CONSUMER_STATUS__ACTIVE,
  MQ_CONSUMER_STATUS__LOST,
};

enum {
  MQ_SUBSCRIBE_STATUS__ACTIVE = 1,
  MQ_SUBSCRIBE_STATUS__DELETED,
};

static char *mndMakeSubscribeKey(const char *cgroup, const char *topicName);

static SSdbRaw *mndSubActionEncode(SMqSubscribeObj *);
static SSdbRow *mndSubActionDecode(SSdbRaw *pRaw);
static int32_t  mndSubActionInsert(SSdb *pSdb, SMqSubscribeObj *);
static int32_t  mndSubActionDelete(SSdb *pSdb, SMqSubscribeObj *);
static int32_t  mndSubActionUpdate(SSdb *pSdb, SMqSubscribeObj *pOldSub, SMqSubscribeObj *pNewSub);

static int32_t mndProcessSubscribeReq(SMnodeMsg *pMsg);
static int32_t mndProcessSubscribeRsp(SMnodeMsg *pMsg);
static int32_t mndProcessSubscribeInternalReq(SMnodeMsg *pMsg);
static int32_t mndProcessSubscribeInternalRsp(SMnodeMsg *pMsg);
static int32_t mndProcessMqTimerMsg(SMnodeMsg *pMsg);
static int32_t mndProcessGetSubEpReq(SMnodeMsg *pMsg);

static int mndPersistMqSetConnReq(SMnode *pMnode, STrans *pTrans, const SMqTopicObj *pTopic, const char *cgroup,
                                  const SMqConsumerEp *pSub);

static int mndInitUnassignedVg(SMnode *pMnode, const SMqTopicObj *pTopic, SMqSubscribeObj *pSub);

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
  mndSetMsgHandle(pMnode, TDMT_MND_MQ_TIMER, mndProcessMqTimerMsg);
  mndSetMsgHandle(pMnode, TDMT_MND_GET_SUB_EP, mndProcessGetSubEpReq);
  return sdbSetTable(pMnode->pSdb, table);
}

static SMqSubscribeObj *mndCreateSubscription(SMnode *pMnode, const SMqTopicObj *pTopic, const char *consumerGroup) {
  SMqSubscribeObj *pSub = tNewSubscribeObj();
  if (pSub == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  char *key = mndMakeSubscribeKey(consumerGroup, pTopic->name);
  if (key == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tDeleteSMqSubscribeObj(pSub);
    free(pSub);
    return NULL;
  }
  strcpy(pSub->key, key);
  free(key);

  if (mndInitUnassignedVg(pMnode, pTopic, pSub) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tDeleteSMqSubscribeObj(pSub);
    free(pSub);
    return NULL;
  }
  // TODO: disable alter subscribed table
  return pSub;
}

static int32_t mndBuildRebalanceMsg(void **pBuf, int32_t *pLen, const SMqTopicObj *pTopic,
                                    const SMqConsumerEp *pConsumerEp, const char *cgroup) {
  SMqSetCVgReq req = {0};
  strcpy(req.cgroup, cgroup);
  strcpy(req.topicName, pTopic->name);
  req.sql = pTopic->sql;
  req.logicalPlan = pTopic->logicalPlan;
  req.physicalPlan = pTopic->physicalPlan;
  req.qmsg = pConsumerEp->qmsg;
  req.oldConsumerId = pConsumerEp->oldConsumerId;
  req.newConsumerId = pConsumerEp->consumerId;
  req.vgId = pConsumerEp->vgId;
  int32_t tlen = tEncodeSMqSetCVgReq(NULL, &req);
  void   *buf = malloc(sizeof(SMsgHead) + tlen);
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

static int32_t mndPersistRebalanceMsg(SMnode *pMnode, STrans *pTrans, const SMqTopicObj *pTopic,
                                      const SMqConsumerEp *pConsumerEp, const char *cgroup) {
  int32_t vgId = pConsumerEp->vgId;
  SVgObj *pVgObj = mndAcquireVgroup(pMnode, vgId);

  void   *buf;
  int32_t tlen;
  if (mndBuildRebalanceMsg(&buf, &tlen, pTopic, pConsumerEp, cgroup) < 0) {
    return -1;
  }

  STransAction action = {0};
  action.epSet = mndGetVgroupEpset(pMnode, pVgObj);
  action.pCont = buf;
  action.contLen = sizeof(SMsgHead) + tlen;
  action.msgType = TDMT_VND_MQ_SET_CONN;

  mndReleaseVgroup(pMnode, pVgObj);
  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    free(buf);
    return -1;
  }

  return 0;
}

static int32_t mndBuildCancelConnReq(void **pBuf, int32_t *pLen, const SMqConsumerEp *pConsumerEp) {
  SMqSetCVgReq req = {0};
  req.oldConsumerId = pConsumerEp->consumerId;
  req.newConsumerId = -1;

  int32_t tlen = tEncodeSMqSetCVgReq(NULL, &req);
  void   *buf = malloc(sizeof(SMsgHead) + tlen);
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
  int32_t vgId = pConsumerEp->vgId;
  SVgObj *pVgObj = mndAcquireVgroup(pMnode, vgId);

  void   *buf;
  int32_t tlen;
  if (mndBuildCancelConnReq(&buf, &tlen, pConsumerEp) < 0) {
    return -1;
  }

  STransAction action = {0};
  action.epSet = mndGetVgroupEpset(pMnode, pVgObj);
  action.pCont = buf;
  action.contLen = sizeof(SMsgHead) + tlen;
  action.msgType = TDMT_VND_MQ_SET_CONN;

  mndReleaseVgroup(pMnode, pVgObj);
  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    free(buf);
    return -1;
  }

  return 0;
}

static int32_t mndProcessGetSubEpReq(SMnodeMsg *pMsg) {
  SMnode           *pMnode = pMsg->pMnode;
  SMqCMGetSubEpReq *pReq = (SMqCMGetSubEpReq *)pMsg->rpcMsg.pCont;
  SMqCMGetSubEpRsp  rsp = {0};
  int64_t           consumerId = be64toh(pReq->consumerId);

  SMqConsumerObj *pConsumer = mndAcquireConsumer(pMsg->pMnode, consumerId);
  if (pConsumer == NULL) {
    terrno = TSDB_CODE_MND_CONSUMER_NOT_EXIST;
    return -1;
  }
  ASSERT(strcmp(pReq->cgroup, pConsumer->cgroup) == 0);

  strcpy(rsp.cgroup, pReq->cgroup);
  rsp.consumerId = consumerId;
  rsp.epoch = pConsumer->epoch;
  if (pReq->epoch != rsp.epoch) {
    SArray *pTopics = pConsumer->topics;
    int     sz = taosArrayGetSize(pTopics);
    rsp.topics = taosArrayInit(sz, sizeof(SMqSubTopicEp));
    for (int i = 0; i < sz; i++) {
      char            *topicName = taosArrayGetP(pTopics, i);
      SMqSubscribeObj *pSub = mndAcquireSubscribe(pMnode, pConsumer->cgroup, topicName);
      ASSERT(pSub);
      int csz = taosArrayGetSize(pSub->consumers);
      //TODO: change to bsearch
      for (int j = 0; j < csz; j++) {
        SMqSubConsumer *pSubConsumer = taosArrayGet(pSub->consumers, j);
        if (consumerId == pSubConsumer->consumerId) {
          int           vgsz = taosArrayGetSize(pSubConsumer->vgInfo);
          SMqSubTopicEp topicEp;
          topicEp.vgs = taosArrayInit(vgsz, sizeof(SMqSubVgEp));
          for (int k = 0; k < vgsz; k++) {
            SMqConsumerEp *pConsumerEp = taosArrayGet(pSubConsumer->vgInfo, k);

            SMqSubVgEp vgEp = {.epSet = pConsumerEp->epSet, .vgId = pConsumerEp->vgId};
            taosArrayPush(topicEp.vgs, &vgEp);
          }
          taosArrayPush(rsp.topics, &topicEp);
          break;
        }
      }
      mndReleaseSubscribe(pMnode, pSub);
    }
  }
  int32_t tlen = tEncodeSMqCMGetSubEpRsp(NULL, &rsp);
  void   *buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  void *abuf = buf;
  tEncodeSMqCMGetSubEpRsp(&abuf, &rsp);
  tDeleteSMqCMGetSubEpRsp(&rsp);
  pMsg->pCont = buf;
  pMsg->contLen = tlen;
  return 0;
}

static int32_t mndSplitSubscribeKey(char *key, char **topic, char **cgroup) {
  int i = 0;
  while (key[i] != ':') {
    i++;
  }
  key[i] = 0;
  *cgroup = strdup(key);
  key[i] = ':';
  *topic = strdup(&key[i + 1]);
  return 0;
}

static int32_t mndProcessMqTimerMsg(SMnodeMsg *pMsg) {
  SMnode *pMnode = pMsg->pMnode;
  SSdb   *pSdb = pMnode->pSdb;
  SMqConsumerObj *pConsumer;
  void           *pIter = NULL;
  while (1) {
    pIter = sdbFetch(pSdb, SDB_CONSUMER, pIter, (void **)&pConsumer);
    if (pIter == NULL) break;
    int32_t hbStatus = atomic_fetch_add_32(&pConsumer->hbStatus, 1);
    if (hbStatus > MND_SUBSCRIBE_REBALANCE_CNT) {
      int32_t old =
          atomic_val_compare_exchange_32(&pConsumer->status, MQ_CONSUMER_STATUS__ACTIVE, MQ_CONSUMER_STATUS__LOST);
      if (old == MQ_CONSUMER_STATUS__ACTIVE) {
        SMqDoRebalanceMsg *pRebMsg = rpcMallocCont(sizeof(SMqDoRebalanceMsg));
        pRebMsg->consumerId = pConsumer->consumerId;
        SRpcMsg rpcMsg = {.msgType = TDMT_MND_MQ_DO_REBALANCE, .pCont = pRebMsg, .contLen = sizeof(SMqDoRebalanceMsg)};
        pMnode->putReqToMWriteQFp(pMnode->pDnode, &rpcMsg);
      }
    }
  }
  return 0;
}

static int32_t mndProcessDoRebalanceMsg(SMnodeMsg *pMsg) {
  SMnode            *pMnode = pMsg->pMnode;
  SMqDoRebalanceMsg *pReq = (SMqDoRebalanceMsg *)pMsg->rpcMsg.pCont;
  SMqConsumerObj    *pConsumer = mndAcquireConsumer(pMnode, pReq->consumerId);
  int                topicSz = taosArrayGetSize(pConsumer->topics);
  STrans            *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, &pMsg->rpcMsg);
  for (int i = 0; i < topicSz; i++) {
    char            *topic = taosArrayGetP(pConsumer->topics, i);
    SMqSubscribeObj *pSub = mndAcquireSubscribe(pMnode, pConsumer->cgroup, topic);
    int32_t          consumerNum = taosArrayGetSize(pSub->consumers);
    if (consumerNum != 0) {
      int32_t vgNum = pSub->vgNum;
      int32_t vgEachConsumer = vgNum / consumerNum;
      int32_t left = vgNum % consumerNum;
      int32_t leftUsed = 0;
      SArray *unassignedVgStash = taosArrayInit(0, sizeof(SMqConsumerEp));
      SArray *unassignedConsumer = taosArrayInit(0, sizeof(int32_t));
      for (int32_t j = 0; j < consumerNum; j++) {
        bool            changed = false;
        SMqSubConsumer *pSubConsumer = taosArrayGet(pSub->consumers, j);
        int32_t         vgOneConsumer = taosArrayGetSize(pSubConsumer->vgInfo);
        bool            canUseLeft = leftUsed < left;
        if (vgOneConsumer > vgEachConsumer + canUseLeft) {
          changed = true;
          if (canUseLeft) leftUsed++;
          // put into unassigned
          while (taosArrayGetSize(pSubConsumer->vgInfo) > vgEachConsumer + canUseLeft) {
            SMqConsumerEp *pConsumerEp = taosArrayPop(pSubConsumer->vgInfo);
            ASSERT(pConsumerEp != NULL);
            taosArrayPush(unassignedVgStash, pConsumerEp);
            // build msg and persist into trans
          }
        } else if (vgOneConsumer < vgEachConsumer) {
          changed = true;
          // assign from unassigned
          while (taosArrayGetSize(pSubConsumer->vgInfo) < vgEachConsumer) {
            // if no unassgined, save j
            if (taosArrayGetSize(unassignedVgStash) == 0) {
              taosArrayPush(unassignedConsumer, &j);
              break;
            }
            SMqConsumerEp *pConsumerEp = taosArrayPop(unassignedVgStash);
            ASSERT(pConsumerEp != NULL);
            pConsumerEp->oldConsumerId = pConsumerEp->consumerId;
            pConsumerEp->consumerId = pSubConsumer->consumerId;
            taosArrayPush(pSubConsumer->vgInfo, pConsumerEp);
            // build msg and persist into trans
          }
        }
        if (changed) {
          SMqConsumerObj *pRebConsumer = mndAcquireConsumer(pMnode, pSubConsumer->consumerId);
          pRebConsumer->epoch++;
          SSdbRaw* pConsumerRaw = mndConsumerActionEncode(pRebConsumer);
          sdbSetRawStatus(pConsumerRaw, SDB_STATUS_READY);
          mndTransAppendRedolog(pTrans, pConsumerRaw);
        }
      }

      for (int32_t j = 0; j < taosArrayGetSize(unassignedConsumer); j++) {
        int32_t         consumerIdx = *(int32_t *)taosArrayGet(unassignedConsumer, j);
        SMqSubConsumer *pSubConsumer = taosArrayGet(pSub->consumers, consumerIdx);
        while (taosArrayGetSize(pSubConsumer->vgInfo) < vgEachConsumer) {
          SMqConsumerEp *pConsumerEp = taosArrayPop(unassignedVgStash);
          ASSERT(pConsumerEp != NULL);
          pConsumerEp->oldConsumerId = pConsumerEp->consumerId;
          pConsumerEp->consumerId = pSubConsumer->consumerId;
          taosArrayPush(pSubConsumer->vgInfo, pConsumerEp);
          // build msg and persist into trans
        }
      }
      ASSERT(taosArrayGetSize(unassignedVgStash) == 0);

      // send msg to vnode
      // log rebalance statistics
      /*SSdbRaw *pSubRaw = mndSubscribeActionEncode(pSub);*/
      /*sdbSetRawStatus(pSubRaw, SDB_STATUS_READY);*/
      /*mndTransAppendRedolog(pTrans, pSubRaw);*/
    }
    mndReleaseSubscribe(pMnode, pSub);
  }
  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("mq-rebalance-trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    mndReleaseConsumer(pMnode, pConsumer);
    return -1;
  }

  mndTransDrop(pTrans);
  mndReleaseConsumer(pMnode, pConsumer);
  return 0;
}

#if 0
    //update consumer status for the subscribption
    for (int i = 0; i < taosArrayGetSize(pSub->assigned); i++) {
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
        for (int j = 0; j < taosArrayGetSize(pSub->availConsumer); j++) {
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
      tfree(topic);
      tfree(cgroup);
    }
    // rebalance condition2 : imbalance assignment
  }
  return 0;
}
#endif

static int mndInitUnassignedVg(SMnode *pMnode, const SMqTopicObj *pTopic, SMqSubscribeObj *pSub) {
  SSdb      *pSdb = pMnode->pSdb;
  SVgObj    *pVgroup = NULL;
  SQueryDag *pDag = qStringToDag(pTopic->physicalPlan);
  SArray    *pArray = NULL;
  SArray    *inner = taosArrayGet(pDag->pSubplans, 0);
  SSubplan  *plan = taosArrayGetP(inner, 0);
  SArray    *unassignedVg = pSub->unassignedVg;

  void *pIter = NULL;
  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;
    if (pVgroup->dbUid != pTopic->dbUid) continue;

    pSub->vgNum++;
    plan->execNode.nodeId = pVgroup->vgId;
    plan->execNode.epset = mndGetVgroupEpset(pMnode, pVgroup);

    if (schedulerConvertDagToTaskList(pDag, &pArray) < 0) {
      terrno = TSDB_CODE_MND_UNSUPPORTED_TOPIC;
      mError("unsupport topic: %s, sql: %s", pTopic->name, pTopic->sql);
      return -1;
    }
    if (pArray && taosArrayGetSize(pArray) != 1) {
      terrno = TSDB_CODE_MND_UNSUPPORTED_TOPIC;
      mError("unsupport topic: %s, sql: %s, plan level: %ld", pTopic->name, pTopic->sql, taosArrayGetSize(pArray));
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

static int mndPersistMqSetConnReq(SMnode *pMnode, STrans *pTrans, const SMqTopicObj *pTopic, const char *cgroup,
                                  const SMqConsumerEp *pConsumerEp) {
  int32_t vgId = pConsumerEp->vgId;
  SVgObj *pVgObj = mndAcquireVgroup(pMnode, vgId);

  SMqSetCVgReq req = {
      .vgId = vgId,
      .oldConsumerId = pConsumerEp->oldConsumerId,
      .newConsumerId = pConsumerEp->consumerId,
      .sql = pTopic->sql,
      .logicalPlan = pTopic->logicalPlan,
      .physicalPlan = pTopic->physicalPlan,
      .qmsg = pConsumerEp->qmsg,
  };

  strcpy(req.cgroup, cgroup);
  strcpy(req.topicName, pTopic->name);
  int32_t tlen = tEncodeSMqSetCVgReq(NULL, &req);
  void   *buf = malloc(sizeof(SMsgHead) + tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  SMsgHead *pMsgHead = (SMsgHead *)buf;

  pMsgHead->contLen = htonl(sizeof(SMsgHead) + tlen);
  pMsgHead->vgId = htonl(vgId);

  void *abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
  tEncodeSMqSetCVgReq(&abuf, &req);

  STransAction action = {0};
  action.epSet = mndGetVgroupEpset(pMnode, pVgObj);
  action.pCont = buf;
  action.contLen = sizeof(SMsgHead) + tlen;
  action.msgType = TDMT_VND_MQ_SET_CONN;

  mndReleaseVgroup(pMnode, pVgObj);
  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    free(buf);
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

  buf = malloc(tlen);
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
  tfree(buf);
  if (terrno != 0) {
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
  buf = malloc(tlen + 1);
  if (buf == NULL) goto SUB_DECODE_OVER;
  SDB_GET_BINARY(pRaw, dataPos, buf, tlen, SUB_DECODE_OVER);
  SDB_GET_RESERVE(pRaw, dataPos, MND_SUBSCRIBE_RESERVE_SIZE, SUB_DECODE_OVER);

  if (tDecodeSubscribeObj(buf, pSub) == NULL) {
    goto SUB_DECODE_OVER;
  }

  terrno = TSDB_CODE_SUCCESS;

SUB_DECODE_OVER:
  tfree(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("subscribe:%s, failed to decode from raw:%p since %s", pSub->key, pRaw, terrstr());
    tfree(pRow);
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

static char *mndMakeSubscribeKey(const char *cgroup, const char *topicName) {
  char *key = malloc(TSDB_SHOW_SUBQUERY_LEN);
  if (key == NULL) {
    return NULL;
  }
  int tlen = strlen(cgroup);
  memcpy(key, cgroup, tlen);
  key[tlen] = ':';
  strcpy(key + tlen + 1, topicName);
  return key;
}

SMqSubscribeObj *mndAcquireSubscribe(SMnode *pMnode, char *cgroup, char *topicName) {
  SSdb            *pSdb = pMnode->pSdb;
  char            *key = mndMakeSubscribeKey(cgroup, topicName);
  SMqSubscribeObj *pSub = sdbAcquire(pSdb, SDB_SUBSCRIBE, key);
  free(key);
  if (pSub == NULL) {
    /*terrno = TSDB_CODE_MND_CONSUMER_NOT_EXIST;*/
  }
  return pSub;
}

void mndReleaseSubscribe(SMnode *pMnode, SMqSubscribeObj *pSub) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pSub);
}

static int32_t mndProcessSubscribeReq(SMnodeMsg *pMsg) {
  SMnode         *pMnode = pMsg->pMnode;
  char           *msgStr = pMsg->rpcMsg.pCont;
  SCMSubscribeReq subscribe;
  tDeserializeSCMSubscribeReq(msgStr, &subscribe);
  int64_t consumerId = subscribe.consumerId;
  char   *cgroup = subscribe.consumerGroup;

  SArray *newSub = subscribe.topicNames;
  int     newTopicNum = subscribe.topicNum;

  taosArraySortString(newSub, taosArrayCompareString);

  SArray *oldSub = NULL;
  int     oldTopicNum = 0;
  bool    createConsumer = false;
  // create consumer if not exist
  SMqConsumerObj *pConsumer = mndAcquireConsumer(pMnode, consumerId);
  if (pConsumer == NULL) {
    // create consumer
    pConsumer = mndCreateConsumer(consumerId, cgroup);
    createConsumer = true;
  } else {
    pConsumer->epoch++;
    oldSub = pConsumer->topics;
  }
  pConsumer->topics = newSub;

  if (oldSub != NULL) {
    oldTopicNum = taosArrayGetSize(oldSub);
  }

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, &pMsg->rpcMsg);
  if (pTrans == NULL) {
    // TODO: free memory
    return -1;
  }

  int i = 0, j = 0;
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

      int comp = compareLenPrefixedStr(newTopicName, oldTopicName);
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
      int csz = taosArrayGetSize(pSub->consumers);
      for (int ci = 0; ci < csz; ci++) {
        SMqSubConsumer *pSubConsumer = taosArrayGet(pSub->consumers, ci);
        if (pSubConsumer->consumerId == consumerId) {
          int vgsz = taosArrayGetSize(pSubConsumer->vgInfo);
          for (int vgi = 0; vgi < vgsz; vgi++) {
            SMqConsumerEp *pConsumerEp = taosArrayGet(pSubConsumer->vgInfo, vgi);
            mndPersistCancelConnReq(pMnode, pTrans, pConsumerEp);
          }
          break;
        }
      }
      pSub->status = MQ_SUBSCRIBE_STATUS__DELETED;
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
        mDebug("create new subscription by consumer %ld, group: %s, topic %s", consumerId, cgroup, newTopicName);
        pSub = mndCreateSubscription(pMnode, pTopic, cgroup);
        createSub = true;
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
        mndPersistMqSetConnReq(pMnode, pTrans, pTopic, cgroup, pConsumerEp);
      }

      SSdbRaw *pRaw = mndSubActionEncode(pSub);
      sdbSetRawStatus(pRaw, SDB_STATUS_READY);
      mndTransAppendRedolog(pTrans, pRaw);

      if (!createSub) mndReleaseSubscribe(pMnode, pSub);
      mndReleaseTopic(pMnode, pTopic);
    }
  }

  if (oldSub) taosArrayDestroyEx(oldSub, free);

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

static int32_t mndProcessSubscribeInternalRsp(SMnodeMsg *pRsp) {
  mndTransProcessRsp(pRsp);
  return 0;
}

static void mndCancelGetNextConsumer(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}
