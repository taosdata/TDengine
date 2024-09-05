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
#include "mndDb.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndSubscribe.h"
#include "mndTopic.h"
#include "mndTrans.h"
#include "mndVgroup.h"
#include "tcompare.h"
#include "tname.h"

#define MND_CONSUMER_VER_NUMBER   3
#define MND_CONSUMER_RESERVE_SIZE 64

#define MND_MAX_GROUP_PER_TOPIC 100

static int32_t mndConsumerActionInsert(SSdb *pSdb, SMqConsumerObj *pConsumer);
static int32_t mndConsumerActionDelete(SSdb *pSdb, SMqConsumerObj *pConsumer);
static int32_t mndConsumerActionUpdate(SSdb *pSdb, SMqConsumerObj *pOldConsumer, SMqConsumerObj *pNewConsumer);
static int32_t mndRetrieveConsumer(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextConsumer(SMnode *pMnode, void *pIter);

static int32_t mndProcessSubscribeReq(SRpcMsg *pMsg);
static int32_t mndProcessAskEpReq(SRpcMsg *pMsg);
static int32_t mndProcessMqHbReq(SRpcMsg *pMsg);
static int32_t mndProcessConsumerClearMsg(SRpcMsg *pMsg);

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
  mndSetMsgHandle(pMnode, TDMT_MND_TMQ_LOST_CONSUMER_CLEAR, mndProcessConsumerClearMsg);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_CONSUMERS, mndRetrieveConsumer);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_CONSUMERS, mndCancelGetNextConsumer);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupConsumer(SMnode *pMnode) {}

int32_t mndSendConsumerMsg(SMnode *pMnode, int64_t consumerId, uint16_t msgType, SRpcHandleInfo *info) {
  int32_t code = 0;
  void   *msg  = rpcMallocCont(sizeof(int64_t));
  MND_TMQ_NULL_CHECK(msg);

  *(int64_t*)msg = consumerId;
  SRpcMsg rpcMsg = {
      .msgType = msgType,
      .pCont = msg,
      .contLen = sizeof(int64_t),
      .info = *info,
  };

  mInfo("mndSendConsumerMsg type:%d consumer:0x%" PRIx64, msgType, consumerId);
  MND_TMQ_RETURN_CHECK(tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg));
  return code;

END:
  taosMemoryFree(msg);
  return code;
}

static int32_t validateTopics(STrans* pTrans, SCMSubscribeReq *subscribe, SMnode *pMnode, const char *pUser) {
  SMqTopicObj *pTopic = NULL;
  int32_t      code = 0;

  int32_t numOfTopics = taosArrayGetSize(subscribe->topicNames);
  for (int32_t i = 0; i < numOfTopics; i++) {
    char *pOneTopic = taosArrayGetP(subscribe->topicNames, i);
    MND_TMQ_RETURN_CHECK(mndAcquireTopic(pMnode, pOneTopic, &pTopic));
    MND_TMQ_RETURN_CHECK(mndCheckTopicPrivilege(pMnode, pUser, MND_OPER_SUBSCRIBE, pTopic));
    MND_TMQ_RETURN_CHECK(grantCheckExpire(TSDB_GRANT_SUBSCRIPTION));

    if (subscribe->enableReplay) {
      if (pTopic->subType != TOPIC_SUB_TYPE__COLUMN) {
        code = TSDB_CODE_TMQ_REPLAY_NOT_SUPPORT;
        goto END;
      } else if (pTopic->ntbUid == 0 && pTopic->ctbStbUid == 0) {
        SDbObj *pDb = mndAcquireDb(pMnode, pTopic->db);
        if (pDb == NULL) {
          code = TSDB_CODE_MND_RETURN_VALUE_NULL;
          goto END;
        }
        if (pDb->cfg.numOfVgroups != 1) {
          mndReleaseDb(pMnode, pDb);
          code = TSDB_CODE_TMQ_REPLAY_NEED_ONE_VGROUP;
          goto END;
        }
        mndReleaseDb(pMnode, pDb);
      }
    }
    char  key[TSDB_CONSUMER_ID_LEN] = {0};
    (void)snprintf(key, TSDB_CONSUMER_ID_LEN, "%"PRIx64, subscribe->consumerId);
    mndTransSetDbName(pTrans, pTopic->db, key);
    MND_TMQ_RETURN_CHECK(mndTransCheckConflict(pMnode, pTrans));
    mndReleaseTopic(pMnode, pTopic);
  }
  return 0;

END:
  mndReleaseTopic(pMnode, pTopic);
  return code;
}

static int32_t mndProcessConsumerClearMsg(SRpcMsg *pMsg) {
  int32_t              code = 0;
  SMnode              *pMnode = pMsg->info.node;
  SMqConsumerClearMsg *pClearMsg = pMsg->pCont;
  SMqConsumerObj      *pConsumerNew = NULL;
  STrans              *pTrans = NULL;
  SMqConsumerObj      *pConsumer = NULL;

  MND_TMQ_RETURN_CHECK(mndAcquireConsumer(pMnode, pClearMsg->consumerId, &pConsumer));
  mInfo("consumer:0x%" PRIx64 " needs to be cleared, status %s", pClearMsg->consumerId,
        mndConsumerStatusName(pConsumer->status));

  MND_TMQ_RETURN_CHECK(tNewSMqConsumerObj(pConsumer->consumerId, pConsumer->cgroup, -1, NULL, NULL, &pConsumerNew));
  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pMsg, "clear-csm");
  MND_TMQ_NULL_CHECK(pTrans);
  MND_TMQ_RETURN_CHECK(mndSetConsumerDropLogs(pTrans, pConsumerNew));
  code = mndTransPrepare(pMnode, pTrans);

END:
  mndReleaseConsumer(pMnode, pConsumer);
  tDeleteSMqConsumerObj(pConsumerNew);
  mndTransDrop(pTrans);
  return code;
}

static int32_t checkPrivilege(SMnode *pMnode, SMqConsumerObj *pConsumer, SMqHbRsp *rsp, char *user) {
  int32_t code = 0;
  rsp->topicPrivileges = taosArrayInit(taosArrayGetSize(pConsumer->currentTopics), sizeof(STopicPrivilege));
  MND_TMQ_NULL_CHECK(rsp->topicPrivileges);
  for (int32_t i = 0; i < taosArrayGetSize(pConsumer->currentTopics); i++) {
    char        *topic = taosArrayGetP(pConsumer->currentTopics, i);
    SMqTopicObj *pTopic = NULL;
    code = mndAcquireTopic(pMnode, topic, &pTopic);
    if (code != TDB_CODE_SUCCESS) {
      continue;
    }
    STopicPrivilege *data = taosArrayReserve(rsp->topicPrivileges, 1);
    MND_TMQ_NULL_CHECK(data);
    (void)strcpy(data->topic, topic);
    if (mndCheckTopicPrivilege(pMnode, user, MND_OPER_SUBSCRIBE, pTopic) != 0 ||
        grantCheckExpire(TSDB_GRANT_SUBSCRIPTION) < 0) {
      data->noPrivilege = 1;
    } else {
      data->noPrivilege = 0;
    }
    mndReleaseTopic(pMnode, pTopic);
  }
END:
  return code;
}

static void storeOffsetRows(SMnode *pMnode, SMqHbReq *req, SMqConsumerObj *pConsumer){
  for (int i = 0; i < taosArrayGetSize(req->topics); i++) {
    TopicOffsetRows *data = taosArrayGet(req->topics, i);
    if (data == NULL){
      continue;
    }
    mInfo("heartbeat report offset rows.%s:%s", pConsumer->cgroup, data->topicName);

    SMqSubscribeObj *pSub = NULL;
    char  key[TSDB_SUBSCRIBE_KEY_LEN] = {0};
    (void)snprintf(key, TSDB_SUBSCRIBE_KEY_LEN, "%s%s%s", pConsumer->cgroup, TMQ_SEPARATOR, data->topicName);
    int32_t code = mndAcquireSubscribeByKey(pMnode, key, &pSub);
    if (code != 0) {
      mError("failed to acquire subscribe by key:%s, code:%d", key, code);
      continue;
    }
    taosWLockLatch(&pSub->lock);
    SMqConsumerEp *pConsumerEp = taosHashGet(pSub->consumerHash, &pConsumer->consumerId, sizeof(int64_t));
    if (pConsumerEp) {
      (void)taosArrayDestroy(pConsumerEp->offsetRows);
      pConsumerEp->offsetRows = data->offsetRows;
      data->offsetRows = NULL;
    }
    taosWUnLockLatch(&pSub->lock);

    mndReleaseSubscribe(pMnode, pSub);
  }
}

static int32_t buildMqHbRsp(SRpcMsg *pMsg, SMqHbRsp *rsp){
  int32_t tlen = tSerializeSMqHbRsp(NULL, 0, rsp);
  if (tlen <= 0){
    return TSDB_CODE_TMQ_INVALID_MSG;
  }
  void   *buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if(tSerializeSMqHbRsp(buf, tlen, rsp) <= 0){
    rpcFreeCont(buf);
    return TSDB_CODE_TMQ_INVALID_MSG;
  }
  pMsg->info.rsp = buf;
  pMsg->info.rspLen = tlen;
  return 0;
}

static int32_t mndProcessMqHbReq(SRpcMsg *pMsg) {
  int32_t         code = 0;
  SMnode         *pMnode = pMsg->info.node;
  SMqHbReq        req = {0};
  SMqHbRsp        rsp = {0};
  SMqConsumerObj *pConsumer = NULL;
  MND_TMQ_RETURN_CHECK(tDeserializeSMqHbReq(pMsg->pCont, pMsg->contLen, &req));
  int64_t consumerId = req.consumerId;
  MND_TMQ_RETURN_CHECK(mndAcquireConsumer(pMnode, consumerId, &pConsumer));
  MND_TMQ_RETURN_CHECK(checkPrivilege(pMnode, pConsumer, &rsp, pMsg->info.conn.user));
  atomic_store_32(&pConsumer->hbStatus, 0);
  if (req.pollFlag == 1){
    atomic_store_32(&pConsumer->pollStatus, 0);
  }

  storeOffsetRows(pMnode, &req, pConsumer);
  rsp.debugFlag = tqDebugFlag;
  code = buildMqHbRsp(pMsg, &rsp);

END:
  tDestroySMqHbRsp(&rsp);
  mndReleaseConsumer(pMnode, pConsumer);
  tDestroySMqHbReq(&req);
  return code;
}

static int32_t addEpSetInfo(SMnode *pMnode, SMqConsumerObj *pConsumer, int32_t epoch, SMqAskEpRsp *rsp){
  taosRLockLatch(&pConsumer->lock);

  int32_t numOfTopics = taosArrayGetSize(pConsumer->currentTopics);

  rsp->topics = taosArrayInit(numOfTopics, sizeof(SMqSubTopicEp));
  if (rsp->topics == NULL) {
    taosRUnLockLatch(&pConsumer->lock);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  // handle all topics subscribed by this consumer
  for (int32_t i = 0; i < numOfTopics; i++) {
    char            *topic = taosArrayGetP(pConsumer->currentTopics, i);
    SMqSubscribeObj *pSub = NULL;
    char  key[TSDB_SUBSCRIBE_KEY_LEN] = {0};
    (void)snprintf(key, TSDB_SUBSCRIBE_KEY_LEN, "%s%s%s", pConsumer->cgroup, TMQ_SEPARATOR, topic);
    int32_t code = mndAcquireSubscribeByKey(pMnode, key, &pSub);
    if (code != 0) {
      continue;
    }
    taosRLockLatch(&pSub->lock);

    SMqSubTopicEp topicEp = {0};
    (void)strcpy(topicEp.topic, topic);

    // 2.1 fetch topic schema
    SMqTopicObj *pTopic = NULL;
    code = mndAcquireTopic(pMnode, topic, &pTopic);
    if (code != TDB_CODE_SUCCESS) {
      taosRUnLockLatch(&pSub->lock);
      mndReleaseSubscribe(pMnode, pSub);
      continue;
    }
    taosRLockLatch(&pTopic->lock);
    tstrncpy(topicEp.db, pTopic->db, TSDB_DB_FNAME_LEN);
    topicEp.schema.nCols = pTopic->schema.nCols;
    if (topicEp.schema.nCols) {
      topicEp.schema.pSchema = taosMemoryCalloc(topicEp.schema.nCols, sizeof(SSchema));
      if (topicEp.schema.pSchema == NULL) {
        taosRUnLockLatch(&pTopic->lock);
        taosRUnLockLatch(&pSub->lock);
        mndReleaseSubscribe(pMnode, pSub);
        mndReleaseTopic(pMnode, pTopic);
        return terrno;
      }
      (void)memcpy(topicEp.schema.pSchema, pTopic->schema.pSchema, topicEp.schema.nCols * sizeof(SSchema));
    }
    taosRUnLockLatch(&pTopic->lock);
    mndReleaseTopic(pMnode, pTopic);

    // 2.2 iterate all vg assigned to the consumer of that topic
    SMqConsumerEp *pConsumerEp = taosHashGet(pSub->consumerHash, &pConsumer->consumerId, sizeof(int64_t));
    if (pConsumerEp == NULL) {
      taosRUnLockLatch(&pConsumer->lock);
      taosRUnLockLatch(&pSub->lock);
      mndReleaseSubscribe(pMnode, pSub);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    int32_t vgNum = taosArrayGetSize(pConsumerEp->vgs);
    topicEp.vgs = taosArrayInit(vgNum, sizeof(SMqSubVgEp));
    if (topicEp.vgs == NULL) {
      taosRUnLockLatch(&pConsumer->lock);
      taosRUnLockLatch(&pSub->lock);
      mndReleaseSubscribe(pMnode, pSub);
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    for (int32_t j = 0; j < vgNum; j++) {
      SMqVgEp *pVgEp = taosArrayGetP(pConsumerEp->vgs, j);
      if (pVgEp == NULL) {
        continue;
      }
      if (epoch == -1) {
        SVgObj *pVgroup = mndAcquireVgroup(pMnode, pVgEp->vgId);
        if (pVgroup) {
          pVgEp->epSet = mndGetVgroupEpset(pMnode, pVgroup);
          mndReleaseVgroup(pMnode, pVgroup);
        }
      }
      SMqSubVgEp vgEp = {.epSet = pVgEp->epSet, .vgId = pVgEp->vgId, .offset = -1};
      if (taosArrayPush(topicEp.vgs, &vgEp) == NULL) {
        taosArrayDestroy(topicEp.vgs);
        taosRUnLockLatch(&pConsumer->lock);
        taosRUnLockLatch(&pSub->lock);
        mndReleaseSubscribe(pMnode, pSub);
        return TSDB_CODE_OUT_OF_MEMORY;
      }
    }
    if (taosArrayPush(rsp->topics, &topicEp) == NULL) {
      taosArrayDestroy(topicEp.vgs);
      taosRUnLockLatch(&pConsumer->lock);
      taosRUnLockLatch(&pSub->lock);
      mndReleaseSubscribe(pMnode, pSub);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    taosRUnLockLatch(&pSub->lock);
    mndReleaseSubscribe(pMnode, pSub);
  }
  taosRUnLockLatch(&pConsumer->lock);
  return 0;
}

static int32_t buildAskEpRsp(SRpcMsg *pMsg, SMqAskEpRsp *rsp, int32_t serverEpoch, int64_t consumerId){
  int32_t code = 0;
  // encode rsp
  int32_t tlen = sizeof(SMqRspHead) + tEncodeSMqAskEpRsp(NULL, rsp);
  void   *buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SMqRspHead *pHead = buf;

  pHead->mqMsgType = TMQ_MSG_TYPE__EP_RSP;
  pHead->epoch = serverEpoch;
  pHead->consumerId = consumerId;
  pHead->walsver = 0;
  pHead->walever = 0;

  void *abuf = POINTER_SHIFT(buf, sizeof(SMqRspHead));
  if (tEncodeSMqAskEpRsp(&abuf, rsp) < 0) {
    rpcFreeCont(buf);
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }

  // send rsp
  pMsg->info.rsp = buf;
  pMsg->info.rspLen = tlen;
  return code;
}

static int32_t mndProcessAskEpReq(SRpcMsg *pMsg) {
  SMnode     *pMnode = pMsg->info.node;
  SMqAskEpReq req = {0};
  SMqAskEpRsp rsp = {0};
  int32_t     code = 0;
  SMqConsumerObj *pConsumer = NULL;

  MND_TMQ_RETURN_CHECK(tDeserializeSMqAskEpReq(pMsg->pCont, pMsg->contLen, &req));
  int64_t consumerId = req.consumerId;
  MND_TMQ_RETURN_CHECK(mndAcquireConsumer(pMnode, consumerId, &pConsumer));
  if (strncmp(req.cgroup, pConsumer->cgroup, tListLen(pConsumer->cgroup)) != 0) {
    mError("consumer:0x%" PRIx64 " group:%s not consistent with data in sdb, saved cgroup:%s", consumerId, req.cgroup,
           pConsumer->cgroup);
    code = TSDB_CODE_MND_CONSUMER_NOT_EXIST;
    goto END;
  }

  // 1. check consumer status
  int32_t status = atomic_load_32(&pConsumer->status);
  if (status != MQ_CONSUMER_STATUS_READY) {
    mInfo("consumer:0x%" PRIx64 " not ready, status: %s", consumerId, mndConsumerStatusName(status));
    code = TSDB_CODE_MND_CONSUMER_NOT_READY;
    goto END;
  }

  int32_t epoch = req.epoch;
  int32_t serverEpoch = atomic_load_32(&pConsumer->epoch);

  // 2. check epoch, only send ep info when epochs do not match
  if (epoch != serverEpoch) {
    mInfo("process ask ep, consumer:0x%" PRIx64 "(epoch %d) update with server epoch %d",
          consumerId, epoch, serverEpoch);
    MND_TMQ_RETURN_CHECK(addEpSetInfo(pMnode, pConsumer, epoch, &rsp));
  }

  code = buildAskEpRsp(pMsg, &rsp, serverEpoch, consumerId);

END:
  tDeleteSMqAskEpRsp(&rsp);
  mndReleaseConsumer(pMnode, pConsumer);
  return code;
}

int32_t mndSetConsumerDropLogs(STrans *pTrans, SMqConsumerObj *pConsumer) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndConsumerActionEncode(pConsumer);
  MND_TMQ_NULL_CHECK(pCommitRaw);
  code = mndTransAppendCommitlog(pTrans, pCommitRaw);
  if (code != 0) {
    sdbFreeRaw(pCommitRaw);
    goto END;
  }
  MND_TMQ_RETURN_CHECK(sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED));
END:
  return code;
}

int32_t mndSetConsumerCommitLogs(STrans *pTrans, SMqConsumerObj *pConsumer) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndConsumerActionEncode(pConsumer);
  MND_TMQ_NULL_CHECK(pCommitRaw);
  code = mndTransAppendCommitlog(pTrans, pCommitRaw);
  if (code != 0) {
    sdbFreeRaw(pCommitRaw);
    goto END;
  }
  MND_TMQ_RETURN_CHECK(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));
END:
  return code;
}

static void freeItem(void *param) {
  void *pItem = *(void **)param;
  if (pItem != NULL) {
    taosMemoryFree(pItem);
  }
}

static int32_t getTopicAddDelete(SMqConsumerObj *pExistedConsumer, SMqConsumerObj *pConsumerNew){
  int32_t code = 0;
  pConsumerNew->rebNewTopics = taosArrayInit(0, sizeof(void *));
  MND_TMQ_NULL_CHECK(pConsumerNew->rebNewTopics);
  pConsumerNew->rebRemovedTopics = taosArrayInit(0, sizeof(void *));
  MND_TMQ_NULL_CHECK(pConsumerNew->rebRemovedTopics);

  int32_t newTopicNum = taosArrayGetSize(pConsumerNew->assignedTopics);
  int32_t oldTopicNum = taosArrayGetSize(pExistedConsumer->currentTopics);
  int32_t i = 0, j = 0;
  while (i < oldTopicNum || j < newTopicNum) {
    if (i >= oldTopicNum) {
      void* tmp = taosArrayGetP(pConsumerNew->assignedTopics, j);
      MND_TMQ_NULL_CHECK(tmp);
      char *newTopicCopy = taosStrdup(tmp);
      MND_TMQ_NULL_CHECK(taosArrayPush(pConsumerNew->rebNewTopics, &newTopicCopy));
      j++;
      continue;
    } else if (j >= newTopicNum) {
      void* tmp = taosArrayGetP(pExistedConsumer->currentTopics, i);
      MND_TMQ_NULL_CHECK(tmp);
      char *oldTopicCopy = taosStrdup(tmp);
      MND_TMQ_NULL_CHECK(taosArrayPush(pConsumerNew->rebRemovedTopics, &oldTopicCopy));
      i++;
      continue;
    } else {
      char *oldTopic = taosArrayGetP(pExistedConsumer->currentTopics, i);
      MND_TMQ_NULL_CHECK(oldTopic);
      char *newTopic = taosArrayGetP(pConsumerNew->assignedTopics, j);
      MND_TMQ_NULL_CHECK(newTopic);
      int   comp = strcmp(oldTopic, newTopic);
      if (comp == 0) {
        i++;
        j++;
        continue;
      } else if (comp < 0) {
        char *oldTopicCopy = taosStrdup(oldTopic);
        MND_TMQ_NULL_CHECK(taosArrayPush(pConsumerNew->rebRemovedTopics, &oldTopicCopy));
        i++;
        continue;
      } else {
        char *newTopicCopy = taosStrdup(newTopic);
        MND_TMQ_NULL_CHECK(taosArrayPush(pConsumerNew->rebNewTopics, &newTopicCopy));
        j++;
        continue;
      }
    }
  }
  // no topics need to be rebalanced
  if (taosArrayGetSize(pConsumerNew->rebNewTopics) == 0 && taosArrayGetSize(pConsumerNew->rebRemovedTopics) == 0) {
    code = TSDB_CODE_TMQ_NO_NEED_REBALANCE;
  }

END:
  return code;
}

static int32_t checkAndSortTopic(SMnode *pMnode, SArray *pTopicList){
  taosArraySort(pTopicList, taosArrayCompareString);
  taosArrayRemoveDuplicate(pTopicList, taosArrayCompareString, freeItem);

  int32_t newTopicNum = taosArrayGetSize(pTopicList);
  for (int i = 0; i < newTopicNum; i++) {
    int32_t gNum = mndGetGroupNumByTopic(pMnode, (const char *)taosArrayGetP(pTopicList, i));
    if (gNum >= MND_MAX_GROUP_PER_TOPIC) {
      return TSDB_CODE_TMQ_GROUP_OUT_OF_RANGE;
    }
  }
  return 0;
}

static int32_t buildSubConsumer(SMnode *pMnode, SCMSubscribeReq *subscribe, SMqConsumerObj** ppConsumer){
  int64_t         consumerId = subscribe->consumerId;
  char           *cgroup     = subscribe->cgroup;
  SMqConsumerObj *pConsumerNew     = NULL;
  SMqConsumerObj *pExistedConsumer = NULL;
  int32_t code = mndAcquireConsumer(pMnode, consumerId, &pExistedConsumer);
  if (code != 0) {
    mInfo("receive subscribe request from new consumer:0x%" PRIx64
              ",cgroup:%s, numOfTopics:%d", consumerId,
          subscribe->cgroup, (int32_t)taosArrayGetSize(subscribe->topicNames));

    MND_TMQ_RETURN_CHECK(tNewSMqConsumerObj(consumerId, cgroup, CONSUMER_INSERT_SUB, NULL, subscribe, &pConsumerNew));
  } else {
    int32_t status = atomic_load_32(&pExistedConsumer->status);

    mInfo("receive subscribe request from existed consumer:0x%" PRIx64
              ",cgroup:%s, current status:%d(%s), subscribe topic num: %d",
          consumerId, subscribe->cgroup, status, mndConsumerStatusName(status),
          (int32_t)taosArrayGetSize(subscribe->topicNames));

    if (status != MQ_CONSUMER_STATUS_READY) {
      code = TSDB_CODE_MND_CONSUMER_NOT_READY;
      goto END;
    }
    MND_TMQ_RETURN_CHECK(tNewSMqConsumerObj(consumerId, cgroup, CONSUMER_UPDATE_SUB, NULL, subscribe, &pConsumerNew));
    MND_TMQ_RETURN_CHECK(getTopicAddDelete(pExistedConsumer, pConsumerNew));
  }
  mndReleaseConsumer(pMnode, pExistedConsumer);
  if (ppConsumer){
    *ppConsumer = pConsumerNew;
  }
  return code;

END:
  mndReleaseConsumer(pMnode, pExistedConsumer);
  tDeleteSMqConsumerObj(pConsumerNew);
  return code;
}

int32_t mndProcessSubscribeReq(SRpcMsg *pMsg) {
  SMnode *pMnode = pMsg->info.node;
  char   *msgStr = pMsg->pCont;
  int32_t code = 0;
  SMqConsumerObj *pConsumerNew = NULL;
  STrans         *pTrans = NULL;

  SCMSubscribeReq subscribe = {0};
  MND_TMQ_RETURN_CHECK(tDeserializeSCMSubscribeReq(msgStr, &subscribe, pMsg->contLen));
  bool unSubscribe = (taosArrayGetSize(subscribe.topicNames) == 0);
  if(unSubscribe){
    SMqConsumerObj *pConsumerTmp = NULL;
    MND_TMQ_RETURN_CHECK(mndAcquireConsumer(pMnode, subscribe.consumerId, &pConsumerTmp));
    if (taosArrayGetSize(pConsumerTmp->assignedTopics) == 0){
      mndReleaseConsumer(pMnode, pConsumerTmp);
      goto END;
    }
    mndReleaseConsumer(pMnode, pConsumerTmp);
  }
  MND_TMQ_RETURN_CHECK(checkAndSortTopic(pMnode, subscribe.topicNames));
  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY,
                          (unSubscribe ? TRN_CONFLICT_NOTHING :TRN_CONFLICT_DB_INSIDE),
                          pMsg, "subscribe");
  MND_TMQ_NULL_CHECK(pTrans);

  MND_TMQ_RETURN_CHECK(validateTopics(pTrans, &subscribe, pMnode, pMsg->info.conn.user));
  MND_TMQ_RETURN_CHECK(buildSubConsumer(pMnode, &subscribe, &pConsumerNew));
  MND_TMQ_RETURN_CHECK(mndSetConsumerCommitLogs(pTrans, pConsumerNew));
  MND_TMQ_RETURN_CHECK(mndTransPrepare(pMnode, pTrans));
  code = TSDB_CODE_ACTION_IN_PROGRESS;

END:
  mndTransDrop(pTrans);
  tDeleteSMqConsumerObj(pConsumerNew);
  taosArrayDestroyP(subscribe.topicNames, (FDelete)taosMemoryFree);

  return (code == TSDB_CODE_TMQ_NO_NEED_REBALANCE || code == TSDB_CODE_MND_CONSUMER_NOT_EXIST) ? 0 : code;
}

SSdbRaw *mndConsumerActionEncode(SMqConsumerObj *pConsumer) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  void   *buf = NULL;
  int32_t tlen = tEncodeSMqConsumerObj(NULL, pConsumer);
  int32_t size = sizeof(int32_t) + tlen + MND_CONSUMER_RESERVE_SIZE;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_CONSUMER, MND_CONSUMER_VER_NUMBER, size);
  if (pRaw == NULL) goto CM_ENCODE_OVER;

  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) goto CM_ENCODE_OVER;

  void *abuf = buf;
  if(tEncodeSMqConsumerObj(&abuf, pConsumer) < 0){
    goto CM_ENCODE_OVER;
  }

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
  int32_t         code = 0;
  int32_t         lino = 0;
  SSdbRow        *pRow = NULL;
  SMqConsumerObj *pConsumer = NULL;
  void           *buf = NULL;

  terrno = 0;
  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    goto CM_DECODE_OVER;
  }

  if (sver < 1 || sver > MND_CONSUMER_VER_NUMBER) {
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

  if (tDecodeSMqConsumerObj(buf, pConsumer, sver) == NULL) {
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
  mInfo("consumer:0x%" PRIx64 " sub insert, cgroup:%s status:%d(%s) epoch:%d", pConsumer->consumerId, pConsumer->cgroup,
        pConsumer->status, mndConsumerStatusName(pConsumer->status), pConsumer->epoch);
  pConsumer->subscribeTime = pConsumer->createTime;
  return 0;
}

static int32_t mndConsumerActionDelete(SSdb *pSdb, SMqConsumerObj *pConsumer) {
  mInfo("consumer:0x%" PRIx64 " perform delete action, status:(%d)%s", pConsumer->consumerId, pConsumer->status,
        mndConsumerStatusName(pConsumer->status));
  tClearSMqConsumerObj(pConsumer);
  return 0;
}

//static void updateConsumerStatus(SMqConsumerObj *pConsumer) {
//  int32_t status = pConsumer->status;
//
//  if (taosArrayGetSize(pConsumer->rebNewTopics) == 0 && taosArrayGetSize(pConsumer->rebRemovedTopics) == 0) {
//    if (status == MQ_CONSUMER_STATUS_REBALANCE) {
//      pConsumer->status = MQ_CONSUMER_STATUS_READY;
//    } else if (status == MQ_CONSUMER_STATUS_READY && taosArrayGetSize(pConsumer->currentTopics) == 0) {
//      pConsumer->status = MQ_CONSUMER_STATUS_LOST;
//    }
//  }
//}

// remove from topic list
static void removeFromTopicList(SArray *topicList, const char *pTopic, int64_t consumerId, char *type) {
  int32_t size = taosArrayGetSize(topicList);
  for (int32_t i = 0; i < size; i++) {
    char *p = taosArrayGetP(topicList, i);
    if (strcmp(pTopic, p) == 0) {
      taosArrayRemove(topicList, i);
      taosMemoryFree(p);

      mInfo("[rebalance] consumer:0x%" PRIx64 " remove topic:%s in the %s topic list, remain newTopics:%d",
            consumerId, pTopic, type, (int)taosArrayGetSize(topicList));
      break;
    }
  }
}

static bool existInCurrentTopicList(const SMqConsumerObj *pConsumer, const char *pTopic) {
  bool    existing = false;
  int32_t size = taosArrayGetSize(pConsumer->currentTopics);
  for (int32_t i = 0; i < size; i++) {
    char *topic = taosArrayGetP(pConsumer->currentTopics, i);
    if (topic && strcmp(topic, pTopic) == 0) {
      existing = true;
      break;
    }
  }

  return existing;
}

static int32_t mndConsumerActionUpdate(SSdb *pSdb, SMqConsumerObj *pOldConsumer, SMqConsumerObj *pNewConsumer) {
  mInfo("consumer:0x%" PRIx64 " perform update action, update type:%d, subscribe-time:%" PRId64 ", createTime:%" PRId64,
        pOldConsumer->consumerId, pNewConsumer->updateType, pOldConsumer->subscribeTime, pOldConsumer->createTime);

  taosWLockLatch(&pOldConsumer->lock);

  if (pNewConsumer->updateType == CONSUMER_UPDATE_SUB) {
    TSWAP(pOldConsumer->rebNewTopics, pNewConsumer->rebNewTopics);
    TSWAP(pOldConsumer->rebRemovedTopics, pNewConsumer->rebRemovedTopics);
    TSWAP(pOldConsumer->assignedTopics, pNewConsumer->assignedTopics);

    pOldConsumer->subscribeTime = taosGetTimestampMs();
    pOldConsumer->status = MQ_CONSUMER_STATUS_REBALANCE;
    mInfo("consumer:0x%" PRIx64 " subscribe update, modify existed consumer", pOldConsumer->consumerId);
  } else if (pNewConsumer->updateType == CONSUMER_UPDATE_REB) {
    (void)atomic_add_fetch_32(&pOldConsumer->epoch, 1);
    pOldConsumer->rebalanceTime = taosGetTimestampMs();
    mInfo("[rebalance] consumer:0x%" PRIx64 " rebalance update, only rebalance time", pOldConsumer->consumerId);
  } else if (pNewConsumer->updateType == CONSUMER_ADD_REB) {
    void *tmp = taosArrayGetP(pNewConsumer->rebNewTopics, 0);
    if (tmp == NULL){
      return TSDB_CODE_TMQ_INVALID_MSG;
    }
    char *pNewTopic = taosStrdup(tmp);
    removeFromTopicList(pOldConsumer->rebNewTopics, pNewTopic, pOldConsumer->consumerId, "new");
    bool existing = existInCurrentTopicList(pOldConsumer, pNewTopic);
    if (existing) {
      mError("[rebalance] consumer:0x%" PRIx64 " add new topic:%s should not in currentTopics", pOldConsumer->consumerId, pNewTopic);
      taosMemoryFree(pNewTopic);
    } else {
      if (taosArrayPush(pOldConsumer->currentTopics, &pNewTopic) == NULL) {
        taosMemoryFree(pNewTopic);
        return TSDB_CODE_TMQ_INVALID_MSG;
      }
      taosArraySort(pOldConsumer->currentTopics, taosArrayCompareString);
    }

    int32_t status = pOldConsumer->status;
//    updateConsumerStatus(pOldConsumer);
    if (taosArrayGetSize(pOldConsumer->rebNewTopics) == 0 && taosArrayGetSize(pOldConsumer->rebRemovedTopics) == 0) {
      pOldConsumer->status = MQ_CONSUMER_STATUS_READY;
    }

    pOldConsumer->rebalanceTime = taosGetTimestampMs();
    (void)atomic_add_fetch_32(&pOldConsumer->epoch, 1);

    mInfo("[rebalance] consumer:0x%" PRIx64 " rebalance update add, state (%d)%s -> (%d)%s, new epoch:%d, reb-time:%" PRId64
          ", current topics:%d, newTopics:%d, removeTopics:%d",
          pOldConsumer->consumerId, status, mndConsumerStatusName(status), pOldConsumer->status,
          mndConsumerStatusName(pOldConsumer->status), pOldConsumer->epoch, pOldConsumer->rebalanceTime,
          (int)taosArrayGetSize(pOldConsumer->currentTopics), (int)taosArrayGetSize(pOldConsumer->rebNewTopics),
          (int)taosArrayGetSize(pOldConsumer->rebRemovedTopics));

  } else if (pNewConsumer->updateType == CONSUMER_REMOVE_REB) {
    char *topic = taosArrayGetP(pNewConsumer->rebRemovedTopics, 0);
    if (topic == NULL){
      return TSDB_CODE_TMQ_INVALID_MSG;
    }
    removeFromTopicList(pOldConsumer->rebRemovedTopics, topic, pOldConsumer->consumerId, "remove");
    removeFromTopicList(pOldConsumer->currentTopics, topic, pOldConsumer->consumerId, "current");

    int32_t status = pOldConsumer->status;
//    updateConsumerStatus(pOldConsumer);
    if (taosArrayGetSize(pOldConsumer->rebNewTopics) == 0 && taosArrayGetSize(pOldConsumer->rebRemovedTopics) == 0) {
      pOldConsumer->status = MQ_CONSUMER_STATUS_READY;
    }
    pOldConsumer->rebalanceTime = taosGetTimestampMs();
    (void)atomic_add_fetch_32(&pOldConsumer->epoch, 1);

    mInfo("[rebalance]consumer:0x%" PRIx64 " rebalance update remove, state (%d)%s -> (%d)%s, new epoch:%d, reb-time:%" PRId64
          ", current topics:%d, newTopics:%d, removeTopics:%d",
          pOldConsumer->consumerId, status, mndConsumerStatusName(status), pOldConsumer->status,
          mndConsumerStatusName(pOldConsumer->status), pOldConsumer->epoch, pOldConsumer->rebalanceTime,
          (int)taosArrayGetSize(pOldConsumer->currentTopics), (int)taosArrayGetSize(pOldConsumer->rebNewTopics),
          (int)taosArrayGetSize(pOldConsumer->rebRemovedTopics));
  }

  taosWUnLockLatch(&pOldConsumer->lock);
  return 0;
}

int32_t mndAcquireConsumer(SMnode *pMnode, int64_t consumerId, SMqConsumerObj** pConsumer) {
  SSdb           *pSdb = pMnode->pSdb;
  *pConsumer = sdbAcquire(pSdb, SDB_CONSUMER, &consumerId);
  if (*pConsumer == NULL) {
    return TSDB_CODE_MND_CONSUMER_NOT_EXIST;
  }
  return 0;
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
  int32_t         code = 0;
  char           *parasStr = NULL;
  char           *status = NULL;

  while (numOfRows < rowsCapacity) {
    pShow->pIter = sdbFetch(pSdb, SDB_CONSUMER, pShow->pIter, (void **)&pConsumer);
    if (pShow->pIter == NULL) {
      break;
    }

    if (taosArrayGetSize(pConsumer->assignedTopics) == 0) {
      mInfo("showing consumer:0x%" PRIx64 " no assigned topic, skip", pConsumer->consumerId);
      sdbRelease(pSdb, pConsumer);
      continue;
    }

    taosRLockLatch(&pConsumer->lock);
    mInfo("showing consumer:0x%" PRIx64, pConsumer->consumerId);

    int32_t topicSz = taosArrayGetSize(pConsumer->assignedTopics);
    bool    hasTopic = true;
    if (topicSz == 0) {
      hasTopic = false;
      topicSz = 1;
    }

    if (numOfRows + topicSz > rowsCapacity) {
      MND_TMQ_RETURN_CHECK(blockDataEnsureCapacity(pBlock, numOfRows + topicSz));
    }

    for (int32_t i = 0; i < topicSz; i++) {
      SColumnInfoData *pColInfo = NULL;
      int32_t          cols = 0;

      // consumer id
      char consumerIdHex[TSDB_CONSUMER_ID_LEN + VARSTR_HEADER_SIZE] = {0};
      (void)sprintf(varDataVal(consumerIdHex), "0x%" PRIx64, pConsumer->consumerId);
      varDataSetLen(consumerIdHex, strlen(varDataVal(consumerIdHex)));

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      MND_TMQ_NULL_CHECK(pColInfo);
      MND_TMQ_RETURN_CHECK(colDataSetVal(pColInfo, numOfRows, (const char *)consumerIdHex, false));

      // consumer group
      char cgroup[TSDB_CGROUP_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_TO_VARSTR(cgroup, pConsumer->cgroup);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      MND_TMQ_NULL_CHECK(pColInfo);
      MND_TMQ_RETURN_CHECK(colDataSetVal(pColInfo, numOfRows, (const char *)cgroup, false));

      // client id
      char clientId[TSDB_CLIENT_ID_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_TO_VARSTR(clientId, pConsumer->clientId);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      MND_TMQ_NULL_CHECK(pColInfo);
      MND_TMQ_RETURN_CHECK(colDataSetVal(pColInfo, numOfRows, (const char *)clientId, false));

      // user
      char user[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_TO_VARSTR(user, pConsumer->user);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      MND_TMQ_NULL_CHECK(pColInfo);
      MND_TMQ_RETURN_CHECK(colDataSetVal(pColInfo, numOfRows, (const char *)user, false));

      // fqdn
      char fqdn[TSDB_FQDN_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_TO_VARSTR(fqdn, pConsumer->fqdn);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      MND_TMQ_NULL_CHECK(pColInfo);
      MND_TMQ_RETURN_CHECK(colDataSetVal(pColInfo, numOfRows, (const char *)fqdn, false));

      // status
      const char *pStatusName = mndConsumerStatusName(pConsumer->status);
      status = taosMemoryCalloc(1, pShow->pMeta->pSchemas[cols].bytes);
      MND_TMQ_NULL_CHECK(status);
      STR_TO_VARSTR(status, pStatusName);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      MND_TMQ_NULL_CHECK(pColInfo);
      MND_TMQ_RETURN_CHECK(colDataSetVal(pColInfo, numOfRows, (const char *)status, false));
      taosMemoryFreeClear(status);

      // one subscribed topic
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      MND_TMQ_NULL_CHECK(pColInfo);
      if (hasTopic) {
        char        topic[TSDB_TOPIC_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
        mndTopicGetShowName(taosArrayGetP(pConsumer->assignedTopics, i), topic + VARSTR_HEADER_SIZE);
        *(VarDataLenT *)(topic) = strlen(topic + VARSTR_HEADER_SIZE);
        MND_TMQ_RETURN_CHECK(colDataSetVal(pColInfo, numOfRows, (const char *)topic, false));
      } else {
        MND_TMQ_RETURN_CHECK(colDataSetVal(pColInfo, numOfRows, NULL, true));
      }

      // up time
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      MND_TMQ_NULL_CHECK(pColInfo);
      MND_TMQ_RETURN_CHECK(colDataSetVal(pColInfo, numOfRows, (const char *)&pConsumer->createTime, false));

      // subscribe time
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      MND_TMQ_NULL_CHECK(pColInfo);
      MND_TMQ_RETURN_CHECK(colDataSetVal(pColInfo, numOfRows, (const char *)&pConsumer->subscribeTime, false));

      // rebalance time
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      MND_TMQ_NULL_CHECK(pColInfo);
      MND_TMQ_RETURN_CHECK(colDataSetVal(pColInfo, numOfRows, (const char *)&pConsumer->rebalanceTime, pConsumer->rebalanceTime == 0));

      char         buf[TSDB_OFFSET_LEN] = {0};
      STqOffsetVal pVal = {.type = pConsumer->resetOffsetCfg};
      tFormatOffset(buf, TSDB_OFFSET_LEN, &pVal);

      parasStr = taosMemoryCalloc(1, pShow->pMeta->pSchemas[cols].bytes);
      MND_TMQ_NULL_CHECK(parasStr);
      (void)sprintf(varDataVal(parasStr), "tbname:%d,commit:%d,interval:%dms,reset:%s", pConsumer->withTbName,
              pConsumer->autoCommit, pConsumer->autoCommitInterval, buf);
      varDataSetLen(parasStr, strlen(varDataVal(parasStr)));

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      MND_TMQ_NULL_CHECK(pColInfo);
      MND_TMQ_RETURN_CHECK(colDataSetVal(pColInfo, numOfRows, (const char *)parasStr, false));
      taosMemoryFreeClear(parasStr);
      numOfRows++;
    }

    taosRUnLockLatch(&pConsumer->lock);
    sdbRelease(pSdb, pConsumer);

    pBlock->info.rows = numOfRows;
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;

END:
  taosMemoryFreeClear(status);
  taosMemoryFreeClear(parasStr);
  return code;
}

static void mndCancelGetNextConsumer(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_CONSUMER);
}

const char *mndConsumerStatusName(int status) {
  switch (status) {
    case MQ_CONSUMER_STATUS_READY:
      return "ready";
//    case MQ_CONSUMER_STATUS_LOST:
//      return "lost";
    case MQ_CONSUMER_STATUS_REBALANCE:
      return "rebalancing";
    default:
      return "unknown";
  }
}
