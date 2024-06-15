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

#define MND_CONSUMER_VER_NUMBER   2
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
  //  mndSetMsgHandle(pMnode, TDMT_MND_TMQ_TIMER, mndProcessMqTimerMsg);
  mndSetMsgHandle(pMnode, TDMT_MND_TMQ_CONSUMER_RECOVER, mndProcessConsumerRecoverMsg);
  mndSetMsgHandle(pMnode, TDMT_MND_TMQ_LOST_CONSUMER_CLEAR, mndProcessConsumerClearMsg);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_CONSUMERS, mndRetrieveConsumer);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_CONSUMERS, mndCancelGetNextConsumer);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupConsumer(SMnode *pMnode) {}

void mndSendConsumerMsg(SMnode *pMnode, int64_t consumerId, uint16_t msgType, SRpcHandleInfo *info) {
  void *msg = rpcMallocCont(sizeof(int64_t));
  if (msg == NULL) {
    mError("consumer:0x%" PRIx64 " failed to clear consumer due to out of memory. alloc size:%d", consumerId,
           (int32_t)sizeof(int64_t));
    return;
  }

  *(int64_t*)msg = consumerId;
  SRpcMsg rpcMsg = {
      .msgType = msgType,
      .pCont = msg,
      .contLen = sizeof(int64_t),
      .info = *info,
  };

  mInfo("mndSendConsumerMsg type:%d consumer:0x%" PRIx64, msgType, consumerId);
  int32_t code = tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg);
  if (code != 0){
    mError("consumer:%"PRId64" send consumer msg:%d error:%d", consumerId, msgType, code);
  }
}

static int32_t validateTopics(const SArray *pTopicList, SMnode *pMnode, const char *pUser,
                              bool enableReplay) {
  SMqTopicObj *pTopic = NULL;
  int32_t      code = 0;

  int32_t numOfTopics = taosArrayGetSize(pTopicList);
  for (int32_t i = 0; i < numOfTopics; i++) {
    char *pOneTopic = taosArrayGetP(pTopicList, i);
    pTopic = mndAcquireTopic(pMnode, pOneTopic);
    if (pTopic == NULL) {  // terrno has been set by callee function
      code = -1;
      goto FAILED;
    }

    if (mndCheckTopicPrivilege(pMnode, pUser, MND_OPER_SUBSCRIBE, pTopic) != 0) {
      code = TSDB_CODE_MND_NO_RIGHTS;
      terrno = TSDB_CODE_MND_NO_RIGHTS;
      goto FAILED;
    }

    if ((terrno = grantCheckExpire(TSDB_GRANT_SUBSCRIPTION)) < 0) {
      code = terrno;
      goto FAILED;
    }

    if (enableReplay) {
      if (pTopic->subType != TOPIC_SUB_TYPE__COLUMN) {
        code = TSDB_CODE_TMQ_REPLAY_NOT_SUPPORT;
        goto FAILED;
      } else if (pTopic->ntbUid == 0 && pTopic->ctbStbUid == 0) {
        SDbObj *pDb = mndAcquireDb(pMnode, pTopic->db);
        if (pDb == NULL) {
          code = -1;
          goto FAILED;
        }
        if (pDb->cfg.numOfVgroups != 1) {
          mndReleaseDb(pMnode, pDb);
          code = TSDB_CODE_TMQ_REPLAY_NEED_ONE_VGROUP;
          goto FAILED;
        }
        mndReleaseDb(pMnode, pDb);
      }
    }

    mndReleaseTopic(pMnode, pTopic);
  }

  return 0;
FAILED:
  mndReleaseTopic(pMnode, pTopic);
  return code;
}

static int32_t mndProcessConsumerRecoverMsg(SRpcMsg *pMsg) {
  int32_t                code = 0;
  SMnode                *pMnode = pMsg->info.node;
  SMqConsumerRecoverMsg *pRecoverMsg = pMsg->pCont;
  SMqConsumerObj        *pConsumerNew = NULL;
  STrans                *pTrans = NULL;
  SMqConsumerObj        *pConsumer = mndAcquireConsumer(pMnode, pRecoverMsg->consumerId);
  if (pConsumer == NULL) {
    mError("cannot find consumer %" PRId64 " when processing consumer recover msg", pRecoverMsg->consumerId);
    code = -1;
    goto END;
  }

  mInfo("receive consumer recover msg, consumer:0x%" PRIx64 " status:%d(%s)", pRecoverMsg->consumerId,
        pConsumer->status, mndConsumerStatusName(pConsumer->status));

  if (pConsumer->status != MQ_CONSUMER_STATUS_LOST) {
    terrno = TSDB_CODE_MND_CONSUMER_NOT_READY;
    code = -1;
    goto END;
  }

  pConsumerNew = tNewSMqConsumerObj(pConsumer->consumerId, pConsumer->cgroup, CONSUMER_UPDATE_REC, NULL, NULL);
  if (pConsumerNew == NULL){
    code = -1;
    goto END;
  }

  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pMsg, "recover-csm");
  if (pTrans == NULL) {
    code = -1;
    goto END;
  }
  code = validateTopics(pConsumer->assignedTopics, pMnode, pMsg->info.conn.user, false);
  if (code != 0) {
    goto END;
  }

  code = mndSetConsumerCommitLogs(pTrans, pConsumerNew);
  if (code != 0) {
    goto END;
  }

  code = mndTransPrepare(pMnode, pTrans);
END:
  mndReleaseConsumer(pMnode, pConsumer);
  tDeleteSMqConsumerObj(pConsumerNew);
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessConsumerClearMsg(SRpcMsg *pMsg) {
  int32_t              code = 0;
  SMnode              *pMnode = pMsg->info.node;
  SMqConsumerClearMsg *pClearMsg = pMsg->pCont;
  SMqConsumerObj      *pConsumerNew = NULL;
  STrans              *pTrans = NULL;

  SMqConsumerObj *pConsumer = mndAcquireConsumer(pMnode, pClearMsg->consumerId);
  if (pConsumer == NULL) {
    mError("consumer:0x%" PRIx64 " failed to be found to clear it", pClearMsg->consumerId);
    return 0;
  }

  mInfo("consumer:0x%" PRIx64 " needs to be cleared, status %s", pClearMsg->consumerId,
        mndConsumerStatusName(pConsumer->status));

  pConsumerNew = tNewSMqConsumerObj(pConsumer->consumerId, pConsumer->cgroup, -1, NULL, NULL);
  if (pConsumerNew == NULL){
    code = -1;
    goto END;
  }

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pMsg, "clear-csm");
  if (pTrans == NULL) {
    code = -1;
    goto END;
  }

  // this is the drop action, not the update action
  code = mndSetConsumerDropLogs(pTrans, pConsumerNew);
  if (code != 0) {
    goto END;
  }

  code = mndTransPrepare(pMnode, pTrans);

END:
  mndReleaseConsumer(pMnode, pConsumer);
  tDeleteSMqConsumerObj(pConsumerNew);
  mndTransDrop(pTrans);
  return code;
}

static int32_t checkPrivilege(SMnode *pMnode, SMqConsumerObj *pConsumer, SMqHbRsp *rsp, char *user) {
  rsp->topicPrivileges = taosArrayInit(taosArrayGetSize(pConsumer->currentTopics), sizeof(STopicPrivilege));
  if (rsp->topicPrivileges == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  for (int32_t i = 0; i < taosArrayGetSize(pConsumer->currentTopics); i++) {
    char        *topic = taosArrayGetP(pConsumer->currentTopics, i);
    SMqTopicObj *pTopic = mndAcquireTopic(pMnode, topic);
    if (pTopic == NULL) {  // terrno has been set by callee function
      continue;
    }
    STopicPrivilege *data = taosArrayReserve(rsp->topicPrivileges, 1);
    strcpy(data->topic, topic);
    if (mndCheckTopicPrivilege(pMnode, user, MND_OPER_SUBSCRIBE, pTopic) != 0 ||
        grantCheckExpire(TSDB_GRANT_SUBSCRIPTION) < 0) {
      data->noPrivilege = 1;
    } else {
      data->noPrivilege = 0;
    }
    mndReleaseTopic(pMnode, pTopic);
  }
  return 0;
}

static void storeOffsetRows(SMnode *pMnode, SMqHbReq *req, SMqConsumerObj *pConsumer){
  for (int i = 0; i < taosArrayGetSize(req->topics); i++) {
    TopicOffsetRows *data = taosArrayGet(req->topics, i);
    mInfo("heartbeat report offset rows.%s:%s", pConsumer->cgroup, data->topicName);

    SMqSubscribeObj *pSub = mndAcquireSubscribe(pMnode, pConsumer->cgroup, data->topicName);
    if (pSub == NULL) {
      continue;
    }
    taosWLockLatch(&pSub->lock);
    SMqConsumerEp *pConsumerEp = taosHashGet(pSub->consumerHash, &pConsumer->consumerId, sizeof(int64_t));
    if (pConsumerEp) {
      taosArrayDestroy(pConsumerEp->offsetRows);
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

  if (tDeserializeSMqHbReq(pMsg->pCont, pMsg->contLen, &req) < 0) {
    code = TSDB_CODE_TMQ_INVALID_MSG;
    goto end;
  }

  int64_t consumerId = req.consumerId;
  pConsumer = mndAcquireConsumer(pMnode, consumerId);
  if (pConsumer == NULL) {
    mError("consumer:0x%" PRIx64 " not exist", consumerId);
    code = TSDB_CODE_MND_CONSUMER_NOT_EXIST;
    goto end;
  }
  code = checkPrivilege(pMnode, pConsumer, &rsp, pMsg->info.conn.user);
  if (code != 0) {
    goto end;
  }

  atomic_store_32(&pConsumer->hbStatus, 0);

  int32_t status = atomic_load_32(&pConsumer->status);

  if (status == MQ_CONSUMER_STATUS_LOST) {
    mInfo("try to recover consumer:0x%" PRIx64, consumerId);
    mndSendConsumerMsg(pMnode, pConsumer->consumerId, TDMT_MND_TMQ_CONSUMER_RECOVER, &pMsg->info);
  }

  storeOffsetRows(pMnode, &req, pConsumer);
  code = buildMqHbRsp(pMsg, &rsp);

end:
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
    SMqSubscribeObj *pSub = mndAcquireSubscribe(pMnode, pConsumer->cgroup, topic);
    // txn guarantees pSub is created
    if (pSub == NULL) {
      continue;
    }
    taosRLockLatch(&pSub->lock);

    SMqSubTopicEp topicEp = {0};
    strcpy(topicEp.topic, topic);

    // 2.1 fetch topic schema
    SMqTopicObj *pTopic = mndAcquireTopic(pMnode, topic);
    if (pTopic == NULL) {
      taosRUnLockLatch(&pSub->lock);
      mndReleaseSubscribe(pMnode, pSub);
      continue;
    }
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
    SMqConsumerEp *pConsumerEp = taosHashGet(pSub->consumerHash, &pConsumer->consumerId, sizeof(int64_t));
    int32_t        vgNum = taosArrayGetSize(pConsumerEp->vgs);

    // this customer assigned vgroups
    topicEp.vgs = taosArrayInit(vgNum, sizeof(SMqSubVgEp));
    if (topicEp.vgs == NULL) {
      taosRUnLockLatch(&pConsumer->lock);
      taosRUnLockLatch(&pSub->lock);
      mndReleaseSubscribe(pMnode, pSub);
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    for (int32_t j = 0; j < vgNum; j++) {
      SMqVgEp *pVgEp = taosArrayGetP(pConsumerEp->vgs, j);
      //        char     offsetKey[TSDB_PARTITION_KEY_LEN];
      //        mndMakePartitionKey(offsetKey, pConsumer->cgroup, topic, pVgEp->vgId);

      if (epoch == -1) {
        SVgObj *pVgroup = mndAcquireVgroup(pMnode, pVgEp->vgId);
        if (pVgroup) {
          pVgEp->epSet = mndGetVgroupEpset(pMnode, pVgroup);
          mndReleaseVgroup(pMnode, pVgroup);
        }
      }
      // 2.2.1 build vg ep
      SMqSubVgEp vgEp = {
          .epSet = pVgEp->epSet,
          .vgId = pVgEp->vgId,
          .offset = -1,
      };

      taosArrayPush(topicEp.vgs, &vgEp);
    }
    taosArrayPush(rsp->topics, &topicEp);

    taosRUnLockLatch(&pSub->lock);
    mndReleaseSubscribe(pMnode, pSub);
  }
  taosRUnLockLatch(&pConsumer->lock);
  return 0;
}

static int32_t buildAskEpRsp(SRpcMsg *pMsg, SMqAskEpRsp *rsp, int32_t serverEpoch, int64_t consumerId){
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
  tEncodeSMqAskEpRsp(&abuf, rsp);

  // send rsp
  pMsg->info.rsp = buf;
  pMsg->info.rspLen = tlen;
  return 0;
}

static int32_t mndProcessAskEpReq(SRpcMsg *pMsg) {
  SMnode     *pMnode = pMsg->info.node;
  SMqAskEpReq req = {0};
  SMqAskEpRsp rsp = {0};
  int32_t     code = 0;

  if (tDeserializeSMqAskEpReq(pMsg->pCont, pMsg->contLen, &req) < 0) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int64_t consumerId = req.consumerId;

  SMqConsumerObj *pConsumer = mndAcquireConsumer(pMnode, consumerId);
  if (pConsumer == NULL) {
    mError("consumer:0x%" PRIx64 " group:%s not exists in sdb", consumerId, req.cgroup);
    return TSDB_CODE_MND_CONSUMER_NOT_EXIST;
  }

  if (strncmp(req.cgroup, pConsumer->cgroup, tListLen(pConsumer->cgroup)) != 0) {
    mError("consumer:0x%" PRIx64 " group:%s not consistent with data in sdb, saved cgroup:%s", consumerId, req.cgroup,
           pConsumer->cgroup);
    code = TSDB_CODE_MND_CONSUMER_NOT_EXIST;
    goto END;
  }

  atomic_store_32(&pConsumer->hbStatus, 0);

  // 1. check consumer status
  int32_t status = atomic_load_32(&pConsumer->status);

  if (status == MQ_CONSUMER_STATUS_LOST) {
    mndSendConsumerMsg(pMnode, pConsumer->consumerId, TDMT_MND_TMQ_CONSUMER_RECOVER, &pMsg->info);
  }

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
    code = addEpSetInfo(pMnode, pConsumer, epoch, &rsp);
    if(code != 0){
      goto END;
    }
  }

  code = buildAskEpRsp(pMsg, &rsp, serverEpoch, consumerId);

END:
  tDeleteSMqAskEpRsp(&rsp);
  mndReleaseConsumer(pMnode, pConsumer);
  return code;
}

int32_t mndSetConsumerDropLogs(STrans *pTrans, SMqConsumerObj *pConsumer) {
  SSdbRaw *pCommitRaw = mndConsumerActionEncode(pConsumer);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED) != 0) return -1;
  return 0;
}

int32_t mndSetConsumerCommitLogs(STrans *pTrans, SMqConsumerObj *pConsumer) {
  SSdbRaw *pCommitRaw = mndConsumerActionEncode(pConsumer);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY) != 0) return -1;
  return 0;
}

static void freeItem(void *param) {
  void *pItem = *(void **)param;
  if (pItem != NULL) {
    taosMemoryFree(pItem);
  }
}

static int32_t getTopicAddDelete(SMqConsumerObj *pExistedConsumer, SMqConsumerObj *pConsumerNew){
  pConsumerNew->rebNewTopics = taosArrayInit(0, sizeof(void *));
  pConsumerNew->rebRemovedTopics = taosArrayInit(0, sizeof(void *));
  if(pConsumerNew->rebNewTopics == NULL || pConsumerNew->rebRemovedTopics == NULL){
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  int32_t newTopicNum = taosArrayGetSize(pConsumerNew->assignedTopics);
  int32_t oldTopicNum = taosArrayGetSize(pExistedConsumer->currentTopics);

  int32_t i = 0, j = 0;
  while (i < oldTopicNum || j < newTopicNum) {
    if (i >= oldTopicNum) {
      char *newTopicCopy = taosStrdup(taosArrayGetP(pConsumerNew->assignedTopics, j));
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
      char *newTopic = taosArrayGetP(pConsumerNew->assignedTopics, j);
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
  return 0;
}

static int32_t checkAndSortTopic(SMnode *pMnode, SArray *pTopicList){
  taosArraySort(pTopicList, taosArrayCompareString);
  taosArrayRemoveDuplicate(pTopicList, taosArrayCompareString, freeItem);

  int32_t newTopicNum = taosArrayGetSize(pTopicList);
  for (int i = 0; i < newTopicNum; i++) {
    int32_t gNum = mndGetGroupNumByTopic(pMnode, (const char *)taosArrayGetP(pTopicList, i));
    if (gNum >= MND_MAX_GROUP_PER_TOPIC) {
      terrno = TSDB_CODE_TMQ_GROUP_OUT_OF_RANGE;
      return -1;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static SMqConsumerObj* buildSubConsumer(SMnode *pMnode, SCMSubscribeReq *subscribe){
  int64_t         consumerId = subscribe->consumerId;
  char           *cgroup     = subscribe->cgroup;
  SMqConsumerObj *pConsumerNew     = NULL;
  SMqConsumerObj *pExistedConsumer = NULL;
  pExistedConsumer = mndAcquireConsumer(pMnode, consumerId);
  if (pExistedConsumer == NULL) {
    mInfo("receive subscribe request from new consumer:0x%" PRIx64
              ",cgroup:%s, numOfTopics:%d", consumerId,
          subscribe->cgroup, (int32_t)taosArrayGetSize(subscribe->topicNames));

    pConsumerNew = tNewSMqConsumerObj(consumerId, cgroup, CONSUMER_INSERT_SUB, NULL, subscribe);
    if (pConsumerNew == NULL) {
      goto _over;
    }
  } else {
    int32_t status = atomic_load_32(&pExistedConsumer->status);

    mInfo("receive subscribe request from existed consumer:0x%" PRIx64
              ",cgroup:%s, current status:%d(%s), subscribe topic num: %d",
          consumerId, subscribe->cgroup, status, mndConsumerStatusName(status),
          (int32_t)taosArrayGetSize(subscribe->topicNames));

    if (status != MQ_CONSUMER_STATUS_READY) {
      terrno = TSDB_CODE_MND_CONSUMER_NOT_READY;
      goto _over;
    }
    pConsumerNew = tNewSMqConsumerObj(consumerId, cgroup, CONSUMER_UPDATE_SUB, NULL, subscribe);
    if (pConsumerNew == NULL) {
      goto _over;
    }

    int32_t code = getTopicAddDelete(pExistedConsumer, pConsumerNew);
    if (code != 0){
      terrno = code;
      goto _over;
    }
  }
  mndReleaseConsumer(pMnode, pExistedConsumer);
  return pConsumerNew;

_over:
  mndReleaseConsumer(pMnode, pExistedConsumer);
  tDeleteSMqConsumerObj(pConsumerNew);
  return NULL;
}

int32_t mndProcessSubscribeReq(SRpcMsg *pMsg) {
  SMnode *pMnode = pMsg->info.node;
  char   *msgStr = pMsg->pCont;
  int32_t code = 0;

  SCMSubscribeReq subscribe = {0};
  tDeserializeSCMSubscribeReq(msgStr, &subscribe);

  SMqConsumerObj *pConsumerNew = NULL;
  STrans         *pTrans = NULL;

  if(taosArrayGetSize(subscribe.topicNames) == 0){
    SMqConsumerObj *pConsumerTmp = mndAcquireConsumer(pMnode, subscribe.consumerId);
    if(pConsumerTmp == NULL){
      goto _over;
    }
    mndReleaseConsumer(pMnode, pConsumerTmp);
  }

  code = checkAndSortTopic(pMnode, subscribe.topicNames);
  if(code != TSDB_CODE_SUCCESS){
    goto _over;
  }

  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pMsg, "subscribe");
  if (pTrans == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _over;
  }

  code = validateTopics(subscribe.topicNames, pMnode, pMsg->info.conn.user, subscribe.enableReplay);
  if (code != TSDB_CODE_SUCCESS) {
    goto _over;
  }

  pConsumerNew = buildSubConsumer(pMnode, &subscribe);
  if(pConsumerNew == NULL){
    code = -1;
    goto _over;
  }
  code = mndSetConsumerCommitLogs(pTrans, pConsumerNew);
  if (code != 0) goto _over;

  code = mndTransPrepare(pMnode, pTrans);
  if (code != 0) goto _over;
  code = TSDB_CODE_ACTION_IN_PROGRESS;

_over:
  mndTransDrop(pTrans);
  tDeleteSMqConsumerObj(pConsumerNew);
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

static void updateConsumerStatus(SMqConsumerObj *pConsumer) {
  int32_t status = pConsumer->status;

  if (taosArrayGetSize(pConsumer->rebNewTopics) == 0 && taosArrayGetSize(pConsumer->rebRemovedTopics) == 0) {
    if (status == MQ_CONSUMER_STATUS_REBALANCE) {
      pConsumer->status = MQ_CONSUMER_STATUS_READY;
    } else if (status == MQ_CONSUMER_STATUS_READY && taosArrayGetSize(pConsumer->currentTopics) == 0) {
      pConsumer->status = MQ_CONSUMER_STATUS_LOST;
    }
  }
}

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

    if (strcmp(topic, pTopic) == 0) {
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
  } else if (pNewConsumer->updateType == CONSUMER_UPDATE_REC) {
    int32_t sz = taosArrayGetSize(pOldConsumer->assignedTopics);
    for (int32_t i = 0; i < sz; i++) {
      char *topic = taosStrdup(taosArrayGetP(pOldConsumer->assignedTopics, i));
      taosArrayPush(pOldConsumer->rebNewTopics, &topic);
    }
    pOldConsumer->status = MQ_CONSUMER_STATUS_REBALANCE;
    mInfo("consumer:0x%" PRIx64 " recover update", pOldConsumer->consumerId);
  } else if (pNewConsumer->updateType == CONSUMER_UPDATE_REB) {
    atomic_add_fetch_32(&pOldConsumer->epoch, 1);

    pOldConsumer->rebalanceTime = taosGetTimestampMs();
    mInfo("[rebalance] consumer:0x%" PRIx64 " rebalance update, only rebalance time", pOldConsumer->consumerId);
  } else if (pNewConsumer->updateType == CONSUMER_ADD_REB) {
    char *pNewTopic = taosStrdup(taosArrayGetP(pNewConsumer->rebNewTopics, 0));
    removeFromTopicList(pOldConsumer->rebNewTopics, pNewTopic, pOldConsumer->consumerId, "new");
    bool existing = existInCurrentTopicList(pOldConsumer, pNewTopic);
    if (existing) {
      mError("[rebalance] consumer:0x%" PRIx64 " add new topic:%s should not in currentTopics", pOldConsumer->consumerId, pNewTopic);
      taosMemoryFree(pNewTopic);
    } else {
      taosArrayPush(pOldConsumer->currentTopics, &pNewTopic);
      taosArraySort(pOldConsumer->currentTopics, taosArrayCompareString);
    }

    int32_t status = pOldConsumer->status;
    updateConsumerStatus(pOldConsumer);
    pOldConsumer->rebalanceTime = taosGetTimestampMs();
    atomic_add_fetch_32(&pOldConsumer->epoch, 1);

    mInfo("[rebalance] consumer:0x%" PRIx64 " rebalance update add, state (%d)%s -> (%d)%s, new epoch:%d, reb-time:%" PRId64
          ", current topics:%d, newTopics:%d, removeTopics:%d",
          pOldConsumer->consumerId, status, mndConsumerStatusName(status), pOldConsumer->status,
          mndConsumerStatusName(pOldConsumer->status), pOldConsumer->epoch, pOldConsumer->rebalanceTime,
          (int)taosArrayGetSize(pOldConsumer->currentTopics), (int)taosArrayGetSize(pOldConsumer->rebNewTopics),
          (int)taosArrayGetSize(pOldConsumer->rebRemovedTopics));

  } else if (pNewConsumer->updateType == CONSUMER_REMOVE_REB) {
    char *topic = taosArrayGetP(pNewConsumer->rebRemovedTopics, 0);
    removeFromTopicList(pOldConsumer->rebRemovedTopics, topic, pOldConsumer->consumerId, "remove");
    removeFromTopicList(pOldConsumer->currentTopics, topic, pOldConsumer->consumerId, "current");

    int32_t status = pOldConsumer->status;
    updateConsumerStatus(pOldConsumer);
    pOldConsumer->rebalanceTime = taosGetTimestampMs();
    atomic_add_fetch_32(&pOldConsumer->epoch, 1);

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
      blockDataEnsureCapacity(pBlock, numOfRows + topicSz);
    }

    for (int32_t i = 0; i < topicSz; i++) {
      SColumnInfoData *pColInfo;
      int32_t          cols = 0;

      // consumer id
      char consumerIdHex[32] = {0};
      sprintf(varDataVal(consumerIdHex), "0x%" PRIx64, pConsumer->consumerId);
      varDataSetLen(consumerIdHex, strlen(varDataVal(consumerIdHex)));

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)consumerIdHex, false);

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
      colDataSetVal(pColInfo, numOfRows, (const char *)&pConsumer->createTime, false);

      // subscribe time
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)&pConsumer->subscribeTime, false);

      // rebalance time
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)&pConsumer->rebalanceTime, pConsumer->rebalanceTime == 0);

      char         buf[TSDB_OFFSET_LEN] = {0};
      STqOffsetVal pVal = {.type = pConsumer->resetOffsetCfg};
      tFormatOffset(buf, TSDB_OFFSET_LEN, &pVal);

      char parasStr[64 + TSDB_OFFSET_LEN + VARSTR_HEADER_SIZE] = {0};
      sprintf(varDataVal(parasStr), "tbname:%d,commit:%d,interval:%dms,reset:%s", pConsumer->withTbName,
              pConsumer->autoCommit, pConsumer->autoCommitInterval, buf);
      varDataSetLen(parasStr, strlen(varDataVal(parasStr)));

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)parasStr, false);

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

const char *mndConsumerStatusName(int status) {
  switch (status) {
    case MQ_CONSUMER_STATUS_READY:
      return "ready";
    case MQ_CONSUMER_STATUS_LOST:
      return "lost";
    case MQ_CONSUMER_STATUS_REBALANCE:
      return "rebalancing";
    default:
      return "unknown";
  }
}
