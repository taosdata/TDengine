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

static char *mndMakeSubscribeKey(char *cgroup, char *topicName);

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

static int mndBuildMqSetConsumerVgReq(SMnode *pMnode, STrans *pTrans, SMqConsumerObj *pConsumer,
                                      SMqConsumerTopic *pConsumerTopic, SMqTopicObj *pTopic, SMqConsumerEp *pSub);

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

static int32_t mndProcessGetSubEpReq(SMnodeMsg *pMsg) {
  SMnode           *pMnode = pMsg->pMnode;
  SMqCMGetSubEpReq *pReq = (SMqCMGetSubEpReq *)pMsg->rpcMsg.pCont;
  SMqCMGetSubEpRsp  rsp;
  int64_t           consumerId = be64toh(pReq->consumerId);

  SMqConsumerObj *pConsumer = mndAcquireConsumer(pMsg->pMnode, consumerId);
  if (pConsumer == NULL) {
    terrno = TSDB_CODE_MND_CONSUMER_NOT_EXIST;
    return -1;
  }
  ASSERT(strcmp(pReq->cgroup, pConsumer->cgroup) == 0);

  strcpy(rsp.cgroup, pReq->cgroup);
  rsp.consumerId = consumerId;
  SArray *pTopics = pConsumer->topics;
  int32_t sz = taosArrayGetSize(pTopics);
  rsp.topics = taosArrayInit(sz, sizeof(SMqSubTopicEp));
  for (int32_t i = 0; i < sz; i++) {
    SMqSubTopicEp     topicEp;
    SMqConsumerTopic *pConsumerTopic = taosArrayGet(pTopics, i);
    strcpy(topicEp.topic, pConsumerTopic->name);

    SMqSubscribeObj *pSub = mndAcquireSubscribe(pMnode, pConsumer->cgroup, pConsumerTopic->name);
    int32_t          assignedSz = taosArrayGetSize(pSub->assigned);
    topicEp.vgs = taosArrayInit(assignedSz, sizeof(SMqSubVgEp));
    for (int32_t j = 0; j < assignedSz; j++) {
      SMqConsumerEp *pCEp = taosArrayGet(pSub->assigned, j);
      if (pCEp->consumerId == consumerId) {
        SMqSubVgEp vgEp = {
          .epSet = pCEp->epSet,
          .vgId = pCEp->vgId
        };
        taosArrayPush(topicEp.vgs, &vgEp);
      }
    }
    if (taosArrayGetSize(topicEp.vgs) != 0) {
      taosArrayPush(rsp.topics, &topicEp);
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
  //TODO: free rsp
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
  *topic = strdup(key);
  key[i] = ':';
  *cgroup = strdup(&key[i + 1]);
  return 0;
}

static int32_t mndProcessMqTimerMsg(SMnodeMsg *pMsg) {
  SMnode          *pMnode = pMsg->pMnode;
  SSdb            *pSdb = pMnode->pSdb;
  SMqSubscribeObj *pSub = NULL;
  void            *pIter = sdbFetch(pSdb, SDB_SUBSCRIBE, NULL, (void **)&pSub);
  int              sz;
  while (pIter != NULL) {
    if ((sz = taosArrayGetSize(pSub->unassignedVg)) > 0) {
      char *topic = NULL;
      char *cgroup = NULL;
      mndSplitSubscribeKey(pSub->key, &topic, &cgroup);

      SMqTopicObj *pTopic = mndAcquireTopic(pMnode, topic);

      // create trans
      STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, &pMsg->rpcMsg);
      for (int i = 0; i < sz; i++) {
        int64_t        consumerId = *(int64_t *)taosArrayGet(pSub->availConsumer, pSub->nextConsumerIdx);
        SMqConsumerEp *pCEp = taosArrayPop(pSub->unassignedVg);
        pCEp->consumerId = consumerId;
        taosArrayPush(pSub->assigned, pCEp);
        pSub->nextConsumerIdx = (pSub->nextConsumerIdx + 1) % taosArrayGetSize(pSub->availConsumer);

        // build msg

        SMqSetCVgReq *pReq = malloc(sizeof(SMqSetCVgReq));
        if (pReq == NULL) {
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          return -1;
        }
        strcpy(pReq->cgroup, cgroup);
        strcpy(pReq->topicName, topic);
        pReq->sql = strdup(pTopic->sql);
        pReq->logicalPlan = strdup(pTopic->logicalPlan);
        pReq->physicalPlan = strdup(pTopic->physicalPlan);
        pReq->qmsg = strdup(pCEp->qmsg);
        int32_t tlen = tEncodeSMqSetCVgReq(NULL, pReq);
        void   *reqStr = malloc(tlen);
        if (reqStr == NULL) {
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          return -1;
        }
        void *abuf = reqStr;
        tEncodeSMqSetCVgReq(&abuf, pReq);

        // persist msg
        STransAction action = {0};
        action.epSet = pCEp->epSet;
        action.pCont = reqStr;
        action.contLen = tlen;
        action.msgType = TDMT_VND_MQ_SET_CONN;
        mndTransAppendRedoAction(pTrans, &action);

        // persist raw
        SSdbRaw *pRaw = mndSubActionEncode(pSub);
        mndTransAppendRedolog(pTrans, pRaw);

        free(pReq);
        tfree(topic);
        tfree(cgroup);
      }
      if (mndTransPrepare(pMnode, pTrans) != 0) {
        mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
      }
      /*mndReleaseTopic(pMnode, pTopic);*/
      mndTransDrop(pTrans);
    }
    pIter = sdbFetch(pSdb, SDB_SUBSCRIBE, NULL, (void **)&pSub);
  }
  return 0;
}

static int mndInitUnassignedVg(SMnode *pMnode, SMqTopicObj *pTopic, SArray *unassignedVg) {
  // convert phyplan to dag
  SQueryDag *pDag = qStringToDag(pTopic->physicalPlan);
  SArray    *pArray;
  SArray    *inner = taosArrayGet(pDag->pSubplans, 0);
  SSubplan  *plan = taosArrayGetP(inner, 0);

  plan->execNode.nodeId = 2;
  SEpSet* pEpSet = &plan->execNode.epset;

  pEpSet->inUse = 0;
  addEpIntoEpSet(pEpSet, "localhost", 6030);
  if (schedulerConvertDagToTaskList(pDag, &pArray) < 0) {
    return -1;
  }
  int32_t sz = taosArrayGetSize(pArray);
  // convert dag to msg
  for (int32_t i = 0; i < sz; i++) {
    SMqConsumerEp CEp;
    CEp.status = 0;
    CEp.consumerId = -1;
    CEp.lastConsumerHbTs = CEp.lastVgHbTs = -1;
    STaskInfo *pTaskInfo = taosArrayGet(pArray, i);
    CEp.epSet = pTaskInfo->addr.epset;
    
    /*mDebug("subscribe convert ep %d %s %s %s %s %s\n", CEp.epSet.numOfEps, CEp.epSet.fqdn[0], CEp.epSet.fqdn[1],
     * CEp.epSet.fqdn[2], CEp.epSet.fqdn[3], CEp.epSet.fqdn[4]);*/
    CEp.vgId = pTaskInfo->addr.nodeId;
    CEp.qmsg = strdup(pTaskInfo->msg->msg);
    taosArrayPush(unassignedVg, &CEp);
  }

  /*qDestroyQueryDag(pDag);*/
  return 0;
}

static int mndBuildMqSetConsumerVgReq(SMnode *pMnode, STrans *pTrans, SMqConsumerObj *pConsumer,
                                      SMqConsumerTopic *pConsumerTopic, SMqTopicObj *pTopic, SMqConsumerEp *pCEp) {
  int32_t sz = taosArrayGetSize(pConsumerTopic->pVgInfo);
  for (int32_t i = 0; i < sz; i++) {
    int32_t      vgId = *(int32_t *)taosArrayGet(pConsumerTopic->pVgInfo, i);
    SVgObj      *pVgObj = mndAcquireVgroup(pMnode, vgId);
    SMqSetCVgReq req = {
        .vgId = vgId,
        .oldConsumerId = -1,
        .newConsumerId = pConsumer->consumerId,
    };
    strcpy(req.cgroup, pConsumer->cgroup);
    strcpy(req.topicName, pTopic->name);
    req.sql = pTopic->sql;
    req.logicalPlan = pTopic->logicalPlan;
    req.physicalPlan = pTopic->physicalPlan;
    req.qmsg = pCEp->qmsg;
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
  }
  return 0;
}

void mndCleanupSubscribe(SMnode *pMnode) {}

static SSdbRaw *mndSubActionEncode(SMqSubscribeObj *pSub) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  int32_t tlen = tEncodeSubscribeObj(NULL, pSub);
  int32_t size = sizeof(int32_t) + tlen + MND_SUBSCRIBE_RESERVE_SIZE;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_SUBSCRIBE, MND_SUBSCRIBE_VER_NUMBER, size);
  if (pRaw == NULL) goto SUB_ENCODE_OVER;

  void *buf = malloc(tlen);
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
  void *buf = malloc(tlen + 1);
  if (buf == NULL) goto SUB_DECODE_OVER;
  SDB_GET_BINARY(pRaw, dataPos, buf, tlen, SUB_DECODE_OVER);
  SDB_GET_RESERVE(pRaw, dataPos, MND_SUBSCRIBE_RESERVE_SIZE, SUB_DECODE_OVER);

  if (tDecodeSubscribeObj(buf, pSub) == NULL) {
    goto SUB_DECODE_OVER;
  }

  terrno = TSDB_CODE_SUCCESS;

SUB_DECODE_OVER:
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("subscribe:%s, failed to decode from raw:%p since %s", pSub->key, pRaw, terrstr());
    // TODO free subscribeobj
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
  return 0;
}

static int32_t mndSubActionUpdate(SSdb *pSdb, SMqSubscribeObj *pOldSub, SMqSubscribeObj *pNewSub) {
  mTrace("subscribe:%s, perform update action", pOldSub->key);
  return 0;
}

static void *mndBuildMqVGroupSetReq(SMnode *pMnode, char *topicName, int32_t vgId, int64_t consumerId, char *cgroup) {
  return 0;
}

static char *mndMakeSubscribeKey(char *cgroup, char *topicName) {
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
  char   *consumerGroup = subscribe.consumerGroup;
  int32_t cgroupLen = strlen(consumerGroup);

  SArray *newSub = subscribe.topicNames;
  int     newTopicNum = subscribe.topicNum;

  taosArraySortString(newSub, taosArrayCompareString);

  SArray *oldSub = NULL;
  int     oldTopicNum = 0;
  // create consumer if not exist
  SMqConsumerObj *pConsumer = mndAcquireConsumer(pMnode, consumerId);
  if (pConsumer == NULL) {
    // create consumer
    pConsumer = malloc(sizeof(SMqConsumerObj));
    if (pConsumer == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
    pConsumer->consumerId = consumerId;
    strcpy(pConsumer->cgroup, consumerGroup);
    taosInitRWLatch(&pConsumer->lock);
  } else {
    oldSub = pConsumer->topics;
  }
  pConsumer->topics = taosArrayInit(newTopicNum, sizeof(SMqConsumerTopic));

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
      oldTopicName = ((SMqConsumerTopic *)taosArrayGet(oldSub, j))->name;
      j++;
    } else if (j >= oldTopicNum) {
      newTopicName = taosArrayGetP(newSub, i);
      i++;
    } else {
      newTopicName = taosArrayGetP(newSub, i);
      oldTopicName = ((SMqConsumerTopic *)taosArrayGet(oldSub, j))->name;

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
#if 0
      // cancel subscribe of that old topic
      ASSERT(pNewTopic == NULL);
      char     *oldTopicName = pOldTopic->name;
      SList    *vgroups = pOldTopic->vgroups;
      SListIter iter;
      tdListInitIter(vgroups, &iter, TD_LIST_FORWARD);
      SListNode *pn;

      SMqTopicObj *pTopic = mndAcquireTopic(pMnode, oldTopicName);
      ASSERT(pTopic != NULL);
      SMqSubscribeObj *pSub = mndAcquireSubscribe(pMnode, consumerGroup, oldTopicName);
      SMqCGroup *pGroup = taosHashGet(pTopic->cgroups, consumerGroup, cgroupLen);
      while ((pn = tdListNext(&iter)) != NULL) {
        int32_t vgId = *(int64_t *)pn->data;
        // acquire and get epset
        SVgObj *pVgObj = mndAcquireVgroup(pMnode, vgId);
        // TODO what time to release?
        if (pVgObj == NULL) {
          // TODO handle error
          continue;
        }
        // build reset msg
        void *pMqVgSetReq = mndBuildMqVGroupSetReq(pMnode, oldTopicName, vgId, consumerId, consumerGroup);
        // TODO:serialize
        if (pMsg == NULL) {
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          return -1;
        }
        STransAction action = {0};
        action.epSet = mndGetVgroupEpset(pMnode, pVgObj);
        action.pCont = pMqVgSetReq;
        action.contLen = 0;  // TODO
        action.msgType = TDMT_VND_MQ_SET_CONN;
        if (mndTransAppendRedoAction(pTrans, &action) != 0) {
          free(pMqVgSetReq);
          mndTransDrop(pTrans);
          // TODO free
          return -1;
        }
      }
      // delete data in mnode
      taosHashRemove(pTopic->cgroups, consumerGroup, cgroupLen);
      mndReleaseSubscribe(pMnode, pSub);
      mndReleaseTopic(pMnode, pTopic);
#endif
    } else if (newTopicName != NULL) {
      // save subscribe info to mnode
      ASSERT(oldTopicName == NULL);

      SMqTopicObj *pTopic = mndAcquireTopic(pMnode, newTopicName);
      if (pTopic == NULL) {
        mError("topic being subscribed not exist: %s", newTopicName);
        continue;
      }

      SMqSubscribeObj *pSub = mndAcquireSubscribe(pMnode, consumerGroup, newTopicName);
      if (pSub == NULL) {
        mDebug("create new subscription, group: %s, topic %s", consumerGroup, newTopicName);
        pSub = tNewSubscribeObj();
        if (pSub == NULL) {
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          return -1;
        }
        char *key = mndMakeSubscribeKey(consumerGroup, newTopicName);
        strcpy(pSub->key, key);
        // set unassigned vg
        mndInitUnassignedVg(pMnode, pTopic, pSub->unassignedVg);
        // TODO: disable alter
      }
      taosArrayPush(pSub->availConsumer, &consumerId);

      SMqConsumerTopic *pConsumerTopic = tNewConsumerTopic(consumerId, pTopic, pSub);
      taosArrayPush(pConsumer->topics, pConsumerTopic);

      if (taosArrayGetSize(pConsumerTopic->pVgInfo) > 0) {
        ASSERT(taosArrayGetSize(pConsumerTopic->pVgInfo) == 1);
        int32_t        vgId = *(int32_t *)taosArrayGetLast(pConsumerTopic->pVgInfo);
        SMqConsumerEp *pCEp = taosArrayGetLast(pSub->assigned);
        if (pCEp->vgId == vgId) {
          if (mndBuildMqSetConsumerVgReq(pMnode, pTrans, pConsumer, pConsumerTopic, pTopic, pCEp) < 0) {
            // TODO
            return -1;
          }
        }
        // send setmsg to vnode
      }

      SSdbRaw *pRaw = mndSubActionEncode(pSub);
      sdbSetRawStatus(pRaw, SDB_STATUS_READY);
      mndTransAppendRedolog(pTrans, pRaw);
#if 0
      SMqCGroup *pGroup = taosHashGet(pTopic->cgroups, consumerGroup, cgroupLen);
      if (pGroup == NULL) {
        // add new group
        pGroup = malloc(sizeof(SMqCGroup));
        if (pGroup == NULL) {
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          return -1;
        }
        pGroup->consumerIds = tdListNew(sizeof(int64_t));
        if (pGroup->consumerIds == NULL) {
          free(pGroup);
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          return -1;
        }
        pGroup->status = 0;
        // add into cgroups
        taosHashPut(pTopic->cgroups, consumerGroup, cgroupLen, pGroup, sizeof(SMqCGroup));
      }
      /*taosHashPut(pTopic->consumers, &pConsumer->consumerId, sizeof(int64_t), pConsumer, sizeof(SMqConsumerObj));*/

      // put the consumer into list
      // rebalance will be triggered by timer
      tdListAppend(pGroup->consumerIds, &consumerId);

      SSdbRaw *pTopicRaw = mndTopicActionEncode(pTopic);
      sdbSetRawStatus(pTopicRaw, SDB_STATUS_READY);
      // TODO: error handling
      mndTransAppendRedolog(pTrans, pTopicRaw);

#endif
      /*mndReleaseTopic(pMnode, pTopic);*/
      /*mndReleaseSubscribe(pMnode, pSub);*/
    }
  }
  // part3. persist consumerObj

  // destroy old sub
  if (oldSub) taosArrayDestroy(oldSub);
  // put new sub into consumerobj

  // persist consumerObj
  SSdbRaw *pConsumerRaw = mndConsumerActionEncode(pConsumer);
  sdbSetRawStatus(pConsumerRaw, SDB_STATUS_READY);
  // TODO: error handling
  mndTransAppendRedolog(pTrans, pConsumerRaw);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    if (newSub) taosArrayDestroy(newSub);
    mndTransDrop(pTrans);
    /*mndReleaseConsumer(pMnode, pConsumer);*/
    return -1;
  }

  if (newSub) taosArrayDestroy(newSub);
  mndTransDrop(pTrans);
  /*mndReleaseConsumer(pMnode, pConsumer);*/
  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static int32_t mndProcessSubscribeInternalRsp(SMnodeMsg *pRsp) {
  mndTransProcessRsp(pRsp);
  return 0;
}

static int32_t mndProcessConsumerMetaMsg(SMnodeMsg *pMsg) {
  SMnode        *pMnode = pMsg->pMnode;
  STableInfoReq *pInfo = pMsg->rpcMsg.pCont;

  mDebug("subscribe:%s, start to retrieve meta", pInfo->tableFname);

#if 0
  SDbObj *pDb = mndAcquireDbByConsumer(pMnode, pInfo->tableFname);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_SELECTED;
    mError("consumer:%s, failed to retrieve meta since %s", pInfo->tableFname, terrstr());
    return -1;
  }

  SConsumerObj *pConsumer = mndAcquireConsumer(pMnode, pInfo->tableFname);
  if (pConsumer == NULL) {
    mndReleaseDb(pMnode, pDb);
    terrno = TSDB_CODE_MND_INVALID_CONSUMER;
    mError("consumer:%s, failed to get meta since %s", pInfo->tableFname, terrstr());
    return -1;
  }

  taosRLockLatch(&pConsumer->lock);
  int32_t totalCols = pConsumer->numOfColumns + pConsumer->numOfTags;
  int32_t contLen = sizeof(STableMetaRsp) + totalCols * sizeof(SSchema);

  STableMetaRsp *pMeta = rpcMallocCont(contLen);
  if (pMeta == NULL) {
    taosRUnLockLatch(&pConsumer->lock);
    mndReleaseDb(pMnode, pDb);
    mndReleaseConsumer(pMnode, pConsumer);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("consumer:%s, failed to get meta since %s", pInfo->tableFname, terrstr());
    return -1;
  }

  memcpy(pMeta->consumerFname, pConsumer->name, TSDB_TABLE_FNAME_LEN);
  pMeta->numOfTags = htonl(pConsumer->numOfTags);
  pMeta->numOfColumns = htonl(pConsumer->numOfColumns);
  pMeta->precision = pDb->cfg.precision;
  pMeta->tableType = TSDB_SUPER_TABLE;
  pMeta->update = pDb->cfg.update;
  pMeta->sversion = htonl(pConsumer->version);
  pMeta->tuid = htonl(pConsumer->uid);

  for (int32_t i = 0; i < totalCols; ++i) {
    SSchema *pSchema = &pMeta->pSchema[i];
    SSchema *pSrcSchema = &pConsumer->pSchema[i];
    memcpy(pSchema->name, pSrcSchema->name, TSDB_COL_NAME_LEN);
    pSchema->type = pSrcSchema->type;
    pSchema->colId = htonl(pSrcSchema->colId);
    pSchema->bytes = htonl(pSrcSchema->bytes);
  }
  taosRUnLockLatch(&pConsumer->lock);
  mndReleaseDb(pMnode, pDb);
  mndReleaseConsumer(pMnode, pConsumer);

  pMsg->pCont = pMeta;
  pMsg->contLen = contLen;

  mDebug("consumer:%s, meta is retrieved, cols:%d tags:%d", pInfo->tableFname, pConsumer->numOfColumns, pConsumer->numOfTags);
#endif
  return 0;
}

static int32_t mndGetNumOfConsumers(SMnode *pMnode, char *dbName, int32_t *pNumOfConsumers) {
  SSdb *pSdb = pMnode->pSdb;

  SDbObj *pDb = mndAcquireDb(pMnode, dbName);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_SELECTED;
    return -1;
  }

  int32_t numOfConsumers = 0;
  void   *pIter = NULL;
  while (1) {
    SMqConsumerObj *pConsumer = NULL;
    pIter = sdbFetch(pSdb, SDB_CONSUMER, pIter, (void **)&pConsumer);
    if (pIter == NULL) break;

    numOfConsumers++;

    sdbRelease(pSdb, pConsumer);
  }

  *pNumOfConsumers = numOfConsumers;
  return 0;
}

static int32_t mndGetConsumerMeta(SMnodeMsg *pMsg, SShowObj *pShow, STableMetaRsp *pMeta) {
  SMnode *pMnode = pMsg->pMnode;
  SSdb   *pSdb = pMnode->pSdb;

  if (mndGetNumOfConsumers(pMnode, pShow->db, &pShow->numOfRows) != 0) {
    return -1;
  }

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->pSchema;

  pShow->bytes[cols] = TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "name");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create_time");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "columns");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "tags");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htonl(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = sdbGetSize(pSdb, SDB_CONSUMER);
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  strcpy(pMeta->tbFname, mndShowStr(pShow->type));

  return 0;
}

static void mndCancelGetNextConsumer(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}
