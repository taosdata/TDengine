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
#include "mndScheduler.h"
#include "mndShow.h"
#include "mndTopic.h"
#include "mndTrans.h"
#include "mndVgroup.h"
#include "tcompare.h"
#include "tname.h"

#define MND_SUBSCRIBE_VER_NUMBER   3
#define MND_SUBSCRIBE_RESERVE_SIZE 64

#define MND_CONSUMER_LOST_HB_CNT          6
#define MND_CONSUMER_LOST_CLEAR_THRESHOLD 43200

static int32_t mqRebInExecCnt = 0;

static SSdbRaw *mndSubActionEncode(SMqSubscribeObj *);
static SSdbRow *mndSubActionDecode(SSdbRaw *pRaw);
static int32_t  mndSubActionInsert(SSdb *pSdb, SMqSubscribeObj *);
static int32_t  mndSubActionDelete(SSdb *pSdb, SMqSubscribeObj *);
static int32_t  mndSubActionUpdate(SSdb *pSdb, SMqSubscribeObj *pOldSub, SMqSubscribeObj *pNewSub);
static int32_t  mndProcessRebalanceReq(SRpcMsg *pMsg);
static int32_t  mndProcessDropCgroupReq(SRpcMsg *pMsg);
static int32_t  mndRetrieveSubscribe(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextSubscribe(SMnode *pMnode, void *pIter);
static int32_t  mndCheckConsumer(SRpcMsg *pMsg, SHashObj* hash);

static int32_t mndSetSubCommitLogs(SMnode *pMnode, STrans *pTrans, SMqSubscribeObj *pSub) {
  SSdbRaw *pCommitRaw = mndSubActionEncode(pSub);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY) != 0) return -1;
  return 0;
}

int32_t mndInitSubscribe(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_SUBSCRIBE,
      .keyType = SDB_KEY_BINARY,
      .encodeFp = (SdbEncodeFp)mndSubActionEncode,
      .decodeFp = (SdbDecodeFp)mndSubActionDecode,
      .insertFp = (SdbInsertFp)mndSubActionInsert,
      .updateFp = (SdbUpdateFp)mndSubActionUpdate,
      .deleteFp = (SdbDeleteFp)mndSubActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_VND_TMQ_SUBSCRIBE_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_TMQ_DELETE_SUB_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_TMQ_TIMER, mndProcessRebalanceReq);
  mndSetMsgHandle(pMnode, TDMT_MND_TMQ_DROP_CGROUP, mndProcessDropCgroupReq);
  mndSetMsgHandle(pMnode, TDMT_MND_TMQ_DROP_CGROUP_RSP, mndTransProcessRsp);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_SUBSCRIPTIONS, mndRetrieveSubscribe);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_TOPICS, mndCancelGetNextSubscribe);

  return sdbSetTable(pMnode->pSdb, table);
}

static SMqSubscribeObj *mndCreateSubscription(SMnode *pMnode, const SMqTopicObj *pTopic, const char *subKey) {
  SMqSubscribeObj *pSub = tNewSubscribeObj(subKey);
  if (pSub == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pSub->dbUid = pTopic->dbUid;
  pSub->stbUid = pTopic->stbUid;
  pSub->subType = pTopic->subType;
  pSub->withMeta = pTopic->withMeta;

  if (mndSchedInitSubEp(pMnode, pTopic, pSub) < 0) {
    tDeleteSubscribeObj(pSub);
    taosMemoryFree(pSub);
    return NULL;
  }

  return pSub;
}

static int32_t mndBuildSubChangeReq(void **pBuf, int32_t *pLen, SMqSubscribeObj *pSub,
                                    const SMqRebOutputVg *pRebVg, SSubplan* pPlan) {
  SMqRebVgReq req = {0};
  req.oldConsumerId = pRebVg->oldConsumerId;
  req.newConsumerId = pRebVg->newConsumerId;
  req.vgId = pRebVg->pVgEp->vgId;
  if(pPlan){
    pPlan->execNode.epSet = pRebVg->pVgEp->epSet;
    pPlan->execNode.nodeId = pRebVg->pVgEp->vgId;
    int32_t msgLen;
    if (qSubPlanToString(pPlan, &req.qmsg, &msgLen) < 0) {
      terrno = TSDB_CODE_QRY_INVALID_INPUT;
      return -1;
    }
  }else{
    req.qmsg = taosStrdup("");
  }
  req.subType = pSub->subType;
  req.withMeta = pSub->withMeta;
  req.suid = pSub->stbUid;
  tstrncpy(req.subKey, pSub->key, TSDB_SUBSCRIBE_KEY_LEN);

  int32_t tlen = 0;
  int32_t ret = 0;
  tEncodeSize(tEncodeSMqRebVgReq, &req, tlen, ret);
  if (ret < 0) {
    taosMemoryFree(req.qmsg);
    return -1;
  }

  tlen += sizeof(SMsgHead);
  void   *buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    taosMemoryFree(req.qmsg);
    return -1;
  }

  SMsgHead *pMsgHead = (SMsgHead *)buf;

  pMsgHead->contLen = htonl(tlen);
  pMsgHead->vgId = htonl(pRebVg->pVgEp->vgId);

  SEncoder encoder = {0};
  tEncoderInit(&encoder, POINTER_SHIFT(buf, sizeof(SMsgHead)), tlen);
  if (tEncodeSMqRebVgReq(&encoder, &req) < 0) {
    taosMemoryFreeClear(buf);
    tEncoderClear(&encoder);
    taosMemoryFree(req.qmsg);
    return -1;
  }
  tEncoderClear(&encoder);
  *pBuf = buf;
  *pLen = tlen;

  taosMemoryFree(req.qmsg);
  return 0;
}

static int32_t mndPersistSubChangeVgReq(SMnode *pMnode, STrans *pTrans, SMqSubscribeObj *pSub,
                                        const SMqRebOutputVg *pRebVg, SSubplan* pPlan) {
  if (pRebVg->oldConsumerId == pRebVg->newConsumerId) {
    if(pRebVg->oldConsumerId == -1) return 0;   //drop stream, no consumer, while split vnode,all consumerId is -1
    terrno = TSDB_CODE_MND_INVALID_SUB_OPTION;
    return -1;
  }

  void   *buf;
  int32_t tlen;
  if (mndBuildSubChangeReq(&buf, &tlen, pSub, pRebVg, pPlan) < 0) {
    return -1;
  }

  int32_t vgId = pRebVg->pVgEp->vgId;
  SVgObj *pVgObj = mndAcquireVgroup(pMnode, vgId);
  if (pVgObj == NULL) {
    taosMemoryFree(buf);
    terrno = TSDB_CODE_MND_VGROUP_NOT_EXIST;
    return -1;
  }

  STransAction action = {0};
  action.epSet = mndGetVgroupEpset(pMnode, pVgObj);
  action.pCont = buf;
  action.contLen = tlen;
  action.msgType = TDMT_VND_TMQ_SUBSCRIBE;

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

static void doRemoveLostConsumers(SMqRebOutputObj *pOutput, SHashObj *pHash, const SMqRebInputObj *pInput) {
  int32_t     numOfRemoved = taosArrayGetSize(pInput->pRebInfo->removedConsumers);
  const char *pSubKey = pOutput->pSub->key;

  int32_t actualRemoved = 0;
  for (int32_t i = 0; i < numOfRemoved; i++) {
    uint64_t consumerId = *(uint64_t *)taosArrayGet(pInput->pRebInfo->removedConsumers, i);

    SMqConsumerEp *pConsumerEp = taosHashGet(pOutput->pSub->consumerHash, &consumerId, sizeof(int64_t));

    // consumer exists till now
    if (pConsumerEp) {
      actualRemoved++;

      int32_t consumerVgNum = taosArrayGetSize(pConsumerEp->vgs);
      for (int32_t j = 0; j < consumerVgNum; j++) {
        SMqVgEp *pVgEp = taosArrayGetP(pConsumerEp->vgs, j);

        SMqRebOutputVg outputVg = {.oldConsumerId = consumerId, .newConsumerId = -1, .pVgEp = pVgEp};
        taosHashPut(pHash, &pVgEp->vgId, sizeof(int32_t), &outputVg, sizeof(SMqRebOutputVg));
        mInfo("sub:%s mq re-balance remove vgId:%d from consumer:0x%" PRIx64, pSubKey, pVgEp->vgId, consumerId);
      }

      taosArrayDestroy(pConsumerEp->vgs);
      taosHashRemove(pOutput->pSub->consumerHash, &consumerId, sizeof(int64_t));

      // put into removed
      taosArrayPush(pOutput->removedConsumers, &consumerId);
    }
  }

  if (numOfRemoved != actualRemoved) {
    mError("sub:%s mq re-balance removedNum:%d not matched with actual:%d", pSubKey, numOfRemoved, actualRemoved);
  } else {
    mInfo("sub:%s removed %d consumers", pSubKey, numOfRemoved);
  }
}

static void doAddNewConsumers(SMqRebOutputObj *pOutput, const SMqRebInputObj *pInput) {
  int32_t     numOfNewConsumers = taosArrayGetSize(pInput->pRebInfo->newConsumers);
  const char *pSubKey = pOutput->pSub->key;

  for (int32_t i = 0; i < numOfNewConsumers; i++) {
    int64_t consumerId = *(int64_t *)taosArrayGet(pInput->pRebInfo->newConsumers, i);

    SMqConsumerEp newConsumerEp = {0};
    newConsumerEp.consumerId = consumerId;
    newConsumerEp.vgs = taosArrayInit(0, sizeof(void *));

    taosHashPut(pOutput->pSub->consumerHash, &consumerId, sizeof(int64_t), &newConsumerEp, sizeof(SMqConsumerEp));
    taosArrayPush(pOutput->newConsumers, &consumerId);
    mInfo("sub:%s mq rebalance add new consumer:0x%" PRIx64, pSubKey, consumerId);
  }
}

static void addUnassignedVgroups(SMqRebOutputObj *pOutput, SHashObj *pHash) {
  const char *pSubKey = pOutput->pSub->key;
  int32_t     numOfVgroups = taosArrayGetSize(pOutput->pSub->unassignedVgs);

  for (int32_t i = 0; i < numOfVgroups; i++) {
    SMqVgEp       *pVgEp = *(SMqVgEp **)taosArrayPop(pOutput->pSub->unassignedVgs);
    SMqRebOutputVg rebOutput = {
        .oldConsumerId = -1,
        .newConsumerId = -1,
        .pVgEp = pVgEp,
    };

    taosHashPut(pHash, &pVgEp->vgId, sizeof(int32_t), &rebOutput, sizeof(SMqRebOutputVg));
    mInfo("sub:%s mq re-balance addUnassignedVgroups vgId:%d from unassigned", pSubKey, pVgEp->vgId);
  }
}

//static void putNoTransferToOutput(SMqRebOutputObj *pOutput, SMqConsumerEp *pConsumerEp){
//  for(int i = 0; i < taosArrayGetSize(pConsumerEp->vgs); i++){
//    SMqVgEp       *pVgEp = (SMqVgEp *)taosArrayGetP(pConsumerEp->vgs, i);
//    SMqRebOutputVg outputVg = {
//        .oldConsumerId = pConsumerEp->consumerId,
//        .newConsumerId = pConsumerEp->consumerId,
//        .pVgEp = pVgEp,
//    };
//    taosArrayPush(pOutput->rebVgs, &outputVg);
//  }
//}

static void transferVgroupsForConsumers(SMqRebOutputObj *pOutput, SHashObj *pHash, int32_t minVgCnt,
                                        int32_t imbConsumerNum) {
  const char *pSubKey = pOutput->pSub->key;

  int32_t imbCnt = 0;
  void   *pIter = NULL;

  while (1) {
    pIter = taosHashIterate(pOutput->pSub->consumerHash, pIter);
    if (pIter == NULL) {
      break;
    }

    SMqConsumerEp *pConsumerEp = (SMqConsumerEp *)pIter;
    int32_t        consumerVgNum = taosArrayGetSize(pConsumerEp->vgs);

    // all old consumers still existing need to be modified
    // TODO optimize: modify only consumer whose vgs changed
    taosArrayPush(pOutput->modifyConsumers, &pConsumerEp->consumerId);
    if (consumerVgNum > minVgCnt) {
      if (imbCnt < imbConsumerNum) {
        // pop until equal minVg + 1
        while (taosArrayGetSize(pConsumerEp->vgs) > minVgCnt + 1) {
          SMqVgEp       *pVgEp = *(SMqVgEp **)taosArrayPop(pConsumerEp->vgs);
          SMqRebOutputVg outputVg = {
              .oldConsumerId = pConsumerEp->consumerId,
              .newConsumerId = -1,
              .pVgEp = pVgEp,
          };
          taosHashPut(pHash, &pVgEp->vgId, sizeof(int32_t), &outputVg, sizeof(SMqRebOutputVg));
          mInfo("sub:%s mq rebalance remove vgId:%d from consumer:0x%" PRIx64 ",(first scan)", pSubKey, pVgEp->vgId,
                pConsumerEp->consumerId);
        }
        imbCnt++;
      } else {
        // all the remain consumers should only have the number of vgroups, which is equalled to the value of minVg
        while (taosArrayGetSize(pConsumerEp->vgs) > minVgCnt) {
          SMqVgEp       *pVgEp = *(SMqVgEp **)taosArrayPop(pConsumerEp->vgs);
          SMqRebOutputVg outputVg = {
              .oldConsumerId = pConsumerEp->consumerId,
              .newConsumerId = -1,
              .pVgEp = pVgEp,
          };
          taosHashPut(pHash, &pVgEp->vgId, sizeof(int32_t), &outputVg, sizeof(SMqRebOutputVg));
          mInfo("sub:%s mq rebalance remove vgId:%d from consumer:0x%" PRIx64 ",(first scan)", pSubKey, pVgEp->vgId,
                pConsumerEp->consumerId);
        }
      }
    }
//    putNoTransferToOutput(pOutput, pConsumerEp);
  }
}

static int32_t processRemoveAddVgs(SMnode *pMnode, SMqRebOutputObj *pOutput){
  int32_t totalVgNum = 0;
  SVgObj* pVgroup = NULL;
  void* pIter = NULL;
  SArray* newVgs = taosArrayInit(0, POINTER_BYTES);
  while (1) {
    pIter = sdbFetch(pMnode->pSdb, SDB_VGROUP, pIter, (void**)&pVgroup);
    if (pIter == NULL) {
      break;
    }

    if (!mndVgroupInDb(pVgroup, pOutput->pSub->dbUid)) {
      sdbRelease(pMnode->pSdb, pVgroup);
      continue;
    }

    totalVgNum++;
    SMqVgEp* pVgEp = taosMemoryMalloc(sizeof(SMqVgEp));
    pVgEp->epSet = mndGetVgroupEpset(pMnode, pVgroup);
    pVgEp->vgId = pVgroup->vgId;
    taosArrayPush(newVgs, &pVgEp);
    sdbRelease(pMnode->pSdb, pVgroup);
  }

  pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pOutput->pSub->consumerHash, pIter);
    if (pIter == NULL) break;
    SMqConsumerEp *pConsumerEp = (SMqConsumerEp *)pIter;

    int32_t j = 0;
    while (j < taosArrayGetSize(pConsumerEp->vgs)) {
      SMqVgEp *pVgEp = taosArrayGetP(pConsumerEp->vgs, j);
      bool find = false;
      for(int32_t k = 0; k < taosArrayGetSize(newVgs); k++){
        SMqVgEp *pnewVgEp = taosArrayGetP(newVgs, k);
        if(pVgEp->vgId == pnewVgEp->vgId){
          tDeleteSMqVgEp(pnewVgEp);
          taosArrayRemove(newVgs, k);
          find = true;
          break;
        }
      }
      if(!find){
        mInfo("processRemoveAddVgs old vgId:%d", pVgEp->vgId);
        tDeleteSMqVgEp(pVgEp);
        taosArrayRemove(pConsumerEp->vgs, j);
        continue;
      }
      j++;
    }
  }

  if(taosArrayGetSize(pOutput->pSub->unassignedVgs) == 0 && taosArrayGetSize(newVgs) != 0){
    taosArrayAddAll(pOutput->pSub->unassignedVgs, newVgs);
    mInfo("processRemoveAddVgs add new vg num:%d", (int)taosArrayGetSize(newVgs));
    taosArrayDestroy(newVgs);
  }else{
    taosArrayDestroyP(newVgs, (FDelete)tDeleteSMqVgEp);
  }
  return totalVgNum;
}

static int32_t mndDoRebalance(SMnode *pMnode, const SMqRebInputObj *pInput, SMqRebOutputObj *pOutput) {
  int32_t totalVgNum = processRemoveAddVgs(pMnode, pOutput);
  const char *pSubKey = pOutput->pSub->key;

  int32_t numOfRemoved = taosArrayGetSize(pInput->pRebInfo->removedConsumers);
  int32_t numOfAdded = taosArrayGetSize(pInput->pRebInfo->newConsumers);
  mInfo("sub:%s mq re-balance %d vgroups, existed consumers:%d, added:%d, removed:%d", pSubKey, totalVgNum,
        pInput->oldConsumerNum, numOfAdded, numOfRemoved);

  // 1. build temporary hash(vgId -> SMqRebOutputVg) to store vg that need to be assigned
  SHashObj *pHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);

  // 2. check and get actual removed consumers, put their vg into pHash
  doRemoveLostConsumers(pOutput, pHash, pInput);

  // 3. if previously no consumer, there are vgs not assigned, put these vg into pHash
  addUnassignedVgroups(pOutput, pHash);

  // 4. calc vg number of each consumer
  int32_t numOfFinal = pInput->oldConsumerNum + numOfAdded - numOfRemoved;

  int32_t minVgCnt = 0;
  int32_t imbConsumerNum = 0;

  // calc num
  if (numOfFinal) {
    minVgCnt = totalVgNum / numOfFinal;
    imbConsumerNum = totalVgNum % numOfFinal;
    mInfo("sub:%s mq re-balance %d consumers: at least %d vgs each, %d consumers has 1 more vgroups than avg value",
          pSubKey, numOfFinal, minVgCnt, imbConsumerNum);
  } else {
    mInfo("sub:%s no consumer subscribe this topic", pSubKey);
  }

  // 5. remove vgroups from consumers who have more vgroups than the threshold value(minVgCnt or minVgCnt + 1), and then another vg into pHash
  transferVgroupsForConsumers(pOutput, pHash, minVgCnt, imbConsumerNum);

  // 6. add new consumer into sub
  doAddNewConsumers(pOutput, pInput);

  SMqRebOutputVg *pRebVg = NULL;
  void           *pRemovedIter = NULL;
  void           *pIter = NULL;

  // 7. extract bgroups from pHash and assign to consumers that do not have enough vgroups
  while (1) {
    pIter = taosHashIterate(pOutput->pSub->consumerHash, pIter);
    if (pIter == NULL) {
      break;
    }

    SMqConsumerEp *pConsumerEp = (SMqConsumerEp *)pIter;

    // push until equal minVg
    while (taosArrayGetSize(pConsumerEp->vgs) < minVgCnt) {
      // iter hash and find one vg
      pRemovedIter = taosHashIterate(pHash, pRemovedIter);
      if (pRemovedIter == NULL) {
        mError("sub:%s removed iter is null, never can reach hear", pSubKey);
        break;
      }

      pRebVg = (SMqRebOutputVg *)pRemovedIter;
      pRebVg->newConsumerId = pConsumerEp->consumerId;
      taosArrayPush(pConsumerEp->vgs, &pRebVg->pVgEp);
      mInfo("mq rebalance: add vgId:%d to consumer:0x%" PRIx64 " for average", pRebVg->pVgEp->vgId, pConsumerEp->consumerId);
    }
  }

  while (1) {
    pIter = taosHashIterate(pOutput->pSub->consumerHash, pIter);
    if (pIter == NULL) {
      break;
    }
    SMqConsumerEp *pConsumerEp = (SMqConsumerEp *)pIter;

    if (taosArrayGetSize(pConsumerEp->vgs) == minVgCnt) {
      pRemovedIter = taosHashIterate(pHash, pRemovedIter);
      if (pRemovedIter == NULL) {
        mInfo("sub:%s removed iter is null", pSubKey);
        break;
      }

      pRebVg = (SMqRebOutputVg *)pRemovedIter;
      pRebVg->newConsumerId = pConsumerEp->consumerId;
      taosArrayPush(pConsumerEp->vgs, &pRebVg->pVgEp);
      mInfo("mq rebalance: add vgId:%d to consumer:0x%" PRIx64 " for average + 1", pRebVg->pVgEp->vgId, pConsumerEp->consumerId);
    }
  }

  // All assigned vg should be put into pOutput->rebVgs
  if(pRemovedIter != NULL){
    mError("sub:%s error pRemovedIter should be NULL", pSubKey);
  }
  while (1) {
    pRemovedIter = taosHashIterate(pHash, pRemovedIter);
    if (pRemovedIter == NULL) {
      break;
    }

    SMqRebOutputVg* pRebOutput = (SMqRebOutputVg *)pRemovedIter;
    taosArrayPush(pOutput->rebVgs, pRebOutput);
    if(taosHashGetSize(pOutput->pSub->consumerHash) == 0){    // if all consumer is removed
      taosArrayPush(pOutput->pSub->unassignedVgs, &pRebOutput->pVgEp);  // put all vg into unassigned
    }
  }

  SMqSubscribeObj *pSub = mndAcquireSubscribeByKey(pMnode, pInput->pRebInfo->key);  // put all offset rows
  if (pSub) {
    taosRLockLatch(&pSub->lock);
    if (pOutput->pSub->offsetRows == NULL) {
      pOutput->pSub->offsetRows = taosArrayInit(4, sizeof(OffsetRows));
    }
    pIter = NULL;
    while (1) {
      pIter = taosHashIterate(pSub->consumerHash, pIter);
      if (pIter == NULL) break;
      SMqConsumerEp *pConsumerEp = (SMqConsumerEp *)pIter;
      SMqConsumerEp *pConsumerEpNew = taosHashGet(pOutput->pSub->consumerHash, &pConsumerEp->consumerId, sizeof(int64_t));

      for (int j = 0; j < taosArrayGetSize(pConsumerEp->offsetRows); j++) {
        OffsetRows *d1 = taosArrayGet(pConsumerEp->offsetRows, j);
        bool jump = false;
        for (int i = 0; pConsumerEpNew && i < taosArrayGetSize(pConsumerEpNew->vgs); i++){
          SMqVgEp *pVgEp = taosArrayGetP(pConsumerEpNew->vgs, i);
          if(pVgEp->vgId == d1->vgId){
            jump = true;
            mInfo("pSub->offsetRows jump, because consumer id:0x%"PRIx64 " and vgId:%d not change", pConsumerEp->consumerId, pVgEp->vgId);
            break;
          }
        }
        if(jump) continue;
        bool find = false;
        for (int i = 0; i < taosArrayGetSize(pOutput->pSub->offsetRows); i++) {
          OffsetRows *d2 = taosArrayGet(pOutput->pSub->offsetRows, i);
          if (d1->vgId == d2->vgId) {
            d2->rows += d1->rows;
            d2->offset = d1->offset;
            d2->ever = d1->ever;
            find = true;
            mInfo("pSub->offsetRows add vgId:%d, after:%"PRId64", before:%"PRId64, d2->vgId, d2->rows, d1->rows);
            break;
          }
        }
        if(!find){
          taosArrayPush(pOutput->pSub->offsetRows, d1);
        }
      }
    }
    taosRUnLockLatch(&pSub->lock);
    mndReleaseSubscribe(pMnode, pSub);
  }

  // 8. generate logs
  mInfo("sub:%s mq re-balance calculation completed, re-balanced vg", pSubKey);
  for (int32_t i = 0; i < taosArrayGetSize(pOutput->rebVgs); i++) {
    SMqRebOutputVg *pOutputRebVg = taosArrayGet(pOutput->rebVgs, i);
    mInfo("sub:%s mq re-balance vgId:%d, moved from consumer:0x%" PRIx64 ", to consumer:0x%" PRIx64, pSubKey,
          pOutputRebVg->pVgEp->vgId, pOutputRebVg->oldConsumerId, pOutputRebVg->newConsumerId);
  }

  pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pOutput->pSub->consumerHash, pIter);
    if (pIter == NULL) break;
    SMqConsumerEp *pConsumerEp = (SMqConsumerEp *)pIter;
    int32_t        sz = taosArrayGetSize(pConsumerEp->vgs);
    mInfo("sub:%s mq re-balance final cfg: consumer:0x%" PRIx64 " has %d vg", pSubKey, pConsumerEp->consumerId, sz);
    for (int32_t i = 0; i < sz; i++) {
      SMqVgEp *pVgEp = taosArrayGetP(pConsumerEp->vgs, i);
      mInfo("sub:%s mq re-balance final cfg: vg %d to consumer:0x%" PRIx64, pSubKey, pVgEp->vgId,
            pConsumerEp->consumerId);
    }
  }

  // 9. clear
  taosHashCleanup(pHash);

  return 0;
}

static int32_t mndPersistRebResult(SMnode *pMnode, SRpcMsg *pMsg, const SMqRebOutputObj *pOutput) {
  struct SSubplan* pPlan = NULL;
  if(strcmp(pOutput->pSub->qmsg, "") != 0){
    int32_t code = qStringToSubplan(pOutput->pSub->qmsg, &pPlan);
    if (code != TSDB_CODE_SUCCESS) {
      terrno = code;
      return -1;
    }
  }

  char topic[TSDB_TOPIC_FNAME_LEN] = {0};
  char cgroup[TSDB_CGROUP_LEN] = {0};
  mndSplitSubscribeKey(pOutput->pSub->key, topic, cgroup, true);

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_TOPIC_INSIDE, pMsg, "tmq-reb");
  if (pTrans == NULL) {
    nodesDestroyNode((SNode*)pPlan);
    return -1;
  }

  mndTransSetDbName(pTrans, topic, cgroup);
  if (mndTransCheckConflict(pMnode, pTrans) != 0) {
    mndTransDrop(pTrans);
    nodesDestroyNode((SNode*)pPlan);
    return -1;
  }

  // make txn:
  // 1. redo action: action to all vg
  const SArray *rebVgs = pOutput->rebVgs;
  int32_t       vgNum = taosArrayGetSize(rebVgs);
  for (int32_t i = 0; i < vgNum; i++) {
    SMqRebOutputVg *pRebVg = taosArrayGet(rebVgs, i);
    if (mndPersistSubChangeVgReq(pMnode, pTrans, pOutput->pSub, pRebVg, pPlan) < 0) {
      mndTransDrop(pTrans);
      nodesDestroyNode((SNode*)pPlan);
      return -1;
    }
  }
  nodesDestroyNode((SNode*)pPlan);

  // 2. redo log: subscribe and vg assignment
  // subscribe
  if (mndSetSubCommitLogs(pMnode, pTrans, pOutput->pSub) != 0) {
    mndTransDrop(pTrans);
    return -1;
  }

  // 3. commit log: consumer to update status and epoch
  // 3.1 set touched consumer
  int32_t consumerNum = taosArrayGetSize(pOutput->modifyConsumers);
  for (int32_t i = 0; i < consumerNum; i++) {
    int64_t         consumerId = *(int64_t *)taosArrayGet(pOutput->modifyConsumers, i);
    SMqConsumerObj *pConsumerNew = tNewSMqConsumerObj(consumerId, cgroup);
    pConsumerNew->updateType = CONSUMER_UPDATE_REB;
    if (mndSetConsumerCommitLogs(pMnode, pTrans, pConsumerNew) != 0) {
      tDeleteSMqConsumerObj(pConsumerNew, true);

      mndTransDrop(pTrans);
      return -1;
    }

    tDeleteSMqConsumerObj(pConsumerNew, true);
  }

  // 3.2 set new consumer
  consumerNum = taosArrayGetSize(pOutput->newConsumers);
  for (int32_t i = 0; i < consumerNum; i++) {
    int64_t consumerId = *(int64_t *)taosArrayGet(pOutput->newConsumers, i);
    SMqConsumerObj *pConsumerNew = tNewSMqConsumerObj(consumerId, cgroup);
    pConsumerNew->updateType = CONSUMER_ADD_REB;

    char* topicTmp = taosStrdup(topic);
    taosArrayPush(pConsumerNew->rebNewTopics, &topicTmp);
    if (mndSetConsumerCommitLogs(pMnode, pTrans, pConsumerNew) != 0) {
      tDeleteSMqConsumerObj(pConsumerNew, true);

      mndTransDrop(pTrans);
      return -1;
    }

    tDeleteSMqConsumerObj(pConsumerNew, true);
  }

  // 3.3 set removed consumer
  consumerNum = taosArrayGetSize(pOutput->removedConsumers);
  for (int32_t i = 0; i < consumerNum; i++) {
    int64_t consumerId = *(int64_t *)taosArrayGet(pOutput->removedConsumers, i);

    SMqConsumerObj *pConsumerNew = tNewSMqConsumerObj(consumerId, cgroup);
    pConsumerNew->updateType = CONSUMER_REMOVE_REB;

    char* topicTmp = taosStrdup(topic);
    taosArrayPush(pConsumerNew->rebRemovedTopics, &topicTmp);
    if (mndSetConsumerCommitLogs(pMnode, pTrans, pConsumerNew) != 0) {
      tDeleteSMqConsumerObj(pConsumerNew, true);

      mndTransDrop(pTrans);
      return -1;
    }

    tDeleteSMqConsumerObj(pConsumerNew, true);
  }

  // 4. TODO commit log: modification log

  // 5. set cb
  mndTransSetCb(pTrans, TRANS_START_FUNC_MQ_REB, TRANS_STOP_FUNC_MQ_REB, NULL, 0);

  // 6. execution
  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("failed to prepare trans rebalance since %s", terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  return 0;
}

static void freeRebalanceItem(void *param) {
  SMqRebInfo *pInfo = param;
  taosArrayDestroy(pInfo->newConsumers);
  taosArrayDestroy(pInfo->removedConsumers);
}

static int32_t mndCheckConsumer(SRpcMsg *pMsg, SHashObj* rebSubHash) {
  SMnode         *pMnode = pMsg->info.node;
  SSdb           *pSdb = pMnode->pSdb;
  SMqConsumerObj *pConsumer;
  void           *pIter = NULL;

  // iterate all consumers, find all modification
  while (1) {
    pIter = sdbFetch(pSdb, SDB_CONSUMER, pIter, (void **)&pConsumer);
    if (pIter == NULL) {
      break;
    }

    int32_t hbStatus = atomic_add_fetch_32(&pConsumer->hbStatus, 1);
    int32_t status = atomic_load_32(&pConsumer->status);

    mDebug("check for consumer:0x%" PRIx64 " status:%d(%s), sub-time:%" PRId64 ", createTime:%" PRId64 ", hbstatus:%d",
          pConsumer->consumerId, status, mndConsumerStatusName(status), pConsumer->subscribeTime, pConsumer->createTime,
          hbStatus);

    if (status == MQ_CONSUMER_STATUS_READY) {
      if (taosArrayGetSize(pConsumer->assignedTopics) == 0) {   // unsubscribe or close
        mndDropConsumerFromSdb(pMnode, pConsumer->consumerId, &pMsg->info);
      } else if (hbStatus > MND_CONSUMER_LOST_HB_CNT) {
        taosRLockLatch(&pConsumer->lock);
        int32_t topicNum = taosArrayGetSize(pConsumer->currentTopics);
        for (int32_t i = 0; i < topicNum; i++) {
          char  key[TSDB_SUBSCRIBE_KEY_LEN];
          char *removedTopic = taosArrayGetP(pConsumer->currentTopics, i);
          mndMakeSubscribeKey(key, pConsumer->cgroup, removedTopic);
          SMqRebInfo *pRebSub = mndGetOrCreateRebSub(rebSubHash, key);
          taosArrayPush(pRebSub->removedConsumers, &pConsumer->consumerId);
        }
        taosRUnLockLatch(&pConsumer->lock);
      }else{
        int32_t newTopicNum = taosArrayGetSize(pConsumer->currentTopics);
        for (int32_t i = 0; i < newTopicNum; i++) {
          char *           topic = taosArrayGetP(pConsumer->currentTopics, i);
          SMqSubscribeObj *pSub = mndAcquireSubscribe(pMnode, pConsumer->cgroup, topic);
          if (pSub == NULL) {
            continue;
          }
          taosRLockLatch(&pSub->lock);

          // 2.2 iterate all vg assigned to the consumer of that topic
          SMqConsumerEp *pConsumerEp = taosHashGet(pSub->consumerHash, &pConsumer->consumerId, sizeof(int64_t));
          int32_t        vgNum = taosArrayGetSize(pConsumerEp->vgs);

          for (int32_t j = 0; j < vgNum; j++) {
            SMqVgEp *pVgEp = taosArrayGetP(pConsumerEp->vgs, j);
            SVgObj * pVgroup = mndAcquireVgroup(pMnode, pVgEp->vgId);
            if (!pVgroup) {
              char key[TSDB_SUBSCRIBE_KEY_LEN];
              mndMakeSubscribeKey(key, pConsumer->cgroup, topic);
              mndGetOrCreateRebSub(rebSubHash, key);
              mInfo("vnode splitted, vgId:%d rebalance will be triggered", pVgEp->vgId);
            }
            mndReleaseVgroup(pMnode, pVgroup);
          }
          taosRUnLockLatch(&pSub->lock);
          mndReleaseSubscribe(pMnode, pSub);
        }
      }
    } else if (status == MQ_CONSUMER_STATUS_LOST) {
      if (hbStatus > MND_CONSUMER_LOST_CLEAR_THRESHOLD) {   // clear consumer if lost a day
        mndDropConsumerFromSdb(pMnode, pConsumer->consumerId, &pMsg->info);
      }
    } else {
      taosRLockLatch(&pConsumer->lock);

      int32_t newTopicNum = taosArrayGetSize(pConsumer->rebNewTopics);
      for (int32_t i = 0; i < newTopicNum; i++) {
        char  key[TSDB_SUBSCRIBE_KEY_LEN];
        char *newTopic = taosArrayGetP(pConsumer->rebNewTopics, i);
        mndMakeSubscribeKey(key, pConsumer->cgroup, newTopic);
        SMqRebInfo *pRebSub = mndGetOrCreateRebSub(rebSubHash, key);
        taosArrayPush(pRebSub->newConsumers, &pConsumer->consumerId);
      }

      int32_t removedTopicNum = taosArrayGetSize(pConsumer->rebRemovedTopics);
      for (int32_t i = 0; i < removedTopicNum; i++) {
        char  key[TSDB_SUBSCRIBE_KEY_LEN];
        char *removedTopic = taosArrayGetP(pConsumer->rebRemovedTopics, i);
        mndMakeSubscribeKey(key, pConsumer->cgroup, removedTopic);
        SMqRebInfo *pRebSub = mndGetOrCreateRebSub(rebSubHash, key);
        taosArrayPush(pRebSub->removedConsumers, &pConsumer->consumerId);
      }

      if (newTopicNum == 0 && removedTopicNum == 0 && taosArrayGetSize(pConsumer->assignedTopics) == 0) {   // unsubscribe or close
        mndDropConsumerFromSdb(pMnode, pConsumer->consumerId, &pMsg->info);
      }

      taosRUnLockLatch(&pConsumer->lock);
    }

    mndReleaseConsumer(pMnode, pConsumer);
  }

  return 0;
}

bool mndRebTryStart() {
  int32_t old = atomic_val_compare_exchange_32(&mqRebInExecCnt, 0, 1);
  mInfo("rebalance counter old val:%d", old);
  return old == 0;
}

void mndRebCntInc() {
  int32_t val = atomic_add_fetch_32(&mqRebInExecCnt, 1);
  mInfo("rebalance cnt inc, value:%d", val);
}

void mndRebCntDec() {
  int32_t val = atomic_sub_fetch_32(&mqRebInExecCnt, 1);
  mInfo("rebalance cnt sub, value:%d", val);
}

static int32_t mndProcessRebalanceReq(SRpcMsg *pMsg) {
  int code = 0;
  mInfo("start to process mq timer");

  if (!mndRebTryStart()) {
    mInfo("mq rebalance already in progress, do nothing");
    return code;
  }

  SHashObj *rebSubHash = taosHashInit(64, MurmurHash3_32, true, HASH_NO_LOCK);
  if (rebSubHash == NULL) {
    mError("failed to create rebalance hashmap");
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    code = -1;
    goto END;
  }

  taosHashSetFreeFp(rebSubHash, freeRebalanceItem);

  mndCheckConsumer(pMsg, rebSubHash);
  mInfo("mq re-balance start, total required re-balanced trans:%d", taosHashGetSize(rebSubHash));

  // here we only handle one topic rebalance requirement to ensure the atomic execution of this transaction.
  void              *pIter = NULL;
  SMnode            *pMnode = pMsg->info.node;
  while (1) {
    pIter = taosHashIterate(rebSubHash, pIter);
    if (pIter == NULL) {
      break;
    }

    SMqRebInputObj  rebInput = {0};
    SMqRebOutputObj rebOutput = {0};
    rebOutput.newConsumers = taosArrayInit(0, sizeof(int64_t));
    rebOutput.removedConsumers = taosArrayInit(0, sizeof(int64_t));
    rebOutput.modifyConsumers = taosArrayInit(0, sizeof(int64_t));
    rebOutput.rebVgs = taosArrayInit(0, sizeof(SMqRebOutputVg));

    if (rebOutput.newConsumers == NULL || rebOutput.removedConsumers == NULL || rebOutput.modifyConsumers == NULL ||
        rebOutput.rebVgs == NULL) {
      taosArrayDestroy(rebOutput.newConsumers);
      taosArrayDestroy(rebOutput.removedConsumers);
      taosArrayDestroy(rebOutput.modifyConsumers);
      taosArrayDestroy(rebOutput.rebVgs);

      taosHashCancelIterate(rebSubHash, pIter);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      mError("mq re-balance failed, due to out of memory");
      code = -1;
      goto END;
    }

    SMqRebInfo      *pRebInfo = (SMqRebInfo *)pIter;
    SMqSubscribeObj *pSub = mndAcquireSubscribeByKey(pMnode, pRebInfo->key);

    rebInput.pRebInfo = pRebInfo;

    if (pSub == NULL) {
      // split sub key and extract topic
      char topic[TSDB_TOPIC_FNAME_LEN];
      char cgroup[TSDB_CGROUP_LEN];
      mndSplitSubscribeKey(pRebInfo->key, topic, cgroup, true);

      SMqTopicObj *pTopic = mndAcquireTopic(pMnode, topic);
      if (pTopic == NULL) {
        mError("mq re-balance %s ignored since topic %s doesn't exist", pRebInfo->key, topic);
        continue;
      }

      taosRLockLatch(&pTopic->lock);

      rebOutput.pSub = mndCreateSubscription(pMnode, pTopic, pRebInfo->key);

      if (rebOutput.pSub == NULL) {
        mError("mq rebalance %s failed create sub since %s, ignore", pRebInfo->key, terrstr());
        taosRUnLockLatch(&pTopic->lock);
        mndReleaseTopic(pMnode, pTopic);
        continue;
      }

      memcpy(rebOutput.pSub->dbName, pTopic->db, TSDB_DB_FNAME_LEN);
      taosRUnLockLatch(&pTopic->lock);
      mndReleaseTopic(pMnode, pTopic);

      rebInput.oldConsumerNum = 0;
      mInfo("sub topic:%s has no consumers sub yet", pRebInfo->key);
    } else {
      taosRLockLatch(&pSub->lock);
      rebInput.oldConsumerNum = taosHashGetSize(pSub->consumerHash);
      rebOutput.pSub = tCloneSubscribeObj(pSub);
      taosRUnLockLatch(&pSub->lock);

      mInfo("sub topic:%s has %d consumers sub till now", pRebInfo->key, rebInput.oldConsumerNum);
      mndReleaseSubscribe(pMnode, pSub);
    }

    if (mndDoRebalance(pMnode, &rebInput, &rebOutput) < 0) {
      mError("mq re-balance internal error");
    }

    // if add more consumer to balanced subscribe,
    // possibly no vg is changed
    // when each topic is re-balanced, issue an trans to save the results in sdb.
    if (mndPersistRebResult(pMnode, pMsg, &rebOutput) < 0) {
      mError("mq re-balance persist output error, possibly vnode splitted or dropped,msg:%s", terrstr());
    }

    taosArrayDestroy(rebOutput.newConsumers);
    taosArrayDestroy(rebOutput.modifyConsumers);
    taosArrayDestroy(rebOutput.removedConsumers);
    taosArrayDestroy(rebOutput.rebVgs);
    tDeleteSubscribeObj(rebOutput.pSub);
    taosMemoryFree(rebOutput.pSub);
  }

  // reset flag
  mInfo("mq re-balance completed successfully");

END:
  taosHashCleanup(rebSubHash);
  mndRebCntDec();

  return code;
}

static int32_t sendDeleteSubToVnode(SMqSubscribeObj *pSub, STrans *pTrans){
  // iter all vnode to delete handle
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
    action.msgType = TDMT_VND_TMQ_DELETE_SUB;
    if (mndTransAppendRedoAction(pTrans, &action) != 0) {
      taosMemoryFree(pReq);
      return -1;
    }
  }
  return 0;
}

static int32_t mndProcessDropCgroupReq(SRpcMsg *pMsg) {
  SMnode          *pMnode = pMsg->info.node;
  SMDropCgroupReq  dropReq = {0};
  STrans          *pTrans = NULL;
  int32_t          code = TSDB_CODE_ACTION_IN_PROGRESS;

  if (tDeserializeSMDropCgroupReq(pMsg->pCont, pMsg->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  SMqSubscribeObj *pSub = mndAcquireSubscribe(pMnode, dropReq.cgroup, dropReq.topic);
  if (pSub == NULL) {
    if (dropReq.igNotExists) {
      mInfo("cgroup:%s on topic:%s, not exist, ignore not exist is set", dropReq.cgroup, dropReq.topic);
      return 0;
    } else {
      terrno = TSDB_CODE_MND_SUBSCRIBE_NOT_EXIST;
      mError("topic:%s, cgroup:%s, failed to drop since %s", dropReq.topic, dropReq.cgroup, terrstr());
      return -1;
    }
  }

  taosWLockLatch(&pSub->lock);
  if (taosHashGetSize(pSub->consumerHash) != 0) {
    terrno = TSDB_CODE_MND_CGROUP_USED;
    mError("cgroup:%s on topic:%s, failed to drop since %s", dropReq.cgroup, dropReq.topic, terrstr());
    code = -1;
    goto end;
  }

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_TOPIC_INSIDE, pMsg, "drop-cgroup");
  if (pTrans == NULL) {
    mError("cgroup: %s on topic:%s, failed to drop since %s", dropReq.cgroup, dropReq.topic, terrstr());
    code = -1;
    goto end;
  }

  mndTransSetDbName(pTrans, dropReq.topic, dropReq.cgroup);
  code = mndTransCheckConflict(pMnode, pTrans);
  if (code != 0) {
    goto end;
  }

  void           *pIter = NULL;
  SMqConsumerObj *pConsumer;
  while (1) {
    pIter = sdbFetch(pMnode->pSdb, SDB_CONSUMER, pIter, (void **)&pConsumer);
    if (pIter == NULL) {
      break;
    }

    if (strcmp(dropReq.cgroup, pConsumer->cgroup) == 0) {
      mndDropConsumerFromSdb(pMnode, pConsumer->consumerId, &pMsg->info);
    }
    sdbRelease(pMnode->pSdb, pConsumer);
  }

  mInfo("trans:%d, used to drop cgroup:%s on topic %s", pTrans->id, dropReq.cgroup, dropReq.topic);

  code = sendDeleteSubToVnode(pSub, pTrans);
  if (code != 0) {
    goto end;
  }

  if (mndSetDropSubCommitLogs(pMnode, pTrans, pSub) < 0) {
    mError("cgroup %s on topic:%s, failed to drop since %s", dropReq.cgroup, dropReq.topic, terrstr());
    code = -1;
    goto end;
  }

  if (mndTransPrepare(pMnode, pTrans) < 0) {
    code = -1;
    goto end;
  }

end:
  taosWUnLockLatch(&pSub->lock);
  mndReleaseSubscribe(pMnode, pSub);
  mndTransDrop(pTrans);

  if (code != 0) {
    mError("cgroup %s on topic:%s, failed to drop", dropReq.cgroup, dropReq.topic);
    return code;
  }
  return TSDB_CODE_ACTION_IN_PROGRESS;
}

void mndCleanupSubscribe(SMnode *pMnode) {}

static SSdbRaw *mndSubActionEncode(SMqSubscribeObj *pSub) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  void   *buf = NULL;
  int32_t tlen = tEncodeSubscribeObj(NULL, pSub);
  if (tlen <= 0) goto SUB_ENCODE_OVER;
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
  SSdbRow         *pRow = NULL;
  SMqSubscribeObj *pSub = NULL;
  void            *buf = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto SUB_DECODE_OVER;

  if (sver > MND_SUBSCRIBE_VER_NUMBER || sver < 1) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto SUB_DECODE_OVER;
  }

  pRow = sdbAllocRow(sizeof(SMqSubscribeObj));
  if (pRow == NULL) goto SUB_DECODE_OVER;

  pSub = sdbGetRowObj(pRow);
  if (pSub == NULL) goto SUB_DECODE_OVER;

  int32_t dataPos = 0;
  int32_t tlen;
  SDB_GET_INT32(pRaw, dataPos, &tlen, SUB_DECODE_OVER);
  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) goto SUB_DECODE_OVER;
  SDB_GET_BINARY(pRaw, dataPos, buf, tlen, SUB_DECODE_OVER);
  SDB_GET_RESERVE(pRaw, dataPos, MND_SUBSCRIBE_RESERVE_SIZE, SUB_DECODE_OVER);

  if (tDecodeSubscribeObj(buf, pSub, sver) == NULL) {
    goto SUB_DECODE_OVER;
  }

  // update epset saved in mnode
  if (pSub->unassignedVgs != NULL) {
    int32_t size = (int32_t)taosArrayGetSize(pSub->unassignedVgs);
    for (int32_t i = 0; i < size; ++i) {
      SMqVgEp *pMqVgEp = (SMqVgEp *)taosArrayGetP(pSub->unassignedVgs, i);
      tmsgUpdateDnodeEpSet(&pMqVgEp->epSet);
    }
  }
  if (pSub->consumerHash != NULL) {
    void *pIter = taosHashIterate(pSub->consumerHash, NULL);
    while (pIter) {
      SMqConsumerEp *pConsumerEp = pIter;
      int32_t        size = (int32_t)taosArrayGetSize(pConsumerEp->vgs);
      for (int32_t i = 0; i < size; ++i) {
        SMqVgEp *pMqVgEp = (SMqVgEp *)taosArrayGetP(pConsumerEp->vgs, i);
        tmsgUpdateDnodeEpSet(&pMqVgEp->epSet);
      }
      pIter = taosHashIterate(pSub->consumerHash, pIter);
    }
  }

  terrno = TSDB_CODE_SUCCESS;

SUB_DECODE_OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("subscribe:%s, failed to decode from raw:%p since %s", pSub == NULL ? "null" : pSub->key, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("subscribe:%s, decode from raw:%p, row:%p", pSub->key, pRaw, pSub);
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

  SArray *tmp2 = pOldSub->offsetRows;
  pOldSub->offsetRows = pNewSub->offsetRows;
  pNewSub->offsetRows = tmp2;

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

int32_t mndGetGroupNumByTopic(SMnode *pMnode, const char *topicName) {
  int32_t num = 0;
  SSdb   *pSdb = pMnode->pSdb;

  void            *pIter = NULL;
  SMqSubscribeObj *pSub = NULL;
  while (1) {
    pIter = sdbFetch(pSdb, SDB_SUBSCRIBE, pIter, (void **)&pSub);
    if (pIter == NULL) break;


    char topic[TSDB_TOPIC_FNAME_LEN] = {0};
    char cgroup[TSDB_CGROUP_LEN] = {0};
    mndSplitSubscribeKey(pSub->key, topic, cgroup, true);
    if (strcmp(topic, topicName) != 0) {
      sdbRelease(pSdb, pSub);
      continue;
    }

    num++;
    sdbRelease(pSdb, pSub);
  }

  return num;
}

void mndReleaseSubscribe(SMnode *pMnode, SMqSubscribeObj *pSub) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pSub);
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

//int32_t mndDropSubByDB(SMnode *pMnode, STrans *pTrans, SDbObj *pDb) {
//  int32_t code = 0;
//  SSdb   *pSdb = pMnode->pSdb;
//
//  void            *pIter = NULL;
//  SMqSubscribeObj *pSub = NULL;
//  while (1) {
//    pIter = sdbFetch(pSdb, SDB_SUBSCRIBE, pIter, (void **)&pSub);
//    if (pIter == NULL) break;
//
//    if (pSub->dbUid != pDb->uid) {
//      sdbRelease(pSdb, pSub);
//      continue;
//    }
//
//    if (mndSetDropSubCommitLogs(pMnode, pTrans, pSub) < 0) {
//      sdbRelease(pSdb, pSub);
//      sdbCancelFetch(pSdb, pIter);
//      code = -1;
//      break;
//    }
//
//    sdbRelease(pSdb, pSub);
//  }
//
//  return code;
//}

int32_t mndDropSubByTopic(SMnode *pMnode, STrans *pTrans, const char *topicName) {
  SSdb   *pSdb = pMnode->pSdb;

  void            *pIter = NULL;
  SMqSubscribeObj *pSub = NULL;
  while (1) {
    pIter = sdbFetch(pSdb, SDB_SUBSCRIBE, pIter, (void **)&pSub);
    if (pIter == NULL) break;

    char topic[TSDB_TOPIC_FNAME_LEN] = {0};
    char cgroup[TSDB_CGROUP_LEN] = {0};
    mndSplitSubscribeKey(pSub->key, topic, cgroup, true);
    if (strcmp(topic, topicName) != 0) {
      sdbRelease(pSdb, pSub);
      continue;
    }

    // iter all vnode to delete handle
    if (taosHashGetSize(pSub->consumerHash) != 0) {
      sdbRelease(pSdb, pSub);
      terrno = TSDB_CODE_MND_IN_REBALANCE;
      sdbCancelFetch(pSdb, pIter);
      return -1;
    }
    if (sendDeleteSubToVnode(pSub, pTrans) != 0) {
      sdbRelease(pSdb, pSub);
      sdbCancelFetch(pSdb, pIter);
      return -1;
    }

    if (mndSetDropSubCommitLogs(pMnode, pTrans, pSub) < 0) {
      sdbRelease(pSdb, pSub);
      sdbCancelFetch(pSdb, pIter);
      return -1;
    }

    sdbRelease(pSdb, pSub);
  }

  return 0;
}

static int32_t buildResult(SSDataBlock *pBlock, int32_t* numOfRows, int64_t consumerId, const char* topic, const char* cgroup, SArray* vgs, SArray *offsetRows){
  int32_t sz = taosArrayGetSize(vgs);
  for (int32_t j = 0; j < sz; j++) {
    SMqVgEp *pVgEp = taosArrayGetP(vgs, j);

    SColumnInfoData *pColInfo;
    int32_t          cols = 0;

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, *numOfRows, (const char *)topic, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, *numOfRows, (const char *)cgroup, false);

    // vg id
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, *numOfRows, (const char *)&pVgEp->vgId, false);

    // consumer id
    char        consumerIdHex[32] = {0};
    sprintf(varDataVal(consumerIdHex), "0x%"PRIx64, consumerId);
    varDataSetLen(consumerIdHex, strlen(varDataVal(consumerIdHex)));
    
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, *numOfRows, (const char *)consumerIdHex, consumerId == -1);
    
    mInfo("mnd show subscriptions: topic %s, consumer:0x%" PRIx64 " cgroup %s vgid %d", varDataVal(topic),
           consumerId, varDataVal(cgroup), pVgEp->vgId);

    // offset
    OffsetRows *data = NULL;
    for(int i = 0; i < taosArrayGetSize(offsetRows); i++){
      OffsetRows *tmp = taosArrayGet(offsetRows, i);
      if(tmp->vgId != pVgEp->vgId){
        continue;
      }
      data = tmp;
    }
    if(data){
      // vg id
      char buf[TSDB_OFFSET_LEN*2 + VARSTR_HEADER_SIZE] = {0};
      tFormatOffset(varDataVal(buf), TSDB_OFFSET_LEN, &data->offset);
      sprintf(varDataVal(buf) + strlen(varDataVal(buf)), "/%"PRId64, data->ever);
      varDataSetLen(buf, strlen(varDataVal(buf)));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, *numOfRows, (const char *)buf, false);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, *numOfRows, (const char *)&data->rows, false);
    }else{
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetNULL(pColInfo, *numOfRows);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetNULL(pColInfo, *numOfRows);
      mError("mnd show subscriptions: do not find vgId:%d in offsetRows", pVgEp->vgId);
    }
    (*numOfRows)++;
  }
  return 0;
}

int32_t mndRetrieveSubscribe(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rowsCapacity) {
  SMnode          *pMnode = pReq->info.node;
  SSdb            *pSdb = pMnode->pSdb;
  int32_t          numOfRows = 0;
  SMqSubscribeObj *pSub = NULL;

  mInfo("mnd show subscriptions begin");

  while (numOfRows < rowsCapacity) {
    pShow->pIter = sdbFetch(pSdb, SDB_SUBSCRIBE, pShow->pIter, (void **)&pSub);
    if (pShow->pIter == NULL) {
      break;
    }

    taosRLockLatch(&pSub->lock);

    if (numOfRows + pSub->vgNum > rowsCapacity) {
      blockDataEnsureCapacity(pBlock, numOfRows + pSub->vgNum);
    }

    // topic and cgroup
    char topic[TSDB_TOPIC_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
    char cgroup[TSDB_CGROUP_LEN + VARSTR_HEADER_SIZE] = {0};
    mndSplitSubscribeKey(pSub->key, varDataVal(topic), varDataVal(cgroup), false);
    varDataSetLen(topic, strlen(varDataVal(topic)));
    varDataSetLen(cgroup, strlen(varDataVal(cgroup)));

    SMqConsumerEp *pConsumerEp = NULL;
    void          *pIter = NULL;
    while (1) {
      pIter = taosHashIterate(pSub->consumerHash, pIter);
      if (pIter == NULL) break;
      pConsumerEp = (SMqConsumerEp *)pIter;

      buildResult(pBlock, &numOfRows, pConsumerEp->consumerId, topic, cgroup, pConsumerEp->vgs, pConsumerEp->offsetRows);
    }

    // do not show for cleared subscription
    buildResult(pBlock, &numOfRows, -1, topic, cgroup, pSub->unassignedVgs, pSub->offsetRows);

    pBlock->info.rows = numOfRows;

    taosRUnLockLatch(&pSub->lock);
    sdbRelease(pSdb, pSub);
  }

  mInfo("mnd end show subscriptions");

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

void mndCancelGetNextSubscribe(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}
