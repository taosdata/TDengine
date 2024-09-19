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
#include "mndArbGroup.h"
#include "audit.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"

#define ARBGROUP_VER_NUMBER   1
#define ARBGROUP_RESERVE_SIZE 63

static SHashObj *arbUpdateHash = NULL;

static int32_t mndArbGroupActionInsert(SSdb *pSdb, SArbGroup *pGroup);
static int32_t mndArbGroupActionUpdate(SSdb *pSdb, SArbGroup *pOld, SArbGroup *pNew);
static int32_t mndArbGroupActionDelete(SSdb *pSdb, SArbGroup *pGroup);

static void mndArbGroupDupObj(SArbGroup *pGroup, SArbGroup *pNew);
static void mndArbGroupSetAssignedLeader(SArbGroup *pGroup, int32_t index);
static void mndArbGroupResetAssignedLeader(SArbGroup *pGroup);

static int32_t mndArbGroupUpdateTrans(SMnode *pMnode, SArbGroup *pNew);
static int32_t mndPullupArbUpdateGroup(SMnode *pMnode, SArbGroup *pNewGroup);
static int32_t mndPullupArbUpdateGroupBatch(SMnode *pMnode, SArray *newGroupArray);

static int32_t mndProcessArbHbTimer(SRpcMsg *pReq);
static int32_t mndProcessArbCheckSyncTimer(SRpcMsg *pReq);
static int32_t mndProcessArbUpdateGroupBatchReq(SRpcMsg *pReq);
static int32_t mndProcessArbHbRsp(SRpcMsg *pRsp);
static int32_t mndProcessArbCheckSyncRsp(SRpcMsg *pRsp);
static int32_t mndProcessArbSetAssignedLeaderRsp(SRpcMsg *pRsp);
static int32_t mndRetrieveArbGroups(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextArbGroup(SMnode *pMnode, void *pIter);

static int32_t mndArbCheckToken(const char *token1, const char *token2) {
  if (token1 == NULL || token2 == NULL) return -1;
  if (strlen(token1) == 0 || strlen(token2) == 0) return -1;
  return strncmp(token1, token2, TSDB_ARB_TOKEN_SIZE);
}

int32_t mndInitArbGroup(SMnode *pMnode) {
  int32_t   code = 0;
  SSdbTable table = {
      .sdbType = SDB_ARBGROUP,
      .keyType = SDB_KEY_INT32,
      .encodeFp = (SdbEncodeFp)mndArbGroupActionEncode,
      .decodeFp = (SdbDecodeFp)mndArbGroupActionDecode,
      .insertFp = (SdbInsertFp)mndArbGroupActionInsert,
      .updateFp = (SdbUpdateFp)mndArbGroupActionUpdate,
      .deleteFp = (SdbDeleteFp)mndArbGroupActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_ARB_HEARTBEAT_TIMER, mndProcessArbHbTimer);
  mndSetMsgHandle(pMnode, TDMT_MND_ARB_CHECK_SYNC_TIMER, mndProcessArbCheckSyncTimer);
  mndSetMsgHandle(pMnode, TDMT_MND_ARB_UPDATE_GROUP_BATCH, mndProcessArbUpdateGroupBatchReq);
  mndSetMsgHandle(pMnode, TDMT_VND_ARB_HEARTBEAT_RSP, mndProcessArbHbRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_ARB_CHECK_SYNC_RSP, mndProcessArbCheckSyncRsp);
  mndSetMsgHandle(pMnode, TDMT_SYNC_SET_ASSIGNED_LEADER_RSP, mndProcessArbSetAssignedLeaderRsp);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_ARBGROUP, mndRetrieveArbGroups);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_ARBGROUP, mndCancelGetNextArbGroup);

  arbUpdateHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
  if (arbUpdateHash == NULL) {
    code = terrno;
    TAOS_RETURN(code);
  }

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupArbGroup(SMnode *pMnode) { taosHashCleanup(arbUpdateHash); }

SArbGroup *mndAcquireArbGroup(SMnode *pMnode, int32_t vgId) {
  SArbGroup *pGroup = sdbAcquire(pMnode->pSdb, SDB_ARBGROUP, &vgId);
  if (pGroup == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_MND_ARBGROUP_NOT_EXIST;
  }
  return pGroup;
}

void mndReleaseArbGroup(SMnode *pMnode, SArbGroup *pGroup) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pGroup);
}

int32_t mndArbGroupInitFromVgObj(SVgObj *pVgObj, SArbGroup *outGroup) {
  if (pVgObj->replica != 2) {
    TAOS_RETURN(TSDB_CODE_INVALID_PARA);
  }
  (void)memset(outGroup, 0, sizeof(SArbGroup));
  outGroup->dbUid = pVgObj->dbUid;
  outGroup->vgId = pVgObj->vgId;
  for (int i = 0; i < TSDB_ARB_GROUP_MEMBER_NUM; i++) {
    SArbGroupMember *pMember = &outGroup->members[i];
    pMember->info.dnodeId = pVgObj->vnodeGid[i].dnodeId;
  }

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

SSdbRaw *mndArbGroupActionEncode(SArbGroup *pGroup) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int32_t  size = sizeof(SArbGroup) + ARBGROUP_RESERVE_SIZE;
  SSdbRaw *pRaw = sdbAllocRaw(SDB_ARBGROUP, ARBGROUP_VER_NUMBER, size);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pGroup->vgId, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pGroup->dbUid, _OVER)
  for (int i = 0; i < TSDB_ARB_GROUP_MEMBER_NUM; i++) {
    SArbGroupMember *pMember = &pGroup->members[i];
    SDB_SET_INT32(pRaw, dataPos, pMember->info.dnodeId, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, pMember->state.token, TSDB_ARB_TOKEN_SIZE, _OVER)
  }
  SDB_SET_INT8(pRaw, dataPos, pGroup->isSync, _OVER)

  SArbAssignedLeader *pLeader = &pGroup->assignedLeader;
  SDB_SET_INT32(pRaw, dataPos, pLeader->dnodeId, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pLeader->token, TSDB_ARB_TOKEN_SIZE, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pGroup->version, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pLeader->acked, _OVER)

  SDB_SET_RESERVE(pRaw, dataPos, ARBGROUP_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("arbgroup:%d, failed to encode to raw:%p since %s", pGroup->vgId, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("arbgroup:%d, encode to raw:%p, row:%p", pGroup->vgId, pRaw, pGroup);
  return pRaw;
}

SSdbRow *mndArbGroupActionDecode(SSdbRaw *pRaw) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  SSdbRow   *pRow = NULL;
  SArbGroup *pGroup = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver != ARBGROUP_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  pRow = sdbAllocRow(sizeof(SArbGroup));
  if (pRow == NULL) goto _OVER;

  pGroup = sdbGetRowObj(pRow);
  if (pGroup == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &pGroup->vgId, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pGroup->dbUid, _OVER)
  for (int i = 0; i < TSDB_ARB_GROUP_MEMBER_NUM; i++) {
    SArbGroupMember *pMember = &pGroup->members[i];
    SDB_GET_INT32(pRaw, dataPos, &pMember->info.dnodeId, _OVER)
    SDB_GET_BINARY(pRaw, dataPos, pMember->state.token, TSDB_ARB_TOKEN_SIZE, _OVER)

    pMember->state.nextHbSeq = 0;
    pMember->state.responsedHbSeq = -1;
    pMember->state.lastHbMs = 0;
  }
  SDB_GET_INT8(pRaw, dataPos, &pGroup->isSync, _OVER)

  SArbAssignedLeader *pLeader = &pGroup->assignedLeader;
  SDB_GET_INT32(pRaw, dataPos, &pLeader->dnodeId, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pLeader->token, TSDB_ARB_TOKEN_SIZE, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pGroup->version, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pLeader->acked, _OVER)

  pGroup->mutexInited = false;

  SDB_GET_RESERVE(pRaw, dataPos, ARBGROUP_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("arbgroup:%d, failed to decode from raw:%p since %s", pGroup == NULL ? 0 : pGroup->vgId, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("arbgroup:%d, decode from raw:%p, row:%p", pGroup->vgId, pRaw, pGroup);
  return pRow;
}

static int32_t mndArbGroupActionInsert(SSdb *pSdb, SArbGroup *pGroup) {
  mTrace("arbgroup:%d, perform insert action, row:%p", pGroup->vgId, pGroup);
  if (!pGroup->mutexInited && (taosThreadMutexInit(&pGroup->mutex, NULL) == 0)) {
    pGroup->mutexInited = true;
  }

  return pGroup->mutexInited ? 0 : -1;
}

static int32_t mndArbGroupActionDelete(SSdb *pSdb, SArbGroup *pGroup) {
  mTrace("arbgroup:%d, perform delete action, row:%p", pGroup->vgId, pGroup);
  if (pGroup->mutexInited) {
    (void)taosThreadMutexDestroy(&pGroup->mutex);
    pGroup->mutexInited = false;
  }
  return 0;
}

static int32_t mndArbGroupActionUpdate(SSdb *pSdb, SArbGroup *pOld, SArbGroup *pNew) {
  mTrace("arbgroup:%d, perform update action, old row:%p new row:%p", pOld->vgId, pOld, pNew);
  (void)taosThreadMutexLock(&pOld->mutex);

  if (pOld->version != pNew->version) {
    mInfo("arbgroup:%d, skip to perform update action, old row:%p new row:%p, old version:%" PRId64
          " new version:%" PRId64,
          pOld->vgId, pOld, pNew, pOld->version, pNew->version);
    goto _OVER;
  }

  for (int i = 0; i < TSDB_ARB_GROUP_MEMBER_NUM; i++) {
    (void)memcpy(pOld->members[i].state.token, pNew->members[i].state.token, TSDB_ARB_TOKEN_SIZE);
  }
  pOld->isSync = pNew->isSync;
  pOld->assignedLeader.dnodeId = pNew->assignedLeader.dnodeId;
  (void)memcpy(pOld->assignedLeader.token, pNew->assignedLeader.token, TSDB_ARB_TOKEN_SIZE);
  pOld->assignedLeader.acked = pNew->assignedLeader.acked;
  pOld->version++;

_OVER:
  (void)taosThreadMutexUnlock(&pOld->mutex);

  if (taosHashRemove(arbUpdateHash, &pOld->vgId, sizeof(int32_t)) != 0) {
    mError("arbgroup:%d, failed to remove from arbUpdateHash", pOld->vgId);
  }
  return 0;
}

int32_t mndSetCreateArbGroupRedoLogs(STrans *pTrans, SArbGroup *pGroup) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndArbGroupActionEncode(pGroup);
  if (pRedoRaw == NULL) {
    code = terrno;
    TAOS_RETURN(code);
  }
  if ((code = mndTransAppendRedolog(pTrans, pRedoRaw)) != 0) TAOS_RETURN(code);
  if ((code = sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING)) != 0) TAOS_RETURN(code);
  return 0;
}

int32_t mndSetCreateArbGroupUndoLogs(STrans *pTrans, SArbGroup *pGroup) {
  int32_t  code = 0;
  SSdbRaw *pUndoRaw = mndArbGroupActionEncode(pGroup);
  if (pUndoRaw == NULL) {
    code = terrno;
    TAOS_RETURN(code);
  }
  if ((code = mndTransAppendUndolog(pTrans, pUndoRaw)) != 0) TAOS_RETURN(code);
  if ((code = sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED)) != 0) TAOS_RETURN(code);
  return 0;
}

int32_t mndSetCreateArbGroupCommitLogs(STrans *pTrans, SArbGroup *pGroup) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndArbGroupActionEncode(pGroup);
  if (pCommitRaw == NULL) {
    code = terrno;
    TAOS_RETURN(code);
  }
  if ((code = mndTransAppendCommitlog(pTrans, pCommitRaw) != 0)) TAOS_RETURN(code);
  if ((code = sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY)) != 0) TAOS_RETURN(code);
  return 0;
}

int32_t mndSetDropArbGroupPrepareLogs(STrans *pTrans, SArbGroup *pGroup) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndArbGroupActionEncode(pGroup);
  if (pRedoRaw == NULL) {
    code = terrno;
    TAOS_RETURN(code);
  }
  if ((code = mndTransAppendPrepareLog(pTrans, pRedoRaw)) != 0) TAOS_RETURN(code);
  if ((code = sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING)) != 0) TAOS_RETURN(code);
  return 0;
}

static int32_t mndSetDropArbGroupRedoLogs(STrans *pTrans, SArbGroup *pGroup) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndArbGroupActionEncode(pGroup);
  if (pRedoRaw == NULL) {
    code = terrno;
    TAOS_RETURN(code);
  }
  if ((code = mndTransAppendRedolog(pTrans, pRedoRaw)) != 0) TAOS_RETURN(code);
  if ((code = sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING)) != 0) TAOS_RETURN(code);
  return 0;
}

int32_t mndSetDropArbGroupCommitLogs(STrans *pTrans, SArbGroup *pGroup) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndArbGroupActionEncode(pGroup);
  if (pCommitRaw == NULL) {
    code = terrno;
    TAOS_RETURN(code);
  }
  if ((code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) TAOS_RETURN(code);
  if ((code = sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED)) != 0) TAOS_RETURN(code);
  return 0;
}

static void *mndBuildArbHeartBeatReq(int32_t *pContLen, char *arbToken, int32_t dnodeId, int64_t arbTerm,
                                     SArray *hbMembers) {
  SVArbHeartBeatReq req = {0};
  req.dnodeId = dnodeId;
  req.arbToken = arbToken;
  req.arbTerm = arbTerm;
  req.hbMembers = hbMembers;

  int32_t contLen = tSerializeSVArbHeartBeatReq(NULL, 0, &req);
  if (contLen <= 0) return NULL;

  void *pReq = rpcMallocCont(contLen);
  if (pReq == NULL) return NULL;

  if (tSerializeSVArbHeartBeatReq(pReq, contLen, &req) <= 0) {
    rpcFreeCont(pReq);
    return NULL;
  }
  *pContLen = contLen;
  return pReq;
}

static int32_t mndSendArbHeartBeatReq(SDnodeObj *pDnode, char *arbToken, int64_t arbTerm, SArray *hbMembers) {
  int32_t contLen = 0;
  void   *pHead = mndBuildArbHeartBeatReq(&contLen, arbToken, pDnode->id, arbTerm, hbMembers);
  if (pHead == NULL) {
    mError("dnodeId:%d, failed to build arb-hb request", pDnode->id);
    return -1;
  }
  SRpcMsg rpcMsg = {.msgType = TDMT_VND_ARB_HEARTBEAT, .pCont = pHead, .contLen = contLen};

  SEpSet epSet = mndGetDnodeEpset(pDnode);
  if (epSet.numOfEps == 0) {
    mError("dnodeId:%d, failed to send arb-hb request to dnode since no epSet found", pDnode->id);
    rpcFreeCont(pHead);
    return -1;
  }

  int32_t code = tmsgSendReq(&epSet, &rpcMsg);
  if (code != 0) {
    mError("dnodeId:%d, failed to send arb-hb request to dnode since 0x%x", pDnode->id, code);
  } else {
    mTrace("dnodeId:%d, send arb-hb request to dnode", pDnode->id);
  }
  return code;
}

static int32_t mndProcessArbHbTimer(SRpcMsg *pReq) {
  int32_t    code = 0;
  SMnode    *pMnode = pReq->info.node;
  SSdb      *pSdb = pMnode->pSdb;
  SArbGroup *pArbGroup = NULL;
  void      *pIter = NULL;

  SHashObj *pDnodeHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);

  // collect member of same dnode
  while (1) {
    pIter = sdbFetch(pSdb, SDB_ARBGROUP, pIter, (void **)&pArbGroup);
    if (pIter == NULL) break;

    (void)taosThreadMutexLock(&pArbGroup->mutex);

    for (int i = 0; i < TSDB_ARB_GROUP_MEMBER_NUM; i++) {
      SArbGroupMember *pMember = &pArbGroup->members[i];
      int32_t          dnodeId = pMember->info.dnodeId;
      void            *pObj = taosHashGet(pDnodeHash, &dnodeId, sizeof(int32_t));
      SArray          *hbMembers = NULL;
      if (pObj) {
        hbMembers = *(SArray **)pObj;
      } else {
        hbMembers = taosArrayInit(16, sizeof(SVArbHbReqMember));
        if (taosHashPut(pDnodeHash, &dnodeId, sizeof(int32_t), &hbMembers, POINTER_BYTES) != 0) {
          mError("dnodeId:%d, failed to push hb member inty]o hash, but conitnue next at this timer round", dnodeId);
        }
      }
      SVArbHbReqMember reqMember = {.vgId = pArbGroup->vgId, .hbSeq = pMember->state.nextHbSeq++};
      if (taosArrayPush(hbMembers, &reqMember) == NULL) {
        mError("dnodeId:%d, failed to push hb member, but conitnue next at this timer round", dnodeId);
      }
    }

    (void)taosThreadMutexUnlock(&pArbGroup->mutex);
    sdbRelease(pSdb, pArbGroup);
  }

  char arbToken[TSDB_ARB_TOKEN_SIZE];
  if ((code = mndGetArbToken(pMnode, arbToken)) != 0) {
    mError("failed to get arb token for arb-hb timer");
    pIter = taosHashIterate(pDnodeHash, NULL);
    while (pIter) {
      SArray *hbMembers = *(SArray **)pIter;
      taosArrayDestroy(hbMembers);
      pIter = taosHashIterate(pDnodeHash, pIter);
    }
    taosHashCleanup(pDnodeHash);
    TAOS_RETURN(code);
  }

  int64_t nowMs = taosGetTimestampMs();

  pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pDnodeHash, pIter);
    if (pIter == NULL) break;

    int32_t dnodeId = *(int32_t *)taosHashGetKey(pIter, NULL);
    SArray *hbMembers = *(SArray **)pIter;

    SDnodeObj *pDnode = mndAcquireDnode(pMnode, dnodeId);
    if (pDnode == NULL) {
      mError("dnodeId:%d, timer failed to send arb-hb request, failed find dnode", dnodeId);
      taosArrayDestroy(hbMembers);
      continue;
    }

    int64_t mndTerm = mndGetTerm(pMnode);

    if (mndIsDnodeOnline(pDnode, nowMs)) {
      TAOS_CHECK_RETURN(mndSendArbHeartBeatReq(pDnode, arbToken, mndTerm, hbMembers));
    }

    mndReleaseDnode(pMnode, pDnode);
    taosArrayDestroy(hbMembers);
  }
  taosHashCleanup(pDnodeHash);

  return 0;
}

static void *mndBuildArbCheckSyncReq(int32_t *pContLen, int32_t vgId, char *arbToken, int64_t arbTerm,
                                     char *member0Token, char *member1Token) {
  SVArbCheckSyncReq req = {0};
  req.arbToken = arbToken;
  req.arbTerm = arbTerm;
  req.member0Token = member0Token;
  req.member1Token = member1Token;

  int32_t reqLen = tSerializeSVArbCheckSyncReq(NULL, 0, &req);
  int32_t contLen = reqLen + sizeof(SMsgHead);

  if (contLen <= 0) return NULL;
  SMsgHead *pHead = rpcMallocCont(contLen);
  if (pHead == NULL) return NULL;

  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(vgId);
  if (tSerializeSVArbCheckSyncReq((char *)pHead + sizeof(SMsgHead), contLen, &req) <= 0) {
    rpcFreeCont(pHead);
    return NULL;
  }
  *pContLen = contLen;
  return pHead;
}

static int32_t mndSendArbCheckSyncReq(SMnode *pMnode, int32_t vgId, char *arbToken, int64_t term, char *member0Token,
                                      char *member1Token) {
  int32_t code = 0;
  int32_t contLen = 0;
  void   *pHead = mndBuildArbCheckSyncReq(&contLen, vgId, arbToken, term, member0Token, member1Token);
  if (!pHead) {
    mError("vgId:%d, failed to build check-sync request", vgId);
    return -1;
  }
  SRpcMsg rpcMsg = {.msgType = TDMT_VND_ARB_CHECK_SYNC, .pCont = pHead, .contLen = contLen};

  SEpSet epSet = mndGetVgroupEpsetById(pMnode, vgId);
  if (epSet.numOfEps == 0) {
    mError("vgId:%d, failed to send check-sync request since no epSet found", vgId);
    rpcFreeCont(pHead);
    code = -1;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  code = tmsgSendReq(&epSet, &rpcMsg);
  if (code != 0) {
    mError("vgId:%d, failed to send check-sync request since 0x%x", vgId, code);
  } else {
    mDebug("vgId:%d, send check-sync request", vgId);
  }
  return code;
}

static bool mndCheckArbMemberHbTimeout(SArbGroup *pArbGroup, int32_t index, int64_t nowMs) {
  SArbGroupMember *pArbMember = &pArbGroup->members[index];
  return pArbMember->state.lastHbMs < (nowMs - tsArbSetAssignedTimeoutSec * 1000);
}

static void *mndBuildArbSetAssignedLeaderReq(int32_t *pContLen, int32_t vgId, char *arbToken, int64_t arbTerm,
                                             char *memberToken) {
  SVArbSetAssignedLeaderReq req = {0};
  req.arbToken = arbToken;
  req.arbTerm = arbTerm;
  req.memberToken = memberToken;

  int32_t reqLen = tSerializeSVArbSetAssignedLeaderReq(NULL, 0, &req);
  int32_t contLen = reqLen + sizeof(SMsgHead);

  if (contLen <= 0) return NULL;
  SMsgHead *pHead = rpcMallocCont(contLen);
  if (pHead == NULL) return NULL;

  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(vgId);
  if (tSerializeSVArbSetAssignedLeaderReq((char *)pHead + sizeof(SMsgHead), contLen, &req) <= 0) {
    rpcFreeCont(pHead);
    return NULL;
  }
  *pContLen = contLen;
  return pHead;
}

static int32_t mndSendArbSetAssignedLeaderReq(SMnode *pMnode, int32_t dnodeId, int32_t vgId, char *arbToken,
                                              int64_t term, char *memberToken) {
  int32_t code = 0;
  int32_t contLen = 0;
  void   *pHead = mndBuildArbSetAssignedLeaderReq(&contLen, vgId, arbToken, term, memberToken);
  if (!pHead) {
    mError("vgId:%d, failed to build set-assigned request", vgId);
    code = -1;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  SRpcMsg rpcMsg = {.msgType = TDMT_SYNC_SET_ASSIGNED_LEADER, .pCont = pHead, .contLen = contLen};

  SEpSet epSet = mndGetDnodeEpsetById(pMnode, dnodeId);
  if (epSet.numOfEps == 0) {
    mError("dnodeId:%d vgId:%d, failed to send arb-set-assigned request to dnode since no epSet found", dnodeId, vgId);
    rpcFreeCont(pHead);
    code = -1;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  code = tmsgSendReq(&epSet, &rpcMsg);
  if (code != 0) {
    mError("dnodeId:%d vgId:%d, failed to send arb-set-assigned request to dnode since 0x%x", dnodeId, vgId, code);
  } else {
    mInfo("dnodeId:%d vgId:%d, send arb-set-assigned request to dnode", dnodeId, vgId);
  }
  return code;
}

static int32_t mndProcessArbCheckSyncTimer(SRpcMsg *pReq) {
  int32_t    code = 0;
  SMnode    *pMnode = pReq->info.node;
  SSdb      *pSdb = pMnode->pSdb;
  SArbGroup *pArbGroup = NULL;
  SArbGroup  arbGroupDup = {0};
  void      *pIter = NULL;

  char arbToken[TSDB_ARB_TOKEN_SIZE];
  if ((code = mndGetArbToken(pMnode, arbToken)) != 0) {
    mError("failed to get arb token for arb-check-sync timer");
    TAOS_RETURN(code);
  }
  int64_t term = mndGetTerm(pMnode);
  if (term < 0) {
    mError("arb failed to get term since %s", terrstr());
    code = -1;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  int64_t roleTimeMs = mndGetRoleTimeMs(pMnode);
  int64_t nowMs = taosGetTimestampMs();
  if (nowMs - roleTimeMs < tsArbHeartBeatIntervalSec * 1000 * 2) {
    mInfo("arb skip to check sync since mnd had just switch over, roleTime:%" PRId64 " now:%" PRId64, roleTimeMs,
          nowMs);
    return 0;
  }

  SArray *pUpdateArray = taosArrayInit(16, sizeof(SArbGroup));

  while (1) {
    pIter = sdbFetch(pSdb, SDB_ARBGROUP, pIter, (void **)&pArbGroup);
    if (pIter == NULL) break;

    (void)taosThreadMutexLock(&pArbGroup->mutex);
    mndArbGroupDupObj(pArbGroup, &arbGroupDup);
    (void)taosThreadMutexUnlock(&pArbGroup->mutex);

    int32_t vgId = arbGroupDup.vgId;

    bool                member0IsTimeout = mndCheckArbMemberHbTimeout(&arbGroupDup, 0, nowMs);
    bool                member1IsTimeout = mndCheckArbMemberHbTimeout(&arbGroupDup, 1, nowMs);
    SArbAssignedLeader *pAssignedLeader = &arbGroupDup.assignedLeader;
    int32_t             currentAssignedDnodeId = pAssignedLeader->dnodeId;

    // 1. has assigned && is sync && no response => send req
    if (currentAssignedDnodeId != 0 && arbGroupDup.isSync == true && pAssignedLeader->acked == false) {
      (void)mndSendArbSetAssignedLeaderReq(pMnode, currentAssignedDnodeId, vgId, arbToken, term,
                                           pAssignedLeader->token);
      mInfo("vgId:%d, arb send set assigned leader to dnodeId:%d", vgId, currentAssignedDnodeId);
      sdbRelease(pSdb, pArbGroup);
      continue;
    }

    // 2. both of the two members are timeout => skip
    if (member0IsTimeout && member1IsTimeout) {
      sdbRelease(pSdb, pArbGroup);
      continue;
    }

    // 3. no member is timeout => check sync
    if (member0IsTimeout == false && member1IsTimeout == false) {
      // no assigned leader and not sync
      if (currentAssignedDnodeId == 0 && !arbGroupDup.isSync) {
        (void)mndSendArbCheckSyncReq(pMnode, arbGroupDup.vgId, arbToken, term, arbGroupDup.members[0].state.token,
                                     arbGroupDup.members[1].state.token);
      }
      sdbRelease(pSdb, pArbGroup);
      continue;
    }

    // 4. one of the members is timeout => set assigned leader
    int32_t          candidateIndex = member0IsTimeout ? 1 : 0;
    SArbGroupMember *pMember = &arbGroupDup.members[candidateIndex];

    // has assigned leader and dnodeId not match => skip
    if (currentAssignedDnodeId != 0 && currentAssignedDnodeId != pMember->info.dnodeId) {
      mInfo("arb skip to set assigned leader to vgId:%d dnodeId:%d, assigned leader has been set to dnodeId:%d", vgId,
            pMember->info.dnodeId, currentAssignedDnodeId);
      sdbRelease(pSdb, pArbGroup);
      continue;
    }

    // not sync => skip
    if (arbGroupDup.isSync == false) {
      if (currentAssignedDnodeId == pMember->info.dnodeId) {
        mDebug("arb skip to set assigned leader to vgId:%d dnodeId:%d, arb group is not sync", vgId,
               pMember->info.dnodeId);
      } else {
        mInfo("arb skip to set assigned leader to vgId:%d dnodeId:%d, arb group is not sync", vgId,
              pMember->info.dnodeId);
      }
      sdbRelease(pSdb, pArbGroup);
      continue;
    }

    // is sync && no assigned leader => write to sdb
    SArbGroup newGroup = {0};
    mndArbGroupDupObj(&arbGroupDup, &newGroup);
    mndArbGroupSetAssignedLeader(&newGroup, candidateIndex);
    if (taosArrayPush(pUpdateArray, &newGroup) == NULL) {
      taosArrayDestroy(pUpdateArray);
      return terrno;
    }

    sdbRelease(pSdb, pArbGroup);
  }

  TAOS_CHECK_RETURN(mndPullupArbUpdateGroupBatch(pMnode, pUpdateArray));

  taosArrayDestroy(pUpdateArray);
  return 0;
}

static void *mndBuildArbUpdateGroupBatchReq(int32_t *pContLen, SArray *updateArray) {
  SMArbUpdateGroupBatchReq req = {0};
  req.updateArray = updateArray;

  int32_t contLen = tSerializeSMArbUpdateGroupBatchReq(NULL, 0, &req);
  if (contLen <= 0) return NULL;
  SMsgHead *pHead = rpcMallocCont(contLen);
  if (pHead == NULL) return NULL;

  if (tSerializeSMArbUpdateGroupBatchReq(pHead, contLen, &req) <= 0) {
    rpcFreeCont(pHead);
    return NULL;
  }
  *pContLen = contLen;
  return pHead;
}

static void mndInitArbUpdateGroup(SArbGroup *pGroup, SMArbUpdateGroup *outGroup) {
  outGroup->vgId = pGroup->vgId;
  outGroup->dbUid = pGroup->dbUid;
  for (int i = 0; i < TSDB_ARB_GROUP_MEMBER_NUM; i++) {
    outGroup->members[i].dnodeId = pGroup->members[i].info.dnodeId;
    outGroup->members[i].token = pGroup->members[i].state.token;  // just copy the pointer
  }
  outGroup->isSync = pGroup->isSync;
  outGroup->assignedLeader.dnodeId = pGroup->assignedLeader.dnodeId;
  outGroup->assignedLeader.token = pGroup->assignedLeader.token;  // just copy the pointer
  outGroup->assignedLeader.acked = pGroup->assignedLeader.acked;
  outGroup->version = pGroup->version;
}

static int32_t mndPullupArbUpdateGroup(SMnode *pMnode, SArbGroup *pNewGroup) {
  if (taosHashGet(arbUpdateHash, &pNewGroup->vgId, sizeof(pNewGroup->vgId)) != NULL) {
    mInfo("vgId:%d, arb skip to pullup arb-update-group request, since it is in process", pNewGroup->vgId);
    return 0;
  }

  int32_t ret = -1;

  SMArbUpdateGroup newGroup = {0};
  mndInitArbUpdateGroup(pNewGroup, &newGroup);

  SArray *pArray = taosArrayInit(1, sizeof(SMArbUpdateGroup));
  if (taosArrayPush(pArray, &newGroup) == NULL) goto _OVER;

  int32_t contLen = 0;
  void   *pHead = mndBuildArbUpdateGroupBatchReq(&contLen, pArray);
  if (!pHead) {
    mError("failed to build arb-update-group request");
    goto _OVER;
  }

  SRpcMsg rpcMsg = {
      .msgType = TDMT_MND_ARB_UPDATE_GROUP_BATCH, .pCont = pHead, .contLen = contLen, .info.noResp = true};
  ret = tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg);
  if (ret != 0) goto _OVER;

  if ((ret = taosHashPut(arbUpdateHash, &pNewGroup->vgId, sizeof(pNewGroup->vgId), NULL, 0)) != 0) goto _OVER;

_OVER:
  taosArrayDestroy(pArray);
  return ret;
}

static int32_t mndPullupArbUpdateGroupBatch(SMnode *pMnode, SArray *newGroupArray) {
  int32_t ret = -1;

  size_t  sz = taosArrayGetSize(newGroupArray);
  SArray *pArray = taosArrayInit(sz, sizeof(SMArbUpdateGroup));
  for (size_t i = 0; i < sz; i++) {
    SArbGroup *pNewGroup = taosArrayGet(newGroupArray, i);
    if (taosHashGet(arbUpdateHash, &pNewGroup->vgId, sizeof(pNewGroup->vgId)) != NULL) {
      mInfo("vgId:%d, arb skip to pullup arb-update-group request, since it is in process", pNewGroup->vgId);
      continue;
    }

    SMArbUpdateGroup newGroup = {0};
    mndInitArbUpdateGroup(pNewGroup, &newGroup);

    if (taosArrayPush(pArray, &newGroup) == NULL) goto _OVER;
    if (taosHashPut(arbUpdateHash, &pNewGroup->vgId, sizeof(pNewGroup->vgId), NULL, 0) != 0) goto _OVER;
  }

  if (taosArrayGetSize(pArray) == 0) {
    ret = 0;
    goto _OVER;
  }

  int32_t contLen = 0;
  void   *pHead = mndBuildArbUpdateGroupBatchReq(&contLen, pArray);
  if (!pHead) {
    mError("failed to build arb-update-group request");
    goto _OVER;
  }

  SRpcMsg rpcMsg = {
      .msgType = TDMT_MND_ARB_UPDATE_GROUP_BATCH, .pCont = pHead, .contLen = contLen, .info.noResp = true};
  ret = tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg);

_OVER:
  taosArrayDestroy(pArray);

  if (ret != 0) {
    for (size_t i = 0; i < sz; i++) {
      SArbGroup *pNewGroup = taosArrayGet(newGroupArray, i);
      if (taosHashRemove(arbUpdateHash, &pNewGroup->vgId, sizeof(pNewGroup->vgId)) != 0) {
        mError("failed to remove vgId:%d from arbUpdateHash", pNewGroup->vgId);
      }
    }
  }

  return ret;
}

static int32_t mndProcessArbUpdateGroupBatchReq(SRpcMsg *pReq) {
  int    code = -1;
  size_t sz = 0;

  SMArbUpdateGroupBatchReq req = {0};
  if ((code = tDeserializeSMArbUpdateGroupBatchReq(pReq->pCont, pReq->contLen, &req)) != 0) {
    mError("arb failed to decode arb-update-group request");
    TAOS_RETURN(code);
  }

  SMnode *pMnode = pReq->info.node;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_ARBGROUP, NULL, "update-arbgroup");
  if (pTrans == NULL) {
    mError("failed to update arbgroup in create trans, since %s", terrstr());
    goto _OVER;
  }

  sz = taosArrayGetSize(req.updateArray);
  for (size_t i = 0; i < sz; i++) {
    SMArbUpdateGroup *pUpdateGroup = taosArrayGet(req.updateArray, i);
    SArbGroup         newGroup = {0};
    newGroup.vgId = pUpdateGroup->vgId;
    newGroup.dbUid = pUpdateGroup->dbUid;
    for (int i = 0; i < TSDB_ARB_GROUP_MEMBER_NUM; i++) {
      newGroup.members[i].info.dnodeId = pUpdateGroup->members[i].dnodeId;
      (void)memcpy(newGroup.members[i].state.token, pUpdateGroup->members[i].token, TSDB_ARB_TOKEN_SIZE);
    }

    newGroup.isSync = pUpdateGroup->isSync;
    newGroup.assignedLeader.dnodeId = pUpdateGroup->assignedLeader.dnodeId;
    (void)memcpy(newGroup.assignedLeader.token, pUpdateGroup->assignedLeader.token, TSDB_ARB_TOKEN_SIZE);
    newGroup.assignedLeader.acked = pUpdateGroup->assignedLeader.acked;
    newGroup.version = pUpdateGroup->version;

    SArbGroup *pOldGroup = sdbAcquire(pMnode->pSdb, SDB_ARBGROUP, &newGroup.vgId);
    if (!pOldGroup) {
      mInfo("vgId:%d, arb skip to update arbgroup, since no obj found", newGroup.vgId);
      if (taosHashRemove(arbUpdateHash, &newGroup.vgId, sizeof(int32_t)) != 0) {
        mError("failed to remove vgId:%d from arbUpdateHash", newGroup.vgId);
      }
      continue;
    }

    mndTransAddArbGroupId(pTrans, newGroup.vgId);

    if ((code = mndSetCreateArbGroupCommitLogs(pTrans, &newGroup)) != 0) {
      mError("failed to update arbgroup in set commit log, vgId:%d, trans:%d, since %s", newGroup.vgId, pTrans->id,
             terrstr());
      goto _OVER;
    }

    mInfo("trans:%d, used to update arbgroup:%d, member0:[%d][%s] member1:[%d][%s] isSync:%d assigned:[%d][%s][%d]",
          pTrans->id, newGroup.vgId, newGroup.members[0].info.dnodeId, newGroup.members[0].state.token,
          newGroup.members[1].info.dnodeId, newGroup.members[1].state.token, newGroup.isSync,
          newGroup.assignedLeader.dnodeId, newGroup.assignedLeader.token, newGroup.assignedLeader.acked);

    sdbRelease(pMnode->pSdb, pOldGroup);
  }

  if ((code = mndTransCheckConflict(pMnode, pTrans)) != 0) goto _OVER;
  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) goto _OVER;

  code = 0;

_OVER:
  if (code != 0) {
    // failed to update arbgroup
    for (size_t i = 0; i < sz; i++) {
      SMArbUpdateGroup *pUpdateGroup = taosArrayGet(req.updateArray, i);
      if (taosHashRemove(arbUpdateHash, &pUpdateGroup->vgId, sizeof(int32_t)) != 0) {
        mError("failed to remove vgId:%d from arbUpdateHash", pUpdateGroup->vgId);
      }
    }
  }

  mndTransDrop(pTrans);
  tFreeSMArbUpdateGroupBatchReq(&req);
  return code;
}

static void mndArbGroupDupObj(SArbGroup *pGroup, SArbGroup *pNew) {
  (void)memcpy(pNew, pGroup, offsetof(SArbGroup, mutexInited));
}

static void mndArbGroupSetAssignedLeader(SArbGroup *pGroup, int32_t index) {
  SArbGroupMember *pMember = &pGroup->members[index];

  pGroup->assignedLeader.dnodeId = pMember->info.dnodeId;
  (void)strncpy(pGroup->assignedLeader.token, pMember->state.token, TSDB_ARB_TOKEN_SIZE);
  pGroup->assignedLeader.acked = false;
}

static void mndArbGroupResetAssignedLeader(SArbGroup *pGroup) {
  pGroup->assignedLeader.dnodeId = 0;
  (void)memset(pGroup->assignedLeader.token, 0, TSDB_ARB_TOKEN_SIZE);
  pGroup->assignedLeader.acked = false;
}

static int32_t mndArbGroupUpdateTrans(SMnode *pMnode, SArbGroup *pNew) {
  int32_t code = -1;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_ARBGROUP, NULL, "update-arbgroup");
  if (pTrans == NULL) {
    mError("failed to update arbgroup in create trans, vgId:%d, since %s", pNew->vgId, terrstr());
    if (terrno != 0) code = terrno;
    goto _OVER;
  }

  mInfo("trans:%d, used to update arbgroup:%d, member0:[%d][%s] member1:[%d][%s] isSync:%d assigned:[%d][%s][%d]",
        pTrans->id, pNew->vgId, pNew->members[0].info.dnodeId, pNew->members[0].state.token,
        pNew->members[1].info.dnodeId, pNew->members[1].state.token, pNew->isSync, pNew->assignedLeader.dnodeId,
        pNew->assignedLeader.token, pNew->assignedLeader.acked);

  mndTransAddArbGroupId(pTrans, pNew->vgId);
  if ((code = mndTransCheckConflict(pMnode, pTrans)) != 0) {
    goto _OVER;
  }

  if ((code = mndSetCreateArbGroupCommitLogs(pTrans, pNew)) != 0) {
    mError("failed to update arbgroup in set commit log, vgId:%d, since %s", pNew->vgId, tstrerror(code));
    goto _OVER;
  }

  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) goto _OVER;

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  return code;
}

bool mndUpdateArbGroupByHeartBeat(SArbGroup *pGroup, SVArbHbRspMember *pRspMember, int64_t nowMs, int32_t dnodeId,
                                  SArbGroup *pNewGroup) {
  bool             updateToken = false;
  SArbGroupMember *pMember = NULL;

  (void)taosThreadMutexLock(&pGroup->mutex);

  int index = 0;
  for (; index < TSDB_ARB_GROUP_MEMBER_NUM; index++) {
    pMember = &pGroup->members[index];
    if (pMember->info.dnodeId == dnodeId) {
      break;
    }
    pMember = NULL;
  }

  if (pMember == NULL) {
    mInfo("dnodeId:%d vgId:%d, arb token update check failed, no obj found", dnodeId, pRspMember->vgId);
    goto _OVER;
  }

  if (pMember->state.responsedHbSeq >= pRspMember->hbSeq) {
    // skip
    mInfo("dnodeId:%d vgId:%d, skip arb token update, heart beat seq expired, local:%d msg:%d", dnodeId,
          pRspMember->vgId, pMember->state.responsedHbSeq, pRspMember->hbSeq);
    goto _OVER;
  }

  // update hb state
  pMember->state.responsedHbSeq = pRspMember->hbSeq;
  pMember->state.lastHbMs = nowMs;
  if (mndArbCheckToken(pMember->state.token, pRspMember->memberToken) == 0) {
    // skip
    mDebug("dnodeId:%d vgId:%d, skip arb token update, token matched", dnodeId, pRspMember->vgId);
    goto _OVER;
  }

  // update token
  mndArbGroupDupObj(pGroup, pNewGroup);
  (void)memcpy(pNewGroup->members[index].state.token, pRspMember->memberToken, TSDB_ARB_TOKEN_SIZE);
  pNewGroup->isSync = false;

  bool resetAssigned = false;
  if (pMember->info.dnodeId == pGroup->assignedLeader.dnodeId) {
    mndArbGroupResetAssignedLeader(pNewGroup);
    resetAssigned = true;
  }

  updateToken = true;
  mInfo("dnodeId:%d vgId:%d, arb token updating, resetAssigned:%d", dnodeId, pRspMember->vgId, resetAssigned);

_OVER:
  (void)taosThreadMutexUnlock(&pGroup->mutex);
  return updateToken;
}

static int32_t mndUpdateArbHeartBeat(SMnode *pMnode, int32_t dnodeId, SArray *memberArray) {
  int64_t nowMs = taosGetTimestampMs();
  size_t  size = taosArrayGetSize(memberArray);
  SArray *pUpdateArray = taosArrayInit(size, sizeof(SArbGroup));

  for (size_t i = 0; i < size; i++) {
    SVArbHbRspMember *pRspMember = taosArrayGet(memberArray, i);

    SArbGroup  newGroup = {0};
    SArbGroup *pGroup = sdbAcquire(pMnode->pSdb, SDB_ARBGROUP, &pRspMember->vgId);
    if (pGroup == NULL) {
      mInfo("failed to update arb token, vgId:%d not found", pRspMember->vgId);
      continue;
    }

    bool updateToken = mndUpdateArbGroupByHeartBeat(pGroup, pRspMember, nowMs, dnodeId, &newGroup);
    if (updateToken) {
      if (taosArrayPush(pUpdateArray, &newGroup) == NULL) {
        mError("failed to push newGroup to updateArray, but continue at this hearbear");
      }
    }

    sdbRelease(pMnode->pSdb, pGroup);
  }

  TAOS_CHECK_RETURN(mndPullupArbUpdateGroupBatch(pMnode, pUpdateArray));

  taosArrayDestroy(pUpdateArray);
  return 0;
}

bool mndUpdateArbGroupByCheckSync(SArbGroup *pGroup, int32_t vgId, char *member0Token, char *member1Token,
                                  bool newIsSync, SArbGroup *pNewGroup) {
  bool updateIsSync = false;

  (void)taosThreadMutexLock(&pGroup->mutex);

  if (pGroup->assignedLeader.dnodeId != 0) {
    terrno = TSDB_CODE_SUCCESS;
    mInfo("skip to update arb sync, vgId:%d has assigned leader:%d", vgId, pGroup->assignedLeader.dnodeId);
    goto _OVER;
  }

  char *local0Token = pGroup->members[0].state.token;
  char *local1Token = pGroup->members[1].state.token;
  if (mndArbCheckToken(local0Token, member0Token) != 0 || mndArbCheckToken(local1Token, member1Token) != 0) {
    terrno = TSDB_CODE_MND_ARB_TOKEN_MISMATCH;
    mInfo("skip to update arb sync, memberToken mismatch local:[%s][%s], msg:[%s][%s]", local0Token, local1Token,
          member0Token, member1Token);
    goto _OVER;
  }

  if (pGroup->isSync != newIsSync) {
    mndArbGroupDupObj(pGroup, pNewGroup);
    pNewGroup->isSync = newIsSync;

    mInfo("vgId:%d, arb isSync updating, new isSync:%d", vgId, newIsSync);
    updateIsSync = true;
  }

_OVER:
  (void)taosThreadMutexUnlock(&pGroup->mutex);
  return updateIsSync;
}

static int32_t mndUpdateArbSync(SMnode *pMnode, int32_t vgId, char *member0Token, char *member1Token, bool newIsSync) {
  int32_t    code = 0;
  SArbGroup *pGroup = sdbAcquire(pMnode->pSdb, SDB_ARBGROUP, &vgId);
  if (pGroup == NULL) {
    mInfo("failed to update arb sync, vgId:%d not found", vgId);
    code = -1;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  SArbGroup newGroup = {0};
  bool      updateIsSync = mndUpdateArbGroupByCheckSync(pGroup, vgId, member0Token, member1Token, newIsSync, &newGroup);
  if (updateIsSync) {
    if (mndPullupArbUpdateGroup(pMnode, &newGroup) != 0) {
      mInfo("failed to pullup update arb sync, vgId:%d, since %s", vgId, terrstr());
    }
  }

  sdbRelease(pMnode->pSdb, pGroup);
  return 0;
}

static int32_t mndProcessArbHbRsp(SRpcMsg *pRsp) {
  if (pRsp->contLen == 0) {
    mDebug("arb hb-rsp contLen is 0");
    return 0;
  }

  int32_t code = -1;

  SMnode *pMnode = pRsp->info.node;
  SSdb   *pSdb = pMnode->pSdb;

  char arbToken[TSDB_ARB_TOKEN_SIZE];
  if ((code = mndGetArbToken(pMnode, arbToken)) != 0) {
    mError("failed to get arb token for arb-hb response");
    TAOS_RETURN(code);
  }

  SVArbHeartBeatRsp arbHbRsp = {0};
  if ((code = tDeserializeSVArbHeartBeatRsp(pRsp->pCont, pRsp->contLen, &arbHbRsp)) != 0) {
    mInfo("arb hb-rsp des failed, since:%s", tstrerror(pRsp->code));
    TAOS_RETURN(code);
  }

  if (mndArbCheckToken(arbToken, arbHbRsp.arbToken) != 0) {
    mInfo("arb hearbeat skip update for dnodeId:%d, arb token mismatch, local:[%s] msg:[%s]", arbHbRsp.dnodeId,
          arbToken, arbHbRsp.arbToken);
    code = TSDB_CODE_MND_ARB_TOKEN_MISMATCH;
    goto _OVER;
  }

  TAOS_CHECK_GOTO(mndUpdateArbHeartBeat(pMnode, arbHbRsp.dnodeId, arbHbRsp.hbMembers), NULL, _OVER);
  code = 0;

_OVER:
  tFreeSVArbHeartBeatRsp(&arbHbRsp);
  return code;
}

static int32_t mndProcessArbCheckSyncRsp(SRpcMsg *pRsp) {
  if (pRsp->contLen == 0) {
    mDebug("arb check-sync-rsp contLen is 0");
    return 0;
  }

  int32_t code = -1;

  SMnode *pMnode = pRsp->info.node;
  SSdb   *pSdb = pMnode->pSdb;

  char arbToken[TSDB_ARB_TOKEN_SIZE];
  if ((code = mndGetArbToken(pMnode, arbToken)) != 0) {
    mError("failed to get arb token for arb-check-sync response");
    TAOS_RETURN(code);
  }

  SVArbCheckSyncRsp syncRsp = {0};
  if ((code = tDeserializeSVArbCheckSyncRsp(pRsp->pCont, pRsp->contLen, &syncRsp)) != 0) {
    mInfo("arb check-sync-rsp des failed, since:%s", tstrerror(pRsp->code));
    if (pRsp->code == TSDB_CODE_MND_ARB_TOKEN_MISMATCH) {
      terrno = TSDB_CODE_SUCCESS;
      return 0;
    }
    TAOS_RETURN(code);
  }

  if (mndArbCheckToken(arbToken, syncRsp.arbToken) != 0) {
    mInfo("skip update arb sync for vgId:%d, arb token mismatch, local:[%s] msg:[%s]", syncRsp.vgId, arbToken,
          syncRsp.arbToken);
    terrno = TSDB_CODE_MND_ARB_TOKEN_MISMATCH;
    goto _OVER;
  }

  bool newIsSync = (syncRsp.errCode == TSDB_CODE_SUCCESS);
  if ((code = mndUpdateArbSync(pMnode, syncRsp.vgId, syncRsp.member0Token, syncRsp.member1Token, newIsSync)) != 0) {
    mInfo("failed to update arb sync for vgId:%d, since:%s", syncRsp.vgId, terrstr());
    goto _OVER;
  }

  code = 0;

_OVER:
  tFreeSVArbCheckSyncRsp(&syncRsp);
  TAOS_RETURN(code);
}

bool mndUpdateArbGroupBySetAssignedLeader(SArbGroup *pGroup, int32_t vgId, char *memberToken, int32_t errcode,
                                          SArbGroup *pNewGroup) {
  bool updateAssigned = false;

  (void)taosThreadMutexLock(&pGroup->mutex);
  if (mndArbCheckToken(pGroup->assignedLeader.token, memberToken) != 0) {
    mInfo("skip update arb assigned for vgId:%d, member token mismatch, local:[%s] msg:[%s]", vgId,
          pGroup->assignedLeader.token, memberToken);
    goto _OVER;
  }

  if (errcode != TSDB_CODE_SUCCESS) {
    mInfo("skip update arb assigned for vgId:%d, since:%s", vgId, tstrerror(errcode));
    goto _OVER;
  }

  if (pGroup->assignedLeader.acked == false) {
    mndArbGroupDupObj(pGroup, pNewGroup);
    pNewGroup->isSync = false;
    pNewGroup->assignedLeader.acked = true;

    mInfo("vgId:%d, arb received assigned ack", vgId);
    updateAssigned = true;
    goto _OVER;
  }

_OVER:
  (void)taosThreadMutexUnlock(&pGroup->mutex);
  return updateAssigned;
}

static int32_t mndProcessArbSetAssignedLeaderRsp(SRpcMsg *pRsp) {
  if (pRsp->contLen == 0) {
    mDebug("arb set-assigned-rsp contLen is 0");
    return 0;
  }

  int32_t code = -1;

  SMnode *pMnode = pRsp->info.node;
  SSdb   *pSdb = pMnode->pSdb;

  char arbToken[TSDB_ARB_TOKEN_SIZE];
  if ((code = mndGetArbToken(pMnode, arbToken)) != 0) {
    mError("failed to get arb token for arb-set-assigned response");
    TAOS_RETURN(code);
  }

  SVArbSetAssignedLeaderRsp setAssignedRsp = {0};
  if ((code = tDeserializeSVArbSetAssignedLeaderRsp(pRsp->pCont, pRsp->contLen, &setAssignedRsp)) != 0) {
    mInfo("arb set-assigned-rsp des failed, since:%s", tstrerror(pRsp->code));
    TAOS_RETURN(code);
  }

  if (mndArbCheckToken(arbToken, setAssignedRsp.arbToken) != 0) {
    mInfo("skip update arb assigned for vgId:%d, arb token mismatch, local:[%s] msg:[%s]", setAssignedRsp.vgId,
          arbToken, setAssignedRsp.arbToken);
    code = TSDB_CODE_MND_ARB_TOKEN_MISMATCH;
    goto _OVER;
  }

  SArbGroup *pGroup = mndAcquireArbGroup(pMnode, setAssignedRsp.vgId);
  if (!pGroup) {
    mError("failed to set arb assigned for vgId:%d, since:%s", setAssignedRsp.vgId, terrstr());
    code = -1;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }

  SArbGroup newGroup = {0};
  bool updateAssigned = mndUpdateArbGroupBySetAssignedLeader(pGroup, setAssignedRsp.vgId, setAssignedRsp.memberToken,
                                                             pRsp->code, &newGroup);
  if (updateAssigned) {
    if ((code = mndPullupArbUpdateGroup(pMnode, &newGroup)) != 0) {
      mInfo("failed to pullup update arb assigned for vgId:%d, since:%s", setAssignedRsp.vgId, tstrerror(code));
      goto _OVER;
    }
  }

  code = 0;

_OVER:
  tFreeSVArbSetAssignedLeaderRsp(&setAssignedRsp);
  return code;
}

static int32_t mndRetrieveArbGroups(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode    *pMnode = pReq->info.node;
  SSdb      *pSdb = pMnode->pSdb;
  int32_t    numOfRows = 0;
  int32_t    cols = 0;
  SArbGroup *pGroup = NULL;
  int32_t    code = 0;
  int32_t    lino = 0;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_ARBGROUP, pShow->pIter, (void **)&pGroup);
    if (pShow->pIter == NULL) break;

    (void)taosThreadMutexLock(&pGroup->mutex);

    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    SVgObj          *pVgObj = sdbAcquire(pSdb, SDB_VGROUP, &pGroup->vgId);
    if (!pVgObj) {
      (void)taosThreadMutexUnlock(&pGroup->mutex);
      sdbRelease(pSdb, pGroup);
      continue;
    }
    char dbNameInGroup[TSDB_DB_FNAME_LEN];
    strncpy(dbNameInGroup, pVgObj->dbName, TSDB_DB_FNAME_LEN);
    sdbRelease(pSdb, pVgObj);

    char dbname[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(dbname, mndGetDbStr(dbNameInGroup), TSDB_ARB_TOKEN_SIZE + VARSTR_HEADER_SIZE);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)dbname, false), pGroup, &lino, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pGroup->vgId, false), pGroup, &lino, _OVER);

    for (int i = 0; i < TSDB_ARB_GROUP_MEMBER_NUM; i++) {
      SArbGroupMember *pMember = &pGroup->members[i];
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pMember->info.dnodeId, false), pGroup,
                          &lino, _OVER);
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pGroup->isSync, false), pGroup, &lino, _OVER);

    if (pGroup->assignedLeader.dnodeId != 0) {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pGroup->assignedLeader.dnodeId, false),
                          pGroup, &lino, _OVER);

      char token[TSDB_ARB_TOKEN_SIZE + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(token, pGroup->assignedLeader.token, TSDB_ARB_TOKEN_SIZE + VARSTR_HEADER_SIZE);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)token, false), pGroup, &lino, _OVER);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      RETRIEVE_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, (const char *)&pGroup->assignedLeader.acked, false),
                          pGroup, &lino, _OVER);
    } else {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetNULL(pColInfo, numOfRows);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetNULL(pColInfo, numOfRows);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetNULL(pColInfo, numOfRows);
    }

    (void)taosThreadMutexUnlock(&pGroup->mutex);

    numOfRows++;
    sdbRelease(pSdb, pGroup);
  }

_OVER:
  if (code != 0) mError("failed to restrieve arb group at line:%d, since %s", lino, tstrerror(code));
  pShow->numOfRows += numOfRows;

  return numOfRows;
}

static void mndCancelGetNextArbGroup(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_ARBGROUP);
}

int32_t mndGetArbGroupSize(SMnode *pMnode) {
  SSdb *pSdb = pMnode->pSdb;
  return sdbGetSize(pSdb, SDB_ARBGROUP);
}
