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
#include "mndVgroup.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndMnode.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndTrans.h"
#include "mndUser.h"

#define VGROUP_VER_NUMBER   1
#define VGROUP_RESERVE_SIZE 64

static SSdbRow *mndVgroupActionDecode(SSdbRaw *pRaw);
static int32_t  mndVgroupActionInsert(SSdb *pSdb, SVgObj *pVgroup);
static int32_t  mndVgroupActionDelete(SSdb *pSdb, SVgObj *pVgroup);
static int32_t  mndVgroupActionUpdate(SSdb *pSdb, SVgObj *pOld, SVgObj *pNew);

static int32_t mndRetrieveVgroups(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextVgroup(SMnode *pMnode, void *pIter);
static int32_t mndRetrieveVnodes(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextVnode(SMnode *pMnode, void *pIter);

static int32_t mndProcessRedistributeVgroupMsg(SRpcMsg *pReq);
static int32_t mndProcessSplitVgroupMsg(SRpcMsg *pReq);
static int32_t mndProcessBalanceVgroupMsg(SRpcMsg *pReq);

int32_t mndInitVgroup(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_VGROUP,
      .keyType = SDB_KEY_INT32,
      .encodeFp = (SdbEncodeFp)mndVgroupActionEncode,
      .decodeFp = (SdbDecodeFp)mndVgroupActionDecode,
      .insertFp = (SdbInsertFp)mndVgroupActionInsert,
      .updateFp = (SdbUpdateFp)mndVgroupActionUpdate,
      .deleteFp = (SdbDeleteFp)mndVgroupActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_DND_CREATE_VNODE_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_ALTER_REPLICA_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_ALTER_CONFIG_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_ALTER_CONFIRM_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_ALTER_HASHRANGE_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_DND_DROP_VNODE_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_COMPACT_RSP, mndTransProcessRsp);

  mndSetMsgHandle(pMnode, TDMT_MND_REDISTRIBUTE_VGROUP, mndProcessRedistributeVgroupMsg);
  mndSetMsgHandle(pMnode, TDMT_MND_MERGE_VGROUP, mndProcessSplitVgroupMsg);
  mndSetMsgHandle(pMnode, TDMT_MND_BALANCE_VGROUP, mndProcessBalanceVgroupMsg);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_VGROUP, mndRetrieveVgroups);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_VGROUP, mndCancelGetNextVgroup);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_VNODES, mndRetrieveVnodes);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_VNODES, mndCancelGetNextVnode);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupVgroup(SMnode *pMnode) {}

SSdbRaw *mndVgroupActionEncode(SVgObj *pVgroup) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_VGROUP, VGROUP_VER_NUMBER, sizeof(SVgObj) + VGROUP_RESERVE_SIZE);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pVgroup->vgId, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pVgroup->createdTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pVgroup->updateTime, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pVgroup->version, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pVgroup->hashBegin, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pVgroup->hashEnd, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pVgroup->dbName, TSDB_DB_FNAME_LEN, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pVgroup->dbUid, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pVgroup->isTsma, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pVgroup->replica, _OVER)
  for (int8_t i = 0; i < pVgroup->replica; ++i) {
    SVnodeGid *pVgid = &pVgroup->vnodeGid[i];
    SDB_SET_INT32(pRaw, dataPos, pVgid->dnodeId, _OVER)
  }
  SDB_SET_RESERVE(pRaw, dataPos, VGROUP_RESERVE_SIZE, _OVER)
  SDB_SET_DATALEN(pRaw, dataPos, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("vgId:%d, failed to encode to raw:%p since %s", pVgroup->vgId, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("vgId:%d, encode to raw:%p, row:%p", pVgroup->vgId, pRaw, pVgroup);
  return pRaw;
}

SSdbRow *mndVgroupActionDecode(SSdbRaw *pRaw) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver != VGROUP_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  SSdbRow *pRow = sdbAllocRow(sizeof(SVgObj));
  if (pRow == NULL) goto _OVER;

  SVgObj *pVgroup = sdbGetRowObj(pRow);
  if (pVgroup == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &pVgroup->vgId, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pVgroup->createdTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pVgroup->updateTime, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pVgroup->version, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pVgroup->hashBegin, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pVgroup->hashEnd, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pVgroup->dbName, TSDB_DB_FNAME_LEN, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pVgroup->dbUid, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pVgroup->isTsma, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pVgroup->replica, _OVER)
  for (int8_t i = 0; i < pVgroup->replica; ++i) {
    SVnodeGid *pVgid = &pVgroup->vnodeGid[i];
    SDB_GET_INT32(pRaw, dataPos, &pVgid->dnodeId, _OVER)
    if (pVgroup->replica == 1) {
      pVgid->role = TAOS_SYNC_STATE_LEADER;
    }
  }
  SDB_GET_RESERVE(pRaw, dataPos, VGROUP_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("vgId:%d, failed to decode from raw:%p since %s", pVgroup->vgId, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("vgId:%d, decode from raw:%p, row:%p", pVgroup->vgId, pRaw, pVgroup);
  return pRow;
}

static int32_t mndVgroupActionInsert(SSdb *pSdb, SVgObj *pVgroup) {
  mTrace("vgId:%d, perform insert action, row:%p", pVgroup->vgId, pVgroup);
  return 0;
}

static int32_t mndVgroupActionDelete(SSdb *pSdb, SVgObj *pVgroup) {
  mTrace("vgId:%d, perform delete action, row:%p", pVgroup->vgId, pVgroup);
  return 0;
}

static int32_t mndVgroupActionUpdate(SSdb *pSdb, SVgObj *pOld, SVgObj *pNew) {
  mTrace("vgId:%d, perform update action, old row:%p new row:%p", pOld->vgId, pOld, pNew);
  pOld->updateTime = pNew->updateTime;
  pOld->version = pNew->version;
  pOld->hashBegin = pNew->hashBegin;
  pOld->hashEnd = pNew->hashEnd;
  pOld->replica = pNew->replica;
  pOld->isTsma = pNew->isTsma;
  memcpy(pOld->vnodeGid, pNew->vnodeGid, TSDB_MAX_REPLICA * sizeof(SVnodeGid));
  return 0;
}

SVgObj *mndAcquireVgroup(SMnode *pMnode, int32_t vgId) {
  SSdb   *pSdb = pMnode->pSdb;
  SVgObj *pVgroup = sdbAcquire(pSdb, SDB_VGROUP, &vgId);
  if (pVgroup == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_MND_VGROUP_NOT_EXIST;
  }
  return pVgroup;
}

void mndReleaseVgroup(SMnode *pMnode, SVgObj *pVgroup) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pVgroup);
}

void *mndBuildCreateVnodeReq(SMnode *pMnode, SDnodeObj *pDnode, SDbObj *pDb, SVgObj *pVgroup, int32_t *pContLen,
                             bool standby) {
  SCreateVnodeReq createReq = {0};
  createReq.vgId = pVgroup->vgId;
  memcpy(createReq.db, pDb->name, TSDB_DB_FNAME_LEN);
  createReq.dbUid = pDb->uid;
  createReq.vgVersion = pVgroup->version;
  createReq.numOfStables = pDb->cfg.numOfStables;
  createReq.buffer = pDb->cfg.buffer;
  createReq.pageSize = pDb->cfg.pageSize;
  createReq.pages = pDb->cfg.pages;
  createReq.cacheLastSize = pDb->cfg.cacheLastSize;
  createReq.daysPerFile = pDb->cfg.daysPerFile;
  createReq.daysToKeep0 = pDb->cfg.daysToKeep0;
  createReq.daysToKeep1 = pDb->cfg.daysToKeep1;
  createReq.daysToKeep2 = pDb->cfg.daysToKeep2;
  createReq.minRows = pDb->cfg.minRows;
  createReq.maxRows = pDb->cfg.maxRows;
  createReq.walFsyncPeriod = pDb->cfg.walFsyncPeriod;
  createReq.walLevel = pDb->cfg.walLevel;
  createReq.precision = pDb->cfg.precision;
  createReq.compression = pDb->cfg.compression;
  createReq.strict = pDb->cfg.strict;
  createReq.cacheLast = pDb->cfg.cacheLast;
  createReq.replica = pVgroup->replica;
  createReq.selfIndex = -1;
  createReq.hashBegin = pVgroup->hashBegin;
  createReq.hashEnd = pVgroup->hashEnd;
  createReq.hashMethod = pDb->cfg.hashMethod;
  createReq.numOfRetensions = pDb->cfg.numOfRetensions;
  createReq.pRetensions = pDb->cfg.pRetensions;
  createReq.standby = standby;
  createReq.isTsma = pVgroup->isTsma;
  createReq.pTsma = pVgroup->pTsma;
  createReq.walRetentionPeriod = pDb->cfg.walRetentionPeriod;
  createReq.walRetentionSize = pDb->cfg.walRetentionSize;
  createReq.walRollPeriod = pDb->cfg.walRollPeriod;
  createReq.walSegmentSize = pDb->cfg.walSegmentSize;

  for (int32_t v = 0; v < pVgroup->replica; ++v) {
    SReplica  *pReplica = &createReq.replicas[v];
    SVnodeGid *pVgid = &pVgroup->vnodeGid[v];
    SDnodeObj *pVgidDnode = mndAcquireDnode(pMnode, pVgid->dnodeId);
    if (pVgidDnode == NULL) {
      return NULL;
    }

    pReplica->id = pVgidDnode->id;
    pReplica->port = pVgidDnode->port;
    memcpy(pReplica->fqdn, pVgidDnode->fqdn, TSDB_FQDN_LEN);
    mndReleaseDnode(pMnode, pVgidDnode);

    if (pDnode->id == pVgid->dnodeId) {
      createReq.selfIndex = v;
    }
  }

  if (createReq.selfIndex == -1) {
    terrno = TSDB_CODE_MND_APP_ERROR;
    return NULL;
  }

  int32_t contLen = tSerializeSCreateVnodeReq(NULL, 0, &createReq);
  if (contLen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  void *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  tSerializeSCreateVnodeReq(pReq, contLen, &createReq);
  *pContLen = contLen;
  return pReq;
}

void *mndBuildAlterVnodeReq(SMnode *pMnode, SDbObj *pDb, SVgObj *pVgroup, int32_t *pContLen) {
  SAlterVnodeReq alterReq = {0};
  alterReq.vgVersion = pVgroup->version;
  alterReq.buffer = pDb->cfg.buffer;
  alterReq.pageSize = pDb->cfg.pageSize;
  alterReq.pages = pDb->cfg.pages;
  alterReq.cacheLastSize = pDb->cfg.cacheLastSize;
  alterReq.daysPerFile = pDb->cfg.daysPerFile;
  alterReq.daysToKeep0 = pDb->cfg.daysToKeep0;
  alterReq.daysToKeep1 = pDb->cfg.daysToKeep1;
  alterReq.daysToKeep2 = pDb->cfg.daysToKeep2;
  alterReq.walFsyncPeriod = pDb->cfg.walFsyncPeriod;
  alterReq.walLevel = pDb->cfg.walLevel;
  alterReq.strict = pDb->cfg.strict;
  alterReq.cacheLast = pDb->cfg.cacheLast;
  alterReq.replica = pVgroup->replica;

  for (int32_t v = 0; v < pVgroup->replica; ++v) {
    SReplica  *pReplica = &alterReq.replicas[v];
    SVnodeGid *pVgid = &pVgroup->vnodeGid[v];
    SDnodeObj *pVgidDnode = mndAcquireDnode(pMnode, pVgid->dnodeId);
    if (pVgidDnode == NULL) {
      return NULL;
    }

    pReplica->id = pVgidDnode->id;
    pReplica->port = pVgidDnode->port;
    memcpy(pReplica->fqdn, pVgidDnode->fqdn, TSDB_FQDN_LEN);
    mndReleaseDnode(pMnode, pVgidDnode);
  }

  int32_t contLen = tSerializeSAlterVnodeReq(NULL, 0, &alterReq);
  if (contLen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  contLen += sizeof(SMsgHead);

  void *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  SMsgHead *pHead = pReq;
  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(pVgroup->vgId);

  tSerializeSAlterVnodeReq((char *)pReq + sizeof(SMsgHead), contLen, &alterReq);
  *pContLen = contLen;
  return pReq;
}

void *mndBuildSetVnodeStandbyReq(SMnode *pMnode, SDnodeObj *pDnode, SDbObj *pDb, SVgObj *pVgroup, int32_t *pContLen) {
  SSetStandbyReq standbyReq = {0};
  standbyReq.dnodeId = pDnode->id;
  standbyReq.standby = 1;

  int32_t contLen = tSerializeSSetStandbyReq(NULL, 0, &standbyReq);
  if (contLen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  contLen += sizeof(SMsgHead);
  void *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  tSerializeSSetStandbyReq((char *)pReq + sizeof(SMsgHead), contLen, &standbyReq);
  SMsgHead *pHead = pReq;
  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(pVgroup->vgId);

  *pContLen = contLen;
  return pReq;
}

void *mndBuildDropVnodeReq(SMnode *pMnode, SDnodeObj *pDnode, SDbObj *pDb, SVgObj *pVgroup, int32_t *pContLen) {
  SDropVnodeReq dropReq = {0};
  dropReq.dnodeId = pDnode->id;
  dropReq.vgId = pVgroup->vgId;
  memcpy(dropReq.db, pDb->name, TSDB_DB_FNAME_LEN);
  dropReq.dbUid = pDb->uid;

  int32_t contLen = tSerializeSDropVnodeReq(NULL, 0, &dropReq);
  if (contLen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  void *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  tSerializeSDropVnodeReq(pReq, contLen, &dropReq);
  *pContLen = contLen;
  return pReq;
}

static bool mndResetDnodesArrayFp(SMnode *pMnode, void *pObj, void *p1, void *p2, void *p3) {
  SDnodeObj *pDnode = pObj;
  pDnode->numOfVnodes = 0;
  return true;
}

static bool mndBuildDnodesArrayFp(SMnode *pMnode, void *pObj, void *p1, void *p2, void *p3) {
  SDnodeObj *pDnode = pObj;
  SArray    *pArray = p1;
  int32_t    exceptDnodeId = *(int32_t *)p2;

  if (exceptDnodeId == pDnode->id) {
    return true;
  }

  int64_t curMs = taosGetTimestampMs();
  bool    online = mndIsDnodeOnline(pDnode, curMs);
  bool    isMnode = mndIsMnode(pMnode, pDnode->id);
  pDnode->numOfVnodes = mndGetVnodesNum(pMnode, pDnode->id);
  pDnode->memUsed = mndGetVnodesMemory(pMnode, pDnode->id);

  mDebug("dnode:%d, vnodes:%d supportVnodes:%d isMnode:%d online:%d memory avail:%" PRId64 " used:%" PRId64, pDnode->id,
         pDnode->numOfVnodes, pDnode->numOfSupportVnodes, isMnode, online, pDnode->memAvail, pDnode->memUsed);

  if (isMnode) {
    pDnode->numOfVnodes++;
  }

  if (online && pDnode->numOfSupportVnodes > 0) {
    taosArrayPush(pArray, pDnode);
  }
  return true;
}

SArray *mndBuildDnodesArray(SMnode *pMnode, int32_t exceptDnodeId) {
  SSdb   *pSdb = pMnode->pSdb;
  int32_t numOfDnodes = mndGetDnodeSize(pMnode);

  SArray *pArray = taosArrayInit(numOfDnodes, sizeof(SDnodeObj));
  if (pArray == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  sdbTraverse(pSdb, SDB_DNODE, mndResetDnodesArrayFp, NULL, NULL, NULL);
  sdbTraverse(pSdb, SDB_DNODE, mndBuildDnodesArrayFp, pArray, &exceptDnodeId, NULL);
  return pArray;
}

static int32_t mndCompareDnodeId(int32_t *dnode1Id, int32_t *dnode2Id) { return *dnode1Id >= *dnode2Id ? 1 : 0; }

static int32_t mndCompareDnodeVnodes(SDnodeObj *pDnode1, SDnodeObj *pDnode2) {
  float d1Score = (float)pDnode1->numOfVnodes / pDnode1->numOfSupportVnodes;
  float d2Score = (float)pDnode2->numOfVnodes / pDnode2->numOfSupportVnodes;
  return d1Score >= d2Score ? 1 : 0;
}

void mndSortVnodeGid(SVgObj *pVgroup) {
  for (int32_t i = 0; i < pVgroup->replica; ++i) {
    for (int32_t j = 0; j < pVgroup->replica - 1 - i; ++j) {
      if (pVgroup->vnodeGid[j].dnodeId > pVgroup->vnodeGid[j + 1].dnodeId) {
        TSWAP(pVgroup->vnodeGid[j], pVgroup->vnodeGid[j + 1]);
      }
    }
  }
}

static int32_t mndGetAvailableDnode(SMnode *pMnode, SDbObj *pDb, SVgObj *pVgroup, SArray *pArray) {
  SSdb   *pSdb = pMnode->pSdb;
  int32_t allocedVnodes = 0;
  void   *pIter = NULL;

  taosArraySort(pArray, (__compar_fn_t)mndCompareDnodeVnodes);

  int32_t size = taosArrayGetSize(pArray);
  if (size < pVgroup->replica) {
    mError("db:%s, vgId:%d, no enough online dnodes:%d to alloc %d replica", pVgroup->dbName, pVgroup->vgId, size,
           pVgroup->replica);
    terrno = TSDB_CODE_MND_NO_ENOUGH_DNODES;
    return -1;
  }

  for (int32_t v = 0; v < pVgroup->replica; ++v) {
    SVnodeGid *pVgid = &pVgroup->vnodeGid[v];
    SDnodeObj *pDnode = taosArrayGet(pArray, v);
    if (pDnode == NULL || pDnode->numOfVnodes > pDnode->numOfSupportVnodes) {
      terrno = TSDB_CODE_MND_NO_ENOUGH_DNODES;
      return -1;
    }

    int64_t vgMem = mndGetVgroupMemory(pMnode, pDb, pVgroup);
    if (pDnode->memAvail - vgMem - pDnode->memUsed <= 0) {
      mError("db:%s, vgId:%d, no enough memory:%" PRId64 " in dnode:%d, avail:%" PRId64 " used:%" PRId64,
             pVgroup->dbName, pVgroup->vgId, vgMem, pDnode->id, pDnode->memAvail, pDnode->memUsed);
      terrno = TSDB_CODE_MND_NO_ENOUGH_MEM_IN_DNODE;
      return -1;
    } else {
      pDnode->memUsed += vgMem;
    }

    pVgid->dnodeId = pDnode->id;
    if (pVgroup->replica == 1) {
      pVgid->role = TAOS_SYNC_STATE_LEADER;
    } else {
      pVgid->role = TAOS_SYNC_STATE_FOLLOWER;
    }

    mInfo("db:%s, vgId:%d, vn:%d is alloced, memory:%" PRId64 ", dnode:%d avail:%" PRId64 " used:%" PRId64,
          pVgroup->dbName, pVgroup->vgId, v, vgMem, pVgid->dnodeId, pDnode->memAvail, pDnode->memUsed);
    pDnode->numOfVnodes++;
  }

  mndSortVnodeGid(pVgroup);
  return 0;
}

int32_t mndAllocSmaVgroup(SMnode *pMnode, SDbObj *pDb, SVgObj *pVgroup) {
  SArray *pArray = mndBuildDnodesArray(pMnode, 0);
  if (pArray == NULL) return -1;

  pVgroup->vgId = sdbGetMaxId(pMnode->pSdb, SDB_VGROUP);
  pVgroup->isTsma = 1;
  pVgroup->createdTime = taosGetTimestampMs();
  pVgroup->updateTime = pVgroup->createdTime;
  pVgroup->version = 1;
  memcpy(pVgroup->dbName, pDb->name, TSDB_DB_FNAME_LEN);
  pVgroup->dbUid = pDb->uid;
  pVgroup->replica = 1;

  if (mndGetAvailableDnode(pMnode, pDb, pVgroup, pArray) != 0) return -1;
  taosArrayDestroy(pArray);

  mInfo("db:%s, sma vgId:%d is alloced", pDb->name, pVgroup->vgId);
  return 0;
}

int32_t mndAllocVgroup(SMnode *pMnode, SDbObj *pDb, SVgObj **ppVgroups) {
  int32_t code = -1;
  SArray *pArray = NULL;
  SVgObj *pVgroups = NULL;

  pVgroups = taosMemoryCalloc(pDb->cfg.numOfVgroups, sizeof(SVgObj));
  if (pVgroups == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  pArray = mndBuildDnodesArray(pMnode, 0);
  if (pArray == NULL) goto _OVER;

  mInfo("db:%s, total %d dnodes used to create %d vgroups (%d vnodes)", pDb->name, (int32_t)taosArrayGetSize(pArray),
        pDb->cfg.numOfVgroups, pDb->cfg.numOfVgroups * pDb->cfg.replications);

  int32_t  allocedVgroups = 0;
  int32_t  maxVgId = sdbGetMaxId(pMnode->pSdb, SDB_VGROUP);
  uint32_t hashMin = 0;
  uint32_t hashMax = UINT32_MAX;
  uint32_t hashInterval = (hashMax - hashMin) / pDb->cfg.numOfVgroups;

  if (maxVgId < 2) maxVgId = 2;

  for (uint32_t v = 0; v < pDb->cfg.numOfVgroups; v++) {
    SVgObj *pVgroup = &pVgroups[v];
    pVgroup->vgId = maxVgId++;
    pVgroup->createdTime = taosGetTimestampMs();
    pVgroup->updateTime = pVgroups->createdTime;
    pVgroup->version = 1;
    pVgroup->hashBegin = hashMin + hashInterval * v;
    if (v == pDb->cfg.numOfVgroups - 1) {
      pVgroup->hashEnd = hashMax;
    } else {
      pVgroup->hashEnd = hashMin + hashInterval * (v + 1) - 1;
    }

    memcpy(pVgroup->dbName, pDb->name, TSDB_DB_FNAME_LEN);
    pVgroup->dbUid = pDb->uid;
    pVgroup->replica = pDb->cfg.replications;

    if (mndGetAvailableDnode(pMnode, pDb, pVgroup, pArray) != 0) {
      goto _OVER;
    }

    allocedVgroups++;
  }

  *ppVgroups = pVgroups;
  code = 0;

  mInfo("db:%s, total %d vgroups is alloced, replica:%d", pDb->name, pDb->cfg.numOfVgroups, pDb->cfg.replications);

_OVER:
  if (code != 0) taosMemoryFree(pVgroups);
  taosArrayDestroy(pArray);
  return code;
}

SEpSet mndGetVgroupEpset(SMnode *pMnode, const SVgObj *pVgroup) {
  SEpSet epset = {0};

  for (int32_t v = 0; v < pVgroup->replica; ++v) {
    const SVnodeGid *pVgid = &pVgroup->vnodeGid[v];
    SDnodeObj       *pDnode = mndAcquireDnode(pMnode, pVgid->dnodeId);
    if (pDnode == NULL) continue;

    if (pVgid->role == TAOS_SYNC_STATE_LEADER) {
      epset.inUse = epset.numOfEps;
    }

    addEpIntoEpSet(&epset, pDnode->fqdn, pDnode->port);
    mndReleaseDnode(pMnode, pDnode);
  }

  return epset;
}

static bool mndGetVgroupMaxReplicaFp(SMnode *pMnode, void *pObj, void *p1, void *p2, void *p3) {
  SVgObj  *pVgroup = pObj;
  int64_t  uid = *(int64_t *)p1;
  int8_t  *pReplica = p2;
  int32_t *pNumOfVgroups = p3;

  if (pVgroup->dbUid == uid) {
    *pReplica = TMAX(*pReplica, pVgroup->replica);
    (*pNumOfVgroups)++;
  }

  return true;
}

static int32_t mndGetVgroupMaxReplica(SMnode *pMnode, char *dbName, int8_t *pReplica, int32_t *pNumOfVgroups) {
  SSdb   *pSdb = pMnode->pSdb;
  SDbObj *pDb = mndAcquireDb(pMnode, dbName);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_SELECTED;
    return -1;
  }

  *pReplica = 1;
  *pNumOfVgroups = 0;
  sdbTraverse(pSdb, SDB_VGROUP, mndGetVgroupMaxReplicaFp, &pDb->uid, pReplica, pNumOfVgroups);
  mndReleaseDb(pMnode, pDb);
  return 0;
}

static int32_t mndRetrieveVgroups(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode *pMnode = pReq->info.node;
  SSdb   *pSdb = pMnode->pSdb;
  int32_t numOfRows = 0;
  SVgObj *pVgroup = NULL;
  int32_t cols = 0;
  int64_t curMs = taosGetTimestampMs();

  SDbObj *pDb = NULL;
  if (strlen(pShow->db) > 0) {
    pDb = mndAcquireDb(pMnode, pShow->db);
    if (pDb == NULL) {
      return 0;
    }
  }

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_VGROUP, pShow->pIter, (void **)&pVgroup);
    if (pShow->pIter == NULL) break;

    if (pDb != NULL && pVgroup->dbUid != pDb->uid) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)&pVgroup->vgId, false);

    SName name = {0};
    char  db[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    tNameFromString(&name, pVgroup->dbName, T_NAME_ACCT | T_NAME_DB);
    tNameGetDbName(&name, varDataVal(db));
    varDataSetLen(db, strlen(varDataVal(db)));

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)db, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)&pVgroup->numOfTables, false);

    // default 3 replica
    for (int32_t i = 0; i < 3; ++i) {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      if (i < pVgroup->replica) {
        colDataAppend(pColInfo, numOfRows, (const char *)&pVgroup->vnodeGid[i].dnodeId, false);

        bool       online = false;
        SDnodeObj *pDnode = mndAcquireDnode(pMnode, pVgroup->vnodeGid[i].dnodeId);
        if (pDnode != NULL) {
          online = mndIsDnodeOnline(pDnode, curMs);
          mndReleaseDnode(pMnode, pDnode);
        }

        char        buf1[20] = {0};
        const char *role = online ? syncStr(pVgroup->vnodeGid[i].role) : "offline";
        STR_WITH_MAXSIZE_TO_VARSTR(buf1, role, pShow->pMeta->pSchemas[cols].bytes);

        pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
        colDataAppend(pColInfo, numOfRows, (const char *)buf1, false);
      } else {
        colDataAppendNULL(pColInfo, numOfRows);
        pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
        colDataAppendNULL(pColInfo, numOfRows);
      }
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppendNULL(pColInfo, numOfRows);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppendNULL(pColInfo, numOfRows);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppendNULL(pColInfo, numOfRows);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)&pVgroup->isTsma, false);

    numOfRows++;
    sdbRelease(pSdb, pVgroup);
  }

  if (pDb != NULL) {
    mndReleaseDb(pMnode, pDb);
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextVgroup(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}

static bool mndGetVnodesNumFp(SMnode *pMnode, void *pObj, void *p1, void *p2, void *p3) {
  SVgObj  *pVgroup = pObj;
  int32_t  dnodeId = *(int32_t *)p1;
  int32_t *pNumOfVnodes = (int32_t *)p2;

  for (int32_t v = 0; v < pVgroup->replica; ++v) {
    if (pVgroup->vnodeGid[v].dnodeId == dnodeId) {
      (*pNumOfVnodes)++;
    }
  }

  return true;
}

int32_t mndGetVnodesNum(SMnode *pMnode, int32_t dnodeId) {
  int32_t numOfVnodes = 0;
  sdbTraverse(pMnode->pSdb, SDB_VGROUP, mndGetVnodesNumFp, &dnodeId, &numOfVnodes, NULL);
  return numOfVnodes;
}

int64_t mndGetVgroupMemory(SMnode *pMnode, SDbObj *pDbInput, SVgObj *pVgroup) {
  SDbObj *pDb = pDbInput;
  if (pDbInput == NULL) {
    pDb = mndAcquireDb(pMnode, pVgroup->dbName);
  }

  int64_t vgroupMemroy = 0;
  if (pDb != NULL) {
    vgroupMemroy = (int64_t)pDb->cfg.buffer * 1024 * 1024 + (int64_t)pDb->cfg.pages * pDb->cfg.pageSize * 1024;
    if (pDb->cfg.cacheLast > 0) {
      vgroupMemroy += (int64_t)pDb->cfg.cacheLastSize * 1024 * 1024;
    }
  }

  if (pDbInput == NULL) {
    mndReleaseDb(pMnode, pDb);
  }
  return vgroupMemroy;
}

static bool mndGetVnodeMemroyFp(SMnode *pMnode, void *pObj, void *p1, void *p2, void *p3) {
  SVgObj  *pVgroup = pObj;
  int32_t  dnodeId = *(int32_t *)p1;
  int64_t *pVnodeMemory = (int64_t *)p2;

  for (int32_t v = 0; v < pVgroup->replica; ++v) {
    if (pVgroup->vnodeGid[v].dnodeId == dnodeId) {
      *pVnodeMemory += mndGetVgroupMemory(pMnode, NULL, pVgroup);
    }
  }

  return true;
}

int64_t mndGetVnodesMemory(SMnode *pMnode, int32_t dnodeId) {
  int64_t vnodeMemory = 0;
  sdbTraverse(pMnode->pSdb, SDB_VGROUP, mndGetVnodeMemroyFp, &dnodeId, &vnodeMemory, NULL);
  return vnodeMemory;
}

static int32_t mndRetrieveVnodes(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode *pMnode = pReq->info.node;
  SSdb   *pSdb = pMnode->pSdb;
  int32_t numOfRows = 0;
  SVgObj *pVgroup = NULL;
  int32_t cols = 0;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_VGROUP, pShow->pIter, (void **)&pVgroup);
    if (pShow->pIter == NULL) break;

    for (int32_t i = 0; i < pVgroup->replica && numOfRows < rows; ++i) {
      SVnodeGid *pVgid = &pVgroup->vnodeGid[i];
      cols = 0;

      SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)&pVgroup->vgId, false);

      SName name = {0};
      char  db[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      tNameFromString(&name, pVgroup->dbName, T_NAME_ACCT | T_NAME_DB);
      tNameGetDbName(&name, varDataVal(db));
      varDataSetLen(db, strlen(varDataVal(db)));

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)db, false);

      uint32_t val = 0;
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)&val, false);

      char buf[20] = {0};
      STR_TO_VARSTR(buf, syncStr(pVgid->role));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)buf, false);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataAppend(pColInfo, numOfRows, (const char *)&pVgroup->replica, false);  // onlines

      numOfRows++;
    }

    sdbRelease(pSdb, pVgroup);
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextVnode(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}

int32_t mndAddVnodeToVgroup(SMnode *pMnode, SVgObj *pVgroup, SArray *pArray) {
  taosArraySort(pArray, (__compar_fn_t)mndCompareDnodeVnodes);
  for (int32_t i = 0; i < taosArrayGetSize(pArray); ++i) {
    SDnodeObj *pDnode = taosArrayGet(pArray, i);
    mDebug("dnode:%d, equivalent vnodes:%d", pDnode->id, pDnode->numOfVnodes);
  }

  SVnodeGid *pVgid = &pVgroup->vnodeGid[pVgroup->replica];
  for (int32_t d = 0; d < taosArrayGetSize(pArray); ++d) {
    SDnodeObj *pDnode = taosArrayGet(pArray, d);

    bool used = false;
    for (int32_t vn = 0; vn < pVgroup->replica; ++vn) {
      if (pDnode->id == pVgroup->vnodeGid[vn].dnodeId) {
        used = true;
        break;
      }
    }
    if (used) continue;

    if (pDnode == NULL || pDnode->numOfVnodes > pDnode->numOfSupportVnodes) {
      terrno = TSDB_CODE_MND_NO_ENOUGH_DNODES;
      return -1;
    }

    int64_t vgMem = mndGetVgroupMemory(pMnode, NULL, pVgroup);
    if (pDnode->memAvail - vgMem - pDnode->memUsed <= 0) {
      mError("db:%s, vgId:%d, no enough memory:%" PRId64 " in dnode:%d avail:%" PRId64 " used:%" PRId64,
             pVgroup->dbName, pVgroup->vgId, vgMem, pDnode->id, pDnode->memAvail, pDnode->memUsed);
      terrno = TSDB_CODE_MND_NO_ENOUGH_MEM_IN_DNODE;
      return -1;
    } else {
      pDnode->memUsed += vgMem;
    }

    pVgid->dnodeId = pDnode->id;
    pVgid->role = TAOS_SYNC_STATE_ERROR;
    mInfo("db:%s, vgId:%d, vn:%d is added, memory:%" PRId64 ", dnode:%d avail:%" PRId64 " used:%" PRId64,
          pVgroup->dbName, pVgroup->vgId, pVgroup->replica, vgMem, pVgid->dnodeId, pDnode->memAvail, pDnode->memUsed);

    pVgroup->replica++;
    pDnode->numOfVnodes++;
    return 0;
  }

  terrno = TSDB_CODE_MND_NO_ENOUGH_DNODES;
  mError("db:%s, failed to add vnode to vgId:%d since %s", pVgroup->dbName, pVgroup->vgId, terrstr());
  return -1;
}

int32_t mndRemoveVnodeFromVgroup(SMnode *pMnode, SVgObj *pVgroup, SArray *pArray, SVnodeGid *pDelVgid) {
  taosArraySort(pArray, (__compar_fn_t)mndCompareDnodeVnodes);
  for (int32_t i = 0; i < taosArrayGetSize(pArray); ++i) {
    SDnodeObj *pDnode = taosArrayGet(pArray, i);
    mDebug("dnode:%d, equivalent vnodes:%d", pDnode->id, pDnode->numOfVnodes);
  }

  int32_t code = -1;
  for (int32_t d = taosArrayGetSize(pArray) - 1; d >= 0; --d) {
    SDnodeObj *pDnode = taosArrayGet(pArray, d);

    for (int32_t vn = 0; vn < pVgroup->replica; ++vn) {
      SVnodeGid *pVgid = &pVgroup->vnodeGid[vn];
      if (pVgid->dnodeId == pDnode->id) {
        int64_t vgMem = mndGetVgroupMemory(pMnode, NULL, pVgroup);
        pDnode->memUsed -= vgMem;
        mInfo("db:%s, vgId:%d, vn:%d is removed, memory:%" PRId64 ", dnode:%d avail:%" PRId64 " used:%" PRId64,
              pVgroup->dbName, pVgroup->vgId, vn, vgMem, pVgid->dnodeId, pDnode->memAvail, pDnode->memUsed);
        pDnode->numOfVnodes--;
        pVgroup->replica--;
        *pDelVgid = *pVgid;
        *pVgid = pVgroup->vnodeGid[pVgroup->replica];
        memset(&pVgroup->vnodeGid[pVgroup->replica], 0, sizeof(SVnodeGid));
        code = 0;
        goto _OVER;
      }
    }
  }

_OVER:
  if (code != 0) {
    terrno = TSDB_CODE_APP_ERROR;
    mError("db:%s, failed to remove vnode from vgId:%d since %s", pVgroup->dbName, pVgroup->vgId, terrstr());
    return -1;
  }

  for (int32_t vn = 0; vn < pVgroup->replica; ++vn) {
    SVnodeGid *pVgid = &pVgroup->vnodeGid[vn];
    mInfo("db:%s, vgId:%d, vn:%d dnode:%d is reserved", pVgroup->dbName, pVgroup->vgId, vn, pVgid->dnodeId);
  }
  return 0;
}

int32_t mndAddCreateVnodeAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup, SVnodeGid *pVgid,
                                bool standby) {
  STransAction action = {0};

  SDnodeObj *pDnode = mndAcquireDnode(pMnode, pVgid->dnodeId);
  if (pDnode == NULL) return -1;
  action.epSet = mndGetDnodeEpset(pDnode);
  mndReleaseDnode(pMnode, pDnode);

  int32_t contLen = 0;
  void   *pReq = mndBuildCreateVnodeReq(pMnode, pDnode, pDb, pVgroup, &contLen, standby);
  if (pReq == NULL) return -1;

  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_CREATE_VNODE;
  action.acceptableCode = TSDB_CODE_NODE_ALREADY_DEPLOYED;

  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }

  return 0;
}

int32_t mndAddAlterVnodeConfirmAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup) {
  STransAction action = {0};
  action.epSet = mndGetVgroupEpset(pMnode, pVgroup);

  int32_t   contLen = sizeof(SMsgHead);
  SMsgHead *pHead = taosMemoryMalloc(contLen);
  if (pHead == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(pVgroup->vgId);

  action.pCont = pHead;
  action.contLen = contLen;
  action.msgType = TDMT_VND_ALTER_CONFIRM;
  // incorrect redirect result will cause this erro
  action.retryCode = TSDB_CODE_VND_INVALID_VGROUP_ID;

  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(pHead);
    return -1;
  }

  return 0;
}

int32_t mndAddAlterVnodeAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup, tmsg_t msgType) {
  STransAction action = {0};
  action.epSet = mndGetVgroupEpset(pMnode, pVgroup);

  int32_t contLen = 0;
  void   *pReq = mndBuildAlterVnodeReq(pMnode, pDb, pVgroup, &contLen);
  if (pReq == NULL) return -1;

  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = msgType;

  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }

  return 0;
}

static int32_t mndAddSetVnodeStandByAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup,
                                           SVnodeGid *pVgid, bool isRedo) {
  STransAction action = {0};

  SDnodeObj *pDnode = mndAcquireDnode(pMnode, pVgid->dnodeId);
  if (pDnode == NULL) return -1;
  action.epSet = mndGetDnodeEpset(pDnode);
  mndReleaseDnode(pMnode, pDnode);

  int32_t contLen = 0;
  void   *pReq = mndBuildSetVnodeStandbyReq(pMnode, pDnode, pDb, pVgroup, &contLen);
  if (pReq == NULL) return -1;

  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_SYNC_SET_VNODE_STANDBY;
  action.acceptableCode = TSDB_CODE_NODE_NOT_DEPLOYED;
  // Keep retrying until the target vnode is not the leader
  action.retryCode = TSDB_CODE_SYN_IS_LEADER;

  if (isRedo) {
    if (mndTransAppendRedoAction(pTrans, &action) != 0) {
      taosMemoryFree(pReq);
      return -1;
    }
  } else {
    if (mndTransAppendUndoAction(pTrans, &action) != 0) {
      taosMemoryFree(pReq);
      return -1;
    }
  }

  return 0;
}

int32_t mndAddDropVnodeAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup, SVnodeGid *pVgid,
                              bool isRedo) {
  STransAction action = {0};

  SDnodeObj *pDnode = mndAcquireDnode(pMnode, pVgid->dnodeId);
  if (pDnode == NULL) return -1;
  action.epSet = mndGetDnodeEpset(pDnode);
  mndReleaseDnode(pMnode, pDnode);

  int32_t contLen = 0;
  void   *pReq = mndBuildDropVnodeReq(pMnode, pDnode, pDb, pVgroup, &contLen);
  if (pReq == NULL) return -1;

  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_DROP_VNODE;
  action.acceptableCode = TSDB_CODE_NODE_NOT_DEPLOYED;

  if (isRedo) {
    if (mndTransAppendRedoAction(pTrans, &action) != 0) {
      taosMemoryFree(pReq);
      return -1;
    }
  } else {
    if (mndTransAppendUndoAction(pTrans, &action) != 0) {
      taosMemoryFree(pReq);
      return -1;
    }
  }

  return 0;
}

int32_t mndSetMoveVgroupInfoToTrans(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup, int32_t vnIndex,
                                    SArray *pArray) {
  SVgObj newVg = {0};
  memcpy(&newVg, pVgroup, sizeof(SVgObj));

  mInfo("vgId:%d, vgroup info before move, replica:%d", newVg.vgId, newVg.replica);
  for (int32_t i = 0; i < newVg.replica; ++i) {
    mInfo("vgId:%d, vnode:%d dnode:%d", newVg.vgId, i, newVg.vnodeGid[i].dnodeId);
  }

  mInfo("vgId:%d, will add 1 vnodes", pVgroup->vgId);
  if (mndAddVnodeToVgroup(pMnode, &newVg, pArray) != 0) return -1;
  if (mndAddCreateVnodeAction(pMnode, pTrans, pDb, &newVg, &newVg.vnodeGid[newVg.replica - 1], true) != 0) return -1;
  if (mndAddAlterVnodeAction(pMnode, pTrans, pDb, &newVg, TDMT_VND_ALTER_REPLICA) != 0) return -1;
  if (mndAddAlterVnodeConfirmAction(pMnode, pTrans, pDb, &newVg) != 0) return -1;

  mInfo("vgId:%d, will remove 1 vnodes", pVgroup->vgId);
  newVg.replica--;
  SVnodeGid del = newVg.vnodeGid[vnIndex];
  newVg.vnodeGid[vnIndex] = newVg.vnodeGid[newVg.replica];
  memset(&newVg.vnodeGid[newVg.replica], 0, sizeof(SVnodeGid));
  if (mndAddSetVnodeStandByAction(pMnode, pTrans, pDb, pVgroup, &del, true) != 0) return -1;
  if (mndAddAlterVnodeAction(pMnode, pTrans, pDb, &newVg, TDMT_VND_ALTER_REPLICA) != 0) return -1;
  if (mndAddDropVnodeAction(pMnode, pTrans, pDb, &newVg, &del, true) != 0) return -1;
  if (mndAddAlterVnodeConfirmAction(pMnode, pTrans, pDb, &newVg) != 0) return -1;

  {
    SSdbRaw *pRaw = mndVgroupActionEncode(&newVg);
    if (pRaw == NULL || mndTransAppendRedolog(pTrans, pRaw) != 0) return -1;
    sdbSetRawStatus(pRaw, SDB_STATUS_READY);
    pRaw = NULL;
  }

  {
    SSdbRaw *pRaw = mndVgroupActionEncode(&newVg);
    if (pRaw == NULL || mndTransAppendCommitlog(pTrans, pRaw) != 0) return -1;
    sdbSetRawStatus(pRaw, SDB_STATUS_READY);
    pRaw = NULL;
  }

  mInfo("vgId:%d, vgroup info after move, replica:%d", newVg.vgId, newVg.replica);
  for (int32_t i = 0; i < newVg.replica; ++i) {
    mInfo("vgId:%d, vnode:%d dnode:%d", newVg.vgId, i, newVg.vnodeGid[i].dnodeId);
  }
  return 0;
}

int32_t mndSetMoveVgroupsInfoToTrans(SMnode *pMnode, STrans *pTrans, int32_t delDnodeId) {
  int32_t code = 0;
  SArray *pArray = mndBuildDnodesArray(pMnode, delDnodeId);
  if (pArray == NULL) return -1;

  void *pIter = NULL;
  while (1) {
    SVgObj *pVgroup = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    int32_t vnIndex = -1;
    for (int32_t i = 0; i < pVgroup->replica; ++i) {
      if (pVgroup->vnodeGid[i].dnodeId == delDnodeId) {
        vnIndex = i;
        break;
      }
    }

    code = 0;
    if (vnIndex != -1) {
      mInfo("vgId:%d, vnode:%d will be removed from dnode:%d", pVgroup->vgId, vnIndex, delDnodeId);
      SDbObj *pDb = mndAcquireDb(pMnode, pVgroup->dbName);
      code = mndSetMoveVgroupInfoToTrans(pMnode, pTrans, pDb, pVgroup, vnIndex, pArray);
      mndReleaseDb(pMnode, pDb);
    }

    sdbRelease(pMnode->pSdb, pVgroup);

    if (code != 0) {
      sdbCancelFetch(pMnode->pSdb, pIter);
      break;
    }
  }

  taosArrayDestroy(pArray);
  return code;
}

static int32_t mndAddIncVgroupReplicaToTrans(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup,
                                             int32_t newDnodeId) {
  mDebug("vgId:%d, will add 1 vnode, replica:%d dnode:%d", pVgroup->vgId, pVgroup->replica, newDnodeId);

  SVnodeGid *pGid = &pVgroup->vnodeGid[pVgroup->replica];
  pVgroup->replica++;
  pGid->dnodeId = newDnodeId;
  pGid->role = TAOS_SYNC_STATE_ERROR;

  if (mndAddCreateVnodeAction(pMnode, pTrans, pDb, pVgroup, pGid, true) != 0) return -1;
  if (mndAddAlterVnodeAction(pMnode, pTrans, pDb, pVgroup, TDMT_VND_ALTER_REPLICA) != 0) return -1;
  if (mndAddAlterVnodeConfirmAction(pMnode, pTrans, pDb, pVgroup) != 0) return -1;

  return 0;
}

static int32_t mndAddDecVgroupReplicaFromTrans(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup,
                                               int32_t delDnodeId) {
  mDebug("vgId:%d, will remove 1 vnode, replica:%d dnode:%d", pVgroup->vgId, pVgroup->replica, delDnodeId);

  SVnodeGid *pGid = NULL;
  SVnodeGid  delGid = {0};
  for (int32_t i = 0; i < pVgroup->replica; ++i) {
    if (pVgroup->vnodeGid[i].dnodeId == delDnodeId) {
      pGid = &pVgroup->vnodeGid[i];
      break;
    }
  }

  if (pGid == NULL) return 0;

  pVgroup->replica--;
  memcpy(&delGid, pGid, sizeof(SVnodeGid));
  memcpy(pGid, &pVgroup->vnodeGid[pVgroup->replica], sizeof(SVnodeGid));
  memset(&pVgroup->vnodeGid[pVgroup->replica], 0, sizeof(SVnodeGid));

  if (mndAddSetVnodeStandByAction(pMnode, pTrans, pDb, pVgroup, &delGid, true) != 0) return -1;
  if (mndAddAlterVnodeAction(pMnode, pTrans, pDb, pVgroup, TDMT_VND_ALTER_REPLICA) != 0) return -1;
  if (mndAddDropVnodeAction(pMnode, pTrans, pDb, pVgroup, &delGid, true) != 0) return -1;
  if (mndAddAlterVnodeConfirmAction(pMnode, pTrans, pDb, pVgroup) != 0) return -1;

  return 0;
}

static int32_t mndRedistributeVgroup(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb, SVgObj *pVgroup, SDnodeObj *pNew1,
                                     SDnodeObj *pOld1, SDnodeObj *pNew2, SDnodeObj *pOld2, SDnodeObj *pNew3,
                                     SDnodeObj *pOld3) {
  int32_t  code = -1;
  SSdbRaw *pRaw = NULL;
  STrans  *pTrans = NULL;

  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_GLOBAL, pReq);
  if (pTrans == NULL) goto _OVER;
  mndTransSetSerial(pTrans);
  mDebug("trans:%d, used to redistribute vgroup, vgId:%d", pTrans->id, pVgroup->vgId);

  SVgObj newVg = {0};
  memcpy(&newVg, pVgroup, sizeof(SVgObj));
  mInfo("vgId:%d, vgroup info before redistribute, replica:%d", newVg.vgId, newVg.replica);
  for (int32_t i = 0; i < newVg.replica; ++i) {
    mInfo("vgId:%d, vnode:%d dnode:%d role:%s", newVg.vgId, i, newVg.vnodeGid[i].dnodeId,
          syncStr(newVg.vnodeGid[i].role));
  }

  if (pNew1 != NULL && pOld1 != NULL) {
    int32_t numOfVnodes = mndGetVnodesNum(pMnode, pNew1->id);
    if (numOfVnodes >= pNew1->numOfSupportVnodes) {
      mError("vgId:%d, no enough vnodes in dnode:%d, numOfVnodes:%d support:%d", newVg.vgId, pNew1->id, numOfVnodes,
             pNew1->numOfSupportVnodes);
      terrno = TSDB_CODE_MND_NO_ENOUGH_DNODES;
      goto _OVER;
    }

    int64_t vgMem = mndGetVgroupMemory(pMnode, NULL, pVgroup);
    if (pNew1->memAvail - vgMem - pNew1->memUsed <= 0) {
      mError("db:%s, vgId:%d, no enough memory:%" PRId64 " in dnode:%d avail:%" PRId64 " used:%" PRId64,
             pVgroup->dbName, pVgroup->vgId, vgMem, pNew1->id, pNew1->memAvail, pNew1->memUsed);
      terrno = TSDB_CODE_MND_NO_ENOUGH_MEM_IN_DNODE;
      return -1;
    } else {
      pNew1->memUsed += vgMem;
    }

    if (mndAddIncVgroupReplicaToTrans(pMnode, pTrans, pDb, &newVg, pNew1->id) != 0) goto _OVER;
    if (mndAddDecVgroupReplicaFromTrans(pMnode, pTrans, pDb, &newVg, pOld1->id) != 0) goto _OVER;
  }

  if (pNew2 != NULL && pOld2 != NULL) {
    int32_t numOfVnodes = mndGetVnodesNum(pMnode, pNew2->id);
    if (numOfVnodes >= pNew2->numOfSupportVnodes) {
      mError("vgId:%d, no enough vnodes in dnode:%d, numOfVnodes:%d support:%d", newVg.vgId, pNew2->id, numOfVnodes,
             pNew2->numOfSupportVnodes);
      terrno = TSDB_CODE_MND_NO_ENOUGH_DNODES;
      goto _OVER;
    }
    int64_t vgMem = mndGetVgroupMemory(pMnode, NULL, pVgroup);
    if (pNew2->memAvail - vgMem - pNew2->memUsed <= 0) {
      mError("db:%s, vgId:%d, no enough memory:%" PRId64 " in dnode:%d avail:%" PRId64 " used:%" PRId64,
             pVgroup->dbName, pVgroup->vgId, vgMem, pNew2->id, pNew2->memAvail, pNew2->memUsed);
      terrno = TSDB_CODE_MND_NO_ENOUGH_MEM_IN_DNODE;
      return -1;
    } else {
      pNew2->memUsed += vgMem;
    }
    if (mndAddIncVgroupReplicaToTrans(pMnode, pTrans, pDb, &newVg, pNew2->id) != 0) goto _OVER;
    if (mndAddDecVgroupReplicaFromTrans(pMnode, pTrans, pDb, &newVg, pOld2->id) != 0) goto _OVER;
  }

  if (pNew3 != NULL && pOld3 != NULL) {
    int32_t numOfVnodes = mndGetVnodesNum(pMnode, pNew3->id);
    if (numOfVnodes >= pNew3->numOfSupportVnodes) {
      mError("vgId:%d, no enough vnodes in dnode:%d, numOfVnodes:%d support:%d", newVg.vgId, pNew3->id, numOfVnodes,
             pNew3->numOfSupportVnodes);
      terrno = TSDB_CODE_MND_NO_ENOUGH_DNODES;
      goto _OVER;
    }
    int64_t vgMem = mndGetVgroupMemory(pMnode, NULL, pVgroup);
    if (pNew3->memAvail - vgMem - pNew3->memUsed <= 0) {
      mError("db:%s, vgId:%d, no enough memory:%" PRId64 " in dnode:%d avail:%" PRId64 " used:%" PRId64,
             pVgroup->dbName, pVgroup->vgId, vgMem, pNew3->id, pNew3->memAvail, pNew3->memUsed);
      terrno = TSDB_CODE_MND_NO_ENOUGH_MEM_IN_DNODE;
      return -1;
    } else {
      pNew3->memUsed += vgMem;
    }
    if (mndAddIncVgroupReplicaToTrans(pMnode, pTrans, pDb, &newVg, pNew3->id) != 0) goto _OVER;
    if (mndAddDecVgroupReplicaFromTrans(pMnode, pTrans, pDb, &newVg, pOld3->id) != 0) goto _OVER;
  }

  {
    pRaw = mndVgroupActionEncode(&newVg);
    if (pRaw == NULL || mndTransAppendRedolog(pTrans, pRaw) != 0) goto _OVER;
    sdbSetRawStatus(pRaw, SDB_STATUS_READY);
    pRaw = NULL;
  }

  {
    pRaw = mndVgroupActionEncode(&newVg);
    if (pRaw == NULL || mndTransAppendCommitlog(pTrans, pRaw) != 0) goto _OVER;
    sdbSetRawStatus(pRaw, SDB_STATUS_READY);
    pRaw = NULL;
  }

  mInfo("vgId:%d, vgroup info after redistribute, replica:%d", newVg.vgId, newVg.replica);
  for (int32_t i = 0; i < newVg.replica; ++i) {
    mInfo("vgId:%d, vnode:%d dnode:%d", newVg.vgId, i, newVg.vnodeGid[i].dnodeId);
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;
  code = 0;

_OVER:
  mndTransDrop(pTrans);
  sdbFreeRaw(pRaw);
  mndReleaseDb(pMnode, pDb);
  return code;
}

static int32_t mndProcessRedistributeVgroupMsg(SRpcMsg *pReq) {
#if 1
  return TSDB_CODE_OPS_NOT_SUPPORT;
#else
  SMnode    *pMnode = pReq->info.node;
  SDnodeObj *pNew1 = NULL;
  SDnodeObj *pNew2 = NULL;
  SDnodeObj *pNew3 = NULL;
  SDnodeObj *pOld1 = NULL;
  SDnodeObj *pOld2 = NULL;
  SDnodeObj *pOld3 = NULL;
  SVgObj    *pVgroup = NULL;
  SDbObj    *pDb = NULL;
  int32_t    code = -1;
  int64_t    curMs = taosGetTimestampMs();
  int32_t    newDnodeId[3] = {0};
  int32_t    oldDnodeId[3] = {0};
  int32_t    newIndex = -1;
  int32_t    oldIndex = -1;

  SRedistributeVgroupReq req = {0};
  if (tDeserializeSRedistributeVgroupReq(pReq->pCont, pReq->contLen, &req) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("vgId:%d, start to redistribute vgroup to dnode %d:%d:%d", req.vgId, req.dnodeId1, req.dnodeId2, req.dnodeId3);
  if (mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_REDISTRIBUTE_VGROUP) != 0) {
    goto _OVER;
  }

  pVgroup = mndAcquireVgroup(pMnode, req.vgId);
  if (pVgroup == NULL) goto _OVER;

  pDb = mndAcquireDb(pMnode, pVgroup->dbName);
  if (pDb == NULL) goto _OVER;

  if (pVgroup->replica == 1) {
    if (req.dnodeId1 <= 0 || req.dnodeId2 > 0 || req.dnodeId3 > 0) {
      terrno = TSDB_CODE_MND_INVALID_REPLICA;
      goto _OVER;
    }

    if (req.dnodeId1 == pVgroup->vnodeGid[0].dnodeId) {
      // terrno = TSDB_CODE_MND_VGROUP_UN_CHANGED;
      code = 0;
      goto _OVER;
    }

    pNew1 = mndAcquireDnode(pMnode, req.dnodeId1);
    if (pNew1 == NULL) goto _OVER;
    if (!mndIsDnodeOnline(pNew1, curMs)) {
      terrno = TSDB_CODE_MND_HAS_OFFLINE_DNODE;
      goto _OVER;
    }

    pOld1 = mndAcquireDnode(pMnode, pVgroup->vnodeGid[0].dnodeId);
    if (pOld1 == NULL) goto _OVER;
    if (!mndIsDnodeOnline(pOld1, curMs)) {
      terrno = TSDB_CODE_MND_HAS_OFFLINE_DNODE;
      goto _OVER;
    }

    code = mndRedistributeVgroup(pMnode, pReq, pDb, pVgroup, pNew1, pOld1, NULL, NULL, NULL, NULL);

  } else if (pVgroup->replica == 3) {
    if (req.dnodeId1 <= 0 || req.dnodeId2 <= 0 || req.dnodeId3 <= 0) {
      terrno = TSDB_CODE_MND_INVALID_REPLICA;
      goto _OVER;
    }

    if (req.dnodeId1 == req.dnodeId2 || req.dnodeId1 == req.dnodeId3 || req.dnodeId2 == req.dnodeId3) {
      terrno = TSDB_CODE_MND_INVALID_REPLICA;
      goto _OVER;
    }

    if (req.dnodeId1 != pVgroup->vnodeGid[0].dnodeId && req.dnodeId1 != pVgroup->vnodeGid[1].dnodeId &&
        req.dnodeId1 != pVgroup->vnodeGid[2].dnodeId) {
      newDnodeId[++newIndex] = req.dnodeId1;
      mInfo("vgId:%d, dnode:%d will be added, index:%d", pVgroup->vgId, newDnodeId[newIndex], newIndex);
    }

    if (req.dnodeId2 != pVgroup->vnodeGid[0].dnodeId && req.dnodeId2 != pVgroup->vnodeGid[1].dnodeId &&
        req.dnodeId2 != pVgroup->vnodeGid[2].dnodeId) {
      newDnodeId[++newIndex] = req.dnodeId2;
      mInfo("vgId:%d, dnode:%d will be added, index:%d", pVgroup->vgId, newDnodeId[newIndex], newIndex);
    }

    if (req.dnodeId3 != pVgroup->vnodeGid[0].dnodeId && req.dnodeId3 != pVgroup->vnodeGid[1].dnodeId &&
        req.dnodeId3 != pVgroup->vnodeGid[2].dnodeId) {
      newDnodeId[++newIndex] = req.dnodeId3;
      mInfo("vgId:%d, dnode:%d will be added, index:%d", pVgroup->vgId, newDnodeId[newIndex], newIndex);
    }

    if (req.dnodeId1 != pVgroup->vnodeGid[0].dnodeId && req.dnodeId2 != pVgroup->vnodeGid[0].dnodeId &&
        req.dnodeId3 != pVgroup->vnodeGid[0].dnodeId) {
      oldDnodeId[++oldIndex] = pVgroup->vnodeGid[0].dnodeId;
      mInfo("vgId:%d, dnode:%d will be removed, index:%d", pVgroup->vgId, oldDnodeId[oldIndex], oldIndex);
    }

    if (req.dnodeId1 != pVgroup->vnodeGid[1].dnodeId && req.dnodeId2 != pVgroup->vnodeGid[1].dnodeId &&
        req.dnodeId3 != pVgroup->vnodeGid[1].dnodeId) {
      oldDnodeId[++oldIndex] = pVgroup->vnodeGid[1].dnodeId;
      mInfo("vgId:%d, dnode:%d will be removed, index:%d", pVgroup->vgId, oldDnodeId[oldIndex], oldIndex);
    }

    if (req.dnodeId1 != pVgroup->vnodeGid[2].dnodeId && req.dnodeId2 != pVgroup->vnodeGid[2].dnodeId &&
        req.dnodeId3 != pVgroup->vnodeGid[2].dnodeId) {
      oldDnodeId[++oldIndex] = pVgroup->vnodeGid[2].dnodeId;
      mInfo("vgId:%d, dnode:%d will be removed, index:%d", pVgroup->vgId, oldDnodeId[oldIndex], oldIndex);
    }

    if (newDnodeId[0] != 0) {
      pNew1 = mndAcquireDnode(pMnode, newDnodeId[0]);
      if (pNew1 == NULL) goto _OVER;
      if (!mndIsDnodeOnline(pNew1, curMs)) {
        terrno = TSDB_CODE_MND_HAS_OFFLINE_DNODE;
        goto _OVER;
      }
    }

    if (newDnodeId[1] != 0) {
      pNew2 = mndAcquireDnode(pMnode, newDnodeId[1]);
      if (pNew2 == NULL) goto _OVER;
      if (!mndIsDnodeOnline(pNew2, curMs)) {
        terrno = TSDB_CODE_MND_HAS_OFFLINE_DNODE;
        goto _OVER;
      }
    }

    if (newDnodeId[2] != 0) {
      pNew3 = mndAcquireDnode(pMnode, newDnodeId[2]);
      if (pNew3 == NULL) goto _OVER;
      if (!mndIsDnodeOnline(pNew3, curMs)) {
        terrno = TSDB_CODE_MND_HAS_OFFLINE_DNODE;
        goto _OVER;
      }
    }

    if (oldDnodeId[0] != 0) {
      pOld1 = mndAcquireDnode(pMnode, oldDnodeId[0]);
      if (pOld1 == NULL) goto _OVER;
      if (!mndIsDnodeOnline(pOld1, curMs)) {
        terrno = TSDB_CODE_MND_HAS_OFFLINE_DNODE;
        goto _OVER;
      }
    }

    if (oldDnodeId[1] != 0) {
      pOld2 = mndAcquireDnode(pMnode, oldDnodeId[1]);
      if (pOld2 == NULL) goto _OVER;
      if (!mndIsDnodeOnline(pOld2, curMs)) {
        terrno = TSDB_CODE_MND_HAS_OFFLINE_DNODE;
        goto _OVER;
      }
    }

    if (oldDnodeId[2] != 0) {
      pOld3 = mndAcquireDnode(pMnode, oldDnodeId[2]);
      if (pOld3 == NULL) goto _OVER;
      if (!mndIsDnodeOnline(pOld3, curMs)) {
        terrno = TSDB_CODE_MND_HAS_OFFLINE_DNODE;
        goto _OVER;
      }
    }

    if (pNew1 == NULL && pOld1 == NULL && pNew2 == NULL && pOld2 == NULL && pNew3 == NULL && pOld3 == NULL) {
      // terrno = TSDB_CODE_MND_VGROUP_UN_CHANGED;
      code = 0;
      goto _OVER;
    }

    code = mndRedistributeVgroup(pMnode, pReq, pDb, pVgroup, pNew1, pOld1, pNew2, pOld2, pNew3, pOld3);

  } else {
    terrno = TSDB_CODE_MND_INVALID_REPLICA;
    goto _OVER;
  }

  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("vgId:%d, failed to redistribute to dnode %d:%d:%d since %s", req.vgId, req.dnodeId1, req.dnodeId2,
           req.dnodeId3, terrstr());
  }

  mndReleaseDnode(pMnode, pNew1);
  mndReleaseDnode(pMnode, pNew2);
  mndReleaseDnode(pMnode, pNew3);
  mndReleaseDnode(pMnode, pOld1);
  mndReleaseDnode(pMnode, pOld2);
  mndReleaseDnode(pMnode, pOld3);
  mndReleaseVgroup(pMnode, pVgroup);
  mndReleaseDb(pMnode, pDb);

  return code;
#endif
}

int32_t mndBuildAlterVgroupAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup, SArray *pArray) {
  if (pVgroup->replica <= 0 || pVgroup->replica == pDb->cfg.replications) {
    if (mndAddAlterVnodeAction(pMnode, pTrans, pDb, pVgroup, TDMT_VND_ALTER_CONFIG) != 0) {
      return -1;
    }
  } else {
    SVgObj newVgroup = {0};
    memcpy(&newVgroup, pVgroup, sizeof(SVgObj));
    mndTransSetSerial(pTrans);

    if (newVgroup.replica < pDb->cfg.replications) {
      mInfo("db:%s, vgId:%d, vn:0 dnode:%d, will add 2 vnodes", pVgroup->dbName, pVgroup->vgId,
            pVgroup->vnodeGid[0].dnodeId);

      if (mndAddVnodeToVgroup(pMnode, &newVgroup, pArray) != 0) return -1;
      if (mndAddCreateVnodeAction(pMnode, pTrans, pDb, &newVgroup, &newVgroup.vnodeGid[1], true) != 0) return -1;
      if (mndAddAlterVnodeAction(pMnode, pTrans, pDb, &newVgroup, TDMT_VND_ALTER_REPLICA) != 0) return -1;
      if (mndAddAlterVnodeConfirmAction(pMnode, pTrans, pDb, &newVgroup) != 0) return -1;

      if (mndAddVnodeToVgroup(pMnode, &newVgroup, pArray) != 0) return -1;
      if (mndAddCreateVnodeAction(pMnode, pTrans, pDb, &newVgroup, &newVgroup.vnodeGid[2], true) != 0) return -1;
      if (mndAddAlterVnodeAction(pMnode, pTrans, pDb, &newVgroup, TDMT_VND_ALTER_REPLICA) != 0) return -1;
      if (mndAddAlterVnodeConfirmAction(pMnode, pTrans, pDb, &newVgroup) != 0) return -1;
    } else if (newVgroup.replica > pDb->cfg.replications) {
      mInfo("db:%s, vgId:%d, will remove 2 vnodes", pVgroup->dbName, pVgroup->vgId);

      SVnodeGid del1 = {0};
      if (mndRemoveVnodeFromVgroup(pMnode, &newVgroup, pArray, &del1) != 0) return -1;
      if (mndAddSetVnodeStandByAction(pMnode, pTrans, pDb, pVgroup, &del1, true) != 0) return -1;
      if (mndAddAlterVnodeAction(pMnode, pTrans, pDb, &newVgroup, TDMT_VND_ALTER_REPLICA) != 0) return -1;
      if (mndAddDropVnodeAction(pMnode, pTrans, pDb, &newVgroup, &del1, true) != 0) return -1;
      if (mndAddAlterVnodeConfirmAction(pMnode, pTrans, pDb, &newVgroup) != 0) return -1;

      SVnodeGid del2 = {0};
      if (mndRemoveVnodeFromVgroup(pMnode, &newVgroup, pArray, &del2) != 0) return -1;
      if (mndAddSetVnodeStandByAction(pMnode, pTrans, pDb, pVgroup, &del2, true) != 0) return -1;
      if (mndAddAlterVnodeAction(pMnode, pTrans, pDb, &newVgroup, TDMT_VND_ALTER_REPLICA) != 0) return -1;
      if (mndAddDropVnodeAction(pMnode, pTrans, pDb, &newVgroup, &del2, true) != 0) return -1;
      if (mndAddAlterVnodeConfirmAction(pMnode, pTrans, pDb, &newVgroup) != 0) return -1;
    } else {
    }

    {
      SSdbRaw *pVgRaw = mndVgroupActionEncode(&newVgroup);
      if (pVgRaw == NULL) return -1;
      if (mndTransAppendRedolog(pTrans, pVgRaw) != 0) {
        sdbFreeRaw(pVgRaw);
        return -1;
      }
      sdbSetRawStatus(pVgRaw, SDB_STATUS_READY);
    }

    {
      SSdbRaw *pVgRaw = mndVgroupActionEncode(&newVgroup);
      if (pVgRaw == NULL) return -1;
      if (mndTransAppendCommitlog(pTrans, pVgRaw) != 0) {
        sdbFreeRaw(pVgRaw);
        return -1;
      }
      sdbSetRawStatus(pVgRaw, SDB_STATUS_READY);
    }
  }

  return 0;
}

static int32_t mndAddAdjustVnodeHashRangeAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup) {
  return 0;
}

static int32_t mndSplitVgroup(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb, SVgObj *pVgroup) {
  int32_t  code = -1;
  SSdbRaw *pRaw = NULL;
  STrans  *pTrans = NULL;
  SArray  *pArray = mndBuildDnodesArray(pMnode, 0);

  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_GLOBAL, pReq);
  if (pTrans == NULL) goto _OVER;
  mndTransSetSerial(pTrans);
  mDebug("trans:%d, used to split vgroup, vgId:%d", pTrans->id, pVgroup->vgId);

  SVgObj newVg1 = {0};
  memcpy(&newVg1, pVgroup, sizeof(SVgObj));
  mInfo("vgId:%d, vgroup info before split, replica:%d hashBegin:%u hashEnd:%u", newVg1.vgId, newVg1.replica,
        newVg1.hashBegin, newVg1.hashEnd);
  for (int32_t i = 0; i < newVg1.replica; ++i) {
    mInfo("vgId:%d, vnode:%d dnode:%d", newVg1.vgId, i, newVg1.vnodeGid[i].dnodeId);
  }

  if (newVg1.replica == 1) {
    if (mndAddVnodeToVgroup(pMnode, &newVg1, pArray) != 0) goto _OVER;
    if (mndAddCreateVnodeAction(pMnode, pTrans, pDb, &newVg1, &newVg1.vnodeGid[1], true) != 0) goto _OVER;
    if (mndAddAlterVnodeAction(pMnode, pTrans, pDb, &newVg1, TDMT_VND_ALTER_REPLICA) != 0) goto _OVER;
    if (mndAddAlterVnodeConfirmAction(pMnode, pTrans, pDb, &newVg1) != 0) goto _OVER;
  } else if (newVg1.replica == 3) {
    SVnodeGid del1 = {0};
    if (mndRemoveVnodeFromVgroup(pMnode, &newVg1, pArray, &del1) != 0) goto _OVER;
    if (mndAddSetVnodeStandByAction(pMnode, pTrans, pDb, pVgroup, &del1, true) != 0) return -1;
    if (mndAddAlterVnodeAction(pMnode, pTrans, pDb, &newVg1, TDMT_VND_ALTER_REPLICA) != 0) goto _OVER;
    if (mndAddDropVnodeAction(pMnode, pTrans, pDb, &newVg1, &del1, true) != 0) goto _OVER;
    if (mndAddAlterVnodeConfirmAction(pMnode, pTrans, pDb, &newVg1) != 0) goto _OVER;
  } else {
    goto _OVER;
  }

  SVgObj newVg2 = {0};
  memcpy(&newVg1, &newVg2, sizeof(SVgObj));
  newVg1.replica = 1;
  newVg1.hashEnd = (newVg1.hashBegin + newVg1.hashEnd) / 2;
  memset(&newVg1.vnodeGid[1], 0, sizeof(SVnodeGid));

  newVg2.replica = 1;
  newVg2.hashBegin = newVg1.hashEnd + 1;
  memcpy(&newVg2.vnodeGid[0], &newVg2.vnodeGid[1], sizeof(SVnodeGid));
  memset(&newVg1.vnodeGid[1], 0, sizeof(SVnodeGid));

  if (mndAddAlterVnodeAction(pMnode, pTrans, pDb, &newVg1, TDMT_VND_ALTER_HASHRANGE) != 0) goto _OVER;
  if (mndAddAlterVnodeAction(pMnode, pTrans, pDb, &newVg2, TDMT_VND_ALTER_HASHRANGE) != 0) goto _OVER;

  // adjust vgroup
  if (mndBuildAlterVgroupAction(pMnode, pTrans, pDb, &newVg1, pArray) != 0) goto _OVER;
  if (mndBuildAlterVgroupAction(pMnode, pTrans, pDb, &newVg2, pArray) != 0) goto _OVER;

_OVER:
  mndTransDrop(pTrans);
  sdbFreeRaw(pRaw);
  return code;
}

static int32_t mndProcessSplitVgroupMsg(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;
  int32_t code = -1;
  int32_t vgId = 2;
  SVgObj *pVgroup = NULL;
  SDbObj *pDb = NULL;

  mDebug("vgId:%d, start to split", vgId);
  if (mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_SPLIT_VGROUP) != 0) {
    goto _OVER;
  }

  pVgroup = mndAcquireVgroup(pMnode, vgId);
  if (pVgroup == NULL) goto _OVER;

  pDb = mndAcquireDb(pMnode, pVgroup->dbName);
  if (pDb == NULL) goto _OVER;

  code = mndSplitVgroup(pMnode, pReq, pDb, pVgroup);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  mndReleaseVgroup(pMnode, pVgroup);
  mndReleaseDb(pMnode, pDb);
  return code;
}

static int32_t mndSetBalanceVgroupInfoToTrans(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup,
                                              SDnodeObj *pSrc, SDnodeObj *pDst) {
  SVgObj newVg = {0};
  memcpy(&newVg, pVgroup, sizeof(SVgObj));
  mInfo("vgId:%d, vgroup info before balance, replica:%d", newVg.vgId, newVg.replica);
  for (int32_t i = 0; i < newVg.replica; ++i) {
    mInfo("vgId:%d, vnode:%d dnode:%d", newVg.vgId, i, newVg.vnodeGid[i].dnodeId);
  }

  if (mndAddIncVgroupReplicaToTrans(pMnode, pTrans, pDb, &newVg, pDst->id) != 0) return -1;
  if (mndAddDecVgroupReplicaFromTrans(pMnode, pTrans, pDb, &newVg, pSrc->id) != 0) return -1;

  {
    SSdbRaw *pRaw = mndVgroupActionEncode(&newVg);
    if (pRaw == NULL || mndTransAppendRedolog(pTrans, pRaw) != 0) {
      sdbFreeRaw(pRaw);
      return -1;
    }
    sdbSetRawStatus(pRaw, SDB_STATUS_READY);
  }

  {
    SSdbRaw *pRaw = mndVgroupActionEncode(&newVg);
    if (pRaw == NULL || mndTransAppendCommitlog(pTrans, pRaw) != 0) {
      sdbFreeRaw(pRaw);
      return -1;
    }
    sdbSetRawStatus(pRaw, SDB_STATUS_READY);
  }

  mInfo("vgId:%d, vgroup info after balance, replica:%d", newVg.vgId, newVg.replica);
  for (int32_t i = 0; i < newVg.replica; ++i) {
    mInfo("vgId:%d, vnode:%d dnode:%d", newVg.vgId, i, newVg.vnodeGid[i].dnodeId);
  }
  return 0;
}

static int32_t mndBalanceVgroupBetweenDnode(SMnode *pMnode, STrans *pTrans, SDnodeObj *pSrc, SDnodeObj *pDst,
                                            SHashObj *pBalancedVgroups) {
  void   *pIter = NULL;
  int32_t code = -1;
  SSdb   *pSdb = pMnode->pSdb;

  while (1) {
    SVgObj *pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;
    if (taosHashGet(pBalancedVgroups, &pVgroup->vgId, sizeof(int32_t)) != NULL) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    bool existInSrc = false;
    bool existInDst = false;
    for (int32_t i = 0; i < pVgroup->replica; ++i) {
      SVnodeGid *pGid = &pVgroup->vnodeGid[i];
      if (pGid->dnodeId == pSrc->id) existInSrc = true;
      if (pGid->dnodeId == pDst->id) existInDst = true;
    }

    if (!existInSrc || existInDst) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    SDbObj *pDb = mndAcquireDb(pMnode, pVgroup->dbName);
    code = mndSetBalanceVgroupInfoToTrans(pMnode, pTrans, pDb, pVgroup, pSrc, pDst);
    if (code == 0) {
      code = taosHashPut(pBalancedVgroups, &pVgroup->vgId, sizeof(int32_t), &pVgroup->vgId, sizeof(int32_t));
    }
    mndReleaseDb(pMnode, pDb);
    sdbRelease(pSdb, pVgroup);
    sdbCancelFetch(pSdb, pIter);
    break;
  }

  return code;
}

static int32_t mndBalanceVgroup(SMnode *pMnode, SRpcMsg *pReq, SArray *pArray) {
  int32_t   code = -1;
  int32_t   numOfVgroups = 0;
  STrans   *pTrans = NULL;
  SHashObj *pBalancedVgroups = NULL;

  pBalancedVgroups = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (pBalancedVgroups == NULL) goto _OVER;

  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_GLOBAL, pReq);
  if (pTrans == NULL) goto _OVER;
  mndTransSetSerial(pTrans);
  mDebug("trans:%d, used to balance vgroup", pTrans->id);

  while (1) {
    taosArraySort(pArray, (__compar_fn_t)mndCompareDnodeVnodes);
    for (int32_t i = 0; i < taosArrayGetSize(pArray); ++i) {
      SDnodeObj *pDnode = taosArrayGet(pArray, i);
      mDebug("dnode:%d, equivalent vnodes:%d support:%d, score:%f", pDnode->id, pDnode->numOfVnodes,
             pDnode->numOfSupportVnodes, (float)pDnode->numOfVnodes / pDnode->numOfSupportVnodes);
    }

    SDnodeObj *pSrc = taosArrayGet(pArray, taosArrayGetSize(pArray) - 1);
    SDnodeObj *pDst = taosArrayGet(pArray, 0);

    float srcScore = (float)(pSrc->numOfVnodes - 1) / pSrc->numOfSupportVnodes;
    float dstScore = (float)(pDst->numOfVnodes + 1) / pDst->numOfSupportVnodes;
    mDebug("trans:%d, after balance, src dnode:%d score:%f, dst dnode:%d score:%f", pTrans->id, pSrc->id, srcScore,
           pDst->id, dstScore);

    if (srcScore > dstScore - 0.000001) {
      code = mndBalanceVgroupBetweenDnode(pMnode, pTrans, pSrc, pDst, pBalancedVgroups);
      if (code == 0) {
        pSrc->numOfVnodes--;
        pDst->numOfVnodes++;
        numOfVgroups++;
        continue;
      } else {
        mDebug("trans:%d, no vgroup need to balance from dnode:%d to dnode:%d", pTrans->id, pSrc->id, pDst->id);
        break;
      }
    } else {
      mDebug("trans:%d, no vgroup need to balance any more", pTrans->id);
      break;
    }
  }

  if (numOfVgroups <= 0) {
    mDebug("no need to balance vgroup");
    code = 0;
  } else {
    mDebug("start to balance vgroup, numOfVgroups:%d", numOfVgroups);
    if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;
    code = TSDB_CODE_ACTION_IN_PROGRESS;
  }

_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessBalanceVgroupMsg(SRpcMsg *pReq) {
#if 1
  return TSDB_CODE_OPS_NOT_SUPPORT;
#else
  SMnode *pMnode = pReq->info.node;
  int32_t code = -1;
  SArray *pArray = NULL;
  void *pIter = NULL;
  int64_t curMs = taosGetTimestampMs();

  SBalanceVgroupReq req = {0};
  if (tDeserializeSBalanceVgroupReq(pReq->pCont, pReq->contLen, &req) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("start to balance vgroup");
  if (mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_BALANCE_VGROUP) != 0) {
    goto _OVER;
  }

  while (1) {
    SDnodeObj *pDnode = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_DNODE, pIter, (void **)&pDnode);
    if (pIter == NULL) break;
    if (!mndIsDnodeOnline(pDnode, curMs)) {
      terrno = TSDB_CODE_MND_HAS_OFFLINE_DNODE;
      mError("failed to balance vgroup since %s, dnode:%d", terrstr(), pDnode->id);
      sdbRelease(pMnode->pSdb, pDnode);
      goto _OVER;
    }

    sdbRelease(pMnode->pSdb, pDnode);
  }

  pArray = mndBuildDnodesArray(pMnode, 0);
  if (pArray == NULL) goto _OVER;

  if (taosArrayGetSize(pArray) < 2) {
    mDebug("no need to balance vgroup since dnode num less than 2");
    code = 0;
  } else {
    code = mndBalanceVgroup(pMnode, pReq, pArray);
  }

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("failed to balance vgroup since %s", terrstr());
  }

  taosArrayDestroy(pArray);
  return code;
#endif
}

bool mndVgroupInDb(SVgObj *pVgroup, int64_t dbUid) { return !pVgroup->isTsma && pVgroup->dbUid == dbUid; }
