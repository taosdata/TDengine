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
#include "mndShow.h"
#include "mndTrans.h"

#define VGROUP_VER_NUMBER   1
#define VGROUP_RESERVE_SIZE 64

static SSdbRow *mndVgroupActionDecode(SSdbRaw *pRaw);
static int32_t  mndVgroupActionInsert(SSdb *pSdb, SVgObj *pVgroup);
static int32_t  mndVgroupActionDelete(SSdb *pSdb, SVgObj *pVgroup);
static int32_t  mndVgroupActionUpdate(SSdb *pSdb, SVgObj *pOld, SVgObj *pNew);

static int32_t mndProcessCreateVnodeRsp(SRpcMsg *pRsp);
static int32_t mndProcessAlterVnodeRsp(SRpcMsg *pRsp);
static int32_t mndProcessDropVnodeRsp(SRpcMsg *pRsp);
static int32_t mndProcessCompactVnodeRsp(SRpcMsg *pRsp);

static int32_t mndRetrieveVgroups(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextVgroup(SMnode *pMnode, void *pIter);
static int32_t mndRetrieveVnodes(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextVnode(SMnode *pMnode, void *pIter);

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

  mndSetMsgHandle(pMnode, TDMT_DND_CREATE_VNODE_RSP, mndProcessCreateVnodeRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_ALTER_VNODE_RSP, mndProcessAlterVnodeRsp);
  mndSetMsgHandle(pMnode, TDMT_DND_DROP_VNODE_RSP, mndProcessDropVnodeRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_COMPACT_VNODE_RSP, mndProcessCompactVnodeRsp);

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

void *mndBuildCreateVnodeReq(SMnode *pMnode, SDnodeObj *pDnode, SDbObj *pDb, SVgObj *pVgroup, int32_t *pContLen) {
  SCreateVnodeReq createReq = {0};
  createReq.vgId = pVgroup->vgId;
  createReq.dnodeId = pDnode->id;
  memcpy(createReq.db, pDb->name, TSDB_DB_FNAME_LEN);
  createReq.dbUid = pDb->uid;
  createReq.vgVersion = pVgroup->version;
  createReq.numOfStables = pDb->cfg.numOfStables;
  createReq.buffer = pDb->cfg.buffer;
  createReq.pageSize = pDb->cfg.pageSize;
  createReq.pages = pDb->cfg.pages;
  createReq.daysPerFile = pDb->cfg.daysPerFile;
  createReq.daysToKeep0 = pDb->cfg.daysToKeep0;
  createReq.daysToKeep1 = pDb->cfg.daysToKeep1;
  createReq.daysToKeep2 = pDb->cfg.daysToKeep2;
  createReq.minRows = pDb->cfg.minRows;
  createReq.maxRows = pDb->cfg.maxRows;
  createReq.fsyncPeriod = pDb->cfg.fsyncPeriod;
  createReq.walLevel = pDb->cfg.walLevel;
  createReq.precision = pDb->cfg.precision;
  createReq.compression = pDb->cfg.compression;
  createReq.strict = pDb->cfg.strict;
  createReq.cacheLastRow = pDb->cfg.cacheLastRow;
  createReq.replica = pVgroup->replica;
  createReq.selfIndex = -1;
  createReq.hashBegin = pVgroup->hashBegin;
  createReq.hashEnd = pVgroup->hashEnd;
  createReq.hashMethod = pDb->cfg.hashMethod;
  createReq.numOfRetensions = pDb->cfg.numOfRetensions;
  createReq.pRetensions = pDb->cfg.pRetensions;

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

void *mndBuildAlterVnodeReq(SMnode *pMnode, SDnodeObj *pDnode, SDbObj *pDb, SVgObj *pVgroup, int32_t *pContLen) {
  SAlterVnodeReq alterReq = {0};
  alterReq.vgVersion = pVgroup->version;
  alterReq.buffer = pDb->cfg.buffer;
  alterReq.pages = pDb->cfg.pages;
  alterReq.pageSize = pDb->cfg.pageSize;
  alterReq.daysPerFile = pDb->cfg.daysPerFile;
  alterReq.daysToKeep0 = pDb->cfg.daysToKeep0;
  alterReq.daysToKeep1 = pDb->cfg.daysToKeep1;
  alterReq.daysToKeep2 = pDb->cfg.daysToKeep2;
  alterReq.fsyncPeriod = pDb->cfg.fsyncPeriod;
  alterReq.walLevel = pDb->cfg.walLevel;
  alterReq.strict = pDb->cfg.strict;
  alterReq.cacheLastRow = pDb->cfg.cacheLastRow;
  alterReq.replica = pVgroup->replica;
  alterReq.selfIndex = -1;

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

    if (pDnode->id == pVgid->dnodeId) {
      alterReq.selfIndex = v;
    }
  }

  if (alterReq.selfIndex == -1) {
    terrno = TSDB_CODE_MND_APP_ERROR;
    return NULL;
  }

  int32_t contLen = tSerializeSAlterVnodeReq(NULL, 0, &alterReq);
  if (contLen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  contLen += +sizeof(SMsgHead);

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

  int64_t curMs = taosGetTimestampMs();
  bool    online = mndIsDnodeOnline(pMnode, pDnode, curMs);
  bool    isMnode = mndIsMnode(pMnode, pDnode->id);
  pDnode->numOfVnodes = mndGetVnodesNum(pMnode, pDnode->id);

  mDebug("dnode:%d, vnodes:%d support_vnodes:%d is_mnode:%d online:%d", pDnode->id, pDnode->numOfVnodes,
         pDnode->numOfSupportVnodes, isMnode, online);

  if (isMnode) {
    pDnode->numOfVnodes++;
  }

  if (online && pDnode->numOfSupportVnodes > 0) {
    taosArrayPush(pArray, pDnode);
  }
  return true;
}

SArray *mndBuildDnodesArray(SMnode *pMnode) {
  SSdb   *pSdb = pMnode->pSdb;
  int32_t numOfDnodes = mndGetDnodeSize(pMnode);

  SArray *pArray = taosArrayInit(numOfDnodes, sizeof(SDnodeObj));
  if (pArray == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  sdbTraverse(pSdb, SDB_DNODE, mndResetDnodesArrayFp, NULL, NULL, NULL);
  sdbTraverse(pSdb, SDB_DNODE, mndBuildDnodesArrayFp, pArray, NULL, NULL);
  return pArray;
}

static int32_t mndCompareDnodeVnodes(SDnodeObj *pDnode1, SDnodeObj *pDnode2) {
  float d1Score = (float)pDnode1->numOfVnodes / pDnode1->numOfSupportVnodes;
  float d2Score = (float)pDnode2->numOfVnodes / pDnode2->numOfSupportVnodes;
  return d1Score >= d2Score ? 1 : 0;
}

static int32_t mndGetAvailableDnode(SMnode *pMnode, SVgObj *pVgroup, SArray *pArray) {
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

    pVgid->dnodeId = pDnode->id;
    if (pVgroup->replica == 1) {
      pVgid->role = TAOS_SYNC_STATE_LEADER;
    } else {
      pVgid->role = TAOS_SYNC_STATE_FOLLOWER;
    }

    mInfo("db:%s, vgId:%d, vn:%d dnode:%d is alloced", pVgroup->dbName, pVgroup->vgId, v, pVgid->dnodeId);
    pDnode->numOfVnodes++;
  }

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

  pArray = mndBuildDnodesArray(pMnode);
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

    if (mndGetAvailableDnode(pMnode, pVgroup, pArray) != 0) {
      terrno = TSDB_CODE_MND_NO_ENOUGH_DNODES;
      goto _OVER;
    }

    allocedVgroups++;
  }

  *ppVgroups = pVgroups;
  code = 0;

  mInfo("db:%s, %d vgroups is alloced, replica:%d", pDb->name, pDb->cfg.numOfVgroups, pDb->cfg.replications);

_OVER:
  if (code != 0) taosMemoryFree(pVgroups);
  taosArrayDestroy(pArray);
  return code;
}

int32_t mndAddVnodeToVgroup(SMnode *pMnode, SVgObj *pVgroup, SArray *pArray) {
  taosArraySort(pArray, (__compar_fn_t)mndCompareDnodeVnodes);
  for (int32_t i = 0; i < taosArrayGetSize(pArray); ++i) {
    SDnodeObj *pDnode = taosArrayGet(pArray, i);
    mDebug("dnode:%d, equivalent vnodes:%d", pDnode->id, pDnode->numOfVnodes);
  }

  int32_t maxPos = 1;
  for (int32_t d = 0; d < taosArrayGetSize(pArray); ++d) {
    SDnodeObj *pDnode = taosArrayGet(pArray, d);

    bool used = false;
    for (int32_t vn = 0; vn < maxPos; ++vn) {
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

    SVnodeGid *pVgid = &pVgroup->vnodeGid[maxPos];
    pVgid->dnodeId = pDnode->id;
    pVgid->role = TAOS_SYNC_STATE_FOLLOWER;
    pDnode->numOfVnodes++;

    mInfo("db:%s, vgId:%d, vn:%d dnode:%d is added", pVgroup->dbName, pVgroup->vgId, maxPos, pVgid->dnodeId);
    maxPos++;
    if (maxPos == 3) return 0;
  }

  terrno = TSDB_CODE_MND_NO_ENOUGH_DNODES;
  return -1;
}

int32_t mndRemoveVnodeFromVgroup(SMnode *pMnode, SVgObj *pVgroup, SArray *pArray, SVnodeGid *del1, SVnodeGid *del2) {
  int32_t removedNum = 0;

  taosArraySort(pArray, (__compar_fn_t)mndCompareDnodeVnodes);
  for (int32_t i = 0; i < taosArrayGetSize(pArray); ++i) {
    SDnodeObj *pDnode = taosArrayGet(pArray, i);
    mDebug("dnode:%d, equivalent vnodes:%d", pDnode->id, pDnode->numOfVnodes);
  }

  for (int32_t d = taosArrayGetSize(pArray) - 1; d >= 0; --d) {
    SDnodeObj *pDnode = taosArrayGet(pArray, d);

    for (int32_t vn = 0; vn < TSDB_MAX_REPLICA; ++vn) {
      SVnodeGid *pVgid = &pVgroup->vnodeGid[vn];
      if (pVgid->dnodeId == pDnode->id) {
        if (removedNum == 0) *del1 = *pVgid;
        if (removedNum == 1) *del2 = *pVgid;

        mInfo("db:%s, vgId:%d, vn:%d dnode:%d is removed", pVgroup->dbName, pVgroup->vgId, vn, pVgid->dnodeId);
        memset(pVgid, 0, sizeof(SVnodeGid));
        removedNum++;
        pDnode->numOfVnodes--;

        if (removedNum == 2) goto _OVER;
      }
    }
  }

_OVER:
  if (removedNum != 2) return -1;

  for (int32_t vn = 1; vn < TSDB_MAX_REPLICA; ++vn) {
    SVnodeGid *pVgid = &pVgroup->vnodeGid[vn];
    if (pVgid->dnodeId != 0) {
      memcpy(&pVgroup->vnodeGid[0], pVgid, sizeof(SVnodeGid));
      memset(pVgid, 0, sizeof(SVnodeGid));
    }
  }

  mInfo("db:%s, vgId:%d, dnode:%d is keeped", pVgroup->dbName, pVgroup->vgId, pVgroup->vnodeGid[0].dnodeId);
  return 0;
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

static int32_t mndProcessCreateVnodeRsp(SRpcMsg *pRsp) {
  mndTransProcessRsp(pRsp);
  return 0;
}

static int32_t mndProcessAlterVnodeRsp(SRpcMsg *pRsp) {
  mndTransProcessRsp(pRsp);
  return 0;
}

static int32_t mndProcessDropVnodeRsp(SRpcMsg *pRsp) {
  mndTransProcessRsp(pRsp);
  return 0;
}

static int32_t mndProcessCompactVnodeRsp(SRpcMsg *pRsp) { return 0; }

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

        char        buf1[20] = {0};
        SDnodeObj *pDnodeObj = mndAcquireDnode(pMnode, pVgroup->vnodeGid[i].dnodeId);
        ASSERT(pDnodeObj != NULL);
        bool isOffLine = !mndIsDnodeOnline(pMnode, pDnodeObj, taosGetTimestampMs());
        const char *role = isOffLine ? "OFFLINE" : syncStr(pVgroup->vnodeGid[i].role);
        
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

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    colDataAppendNULL(pColInfo, numOfRows);

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
