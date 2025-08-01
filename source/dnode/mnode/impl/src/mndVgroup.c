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
#include "audit.h"
#include "mndArbGroup.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndMnode.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndStb.h"
#include "mndStream.h"
#include "mndTopic.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"
#include "tmisce.h"

#define VGROUP_VER_NUMBER   1
#define VGROUP_RESERVE_SIZE 60

static int32_t mndVgroupActionInsert(SSdb *pSdb, SVgObj *pVgroup);
static int32_t mndVgroupActionDelete(SSdb *pSdb, SVgObj *pVgroup);
static int32_t mndVgroupActionUpdate(SSdb *pSdb, SVgObj *pOld, SVgObj *pNew);
static int32_t mndNewVgActionValidate(SMnode *pMnode, STrans *pTrans, SSdbRaw *pRaw);

static int32_t mndRetrieveVgroups(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextVgroup(SMnode *pMnode, void *pIter);
static int32_t mndRetrieveVnodes(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextVnode(SMnode *pMnode, void *pIter);

static int32_t mndProcessRedistributeVgroupMsg(SRpcMsg *pReq);
static int32_t mndProcessSplitVgroupMsg(SRpcMsg *pReq);
static int32_t mndProcessBalanceVgroupMsg(SRpcMsg *pReq);
static int32_t mndProcessVgroupBalanceLeaderMsg(SRpcMsg *pReq);
int32_t mndTransProcessSsMigrateVgroupRsp(SRpcMsg *pRsp);

int32_t mndInitVgroup(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_VGROUP,
      .keyType = SDB_KEY_INT32,
      .encodeFp = (SdbEncodeFp)mndVgroupActionEncode,
      .decodeFp = (SdbDecodeFp)mndVgroupActionDecode,
      .insertFp = (SdbInsertFp)mndVgroupActionInsert,
      .updateFp = (SdbUpdateFp)mndVgroupActionUpdate,
      .deleteFp = (SdbDeleteFp)mndVgroupActionDelete,
      .validateFp = (SdbValidateFp)mndNewVgActionValidate,
  };

  mndSetMsgHandle(pMnode, TDMT_DND_CREATE_VNODE_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_ALTER_REPLICA_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_ALTER_CONFIG_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_ALTER_CONFIRM_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_ALTER_HASHRANGE_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_DND_DROP_VNODE_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_COMPACT_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_SSMIGRATE_RSP, mndTransProcessSsMigrateVgroupRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_DISABLE_WRITE_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_SYNC_FORCE_FOLLOWER_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_DND_ALTER_VNODE_TYPE_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_DND_CHECK_VNODE_LEARNER_CATCHUP_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_SYNC_CONFIG_CHANGE_RSP, mndTransProcessRsp);

  mndSetMsgHandle(pMnode, TDMT_MND_REDISTRIBUTE_VGROUP, mndProcessRedistributeVgroupMsg);
  mndSetMsgHandle(pMnode, TDMT_MND_SPLIT_VGROUP, mndProcessSplitVgroupMsg);
  // mndSetMsgHandle(pMnode, TDMT_MND_BALANCE_VGROUP, mndProcessVgroupBalanceLeaderMsg);
  mndSetMsgHandle(pMnode, TDMT_MND_BALANCE_VGROUP, mndProcessBalanceVgroupMsg);
  mndSetMsgHandle(pMnode, TDMT_MND_BALANCE_VGROUP_LEADER, mndProcessVgroupBalanceLeaderMsg);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_VGROUP, mndRetrieveVgroups);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_VGROUP, mndCancelGetNextVgroup);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_VNODES, mndRetrieveVnodes);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_VNODES, mndCancelGetNextVnode);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupVgroup(SMnode *pMnode) {}

SSdbRaw *mndVgroupActionEncode(SVgObj *pVgroup) {
  int32_t code = 0;
  int32_t lino = 0;
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
  SDB_SET_INT32(pRaw, dataPos, pVgroup->syncConfChangeVer, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pVgroup->mountVgId, _OVER)
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
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  SSdbRow *pRow = NULL;
  SVgObj  *pVgroup = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver < 1 || sver > VGROUP_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  pRow = sdbAllocRow(sizeof(SVgObj));
  if (pRow == NULL) goto _OVER;

  pVgroup = sdbGetRowObj(pRow);
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
      pVgid->syncState = TAOS_SYNC_STATE_LEADER;
    }
  }
  if (dataPos + 2 * sizeof(int32_t) + VGROUP_RESERVE_SIZE <= pRaw->dataLen) {
    SDB_GET_INT32(pRaw, dataPos, &pVgroup->syncConfChangeVer, _OVER)
  }
  SDB_GET_INT32(pRaw, dataPos, &pVgroup->mountVgId, _OVER)
  SDB_GET_RESERVE(pRaw, dataPos, VGROUP_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("vgId:%d, failed to decode from raw:%p since %s", pVgroup == NULL ? 0 : pVgroup->vgId, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("vgId:%d, decode from raw:%p, row:%p", pVgroup->vgId, pRaw, pVgroup);
  return pRow;
}

static int32_t mndNewVgActionValidate(SMnode *pMnode, STrans *pTrans, SSdbRaw *pRaw) {
  SSdb    *pSdb = pMnode->pSdb;
  SSdbRow *pRow = NULL;
  SVgObj  *pVgroup = NULL;
  int      code = -1;

  pRow = mndVgroupActionDecode(pRaw);
  if (pRow == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }
  pVgroup = sdbGetRowObj(pRow);
  if (pVgroup == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }

  int32_t maxVgId = sdbGetMaxId(pMnode->pSdb, SDB_VGROUP);
  if (maxVgId > pVgroup->vgId) {
    mError("trans:%d, vgroup id %d already in use. maxVgId:%d", pTrans->id, pVgroup->vgId, maxVgId);
    goto _OVER;
  }

  code = 0;
_OVER:
  if (pVgroup) mndVgroupActionDelete(pSdb, pVgroup);
  taosMemoryFreeClear(pRow);
  TAOS_RETURN(code);
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
  for (int32_t i = 0; i < pNew->replica; ++i) {
    SVnodeGid *pNewGid = &pNew->vnodeGid[i];
    for (int32_t j = 0; j < pOld->replica; ++j) {
      SVnodeGid *pOldGid = &pOld->vnodeGid[j];
      if (pNewGid->dnodeId == pOldGid->dnodeId) {
        pNewGid->syncState = pOldGid->syncState;
        pNewGid->syncRestore = pOldGid->syncRestore;
        pNewGid->syncCanRead = pOldGid->syncCanRead;
        pNewGid->syncAppliedIndex = pOldGid->syncAppliedIndex;
        pNewGid->syncCommitIndex = pOldGid->syncCommitIndex;
        pNewGid->bufferSegmentUsed = pOldGid->bufferSegmentUsed;
        pNewGid->bufferSegmentSize = pOldGid->bufferSegmentSize;
      }
    }
  }
  pNew->numOfTables = pOld->numOfTables;
  pNew->numOfTimeSeries = pOld->numOfTimeSeries;
  pNew->totalStorage = pOld->totalStorage;
  pNew->compStorage = pOld->compStorage;
  pNew->pointsWritten = pOld->pointsWritten;
  pNew->compact = pOld->compact;
  memcpy(pOld->vnodeGid, pNew->vnodeGid, (TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA) * sizeof(SVnodeGid));
  pOld->syncConfChangeVer = pNew->syncConfChangeVer;
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
  createReq.keepTimeOffset = pDb->cfg.keepTimeOffset;
  createReq.ssChunkSize = pDb->cfg.ssChunkSize;
  createReq.ssKeepLocal = pDb->cfg.ssKeepLocal;
  createReq.ssCompact = pDb->cfg.ssCompact;
  createReq.minRows = pDb->cfg.minRows;
  createReq.maxRows = pDb->cfg.maxRows;
  createReq.walFsyncPeriod = pDb->cfg.walFsyncPeriod;
  createReq.walLevel = pDb->cfg.walLevel;
  createReq.precision = pDb->cfg.precision;
  createReq.compression = pDb->cfg.compression;
  createReq.strict = pDb->cfg.strict;
  createReq.cacheLast = pDb->cfg.cacheLast;
  createReq.replica = 0;
  createReq.learnerReplica = 0;
  createReq.selfIndex = -1;
  createReq.learnerSelfIndex = -1;
  createReq.hashBegin = pVgroup->hashBegin;
  createReq.hashEnd = pVgroup->hashEnd;
  createReq.hashMethod = pDb->cfg.hashMethod;
  createReq.numOfRetensions = pDb->cfg.numOfRetensions;
  createReq.pRetensions = pDb->cfg.pRetensions;
  createReq.isTsma = pVgroup->isTsma;
  createReq.pTsma = pVgroup->pTsma;
  createReq.walRetentionPeriod = pDb->cfg.walRetentionPeriod;
  createReq.walRetentionSize = pDb->cfg.walRetentionSize;
  createReq.walRollPeriod = pDb->cfg.walRollPeriod;
  createReq.walSegmentSize = pDb->cfg.walSegmentSize;
  createReq.sstTrigger = pDb->cfg.sstTrigger;
  createReq.hashPrefix = pDb->cfg.hashPrefix;
  createReq.hashSuffix = pDb->cfg.hashSuffix;
  createReq.tsdbPageSize = pDb->cfg.tsdbPageSize;
  createReq.changeVersion = ++(pVgroup->syncConfChangeVer);
  createReq.encryptAlgorithm = pDb->cfg.encryptAlgorithm;
  int32_t code = 0;

  for (int32_t v = 0; v < pVgroup->replica; ++v) {
    SReplica *pReplica = NULL;

    if (pVgroup->vnodeGid[v].nodeRole == TAOS_SYNC_ROLE_VOTER) {
      pReplica = &createReq.replicas[createReq.replica];
    } else {
      pReplica = &createReq.learnerReplicas[createReq.learnerReplica];
    }

    SVnodeGid *pVgid = &pVgroup->vnodeGid[v];
    SDnodeObj *pVgidDnode = mndAcquireDnode(pMnode, pVgid->dnodeId);
    if (pVgidDnode == NULL) {
      return NULL;
    }

    pReplica->id = pVgidDnode->id;
    pReplica->port = pVgidDnode->port;
    memcpy(pReplica->fqdn, pVgidDnode->fqdn, TSDB_FQDN_LEN);
    mndReleaseDnode(pMnode, pVgidDnode);

    if (pVgroup->vnodeGid[v].nodeRole == TAOS_SYNC_ROLE_VOTER) {
      if (pDnode->id == pVgid->dnodeId) {
        createReq.selfIndex = createReq.replica;
      }
    } else {
      if (pDnode->id == pVgid->dnodeId) {
        createReq.learnerSelfIndex = createReq.learnerReplica;
      }
    }

    if (pVgroup->vnodeGid[v].nodeRole == TAOS_SYNC_ROLE_VOTER) {
      createReq.replica++;
    } else {
      createReq.learnerReplica++;
    }
  }

  if (createReq.selfIndex == -1 && createReq.learnerSelfIndex == -1) {
    terrno = TSDB_CODE_APP_ERROR;
    return NULL;
  }

  createReq.changeVersion = pVgroup->syncConfChangeVer;

  mInfo(
      "vgId:%d, build create vnode req, replica:%d selfIndex:%d learnerReplica:%d learnerSelfIndex:%d strict:%d "
      "changeVersion:%d",
      createReq.vgId, createReq.replica, createReq.selfIndex, createReq.learnerReplica, createReq.learnerSelfIndex,
      createReq.strict, createReq.changeVersion);
  for (int32_t i = 0; i < createReq.replica; ++i) {
    mInfo("vgId:%d, replica:%d ep:%s:%u", createReq.vgId, i, createReq.replicas[i].fqdn, createReq.replicas[i].port);
  }
  for (int32_t i = 0; i < createReq.learnerReplica; ++i) {
    mInfo("vgId:%d, replica:%d ep:%s:%u", createReq.vgId, i, createReq.learnerReplicas[i].fqdn,
          createReq.learnerReplicas[i].port);
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

  code = tSerializeSCreateVnodeReq(pReq, contLen, &createReq);
  if (code < 0) {
    terrno = TSDB_CODE_APP_ERROR;
    taosMemoryFree(pReq);
    mError("vgId:%d, failed to serialize create vnode req,since %s", createReq.vgId, terrstr());
    return NULL;
  }
  *pContLen = contLen;
  return pReq;
}

static void *mndBuildAlterVnodeConfigReq(SMnode *pMnode, SDbObj *pDb, SVgObj *pVgroup, int32_t *pContLen) {
  SAlterVnodeConfigReq alterReq = {0};
  alterReq.vgVersion = pVgroup->version;
  alterReq.buffer = pDb->cfg.buffer;
  alterReq.pageSize = pDb->cfg.pageSize;
  alterReq.pages = pDb->cfg.pages;
  alterReq.cacheLastSize = pDb->cfg.cacheLastSize;
  alterReq.daysPerFile = pDb->cfg.daysPerFile;
  alterReq.daysToKeep0 = pDb->cfg.daysToKeep0;
  alterReq.daysToKeep1 = pDb->cfg.daysToKeep1;
  alterReq.daysToKeep2 = pDb->cfg.daysToKeep2;
  alterReq.keepTimeOffset = pDb->cfg.keepTimeOffset;
  alterReq.walFsyncPeriod = pDb->cfg.walFsyncPeriod;
  alterReq.walLevel = pDb->cfg.walLevel;
  alterReq.strict = pDb->cfg.strict;
  alterReq.cacheLast = pDb->cfg.cacheLast;
  alterReq.sttTrigger = pDb->cfg.sstTrigger;
  alterReq.minRows = pDb->cfg.minRows;
  alterReq.walRetentionPeriod = pDb->cfg.walRetentionPeriod;
  alterReq.walRetentionSize = pDb->cfg.walRetentionSize;
  alterReq.ssKeepLocal = pDb->cfg.ssKeepLocal;
  alterReq.ssCompact = pDb->cfg.ssCompact;

  mInfo("vgId:%d, build alter vnode config req", pVgroup->vgId);
  int32_t contLen = tSerializeSAlterVnodeConfigReq(NULL, 0, &alterReq);
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

  if (tSerializeSAlterVnodeConfigReq((char *)pReq + sizeof(SMsgHead), contLen, &alterReq) < 0) {
    taosMemoryFree(pReq);
    mError("vgId:%d, failed to serialize alter vnode config req,since %s", pVgroup->vgId, terrstr());
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  *pContLen = contLen;
  return pReq;
}

static void *mndBuildAlterVnodeReplicaReq(SMnode *pMnode, SDbObj *pDb, SVgObj *pVgroup, int32_t dnodeId,
                                          int32_t *pContLen) {
  SAlterVnodeReplicaReq alterReq = {
      .vgId = pVgroup->vgId,
      .strict = pDb->cfg.strict,
      .replica = 0,
      .learnerReplica = 0,
      .selfIndex = -1,
      .learnerSelfIndex = -1,
      .changeVersion = ++(pVgroup->syncConfChangeVer),
  };

  for (int32_t v = 0; v < pVgroup->replica; ++v) {
    SReplica *pReplica = NULL;

    if (pVgroup->vnodeGid[v].nodeRole == TAOS_SYNC_ROLE_VOTER) {
      pReplica = &alterReq.replicas[alterReq.replica];
      alterReq.replica++;
    } else {
      pReplica = &alterReq.learnerReplicas[alterReq.learnerReplica];
      alterReq.learnerReplica++;
    }

    SVnodeGid *pVgid = &pVgroup->vnodeGid[v];
    SDnodeObj *pVgidDnode = mndAcquireDnode(pMnode, pVgid->dnodeId);
    if (pVgidDnode == NULL) return NULL;

    pReplica->id = pVgidDnode->id;
    pReplica->port = pVgidDnode->port;
    memcpy(pReplica->fqdn, pVgidDnode->fqdn, TSDB_FQDN_LEN);
    mndReleaseDnode(pMnode, pVgidDnode);

    if (pVgroup->vnodeGid[v].nodeRole == TAOS_SYNC_ROLE_VOTER) {
      if (dnodeId == pVgid->dnodeId) {
        alterReq.selfIndex = v;
      }
    } else {
      if (dnodeId == pVgid->dnodeId) {
        alterReq.learnerSelfIndex = v;
      }
    }
  }

  mInfo(
      "vgId:%d, build alter vnode req, replica:%d selfIndex:%d learnerReplica:%d learnerSelfIndex:%d strict:%d "
      "changeVersion:%d",
      alterReq.vgId, alterReq.replica, alterReq.selfIndex, alterReq.learnerReplica, alterReq.learnerSelfIndex,
      alterReq.strict, alterReq.changeVersion);
  for (int32_t i = 0; i < alterReq.replica; ++i) {
    mInfo("vgId:%d, replica:%d ep:%s:%u", alterReq.vgId, i, alterReq.replicas[i].fqdn, alterReq.replicas[i].port);
  }
  for (int32_t i = 0; i < alterReq.learnerReplica; ++i) {
    mInfo("vgId:%d, learnerReplica:%d ep:%s:%u", alterReq.vgId, i, alterReq.learnerReplicas[i].fqdn,
          alterReq.learnerReplicas[i].port);
  }

  if (alterReq.selfIndex == -1 && alterReq.learnerSelfIndex == -1) {
    terrno = TSDB_CODE_APP_ERROR;
    return NULL;
  }

  int32_t contLen = tSerializeSAlterVnodeReplicaReq(NULL, 0, &alterReq);
  if (contLen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  void *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  if (tSerializeSAlterVnodeReplicaReq(pReq, contLen, &alterReq) < 0) {
    mError("vgId:%d, failed to serialize alter vnode req,since %s", alterReq.vgId, terrstr());
    taosMemoryFree(pReq);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  *pContLen = contLen;
  return pReq;
}

static void *mndBuildCheckLearnCatchupReq(SMnode *pMnode, SDbObj *pDb, SVgObj *pVgroup, int32_t dnodeId,
                                          int32_t *pContLen) {
  SCheckLearnCatchupReq req = {
      .vgId = pVgroup->vgId,
      .strict = pDb->cfg.strict,
      .replica = 0,
      .learnerReplica = 0,
      .selfIndex = -1,
      .learnerSelfIndex = -1,
  };

  for (int32_t v = 0; v < pVgroup->replica; ++v) {
    SReplica *pReplica = NULL;

    if (pVgroup->vnodeGid[v].nodeRole == TAOS_SYNC_ROLE_VOTER) {
      pReplica = &req.replicas[req.replica];
      req.replica++;
    } else {
      pReplica = &req.learnerReplicas[req.learnerReplica];
      req.learnerReplica++;
    }

    SVnodeGid *pVgid = &pVgroup->vnodeGid[v];
    SDnodeObj *pVgidDnode = mndAcquireDnode(pMnode, pVgid->dnodeId);
    if (pVgidDnode == NULL) return NULL;

    pReplica->id = pVgidDnode->id;
    pReplica->port = pVgidDnode->port;
    memcpy(pReplica->fqdn, pVgidDnode->fqdn, TSDB_FQDN_LEN);
    mndReleaseDnode(pMnode, pVgidDnode);

    if (pVgroup->vnodeGid[v].nodeRole == TAOS_SYNC_ROLE_VOTER) {
      if (dnodeId == pVgid->dnodeId) {
        req.selfIndex = v;
      }
    } else {
      if (dnodeId == pVgid->dnodeId) {
        req.learnerSelfIndex = v;
      }
    }
  }

  mInfo("vgId:%d, build alter vnode req, replica:%d selfIndex:%d learnerReplica:%d learnerSelfIndex:%d strict:%d",
        req.vgId, req.replica, req.selfIndex, req.learnerReplica, req.learnerSelfIndex, req.strict);
  for (int32_t i = 0; i < req.replica; ++i) {
    mInfo("vgId:%d, replica:%d ep:%s:%u", req.vgId, i, req.replicas[i].fqdn, req.replicas[i].port);
  }
  for (int32_t i = 0; i < req.learnerReplica; ++i) {
    mInfo("vgId:%d, learnerReplica:%d ep:%s:%u", req.vgId, i, req.learnerReplicas[i].fqdn, req.learnerReplicas[i].port);
  }

  if (req.selfIndex == -1 && req.learnerSelfIndex == -1) {
    terrno = TSDB_CODE_APP_ERROR;
    return NULL;
  }

  int32_t contLen = tSerializeSAlterVnodeReplicaReq(NULL, 0, &req);
  if (contLen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  void *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  if (tSerializeSAlterVnodeReplicaReq(pReq, contLen, &req) < 0) {
    mError("vgId:%d, failed to serialize alter vnode req,since %s", req.vgId, terrstr());
    taosMemoryFree(pReq);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  *pContLen = contLen;
  return pReq;
}

static void *mndBuildDisableVnodeWriteReq(SMnode *pMnode, SDbObj *pDb, int32_t vgId, int32_t *pContLen) {
  SDisableVnodeWriteReq disableReq = {
      .vgId = vgId,
      .disable = 1,
  };

  mInfo("vgId:%d, build disable vnode write req", vgId);
  int32_t contLen = tSerializeSDisableVnodeWriteReq(NULL, 0, &disableReq);
  if (contLen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  void *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  if (tSerializeSDisableVnodeWriteReq(pReq, contLen, &disableReq) < 0) {
    mError("vgId:%d, failed to serialize disable vnode write req,since %s", vgId, terrstr());
    taosMemoryFree(pReq);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  *pContLen = contLen;
  return pReq;
}

static void *mndBuildAlterVnodeHashRangeReq(SMnode *pMnode, int32_t srcVgId, SVgObj *pVgroup, int32_t *pContLen) {
  SAlterVnodeHashRangeReq alterReq = {
      .srcVgId = srcVgId,
      .dstVgId = pVgroup->vgId,
      .hashBegin = pVgroup->hashBegin,
      .hashEnd = pVgroup->hashEnd,
      .changeVersion = ++(pVgroup->syncConfChangeVer),
  };

  mInfo("vgId:%d, build alter vnode hashrange req, dstVgId:%d, hashrange:[%u, %u]", srcVgId, pVgroup->vgId,
        pVgroup->hashBegin, pVgroup->hashEnd);
  int32_t contLen = tSerializeSAlterVnodeHashRangeReq(NULL, 0, &alterReq);
  if (contLen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  void *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  if (tSerializeSAlterVnodeHashRangeReq(pReq, contLen, &alterReq) < 0) {
    mError("vgId:%d, failed to serialize alter vnode hashrange req,since %s", srcVgId, terrstr());
    taosMemoryFree(pReq);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  *pContLen = contLen;
  return pReq;
}

void *mndBuildDropVnodeReq(SMnode *pMnode, SDnodeObj *pDnode, SDbObj *pDb, SVgObj *pVgroup, int32_t *pContLen) {
  SDropVnodeReq dropReq = {0};
  dropReq.dnodeId = pDnode->id;
  dropReq.vgId = pVgroup->vgId;
  memcpy(dropReq.db, pDb->name, TSDB_DB_FNAME_LEN);
  dropReq.dbUid = pDb->uid;

  mInfo("vgId:%d, build drop vnode req", dropReq.vgId);
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

  if (tSerializeSDropVnodeReq(pReq, contLen, &dropReq) < 0) {
    mError("vgId:%d, failed to serialize drop vnode req,since %s", dropReq.vgId, terrstr());
    taosMemoryFree(pReq);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  *pContLen = contLen;
  return pReq;
}

static bool mndResetDnodesArrayFp(SMnode *pMnode, void *pObj, void *p1, void *p2, void *p3) {
  SDnodeObj *pDnode = pObj;
  pDnode->numOfVnodes = 0;
  pDnode->numOfOtherNodes = 0;
  return true;
}

static bool mndBuildDnodesArrayFp(SMnode *pMnode, void *pObj, void *p1, void *p2, void *p3) {
  SDnodeObj *pDnode = pObj;
  SArray    *pArray = p1;
  int32_t    exceptDnodeId = *(int32_t *)p2;
  SArray    *dnodeList = p3;

  if (exceptDnodeId == pDnode->id) {
    return true;
  }

  if (dnodeList != NULL) {
    int32_t dnodeListSize = taosArrayGetSize(dnodeList);
    if (dnodeListSize > 0) {
      bool inDnodeList = false;
      for (int32_t index = 0; index < dnodeListSize; ++index) {
        int32_t dnodeId = *(int32_t *)taosArrayGet(dnodeList, index);
        if (pDnode->id == dnodeId) {
          inDnodeList = true;
        }
      }
      if (!inDnodeList) {
        return true;
      }
    } else {
      return true;  // TS-6191
    }
  }

  int64_t curMs = taosGetTimestampMs();
  bool    online = mndIsDnodeOnline(pDnode, curMs);
  bool    isMnode = mndIsMnode(pMnode, pDnode->id);
  pDnode->numOfVnodes = mndGetVnodesNum(pMnode, pDnode->id);
  pDnode->memUsed = mndGetVnodesMemory(pMnode, pDnode->id);

  mInfo("dnode:%d, vnodes:%d supportVnodes:%d isMnode:%d online:%d memory avail:%" PRId64 " used:%" PRId64, pDnode->id,
        pDnode->numOfVnodes, pDnode->numOfSupportVnodes, isMnode, online, pDnode->memAvail, pDnode->memUsed);

  if (isMnode) {
    pDnode->numOfOtherNodes++;
  }

  if (online && pDnode->numOfSupportVnodes > 0) {
    if (taosArrayPush(pArray, pDnode) == NULL) return false;
  }
  return true;
}

static bool isDnodeInList(SArray *dnodeList, int32_t dnodeId) {
  int32_t dnodeListSize = taosArrayGetSize(dnodeList);
  for (int32_t i = 0; i < dnodeListSize; ++i) {
    int32_t id = *(int32_t *)TARRAY_GET_ELEM(dnodeList, i);
    if (id == dnodeId) {
      return true;
    }
  }
  return false;
}

#ifdef TD_ENTERPRISE
static float mndGetDnodeScore1(SDnodeObj *pDnode, int32_t additionDnodes, float ratio) {
  float totalDnodes = pDnode->numOfVnodes + (float)pDnode->numOfOtherNodes * ratio + additionDnodes;
  float result = totalDnodes / pDnode->numOfSupportVnodes;
  return pDnode->numOfVnodes > 0 ? -result : result;
}

static int32_t mndCompareDnodeVnodes1(SDnodeObj *pDnode1, SDnodeObj *pDnode2) {
  float d1Score = mndGetDnodeScore1(pDnode1, 0, 0.9);
  float d2Score = mndGetDnodeScore1(pDnode2, 0, 0.9);
  if (d1Score == d2Score) {
    if (pDnode1->id == pDnode2->id) {
      return 0;
    }
    return pDnode1->id > pDnode2->id ? 1 : -1;
  }
  return d1Score > d2Score ? 1 : -1;
}

static bool mndBuildDnodesListFp(SMnode *pMnode, void *pObj, void *p1, void *p2, void *p3) {
  SDnodeObj *pDnode = pObj;
  SArray    *pArray = p1;

  bool isMnode = mndIsMnode(pMnode, pDnode->id);
  pDnode->numOfVnodes = mndGetVnodesNum(pMnode, pDnode->id);

  if (isMnode) {
    pDnode->numOfOtherNodes++;
  }

  if (pDnode->numOfSupportVnodes > 0) {
    if (taosArrayPush(pArray, pDnode) == NULL) return false;
  }
  return true;
}

// TS-6191
static int32_t mndBuildNodesCheckDualReplica(SMnode *pMnode, int32_t nDnodes, SArray *dnodeList, SArray **ppDnodeList) {
  int32_t code = 0;
  if (!grantCheckDualReplicaDnodes(pMnode)) {
    TAOS_RETURN(code);
  }
  SSdb   *pSdb = pMnode->pSdb;
  SArray *pArray = taosArrayInit(nDnodes, sizeof(SDnodeObj));
  if (pArray == NULL) {
    TAOS_RETURN(code = terrno);
  }
  *ppDnodeList = pArray;

  sdbTraverse(pSdb, SDB_DNODE, mndResetDnodesArrayFp, NULL, NULL, NULL);
  sdbTraverse(pSdb, SDB_DNODE, mndBuildDnodesListFp, pArray, NULL, NULL);

  int32_t arrSize = taosArrayGetSize(pArray);
  if (arrSize <= 0) {
    TAOS_RETURN(code);
  }
  if (arrSize > 1) taosArraySort(pArray, (__compar_fn_t)mndCompareDnodeVnodes1);

  int32_t dnodeListSize = taosArrayGetSize(dnodeList);
  if (dnodeListSize <= 0) {
    if (arrSize > 2) taosArrayRemoveBatch(pArray, 2, arrSize - 2, NULL);
  } else {
    int32_t nDnodesWithVnodes = 0;
    for (int32_t i = 0; i < arrSize; ++i) {
      SDnodeObj *pDnode = TARRAY_GET_ELEM(pArray, i);
      if (pDnode->numOfVnodes <= 0) {
        break;
      }
      ++nDnodesWithVnodes;
    }
    int32_t dnodeId = -1;
    if (nDnodesWithVnodes == 1) {
      dnodeId = ((SDnodeObj *)TARRAY_GET_ELEM(pArray, 0))->id;
    } else if (nDnodesWithVnodes >= 2) {
      // must select the dnodes from the 1st 2 dnodes
      taosArrayRemoveBatch(pArray, 2, arrSize - 2, NULL);
    }
    for (int32_t i = 0; i < TARRAY_SIZE(pArray);) {
      SDnodeObj *pDnode = taosArrayGet(pArray, i);
      if (!isDnodeInList(dnodeList, pDnode->id)) {
        taosArrayRemove(pArray, i);
        continue;
      }
      ++i;
    }
    if (nDnodesWithVnodes == 1) {
      SDnodeObj *pDnode = taosArrayGet(pArray, 0);
      if (pDnode && (pDnode->id != dnodeId)) {  // the first dnode is not in dnodeList, remove the last element
        taosArrayRemove(pArray, taosArrayGetSize(pArray) - 1);
      }
    }
  }

  TAOS_RETURN(code);
}
#endif

SArray *mndBuildDnodesArray(SMnode *pMnode, int32_t exceptDnodeId, SArray *dnodeList) {
  SSdb   *pSdb = pMnode->pSdb;
  int32_t numOfDnodes = mndGetDnodeSize(pMnode);
  SArray *tDnodeList = NULL;
  SArray *pDnodeList = NULL;

  SArray *pArray = taosArrayInit(numOfDnodes, sizeof(SDnodeObj));
  if (pArray == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  if (taosArrayGetSize(dnodeList) > 0) {
    tDnodeList = dnodeList;
  }
#ifdef TD_ENTERPRISE
  if (0 != mndBuildNodesCheckDualReplica(pMnode, numOfDnodes, tDnodeList, &pDnodeList)) {
    taosArrayDestroy(pArray);
    return NULL;
  }
#endif
  sdbTraverse(pSdb, SDB_DNODE, mndResetDnodesArrayFp, NULL, NULL, NULL);
  sdbTraverse(pSdb, SDB_DNODE, mndBuildDnodesArrayFp, pArray, &exceptDnodeId, pDnodeList ? pDnodeList : tDnodeList);

  mDebug("build %d dnodes array", (int32_t)taosArrayGetSize(pArray));
  for (int32_t i = 0; i < (int32_t)taosArrayGetSize(pArray); ++i) {
    SDnodeObj *pDnode = taosArrayGet(pArray, i);
    mDebug("dnode:%d, vnodes:%d others:%d", pDnode->id, pDnode->numOfVnodes, pDnode->numOfOtherNodes);
  }
  taosArrayDestroy(pDnodeList);
  return pArray;
}

static int32_t mndCompareDnodeId(int32_t *dnode1Id, int32_t *dnode2Id) {
  if (*dnode1Id == *dnode2Id) {
    return 0;
  }
  return *dnode1Id > *dnode2Id ? 1 : -1;
}

static float mndGetDnodeScore(SDnodeObj *pDnode, int32_t additionDnodes, float ratio) {
  float totalDnodes = pDnode->numOfVnodes + (float)pDnode->numOfOtherNodes * ratio + additionDnodes;
  return totalDnodes / pDnode->numOfSupportVnodes;
}

static int32_t mndCompareDnodeVnodes(SDnodeObj *pDnode1, SDnodeObj *pDnode2) {
  float d1Score = mndGetDnodeScore(pDnode1, 0, 0.9);
  float d2Score = mndGetDnodeScore(pDnode2, 0, 0.9);
  if (d1Score == d2Score) {
    return 0;
  }
  return d1Score > d2Score ? 1 : -1;
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
  mDebug("start to sort %d dnodes", (int32_t)taosArrayGetSize(pArray));
  taosArraySort(pArray, (__compar_fn_t)mndCompareDnodeVnodes);
  for (int32_t i = 0; i < (int32_t)taosArrayGetSize(pArray); ++i) {
    SDnodeObj *pDnode = taosArrayGet(pArray, i);
    mDebug("dnode:%d, score:%f", pDnode->id, mndGetDnodeScore(pDnode, 0, 0.9));
  }

  int32_t size = taosArrayGetSize(pArray);
  if (size < pVgroup->replica) {
    mError("db:%s, vgId:%d, no enough online dnodes:%d to alloc %d replica", pVgroup->dbName, pVgroup->vgId, size,
           pVgroup->replica);
    TAOS_RETURN(TSDB_CODE_MND_NO_ENOUGH_DNODES);
  }

  for (int32_t v = 0; v < pVgroup->replica; ++v) {
    SVnodeGid *pVgid = &pVgroup->vnodeGid[v];
    SDnodeObj *pDnode = taosArrayGet(pArray, v);
    if (pDnode == NULL) {
      TAOS_RETURN(TSDB_CODE_MND_NO_ENOUGH_DNODES);
    }
    if (pDnode->numOfVnodes >= pDnode->numOfSupportVnodes) {
      TAOS_RETURN(TSDB_CODE_MND_NO_ENOUGH_VNODES);
    }

    int64_t vgMem = mndGetVgroupMemory(pMnode, pDb, pVgroup);
    if (pDnode->memAvail - vgMem - pDnode->memUsed <= 0) {
      mError("db:%s, vgId:%d, no enough memory:%" PRId64 " in dnode:%d, avail:%" PRId64 " used:%" PRId64,
             pVgroup->dbName, pVgroup->vgId, vgMem, pDnode->id, pDnode->memAvail, pDnode->memUsed);
      TAOS_RETURN(TSDB_CODE_MND_NO_ENOUGH_MEM_IN_DNODE);
    } else {
      pDnode->memUsed += vgMem;
    }

    pVgid->dnodeId = pDnode->id;
    if (pVgroup->replica == 1) {
      pVgid->syncState = TAOS_SYNC_STATE_LEADER;
    } else {
      pVgid->syncState = TAOS_SYNC_STATE_FOLLOWER;
    }

    mInfo("db:%s, vgId:%d, vn:%d is alloced, memory:%" PRId64 ", dnode:%d avail:%" PRId64 " used:%" PRId64,
          pVgroup->dbName, pVgroup->vgId, v, vgMem, pVgid->dnodeId, pDnode->memAvail, pDnode->memUsed);
    pDnode->numOfVnodes++;
  }

  mndSortVnodeGid(pVgroup);
  return 0;
}

int32_t mndAllocSmaVgroup(SMnode *pMnode, SDbObj *pDb, SVgObj *pVgroup) {
  int32_t code = 0;
  SArray *pArray = mndBuildDnodesArray(pMnode, 0, NULL);
  if (pArray == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

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

int32_t mndAllocVgroup(SMnode *pMnode, SDbObj *pDb, SVgObj **ppVgroups, SArray *dnodeList) {
  int32_t code = -1;
  SArray *pArray = NULL;
  SVgObj *pVgroups = NULL;

  pVgroups = taosMemoryCalloc(pDb->cfg.numOfVgroups, sizeof(SVgObj));
  if (pVgroups == NULL) {
    code = terrno;
    goto _OVER;
  }

  pArray = mndBuildDnodesArray(pMnode, 0, dnodeList);
  if (pArray == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }

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

    if ((code = mndGetAvailableDnode(pMnode, pDb, pVgroup, pArray)) != 0) {
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
  TAOS_RETURN(code);
}

SEpSet mndGetVgroupEpset(SMnode *pMnode, const SVgObj *pVgroup) {
  SEpSet epset = {0};

  for (int32_t v = 0; v < pVgroup->replica; ++v) {
    const SVnodeGid *pVgid = &pVgroup->vnodeGid[v];
    SDnodeObj       *pDnode = mndAcquireDnode(pMnode, pVgid->dnodeId);
    if (pDnode == NULL) continue;

    if (pVgid->syncState == TAOS_SYNC_STATE_LEADER || pVgid->syncState == TAOS_SYNC_STATE_ASSIGNED_LEADER) {
      epset.inUse = epset.numOfEps;
    }

    if (addEpIntoEpSet(&epset, pDnode->fqdn, pDnode->port) != 0) {
      mWarn("vgId:%d, failed to add ep:%s:%d into epset", pVgroup->vgId, pDnode->fqdn, pDnode->port);
    }
    mndReleaseDnode(pMnode, pDnode);
  }
  epsetSort(&epset);

  return epset;
}

SEpSet mndGetVgroupEpsetById(SMnode *pMnode, int32_t vgId) {
  SEpSet epset = {0};

  SVgObj *pVgroup = mndAcquireVgroup(pMnode, vgId);
  if (!pVgroup) return epset;

  for (int32_t v = 0; v < pVgroup->replica; ++v) {
    const SVnodeGid *pVgid = &pVgroup->vnodeGid[v];
    SDnodeObj       *pDnode = mndAcquireDnode(pMnode, pVgid->dnodeId);
    if (pDnode == NULL) continue;

    if (pVgid->syncState == TAOS_SYNC_STATE_LEADER || pVgid->syncState == TAOS_SYNC_STATE_ASSIGNED_LEADER) {
      epset.inUse = epset.numOfEps;
    }

    if (addEpIntoEpSet(&epset, pDnode->fqdn, pDnode->port) != 0) {
      mWarn("vgId:%d, failed to add ep:%s:%d into epset", pVgroup->vgId, pDnode->fqdn, pDnode->port);
    }
    mndReleaseDnode(pMnode, pDnode);
  }

  mndReleaseVgroup(pMnode, pVgroup);
  return epset;
}

static int32_t mndRetrieveVgroups(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode *pMnode = pReq->info.node;
  SSdb   *pSdb = pMnode->pSdb;
  int32_t numOfRows = 0;
  SVgObj *pVgroup = NULL;
  int32_t cols = 0;
  int64_t curMs = taosGetTimestampMs();
  int32_t code = 0, lino = 0;

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
    COL_DATA_SET_VAL_GOTO((const char *)&pVgroup->vgId, false, pVgroup, pShow->pIter, _OVER);

    SName name = {0};
    char  db[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    code = tNameFromString(&name, pVgroup->dbName, T_NAME_ACCT | T_NAME_DB);
    if (code != 0) {
      mError("vgId:%d, failed to set dbName, since %s", pVgroup->vgId, tstrerror(code));
      sdbRelease(pSdb, pVgroup);
      sdbCancelFetch(pSdb, pShow->pIter);
      return code;
    }
    (void)tNameGetDbName(&name, varDataVal(db));
    varDataSetLen(db, strlen(varDataVal(db)));

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)db, false, pVgroup, pShow->pIter, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)&pVgroup->numOfTables, false, pVgroup, pShow->pIter, _OVER);

    // default 3 replica, add 1 replica if move vnode
    for (int32_t i = 0; i < 4; ++i) {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      if (i < pVgroup->replica) {
        int16_t dnodeId = (int16_t)pVgroup->vnodeGid[i].dnodeId;
        COL_DATA_SET_VAL_GOTO((const char *)&dnodeId, false, pVgroup, pShow->pIter, _OVER);

        bool       exist = false;
        bool       online = false;
        SDnodeObj *pDnode = mndAcquireDnode(pMnode, pVgroup->vnodeGid[i].dnodeId);
        if (pDnode != NULL) {
          exist = true;
          online = mndIsDnodeOnline(pDnode, curMs);
          mndReleaseDnode(pMnode, pDnode);
        }

        char buf1[20] = {0};
        char role[20] = "offline";
        if (!exist) {
          tstrncpy(role, "dropping", sizeof(role));
        } else if (online) {
          char *star = "";
          if (pVgroup->vnodeGid[i].syncState == TAOS_SYNC_STATE_LEADER ||
              pVgroup->vnodeGid[i].syncState == TAOS_SYNC_STATE_ASSIGNED_LEADER) {
            if (!pVgroup->vnodeGid[i].syncRestore && !pVgroup->vnodeGid[i].syncCanRead) {
              star = "**";
            } else if (!pVgroup->vnodeGid[i].syncRestore && pVgroup->vnodeGid[i].syncCanRead) {
              star = "*";
            } else {
            }
          }
          snprintf(role, sizeof(role), "%s%s", syncStr(pVgroup->vnodeGid[i].syncState), star);
          /*
          mInfo("db:%s, learner progress:%d", pDb->name, pVgroup->vnodeGid[i].learnerProgress);

          if (pVgroup->vnodeGid[i].syncState == TAOS_SYNC_STATE_LEARNER) {
            if(pVgroup->vnodeGid[i].learnerProgress < 0){
              snprintf(role, sizeof(role), "%s-",
                syncStr(pVgroup->vnodeGid[i].syncState));

            }
            else if(pVgroup->vnodeGid[i].learnerProgress >= 100){
              snprintf(role, sizeof(role), "%s--",
                syncStr(pVgroup->vnodeGid[i].syncState));
            }
            else{
              snprintf(role, sizeof(role), "%s%d",
                syncStr(pVgroup->vnodeGid[i].syncState), pVgroup->vnodeGid[i].learnerProgress);
            }
          }
          else{
            snprintf(role, sizeof(role), "%s%s", syncStr(pVgroup->vnodeGid[i].syncState), star);
          }
          */
        } else {
        }
        STR_WITH_MAXSIZE_TO_VARSTR(buf1, role, pShow->pMeta->pSchemas[cols].bytes);

        pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
        COL_DATA_SET_VAL_GOTO((const char *)buf1, false, pVgroup, pShow->pIter, _OVER);

        char applyStr[TSDB_SYNC_APPLY_COMMIT_LEN + 1] = {0};
        char buf[TSDB_SYNC_APPLY_COMMIT_LEN + VARSTR_HEADER_SIZE + 1] = {0};
        snprintf(applyStr, sizeof(applyStr), "%" PRId64 "/%" PRId64, pVgroup->vnodeGid[i].syncAppliedIndex,
                 pVgroup->vnodeGid[i].syncCommitIndex);
        STR_WITH_MAXSIZE_TO_VARSTR(buf, applyStr, pShow->pMeta->pSchemas[cols].bytes);

        pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
        COL_DATA_SET_VAL_GOTO((const char *)&buf, false, pVgroup, pShow->pIter, _OVER);
      } else {
        colDataSetNULL(pColInfo, numOfRows);
        pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
        colDataSetNULL(pColInfo, numOfRows);
        pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
        colDataSetNULL(pColInfo, numOfRows);
      }
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    int32_t cacheUsage = (int32_t)pVgroup->cacheUsage;
    COL_DATA_SET_VAL_GOTO((const char *)&cacheUsage, false, pVgroup, pShow->pIter, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)&pVgroup->numOfCachedTables, false, pVgroup, pShow->pIter, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)&pVgroup->isTsma, false, pVgroup, pShow->pIter, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)&pVgroup->mountVgId, false, pVgroup, pShow->pIter, _OVER);

    numOfRows++;
    sdbRelease(pSdb, pVgroup);
  }
_OVER:
  if (pDb != NULL) {
    mndReleaseDb(pMnode, pDb);
  }
  if (code != 0) {
    mError("failed to retrieve vgroup info at line %d since %s", lino, tstrerror(code));
    TAOS_RETURN(code);
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextVgroup(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_VGROUP);
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
    int64_t buffer = (int64_t)pDb->cfg.buffer * 1024 * 1024;
    int64_t cache = (int64_t)pDb->cfg.pages * pDb->cfg.pageSize * 1024;
    vgroupMemroy = buffer + cache;
    int64_t cacheLast = (int64_t)pDb->cfg.cacheLastSize * 1024 * 1024;
    if (pDb->cfg.cacheLast > 0) {
      vgroupMemroy += cacheLast;
    }
    mDebug("db:%s, vgroup:%d, buffer:%" PRId64 " cache:%" PRId64 " cacheLast:%" PRId64, pDb->name, pVgroup->vgId,
           buffer, cache, cacheLast);
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

void calculateRstoreFinishTime(double rate, int64_t applyCount, char *restoreStr, size_t restoreStrSize) {
  if (rate == 0) {
    snprintf(restoreStr, restoreStrSize, "0:0:0");
    return;
  }

  int64_t costTime = applyCount / rate;
  int64_t totalSeconds = costTime / 1000;
  int64_t hours = totalSeconds / 3600;
  totalSeconds %= 3600;
  int64_t minutes = totalSeconds / 60;
  int64_t seconds = totalSeconds % 60;
  snprintf(restoreStr, restoreStrSize, "%" PRId64 ":%" PRId64 ":%" PRId64, hours, minutes, seconds);
}

static int32_t mndRetrieveVnodes(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode *pMnode = pReq->info.node;
  SSdb   *pSdb = pMnode->pSdb;
  int32_t numOfRows = 0;
  SVgObj *pVgroup = NULL;
  int32_t cols = 0;
  int64_t curMs = taosGetTimestampMs();
  int32_t code = 0;

  while (numOfRows < rows - TSDB_MAX_REPLICA) {
    pShow->pIter = sdbFetch(pSdb, SDB_VGROUP, pShow->pIter, (void **)&pVgroup);
    if (pShow->pIter == NULL) break;

    for (int32_t i = 0; i < pVgroup->replica && numOfRows < rows; ++i) {
      SVnodeGid       *pGid = &pVgroup->vnodeGid[i];
      SColumnInfoData *pColInfo = NULL;
      cols = 0;

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)&pGid->dnodeId, false);
      if (code != 0) {
        mError("vgId:%d, failed to set dnodeId, since %s", pVgroup->vgId, tstrerror(code));
        return code;
      }
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)&pVgroup->vgId, false);
      if (code != 0) {
        mError("vgId:%d, failed to set vgId, since %s", pVgroup->vgId, tstrerror(code));
        return code;
      }

      // db_name
      const char *dbname = mndGetDbStr(pVgroup->dbName);
      char        b1[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      if (dbname != NULL) {
        STR_WITH_MAXSIZE_TO_VARSTR(b1, dbname, TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE);
      } else {
        STR_WITH_MAXSIZE_TO_VARSTR(b1, "NULL", TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE);
      }
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)b1, false);
      if (code != 0) {
        mError("vgId:%d, failed to set dbName, since %s", pVgroup->vgId, tstrerror(code));
        return code;
      }

      // dnode is online?
      SDnodeObj *pDnode = mndAcquireDnode(pMnode, pGid->dnodeId);
      if (pDnode == NULL) {
        mError("failed to acquire dnode. dnodeId:%d", pGid->dnodeId);
        break;
      }
      bool isDnodeOnline = mndIsDnodeOnline(pDnode, curMs);

      char       buf[20] = {0};
      ESyncState syncState = (isDnodeOnline) ? pGid->syncState : TAOS_SYNC_STATE_OFFLINE;
      STR_TO_VARSTR(buf, syncStr(syncState));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)buf, false);
      if (code != 0) {
        mError("vgId:%d, failed to set syncState, since %s", pVgroup->vgId, tstrerror(code));
        return code;
      }

      int64_t roleTimeMs = (isDnodeOnline) ? pGid->roleTimeMs : 0;
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)&roleTimeMs, false);
      if (code != 0) {
        mError("vgId:%d, failed to set roleTimeMs, since %s", pVgroup->vgId, tstrerror(code));
        return code;
      }

      int64_t startTimeMs = (isDnodeOnline) ? pGid->startTimeMs : 0;
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)&startTimeMs, false);
      if (code != 0) {
        mError("vgId:%d, failed to set startTimeMs, since %s", pVgroup->vgId, tstrerror(code));
        return code;
      }

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)&pGid->syncRestore, false);
      if (code != 0) {
        mError("vgId:%d, failed to set syncRestore, since %s", pVgroup->vgId, tstrerror(code));
        return code;
      }

      int64_t unappliedCount = pGid->syncCommitIndex - pGid->syncAppliedIndex;
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      char restoreStr[20] = {0};
      if (unappliedCount > 0) {
        calculateRstoreFinishTime(pGid->appliedRate, unappliedCount, restoreStr, sizeof(restoreStr));
      }
      STR_TO_VARSTR(buf, restoreStr);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)&buf, false);
      if (code != 0) {
        mError("vgId:%d, failed to set syncRestore finish time, since %s", pVgroup->vgId, tstrerror(code));
        return code;
      }

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)&unappliedCount, false);
      if (code != 0) {
        mError("vgId:%d, failed to set syncRestore, since %s", pVgroup->vgId, tstrerror(code));
        return code;
      }

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)&pGid->bufferSegmentUsed, false);
      if (code != 0) {
        mError("vgId:%d, failed to set buffer segment used, since %s", pVgroup->vgId, tstrerror(code));
        return code;
      }

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)&pGid->bufferSegmentSize, false);
      if (code != 0) {
        mError("vgId:%d, failed to set buffer segment size, since %s", pVgroup->vgId, tstrerror(code));
        return code;
      }

      numOfRows++;
      sdbRelease(pSdb, pDnode);
    }

    sdbRelease(pSdb, pVgroup);
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextVnode(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_VGROUP);
}

static int32_t mndAddVnodeToVgroup(SMnode *pMnode, STrans *pTrans, SVgObj *pVgroup, SArray *pArray) {
  int32_t code = 0;
  taosArraySort(pArray, (__compar_fn_t)mndCompareDnodeVnodes);
  for (int32_t i = 0; i < taosArrayGetSize(pArray); ++i) {
    SDnodeObj *pDnode = taosArrayGet(pArray, i);
    mInfo("trans:%d, dnode:%d, equivalent vnodes:%d others:%d", pTrans->id, pDnode->id, pDnode->numOfVnodes,
          pDnode->numOfOtherNodes);
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

    if (pDnode == NULL) {
      TAOS_RETURN(TSDB_CODE_MND_NO_ENOUGH_DNODES);
    }
    if (pDnode->numOfVnodes >= pDnode->numOfSupportVnodes) {
      TAOS_RETURN(TSDB_CODE_MND_NO_ENOUGH_VNODES);
    }

    int64_t vgMem = mndGetVgroupMemory(pMnode, NULL, pVgroup);
    if (pDnode->memAvail - vgMem - pDnode->memUsed <= 0) {
      mError("trans:%d, db:%s, vgId:%d, no enough memory:%" PRId64 " in dnode:%d avail:%" PRId64 " used:%" PRId64,
             pTrans->id, pVgroup->dbName, pVgroup->vgId, vgMem, pDnode->id, pDnode->memAvail, pDnode->memUsed);
      TAOS_RETURN(TSDB_CODE_MND_NO_ENOUGH_MEM_IN_DNODE);
    } else {
      pDnode->memUsed += vgMem;
    }

    pVgid->dnodeId = pDnode->id;
    pVgid->syncState = TAOS_SYNC_STATE_OFFLINE;
    mInfo("trans:%id, db:%s, vgId:%d, vn:%d is added, memory:%" PRId64 ", dnode:%d avail:%" PRId64 " used:%" PRId64,
          pTrans->id, pVgroup->dbName, pVgroup->vgId, pVgroup->replica, vgMem, pVgid->dnodeId, pDnode->memAvail,
          pDnode->memUsed);

    pVgroup->replica++;
    pDnode->numOfVnodes++;

    SSdbRaw *pVgRaw = mndVgroupActionEncode(pVgroup);
    if (pVgRaw == NULL) {
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      TAOS_RETURN(code);
    }
    if ((code = mndTransAppendGroupRedolog(pTrans, pVgRaw, pVgroup->vgId)) != 0) {
      sdbFreeRaw(pVgRaw);
      TAOS_RETURN(code);
    }
    code = sdbSetRawStatus(pVgRaw, SDB_STATUS_READY);
    if (code != 0) {
      mError("trans:%d, vgId:%d, failed to set raw status since %s at line:%d", pTrans->id, pVgroup->vgId,
             tstrerror(code), __LINE__);
    }
    TAOS_RETURN(code);
  }

  code = TSDB_CODE_MND_NO_ENOUGH_DNODES;
  mError("trans:%d, db:%s, failed to add vnode to vgId:%d since %s", pTrans->id, pVgroup->dbName, pVgroup->vgId,
         tstrerror(code));
  TAOS_RETURN(code);
}

static int32_t mndRemoveVnodeFromVgroup(SMnode *pMnode, STrans *pTrans, SVgObj *pVgroup, SArray *pArray,
                                        SVnodeGid *pDelVgid) {
  taosArraySort(pArray, (__compar_fn_t)mndCompareDnodeVnodes);
  for (int32_t i = 0; i < taosArrayGetSize(pArray); ++i) {
    SDnodeObj *pDnode = taosArrayGet(pArray, i);
    mInfo("trans:%d, dnode:%d, equivalent vnodes:%d others:%d", pTrans->id, pDnode->id, pDnode->numOfVnodes,
          pDnode->numOfOtherNodes);
  }

  int32_t code = -1;
  for (int32_t d = taosArrayGetSize(pArray) - 1; d >= 0; --d) {
    SDnodeObj *pDnode = taosArrayGet(pArray, d);

    for (int32_t vn = 0; vn < pVgroup->replica; ++vn) {
      SVnodeGid *pVgid = &pVgroup->vnodeGid[vn];
      if (pVgid->dnodeId == pDnode->id) {
        int64_t vgMem = mndGetVgroupMemory(pMnode, NULL, pVgroup);
        pDnode->memUsed -= vgMem;
        mInfo("trans:%d, db:%s, vgId:%d, vn:%d is removed, memory:%" PRId64 ", dnode:%d avail:%" PRId64
              " used:%" PRId64,
              pTrans->id, pVgroup->dbName, pVgroup->vgId, vn, vgMem, pVgid->dnodeId, pDnode->memAvail, pDnode->memUsed);
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
    code = TSDB_CODE_APP_ERROR;
    mError("trans:%d, db:%s, failed to remove vnode from vgId:%d since %s", pTrans->id, pVgroup->dbName, pVgroup->vgId,
           tstrerror(code));
    TAOS_RETURN(code);
  }

  for (int32_t vn = 0; vn < pVgroup->replica; ++vn) {
    SVnodeGid *pVgid = &pVgroup->vnodeGid[vn];
    mInfo("trans:%d, db:%s, vgId:%d, vn:%d dnode:%d is reserved", pTrans->id, pVgroup->dbName, pVgroup->vgId, vn,
          pVgid->dnodeId);
  }

  SSdbRaw *pVgRaw = mndVgroupActionEncode(pVgroup);
  if (pVgRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  if (mndTransAppendGroupRedolog(pTrans, pVgRaw, pVgroup->vgId) != 0) {
    sdbFreeRaw(pVgRaw);
    TAOS_RETURN(code);
  }
  code = sdbSetRawStatus(pVgRaw, SDB_STATUS_READY);
  if (code != 0) {
    mError("trans:%d, vgId:%d, failed to set raw status since %s at line:%d", pTrans->id, pVgroup->vgId,
           tstrerror(code), __LINE__);
  }

  TAOS_RETURN(code);
}

static int32_t mndRemoveVnodeFromVgroupWithoutSave(SMnode *pMnode, STrans *pTrans, SVgObj *pVgroup, SArray *pArray,
                                                   SVnodeGid *pDelVgid) {
  taosArraySort(pArray, (__compar_fn_t)mndCompareDnodeVnodes);
  for (int32_t i = 0; i < taosArrayGetSize(pArray); ++i) {
    SDnodeObj *pDnode = taosArrayGet(pArray, i);
    mInfo("dnode:%d, equivalent vnodes:%d others:%d", pDnode->id, pDnode->numOfVnodes, pDnode->numOfOtherNodes);
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
    code = TSDB_CODE_APP_ERROR;
    mError("db:%s, failed to remove vnode from vgId:%d since %s", pVgroup->dbName, pVgroup->vgId, tstrerror(code));
    TAOS_RETURN(code);
  }

  for (int32_t vn = 0; vn < pVgroup->replica; ++vn) {
    SVnodeGid *pVgid = &pVgroup->vnodeGid[vn];
    mInfo("db:%s, vgId:%d, vn:%d dnode:%d is reserved", pVgroup->dbName, pVgroup->vgId, vn, pVgid->dnodeId);
  }

  TAOS_RETURN(code);
}

int32_t mndAddCreateVnodeAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup, SVnodeGid *pVgid) {
  int32_t      code = 0;
  STransAction action = {0};

  SDnodeObj *pDnode = mndAcquireDnode(pMnode, pVgid->dnodeId);
  if (pDnode == NULL) return -1;
  action.epSet = mndGetDnodeEpset(pDnode);
  mndReleaseDnode(pMnode, pDnode);

  int32_t contLen = 0;
  void   *pReq = mndBuildCreateVnodeReq(pMnode, pDnode, pDb, pVgroup, &contLen);
  if (pReq == NULL) return -1;

  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_CREATE_VNODE;
  action.acceptableCode = TSDB_CODE_VND_ALREADY_EXIST;
  action.groupId = pVgroup->vgId;

  if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pReq);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

int32_t mndRestoreAddCreateVnodeAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup,
                                       SDnodeObj *pDnode) {
  int32_t      code = 0;
  STransAction action = {0};

  action.epSet = mndGetDnodeEpset(pDnode);

  int32_t contLen = 0;
  void   *pReq = mndBuildCreateVnodeReq(pMnode, pDnode, pDb, pVgroup, &contLen);
  if (pReq == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_CREATE_VNODE;
  action.acceptableCode = TSDB_CODE_VND_ALREADY_EXIST;
  action.groupId = pVgroup->vgId;

  if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pReq);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

int32_t mndAddAlterVnodeConfirmAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup) {
  int32_t      code = 0;
  STransAction action = {0};
  action.epSet = mndGetVgroupEpset(pMnode, pVgroup);

  mInfo("trans:%d, vgId:%d, build alter vnode confirm req", pTrans->id, pVgroup->vgId);
  int32_t   contLen = sizeof(SMsgHead);
  SMsgHead *pHead = taosMemoryMalloc(contLen);
  if (pHead == NULL) {
    TAOS_RETURN(terrno);
  }

  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(pVgroup->vgId);

  action.pCont = pHead;
  action.contLen = contLen;
  action.msgType = TDMT_VND_ALTER_CONFIRM;
  // incorrect redirect result will cause this erro
  action.retryCode = TSDB_CODE_VND_INVALID_VGROUP_ID;
  action.groupId = pVgroup->vgId;

  if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pHead);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

int32_t mndAddChangeConfigAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pOldVgroup, SVgObj *pNewVgroup,
                                 int32_t dnodeId) {
  int32_t      code = 0;
  STransAction action = {0};
  action.epSet = mndGetVgroupEpset(pMnode, pNewVgroup);

  int32_t contLen = 0;
  void   *pReq = mndBuildAlterVnodeReplicaReq(pMnode, pDb, pNewVgroup, dnodeId, &contLen);
  if (pReq == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  int32_t totallen = contLen + sizeof(SMsgHead);

  SMsgHead *pHead = taosMemoryMalloc(totallen);
  if (pHead == NULL) {
    taosMemoryFree(pReq);
    TAOS_RETURN(terrno);
  }

  pHead->contLen = htonl(totallen);
  pHead->vgId = htonl(pNewVgroup->vgId);

  memcpy((void *)(pHead + 1), pReq, contLen);
  taosMemoryFree(pReq);

  action.pCont = pHead;
  action.contLen = totallen;
  action.msgType = TDMT_SYNC_CONFIG_CHANGE;

  if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pHead);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

static int32_t mndAddAlterVnodeHashRangeAction(SMnode *pMnode, STrans *pTrans, int32_t srcVgId, SVgObj *pVgroup) {
  int32_t      code = 0;
  STransAction action = {0};
  action.epSet = mndGetVgroupEpset(pMnode, pVgroup);

  int32_t contLen = 0;
  void   *pReq = mndBuildAlterVnodeHashRangeReq(pMnode, srcVgId, pVgroup, &contLen);
  if (pReq == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_VND_ALTER_HASHRANGE;
  action.acceptableCode = TSDB_CODE_VND_ALREADY_EXIST;

  if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pReq);
    TAOS_RETURN(code);
  }

  mInfo("trans:%d, add alter vnode hash range action for from vgId:%d to vgId:%d", pTrans->id, srcVgId, pVgroup->vgId);
  TAOS_RETURN(code);
}

int32_t mndAddAlterVnodeConfigAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup) {
  int32_t      code = 0;
  STransAction action = {0};
  action.epSet = mndGetVgroupEpset(pMnode, pVgroup);

  int32_t contLen = 0;
  void   *pReq = mndBuildAlterVnodeConfigReq(pMnode, pDb, pVgroup, &contLen);
  if (pReq == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_VND_ALTER_CONFIG;
  action.groupId = pVgroup->vgId;

  if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pReq);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

int32_t mndAddNewVgPrepareAction(SMnode *pMnode, STrans *pTrans, SVgObj *pVg) {
  int32_t  code = 0;
  SSdbRaw *pRaw = mndVgroupActionEncode(pVg);
  if (pRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _err;
  }

  TAOS_CHECK_GOTO(mndTransAppendPrepareLog(pTrans, pRaw), NULL, _err);
  if (sdbSetRawStatus(pRaw, SDB_STATUS_CREATING) != 0) {
    mError("vgId:%d, failed to set raw status at line:%d", pVg->vgId, __LINE__);
  }
  if (code != 0) {
    mError("vgId:%d, failed to set raw status since %s at line:%d", pVg->vgId, tstrerror(code), __LINE__);
    TAOS_RETURN(code);
  }
  pRaw = NULL;
  TAOS_RETURN(code);

_err:
  sdbFreeRaw(pRaw);
  TAOS_RETURN(code);
}

int32_t mndAddAlterVnodeReplicaAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup, int32_t dnodeId) {
  int32_t    code = 0;
  SDnodeObj *pDnode = mndAcquireDnode(pMnode, dnodeId);
  if (pDnode == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  STransAction action = {0};
  action.epSet = mndGetDnodeEpset(pDnode);
  mndReleaseDnode(pMnode, pDnode);

  int32_t contLen = 0;
  void   *pReq = mndBuildAlterVnodeReplicaReq(pMnode, pDb, pVgroup, dnodeId, &contLen);
  if (pReq == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_VND_ALTER_REPLICA;
  action.groupId = pVgroup->vgId;

  if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pReq);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

int32_t mndAddCheckLearnerCatchupAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup, int32_t dnodeId) {
  int32_t    code = 0;
  SDnodeObj *pDnode = mndAcquireDnode(pMnode, dnodeId);
  if (pDnode == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  STransAction action = {0};
  action.epSet = mndGetDnodeEpset(pDnode);
  mndReleaseDnode(pMnode, pDnode);

  int32_t contLen = 0;
  void   *pReq = mndBuildCheckLearnCatchupReq(pMnode, pDb, pVgroup, dnodeId, &contLen);
  if (pReq == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_CHECK_VNODE_LEARNER_CATCHUP;
  action.acceptableCode = TSDB_CODE_VND_ALREADY_IS_VOTER;
  action.retryCode = TSDB_CODE_VND_NOT_CATCH_UP;

  if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pReq);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

int32_t mndAddAlterVnodeTypeAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup, int32_t dnodeId) {
  int32_t    code = 0;
  SDnodeObj *pDnode = mndAcquireDnode(pMnode, dnodeId);
  if (pDnode == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  STransAction action = {0};
  action.epSet = mndGetDnodeEpset(pDnode);
  mndReleaseDnode(pMnode, pDnode);

  int32_t contLen = 0;
  void   *pReq = mndBuildAlterVnodeReplicaReq(pMnode, pDb, pVgroup, dnodeId, &contLen);
  if (pReq == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_ALTER_VNODE_TYPE;
  action.acceptableCode = TSDB_CODE_VND_ALREADY_IS_VOTER;
  action.retryCode = TSDB_CODE_VND_NOT_CATCH_UP;
  action.groupId = pVgroup->vgId;

  if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pReq);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

int32_t mndRestoreAddAlterVnodeTypeAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup,
                                          SDnodeObj *pDnode) {
  int32_t      code = 0;
  STransAction action = {0};
  action.epSet = mndGetDnodeEpset(pDnode);

  int32_t contLen = 0;
  void   *pReq = mndBuildAlterVnodeReplicaReq(pMnode, pDb, pVgroup, pDnode->id, &contLen);
  if (pReq == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_ALTER_VNODE_TYPE;
  action.acceptableCode = TSDB_CODE_VND_ALREADY_IS_VOTER;
  action.retryCode = TSDB_CODE_VND_NOT_CATCH_UP;
  action.groupId = pVgroup->vgId;

  if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pReq);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

static int32_t mndAddDisableVnodeWriteAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup,
                                             int32_t dnodeId) {
  int32_t    code = 0;
  SDnodeObj *pDnode = mndAcquireDnode(pMnode, dnodeId);
  if (pDnode == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  STransAction action = {0};
  action.epSet = mndGetDnodeEpset(pDnode);
  mndReleaseDnode(pMnode, pDnode);

  int32_t contLen = 0;
  void   *pReq = mndBuildDisableVnodeWriteReq(pMnode, pDb, pVgroup->vgId, &contLen);
  if (pReq == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_VND_DISABLE_WRITE;

  if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pReq);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

int32_t mndAddDropVnodeAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup, SVnodeGid *pVgid,
                              bool isRedo) {
  int32_t      code = 0;
  STransAction action = {0};

  SDnodeObj *pDnode = mndAcquireDnode(pMnode, pVgid->dnodeId);
  if (pDnode == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  action.epSet = mndGetDnodeEpset(pDnode);
  mndReleaseDnode(pMnode, pDnode);

  int32_t contLen = 0;
  void   *pReq = mndBuildDropVnodeReq(pMnode, pDnode, pDb, pVgroup, &contLen);
  if (pReq == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_DROP_VNODE;
  action.acceptableCode = TSDB_CODE_VND_NOT_EXIST;
  action.groupId = pVgroup->vgId;

  if (isRedo) {
    if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
      taosMemoryFree(pReq);
      TAOS_RETURN(code);
    }
  } else {
    if ((code = mndTransAppendUndoAction(pTrans, &action)) != 0) {
      taosMemoryFree(pReq);
      TAOS_RETURN(code);
    }
  }

  TAOS_RETURN(code);
}

int32_t mndSetMoveVgroupInfoToTrans(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup, int32_t vnIndex,
                                    SArray *pArray, bool force, bool unsafe) {
  int32_t code = 0;
  SVgObj  newVg = {0};
  memcpy(&newVg, pVgroup, sizeof(SVgObj));

  mInfo("vgId:%d, trans:%d, vgroup info before move, replica:%d", newVg.vgId, pTrans->id, newVg.replica);
  for (int32_t i = 0; i < newVg.replica; ++i) {
    mInfo("vgId:%d, trans:%d, vnode:%d dnode:%d", newVg.vgId, pTrans->id, i, newVg.vnodeGid[i].dnodeId);
  }

  if (!force) {
#if 1
    {
#else
    if (newVg.replica == 1) {
#endif
      mInfo("vgId:%d, trans:%d, will add 1 vnode, replca:%d", pVgroup->vgId, pTrans->id, newVg.replica);
      TAOS_CHECK_RETURN(mndAddVnodeToVgroup(pMnode, pTrans, &newVg, pArray));
      for (int32_t i = 0; i < newVg.replica - 1; ++i) {
        TAOS_CHECK_RETURN(mndAddAlterVnodeReplicaAction(pMnode, pTrans, pDb, &newVg, newVg.vnodeGid[i].dnodeId));
      }
      TAOS_CHECK_RETURN(mndAddCreateVnodeAction(pMnode, pTrans, pDb, &newVg, &newVg.vnodeGid[newVg.replica - 1]));
      TAOS_CHECK_RETURN(mndAddAlterVnodeConfirmAction(pMnode, pTrans, pDb, &newVg));

      mInfo("vgId:%d, trans:%d, will remove 1 vnode, replca:2", pVgroup->vgId, pTrans->id);
      newVg.replica--;
      SVnodeGid del = newVg.vnodeGid[vnIndex];
      newVg.vnodeGid[vnIndex] = newVg.vnodeGid[newVg.replica];
      memset(&newVg.vnodeGid[newVg.replica], 0, sizeof(SVnodeGid));
      {
        SSdbRaw *pRaw = mndVgroupActionEncode(&newVg);
        if (pRaw == NULL) {
          code = TSDB_CODE_MND_RETURN_VALUE_NULL;
          if (terrno != 0) code = terrno;
          TAOS_RETURN(code);
        }
        if ((code = mndTransAppendGroupRedolog(pTrans, pRaw, pVgroup->vgId)) != 0) {
          sdbFreeRaw(pRaw);
          TAOS_RETURN(code);
        }
        code = sdbSetRawStatus(pRaw, SDB_STATUS_READY);
        if (code != 0) {
          mError("vgId:%d, failed to set raw status since %s at line:%d", newVg.vgId, tstrerror(code), __LINE__);
          return code;
        }
      }

      TAOS_CHECK_RETURN(mndAddDropVnodeAction(pMnode, pTrans, pDb, &newVg, &del, true));
      for (int32_t i = 0; i < newVg.replica; ++i) {
        TAOS_CHECK_RETURN(mndAddAlterVnodeReplicaAction(pMnode, pTrans, pDb, &newVg, newVg.vnodeGid[i].dnodeId));
      }
      TAOS_CHECK_RETURN(mndAddAlterVnodeConfirmAction(pMnode, pTrans, pDb, &newVg));
#if 1
    }
#else
    } else {  // new replica == 3
      mInfo("vgId:%d, will add 1 vnode, replca:3", pVgroup->vgId);
      if (mndAddVnodeToVgroup(pMnode, pTrans, &newVg, pArray) != 0) return -1;
      mInfo("vgId:%d, will remove 1 vnode, replca:4", pVgroup->vgId);
      newVg.replica--;
      SVnodeGid del = newVg.vnodeGid[vnIndex];
      newVg.vnodeGid[vnIndex] = newVg.vnodeGid[newVg.replica];
      memset(&newVg.vnodeGid[newVg.replica], 0, sizeof(SVnodeGid));
      {
        SSdbRaw *pRaw = mndVgroupActionEncode(&newVg);
        if (pRaw == NULL) return -1;
        if (mndTransAppendRedolog(pTrans, pRaw) != 0) {
          sdbFreeRaw(pRaw);
          return -1;
        }
      }

      if (mndAddDropVnodeAction(pMnode, pTrans, pDb, &newVg, &del, true) != 0) return -1;
      for (int32_t i = 0; i < newVg.replica; ++i) {
        if (i == vnIndex) continue;
        if (mndAddAlterVnodeReplicaAction(pMnode, pTrans, pDb, &newVg, newVg.vnodeGid[i].dnodeId) != 0) return -1;
      }
      if (mndAddCreateVnodeAction(pMnode, pTrans, pDb, &newVg, &newVg.vnodeGid[vnIndex]) != 0) return -1;
      if (mndAddAlterVnodeConfirmAction(pMnode, pTrans, pDb, &newVg) != 0) return -1;
    }
#endif
  } else {
    mInfo("vgId:%d, will add 1 vnode and force remove 1 vnode", pVgroup->vgId);
    TAOS_CHECK_RETURN(mndAddVnodeToVgroup(pMnode, pTrans, &newVg, pArray));
    newVg.replica--;
    // SVnodeGid del = newVg.vnodeGid[vnIndex];
    newVg.vnodeGid[vnIndex] = newVg.vnodeGid[newVg.replica];
    memset(&newVg.vnodeGid[newVg.replica], 0, sizeof(SVnodeGid));
    {
      SSdbRaw *pRaw = mndVgroupActionEncode(&newVg);
      if (pRaw == NULL) {
        code = TSDB_CODE_MND_RETURN_VALUE_NULL;
        if (terrno != 0) code = terrno;
        TAOS_RETURN(code);
      }
      if ((code = mndTransAppendGroupRedolog(pTrans, pRaw, pVgroup->vgId)) != 0) {
        sdbFreeRaw(pRaw);
        TAOS_RETURN(code);
      }
      code = sdbSetRawStatus(pRaw, SDB_STATUS_READY);
      if (code != 0) {
        mError("vgId:%d, failed to set raw status since %s at line:%d", newVg.vgId, tstrerror(code), __LINE__);
        return code;
      }
    }

    for (int32_t i = 0; i < newVg.replica; ++i) {
      if (i != vnIndex) {
        TAOS_CHECK_RETURN(mndAddAlterVnodeReplicaAction(pMnode, pTrans, pDb, &newVg, newVg.vnodeGid[i].dnodeId));
      }
    }
    TAOS_CHECK_RETURN(mndAddCreateVnodeAction(pMnode, pTrans, pDb, &newVg, &newVg.vnodeGid[vnIndex]));
    TAOS_CHECK_RETURN(mndAddAlterVnodeConfirmAction(pMnode, pTrans, pDb, &newVg));

    if (newVg.replica == 1) {
      if (force && !unsafe) {
        TAOS_RETURN(TSDB_CODE_VND_META_DATA_UNSAFE_DELETE);
      }

      SSdb *pSdb = pMnode->pSdb;
      void *pIter = NULL;

      while (1) {
        SStbObj *pStb = NULL;
        pIter = sdbFetch(pSdb, SDB_STB, pIter, (void **)&pStb);
        if (pIter == NULL) break;

        if (strcmp(pStb->db, pDb->name) == 0) {
          if ((code = mndSetForceDropCreateStbRedoActions(pMnode, pTrans, &newVg, pStb)) != 0) {
            sdbCancelFetch(pSdb, pIter);
            sdbRelease(pSdb, pStb);
            TAOS_RETURN(code);
          }
        }

        sdbRelease(pSdb, pStb);
      }

      mInfo("vgId:%d, all data is dropped since replica=1", pVgroup->vgId);
    }
  }

  {
    SSdbRaw *pRaw = mndVgroupActionEncode(&newVg);
    if (pRaw == NULL) {
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      TAOS_RETURN(code);
    }
    if ((code = mndTransAppendCommitlog(pTrans, pRaw)) != 0) {
      sdbFreeRaw(pRaw);
      TAOS_RETURN(code);
    }
    code = sdbSetRawStatus(pRaw, SDB_STATUS_READY);
    if (code != 0) {
      mError("vgId:%d, failed to set raw status since %s at line:%d", newVg.vgId, tstrerror(code), __LINE__);
      return code;
    }
  }

  mInfo("vgId:%d, vgroup info after move, replica:%d", newVg.vgId, newVg.replica);
  for (int32_t i = 0; i < newVg.replica; ++i) {
    mInfo("vgId:%d, vnode:%d dnode:%d", newVg.vgId, i, newVg.vnodeGid[i].dnodeId);
  }
  TAOS_RETURN(code);
}

int32_t mndSetMoveVgroupsInfoToTrans(SMnode *pMnode, STrans *pTrans, int32_t delDnodeId, bool force, bool unsafe) {
  int32_t code = 0;
  SArray *pArray = mndBuildDnodesArray(pMnode, delDnodeId, NULL);
  if (pArray == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

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
      mInfo("vgId:%d, trans:%d, vnode:%d will be removed from dnode:%d, force:%d", pVgroup->vgId, pTrans->id, vnIndex,
            delDnodeId, force);
      SDbObj *pDb = mndAcquireDb(pMnode, pVgroup->dbName);
      code = mndSetMoveVgroupInfoToTrans(pMnode, pTrans, pDb, pVgroup, vnIndex, pArray, force, unsafe);
      mndReleaseDb(pMnode, pDb);
    }

    sdbRelease(pMnode->pSdb, pVgroup);

    if (code != 0) {
      sdbCancelFetch(pMnode->pSdb, pIter);
      break;
    }
  }

  taosArrayDestroy(pArray);
  TAOS_RETURN(code);
}

static int32_t mndAddIncVgroupReplicaToTrans(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup,
                                             int32_t newDnodeId) {
  int32_t code = 0;
  mInfo("vgId:%d, will add 1 vnode, replica:%d dnode:%d", pVgroup->vgId, pVgroup->replica, newDnodeId);

  // assoc dnode
  SVnodeGid *pGid = &pVgroup->vnodeGid[pVgroup->replica];
  pVgroup->replica++;
  pGid->dnodeId = newDnodeId;
  pGid->syncState = TAOS_SYNC_STATE_OFFLINE;
  pGid->nodeRole = TAOS_SYNC_ROLE_LEARNER;

  SSdbRaw *pVgRaw = mndVgroupActionEncode(pVgroup);
  if (pVgRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  if ((code = mndTransAppendRedolog(pTrans, pVgRaw)) != 0) {
    sdbFreeRaw(pVgRaw);
    TAOS_RETURN(code);
  }
  code = sdbSetRawStatus(pVgRaw, SDB_STATUS_READY);
  if (code != 0) {
    mError("vgId:%d, failed to set raw status since %s at line:%d", pVgroup->vgId, tstrerror(code), __LINE__);
    TAOS_RETURN(code);
  }

  // learner
  for (int32_t i = 0; i < pVgroup->replica - 1; ++i) {
    TAOS_CHECK_RETURN(mndAddAlterVnodeReplicaAction(pMnode, pTrans, pDb, pVgroup, pVgroup->vnodeGid[i].dnodeId));
  }
  TAOS_CHECK_RETURN(mndAddCreateVnodeAction(pMnode, pTrans, pDb, pVgroup, pGid));

  // voter
  pGid->nodeRole = TAOS_SYNC_ROLE_VOTER;
  TAOS_CHECK_RETURN(mndAddAlterVnodeTypeAction(pMnode, pTrans, pDb, pVgroup, pGid->dnodeId));
  for (int32_t i = 0; i < pVgroup->replica - 1; ++i) {
    TAOS_CHECK_RETURN(mndAddAlterVnodeReplicaAction(pMnode, pTrans, pDb, pVgroup, pVgroup->vnodeGid[i].dnodeId));
  }

  // confirm
  TAOS_CHECK_RETURN(mndAddAlterVnodeConfirmAction(pMnode, pTrans, pDb, pVgroup));

  TAOS_RETURN(code);
}

static int32_t mndAddDecVgroupReplicaFromTrans(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup,
                                               int32_t delDnodeId) {
  int32_t code = 0;
  mInfo("vgId:%d, will remove 1 vnode, replica:%d dnode:%d", pVgroup->vgId, pVgroup->replica, delDnodeId);

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

  SSdbRaw *pVgRaw = mndVgroupActionEncode(pVgroup);
  if (pVgRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  if ((code = mndTransAppendRedolog(pTrans, pVgRaw)) != 0) {
    sdbFreeRaw(pVgRaw);
    TAOS_RETURN(code);
  }
  code = sdbSetRawStatus(pVgRaw, SDB_STATUS_READY);
  if (code != 0) {
    mError("vgId:%d, failed to set raw status since %s at line:%d", pVgroup->vgId, tstrerror(code), __LINE__);
    TAOS_RETURN(code);
  }

  TAOS_CHECK_RETURN(mndAddDropVnodeAction(pMnode, pTrans, pDb, pVgroup, &delGid, true));
  for (int32_t i = 0; i < pVgroup->replica; ++i) {
    TAOS_CHECK_RETURN(mndAddAlterVnodeReplicaAction(pMnode, pTrans, pDb, pVgroup, pVgroup->vnodeGid[i].dnodeId));
  }
  TAOS_CHECK_RETURN(mndAddAlterVnodeConfirmAction(pMnode, pTrans, pDb, pVgroup));

  TAOS_RETURN(code);
}

static int32_t mndRedistributeVgroup(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb, SVgObj *pVgroup, SDnodeObj *pNew1,
                                     SDnodeObj *pOld1, SDnodeObj *pNew2, SDnodeObj *pOld2, SDnodeObj *pNew3,
                                     SDnodeObj *pOld3) {
  int32_t code = -1;
  STrans *pTrans = NULL;

  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_GLOBAL, pReq, "red-vgroup");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }

  mndTransSetDbName(pTrans, pVgroup->dbName, NULL);
  TAOS_CHECK_GOTO(mndTransCheckConflictWithCompact(pMnode, pTrans), NULL, _OVER);

  mndTransSetSerial(pTrans);
  mInfo("trans:%d, used to redistribute vgroup, vgId:%d", pTrans->id, pVgroup->vgId);

  SVgObj newVg = {0};
  memcpy(&newVg, pVgroup, sizeof(SVgObj));
  mInfo("vgId:%d, vgroup info before redistribute, replica:%d", newVg.vgId, newVg.replica);
  for (int32_t i = 0; i < newVg.replica; ++i) {
    mInfo("vgId:%d, vnode:%d dnode:%d role:%s", newVg.vgId, i, newVg.vnodeGid[i].dnodeId,
          syncStr(newVg.vnodeGid[i].syncState));
  }

  if (pNew1 != NULL && pOld1 != NULL) {
    int32_t numOfVnodes = mndGetVnodesNum(pMnode, pNew1->id);
    if (numOfVnodes >= pNew1->numOfSupportVnodes) {
      mError("vgId:%d, no enough vnodes in dnode:%d, numOfVnodes:%d support:%d", newVg.vgId, pNew1->id, numOfVnodes,
             pNew1->numOfSupportVnodes);
      code = TSDB_CODE_MND_NO_ENOUGH_VNODES;
      goto _OVER;
    }

    int64_t vgMem = mndGetVgroupMemory(pMnode, NULL, pVgroup);
    if (pNew1->memAvail - vgMem - pNew1->memUsed <= 0) {
      mError("db:%s, vgId:%d, no enough memory:%" PRId64 " in dnode:%d avail:%" PRId64 " used:%" PRId64,
             pVgroup->dbName, pVgroup->vgId, vgMem, pNew1->id, pNew1->memAvail, pNew1->memUsed);
      code = TSDB_CODE_MND_NO_ENOUGH_MEM_IN_DNODE;
      goto _OVER;
    } else {
      pNew1->memUsed += vgMem;
    }

    TAOS_CHECK_GOTO(mndAddIncVgroupReplicaToTrans(pMnode, pTrans, pDb, &newVg, pNew1->id), NULL, _OVER);
    TAOS_CHECK_GOTO(mndAddDecVgroupReplicaFromTrans(pMnode, pTrans, pDb, &newVg, pOld1->id), NULL, _OVER);
  }

  if (pNew2 != NULL && pOld2 != NULL) {
    int32_t numOfVnodes = mndGetVnodesNum(pMnode, pNew2->id);
    if (numOfVnodes >= pNew2->numOfSupportVnodes) {
      mError("vgId:%d, no enough vnodes in dnode:%d, numOfVnodes:%d support:%d", newVg.vgId, pNew2->id, numOfVnodes,
             pNew2->numOfSupportVnodes);
      code = TSDB_CODE_MND_NO_ENOUGH_VNODES;
      goto _OVER;
    }
    int64_t vgMem = mndGetVgroupMemory(pMnode, NULL, pVgroup);
    if (pNew2->memAvail - vgMem - pNew2->memUsed <= 0) {
      mError("db:%s, vgId:%d, no enough memory:%" PRId64 " in dnode:%d avail:%" PRId64 " used:%" PRId64,
             pVgroup->dbName, pVgroup->vgId, vgMem, pNew2->id, pNew2->memAvail, pNew2->memUsed);
      code = TSDB_CODE_MND_NO_ENOUGH_MEM_IN_DNODE;
      goto _OVER;
    } else {
      pNew2->memUsed += vgMem;
    }
    TAOS_CHECK_GOTO(mndAddIncVgroupReplicaToTrans(pMnode, pTrans, pDb, &newVg, pNew2->id), NULL, _OVER);
    TAOS_CHECK_GOTO(mndAddDecVgroupReplicaFromTrans(pMnode, pTrans, pDb, &newVg, pOld2->id), NULL, _OVER);
  }

  if (pNew3 != NULL && pOld3 != NULL) {
    int32_t numOfVnodes = mndGetVnodesNum(pMnode, pNew3->id);
    if (numOfVnodes >= pNew3->numOfSupportVnodes) {
      mError("vgId:%d, no enough vnodes in dnode:%d, numOfVnodes:%d support:%d", newVg.vgId, pNew3->id, numOfVnodes,
             pNew3->numOfSupportVnodes);
      code = TSDB_CODE_MND_NO_ENOUGH_VNODES;
      goto _OVER;
    }
    int64_t vgMem = mndGetVgroupMemory(pMnode, NULL, pVgroup);
    if (pNew3->memAvail - vgMem - pNew3->memUsed <= 0) {
      mError("db:%s, vgId:%d, no enough memory:%" PRId64 " in dnode:%d avail:%" PRId64 " used:%" PRId64,
             pVgroup->dbName, pVgroup->vgId, vgMem, pNew3->id, pNew3->memAvail, pNew3->memUsed);
      code = TSDB_CODE_MND_NO_ENOUGH_MEM_IN_DNODE;
      goto _OVER;
    } else {
      pNew3->memUsed += vgMem;
    }
    TAOS_CHECK_GOTO(mndAddIncVgroupReplicaToTrans(pMnode, pTrans, pDb, &newVg, pNew3->id), NULL, _OVER);
    TAOS_CHECK_GOTO(mndAddDecVgroupReplicaFromTrans(pMnode, pTrans, pDb, &newVg, pOld3->id), NULL, _OVER);
  }

  {
    SSdbRaw *pRaw = mndVgroupActionEncode(&newVg);
    if (pRaw == NULL) {
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      goto _OVER;
    }
    if ((code = mndTransAppendCommitlog(pTrans, pRaw)) != 0) {
      sdbFreeRaw(pRaw);
      goto _OVER;
    }
    code = sdbSetRawStatus(pRaw, SDB_STATUS_READY);
    if (code != 0) {
      mError("vgId:%d, failed to set raw status since %s at line:%d", newVg.vgId, tstrerror(code), __LINE__);
      goto _OVER;
    }
  }

  mInfo("vgId:%d, vgroup info after redistribute, replica:%d", newVg.vgId, newVg.replica);
  for (int32_t i = 0; i < newVg.replica; ++i) {
    mInfo("vgId:%d, vnode:%d dnode:%d", newVg.vgId, i, newVg.vnodeGid[i].dnodeId);
  }

  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);
  code = 0;

_OVER:
  mndTransDrop(pTrans);
  mndReleaseDb(pMnode, pDb);
  TAOS_RETURN(code);
}

static int32_t mndProcessRedistributeVgroupMsg(SRpcMsg *pReq) {
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
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("vgId:%d, start to redistribute vgroup to dnode %d:%d:%d", req.vgId, req.dnodeId1, req.dnodeId2, req.dnodeId3);
  if ((code = mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_REDISTRIBUTE_VGROUP)) != 0) {
    goto _OVER;
  }

  pVgroup = mndAcquireVgroup(pMnode, req.vgId);
  if (pVgroup == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }
  if (pVgroup->mountVgId) {
    code = TSDB_CODE_MND_MOUNT_OBJ_NOT_SUPPORT;
    goto _OVER;
  }
  pDb = mndAcquireDb(pMnode, pVgroup->dbName);
  if (pDb == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }

  if (pVgroup->replica == 1) {
    if (req.dnodeId1 <= 0 || req.dnodeId2 > 0 || req.dnodeId3 > 0) {
      code = TSDB_CODE_MND_INVALID_REPLICA;
      goto _OVER;
    }

    if (req.dnodeId1 == pVgroup->vnodeGid[0].dnodeId) {
      // terrno = TSDB_CODE_MND_VGROUP_UN_CHANGED;
      code = 0;
      goto _OVER;
    }

    pNew1 = mndAcquireDnode(pMnode, req.dnodeId1);
    if (pNew1 == NULL) {
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      goto _OVER;
    }
    if (!mndIsDnodeOnline(pNew1, curMs)) {
      code = TSDB_CODE_MND_HAS_OFFLINE_DNODE;
      goto _OVER;
    }

    pOld1 = mndAcquireDnode(pMnode, pVgroup->vnodeGid[0].dnodeId);
    if (pOld1 == NULL) {
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      goto _OVER;
    }
    if (!mndIsDnodeOnline(pOld1, curMs)) {
      code = TSDB_CODE_MND_HAS_OFFLINE_DNODE;
      goto _OVER;
    }

    code = mndRedistributeVgroup(pMnode, pReq, pDb, pVgroup, pNew1, pOld1, NULL, NULL, NULL, NULL);

  } else if (pVgroup->replica == 3) {
    if (req.dnodeId1 <= 0 || req.dnodeId2 <= 0 || req.dnodeId3 <= 0) {
      code = TSDB_CODE_MND_INVALID_REPLICA;
      goto _OVER;
    }

    if (req.dnodeId1 == req.dnodeId2 || req.dnodeId1 == req.dnodeId3 || req.dnodeId2 == req.dnodeId3) {
      code = TSDB_CODE_MND_INVALID_REPLICA;
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
      if (pNew1 == NULL) {
        code = TSDB_CODE_MND_RETURN_VALUE_NULL;
        if (terrno != 0) code = terrno;
        goto _OVER;
      }
      if (!mndIsDnodeOnline(pNew1, curMs)) {
        code = TSDB_CODE_MND_HAS_OFFLINE_DNODE;
        goto _OVER;
      }
    }

    if (newDnodeId[1] != 0) {
      pNew2 = mndAcquireDnode(pMnode, newDnodeId[1]);
      if (pNew2 == NULL) {
        code = TSDB_CODE_MND_RETURN_VALUE_NULL;
        if (terrno != 0) code = terrno;
        goto _OVER;
      }
      if (!mndIsDnodeOnline(pNew2, curMs)) {
        code = TSDB_CODE_MND_HAS_OFFLINE_DNODE;
        goto _OVER;
      }
    }

    if (newDnodeId[2] != 0) {
      pNew3 = mndAcquireDnode(pMnode, newDnodeId[2]);
      if (pNew3 == NULL) {
        code = TSDB_CODE_MND_RETURN_VALUE_NULL;
        if (terrno != 0) code = terrno;
        goto _OVER;
      }
      if (!mndIsDnodeOnline(pNew3, curMs)) {
        code = TSDB_CODE_MND_HAS_OFFLINE_DNODE;
        goto _OVER;
      }
    }

    if (oldDnodeId[0] != 0) {
      pOld1 = mndAcquireDnode(pMnode, oldDnodeId[0]);
      if (pOld1 == NULL) {
        code = TSDB_CODE_MND_RETURN_VALUE_NULL;
        if (terrno != 0) code = terrno;
        goto _OVER;
      }
      if (!mndIsDnodeOnline(pOld1, curMs)) {
        code = TSDB_CODE_MND_HAS_OFFLINE_DNODE;
        goto _OVER;
      }
    }

    if (oldDnodeId[1] != 0) {
      pOld2 = mndAcquireDnode(pMnode, oldDnodeId[1]);
      if (pOld2 == NULL) {
        code = TSDB_CODE_MND_RETURN_VALUE_NULL;
        if (terrno != 0) code = terrno;
        goto _OVER;
      }
      if (!mndIsDnodeOnline(pOld2, curMs)) {
        code = TSDB_CODE_MND_HAS_OFFLINE_DNODE;
        goto _OVER;
      }
    }

    if (oldDnodeId[2] != 0) {
      pOld3 = mndAcquireDnode(pMnode, oldDnodeId[2]);
      if (pOld3 == NULL) {
        code = TSDB_CODE_MND_RETURN_VALUE_NULL;
        if (terrno != 0) code = terrno;
        goto _OVER;
      }
      if (!mndIsDnodeOnline(pOld3, curMs)) {
        code = TSDB_CODE_MND_HAS_OFFLINE_DNODE;
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
    code = TSDB_CODE_MND_REQ_REJECTED;
    goto _OVER;
  }

  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  char obj[33] = {0};
  (void)tsnprintf(obj, sizeof(obj), "%d", req.vgId);

  auditRecord(pReq, pMnode->clusterId, "RedistributeVgroup", "", obj, req.sql, req.sqlLen);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("vgId:%d, failed to redistribute to dnode %d:%d:%d since %s", req.vgId, req.dnodeId1, req.dnodeId2,
           req.dnodeId3, tstrerror(code));
  }

  mndReleaseDnode(pMnode, pNew1);
  mndReleaseDnode(pMnode, pNew2);
  mndReleaseDnode(pMnode, pNew3);
  mndReleaseDnode(pMnode, pOld1);
  mndReleaseDnode(pMnode, pOld2);
  mndReleaseDnode(pMnode, pOld3);
  mndReleaseVgroup(pMnode, pVgroup);
  mndReleaseDb(pMnode, pDb);
  tFreeSRedistributeVgroupReq(&req);

  TAOS_RETURN(code);
}

static void *mndBuildSForceBecomeFollowerReq(SMnode *pMnode, SVgObj *pVgroup, int32_t dnodeId, int32_t *pContLen) {
  SForceBecomeFollowerReq balanceReq = {
      .vgId = pVgroup->vgId,
  };

  int32_t contLen = tSerializeSForceBecomeFollowerReq(NULL, 0, &balanceReq);
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

  if (tSerializeSForceBecomeFollowerReq((char *)pReq + sizeof(SMsgHead), contLen, &balanceReq) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    taosMemoryFree(pReq);
    return NULL;
  }
  *pContLen = contLen;
  return pReq;
}

int32_t mndAddBalanceVgroupLeaderAction(SMnode *pMnode, STrans *pTrans, SVgObj *pVgroup, int32_t dnodeId) {
  int32_t    code = 0;
  SDnodeObj *pDnode = mndAcquireDnode(pMnode, dnodeId);
  if (pDnode == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  STransAction action = {0};
  action.epSet = mndGetDnodeEpset(pDnode);
  mndReleaseDnode(pMnode, pDnode);

  int32_t contLen = 0;
  void   *pReq = mndBuildSForceBecomeFollowerReq(pMnode, pVgroup, dnodeId, &contLen);
  if (pReq == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_SYNC_FORCE_FOLLOWER;

  if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pReq);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

int32_t mndAddVgroupBalanceToTrans(SMnode *pMnode, SVgObj *pVgroup, STrans *pTrans) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;

  int32_t vgid = pVgroup->vgId;
  int8_t  replica = pVgroup->replica;

  if (pVgroup->replica <= 1) {
    mInfo("trans:%d, vgid:%d no need to balance, replica:%d", pTrans->id, vgid, replica);
    return -1;
  }

  int32_t dnodeId = 0;

  for (int i = 0; i < replica; i++) {
    if (pVgroup->vnodeGid[i].syncState == TAOS_SYNC_STATE_LEADER) {
      dnodeId = pVgroup->vnodeGid[i].dnodeId;
      break;
    }
  }

  bool       exist = false;
  bool       online = false;
  int64_t    curMs = taosGetTimestampMs();
  SDnodeObj *pDnode = mndAcquireDnode(pMnode, dnodeId);
  if (pDnode != NULL) {
    exist = true;
    online = mndIsDnodeOnline(pDnode, curMs);
    mndReleaseDnode(pMnode, pDnode);
  }

  if (exist && online) {
    mInfo("trans:%d, vgid:%d leader to dnode:%d", pTrans->id, vgid, dnodeId);

    if ((code = mndAddBalanceVgroupLeaderAction(pMnode, pTrans, pVgroup, dnodeId)) != 0) {
      mError("trans:%d, vgid:%d failed to be balanced to dnode:%d", pTrans->id, vgid, dnodeId);
      TAOS_RETURN(code);
    }

    SDbObj *pDb = mndAcquireDb(pMnode, pVgroup->dbName);
    if (pDb == NULL) {
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      mError("trans:%d, vgid:%d failed to be balanced to dnode:%d, because db not exist", pTrans->id, vgid, dnodeId);
      TAOS_RETURN(code);
    }

    mndReleaseDb(pMnode, pDb);
  } else {
    mInfo("trans:%d, vgid:%d cant be balanced to dnode:%d, exist:%d, online:%d", pTrans->id, vgid, dnodeId, exist,
          online);
  }

  TAOS_RETURN(code);
}

extern int32_t mndProcessVgroupBalanceLeaderMsgImp(SRpcMsg *pReq);

int32_t mndProcessVgroupBalanceLeaderMsg(SRpcMsg *pReq) { return mndProcessVgroupBalanceLeaderMsgImp(pReq); }

#ifndef TD_ENTERPRISE
int32_t mndProcessVgroupBalanceLeaderMsgImp(SRpcMsg *pReq) { return 0; }
#endif

static int32_t mndCheckDnodeMemory(SMnode *pMnode, SDbObj *pOldDb, SDbObj *pNewDb, SVgObj *pOldVgroup,
                                   SVgObj *pNewVgroup, SArray *pArray) {
  for (int32_t i = 0; i < (int32_t)taosArrayGetSize(pArray); ++i) {
    SDnodeObj *pDnode = taosArrayGet(pArray, i);
    bool       inVgroup = false;
    int64_t    oldMemUsed = 0;
    int64_t    newMemUsed = 0;
    mDebug("db:%s, vgId:%d, check dnode:%d, avail:%" PRId64 " used:%" PRId64, pNewVgroup->dbName, pNewVgroup->vgId,
           pDnode->id, pDnode->memAvail, pDnode->memUsed);
    for (int32_t j = 0; j < pOldVgroup->replica; ++j) {
      SVnodeGid *pVgId = &pOldVgroup->vnodeGid[j];
      if (pDnode->id == pVgId->dnodeId) {
        oldMemUsed = mndGetVgroupMemory(pMnode, pOldDb, pOldVgroup);
        inVgroup = true;
      }
    }
    for (int32_t j = 0; j < pNewVgroup->replica; ++j) {
      SVnodeGid *pVgId = &pNewVgroup->vnodeGid[j];
      if (pDnode->id == pVgId->dnodeId) {
        newMemUsed = mndGetVgroupMemory(pMnode, pNewDb, pNewVgroup);
        inVgroup = true;
      }
    }

    mDebug("db:%s, vgId:%d, memory in dnode:%d, oldUsed:%" PRId64 ", newUsed:%" PRId64, pNewVgroup->dbName,
           pNewVgroup->vgId, pDnode->id, oldMemUsed, newMemUsed);

    pDnode->memUsed = pDnode->memUsed - oldMemUsed + newMemUsed;
    if (pDnode->memAvail - pDnode->memUsed <= 0) {
      mError("db:%s, vgId:%d, no enough memory in dnode:%d, avail:%" PRId64 " used:%" PRId64, pNewVgroup->dbName,
             pNewVgroup->vgId, pDnode->id, pDnode->memAvail, pDnode->memUsed);
      TAOS_RETURN(TSDB_CODE_MND_NO_ENOUGH_MEM_IN_DNODE);
    } else if (inVgroup) {
      mInfo("db:%s, vgId:%d, memory in dnode:%d, avail:%" PRId64 " used:%" PRId64, pNewVgroup->dbName, pNewVgroup->vgId,
            pDnode->id, pDnode->memAvail, pDnode->memUsed);
    } else {
    }
  }
  return 0;
}

int32_t mndBuildAlterVgroupAction(SMnode *pMnode, STrans *pTrans, SDbObj *pOldDb, SDbObj *pNewDb, SVgObj *pVgroup,
                                  SArray *pArray, SVgObj *pNewVgroup) {
  int32_t code = 0;
  memcpy(pNewVgroup, pVgroup, sizeof(SVgObj));

  if (pVgroup->replica <= 0 || pVgroup->replica == pNewDb->cfg.replications) {
    TAOS_CHECK_RETURN(mndAddAlterVnodeConfigAction(pMnode, pTrans, pNewDb, pVgroup));
    TAOS_CHECK_RETURN(mndCheckDnodeMemory(pMnode, pOldDb, pNewDb, pNewVgroup, pVgroup, pArray));
    return 0;
  }

  // mndTransSetGroupParallel(pTrans);

  if (pNewDb->cfg.replications == 3) {
    mInfo("trans:%d, db:%s, vgId:%d, will add 2 vnodes, vn:0 dnode:%d", pTrans->id, pVgroup->dbName, pVgroup->vgId,
          pVgroup->vnodeGid[0].dnodeId);

    // add second
    if (pNewVgroup->replica == 1) {
      TAOS_CHECK_RETURN(mndAddVnodeToVgroup(pMnode, pTrans, pNewVgroup, pArray));
    }

    // learner stage
    pNewVgroup->vnodeGid[0].nodeRole = TAOS_SYNC_ROLE_VOTER;
    pNewVgroup->vnodeGid[1].nodeRole = TAOS_SYNC_ROLE_LEARNER;
    TAOS_CHECK_RETURN(
        mndAddAlterVnodeReplicaAction(pMnode, pTrans, pNewDb, pNewVgroup, pNewVgroup->vnodeGid[0].dnodeId));

    TAOS_CHECK_RETURN(mndAddCreateVnodeAction(pMnode, pTrans, pNewDb, pNewVgroup, &pNewVgroup->vnodeGid[1]));

    // follower stage
    pNewVgroup->vnodeGid[1].nodeRole = TAOS_SYNC_ROLE_VOTER;
    TAOS_CHECK_RETURN(mndAddAlterVnodeTypeAction(pMnode, pTrans, pNewDb, pNewVgroup, pNewVgroup->vnodeGid[1].dnodeId));
    TAOS_CHECK_RETURN(
        mndAddAlterVnodeReplicaAction(pMnode, pTrans, pNewDb, pNewVgroup, pNewVgroup->vnodeGid[0].dnodeId));

    TAOS_CHECK_RETURN(mndAddAlterVnodeConfirmAction(pMnode, pTrans, pNewDb, pNewVgroup));

    // add third
    if (pNewVgroup->replica == 2) {
      TAOS_CHECK_RETURN(mndAddVnodeToVgroup(pMnode, pTrans, pNewVgroup, pArray));
    }

    pNewVgroup->vnodeGid[0].nodeRole = TAOS_SYNC_ROLE_VOTER;
    pNewVgroup->vnodeGid[1].nodeRole = TAOS_SYNC_ROLE_VOTER;
    pNewVgroup->vnodeGid[2].nodeRole = TAOS_SYNC_ROLE_VOTER;
    TAOS_CHECK_RETURN(
        mndAddAlterVnodeReplicaAction(pMnode, pTrans, pNewDb, pNewVgroup, pNewVgroup->vnodeGid[0].dnodeId));
    TAOS_CHECK_RETURN(
        mndAddAlterVnodeReplicaAction(pMnode, pTrans, pNewDb, pNewVgroup, pNewVgroup->vnodeGid[1].dnodeId));
    TAOS_CHECK_RETURN(mndAddCreateVnodeAction(pMnode, pTrans, pNewDb, pNewVgroup, &pNewVgroup->vnodeGid[2]));

    TAOS_CHECK_RETURN(mndAddAlterVnodeConfirmAction(pMnode, pTrans, pNewDb, pNewVgroup));
  } else if (pNewDb->cfg.replications == 1) {
    mInfo("trans:%d, db:%s, vgId:%d, will remove 2 vnodes, vn:0 dnode:%d vn:1 dnode:%d vn:2 dnode:%d", pTrans->id,
          pVgroup->dbName, pVgroup->vgId, pVgroup->vnodeGid[0].dnodeId, pVgroup->vnodeGid[1].dnodeId,
          pVgroup->vnodeGid[2].dnodeId);

    SVnodeGid del1 = {0};
    SVnodeGid del2 = {0};
    TAOS_CHECK_RETURN(mndRemoveVnodeFromVgroup(pMnode, pTrans, pNewVgroup, pArray, &del1));
    TAOS_CHECK_RETURN(mndAddDropVnodeAction(pMnode, pTrans, pNewDb, pNewVgroup, &del1, true));
    TAOS_CHECK_RETURN(
        mndAddAlterVnodeReplicaAction(pMnode, pTrans, pNewDb, pNewVgroup, pNewVgroup->vnodeGid[0].dnodeId));
    TAOS_CHECK_RETURN(
        mndAddAlterVnodeReplicaAction(pMnode, pTrans, pNewDb, pNewVgroup, pNewVgroup->vnodeGid[1].dnodeId));
    TAOS_CHECK_RETURN(mndAddAlterVnodeConfirmAction(pMnode, pTrans, pNewDb, pNewVgroup));

    TAOS_CHECK_RETURN(mndRemoveVnodeFromVgroup(pMnode, pTrans, pNewVgroup, pArray, &del2));
    TAOS_CHECK_RETURN(mndAddDropVnodeAction(pMnode, pTrans, pNewDb, pNewVgroup, &del2, true));
    TAOS_CHECK_RETURN(
        mndAddAlterVnodeReplicaAction(pMnode, pTrans, pNewDb, pNewVgroup, pNewVgroup->vnodeGid[0].dnodeId));
    TAOS_CHECK_RETURN(mndAddAlterVnodeConfirmAction(pMnode, pTrans, pNewDb, pNewVgroup));
  } else if (pNewDb->cfg.replications == 2) {
    mInfo("trans:%d, db:%s, vgId:%d, will add 1 vnode, vn:0 dnode:%d", pTrans->id, pVgroup->dbName, pVgroup->vgId,
          pVgroup->vnodeGid[0].dnodeId);

    // add second
    TAOS_CHECK_RETURN(mndAddVnodeToVgroup(pMnode, pTrans, pNewVgroup, pArray));

    // learner stage
    pNewVgroup->vnodeGid[0].nodeRole = TAOS_SYNC_ROLE_VOTER;
    pNewVgroup->vnodeGid[1].nodeRole = TAOS_SYNC_ROLE_LEARNER;
    TAOS_CHECK_RETURN(
        mndAddAlterVnodeReplicaAction(pMnode, pTrans, pNewDb, pNewVgroup, pNewVgroup->vnodeGid[0].dnodeId));

    TAOS_CHECK_RETURN(mndAddCreateVnodeAction(pMnode, pTrans, pNewDb, pNewVgroup, &pNewVgroup->vnodeGid[1]));

    // follower stage
    pNewVgroup->vnodeGid[1].nodeRole = TAOS_SYNC_ROLE_VOTER;
    TAOS_CHECK_RETURN(mndAddAlterVnodeTypeAction(pMnode, pTrans, pNewDb, pNewVgroup, pNewVgroup->vnodeGid[1].dnodeId));
    TAOS_CHECK_RETURN(
        mndAddAlterVnodeReplicaAction(pMnode, pTrans, pNewDb, pNewVgroup, pNewVgroup->vnodeGid[0].dnodeId));

    TAOS_CHECK_RETURN(mndAddAlterVnodeConfirmAction(pMnode, pTrans, pNewDb, pNewVgroup));
  } else {
    return -1;
  }

  mndSortVnodeGid(pNewVgroup);

  {
    SSdbRaw *pVgRaw = mndVgroupActionEncode(pNewVgroup);
    if (pVgRaw == NULL) {
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      TAOS_RETURN(code);
    }
    if ((code = mndTransAppendCommitlog(pTrans, pVgRaw)) != 0) {
      sdbFreeRaw(pVgRaw);
      TAOS_RETURN(code);
    }
    code = sdbSetRawStatus(pVgRaw, SDB_STATUS_READY);
    if (code != 0) {
      mError("vgId:%d, failed to set raw status since %s at line:%d", pNewVgroup->vgId, tstrerror(code), __LINE__);
      TAOS_RETURN(code);
    }
  }

  TAOS_RETURN(code);
}

int32_t mndBuildRaftAlterVgroupAction(SMnode *pMnode, STrans *pTrans, SDbObj *pOldDb, SDbObj *pNewDb, SVgObj *pVgroup,
                                      SArray *pArray) {
  int32_t code = 0;
  SVgObj  newVgroup = {0};
  memcpy(&newVgroup, pVgroup, sizeof(SVgObj));

  if (pVgroup->replica <= 0 || pVgroup->replica == pNewDb->cfg.replications) {
    TAOS_CHECK_RETURN(mndAddAlterVnodeConfigAction(pMnode, pTrans, pNewDb, pVgroup));
    TAOS_CHECK_RETURN(mndCheckDnodeMemory(pMnode, pOldDb, pNewDb, &newVgroup, pVgroup, pArray));
    return 0;
  }

  mndTransSetSerial(pTrans);

  mInfo("trans:%d, vgId:%d, alter vgroup, syncConfChangeVer:%d, version:%d, replica:%d", pTrans->id, pVgroup->vgId,
        pVgroup->syncConfChangeVer, pVgroup->version, pVgroup->replica);

  if (newVgroup.replica == 1 && pNewDb->cfg.replications == 3) {
    mInfo("db:%s, vgId:%d, will add 2 vnodes, vn:0 dnode:%d", pVgroup->dbName, pVgroup->vgId,
          pVgroup->vnodeGid[0].dnodeId);

    // add second
    TAOS_CHECK_RETURN(mndAddVnodeToVgroup(pMnode, pTrans, &newVgroup, pArray));
    // add third
    TAOS_CHECK_RETURN(mndAddVnodeToVgroup(pMnode, pTrans, &newVgroup, pArray));

    // add learner stage
    newVgroup.vnodeGid[0].nodeRole = TAOS_SYNC_ROLE_VOTER;
    newVgroup.vnodeGid[1].nodeRole = TAOS_SYNC_ROLE_LEARNER;
    newVgroup.vnodeGid[2].nodeRole = TAOS_SYNC_ROLE_LEARNER;
    TAOS_CHECK_RETURN(
        mndAddChangeConfigAction(pMnode, pTrans, pNewDb, pVgroup, &newVgroup, newVgroup.vnodeGid[0].dnodeId));
    mInfo("trans:%d, vgId:%d, add change config, syncConfChangeVer:%d, version:%d, replica:%d", pTrans->id,
          pVgroup->vgId, newVgroup.syncConfChangeVer, pVgroup->version, pVgroup->replica);
    TAOS_CHECK_RETURN(mndAddCreateVnodeAction(pMnode, pTrans, pNewDb, &newVgroup, &newVgroup.vnodeGid[1]));
    mInfo("trans:%d, vgId:%d, create vnode, syncConfChangeVer:%d, version:%d, replica:%d", pTrans->id, pVgroup->vgId,
          newVgroup.syncConfChangeVer, pVgroup->version, pVgroup->replica);
    TAOS_CHECK_RETURN(mndAddCreateVnodeAction(pMnode, pTrans, pNewDb, &newVgroup, &newVgroup.vnodeGid[2]));
    mInfo("trans:%d, vgId:%d, create vnode, syncConfChangeVer:%d, version:%d, replica:%d", pTrans->id, pVgroup->vgId,
          newVgroup.syncConfChangeVer, pVgroup->version, pVgroup->replica);

    // check learner
    newVgroup.vnodeGid[0].nodeRole = TAOS_SYNC_ROLE_VOTER;
    newVgroup.vnodeGid[1].nodeRole = TAOS_SYNC_ROLE_VOTER;
    newVgroup.vnodeGid[2].nodeRole = TAOS_SYNC_ROLE_VOTER;
    TAOS_CHECK_RETURN(
        mndAddCheckLearnerCatchupAction(pMnode, pTrans, pNewDb, &newVgroup, newVgroup.vnodeGid[1].dnodeId));
    TAOS_CHECK_RETURN(
        mndAddCheckLearnerCatchupAction(pMnode, pTrans, pNewDb, &newVgroup, newVgroup.vnodeGid[2].dnodeId));

    // change raft type
    newVgroup.vnodeGid[0].nodeRole = TAOS_SYNC_ROLE_VOTER;
    newVgroup.vnodeGid[1].nodeRole = TAOS_SYNC_ROLE_VOTER;
    newVgroup.vnodeGid[2].nodeRole = TAOS_SYNC_ROLE_LEARNER;
    TAOS_CHECK_RETURN(
        mndAddChangeConfigAction(pMnode, pTrans, pNewDb, pVgroup, &newVgroup, newVgroup.vnodeGid[0].dnodeId));

    TAOS_CHECK_RETURN(mndAddAlterVnodeConfirmAction(pMnode, pTrans, pNewDb, &newVgroup));

    newVgroup.vnodeGid[0].nodeRole = TAOS_SYNC_ROLE_VOTER;
    newVgroup.vnodeGid[1].nodeRole = TAOS_SYNC_ROLE_VOTER;
    newVgroup.vnodeGid[2].nodeRole = TAOS_SYNC_ROLE_VOTER;
    TAOS_CHECK_RETURN(
        mndAddChangeConfigAction(pMnode, pTrans, pNewDb, pVgroup, &newVgroup, newVgroup.vnodeGid[0].dnodeId));

    TAOS_CHECK_RETURN(mndAddAlterVnodeConfirmAction(pMnode, pTrans, pNewDb, &newVgroup));

    SSdbRaw *pVgRaw = mndVgroupActionEncode(&newVgroup);
    if (pVgRaw == NULL) {
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      TAOS_RETURN(code);
    }
    if ((code = mndTransAppendRedolog(pTrans, pVgRaw)) != 0) {
      sdbFreeRaw(pVgRaw);
      TAOS_RETURN(code);
    }
    code = sdbSetRawStatus(pVgRaw, SDB_STATUS_READY);
    if (code != 0) {
      mError("vgId:%d, failed to set raw status to ready, error:%s, line:%d", newVgroup.vgId, tstrerror(code),
             __LINE__);
      TAOS_RETURN(code);
    }
  } else if (newVgroup.replica == 3 && pNewDb->cfg.replications == 1) {
    mInfo("db:%s, vgId:%d, will remove 2 vnodes, vn:0 dnode:%d vn:1 dnode:%d vn:2 dnode:%d", pVgroup->dbName,
          pVgroup->vgId, pVgroup->vnodeGid[0].dnodeId, pVgroup->vnodeGid[1].dnodeId, pVgroup->vnodeGid[2].dnodeId);

    SVnodeGid del1 = {0};
    TAOS_CHECK_RETURN(mndRemoveVnodeFromVgroupWithoutSave(pMnode, pTrans, &newVgroup, pArray, &del1));

    TAOS_CHECK_RETURN(
        mndAddChangeConfigAction(pMnode, pTrans, pNewDb, pVgroup, &newVgroup, newVgroup.vnodeGid[0].dnodeId));

    TAOS_CHECK_RETURN(mndAddAlterVnodeConfirmAction(pMnode, pTrans, pNewDb, &newVgroup));

    TAOS_CHECK_RETURN(mndAddDropVnodeAction(pMnode, pTrans, pNewDb, &newVgroup, &del1, true));

    SSdbRaw *pVgRaw = mndVgroupActionEncode(&newVgroup);
    if (pVgRaw == NULL) {
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      TAOS_RETURN(code);
    }
    if ((code = mndTransAppendRedolog(pTrans, pVgRaw)) != 0) {
      sdbFreeRaw(pVgRaw);
      TAOS_RETURN(code);
    }
    code = sdbSetRawStatus(pVgRaw, SDB_STATUS_READY);
    if (code != 0) {
      mError("vgId:%d, failed to set raw status to ready, error:%s, line:%d", newVgroup.vgId, tstrerror(code),
             __LINE__);
      TAOS_RETURN(code);
    }

    SVnodeGid del2 = {0};
    TAOS_CHECK_RETURN(mndRemoveVnodeFromVgroupWithoutSave(pMnode, pTrans, &newVgroup, pArray, &del2));

    TAOS_CHECK_RETURN(
        mndAddChangeConfigAction(pMnode, pTrans, pNewDb, pVgroup, &newVgroup, newVgroup.vnodeGid[0].dnodeId));

    TAOS_CHECK_RETURN(mndAddAlterVnodeConfirmAction(pMnode, pTrans, pNewDb, &newVgroup));

    TAOS_CHECK_RETURN(mndAddDropVnodeAction(pMnode, pTrans, pNewDb, &newVgroup, &del2, true));

    pVgRaw = mndVgroupActionEncode(&newVgroup);
    if (pVgRaw == NULL) {
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      TAOS_RETURN(code);
    }
    if ((code = mndTransAppendRedolog(pTrans, pVgRaw)) != 0) {
      sdbFreeRaw(pVgRaw);
      TAOS_RETURN(code);
    }
    code = sdbSetRawStatus(pVgRaw, SDB_STATUS_READY);
    if (code != 0) {
      mError("vgId:%d, failed to set raw status to ready, error:%s, line:%d", newVgroup.vgId, tstrerror(code),
             __LINE__);
      TAOS_RETURN(code);
    }
  } else {
    return -1;
  }

  mndSortVnodeGid(&newVgroup);

  {
    SSdbRaw *pVgRaw = mndVgroupActionEncode(&newVgroup);
    if (pVgRaw == NULL) {
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      TAOS_RETURN(code);
    }
    if ((code = mndTransAppendCommitlog(pTrans, pVgRaw)) != 0) {
      sdbFreeRaw(pVgRaw);
      TAOS_RETURN(code);
    }
    code = sdbSetRawStatus(pVgRaw, SDB_STATUS_READY);
    if (code != 0) {
      mError("vgId:%d, failed to set raw status to ready, error:%s, line:%d", newVgroup.vgId, tstrerror(code),
             __LINE__);
      TAOS_RETURN(code);
    }
  }

  TAOS_RETURN(code);
}

int32_t mndBuildRestoreAlterVgroupAction(SMnode *pMnode, STrans *pTrans, SDbObj *db, SVgObj *pVgroup, SDnodeObj *pDnode,
                                         SDnodeObj *pAnotherDnode) {
  int32_t code = 0;
  SVgObj  newVgroup = {0};
  memcpy(&newVgroup, pVgroup, sizeof(SVgObj));

  mInfo("trans:%d, db:%s, vgId:%d, restore vnodes, vn:0 dnode:%d", pTrans->id, pVgroup->dbName, pVgroup->vgId,
        pVgroup->vnodeGid[0].dnodeId);

  if (newVgroup.replica == 1) {
    int selected = 0;
    for (int i = 0; i < newVgroup.replica; i++) {
      newVgroup.vnodeGid[i].nodeRole = TAOS_SYNC_ROLE_VOTER;
      if (newVgroup.vnodeGid[i].dnodeId == pDnode->id) {
        selected = i;
      }
    }
    TAOS_CHECK_RETURN(mndAddCreateVnodeAction(pMnode, pTrans, db, &newVgroup, &newVgroup.vnodeGid[selected]));
  } else if (newVgroup.replica == 2) {
    for (int i = 0; i < newVgroup.replica; i++) {
      if (newVgroup.vnodeGid[i].dnodeId == pDnode->id) {
        newVgroup.vnodeGid[i].nodeRole = TAOS_SYNC_ROLE_LEARNER;
      } else {
        newVgroup.vnodeGid[i].nodeRole = TAOS_SYNC_ROLE_VOTER;
      }
    }
    TAOS_CHECK_RETURN(mndRestoreAddAlterVnodeTypeAction(pMnode, pTrans, db, &newVgroup, pAnotherDnode));

    for (int i = 0; i < newVgroup.replica; i++) {
      if (newVgroup.vnodeGid[i].dnodeId == pDnode->id) {
        newVgroup.vnodeGid[i].nodeRole = TAOS_SYNC_ROLE_LEARNER;
      } else {
        newVgroup.vnodeGid[i].nodeRole = TAOS_SYNC_ROLE_VOTER;
      }
    }
    TAOS_CHECK_RETURN(mndRestoreAddCreateVnodeAction(pMnode, pTrans, db, &newVgroup, pDnode));

    for (int i = 0; i < newVgroup.replica; i++) {
      newVgroup.vnodeGid[i].nodeRole = TAOS_SYNC_ROLE_VOTER;
      if (newVgroup.vnodeGid[i].dnodeId == pDnode->id) {
      }
    }
    TAOS_CHECK_RETURN(mndRestoreAddAlterVnodeTypeAction(pMnode, pTrans, db, &newVgroup, pDnode));
    TAOS_CHECK_RETURN(mndRestoreAddAlterVnodeTypeAction(pMnode, pTrans, db, &newVgroup, pAnotherDnode));
  } else if (newVgroup.replica == 3) {
    for (int i = 0; i < newVgroup.replica; i++) {
      if (newVgroup.vnodeGid[i].dnodeId == pDnode->id) {
        newVgroup.vnodeGid[i].nodeRole = TAOS_SYNC_ROLE_LEARNER;
      } else {
        newVgroup.vnodeGid[i].nodeRole = TAOS_SYNC_ROLE_VOTER;
      }
    }
    TAOS_CHECK_RETURN(mndRestoreAddCreateVnodeAction(pMnode, pTrans, db, &newVgroup, pDnode));

    for (int i = 0; i < newVgroup.replica; i++) {
      newVgroup.vnodeGid[i].nodeRole = TAOS_SYNC_ROLE_VOTER;
      if (newVgroup.vnodeGid[i].dnodeId == pDnode->id) {
      }
    }
    TAOS_CHECK_RETURN(mndRestoreAddAlterVnodeTypeAction(pMnode, pTrans, db, &newVgroup, pDnode));
  }
  SSdbRaw *pVgRaw = mndVgroupActionEncode(&newVgroup);
  if (pVgRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  if ((code = mndTransAppendCommitlog(pTrans, pVgRaw)) != 0) {
    sdbFreeRaw(pVgRaw);
    TAOS_RETURN(code);
  }
  code = sdbSetRawStatus(pVgRaw, SDB_STATUS_READY);
  if (code != 0) {
    mError("vgId:%d, failed to set raw status to ready, error:%s, line:%d", newVgroup.vgId, tstrerror(code), __LINE__);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

static int32_t mndAddAdjustVnodeHashRangeAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup) {
  return 0;
}

typedef int32_t (*FpTransActionCb)(STrans *pTrans, SSdbRaw *pRaw);

static int32_t mndAddVgStatusAction(STrans *pTrans, SVgObj *pVg, ESdbStatus vgStatus, ETrnStage stage) {
  int32_t         code = 0;
  FpTransActionCb appendActionCb = (stage == TRN_STAGE_COMMIT_ACTION) ? mndTransAppendCommitlog : mndTransAppendRedolog;
  SSdbRaw        *pRaw = mndVgroupActionEncode(pVg);
  if (pRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _err;
  }
  if ((code = appendActionCb(pTrans, pRaw)) != 0) goto _err;
  code = sdbSetRawStatus(pRaw, vgStatus);
  if (code != 0) {
    mError("vgId:%d, failed to set raw status to ready, error:%s, line:%d", pVg->vgId, tstrerror(code), __LINE__);
    goto _err;
  }
  pRaw = NULL;
  TAOS_RETURN(code);
_err:
  sdbFreeRaw(pRaw);
  TAOS_RETURN(code);
}

static int32_t mndAddDbStatusAction(STrans *pTrans, SDbObj *pDb, ESdbStatus dbStatus, ETrnStage stage) {
  int32_t         code = 0;
  FpTransActionCb appendActionCb = (stage == TRN_STAGE_COMMIT_ACTION) ? mndTransAppendCommitlog : mndTransAppendRedolog;
  SSdbRaw        *pRaw = mndDbActionEncode(pDb);
  if (pRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _err;
  }
  if ((code = appendActionCb(pTrans, pRaw)) != 0) goto _err;
  code = sdbSetRawStatus(pRaw, dbStatus);
  if (code != 0) {
    mError("db:%s, failed to set raw status to ready, error:%s, line:%d", pDb->name, tstrerror(code), __LINE__);
    goto _err;
  }
  pRaw = NULL;
  TAOS_RETURN(code);
_err:
  sdbFreeRaw(pRaw);
  TAOS_RETURN(code);
}

int32_t mndSplitVgroup(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb, SVgObj *pVgroup) {
  int32_t code = -1;
  STrans *pTrans = NULL;
  SDbObj  dbObj = {0};
  SArray *pArray = mndBuildDnodesArray(pMnode, 0, NULL);

#if defined(USE_SHARED_STORAGE)
  if (tsSsEnabled) {
    code = TSDB_CODE_OPS_NOT_SUPPORT;
    mError("vgId:%d, db:%s, shared storage exists, split vgroup not allowed", pVgroup->vgId, pVgroup->dbName);
    goto _OVER;
  }
#endif

  if (pDb->cfg.withArbitrator) {
    code = TSDB_CODE_OPS_NOT_SUPPORT;
    mError("vgId:%d, db:%s, with arbitrator, split vgroup not allowed", pVgroup->vgId, pVgroup->dbName);
    goto _OVER;
  }

  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB, pReq, "split-vgroup");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }
  mndTransSetSerial(pTrans);
  mInfo("trans:%d, used to split vgroup, vgId:%d", pTrans->id, pVgroup->vgId);

  mndTransSetDbName(pTrans, pDb->name, NULL);
  TAOS_CHECK_GOTO(mndTransCheckConflictWithCompact(pMnode, pTrans), NULL, _OVER);

  SVgObj newVg1 = {0};
  memcpy(&newVg1, pVgroup, sizeof(SVgObj));
  mInfo("vgId:%d, vgroup info before split, replica:%d hashBegin:%u hashEnd:%u", newVg1.vgId, newVg1.replica,
        newVg1.hashBegin, newVg1.hashEnd);
  for (int32_t i = 0; i < newVg1.replica; ++i) {
    mInfo("vgId:%d, vnode:%d dnode:%d", newVg1.vgId, i, newVg1.vnodeGid[i].dnodeId);
  }

  if (newVg1.replica == 1) {
    TAOS_CHECK_GOTO(mndAddVnodeToVgroup(pMnode, pTrans, &newVg1, pArray), NULL, _OVER);

    newVg1.vnodeGid[1].nodeRole = TAOS_SYNC_ROLE_LEARNER;
    TAOS_CHECK_GOTO(mndAddAlterVnodeReplicaAction(pMnode, pTrans, pDb, &newVg1, newVg1.vnodeGid[0].dnodeId), NULL,
                    _OVER);
    TAOS_CHECK_GOTO(mndAddCreateVnodeAction(pMnode, pTrans, pDb, &newVg1, &newVg1.vnodeGid[1]), NULL, _OVER);

    newVg1.vnodeGid[1].nodeRole = TAOS_SYNC_ROLE_VOTER;
    TAOS_CHECK_GOTO(mndAddAlterVnodeTypeAction(pMnode, pTrans, pDb, &newVg1, newVg1.vnodeGid[1].dnodeId), NULL, _OVER);
    TAOS_CHECK_GOTO(mndAddAlterVnodeReplicaAction(pMnode, pTrans, pDb, &newVg1, newVg1.vnodeGid[0].dnodeId), NULL,
                    _OVER);

    TAOS_CHECK_GOTO(mndAddAlterVnodeConfirmAction(pMnode, pTrans, pDb, &newVg1), NULL, _OVER);
  } else if (newVg1.replica == 3) {
    SVnodeGid del1 = {0};
    TAOS_CHECK_GOTO(mndRemoveVnodeFromVgroup(pMnode, pTrans, &newVg1, pArray, &del1), NULL, _OVER);
    TAOS_CHECK_GOTO(mndAddDropVnodeAction(pMnode, pTrans, pDb, &newVg1, &del1, true), NULL, _OVER);
    TAOS_CHECK_GOTO(mndAddAlterVnodeReplicaAction(pMnode, pTrans, pDb, &newVg1, newVg1.vnodeGid[0].dnodeId), NULL,
                    _OVER);
    TAOS_CHECK_GOTO(mndAddAlterVnodeReplicaAction(pMnode, pTrans, pDb, &newVg1, newVg1.vnodeGid[1].dnodeId), NULL,
                    _OVER);
  } else {
    goto _OVER;
  }

  for (int32_t i = 0; i < newVg1.replica; ++i) {
    TAOS_CHECK_GOTO(mndAddDisableVnodeWriteAction(pMnode, pTrans, pDb, &newVg1, newVg1.vnodeGid[i].dnodeId), NULL,
                    _OVER);
  }
  TAOS_CHECK_GOTO(mndAddAlterVnodeConfirmAction(pMnode, pTrans, pDb, &newVg1), NULL, _OVER);

  SVgObj newVg2 = {0};
  memcpy(&newVg2, &newVg1, sizeof(SVgObj));
  newVg1.replica = 1;
  newVg1.hashEnd = newVg1.hashBegin / 2 + newVg1.hashEnd / 2;
  memset(&newVg1.vnodeGid[1], 0, sizeof(SVnodeGid));

  newVg2.replica = 1;
  newVg2.hashBegin = newVg1.hashEnd + 1;
  memcpy(&newVg2.vnodeGid[0], &newVg2.vnodeGid[1], sizeof(SVnodeGid));
  memset(&newVg2.vnodeGid[1], 0, sizeof(SVnodeGid));

  mInfo("vgId:%d, vgroup info after split, replica:%d hashrange:[%u, %u] vnode:0 dnode:%d", newVg1.vgId, newVg1.replica,
        newVg1.hashBegin, newVg1.hashEnd, newVg1.vnodeGid[0].dnodeId);
  for (int32_t i = 0; i < newVg1.replica; ++i) {
    mInfo("vgId:%d, vnode:%d dnode:%d", newVg1.vgId, i, newVg1.vnodeGid[i].dnodeId);
  }
  mInfo("vgId:%d, vgroup info after split, replica:%d hashrange:[%u, %u] vnode:0 dnode:%d", newVg2.vgId, newVg2.replica,
        newVg2.hashBegin, newVg2.hashEnd, newVg2.vnodeGid[0].dnodeId);
  for (int32_t i = 0; i < newVg1.replica; ++i) {
    mInfo("vgId:%d, vnode:%d dnode:%d", newVg2.vgId, i, newVg2.vnodeGid[i].dnodeId);
  }

  // alter vgId and hash range
  int32_t maxVgId = sdbGetMaxId(pMnode->pSdb, SDB_VGROUP);
  int32_t srcVgId = newVg1.vgId;
  newVg1.vgId = maxVgId;
  TAOS_CHECK_GOTO(mndAddNewVgPrepareAction(pMnode, pTrans, &newVg1), NULL, _OVER);
  TAOS_CHECK_GOTO(mndAddAlterVnodeHashRangeAction(pMnode, pTrans, srcVgId, &newVg1), NULL, _OVER);

  maxVgId++;
  srcVgId = newVg2.vgId;
  newVg2.vgId = maxVgId;
  TAOS_CHECK_GOTO(mndAddNewVgPrepareAction(pMnode, pTrans, &newVg2), NULL, _OVER);
  TAOS_CHECK_GOTO(mndAddAlterVnodeHashRangeAction(pMnode, pTrans, srcVgId, &newVg2), NULL, _OVER);

  TAOS_CHECK_GOTO(mndAddAlterVnodeConfirmAction(pMnode, pTrans, pDb, &newVg1), NULL, _OVER);
  TAOS_CHECK_GOTO(mndAddAlterVnodeConfirmAction(pMnode, pTrans, pDb, &newVg2), NULL, _OVER);

  TAOS_CHECK_GOTO(mndAddVgStatusAction(pTrans, &newVg1, SDB_STATUS_READY, TRN_STAGE_REDO_ACTION), NULL, _OVER);
  TAOS_CHECK_GOTO(mndAddVgStatusAction(pTrans, &newVg2, SDB_STATUS_READY, TRN_STAGE_REDO_ACTION), NULL, _OVER);
  TAOS_CHECK_GOTO(mndAddVgStatusAction(pTrans, pVgroup, SDB_STATUS_DROPPED, TRN_STAGE_REDO_ACTION), NULL, _OVER);

  // update db status
  memcpy(&dbObj, pDb, sizeof(SDbObj));
  if (dbObj.cfg.pRetensions != NULL) {
    dbObj.cfg.pRetensions = taosArrayDup(pDb->cfg.pRetensions, NULL);
    if (dbObj.cfg.pRetensions == NULL) {
      code = terrno;
      goto _OVER;
    }
  }
  dbObj.vgVersion++;
  dbObj.updateTime = taosGetTimestampMs();
  dbObj.cfg.numOfVgroups++;
  TAOS_CHECK_GOTO(mndAddDbStatusAction(pTrans, &dbObj, SDB_STATUS_READY, TRN_STAGE_REDO_ACTION), NULL, _OVER);

  // adjust vgroup replica
  if (pDb->cfg.replications != newVg1.replica) {
    SVgObj tmpGroup = {0};
    TAOS_CHECK_GOTO(mndBuildAlterVgroupAction(pMnode, pTrans, pDb, pDb, &newVg1, pArray, &tmpGroup), NULL, _OVER);
  } else {
    TAOS_CHECK_GOTO(mndAddVgStatusAction(pTrans, &newVg1, SDB_STATUS_READY, TRN_STAGE_COMMIT_ACTION), NULL, _OVER);
  }

  if (pDb->cfg.replications != newVg2.replica) {
    SVgObj tmpGroup = {0};
    TAOS_CHECK_GOTO(mndBuildAlterVgroupAction(pMnode, pTrans, pDb, pDb, &newVg2, pArray, &tmpGroup), NULL, _OVER);
  } else {
    TAOS_CHECK_GOTO(mndAddVgStatusAction(pTrans, &newVg2, SDB_STATUS_READY, TRN_STAGE_COMMIT_ACTION), NULL, _OVER);
  }

  TAOS_CHECK_GOTO(mndAddVgStatusAction(pTrans, pVgroup, SDB_STATUS_DROPPED, TRN_STAGE_COMMIT_ACTION), NULL, _OVER);

  // commit db status
  dbObj.vgVersion++;
  dbObj.updateTime = taosGetTimestampMs();
  TAOS_CHECK_GOTO(mndAddDbStatusAction(pTrans, &dbObj, SDB_STATUS_READY, TRN_STAGE_COMMIT_ACTION), NULL, _OVER);

  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);
  code = 0;

_OVER:
  taosArrayDestroy(pArray);
  mndTransDrop(pTrans);
  taosArrayDestroy(dbObj.cfg.pRetensions);
  TAOS_RETURN(code);
}

extern int32_t mndProcessSplitVgroupMsgImp(SRpcMsg *pReq);

static int32_t mndProcessSplitVgroupMsg(SRpcMsg *pReq) { return mndProcessSplitVgroupMsgImp(pReq); }

#ifndef TD_ENTERPRISE
int32_t mndProcessSplitVgroupMsgImp(SRpcMsg *pReq) { return 0; }
#endif

static int32_t mndSetBalanceVgroupInfoToTrans(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup,
                                              SDnodeObj *pSrc, SDnodeObj *pDst) {
  int32_t code = 0;
  SVgObj  newVg = {0};
  memcpy(&newVg, pVgroup, sizeof(SVgObj));
  mInfo("vgId:%d, vgroup info before balance, replica:%d", newVg.vgId, newVg.replica);
  for (int32_t i = 0; i < newVg.replica; ++i) {
    mInfo("vgId:%d, vnode:%d dnode:%d", newVg.vgId, i, newVg.vnodeGid[i].dnodeId);
  }

  TAOS_CHECK_RETURN(mndAddIncVgroupReplicaToTrans(pMnode, pTrans, pDb, &newVg, pDst->id));
  TAOS_CHECK_RETURN(mndAddDecVgroupReplicaFromTrans(pMnode, pTrans, pDb, &newVg, pSrc->id));

  {
    SSdbRaw *pRaw = mndVgroupActionEncode(&newVg);
    if (pRaw == NULL) {
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      TAOS_RETURN(code);
    }
    if ((code = mndTransAppendCommitlog(pTrans, pRaw)) != 0) {
      sdbFreeRaw(pRaw);
      TAOS_RETURN(code);
    }
    code = sdbSetRawStatus(pRaw, SDB_STATUS_READY);
    if (code != 0) {
      mError("vgId:%d, failed to set raw status to ready, error:%s, line:%d", newVg.vgId, tstrerror(code), __LINE__);
      TAOS_RETURN(code);
    }
  }

  mInfo("vgId:%d, vgroup info after balance, replica:%d", newVg.vgId, newVg.replica);
  for (int32_t i = 0; i < newVg.replica; ++i) {
    mInfo("vgId:%d, vnode:%d dnode:%d", newVg.vgId, i, newVg.vnodeGid[i].dnodeId);
  }
  TAOS_RETURN(code);
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
    if (pDb == NULL) {
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      mError("vgId:%d, balance vgroup can't find db obj dbName:%s", pVgroup->vgId, pVgroup->dbName);
      goto _OUT;
    }

    if (pDb->cfg.withArbitrator) {
      mInfo("vgId:%d, db:%s, with arbitrator, balance vgroup not allowed", pVgroup->vgId, pVgroup->dbName);
      goto _OUT;
    }

    code = mndSetBalanceVgroupInfoToTrans(pMnode, pTrans, pDb, pVgroup, pSrc, pDst);
    if (code == 0) {
      code = taosHashPut(pBalancedVgroups, &pVgroup->vgId, sizeof(int32_t), &pVgroup->vgId, sizeof(int32_t));
    }

  _OUT:
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

  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_GLOBAL, pReq, "balance-vgroup");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }
  mndTransSetSerial(pTrans);
  mInfo("trans:%d, used to balance vgroup", pTrans->id);
  TAOS_CHECK_GOTO(mndTransCheckConflict(pMnode, pTrans), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransCheckConflictWithCompact(pMnode, pTrans), NULL, _OVER);

  while (1) {
    taosArraySort(pArray, (__compar_fn_t)mndCompareDnodeVnodes);
    for (int32_t i = 0; i < taosArrayGetSize(pArray); ++i) {
      SDnodeObj *pDnode = taosArrayGet(pArray, i);
      mInfo("dnode:%d, equivalent vnodes:%d others:%d support:%d, score:%f", pDnode->id, pDnode->numOfVnodes,
            pDnode->numOfSupportVnodes, pDnode->numOfOtherNodes, mndGetDnodeScore(pDnode, 0, 1));
    }

    SDnodeObj *pSrc = taosArrayGet(pArray, taosArrayGetSize(pArray) - 1);
    SDnodeObj *pDst = taosArrayGet(pArray, 0);

    float srcScore = mndGetDnodeScore(pSrc, -1, 1);
    float dstScore = mndGetDnodeScore(pDst, 1, 1);
    mInfo("trans:%d, after balance, src dnode:%d score:%f, dst dnode:%d score:%f", pTrans->id, pSrc->id, dstScore,
          pDst->id, dstScore);

    if (srcScore > dstScore - 0.000001) {
      code = mndBalanceVgroupBetweenDnode(pMnode, pTrans, pSrc, pDst, pBalancedVgroups);
      if (code == 0) {
        pSrc->numOfVnodes--;
        pDst->numOfVnodes++;
        numOfVgroups++;
        continue;
      } else {
        mInfo("trans:%d, no vgroup need to balance from dnode:%d to dnode:%d", pTrans->id, pSrc->id, pDst->id);
        break;
      }
    } else {
      mInfo("trans:%d, no vgroup need to balance any more", pTrans->id);
      break;
    }
  }

  if (numOfVgroups <= 0) {
    mInfo("no need to balance vgroup");
    code = 0;
  } else {
    mInfo("start to balance vgroup, numOfVgroups:%d", numOfVgroups);
    if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;
    code = TSDB_CODE_ACTION_IN_PROGRESS;
  }

_OVER:
  taosHashCleanup(pBalancedVgroups);
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndProcessBalanceVgroupMsg(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;
  int32_t code = -1;
  SArray *pArray = NULL;
  void   *pIter = NULL;
  int64_t curMs = taosGetTimestampMs();

  SBalanceVgroupReq req = {0};
  if (tDeserializeSBalanceVgroupReq(pReq->pCont, pReq->contLen, &req) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("start to balance vgroup");
  if ((code = mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_BALANCE_VGROUP)) != 0) {
    goto _OVER;
  }

  if (sdbGetSize(pMnode->pSdb, SDB_MOUNT) > 0) {
    code = TSDB_CODE_MND_MOUNT_NOT_EMPTY;
    goto _OVER;
  }

  while (1) {
    SDnodeObj *pDnode = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_DNODE, pIter, (void **)&pDnode);
    if (pIter == NULL) break;
    if (!mndIsDnodeOnline(pDnode, curMs)) {
      sdbCancelFetch(pMnode->pSdb, pIter);
      code = TSDB_CODE_MND_HAS_OFFLINE_DNODE;
      mError("failed to balance vgroup since %s, dnode:%d", terrstr(), pDnode->id);
      sdbRelease(pMnode->pSdb, pDnode);
      goto _OVER;
    }

    sdbRelease(pMnode->pSdb, pDnode);
  }

  pArray = mndBuildDnodesArray(pMnode, 0, NULL);
  if (pArray == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }

  if (taosArrayGetSize(pArray) < 2) {
    mInfo("no need to balance vgroup since dnode num less than 2");
    code = 0;
  } else {
    code = mndBalanceVgroup(pMnode, pReq, pArray);
  }

  auditRecord(pReq, pMnode->clusterId, "balanceVgroup", "", "", req.sql, req.sqlLen);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("failed to balance vgroup since %s", tstrerror(code));
  }

  taosArrayDestroy(pArray);
  tFreeSBalanceVgroupReq(&req);
  TAOS_RETURN(code);
}

bool mndVgroupInDb(SVgObj *pVgroup, int64_t dbUid) { return !pVgroup->isTsma && pVgroup->dbUid == dbUid; }

bool mndVgroupInDnode(SVgObj *pVgroup, int32_t dnodeId) {
  for (int i = 0; i < pVgroup->replica; i++) {
    if (pVgroup->vnodeGid[i].dnodeId == dnodeId) return true;
  }
  return false;
}

static void *mndBuildCompactVnodeReq(SMnode *pMnode, SDbObj *pDb, SVgObj *pVgroup, int32_t *pContLen, int64_t compactTs,
                                     STimeWindow tw, bool metaOnly) {
  SCompactVnodeReq compactReq = {0};
  compactReq.dbUid = pDb->uid;
  compactReq.compactStartTime = compactTs;
  compactReq.tw = tw;
  compactReq.metaOnly = metaOnly;
  tstrncpy(compactReq.db, pDb->name, TSDB_DB_FNAME_LEN);

  mInfo("vgId:%d, build compact vnode config req", pVgroup->vgId);
  int32_t contLen = tSerializeSCompactVnodeReq(NULL, 0, &compactReq);
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

  if (tSerializeSCompactVnodeReq((char *)pReq + sizeof(SMsgHead), contLen, &compactReq) < 0) {
    taosMemoryFree(pReq);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  *pContLen = contLen;
  return pReq;
}

static int32_t mndAddCompactVnodeAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup, int64_t compactTs,
                                        STimeWindow tw, bool metaOnly) {
  int32_t      code = 0;
  STransAction action = {0};
  action.epSet = mndGetVgroupEpset(pMnode, pVgroup);

  int32_t contLen = 0;
  void   *pReq = mndBuildCompactVnodeReq(pMnode, pDb, pVgroup, &contLen, compactTs, tw, metaOnly);
  if (pReq == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_VND_COMPACT;

  if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pReq);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

int32_t mndBuildCompactVgroupAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup, int64_t compactTs,
                                    STimeWindow tw, bool metaOnly) {
  TAOS_CHECK_RETURN(mndAddCompactVnodeAction(pMnode, pTrans, pDb, pVgroup, compactTs, tw, metaOnly));
  return 0;
}

static void *mndBuildSsMigrateVgroupReq(SMnode *pMnode, SDbObj *pDb, SVgObj *pVgroup, int32_t *pContLen, SSsMigrateObj* pMigrateObj) {
  SSsMigrateVgroupReq req = {.ssMigrateId = pMigrateObj->id, .nodeId = 0, .timestamp = pMigrateObj->startTime };

  mInfo("vgId:%d, build ssmigrate vnode config req", pVgroup->vgId);
  int32_t contLen = tSerializeSSsMigrateVgroupReq(NULL, 0, &req);
  if (contLen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  contLen += sizeof(SMsgHead);

  void *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    return NULL;
  }

  SMsgHead *pHead = pReq;
  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(pVgroup->vgId);

  if (tSerializeSSsMigrateVgroupReq((char *)pReq + sizeof(SMsgHead), contLen - sizeof(SMsgHead), &req) < 0) {
    taosMemoryFree(pReq);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  *pContLen = contLen;
  return pReq;
}

static int32_t mndAddSsMigrateVgroupAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup, SSsMigrateObj* pMigrateObj) {
  int32_t      code = 0;
  STransAction action = {0};
  action.epSet = mndGetVgroupEpset(pMnode, pVgroup);

  int32_t contLen = 0;
  void   *pReq = mndBuildSsMigrateVgroupReq(pMnode, pDb, pVgroup, &contLen, pMigrateObj);
  if (pReq == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_VND_SSMIGRATE;

  if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pReq);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

int32_t mndBuildSsMigrateVgroupAction(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SVgObj *pVgroup, SSsMigrateObj* pMigrateObj) {
  TAOS_CHECK_RETURN(mndAddSsMigrateVgroupAction(pMnode, pTrans, pDb, pVgroup, pMigrateObj));
  return 0;
}
