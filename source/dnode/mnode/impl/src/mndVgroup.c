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

#define TSDB_VGROUP_VER_NUMBER 1
#define TSDB_VGROUP_RESERVE_SIZE 64

static SSdbRow *mndVgroupActionDecode(SSdbRaw *pRaw);
static int32_t  mndVgroupActionInsert(SSdb *pSdb, SVgObj *pVgroup);
static int32_t  mndVgroupActionDelete(SSdb *pSdb, SVgObj *pVgroup);
static int32_t  mndVgroupActionUpdate(SSdb *pSdb, SVgObj *pOldVgroup, SVgObj *pNewVgroup);

static int32_t mndProcessCreateVnodeRsp(SMnodeMsg *pMsg);
static int32_t mndProcessAlterVnodeRsp(SMnodeMsg *pMsg);
static int32_t mndProcessDropVnodeRsp(SMnodeMsg *pMsg);
static int32_t mndProcessSyncVnodeRsp(SMnodeMsg *pMsg);
static int32_t mndProcessCompactVnodeRsp(SMnodeMsg *pMsg);

static int32_t mndGetVgroupMeta(SMnodeMsg *pMsg, SShowObj *pShow, STableMetaMsg *pMeta);
static int32_t mndRetrieveVgroups(SMnodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows);
static void    mndCancelGetNextVgroup(SMnode *pMnode, void *pIter);
static int32_t mndGetVnodeMeta(SMnodeMsg *pMsg, SShowObj *pShow, STableMetaMsg *pMeta);
static int32_t mndRetrieveVnodes(SMnodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows);
static void    mndCancelGetNextVnode(SMnode *pMnode, void *pIter);

int32_t mndInitVgroup(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_VGROUP,
                     .keyType = SDB_KEY_INT32,
                     .encodeFp = (SdbEncodeFp)mndVgroupActionEncode,
                     .decodeFp = (SdbDecodeFp)mndVgroupActionDecode,
                     .insertFp = (SdbInsertFp)mndVgroupActionInsert,
                     .updateFp = (SdbUpdateFp)mndVgroupActionDelete,
                     .deleteFp = (SdbDeleteFp)mndVgroupActionUpdate};

  mndSetMsgHandle(pMnode, TDMT_DND_CREATE_VNODE_RSP, mndProcessCreateVnodeRsp);
  mndSetMsgHandle(pMnode, TDMT_DND_ALTER_VNODE_RSP, mndProcessAlterVnodeRsp);
  mndSetMsgHandle(pMnode, TDMT_DND_DROP_VNODE_RSP, mndProcessDropVnodeRsp);
  mndSetMsgHandle(pMnode, TDMT_DND_SYNC_VNODE_RSP, mndProcessSyncVnodeRsp);
  mndSetMsgHandle(pMnode, TDMT_DND_COMPACT_VNODE_RSP, mndProcessCompactVnodeRsp);

  mndAddShowMetaHandle(pMnode, TSDB_MGMT_TABLE_VGROUP, mndGetVgroupMeta);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_VGROUP, mndRetrieveVgroups);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_VGROUP, mndCancelGetNextVgroup);
  mndAddShowMetaHandle(pMnode, TSDB_MGMT_TABLE_VNODES, mndGetVnodeMeta);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_VNODES, mndRetrieveVnodes);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_VNODES, mndCancelGetNextVnode);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupVgroup(SMnode *pMnode) {}

SSdbRaw *mndVgroupActionEncode(SVgObj *pVgroup) {
  SSdbRaw *pRaw = sdbAllocRaw(SDB_VGROUP, TSDB_VGROUP_VER_NUMBER, sizeof(SVgObj) + TSDB_VGROUP_RESERVE_SIZE);
  if (pRaw == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pVgroup->vgId)
  SDB_SET_INT64(pRaw, dataPos, pVgroup->createdTime)
  SDB_SET_INT64(pRaw, dataPos, pVgroup->updateTime)
  SDB_SET_INT32(pRaw, dataPos, pVgroup->version)
  SDB_SET_INT32(pRaw, dataPos, pVgroup->hashBegin)
  SDB_SET_INT32(pRaw, dataPos, pVgroup->hashEnd)
  SDB_SET_BINARY(pRaw, dataPos, pVgroup->dbName, TSDB_DB_FNAME_LEN)
  SDB_SET_INT64(pRaw, dataPos, pVgroup->dbUid)
  SDB_SET_INT8(pRaw, dataPos, pVgroup->replica)
  for (int8_t i = 0; i < pVgroup->replica; ++i) {
    SVnodeGid *pVgid = &pVgroup->vnodeGid[i];
    SDB_SET_INT32(pRaw, dataPos, pVgid->dnodeId)
    SDB_SET_INT8(pRaw, dataPos, pVgid->role)
  }
  SDB_SET_RESERVE(pRaw, dataPos, TSDB_VGROUP_RESERVE_SIZE)
  SDB_SET_DATALEN(pRaw, dataPos);

  return pRaw;
}

SSdbRow *mndVgroupActionDecode(SSdbRaw *pRaw) {
  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) return NULL;

  if (sver != TSDB_VGROUP_VER_NUMBER) {
    mError("failed to decode vgroup since %s", terrstr());
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    return NULL;
  }

  SSdbRow *pRow = sdbAllocRow(sizeof(SVgObj));
  SVgObj  *pVgroup = sdbGetRowObj(pRow);
  if (pVgroup == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, pRow, dataPos, &pVgroup->vgId)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pVgroup->createdTime)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pVgroup->updateTime)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pVgroup->version)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pVgroup->hashBegin)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pVgroup->hashEnd)
  SDB_GET_BINARY(pRaw, pRow, dataPos, pVgroup->dbName, TSDB_DB_FNAME_LEN)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pVgroup->dbUid)
  SDB_GET_INT8(pRaw, pRow, dataPos, &pVgroup->replica)
  for (int8_t i = 0; i < pVgroup->replica; ++i) {
    SVnodeGid *pVgid = &pVgroup->vnodeGid[i];
    SDB_GET_INT32(pRaw, pRow, dataPos, &pVgid->dnodeId)
    SDB_GET_INT8(pRaw, pRow, dataPos, (int8_t *)&pVgid->role)
  }
  SDB_GET_RESERVE(pRaw, pRow, dataPos, TSDB_VGROUP_RESERVE_SIZE)

  return pRow;
}

static int32_t mndVgroupActionInsert(SSdb *pSdb, SVgObj *pVgroup) {
  mTrace("vgId:%d, perform insert action", pVgroup->vgId);
  return 0;
}

static int32_t mndVgroupActionDelete(SSdb *pSdb, SVgObj *pVgroup) {
  mTrace("vgId:%d, perform delete action", pVgroup->vgId);
  return 0;
}

static int32_t mndVgroupActionUpdate(SSdb *pSdb, SVgObj *pOldVgroup, SVgObj *pNewVgroup) {
  mTrace("vgId:%d, perform update action", pOldVgroup->vgId);
  pOldVgroup->updateTime = pNewVgroup->updateTime;
  pOldVgroup->version = pNewVgroup->version;
  pOldVgroup->hashBegin = pNewVgroup->hashBegin;
  pOldVgroup->hashEnd = pNewVgroup->hashEnd;
  pOldVgroup->replica = pNewVgroup->replica;
  memcpy(pOldVgroup->vnodeGid, pNewVgroup->vnodeGid, TSDB_MAX_REPLICA * sizeof(SVnodeGid));
  return 0;
}

SVgObj *mndAcquireVgroup(SMnode *pMnode, int32_t vgId) {
  SSdb   *pSdb = pMnode->pSdb;
  SVgObj *pVgroup = sdbAcquire(pSdb, SDB_VGROUP, &vgId);
  if (pVgroup == NULL) {
    terrno = TSDB_CODE_MND_VGROUP_NOT_EXIST;
  }
  return pVgroup;
}

void mndReleaseVgroup(SMnode *pMnode, SVgObj *pVgroup) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pVgroup);
}

SCreateVnodeMsg *mndBuildCreateVnodeMsg(SMnode *pMnode, SDnodeObj *pDnode, SDbObj *pDb, SVgObj *pVgroup) {
  SCreateVnodeMsg *pCreate = calloc(1, sizeof(SCreateVnodeMsg));
  if (pCreate == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pCreate->vgId = htonl(pVgroup->vgId);
  pCreate->dnodeId = htonl(pDnode->id);
  memcpy(pCreate->db, pDb->name, TSDB_DB_FNAME_LEN);
  pCreate->dbUid = htobe64(pDb->uid);
  pCreate->vgVersion = htonl(pVgroup->version);
  pCreate->cacheBlockSize = htonl(pDb->cfg.cacheBlockSize);
  pCreate->totalBlocks = htonl(pDb->cfg.totalBlocks);
  pCreate->daysPerFile = htonl(pDb->cfg.daysPerFile);
  pCreate->daysToKeep0 = htonl(pDb->cfg.daysToKeep0);
  pCreate->daysToKeep1 = htonl(pDb->cfg.daysToKeep1);
  pCreate->daysToKeep2 = htonl(pDb->cfg.daysToKeep2);
  pCreate->minRows = htonl(pDb->cfg.minRows);
  pCreate->maxRows = htonl(pDb->cfg.maxRows);
  pCreate->commitTime = htonl(pDb->cfg.commitTime);
  pCreate->fsyncPeriod = htonl(pDb->cfg.fsyncPeriod);
  pCreate->walLevel = pDb->cfg.walLevel;
  pCreate->precision = pDb->cfg.precision;
  pCreate->compression = pDb->cfg.compression;
  pCreate->quorum = pDb->cfg.quorum;
  pCreate->update = pDb->cfg.update;
  pCreate->cacheLastRow = pDb->cfg.cacheLastRow;
  pCreate->replica = pVgroup->replica;
  pCreate->selfIndex = -1;

  for (int32_t v = 0; v < pVgroup->replica; ++v) {
    SReplica  *pReplica = &pCreate->replicas[v];
    SVnodeGid *pVgid = &pVgroup->vnodeGid[v];
    SDnodeObj *pVgidDnode = mndAcquireDnode(pMnode, pVgid->dnodeId);
    if (pVgidDnode == NULL) {
      free(pCreate);
      return NULL;
    }

    pReplica->id = htonl(pVgidDnode->id);
    pReplica->port = htons(pVgidDnode->port);
    memcpy(pReplica->fqdn, pVgidDnode->fqdn, TSDB_FQDN_LEN);
    mndReleaseDnode(pMnode, pVgidDnode);

    if (pDnode->id == pVgid->dnodeId) {
      pCreate->selfIndex = v;
    }
  }

  if (pCreate->selfIndex == -1) {
    free(pCreate);
    terrno = TSDB_CODE_MND_APP_ERROR;
    return NULL;
  }

  return pCreate;
}

SDropVnodeMsg *mndBuildDropVnodeMsg(SMnode *pMnode, SDnodeObj *pDnode, SDbObj *pDb, SVgObj *pVgroup) {
  SDropVnodeMsg *pDrop = calloc(1, sizeof(SDropVnodeMsg));
  if (pDrop == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pDrop->dnodeId = htonl(pDnode->id);
  pDrop->vgId = htonl(pVgroup->vgId);
  memcpy(pDrop->db, pDb->name, TSDB_DB_FNAME_LEN);
  pDrop->dbUid = htobe64(pDb->uid);

  return pDrop;
}

static int32_t mndGetAvailableDnode(SMnode *pMnode, SVgObj *pVgroup) {
  SSdb   *pSdb = pMnode->pSdb;
  int32_t allocedVnodes = 0;
  void   *pIter = NULL;

  while (allocedVnodes < pVgroup->replica) {
    SDnodeObj *pDnode = NULL;
    pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pDnode);
    if (pIter == NULL) break;

    // todo
    if (mndIsDnodeInReadyStatus(pMnode, pDnode)) {
      SVnodeGid *pVgid = &pVgroup->vnodeGid[allocedVnodes];
      pVgid->dnodeId = pDnode->id;
      if (pVgroup->replica == 1) {
        pVgid->role = TAOS_SYNC_STATE_LEADER;
      } else {
        pVgid->role = TAOS_SYNC_STATE_FOLLOWER;
      }
      allocedVnodes++;
    }
    sdbRelease(pSdb, pDnode);
  }

  if (allocedVnodes != pVgroup->replica) {
    terrno = TSDB_CODE_MND_NO_ENOUGH_DNODES;
    return -1;
  }
  return 0;
}

int32_t mndAllocVgroup(SMnode *pMnode, SDbObj *pDb, SVgObj **ppVgroups) {
  SVgObj *pVgroups = calloc(pDb->cfg.numOfVgroups, sizeof(SVgObj));
  if (pVgroups == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  int32_t  allocedVgroups = 0;
  int32_t  maxVgId = sdbGetMaxId(pMnode->pSdb, SDB_VGROUP);
  uint32_t hashMin = 0;
  uint32_t hashMax = UINT32_MAX;
  uint32_t hashInterval = (hashMax - hashMin) / pDb->cfg.numOfVgroups;

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

    if (mndGetAvailableDnode(pMnode, pVgroup) != 0) {
      terrno = TSDB_CODE_MND_NO_ENOUGH_DNODES;
      free(pVgroups);
      return -1;
    }

    allocedVgroups++;
  }

  *ppVgroups = pVgroups;
  return 0;
}

SEpSet mndGetVgroupEpset(SMnode *pMnode, SVgObj *pVgroup) {
  SEpSet epset = {0};

  for (int32_t v = 0; v < pVgroup->replica; ++v) {
    SVnodeGid *pVgid = &pVgroup->vnodeGid[v];
    SDnodeObj *pDnode = mndAcquireDnode(pMnode, pVgid->dnodeId);
    if (pDnode == NULL) continue;

    if (pVgid->role == TAOS_SYNC_STATE_LEADER) {
      epset.inUse = epset.numOfEps;
    }

    epset.port[epset.numOfEps] = pDnode->port;
    memcpy(&epset.fqdn[epset.numOfEps], pDnode->fqdn, TSDB_FQDN_LEN);
    epset.numOfEps++;
    mndReleaseDnode(pMnode, pDnode);
  }

  return epset;
}

static int32_t mndProcessCreateVnodeRsp(SMnodeMsg *pMsg) {
  mndTransProcessRsp(pMsg);
  return 0;
}

static int32_t mndProcessAlterVnodeRsp(SMnodeMsg *pMsg) {
  mndTransProcessRsp(pMsg);
  return 0;
}

static int32_t mndProcessDropVnodeRsp(SMnodeMsg *pMsg) {
  mndTransProcessRsp(pMsg);
  return 0;
}

static int32_t mndProcessSyncVnodeRsp(SMnodeMsg *pMsg) { return 0; }
static int32_t mndProcessCompactVnodeRsp(SMnodeMsg *pMsg) { return 0; }

static int32_t mndGetVgroupMaxReplica(SMnode *pMnode, char *dbName, int8_t *pReplica, int32_t *pNumOfVgroups) {
  SSdb   *pSdb = pMnode->pSdb;
  SDbObj *pDb = mndAcquireDb(pMnode, dbName);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_SELECTED;
    return -1;
  }

  int8_t  replica = 1;
  int32_t numOfVgroups = 0;

  void *pIter = NULL;
  while (1) {
    SVgObj *pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    if (pVgroup->dbUid == pDb->uid) {
      replica = MAX(replica, pVgroup->replica);
      numOfVgroups++;
    }

    sdbRelease(pSdb, pVgroup);
  }

  *pReplica = replica;
  *pNumOfVgroups = numOfVgroups;
  return 0;
}

static int32_t mndGetVgroupMeta(SMnodeMsg *pMsg, SShowObj *pShow, STableMetaMsg *pMeta) {
  SMnode *pMnode = pMsg->pMnode;
  SSdb   *pSdb = pMnode->pSdb;

  if (mndGetVgroupMaxReplica(pMnode, pShow->db, &pShow->replica, &pShow->numOfRows) != 0) {
    return -1;
  }

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->pSchema;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "vgId");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "tables");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  for (int32_t i = 0; i < pShow->replica; ++i) {
    pShow->bytes[cols] = 2;
    pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
    snprintf(pSchema[cols].name, TSDB_COL_NAME_LEN, "v%d_dnode", i + 1);
    pSchema[cols].bytes = htonl(pShow->bytes[cols]);
    cols++;

    pShow->bytes[cols] = 9 + VARSTR_HEADER_SIZE;
    pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
    snprintf(pSchema[cols].name, TSDB_COL_NAME_LEN, "v%d_status", i + 1);
    pSchema[cols].bytes = htonl(pShow->bytes[cols]);
    cols++;
  }

  pMeta->numOfColumns = htonl(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  strcpy(pMeta->tbFname, mndShowStr(pShow->type));

  return 0;
}

static int32_t mndRetrieveVgroups(SMnodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows) {
  SMnode *pMnode = pMsg->pMnode;
  SSdb   *pSdb = pMnode->pSdb;
  int32_t numOfRows = 0;
  SVgObj *pVgroup = NULL;
  int32_t cols = 0;
  char   *pWrite;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_VGROUP, pShow->pIter, (void **)&pVgroup);
    if (pShow->pIter == NULL) break;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pVgroup->vgId;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pVgroup->numOfTables;
    cols++;

    for (int32_t i = 0; i < pShow->replica; ++i) {
      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int16_t *)pWrite = pVgroup->vnodeGid[i].dnodeId;
      cols++;

      const char *role = mndGetRoleStr(pVgroup->vnodeGid[i].role);
      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, role, pShow->bytes[cols]);
      cols++;
    }

    sdbRelease(pSdb, pVgroup);
    numOfRows++;
  }

  mndVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  pShow->numOfReads += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextVgroup(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}

static int32_t mndGetVnodesNum(SMnode *pMnode, int32_t dnodeId) {
  SSdb   *pSdb = pMnode->pSdb;
  int32_t numOfVnodes = 0;
  void   *pIter = NULL;

  while (1) {
    SVgObj *pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    for (int32_t v = 0; v < pVgroup->replica; ++v) {
      if (pVgroup->vnodeGid[v].dnodeId == dnodeId) {
        numOfVnodes++;
      }
    }

    sdbRelease(pSdb, pVgroup);
  }

  return numOfVnodes;
}

static int32_t mndGetVnodeMeta(SMnodeMsg *pMsg, SShowObj *pShow, STableMetaMsg *pMeta) {
  SMnode *pMnode = pMsg->pMnode;
  SSdb   *pSdb = pMnode->pSdb;

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->pSchema;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "vgId");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 12 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "status");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htonl(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  int32_t dnodeId = 0;
  if (pShow->payloadLen > 0) {
    dnodeId = atoi(pShow->payload);
  }

  pShow->replica = dnodeId;
  pShow->numOfRows = mndGetVnodesNum(pMnode, dnodeId);
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  strcpy(pMeta->tbFname, mndShowStr(pShow->type));

  return 0;
}

static int32_t mndRetrieveVnodes(SMnodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows) {
  SMnode *pMnode = pMsg->pMnode;
  SSdb   *pSdb = pMnode->pSdb;
  int32_t numOfRows = 0;
  SVgObj *pVgroup = NULL;
  char   *pWrite;
  int32_t cols = 0;
  int32_t dnodeId = pShow->replica;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_VGROUP, pShow->pIter, (void **)&pVgroup);
    if (pShow->pIter == NULL) break;

    for (int32_t i = 0; i < pVgroup->replica && numOfRows < rows; ++i) {
      SVnodeGid *pVgid = &pVgroup->vnodeGid[i];
      if (pVgid->dnodeId != dnodeId) continue;

      cols = 0;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(uint32_t *)pWrite = pVgroup->vgId;
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      STR_TO_VARSTR(pWrite, mndGetRoleStr(pVgid->role));
      cols++;
      numOfRows++;
    }

    sdbRelease(pSdb, pVgroup);
  }

  mndVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  pShow->numOfReads += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextVnode(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}