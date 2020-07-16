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
#include "os.h"
#include "taoserror.h"
#include "tutil.h"
#include "tsocket.h"
#include "tidpool.h"
#include "tsync.h"
#include "ttime.h"
#include "tbalance.h"
#include "tglobal.h"
#include "tdataformat.h"
#include "dnode.h"
#include "mnode.h"
#include "mnodeDef.h"
#include "mnodeInt.h"
#include "mnodeDb.h"
#include "mnodeDnode.h"
#include "mnodeMnode.h"
#include "mnodeProfile.h"
#include "mnodeSdb.h"
#include "mnodeShow.h"
#include "mnodeTable.h"
#include "mnodeVgroup.h"
#include "mnodePeer.h"

typedef enum {
  TAOS_VG_STATUS_READY,
  TAOS_VG_STATUS_DROPPING,
  TAOS_VG_STATUS_CREATING,
  TAOS_VG_STATUS_UPDATING,
} EVgroupStatus;

char* vgroupStatus[] = {
  "ready",
  "dropping",
  "creating",
  "updating"
};

static void   *tsVgroupSdb = NULL;
static int32_t tsVgUpdateSize = 0;

static int32_t mnodeAllocVgroupIdPool(SVgObj *pInputVgroup);
static int32_t mnodeGetVgroupMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t mnodeRetrieveVgroups(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static void    mnodeProcessCreateVnodeRsp(SRpcMsg *rpcMsg);
static void    mnodeProcessAlterVnodeRsp(SRpcMsg *rpcMsg);
static void    mnodeProcessDropVnodeRsp(SRpcMsg *rpcMsg);
static int32_t mnodeProcessVnodeCfgMsg(SMnodeMsg *pMsg) ;
static void    mnodeSendDropVgroupMsg(SVgObj *pVgroup, void *ahandle);

static void mnodeDestroyVgroup(SVgObj *pVgroup) {
  if (pVgroup->idPool) {
    taosIdPoolCleanUp(pVgroup->idPool);
    pVgroup->idPool = NULL;
  }

  tfree(pVgroup);
}

static int32_t mnodeVgroupActionDestroy(SSdbOper *pOper) {
  mnodeDestroyVgroup(pOper->pObj);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeVgroupActionInsert(SSdbOper *pOper) {
  SVgObj *pVgroup = pOper->pObj;

  // refer to db
  SDbObj *pDb = mnodeGetDb(pVgroup->dbName);
  if (pDb == NULL) {
    return TSDB_CODE_MND_INVALID_DB;
  }
  
  if (pDb->status != TSDB_DB_STATUS_READY) {
    mError("db:%s, status:%d, in dropping", pDb->name, pDb->status);
    return TSDB_CODE_MND_DB_IN_DROPPING;
  }

  pVgroup->pDb = pDb;
  pVgroup->status = TAOS_VG_STATUS_CREATING;
  pVgroup->accessState = TSDB_VN_ALL_ACCCESS;
  if (mnodeAllocVgroupIdPool(pVgroup) < 0) {
    mError("vgId:%d, failed to init idpool for vgroups", pVgroup->vgId);
    return -1;
  }

  for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
    SDnodeObj *pDnode = mnodeGetDnode(pVgroup->vnodeGid[i].dnodeId);
    if (pDnode != NULL) {
      pVgroup->vnodeGid[i].pDnode = pDnode;
      atomic_add_fetch_32(&pDnode->openVnodes, 1);
      mnodeDecDnodeRef(pDnode);
    }
  }

  mnodeAddVgroupIntoDb(pVgroup);

  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeVgroupActionDelete(SSdbOper *pOper) {
  SVgObj *pVgroup = pOper->pObj;

  if (pVgroup->pDb != NULL) {
    mnodeRemoveVgroupFromDb(pVgroup);
  }

  mnodeDecDbRef(pVgroup->pDb);

  for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
    SDnodeObj *pDnode = mnodeGetDnode(pVgroup->vnodeGid[i].dnodeId);
    if (pDnode != NULL) {
      atomic_sub_fetch_32(&pDnode->openVnodes, 1);
    }
    mnodeDecDnodeRef(pDnode);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeVgroupActionUpdate(SSdbOper *pOper) {
  SVgObj *pNew = pOper->pObj;
  SVgObj *pVgroup = mnodeGetVgroup(pNew->vgId);

  if (pVgroup != pNew) {
    for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
      SDnodeObj *pDnode = pVgroup->vnodeGid[i].pDnode;
      if (pDnode != NULL) {
        atomic_sub_fetch_32(&pDnode->openVnodes, 1);
      }
    }

    memcpy(pVgroup, pNew, tsVgUpdateSize);

    for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
      SDnodeObj *pDnode = mnodeGetDnode(pVgroup->vnodeGid[i].dnodeId);
      pVgroup->vnodeGid[i].pDnode = pDnode;
      if (pDnode != NULL) {
        atomic_add_fetch_32(&pDnode->openVnodes, 1);
      }
      mnodeDecDnodeRef(pDnode);
    }

    free(pNew);
  }


  // reset vgid status on vgroup changed
  mDebug("vgId:%d, reset sync status to unsynced", pVgroup->vgId);
  for (int32_t v = 0; v < pVgroup->numOfVnodes; ++v) {
    pVgroup->vnodeGid[v].role = TAOS_SYNC_ROLE_UNSYNCED;
  }

  mnodeDecVgroupRef(pVgroup);

  mDebug("vgId:%d, is updated, numOfVnode:%d", pVgroup->vgId, pVgroup->numOfVnodes);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeVgroupActionEncode(SSdbOper *pOper) {
  SVgObj *pVgroup = pOper->pObj;
  memcpy(pOper->rowData, pVgroup, tsVgUpdateSize);
  SVgObj *pTmpVgroup = pOper->rowData;
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; ++i) {
    pTmpVgroup->vnodeGid[i].pDnode = NULL;
    pTmpVgroup->vnodeGid[i].role = 0;
  }

  pOper->rowSize = tsVgUpdateSize;
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeVgroupActionDecode(SSdbOper *pOper) {
  SVgObj *pVgroup = (SVgObj *) calloc(1, sizeof(SVgObj));
  if (pVgroup == NULL) return TSDB_CODE_MND_OUT_OF_MEMORY;

  memcpy(pVgroup, pOper->rowData, tsVgUpdateSize);
  pOper->pObj = pVgroup;
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeVgroupActionRestored() {
  return 0;
}

int32_t mnodeInitVgroups() {
  SVgObj tObj;
  tsVgUpdateSize = (int8_t *)tObj.updateEnd - (int8_t *)&tObj;

  SSdbTableDesc tableDesc = {
    .tableId      = SDB_TABLE_VGROUP,
    .tableName    = "vgroups",
    .hashSessions = TSDB_DEFAULT_VGROUPS_HASH_SIZE,
    .maxRowSize   = tsVgUpdateSize,
    .refCountPos  = (int8_t *)(&tObj.refCount) - (int8_t *)&tObj,
    .keyType      = SDB_KEY_AUTO,
    .insertFp     = mnodeVgroupActionInsert,
    .deleteFp     = mnodeVgroupActionDelete,
    .updateFp     = mnodeVgroupActionUpdate,
    .encodeFp     = mnodeVgroupActionEncode,
    .decodeFp     = mnodeVgroupActionDecode,
    .destroyFp    = mnodeVgroupActionDestroy,
    .restoredFp   = mnodeVgroupActionRestored,
  };

  tsVgroupSdb = sdbOpenTable(&tableDesc);
  if (tsVgroupSdb == NULL) {
    mError("failed to init vgroups data");
    return -1;
  }

  mnodeAddShowMetaHandle(TSDB_MGMT_TABLE_VGROUP, mnodeGetVgroupMeta);
  mnodeAddShowRetrieveHandle(TSDB_MGMT_TABLE_VGROUP, mnodeRetrieveVgroups);
  mnodeAddPeerRspHandle(TSDB_MSG_TYPE_MD_CREATE_VNODE_RSP, mnodeProcessCreateVnodeRsp);
  mnodeAddPeerRspHandle(TSDB_MSG_TYPE_MD_ALTER_VNODE_RSP, mnodeProcessAlterVnodeRsp);
  mnodeAddPeerRspHandle(TSDB_MSG_TYPE_MD_DROP_VNODE_RSP, mnodeProcessDropVnodeRsp);
  mnodeAddPeerMsgHandle(TSDB_MSG_TYPE_DM_CONFIG_VNODE, mnodeProcessVnodeCfgMsg);

  mDebug("table:vgroups is created");
  
  return 0;
}

void mnodeIncVgroupRef(SVgObj *pVgroup) {
  return sdbIncRef(tsVgroupSdb, pVgroup); 
}

void mnodeDecVgroupRef(SVgObj *pVgroup) { 
  return sdbDecRef(tsVgroupSdb, pVgroup); 
}

SVgObj *mnodeGetVgroup(int32_t vgId) {
  return (SVgObj *)sdbGetRow(tsVgroupSdb, &vgId);
}

void mnodeUpdateVgroup(SVgObj *pVgroup) {
  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsVgroupSdb,
    .pObj = pVgroup
  };

  if (sdbUpdateRow(&oper) != TSDB_CODE_SUCCESS) {
    mError("vgId:%d, failed to update vgroup", pVgroup->vgId);
  }
  mnodeSendAlterVgroupMsg(pVgroup);
}

/*
  Traverse all vgroups on mnode, if there no such vgId on a dnode, so send msg to this dnode for re-creating this vgId/vnode 
*/
void mnodeCheckUnCreatedVgroup(SDnodeObj *pDnode, SVnodeLoad *pVloads, int32_t openVnodes) {
  SVnodeLoad *pNextV = NULL;

  void *pIter = NULL;
  while (1) {
    SVgObj *pVgroup;
    pIter = mnodeGetNextVgroup(pIter, &pVgroup);
    if (pVgroup == NULL) break;

    pNextV = pVloads;
    int32_t i;
    for (i = 0; i < openVnodes; ++i) {
      if ((pVgroup->vnodeGid[i].pDnode == pDnode) && (pVgroup->vgId == pNextV->vgId)) {
        break;
      }
      pNextV++;
    }

    if (i == openVnodes) {
      if (pVgroup->status == TAOS_VG_STATUS_CREATING || pVgroup->status == TAOS_VG_STATUS_DROPPING) {
        mDebug("vgId:%d, not exist in dnode:%d and status is %s, do nothing", pVgroup->vgId, pDnode->dnodeId,
               vgroupStatus[pVgroup->status]);
      } else {
        mDebug("vgId:%d, not exist in dnode:%d and status is %s, send create msg", pVgroup->vgId, pDnode->dnodeId,
               vgroupStatus[pVgroup->status]);
        mnodeSendCreateVgroupMsg(pVgroup, NULL);
      }
    }

    mnodeDecVgroupRef(pVgroup);
  }

  sdbFreeIter(pIter);
  return;
}

void mnodeUpdateVgroupStatus(SVgObj *pVgroup, SDnodeObj *pDnode, SVnodeLoad *pVload) {
  bool dnodeExist = false;
  for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
    SVnodeGid *pVgid = &pVgroup->vnodeGid[i];
    if (pVgid->pDnode == pDnode) {
      mTrace("dnode:%d, receive status from dnode, vgId:%d status is %d", pVgroup->vgId, pDnode->dnodeId, pVgid->role);
      pVgid->role = pVload->role;
      if (pVload->role == TAOS_SYNC_ROLE_MASTER) {
        pVgroup->inUse = i;
      }
      dnodeExist = true;
      break;
    }
  }

  if (!dnodeExist) {
    SRpcIpSet ipSet = mnodeGetIpSetFromIp(pDnode->dnodeEp);
    mError("vgId:%d, dnode:%d not exist in mnode, drop it", pVload->vgId, pDnode->dnodeId);
    mnodeSendDropVnodeMsg(pVload->vgId, &ipSet, NULL);
    return;
  }

  if (pVload->role == TAOS_SYNC_ROLE_MASTER) {
    pVgroup->totalStorage = htobe64(pVload->totalStorage);
    pVgroup->compStorage = htobe64(pVload->compStorage);
    pVgroup->pointsWritten = htobe64(pVload->pointsWritten);
  }

  if (pVload->cfgVersion != pVgroup->pDb->cfgVersion || pVload->replica != pVgroup->numOfVnodes) {
    mError("dnode:%d, vgId:%d, vnode cfgVersion:%d repica:%d not match with mnode cfgVersion:%d replica:%d",
           pDnode->dnodeId, pVload->vgId, pVload->cfgVersion, pVload->replica, pVgroup->pDb->cfgVersion,
           pVgroup->numOfVnodes);
    mnodeSendAlterVgroupMsg(pVgroup);
  }
}

static int32_t mnodeAllocVgroupIdPool(SVgObj *pInputVgroup) {
  SDbObj *pDb = pInputVgroup->pDb;
  if (pDb == NULL) return TSDB_CODE_MND_APP_ERROR;

  int32_t minIdPoolSize = TSDB_MAX_TABLES;
  int32_t maxIdPoolSize = tsMinTablePerVnode;
  for (int32_t v = 0; v < pDb->numOfVgroups; ++v) {
    SVgObj *pVgroup = pDb->vgList[v];
    if (pVgroup == NULL) continue;

    int32_t idPoolSize = taosIdPoolMaxSize(pVgroup->idPool);
    minIdPoolSize = MIN(minIdPoolSize, idPoolSize);
    maxIdPoolSize = MAX(maxIdPoolSize, idPoolSize);
  }

  // new vgroup
  if (pInputVgroup->idPool == NULL) {
    pInputVgroup->idPool = taosInitIdPool(maxIdPoolSize);
    if (pInputVgroup->idPool == NULL) {
      mError("vgId:%d, failed to init idPool for vgroup, size:%d", pInputVgroup->vgId, maxIdPoolSize);
      return TSDB_CODE_MND_OUT_OF_MEMORY;
    } else {
      mDebug("vgId:%d, init idPool for vgroup, size:%d", pInputVgroup->vgId, maxIdPoolSize);
      return TSDB_CODE_SUCCESS;
    }
  }

  // realloc all vgroups in db
  int32_t newIdPoolSize;
  if (minIdPoolSize * 4 < tsTableIncStepPerVnode) {
    newIdPoolSize = minIdPoolSize * 4;
  } else {
    newIdPoolSize = ((minIdPoolSize / tsTableIncStepPerVnode) + 1) * tsTableIncStepPerVnode;
  }

  if (newIdPoolSize > tsMaxTablePerVnode) {
    if (minIdPoolSize >= tsMaxTablePerVnode) {
      mError("db:%s, minIdPoolSize:%d newIdPoolSize:%d larger than maxTablesPerVnode:%d", pDb->name, minIdPoolSize, newIdPoolSize,
             tsMaxTablePerVnode);
      return TSDB_CODE_MND_NO_ENOUGH_DNODES;
    } else {
      newIdPoolSize = tsMaxTablePerVnode;
    }
  }

  for (int32_t v = 0; v < pDb->numOfVgroups; ++v) {
    SVgObj *pVgroup = pDb->vgList[v];
    if (pVgroup == NULL) continue;

    int32_t oldIdPoolSize = taosIdPoolMaxSize(pVgroup->idPool);
    if (newIdPoolSize == oldIdPoolSize) continue;

    if (taosUpdateIdPool(pVgroup->idPool, newIdPoolSize) < 0) {
      mError("vgId:%d, failed to update idPoolSize from %d to %d", pVgroup->vgId, oldIdPoolSize, newIdPoolSize);
      return TSDB_CODE_MND_NO_ENOUGH_DNODES;
    } else {
      mDebug("vgId:%d, idPoolSize update from %d to %d", pVgroup->vgId, oldIdPoolSize, newIdPoolSize);
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t mnodeGetAvailableVgroup(SMnodeMsg *pMsg, SVgObj **ppVgroup, int32_t *pSid) {
  SDbObj *pDb = pMsg->pDb;
  pthread_mutex_lock(&pDb->mutex);
  
  for (int32_t v = 0; v < pDb->numOfVgroups; ++v) {
    int vgIndex = (v + pDb->vgListIndex) % pDb->numOfVgroups;
    SVgObj *pVgroup = pDb->vgList[vgIndex];
    if (pVgroup == NULL) {
      mError("db:%s, index:%d vgroup is null", pDb->name, vgIndex);
      pthread_mutex_unlock(&pDb->mutex);
      return TSDB_CODE_MND_APP_ERROR;
    }

    int32_t sid = taosAllocateId(pVgroup->idPool);
    if (sid <= 0) {
      mDebug("app:%p:%p, db:%s, no enough sid in vgId:%d", pMsg->rpcMsg.ahandle, pMsg, pDb->name, pVgroup->vgId);
      continue;
    }

    *pSid = sid;
    *ppVgroup = pVgroup;
    pDb->vgListIndex = vgIndex;

    pthread_mutex_unlock(&pDb->mutex);
    return TSDB_CODE_SUCCESS;
  }

  int maxVgroupsPerDb = tsMaxVgroupsPerDb;
  if (maxVgroupsPerDb <= 0) {
    maxVgroupsPerDb = mnodeGetOnlinDnodesCpuCoreNum();
    maxVgroupsPerDb = MAX(maxVgroupsPerDb, 2);
  }

  if (pDb->numOfVgroups < maxVgroupsPerDb) {
    mDebug("app:%p:%p, db:%s, try to create a new vgroup, numOfVgroups:%d maxVgroupsPerDb:%d", pMsg->rpcMsg.ahandle, pMsg,
           pDb->name, pDb->numOfVgroups, maxVgroupsPerDb);
    pthread_mutex_unlock(&pDb->mutex);
    int32_t code = mnodeCreateVgroup(pMsg);
    if (code == TSDB_CODE_MND_ACTION_IN_PROGRESS) return code;
  }

  SVgObj *pVgroup = pDb->vgList[0];
  if (pVgroup == NULL) return TSDB_CODE_MND_NO_ENOUGH_DNODES;

  int32_t code = mnodeAllocVgroupIdPool(pVgroup);
  if (code != TSDB_CODE_SUCCESS) {
    pthread_mutex_unlock(&pDb->mutex);
    return code;
  }

  int32_t sid = taosAllocateId(pVgroup->idPool);
  if (sid <= 0) {
    mError("app:%p:%p, db:%s, no enough sid in vgId:%d", pMsg->rpcMsg.ahandle, pMsg, pDb->name, pVgroup->vgId);
    pthread_mutex_unlock(&pDb->mutex);
    return TSDB_CODE_MND_NO_ENOUGH_DNODES;
  }

  *pSid = sid;
  *ppVgroup = pVgroup;
  pDb->vgListIndex = 0;
  pthread_mutex_unlock(&pDb->mutex);

  return TSDB_CODE_SUCCESS;
}

void *mnodeGetNextVgroup(void *pIter, SVgObj **pVgroup) { 
  return sdbFetchRow(tsVgroupSdb, pIter, (void **)pVgroup); 
}

static int32_t mnodeCreateVgroupCb(SMnodeMsg *pMsg, int32_t code) {
  SVgObj *pVgroup = pMsg->pVgroup;
  SDbObj *pDb = pMsg->pDb;
  assert(pVgroup);

  if (code != TSDB_CODE_SUCCESS) {
    mError("app:%p:%p, vgId:%d, failed to create in sdb, reason:%s", pMsg->rpcMsg.ahandle, pMsg, pVgroup->vgId,
           tstrerror(code));
    SSdbOper desc = {.type = SDB_OPER_GLOBAL, .pObj = pVgroup, .table = tsVgroupSdb};
    sdbDeleteRow(&desc);
    return code;
  } else {
    pVgroup->status = TAOS_VG_STATUS_READY;
    SSdbOper desc = {.type = SDB_OPER_GLOBAL, .pObj = pVgroup, .table = tsVgroupSdb};
    sdbUpdateRow(&desc);
  }

  mInfo("app:%p:%p, vgId:%d, is created in mnode, db:%s replica:%d", pMsg->rpcMsg.ahandle, pMsg, pVgroup->vgId,
        pDb->name, pVgroup->numOfVnodes);
  for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
    mInfo("app:%p:%p, vgId:%d, index:%d, dnode:%d", pMsg->rpcMsg.ahandle, pMsg, pVgroup->vgId, i,
          pVgroup->vnodeGid[i].dnodeId);
  }

  pMsg->expected = pVgroup->numOfVnodes;
  pMsg->successed = 0;
  pMsg->received = 0;
  mnodeSendCreateVgroupMsg(pVgroup, pMsg);

  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

int32_t mnodeCreateVgroup(SMnodeMsg *pMsg) {
  if (pMsg == NULL) return TSDB_CODE_MND_APP_ERROR;
  SDbObj *pDb = pMsg->pDb;

  SVgObj *pVgroup = (SVgObj *)calloc(1, sizeof(SVgObj));
  tstrncpy(pVgroup->dbName, pDb->name, TSDB_ACCT_LEN + TSDB_DB_NAME_LEN);
  pVgroup->numOfVnodes = pDb->cfg.replications;
  pVgroup->createdTime = taosGetTimestampMs();
  pVgroup->accessState = TSDB_VN_ALL_ACCCESS;
  if (balanceAllocVnodes(pVgroup) != 0) {
    mError("db:%s, no enough dnode to alloc %d vnodes to vgroup", pDb->name, pVgroup->numOfVnodes);
    free(pVgroup);
    return TSDB_CODE_MND_NO_ENOUGH_DNODES;
  }

  pMsg->pVgroup = pVgroup;
  mnodeIncVgroupRef(pVgroup);

  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsVgroupSdb,
    .pObj = pVgroup,
    .rowSize = sizeof(SVgObj),
    .pMsg = pMsg,
    .cb = mnodeCreateVgroupCb
  };

  int32_t code = sdbInsertRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    pMsg->pVgroup = NULL;
    mnodeDestroyVgroup(pVgroup);
  } else {
    code = TSDB_CODE_MND_ACTION_IN_PROGRESS;
  }

  return code;
}

void mnodeDropVgroup(SVgObj *pVgroup, void *ahandle) {
  if (ahandle != NULL) {
    mnodeSendDropVgroupMsg(pVgroup, ahandle);
  } else {
    mDebug("vgId:%d, replica:%d is deleting from sdb", pVgroup->vgId, pVgroup->numOfVnodes);
    mnodeSendDropVgroupMsg(pVgroup, NULL);
    SSdbOper oper = {
      .type = SDB_OPER_GLOBAL,
      .table = tsVgroupSdb,
      .pObj = pVgroup
    };
    sdbDeleteRow(&oper);
  }
}

void mnodeCleanupVgroups() {
  sdbCloseTable(tsVgroupSdb);
  tsVgroupSdb = NULL;
}

static int32_t mnodeGetVgroupMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SDbObj *pDb = mnodeGetDb(pShow->db);
  if (pDb == NULL) {
    return TSDB_CODE_MND_DB_NOT_SELECTED;
  }
  
  if (pDb->status != TSDB_DB_STATUS_READY) {
    mError("db:%s, status:%d, in dropping", pDb->name, pDb->status);
    return TSDB_CODE_MND_DB_IN_DROPPING;
  }

  int32_t cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "vgId");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "tables");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "poolSize");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "maxTables");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->maxReplica = 1;
  for (int32_t v = 0; v < pDb->numOfVgroups; ++v) {
    SVgObj *pVgroup = pDb->vgList[v];
    if (pVgroup != NULL) {
      pShow->maxReplica = pVgroup->numOfVnodes > pShow->maxReplica ? pVgroup->numOfVnodes : pShow->maxReplica;
    }
  }

  for (int32_t i = 0; i < pShow->maxReplica; ++i) {
    pShow->bytes[cols] = 2;
    pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
    strcpy(pSchema[cols].name, "dnode");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;

    pShow->bytes[cols] = 40 + VARSTR_HEADER_SIZE;
    pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
    strcpy(pSchema[cols].name, "end_point");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;

    pShow->bytes[cols] = 9 + VARSTR_HEADER_SIZE;
    pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
    strcpy(pSchema[cols].name, "vstatus");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;
  }

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = pDb->numOfVgroups;
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  mnodeDecDbRef(pDb);
  return 0;
}

static bool mnodeFilterVgroups(SVgObj *pVgroup, STableObj *pTable) {
  if (NULL == pTable || pTable->type == TSDB_SUPER_TABLE) {
    return true;
  }

  SChildTableObj *pCTable = (SChildTableObj *)pTable;
  if (pVgroup->vgId == pCTable->vgId) {
    return true;
  } else {
    return false;
  }
}

static int32_t mnodeRetrieveVgroups(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t numOfRows = 0;
  SVgObj *pVgroup = NULL;
  int32_t cols = 0;
  char *  pWrite;

  SDbObj *pDb = mnodeGetDb(pShow->db);
  if (pDb == NULL) return 0;

  if (pDb->status != TSDB_DB_STATUS_READY) {
    mError("db:%s, status:%d, in dropping", pDb->name, pDb->status);
    return 0;
  }

  STableObj *pTable = NULL;
  if (pShow->payloadLen > 0 ) {
    pTable = mnodeGetTable(pShow->payload);
  }

  while (numOfRows < rows) {
    pShow->pIter = mnodeGetNextVgroup(pShow->pIter, &pVgroup);
    if (pVgroup == NULL) break;
    if (pVgroup->pDb != pDb) continue;
    if (!mnodeFilterVgroups(pVgroup, pTable)) continue;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *) pWrite = pVgroup->vgId;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *) pWrite = pVgroup->numOfTables;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = taosIdPoolMaxSize(pVgroup->idPool);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = tsMaxTablePerVnode;
    cols++;

    for (int32_t i = 0; i < pShow->maxReplica; ++i) {
      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int16_t *) pWrite = pVgroup->vnodeGid[i].dnodeId;
      cols++;

      SDnodeObj *pDnode = pVgroup->vnodeGid[i].pDnode;

      if (pDnode != NULL) {
        pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
        STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pDnode->dnodeEp, pShow->bytes[cols]);
        cols++;

        pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
        char *role = mnodeGetMnodeRoleStr(pVgroup->vnodeGid[i].role);
        STR_WITH_MAXSIZE_TO_VARSTR(pWrite, role, pShow->bytes[cols]);
        cols++;
      } else {
        pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
        const char *src = "NULL";
        STR_WITH_SIZE_TO_VARSTR(pWrite, src, strlen(src));
        cols++;
        
        pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
        STR_WITH_SIZE_TO_VARSTR(pWrite, src, strlen(src));
        cols++;
      }
    }

    mnodeDecVgroupRef(pVgroup);
    numOfRows++;
  }

  pShow->numOfReads += numOfRows;
  mnodeDecTableRef(pTable);
  mnodeDecDbRef(pDb);

  return numOfRows;
}

void mnodeAddTableIntoVgroup(SVgObj *pVgroup, SChildTableObj *pTable) {
  int32_t idPoolSize = taosIdPoolMaxSize(pVgroup->idPool);
  if (pTable->sid > idPoolSize) {
    mnodeAllocVgroupIdPool(pVgroup);
  }

  if (pTable->sid >= 1) {
    taosIdPoolMarkStatus(pVgroup->idPool, pTable->sid);
    pVgroup->numOfTables++;
    mnodeIncVgroupRef(pVgroup);
  }
}

void mnodeRemoveTableFromVgroup(SVgObj *pVgroup, SChildTableObj *pTable) {
  if (pTable->sid >= 1) {
    taosFreeId(pVgroup->idPool, pTable->sid);
    pVgroup->numOfTables--;
    mnodeDecVgroupRef(pVgroup);
  }
}

static SMDCreateVnodeMsg *mnodeBuildVnodeMsg(SVgObj *pVgroup) {
  SDbObj *pDb = pVgroup->pDb;
  if (pDb == NULL) return NULL;

  SMDCreateVnodeMsg *pVnode = rpcMallocCont(sizeof(SMDCreateVnodeMsg));
  if (pVnode == NULL) return NULL;

  strcpy(pVnode->db, pVgroup->dbName);
  int32_t maxTables = taosIdPoolMaxSize(pVgroup->idPool);
  //TODO: dynamic alloc tables in tsdb
  maxTables = MAX(10000, tsMaxTablePerVnode);

  SMDVnodeCfg *pCfg = &pVnode->cfg;
  pCfg->vgId                = htonl(pVgroup->vgId);
  pCfg->cfgVersion          = htonl(pDb->cfgVersion);
  pCfg->cacheBlockSize      = htonl(pDb->cfg.cacheBlockSize);
  pCfg->totalBlocks         = htonl(pDb->cfg.totalBlocks);
  pCfg->maxTables           = htonl(maxTables + 1);
  pCfg->daysPerFile         = htonl(pDb->cfg.daysPerFile);
  pCfg->daysToKeep          = htonl(pDb->cfg.daysToKeep);
  pCfg->daysToKeep1         = htonl(pDb->cfg.daysToKeep1);
  pCfg->daysToKeep2         = htonl(pDb->cfg.daysToKeep2);  
  pCfg->minRowsPerFileBlock = htonl(pDb->cfg.minRowsPerFileBlock);
  pCfg->maxRowsPerFileBlock = htonl(pDb->cfg.maxRowsPerFileBlock);
  pCfg->fsyncPeriod         = htonl(pDb->cfg.fsyncPeriod);
  pCfg->commitTime          = htonl(pDb->cfg.commitTime);
  pCfg->precision           = pDb->cfg.precision;
  pCfg->compression         = pDb->cfg.compression;
  pCfg->walLevel            = pDb->cfg.walLevel;
  pCfg->replications        = (int8_t) pVgroup->numOfVnodes;
  pCfg->wals                = 3;
  pCfg->quorum              = 1;
  
  SMDVnodeDesc *pNodes = pVnode->nodes;
  for (int32_t j = 0; j < pVgroup->numOfVnodes; ++j) {
    SDnodeObj *pDnode = pVgroup->vnodeGid[j].pDnode;
    if (pDnode != NULL) {
      pNodes[j].nodeId = htonl(pDnode->dnodeId);
      strcpy(pNodes[j].nodeEp, pDnode->dnodeEp);
    }
  }

  return pVnode;
}

SRpcIpSet mnodeGetIpSetFromVgroup(SVgObj *pVgroup) {
  SRpcIpSet ipSet = {
    .numOfIps = pVgroup->numOfVnodes,
    .inUse = 0,
  };
  for (int i = 0; i < pVgroup->numOfVnodes; ++i) {
    strcpy(ipSet.fqdn[i], pVgroup->vnodeGid[i].pDnode->dnodeFqdn);
    ipSet.port[i] = pVgroup->vnodeGid[i].pDnode->dnodePort + TSDB_PORT_DNODEDNODE;
  }
  return ipSet;
}

SRpcIpSet mnodeGetIpSetFromIp(char *ep) {
  SRpcIpSet ipSet;

  ipSet.numOfIps = 1;
  ipSet.inUse = 0;
  taosGetFqdnPortFromEp(ep, ipSet.fqdn[0], &ipSet.port[0]);
  ipSet.port[0] += TSDB_PORT_DNODEDNODE;
  return ipSet;
}

static void mnodeSendAlterVnodeMsg(SVgObj *pVgroup, SRpcIpSet *ipSet) {
  SMDAlterVnodeMsg *pAlter = mnodeBuildVnodeMsg(pVgroup);
  SRpcMsg rpcMsg = {
    .ahandle = NULL,
    .pCont   = pAlter,
    .contLen = pAlter ? sizeof(SMDAlterVnodeMsg) : 0,
    .code    = 0,
    .msgType = TSDB_MSG_TYPE_MD_ALTER_VNODE
  };
  dnodeSendMsgToDnode(ipSet, &rpcMsg);
}

void mnodeSendAlterVgroupMsg(SVgObj *pVgroup) {
  mDebug("vgId:%d, send alter all vnodes msg, numOfVnodes:%d db:%s", pVgroup->vgId, pVgroup->numOfVnodes,
         pVgroup->dbName);
  for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
    SRpcIpSet ipSet = mnodeGetIpSetFromIp(pVgroup->vnodeGid[i].pDnode->dnodeEp);
    mDebug("vgId:%d, index:%d, send alter vnode msg to dnode %s", pVgroup->vgId, i,
           pVgroup->vnodeGid[i].pDnode->dnodeEp);
    mnodeSendAlterVnodeMsg(pVgroup, &ipSet);
  }
}

static void mnodeSendCreateVnodeMsg(SVgObj *pVgroup, SRpcIpSet *ipSet, void *ahandle) {
  SMDCreateVnodeMsg *pCreate = mnodeBuildVnodeMsg(pVgroup);
  SRpcMsg rpcMsg = {
    .ahandle = ahandle,
    .pCont   = pCreate,
    .contLen = pCreate ? sizeof(SMDCreateVnodeMsg) : 0,
    .code    = 0,
    .msgType = TSDB_MSG_TYPE_MD_CREATE_VNODE
  };
  dnodeSendMsgToDnode(ipSet, &rpcMsg);
}

void mnodeSendCreateVgroupMsg(SVgObj *pVgroup, void *ahandle) {
  mDebug("vgId:%d, send create all vnodes msg, numOfVnodes:%d db:%s", pVgroup->vgId, pVgroup->numOfVnodes,
         pVgroup->dbName);
  for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
    SRpcIpSet ipSet = mnodeGetIpSetFromIp(pVgroup->vnodeGid[i].pDnode->dnodeEp);
    mDebug("vgId:%d, index:%d, send create vnode msg to dnode %s, ahandle:%p", pVgroup->vgId,
           i, pVgroup->vnodeGid[i].pDnode->dnodeEp, ahandle);
    mnodeSendCreateVnodeMsg(pVgroup, &ipSet, ahandle);
  }
}

static void mnodeProcessAlterVnodeRsp(SRpcMsg *rpcMsg) {
  mDebug("alter vnode rsp received");
}

static void mnodeProcessCreateVnodeRsp(SRpcMsg *rpcMsg) {
  if (rpcMsg->ahandle == NULL) return;

  SMnodeMsg *mnodeMsg = rpcMsg->ahandle;
  atomic_add_fetch_8(&mnodeMsg->received, 1);
  if (rpcMsg->code == TSDB_CODE_SUCCESS) {
    atomic_add_fetch_8(&mnodeMsg->successed, 1);
  } else {
    mnodeMsg->code = rpcMsg->code;
  }

  SVgObj *pVgroup = mnodeMsg->pVgroup;
  mDebug("vgId:%d, create vnode rsp received, result:%s received:%d successed:%d expected:%d, thandle:%p ahandle:%p",
         pVgroup->vgId, tstrerror(rpcMsg->code), mnodeMsg->received, mnodeMsg->successed, mnodeMsg->expected,
         mnodeMsg->rpcMsg.handle, rpcMsg->ahandle);

  assert(mnodeMsg->received <= mnodeMsg->expected);

  if (mnodeMsg->received != mnodeMsg->expected) return;

  if (mnodeMsg->received == mnodeMsg->successed) {
    dnodeReprocessMnodeWriteMsg(mnodeMsg);
  } else {
    SSdbOper oper = {
      .type = SDB_OPER_GLOBAL,
      .table = tsVgroupSdb,
      .pObj = pVgroup
    };
    int32_t code = sdbDeleteRow(&oper);
    if (code != 0) {
      code = TSDB_CODE_MND_SDB_ERROR;
    }

    dnodeSendRpcMnodeWriteRsp(mnodeMsg, mnodeMsg->code);
  }
}

static SMDDropVnodeMsg *mnodeBuildDropVnodeMsg(int32_t vgId) {
  SMDDropVnodeMsg *pDrop = rpcMallocCont(sizeof(SMDDropVnodeMsg));
  if (pDrop == NULL) return NULL;

  pDrop->vgId = htonl(vgId);
  return pDrop;
}

void mnodeSendDropVnodeMsg(int32_t vgId, SRpcIpSet *ipSet, void *ahandle) {
  SMDDropVnodeMsg *pDrop = mnodeBuildDropVnodeMsg(vgId);
  SRpcMsg rpcMsg = {
      .ahandle = ahandle,
      .pCont   = pDrop,
      .contLen = pDrop ? sizeof(SMDDropVnodeMsg) : 0,
      .code    = 0,
      .msgType = TSDB_MSG_TYPE_MD_DROP_VNODE
  };
  dnodeSendMsgToDnode(ipSet, &rpcMsg);
}

static void mnodeSendDropVgroupMsg(SVgObj *pVgroup, void *ahandle) {
  pVgroup->status = TAOS_VG_STATUS_DROPPING; // deleting
  mDebug("vgId:%d, send drop all vnodes msg, ahandle:%p", pVgroup->vgId, ahandle);
  for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
    SRpcIpSet ipSet = mnodeGetIpSetFromIp(pVgroup->vnodeGid[i].pDnode->dnodeEp);
    mDebug("vgId:%d, send drop vnode msg to dnode:%d, ahandle:%p", pVgroup->vgId, pVgroup->vnodeGid[i].dnodeId, ahandle);
    mnodeSendDropVnodeMsg(pVgroup->vgId, &ipSet, ahandle);
  }
}

static void mnodeProcessDropVnodeRsp(SRpcMsg *rpcMsg) {
  mDebug("drop vnode rsp is received, handle:%p", rpcMsg->ahandle);
  if (rpcMsg->ahandle == NULL) return;

  SMnodeMsg *mnodeMsg = rpcMsg->ahandle;
  mnodeMsg->received++;
  if (rpcMsg->code == TSDB_CODE_SUCCESS) {
    mnodeMsg->code = rpcMsg->code;
    mnodeMsg->successed++;
  }

  SVgObj *pVgroup = mnodeMsg->pVgroup;
  mDebug("vgId:%d, drop vnode rsp received, result:%s received:%d successed:%d expected:%d, thandle:%p ahandle:%p",
         pVgroup->vgId, tstrerror(rpcMsg->code), mnodeMsg->received, mnodeMsg->successed, mnodeMsg->expected,
         mnodeMsg->rpcMsg.handle, rpcMsg->ahandle);

  if (mnodeMsg->received != mnodeMsg->expected) return;

  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsVgroupSdb,
    .pObj = pVgroup
  };
  int32_t code = sdbDeleteRow(&oper);
  if (code != 0) {
    code = TSDB_CODE_MND_SDB_ERROR;
  }

  dnodeReprocessMnodeWriteMsg(mnodeMsg);
}

static int32_t mnodeProcessVnodeCfgMsg(SMnodeMsg *pMsg) {
  SDMConfigVnodeMsg *pCfg = pMsg->rpcMsg.pCont;
  pCfg->dnodeId = htonl(pCfg->dnodeId);
  pCfg->vgId    = htonl(pCfg->vgId);

  SDnodeObj *pDnode = mnodeGetDnode(pCfg->dnodeId);
  if (pDnode == NULL) {
    mDebug("dnode:%s, vgId:%d, invalid dnode", taosIpStr(pCfg->dnodeId), pCfg->vgId);
    return TSDB_CODE_MND_VGROUP_NOT_EXIST;
  }

  SVgObj *pVgroup = mnodeGetVgroup(pCfg->vgId);
  if (pVgroup == NULL) {
    mDebug("dnode:%s, vgId:%d, no vgroup info", taosIpStr(pCfg->dnodeId), pCfg->vgId);
    mnodeDecDnodeRef(pDnode);
    return TSDB_CODE_MND_VGROUP_NOT_EXIST;
  }

  mDebug("vgId:%d, send create vnode msg to dnode %s for vnode cfg msg", pVgroup->vgId, pDnode->dnodeEp);
  SRpcIpSet ipSet = mnodeGetIpSetFromIp(pDnode->dnodeEp);
  mnodeSendCreateVnodeMsg(pVgroup, &ipSet, NULL);

  mnodeDecDnodeRef(pDnode);
  mnodeDecVgroupRef(pVgroup);
  return TSDB_CODE_SUCCESS;
}

void mnodeDropAllDnodeVgroups(SDnodeObj *pDropDnode) {
  void *  pIter = NULL;
  SVgObj *pVgroup = NULL;
  int32_t numOfVgroups = 0;

  mInfo("dnode:%d, all vgroups will be dropped from sdb", pDropDnode->dnodeId);

  while (1) {
    pIter = mnodeGetNextVgroup(pIter, &pVgroup);
    if (pVgroup == NULL) break;

    if (pVgroup->vnodeGid[0].dnodeId == pDropDnode->dnodeId) {
      mnodeDropAllChildTablesInVgroups(pVgroup);
      SSdbOper oper = {
        .type = SDB_OPER_LOCAL,
        .table = tsVgroupSdb,
        .pObj = pVgroup,
      };
      sdbDeleteRow(&oper);
      numOfVgroups++;
    }
    mnodeDecVgroupRef(pVgroup);
  }

  sdbFreeIter(pIter);

  mInfo("dnode:%d, all vgroups:%d is dropped from sdb", pDropDnode->dnodeId, numOfVgroups);
}

#if 0
void mnodeUpdateAllDbVgroups(SDbObj *pAlterDb) {
  void *  pIter = NULL;
  SVgObj *pVgroup = NULL;

  mInfo("db:%s, all vgroups will be update in sdb", pAlterDb->name);

  while (1) {
    pIter = mnodeGetNextVgroup(pIter, &pVgroup);
    if (pVgroup == NULL) break;

    if (pVgroup->pDb == pAlterDb) {
      mnodeVgroupUpdateIdPool(pVgroup);
    }

    mnodeDecVgroupRef(pVgroup);
  }

  sdbFreeIter(pIter);

  mInfo("db:%s, all vgroups is updated in sdb", pAlterDb->name);
}
#endif

void mnodeDropAllDbVgroups(SDbObj *pDropDb) {
  void *  pIter = NULL;
  int32_t numOfVgroups = 0;
  SVgObj *pVgroup = NULL;

  mInfo("db:%s, all vgroups will be dropped from sdb", pDropDb->name);
  while (1) {
    pIter = mnodeGetNextVgroup(pIter, &pVgroup);
    if (pVgroup == NULL) break;

    if (pVgroup->pDb == pDropDb) {
      SSdbOper oper = {
        .type = SDB_OPER_LOCAL,
        .table = tsVgroupSdb,
        .pObj = pVgroup,
      };
      sdbDeleteRow(&oper);
      numOfVgroups++;
    }

    mnodeDecVgroupRef(pVgroup);
  }

  sdbFreeIter(pIter);

  mInfo("db:%s, all vgroups:%d is dropped from sdb", pDropDb->name, numOfVgroups);
}

void mnodeSendDropAllDbVgroupsMsg(SDbObj *pDropDb) {
  void *  pIter = NULL;
  int32_t numOfVgroups = 0;
  SVgObj *pVgroup = NULL;

  mInfo("db:%s, all vgroups will be dropped in dnode", pDropDb->name);
  while (1) {
    pIter = mnodeGetNextVgroup(pIter, &pVgroup);
    if (pVgroup == NULL) break;

    if (pVgroup->pDb == pDropDb) {
      mnodeSendDropVgroupMsg(pVgroup, NULL);
    }

    mnodeDecVgroupRef(pVgroup);
  }

  sdbFreeIter(pIter);

  mInfo("db:%s, all vgroups:%d drop msg is sent to dnode", pDropDb->name, numOfVgroups);
}
