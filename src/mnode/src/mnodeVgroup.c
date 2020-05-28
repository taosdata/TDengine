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

static void   *tsVgroupSdb = NULL;
static int32_t tsVgUpdateSize = 0;

static int32_t mnodeGetVgroupMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t mnodeRetrieveVgroups(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static void    mnodeProcessCreateVnodeRsp(SRpcMsg *rpcMsg);
static void    mnodeProcessDropVnodeRsp(SRpcMsg *rpcMsg);
static int32_t mnodeProcessVnodeCfgMsg(SMnodeMsg *pMsg) ;
static void    mnodeSendDropVgroupMsg(SVgObj *pVgroup, void *ahandle);

static int32_t mnodeVgroupActionDestroy(SSdbOper *pOper) {
  SVgObj *pVgroup = pOper->pObj;
  if (pVgroup->idPool) {
    taosIdPoolCleanUp(pVgroup->idPool);
    pVgroup->idPool = NULL;
  }
  if (pVgroup->tableList) {
    tfree(pVgroup->tableList);
  }

  tfree(pOper->pObj);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeVgroupActionInsert(SSdbOper *pOper) {
  SVgObj *pVgroup = pOper->pObj;

  // refer to db
  SDbObj *pDb = mnodeGetDb(pVgroup->dbName);
  if (pDb == NULL) {
    return TSDB_CODE_INVALID_DB;
  }

  pVgroup->pDb = pDb;
  pVgroup->prev = NULL;
  pVgroup->next = NULL;

  int32_t size = sizeof(SChildTableObj *) * pDb->cfg.maxTables;
  pVgroup->tableList = calloc(pDb->cfg.maxTables, sizeof(SChildTableObj *));
  if (pVgroup->tableList == NULL) {
    mError("vgId:%d, failed to malloc(size:%d) for the tableList of vgroups", pVgroup->vgId, size);
    return -1;
  }

  pVgroup->idPool = taosInitIdPool(pDb->cfg.maxTables);
  if (pVgroup->idPool == NULL) {
    mError("vgId:%d, failed to taosInitIdPool for vgroups", pVgroup->vgId);
    tfree(pVgroup->tableList);
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

static void mnodeVgroupUpdateIdPool(SVgObj *pVgroup) {
  int32_t oldTables = taosIdPoolMaxSize(pVgroup->idPool);
  SDbObj *pDb = pVgroup->pDb;
  if (pDb != NULL) {
    if (pDb->cfg.maxTables != oldTables) {
      mPrint("vgId:%d tables change from %d to %d", pVgroup->vgId, oldTables, pDb->cfg.maxTables);
      taosUpdateIdPool(pVgroup->idPool, pDb->cfg.maxTables);
      int32_t size = sizeof(SChildTableObj *) * pDb->cfg.maxTables;
      pVgroup->tableList = (SChildTableObj **)realloc(pVgroup->tableList, size);
      memset(pVgroup->tableList + oldTables, 0, (pDb->cfg.maxTables - oldTables) * sizeof(SChildTableObj **));
    }
  }
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

    memcpy(pVgroup, pNew, pOper->rowSize);
    free(pNew);

    for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
      SDnodeObj *pDnode = mnodeGetDnode(pVgroup->vnodeGid[i].dnodeId);
      pVgroup->vnodeGid[i].pDnode = pDnode;
      if (pDnode != NULL) {
        atomic_add_fetch_32(&pDnode->openVnodes, 1);
      }
      mnodeDecDnodeRef(pDnode);
    }
  }

  mnodeVgroupUpdateIdPool(pVgroup);

  mnodeDecVgroupRef(pVgroup);

  mTrace("vgId:%d, is updated, numOfVnode:%d", pVgroup->vgId, pVgroup->numOfVnodes);
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
  if (pVgroup == NULL) return TSDB_CODE_SERV_OUT_OF_MEMORY;

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
  mnodeAddPeerRspHandle(TSDB_MSG_TYPE_MD_DROP_VNODE_RSP, mnodeProcessDropVnodeRsp);
  mnodeAddPeerMsgHandle(TSDB_MSG_TYPE_DM_CONFIG_VNODE, mnodeProcessVnodeCfgMsg);

  mTrace("table:vgroups is created");
  
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

  sdbUpdateRow(&oper);
  mnodeSendCreateVgroupMsg(pVgroup, NULL);
}

void mnodeUpdateVgroupStatus(SVgObj *pVgroup, SDnodeObj *pDnode, SVnodeLoad *pVload) {
  bool dnodeExist = false;
  for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
    SVnodeGid *pVgid = &pVgroup->vnodeGid[i];
    if (pVgid->pDnode == pDnode) {
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
    mnodeSendCreateVgroupMsg(pVgroup, NULL);
  }
}

SVgObj *mnodeGetAvailableVgroup(SDbObj *pDb) {
  return pDb->pHead;
}

void *mnodeGetNextVgroup(void *pIter, SVgObj **pVgroup) { 
  return sdbFetchRow(tsVgroupSdb, pIter, (void **)pVgroup); 
}

int32_t mnodeCreateVgroup(SMnodeMsg *pMsg, SDbObj *pDb) {
  SVgObj *pVgroup = (SVgObj *)calloc(1, sizeof(SVgObj));
  strcpy(pVgroup->dbName, pDb->name);
  pVgroup->numOfVnodes = pDb->cfg.replications;
  pVgroup->createdTime = taosGetTimestampMs();
  if (balanceAllocVnodes(pVgroup) != 0) {
    mError("db:%s, no enough dnode to alloc %d vnodes to vgroup", pDb->name, pVgroup->numOfVnodes);
    free(pVgroup);
    return TSDB_CODE_NO_ENOUGH_DNODES;
  }

  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsVgroupSdb,
    .pObj = pVgroup,
    .rowSize = sizeof(SVgObj)
  };

  int32_t code = sdbInsertRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    tfree(pVgroup);
    return TSDB_CODE_SDB_ERROR;
  }

  mPrint("vgId:%d, is created in mnode, db:%s replica:%d", pVgroup->vgId, pDb->name, pVgroup->numOfVnodes);
  for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
    mPrint("vgId:%d, index:%d, dnode:%d", pVgroup->vgId, i, pVgroup->vnodeGid[i].dnodeId);
  }

  mnodeIncVgroupRef(pVgroup);
  pMsg->pVgroup = pVgroup;
  pMsg->expected = pVgroup->numOfVnodes;
  mnodeSendCreateVgroupMsg(pVgroup, pMsg);

  return TSDB_CODE_ACTION_IN_PROGRESS;
}

void mnodeDropVgroup(SVgObj *pVgroup, void *ahandle) {
  if (ahandle != NULL) {
    mnodeSendDropVgroupMsg(pVgroup, ahandle);
  } else {
    mTrace("vgId:%d, replica:%d is deleting from sdb", pVgroup->vgId, pVgroup->numOfVnodes);
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
}

int32_t mnodeGetVgroupMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SDbObj *pDb = mnodeGetDb(pShow->db);
  if (pDb == NULL) {
    return TSDB_CODE_DB_NOT_SELECTED;
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

  int32_t maxReplica = 0;
  SVgObj  *pVgroup   = NULL;
  STableObj *pTable = NULL;
  if (pShow->payloadLen > 0 ) {
    pTable = mnodeGetTable(pShow->payload);
    if (NULL == pTable || pTable->type == TSDB_SUPER_TABLE) {
      mnodeDecTableRef(pTable);
      return TSDB_CODE_INVALID_TABLE_ID;
    }
    mnodeDecTableRef(pTable);
    pVgroup = mnodeGetVgroup(((SChildTableObj*)pTable)->vgId);
    if (NULL == pVgroup) return TSDB_CODE_INVALID_TABLE_ID;
    mnodeDecVgroupRef(pVgroup);
    maxReplica = pVgroup->numOfVnodes > maxReplica ? pVgroup->numOfVnodes : maxReplica;
  } else {
    SVgObj *pVgroup = pDb->pHead;
    while (pVgroup != NULL) {
      maxReplica = pVgroup->numOfVnodes > maxReplica ? pVgroup->numOfVnodes : maxReplica;
      pVgroup = pVgroup->next;
    }
  }

  for (int32_t i = 0; i < maxReplica; ++i) {
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
  for (int32_t i = 1; i < cols; ++i) pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];

  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  if (NULL == pTable) {
    pShow->numOfRows = pDb->numOfVgroups;
    pShow->pIter = pDb->pHead;
  } else {
    pShow->numOfRows = 1;
    pShow->pIter = pVgroup;
  }

   mnodeDecDbRef(pDb);

  return 0;
}

int32_t mnodeRetrieveVgroups(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t numOfRows = 0;
  SVgObj *pVgroup = NULL;
  int32_t maxReplica = 0;
  int32_t cols = 0;
  char *  pWrite;

  SDbObj *pDb = mnodeGetDb(pShow->db);
  if (pDb == NULL) return 0;

  pVgroup = pDb->pHead;
  while (pVgroup != NULL) {
    maxReplica = pVgroup->numOfVnodes > maxReplica ? pVgroup->numOfVnodes : maxReplica;
    pVgroup    = pVgroup->next;
  }

  while (numOfRows < rows) {
    pVgroup = (SVgObj *) pShow->pIter;
    if (pVgroup == NULL) break;
    pShow->pIter = (void *) pVgroup->next;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *) pWrite = pVgroup->vgId;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *) pWrite = pVgroup->numOfTables;
    cols++;

    for (int32_t i = 0; i < maxReplica; ++i) {
      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int16_t *) pWrite = pVgroup->vnodeGid[i].dnodeId;
      cols++;

      SDnodeObj *pDnode = pVgroup->vnodeGid[i].pDnode;

      if (pDnode != NULL) {
        pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
        STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pDnode->dnodeEp, pShow->bytes[cols] - VARSTR_HEADER_SIZE);
        cols++;

        pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
        char *role = mnodeGetMnodeRoleStr(pVgroup->vnodeGid[i].role);
        STR_TO_VARSTR(pWrite, role);
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

    numOfRows++;
  }

  pShow->numOfReads += numOfRows;
  mnodeDecDbRef(pDb);

  return numOfRows;
}

void mnodeAddTableIntoVgroup(SVgObj *pVgroup, SChildTableObj *pTable) {
  if (pTable->sid >= 1 && pVgroup->tableList[pTable->sid - 1] == NULL) {
    pVgroup->tableList[pTable->sid - 1] = pTable;
    taosIdPoolMarkStatus(pVgroup->idPool, pTable->sid);
    pVgroup->numOfTables++;
  }
  
  if (pVgroup->numOfTables >= pVgroup->pDb->cfg.maxTables) {
    mnodeMoveVgroupToTail(pVgroup);
  }

  mnodeIncVgroupRef(pVgroup);
}

void mnodeRemoveTableFromVgroup(SVgObj *pVgroup, SChildTableObj *pTable) {
  if (pTable->sid >= 1 && pVgroup->tableList[pTable->sid - 1] != NULL) {
    pVgroup->tableList[pTable->sid - 1] = NULL;
    taosFreeId(pVgroup->idPool, pTable->sid);
    pVgroup->numOfTables--;
  }

  mnodeMoveVgroupToHead(pVgroup);
  mnodeDecVgroupRef(pVgroup);
}

SMDCreateVnodeMsg *mnodeBuildCreateVnodeMsg(SVgObj *pVgroup) {
  SDbObj *pDb = pVgroup->pDb;
  if (pDb == NULL) return NULL;

  SMDCreateVnodeMsg *pVnode = rpcMallocCont(sizeof(SMDCreateVnodeMsg));
  if (pVnode == NULL) return NULL;

  SMDVnodeCfg *pCfg = &pVnode->cfg;
  pCfg->vgId                = htonl(pVgroup->vgId);
  pCfg->cfgVersion          = htonl(pDb->cfgVersion);
  pCfg->cacheBlockSize      = htonl(pDb->cfg.cacheBlockSize);
  pCfg->totalBlocks         = htonl(pDb->cfg.totalBlocks);
  pCfg->maxTables           = htonl(pDb->cfg.maxTables + 1);
  pCfg->daysPerFile         = htonl(pDb->cfg.daysPerFile);
  pCfg->daysToKeep          = htonl(pDb->cfg.daysToKeep);
  pCfg->daysToKeep1         = htonl(pDb->cfg.daysToKeep1);
  pCfg->daysToKeep2         = htonl(pDb->cfg.daysToKeep2);  
  pCfg->minRowsPerFileBlock = htonl(pDb->cfg.minRowsPerFileBlock);
  pCfg->maxRowsPerFileBlock = htonl(pDb->cfg.maxRowsPerFileBlock);
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

void mnodeSendCreateVnodeMsg(SVgObj *pVgroup, SRpcIpSet *ipSet, void *ahandle) {
  mTrace("vgId:%d, send create vnode:%d msg, ahandle:%p", pVgroup->vgId, pVgroup->vgId, ahandle);
  SMDCreateVnodeMsg *pCreate = mnodeBuildCreateVnodeMsg(pVgroup);
  SRpcMsg rpcMsg = {
    .handle  = ahandle,
    .pCont   = pCreate,
    .contLen = pCreate ? sizeof(SMDCreateVnodeMsg) : 0,
    .code    = 0,
    .msgType = TSDB_MSG_TYPE_MD_CREATE_VNODE
  };
  dnodeSendMsgToDnode(ipSet, &rpcMsg);
}

void mnodeSendCreateVgroupMsg(SVgObj *pVgroup, void *ahandle) {
  mTrace("vgId:%d, send create all vnodes msg, ahandle:%p", pVgroup->vgId, ahandle);
  for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
    SRpcIpSet ipSet = mnodeGetIpSetFromIp(pVgroup->vnodeGid[i].pDnode->dnodeEp);
    mnodeSendCreateVnodeMsg(pVgroup, &ipSet, ahandle);
  }
}

static void mnodeProcessCreateVnodeRsp(SRpcMsg *rpcMsg) {
  if (rpcMsg->handle == NULL) return;

  SMnodeMsg *mnodeMsg = rpcMsg->handle;
  mnodeMsg->received++;
  if (rpcMsg->code == TSDB_CODE_SUCCESS) {
    mnodeMsg->code = rpcMsg->code;
    mnodeMsg->successed++;
  }

  SVgObj *pVgroup = mnodeMsg->pVgroup;
  mTrace("vgId:%d, create vnode rsp received, result:%s received:%d successed:%d expected:%d, thandle:%p ahandle:%p",
         pVgroup->vgId, tstrerror(rpcMsg->code), mnodeMsg->received, mnodeMsg->successed, mnodeMsg->expected,
         mnodeMsg->rpcMsg.handle, rpcMsg->handle);

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
      code = TSDB_CODE_SDB_ERROR;
    }

    dnodeSendRpcMnodeWriteRsp(mnodeMsg, code);
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
      .handle  = ahandle,
      .pCont   = pDrop,
      .contLen = pDrop ? sizeof(SMDDropVnodeMsg) : 0,
      .code    = 0,
      .msgType = TSDB_MSG_TYPE_MD_DROP_VNODE
  };
  dnodeSendMsgToDnode(ipSet, &rpcMsg);
}

static void mnodeSendDropVgroupMsg(SVgObj *pVgroup, void *ahandle) {
  mTrace("vgId:%d, send drop all vnodes msg, ahandle:%p", pVgroup->vgId, ahandle);
  for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
    SRpcIpSet ipSet = mnodeGetIpSetFromIp(pVgroup->vnodeGid[i].pDnode->dnodeEp);
    mTrace("vgId:%d, send drop vnode msg to dnode:%d, ahandle:%p", pVgroup->vgId, pVgroup->vnodeGid[i].dnodeId, ahandle);
    mnodeSendDropVnodeMsg(pVgroup->vgId, &ipSet, ahandle);
  }
}

static void mnodeProcessDropVnodeRsp(SRpcMsg *rpcMsg) {
  mTrace("drop vnode rsp is received, handle:%p", rpcMsg->handle);
  if (rpcMsg->handle == NULL) return;

  SMnodeMsg *mnodeMsg = rpcMsg->handle;
  mnodeMsg->received++;
  if (rpcMsg->code == TSDB_CODE_SUCCESS) {
    mnodeMsg->code = rpcMsg->code;
    mnodeMsg->successed++;
  }

  SVgObj *pVgroup = mnodeMsg->pVgroup;
  mTrace("vgId:%d, drop vnode rsp received, result:%s received:%d successed:%d expected:%d, thandle:%p ahandle:%p",
         pVgroup->vgId, tstrerror(rpcMsg->code), mnodeMsg->received, mnodeMsg->successed, mnodeMsg->expected,
         mnodeMsg->rpcMsg.handle, rpcMsg->handle);

  if (mnodeMsg->received != mnodeMsg->expected) return;

  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsVgroupSdb,
    .pObj = pVgroup
  };
  int32_t code = sdbDeleteRow(&oper);
  if (code != 0) {
    code = TSDB_CODE_SDB_ERROR;
  }

  dnodeReprocessMnodeWriteMsg(mnodeMsg);
}

static int32_t mnodeProcessVnodeCfgMsg(SMnodeMsg *pMsg) {
  SDMConfigVnodeMsg *pCfg = pMsg->rpcMsg.pCont;
  pCfg->dnodeId = htonl(pCfg->dnodeId);
  pCfg->vgId    = htonl(pCfg->vgId);

  SDnodeObj *pDnode = mnodeGetDnode(pCfg->dnodeId);
  if (pDnode == NULL) {
    mTrace("dnode:%s, invalid dnode", taosIpStr(pCfg->dnodeId), pCfg->vgId);
    return TSDB_CODE_NOT_ACTIVE_VNODE;
  }
  mnodeDecDnodeRef(pDnode);

  SVgObj *pVgroup = mnodeGetVgroup(pCfg->vgId);
  if (pVgroup == NULL) {
    mTrace("dnode:%s, vgId:%d, no vgroup info", taosIpStr(pCfg->dnodeId), pCfg->vgId);
    return TSDB_CODE_NOT_ACTIVE_VNODE;
  }
  mnodeDecVgroupRef(pVgroup);

  SRpcIpSet ipSet = mnodeGetIpSetFromIp(pDnode->dnodeEp);
  mnodeSendCreateVnodeMsg(pVgroup, &ipSet, NULL);

  return TSDB_CODE_SUCCESS;
}

void mnodeDropAllDnodeVgroups(SDnodeObj *pDropDnode) {
  void *  pIter = NULL;
  SVgObj *pVgroup = NULL;
  int32_t numOfVgroups = 0;

  mPrint("dnode:%d, all vgroups will be dropped from sdb", pDropDnode->dnodeId);

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

  mPrint("dnode:%d, all vgroups is dropped from sdb", pDropDnode->dnodeId);
}

void mnodeUpdateAllDbVgroups(SDbObj *pAlterDb) {
  void *  pIter = NULL;
  SVgObj *pVgroup = NULL;

  mPrint("db:%s, all vgroups will be update in sdb", pAlterDb->name);

  while (1) {
    pIter = mnodeGetNextVgroup(pIter, &pVgroup);
    if (pVgroup == NULL) break;

    if (pVgroup->pDb == pAlterDb) {
      mnodeVgroupUpdateIdPool(pVgroup);
    }

    mnodeDecVgroupRef(pVgroup);
  }

  sdbFreeIter(pIter);

  mPrint("db:%s, all vgroups is updated in sdb", pAlterDb->name);
}

void mnodeDropAllDbVgroups(SDbObj *pDropDb, bool sendMsg) {
  void *  pIter = NULL;
  int32_t numOfVgroups = 0;
  SVgObj *pVgroup = NULL;

  mPrint("db:%s, all vgroups will be dropped from sdb", pDropDb->name);
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

      if (sendMsg) {
        mnodeSendDropVgroupMsg(pVgroup, NULL);
      }
    }

    mnodeDecVgroupRef(pVgroup);
  }

  sdbFreeIter(pIter);

  mPrint("db:%s, all vgroups:%d is dropped from sdb", pDropDb->name, numOfVgroups);
}
