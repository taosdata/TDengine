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
#include "tlog.h"
#include "tschemautil.h"
#include "tstatus.h"
#include "mnode.h"
#include "mgmtBalance.h"
#include "mgmtDb.h"
#include "mgmtDClient.h"
#include "mgmtDnode.h"
#include "mgmtProfile.h"
#include "mgmtShell.h"
#include "mgmtTable.h"
#include "mgmtVgroup.h"

static void *tsVgroupSdb = NULL;
static int32_t tsVgUpdateSize = 0;

static void *(*mgmtVgroupActionFp[SDB_MAX_ACTION_TYPES])(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtVgroupActionInsert(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtVgroupActionDelete(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtVgroupActionUpdate(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtVgroupActionEncode(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtVgroupActionDecode(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtVgroupActionReset(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtVgroupActionDestroy(void *row, char *str, int32_t size, int32_t *ssize);

static int32_t mgmtGetVgroupMeta(STableMeta *pMeta, SShowObj *pShow, void *pConn);
static int32_t mgmtRetrieveVgroups(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static void    mgmtProcessCreateVnodeRsp(SRpcMsg *rpcMsg);

void mgmtSendCreateVgroupMsg(SVgObj *pVgroup, void *ahandle);

static void mgmtVgroupActionInit() {
  SVgObj tObj;
  tsVgUpdateSize = tObj.updateEnd - (int8_t *)&tObj;

  mgmtVgroupActionFp[SDB_TYPE_INSERT]  = mgmtVgroupActionInsert;
  mgmtVgroupActionFp[SDB_TYPE_DELETE]  = mgmtVgroupActionDelete;
  mgmtVgroupActionFp[SDB_TYPE_UPDATE]  = mgmtVgroupActionUpdate;
  mgmtVgroupActionFp[SDB_TYPE_ENCODE]  = mgmtVgroupActionEncode;
  mgmtVgroupActionFp[SDB_TYPE_DECODE]  = mgmtVgroupActionDecode;
  mgmtVgroupActionFp[SDB_TYPE_RESET]   = mgmtVgroupActionReset;
  mgmtVgroupActionFp[SDB_TYPE_DESTROY] = mgmtVgroupActionDestroy;
}

static void *mgmtVgroupAction(char action, void *row, char *str, int32_t size, int32_t *ssize) {
  if (mgmtVgroupActionFp[(uint8_t) action] != NULL) {
    return (*(mgmtVgroupActionFp[(uint8_t) action]))(row, str, size, ssize);
  }
  return NULL;
}

int32_t mgmtInitVgroups() {
  void *pNode = NULL;
  SVgObj *pVgroup = NULL;

  mgmtVgroupActionInit();

  tsVgroupSdb = sdbOpenTable(tsMaxVGroups, tsVgUpdateSize, "vgroups", SDB_KEYTYPE_AUTO, tsMgmtDirectory, mgmtVgroupAction);
  if (tsVgroupSdb == NULL) {
    mError("failed to init vgroups data");
    return -1;
  }

  while (1) {
    pNode = sdbFetchRow(tsVgroupSdb, pNode, (void **)&pVgroup);
    if (pVgroup == NULL) break;

    SDbObj *pDb = mgmtGetDb(pVgroup->dbName);
    if (pDb == NULL) continue;

    pVgroup->prev = NULL;
    pVgroup->next = NULL;

    int32_t size = sizeof(STableInfo *) * pDb->cfg.maxSessions;
    pVgroup->tableList = (STableInfo **)malloc(size);
    if (pVgroup->tableList == NULL) {
      mError("failed to malloc(size:%d) for the tableList of vgroups", size);
      return -1;
    }
    
    memset(pVgroup->tableList, 0, size);

    pVgroup->idPool = taosInitIdPool(pDb->cfg.maxSessions);
    if (pVgroup->idPool == NULL) {
      mError("failed to taosInitIdPool for vgroups");
      free(pVgroup->tableList);
      return -1;
    }
    
    taosIdPoolReinit(pVgroup->idPool);

    if (tsIsCluster && pVgroup->vnodeGid[0].publicIp == 0) {
      pVgroup->vnodeGid[0].publicIp = inet_addr(tsPublicIp);
      pVgroup->vnodeGid[0].ip = inet_addr(tsPrivateIp);
      sdbUpdateRow(tsVgroupSdb, pVgroup, tsVgUpdateSize, 1);
    }

    mgmtSetDnodeVgid(pVgroup->vnodeGid, pVgroup->numOfVnodes, pVgroup->vgId);
  }

  mgmtAddShellShowMetaHandle(TSDB_MGMT_TABLE_VGROUP, mgmtGetVgroupMeta);
  mgmtAddShellShowRetrieveHandle(TSDB_MGMT_TABLE_VGROUP, mgmtRetrieveVgroups);
  mgmtAddDClientRspHandle(TSDB_MSG_TYPE_MD_CREATE_VNODE_RSP, mgmtProcessCreateVnodeRsp);

  mTrace("vgroup is initialized");
  return 0;
}

SVgObj *mgmtGetVgroup(int32_t vgId) {
  return (SVgObj *)sdbGetRow(tsVgroupSdb, &vgId);
}

/*
 * TODO: check if there is enough sids
 */
SVgObj *mgmtGetAvailableVgroup(SDbObj *pDb) {
  return pDb->pHead;
}

void mgmtProcessVgTimer(void *handle, void *tmrId) {
  SDbObj *pDb = (SDbObj *)handle;
  if (pDb == NULL) return;

  if (pDb->vgStatus > TSDB_VG_STATUS_IN_PROGRESS) {
    mTrace("db:%s, set vgroup status from %d to ready", pDb->name, pDb->vgStatus);
    pDb->vgStatus = TSDB_VG_STATUS_READY;
  }

  pDb->vgTimer = NULL;
}

void mgmtCreateVgroup(SQueuedMsg *pMsg) {
  SDbObj *pDb = pMsg->pDb;
  if (pDb == NULL) {
    mError("thandle:%p, failed to create vgroup, db not found", pMsg->thandle);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_INVALID_DB);
    return;
  }

  SVgObj *pVgroup = (SVgObj *)calloc(sizeof(SVgObj), 1);
  strcpy(pVgroup->dbName, pDb->name);
  pVgroup->numOfVnodes = pDb->cfg.replications;
  if (mgmtAllocVnodes(pVgroup) != 0) {
    mError("thandle:%p, db:%s no enough dnode to alloc %d vnodes", pMsg->thandle, pDb->name, pVgroup->numOfVnodes);
    free(pVgroup);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_NO_ENOUGH_DNODES);
    return;
  }

  pVgroup->createdTime = taosGetTimestampMs();
  pVgroup->tableList   = (STableInfo **) calloc(sizeof(STableInfo *), pDb->cfg.maxSessions);
  pVgroup->numOfTables = 0;
  pVgroup->idPool      = taosInitIdPool(pDb->cfg.maxSessions);

  mgmtAddVgroupIntoDb(pDb, pVgroup);
  mgmtSetDnodeVgid(pVgroup->vnodeGid, pVgroup->numOfVnodes, pVgroup->vgId);

  sdbInsertRow(tsVgroupSdb, pVgroup, 0);

  mPrint("thandle:%p, vgroup:%d is created in mnode, db:%s replica:%d", pMsg->thandle, pVgroup->vgId, pDb->name,
         pVgroup->numOfVnodes);
  for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
    mPrint("thandle:%p, vgroup:%d, dnode:%s vnode:%d", pMsg->thandle, pVgroup->vgId,
           taosIpStr(pVgroup->vnodeGid[i].ip), pVgroup->vnodeGid[i].vnode);
  }

  pMsg->ahandle = pVgroup;
  pMsg->expected = pVgroup->numOfVnodes;
  mgmtSendCreateVgroupMsg(pVgroup, pMsg);
}

int32_t mgmtDropVgroup(SDbObj *pDb, SVgObj *pVgroup) {
  STableInfo *pTable;

  if (pVgroup->numOfTables > 0) {
    for (int32_t i = 0; i < pDb->cfg.maxSessions; ++i) {
      if (pVgroup->tableList != NULL) {
        pTable = pVgroup->tableList[i];
        if (pTable) mgmtDropTable(pDb, pTable->tableId, 0);
      }
    }
  }

  mTrace("vgroup:%d, db:%s replica:%d is deleted", pVgroup->vgId, pDb->name, pVgroup->numOfVnodes);

  //mgmtSendDropVgroupMsg(pVgroup, NULL);

  sdbDeleteRow(tsVgroupSdb, pVgroup);

  return TSDB_CODE_SUCCESS;
}

void mgmtSetVgroupIdPool() {
  void *  pNode = NULL;
  SVgObj *pVgroup = NULL;
  SDbObj *pDb;

  while (1) {
    pNode = sdbFetchRow(tsVgroupSdb, pNode, (void **)&pVgroup);
    if (pVgroup == NULL || pVgroup->idPool == 0) break;

    taosIdPoolSetFreeList(pVgroup->idPool);
    pVgroup->numOfTables = taosIdPoolNumOfUsed(pVgroup->idPool);

    pDb = mgmtGetDb(pVgroup->dbName);
    pDb->numOfTables += pVgroup->numOfTables;
    if (pVgroup->numOfTables >= pDb->cfg.maxSessions - 1)
      mgmtAddVgroupIntoDbTail(pDb, pVgroup);
    else
      mgmtAddVgroupIntoDb(pDb, pVgroup);
  }
}

void mgmtCleanUpVgroups() {
  sdbCloseTable(tsVgroupSdb);
}

int32_t mgmtGetVgroupMeta(STableMeta *pMeta, SShowObj *pShow, void *pConn) {
  SDbObj *pDb = mgmtGetDb(pShow->db);
  if (pDb == NULL) {
    return TSDB_CODE_DB_NOT_SELECTED;
  }

  int32_t cols = 0;
  SSchema *pSchema = tsGetSchema(pMeta);

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

  pShow->bytes[cols] = 9;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "vgroup status");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  int32_t maxReplica = 0;
  SVgObj  *pVgroup   = NULL;
  STableInfo *pTable = NULL;
  if (pShow->payloadLen > 0 ) {
    pTable = mgmtGetTable(pShow->payload);
    if (NULL == pTable) {
      return TSDB_CODE_INVALID_TABLE_ID;
    }

    pVgroup = mgmtGetVgroup(pTable->vgId);
    if (NULL == pVgroup) return TSDB_CODE_INVALID_TABLE_ID;

    maxReplica = pVgroup->numOfVnodes > maxReplica ? pVgroup->numOfVnodes : maxReplica;
  } else {
    SVgObj *pVgroup = pDb->pHead;
    while (pVgroup != NULL) {
      maxReplica = pVgroup->numOfVnodes > maxReplica ? pVgroup->numOfVnodes : maxReplica;
      pVgroup = pVgroup->next;
    }
  }

  for (int32_t i = 0; i < maxReplica; ++i) {
    pShow->bytes[cols] = 16;
    pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
    strcpy(pSchema[cols].name, "ip");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;

    pShow->bytes[cols] = 2;
    pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
    strcpy(pSchema[cols].name, "vnode");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;

    pShow->bytes[cols] = 9;
    pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
    strcpy(pSchema[cols].name, "vnode status");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;

    pShow->bytes[cols] = 16;
    pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
    strcpy(pSchema[cols].name, "public ip");
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
    pShow->pNode = pDb->pHead;
  } else {
    pShow->numOfRows = 1;
    pShow->pNode = pVgroup;
  }

  return 0;
}

char *mgmtGetVnodeStatus(SVgObj *pVgroup, SVnodeGid *pVnode) {
  SDnodeObj *pDnode = mgmtGetDnode(pVnode->ip);
  if (pDnode == NULL) {
    mError("dnode:%s, vgroup:%d, vnode:%d dnode not exist", taosIpStr(pVnode->ip), pVgroup->vgId, pVnode->vnode);
    return "null";
  }

  if (pDnode->status == TSDB_DN_STATUS_OFFLINE) {
    return "offline";
  }

  SVnodeLoad *vload = pDnode->vload + pVnode->vnode;
  if (vload->vgId != pVgroup->vgId || vload->vnode != pVnode->vnode) {
    mError("dnode:%s, vgroup:%d, vnode:%d not same with dnode vgroup:%d vnode:%d",
           taosIpStr(pVnode->ip), pVgroup->vgId, pVnode->vnode, vload->vgId, vload->vnode);
    return "null";
  }

  return (char*)taosGetVnodeStatusStr(vload->status);
}

int32_t mgmtRetrieveVgroups(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t numOfRows = 0;
  SVgObj *pVgroup = NULL;
  int32_t maxReplica = 0;
  int32_t cols = 0;
  char    ipstr[20];
  char *  pWrite;

  SDbObj *pDb = mgmtGetDb(pShow->db);
  if (pDb == NULL) return 0;

  pVgroup = pDb->pHead;
  while (pVgroup != NULL) {
    maxReplica = pVgroup->numOfVnodes > maxReplica ? pVgroup->numOfVnodes : maxReplica;
    pVgroup    = pVgroup->next;
  }

  while (numOfRows < rows) {
    pVgroup = (SVgObj *) pShow->pNode;
    if (pVgroup == NULL) break;
    pShow->pNode = (void *) pVgroup->next;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *) pWrite = pVgroup->vgId;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *) pWrite = pVgroup->numOfTables;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, taosGetVgroupLbStatusStr(pVgroup->lbStatus));
    cols++;

    for (int32_t i = 0; i < maxReplica; ++i) {
      tinet_ntoa(ipstr, pVgroup->vnodeGid[i].ip);
      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      strcpy(pWrite, ipstr);
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int16_t *) pWrite = pVgroup->vnodeGid[i].vnode;
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      if (pVgroup->vnodeGid[i].ip != 0) {
        char *vnodeStatus = mgmtGetVnodeStatus(pVgroup, pVgroup->vnodeGid + i);
        strcpy(pWrite, vnodeStatus);
      } else {
        strcpy(pWrite, "null");
      }
      cols++;

      tinet_ntoa(ipstr, pVgroup->vnodeGid[i].publicIp);
      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      strcpy(pWrite, ipstr);
      cols++;
    }

    numOfRows++;
  }

  pShow->numOfReads += numOfRows;
  return numOfRows;
}

static void *mgmtVgroupActionInsert(void *row, char *str, int32_t size, int32_t *ssize) {
  return NULL;
}

static void *mgmtVgroupActionDelete(void *row, char *str, int32_t size, int32_t *ssize) {
  SVgObj *pVgroup = row;
  SDbObj *pDb = mgmtGetDb(pVgroup->dbName);

  if (pDb != NULL) {
    mgmtRemoveVgroupFromDb(pDb, pVgroup);
  }

  mgmtUnSetDnodeVgid(pVgroup->vnodeGid, pVgroup->numOfVnodes);
  tfree(pVgroup->tableList);

  return NULL;
}

static void *mgmtVgroupActionUpdate(void *row, char *str, int32_t size, int32_t *ssize) {
  mgmtVgroupActionReset(row, str, size, ssize);

  SVgObj  *pVgroup  = (SVgObj *) row;
  int32_t oldTables = taosIdPoolMaxSize(pVgroup->idPool);

  SDbObj *pDb = mgmtGetDb(pVgroup->dbName);
  if (pDb != NULL) {
    if (pDb->cfg.maxSessions != oldTables) {
      mPrint("vgroup:%d tables change from %d to %d", pVgroup->vgId, oldTables, pDb->cfg.maxSessions);
      taosUpdateIdPool(pVgroup->idPool, pDb->cfg.maxSessions);
      int32_t size = sizeof(STableInfo *) * pDb->cfg.maxSessions;
      pVgroup->tableList = (STableInfo **)realloc(pVgroup->tableList, size);
    }
  }

  mTrace("vgroup:%d update, numOfVnode:%d", pVgroup->vgId, pVgroup->numOfVnodes);

  return NULL;
}

static void *mgmtVgroupActionEncode(void *row, char *str, int32_t size, int32_t *ssize) {
  SVgObj *pVgroup = (SVgObj *) row;
  if (size < tsVgUpdateSize) {
    *ssize = -1;
  } else {
    memcpy(str, pVgroup, tsVgUpdateSize);
    *ssize = tsVgUpdateSize;
  }

  return NULL;
}

static void *mgmtVgroupActionDecode(void *row, char *str, int32_t size, int32_t *ssize) {
  SVgObj *pVgroup = (SVgObj *) malloc(sizeof(SVgObj));
  if (pVgroup == NULL) return NULL;
  memset(pVgroup, 0, sizeof(SVgObj));

  int32_t tsVgUpdateSize = pVgroup->updateEnd - (int8_t *) pVgroup;
  memcpy(pVgroup, str, tsVgUpdateSize);

  return (void *) pVgroup;
}

static void *mgmtVgroupActionReset(void *row, char *str, int32_t size, int32_t *ssize) {
  SVgObj *pVgroup = (SVgObj *) row;
  memcpy(pVgroup, str, tsVgUpdateSize);
  return NULL;
}

static void *mgmtVgroupActionDestroy(void *row, char *str, int32_t size, int32_t *ssize) {
  SVgObj *pVgroup = (SVgObj *) row;
  if (pVgroup->idPool) {
    taosIdPoolCleanUp(pVgroup->idPool);
    pVgroup->idPool = NULL;
  }
  if (pVgroup->tableList) tfree(pVgroup->tableList);
  tfree(row);
  return NULL;
}

void mgmtUpdateVgroup(SVgObj *pVgroup) {
  sdbUpdateRow(tsVgroupSdb, pVgroup, tsVgUpdateSize, 0);
}

void mgmtAddTableIntoVgroup(SVgObj *pVgroup, STableInfo *pTable) {
  pVgroup->numOfTables++;
  if (pTable->sid >= 0)
    pVgroup->tableList[pTable->sid] = pTable;
}

void mgmtRemoveTableFromVgroup(SVgObj *pVgroup, STableInfo *pTable) {
  pVgroup->numOfTables--;
  if (pTable->sid >= 0)
    pVgroup->tableList[pTable->sid] = NULL;
  taosFreeId(pVgroup->idPool, pTable->sid);
}

SMDCreateVnodeMsg *mgmtBuildCreateVnodeMsg(SVgObj *pVgroup, int32_t vnode) {
  SDbObj *pDb = mgmtGetDb(pVgroup->dbName);
  if (pDb == NULL) return NULL;

  SMDCreateVnodeMsg *pVnode = rpcMallocCont(sizeof(SMDCreateVnodeMsg));
  if (pVnode == NULL) return NULL;

  pVnode->vnode = htonl(vnode);
  pVnode->cfg   = pDb->cfg;

  SVnodeCfg *pCfg = &pVnode->cfg;
  pCfg->vgId                         = htonl(pVgroup->vgId);
  pCfg->maxSessions                  = htonl(pCfg->maxSessions);
  pCfg->cacheBlockSize               = htonl(pCfg->cacheBlockSize);
  pCfg->cacheNumOfBlocks.totalBlocks = htonl(pCfg->cacheNumOfBlocks.totalBlocks);
  pCfg->daysPerFile                  = htonl(pCfg->daysPerFile);
  pCfg->daysToKeep1                  = htonl(pCfg->daysToKeep1);
  pCfg->daysToKeep2                  = htonl(pCfg->daysToKeep2);
  pCfg->daysToKeep                   = htonl(pCfg->daysToKeep);
  pCfg->commitTime                   = htonl(pCfg->commitTime);
  pCfg->blocksPerTable               = htons(pCfg->blocksPerTable);
  pCfg->replications                 = (char) pVgroup->numOfVnodes;
  pCfg->rowsInFileBlock              = htonl(pCfg->rowsInFileBlock);

  SVnodeDesc *vpeerDesc = pVnode->vpeerDesc;
  for (int32_t j = 0; j < pVgroup->numOfVnodes; ++j) {
    vpeerDesc[j].vgId  = htonl(pVgroup->vgId);
    vpeerDesc[j].ip    = htonl(pVgroup->vnodeGid[j].ip);
    vpeerDesc[j].vnode = htonl(pVgroup->vnodeGid[j].vnode);
  }

  return pVnode;
}

SVgObj *mgmtGetVgroupByVnode(uint32_t dnode, int32_t vnode) {
  if (vnode < 0 || vnode >= TSDB_MAX_VNODES) {
    return NULL;
  }

  SDnodeObj *pDnode = mgmtGetDnode(dnode);
  if (pDnode == NULL) {
    return NULL;
  }

  int32_t vgId = pDnode->vload[vnode].vgId;
  return mgmtGetVgroup(vgId);
}

SRpcIpSet mgmtGetIpSetFromVgroup(SVgObj *pVgroup) {
  SRpcIpSet ipSet = {
    .numOfIps = pVgroup->numOfVnodes,
    .inUse = 0,
    .port = tsDnodeMnodePort
  };
  for (int i = 0; i < pVgroup->numOfVnodes; ++i) {
    ipSet.ip[i] = pVgroup->vnodeGid[i].ip;
  }
  return ipSet;
}

SRpcIpSet mgmtGetIpSetFromIp(uint32_t ip) {
  SRpcIpSet ipSet = {
    .ip[0]    = ip,
    .numOfIps = 1,
    .inUse    = 0,
    .port     = tsDnodeMnodePort
  };
  return ipSet;
}

void mgmtSendCreateVnodeMsg(SVgObj *pVgroup, int32_t vnode, SRpcIpSet *ipSet, void *ahandle) {
  mTrace("vgroup:%d, send create vnode:%d msg, ahandle:%p", pVgroup->vgId, vnode, ahandle);
  SMDCreateVnodeMsg *pCreate = mgmtBuildCreateVnodeMsg(pVgroup, vnode);
  SRpcMsg rpcMsg = {
    .handle  = ahandle,
    .pCont   = pCreate,
    .contLen = pCreate ? sizeof(SMDCreateVnodeMsg) : 0,
    .code    = 0,
    .msgType = TSDB_MSG_TYPE_MD_CREATE_VNODE
  };
  mgmtSendMsgToDnode(ipSet, &rpcMsg);
}

void mgmtSendCreateVgroupMsg(SVgObj *pVgroup, void *ahandle) {
  mTrace("send create vgroup:%d msg, ahandle:%p", pVgroup->vgId, ahandle);
  for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
    SRpcIpSet ipSet = mgmtGetIpSetFromIp(pVgroup->vnodeGid[i].ip);
    mgmtSendCreateVnodeMsg(pVgroup, pVgroup->vnodeGid[i].vnode, &ipSet, ahandle);
  }
}

static void mgmtProcessCreateVnodeRsp(SRpcMsg *rpcMsg) {
  if (rpcMsg->handle == NULL) return;

  SQueuedMsg *queueMsg = rpcMsg->handle;
  queueMsg->received++;
  if (rpcMsg->code == TSDB_CODE_SUCCESS) {
    queueMsg->code = rpcMsg->code;
    queueMsg->successed++;
  }

  SVgObj *pVgroup = queueMsg->ahandle;
  mTrace("thandle:%p, vgroup:%d create vnode rsp received, ahandle:%p code:%d received:%d successed:%d expected:%d",
         queueMsg->thandle, pVgroup->vgId, rpcMsg->handle, rpcMsg->code, queueMsg->received, queueMsg->successed,
         queueMsg->expected);

  if (queueMsg->received != queueMsg->expected) return;

  if (queueMsg->received == queueMsg->successed) {
    SQueuedMsg *newMsg = calloc(1, sizeof(SQueuedMsg));
    newMsg->msgType = queueMsg->msgType;
    newMsg->thandle = queueMsg->thandle;
    newMsg->pDb     = queueMsg->pDb;
    newMsg->pUser   = queueMsg->pUser;
    newMsg->contLen = queueMsg->contLen;
    newMsg->pCont   = rpcMallocCont(newMsg->contLen);
    memcpy(newMsg->pCont, queueMsg->pCont, newMsg->contLen);
    mgmtAddToShellQueue(newMsg);
  } else {
    sdbDeleteRow(tsVgroupSdb, pVgroup);
    mgmtSendSimpleResp(queueMsg->thandle, rpcMsg->code);
  }

  free(queueMsg);
}