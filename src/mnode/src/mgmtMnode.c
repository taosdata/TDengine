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
#include "trpc.h"
#include "tsync.h"
#include "tbalance.h"
#include "tutil.h"
#include "ttime.h"
#include "tsocket.h"
#include "tdataformat.h"
#include "mgmtDef.h"
#include "mgmtInt.h"
#include "mgmtMnode.h"
#include "mgmtDnode.h"
#include "mgmtSdb.h"
#include "mgmtShell.h"
#include "mgmtUser.h"

static void *  tsMnodeSdb = NULL;
static int32_t tsMnodeUpdateSize = 0;
static int32_t mgmtGetMnodeMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t mgmtRetrieveMnodes(SShowObj *pShow, char *data, int32_t rows, void *pConn);

static SRpcIpSet tsMnodeRpcIpSet;
static SDMMnodeInfos tsMnodeInfos;

#if defined(LINUX)
  static pthread_rwlock_t        tsMnodeLock;
  #define mgmtMnodeWrLock()      pthread_rwlock_wrlock(&tsMnodeLock)
  #define mgmtMnodeRdLock()      pthread_rwlock_rdlock(&tsMnodeLock)
  #define mgmtMnodeUnLock()      pthread_rwlock_unlock(&tsMnodeLock)
  #define mgmtMnodeInitLock()    pthread_rwlock_init(&tsMnodeLock, NULL)
  #define mgmtMnodeDestroyLock() pthread_rwlock_destroy(&tsMnodeLock)
#else
  static pthread_mutex_t         tsMnodeLock;
  #define mgmtMnodeWrLock()      pthread_mutex_lock(&tsMnodeLock)
  #define mgmtMnodeRdLock()      pthread_mutex_lock(&tsMnodeLock)
  #define mgmtMnodeUnLock()      pthread_mutex_unlock(&tsMnodeLock)
  #define mgmtMnodeInitLock()    pthread_mutex_init(&tsMnodeLock, NULL)
  #define mgmtMnodeDestroyLock() pthread_mutex_destroy(&tsMnodeLock)
#endif

static int32_t mgmtMnodeActionDestroy(SSdbOper *pOper) {
  tfree(pOper->pObj);
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtMnodeActionInsert(SSdbOper *pOper) {
  SMnodeObj *pMnode = pOper->pObj;
  SDnodeObj *pDnode = mgmtGetDnode(pMnode->mnodeId);
  if (pDnode == NULL) return TSDB_CODE_DNODE_NOT_EXIST;

  pDnode->isMgmt = true;
  mgmtDecDnodeRef(pDnode);
  
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtMnodeActionDelete(SSdbOper *pOper) {
  SMnodeObj *pMnode = pOper->pObj;

  SDnodeObj *pDnode = mgmtGetDnode(pMnode->mnodeId);
  if (pDnode == NULL) return TSDB_CODE_DNODE_NOT_EXIST;
  pDnode->isMgmt = false;
  mgmtDecDnodeRef(pDnode);

  mTrace("mnode:%d, is dropped from sdb", pMnode->mnodeId);
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtMnodeActionUpdate(SSdbOper *pOper) {
  SMnodeObj *pMnode = pOper->pObj;
  SMnodeObj *pSaved = mgmtGetMnode(pMnode->mnodeId);
  if (pMnode != pSaved) {
    memcpy(pSaved, pMnode, pOper->rowSize);
    free(pMnode);
  }
  mgmtDecMnodeRef(pSaved);
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtMnodeActionEncode(SSdbOper *pOper) {
  SMnodeObj *pMnode = pOper->pObj;
  memcpy(pOper->rowData, pMnode, tsMnodeUpdateSize);
  pOper->rowSize = tsMnodeUpdateSize;
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtMnodeActionDecode(SSdbOper *pOper) {
  SMnodeObj *pMnode = calloc(1, sizeof(SMnodeObj));
  if (pMnode == NULL) return TSDB_CODE_SERV_OUT_OF_MEMORY;

  memcpy(pMnode, pOper->rowData, tsMnodeUpdateSize);
  pOper->pObj = pMnode;
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtMnodeActionRestored() {
  if (mgmtGetMnodesNum() == 1) {
    SMnodeObj *pMnode = NULL;
    void *pIter = mgmtGetNextMnode(NULL, &pMnode);
    if (pMnode != NULL) {
      pMnode->role = TAOS_SYNC_ROLE_MASTER;
      mgmtDecMnodeRef(pMnode);
    }
    sdbFreeIter(pIter);
  }

  mgmtUpdateMnodeIpSet();

  return TSDB_CODE_SUCCESS;
}

int32_t mgmtInitMnodes() {
  mgmtMnodeInitLock();

  SMnodeObj tObj;
  tsMnodeUpdateSize = (int8_t *)tObj.updateEnd - (int8_t *)&tObj;

  SSdbTableDesc tableDesc = {
    .tableId      = SDB_TABLE_MNODE,
    .tableName    = "mnodes",
    .hashSessions = TSDB_DEFAULT_MNODES_HASH_SIZE,
    .maxRowSize   = tsMnodeUpdateSize,
    .refCountPos  = (int8_t *)(&tObj.refCount) - (int8_t *)&tObj,
    .keyType      = SDB_KEY_INT,
    .insertFp     = mgmtMnodeActionInsert,
    .deleteFp     = mgmtMnodeActionDelete,
    .updateFp     = mgmtMnodeActionUpdate,
    .encodeFp     = mgmtMnodeActionEncode,
    .decodeFp     = mgmtMnodeActionDecode,
    .destroyFp    = mgmtMnodeActionDestroy,
    .restoredFp   = mgmtMnodeActionRestored
  };

  tsMnodeSdb = sdbOpenTable(&tableDesc);
  if (tsMnodeSdb == NULL) {
    mError("failed to init mnodes data");
    return -1;
  }

  mgmtAddShellShowMetaHandle(TSDB_MGMT_TABLE_MNODE, mgmtGetMnodeMeta);
  mgmtAddShellShowRetrieveHandle(TSDB_MGMT_TABLE_MNODE, mgmtRetrieveMnodes);

  mTrace("table:mnodes table is created");
  return TSDB_CODE_SUCCESS;
}

void mgmtCleanupMnodes() {
  sdbCloseTable(tsMnodeSdb);
  mgmtMnodeDestroyLock();
}

int32_t mgmtGetMnodesNum() { 
  return sdbGetNumOfRows(tsMnodeSdb); 
}

void *mgmtGetMnode(int32_t mnodeId) {
  return sdbGetRow(tsMnodeSdb, &mnodeId);
}

void mgmtIncMnodeRef(SMnodeObj *pMnode) {
  sdbIncRef(tsMnodeSdb, pMnode);
}

void mgmtDecMnodeRef(SMnodeObj *pMnode) {
  sdbDecRef(tsMnodeSdb, pMnode);
}

void *mgmtGetNextMnode(void *pIter, SMnodeObj **pMnode) { 
  return sdbFetchRow(tsMnodeSdb, pIter, (void **)pMnode); 
}

char *mgmtGetMnodeRoleStr(int32_t role) {
  switch (role) {
    case TAOS_SYNC_ROLE_OFFLINE:
      return "offline";
    case TAOS_SYNC_ROLE_UNSYNCED:
      return "unsynced";
    case TAOS_SYNC_ROLE_SLAVE:
      return "slave";
    case TAOS_SYNC_ROLE_MASTER:
      return "master";
    default:
      return "undefined";
  }
}

void mgmtUpdateMnodeIpSet() {
  SRpcIpSet *ipSet = &tsMnodeRpcIpSet;
  SDMMnodeInfos *mnodes = &tsMnodeInfos;

  mPrint("update mnodes ipset, numOfIps:%d ", mgmtGetMnodesNum());

  mgmtMnodeWrLock();

  memset(ipSet, 0, sizeof(tsMnodeRpcIpSet));
  memset(mnodes, 0, sizeof(SDMMnodeInfos));

  int32_t index = 0;
  void *  pIter = NULL;
  while (1) {
    SMnodeObj *pMnode = NULL;
    pIter = mgmtGetNextMnode(pIter, &pMnode);
    if (pMnode == NULL) break;

    SDnodeObj *pDnode = mgmtGetDnode(pMnode->mnodeId);
    if (pDnode != NULL) {
      strcpy(ipSet->fqdn[ipSet->numOfIps], pDnode->dnodeFqdn);
      ipSet->port[ipSet->numOfIps] = htons(pDnode->dnodePort);

      mnodes->nodeInfos[index].nodeId = htonl(pMnode->mnodeId);
      strcpy(mnodes->nodeInfos[index].nodeEp, pDnode->dnodeEp);

      if (pMnode->role == TAOS_SYNC_ROLE_MASTER) {
        ipSet->inUse = ipSet->numOfIps;
        mnodes->inUse = index;
      }

      mPrint("mnode:%d, ep:%s %s", index, pDnode->dnodeEp,
             pMnode->role == TAOS_SYNC_ROLE_MASTER ? "master" : "");

      ipSet->numOfIps++;
      index++;
    }

    mgmtDecDnodeRef(pDnode);
    mgmtDecMnodeRef(pMnode);
  }

  mnodes->nodeNum = index;

  sdbFreeIter(pIter);

  mgmtMnodeUnLock();
}

void mgmtGetMnodeIpSet(SRpcIpSet *ipSet) {
  mgmtMnodeRdLock();
  *ipSet = tsMnodeRpcIpSet;
  mgmtMnodeUnLock();
}

void mgmtGetMnodeInfos(void *mnodeInfos) {
  mgmtMnodeRdLock();
  *(SDMMnodeInfos *)mnodeInfos = tsMnodeInfos;
  mgmtMnodeUnLock();
}

int32_t mgmtAddMnode(int32_t dnodeId) {
  SMnodeObj *pMnode = calloc(1, sizeof(SMnodeObj));
  pMnode->mnodeId = dnodeId;
  pMnode->createdTime = taosGetTimestampMs();

  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsMnodeSdb,
    .pObj = pMnode,
  };

  int32_t code = sdbInsertRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    tfree(pMnode);
    code = TSDB_CODE_SDB_ERROR;
  }

  mgmtUpdateMnodeIpSet();

  return code;
}

void mgmtDropMnodeLocal(int32_t dnodeId) {
  SMnodeObj *pMnode = mgmtGetMnode(dnodeId);
  if (pMnode != NULL) {
    SSdbOper oper = {.type = SDB_OPER_LOCAL, .table = tsMnodeSdb, .pObj = pMnode};
    sdbDeleteRow(&oper);
    mgmtDecMnodeRef(pMnode);
  }

  mgmtUpdateMnodeIpSet();
}

int32_t mgmtDropMnode(int32_t dnodeId) {
  SMnodeObj *pMnode = mgmtGetMnode(dnodeId);
  if (pMnode == NULL) {
    return TSDB_CODE_DNODE_NOT_EXIST;
  }
  
  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsMnodeSdb,
    .pObj = pMnode
  };

  int32_t code = sdbDeleteRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_SDB_ERROR;
  }

  sdbDecRef(tsMnodeSdb, pMnode);

  mgmtUpdateMnodeIpSet();

  return code;
}

static int32_t mgmtGetMnodeMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  sdbUpdateMnodeRoles();
  SUserObj *pUser = mgmtGetUserFromConn(pConn);
  if (pUser == NULL) return 0;

  if (strcmp(pUser->pAcct->user, "root") != 0)  {
    mgmtDecUserRef(pUser);
    return TSDB_CODE_NO_RIGHTS;
  }

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "id");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 40 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "end_point");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 12 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "role");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;
  
  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create_time");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = mgmtGetMnodesNum();
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->pIter = NULL;
  mgmtDecUserRef(pUser);

  return 0;
}

static int32_t mgmtRetrieveMnodes(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t    numOfRows = 0;
  int32_t    cols      = 0;
  SMnodeObj *pMnode   = NULL;
  char      *pWrite;

  while (numOfRows < rows) {
    pShow->pIter = mgmtGetNextMnode(pShow->pIter, &pMnode);
    if (pMnode == NULL) break;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pMnode->mnodeId;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    
    SDnodeObj *pDnode = mgmtGetDnode(pMnode->mnodeId);
    if (pDnode != NULL) {
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pDnode->dnodeEp, pShow->bytes[cols] - VARSTR_HEADER_SIZE);
    } else {
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, "invalid ep", pShow->bytes[cols] - VARSTR_HEADER_SIZE);
    }
    mgmtDecDnodeRef(pDnode);

    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    char* roles = mgmtGetMnodeRoleStr(pMnode->role);
    STR_TO_VARSTR(pWrite, roles);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pMnode->createdTime;
    cols++;
    
    numOfRows++;

    mgmtDecMnodeRef(pMnode);
  }

  pShow->numOfReads += numOfRows;

  return numOfRows;
}
