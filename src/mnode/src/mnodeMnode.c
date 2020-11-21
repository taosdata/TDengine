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
#include "tglobal.h"
#include "trpc.h"
#include "tsync.h"
#include "tbalance.h"
#include "tutil.h"
#include "tsocket.h"
#include "tdataformat.h"
#include "dnode.h"
#include "mnode.h"
#include "mnodeDef.h"
#include "mnodeInt.h"
#include "mnodeMnode.h"
#include "mnodeDnode.h"
#include "mnodeSdb.h"
#include "mnodeShow.h"
#include "mnodeUser.h"
#include "mnodeVgroup.h"

static void *        tsMnodeSdb = NULL;
static int32_t       tsMnodeUpdateSize = 0;
static SRpcEpSet     tsMnodeEpSetForShell;
static SRpcEpSet     tsMnodeEpSetForPeer;
static SMnodeInfos   tsMnodeInfos;
static int32_t mnodeGetMnodeMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t mnodeRetrieveMnodes(SShowObj *pShow, char *data, int32_t rows, void *pConn);

#if defined(LINUX)
  static pthread_rwlock_t         tsMnodeLock;
  #define mnodeMnodeWrLock()      pthread_rwlock_wrlock(&tsMnodeLock)
  #define mnodeMnodeRdLock()      pthread_rwlock_rdlock(&tsMnodeLock)
  #define mnodeMnodeUnLock()      pthread_rwlock_unlock(&tsMnodeLock)
  #define mnodeMnodeInitLock()    pthread_rwlock_init(&tsMnodeLock, NULL)
  #define mnodeMnodeDestroyLock() pthread_rwlock_destroy(&tsMnodeLock)
#else
  static pthread_mutex_t          tsMnodeLock;
  #define mnodeMnodeWrLock()      pthread_mutex_lock(&tsMnodeLock)
  #define mnodeMnodeRdLock()      pthread_mutex_lock(&tsMnodeLock)
  #define mnodeMnodeUnLock()      pthread_mutex_unlock(&tsMnodeLock)
  #define mnodeMnodeInitLock()    pthread_mutex_init(&tsMnodeLock, NULL)
  #define mnodeMnodeDestroyLock() pthread_mutex_destroy(&tsMnodeLock)
#endif

static int32_t mnodeMnodeActionDestroy(SSdbRow *pRow) {
  tfree(pRow->pObj);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeMnodeActionInsert(SSdbRow *pRow) {
  SMnodeObj *pMnode = pRow->pObj;
  SDnodeObj *pDnode = mnodeGetDnode(pMnode->mnodeId);
  if (pDnode == NULL) return TSDB_CODE_MND_DNODE_NOT_EXIST;

  pDnode->isMgmt = true;
  mnodeDecDnodeRef(pDnode);

  mInfo("mnode:%d, fqdn:%s ep:%s port:%u, do insert action", pMnode->mnodeId, pDnode->dnodeFqdn, pDnode->dnodeEp,
        pDnode->dnodePort);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeMnodeActionDelete(SSdbRow *pRow) {
  SMnodeObj *pMnode = pRow->pObj;

  SDnodeObj *pDnode = mnodeGetDnode(pMnode->mnodeId);
  if (pDnode == NULL) return TSDB_CODE_MND_DNODE_NOT_EXIST;
  pDnode->isMgmt = false;
  mnodeDecDnodeRef(pDnode);

  mDebug("mnode:%d, is dropped from sdb", pMnode->mnodeId);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeMnodeActionUpdate(SSdbRow *pRow) {
  SMnodeObj *pMnode = pRow->pObj;
  SMnodeObj *pSaved = mnodeGetMnode(pMnode->mnodeId);
  if (pMnode != pSaved) {
    memcpy(pSaved, pMnode, pRow->rowSize);
    free(pMnode);
  }
  mnodeDecMnodeRef(pSaved);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeMnodeActionEncode(SSdbRow *pRow) {
  SMnodeObj *pMnode = pRow->pObj;
  memcpy(pRow->rowData, pMnode, tsMnodeUpdateSize);
  pRow->rowSize = tsMnodeUpdateSize;
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeMnodeActionDecode(SSdbRow *pRow) {
  SMnodeObj *pMnode = calloc(1, sizeof(SMnodeObj));
  if (pMnode == NULL) return TSDB_CODE_MND_OUT_OF_MEMORY;

  memcpy(pMnode, pRow->rowData, tsMnodeUpdateSize);
  pRow->pObj = pMnode;
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeMnodeActionRestored() {
  if (mnodeGetMnodesNum() == 1) {
    SMnodeObj *pMnode = NULL;
    void *pIter = mnodeGetNextMnode(NULL, &pMnode);
    if (pMnode != NULL) {
      pMnode->role = TAOS_SYNC_ROLE_MASTER;
      mnodeDecMnodeRef(pMnode);
    }
    sdbFreeIter(pIter);
  }

  mnodeUpdateMnodeEpSet();

  return TSDB_CODE_SUCCESS;
}

int32_t mnodeInitMnodes() {
  mnodeMnodeInitLock();

  SMnodeObj tObj;
  tsMnodeUpdateSize = (int8_t *)tObj.updateEnd - (int8_t *)&tObj;

  SSdbTableDesc desc = {
    .id           = SDB_TABLE_MNODE,
    .name         = "mnodes",
    .hashSessions = TSDB_DEFAULT_MNODES_HASH_SIZE,
    .maxRowSize   = tsMnodeUpdateSize,
    .refCountPos  = (int8_t *)(&tObj.refCount) - (int8_t *)&tObj,
    .keyType      = SDB_KEY_INT,
    .fpInsert     = mnodeMnodeActionInsert,
    .fpDelete     = mnodeMnodeActionDelete,
    .fpUpdate     = mnodeMnodeActionUpdate,
    .fpEncode     = mnodeMnodeActionEncode,
    .fpDecode     = mnodeMnodeActionDecode,
    .fpDestroy    = mnodeMnodeActionDestroy,
    .fpRestored   = mnodeMnodeActionRestored
  };

  tsMnodeSdb = sdbOpenTable(&desc);
  if (tsMnodeSdb == NULL) {
    mError("failed to init mnodes data");
    return -1;
  }

  mnodeAddShowMetaHandle(TSDB_MGMT_TABLE_MNODE, mnodeGetMnodeMeta);
  mnodeAddShowRetrieveHandle(TSDB_MGMT_TABLE_MNODE, mnodeRetrieveMnodes);

  mDebug("table:mnodes table is created");
  return TSDB_CODE_SUCCESS;
}

void mnodeCleanupMnodes() {
  sdbCloseTable(tsMnodeSdb);
  tsMnodeSdb = NULL;
  mnodeMnodeDestroyLock();
}

int32_t mnodeGetMnodesNum() { 
  return sdbGetNumOfRows(tsMnodeSdb); 
}

void *mnodeGetMnode(int32_t mnodeId) {
  return sdbGetRow(tsMnodeSdb, &mnodeId);
}

void mnodeIncMnodeRef(SMnodeObj *pMnode) {
  sdbIncRef(tsMnodeSdb, pMnode);
}

void mnodeDecMnodeRef(SMnodeObj *pMnode) {
  sdbDecRef(tsMnodeSdb, pMnode);
}

void *mnodeGetNextMnode(void *pIter, SMnodeObj **pMnode) { 
  return sdbFetchRow(tsMnodeSdb, pIter, (void **)pMnode); 
}

void mnodeUpdateMnodeEpSet() {
  mInfo("update mnodes epSet, numOfEps:%d ", mnodeGetMnodesNum());

  mnodeMnodeWrLock();

  memset(&tsMnodeEpSetForShell, 0, sizeof(SRpcEpSet));
  memset(&tsMnodeEpSetForPeer, 0, sizeof(SRpcEpSet));
  memset(&tsMnodeInfos, 0, sizeof(SMnodeInfos));

  int32_t index = 0;
  void *  pIter = NULL;
  while (1) {
    SMnodeObj *pMnode = NULL;
    pIter = mnodeGetNextMnode(pIter, &pMnode);
    if (pMnode == NULL) break;

    SDnodeObj *pDnode = mnodeGetDnode(pMnode->mnodeId);
    if (pDnode != NULL) {
      strcpy(tsMnodeEpSetForShell.fqdn[index], pDnode->dnodeFqdn);
      tsMnodeEpSetForShell.port[index] = htons(pDnode->dnodePort);
      mDebug("mnode:%d, for shell fqdn:%s %d", pDnode->dnodeId, tsMnodeEpSetForShell.fqdn[index], htons(tsMnodeEpSetForShell.port[index]));      

      strcpy(tsMnodeEpSetForPeer.fqdn[index], pDnode->dnodeFqdn);
      tsMnodeEpSetForPeer.port[index] = htons(pDnode->dnodePort + TSDB_PORT_DNODEDNODE);
      mDebug("mnode:%d, for peer fqdn:%s %d", pDnode->dnodeId, tsMnodeEpSetForPeer.fqdn[index], htons(tsMnodeEpSetForPeer.port[index]));

      tsMnodeInfos.mnodeInfos[index].mnodeId = htonl(pMnode->mnodeId);
      strcpy(tsMnodeInfos.mnodeInfos[index].mnodeEp, pDnode->dnodeEp);

      if (pMnode->role == TAOS_SYNC_ROLE_MASTER) {
        tsMnodeEpSetForShell.inUse = index;
        tsMnodeEpSetForPeer.inUse = index;
        tsMnodeInfos.inUse = index;
      }

      mInfo("mnode:%d, ep:%s %s", pDnode->dnodeId, pDnode->dnodeEp, pMnode->role == TAOS_SYNC_ROLE_MASTER ? "master" : "");
      index++;
    }

    mnodeDecDnodeRef(pDnode);
    mnodeDecMnodeRef(pMnode);
  }

  tsMnodeInfos.mnodeNum = index;
  tsMnodeEpSetForShell.numOfEps = index;
  tsMnodeEpSetForPeer.numOfEps = index;

  sdbFreeIter(pIter);

  mnodeMnodeUnLock();
}

void mnodeGetMnodeEpSetForPeer(SRpcEpSet *epSet) {
  mnodeMnodeRdLock();
  *epSet = tsMnodeEpSetForPeer;
  mnodeMnodeUnLock();
}

void mnodeGetMnodeEpSetForShell(SRpcEpSet *epSet) {
  mnodeMnodeRdLock();
  *epSet = tsMnodeEpSetForShell;
  mnodeMnodeUnLock();
}

char* mnodeGetMnodeMasterEp() {
  return tsMnodeInfos.mnodeInfos[tsMnodeInfos.inUse].mnodeEp;
}

void mnodeGetMnodeInfos(void *mnodeInfos) {
  mnodeMnodeRdLock();
  *(SMnodeInfos *)mnodeInfos = tsMnodeInfos;
  mnodeMnodeUnLock();
}

static int32_t mnodeSendCreateMnodeMsg(int32_t dnodeId, char *dnodeEp) {
  mDebug("dnode:%d, send create mnode msg to dnode %s", dnodeId, dnodeEp);

  SCreateMnodeMsg *pCreate = rpcMallocCont(sizeof(SCreateMnodeMsg));
  if (pCreate == NULL) {
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  } else {
    pCreate->dnodeId = htonl(dnodeId);
    tstrncpy(pCreate->dnodeEp, dnodeEp, sizeof(pCreate->dnodeEp));
    pCreate->mnodes = tsMnodeInfos;
    bool found = false;
    for (int i = 0; i < pCreate->mnodes.mnodeNum; ++i) {
      if (pCreate->mnodes.mnodeInfos[i].mnodeId == htonl(dnodeId)) {
        found = true;
      }
    }
    if (!found) {
      pCreate->mnodes.mnodeInfos[pCreate->mnodes.mnodeNum].mnodeId = htonl(dnodeId);
      tstrncpy(pCreate->mnodes.mnodeInfos[pCreate->mnodes.mnodeNum].mnodeEp, dnodeEp, sizeof(pCreate->dnodeEp));
      pCreate->mnodes.mnodeNum++;
    }
  }

  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = pCreate;
  rpcMsg.contLen = sizeof(SCreateMnodeMsg);
  rpcMsg.msgType = TSDB_MSG_TYPE_MD_CREATE_MNODE;

  SRpcMsg   rpcRsp = {0};
  SRpcEpSet epSet = mnodeGetEpSetFromIp(pCreate->dnodeEp);
  dnodeSendMsgToDnodeRecv(&rpcMsg, &rpcRsp, &epSet);

  if (rpcRsp.code != TSDB_CODE_SUCCESS) {
    mError("dnode:%d, failed to send create mnode msg, ep:%s reason:%s", dnodeId, dnodeEp, tstrerror(rpcRsp.code));
  } else {
    mDebug("dnode:%d, create mnode msg is disposed, mnode is created in dnode", dnodeId);
  }

  rpcFreeCont(rpcRsp.pCont);
  return rpcRsp.code;
}

static int32_t mnodeCreateMnodeCb(SMnodeMsg *pMsg, int32_t code) {
  if (code != TSDB_CODE_SUCCESS) {
    mError("failed to create mnode, reason:%s", tstrerror(code));
  } else {
    mDebug("mnode is created successfully");
    mnodeUpdateMnodeEpSet();
    sdbUpdateAsync();
  }

  return code;
}

void mnodeCreateMnode(int32_t dnodeId, char *dnodeEp, bool needConfirm) {
  SMnodeObj *pMnode = calloc(1, sizeof(SMnodeObj));
  pMnode->mnodeId = dnodeId;
  pMnode->createdTime = taosGetTimestampMs();

  SSdbRow row = {
    .type    = SDB_OPER_GLOBAL,
    .pTable  = tsMnodeSdb,
    .pObj    = pMnode,
    .fpRsp   = mnodeCreateMnodeCb
  };

  int32_t code = TSDB_CODE_SUCCESS;
  if (needConfirm) {
    code = mnodeSendCreateMnodeMsg(dnodeId, dnodeEp);
  }

  if (code != TSDB_CODE_SUCCESS) {
    tfree(pMnode);
    return;
  }

  code = sdbInsertRow(&row);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("dnode:%d, failed to create mnode, ep:%s reason:%s", dnodeId, dnodeEp, tstrerror(code));
    tfree(pMnode);
  }
}

void mnodeDropMnodeLocal(int32_t dnodeId) {
  SMnodeObj *pMnode = mnodeGetMnode(dnodeId);
  if (pMnode != NULL) {
    SSdbRow row = {.type = SDB_OPER_LOCAL, .pTable = tsMnodeSdb, .pObj = pMnode};
    sdbDeleteRow(&row);
    mnodeDecMnodeRef(pMnode);
  }

  mnodeUpdateMnodeEpSet();
  sdbUpdateAsync();
}

int32_t mnodeDropMnode(int32_t dnodeId) {
  SMnodeObj *pMnode = mnodeGetMnode(dnodeId);
  if (pMnode == NULL) {
    return TSDB_CODE_MND_DNODE_NOT_EXIST;
  }
  
  SSdbRow row = {
    .type   = SDB_OPER_GLOBAL,
    .pTable = tsMnodeSdb,
    .pObj   = pMnode
  };

  int32_t code = sdbDeleteRow(&row);

  sdbDecRef(tsMnodeSdb, pMnode);

  mnodeUpdateMnodeEpSet();
  sdbUpdateAsync();

  return code;
}

static int32_t mnodeGetMnodeMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  sdbUpdateMnodeRoles();
  SUserObj *pUser = mnodeGetUserFromConn(pConn);
  if (pUser == NULL) return 0;

  if (strcmp(pUser->pAcct->user, TSDB_DEFAULT_USER) != 0)  {
    mnodeDecUserRef(pUser);
    return TSDB_CODE_MND_NO_RIGHTS;
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

  pShow->numOfRows = mnodeGetMnodesNum();
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->pIter = NULL;
  mnodeDecUserRef(pUser);

  return 0;
}

static int32_t mnodeRetrieveMnodes(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t    numOfRows = 0;
  int32_t    cols      = 0;
  SMnodeObj *pMnode   = NULL;
  char      *pWrite;

  while (numOfRows < rows) {
    pShow->pIter = mnodeGetNextMnode(pShow->pIter, &pMnode);
    if (pMnode == NULL) break;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pMnode->mnodeId;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    
    SDnodeObj *pDnode = mnodeGetDnode(pMnode->mnodeId);
    if (pDnode != NULL) {
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pDnode->dnodeEp, pShow->bytes[cols]);
    } else {
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, "invalid ep", pShow->bytes[cols]);
    }
    mnodeDecDnodeRef(pDnode);

    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    char* roles = syncRole[pMnode->role];
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, roles, pShow->bytes[cols]);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pMnode->createdTime;
    cols++;
    
    numOfRows++;

    mnodeDecMnodeRef(pMnode);
  }

  mnodeVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  pShow->numOfReads += numOfRows;

  return numOfRows;
}
