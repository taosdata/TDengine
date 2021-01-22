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
#include "tbn.h"
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

int64_t          tsMnodeRid = -1;
static void *    tsMnodeSdb = NULL;
static int32_t   tsMnodeUpdateSize = 0;
static SRpcEpSet tsMEpForShell;
static SRpcEpSet tsMEpForPeer;
static SMInfos   tsMInfos;
static int32_t   mnodeGetMnodeMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t   mnodeRetrieveMnodes(SShowObj *pShow, char *data, int32_t rows, void *pConn);

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

  mInfo("mnode:%d, fqdn:%s ep:%s port:%u is created", pMnode->mnodeId, pDnode->dnodeFqdn, pDnode->dnodeEp,
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
    mnodeCancelGetNextMnode(pIter);
  }

  mnodeUpdateMnodeEpSet(NULL);

  return TSDB_CODE_SUCCESS;
}

int32_t mnodeInitMnodes() {
  mnodeMnodeInitLock();

  SMnodeObj tObj;
  tsMnodeUpdateSize = (int32_t)((int8_t *)tObj.updateEnd - (int8_t *)&tObj);

  SSdbTableDesc desc = {
    .id           = SDB_TABLE_MNODE,
    .name         = "mnodes",
    .hashSessions = TSDB_DEFAULT_MNODES_HASH_SIZE,
    .maxRowSize   = tsMnodeUpdateSize,
    .refCountPos  = (int32_t)((int8_t *)(&tObj.refCount) - (int8_t *)&tObj),
    .keyType      = SDB_KEY_INT,
    .fpInsert     = mnodeMnodeActionInsert,
    .fpDelete     = mnodeMnodeActionDelete,
    .fpUpdate     = mnodeMnodeActionUpdate,
    .fpEncode     = mnodeMnodeActionEncode,
    .fpDecode     = mnodeMnodeActionDecode,
    .fpDestroy    = mnodeMnodeActionDestroy,
    .fpRestored   = mnodeMnodeActionRestored
  };

  tsMnodeRid = sdbOpenTable(&desc);
  tsMnodeSdb = sdbGetTableByRid(tsMnodeRid);
  if (tsMnodeSdb == NULL) {
    mError("failed to init mnodes data");
    return -1;
  }

  mnodeAddShowMetaHandle(TSDB_MGMT_TABLE_MNODE, mnodeGetMnodeMeta);
  mnodeAddShowRetrieveHandle(TSDB_MGMT_TABLE_MNODE, mnodeRetrieveMnodes);
  mnodeAddShowFreeIterHandle(TSDB_MGMT_TABLE_MNODE, mnodeCancelGetNextMnode);

  mDebug("table:mnodes table is created");
  return TSDB_CODE_SUCCESS;
}

void mnodeCleanupMnodes() {
  sdbCloseTable(tsMnodeRid);
  tsMnodeSdb = NULL;
  mnodeMnodeDestroyLock();
}

int32_t mnodeGetMnodesNum() { 
  return (int32_t)sdbGetNumOfRows(tsMnodeSdb); 
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

void mnodeCancelGetNextMnode(void *pIter) {
  sdbFreeIter(tsMnodeSdb, pIter);
}

void mnodeUpdateMnodeEpSet(SMInfos *pMinfos) {
  bool    set = false;
  SMInfos mInfos = {0};

  if (pMinfos != NULL) {
    mInfo("vgId:1, update mnodes epSet, numOfMinfos:%d", pMinfos->mnodeNum);
    set = true;
    mInfos = *pMinfos;
  } else {
    mInfo("vgId:1, update mnodes epSet, numOfMnodes:%d", mnodeGetMnodesNum());
    int32_t index = 0;
    void *  pIter = NULL;
    while (1) {
      SMnodeObj *pMnode = NULL;
      pIter = mnodeGetNextMnode(pIter, &pMnode);
      if (pMnode == NULL) break;

      SDnodeObj *pDnode = mnodeGetDnode(pMnode->mnodeId);
      if (pDnode != NULL) {
        set = true;
        mInfos.mnodeInfos[index].mnodeId = pMnode->mnodeId;
        strcpy(mInfos.mnodeInfos[index].mnodeEp, pDnode->dnodeEp);
        if (pMnode->role == TAOS_SYNC_ROLE_MASTER) mInfos.inUse = index;
        index++;
      } else {
        set = false;
      }

      mnodeDecDnodeRef(pDnode);
      mnodeDecMnodeRef(pMnode);
    }

    mInfos.mnodeNum = index;
    if (mInfos.mnodeNum < sdbGetReplicaNum()) {
      set = false;
      mDebug("vgId:1, mnodes info not synced, current:%d syncCfgNum:%d", mInfos.mnodeNum, sdbGetReplicaNum());
    }
  }

  mnodeMnodeWrLock();

  if (set) {
    memset(&tsMEpForShell, 0, sizeof(SRpcEpSet));
    memset(&tsMEpForPeer, 0, sizeof(SRpcEpSet));
    memcpy(&tsMInfos, &mInfos, sizeof(SMInfos));
    tsMEpForShell.inUse = tsMInfos.inUse;
    tsMEpForPeer.inUse = tsMInfos.inUse;
    tsMEpForShell.numOfEps = tsMInfos.mnodeNum;
    tsMEpForPeer.numOfEps = tsMInfos.mnodeNum;

    mInfo("vgId:1, mnodes epSet is set, num:%d inUse:%d", tsMInfos.mnodeNum, tsMInfos.inUse);
    for (int index = 0; index < mInfos.mnodeNum; ++index) {
      SMInfo *pInfo = &tsMInfos.mnodeInfos[index];
      taosGetFqdnPortFromEp(pInfo->mnodeEp, tsMEpForShell.fqdn[index], &tsMEpForShell.port[index]);
      taosGetFqdnPortFromEp(pInfo->mnodeEp, tsMEpForPeer.fqdn[index], &tsMEpForPeer.port[index]);
      tsMEpForPeer.port[index] = tsMEpForPeer.port[index] + TSDB_PORT_DNODEDNODE;

      mInfo("vgId:1, mnode:%d, fqdn:%s shell:%u peer:%u", pInfo->mnodeId, tsMEpForShell.fqdn[index],
            tsMEpForShell.port[index], tsMEpForPeer.port[index]);

      tsMEpForShell.port[index] = htons(tsMEpForShell.port[index]);
      tsMEpForPeer.port[index] = htons(tsMEpForPeer.port[index]);
      pInfo->mnodeId = htonl(pInfo->mnodeId);
    }
  } else {
    mInfo("vgId:1, mnodes epSet not set, num:%d inUse:%d", tsMInfos.mnodeNum, tsMInfos.inUse);
    for (int index = 0; index < tsMInfos.mnodeNum; ++index) {
      mInfo("vgId:1, index:%d, ep:%s:%u", index, tsMEpForShell.fqdn[index], htons(tsMEpForShell.port[index]));
    }
  }

  mnodeMnodeUnLock();
}

void mnodeGetMnodeEpSetForPeer(SRpcEpSet *epSet, bool redirect) {
  mnodeMnodeRdLock();
  *epSet = tsMEpForPeer;
  mnodeMnodeUnLock();

  mTrace("vgId:1, mnodes epSet for peer is returned, num:%d inUse:%d", tsMEpForPeer.numOfEps, tsMEpForPeer.inUse);
  for (int32_t i = 0; i < epSet->numOfEps; ++i) {
    if (redirect && strcmp(epSet->fqdn[i], tsLocalFqdn) == 0 && htons(epSet->port[i]) == tsServerPort + TSDB_PORT_DNODEDNODE) {
      epSet->inUse = (i + 1) % epSet->numOfEps;
      mTrace("vgId:1, mnode:%d, for peer ep:%s:%u, set inUse to %d", i, epSet->fqdn[i], htons(epSet->port[i]), epSet->inUse);
    } else {
      mTrace("vgId:1, mpeer:%d, for peer ep:%s:%u", i, epSet->fqdn[i], htons(epSet->port[i]));
    }
  }
}

void mnodeGetMnodeEpSetForShell(SRpcEpSet *epSet, bool redirect) {
  mnodeMnodeRdLock();
  *epSet = tsMEpForShell;
  mnodeMnodeUnLock();

  if (mnodeGetDnodesNum() <= 1) {
    epSet->numOfEps = 0;
    return;
  }

  mTrace("vgId:1, mnodes epSet for shell is returned, num:%d inUse:%d", tsMEpForShell.numOfEps, tsMEpForShell.inUse);
  for (int32_t i = 0; i < epSet->numOfEps; ++i) {
    if (redirect && strcmp(epSet->fqdn[i], tsLocalFqdn) == 0 && htons(epSet->port[i]) == tsServerPort) {
      epSet->inUse = (i + 1) % epSet->numOfEps;
      mTrace("vgId:1, mnode:%d, for shell ep:%s:%u, set inUse to %d", i, epSet->fqdn[i], htons(epSet->port[i]), epSet->inUse);
    } else {
      mTrace("vgId:1, mnode:%d, for shell ep:%s:%u", i, epSet->fqdn[i], htons(epSet->port[i]));
    }
  }
}

char* mnodeGetMnodeMasterEp() {
  return tsMInfos.mnodeInfos[tsMInfos.inUse].mnodeEp;
}

void mnodeGetMnodeInfos(void *pMinfos) {
  mnodeMnodeRdLock();
  *(SMInfos *)pMinfos = tsMInfos;
  mnodeMnodeUnLock();
}

static int32_t mnodeSendCreateMnodeMsg(int32_t dnodeId, char *dnodeEp) {
  SCreateMnodeMsg *pCreate = rpcMallocCont(sizeof(SCreateMnodeMsg));
  if (pCreate == NULL) {
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  } else {
    pCreate->dnodeId = htonl(dnodeId);
    tstrncpy(pCreate->dnodeEp, dnodeEp, sizeof(pCreate->dnodeEp));
    mnodeGetMnodeInfos(&pCreate->mnodes);
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

  mDebug("dnode:%d, send create mnode msg to dnode %s, numOfMnodes:%d", dnodeId, dnodeEp, pCreate->mnodes.mnodeNum);
  for (int32_t i = 0; i < pCreate->mnodes.mnodeNum; ++i) {
    mDebug("index:%d, mnodeId:%d ep:%s", i, pCreate->mnodes.mnodeInfos[i].mnodeId, pCreate->mnodes.mnodeInfos[i].mnodeEp);
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
    mnodeUpdateMnodeEpSet(NULL);
    sdbUpdateAsync();
  }

  return code;
}

static bool mnodeAllOnline() {
  void *pIter = NULL;
  bool  allOnline = true;

  while (1) {
    SMnodeObj *pMnode = NULL;
    pIter = mnodeGetNextMnode(pIter, &pMnode);
    if (pMnode == NULL) break;
    if (pMnode->role != TAOS_SYNC_ROLE_MASTER && pMnode->role != TAOS_SYNC_ROLE_SLAVE) {
      allOnline = false;
      mDebug("mnode:%d, role:%s, not online", pMnode->mnodeId, syncRole[pMnode->role]);
      mnodeDecMnodeRef(pMnode);
    }
  }
  mnodeCancelGetNextMnode(pIter);

  return allOnline;
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

  if (needConfirm && !mnodeAllOnline()) {
    mDebug("wait all mnode online then create new mnode");
    return;
  }

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

  mnodeUpdateMnodeEpSet(NULL);
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

  mnodeUpdateMnodeEpSet(NULL);
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
