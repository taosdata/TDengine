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
#include "tglobalcfg.h"
#include "tmodule.h"
#include "tstatus.h"
#include "taosdef.h"
#include "taosmsg.h"
#include "tlog.h"
#include "mnode.h"
#include "mgmtDnode.h"
#include "mgmtGrant.h"
#include "mgmtMnode.h"
#include "mgmtSdb.h"
#include "mgmtShell.h"
#include "mgmtUser.h"
#include "mgmtVgroup.h"
#include "dnodeMClient.h"

void   *tsDnodeSdb = NULL;
int32_t tsDnodeUpdateSize = 0;
extern void *  tsVgroupSdb;
static int32_t clusterCreateDnode(uint32_t ip);
static int32_t clusterDropDnode(SDnodeObj *pDnode);
static void    clusterProcessCreateDnodeMsg(SQueuedMsg *pMsg);
static void    clusterProcessDropDnodeMsg(SQueuedMsg *pMsg);

static int32_t clusterDnodeActionDestroy(SSdbOperDesc *pOper) {
  tfree(pOper->pObj);
  return TSDB_CODE_SUCCESS;
}

static int32_t clusterDnodeActionInsert(SSdbOperDesc *pOper) {
  return TSDB_CODE_SUCCESS;
}

static int32_t clusterDnodeActionDelete(SSdbOperDesc *pOper) {
  SDnodeObj *pDnode = pOper->pObj;
  void *     pNode = NULL;
  void *     pLastNode = NULL;
  SVgObj *   pVgroup = NULL;
  int32_t    numOfVgroups = 0;

  while (1) {
    pLastNode = pNode;
    pNode = sdbFetchRow(tsVgroupSdb, pNode, (void **)&pVgroup);
    if (pVgroup == NULL) break;

    if (pVgroup->vnodeGid[0].dnodeId == pDnode->dnodeId) {
      SSdbOperDesc oper = {
        .type = SDB_OPER_TYPE_LOCAL,
        .table = tsVgroupSdb,
        .pObj = pVgroup,
      };
      sdbDeleteRow(&oper);
      pNode = pLastNode;
      numOfVgroups++;
      continue;
    }
  }

  mTrace("dnode:%d, all vgroups:%d is dropped from sdb", pDnode->dnodeId, numOfVgroups);
  return TSDB_CODE_SUCCESS;
}

static int32_t clusterDnodeActionUpdate(SSdbOperDesc *pOper) {
  return TSDB_CODE_SUCCESS;
}

static int32_t clusterDnodeActionEncode(SSdbOperDesc *pOper) {
  SDnodeObj *pDnode = pOper->pObj;

  if (pOper->maxRowSize < tsDnodeUpdateSize) {
    return -1;
  } else {
    memcpy(pOper->rowData, pDnode, tsDnodeUpdateSize);
    pOper->rowSize = tsDnodeUpdateSize;
    return TSDB_CODE_SUCCESS;
  }
}

static int32_t clusterDnodeActionDecode(SSdbOperDesc *pOper) {
  SDnodeObj *pDnode = (SDnodeObj *) calloc(1, sizeof(SDnodeObj));
  if (pDnode == NULL) return TSDB_CODE_SERV_OUT_OF_MEMORY;

  memcpy(pDnode, pOper->rowData, tsDnodeUpdateSize);
  pOper->pObj = pDnode;
  return TSDB_CODE_SUCCESS;
}

int32_t clusterInit() {
  SDnodeObj tObj;
  tsDnodeUpdateSize = (int8_t *)tObj.updateEnd - (int8_t *)&tObj;

  SSdbTableDesc tableDesc = {
    .tableName    = "dnodes",
    .hashSessions = TSDB_MAX_DNODES,
    .maxRowSize   = tsDnodeUpdateSize,
    .refCountPos  = (int8_t *)(&tObj.refCount) - (int8_t *)&tObj,
    .keyType      = SDB_KEY_TYPE_AUTO,
    .insertFp     = clusterDnodeActionInsert,
    .deleteFp     = clusterDnodeActionDelete,
    .updateFp     = clusterDnodeActionUpdate,
    .encodeFp     = clusterDnodeActionEncode,
    .decodeFp     = clusterDnodeActionDecode,
    .destroyFp    = clusterDnodeActionDestroy,
  };

  tsDnodeSdb = sdbOpenTable(&tableDesc);
  if (tsDnodeSdb == NULL) {
    mError("failed to init dnodes data");
    return -1;
  }

  int32_t numOfRows = sdbGetNumOfRows(tsDnodeSdb);
  if (numOfRows <= 0) {
    if (strcmp(tsMasterIp, tsPrivateIp) == 0) {
      clusterCreateDnode(inet_addr(tsPrivateIp));
    }
  }

  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_CREATE_DNODE, clusterProcessCreateDnodeMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_DROP_DNODE, clusterProcessDropDnodeMsg);
  
  mTrace("dnodes is initialized");
  return 0;
}

void clusterCleanUp() {
  sdbCloseTable(tsDnodeSdb);
}

int32_t clusterGetDnodesNum() {
  return sdbGetNumOfRows(tsDnodeSdb);
}

SDnodeObj *clusterGetDnode(int32_t dnodeId) {
  return (SDnodeObj *)sdbGetRow(tsDnodeSdb, &dnodeId);
}

void clusterIncDnodeRef(SDnodeObj *pDnode) {
  sdbIncRef(tsDnodeSdb, pDnode);
}
void clusterDecDnodeRef(SDnodeObj *pDnode) {
  sdbDecRef(tsDnodeSdb, pDnode);
}

SDnodeObj *clusterGetDnodeByIp(uint32_t ip) {
  SDnodeObj *pDnode = NULL;
  void *     pNode = NULL;

  while (1) {
    pNode = sdbFetchRow(tsDnodeSdb, pNode, (void**)&pDnode);
    if (pDnode == NULL) break;
    if (ip == pDnode->privateIp) {
      return pDnode;
    }
  }

  return NULL;
}

static int32_t clusterCreateDnode(uint32_t ip) {
  int32_t grantCode = grantCheck(TSDB_GRANT_DNODE);
  if (grantCode != TSDB_CODE_SUCCESS) {
    return grantCode;
  }

  SDnodeObj *pDnode = mgmtGetDnodeByIp(ip);
  if (pDnode != NULL) {
    mError("dnode:%d is alredy exist, ip:%s", pDnode->dnodeId, taosIpStr(pDnode->privateIp));
    return TSDB_CODE_DNODE_ALREADY_EXIST;
  }

  pDnode = (SDnodeObj *) calloc(1, sizeof(SDnodeObj));
  pDnode->privateIp = ip;
  pDnode->publicIp = ip;
  pDnode->createdTime = taosGetTimestampMs();
  pDnode->status = TSDB_DN_STATUS_OFFLINE; 
  pDnode->numOfTotalVnodes = TSDB_INVALID_VNODE_NUM; 

  if (pDnode->privateIp == inet_addr(tsMasterIp)) {
    pDnode->moduleStatus |= (1 << TSDB_MOD_MGMT);
  }
  
  SSdbOperDesc oper = {
    .type = SDB_OPER_TYPE_GLOBAL,
    .table = tsDnodeSdb,
    .pObj = pDnode,
    .rowSize = sizeof(SDnodeObj)
  };

  int32_t code = sdbInsertRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    tfree(pDnode);
    code = TSDB_CODE_SDB_ERROR;
  }

  mPrint("dnode:%d is created, result:%s", pDnode->dnodeId, tstrerror(code));
  return code;
}

static int32_t clusterDropDnode(SDnodeObj *pDnode) {
  SSdbOperDesc oper = {
    .type = SDB_OPER_TYPE_GLOBAL,
    .table = tsDnodeSdb,
    .pObj = pDnode
  };

  int32_t code = sdbDeleteRow(&oper); 
  if (code != TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_SDB_ERROR;
  }

  mLPrint("dnode:%d is dropped from cluster, result:%s", pDnode->dnodeId, tstrerror(code));
  return code;
}

static int32_t clusterDropDnodeByIp(uint32_t ip) {
  SDnodeObj *pDnode = clusterGetDnodeByIp(ip);
  if (pDnode == NULL) {
    mError("dnode:%s, is not exist", taosIpStr(ip));
    return TSDB_CODE_INVALID_VALUE;
  }

  if (pDnode->privateIp == dnodeGetMnodeMasteIp()) {
    mError("dnode:%d, can't drop dnode which is master", pDnode->dnodeId);
    return TSDB_CODE_NO_REMOVE_MASTER;
  }

  return clusterDropDnode(pDnode);
}

static void clusterProcessCreateDnodeMsg(SQueuedMsg *pMsg) {
  SRpcMsg rpcRsp = {.handle = pMsg->thandle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
  if (mgmtCheckRedirect(pMsg->thandle)) return;

  SCMCreateDnodeMsg *pCreate = pMsg->pCont;

  if (strcmp(pMsg->pUser->pAcct->user, "root") != 0) {
    rpcRsp.code = TSDB_CODE_NO_RIGHTS;
  } else {
    uint32_t ip = inet_addr(pCreate->ip);
    rpcRsp.code = clusterCreateDnode(ip);
    if (rpcRsp.code == TSDB_CODE_SUCCESS) {
      SDnodeObj *pDnode = mgmtGetDnodeByIp(ip);
      mLPrint("dnode:%d, ip:%s is created by %s", pDnode->dnodeId, pCreate->ip, pMsg->pUser->user);
    } else {
      mError("failed to create dnode:%s, reason:%s", pCreate->ip, tstrerror(rpcRsp.code));
    }
  }
  rpcSendResponse(&rpcRsp);
}

static void clusterProcessDropDnodeMsg(SQueuedMsg *pMsg) {
  SRpcMsg rpcRsp = {.handle = pMsg->thandle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
  if (mgmtCheckRedirect(pMsg->thandle)) return;

  SCMDropDnodeMsg *pDrop = pMsg->pCont;
  if (strcmp(pMsg->pUser->pAcct->user, "root") != 0) {
    rpcRsp.code = TSDB_CODE_NO_RIGHTS;
  } else {
    uint32_t ip = inet_addr(pDrop->ip);
    rpcRsp.code = clusterDropDnodeByIp(ip);
    if (rpcRsp.code == TSDB_CODE_SUCCESS) {
      mLPrint("dnode:%s is dropped by %s", pDrop->ip, pMsg->pUser->user);
    } else {
      mError("failed to drop dnode:%s, reason:%s", pDrop->ip, tstrerror(rpcRsp.code));
    }
  }

  rpcSendResponse(&rpcRsp);
}

void *clusterGetNextDnode(void *pNode, SDnodeObj **pDnode) { 
  return sdbFetchRow(tsDnodeSdb, pNode, (void **)pDnode); 
}