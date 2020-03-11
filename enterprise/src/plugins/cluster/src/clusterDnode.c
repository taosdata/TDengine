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
#include "tmodule.h"
#include "tstatus.h"
#include "tschemautil.h"
#include "mgmtGrant.h"
#include "dnode.h"
#include "dnodeModule.h"
#include "clusterDnode.h"
#include "clusterDnodeConn.h"

void *tsDnodeSdb = NULL;

static int32_t tsDnodeUpdateSize = 0;

static void *(*mgmtDnodeActionFp[SDB_MAX_ACTION_TYPES])(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtDnodeActionInsert(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtDnodeActionDelete(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtDnodeActionUpdate(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtDnodeActionEncode(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtDnodeActionDecode(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtDnodeActionReset(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtDnodeActionDestroy(void *row, char *str, int32_t size, int32_t *ssize);

static void mgmtDnodeActionInit() {
  mgmtDnodeActionFp[SDB_TYPE_INSERT] = mgmtDnodeActionInsert;
  mgmtDnodeActionFp[SDB_TYPE_DELETE] = mgmtDnodeActionDelete;
  mgmtDnodeActionFp[SDB_TYPE_UPDATE] = mgmtDnodeActionUpdate;
  mgmtDnodeActionFp[SDB_TYPE_ENCODE] = mgmtDnodeActionEncode;
  mgmtDnodeActionFp[SDB_TYPE_DECODE] = mgmtDnodeActionDecode;
  mgmtDnodeActionFp[SDB_TYPE_RESET] = mgmtDnodeActionReset;
  mgmtDnodeActionFp[SDB_TYPE_DESTROY] = mgmtDnodeActionDestroy;
}

static void *mgmtDnodeAction(char action, void *row, char *str, int32_t size, int32_t *ssize) {
  if (mgmtDnodeActionFp[(uint8_t)action] != NULL) {
    return (*(mgmtDnodeActionFp[(uint8_t)action]))(row, str, size, ssize);
  }
  return NULL;
}

int32_t mgmtCreateDnode(uint32_t ip) {
  int32_t numOfDnodes = sdbGetNumOfRows(tsDnodeSdb);
  if (numOfDnodes >= tsMaxDnodes) {
    mWarn("numOfDnodes:%d, exceed tsMaxDnodes:%d", numOfDnodes, tsMaxDnodes);
    return TSDB_CODE_TOO_MANY_DNODES;
  }

  int32_t grantCode = mgmtCheckDbGrant();
  if (grantCode != 0) {
    return grantCode;
  }

  SDnodeObj *pDnode = (SDnodeObj *) calloc(1, sizeof(SDnodeObj));
  pDnode->privateIp       = ip;
  pDnode->numOfVnodes     = TSDB_INVALID_VNODE_NUM;
  pDnode->numOfFreeVnodes = TSDB_INVALID_VNODE_NUM;
  pDnode->createdTime     = taosGetTimestampMs();
  pDnode->lastAccess      = 0;

  int32_t code = TSDB_CODE_SUCCESS;
  if (sdbInsertRow(tsDnodeSdb, pDnode, 0) < 0) {
    code = TSDB_CODE_SDB_ERROR;
    tfree(pDnode);
  }

  return code;
}

int32_t mgmtDropDnode(SDnodeObj *pDnode) {
  char ipstr[20] = {0};
  tinet_ntoa(ipstr, pDnode->privateIp);

  //TODO mpeer implementation
  //mgmtUnSetModuleInDnode(pDnode, TSDB_MOD_MGMT);
  sdbDeleteRow(tsDnodeSdb, pDnode);
  mLPrint("dnode:%s is dropped from cluster", ipstr);

  return 0;
}

int32_t mgmtDropDnodeByIp(uint32_t ip) {
  SDnodeObj *pDnode = sdbGetRow(tsDnodeSdb, &ip);
  if (pDnode == NULL) return TSDB_CODE_INVALID_VALUE;

  if (pDnode->privateIp == dnodeGetMgmtIp()) {
    mError("dnode:%s, can't drop dnode which is master", taosIpStr(pDnode->privateIp));
    return TSDB_CODE_NO_REMOVE_MASTER;
  }

  return mgmtDropDnode(pDnode);
  //return mgmtSetDnodeShellRemoving(pDnode);
}

int32_t mgmtUpdateDnodeImp(SDnodeObj *pDnode) {
  return sdbUpdateRow(tsDnodeSdb, pDnode, 0, 1);
}

static void *mgmtDnodeActionInsert(void *row, char *str, int32_t size, int32_t *ssize) {
  SDnodeObj *pDnode = (SDnodeObj *)row;

  pDnode->status = TSDB_DN_STATUS_OFFLINE;
  pDnode->numOfFreeVnodes = pDnode->numOfVnodes;
  for (int32_t vnode = 0; vnode < pDnode->numOfVnodes; ++vnode) {
    pDnode->vload[vnode].vgId = 0;
  }

  return NULL;
}

static void *mgmtDnodeActionDelete(void *row, char *str, int32_t size, int32_t *ssize) {
  return NULL;
}

static void *mgmtDnodeActionUpdate(void *row, char *str, int32_t size, int32_t *ssize) {
  return mgmtDnodeActionReset(row, str, size, ssize);
}

static void *mgmtDnodeActionEncode(void *row, char *str, int32_t size, int32_t *ssize) {
  SDnodeObj *pDnode = (SDnodeObj *)row;
  if (size < tsDnodeUpdateSize) {
    *ssize = -1;
  } else {
    memcpy(str, pDnode, tsDnodeUpdateSize);
    *ssize = tsDnodeUpdateSize;
  }

  return NULL;
}

void *mgmtDnodeActionDecode(void *row, char *str, int32_t size, int32_t *ssize) {
  SDnodeObj *pDnode = calloc(1, sizeof(SDnodeObj));
  memcpy(pDnode, str, tsDnodeUpdateSize);

  return (void *)pDnode;
}

void *mgmtDnodeActionReset(void *row, char *str, int32_t size, int32_t *ssize) {
  SDnodeObj *pDnode = (SDnodeObj *)row;
  memcpy(pDnode, str, tsDnodeUpdateSize);

  return NULL;
}

void *mgmtDnodeActionDestroy(void *row, char *str, int32_t size, int32_t *ssize) {
  tfree(row);
  return NULL;
}

int32_t mgmtInitDnodesImp() {
  void *     pNode = NULL;
  SDnodeObj *pDnode = NULL;
  int64_t    numOfRows = 0;

  mgmtDnodeActionInit();

  SDnodeObj tObj;
  tsDnodeUpdateSize = tObj.updateEnd - (char *)&tObj;

  tsDnodeSdb = sdbOpenTable(tsMaxDnodes, tsDnodeUpdateSize, "dnodes", SDB_KEYTYPE_UINT32, tsMgmtDirectory, mgmtDnodeAction);
  if (tsDnodeSdb == NULL) {
    mError("failed to init dnode data");
    return -1;
  }

  numOfRows = sdbGetNumOfRows(tsDnodeSdb);
  if (numOfRows <= 0) {
    if (strcmp(tsMasterIp, tsPrivateIp) == 0) {
      mgmtCreateDnode(inet_addr(tsPrivateIp));
      pDnode = mgmtGetDnodeImp(inet_addr(tsPrivateIp));
      pDnode->moduleStatus |= (1 << TSDB_MOD_MGMT);
      sdbUpdateRow(tsDnodeSdb, pDnode, tsDnodeUpdateSize, 1);
    }
  }

  numOfRows = 0;
  while (1) {
    pNode = sdbFetchRow(tsDnodeSdb, pNode, (void **)&pDnode);
    if (pDnode == NULL) break;

    pDnode->status = TSDB_METER_STATE_OFFLINE;
    pDnode->thandle = NULL;
    pDnode->numOfFreeVnodes = pDnode->numOfVnodes;
    for (int32_t vnode = 0; vnode < TSDB_MAX_VNODES; ++vnode) {
      pDnode->vload[vnode].vgId = 0;
    }

    numOfRows++;
  }

  mTrace("dnodes is initialized");
  return 0;
}

SDnodeObj *mgmtGetDnodeImp(uint32_t ip) {
  return (SDnodeObj *)sdbGetRow(tsDnodeSdb, &ip);
}

void mgmtCleanUpDnodesImp() {
  sdbCloseTable(tsDnodeSdb);
}

int32_t mgmtGetDnodesNumImp() {
  return sdbGetNumOfRows(tsDnodeSdb);
}

void *mgmtGetNextDnodeImp(SShowObj *pShow, SDnodeObj **pDnode) {
  return sdbFetchRow(tsDnodeSdb, pShow->pNode, (void**)pDnode);
}

int32_t mgmtGetDnodeMeta(STableMeta *pMeta, SShowObj *pShow, void *pConn) {
  int32_t cols = 0;

  SUserObj *pUser = mgmtGetUserFromConn(pConn);
  if (pUser == NULL) return 0;

  if (strcmp(pUser->user, "root") != 0) return TSDB_CODE_NO_RIGHTS;

  SSchema *pSchema = tsGetSchema(pMeta);

  pShow->bytes[cols] = 16;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "IP");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "created time");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "open vnodes");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "free vnodes");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 10;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "status");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 18;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "balance state");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 16;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "public ip");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = mgmtGetDnodesNum();
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->pNode = NULL;

  return 0;
}

int32_t mgmtRetrieveDnodes(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t   numOfRows = 0;
  int32_t   cols      = 0;
  SDnodeObj *pDnode   = NULL;
  char      *pWrite;
  char      ipstr[20];

  while (numOfRows < rows) {
    pShow->pNode = mgmtGetNextDnode(pShow, (SDnodeObj **)&pDnode);
    if (pDnode == NULL) break;

    cols = 0;

    tinet_ntoa(ipstr, pDnode->privateIp);
    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, ipstr);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pDnode->createdTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pDnode->openVnodes;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pDnode->numOfFreeVnodes;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, taosGetDnodeStatusStr(pDnode->status) );
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, taosGetDnodeLbStatusStr(pDnode->lbStatus));
    cols++;

    tinet_ntoa(ipstr, pDnode->publicIp);
    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, ipstr);
    cols++;

    numOfRows++;
  }

  pShow->numOfReads += numOfRows;
  return numOfRows;
}


int32_t mgmtGetScoresMeta(STableMeta *pMeta, SShowObj *pShow, void *pConn) {
  if (mgmtGetScoresMetaFp) {
    SUserObj *pUser = mgmtGetUserFromConn(pConn);
    if (pUser == NULL) return 0;
    if (strcmp(pUser->user, "root") != 0) return TSDB_CODE_NO_RIGHTS;
    return mgmtGetScoresMetaFp(pMeta, pShow, pConn);
  } else {
    return TSDB_CODE_OPS_NOT_SUPPORT;
  }
}

int32_t mgmtRetrieveScores(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  if (mgmtRetrieveScoresFp) {
    return mgmtRetrieveScoresFp(pShow, data, rows, pConn);
  } else {
    return 0;
  }
}

//  tsMgmtShowMetaFp[TSDB_MGMT_TABLE_SCORES]  = mgmtGetScoresMeta;
//  tsMgmtShowRetrieveFp[TSDB_MGMT_TABLE_SCORES]  = mgmtRetrieveScores;
//  tsMgmtShowRetrieveFp[TSDB_MGMT_TABLE_DNODE]   = mgmtRetrieveDnodes;
//  tsMgmtShowMetaFp[TSDB_MGMT_TABLE_DNODE]   = mgmtGetDnodeMeta;


//static void mgmtProcessCreateDnodeMsg(SRpcMsg *rpcMsg) {
//  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
//  if (!mgmtCreateDnodeFp) {
//    rpcRsp.code = TSDB_CODE_OPS_NOT_SUPPORT;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  SCreateDnodeMsg *pCreate = (SCreateDnodeMsg *) rpcMsg->pCont;
//  if (mgmtCheckRedirect(rpcMsg->handle) != TSDB_CODE_SUCCESS) {
//    mError("failed to create dnode:%s, redirect this message", pCreate->ip);
//    return;
//  }
//
//  SUserObj *pUser = mgmtGetUserFromConn(rpcMsg->handle);
//  if (pUser == NULL) {
//    mError("failed to create dnode:%s, reason:%s", pCreate->ip, tstrerror(TSDB_CODE_INVALID_USER));
//    rpcRsp.code = TSDB_CODE_INVALID_USER;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  if (strcmp(pUser->user, "root") != 0) {
//    mError("failed to create dnode:%s, reason:%s", pCreate->ip, tstrerror(TSDB_CODE_NO_RIGHTS));
//    rpcRsp.code = TSDB_CODE_NO_RIGHTS;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  int32_t code = (*mgmtCreateDnodeFp)(inet_addr(pCreate->ip));
//  if (code == TSDB_CODE_SUCCESS) {
//    mLPrint("dnode:%s is created by %s", pCreate->ip, pUser->user);
//  } else {
//    mError("failed to create dnode:%s, reason:%s", pCreate->ip, tstrerror(code));
//  }
//
//  rpcRsp.code = code;
//  rpcSendResponse(&rpcRsp);
//}
//
//static void mgmtProcessDropDnodeMsg(SRpcMsg *rpcMsg) {
//  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
//  if (!mgmtDropDnodeByIpFp) {
//    rpcRsp.code = TSDB_CODE_OPS_NOT_SUPPORT;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  SDropDnodeMsg *pDrop = (SDropDnodeMsg *) rpcMsg->pCont;
//  if (mgmtCheckRedirect(rpcMsg->handle) != TSDB_CODE_SUCCESS) {
//    mError("failed to drop dnode:%s, redirect this message", pDrop->ip);
//    return;
//  }
//
//  SUserObj *pUser = mgmtGetUserFromConn(rpcMsg->handle);
//  if (pUser == NULL) {
//    mError("failed to drop dnode:%s, reason:%s", pDrop->ip, tstrerror(TSDB_CODE_INVALID_USER));
//    rpcRsp.code = TSDB_CODE_INVALID_USER;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  if (strcmp(pUser->user, "root") != 0) {
//    mError("failed to drop dnode:%s, reason:%s", pDrop->ip, tstrerror(TSDB_CODE_NO_RIGHTS));
//    rpcRsp.code = TSDB_CODE_NO_RIGHTS;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  int32_t code = (*mgmtDropDnodeByIpFp)(inet_addr(pDrop->ip));
//  if (code == TSDB_CODE_SUCCESS) {
//    mLPrint("dnode:%s set to removing state by %s", pDrop->ip, pUser->user);
//  } else {
//    mError("failed to drop dnode:%s, reason:%s", pDrop->ip, tstrerror(code));
//  }
//
//  rpcRsp.code = code;
//  rpcSendResponse(&rpcRsp);
//}
//

//  mgmtProcessShellMsg[TSDB_MSG_TYPE_CM_CREATE_DNODE]     = mgmtProcessCreateDnodeMsg;
//  mgmtProcessShellMsg[TSDB_MSG_TYPE_CM_DROP_DNODE]       = mgmtProcessDropDnodeMsg;
