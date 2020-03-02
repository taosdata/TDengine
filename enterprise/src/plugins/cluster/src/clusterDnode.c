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
#include "tschemautil.h"
#include "dnodeSystem.h"
#include "mnode.h"
#include "dnodeModule.h"

void *tsDnodeSdb = NULL;

extern uint32_t mgmtAccessSquence;
static int32_t tsDnodeUpdateSize = 0;

static void *(*mgmtDnodeActionFp[SDB_MAX_ACTION_TYPES])(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtDnodeActionInsert(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtDnodeActionDelete(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtDnodeActionUpdate(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtDnodeActionEncode(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtDnodeActionDecode(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtDnodeActionReset(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtDnodeActionDestroy(void *row, char *str, int32_t size, int32_t *ssize);

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
      pDnode = mgmtGetDnode(inet_addr(tsPrivateIp));
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

static int32_t mgmtCreateDnode(uint32_t ip) {
  int32_t numOfDnodes = sdbGetNumOfRows(tsDnodeSdb);
  if (numOfDnodes >= tsMaxDnodes) {
    mWarn("numOfDnodes:%d, exceed tsMaxDnodes:%d", numOfDnodes, tsMaxDnodes);
    return TSDB_CODE_TOO_MANY_DNODES;
  }

  int32_t grantCode = grantCheckDnodes();
  if (grantCode != 0) {
    return grantCode;
  }

  SDnodeObj *pDnode = (SDnodeObj *) calloc(1, sizeof(SDnodeObj));
  pDnode->privateIp       = ip;
  pDnode->numOfVnodes     = TSDB_INVALID_VNODE_NUM;
  pDnode->numOfFreeVnodes = TSDB_INVALID_VNODE_NUM;
  pDnode->createdTime     = taosGetTimestampMs();
  pDnode->lastAccess      = mgmtAccessSquence;

  int32_t code = TSDB_CODE_SUCCESS;
  if (sdbInsertRow(tsDnodeSdb, pDnode, 0) < 0) {
    code = TSDB_CODE_SDB_ERROR;
    tfree(pDnode);
  }

  return code;
}

static int32_t mgmtDropDnode(SDnodeObj *pDnode) {
  char ipstr[20] = {0};
  tinet_ntoa(ipstr, pDnode->privateIp);

  mgmtUnSetModuleInDnode(pDnode, TSDB_MOD_MGMT);
  sdbDeleteRow(tsDnodeSdb, pDnode);
  mLPrint("dnode:%s is dropped from cluster", ipstr);

  return 0;
}

static int32_t mgmtDropDnodeByIp(uint32_t ip) {
  SDnodeObj *pDnode = sdbGetRow(tsDnodeSdb, &ip);
  if (pDnode == NULL) return TSDB_CODE_INVALID_VALUE;

  if (pDnode->privateIp == mgmtIpList.ip[0]) {
    mError("dnode:%s, can't drop dnode which is master", taosIpStr(pDnode->privateIp));
    return TSDB_CODE_NO_REMOVE_MASTER;
  }

  return mgmtSetDnodeShellRemoving(pDnode);
}

static int32_t mgmtUpdateDnode(SDnodeObj *pDnode) {
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
  int32_t        tsize = pDnode->updateEnd - (char *)pDnode;
  if (size < tsize) {
    *ssize = -1;
  } else {
    memcpy(str, pDnode, tsize);
    *ssize = tsize;
  }

  return NULL;
}

void *mgmtDnodeActionDecode(void *row, char *str, int32_t size, int32_t *ssize) {
  SDnodeObj *pDnode = (SDnodeObj *)malloc(sizeof(SDnodeObj));
  if (pDnode == NULL) return NULL;
  memset(pDnode, 0, sizeof(SDnodeObj));

  int32_t tsize = pDnode->updateEnd - (char *)pDnode;
  memcpy(pDnode, str, tsize);

  return (void *)pDnode;
}

void *mgmtDnodeActionBeforeBatchUpdate(void *row, char *str, int32_t size, int32_t *ssize) { return NULL; }

void *mgmtDnodeActionBatchUpdate(void *row, char *str, int32_t size, int32_t *ssize) { return NULL; }

void *mgmtDnodeActionAfterBatchUpdate(void *row, char *str, int32_t size, int32_t *ssize) { return NULL; }

void *mgmtDnodeActionReset(void *row, char *str, int32_t size, int32_t *ssize) {
  SDnodeObj *pDnode = (SDnodeObj *)row;
  int32_t        tsize = pDnode->updateEnd - (char *)pDnode;
  memcpy(pDnode, str, tsize);

  return NULL;
}

void *mgmtDnodeActionDestroy(void *row, char *str, int32_t size, int32_t *ssize) {
  tfree(row);
  return NULL;
}

bool mgmtCheckConfigShow(SGlobalConfig *cfg) {
  if (!(cfg->cfgType & TSDB_CFG_CTYPE_B_SHOW))
    return false;
  return true;
}


int32_t mgmtProcessCreateDnodeMsg(char *pMsg, int32_t msgLen, void *pConn) {
  SCreateDnodeMsg *pCreate = (SCreateDnodeMsg *)pMsg;
  int32_t              code = 0;

  if (!sdbMaster) return mgmtRedirectMsg(pConn, TSDB_MSG_TYPE_CREATE_DNODE_RSP);

  if (strcmp(pConn->pUser->user, "root") != 0) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    code = mgmtCreateDnode(inet_addr(pCreate->ip));
    if (code == TSDB_CODE_SUCCESS) {
      mLPrint("dnode:%s is created by %s", pCreate->ip, pConn->pUser->user);
    }
  }

  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_CREATE_DNODE_RSP, code);

  return 0;
}


int32_t mgmtProcessDropDnodeMsg(char *pMsg, int32_t msgLen, void *pConn) {
  SDropDnodeMsg *pDrop = (SDropDnodeMsg *)pMsg;
  int32_t            code = 0;

  if (!sdbMaster) return mgmtRedirectMsg(pConn, TSDB_MSG_TYPE_DROP_DNODE_RSP);

  if (strcmp(pConn->pUser->user, "root") != 0) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    code = mgmtDropDnodeByIp(inet_addr(pDrop->ip));
  }

  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_DROP_DNODE_RSP, code);

  if (code == 0) {
    mLPrint("dnode:%s set to removing state by %s", pDrop->ip, pConn->pUser->user);
  }

  return 0;
}
