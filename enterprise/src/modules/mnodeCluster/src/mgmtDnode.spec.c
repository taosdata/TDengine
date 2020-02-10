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

#include <arpa/inet.h>
#include <endian.h>
#include <stdbool.h>

#include "dnodeSystem.h"
#include "mnode.h"
#include "tschemautil.h"
#include "vnodeStatus.h"
#include "dnodeModule.h"

void *dnodeSdb = NULL;
int   tsDnodeUpdateSize;

extern uint32_t mgmtAccessSquence;
extern SMgmtIpList mgmtIpList;

void *(*mgmtDnodeActionFp[SDB_MAX_ACTION_TYPES])(void *row, char *str, int size, int *ssize);
void *mgmtDnodeActionInsert(void *row, char *str, int size, int *ssize);
void *mgmtDnodeActionDelete(void *row, char *str, int size, int *ssize);
void *mgmtDnodeActionUpdate(void *row, char *str, int size, int *ssize);
void *mgmtDnodeActionEncode(void *row, char *str, int size, int *ssize);
void *mgmtDnodeActionDecode(void *row, char *str, int size, int *ssize);
void *mgmtDnodeActionBeforeBatchUpdate(void *row, char *str, int size, int *ssize);
void *mgmtDnodeActionBatchUpdate(void *row, char *str, int size, int *ssize);
void *mgmtDnodeActionAfterBatchUpdate(void *row, char *str, int size, int *ssize);
void *mgmtDnodeActionReset(void *row, char *str, int size, int *ssize);
void *mgmtDnodeActionDestroy(void *row, char *str, int size, int *ssize);

void mgmtDnodeActionInit() {
  mgmtDnodeActionFp[SDB_TYPE_INSERT] = mgmtDnodeActionInsert;
  mgmtDnodeActionFp[SDB_TYPE_DELETE] = mgmtDnodeActionDelete;
  mgmtDnodeActionFp[SDB_TYPE_UPDATE] = mgmtDnodeActionUpdate;
  mgmtDnodeActionFp[SDB_TYPE_ENCODE] = mgmtDnodeActionEncode;
  mgmtDnodeActionFp[SDB_TYPE_DECODE] = mgmtDnodeActionDecode;
  mgmtDnodeActionFp[SDB_TYPE_BEFORE_BATCH_UPDATE] = mgmtDnodeActionBeforeBatchUpdate;
  mgmtDnodeActionFp[SDB_TYPE_BATCH_UPDATE] = mgmtDnodeActionBatchUpdate;
  mgmtDnodeActionFp[SDB_TYPE_AFTER_BATCH_UPDATE] = mgmtDnodeActionAfterBatchUpdate;
  mgmtDnodeActionFp[SDB_TYPE_RESET] = mgmtDnodeActionReset;
  mgmtDnodeActionFp[SDB_TYPE_DESTROY] = mgmtDnodeActionDestroy;
}

void *mgmtDnodeAction(char action, void *row, char *str, int size, int *ssize) {
  if (mgmtDnodeActionFp[(uint8_t)action] != NULL) {
    return (*(mgmtDnodeActionFp[(uint8_t)action]))(row, str, size, ssize);
  }
  return NULL;
}

int mgmtInitDnodes() {
  void *     pNode = NULL;
  SDnodeObj *pDnode = NULL;
  int64_t    numOfRows = 0;

  mgmtDnodeActionInit();

  dnodeSdb = sdbOpenTable(tsMaxDnodes, sizeof(SDnodeObj), "dnodes", SDB_KEYTYPE_UINT32, mgmtDirectory, mgmtDnodeAction);
  if (dnodeSdb == NULL) {
    mError("failed to init dnode data");
    return -1;
  }

  numOfRows = sdbGetNumOfRows(dnodeSdb);
  if (numOfRows <= 0) {
    if (strcmp(tsMasterIp, tsPrivateIp) == 0) {
      mgmtCreateDnode(inet_addr(tsPrivateIp));
      pDnode = mgmtGetDnode(inet_addr(tsPrivateIp));
      pDnode->moduleStatus |= (1 << TSDB_MOD_MGMT);
      sdbUpdateRow(dnodeSdb, pDnode, tsDnodeUpdateSize, 1);
    }
  }

  numOfRows = 0;
  while (1) {
    pNode = sdbFetchRow(dnodeSdb, pNode, (void **)&pDnode);
    if (pDnode == NULL) break;

    pDnode->status = TSDB_METER_STATE_OFFLINE;
    pDnode->thandle = NULL;
    pDnode->numOfFreeVnodes = pDnode->numOfVnodes;
    //    for (int vnode = 0; vnode<pDnode->numOfVnodes; ++vnode)
    for (int vnode = 0; vnode < TSDB_MAX_VNODES; ++vnode) pDnode->vload[vnode].vgId = 0;

    numOfRows++;
  }

  SDnodeObj tObj;
  tsDnodeUpdateSize = tObj.updateEnd - (char *)&tObj;

  mTrace("dnodes is initialized");
  return 0;
}

SDnodeObj *mgmtGetDnode(uint32_t ip) { return (SDnodeObj *)sdbGetRow(dnodeSdb, &ip); }

int mgmtCreateDnode(uint32_t ip) {
  SDnodeObj *pDnode;
  int        size;

  int numOfDnodes = sdbGetNumOfRows(dnodeSdb);
  if (numOfDnodes >= tsMaxDnodes) {
    mWarn("numOfDnodes:%d, exceed tsMaxDnodes:%d", numOfDnodes, tsMaxDnodes);
    return TSDB_CODE_TOO_MANY_DNODES;
  }

  int grantCode = grantCheckDnodes();
  if (grantCode != 0) return grantCode;

  size = sizeof(SDnodeObj);
  pDnode = (SDnodeObj *)malloc(size);
  memset(pDnode, 0, size);
  pDnode->privateIp = ip;
  pDnode->numOfVnodes = TSDB_INVALID_VNODE_NUM;
  pDnode->numOfFreeVnodes = TSDB_INVALID_VNODE_NUM;
  pDnode->createdTime = taosGetTimestampMs();
  pDnode->lastAccess = mgmtAccessSquence;

  int code = TSDB_CODE_SUCCESS;
  if (sdbInsertRow(dnodeSdb, pDnode, 0) < 0) {
    code = TSDB_CODE_SDB_ERROR;
    tfree(pDnode);
  }

  return code;
}

int mgmtDropDnode(SDnodeObj *pDnode) {
  char ipstr[20] = {0};
  tinet_ntoa(ipstr, pDnode->privateIp);

  mgmtUnSetModuleInDnode(pDnode, TSDB_MOD_MGMT);
  sdbDeleteRow(dnodeSdb, pDnode);
  mLPrint("dnode:%s is dropped from cluster", ipstr);

  return 0;
}

int mgmtDropDnodeByIp(uint32_t ip) {
  SDnodeObj *pDnode;

  pDnode = sdbGetRow(dnodeSdb, &ip);
  if (pDnode == NULL) return TSDB_CODE_INVALID_VALUE;

  if (pDnode->privateIp == mgmtIpList.ip[0]) {
    mError("dnode:%s, can't drop dnode which is master", taosIpStr(pDnode->privateIp));
    return TSDB_CODE_NO_REMOVE_MASTER;
  }

  return mgmtSetDnodeShellRemoving(pDnode);
}

int mgmtUpdateDnode(SDnodeObj *pDnode) { return sdbUpdateRow(dnodeSdb, pDnode, 0, 1); }

void mgmtCleanUpDnodes() { sdbCloseTable(dnodeSdb); }

int mgmtGetDnodesNum() {
  return sdbGetNumOfRows(dnodeSdb);
}

void *mgmtGetNextDnode(SShowObj *pShow, SDnodeObj **pDnode) {
  return sdbFetchRow(dnodeSdb, pShow->pNode, (void**)pDnode);
}

void *mgmtDnodeActionInsert(void *row, char *str, int size, int *ssize) {
  SDnodeObj *pDnode = (SDnodeObj *)row;

  pDnode->thandle = NULL;
  pDnode->status = TSDB_DN_STATUS_OFFLINE;
  pDnode->numOfFreeVnodes = pDnode->numOfVnodes;
  for (int vnode = 0; vnode < pDnode->numOfVnodes; ++vnode) pDnode->vload[vnode].vgId = 0;

  return NULL;
}

void *mgmtDnodeActionDelete(void *row, char *str, int size, int *ssize) { return NULL; }

void *mgmtDnodeActionUpdate(void *row, char *str, int size, int *ssize) {
  return mgmtDnodeActionReset(row, str, size, ssize);
}

void *mgmtDnodeActionEncode(void *row, char *str, int size, int *ssize) {
  SDnodeObj *pDnode = (SDnodeObj *)row;
  int        tsize = pDnode->updateEnd - (char *)pDnode;
  if (size < tsize) {
    *ssize = -1;
  } else {
    memcpy(str, pDnode, tsize);
    *ssize = tsize;
  }

  return NULL;
}

void *mgmtDnodeActionDecode(void *row, char *str, int size, int *ssize) {
  SDnodeObj *pDnode = (SDnodeObj *)malloc(sizeof(SDnodeObj));
  if (pDnode == NULL) return NULL;
  memset(pDnode, 0, sizeof(SDnodeObj));

  int tsize = pDnode->updateEnd - (char *)pDnode;
  memcpy(pDnode, str, tsize);

  return (void *)pDnode;
}

void *mgmtDnodeActionBeforeBatchUpdate(void *row, char *str, int size, int *ssize) { return NULL; }

void *mgmtDnodeActionBatchUpdate(void *row, char *str, int size, int *ssize) { return NULL; }

void *mgmtDnodeActionAfterBatchUpdate(void *row, char *str, int size, int *ssize) { return NULL; }

void *mgmtDnodeActionReset(void *row, char *str, int size, int *ssize) {
  SDnodeObj *pDnode = (SDnodeObj *)row;
  int        tsize = pDnode->updateEnd - (char *)pDnode;
  memcpy(pDnode, str, tsize);

  return NULL;
}

void *mgmtDnodeActionDestroy(void *row, char *str, int size, int *ssize) {
  tfree(row);
  return NULL;
}

bool mgmtCheckConfigShow(SGlobalConfig *cfg) {
  if (!(cfg->cfgType & TSDB_CFG_CTYPE_B_SHOW))
    return false;
  return true;
}
