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

#include <arpa/inet.h>

#include "mgmt.h"
#include "tschemautil.h"
#pragma GCC diagnostic ignored "-Wunused-variable"

void *       vgSdb = NULL;
int          tsVgUpdateSize;
extern void *dbSdb;
extern void *acctSdb;
extern void *userSdb;
extern void *dnodeSdb;

void *(*mgmtVgroupActionFp[SDB_MAX_ACTION_TYPES])(void *row, char *str, int size, int *ssize);
void *mgmtVgroupActionInsert(void *row, char *str, int size, int *ssize);
void *mgmtVgroupActionDelete(void *row, char *str, int size, int *ssize);
void *mgmtVgroupActionUpdate(void *row, char *str, int size, int *ssize);
void *mgmtVgroupActionEncode(void *row, char *str, int size, int *ssize);
void *mgmtVgroupActionDecode(void *row, char *str, int size, int *ssize);
void *mgmtVgroupActionBeforeBatchUpdate(void *row, char *str, int size, int *ssize);
void *mgmtVgroupActionBatchUpdate(void *row, char *str, int size, int *ssize);
void *mgmtVgroupActionAfterBatchUpdate(void *row, char *str, int size, int *ssize);
void *mgmtVgroupActionReset(void *row, char *str, int size, int *ssize);
void *mgmtVgroupActionDestroy(void *row, char *str, int size, int *ssize);

void mgmtVgroupActionInit() {
  mgmtVgroupActionFp[SDB_TYPE_INSERT] = mgmtVgroupActionInsert;
  mgmtVgroupActionFp[SDB_TYPE_DELETE] = mgmtVgroupActionDelete;
  mgmtVgroupActionFp[SDB_TYPE_UPDATE] = mgmtVgroupActionUpdate;
  mgmtVgroupActionFp[SDB_TYPE_ENCODE] = mgmtVgroupActionEncode;
  mgmtVgroupActionFp[SDB_TYPE_DECODE] = mgmtVgroupActionDecode;
  mgmtVgroupActionFp[SDB_TYPE_BEFORE_BATCH_UPDATE] = mgmtVgroupActionBeforeBatchUpdate;
  mgmtVgroupActionFp[SDB_TYPE_BATCH_UPDATE] = mgmtVgroupActionBatchUpdate;
  mgmtVgroupActionFp[SDB_TYPE_AFTER_BATCH_UPDATE] = mgmtVgroupActionAfterBatchUpdate;
  mgmtVgroupActionFp[SDB_TYPE_RESET] = mgmtVgroupActionReset;
  mgmtVgroupActionFp[SDB_TYPE_DESTROY] = mgmtVgroupActionDestroy;
}

void *mgmtVgroupAction(char action, void *row, char *str, int size, int *ssize) {
  if (mgmtVgroupActionFp[action] != NULL) {
    return (*(mgmtVgroupActionFp[action]))(row, str, size, ssize);
  }
  return NULL;
}

int mgmtInitVgroups() {
  void *  pNode = NULL;
  SVgObj *pVgroup = NULL;

  mgmtVgroupActionInit();

  vgSdb = sdbOpenTable(tsMaxVGroups, sizeof(SVgObj), "vgroups", SDB_KEYTYPE_AUTO, mgmtDirectory, mgmtVgroupAction);
  if (vgSdb == NULL) {
    mError("failed to init vgroup data");
    return -1;
  }

  while (1) {
    pNode = sdbFetchRow(vgSdb, pNode, (void **)&pVgroup);
    if (pVgroup == NULL) break;

    SDbObj *pDb = mgmtGetDb(pVgroup->dbName);
    if (pDb == NULL) continue;

    pVgroup->prev = NULL;
    pVgroup->next = NULL;
    int size = sizeof(STabObj *) * pDb->cfg.maxSessions;
    pVgroup->meterList = (STabObj **)malloc(size);
    memset(pVgroup->meterList, 0, size);

    pVgroup->idPool = taosInitIdPool(pDb->cfg.maxSessions);
    taosIdPoolReinit(pVgroup->idPool);

    mgmtSetDnodeVgid(pVgroup->vnodeGid[0].vnode, pVgroup->vgId);
  }

  SVgObj tObj;
  tsVgUpdateSize = tObj.updateEnd - (char *)&tObj;

  mTrace("vgroup is initialized");
  return 0;
}

SVgObj *mgmtGetVgroup(int vgId) { return (SVgObj *)sdbGetRow(vgSdb, &vgId); }

void mgmtProcessVgTimer(void *handle, void *tmrId) {
  SDbObj *pDb = (SDbObj *)handle;
  if (pDb == NULL) return;

  if (pDb->vgStatus > TSDB_VG_STATUS_IN_PROGRESS) {
    mTrace("db:%s, set vgstatus from %d to %d", pDb->name, pDb->vgStatus, TSDB_VG_STATUS_READY);
    pDb->vgStatus = TSDB_VG_STATUS_READY;
  }

  pDb->vgTimer = NULL;
}

bool mgmtAllocateVnode(SVgObj *pVgroup) {
  int        selectedVnode = -1;
  SDnodeObj *pDnode = &dnodeObj;

  for (int i = 0; i < pDnode->numOfVnodes; i++) {
    int vnode = (i + pDnode->lastAllocVnode) % pDnode->numOfVnodes;
    if (pDnode->vload[vnode].vgId == 0 && pDnode->vload[vnode].status == TSDB_VN_STATUS_READY) {
      selectedVnode = vnode;
      break;
    }
  }

  if (selectedVnode == -1) {
    mError("vgroup:%d alloc vnode failed, free vnodes:%d", pVgroup->vgId, pDnode->numOfFreeVnodes);
    return false;
  } else {
    mTrace("vgroup:%d allocate vnode:%d, last allocated vnode:%d", pVgroup->vgId, selectedVnode,
           pDnode->lastAllocVnode);
    pVgroup->vnodeGid[0].vnode = selectedVnode;
    pDnode->lastAllocVnode = selectedVnode + 1;
    if (pDnode->lastAllocVnode >= pDnode->numOfVnodes) pDnode->lastAllocVnode = 0;
    return true;
  }
}

SVgObj *mgmtCreateVgroup(SDbObj *pDb) {
  SVgObj *pVgroup;
  int     size;

  size = sizeof(SVgObj);
  pVgroup = (SVgObj *)malloc(size);
  memset(pVgroup, 0, size);
  strcpy(pVgroup->dbName, pDb->name);
  pVgroup->numOfVnodes = 1;
  pVgroup->createdTime = taosGetTimestampMs();

  if (!mgmtAllocateVnode(pVgroup)) {
    mWarn("no enough free dnode");
    free(pVgroup);
    pDb->vgStatus = TSDB_VG_STATUS_FULL;
    taosTmrReset(mgmtProcessVgTimer, 5000, pDb, mgmtTmr, &pDb->vgTimer);
    return NULL;
  }

  sdbInsertRow(vgSdb, pVgroup, 0);

  mTrace("vgroup:%d vnode:%d db:%s is created", pVgroup->vgId, pVgroup->vnodeGid[0].vnode, pDb->name);

  mgmtSendVPeersMsg(pVgroup, pDb);

  return pVgroup;
}

int mgmtDropVgroup(SDbObj *pDb, SVgObj *pVgroup) {
  STabObj *pMeter;

  if (pVgroup->numOfMeters > 0) {
    for (int i = 0; i < pDb->cfg.maxSessions; ++i) {
      if (pVgroup->meterList != NULL) {
        pMeter = pVgroup->meterList[i];
        if (pMeter) mgmtDropMeter(pDb, pMeter->meterId, 0);
      }
    }
  }

  mgmtSendFreeVnodeMsg(pVgroup->vnodeGid[0].vnode);
  sdbDeleteRow(vgSdb, pVgroup);

  return 0;
}

void mgmtSetVgroupIdPool() {
  void *  pNode = NULL;
  SVgObj *pVgroup = NULL;
  SDbObj *pDb;

  while (1) {
    pNode = sdbFetchRow(vgSdb, pNode, (void **)&pVgroup);
    if (pVgroup == NULL || pVgroup->idPool == 0) break;

    taosIdPoolSetFreeList(pVgroup->idPool);
    pVgroup->numOfMeters = taosIdPoolNumOfUsed(pVgroup->idPool);

    pDb = mgmtGetDb(pVgroup->dbName);
    pDb->numOfTables += pVgroup->numOfMeters;
    if (pVgroup->numOfMeters >= pDb->cfg.maxSessions - 1)
      mgmtAddVgroupIntoDbTail(pDb, pVgroup);
    else
      mgmtAddVgroupIntoDb(pDb, pVgroup);
  }
}

void mgmtCleanUpVgroups() { sdbCloseTable(vgSdb); }

int mgmtGetVgroupMeta(SMeterMeta *pMeta, SShowObj *pShow, SConnObj *pConn) {
  int cols = 0;

  if (pConn->pDb == NULL) return TSDB_CODE_DB_NOT_SELECTED;

  SSchema *pSchema = tsGetSchema(pMeta);

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "vgId");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "meters");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "vnode");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int i = 1; i < cols; ++i) pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];

  pShow->numOfRows = pConn->pDb->numOfVgroups;
  pShow->pNode = pConn->pDb->pHead;
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  return 0;
}

int mgmtRetrieveVgroups(SShowObj *pShow, char *data, int rows, SConnObj *pConn) {
  int     numOfRows = 0;
  SVgObj *pVgroup = NULL;
  char *  pWrite;
  int     cols = 0;
  char    ipstr[20];

  while (numOfRows < rows) {
    pVgroup = (SVgObj *)pShow->pNode;
    if (pVgroup == NULL) break;
    pShow->pNode = (void *)pVgroup->next;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pVgroup->vgId;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pVgroup->numOfMeters;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pVgroup->vnodeGid[0].vnode;
    cols++;

    numOfRows++;
  }

  pShow->numOfReads += numOfRows;
  return numOfRows;
}

void *mgmtVgroupActionInsert(void *row, char *str, int size, int *ssize) {
  SVgObj *pVgroup = (SVgObj *)row;
  SDbObj *pDb = mgmtGetDb(pVgroup->dbName);

  if (pDb == NULL) return NULL;

  int tsize = sizeof(STabObj *) * pDb->cfg.maxSessions;
  pVgroup->meterList = (STabObj **)malloc(tsize);
  memset(pVgroup->meterList, 0, tsize);
  pVgroup->numOfMeters = 0;
  pVgroup->idPool = taosInitIdPool(pDb->cfg.maxSessions);
  mgmtAddVgroupIntoDb(pDb, pVgroup);
  mgmtSetDnodeVgid(pVgroup->vnodeGid[0].vnode, pVgroup->vgId);

  return NULL;
}

void *mgmtVgroupActionDelete(void *row, char *str, int size, int *ssize) {
  SVgObj *pVgroup = (SVgObj *)row;
  SDbObj *pDb = mgmtGetDb(pVgroup->dbName);

  if (pDb != NULL) mgmtRemoveVgroupFromDb(pDb, pVgroup);
  mgmtUnSetDnodeVgid(pVgroup->vnodeGid[0].vnode);
  tfree(pVgroup->meterList);

  return NULL;
}

void *mgmtVgroupActionUpdate(void *row, char *str, int size, int *ssize) {
  mgmtVgroupActionReset(row, str, size, ssize);
  SVgObj *pVgroup = (SVgObj *)row;

  mTrace("vgroup:%d update, numOfVnode:%d", pVgroup->vgId, pVgroup->numOfVnodes);

  return NULL;
}
void *mgmtVgroupActionEncode(void *row, char *str, int size, int *ssize) {
  SVgObj *pVgroup = (SVgObj *)row;
  int     tsize = pVgroup->updateEnd - (char *)pVgroup;
  if (size < tsize) {
    *ssize = -1;
  } else {
    memcpy(str, pVgroup, tsize);
    *ssize = tsize;
  }

  return NULL;
}
void *mgmtVgroupActionDecode(void *row, char *str, int size, int *ssize) {
  SVgObj *pVgroup = (SVgObj *)malloc(sizeof(SVgObj));
  if (pVgroup == NULL) return NULL;
  memset(pVgroup, 0, sizeof(SVgObj));

  int tsize = pVgroup->updateEnd - (char *)pVgroup;
  memcpy(pVgroup, str, tsize);

  return (void *)pVgroup;
}
void *mgmtVgroupActionBeforeBatchUpdate(void *row, char *str, int size, int *ssize) { return NULL; }
void *mgmtVgroupActionBatchUpdate(void *row, char *str, int size, int *ssize) { return NULL; }
void *mgmtVgroupActionAfterBatchUpdate(void *row, char *str, int size, int *ssize) { return NULL; }
void *mgmtVgroupActionReset(void *row, char *str, int size, int *ssize) {
  SVgObj *pVgroup = (SVgObj *)row;
  int     tsize = pVgroup->updateEnd - (char *)pVgroup;

  memcpy(pVgroup, str, tsize);

  return NULL;
}
void *mgmtVgroupActionDestroy(void *row, char *str, int size, int *ssize) {
  SVgObj *pVgroup = (SVgObj *)row;
  if (pVgroup->idPool) {
    taosIdPoolCleanUp(pVgroup->idPool);
    pVgroup->idPool = NULL;
  }
  if (pVgroup->meterList) tfree(pVgroup->meterList);
  tfree(row);
  return NULL;
}