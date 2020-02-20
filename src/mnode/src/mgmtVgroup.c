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
#include "mgmtDnode.h"
#include "mgmtDnodeInt.h"
#include "mgmtTable.h"
#include "mgmtVgroup.h"

void *       tsVgroupSdb = NULL;
int32_t      tsVgUpdateSize;
extern void *tsDbSdb;
extern void *tsUserSdb;

void *(*mgmtVgroupActionFp[SDB_MAX_ACTION_TYPES])(void *row, char *str, int32_t size, int32_t *ssize);
void *mgmtVgroupActionInsert(void *row, char *str, int32_t size, int32_t *ssize);
void *mgmtVgroupActionDelete(void *row, char *str, int32_t size, int32_t *ssize);
void *mgmtVgroupActionUpdate(void *row, char *str, int32_t size, int32_t *ssize);
void *mgmtVgroupActionEncode(void *row, char *str, int32_t size, int32_t *ssize);
void *mgmtVgroupActionDecode(void *row, char *str, int32_t size, int32_t *ssize);
void *mgmtVgroupActionBeforeBatchUpdate(void *row, char *str, int32_t size, int32_t *ssize);
void *mgmtVgroupActionBatchUpdate(void *row, char *str, int32_t size, int32_t *ssize);
void *mgmtVgroupActionAfterBatchUpdate(void *row, char *str, int32_t size, int32_t *ssize);
void *mgmtVgroupActionReset(void *row, char *str, int32_t size, int32_t *ssize);
void *mgmtVgroupActionDestroy(void *row, char *str, int32_t size, int32_t *ssize);

void mgmtVgroupActionInit() {
  mgmtVgroupActionFp[SDB_TYPE_INSERT] = mgmtVgroupActionInsert;
  mgmtVgroupActionFp[SDB_TYPE_DELETE] = mgmtVgroupActionDelete;
  mgmtVgroupActionFp[SDB_TYPE_UPDATE] = mgmtVgroupActionUpdate;
  mgmtVgroupActionFp[SDB_TYPE_ENCODE] = mgmtVgroupActionEncode;
  mgmtVgroupActionFp[SDB_TYPE_DECODE] = mgmtVgroupActionDecode;
  mgmtVgroupActionFp[SDB_TYPE_RESET] = mgmtVgroupActionReset;
  mgmtVgroupActionFp[SDB_TYPE_DESTROY] = mgmtVgroupActionDestroy;
}

void *mgmtVgroupAction(char action, void *row, char *str, int32_t size, int32_t *ssize) {
  if (mgmtVgroupActionFp[(uint8_t)action] != NULL) {
    return (*(mgmtVgroupActionFp[(uint8_t)action]))(row, str, size, ssize);
  }
  return NULL;
}

int32_t mgmtInitVgroups() {
  void *  pNode = NULL;
  SVgObj *pVgroup = NULL;

  mgmtVgroupActionInit();

  SVgObj tObj;
  tsVgUpdateSize = tObj.updateEnd - (int8_t *)&tObj;

  tsVgroupSdb = sdbOpenTable(tsMaxVGroups, sizeof(SVgObj), "vgroups", SDB_KEYTYPE_AUTO, tsMgmtDirectory, mgmtVgroupAction);
  if (tsVgroupSdb == NULL) {
    mError("failed to init vgroup data");
    return -1;
  }

  while (1) {
    pNode = sdbFetchRow(tsVgroupSdb, pNode, (void **)&pVgroup);
    if (pVgroup == NULL) break;

    SDbObj *pDb = mgmtGetDb(pVgroup->dbName);
    if (pDb == NULL) continue;

    pVgroup->prev = NULL;
    pVgroup->next = NULL;
    int32_t size = sizeof(STabObj *) * pDb->cfg.maxSessions;
    pVgroup->meterList = (STabObj **)malloc(size);
    if (pVgroup->meterList == NULL) {
      mError("failed to malloc(size:%d) for the meterList of vgroups", size);
      return -1;
    }
    
    memset(pVgroup->meterList, 0, size);

    pVgroup->idPool = taosInitIdPool(pDb->cfg.maxSessions);
    if (pVgroup->idPool == NULL) {
      mError("failed to taosInitIdPool for vgroups");
      free(pVgroup->meterList);
      return -1;
    }
    
    taosIdPoolReinit(pVgroup->idPool);

    if (pVgroup->vnodeGid[0].publicIp == 0) {
      pVgroup->vnodeGid[0].publicIp = inet_addr(tsPublicIp);
      pVgroup->vnodeGid[0].ip = inet_addr(tsPrivateIp);
      sdbUpdateRow(tsVgroupSdb, pVgroup, tsVgUpdateSize, 1);
    }

    mgmtSetDnodeVgid(pVgroup->vnodeGid, pVgroup->numOfVnodes, pVgroup->vgId);
  }

  mTrace("vgroup is initialized");
  return 0;
}

SVgObj *mgmtGetVgroup(int32_t vgId) {
  return (SVgObj *)sdbGetRow(tsVgroupSdb, &vgId);
}

SVgObj *mgmtGetAvailVgroup(SDbObj *pDb) {
  SVgObj *pVgroup = pDb->pHead;

  if (pDb->vgStatus == TSDB_VG_STATUS_IN_PROGRESS) {
    terrno = TSDB_CODE_ACTION_IN_PROGRESS;
    return NULL;
  }

  if (pDb->vgStatus == TSDB_VG_STATUS_FULL) {
    mError("db:%s, vgroup is full", pDb->name);
    terrno = TSDB_CODE_NO_ENOUGH_DNODES;
    return NULL;
  }

  if (pDb->vgStatus == TSDB_VG_STATUS_NO_DISK_PERMISSIONS ||
      pDb->vgStatus == TSDB_VG_STATUS_SERVER_NO_PACE ||
      pDb->vgStatus == TSDB_VG_STATUS_SERV_OUT_OF_MEMORY ||
      pDb->vgStatus == TSDB_VG_STATUS_INIT_FAILED ) {
    mError("db:%s, vgroup init failed, reason:%d %s", pDb->name, pDb->vgStatus, taosGetVgroupStatusStr(pDb->vgStatus));
    terrno = pDb->vgStatus;
    return NULL;
  }

  if (pVgroup == NULL) {
    pDb->vgStatus = TSDB_VG_STATUS_IN_PROGRESS;
    mgmtCreateVgroup(pDb);
    mTrace("db:%s, vgroup malloced, wait for create progress finished", pDb->name);
    terrno = TSDB_CODE_ACTION_IN_PROGRESS;
    return NULL;
  }

  terrno = 0;
  return pVgroup;
}

int32_t mgmtAllocateSid(SDbObj *pDb, SVgObj *pVgroup) {
  int32_t sid = taosAllocateId(pVgroup->idPool);
  if (sid < 0) {
    mWarn("table:%s, vgroup:%d run out of ID, num:%d", pDb->name, pVgroup->vgId, taosIdPoolNumOfUsed(pVgroup->idPool));
    pDb->vgStatus = TSDB_VG_STATUS_IN_PROGRESS;
    mgmtCreateVgroup(pDb);
    terrno = TSDB_CODE_ACTION_IN_PROGRESS;
  }

  terrno = 0;
  return sid;
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

SVgObj *mgmtCreateVgroup(SDbObj *pDb) {
  SVgObj *pVgroup;
  int32_t     size;

  size = sizeof(SVgObj);
  pVgroup = (SVgObj *)malloc(size);
  memset(pVgroup, 0, size);
  strcpy(pVgroup->dbName, pDb->name);
  pVgroup->numOfVnodes = pDb->cfg.replications;
  pVgroup->createdTime = taosGetTimestampMs();

  // based on load balance, create a new one
  if (mgmtAllocVnodes(pVgroup) != 0) {
    mError("db:%s, no enough free dnode to alloc %d vnodes", pDb->name, pVgroup->numOfVnodes);
    free(pVgroup);
    pDb->vgStatus = TSDB_VG_STATUS_FULL;
    taosTmrReset(mgmtProcessVgTimer, 5000, pDb, tsMgmtTmr, &pDb->vgTimer);
    return NULL;
  }

  sdbInsertRow(tsVgroupSdb, pVgroup, 0);

  mTrace("vgroup:%d, vgroup is created, db:%s replica:%d", pVgroup->vgId, pDb->name, pVgroup->numOfVnodes);
  for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i)
    mTrace("vgroup:%d, dnode:%s vnode:%d is created", pVgroup->vgId, taosIpStr(pVgroup->vnodeGid[i].ip), pVgroup->vnodeGid[i].vnode);

  mgmtSendVPeersMsg(pVgroup);

  return pVgroup;
}

int32_t mgmtDropVgroup(SDbObj *pDb, SVgObj *pVgroup) {
  STabObj *pTable;

  if (pVgroup->numOfMeters > 0) {
    for (int32_t i = 0; i < pDb->cfg.maxSessions; ++i) {
      if (pVgroup->meterList != NULL) {
        pTable = pVgroup->meterList[i];
        if (pTable) mgmtDropTable(pDb, pTable->meterId, 0);
      }
    }
  }

  mTrace("vgroup:%d, db:%s replica:%d is deleted", pVgroup->vgId, pDb->name, pVgroup->numOfVnodes);
  mgmtSendFreeVnodeMsg(pVgroup);
  sdbDeleteRow(tsVgroupSdb, pVgroup);

  return 0;
}

void mgmtSetVgroupIdPool() {
  void *  pNode = NULL;
  SVgObj *pVgroup = NULL;
  SDbObj *pDb;

  while (1) {
    pNode = sdbFetchRow(tsVgroupSdb, pNode, (void **)&pVgroup);
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

void mgmtCleanUpVgroups() { sdbCloseTable(tsVgroupSdb); }

int32_t mgmtGetVgroupMeta(SMeterMeta *pMeta, SShowObj *pShow, SConnObj *pConn) {
  int32_t cols = 0;

  SDbObj *pDb = NULL;
  if (pConn->pDb != NULL) pDb = mgmtGetDb(pConn->pDb->name);

  if (pDb == NULL) return TSDB_CODE_DB_NOT_SELECTED;

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

  pShow->bytes[cols] = 9;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "vgroup status");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  int32_t      maxReplica = 0;
  SVgObj  *pVgroup    = NULL;
  STabObj *pTable     = NULL;
  if (pShow->payloadLen > 0 ) {
//    pTable = mgmtGetTable(pShow->payload);
//    if (NULL == pTable) {
//      return TSDB_CODE_INVALID_TABLE_ID;
//    }
//
//    pVgroup = mgmtGetVgroup(pTable->gid.vgId);
//    if (NULL == pVgroup) return TSDB_CODE_INVALID_TABLE_ID;
//
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

int32_t mgmtRetrieveVgroups(SShowObj *pShow, char *data, int32_t rows, SConnObj *pConn) {
  int32_t     numOfRows = 0;
  SVgObj *pVgroup = NULL;
  char *  pWrite;
  int32_t     cols = 0;
  char    ipstr[20];

  int32_t maxReplica = 0;

  SDbObj *pDb = NULL;
  if (pConn->pDb != NULL) pDb = mgmtGetDb(pConn->pDb->name);
  assert(pDb != NULL);

  pVgroup = pDb->pHead;
  while (pVgroup != NULL) {
    maxReplica = pVgroup->numOfVnodes > maxReplica ? pVgroup->numOfVnodes : maxReplica;
    pVgroup = pVgroup->next;
  }

  while (numOfRows < rows) {
    //    pShow->pNode = sdbFetchRow(tsVgroupSdb, pShow->pNode, (void **)&pVgroup);
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
    strcpy(pWrite, taosGetVgroupLbStatusStr(pVgroup->lbStatus));
    cols++;

    for (int32_t i = 0; i < maxReplica; ++i) {
      tinet_ntoa(ipstr, pVgroup->vnodeGid[i].ip);
      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      strcpy(pWrite, ipstr);
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int16_t *)pWrite = pVgroup->vnodeGid[i].vnode;
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

void *mgmtVgroupActionInsert(void *row, char *str, int32_t size, int32_t *ssize) {
  SVgObj *pVgroup = (SVgObj *)row;
  SDbObj *pDb = mgmtGetDb(pVgroup->dbName);

  if (pDb == NULL) return NULL;

  int32_t tsize = sizeof(STabObj *) * pDb->cfg.maxSessions;
  pVgroup->meterList = (STabObj **)malloc(tsize);
  memset(pVgroup->meterList, 0, tsize);
  pVgroup->numOfMeters = 0;
  pVgroup->idPool = taosInitIdPool(pDb->cfg.maxSessions);
  mgmtAddVgroupIntoDb(pDb, pVgroup);
  mgmtSetDnodeVgid(pVgroup->vnodeGid, pVgroup->numOfVnodes, pVgroup->vgId);

  return NULL;
}

void *mgmtVgroupActionDelete(void *row, char *str, int32_t size, int32_t *ssize) {
  SVgObj *pVgroup = (SVgObj *)row;
  SDbObj *pDb = mgmtGetDb(pVgroup->dbName);

  if (pDb != NULL) mgmtRemoveVgroupFromDb(pDb, pVgroup);
  mgmtUnSetDnodeVgid(pVgroup->vnodeGid, pVgroup->numOfVnodes);
  tfree(pVgroup->meterList);

  return NULL;
}

void *mgmtVgroupActionUpdate(void *row, char *str, int32_t size, int32_t *ssize) {
  mgmtVgroupActionReset(row, str, size, ssize);
  SVgObj *pVgroup = (SVgObj *)row;
  int32_t oldTables = taosIdPoolMaxSize(pVgroup->idPool);

  SDbObj *pDb = mgmtGetDb(pVgroup->dbName);
  if (pDb != NULL) {
    if (pDb->cfg.maxSessions != oldTables) {
      mPrint("vgroup:%d tables change from %d to %d", pVgroup->vgId, oldTables, pDb->cfg.maxSessions);
      taosUpdateIdPool(pVgroup->idPool, pDb->cfg.maxSessions);
      int32_t size = sizeof(STabObj *) * pDb->cfg.maxSessions;
      pVgroup->meterList = (STabObj **)realloc(pVgroup->meterList, size);
    }
  }

  mTrace("vgroup:%d update, numOfVnode:%d", pVgroup->vgId, pVgroup->numOfVnodes);

  return NULL;
}
void *mgmtVgroupActionEncode(void *row, char *str, int32_t size, int32_t *ssize) {
  SVgObj *pVgroup = (SVgObj *)row;
  int32_t     tsize = pVgroup->updateEnd - (int8_t *)pVgroup;
  if (size < tsize) {
    *ssize = -1;
  } else {
    memcpy(str, pVgroup, tsize);
    *ssize = tsize;
  }

  return NULL;
}
void *mgmtVgroupActionDecode(void *row, char *str, int32_t size, int32_t *ssize) {
  SVgObj *pVgroup = (SVgObj *)malloc(sizeof(SVgObj));
  if (pVgroup == NULL) return NULL;
  memset(pVgroup, 0, sizeof(SVgObj));

  int32_t tsize = pVgroup->updateEnd - (int8_t *)pVgroup;
  memcpy(pVgroup, str, tsize);

  return (void *)pVgroup;
}
void *mgmtVgroupActionBeforeBatchUpdate(void *row, char *str, int32_t size, int32_t *ssize) { return NULL; }
void *mgmtVgroupActionBatchUpdate(void *row, char *str, int32_t size, int32_t *ssize) { return NULL; }
void *mgmtVgroupActionAfterBatchUpdate(void *row, char *str, int32_t size, int32_t *ssize) { return NULL; }
void *mgmtVgroupActionReset(void *row, char *str, int32_t size, int32_t *ssize) {
  SVgObj *pVgroup = (SVgObj *)row;
  int32_t     tsize = pVgroup->updateEnd - (int8_t *)pVgroup;

  memcpy(pVgroup, str, tsize);

  return NULL;
}
void *mgmtVgroupActionDestroy(void *row, char *str, int32_t size, int32_t *ssize) {
  SVgObj *pVgroup = (SVgObj *)row;
  if (pVgroup->idPool) {
    taosIdPoolCleanUp(pVgroup->idPool);
    pVgroup->idPool = NULL;
  }
  if (pVgroup->meterList) tfree(pVgroup->meterList);
  tfree(row);
  return NULL;
}
