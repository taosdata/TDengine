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

#include "taosmsg.h"
#include "vnode.h"
#include "vnodeUtil.h"

/* static TAOS *dbConn = NULL; */
void vnodeCloseStreamCallback(void *param);

void vnodeProcessStreamRes(void *param, TAOS_RES *tres, TAOS_ROW row) {
  SMeterObj *pObj = (SMeterObj *)param;
  dTrace("vid:%d sid:%d id:%s, stream result is ready", pObj->vnode, pObj->sid, pObj->meterId);

  // construct data
  int32_t     contLen = pObj->bytesPerPoint;
  char *      pTemp = calloc(1, sizeof(SSubmitMsg) + pObj->bytesPerPoint + sizeof(SVMsgHeader));
  SSubmitMsg *pMsg = (SSubmitMsg *)(pTemp + sizeof(SVMsgHeader));

  pMsg->numOfRows = htons(1);

  char ncharBuf[TSDB_MAX_BYTES_PER_ROW] = {0};

  int32_t offset = 0;
  for (int32_t i = 0; i < pObj->numOfColumns; ++i) {
    char *dst = row[i];
    if (dst == NULL) {
      setNull(pMsg->payLoad + offset, pObj->schema[i].type, pObj->schema[i].bytes);
    } else {
      // here, we need to transfer nchar(utf8) to unicode(ucs-4)
      if (pObj->schema[i].type == TSDB_DATA_TYPE_NCHAR) {
        taosMbsToUcs4(row[i], pObj->schema[i].bytes, ncharBuf, TSDB_MAX_BYTES_PER_ROW);
        dst = ncharBuf;
      }

      memcpy(pMsg->payLoad + offset, dst, pObj->schema[i].bytes);
    }

    offset += pObj->schema[i].bytes;
  }

  contLen += sizeof(SSubmitMsg);

  int32_t numOfPoints = 0;

  int32_t state = vnodeSetMeterState(pObj, TSDB_METER_STATE_INSERT);
  if (state == TSDB_METER_STATE_READY) {
    vnodeInsertPoints(pObj, (char *)pMsg, contLen, TSDB_DATA_SOURCE_SHELL, NULL, pObj->sversion, &numOfPoints);
    vnodeClearMeterState(pObj, TSDB_METER_STATE_INSERT);
  } else {
    dError("vid:%d sid:%d id:%s, failed to insert continuous query results, state:%d", pObj->vnode, pObj->sid,
           pObj->meterId, state);
  }

  assert(numOfPoints >= 0 && numOfPoints <= 1);
  tfree(pTemp);
}

static void vnodeGetDBFromMeterId(SMeterObj *pObj, char *db) {
  char *st = strstr(pObj->meterId, ".");
  char *end = strstr(st + 1, ".");

  memcpy(db, st + 1, end - (st + 1));
}

void vnodeOpenStreams(void *param, void *tmrId) {
  SVnodeObj *pVnode = (SVnodeObj *)param;
  SMeterObj *pObj;

  if (pVnode->meterList == NULL) return;

  taosTmrStopA(&pVnode->streamTimer);
  pVnode->streamTimer = NULL;

  for (int sid = 0; sid < pVnode->cfg.maxSessions; ++sid) {
    pObj = pVnode->meterList[sid];
    if (pObj == NULL || pObj->sqlLen == 0 || vnodeIsMeterState(pObj, TSDB_METER_STATE_DELETING)) continue;

    dTrace("vid:%d sid:%d id:%s, open stream:%s", pObj->vnode, sid, pObj->meterId, pObj->pSql);

    if (pVnode->dbConn == NULL) {
      char db[64] = {0};
      char user[64] = {0};
      vnodeGetDBFromMeterId(pObj, db);
      sprintf(user, "_%s", pVnode->cfg.acct);
      pVnode->dbConn = taos_connect(NULL, user, tsInternalPass, db, 0);
    }

    if (pVnode->dbConn == NULL) {
      dError("vid:%d, failed to connect to mgmt node: %s", pVnode->vnode, tsInternalIp);
      taosTmrReset(vnodeOpenStreams, 1000, param, vnodeTmrCtrl, &pVnode->streamTimer);
      return;
    }

    if (pObj->pStream == NULL) {
      pObj->pStream = taos_open_stream(pVnode->dbConn, pObj->pSql, vnodeProcessStreamRes, pObj->lastKey, pObj,
                                       vnodeCloseStreamCallback);
      if (pObj->pStream) pVnode->numOfStreams++;
    }
  }
}

void vnodeCreateStream(SMeterObj *pObj) {
  if (pObj->sqlLen <= 0) return;

  SVnodeObj *pVnode = vnodeList + pObj->vnode;

  if (pObj->pStream) return;

  dTrace("vid:%d sid:%d id:%s stream:%s is created", pObj->vnode, pObj->sid, pObj->meterId, pObj->pSql);
  if (pVnode->dbConn == NULL) {
    if (pVnode->streamTimer == NULL) taosTmrReset(vnodeOpenStreams, 1000, pVnode, vnodeTmrCtrl, &pVnode->streamTimer);
  } else {
    pObj->pStream = taos_open_stream(pVnode->dbConn, pObj->pSql, vnodeProcessStreamRes, pObj->lastKey, pObj,
                                     vnodeCloseStreamCallback);
    if (pObj->pStream) pVnode->numOfStreams++;
  }
}

// Close only one stream
void vnodeRemoveStream(SMeterObj *pObj) {
  SVnodeObj *pVnode = vnodeList + pObj->vnode;
  if (pObj->sqlLen <= 0) return;

  if (pObj->pStream) {
    taos_close_stream(pObj->pStream);
    pVnode->numOfStreams--;
  }

  pObj->pStream = NULL;
  if (pVnode->numOfStreams == 0) {
    taos_close(pVnode->dbConn);
    pVnode->dbConn = NULL;
  }

  dTrace("vid:%d sid:%d id:%d stream is removed", pObj->vnode, pObj->sid, pObj->meterId);
}

// Close all streams in a vnode
void vnodeCloseStream(SVnodeObj *pVnode) {
  SMeterObj *pObj;
  dTrace("vid:%d, stream is closed", pVnode->vnode);

  // stop stream computing
  for (int sid = 0; sid < pVnode->cfg.maxSessions; ++sid) {
    pObj = pVnode->meterList[sid];
    if (pObj == NULL) continue;
    if (pObj->sqlLen > 0 && pObj->pStream) {
      taos_close_stream(pObj->pStream);
      pVnode->numOfStreams--;
    }
    pObj->pStream = NULL;
  }
}

// Callback function called from client
void vnodeCloseStreamCallback(void *param) {
  SMeterObj *pMeter = (SMeterObj *)param;
  SVnodeObj *pVnode = NULL;

  if (pMeter == NULL || pMeter->sqlLen == 0) return;
  pVnode = vnodeList + pMeter->vnode;

  pMeter->sqlLen = 0;
  pMeter->pSql = NULL;
  pMeter->pStream = NULL;

  pVnode->numOfStreams--;

  if (pVnode->numOfStreams == 0) {
    taos_close(pVnode->dbConn);
    pVnode->dbConn = NULL;
  }

  vnodeSaveMeterObjToFile(pMeter);
}