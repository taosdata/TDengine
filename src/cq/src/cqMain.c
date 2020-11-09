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

#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>

#include "taos.h"
#include "tsclient.h"
#include "taosdef.h"
#include "taosmsg.h"
#include "ttimer.h"
#include "tcq.h"
#include "tdataformat.h"
#include "tglobal.h"
#include "tlog.h"
#include "twal.h"

#define cFatal(...) { if (cqDebugFlag & DEBUG_FATAL) { taosPrintLog("CQ  FATAL ", 255, __VA_ARGS__); }}
#define cError(...) { if (cqDebugFlag & DEBUG_ERROR) { taosPrintLog("CQ  ERROR ", 255, __VA_ARGS__); }}
#define cWarn(...)  { if (cqDebugFlag & DEBUG_WARN)  { taosPrintLog("CQ  WARN ", 255, __VA_ARGS__); }}
#define cInfo(...)  { if (cqDebugFlag & DEBUG_INFO)  { taosPrintLog("CQ  ", 255, __VA_ARGS__); }}
#define cDebug(...) { if (cqDebugFlag & DEBUG_DEBUG) { taosPrintLog("CQ  ", cqDebugFlag, __VA_ARGS__); }}
#define cTrace(...) { if (cqDebugFlag & DEBUG_TRACE) { taosPrintLog("CQ  ", cqDebugFlag, __VA_ARGS__); }}

typedef struct {
  int32_t  vgId;
  char     user[TSDB_USER_LEN];
  char     pass[TSDB_PASSWORD_LEN];
  char     db[TSDB_DB_NAME_LEN];
  FCqWrite cqWrite;
  void    *ahandle;
  int32_t  num;      // number of continuous streams
  struct SCqObj *pHead;
  void    *dbConn;
  int32_t  master;
  void    *tmrCtrl;
  pthread_mutex_t mutex;
} SCqContext;

typedef struct SCqObj {
  tmr_h          tmrId;
  uint64_t       uid;
  int32_t        tid;      // table ID
  int32_t        rowSize;  // bytes of a row
  char *         sqlStr;   // SQL string
  STSchema *     pSchema;  // pointer to schema array
  void *         pStream;
  struct SCqObj *prev;
  struct SCqObj *next;
  SCqContext *   pContext;
} SCqObj;

static void cqProcessStreamRes(void *param, TAOS_RES *tres, TAOS_ROW row); 
static void cqCreateStream(SCqContext *pContext, SCqObj *pObj);

void *cqOpen(void *ahandle, const SCqCfg *pCfg) {
  SCqContext *pContext = calloc(sizeof(SCqContext), 1);
  if (pContext == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return NULL;
  }

  pContext->tmrCtrl = taosTmrInit(0, 0, 0, "CQ");

  tstrncpy(pContext->user, pCfg->user, sizeof(pContext->user));
  tstrncpy(pContext->pass, pCfg->pass, sizeof(pContext->pass));
  const char* db = pCfg->db;
  for (const char* p = db; *p != 0; p++) {
    if (*p == '.') {
      db = p + 1;
      break;
    }
  }
  tstrncpy(pContext->db, db, sizeof(pContext->db));
  pContext->vgId = pCfg->vgId;
  pContext->cqWrite = pCfg->cqWrite;
  pContext->ahandle = ahandle;
  tscEmbedded = 1;

  pthread_mutex_init(&pContext->mutex, NULL);

  cInfo("vgId:%d, CQ is opened", pContext->vgId);

  return pContext;
}

void cqClose(void *handle) {
  SCqContext *pContext = handle;
  if (handle == NULL) return;

  // stop all CQs
  cqStop(pContext);

  // free all resources
  pthread_mutex_lock(&pContext->mutex);

  SCqObj *pObj = pContext->pHead;
  while (pObj) {
    SCqObj *pTemp = pObj;
    pObj = pObj->next;
    tdFreeSchema(pTemp->pSchema);
    taosTFree(pTemp->sqlStr);
    free(pTemp);
  } 
  
  pthread_mutex_unlock(&pContext->mutex);

  pthread_mutex_destroy(&pContext->mutex);

  taosTmrCleanUp(pContext->tmrCtrl);
  pContext->tmrCtrl = NULL;

  cInfo("vgId:%d, CQ is closed", pContext->vgId);
  free(pContext);
}

void cqStart(void *handle) {
  SCqContext *pContext = handle;
  if (pContext->dbConn || pContext->master) return;

  cInfo("vgId:%d, start all CQs", pContext->vgId);
  pthread_mutex_lock(&pContext->mutex);

  pContext->master = 1;

  SCqObj *pObj = pContext->pHead;
  while (pObj) {
    cqCreateStream(pContext, pObj);
    pObj = pObj->next;
  }

  pthread_mutex_unlock(&pContext->mutex);
}

void cqStop(void *handle) {
  SCqContext *pContext = handle;
  cInfo("vgId:%d, stop all CQs", pContext->vgId);
  if (pContext->dbConn == NULL || pContext->master == 0) return;

  pthread_mutex_lock(&pContext->mutex);

  pContext->master = 0;
  SCqObj *pObj = pContext->pHead;
  while (pObj) {
    if (pObj->pStream) {
      taos_close_stream(pObj->pStream);
      pObj->pStream = NULL;
      cInfo("vgId:%d, id:%d CQ:%s is closed", pContext->vgId, pObj->tid, pObj->sqlStr);
    } else {
      taosTmrStop(pObj->tmrId);
      pObj->tmrId = 0;
    }
    pObj = pObj->next;
  }

  if (pContext->dbConn) taos_close(pContext->dbConn);
  pContext->dbConn = NULL;

  pthread_mutex_unlock(&pContext->mutex);
}

void *cqCreate(void *handle, uint64_t uid, int32_t tid, char *sqlStr, STSchema *pSchema) {
  SCqContext *pContext = handle;

  SCqObj *pObj = calloc(sizeof(SCqObj), 1);
  if (pObj == NULL) return NULL;

  pObj->uid = uid;
  pObj->tid = tid;
  pObj->sqlStr = malloc(strlen(sqlStr)+1);
  strcpy(pObj->sqlStr, sqlStr);

  pObj->pSchema = tdDupSchema(pSchema);
  pObj->rowSize = schemaTLen(pSchema);

  cInfo("vgId:%d, id:%d CQ:%s is created", pContext->vgId, pObj->tid, pObj->sqlStr);

  pthread_mutex_lock(&pContext->mutex);

  pObj->next = pContext->pHead;
  if (pContext->pHead) pContext->pHead->prev = pObj;
  pContext->pHead = pObj;

  cqCreateStream(pContext, pObj);

  pthread_mutex_unlock(&pContext->mutex);

  return pObj;
}

void cqDrop(void *handle) {
  SCqObj *pObj = handle;
  SCqContext *pContext = pObj->pContext;

  pthread_mutex_lock(&pContext->mutex);

  if (pObj->prev) {
    pObj->prev->next = pObj->next;
  } else {
    pContext->pHead = pObj->next;
  }

  if (pObj->next) {
    pObj->next->prev = pObj->prev;
  }

  // free the resources associated
  if (pObj->pStream) {
    taos_close_stream(pObj->pStream);
    pObj->pStream = NULL;
  } else {
    taosTmrStop(pObj->tmrId);
    pObj->tmrId = 0;
  }

  cInfo("vgId:%d, id:%d CQ:%s is dropped", pContext->vgId, pObj->tid, pObj->sqlStr); 
  tdFreeSchema(pObj->pSchema);
  free(pObj->sqlStr);
  free(pObj);

  pthread_mutex_unlock(&pContext->mutex);
}

static void doCreateStream(void *param, TAOS_RES *result, int32_t code) {
  SCqObj* pObj = (SCqObj*)param;
  SCqContext* pContext = pObj->pContext;
  SSqlObj* pSql = (SSqlObj*)result;
  pContext->dbConn = pSql->pTscObj;
  cqCreateStream(pContext, pObj);
}

static void cqProcessCreateTimer(void *param, void *tmrId) {
  SCqObj* pObj = (SCqObj*)param;
  SCqContext* pContext = pObj->pContext;

  if (pContext->dbConn == NULL) {
    cDebug("vgId:%d, try connect to TDengine", pContext->vgId);
    taos_connect_a(NULL, pContext->user, pContext->pass, pContext->db, 0, doCreateStream, param, NULL);
  } else {
    cqCreateStream(pContext, pObj);
  }
}

static void cqCreateStream(SCqContext *pContext, SCqObj *pObj) {
  pObj->pContext = pContext;

  if (pContext->dbConn == NULL) {
    cDebug("vgId:%d, create dbConn after 1000 ms", pContext->vgId);
    pObj->tmrId = taosTmrStart(cqProcessCreateTimer, 1000, pObj, pContext->tmrCtrl);
    return;
  }
  pObj->tmrId = 0;

  pObj->pStream = taos_open_stream(pContext->dbConn, pObj->sqlStr, cqProcessStreamRes, 0, pObj, NULL);
  if (pObj->pStream) {
    pContext->num++;
    cInfo("vgId:%d, id:%d CQ:%s is openned", pContext->vgId, pObj->tid, pObj->sqlStr);
  } else {
    cError("vgId:%d, id:%d CQ:%s, failed to open", pContext->vgId, pObj->tid, pObj->sqlStr);
  }
}

static void cqProcessStreamRes(void *param, TAOS_RES *tres, TAOS_ROW row) {
  SCqObj     *pObj = (SCqObj *)param;
  if (tres == NULL && row == NULL) {
    pObj->pStream = NULL;
    return;
  }
  SCqContext *pContext = pObj->pContext;
  STSchema   *pSchema = pObj->pSchema;
  if (pObj->pStream == NULL) return;

  cDebug("vgId:%d, id:%d CQ:%s stream result is ready", pContext->vgId, pObj->tid, pObj->sqlStr);

  int32_t size = sizeof(SWalHead) + sizeof(SSubmitMsg) + sizeof(SSubmitBlk) + TD_DATA_ROW_HEAD_SIZE + pObj->rowSize;
  char *buffer = calloc(size, 1);

  SWalHead   *pHead = (SWalHead *)buffer;
  SSubmitMsg *pMsg = (SSubmitMsg *) (buffer + sizeof(SWalHead));
  SSubmitBlk *pBlk = (SSubmitBlk *) (buffer + sizeof(SWalHead) + sizeof(SSubmitMsg));

  SDataRow trow = (SDataRow)pBlk->data;
  tdInitDataRow(trow, pSchema);

  for (int32_t i = 0; i < pSchema->numOfCols; i++) {
    STColumn *c = pSchema->columns + i;
    void* val = row[i];
    if (val == NULL) {
      val = getNullValue(c->type);
    } else if (c->type == TSDB_DATA_TYPE_BINARY) {
      val = ((char*)val) - sizeof(VarDataLenT);
    } else if (c->type == TSDB_DATA_TYPE_NCHAR) {
      char buf[TSDB_MAX_NCHAR_LEN];
      size_t len = taos_fetch_lengths(tres)[i];
      taosMbsToUcs4(val, len, buf, sizeof(buf), &len);
      memcpy(val + sizeof(VarDataLenT), buf, len);
      varDataLen(val) = len;
    }
    tdAppendColVal(trow, val, c->type, c->bytes, c->offset);
  }
  pBlk->dataLen = htonl(dataRowLen(trow));
  pBlk->schemaLen = 0;

  pBlk->uid = htobe64(pObj->uid);
  pBlk->tid = htonl(pObj->tid);
  pBlk->numOfRows = htons(1);
  pBlk->sversion = htonl(pSchema->version);
  pBlk->padding = 0;

  pHead->len = sizeof(SSubmitMsg) + sizeof(SSubmitBlk) + dataRowLen(trow);

  pMsg->header.vgId = htonl(pContext->vgId);
  pMsg->header.contLen = htonl(pHead->len);
  pMsg->length = pMsg->header.contLen;
  pMsg->numOfBlocks = htonl(1);

  pHead->msgType = TSDB_MSG_TYPE_SUBMIT;
  pHead->version = 0;

  // write into vnode write queue
  pContext->cqWrite(pContext->ahandle, pHead, TAOS_QTYPE_CQ, NULL);
  free(buffer);
}

