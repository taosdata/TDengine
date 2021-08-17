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


typedef struct SCqObj {
  tmr_h          tmrId;
  int64_t        rid;
  uint64_t       uid;
  int32_t        tid;      // table ID
  int32_t        rowSize;  // bytes of a row
  char *         dstTable;
  char *         sqlStr;   // SQL string
  STSchema *     pSchema;  // pointer to schema array
  void *         pStream;
  struct SCqObj *prev;
  struct SCqObj *next;
  SCqContext *   pContext;
} SCqObj;

static void cqProcessStreamRes(void *param, TAOS_RES *tres, TAOS_ROW row); 
static void cqCreateStream(SCqContext *pContext, SCqObj *pObj);

int32_t    cqObjRef = -1;
int32_t    cqVnodeNum = 0;

void cqRmFromList(SCqObj *pObj) {
  //LOCK in caller

  SCqContext *pContext = pObj->pContext;

  if (pObj->prev) {
    pObj->prev->next = pObj->next;
  } else {
    pContext->pHead = pObj->next;
  }

  if (pObj->next) {
    pObj->next->prev = pObj->prev;
  }

}

static void freeSCqContext(void *handle) {
  if (handle == NULL) {
    return;
  }
  SCqContext *pContext = handle;
  pthread_mutex_destroy(&pContext->mutex);

  taosTmrCleanUp(pContext->tmrCtrl);
  pContext->tmrCtrl = NULL;
  cDebug("vgId:%d, CQ is closed", pContext->vgId);
  free(pContext);
}


void cqFree(void *handle) {
  if (tsEnableStream == 0) {
    return;
  }
  SCqObj *pObj = handle;
  SCqContext *pContext = pObj->pContext;
  int32_t delete = 0;

  pthread_mutex_lock(&pContext->mutex);
  
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
  free(pObj->dstTable);
  free(pObj->sqlStr);
  free(pObj);

  pContext->cqObjNum--;

  if (pContext->cqObjNum <= 0 && pContext->delete) {
    delete = 1;
  }

  pthread_mutex_unlock(&pContext->mutex);

  if (delete) {
    freeSCqContext(pContext);
  }
}


void cqCreateRef() {
  int32_t ref = atomic_load_32(&cqObjRef);
  if (ref == -1) {
    ref = taosOpenRef(4096, cqFree);

    if (atomic_val_compare_exchange_32(&cqObjRef, -1, ref) != -1) {
      taosCloseRef(ref);
    }  
  }
}


void *cqOpen(void *ahandle, const SCqCfg *pCfg) {
  if (tsEnableStream == 0) {
    return NULL;
  }
  SCqContext *pContext = calloc(sizeof(SCqContext), 1);
  if (pContext == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return NULL;
  }

  atomic_add_fetch_32(&cqVnodeNum, 1);
  
  cqCreateRef();

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
  tscEmbedded = 1;

  pthread_mutex_init(&pContext->mutex, NULL);


  cDebug("vgId:%d, CQ is opened", pContext->vgId);

  return pContext;
}


void cqClose(void *handle) {
  if (tsEnableStream == 0) {
    return;
  }
  SCqContext *pContext = handle;
  if (handle == NULL) return;

  pContext->delete = 1;
  int32_t hasCq = 0;
  int32_t existLoop = 0;

  // stop all CQs
  cqStop(pContext);

  int64_t rid = 0;

  while (1) {
    pthread_mutex_lock(&pContext->mutex);

    SCqObj *pObj = pContext->pHead;
    if (pObj) {
      cqRmFromList(pObj);

      rid = pObj->rid;

      hasCq = 1;

      if (pContext->pHead == NULL) {
        existLoop = 1;
      }
    } else {
      pthread_mutex_unlock(&pContext->mutex);
      break;
    }
    
    pthread_mutex_unlock(&pContext->mutex);
    
    taosRemoveRef(cqObjRef, rid);

    if (existLoop) {
      break;
    }
  }

  if (hasCq == 0) {
    freeSCqContext(pContext);
  }

  int32_t remainn = atomic_sub_fetch_32(&cqVnodeNum, 1);
  if (remainn <= 0) {
    int32_t ref = cqObjRef;
    cqObjRef = -1;    
    taosCloseRef(ref);
  }
}

void cqStart(void *handle) {
  if (tsEnableStream == 0) {
    return;
  }
  SCqContext *pContext = handle;
  if (pContext->dbConn || pContext->master) return;

  cDebug("vgId:%d, start all CQs", pContext->vgId);
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
  if (tsEnableStream == 0) {
    return;
  }

  SCqContext *pContext = handle;
  cDebug("vgId:%d, stop all CQs", pContext->vgId);
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

void *cqCreate(void *handle, uint64_t uid, int32_t sid, const char* dstTable, char *sqlStr, STSchema *pSchema, int start) {
  if (tsEnableStream == 0) {
    return NULL;
  }
  SCqContext *pContext = handle;
  int64_t rid = 0;

  pthread_mutex_lock(&pContext->mutex);

  SCqObj *pObj = pContext->pHead;
  while (pObj) {
    if (pObj->uid == uid) {
      rid = pObj->rid;
      pthread_mutex_unlock(&pContext->mutex);
      return (void *)rid;
    }
    
    pObj = pObj->next;
  }
  
  pthread_mutex_unlock(&pContext->mutex);
  
  pObj = calloc(sizeof(SCqObj), 1);
  if (pObj == NULL) return NULL;

  pObj->uid = uid;
  pObj->tid = sid;
  if (dstTable != NULL) {
    pObj->dstTable = strdup(dstTable);
  }
  pObj->sqlStr = strdup(sqlStr);

  pObj->pSchema = tdDupSchema(pSchema);
  pObj->rowSize = schemaTLen(pSchema);

  cInfo("vgId:%d, id:%d CQ:%s is created", pContext->vgId, pObj->tid, pObj->sqlStr);

  pthread_mutex_lock(&pContext->mutex);

  pObj->next = pContext->pHead;
  if (pContext->pHead) pContext->pHead->prev = pObj;
  pContext->pHead = pObj;

  pContext->cqObjNum++;

  pObj->rid = taosAddRef(cqObjRef, pObj);

  if(start && pContext->master) {
    cqCreateStream(pContext, pObj);
  } else {
    pObj->pContext = pContext;
  }

  rid = pObj->rid;

  pthread_mutex_unlock(&pContext->mutex);


  return (void *)rid;
}

void cqDrop(void *handle) {
  if (tsEnableStream == 0) {
    return;
  }

  SCqObj* pObj = (SCqObj*)taosAcquireRef(cqObjRef, (int64_t)handle);
  if (pObj == NULL) {
    return;
  }
  
  SCqContext *pContext = pObj->pContext;
  
  pthread_mutex_lock(&pContext->mutex);

  cqRmFromList(pObj);
  
  // free the resources associated
  if (pObj->pStream) {
    taos_close_stream(pObj->pStream);
    pObj->pStream = NULL;
  } else {
    taosTmrStop(pObj->tmrId);
    pObj->tmrId = 0;
  }

  pthread_mutex_unlock(&pContext->mutex);

  taosRemoveRef(cqObjRef, (int64_t)handle);
  taosReleaseRef(cqObjRef, (int64_t)handle);
}

static void doCreateStream(void *param, TAOS_RES *result, int32_t code) {
  SCqObj* pObj = (SCqObj*)taosAcquireRef(cqObjRef, (int64_t)param);
  if (pObj == NULL) {
    return;
  }

  SCqContext* pContext = pObj->pContext;
  SSqlObj* pSql = (SSqlObj*)result;  
  if (code == TSDB_CODE_SUCCESS) {
    if (atomic_val_compare_exchange_ptr(&(pContext->dbConn), NULL, pSql->pTscObj) != NULL) {
      taos_close(pSql->pTscObj);
    }
  }
  
  pthread_mutex_lock(&pContext->mutex);
  cqCreateStream(pContext, pObj);
  pthread_mutex_unlock(&pContext->mutex);

  taosReleaseRef(cqObjRef, (int64_t)param);
}

static void cqProcessCreateTimer(void *param, void *tmrId) {
  SCqObj* pObj = (SCqObj*)taosAcquireRef(cqObjRef, (int64_t)param);
  if (pObj == NULL) {
    return;
  }
  
  SCqContext* pContext = pObj->pContext;

  if (pContext->dbConn == NULL) {
    cDebug("vgId:%d, try connect to TDengine", pContext->vgId);
    taos_connect_a(NULL, pContext->user, pContext->pass, pContext->db, 0, doCreateStream, param, NULL);
  } else {
    pthread_mutex_lock(&pContext->mutex);
    cqCreateStream(pContext, pObj);
    pthread_mutex_unlock(&pContext->mutex);
  }

  taosReleaseRef(cqObjRef, (int64_t)param);
}

// inner implement in tscStream.c
TAOS_STREAM *taos_open_stream_withname(TAOS *taos, const char* desName, const char *sqlstr, void (*fp)(void *param, TAOS_RES *, TAOS_ROW row),
                              int64_t stime, void *param, void (*callback)(void *), void* cqhandle);

static void cqCreateStream(SCqContext *pContext, SCqObj *pObj) {
  pObj->pContext = pContext;

  if (pContext->dbConn == NULL) {
    cDebug("vgId:%d, create dbConn after 1000 ms", pContext->vgId);
    pObj->tmrId = taosTmrStart(cqProcessCreateTimer, 1000, (void *)pObj->rid, pContext->tmrCtrl);
    return;
  }

  pObj->tmrId = 0;

  if (pObj->pStream == NULL) {
    pObj->pStream = taos_open_stream_withname(pContext->dbConn, pObj->dstTable, pObj->sqlStr, cqProcessStreamRes, \
                                               INT64_MIN, (void *)pObj->rid, NULL, pContext);

    // TODO the pObj->pStream may be released if error happens
    if (pObj->pStream) {
      pContext->num++;
      cDebug("vgId:%d, id:%d CQ:%s is opened", pContext->vgId, pObj->tid, pObj->sqlStr);
    } else {
      cError("vgId:%d, id:%d CQ:%s, failed to open", pContext->vgId, pObj->tid, pObj->sqlStr);
    }
  }
}

static void cqProcessStreamRes(void *param, TAOS_RES *tres, TAOS_ROW row) {
  SCqObj* pObj = (SCqObj*)taosAcquireRef(cqObjRef, (int64_t)param);
  if (pObj == NULL) {
    return;
  }
  
  if (tres == NULL && row == NULL) {
    taos_close_stream(pObj->pStream);

    pObj->pStream = NULL;

    taosReleaseRef(cqObjRef, (int64_t)param);

    return;
  }

  SCqContext *pContext = pObj->pContext;
  STSchema   *pSchema = pObj->pSchema;
  if (pObj->pStream == NULL) {    
    taosReleaseRef(cqObjRef, (int64_t)param);
    return;
  }
  
  cDebug("vgId:%d, id:%d CQ:%s stream result is ready", pContext->vgId, pObj->tid, pObj->sqlStr);

  int32_t size = sizeof(SWalHead) + sizeof(SSubmitMsg) + sizeof(SSubmitBlk) + TD_MEM_ROW_DATA_HEAD_SIZE + pObj->rowSize;
  char *buffer = calloc(size, 1);

  SWalHead   *pHead = (SWalHead *)buffer;
  SSubmitMsg *pMsg = (SSubmitMsg *) (buffer + sizeof(SWalHead));
  SSubmitBlk *pBlk = (SSubmitBlk *) (buffer + sizeof(SWalHead) + sizeof(SSubmitMsg));

  SMemRow trow = (SMemRow)pBlk->data;
  SDataRow dataRow = (SDataRow)memRowDataBody(trow);
  memRowSetType(trow, SMEM_ROW_DATA);
  tdInitDataRow(dataRow, pSchema);

  for (int32_t i = 0; i < pSchema->numOfCols; i++) {
    STColumn *c = pSchema->columns + i;
    void *val = row[i];
    if (val == NULL) {
      val = (void *)getNullValue(c->type);
    } else if (c->type == TSDB_DATA_TYPE_BINARY) {
      val = ((char*)val) - sizeof(VarDataLenT);
    } else if (c->type == TSDB_DATA_TYPE_NCHAR) {
      char buf[TSDB_MAX_NCHAR_LEN];
      int32_t len = taos_fetch_lengths(tres)[i];
      taosMbsToUcs4(val, len, buf, sizeof(buf), &len);
      memcpy((char *)val + sizeof(VarDataLenT), buf, len);
      varDataLen(val) = len;
    }
    tdAppendColVal(dataRow, val, c->type, c->offset);
  }
  pBlk->dataLen = htonl(memRowDataTLen(trow));
  pBlk->schemaLen = 0;

  pBlk->uid = htobe64(pObj->uid);
  pBlk->tid = htonl(pObj->tid);
  pBlk->numOfRows = htons(1);
  pBlk->sversion = htonl(pSchema->version);
  pBlk->padding = 0;

  pHead->len = sizeof(SSubmitMsg) + sizeof(SSubmitBlk) + memRowDataTLen(trow);

  pMsg->header.vgId = htonl(pContext->vgId);
  pMsg->header.contLen = htonl(pHead->len);
  pMsg->length = pMsg->header.contLen;
  pMsg->numOfBlocks = htonl(1);

  pHead->msgType = TSDB_MSG_TYPE_SUBMIT;
  pHead->version = 0;

  // write into vnode write queue
  pContext->cqWrite(pContext->vgId, pHead, TAOS_QTYPE_CQ, NULL);
  free(buffer);
  
  taosReleaseRef(cqObjRef, (int64_t)param);
}

