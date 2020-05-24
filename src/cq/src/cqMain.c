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

#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>
#include "taosdef.h"
#include "taosmsg.h"
#include "tglobal.h"
#include "tlog.h"
#include "twal.h"
#include "tcq.h"
#include "taos.h"

#define cError(...) if (cqDebugFlag & DEBUG_ERROR) {taosPrintLog("ERROR CQ  ", cqDebugFlag, __VA_ARGS__);}
#define cWarn(...) if (cqDebugFlag & DEBUG_WARN) {taosPrintLog("WARN CQ  ", cqDebugFlag, __VA_ARGS__);}
#define cTrace(...) if (cqDebugFlag & DEBUG_TRACE) {taosPrintLog("CQ  ", cqDebugFlag, __VA_ARGS__);}
#define cPrint(...) {taosPrintLog("CQ  ", 255, __VA_ARGS__);}

typedef struct {
  int      vgId;
  char     user[TSDB_USER_LEN];
  char     pass[TSDB_PASSWORD_LEN];
  FCqWrite cqWrite;
  void    *ahandle;
  int      num;      // number of continuous streams
  struct SCqObj *pHead;
  void    *dbConn;
  int      master;
  pthread_mutex_t mutex;
} SCqContext;

typedef struct SCqObj {
  int      tid;      // table ID
  int      rowSize;  // bytes of a row 
  char    *sqlStr;   // SQL string
  int      columns;  // number of columns
  SSchema *pSchema;  // pointer to schema array
  void    *pStream;
  struct SCqObj *prev; 
  struct SCqObj *next; 
  SCqContext *pContext;
} SCqObj;

int cqDebugFlag = 135;

static void cqProcessStreamRes(void *param, TAOS_RES *tres, TAOS_ROW row); 
static void cqCreateStream(SCqContext *pContext, SCqObj *pObj);

void *cqOpen(void *ahandle, const SCqCfg *pCfg) {
  
  SCqContext *pContext = calloc(sizeof(SCqContext), 1);
  if (pContext == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return NULL;
  }

  strcpy(pContext->user, pCfg->user);
  strcpy(pContext->pass, pCfg->pass);
  pContext->vgId = pCfg->vgId;
  pContext->cqWrite = pCfg->cqWrite;
  pContext->ahandle = ahandle;
  tscEmbedded = 1;

  pthread_mutex_init(&pContext->mutex, NULL);

  cTrace("vgId:%d, CQ is opened", pContext->vgId);

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
    free(pTemp);
  } 
  
  pthread_mutex_unlock(&pContext->mutex);

  pthread_mutex_destroy(&pContext->mutex);

  cTrace("vgId:%d, CQ is closed", pContext->vgId);
  free(pContext);
}

void cqStart(void *handle) {
  SCqContext *pContext = handle;
  if (pContext->dbConn || pContext->master) return;

  cTrace("vgId:%d, start all CQs", pContext->vgId);
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
  cTrace("vgId:%d, stop all CQs", pContext->vgId);
  if (pContext->dbConn == NULL || pContext->master == 0) return;

  pthread_mutex_lock(&pContext->mutex);

  pContext->master = 0;
  SCqObj *pObj = pContext->pHead;
  while (pObj) {
    if (pObj->pStream) {
      taos_close_stream(pObj->pStream);
      pObj->pStream = NULL;
      cTrace("vgId:%d, id:%d CQ:%s is closed", pContext->vgId, pObj->tid, pObj->sqlStr);
    }

    pObj = pObj->next;
  }

  if (pContext->dbConn) taos_close(pContext->dbConn);
  pContext->dbConn = NULL;

  pthread_mutex_unlock(&pContext->mutex);
}

void *cqCreate(void *handle, int tid, char *sqlStr, SSchema *pSchema, int columns) {
  SCqContext *pContext = handle;

  SCqObj *pObj = calloc(sizeof(SCqObj), 1);
  if (pObj == NULL) return NULL;

  pObj->tid = tid;
  pObj->sqlStr = malloc(strlen(sqlStr)+1);
  strcpy(pObj->sqlStr, sqlStr);

  pObj->columns = columns;

  int size = sizeof(SSchema) * columns;
  pObj->pSchema = malloc(size);
  memcpy(pObj->pSchema, pSchema, size);

  cTrace("vgId:%d, id:%d CQ:%s is created", pContext->vgId, pObj->tid, pObj->sqlStr);

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
  if (pObj->pStream) taos_close_stream(pObj->pStream);
  pObj->pStream = NULL;

  cTrace("vgId:%d, id:%d CQ:%s is dropped", pContext->vgId, pObj->tid, pObj->sqlStr); 
  free(pObj);

  pthread_mutex_lock(&pContext->mutex);
}

static void cqCreateStream(SCqContext *pContext, SCqObj *pObj) {

  if (pContext->dbConn == NULL) {
    pContext->dbConn = taos_connect("localhost", pContext->user, pContext->pass, NULL, 0);
    if (pContext->dbConn == NULL) {
      cError("vgId:%d, failed to connect to TDengine(%s)", pContext->vgId, tstrerror(terrno));
    }
    return;
  }

  int64_t lastKey = 0;
  pObj->pStream = taos_open_stream(pContext->dbConn, pObj->sqlStr, cqProcessStreamRes, lastKey, pObj, NULL);
  if (pObj->pStream) {
    pContext->num++;
    cTrace("vgId:%d, id:%d CQ:%s is openned", pContext->vgId, pObj->tid, pObj->sqlStr);
  } else {
    cError("vgId:%d, id:%d CQ:%s, failed to open", pContext->vgId, pObj->tid, pObj->sqlStr);
  }
}

static void cqProcessStreamRes(void *param, TAOS_RES *tres, TAOS_ROW row) {
  SCqObj     *pObj = (SCqObj *)param;
  SCqContext *pContext = pObj->pContext;
  if (pObj->pStream == NULL) return;

  cTrace("vgId:%d, id:%d CQ:%s stream result is ready", pContext->vgId, pObj->tid, pObj->sqlStr);

  // construct data
  int size = sizeof(SWalHead) + sizeof(SSubmitMsg) + sizeof(SSubmitBlk) + pObj->rowSize;
  char *buffer = calloc(size, 1);

  SWalHead   *pHead = (SWalHead *)buffer;
  pHead->msgType = TSDB_MSG_TYPE_SUBMIT;
  pHead->len = size - sizeof(SWalHead);
  
  SSubmitMsg *pSubmit = (SSubmitMsg *) (buffer + sizeof(SWalHead));
  // to do: fill in the SSubmitMsg structure
  pSubmit->numOfBlocks = 1;


  SSubmitBlk *pBlk = (SSubmitBlk *) (buffer + sizeof(SWalHead) + sizeof(SSubmitMsg));
  // to do: fill in the SSubmitBlk strucuture
  pBlk->tid = pObj->tid;


  // write into vnode write queue
  pContext->cqWrite(pContext->ahandle, pHead, TAOS_QTYPE_CQ);
}

