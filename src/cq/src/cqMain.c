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
#include "taosdef.h"
#include "taosmsg.h"
#include "tlog.h"
#include "twal.h"
#include "tcq.h"
#include "taos.h"

#define cError(...) if (cqDebugFlag & DEBUG_ERROR) {taosPrintLog("ERROR CQ ", cqDebugFlag, __VA_ARGS__);}
#define cWarn(...) if (cqDebugFlag & DEBUG_WARN) {taosPrintLog("WARN CQ ", cqDebugFlag, __VA_ARGS__);}
#define cTrace(...) if (cqDebugFlag & DEBUG_TRACE) {taosPrintLog("CQ ", cqDebugFlag, __VA_ARGS__);}
#define cPrint(...) {taosPrintLog("WAL ", 255, __VA_ARGS__);}

typedef struct {
  int      vgId;
  char     path[TSDB_FILENAME_LEN];
  char     user[TSDB_USER_LEN];
  char     pass[TSDB_PASSWORD_LEN];
  FCqWrite cqWrite;
  void    *ahandle;
  int      num;      // number of continuous streams
  struct SCqObj *pHead;
  void    *dbConn;
  pthread_mutex_t mutex;
} SCqContext;

typedef struct SCqObj {
  int      sid;      // table ID
  int      rowSize;  // bytes of a row 
  char    *sqlStr;   // SQL string
  int      columns;  // number of columns
  SSchema *pSchema;  // pointer to schema array
  void    *pStream;
  struct SCqObj *next; 
  SCqContext *pContext;
} SCqObj;

int cqDebugFlag = 135;

static void cqProcessStreamRes(void *param, TAOS_RES *tres, TAOS_ROW row); 

void *cqOpen(void *ahandle, const SCqCfg *pCfg) {
  
  SCqContext *pContext = calloc(sizeof(SCqContext), 1);
  if (pContext == NULL) return NULL;

  strcpy(pContext->user, pCfg->user);
  strcpy(pContext->pass, pCfg->pass);
  strcpy(pContext->path, pCfg->path);
  pContext->vgId = pCfg->vgId;
  pContext->cqWrite = pCfg->cqWrite;
  pContext->ahandle = ahandle;

  // open meta data file
  
  // loop each record
  while (1) {
    SCqObj *pObj = calloc(sizeof(SCqObj), 1);
    if (pObj == NULL) { 
      cError("vgId:%d, no memory", pContext->vgId);
      continue;
    }

    pObj->next = pContext->pHead;
    pContext->pHead = pObj;

    // assigne each field in SCqObj 
    // pObj->sid = 
    // strcpy(pObj->sqlStr, ?? );
    // schema, columns
  }

  pthread_mutex_init(&pContext->mutex, NULL);

  cTrace("vgId:%d, CQ is opened", pContext->vgId);

  return pContext;
}

void cqClose(void *handle) {
  SCqContext *pContext = handle;

  // stop all CQs
  cqStop(pContext);

  // save the meta data 

  // free all resources
  SCqObj *pObj = pContext->pHead;
  while (pObj) {
    SCqObj *pTemp = pObj;
    pObj = pObj->next;
    free(pTemp);
  } 
  
  pthread_mutex_destroy(&pContext->mutex);

  cTrace("vgId:%d, CQ is closed", pContext->vgId);
  free(pContext);
}

void cqStart(void *handle) {
  SCqContext *pContext = handle;
  cTrace("vgId:%d, start all CQs", pContext->vgId);
  if (pContext->dbConn) return;

  pthread_mutex_lock(&pContext->mutex);

  pContext->dbConn = taos_connect("localhost", pContext->user, pContext->pass, NULL, 0);
  if (pContext->dbConn) {
    cError("vgId:%d, failed to connect to TDengine(%s)", pContext->vgId, tstrerror(terrno));
    pthread_mutex_unlock(&pContext->mutex);
    return;
  }


  SCqObj *pObj = pContext->pHead;
  while (pObj) {
    int64_t lastKey = 0;
    pObj->pStream = taos_open_stream(pContext->dbConn, pObj->sqlStr, cqProcessStreamRes, lastKey, pObj, NULL);
    if (pObj->pStream) {
      pContext->num++;
      cTrace("vgId:%d, id:%d CQ:%s is openned", pContext->vgId, pObj->sid, pObj->sqlStr);
    } else {
      cError("vgId:%d, id:%d CQ:%s, failed to open", pContext->vgId, pObj->sqlStr);
    }
    pObj = pObj->next;
  }

  pthread_mutex_unlock(&pContext->mutex);
}

void cqStop(void *handle) {
  SCqContext *pContext = handle;
  cTrace("vgId:%d, stop all CQs", pContext->vgId);
  if (pContext->dbConn == NULL) return;

  pthread_mutex_lock(&pContext->mutex);

  SCqObj *pObj = pContext->pHead;
  while (pObj) {
    if (pObj->pStream) taos_close_stream(pObj->pStream);
    pObj->pStream = NULL;
    cTrace("vgId:%d, id:%d CQ:%s is closed", pContext->vgId, pObj->sid, pObj->sqlStr);

    pObj = pObj->next;
  }

  if (pContext->dbConn) taos_close(pContext->dbConn);
  pContext->dbConn = NULL;

  pthread_mutex_unlock(&pContext->mutex);
}

void cqCreate(void *handle, int sid, char *sqlStr, SSchema *pSchema, int columns) {
  SCqContext *pContext = handle;

  SCqObj *pObj = calloc(sizeof(SCqObj), 1);
  if (pObj == NULL) return;

  pObj->sid = sid;
  pObj->sqlStr = malloc(strlen(sqlStr)+1);
  strcpy(pObj->sqlStr, sqlStr);

  pObj->columns = columns;

  int size = sizeof(SSchema) * columns;
  pObj->pSchema = malloc(size);
  memcpy(pObj->pSchema, pSchema, size);

  cTrace("vgId:%d, id:%d CQ:%s is created", pContext->vgId, pObj->sid, pObj->sqlStr);

  pthread_mutex_lock(&pContext->mutex);

  pObj->next = pContext->pHead;
  pContext->pHead = pObj;

  if (pContext->dbConn) {
    int64_t lastKey = 0;
    pObj->pStream = taos_open_stream(pContext->dbConn, pObj->sqlStr, cqProcessStreamRes, lastKey, pObj, NULL);
    if (pObj->pStream) {
      pContext->num++;
      cTrace("vgId:%d, id:%d CQ:%s is openned", pContext->vgId, pObj->sid, pObj->sqlStr);
    } else {
      cError("vgId:%d, id:%d CQ:%s, failed to launch", pContext->vgId, pObj->sid, pObj->sqlStr);
    }
  }

  pthread_mutex_unlock(&pContext->mutex);
}

void cqDrop(void *handle, int sid) {
  SCqContext *pContext = handle;

  pthread_mutex_lock(&pContext->mutex);

  // locate the pObj;
  SCqObj *prev = NULL;
  SCqObj *pObj = pContext->pHead;
  while (pObj) {
    if (pObj->sid != sid) {
      prev = pObj;
      pObj = pObj->next;
      continue;
    }

    // remove from the linked list
    if (prev) {
      prev->next = pObj->next;
    } else {
      pContext->pHead = pObj->next;
    } 

    break;
  }

  if (pObj) {
    // update the meta data 

    // free the resources associated
    if (pObj->pStream) taos_close_stream(pObj->pStream);
    pObj->pStream = NULL;

    cTrace("vgId:%d, id:%d CQ:%s is dropped", pContext->vgId, pObj->sid, pObj->sqlStr); 
    free(pObj);
  }

  pthread_mutex_lock(&pContext->mutex);
}

static void cqProcessStreamRes(void *param, TAOS_RES *tres, TAOS_ROW row) {
  SCqObj     *pObj = (SCqObj *)param;
  SCqContext *pContext = pObj->pContext;
  if (pObj->pStream == NULL) return;

  cTrace("vgId:%d, id:%d CQ:%s stream result is ready", pContext->vgId, pObj->sid, pObj->sqlStr);

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
  pBlk->tid = pObj->sid;


  // write into vnode write queue
  pContext->cqWrite(pContext->ahandle, pHead, TAOS_QTYPE_CQ);

}

