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
#include "mndShow.h"

#define SHOW_STEP_SIZE 100

static SShowObj *mndCreateShowObj(SMnode *pMnode, SShowMsg *pMsg);
static void      mndFreeShowObj(SShowObj *pShow);
static SShowObj *mndAcquireShowObj(SMnode *pMnode, int32_t showId);
static void      mndReleaseShowObj(SShowObj *pShow, bool forceRemove);
static int32_t   mndProcessShowMsg(SMnodeMsg *pMnodeMsg);
static int32_t   mndProcessRetrieveMsg(SMnodeMsg *pMsg);
static bool      mndCheckRetrieveFinished(SShowObj *pShow);

int32_t mndInitShow(SMnode *pMnode) {
  SShowMgmt *pMgmt = &pMnode->showMgmt;

  pMgmt->cache = taosCacheInit(TSDB_DATA_TYPE_INT, 5, true, (__cache_free_fn_t)mndFreeShowObj, "show");
  if (pMgmt->cache == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to alloc show cache since %s", terrstr());
    return -1;
  }

  mndSetMsgHandle(pMnode, TDMT_MND_SHOW, mndProcessShowMsg);
  mndSetMsgHandle(pMnode, TDMT_MND_SHOW_RETRIEVE, mndProcessRetrieveMsg);
  return 0;
}

void mndCleanupShow(SMnode *pMnode) {
  SShowMgmt *pMgmt = &pMnode->showMgmt;
  if (pMgmt->cache != NULL) {
    taosCacheCleanup(pMgmt->cache);
    pMgmt->cache = NULL;
  }
}

static SShowObj *mndCreateShowObj(SMnode *pMnode, SShowMsg *pMsg) {
  SShowMgmt *pMgmt = &pMnode->showMgmt;

  int32_t showId = atomic_add_fetch_32(&pMgmt->showId, 1);
  if (showId == 0) atomic_add_fetch_32(&pMgmt->showId, 1);

  int32_t   size = sizeof(SShowObj) + pMsg->payloadLen;
  SShowObj *pShow = calloc(1, size);
  if (pShow != NULL) {
    pShow->id = showId;
    pShow->pMnode = pMnode;
    pShow->type = pMsg->type;
    pShow->payloadLen = pMsg->payloadLen;
    memcpy(pShow->db, pMsg->db, TSDB_DB_FNAME_LEN);
    memcpy(pShow->payload, pMsg->payload, pMsg->payloadLen);
  } else {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to process show-meta msg:%s since %s", mndShowStr(pMsg->type), terrstr());
    return NULL;
  }

  int32_t   keepTime = pMnode->cfg.shellActivityTimer * 6 * 1000;
  SShowObj *pShowRet = taosCachePut(pMgmt->cache, &showId, sizeof(int32_t), pShow, size, keepTime);
  free(pShow);
  if (pShowRet == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("show:%d, failed to put into cache since %s", showId, terrstr());
    return NULL;
  } else {
    mTrace("show:%d, data:%p created", showId, pShowRet);
    return pShowRet;
  }
}

static void mndFreeShowObj(SShowObj *pShow) {
  SMnode    *pMnode = pShow->pMnode;
  SShowMgmt *pMgmt = &pMnode->showMgmt;

  ShowFreeIterFp freeFp = pMgmt->freeIterFps[pShow->type];
  if (freeFp != NULL) {
    if (pShow->pIter != NULL) {
      (*freeFp)(pMnode, pShow->pIter);
    }
  }

  mTrace("show:%d, data:%p destroyed", pShow->id, pShow);
}

static SShowObj *mndAcquireShowObj(SMnode *pMnode, int32_t showId) {
  SShowMgmt *pMgmt = &pMnode->showMgmt;

  SShowObj *pShow = taosCacheAcquireByKey(pMgmt->cache, &showId, sizeof(int32_t));
  if (pShow == NULL) {
    mError("show:%d, already destroyed", showId);
    return NULL;
  }

  mTrace("show:%d, data:%p acquired from cache", pShow->id, pShow);
  return pShow;
}

static void mndReleaseShowObj(SShowObj *pShow, bool forceRemove) {
  if (pShow == NULL) return;
  mTrace("show:%d, data:%p released from cache, force:%d", pShow->id, pShow, forceRemove);
  
  // A bug in tcache.c
  forceRemove = 0;

  SMnode    *pMnode = pShow->pMnode;
  SShowMgmt *pMgmt = &pMnode->showMgmt;
  taosCacheRelease(pMgmt->cache, (void **)(&pShow), forceRemove);
}

static int32_t mndProcessShowMsg(SMnodeMsg *pMnodeMsg) {
  SMnode    *pMnode = pMnodeMsg->pMnode;
  SShowMgmt *pMgmt = &pMnode->showMgmt;
  SShowMsg  *pMsg = pMnodeMsg->rpcMsg.pCont;
  int8_t     type = pMsg->type;
  int16_t    payloadLen = htonl(pMsg->payloadLen);

  if (type <= TSDB_MGMT_TABLE_START || type >= TSDB_MGMT_TABLE_MAX) {
    terrno = TSDB_CODE_MND_INVALID_MSG_TYPE;
    mError("failed to process show msg since %s", terrstr());
    return -1;
  }

  ShowMetaFp metaFp = pMgmt->metaFps[type];
  if (metaFp == NULL) {
    terrno = TSDB_CODE_MND_INVALID_MSG_TYPE;
    mError("failed to process show-meta msg:%s since %s", mndShowStr(type), terrstr());
    return -1;
  }

  SShowObj *pShow = mndCreateShowObj(pMnode, pMsg);
  if (pShow == NULL) {
    mError("failed to process show-meta msg:%s since %s", mndShowStr(type), terrstr());
    return -1;
  }

  int32_t   size = sizeof(SShowRsp) + sizeof(SSchema) * TSDB_MAX_COLUMNS + TSDB_EXTRA_PAYLOAD_SIZE;
  SShowRsp *pRsp = rpcMallocCont(size);
  if (pRsp == NULL) {
    mndReleaseShowObj(pShow, true);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("show:%d, failed to process show-meta msg:%s since malloc rsp error", pShow->id, mndShowStr(type));
    return -1;
  }

  int32_t code = (*metaFp)(pMnodeMsg, pShow, &pRsp->tableMeta);
  mDebug("show:%d, data:%p get meta finished, numOfRows:%d cols:%d type:%s result:%s", pShow->id, pShow,
         pShow->numOfRows, pShow->numOfColumns, mndShowStr(type), tstrerror(code));

  if (code == TSDB_CODE_SUCCESS) {
    pMnodeMsg->contLen = sizeof(SShowRsp) + sizeof(SSchema) * pShow->numOfColumns;
    pMnodeMsg->pCont = pRsp;
    pRsp->showId = htonl(pShow->id);
    mndReleaseShowObj(pShow, false);
    return TSDB_CODE_SUCCESS;
  } else {
    rpcFreeCont(pRsp);
    mndReleaseShowObj(pShow, true);
    return code;
  }
}

static int32_t mndProcessRetrieveMsg(SMnodeMsg *pMnodeMsg) {
  SMnode    *pMnode = pMnodeMsg->pMnode;
  SShowMgmt *pMgmt = &pMnode->showMgmt;
  int32_t    rowsToRead = 0;
  int32_t    size = 0;
  int32_t    rowsRead = 0;

  SRetrieveTableMsg *pRetrieve = pMnodeMsg->rpcMsg.pCont;
  int32_t            showId = htonl(pRetrieve->showId);

  SShowObj *pShow = mndAcquireShowObj(pMnode, showId);
  if (pShow == NULL) {
    terrno = TSDB_CODE_MND_INVALID_SHOWOBJ;
    mError("failed to process show-retrieve msg:%p since %s", pShow, terrstr());
    return -1;
  }

  ShowRetrieveFp retrieveFp = pMgmt->retrieveFps[pShow->type];
  if (retrieveFp == NULL) {
    mndReleaseShowObj(pShow, false);
    terrno = TSDB_CODE_MSG_NOT_PROCESSED;
    mError("show:%d, data:%p failed to retrieve data since %s", pShow->id, pShow, terrstr());
    return -1;
  }

  mDebug("show:%d, data:%p start retrieve data, numOfReads:%d numOfRows:%d type:%s", pShow->id, pShow,
         pShow->numOfReads, pShow->numOfRows, mndShowStr(pShow->type));

  if (mndCheckRetrieveFinished(pShow)) {
    mDebug("show:%d, data:%p read finished, numOfReads:%d numOfRows:%d", pShow->id, pShow, pShow->numOfReads,
           pShow->numOfRows);
    pShow->numOfReads = pShow->numOfRows;
  }

  if ((pRetrieve->free & TSDB_QUERY_TYPE_FREE_RESOURCE) != TSDB_QUERY_TYPE_FREE_RESOURCE) {
    rowsToRead = pShow->numOfRows - pShow->numOfReads;
  }

  /* return no more than 100 tables in one round trip */
  if (rowsToRead > SHOW_STEP_SIZE) rowsToRead = SHOW_STEP_SIZE;

  /*
   * the actual number of table may be larger than the value of pShow->numOfRows, if a query is
   * issued during a continuous create table operation. Therefore, rowToRead may be less than 0.
   */
  if (rowsToRead < 0) rowsToRead = 0;
  size = pShow->rowSize * rowsToRead;

  size += SHOW_STEP_SIZE;
  SRetrieveTableRsp *pRsp = rpcMallocCont(size);
  if (pRsp == NULL) {
    mndReleaseShowObj(pShow, false);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("show:%d, data:%p failed to retrieve data since %s", pShow->id, pShow, terrstr());
    return -1;
  }

  // if free flag is set, client wants to clean the resources
  if ((pRetrieve->free & TSDB_QUERY_TYPE_FREE_RESOURCE) != TSDB_QUERY_TYPE_FREE_RESOURCE) {
    rowsRead = (*retrieveFp)(pMnodeMsg, pShow, pRsp->data, rowsToRead);
  }

  mDebug("show:%d, data:%p stop retrieve data, rowsRead:%d rowsToRead:%d", pShow->id, pShow, rowsRead, rowsToRead);

  pRsp->numOfRows = htonl(rowsRead);
  pRsp->precision = TSDB_TIME_PRECISION_MILLI;  // millisecond time precision

  pMnodeMsg->pCont = pRsp;
  pMnodeMsg->contLen = size;

  if (rowsRead == 0 || rowsToRead == 0 || (rowsRead == rowsToRead && pShow->numOfRows == pShow->numOfReads)) {
    pRsp->completed = 1;
    mDebug("show:%d, data:%p retrieve completed", pShow->id, pShow);
    mndReleaseShowObj(pShow, true);
  } else {
    mDebug("show:%d, data:%p retrieve not completed yet", pShow->id, pShow);
    mndReleaseShowObj(pShow, false);
  }

  return TSDB_CODE_SUCCESS;
}

char *mndShowStr(int32_t showType) {
  switch (showType) {
    case TSDB_MGMT_TABLE_ACCT:
      return "show accounts";
    case TSDB_MGMT_TABLE_USER:
      return "show users";
    case TSDB_MGMT_TABLE_DB:
      return "show databases";
    case TSDB_MGMT_TABLE_TABLE:
      return "show tables";
    case TSDB_MGMT_TABLE_DNODE:
      return "show dnodes";
    case TSDB_MGMT_TABLE_MNODE:
      return "show mnodes";
    case TSDB_MGMT_TABLE_VGROUP:
      return "show vgroups";
    case TSDB_MGMT_TABLE_STB:
      return "show stables";
    case TSDB_MGMT_TABLE_MODULE:
      return "show modules";
    case TSDB_MGMT_TABLE_QUERIES:
      return "show queries";
    case TSDB_MGMT_TABLE_STREAMS:
      return "show streams";
    case TSDB_MGMT_TABLE_VARIABLES:
      return "show configs";
    case TSDB_MGMT_TABLE_CONNS:
      return "show connections";
    case TSDB_MGMT_TABLE_SCORES:
      return "show scores";
    case TSDB_MGMT_TABLE_GRANTS:
      return "show grants";
    case TSDB_MGMT_TABLE_VNODES:
      return "show vnodes";
    case TSDB_MGMT_TABLE_CLUSTER:
      return "show cluster";
    case TSDB_MGMT_TABLE_STREAMTABLES:
      return "show streamtables";
    case TSDB_MGMT_TABLE_TP:
      return "show topics";
    case TSDB_MGMT_TABLE_FUNCTION:
      return "show functions";
    default:
      return "undefined";
  }
}

static bool mndCheckRetrieveFinished(SShowObj *pShow) {
  if (pShow->pIter == NULL && pShow->numOfReads != 0) {
    return true;
  }
  return false;
}

void mndVacuumResult(char *data, int32_t numOfCols, int32_t rows, int32_t capacity, SShowObj *pShow) {
  if (rows < capacity) {
    for (int32_t i = 0; i < numOfCols; ++i) {
      memmove(data + pShow->offset[i] * rows, data + pShow->offset[i] * capacity, pShow->bytes[i] * rows);
    }
  }
}

void mndAddShowMetaHandle(SMnode *pMnode, EShowType showType, ShowMetaFp fp) {
  SShowMgmt *pMgmt = &pMnode->showMgmt;
  pMgmt->metaFps[showType] = fp;
}

void mndAddShowRetrieveHandle(SMnode *pMnode, EShowType showType, ShowRetrieveFp fp) {
  SShowMgmt *pMgmt = &pMnode->showMgmt;
  pMgmt->retrieveFps[showType] = fp;
}

void mndAddShowFreeIterHandle(SMnode *pMnode, EShowType showType, ShowFreeIterFp fp) {
  SShowMgmt *pMgmt = &pMnode->showMgmt;
  pMgmt->freeIterFps[showType] = fp;
}
