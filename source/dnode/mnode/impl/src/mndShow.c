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

static int32_t mndProcessShowMsg(SMnode *pMnode, SMnodeMsg *pMnodeMsg);
static int32_t mndProcessRetrieveMsg(SMnode *pMnode, SMnodeMsg *pMsg);
static bool    mndCheckRetrieveFinished(SShowObj *pShow);
static int32_t mndAcquireShowObj(SMnode *pMnode, SShowObj *pShow);
static void    mndReleaseShowObj(SShowObj *pShow, bool forceRemove);
static int32_t mndPutShowObj(SMnode *pMnode, SShowObj *pShow);
static void    mndFreeShowObj(void *ppShow);
static char   *mndShowStr(int32_t showType);

int32_t mndInitShow(SMnode *pMnode) {
  SShowMgmt *pMgmt = &pMnode->showMgmt;

  pMgmt->cache = taosCacheInit(TSDB_CACHE_PTR_KEY, 5, true, mndFreeShowObj, "show");
  if (pMgmt->cache == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to alloc show cache since %s", terrstr());
    return -1;
  }

  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_SHOW, mndProcessShowMsg);
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_SHOW_RETRIEVE, mndProcessRetrieveMsg);
  return 0;
}

void mndCleanupShow(SMnode *pMnode) {}

static int32_t mndProcessShowMsg(SMnode *pMnode, SMnodeMsg *pMnodeMsg) {
  SShowMgmt *pMgmt = &pMnode->showMgmt;
  SShowMsg  *pMsg = pMnodeMsg->rpcMsg.pCont;
  int8_t     type = pMsg->type;
  uint16_t   payloadLen = htonl(pMsg->payloadLen);

  if (type <= TSDB_MGMT_TABLE_START || type >= TSDB_MGMT_TABLE_MAX) {
    terrno = TSDB_CODE_MND_INVALID_MSG_TYPE;
    mError("failed to process show msg since %s", terrstr());
    return -1;
  }

  ShowMetaFp metaFp = pMgmt->metaFps[type];
  if (metaFp == NULL) {
    terrno = TSDB_CODE_MND_INVALID_MSG_TYPE;
    mError("failed to process show-meta msg:%s since no message handle", mndShowStr(type));
    return -1;
  }

  int32_t   size = sizeof(SShowObj) + payloadLen;
  SShowObj *pShow = calloc(1, size);
  if (pShow != NULL) {
    pShow->pMnode = pMnode;
    pShow->type = type;
    pShow->payloadLen = payloadLen;
    memcpy(pShow->db, pMsg->db, TSDB_FULL_DB_NAME_LEN);
    memcpy(pShow->payload, pMsg->payload, payloadLen);
  } else {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to process show-meta msg:%s since %s", mndShowStr(type), terrstr());
    return -1;
  }

  if (mndPutShowObj(pMnode, pShow) == 0) {
    mError("failed to process show-meta msg:%s since %s", mndShowStr(type), terrstr());
    free(pShow);
    return -1;
  }

  size = sizeof(SShowRsp) + sizeof(SSchema) * TSDB_MAX_COLUMNS + TSDB_EXTRA_PAYLOAD_SIZE;
  SShowRsp *pRsp = rpcMallocCont(size);
  if (pRsp == NULL) {
    mndReleaseShowObj(pShow, true);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("show:%d, failed to process show-meta msg:%s since malloc rsp error", pShow->id, mndShowStr(type));
    return -1;
  }

  pRsp->qhandle = htobe64((uint64_t)pShow);

  int32_t code = (*metaFp)(pMnode, &pRsp->tableMeta, pShow, pMnodeMsg->rpcMsg.handle);
  mDebug("show:%d, type:%s, get meta finished, numOfRows:%d cols:%d result:%s", pShow->id, mndShowStr(type),
         pShow->numOfRows, pShow->numOfColumns, tstrerror(code));

  if (code == TSDB_CODE_SUCCESS) {
    pMnodeMsg->contLen = sizeof(SShowRsp) + sizeof(SSchema) * pShow->numOfColumns;
    pMnodeMsg->pCont = pRsp;
    mndReleaseShowObj(pShow, false);
    return TSDB_CODE_SUCCESS;
  } else {
    rpcFreeCont(pRsp);
    mndReleaseShowObj(pShow, true);
    return code;
  }
}

static int32_t mndProcessRetrieveMsg(SMnode *pMnode, SMnodeMsg *pMnodeMsg) {
  SShowMgmt *pMgmt = &pMnode->showMgmt;
  int32_t    rowsToRead = 0;
  int32_t    size = 0;
  int32_t    rowsRead = 0;

  SRetrieveTableMsg *pRetrieve = pMnodeMsg->rpcMsg.pCont;
  pRetrieve->qhandle = htobe64(pRetrieve->qhandle);
  SShowObj *pShow = (SShowObj *)pRetrieve->qhandle;

  /*
   * in case of server restart, apps may hold qhandle created by server before
   * restart, which is actually invalid, therefore, signature check is required.
   */
  if (mndAcquireShowObj(pMnode, pShow) != 0) {
    terrno = TSDB_CODE_MND_INVALID_SHOWOBJ;
    mError("failed to process show-retrieve msg:%p since %s", pShow, terrstr());
    return -1;
  }

  ShowRetrieveFp retrieveFp = pMgmt->retrieveFps[pShow->type];
  if (retrieveFp == NULL) {
    mndReleaseShowObj(pShow, false);
    terrno = TSDB_CODE_MSG_NOT_PROCESSED;
    mError("show:%d, failed to retrieve data since %s", pShow->id, terrstr());
    return -1;
  }

  mDebug("show:%d, type:%s, start retrieve data, numOfReads:%d numOfRows:%d", pShow->id, mndShowStr(pShow->type),
         pShow->numOfReads, pShow->numOfRows);

  if (mndCheckRetrieveFinished(pShow)) {
    mDebug("show:%d, read finished, numOfReads:%d numOfRows:%d", pShow->id, pShow->numOfReads, pShow->numOfRows);
    pShow->numOfReads = pShow->numOfRows;
  }

  if ((pRetrieve->free & TSDB_QUERY_TYPE_FREE_RESOURCE) != TSDB_QUERY_TYPE_FREE_RESOURCE) {
    rowsToRead = pShow->numOfRows - pShow->numOfReads;
  }

  /* return no more than 100 tables in one round trip */
  if (rowsToRead > 100) rowsToRead = 100;

  /*
   * the actual number of table may be larger than the value of pShow->numOfRows, if a query is
   * issued during a continuous create table operation. Therefore, rowToRead may be less than 0.
   */
  if (rowsToRead < 0) rowsToRead = 0;
  size = pShow->rowSize * rowsToRead;

  size += 100;
  SRetrieveTableRsp *pRsp = rpcMallocCont(size);
  if (pRsp == NULL) {
    mndReleaseShowObj(pShow, false);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("show:%d, failed to retrieve data since %s", pShow->id, terrstr());
    return -1;
  }

  // if free flag is set, client wants to clean the resources
  if ((pRetrieve->free & TSDB_QUERY_TYPE_FREE_RESOURCE) != TSDB_QUERY_TYPE_FREE_RESOURCE) {
    rowsRead = (*retrieveFp)(pMnode, pShow, pRsp->data, rowsToRead, pMnodeMsg->rpcMsg.handle);
  }

  mDebug("show:%d, stop retrieve data, rowsRead:%d rowsToRead:%d", pShow->id, rowsRead, rowsToRead);

  pRsp->numOfRows = htonl(rowsRead);
  pRsp->precision = (int16_t)htonl(TSDB_TIME_PRECISION_MILLI);  // millisecond time precision

  pMnodeMsg->pCont = pRsp;
  pMnodeMsg->contLen = size;

  if (rowsToRead == 0 || (rowsRead == rowsToRead && pShow->numOfRows == pShow->numOfReads)) {
    pRsp->completed = 1;
    mDebug("%p, retrieve completed", pShow);
    mndReleaseShowObj(pShow, true);
  } else {
    mDebug("%p, retrieve not completed yet", pShow);
    mndReleaseShowObj(pShow, false);
  }

  return TSDB_CODE_SUCCESS;
}

static char *mndShowStr(int32_t showType) {
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
    case TSDB_MGMT_TABLE_METRIC:
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
      return "show clusters";
    case TSDB_MGMT_TABLE_STREAMTABLES:
      return "show streamtables";
    case TSDB_MGMT_TABLE_TP:
      return "show topics";
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

static int32_t mndAcquireShowObj(SMnode *pMnode, SShowObj *pShow) {
  TSDB_CACHE_PTR_TYPE handleVal = (TSDB_CACHE_PTR_TYPE)pShow;

  SShowMgmt *pMgmt = &pMnode->showMgmt;
  SShowObj **ppShow = taosCacheAcquireByKey(pMgmt->cache, &handleVal, sizeof(TSDB_CACHE_PTR_TYPE));
  if (ppShow) {
    mTrace("show:%d, data:%p acquired from cache", pShow->id, ppShow);
    return 0;
  }

  return -1;
}

static void mndReleaseShowObj(SShowObj *pShow, bool forceRemove) {
  SMnode    *pMnode = pShow->pMnode;
  SShowMgmt *pMgmt = &pMnode->showMgmt;
  SShowObj **ppShow = (SShowObj **)pShow->ppShow;
  taosCacheRelease(pMgmt->cache, (void **)(&ppShow), forceRemove);
  mDebug("show:%d, data:%p released from cache, force:%d", pShow->id, ppShow, forceRemove);
}

static int32_t mndPutShowObj(SMnode *pMnode, SShowObj *pShow) {
  SShowMgmt *pMgmt = &pMnode->showMgmt;
  int32_t    lifeSpan = pMnode->shellActivityTimer * 6 * 1000;

  TSDB_CACHE_PTR_TYPE val = (TSDB_CACHE_PTR_TYPE)pShow;
  pShow->id = atomic_add_fetch_32(&pMgmt->showId, 1);
  SShowObj **ppShow =
      taosCachePut(pMgmt->cache, &val, sizeof(TSDB_CACHE_PTR_TYPE), &pShow, sizeof(TSDB_CACHE_PTR_TYPE), lifeSpan);
  if (ppShow == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("show:%d, failed to put into cache", pShow->id);
    return -1;
  }

  mTrace("show:%d, data:%p put into cache", pShow->id, ppShow);
  return 0;
}

static void mndFreeShowObj(void *ppShow) {
  SShowObj  *pShow = *(SShowObj **)ppShow;
  SMnode    *pMnode = pShow->pMnode;
  SShowMgmt *pMgmt = &pMnode->showMgmt;

  ShowFreeIterFp freeFp = pMgmt->freeIterFps[pShow->type];
  if (freeFp != NULL) {
    if (pShow->pVgIter != NULL) {
      // only used in 'show vnodes "ep"'
      (*freeFp)(pMnode, pShow->pVgIter);
    }
    if (pShow->pIter != NULL) {
      (*freeFp)(pMnode, pShow->pIter);
    }
  }

  mDebug("show:%d, data:%p destroyed", pShow->id, ppShow);
  tfree(pShow);
}

void mnodeVacuumResult(char *data, int32_t numOfCols, int32_t rows, int32_t capacity, SShowObj *pShow) {
  if (rows < capacity) {
    for (int32_t i = 0; i < numOfCols; ++i) {
      memmove(data + pShow->offset[i] * rows, data + pShow->offset[i] * capacity, pShow->bytes[i] * rows);
    }
  }
}

void mnodeAddShowMetaHandle(SMnode *pMnode, EShowType showType, ShowMetaFp fp) {
  SShowMgmt *pMgmt = &pMnode->showMgmt;
  pMgmt->metaFps[showType] = fp;
}

void mnodeAddShowRetrieveHandle(SMnode *pMnode, EShowType showType, ShowRetrieveFp fp) {
  SShowMgmt *pMgmt = &pMnode->showMgmt;
  pMgmt->retrieveFps[showType] = fp;
}

void mnodeAddShowFreeIterHandle(SMnode *pMnode, EShowType showType, ShowFreeIterFp fp) {
  SShowMgmt *pMgmt = &pMnode->showMgmt;
  pMgmt->freeIterFps[showType] = fp;
}
