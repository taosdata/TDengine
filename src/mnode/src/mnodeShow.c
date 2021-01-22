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
#include "taosmsg.h"
#include "taoserror.h"
#include "tsched.h"
#include "tutil.h"
#include "ttimer.h"
#include "tgrant.h"
#include "tglobal.h"
#include "tcache.h"
#include "mnode.h"
#include "dnode.h"
#include "mnodeDef.h"
#include "mnodeInt.h"
#include "mnodeAcct.h"
#include "mnodeDb.h"
#include "mnodeDnode.h"
#include "mnodeMnode.h"
#include "mnodeProfile.h"
#include "mnodeSdb.h"
#include "mnodeShow.h"
#include "mnodeTable.h"
#include "mnodeUser.h"
#include "mnodeVgroup.h"
#include "mnodeWrite.h"
#include "mnodeRead.h"

static int32_t mnodeProcessShowMsg(SMnodeMsg *mnodeMsg);
static int32_t mnodeProcessRetrieveMsg(SMnodeMsg *mnodeMsg);
static int32_t mnodeProcessHeartBeatMsg(SMnodeMsg *mnodeMsg);
static int32_t mnodeProcessConnectMsg(SMnodeMsg *mnodeMsg);
static int32_t mnodeProcessUseMsg(SMnodeMsg *mnodeMsg);

static void  mnodeFreeShowObj(void *data);
static bool  mnodeAccquireShowObj(SShowObj *pShow);
static bool  mnodeCheckShowFinished(SShowObj *pShow);
static void *mnodePutShowObj(SShowObj *pShow);
static void  mnodeReleaseShowObj(SShowObj *pShow, bool forceRemove);

static void *tsMnodeShowCache = NULL;
static int32_t tsShowObjIndex = 0;
static SShowMetaFp     tsMnodeShowMetaFp[TSDB_MGMT_TABLE_MAX]     = {0};
static SShowRetrieveFp tsMnodeShowRetrieveFp[TSDB_MGMT_TABLE_MAX] = {0};
static SShowFreeIterFp tsMnodeShowFreeIterFp[TSDB_MGMT_TABLE_MAX] = {0};

int32_t mnodeInitShow() {
  mnodeAddReadMsgHandle(TSDB_MSG_TYPE_CM_SHOW, mnodeProcessShowMsg);
  mnodeAddReadMsgHandle(TSDB_MSG_TYPE_CM_RETRIEVE, mnodeProcessRetrieveMsg);
  mnodeAddReadMsgHandle(TSDB_MSG_TYPE_CM_HEARTBEAT, mnodeProcessHeartBeatMsg);
  mnodeAddReadMsgHandle(TSDB_MSG_TYPE_CM_CONNECT, mnodeProcessConnectMsg);
  mnodeAddReadMsgHandle(TSDB_MSG_TYPE_CM_USE_DB, mnodeProcessUseMsg);
  
  tsMnodeShowCache = taosCacheInit(TSDB_CACHE_PTR_KEY, 5, true, mnodeFreeShowObj, "show");
  return 0;
}

void mnodeCleanUpShow() {
  if (tsMnodeShowCache != NULL) {
    mInfo("show cache is cleanup");
    taosCacheCleanup(tsMnodeShowCache);
    tsMnodeShowCache = NULL;
  }
}

void mnodeAddShowMetaHandle(uint8_t showType, SShowMetaFp fp) {
  tsMnodeShowMetaFp[showType] = fp;
}

void mnodeAddShowRetrieveHandle(uint8_t msgType, SShowRetrieveFp fp) {
  tsMnodeShowRetrieveFp[msgType] = fp;
}

void mnodeAddShowFreeIterHandle(uint8_t msgType, SShowFreeIterFp fp) {
  tsMnodeShowFreeIterFp[msgType] = fp;
}

static char *mnodeGetShowType(int32_t showType) {
  switch (showType) {
    case TSDB_MGMT_TABLE_ACCT:    return "show accounts";
    case TSDB_MGMT_TABLE_USER:    return "show users";
    case TSDB_MGMT_TABLE_DB:      return "show databases";
    case TSDB_MGMT_TABLE_TABLE:   return "show tables";
    case TSDB_MGMT_TABLE_DNODE:   return "show dnodes";
    case TSDB_MGMT_TABLE_MNODE:   return "show mnodes";
    case TSDB_MGMT_TABLE_VGROUP:  return "show vgroups";
    case TSDB_MGMT_TABLE_METRIC:  return "show stables";
    case TSDB_MGMT_TABLE_MODULE:  return "show modules";
    case TSDB_MGMT_TABLE_QUERIES: return "show queries";
    case TSDB_MGMT_TABLE_STREAMS: return "show streams";
    case TSDB_MGMT_TABLE_VARIABLES: return "show configs";
    case TSDB_MGMT_TABLE_CONNS:   return "show connections";
    case TSDB_MGMT_TABLE_SCORES:  return "show scores";
    case TSDB_MGMT_TABLE_GRANTS:  return "show grants";
    case TSDB_MGMT_TABLE_VNODES:  return "show vnodes";
    case TSDB_MGMT_TABLE_CLUSTER: return "show clusters";
    case TSDB_MGMT_TABLE_STREAMTABLES : return "show streamtables";
    default:                      return "undefined";
  }
}

static int32_t mnodeProcessShowMsg(SMnodeMsg *pMsg) {
  SShowMsg *pShowMsg = pMsg->rpcMsg.pCont;
  if (pShowMsg->type >= TSDB_MGMT_TABLE_MAX) {
    return TSDB_CODE_MND_INVALID_MSG_TYPE;
  }

  if (!tsMnodeShowMetaFp[pShowMsg->type] || !tsMnodeShowRetrieveFp[pShowMsg->type]) {
    mError("show type:%s is not support", mnodeGetShowType(pShowMsg->type));
    return TSDB_CODE_COM_OPS_NOT_SUPPORT;
  }

  int32_t showObjSize = sizeof(SShowObj) + htons(pShowMsg->payloadLen);
  SShowObj *pShow = calloc(1, showObjSize);
  pShow->type       = pShowMsg->type;
  pShow->payloadLen = htons(pShowMsg->payloadLen);
  tstrncpy(pShow->db, pShowMsg->db, TSDB_DB_NAME_LEN);
  memcpy(pShow->payload, pShowMsg->payload, pShow->payloadLen);

  pShow = mnodePutShowObj(pShow);
  if (pShow == NULL) {    
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  }

  int32_t size = sizeof(SShowRsp) + sizeof(SSchema) * TSDB_MAX_COLUMNS + TSDB_EXTRA_PAYLOAD_SIZE;
  SShowRsp *pShowRsp = rpcMallocCont(size);
  if (pShowRsp == NULL) {
    mnodeReleaseShowObj(pShow, true);
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  }
  pShowRsp->qhandle = htobe64((uint64_t) pShow);

  int32_t code = (*tsMnodeShowMetaFp[pShowMsg->type])(&pShowRsp->tableMeta, pShow, pMsg->rpcMsg.handle);
  mDebug("%p, show type:%s index:%d, get meta finished, numOfRows:%d cols:%d result:%s", pShow,
         mnodeGetShowType(pShowMsg->type), pShow->index, pShow->numOfRows, pShow->numOfColumns, tstrerror(code));

  if (code == TSDB_CODE_SUCCESS) {
    pMsg->rpcRsp.rsp = pShowRsp;
    pMsg->rpcRsp.len = sizeof(SShowRsp) + sizeof(SSchema) * pShow->numOfColumns;
    mnodeReleaseShowObj(pShow, false);
    return TSDB_CODE_SUCCESS;
  } else {
    rpcFreeCont(pShowRsp);
    mnodeReleaseShowObj(pShow, true);
    return code;
  }
}

static int32_t mnodeProcessRetrieveMsg(SMnodeMsg *pMsg) {
  int32_t rowsToRead = 0;
  int32_t size = 0;
  int32_t rowsRead = 0;
  SRetrieveTableMsg *pRetrieve = pMsg->rpcMsg.pCont;
  pRetrieve->qhandle = htobe64(pRetrieve->qhandle);

  SShowObj *pShow = (SShowObj *)pRetrieve->qhandle;
  
  /*
   * in case of server restart, apps may hold qhandle created by server before
   * restart, which is actually invalid, therefore, signature check is required.
   */
  if (!mnodeAccquireShowObj(pShow)) {
    mError("%p, show is invalid", pShow);
    return TSDB_CODE_MND_INVALID_SHOWOBJ;
  }

  mDebug("%p, show type:%s index:%d, start retrieve data, numOfReads:%d numOfRows:%d", pShow,
         mnodeGetShowType(pShow->type), pShow->index, pShow->numOfReads, pShow->numOfRows);

  if (mnodeCheckShowFinished(pShow)) {
    mDebug("%p, show is already read finished, numOfReads:%d numOfRows:%d", pShow, pShow->numOfReads, pShow->numOfRows);
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

  // if free flag is set, client wants to clean the resources
  if ((pRetrieve->free & TSDB_QUERY_TYPE_FREE_RESOURCE) != TSDB_QUERY_TYPE_FREE_RESOURCE)
    rowsRead = (*tsMnodeShowRetrieveFp[pShow->type])(pShow, pRsp->data, rowsToRead, pMsg->rpcMsg.handle);

  mDebug("%p, show type:%s index:%d, stop retrieve data, rowsRead:%d rowsToRead:%d", pShow,
         mnodeGetShowType(pShow->type), pShow->index, rowsRead, rowsToRead);

  if (rowsRead < 0) {
    rpcFreeCont(pRsp);
    mnodeReleaseShowObj(pShow, false);
    assert(false);
    return TSDB_CODE_MND_ACTION_IN_PROGRESS;
  }

  pRsp->numOfRows = htonl(rowsRead);
  pRsp->precision = (int16_t)htonl(TSDB_TIME_PRECISION_MILLI);  // millisecond time precision

  pMsg->rpcRsp.rsp = pRsp;
  pMsg->rpcRsp.len = size;

  if (rowsToRead == 0 || (rowsRead == rowsToRead && pShow->numOfRows == pShow->numOfReads)) {
    pRsp->completed = 1;
    mDebug("%p, retrieve completed", pShow);
    mnodeReleaseShowObj(pShow, true);
  } else {
    mDebug("%p, retrieve not completed yet", pShow);
    mnodeReleaseShowObj(pShow, false);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeProcessHeartBeatMsg(SMnodeMsg *pMsg) {
  SHeartBeatRsp *pRsp = (SHeartBeatRsp *)rpcMallocCont(sizeof(SHeartBeatRsp));
  if (pRsp == NULL) {
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  }

  SHeartBeatMsg *pHBMsg = pMsg->rpcMsg.pCont;
  if (taosCheckVersion(pHBMsg->clientVer, version, 3) != TSDB_CODE_SUCCESS) {
    rpcFreeCont(pRsp);
    return TSDB_CODE_TSC_INVALID_VERSION;  // todo change the error code
  }

  SRpcConnInfo connInfo = {0};
  rpcGetConnInfo(pMsg->rpcMsg.handle, &connInfo);
    
  int32_t connId = htonl(pHBMsg->connId);
  SConnObj *pConn = mnodeAccquireConn(connId, connInfo.user, connInfo.clientIp, connInfo.clientPort);
  if (pConn == NULL) {
    pHBMsg->pid = htonl(pHBMsg->pid);
    pConn = mnodeCreateConn(connInfo.user, connInfo.clientIp, connInfo.clientPort, pHBMsg->pid, pHBMsg->appName);
  }

  if (pConn == NULL) {
    // do not close existing links, otherwise
    // mError("failed to create connId, close connect");
    // pRsp->killConnection = 1;
  } else {
    pRsp->connId = htonl(pConn->connId);
    mnodeSaveQueryStreamList(pConn, pHBMsg);
    
    if (pConn->killed != 0) {
      pRsp->killConnection = 1;
    }

    if (pConn->streamId != 0) {
      pRsp->streamId = htonl(pConn->streamId);
      pConn->streamId = 0;
    }

    if (pConn->queryId != 0) {
      pRsp->queryId = htonl(pConn->queryId);
      pConn->queryId = 0;
    }
  }

  pRsp->onlineDnodes = htonl(mnodeGetOnlineDnodesNum());
  pRsp->totalDnodes = htonl(mnodeGetDnodesNum());
  mnodeGetMnodeEpSetForShell(&pRsp->epSet, false);

  pMsg->rpcRsp.rsp = pRsp;
  pMsg->rpcRsp.len = sizeof(SHeartBeatRsp);

  mnodeReleaseConn(pConn);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeProcessConnectMsg(SMnodeMsg *pMsg) {
  SConnectMsg *pConnectMsg = pMsg->rpcMsg.pCont;
  SConnectRsp *pConnectRsp = NULL;
  int32_t code = TSDB_CODE_SUCCESS;

  SRpcConnInfo connInfo = {0};
  if (rpcGetConnInfo(pMsg->rpcMsg.handle, &connInfo) != 0) {
    mError("thandle:%p is already released while process connect msg", pMsg->rpcMsg.handle);
    code = TSDB_CODE_MND_INVALID_CONNECTION;
    goto connect_over;
  }

  code = taosCheckVersion(pConnectMsg->clientVersion, version, 3);
  if (code != TSDB_CODE_SUCCESS) {
    goto connect_over;
  }

  SUserObj *pUser = pMsg->pUser;
  SAcctObj *pAcct = pUser->pAcct;

  if (pConnectMsg->db[0]) {
    char dbName[TSDB_TABLE_FNAME_LEN * 3] = {0};
    sprintf(dbName, "%x%s%s", pAcct->acctId, TS_PATH_DELIMITER, pConnectMsg->db);
    SDbObj *pDb = mnodeGetDb(dbName);
    if (pDb == NULL) {
      code = TSDB_CODE_MND_INVALID_DB;
      goto connect_over;
    }
    
    if (pDb->status != TSDB_DB_STATUS_READY) {
      mError("db:%s, status:%d, in dropping", pDb->name, pDb->status);
      code = TSDB_CODE_MND_DB_IN_DROPPING;
      mnodeDecDbRef(pDb);
      goto connect_over;
    }
    mnodeDecDbRef(pDb);
  }

  pConnectRsp = rpcMallocCont(sizeof(SConnectRsp));
  if (pConnectRsp == NULL) {
    code = TSDB_CODE_MND_OUT_OF_MEMORY;
    goto connect_over;
  }

  pConnectMsg->pid = htonl(pConnectMsg->pid);
  SConnObj *pConn = mnodeCreateConn(connInfo.user, connInfo.clientIp, connInfo.clientPort, pConnectMsg->pid, pConnectMsg->appName);
  if (pConn == NULL) {
    code = terrno;
  } else {
    pConnectRsp->connId = htonl(pConn->connId);
    mnodeReleaseConn(pConn);
  }

  sprintf(pConnectRsp->acctId, "%x", pAcct->acctId);
  memcpy(pConnectRsp->serverVersion, version, TSDB_VERSION_LEN);
  pConnectRsp->writeAuth = pUser->writeAuth;
  pConnectRsp->superAuth = pUser->superAuth;
  
  mnodeGetMnodeEpSetForShell(&pConnectRsp->epSet, false);

  dnodeGetClusterId(pConnectRsp->clusterId);

connect_over:
  if (code != TSDB_CODE_SUCCESS) {
    if (pConnectRsp) rpcFreeCont(pConnectRsp);
    mLError("user:%s login from %s, result:%s", connInfo.user, taosIpStr(connInfo.clientIp), tstrerror(code));
  } else {
    mLInfo("user:%s login from %s, result:%s", connInfo.user, taosIpStr(connInfo.clientIp), tstrerror(code));
    pMsg->rpcRsp.rsp = pConnectRsp;
    pMsg->rpcRsp.len = sizeof(SConnectRsp);
  }

  return code;
}

static int32_t mnodeProcessUseMsg(SMnodeMsg *pMsg) {
  SUseDbMsg *pUseDbMsg = pMsg->rpcMsg.pCont;

  int32_t code = TSDB_CODE_SUCCESS;
  if (pMsg->pDb == NULL) pMsg->pDb = mnodeGetDb(pUseDbMsg->db);
  if (pMsg->pDb == NULL) {
    return TSDB_CODE_MND_INVALID_DB;
  }
  
  if (pMsg->pDb->status != TSDB_DB_STATUS_READY) {
    mError("db:%s, status:%d, in dropping", pMsg->pDb->name, pMsg->pDb->status);
    return TSDB_CODE_MND_DB_IN_DROPPING;
  }

  return code;
}

static bool mnodeCheckShowFinished(SShowObj *pShow) {
  if (pShow->pIter == NULL && pShow->numOfReads != 0) {
    return true;
  } 
  return false;
}

static bool mnodeAccquireShowObj(SShowObj *pShow) {
  TSDB_CACHE_PTR_TYPE handleVal = (TSDB_CACHE_PTR_TYPE)pShow;
  SShowObj **ppShow = taosCacheAcquireByKey(tsMnodeShowCache, &handleVal, sizeof(TSDB_CACHE_PTR_TYPE));
  if (ppShow) {
    mDebug("%p, show is accquired from cache, data:%p, index:%d", pShow, ppShow, pShow->index);
    return true;
  }

  return false;
}

static void* mnodePutShowObj(SShowObj *pShow) {
  const int32_t DEFAULT_SHOWHANDLE_LIFE_SPAN = tsShellActivityTimer * 6 * 1000;

  if (tsMnodeShowCache != NULL) {
    pShow->index = atomic_add_fetch_32(&tsShowObjIndex, 1);
    TSDB_CACHE_PTR_TYPE handleVal = (TSDB_CACHE_PTR_TYPE)pShow;
    SShowObj **ppShow = taosCachePut(tsMnodeShowCache, &handleVal, sizeof(TSDB_CACHE_PTR_TYPE), &pShow, sizeof(TSDB_CACHE_PTR_TYPE), DEFAULT_SHOWHANDLE_LIFE_SPAN);
    pShow->ppShow = (void**)ppShow;
    mDebug("%p, show is put into cache, data:%p index:%d", pShow, ppShow, pShow->index);
    return pShow;
  }

  return NULL;
}

static void mnodeFreeShowObj(void *data) {
  SShowObj *pShow = *(SShowObj **)data;
  if (tsMnodeShowFreeIterFp[pShow->type] != NULL && pShow->pIter != NULL) {
    (*tsMnodeShowFreeIterFp[pShow->type])(pShow->pIter);
  }

  mDebug("%p, show is destroyed, data:%p index:%d", pShow, data, pShow->index);
  tfree(pShow);
}

static void mnodeReleaseShowObj(SShowObj *pShow, bool forceRemove) {
  SShowObj **ppShow = (SShowObj **)pShow->ppShow;
  mDebug("%p, show is released, force:%s data:%p index:%d", pShow, forceRemove ? "true" : "false", ppShow,
         pShow->index);

  taosCacheRelease(tsMnodeShowCache, (void **)(&ppShow), forceRemove);
}

void mnodeVacuumResult(char *data, int32_t numOfCols, int32_t rows, int32_t capacity, SShowObj *pShow) {
  if (rows < capacity) {
    for (int32_t i = 0; i < numOfCols; ++i) {
      memmove(data + pShow->offset[i] * rows, data + pShow->offset[i] * capacity, pShow->bytes[i] * rows);
    }
  }
}
