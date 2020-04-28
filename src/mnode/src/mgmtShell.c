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
#include "trpc.h"
#include "tsched.h"
#include "tutil.h"
#include "ttimer.h"
#include "tgrant.h"
#include "tglobal.h"
#include "dnode.h"
#include "mgmtDef.h"
#include "mgmtLog.h"
#include "mgmtAcct.h"
#include "mgmtDb.h"
#include "mgmtDnode.h"
#include "mgmtMnode.h"
#include "mgmtProfile.h"
#include "mgmtSdb.h"
#include "mgmtShell.h"
#include "mgmtTable.h"
#include "mgmtUser.h"
#include "mgmtVgroup.h"

typedef int32_t (*SShowMetaFp)(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
typedef int32_t (*SShowRetrieveFp)(SShowObj *pShow, char *data, int32_t rows, void *pConn);

static int  mgmtShellRetriveAuth(char *user, char *spi, char *encrypt, char *secret, char *ckey);
static bool mgmtCheckMsgReadOnly(SQueuedMsg *pMsg);
static void mgmtProcessMsgFromShell(SRpcMsg *pMsg);
static void mgmtProcessUnSupportMsg(SRpcMsg *rpcMsg);
static void mgmtProcessShowMsg(SQueuedMsg *queuedMsg);
static void mgmtProcessRetrieveMsg(SQueuedMsg *queuedMsg);
static void mgmtProcessHeartBeatMsg(SQueuedMsg *queuedMsg);
static void mgmtProcessConnectMsg(SQueuedMsg *queuedMsg);
static void mgmtProcessUseMsg(SQueuedMsg *queuedMsg);

extern void *tsMgmtTmr;
static void *tsMgmtShellRpc = NULL;
static void *tsMgmtTranQhandle = NULL;
static void (*tsMgmtProcessShellMsgFp[TSDB_MSG_TYPE_MAX])(SQueuedMsg *) = {0};
static SShowMetaFp     tsMgmtShowMetaFp[TSDB_MGMT_TABLE_MAX]     = {0};
static SShowRetrieveFp tsMgmtShowRetrieveFp[TSDB_MGMT_TABLE_MAX] = {0};

int32_t mgmtInitShell() {
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_SHOW, mgmtProcessShowMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_RETRIEVE, mgmtProcessRetrieveMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_HEARTBEAT, mgmtProcessHeartBeatMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_CONNECT, mgmtProcessConnectMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_USE_DB, mgmtProcessUseMsg);
  
  tsMgmtTranQhandle = taosInitScheduler(tsMaxShellConns, 1, "mnodeT");

  int32_t numOfThreads = tsNumOfCores * tsNumOfThreadsPerCore / 4.0;
  if (numOfThreads < 1) {
    numOfThreads = 1;
  }

  SRpcInit rpcInit = {0};
  rpcInit.localPort    = tsMnodeShellPort;
  rpcInit.label        = "MND-shell";
  rpcInit.numOfThreads = numOfThreads;
  rpcInit.cfp          = mgmtProcessMsgFromShell;
  rpcInit.sessions     = tsMaxShellConns;
  rpcInit.connType     = TAOS_CONN_SERVER;
  rpcInit.idleTime     = tsShellActivityTimer * 1000;
  rpcInit.afp          = mgmtShellRetriveAuth;

  tsMgmtShellRpc = rpcOpen(&rpcInit);
  if (tsMgmtShellRpc == NULL) {
    mError("failed to init server connection to shell");
    return -1;
  }

  mPrint("server connection to shell is opened");
  return 0;
}

void mgmtCleanUpShell() {
  if (tsMgmtTranQhandle) {
    taosCleanUpScheduler(tsMgmtTranQhandle);
    tsMgmtTranQhandle = NULL;
  }

  if (tsMgmtShellRpc) {
    rpcClose(tsMgmtShellRpc);
    tsMgmtShellRpc = NULL;
    mPrint("server connection to shell is closed");
  }
}

void mgmtAddShellMsgHandle(uint8_t showType, void (*fp)(SQueuedMsg *queuedMsg)) {
  tsMgmtProcessShellMsgFp[showType] = fp;
}

void mgmtAddShellShowMetaHandle(uint8_t showType, SShowMetaFp fp) {
  tsMgmtShowMetaFp[showType] = fp;
}

void mgmtAddShellShowRetrieveHandle(uint8_t msgType, SShowRetrieveFp fp) {
  tsMgmtShowRetrieveFp[msgType] = fp;
}

void mgmtProcessTranRequest(SSchedMsg *sched) {
  SQueuedMsg *queuedMsg = sched->msg;
  (*tsMgmtProcessShellMsgFp[queuedMsg->msgType])(queuedMsg);
  mgmtFreeQueuedMsg(queuedMsg);
}

void mgmtAddToShellQueue(SQueuedMsg *queuedMsg) {
  SSchedMsg schedMsg;
  schedMsg.msg = queuedMsg;
  schedMsg.fp  = mgmtProcessTranRequest;
  taosScheduleTask(tsMgmtTranQhandle, &schedMsg);
}

static void mgmtDoDealyedAddToShellQueue(void *param, void *tmrId) {
  mgmtAddToShellQueue(param);
}

void mgmtDealyedAddToShellQueue(SQueuedMsg *queuedMsg) {
  void *unUsed = NULL;
  taosTmrReset(mgmtDoDealyedAddToShellQueue, 1000, queuedMsg, tsMgmtTmr, &unUsed);
}

static void mgmtProcessMsgFromShell(SRpcMsg *rpcMsg) {
  if (rpcMsg == NULL || rpcMsg->pCont == NULL) {
    mgmtSendSimpleResp(rpcMsg->handle, TSDB_CODE_INVALID_MSG_LEN);
    return;
  }

  if (!sdbIsMaster()) {
    SRpcConnInfo connInfo;
    rpcGetConnInfo(rpcMsg->handle, &connInfo);
    
    SRpcIpSet ipSet = {0};
    mgmtGetMnodeIpSet(&ipSet);
    mTrace("conn from shell ip:%s user:%s redirect msg, inUse:%d", taosIpStr(connInfo.clientIp), connInfo.user, ipSet.inUse);
    for (int32_t i = 0; i < ipSet.numOfIps; ++i) {
      mTrace("index:%d ip:%s:%d", i, ipSet.fqdn[i], ipSet.port[i]);
    }

    rpcSendRedirectRsp(rpcMsg->handle, &ipSet);
    return;
  }

  if (grantCheck(TSDB_GRANT_TIME) != TSDB_CODE_SUCCESS) {
    mgmtSendSimpleResp(rpcMsg->handle, TSDB_CODE_GRANT_EXPIRED);
    rpcFreeCont(rpcMsg->pCont);
    return;
  }

  if (tsMgmtProcessShellMsgFp[rpcMsg->msgType] == NULL) {
    mgmtProcessUnSupportMsg(rpcMsg);
    rpcFreeCont(rpcMsg->pCont);
    return;
  }

  SQueuedMsg *pMsg = mgmtMallocQueuedMsg(rpcMsg);
  if (pMsg == NULL) {
    mgmtSendSimpleResp(rpcMsg->handle, TSDB_CODE_INVALID_USER);
    rpcFreeCont(rpcMsg->pCont);
    return;
  }
  
  if (mgmtCheckMsgReadOnly(pMsg)) {
    (*tsMgmtProcessShellMsgFp[rpcMsg->msgType])(pMsg);
    mgmtFreeQueuedMsg(pMsg);
  } else {
    if (!pMsg->pUser->writeAuth) {
      mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_NO_RIGHTS);
      mgmtFreeQueuedMsg(pMsg);
    } else {
      mgmtAddToShellQueue(pMsg);
    }
  }
}

char *mgmtGetShowTypeStr(int32_t showType) {
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
    case TSDB_MGMT_TABLE_CONFIGS: return "show configs";
    case TSDB_MGMT_TABLE_CONNS:   return "show connections";
    case TSDB_MGMT_TABLE_SCORES:  return "show scores";
    case TSDB_MGMT_TABLE_GRANTS:  return "show grants";
    case TSDB_MGMT_TABLE_VNODES:  return "show vnodes";
    default:                      return "undefined";
  }
}

static void mgmtProcessShowMsg(SQueuedMsg *pMsg) {
  SCMShowMsg *pShowMsg = pMsg->pCont;
  if (pShowMsg->type >= TSDB_MGMT_TABLE_MAX) {
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_INVALID_MSG_TYPE);
    return;
  }

  if (!tsMgmtShowMetaFp[pShowMsg->type] || !tsMgmtShowRetrieveFp[pShowMsg->type]) {
    mError("show type:%s is not support", mgmtGetShowTypeStr(pShowMsg->type));
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_OPS_NOT_SUPPORT);
    return;
  }

  int32_t size = sizeof(SCMShowRsp) + sizeof(SSchema) * TSDB_MAX_COLUMNS + TSDB_EXTRA_PAYLOAD_SIZE;
  SCMShowRsp *pShowRsp = rpcMallocCont(size);
  if (pShowRsp == NULL) {
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_SERV_OUT_OF_MEMORY);
    return;
  }

  SShowObj *pShow = (SShowObj *) calloc(1, sizeof(SShowObj) + htons(pShowMsg->payloadLen));
  pShow->signature  = pShow;
  pShow->type       = pShowMsg->type;
  pShow->payloadLen = htons(pShowMsg->payloadLen);
  strcpy(pShow->db, pShowMsg->db);
  memcpy(pShow->payload, pShowMsg->payload, pShow->payloadLen);

  mgmtSaveQhandle(pShow);
  pShowRsp->qhandle = htobe64((uint64_t) pShow);

  mTrace("show:%p, type:%s, start to get meta", pShow, mgmtGetShowTypeStr(pShowMsg->type));
  int32_t code = (*tsMgmtShowMetaFp[pShowMsg->type])(&pShowRsp->tableMeta, pShow, pMsg->thandle);
  if (code == 0) {
    SRpcMsg rpcRsp = {
      .handle  = pMsg->thandle,
      .pCont   = pShowRsp,
      .contLen = sizeof(SCMShowRsp) + sizeof(SSchema) * pShow->numOfColumns,
      .code    = code
    };
    rpcSendResponse(&rpcRsp);
  } else {
    mError("show:%p, type:%s, failed to get meta, reason:%s", pShow, mgmtGetShowTypeStr(pShowMsg->type), tstrerror(code));
    mgmtFreeQhandle(pShow);
    SRpcMsg rpcRsp = {
      .handle  = pMsg->thandle,
      .code    = code
    };
    rpcSendResponse(&rpcRsp);
  }
}

static void mgmtProcessRetrieveMsg(SQueuedMsg *pMsg) {
  int32_t rowsToRead = 0;
  int32_t size = 0;
  int32_t rowsRead = 0;
  SRetrieveTableMsg *pRetrieve = pMsg->pCont;
  pRetrieve->qhandle = htobe64(pRetrieve->qhandle);

  /*
   * in case of server restart, apps may hold qhandle created by server before
   * restart, which is actually invalid, therefore, signature check is required.
   */
  if (!mgmtCheckQhandle(pRetrieve->qhandle)) {
    mError("retrieve:%p, qhandle:%p is invalid", pRetrieve, pRetrieve->qhandle);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_INVALID_QHANDLE);
    return;
  }

  SShowObj *pShow = (SShowObj *)pRetrieve->qhandle;
  mTrace("show:%p, type:%s, retrieve data", pShow, mgmtGetShowTypeStr(pShow->type));

  if (!mgmtCheckQhandle(pRetrieve->qhandle)) {
    mError("pShow:%p, query memory is corrupted", pShow);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_MEMORY_CORRUPTED);
    return;
  } else {
    if ((pRetrieve->free & TSDB_QUERY_TYPE_FREE_RESOURCE) != TSDB_QUERY_TYPE_FREE_RESOURCE) {
      rowsToRead = pShow->numOfRows - pShow->numOfReads;
    }

    /* return no more than 100 meters in one round trip */
    if (rowsToRead > 100) rowsToRead = 100;

    /*
     * the actual number of table may be larger than the value of pShow->numOfRows, if a query is
     * issued during a continuous create table operation. Therefore, rowToRead may be less than 0.
     */
    if (rowsToRead < 0) rowsToRead = 0;
    size = pShow->rowSize * rowsToRead;
  }

  size += 100;
  SRetrieveTableRsp *pRsp = rpcMallocCont(size);

  // if free flag is set, client wants to clean the resources
  if ((pRetrieve->free & TSDB_QUERY_TYPE_FREE_RESOURCE) != TSDB_QUERY_TYPE_FREE_RESOURCE)
    rowsRead = (*tsMgmtShowRetrieveFp[pShow->type])(pShow, pRsp->data, rowsToRead, pMsg->thandle);

  if (rowsRead < 0) {  // TSDB_CODE_ACTION_IN_PROGRESS;
    rpcFreeCont(pRsp);
    return;
  }

  pRsp->numOfRows = htonl(rowsRead);
  pRsp->precision = htonl(TSDB_TIME_PRECISION_MILLI);  // millisecond time precision

  SRpcMsg rpcRsp = {
      .handle  = pMsg->thandle,
      .pCont   = pRsp,
      .contLen = size,
      .code    = 0,
      .msgType = 0
  };
  rpcSendResponse(&rpcRsp);

  if (rowsToRead == 0) {
    mgmtFreeQhandle(pShow);
  }
}

static void mgmtProcessHeartBeatMsg(SQueuedMsg *pMsg) {
  SCMHeartBeatRsp *pHBRsp = (SCMHeartBeatRsp *) rpcMallocCont(sizeof(SCMHeartBeatRsp));
  if (pHBRsp == NULL) {
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_SERV_OUT_OF_MEMORY);
    return;
  }

  mgmtGetMnodeIpSet(&pHBRsp->ipList);
  
  /*
   * TODO
   * Dispose kill stream or kill query message
   */
  pHBRsp->queryId = 0;
  pHBRsp->streamId = 0;
  pHBRsp->killConnection = 0;

  SRpcMsg rpcRsp = {
      .handle  = pMsg->thandle,
      .pCont   = pHBRsp,
      .contLen = sizeof(SCMHeartBeatRsp),
      .code    = 0,
      .msgType = 0
  };
  rpcSendResponse(&rpcRsp);
}

static int mgmtShellRetriveAuth(char *user, char *spi, char *encrypt, char *secret, char *ckey) {
  *spi = 1;
  *encrypt = 0;
  *ckey = 0;

  if (!sdbIsMaster()) {
    *secret = 0;
    return TSDB_CODE_SUCCESS;
  }

  SUserObj *pUser = mgmtGetUser(user);
  if (pUser == NULL) {
    *secret = 0;
    return TSDB_CODE_INVALID_USER;
  } else {
    memcpy(secret, pUser->pass, TSDB_KEY_LEN);
    mgmtDecUserRef(pUser);
    return TSDB_CODE_SUCCESS;
  }
}

static void mgmtProcessConnectMsg(SQueuedMsg *pMsg) {
  SRpcMsg rpcRsp = {.handle = pMsg->thandle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
  SCMConnectMsg *pConnectMsg = pMsg->pCont;

  SRpcConnInfo connInfo;
  if (rpcGetConnInfo(pMsg->thandle, &connInfo) != 0) {
    mError("thandle:%p is already released while process connect msg", pMsg->thandle);
    return;
  }

  int32_t code;
  if (grantCheck(TSDB_GRANT_TIME) != TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_GRANT_EXPIRED;
    goto connect_over;
  }

  code = taosCheckVersion(pConnectMsg->clientVersion, version, 3);
  if (code != TSDB_CODE_SUCCESS) {
    goto connect_over;
  }

  SUserObj *pUser = pMsg->pUser;
  SAcctObj *pAcct = pUser->pAcct;

  if (pConnectMsg->db[0]) {
    char dbName[TSDB_TABLE_ID_LEN * 3] = {0};
    sprintf(dbName, "%x%s%s", pAcct->acctId, TS_PATH_DELIMITER, pConnectMsg->db);
    SDbObj *pDb = mgmtGetDb(dbName);
    if (pDb == NULL) {
      code = TSDB_CODE_INVALID_DB;
      goto connect_over;
    }
  }

  SCMConnectRsp *pConnectRsp = rpcMallocCont(sizeof(SCMConnectRsp));
  if (pConnectRsp == NULL) {
    code = TSDB_CODE_SERV_OUT_OF_MEMORY;
    goto connect_over;
  }

  sprintf(pConnectRsp->acctId, "%x", pAcct->acctId);
  strcpy(pConnectRsp->serverVersion, version);
  pConnectRsp->writeAuth = pUser->writeAuth;
  pConnectRsp->superAuth = pUser->superAuth;

  mgmtGetMnodeIpSet(&pConnectRsp->ipList);
  
connect_over:
  rpcRsp.code = code;
  if (code != TSDB_CODE_SUCCESS) {
    mLError("user:%s login from %s, result:%s", connInfo.user, taosIpStr(connInfo.clientIp), tstrerror(code));
  } else {
    mLPrint("user:%s login from %s, result:%s", connInfo.user, taosIpStr(connInfo.clientIp), tstrerror(code));
    rpcRsp.pCont   = pConnectRsp;
    rpcRsp.contLen = sizeof(SCMConnectRsp);
  }
  rpcSendResponse(&rpcRsp);
}

static void mgmtProcessUseMsg(SQueuedMsg *pMsg) {
  SRpcMsg rpcRsp = {.handle = pMsg->thandle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
  
  SCMUseDbMsg *pUseDbMsg = pMsg->pCont;
  
  // todo check for priority of current user
  pMsg->pDb = mgmtGetDb(pUseDbMsg->db);
  
  int32_t code = TSDB_CODE_SUCCESS;
  if (pMsg->pDb == NULL) {
    code = TSDB_CODE_INVALID_DB;
  }
  
  rpcRsp.code = code;
  rpcSendResponse(&rpcRsp);
}

/**
 * check if we need to add mgmtProcessTableMetaMsg into tranQueue, which will be executed one-by-one.
 */
static bool mgmtCheckTableMetaMsgReadOnly(SQueuedMsg *pMsg) {
  SCMTableInfoMsg *pInfo = pMsg->pCont;
  pMsg->pTable = mgmtGetTable(pInfo->tableId);
  if (pMsg->pTable != NULL) return true;

  // If table does not exists and autoCreate flag is set, we add the handler into task queue
  int16_t autoCreate = htons(pInfo->createFlag);
  if (autoCreate == 1) {
    mTrace("table:%s auto created task added", pInfo->tableId);
    return false;
  }
  
  return true;
}

static bool mgmtCheckMsgReadOnly(SQueuedMsg *pMsg) {
  if (pMsg->msgType == TSDB_MSG_TYPE_CM_TABLE_META) {
    return mgmtCheckTableMetaMsgReadOnly(pMsg);
  }

  if (pMsg->msgType == TSDB_MSG_TYPE_CM_STABLE_VGROUP || pMsg->msgType == TSDB_MSG_TYPE_RETRIEVE       ||
      pMsg->msgType == TSDB_MSG_TYPE_CM_SHOW          || pMsg->msgType == TSDB_MSG_TYPE_CM_TABLES_META ||
      pMsg->msgType == TSDB_MSG_TYPE_CM_CONNECT) {
    return true;
  }

  return false;
}

static void mgmtProcessUnSupportMsg(SRpcMsg *rpcMsg) {
  mError("%s is not processed in mnode shell", taosMsg[rpcMsg->msgType]);
  SRpcMsg rpcRsp = {
    .msgType = 0,
    .pCont   = 0,
    .contLen = 0,
    .code    = TSDB_CODE_OPS_NOT_SUPPORT,
    .handle  = rpcMsg->handle
  };
  rpcSendResponse(&rpcRsp);
}

void mgmtSendSimpleResp(void *thandle, int32_t code) {
  SRpcMsg rpcRsp = {
      .msgType = 0,
      .pCont   = 0,
      .contLen = 0,
      .code    = code,
      .handle  = thandle
  };
  rpcSendResponse(&rpcRsp);
}
