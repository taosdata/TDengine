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
#include "tlog.h"
#include "trpc.h"
#include "tstatus.h"
#include "tsched.h"
#include "dnode.h"
#include "mnode.h"
#include "mgmtAcct.h"
#include "mgmtBalance.h"
#include "mgmtChildTable.h"
#include "mgmtDb.h"
#include "mgmtDnode.h"
#include "mgmtGrant.h"
#include "mgmtMnode.h"
#include "mgmtProfile.h"
#include "mgmtSdb.h"
#include "mgmtShell.h"
#include "mgmtSuperTable.h"
#include "mgmtTable.h"
#include "mgmtUser.h"
#include "mgmtVgroup.h"

typedef int32_t (*SShowMetaFp)(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
typedef int32_t (*SShowRetrieveFp)(SShowObj *pShow, char *data, int32_t rows, void *pConn);

static int  mgmtShellRetriveAuth(char *user, char *spi, char *encrypt, char *secret, char *ckey);
static bool mgmtCheckMsgReadOnly(int8_t type, void *pCont);
static void mgmtProcessMsgFromShell(SRpcMsg *pMsg);
static void mgmtProcessUnSupportMsg(SRpcMsg *rpcMsg);
static void mgmtProcessMsgWhileNotReady(SRpcMsg *rpcMsg);
static void mgmtProcessShowMsg(SQueuedMsg *queuedMsg);
static void mgmtProcessRetrieveMsg(SQueuedMsg *queuedMsg);
static void mgmtProcessHeartBeatMsg(SQueuedMsg *queuedMsg);
static void mgmtProcessConnectMsg(SQueuedMsg *queuedMsg);
static void mgmtProcessUseMsg(SQueuedMsg *queuedMsg);

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
  rpcInit.localIp      = tsAnyIp ? "0.0.0.0" : tsPrivateIp;
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
  rpcFreeCont(queuedMsg->pCont);
  free(queuedMsg);
}

void mgmtAddToShellQueue(SQueuedMsg *queuedMsg) {
  SSchedMsg schedMsg;
  schedMsg.msg = queuedMsg;
  schedMsg.fp  = mgmtProcessTranRequest;
  taosScheduleTask(tsMgmtTranQhandle, &schedMsg);
}

static void mgmtProcessMsgFromShell(SRpcMsg *rpcMsg) {
  if (rpcMsg == NULL || rpcMsg->pCont == NULL) {
    return;
  }

  if (!mgmtInServerStatus()) {
    mgmtProcessMsgWhileNotReady(rpcMsg);
    rpcFreeCont(rpcMsg->pCont);
    return;
  }

  if (grantCheck(TSDB_GRANT_TIME) != TSDB_CODE_SUCCESS) {
    mgmtSendSimpleResp(rpcMsg->handle, TSDB_CODE_GRANT_EXPIRED);
    return;
  }

  if (tsMgmtProcessShellMsgFp[rpcMsg->msgType] == NULL) {
    mgmtProcessUnSupportMsg(rpcMsg);
    rpcFreeCont(rpcMsg->pCont);
    return;
  }

  bool usePublicIp = false;
  SUserObj *pUser = mgmtGetUserFromConn(rpcMsg->handle, &usePublicIp);
  if (pUser == NULL) {
    mgmtSendSimpleResp(rpcMsg->handle, TSDB_CODE_INVALID_USER);
    rpcFreeCont(rpcMsg->pCont);
    return;
  }

  if (mgmtCheckMsgReadOnly(rpcMsg->msgType, rpcMsg->pCont)) {
    SQueuedMsg queuedMsg = {0};
    queuedMsg.thandle = rpcMsg->handle;
    queuedMsg.msgType = rpcMsg->msgType;
    queuedMsg.contLen = rpcMsg->contLen;
    queuedMsg.pCont   = rpcMsg->pCont;
    queuedMsg.pUser   = pUser;
    queuedMsg.usePublicIp = usePublicIp;
    (*tsMgmtProcessShellMsgFp[rpcMsg->msgType])(&queuedMsg);
    rpcFreeCont(rpcMsg->pCont);
  } else {
    SQueuedMsg *queuedMsg = calloc(1, sizeof(SQueuedMsg));
    queuedMsg->thandle = rpcMsg->handle;
    queuedMsg->msgType = rpcMsg->msgType;
    queuedMsg->contLen = rpcMsg->contLen;
    queuedMsg->pCont   = rpcMsg->pCont;
    queuedMsg->pUser   = pUser;
    queuedMsg->usePublicIp = usePublicIp;
    mgmtAddToShellQueue(queuedMsg);
  }
}

static void mgmtProcessShowMsg(SQueuedMsg *pMsg) {
  SCMShowMsg *pShowMsg = pMsg->pCont;
  if (pShowMsg->type == TSDB_MGMT_TABLE_DNODE || TSDB_MGMT_TABLE_GRANTS || TSDB_MGMT_TABLE_SCORES) {
    if (mgmtCheckRedirect(pMsg->thandle)) {
      return;
    }
  }

  if (pShowMsg->type >= TSDB_MGMT_TABLE_MAX) {
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_INVALID_MSG_TYPE);
    return;
  }

  if (!tsMgmtShowMetaFp[pShowMsg->type]) {
    mError("show type:%s is not support", taosGetShowTypeStr(pShowMsg->type));
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

  mTrace("show:%p, type:%s, start to get meta", pShow, taosGetShowTypeStr(pShowMsg->type));
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
    mError("show:%p, type:%s, failed to get meta, reason:%s", pShow, taosGetShowTypeStr(pShowMsg->type), tstrerror(code));
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
  mTrace("show:%p, type:%s, retrieve data", pShow, taosGetShowTypeStr(pShow->type));

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
  //SCMHeartBeatMsg *pHBMsg = (SCMHeartBeatMsg *) rpcMsg->pCont;
  //mgmtSaveQueryStreamList(pHBMsg);

  SCMHeartBeatRsp *pHBRsp = (SCMHeartBeatRsp *) rpcMallocCont(sizeof(SCMHeartBeatRsp));
  if (pHBRsp == NULL) {
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_SERV_OUT_OF_MEMORY);
    return;
  }

  SRpcConnInfo connInfo;
  if (rpcGetConnInfo(pMsg->thandle, &connInfo) != 0) {
    mError("conn:%p is already released while process heart beat msg", pMsg->thandle);
    return;
  }

  if (connInfo.serverIp == tsPublicIpInt) {
    mgmtGetMnodePublicIpList(&pHBRsp->ipList);
  } else {
    mgmtGetMnodePrivateIpList(&pHBRsp->ipList);
  }

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
  *spi = 0;
  *encrypt = 0;
  *ckey = 0;

  SUserObj *pUser = mgmtGetUser(user);
  if (pUser == NULL) {
    *secret = 0;
    return TSDB_CODE_INVALID_USER;
  } else {
    memcpy(secret, pUser->pass, TSDB_KEY_LEN);
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
  SUserObj *pUser = mgmtGetUser(connInfo.user);
  if (pUser == NULL) {
    code = TSDB_CODE_INVALID_USER;
    goto connect_over;
  }

  if (grantCheck(TSDB_GRANT_TIME) != TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_GRANT_EXPIRED;
    goto connect_over;
  }

  SAcctObj *pAcct = acctGetAcct(pUser->acct);
  if (pAcct == NULL) {
    code = TSDB_CODE_INVALID_ACCT;
    goto connect_over;
  }

  code = taosCheckVersion(pConnectMsg->clientVersion, version, 3);
  if (code != TSDB_CODE_SUCCESS) {
    goto connect_over;
  }

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

  if (connInfo.serverIp == tsPublicIpInt) {
    mgmtGetMnodePublicIpList(&pConnectRsp->ipList);
  } else {
    mgmtGetMnodePrivateIpList(&pConnectRsp->ipList);
  }

connect_over:
  rpcRsp.code = code;
  if (code != TSDB_CODE_SUCCESS) {
    mLError("user:%s login from %s, code:%d", connInfo.user, taosIpStr(connInfo.clientIp), code);
  } else {
    mLPrint("user:%s login from %s, code:%d", connInfo.user, taosIpStr(connInfo.clientIp), code);
    rpcRsp.pCont   = pConnectRsp;
    rpcRsp.contLen = sizeof(SCMConnectRsp);
  }
  rpcSendResponse(&rpcRsp);
}

static void mgmtProcessUseMsg(SQueuedMsg *pMsg) {
  SRpcMsg rpcRsp = {.handle = pMsg->thandle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
  
  SCMUseDbMsg *pUseDbMsg = pMsg->pCont;
  
  // todo check for priority of current user
  SDbObj* pDbObj = mgmtGetDb(pUseDbMsg->db);
  
  int32_t code = TSDB_CODE_SUCCESS;
  if (pDbObj == NULL) {
    code = TSDB_CODE_INVALID_DB;
  }
  
  rpcRsp.code = code;
  rpcSendResponse(&rpcRsp);
}

/**
 * check if we need to add mgmtProcessTableMetaMsg into tranQueue, which will be executed one-by-one.
 */
static bool mgmtCheckMeterMetaMsgType(void *pMsg) {
  SCMTableInfoMsg *pInfo = (SCMTableInfoMsg *) pMsg;
  int16_t autoCreate = htons(pInfo->createFlag);
  STableInfo *pTable = mgmtGetTable(pInfo->tableId);

  // If table does not exists and autoCreate flag is set, we add the handler into task queue
  bool addIntoTranQueue = (pTable == NULL && autoCreate == 1);
  if (addIntoTranQueue) {
    mTrace("table:%s auto created task added", pInfo->tableId);
  }

  return addIntoTranQueue;
}

static bool mgmtCheckMsgReadOnly(int8_t type, void *pCont) {
  if ((type == TSDB_MSG_TYPE_CM_TABLE_META && (!mgmtCheckMeterMetaMsgType(pCont)))  ||
       type == TSDB_MSG_TYPE_CM_STABLE_VGROUP || type == TSDB_MSG_TYPE_RETRIEVE ||
       type == TSDB_MSG_TYPE_CM_SHOW || type == TSDB_MSG_TYPE_CM_TABLES_META      ||
       type == TSDB_MSG_TYPE_CM_CONNECT) {
    return true;
  }

  return false;
}

static void mgmtProcessUnSupportMsg(SRpcMsg *rpcMsg) {
  mError("%s is not processed in shell", taosMsg[rpcMsg->msgType]);
  SRpcMsg rpcRsp = {
    .msgType = 0,
    .pCont   = 0,
    .contLen = 0,
    .code    = TSDB_CODE_OPS_NOT_SUPPORT,
    .handle  = rpcMsg->handle
  };
  rpcSendResponse(&rpcRsp);
}

static void mgmtProcessMsgWhileNotReady(SRpcMsg *rpcMsg) {
  mTrace("%s is ignored since SDB is not ready", taosMsg[rpcMsg->msgType]);
  SRpcMsg rpcRsp = {
    .msgType = 0,
    .pCont   = 0,
    .contLen = 0,
    .code    = TSDB_CODE_NOT_READY,
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
