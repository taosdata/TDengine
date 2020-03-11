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
#include "mgmtNormalTable.h"
#include "mgmtProfile.h"
#include "mgmtShell.h"
#include "mgmtSuperTable.h"
#include "mgmtTable.h"
#include "mgmtUser.h"
#include "mgmtVgroup.h"

typedef int32_t (*SShowMetaFp)(STableMeta *pMeta, SShowObj *pShow, void *pConn);
typedef int32_t (*SShowRetrieveFp)(SShowObj *pShow, char *data, int32_t rows, void *pConn);

static void mgmtProcessMsgFromShell(SRpcMsg *pMsg);
static void mgmtProcessShowMsg(SRpcMsg *rpcMsg);
static void mgmtProcessRetrieveMsg(SRpcMsg *rpcMsg);
static void mgmtProcessUnSupportMsg(SRpcMsg *rpcMsg);
static int  mgmtShellRetriveAuth(char *user, char *spi, char *encrypt, char *secret, char *ckey);
static bool mgmtCheckMsgReadOnly(int8_t type, void *pCont);
static void mgmtProcessHeartBeatMsg(SRpcMsg *rpcMsg);
static void mgmtProcessConnectMsg(SRpcMsg *rpcMsg);

static void *tsMgmtShellRpc = NULL;
static void *tsMgmtTranQhandle = NULL;
static void (*tsMgmtProcessShellMsgFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *) = {0};
static SShowMetaFp     tsMgmtShowMetaFp[TSDB_MGMT_TABLE_MAX]     = {0};
static SShowRetrieveFp tsMgmtShowRetrieveFp[TSDB_MGMT_TABLE_MAX] = {0};

int32_t mgmtInitShell() {
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_SHOW, mgmtProcessShowMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_RETRIEVE, mgmtProcessRetrieveMsg);

  tsMgmtTranQhandle = taosInitScheduler(tsMaxDnodes + tsMaxShellConns, 1, "mnodeT");

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

  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_HEARTBEAT, mgmtProcessHeartBeatMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_CONNECT, mgmtProcessConnectMsg);

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

void mgmtAddShellMsgHandle(uint8_t showType, void (*fp)(SRpcMsg *rpcMsg)) {
  tsMgmtProcessShellMsgFp[showType] = fp;
}

void mgmtAddShellShowMetaHandle(uint8_t showType, SShowMetaFp fp) {
  tsMgmtShowMetaFp[showType] = fp;
}

void mgmtAddShellShowRetrieveHandle(uint8_t msgType, SShowRetrieveFp fp) {
  tsMgmtShowRetrieveFp[msgType] = fp;
}

void mgmtProcessTranRequest(SSchedMsg *sched) {
  SRpcMsg *rpcMsg = sched->msg;
  (*tsMgmtProcessShellMsgFp[rpcMsg->msgType])(rpcMsg);
  rpcFreeCont(rpcMsg->pCont);
  free(rpcMsg);
}

void mgmtAddToTranRequest(SRpcMsg *rpcMsg) {
  SRpcMsg *queuedRpcMsg = malloc(sizeof(SRpcMsg));
  memcpy(queuedRpcMsg, rpcMsg, sizeof(SRpcMsg));

  SSchedMsg schedMsg;
  schedMsg.msg = queuedRpcMsg;
  schedMsg.fp  = mgmtProcessTranRequest;
  taosScheduleTask(tsMgmtTranQhandle, &schedMsg);
}

static void mgmtProcessMsgFromShell(SRpcMsg *rpcMsg) {
  if (sdbGetRunStatus() != SDB_STATUS_SERVING) {
    mTrace("shell msg is ignored since SDB is not ready");
    SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = TSDB_CODE_NOT_READY, .msgType = 0};
    rpcSendResponse(&rpcRsp);
    rpcFreeCont(rpcMsg->pCont);
    return;
  }

  mTrace("%s is received", taosMsg[rpcMsg->msgType]);
  if (tsMgmtProcessShellMsgFp[rpcMsg->msgType]) {
    if (mgmtCheckMsgReadOnly(rpcMsg->msgType, rpcMsg->pCont)) {
      (*tsMgmtProcessShellMsgFp[rpcMsg->msgType])(rpcMsg);
      rpcFreeCont(rpcMsg->pCont);
    } else {
      mgmtAddToTranRequest(rpcMsg);
    }
  } else {
    mError("%s is not processed", taosMsg[rpcMsg->msgType]);
    mgmtProcessUnSupportMsg(rpcMsg);
    rpcFreeCont(rpcMsg->pCont);
  }
}

static void mgmtProcessShowMsg(SRpcMsg *rpcMsg) {
  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};

  SCMShowMsg *pShowMsg = rpcMsg->pCont;
  if (pShowMsg->type == TSDB_MGMT_TABLE_DNODE || TSDB_MGMT_TABLE_GRANTS || TSDB_MGMT_TABLE_SCORES) {
    if (mgmtCheckRedirect(rpcMsg->handle) != TSDB_CODE_SUCCESS) {
      return;
    }
  }

  int32_t  size = sizeof(SCMShowRsp) + sizeof(SSchema) * TSDB_MAX_COLUMNS + TSDB_EXTRA_PAYLOAD_SIZE;
  SCMShowRsp *pShowRsp = rpcMallocCont(size);
  if (pShowRsp == NULL) {
    rpcRsp.code = TSDB_CODE_SERV_OUT_OF_MEMORY;
    rpcSendResponse(&rpcRsp);
    return;
  }

  int32_t code;
  if (pShowMsg->type >= TSDB_MGMT_TABLE_MAX) {
    code = TSDB_CODE_INVALID_MSG_TYPE;
  } else {
    SShowObj *pShow = (SShowObj *) calloc(1, sizeof(SShowObj) + htons(pShowMsg->payloadLen));
    pShow->signature = pShow;
    pShow->type      = pShowMsg->type;
    strcpy(pShow->db, pShowMsg->db);
    mTrace("pShow:%p is allocated", pShow);

    // set the table name query condition
    pShow->payloadLen = htons(pShowMsg->payloadLen);
    memcpy(pShow->payload, pShowMsg->payload, pShow->payloadLen);

    mgmtSaveQhandle(pShow);
    pShowRsp->qhandle = htobe64((uint64_t) pShow);
    if (tsMgmtShowMetaFp[pShowMsg->type]) {
      code = (*tsMgmtShowMetaFp[pShowMsg->type])(&pShowRsp->tableMeta, pShow, rpcMsg->handle);
      if (code == 0) {
        size = sizeof(SCMShowRsp) + sizeof(SSchema) * pShow->numOfColumns;
      } else {
        mError("pShow:%p, type:%d %s, failed to get Meta, code:%d", pShow, pShowMsg->type,
               taosMsg[(uint8_t) pShowMsg->type], code);
        free(pShow);
      }
    } else {
      code = TSDB_CODE_OPS_NOT_SUPPORT;
    }
  }

  rpcRsp.code = code;
  rpcRsp.pCont = pShowRsp;
  rpcRsp.contLen = size;
  rpcSendResponse(&rpcRsp);
}

static void mgmtProcessRetrieveMsg(SRpcMsg *rpcMsg) {
  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};

  int32_t rowsToRead = 0;
  int32_t size = 0;
  int32_t rowsRead = 0;
  SRetrieveTableMsg *pRetrieve = (SRetrieveTableMsg *) rpcMsg->pCont;
  pRetrieve->qhandle = htobe64(pRetrieve->qhandle);

  /*
   * in case of server restart, apps may hold qhandle created by server before
   * restart, which is actually invalid, therefore, signature check is required.
   */
  if (!mgmtCheckQhandle(pRetrieve->qhandle)) {
    mError("retrieve:%p, qhandle:%p is invalid", pRetrieve, pRetrieve->qhandle);
    rpcRsp.code = TSDB_CODE_INVALID_QHANDLE;
    rpcSendResponse(&rpcRsp);
    return;
  }

  SShowObj *pShow = (SShowObj *)pRetrieve->qhandle;
  if (pShow->signature != (void *)pShow) {
    mError("pShow:%p, signature:%p, query memory is corrupted", pShow, pShow->signature);
    rpcRsp.code = TSDB_CODE_MEMORY_CORRUPTED;
    rpcSendResponse(&rpcRsp);
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
    rowsRead = (*tsMgmtShowRetrieveFp[pShow->type])(pShow, pRsp->data, rowsToRead, rpcMsg->handle);

  if (rowsRead < 0) {
    rowsRead = 0;  // TSDB_CODE_ACTION_IN_PROGRESS;
    rpcFreeCont(pRsp);
    return;
  }

  pRsp->numOfRows = htonl(rowsRead);
  pRsp->precision = htonl(TSDB_TIME_PRECISION_MILLI);  // millisecond time precision

  rpcRsp.pCont = pRsp;
  rpcRsp.contLen = size;
  rpcSendResponse(&rpcRsp);

  if (rowsToRead == 0) {
    mgmtFreeQhandle(pShow);
  }
}

static void mgmtProcessHeartBeatMsg(SRpcMsg *rpcMsg) {
  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
  //SCMHeartBeatMsg *pHBMsg = (SCMHeartBeatMsg *) rpcMsg->pCont;
  //mgmtSaveQueryStreamList(pHBMsg);

  SCMHeartBeatRsp *pHBRsp = (SCMHeartBeatRsp *) rpcMallocCont(sizeof(SCMHeartBeatRsp));
  if (pHBRsp == NULL) {
    rpcRsp.code = TSDB_CODE_SERV_OUT_OF_MEMORY;
    rpcSendResponse(&rpcRsp);
    return;
  }

  SRpcConnInfo connInfo;
  if (rpcGetConnInfo(rpcMsg->handle, &connInfo) != 0) {
    mError("conn:%p is already released while process heart beat msg", rpcMsg->handle);
    return;
  }

  pHBRsp->ipList.inUse = 0;
  pHBRsp->ipList.port = htons(tsMnodeShellPort);
  pHBRsp->ipList.numOfIps = 0;
  if (pSdbPublicIpList != NULL && pSdbIpList != NULL) {
    pHBRsp->ipList.numOfIps = htons(pSdbPublicIpList->numOfIps);
    if (connInfo.serverIp == tsPublicIpInt) {
      for (int i = 0; i < pSdbPublicIpList->numOfIps; ++i) {
        pHBRsp->ipList.ip[i] = htonl(pSdbPublicIpList->ip[i]);
      }
    } else {
      for (int i = 0; i < pSdbIpList->numOfIps; ++i) {
        pHBRsp->ipList.ip[i] = htonl(pSdbIpList->ip[i]);
      }
    }
  }

  /*
   * TODO
   * Dispose kill stream or kill query message
   */
  pHBRsp->queryId = 0;
  pHBRsp->streamId = 0;
  pHBRsp->killConnection = 0;

  rpcRsp.pCont = pHBRsp;
  rpcRsp.contLen = sizeof(SCMHeartBeatRsp);
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

static void mgmtProcessConnectMsg(SRpcMsg *rpcMsg) {
  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
  SCMConnectMsg *pConnectMsg = (SCMConnectMsg *) rpcMsg->pCont;

  SRpcConnInfo connInfo;
  if (rpcGetConnInfo(rpcMsg->handle, &connInfo) != 0) {
    mError("conn:%p is already released while process connect msg", rpcMsg->handle);
    return;
  }

  int32_t code;
  SUserObj *pUser = mgmtGetUser(connInfo.user);
  if (pUser == NULL) {
    code = TSDB_CODE_INVALID_USER;
    goto connect_over;
  }

  if (mgmtCheckExpired()) {
    code = TSDB_CODE_GRANT_EXPIRED;
    goto connect_over;
  }

  SAcctObj *pAcct = mgmtGetAcct(pUser->acct);
  if (pAcct == NULL) {
    code = TSDB_CODE_INVALID_ACCT;
    goto connect_over;
  }

  code = taosCheckVersion(pConnectMsg->clientVersion, version, 3);
  if (code != TSDB_CODE_SUCCESS) {
    goto connect_over;
  }

  if (pConnectMsg->db[0]) {
    char dbName[TSDB_TABLE_ID_LEN] = {0};
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
  pConnectRsp->ipList.inUse = 0;
  pConnectRsp->ipList.port = htons(tsMnodeShellPort);
  pConnectRsp->ipList.numOfIps = 0;
  if (pSdbPublicIpList != NULL && pSdbIpList != NULL) {
    pConnectRsp->ipList.numOfIps = htons(pSdbPublicIpList->numOfIps);
    if (connInfo.serverIp == tsPublicIpInt) {
      for (int i = 0; i < pSdbPublicIpList->numOfIps; ++i) {
        pConnectRsp->ipList.ip[i] = htonl(pSdbPublicIpList->ip[i]);
      }
    } else {
      for (int i = 0; i < pSdbIpList->numOfIps; ++i) {
        pConnectRsp->ipList.ip[i] = htonl(pSdbIpList->ip[i]);
      }
    }
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
       type == TSDB_MSG_TYPE_CM_STABLE_META || type == TSDB_MSG_TYPE_RETRIEVE ||
       type == TSDB_MSG_TYPE_CM_SHOW || type == TSDB_MSG_TYPE_CM_TABLES_META      ||
       type == TSDB_MSG_TYPE_CM_CONNECT) {
    return true;
  }

  return false;
}

static void mgmtProcessUnSupportMsg(SRpcMsg *rpcMsg) {
  SRpcMsg rpcRsp = {
    .msgType = 0,
    .pCont   = 0,
    .contLen = 0,
    .code    = TSDB_CODE_OPS_NOT_SUPPORT,
    .handle  = rpcMsg->handle
  };
  rpcSendResponse(&rpcRsp);
}
