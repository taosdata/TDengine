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
#include "dnodeSystem.h"
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
static int  mgmtShellRetriveAuth(char *user, char *spi, char *encrypt, char *secret, char *ckey);

static void *tsMgmtShellRpc = NULL;
static void (*tsMgmtProcessShellMsgFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *) = {0};
static SShowMetaFp     tsMgmtShowMetaFp[TSDB_MGMT_TABLE_MAX]     = {0};
static SShowRetrieveFp tsMgmtShowRetrieveFp[TSDB_MGMT_TABLE_MAX] = {0};

int32_t mgmtInitShell() {
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_SHOW, mgmtProcessShowMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_RETRIEVE, mgmtProcessRetrieveMsg);

  int32_t numOfThreads = tsNumOfCores * tsNumOfThreadsPerCore / 4.0;
  if (numOfThreads < 1) {
    numOfThreads = 1;
  }

  SRpcInit rpcInit = {0};
  rpcInit.localIp      = tsAnyIp ? "0.0.0.0" : tsPrivateIp;
  rpcInit.localPort    = tsMgmtShellPort;
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
  if (tsMgmtShellRpc) {
    rpcClose(tsMgmtShellRpc);
    tsMgmtShellRpc = NULL;
    mPrint("server connection to shell is closed");
  }
}

void mgmtAddShellHandle(uint8_t showType, void (*fp)(SRpcMsg *rpcMsg)) {
  tsMgmtProcessShellMsgFp[showType] = fp;
}

void mgmtAddShellShowMetaHandle(uint8_t showType, SShowMetaFp fp) {
  tsMgmtShowMetaFp[showType] = fp;
}

void mgmtAddShellShowRetrieveHandle(uint8_t msgType, SShowRetrieveFp fp) {
  tsMgmtShowRetrieveFp[msgType] = fp;
}

static void mgmtProcessMsgFromShell(SRpcMsg *rpcMsg) {
  if (tsMgmtProcessShellMsgFp[rpcMsg->msgType]) {
    (*tsMgmtProcessShellMsgFp[rpcMsg->msgType])(rpcMsg);
  } else {
    mError("%s is not processed", taosMsg[rpcMsg->msgType]);
  }

  rpcFreeCont(rpcMsg->pCont);
}


//static void mgmtInitShowMsgFp();
//static void mgmtInitProcessShellMsg();
//static void mgmtProcessMsgFromShell(SRpcMsg *msg);
//static void (*mgmtProcessShellMsg[TSDB_MSG_TYPE_MAX])(SRpcMsg *msg);
//static void mgmtProcessUnSupportMsg(SRpcMsg *msg);
//
//void *tsMgmtShellRpc = NULL;
//
//void mgmtProcessTranRequest(SSchedMsg *sched) {
//  SRpcMsg rpcMsg;
//  rpcMsg.msgType = *(int8_t *) (sched->msg);
//  rpcMsg.contLen = *(int32_t *) (sched->msg + sizeof(int8_t));
//  rpcMsg.pCont   = sched->msg + sizeof(int32_t) + sizeof(int8_t);
//  rpcMsg.handle  = sched->thandle;
//  rpcMsg.code    = TSDB_CODE_SUCCESS;
//
//  (*mgmtProcessShellMsg[rpcMsg.msgType])(&rpcMsg);
//  if (sched->msg) {
//    free(sched->msg);
//  }
//}
//
//void mgmtAddToTranRequest(SRpcMsg *rpcMsg) {
//  SSchedMsg schedMsg;
//  schedMsg.msg     = malloc(rpcMsg->contLen + sizeof(int32_t) + sizeof(int8_t));
//  schedMsg.fp      = mgmtProcessTranRequest;
//  schedMsg.tfp     = NULL;
//  schedMsg.thandle = rpcMsg->handle;
//  *(int8_t *) (schedMsg.msg) = rpcMsg->msgType;
//  *(int32_t *) (schedMsg.msg + sizeof(int8_t)) = rpcMsg->contLen;
//  memcpy(schedMsg.msg + sizeof(int32_t) + sizeof(int8_t), rpcMsg->pCont, rpcMsg->contLen);
//
//  taosScheduleTask(tsMgmtTranQhandle, &schedMsg);
//}
//

//
//void mgmtProcessTableMetaMsg(SRpcMsg *rpcMsg) {
//  SRpcMsg rpcRsp;
//  rpcRsp.handle  = rpcMsg->handle;
//  rpcRsp.pCont   = NULL;
//  rpcRsp.contLen = 0;
//
//  STableInfoMsg *pInfo = rpcMsg->pCont;
//  pInfo->createFlag = htons(pInfo->createFlag);
//
//  SUserObj *pUser = mgmtGetUserFromConn(rpcMsg->handle);
//  if (pUser == NULL) {
//    mError("table:%s, failed to get table meta, invalid user", pInfo->tableId);
//    rpcRsp.code = TSDB_CODE_INVALID_USER;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  STableInfo *pTable = mgmtGetTable(pInfo->tableId);
//  if (pTable == NULL) {
//    if (pInfo->createFlag != 1) {
//      mError("table:%s, failed to get table meta, table not exist", pInfo->tableId);
//      rpcRsp.code = TSDB_CODE_INVALID_TABLE;
//      rpcSendResponse(&rpcRsp);
//      return;
//    } else {
//      // on demand create table from super table if table does not exists
//      if (mgmtCheckRedirect(rpcMsg->handle) != TSDB_CODE_SUCCESS) {
//        mError("table:%s, failed to create table while get meta info, need redirect message", pInfo->tableId);
//        return;
//      }
//
//      int32_t contLen = sizeof(SCreateTableMsg) + sizeof(STagData);
//      SCreateTableMsg *pCreateMsg = rpcMallocCont(contLen);
//      if (pCreateMsg == NULL) {
//        mError("table:%s, failed to create table while get meta info, no enough memory", pInfo->tableId);
//        rpcRsp.code = TSDB_CODE_SERV_OUT_OF_MEMORY;
//        rpcSendResponse(&rpcRsp);
//        return;
//      }
//
//      memcpy(pCreateMsg->schema, pInfo->tags, sizeof(STagData));
//      strcpy(pCreateMsg->tableId, pInfo->tableId);
//
//      mError("table:%s, start to create table while get meta info", pInfo->tableId);
//      mgmtCreateTable(pCreateMsg, contLen, rpcMsg->handle, true);
//    }
//  } else {
//    mgmtProcessGetTableMeta(pTable, rpcMsg->handle);
//  }
//}
//
//void mgmtProcessMultiTableMetaMsg(SRpcMsg *rpcMsg) {
//  SRpcMsg rpcRsp;
//  rpcRsp.handle  = rpcMsg->handle;
//  rpcRsp.pCont   = NULL;
//  rpcRsp.contLen = 0;
//
//  SRpcConnInfo connInfo;
//  rpcGetConnInfo(rpcMsg->handle, &connInfo);
//
//  bool usePublicIp = (connInfo.serverIp == tsPublicIpInt);
//  SUserObj *pUser = mgmtGetUser(connInfo.user);
//  if (pUser == NULL) {
//    rpcRsp.code = TSDB_CODE_INVALID_USER;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  SMultiTableInfoMsg *pInfo = rpcMsg->pCont;
//  pInfo->numOfTables = htonl(pInfo->numOfTables);
//
//  int32_t totalMallocLen = 4*1024*1024; // first malloc 4 MB, subsequent reallocation as twice
//  SMultiTableMeta *pMultiMeta = rpcMallocCont(totalMallocLen);
//  if (pMultiMeta == NULL) {
//    rpcRsp.code = TSDB_CODE_SERV_OUT_OF_MEMORY;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  pMultiMeta->contLen = sizeof(SMultiTableMeta);
//  pMultiMeta->numOfTables = 0;
//
//  for (int t = 0; t < pInfo->numOfTables; ++t) {
//    char *tableId = (char*)(pInfo->tableIds + t * TSDB_TABLE_ID_LEN);
//    STableInfo *pTable = mgmtGetTable(tableId);
//    if (pTable == NULL) continue;
//
//    SDbObj *pDb = mgmtGetDbByTableId(tableId);
//    if (pDb == NULL) continue;
//
//    int availLen = totalMallocLen - pMultiMeta->contLen;
//    if (availLen <= sizeof(STableMeta) + sizeof(SSchema) * TSDB_MAX_COLUMNS) {
//      //TODO realloc
//      //totalMallocLen *= 2;
//      //pMultiMeta = rpcReMalloc(pMultiMeta, totalMallocLen);
//      //if (pMultiMeta == NULL) {
//      ///  rpcSendResponse(ahandle, TSDB_CODE_SERV_OUT_OF_MEMORY, NULL, 0);
//      //  return TSDB_CODE_SERV_OUT_OF_MEMORY;
//      //} else {
//      //  t--;
//      //  continue;
//      //}
//    }
//
//    STableMeta *pMeta = (STableMeta *)(pMultiMeta->metas + pMultiMeta->contLen);
//    int32_t code = mgmtGetTableMeta(pDb, pTable, pMeta, usePublicIp);
//    if (code == TSDB_CODE_SUCCESS) {
//      pMultiMeta->numOfTables ++;
//      pMultiMeta->contLen += pMeta->contLen;
//    }
//  }
//
//  rpcRsp.pCont = pMultiMeta;
//  rpcRsp.contLen = pMultiMeta->contLen;
//  rpcSendResponse(&rpcRsp);
//}
//
//void mgmtProcessSuperTableMetaMsg(SRpcMsg *rpcMsg) {
//  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
//  SSuperTableInfoMsg *pInfo = rpcMsg->pCont;
//  STableInfo *pTable = mgmtGetSuperTable(pInfo->tableId);
//  if (pTable == NULL) {
//    rpcRsp.code = TSDB_CODE_INVALID_TABLE;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  SSuperTableInfoRsp *pRsp = mgmtGetSuperTableVgroup((SSuperTableObj *) pTable);
//  if (pRsp != NULL) {
//    int32_t msgLen = sizeof(SSuperTableObj) + htonl(pRsp->numOfDnodes) * sizeof(int32_t);
//    rpcRsp.pCont = pRsp;
//    rpcRsp.contLen = msgLen;
//    rpcSendResponse(&rpcRsp);
//  } else {
//    rpcRsp.code = TSDB_CODE_INVALID_TABLE;
//    rpcSendResponse(&rpcRsp);
//  }
//}
//
//void mgmtProcessCreateDbMsg(SRpcMsg *rpcMsg) {
//  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
//
//  if (mgmtCheckRedirect(rpcMsg->handle) != TSDB_CODE_SUCCESS) {
//    return;
//  }
//
//  SUserObj *pUser = mgmtGetUserFromConn(rpcMsg->handle);
//  if (pUser == NULL) {
//    rpcRsp.code = TSDB_CODE_INVALID_USER;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  SCreateDbMsg *pCreate = (SCreateDbMsg *) rpcMsg->pCont;
//
//  pCreate->maxSessions     = htonl(pCreate->maxSessions);
//  pCreate->cacheBlockSize  = htonl(pCreate->cacheBlockSize);
//  pCreate->daysPerFile     = htonl(pCreate->daysPerFile);
//  pCreate->daysToKeep      = htonl(pCreate->daysToKeep);
//  pCreate->daysToKeep1     = htonl(pCreate->daysToKeep1);
//  pCreate->daysToKeep2     = htonl(pCreate->daysToKeep2);
//  pCreate->commitTime      = htonl(pCreate->commitTime);
//  pCreate->blocksPerTable  = htons(pCreate->blocksPerTable);
//  pCreate->rowsInFileBlock = htonl(pCreate->rowsInFileBlock);
//  // pCreate->cacheNumOfBlocks = htonl(pCreate->cacheNumOfBlocks);
//
//  int32_t code;
//  if (mgmtCheckExpired()) {
//    code = TSDB_CODE_GRANT_EXPIRED;
//  } else if (!pUser->writeAuth) {
//    code = TSDB_CODE_NO_RIGHTS;
//  } else {
//    code = mgmtCreateDb(pUser->pAcct, pCreate);
//    if (code == TSDB_CODE_SUCCESS) {
//      mLPrint("DB:%s is created by %s", pCreate->db, pUser->user);
//    }
//  }
//
//  rpcRsp.code = code;
//  rpcSendResponse(&rpcRsp);
//}
//
//void mgmtProcessAlterDbMsg(SRpcMsg *rpcMsg) {
//  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
//  if (mgmtCheckRedirect(rpcMsg->handle) != TSDB_CODE_SUCCESS) {
//    return;
//  }
//
//  SUserObj *pUser = mgmtGetUserFromConn(rpcMsg->handle);
//  if (pUser == NULL) {
//    rpcRsp.code = TSDB_CODE_INVALID_USER;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  SAlterDbMsg *pAlter = (SAlterDbMsg *) rpcMsg->pCont;
//  pAlter->daysPerFile = htonl(pAlter->daysPerFile);
//  pAlter->daysToKeep  = htonl(pAlter->daysToKeep);
//  pAlter->maxSessions = htonl(pAlter->maxSessions) + 1;
//
//  int32_t code;
//  if (!pUser->writeAuth) {
//    code = TSDB_CODE_NO_RIGHTS;
//  } else {
//    code = mgmtAlterDb(pUser->pAcct, pAlter);
//    if (code == TSDB_CODE_SUCCESS) {
//      mLPrint("DB:%s is altered by %s", pAlter->db, pUser->user);
//    }
//  }
//
//  rpcRsp.code = code;
//  rpcSendResponse(&rpcRsp);
//}
//
//void mgmtProcessKillQueryMsg(SRpcMsg *rpcMsg) {
//  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
//  if (mgmtCheckRedirect(rpcMsg->handle) != TSDB_CODE_SUCCESS) {
//    return;
//  }
//
//  SUserObj *pUser = mgmtGetUserFromConn(rpcMsg->handle);
//  if (pUser == NULL) {
//    rpcRsp.code = TSDB_CODE_INVALID_USER;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  SKillQueryMsg *pKill = (SKillQueryMsg *) rpcMsg->pCont;
//  int32_t code;
//
//  if (!pUser->writeAuth) {
//    code = TSDB_CODE_NO_RIGHTS;
//  } else {
//    code = mgmtKillQuery(pKill->queryId, rpcMsg->handle);
//  }
//
//  rpcRsp.code = code;
//  rpcSendResponse(&rpcRsp);
//}
//
//void mgmtProcessKillStreamMsg(SRpcMsg *rpcMsg) {
//  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
//  if (mgmtCheckRedirect(rpcMsg->handle) != TSDB_CODE_SUCCESS) {
//    return;
//  }
//
//  SUserObj *pUser = mgmtGetUserFromConn(rpcMsg->handle);
//  if (pUser == NULL) {
//    rpcRsp.code = TSDB_CODE_INVALID_USER;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  SKillStreamMsg *pKill = (SKillStreamMsg *) rpcMsg->pCont;
//  int32_t code;
//
//  if (!pUser->writeAuth) {
//    code = TSDB_CODE_NO_RIGHTS;
//  } else {
//    code = mgmtKillStream(pKill->queryId, rpcMsg->handle);
//  }
//
//  rpcRsp.code = code;
//  rpcSendResponse(&rpcRsp);
//}
//
//void mgmtProcessKillConnectionMsg(SRpcMsg *rpcMsg) {
//  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
//  if (mgmtCheckRedirect(rpcMsg->handle) != TSDB_CODE_SUCCESS) {
//    return;
//  }
//
//  SUserObj *pUser = mgmtGetUserFromConn(rpcMsg->handle);
//  if (pUser == NULL) {
//    rpcRsp.code = TSDB_CODE_INVALID_USER;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  SKillConnectionMsg *pKill = (SKillConnectionMsg *) rpcMsg->pCont;
//  int32_t code;
//
//  if (!pUser->writeAuth) {
//    code = TSDB_CODE_NO_RIGHTS;
//  } else {
//    code = mgmtKillConnection(pKill->queryId, rpcMsg->handle);
//  }
//
//  rpcRsp.code = code;
//  rpcSendResponse(&rpcRsp);
//}
//

//void mgmtProcessDropDbMsg(SRpcMsg *rpcMsg) {
//  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
//  if (mgmtCheckRedirect(rpcMsg->handle) != TSDB_CODE_SUCCESS) {
//    return ;
//  }
//
//  SUserObj *pUser = mgmtGetUserFromConn(rpcMsg->handle);
//  if (pUser == NULL) {
//    rpcRsp.code = TSDB_CODE_INVALID_USER;
//    rpcSendResponse(&rpcRsp);
//    return ;
//  }
//
//  int32_t code;
//  if (pUser->superAuth) {
//    SDropDbMsg *pDrop = rpcMsg->pCont;
//    code = mgmtDropDbByName(pUser->pAcct, pDrop->db, pDrop->ignoreNotExists);
//    if (code == TSDB_CODE_SUCCESS) {
//      mLPrint("DB:%s is dropped by %s", pDrop->db, pUser->user);
//    }
//  } else {
//    code = TSDB_CODE_NO_RIGHTS;
//  }
//
//  rpcRsp.code = code;
//  rpcSendResponse(&rpcRsp);
//}
//
//static void mgmtInitShowMsgFp() {
//  tsMgmtShowMetaFp[TSDB_MGMT_TABLE_ACCT]    = mgmtGetAcctMeta;
//  tsMgmtShowMetaFp[]    = ;
//  tsMgmtShowMetaFp[TSDB_MGMT_TABLE_DB]      = mgmtGetDbMeta;
//  tsMgmtShowMetaFp[TSDB_MGMT_TABLE_TABLE]   = mgmtGetShowTableMeta;
//  tsMgmtShowMetaFp[TSDB_MGMT_TABLE_DNODE]   = mgmtGetDnodeMeta;
//  tsMgmtShowMetaFp[TSDB_MGMT_TABLE_MNODE]   = mgmtGetMnodeMeta;
//  tsMgmtShowMetaFp[TSDB_MGMT_TABLE_VGROUP]  = mgmtGetVgroupMeta;
//  tsMgmtShowMetaFp[TSDB_MGMT_TABLE_METRIC]  = mgmtGetShowSuperTableMeta;
//  tsMgmtShowMetaFp[TSDB_MGMT_TABLE_MODULE]  = mgmtGetModuleMeta;
//  tsMgmtShowMetaFp[TSDB_MGMT_TABLE_QUERIES] = mgmtGetQueryMeta;
//  tsMgmtShowMetaFp[TSDB_MGMT_TABLE_STREAMS] = mgmtGetStreamMeta;
//  tsMgmtShowMetaFp[TSDB_MGMT_TABLE_CONFIGS] = mgmtGetConfigMeta;
//  tsMgmtShowMetaFp[TSDB_MGMT_TABLE_CONNS]   = mgmtGetConnsMeta;
//  tsMgmtShowMetaFp[TSDB_MGMT_TABLE_SCORES]  = mgmtGetScoresMeta;
//  tsMgmtShowMetaFp[TSDB_MGMT_TABLE_GRANTS]  = mgmtGetGrantsMeta;
//  tsMgmtShowMetaFp[TSDB_MGMT_TABLE_VNODES]  = mgmtGetVnodeMeta;
//
//  tsMgmtShowRetrieveFp[TSDB_MGMT_TABLE_ACCT]    = mgmtRetrieveAccts;
//  tsMgmtShowRetrieveFp[]    = ;
//  tsMgmtShowRetrieveFp[TSDB_MGMT_TABLE_DB]      = mgmtRetrieveDbs;
//  tsMgmtShowRetrieveFp[TSDB_MGMT_TABLE_TABLE]   = mgmtRetrieveShowTables;
//  tsMgmtShowRetrieveFp[TSDB_MGMT_TABLE_DNODE]   = mgmtRetrieveDnodes;
//  tsMgmtShowRetrieveFp[TSDB_MGMT_TABLE_MNODE]   = mgmtRetrieveMnodes;
//  tsMgmtShowRetrieveFp[TSDB_MGMT_TABLE_VGROUP]  = mgmtRetrieveVgroups;
//  tsMgmtShowRetrieveFp[TSDB_MGMT_TABLE_METRIC]  = mgmtRetrieveShowSuperTables;
//  tsMgmtShowRetrieveFp[TSDB_MGMT_TABLE_MODULE]  = mgmtRetrieveModules;
//  tsMgmtShowRetrieveFp[TSDB_MGMT_TABLE_QUERIES] = mgmtRetrieveQueries;
//  tsMgmtShowRetrieveFp[TSDB_MGMT_TABLE_STREAMS] = mgmtRetrieveStreams;
//  tsMgmtShowRetrieveFp[TSDB_MGMT_TABLE_CONFIGS] = mgmtRetrieveConfigs;
//  tsMgmtShowRetrieveFp[TSDB_MGMT_TABLE_CONNS]   = mgmtRetrieveConns;
//  tsMgmtShowRetrieveFp[TSDB_MGMT_TABLE_SCORES]  = mgmtRetrieveScores;
//  tsMgmtShowRetrieveFp[TSDB_MGMT_TABLE_GRANTS]  = mgmtRetrieveGrants;
//  tsMgmtShowRetrieveFp[TSDB_MGMT_TABLE_VNODES]  = mgmtRetrieveVnodes;
//}

static void mgmtProcessShowMsg(SRpcMsg *rpcMsg) {
  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};

  SShowMsg *pShowMsg = rpcMsg->pCont;
  if (pShowMsg->type == TSDB_MGMT_TABLE_DNODE || TSDB_MGMT_TABLE_GRANTS || TSDB_MGMT_TABLE_SCORES) {
    if (mgmtCheckRedirect(rpcMsg->handle) != TSDB_CODE_SUCCESS) {
      return;
    }
  }

  int32_t  size = sizeof(SShowRsp) + sizeof(SSchema) * TSDB_MAX_COLUMNS + TSDB_EXTRA_PAYLOAD_SIZE;
  SShowRsp *pShowRsp = rpcMallocCont(size);
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
    code = (*tsMgmtShowMetaFp[(uint8_t) pShowMsg->type])(&pShowRsp->tableMeta, pShow, rpcMsg->handle);
    if (code == 0) {
      size = sizeof(SShowRsp) + sizeof(SSchema) * pShow->numOfColumns;
    } else {
      mError("pShow:%p, type:%d %s, failed to get Meta, code:%d", pShow, pShowMsg->type,
             taosMsg[(uint8_t) pShowMsg->type], code);
      free(pShow);
    }
  }

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
    rowsRead = (*tsMgmtShowRetrieveFp[(uint8_t) pShow->type])(pShow, pRsp->data, rowsToRead, rpcMsg->handle);

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
//
//void mgmtProcessCreateTableMsg(SRpcMsg *rpcMsg) {
//  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
//
//  SCreateTableMsg *pCreate = (SCreateTableMsg *) rpcMsg->pCont;
//  pCreate->numOfColumns = htons(pCreate->numOfColumns);
//  pCreate->numOfTags    = htons(pCreate->numOfTags);
//  pCreate->sqlLen       = htons(pCreate->sqlLen);
//
//  SSchema *pSchema = (SSchema*) pCreate->schema;
//  for (int32_t i = 0; i < pCreate->numOfColumns + pCreate->numOfTags; ++i) {
//    pSchema->bytes = htons(pSchema->bytes);
//    pSchema->colId = i;
//    pSchema++;
//  }
//
//  if (mgmtCheckRedirect(rpcMsg->handle) != TSDB_CODE_SUCCESS) {
//    mError("table:%s, failed to create table, need redirect message", pCreate->tableId);
//    return;
//  }
//
//  SUserObj *pUser = mgmtGetUserFromConn(rpcMsg->handle);
//  if (pUser == NULL) {
//    mError("table:%s, failed to create table, invalid user", pCreate->tableId);
//    rpcRsp.code = TSDB_CODE_INVALID_USER;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  if (!pUser->writeAuth) {
//    mError("table:%s, failed to create table, no rights", pCreate->tableId);
//    rpcRsp.code = TSDB_CODE_NO_RIGHTS;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  int32_t code = mgmtCreateTable(pCreate, rpcMsg->contLen, rpcMsg->handle, false);
//  if (code != TSDB_CODE_ACTION_IN_PROGRESS) {
//    rpcRsp.code = code;
//    rpcSendResponse(&rpcRsp);
//  }
//}
//
//void mgmtProcessDropTableMsg(SRpcMsg *rpcMsg) {
//  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
//  SDropTableMsg *pDrop = (SDropTableMsg *) rpcMsg->pCont;
//
//  if (mgmtCheckRedirect(rpcMsg->handle) != TSDB_CODE_SUCCESS) {
//    mError("table:%s, failed to drop table, need redirect message", pDrop->tableId);
//    return;
//  }
//
//  SUserObj *pUser = mgmtGetUserFromConn(rpcMsg->handle);
//  if (pUser == NULL) {
//    mError("table:%s, failed to drop table, invalid user", pDrop->tableId);
//    rpcRsp.code = TSDB_CODE_INVALID_USER;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  if (!pUser->writeAuth) {
//    mError("table:%s, failed to drop table, no rights", pDrop->tableId);
//    rpcRsp.code = TSDB_CODE_NO_RIGHTS;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  SDbObj *pDb = mgmtGetDbByTableId(pDrop->tableId);
//  if (pDb == NULL) {
//    mError("table:%s, failed to drop table, db not selected", pDrop->tableId);
//    rpcRsp.code = TSDB_CODE_DB_NOT_SELECTED;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  int32_t code = mgmtDropTable(pDb, pDrop->tableId, pDrop->igNotExists);
//  if (code != TSDB_CODE_ACTION_IN_PROGRESS) {
//    rpcRsp.code = code;
//    rpcSendResponse(&rpcRsp);
//  }
//}
//
//void mgmtProcessAlterTableMsg(SRpcMsg *rpcMsg) {
//  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
//  if (mgmtCheckRedirect(rpcMsg->handle) != TSDB_CODE_SUCCESS) {
//    return;
//  }
//
//  SUserObj *pUser = mgmtGetUserFromConn(rpcMsg->handle);
//  if (pUser == NULL) {
//    rpcRsp.code = TSDB_CODE_INVALID_USER;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  SAlterTableMsg *pAlter = (SAlterTableMsg *) rpcMsg->pCont;
//
//  if (!pUser->writeAuth) {
//    rpcRsp.code = TSDB_CODE_NO_RIGHTS;
//  } else {
//    pAlter->type      = htons(pAlter->type);
//    pAlter->numOfCols = htons(pAlter->numOfCols);
//
//    if (pAlter->numOfCols > 2) {
//      mError("table:%s error numOfCols:%d in alter table", pAlter->tableId, pAlter->numOfCols);
//      rpcRsp.code = TSDB_CODE_APP_ERROR;
//    } else {
//      SDbObj *pDb = mgmtGetDb(pAlter->db);
//      if (pDb) {
//        for (int32_t i = 0; i < pAlter->numOfCols; ++i) {
//          pAlter->schema[i].bytes = htons(pAlter->schema[i].bytes);
//        }
//
//        rpcRsp.code = mgmtAlterTable(pDb, pAlter);
//        if (rpcRsp.code == 0) {
//          mLPrint("table:%s is altered by %s", pAlter->tableId, pUser->user);
//        }
//      } else {
//        rpcRsp.code = TSDB_CODE_DB_NOT_SELECTED;
//      }
//    }
//  }
//
//  rpcSendResponse(&rpcRsp);
//}
//
//void mgmtProcessCfgDnodeMsg(SRpcMsg *rpcMsg) {
//  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
//  if (mgmtCheckRedirect(rpcMsg->handle) != TSDB_CODE_SUCCESS) {
//    return;
//  }
//
//  SUserObj *pUser = mgmtGetUserFromConn(rpcMsg->handle);
//  if (pUser == NULL) {
//    rpcRsp.code = TSDB_CODE_INVALID_USER;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  SCfgDnodeMsg *pCfg = (SCfgDnodeMsg *) rpcMsg->pCont;
//
//  if (strcmp(pUser->pAcct->user, "root") != 0) {
//    rpcRsp.code = TSDB_CODE_NO_RIGHTS;
//  } else {
//    rpcRsp.code = mgmtSendCfgDnodeMsg(rpcMsg->pCont);
//  }
//
//  if (rpcRsp.code == TSDB_CODE_SUCCESS) {
//    mTrace("dnode:%s is configured by %s", pCfg->ip, pUser->user);
//  }
//
//  rpcSendResponse(&rpcRsp);
//}
//
//void mgmtProcessHeartBeatMsg(SRpcMsg *rpcMsg) {
//  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
//  SHeartBeatMsg *pHBMsg = (SHeartBeatMsg *) rpcMsg->pCont;
//  mgmtSaveQueryStreamList(pHBMsg);
//
//  SHeartBeatRsp *pHBRsp = (SHeartBeatRsp *) rpcMallocCont(sizeof(SHeartBeatRsp));
//  if (pHBRsp == NULL) {
//    rpcRsp.code = TSDB_CODE_SERV_OUT_OF_MEMORY;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  SRpcConnInfo connInfo;
//  rpcGetConnInfo(rpcMsg->handle, &connInfo);
//
//  pHBRsp->ipList.inUse = 0;
//  pHBRsp->ipList.port = htons(tsMgmtShellPort);
//  pHBRsp->ipList.numOfIps = 0;
//  if (pSdbPublicIpList != NULL && pSdbIpList != NULL) {
//    pHBRsp->ipList.numOfIps = htons(pSdbPublicIpList->numOfIps);
//    if (connInfo.serverIp == tsPublicIpInt) {
//      for (int i = 0; i < pSdbPublicIpList->numOfIps; ++i) {
//        pHBRsp->ipList.ip[i] = htonl(pSdbPublicIpList->ip[i]);
//      }
//    } else {
//      for (int i = 0; i < pSdbIpList->numOfIps; ++i) {
//        pHBRsp->ipList.ip[i] = htonl(pSdbIpList->ip[i]);
//      }
//    }
//  }
//
//  /*
//   * TODO
//   * Dispose kill stream or kill query message
//   */
//  pHBRsp->queryId = 0;
//  pHBRsp->streamId = 0;
//  pHBRsp->killConnection = 0;
//
//  rpcRsp.pCont = pHBRsp;
//  rpcRsp.contLen = sizeof(SHeartBeatRsp);
//  rpcSendResponse(&rpcRsp);
//}

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

//static void mgmtProcessConnectMsg(SRpcMsg *rpcMsg) {
//  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
//  SConnectMsg *pConnectMsg = (SConnectMsg *) rpcMsg->pCont;
//
//  SRpcConnInfo connInfo;
//  rpcGetConnInfo(rpcMsg->handle, &connInfo);
//  int32_t code;
//
//  SUserObj *pUser = mgmtGetUser(connInfo.user);
//  if (pUser == NULL) {
//    code = TSDB_CODE_INVALID_USER;
//    goto connect_over;
//  }
//
//  if (mgmtCheckExpired()) {
//    code = TSDB_CODE_GRANT_EXPIRED;
//    goto connect_over;
//  }
//
//  SAcctObj *pAcct = mgmtGetAcct(pUser->acct);
//  if (pAcct == NULL) {
//    code = TSDB_CODE_INVALID_ACCT;
//    goto connect_over;
//  }
//
//  code = taosCheckVersion(pConnectMsg->clientVersion, version, 3);
//  if (code != TSDB_CODE_SUCCESS) {
//    goto connect_over;
//  }
//
//  if (pConnectMsg->db[0]) {
//    char dbName[TSDB_TABLE_ID_LEN] = {0};
//    sprintf(dbName, "%x%s%s", pAcct->acctId, TS_PATH_DELIMITER, pConnectMsg->db);
//    SDbObj *pDb = mgmtGetDb(dbName);
//    if (pDb == NULL) {
//      code = TSDB_CODE_INVALID_DB;
//      goto connect_over;
//    }
//  }
//
//  SConnectRsp *pConnectRsp = rpcMallocCont(sizeof(SConnectRsp));
//  if (pConnectRsp == NULL) {
//    code = TSDB_CODE_SERV_OUT_OF_MEMORY;
//    goto connect_over;
//  }
//
//  sprintf(pConnectRsp->acctId, "%x", pAcct->acctId);
//  strcpy(pConnectRsp->serverVersion, version);
//  pConnectRsp->writeAuth = pUser->writeAuth;
//  pConnectRsp->superAuth = pUser->superAuth;
//  pConnectRsp->ipList.inUse = 0;
//  pConnectRsp->ipList.port = htons(tsMgmtShellPort);
//  pConnectRsp->ipList.numOfIps = 0;
//  if (pSdbPublicIpList != NULL && pSdbIpList != NULL) {
//    pConnectRsp->ipList.numOfIps = htons(pSdbPublicIpList->numOfIps);
//    if (connInfo.serverIp == tsPublicIpInt) {
//      for (int i = 0; i < pSdbPublicIpList->numOfIps; ++i) {
//        pConnectRsp->ipList.ip[i] = htonl(pSdbPublicIpList->ip[i]);
//      }
//    } else {
//      for (int i = 0; i < pSdbIpList->numOfIps; ++i) {
//        pConnectRsp->ipList.ip[i] = htonl(pSdbIpList->ip[i]);
//      }
//    }
//  }
//
//connect_over:
//  rpcRsp.code = code;
//  if (code != TSDB_CODE_SUCCESS) {
//    mLError("user:%s login from %s, code:%d", connInfo.user, taosIpStr(connInfo.clientIp), code);
//  } else {
//    mLPrint("user:%s login from %s, code:%d", connInfo.user, taosIpStr(connInfo.clientIp), code);
//    rpcRsp.pCont   = pConnectRsp;
//    rpcRsp.contLen = sizeof(SConnectRsp);
//  }
//  rpcSendResponse(&rpcRsp);
//}
//
///**
// * check if we need to add mgmtProcessTableMetaMsg into tranQueue, which will be executed one-by-one.
// */
//static bool mgmtCheckMeterMetaMsgType(void *pMsg) {
//  STableInfoMsg *pInfo = (STableInfoMsg *) pMsg;
//  int16_t autoCreate = htons(pInfo->createFlag);
//  STableInfo *pTable = mgmtGetTable(pInfo->tableId);
//
//  // If table does not exists and autoCreate flag is set, we add the handler into task queue
//  bool addIntoTranQueue = (pTable == NULL && autoCreate == 1);
//  if (addIntoTranQueue) {
//    mTrace("table:%s auto created task added", pInfo->tableId);
//  }
//
//  return addIntoTranQueue;
//}
//
//static bool mgmtCheckMsgReadOnly(int8_t type, void *pCont) {
//  if ((type == TSDB_MSG_TYPE_TABLE_META && (!mgmtCheckMeterMetaMsgType(pCont)))  ||
//       type == TSDB_MSG_TYPE_STABLE_META || type == TSDB_MSG_TYPE_RETRIEVE ||
//       type == TSDB_MSG_TYPE_SHOW || type == TSDB_MSG_TYPE_MULTI_TABLE_META      ||
//       type == TSDB_MSG_TYPE_CONNECT) {
//    return true;
//  }
//
//  return false;
//}
//
//static void mgmtProcessMsgFromShell(SRpcMsg *rpcMsg) {
//  if (sdbGetRunStatus() != SDB_STATUS_SERVING) {
//    mTrace("shell msg is ignored since SDB is not ready");
//    SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = TSDB_CODE_NOT_READY, .msgType = 0};
//    rpcSendResponse(&rpcRsp);
//    rpcFreeCont(rpcMsg->pCont);
//    return;
//  }
//
//  if (mgmtCheckMsgReadOnly(rpcMsg->msgType, rpcMsg->pCont)) {
//    (*mgmtProcessShellMsg[rpcMsg->msgType])(rpcMsg);
//  } else {
//    if (mgmtProcessShellMsg[rpcMsg->msgType]) {
//      mgmtAddToTranRequest(rpcMsg);
//    } else {
//      mError("%s from shell is not processed", taosMsg[rpcMsg->msgType]);
//    }
//  }
//
//  rpcFreeCont(rpcMsg->pCont);
//}
//
//void mgmtProcessCreateVgroup(SCreateTableMsg *pCreate, int32_t contLen, void *thandle, bool isGetMeta) {
//  SRpcMsg rpcRsp = {.handle = thandle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
//  SDbObj *pDb = mgmtGetDb(pCreate->db);
//  if (pDb == NULL) {
//    mError("table:%s, failed to create vgroup, db not found", pCreate->tableId);
//    rpcRsp.code = TSDB_CODE_INVALID_DB;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  SVgObj *pVgroup = mgmtCreateVgroup(pDb);
//  if (pVgroup == NULL) {
//    mError("table:%s, failed to alloc vnode to vgroup", pCreate->tableId);
//    rpcRsp.code = TSDB_CODE_NO_ENOUGH_DNODES;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  void *cont = rpcMallocCont(contLen);
//  if (cont == NULL) {
//    mError("table:%s, failed to create table, can not alloc memory", pCreate->tableId);
//    rpcRsp.code = TSDB_CODE_SERV_OUT_OF_MEMORY;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  memcpy(cont, pCreate, contLen);
//
//  SProcessInfo *info = calloc(1, sizeof(SProcessInfo));
//  info->type    = TSDB_PROCESS_CREATE_VGROUP;
//  info->thandle = thandle;
//  info->ahandle = pVgroup;
//  info->cont    = cont;
//  info->contLen = contLen;
//
//  if (isGetMeta) {
//    info->type = TSDB_PROCESS_CREATE_VGROUP_GET_META;
//  }
//
//  mgmtSendCreateVgroupMsg(pVgroup, info);
//}
//
//void mgmtProcessCreateTable(SVgObj *pVgroup, SCreateTableMsg *pCreate, int32_t contLen, void *thandle, bool isGetMeta) {
//  assert(pVgroup != NULL);
//  SRpcMsg rpcRsp = {.handle = thandle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
//
//  int32_t sid = taosAllocateId(pVgroup->idPool);
//  if (sid < 0) {
//    mTrace("table:%s, no enough sid in vgroup:%d, start to create a new vgroup", pCreate->tableId, pVgroup->vgId);
//    mgmtProcessCreateVgroup(pCreate, contLen, thandle, isGetMeta);
//    return;
//  }
//
//  STableInfo *pTable;
//  SDMCreateTableMsg *pDCreate = NULL;
//
//  if (pCreate->numOfColumns == 0) {
//    mTrace("table:%s, start to create child table, vgroup:%d sid:%d", pCreate->tableId, pVgroup->vgId, sid);
//    rpcRsp.code = mgmtCreateChildTable(pCreate, contLen, pVgroup, sid, &pDCreate, &pTable);
//  } else {
//    mTrace("table:%s, start to create normal table, vgroup:%d sid:%d", pCreate->tableId, pVgroup->vgId, sid);
//    rpcRsp.code = mgmtCreateNormalTable(pCreate, contLen, pVgroup, sid, &pDCreate, &pTable);
//  }
//
//  if (rpcRsp.code != TSDB_CODE_SUCCESS) {
//    mTrace("table:%s, failed to create table in vgroup:%d sid:%d ", pCreate->tableId, pVgroup->vgId, sid);
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  assert(pDCreate != NULL);
//  assert(pTable != NULL);
//
//  SProcessInfo *info = calloc(1, sizeof(SProcessInfo));
//  info->type    = TSDB_PROCESS_CREATE_TABLE;
//  info->thandle = thandle;
//  info->ahandle = pTable;
//  SRpcIpSet ipSet = mgmtGetIpSetFromVgroup(pVgroup);
//  if (isGetMeta) {
//    info->type = TSDB_PROCESS_CREATE_TABLE_GET_META;
//  }
//
//  mgmtSendCreateTableMsg(pDCreate, &ipSet, info);
//}
//
//void mgmtProcessGetTableMeta(STableInfo *pTable, void *thandle) {
//  SRpcMsg rpcRsp = {.handle = thandle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
//  SDbObj* pDb = mgmtGetDbByTableId(pTable->tableId);
//  if (pDb == NULL || pDb->dropStatus != TSDB_DB_STATUS_READY) {
//    mError("table:%s, failed to get table meta, db not selected", pTable->tableId);
//    rpcRsp.code = TSDB_CODE_DB_NOT_SELECTED;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  SRpcConnInfo connInfo;
//  rpcGetConnInfo(thandle, &connInfo);
//  bool usePublicIp = (connInfo.serverIp == tsPublicIpInt);
//
//  STableMeta *pMeta = rpcMallocCont(sizeof(STableMeta) + sizeof(SSchema) * TSDB_MAX_COLUMNS);
//  rpcRsp.code = mgmtGetTableMeta(pDb, pTable, pMeta, usePublicIp);
//
//  if (rpcRsp.code != TSDB_CODE_SUCCESS) {
//    rpcFreeCont(pMeta);
//  } else {
//    pMeta->contLen = htons(pMeta->contLen);
//    rpcRsp.pCont   = pMeta;
//    rpcRsp.contLen = pMeta->contLen;
//  }
//
//  rpcSendResponse(&rpcRsp);
//}
//
//static int32_t mgmtCheckRedirectMsgImp(void *pConn) {
//  return 0;
//}
//
//int32_t (*mgmtCheckRedirect)(void *pConn) = mgmtCheckRedirectMsgImp;
//
//static void mgmtProcessUnSupportMsg(SRpcMsg *rpcMsg) {
//  SRpcMsg rpcRsp = {
//    .msgType = 0,
//    .pCont   = 0,
//    .contLen = 0,
//    .code    = TSDB_CODE_OPS_NOT_SUPPORT,
//    .handle  = rpcMsg->handle
//  };
//  rpcSendResponse(&rpcRsp);
//}
//
//static void mgmtProcessAlterAcctMsg(SRpcMsg *rpcMsg) {
//  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
//  if (!mgmtAlterAcctFp) {
//    rpcRsp.code = TSDB_CODE_OPS_NOT_SUPPORT;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  SAlterAcctMsg *pAlter = rpcMsg->pCont;
//  pAlter->cfg.maxUsers           = htonl(pAlter->cfg.maxUsers);
//  pAlter->cfg.maxDbs             = htonl(pAlter->cfg.maxDbs);
//  pAlter->cfg.maxTimeSeries      = htonl(pAlter->cfg.maxTimeSeries);
//  pAlter->cfg.maxConnections     = htonl(pAlter->cfg.maxConnections);
//  pAlter->cfg.maxStreams         = htonl(pAlter->cfg.maxStreams);
//  pAlter->cfg.maxPointsPerSecond = htonl(pAlter->cfg.maxPointsPerSecond);
//  pAlter->cfg.maxStorage         = htobe64(pAlter->cfg.maxStorage);
//  pAlter->cfg.maxQueryTime       = htobe64(pAlter->cfg.maxQueryTime);
//  pAlter->cfg.maxInbound         = htobe64(pAlter->cfg.maxInbound);
//  pAlter->cfg.maxOutbound        = htobe64(pAlter->cfg.maxOutbound);
//
//  if (mgmtCheckRedirect(rpcMsg->handle) != TSDB_CODE_SUCCESS) {
//    mError("account:%s, failed to alter account, need redirect message", pAlter->user);
//    return;
//  }
//
//  SUserObj *pUser = mgmtGetUserFromConn(rpcMsg->handle);
//  if (pUser == NULL) {
//    mError("account:%s, failed to alter account, invalid user", pAlter->user);
//    rpcRsp.code = TSDB_CODE_INVALID_USER;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  if (strcmp(pUser->user, "root") != 0) {
//    mError("account:%s, failed to alter account, no rights", pAlter->user);
//    rpcRsp.code = TSDB_CODE_NO_RIGHTS;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  int32_t code = mgmtAlterAcctFp(pAlter->user, pAlter->pass, &(pAlter->cfg));;
//  if (code == TSDB_CODE_SUCCESS) {
//    mLPrint("account:%s is altered by %s", pAlter->user, pUser->user);
//  } else {
//    mError("account:%s, failed to alter account, reason:%s", pAlter->user, tstrerror(code));
//  }
//
//  rpcRsp.code = code;
//  rpcSendResponse(&rpcRsp);
//}
//
//static void mgmtProcessDropAcctMsg(SRpcMsg *rpcMsg) {
//  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
//  if (!mgmtDropAcctFp) {
//    rpcRsp.code = TSDB_CODE_OPS_NOT_SUPPORT;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  SDropAcctMsg *pDrop = (SDropAcctMsg *) rpcMsg->pCont;
//  if (mgmtCheckRedirect(rpcMsg->handle) != TSDB_CODE_SUCCESS) {
//    mError("account:%s, failed to drop account, need redirect message", pDrop->user);
//    return;
//  }
//
//  SUserObj *pUser = mgmtGetUserFromConn(rpcMsg->handle);
//  if (pUser == NULL) {
//    mError("account:%s, failed to drop account, invalid user", pDrop->user);
//    rpcRsp.code = TSDB_CODE_INVALID_USER;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  if (strcmp(pUser->user, "root") != 0) {
//    mError("account:%s, failed to drop account, no rights", pDrop->user);
//    rpcRsp.code = TSDB_CODE_NO_RIGHTS;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  int32_t code = mgmtDropAcctFp(pDrop->user);
//  if (code == TSDB_CODE_SUCCESS) {
//    mLPrint("account:%s is dropped by %s", pDrop->user, pUser->user);
//  } else {
//    mError("account:%s, failed to drop account, reason:%s", pDrop->user, tstrerror(code));
//  }
//
//  rpcRsp.code = code;
//  rpcSendResponse(&rpcRsp);
//}
//
//static void mgmtProcessCreateAcctMsg(SRpcMsg *rpcMsg) {
//  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
//  if (!mgmtCreateAcctFp) {
//    rpcRsp.code = TSDB_CODE_OPS_NOT_SUPPORT;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  SCreateAcctMsg *pCreate = (SCreateAcctMsg *) rpcMsg->pCont;
//  pCreate->cfg.maxUsers           = htonl(pCreate->cfg.maxUsers);
//  pCreate->cfg.maxDbs             = htonl(pCreate->cfg.maxDbs);
//  pCreate->cfg.maxTimeSeries      = htonl(pCreate->cfg.maxTimeSeries);
//  pCreate->cfg.maxConnections     = htonl(pCreate->cfg.maxConnections);
//  pCreate->cfg.maxStreams         = htonl(pCreate->cfg.maxStreams);
//  pCreate->cfg.maxPointsPerSecond = htonl(pCreate->cfg.maxPointsPerSecond);
//  pCreate->cfg.maxStorage         = htobe64(pCreate->cfg.maxStorage);
//  pCreate->cfg.maxQueryTime       = htobe64(pCreate->cfg.maxQueryTime);
//  pCreate->cfg.maxInbound         = htobe64(pCreate->cfg.maxInbound);
//  pCreate->cfg.maxOutbound        = htobe64(pCreate->cfg.maxOutbound);
//
//  if (mgmtCheckRedirect(rpcMsg->handle) != TSDB_CODE_SUCCESS) {
//    mError("account:%s, failed to create account, need redirect message", pCreate->user);
//    return;
//  }
//
//  SUserObj *pUser = mgmtGetUserFromConn(rpcMsg->handle);
//  if (pUser == NULL) {
//    mError("account:%s, failed to create account, invalid user", pCreate->user);
//    rpcRsp.code = TSDB_CODE_INVALID_USER;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  if (strcmp(pUser->user, "root") != 0) {
//    mError("account:%s, failed to create account, no rights", pCreate->user);
//    rpcRsp.code = TSDB_CODE_NO_RIGHTS;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  int32_t code = mgmtCreateAcctFp(pCreate->user, pCreate->pass, &(pCreate->cfg));
//  if (code == TSDB_CODE_SUCCESS) {
//    mLPrint("account:%s is created by %s", pCreate->user, pUser->user);
//  } else {
//    mError("account:%s, failed to create account, reason:%s", pCreate->user, tstrerror(code));
//  }
//
//  rpcRsp.code = code;
//  rpcSendResponse(&rpcRsp);
//}
//
//static void mgmtProcessCreateDnodeMsg(SRpcMsg *rpcMsg) {
//  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
//  if (!mgmtCreateDnodeFp) {
//    rpcRsp.code = TSDB_CODE_OPS_NOT_SUPPORT;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  SCreateDnodeMsg *pCreate = (SCreateDnodeMsg *) rpcMsg->pCont;
//  if (mgmtCheckRedirect(rpcMsg->handle) != TSDB_CODE_SUCCESS) {
//    mError("failed to create dnode:%s, redirect this message", pCreate->ip);
//    return;
//  }
//
//  SUserObj *pUser = mgmtGetUserFromConn(rpcMsg->handle);
//  if (pUser == NULL) {
//    mError("failed to create dnode:%s, reason:%s", pCreate->ip, tstrerror(TSDB_CODE_INVALID_USER));
//    rpcRsp.code = TSDB_CODE_INVALID_USER;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  if (strcmp(pUser->user, "root") != 0) {
//    mError("failed to create dnode:%s, reason:%s", pCreate->ip, tstrerror(TSDB_CODE_NO_RIGHTS));
//    rpcRsp.code = TSDB_CODE_NO_RIGHTS;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  int32_t code = (*mgmtCreateDnodeFp)(inet_addr(pCreate->ip));
//  if (code == TSDB_CODE_SUCCESS) {
//    mLPrint("dnode:%s is created by %s", pCreate->ip, pUser->user);
//  } else {
//    mError("failed to create dnode:%s, reason:%s", pCreate->ip, tstrerror(code));
//  }
//
//  rpcRsp.code = code;
//  rpcSendResponse(&rpcRsp);
//}
//
//static void mgmtProcessDropDnodeMsg(SRpcMsg *rpcMsg) {
//  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
//  if (!mgmtDropDnodeByIpFp) {
//    rpcRsp.code = TSDB_CODE_OPS_NOT_SUPPORT;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  SDropDnodeMsg *pDrop = (SDropDnodeMsg *) rpcMsg->pCont;
//  if (mgmtCheckRedirect(rpcMsg->handle) != TSDB_CODE_SUCCESS) {
//    mError("failed to drop dnode:%s, redirect this message", pDrop->ip);
//    return;
//  }
//
//  SUserObj *pUser = mgmtGetUserFromConn(rpcMsg->handle);
//  if (pUser == NULL) {
//    mError("failed to drop dnode:%s, reason:%s", pDrop->ip, tstrerror(TSDB_CODE_INVALID_USER));
//    rpcRsp.code = TSDB_CODE_INVALID_USER;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  if (strcmp(pUser->user, "root") != 0) {
//    mError("failed to drop dnode:%s, reason:%s", pDrop->ip, tstrerror(TSDB_CODE_NO_RIGHTS));
//    rpcRsp.code = TSDB_CODE_NO_RIGHTS;
//    rpcSendResponse(&rpcRsp);
//    return;
//  }
//
//  int32_t code = (*mgmtDropDnodeByIpFp)(inet_addr(pDrop->ip));
//  if (code == TSDB_CODE_SUCCESS) {
//    mLPrint("dnode:%s set to removing state by %s", pDrop->ip, pUser->user);
//  } else {
//    mError("failed to drop dnode:%s, reason:%s", pDrop->ip, tstrerror(code));
//  }
//
//  rpcRsp.code = code;
//  rpcSendResponse(&rpcRsp);
//}
//
//void mgmtInitProcessShellMsg() {
//  mgmtProcessShellMsg[TSDB_MSG_TYPE_CONNECT]          = mgmtProcessConnectMsg;
//  mgmtProcessShellMsg[TSDB_MSG_TYPE_HEARTBEAT]        = mgmtProcessHeartBeatMsg;
//  mgmtProcessShellMsg[TSDB_MSG_TYPE_CREATE_DB]        = mgmtProcessCreateDbMsg;
//  mgmtProcessShellMsg[TSDB_MSG_TYPE_ALTER_DB]         = mgmtProcessAlterDbMsg;
//  mgmtProcessShellMsg[TSDB_MSG_TYPE_DROP_DB]          = mgmtProcessDropDbMsg;
//  mgmtProcessShellMsg[TSDB_MSG_TYPE_USE_DB]           = mgmtProcessUnSupportMsg;
//  mgmtProcessShellMsg[TSDB_MSG_TYPE_CREATE_ACCT]      = mgmtProcessCreateAcctMsg;
//  mgmtProcessShellMsg[TSDB_MSG_TYPE_DROP_ACCT]        = mgmtProcessDropAcctMsg;
//  mgmtProcessShellMsg[TSDB_MSG_TYPE_ALTER_ACCT]       = mgmtProcessAlterAcctMsg;
//  mgmtProcessShellMsg[TSDB_MSG_TYPE_CREATE_TABLE]     = mgmtProcessCreateTableMsg;
//  mgmtProcessShellMsg[TSDB_MSG_TYPE_DROP_TABLE]       = mgmtProcessDropTableMsg;
//  mgmtProcessShellMsg[TSDB_MSG_TYPE_ALTER_TABLE]      = mgmtProcessAlterTableMsg;
//  mgmtProcessShellMsg[TSDB_MSG_TYPE_CREATE_DNODE]     = mgmtProcessCreateDnodeMsg;
//  mgmtProcessShellMsg[TSDB_MSG_TYPE_DROP_DNODE]       = mgmtProcessDropDnodeMsg;
//  mgmtProcessShellMsg[TSDB_MSG_TYPE_MD_CONFIG_DNODE]        = mgmtProcessCfgDnodeMsg;
//  mgmtProcessShellMsg[TSDB_MSG_TYPE_CREATE_MNODE]     = mgmtProcessUnSupportMsg;
//  mgmtProcessShellMsg[TSDB_MSG_TYPE_DROP_MNODE]       = mgmtProcessUnSupportMsg;
//  mgmtProcessShellMsg[TSDB_MSG_TYPE_CFG_MNODE]        = mgmtProcessUnSupportMsg;
//  mgmtProcessShellMsg[TSDB_MSG_TYPE_KILL_QUERY]       = mgmtProcessKillQueryMsg;
//  mgmtProcessShellMsg[TSDB_MSG_TYPE_KILL_STREAM]      = mgmtProcessKillStreamMsg;
//  mgmtProcessShellMsg[TSDB_MSG_TYPE_KILL_CONNECTION]  = mgmtProcessKillConnectionMsg;
//  mgmtProcessShellMsg[TSDB_MSG_TYPE_TABLE_META]       = mgmtProcessTableMetaMsg;
//  mgmtProcessShellMsg[TSDB_MSG_TYPE_MULTI_TABLE_META] = mgmtProcessMultiTableMetaMsg;
//  mgmtProcessShellMsg[TSDB_MSG_TYPE_STABLE_META]      = mgmtProcessSuperTableMetaMsg;
//}
