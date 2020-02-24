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

#include <arpa/inet.h>

#include "dnodeSystem.h"
#include "mnode.h"
#include "mgmtProfile.h"
#include "taosmsg.h"
#include "tlog.h"
#include "dnodeModule.h"

#define MAX_LEN_OF_METER_META (sizeof(SMultiTableMeta) + sizeof(SSchema) * TSDB_MAX_COLUMNS + sizeof(SSchema) * TSDB_MAX_TAGS + TSDB_MAX_TAGS_LEN)

void *mgmtProcessMsgFromShell(char *msg, void *ahandle, void *thandle);
int (*mgmtProcessShellMsg[TSDB_MSG_TYPE_MAX])(char *, int, void *);
void  mgmtInitProcessShellMsg();

int mgmtRedirectMsg(void *pConn, int msgType) {
  char *    pStart, *pMsg;
  int       size, msgLen;
  STaosRsp *pRsp;

  pStart = taosBuildRspMsgWithSize(pConn->thandle, msgType, 128);
  if (pStart == NULL) return 0;
  pMsg = pStart;
  pRsp = (STaosRsp *)pMsg;
  pRsp->code = TSDB_CODE_REDIRECT;
  pMsg = (char *)pRsp->more;

  if (pConn->usePublicIp) {
    size = sizeof(SIpList) + pSdbPublicIpList->numOfIps * 4;
    memcpy(pMsg, pSdbPublicIpList, size);
    pMsg += size;
  } else {
    size = sizeof(SIpList) + pSdbIpList->numOfIps * 4;
    memcpy(pMsg, pSdbIpList, size);
    pMsg += size;
  }

  msgLen = pMsg - pStart;

  taosSendMsgToPeer(pConn->thandle, pStart, msgLen);

  return 0;
}

int mgmtCheckRedirectMsg(void *pConn, int msgType) {
  if (!sdbMaster) {
    rpcSendRedirectRsp(pConn, msgType);
    return 1;
  } else {
    return 0;
  }
}

int mgmtProcessAlterAcctMsg(char *pMsg, int msgLen, void *pConn) {
  SAlterAcctMsg *pAlter = NULL;
  int            code = 0;

  if (!sdbMaster) return mgmtRedirectMsg(pConn, TSDB_MSG_TYPE_ALTER_ACCT_RSP);

  pAlter = (SAlterAcctMsg *)pMsg;

  pAlter->cfg.maxUsers = htonl(pAlter->cfg.maxUsers);
  pAlter->cfg.maxDbs = htonl(pAlter->cfg.maxDbs);
  pAlter->cfg.maxTimeSeries = htonl(pAlter->cfg.maxTimeSeries);
  pAlter->cfg.maxConnections = htonl(pAlter->cfg.maxConnections);
  pAlter->cfg.maxStreams = htonl(pAlter->cfg.maxStreams);
  pAlter->cfg.maxPointsPerSecond = htonl(pAlter->cfg.maxPointsPerSecond);
  pAlter->cfg.maxStorage = htobe64(pAlter->cfg.maxStorage);
  pAlter->cfg.maxQueryTime = htobe64(pAlter->cfg.maxQueryTime);
  pAlter->cfg.maxInbound = htobe64(pAlter->cfg.maxInbound);
  pAlter->cfg.maxOutbound = htobe64(pAlter->cfg.maxOutbound);

  if (strcmp(pConn->pAcct->user, "root") != 0) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    code = mgmtAlterAcct(pAlter->user, pAlter->pass, &(pAlter->cfg));
    if (code == TSDB_CODE_SUCCESS) {
      mLPrint("Account: %s is altered by %s", pAlter->user, pConn->pUser->user);
    }
  }

  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_ALTER_ACCT_RSP, code);

  return 0;
}

int mgmtProcessCreateDnodeMsg(char *pMsg, int msgLen, void *pConn) {
  SCreateDnodeMsg *pCreate = (SCreateDnodeMsg *)pMsg;
  int              code = 0;

  if (!sdbMaster) return mgmtRedirectMsg(pConn, TSDB_MSG_TYPE_CREATE_DNODE_RSP);

  if (strcmp(pConn->pUser->user, "root") != 0) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    code = mgmtCreateDnode(inet_addr(pCreate->ip));
    if (code == TSDB_CODE_SUCCESS) {
      mLPrint("dnode:%s is created by %s", pCreate->ip, pConn->pUser->user);
    }
  }

  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_CREATE_DNODE_RSP, code);

  return 0;
}

int mgmtProcessCfgMnodeMsg(char *pMsg, int msgLen, void *pConn) {
  int      code = 0;
  SCfgDnodeMsg *pCfg = (SCfgDnodeMsg *)pMsg;

  if (!sdbMaster) return mgmtRedirectMsg(pConn, TSDB_MSG_TYPE_CFG_MNODE_RSP);

  if (strcmp(pConn->pAcct->user, "root") != 0) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    code = sdbCfgNode(pMsg);
  }

  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_CFG_MNODE_RSP, code);

  if (code == 0) mTrace("mnode:%s is configured by %s", pCfg->ip, pConn->pUser->user);

  return 0;
}

int mgmtProcessDropMnodeMsg(char *pMsg, int msgLen, void *pConn) {
  SDropMnodeMsg *pDrop = (SDropMnodeMsg *)pMsg;
  int            code = 0;

  if (!sdbMaster) return mgmtRedirectMsg(pConn, TSDB_MSG_TYPE_DROP_MNODE_RSP);

  if (strcmp(pConn->pUser->user, "root") != 0) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    // code = sdbRemovePeerByIp(inet_addr(pDrop->ip));
    SDnodeObj *pDnode = mgmtGetDnode(inet_addr(pDrop->ip));
    if (pDnode != NULL) {
      code = mgmtUnSetModuleInDnode(pDnode, TSDB_MOD_MGMT);
    }
  }

  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_DROP_MNODE_RSP, code);

  if (code == 0) {
    mLPrint("Mnode:%s is dropped by %s", pDrop->ip, pConn->pUser->user);
  }

  return 0;
}

int mgmtProcessDropDnodeMsg(char *pMsg, int msgLen, void *pConn) {
  SDropDnodeMsg *pDrop = (SDropDnodeMsg *)pMsg;
  int            code = 0;

  if (!sdbMaster) return mgmtRedirectMsg(pConn, TSDB_MSG_TYPE_DROP_DNODE_RSP);

  if (strcmp(pConn->pUser->user, "root") != 0) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    code = mgmtDropDnodeByIp(inet_addr(pDrop->ip));
  }

  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_DROP_DNODE_RSP, code);

  if (code == 0) {
    mLPrint("dnode:%s set to removing state by %s", pDrop->ip, pConn->pUser->user);
  }

  return 0;
}

int mgmtProcessDropAcctMsg(char *pMsg, int msgLen, void *pConn) {
  SDropAcctMsg *pDrop = (SDropAcctMsg *)pMsg;
  int           code = 0;

  if (!sdbMaster) return mgmtRedirectMsg(pConn, TSDB_MSG_TYPE_DROP_ACCT_RSP);

  if (strcmp(pDrop->user, "root") == 0) {
    code = TSDB_CODE_NO_RIGHTS;
  } else if (strcmp(pConn->pUser->user, "root") == 0) {
    code = mgmtDropAcct(pDrop->user);
    if (code == 0) {
      mLPrint("account:%s is dropped by %s", pDrop->user, pConn->pUser->user);
    }
  } else {
    code = TSDB_CODE_NO_RIGHTS;
  }

  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_DROP_ACCT_RSP, code);

  return 0;
}

int mgmtProcessCreateAcctMsg(char *pMsg, int msgLen, void *pConn) {
  SCreateAcctMsg *pCreate = (SCreateAcctMsg *)pMsg;
  int             code = 0;

  if (!sdbMaster) return mgmtRedirectMsg(pConn, TSDB_MSG_TYPE_CREATE_ACCT_RSP);

  pCreate->cfg.maxUsers = htonl(pCreate->cfg.maxUsers);
  pCreate->cfg.maxDbs = htonl(pCreate->cfg.maxDbs);
  pCreate->cfg.maxTimeSeries = htonl(pCreate->cfg.maxTimeSeries);
  pCreate->cfg.maxConnections = htonl(pCreate->cfg.maxConnections);
  pCreate->cfg.maxStreams = htonl(pCreate->cfg.maxStreams);
  pCreate->cfg.maxPointsPerSecond = htonl(pCreate->cfg.maxPointsPerSecond);
  pCreate->cfg.maxStorage = htobe64(pCreate->cfg.maxStorage);
  pCreate->cfg.maxQueryTime = htobe64(pCreate->cfg.maxQueryTime);

  if (strcmp(pConn->pUser->user, "root") == 0) {
    // TODO : Convert from server format to host format
    code = mgmtCreateAcct(pCreate->user, pCreate->pass, &(pCreate->cfg));
    if (code == TSDB_CODE_SUCCESS) {
      mLPrint("account:%s is created by %s", pCreate->user, pConn->pUser->user);
    }
  } else {
    code = TSDB_CODE_NO_RIGHTS;
  }

  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_CREATE_ACCT_RSP, code);

  return 0;
}
