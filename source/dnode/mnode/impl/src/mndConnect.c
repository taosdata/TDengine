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



// static int32_t mnodeProcessHeartBeatMsg(SMnodeMsg *pMsg) {
//   SHeartBeatRsp *pRsp = (SHeartBeatRsp *)rpcMallocCont(sizeof(SHeartBeatRsp));
//   if (pRsp == NULL) {
//     return TSDB_CODE_MND_OUT_OF_MEMORY;
//   }

//   SHeartBeatMsg *pHBMsg = pMsg->rpcMsg.pCont;
//   if (taosCheckVersion(pHBMsg->clientVer, version, 3) != TSDB_CODE_SUCCESS) {
//     rpcFreeCont(pRsp);
//     return TSDB_CODE_TSC_INVALID_VERSION;  // todo change the error code
//   }

//   SRpcConnInfo connInfo = {0};
//   rpcGetConnInfo(pMsg->rpcMsg.handle, &connInfo);
    
//   int32_t connId = htonl(pHBMsg->connId);
//   SConnObj *pConn = mnodeAccquireConn(connId, connInfo.user, connInfo.clientIp, connInfo.clientPort);
//   if (pConn == NULL) {
//     pHBMsg->pid = htonl(pHBMsg->pid);
//     pConn = mnodeCreateConn(connInfo.user, connInfo.clientIp, connInfo.clientPort, pHBMsg->pid, pHBMsg->appName);
//   }

//   if (pConn == NULL) {
//     // do not close existing links, otherwise
//     // mError("failed to create connId, close connect");
//     // pRsp->killConnection = 1;    
//   } else {
//     pRsp->connId = htonl(pConn->connId);
//     mnodeSaveQueryStreamList(pConn, pHBMsg);
    
//     if (pConn->killed != 0) {
//       pRsp->killConnection = 1;
//     }

//     if (pConn->streamId != 0) {
//       pRsp->streamId = htonl(pConn->streamId);
//       pConn->streamId = 0;
//     }

//     if (pConn->queryId != 0) {
//       pRsp->queryId = htonl(pConn->queryId);
//       pConn->queryId = 0;
//     }
//   }

//   int32_t    onlineDnodes = 0, totalDnodes = 0;
//   mnodeGetOnlineAndTotalDnodesNum(&onlineDnodes, &totalDnodes);

//   pRsp->onlineDnodes = htonl(onlineDnodes);
//   pRsp->totalDnodes = htonl(totalDnodes);
//   mnodeGetMnodeEpSetForShell(&pRsp->epSet, false);

//   pMsg->rpcRsp.rsp = pRsp;
//   pMsg->rpcRsp.len = sizeof(SHeartBeatRsp);

//   mnodeReleaseConn(pConn);
//   return TSDB_CODE_SUCCESS;
// }

// static int32_t mnodeProcessConnectMsg(SMnodeMsg *pMsg) {
//   SConnectMsg *pConnectMsg = pMsg->rpcMsg.pCont;
//   SConnectRsp *pConnectRsp = NULL;
//   int32_t code = TSDB_CODE_SUCCESS;

//   SRpcConnInfo connInfo = {0};
//   if (rpcGetConnInfo(pMsg->rpcMsg.handle, &connInfo) != 0) {
//     mError("thandle:%p is already released while process connect msg", pMsg->rpcMsg.handle);
//     code = TSDB_CODE_MND_INVALID_CONNECTION;
//     goto connect_over;
//   }

//   code = taosCheckVersion(pConnectMsg->clientVersion, version, 3);
//   if (code != TSDB_CODE_SUCCESS) {
//     goto connect_over;
//   }

//   SUserObj *pUser = pMsg->pUser;
//   SAcctObj *pAcct = pUser->pAcct;

//   if (pConnectMsg->db[0]) {
//     char dbName[TSDB_TABLE_FNAME_LEN * 3] = {0};
//     sprintf(dbName, "%x%s%s", pAcct->acctId, TS_PATH_DELIMITER, pConnectMsg->db);
//     SDbObj *pDb = mnodeGetDb(dbName);
//     if (pDb == NULL) {
//       code = TSDB_CODE_MND_INVALID_DB;
//       goto connect_over;
//     }
    
//     if (pDb->status != TSDB_DB_STATUS_READY) {
//       mError("db:%s, status:%d, in dropping", pDb->name, pDb->status);
//       code = TSDB_CODE_MND_DB_IN_DROPPING;
//       mnodeDecDbRef(pDb);
//       goto connect_over;
//     }
//     mnodeDecDbRef(pDb);
//   }

//   pConnectRsp = rpcMallocCont(sizeof(SConnectRsp));
//   if (pConnectRsp == NULL) {
//     code = TSDB_CODE_MND_OUT_OF_MEMORY;
//     goto connect_over;
//   }

//   pConnectMsg->pid = htonl(pConnectMsg->pid);
//   SConnObj *pConn = mnodeCreateConn(connInfo.user, connInfo.clientIp, connInfo.clientPort, pConnectMsg->pid, pConnectMsg->appName);
//   if (pConn == NULL) {
//     code = terrno;
//   } else {
//     pConnectRsp->connId = htonl(pConn->connId);
//     mnodeReleaseConn(pConn);
//   }

//   sprintf(pConnectRsp->acctId, "%x", pAcct->acctId);
//   memcpy(pConnectRsp->serverVersion, version, TSDB_VERSION_LEN);
//   pConnectRsp->writeAuth = pUser->writeAuth;
//   pConnectRsp->superAuth = pUser->superAuth;
  
//   mnodeGetMnodeEpSetForShell(&pConnectRsp->epSet, false);

//   dnodeGetClusterId(pConnectRsp->clusterId);

// connect_over:
//   if (code != TSDB_CODE_SUCCESS) {
//     if (pConnectRsp) rpcFreeCont(pConnectRsp);
//     mLError("user:%s login from %s, result:%s", connInfo.user, taosIpStr(connInfo.clientIp), tstrerror(code));
//   } else {
//     mLInfo("user:%s login from %s, result:%s", connInfo.user, taosIpStr(connInfo.clientIp), tstrerror(code));
//     pMsg->rpcRsp.rsp = pConnectRsp;
//     pMsg->rpcRsp.len = sizeof(SConnectRsp);
//   }

//   return code;
// }


