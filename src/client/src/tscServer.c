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

#include "os.h"
#include "tcache.h"
#include "trpc.h"
#include "tscJoinProcess.h"
#include "tscProfile.h"
#include "tscSecondaryMerge.h"
#include "tscUtil.h"
#include "tschemautil.h"
#include "tsclient.h"
#include "tscompression.h"
#include "tsocket.h"
#include "tscSQLParser.h"
#include "ttime.h"
#include "ttimer.h"
#include "tutil.h"

#define TSC_MGMT_VNODE 999

#ifdef CLUSTER
  SIpStrList tscMgmtIpList;
  int        tsMasterIndex = 0;
  int        tsSlaveIndex = 1;
#else
  int        tsMasterIndex = 0;
  int        tsSlaveIndex = 0;  // slave == master for single node edition
  uint32_t   tsServerIp;
#endif

int (*tscBuildMsg[TSDB_SQL_MAX])(SSqlObj *pSql);
int (*tscProcessMsgRsp[TSDB_SQL_MAX])(SSqlObj *pSql);
void (*tscUpdateVnodeMsg[TSDB_SQL_MAX])(SSqlObj *pSql, char *buf);
void tscProcessActivityTimer(void *handle, void *tmrId);
int tscKeepConn[TSDB_SQL_MAX] = {0};

static int32_t minMsgSize() { return tsRpcHeadSize + sizeof(STaosDigest); }

#ifdef CLUSTER
void tscPrintMgmtIp() {
  if (tscMgmtIpList.numOfIps <= 0) {
    tscError("invalid IP list:%d", tscMgmtIpList.numOfIps);
  } else {
    for (int i = 0; i < tscMgmtIpList.numOfIps; ++i) tscTrace("mgmt index:%d ip:%s", i, tscMgmtIpList.ipstr[i]);
  }
}
#endif

/*
 * For each management node, try twice at least in case of poor network situation.
 * If the client start to connect to a non-management node from the client, and the first retry may fail due to
 * the poor network quality. And then, the second retry get the response with redirection command.
 * The retry will not be executed since only *two* retry is allowed in case of single management node in the cluster.
 * Therefore, we need to multiply the retry times by factor of 2 to fix this problem.
 */
static int32_t tscGetMgmtConnMaxRetryTimes() {
  int32_t factor = 2;
#ifdef CLUSTER
  return tscMgmtIpList.numOfIps * factor;
#else
  return 1*factor;
#endif
}

void tscProcessHeartBeatRsp(void *param, TAOS_RES *tres, int code) {
  STscObj *pObj = (STscObj *)param;
  if (pObj == NULL) return;
  if (pObj != pObj->signature) {
    tscError("heart beat msg, pObj:%p, signature:%p invalid", pObj, pObj->signature);
    return;
  }

  SSqlObj *pSql = pObj->pHb;
  SSqlRes *pRes = &pSql->res;

  if (code == 0) {
    SHeartBeatRsp *pRsp = (SHeartBeatRsp *)pRes->pRsp;
#ifdef CLUSTER
    SIpList *      pIpList = &pRsp->ipList;
    tscMgmtIpList.numOfIps = pIpList->numOfIps;
    if (memcmp(tscMgmtIpList.ip, pIpList->ip, pIpList->numOfIps * 4) != 0) {
      for (int i = 0; i < pIpList->numOfIps; ++i) {
        tinet_ntoa(tscMgmtIpList.ipstr[i], pIpList->ip[i]);
        tscMgmtIpList.ip[i] = pIpList->ip[i];
      }
      tscTrace("new mgmt IP list:");
      tscPrintMgmtIp();
    }
#endif
    if (pRsp->killConnection) {
      tscKillConnection(pObj);
    } else {
      if (pRsp->queryId) tscKillQuery(pObj, pRsp->queryId);
      if (pRsp->streamId) tscKillStream(pObj, pRsp->streamId);
    }
  } else {
    tscTrace("heart beat failed, code:%d", code);
  }

  taosTmrReset(tscProcessActivityTimer, tsShellActivityTimer * 500, pObj, tscTmr, &pObj->pTimer);
}

void tscProcessActivityTimer(void *handle, void *tmrId) {
  STscObj *pObj = (STscObj *)handle;

  if (pObj == NULL) return;
  if (pObj->signature != pObj) return;
  if (pObj->pTimer != tmrId) return;

  if (pObj->pHb == NULL) {
    SSqlObj *pSql = (SSqlObj *)calloc(1, sizeof(SSqlObj));
    if (NULL == pSql) return;

    pSql->fp = tscProcessHeartBeatRsp;
    pSql->cmd.command = TSDB_SQL_HB;
    if (TSDB_CODE_SUCCESS != tscAllocPayload(&(pSql->cmd), TSDB_DEFAULT_PAYLOAD_SIZE)) {
      tfree(pSql);
      return;
    }

    pSql->param = pObj;
    pSql->pTscObj = pObj;
    pSql->signature = pSql;
    pObj->pHb = pSql;
    tscTrace("%p pHb is allocated, pObj:%p", pObj->pHb, pObj);
  }

  if (tscShouldFreeHeatBeat(pObj->pHb)) {
    tscTrace("%p free HB object and release connection, pConn:%p", pObj, pObj->pHb->thandle);
    taosCloseRpcConn(pObj->pHb->thandle);

    tscFreeSqlObj(pObj->pHb);
    tscCloseTscObj(pObj);
    return;
  }

  tscProcessSql(pObj->pHb);
}

void tscGetConnToMgmt(SSqlObj *pSql, uint8_t *pCode) {
  STscObj *pTscObj = pSql->pTscObj;
#ifdef CLUSTER
  if (pSql->retry < tscGetMgmtConnMaxRetryTimes()) {
    *pCode = 0;
    pSql->retry++;
    pSql->index = pSql->index % tscMgmtIpList.numOfIps;
    if (pSql->cmd.command > TSDB_SQL_READ && pSql->index == 0) pSql->index = 1;
    void *thandle = taosGetConnFromCache(tscConnCache, tscMgmtIpList.ip[pSql->index], TSC_MGMT_VNODE, pTscObj->user);
#else
  if (pSql->retry < tscGetMgmtConnMaxRetryTimes()) {
    *pCode = 0;
    pSql->retry++;
    void *thandle = taosGetConnFromCache(tscConnCache, tsServerIp, TSC_MGMT_VNODE, pTscObj->user);
#endif

    if (thandle == NULL) {
      SRpcConnInit connInit;
      memset(&connInit, 0, sizeof(connInit));
      connInit.cid = 0;
      connInit.sid = 0;
      connInit.meterId = pSql->pTscObj->user;
      connInit.peerId = 0;
      connInit.shandle = pTscMgmtConn;
      connInit.ahandle = pSql;
      connInit.peerPort = tsMgmtShellPort;
      connInit.spi = 1;
      connInit.encrypt = 0;
      connInit.secret = pSql->pTscObj->pass;
      
#ifdef CLUSTER
      connInit.peerIp = tscMgmtIpList.ipstr[pSql->index];
#else
	    connInit.peerIp = tsServerIpStr;
#endif
      thandle = taosOpenRpcConn(&connInit, pCode);
    }

    pSql->thandle = thandle;
#ifdef CLUSTER
    pSql->ip = tscMgmtIpList.ip[pSql->index];
    pSql->vnode = TSC_MGMT_VNODE;
    tscTrace("%p mgmt index:%d ip:0x%x is picked up, pConn:%p", pSql, pSql->index, tscMgmtIpList.ip[pSql->index],
             pSql->thandle);
#else
    pSql->ip = tsServerIp;
    pSql->vnode = TSC_MGMT_VNODE;
#endif
  }
  
  // the pSql->res.code is the previous error(status) code.
  if (pSql->thandle == NULL && pSql->retry >= pSql->maxRetry) {
    if (pSql->res.code != TSDB_CODE_SUCCESS && pSql->res.code != TSDB_CODE_ACTION_IN_PROGRESS) {
      *pCode = pSql->res.code;
    }
    
    tscError("%p reach the max retry:%d, code:%d", pSql, pSql->retry, *pCode);
  }
}

void tscGetConnToVnode(SSqlObj *pSql, uint8_t *pCode) {
  char        ipstr[40] = {0};
  SVPeerDesc *pVPeersDesc = NULL;
  static int  vidIndex = 0;
  STscObj *   pTscObj = pSql->pTscObj;

  pSql->thandle = NULL;

  SSqlCmd *       pCmd = &pSql->cmd;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  if (UTIL_METER_IS_METRIC(pMeterMetaInfo)) {  // multiple vnode query
    SVnodeSidList *vnodeList = tscGetVnodeSidList(pMeterMetaInfo->pMetricMeta, pCmd->vnodeIdx);
    if (vnodeList != NULL) {
      pVPeersDesc = vnodeList->vpeerDesc;
    }
  } else {
    SMeterMeta *pMeta = pMeterMetaInfo->pMeterMeta;
    if (pMeta == NULL) {
      tscError("%p pMeterMeta is NULL", pSql);
      pSql->retry = pSql->maxRetry;
      return;
    }
    pVPeersDesc = pMeta->vpeerDesc;
  }

  if (pVPeersDesc == NULL) {
    pSql->retry = pSql->maxRetry;
    tscError("%p pVPeerDesc is NULL", pSql);
  }

  while (pSql->retry < pSql->maxRetry) {
    (pSql->retry)++;
#ifdef CLUSTER
    if (pVPeersDesc[pSql->index].ip == 0) {
      (pSql->index) = (pSql->index + 1) % TSDB_VNODES_SUPPORT;
      continue;
    }
    *pCode = TSDB_CODE_SUCCESS;

    void *thandle =
        taosGetConnFromCache(tscConnCache, pVPeersDesc[pSql->index].ip, pVPeersDesc[pSql->index].vnode, pTscObj->user);

    if (thandle == NULL) {
      SRpcConnInit connInit;
      tinet_ntoa(ipstr, pVPeersDesc[pSql->index].ip);
      memset(&connInit, 0, sizeof(connInit));
      connInit.cid = vidIndex;
      connInit.sid = 0;
      connInit.spi = 0;
      connInit.encrypt = 0;
      connInit.meterId = pSql->pTscObj->user;
      connInit.peerId = htonl((pVPeersDesc[pSql->index].vnode << TSDB_SHELL_VNODE_BITS));
      connInit.shandle = pVnodeConn;
      connInit.ahandle = pSql;
      connInit.peerIp = ipstr;
      connInit.peerPort = tsVnodeShellPort;
      thandle = taosOpenRpcConn(&connInit, pCode);
      vidIndex = (vidIndex + 1) % tscNumOfThreads;
    }

    pSql->thandle = thandle;
    pSql->ip = pVPeersDesc[pSql->index].ip;
    pSql->vnode = pVPeersDesc[pSql->index].vnode;
    tscTrace("%p vnode:%d ip:%p index:%d is picked up, pConn:%p", pSql, pVPeersDesc[pSql->index].vnode,
             pVPeersDesc[pSql->index].ip, pSql->index, pSql->thandle);
#else
    *pCode = 0;
    void *thandle = taosGetConnFromCache(tscConnCache, tsServerIp, pVPeersDesc[0].vnode, pTscObj->user);

    if (thandle == NULL) {
      SRpcConnInit connInit;
      memset(&connInit, 0, sizeof(connInit));
      connInit.cid = vidIndex;
      connInit.sid = 0;
      connInit.spi = 0;
      connInit.encrypt = 0;
      connInit.meterId = pSql->pTscObj->user;
      connInit.peerId = htonl((pVPeersDesc[0].vnode << TSDB_SHELL_VNODE_BITS));
      connInit.shandle = pVnodeConn;
      connInit.ahandle = pSql;
      connInit.peerIp = tsServerIpStr;
      connInit.peerPort = tsVnodeShellPort;
      thandle = taosOpenRpcConn(&connInit, pCode);
      vidIndex = (vidIndex + 1) % tscNumOfThreads;
    }

    pSql->thandle = thandle;
    pSql->ip = tsServerIp;
    pSql->vnode = pVPeersDesc[0].vnode;
#endif

    break;
  }
  
  // the pSql->res.code is the previous error(status) code.
  if (pSql->thandle == NULL && pSql->retry >= pSql->maxRetry) {
    if (pSql->res.code != TSDB_CODE_SUCCESS && pSql->res.code != TSDB_CODE_ACTION_IN_PROGRESS) {
      *pCode = pSql->res.code;
    }
    
    tscError("%p reach the max retry:%d, code:%d", pSql, pSql->retry, *pCode);
  }
}

int tscSendMsgToServer(SSqlObj *pSql) {
  uint8_t code = TSDB_CODE_NETWORK_UNAVAIL;

  if (pSql->thandle == NULL) {
    if (pSql->cmd.command < TSDB_SQL_MGMT)
      tscGetConnToVnode(pSql, &code);
    else
      tscGetConnToMgmt(pSql, &code);
  }

  if (pSql->thandle) {
    /*
     * the total length of message
     * rpc header + actual message body + digest
     *
     * the pSql object may be released automatically during insert procedure, in which the access of
     * message body by using "if (pHeader->msgType & 1)" may cause the segment fault.
     *
     */
    size_t totalLen = pSql->cmd.payloadLen + tsRpcHeadSize + sizeof(STaosDigest);

    // the memory will be released by taosProcessResponse, so no memory leak here
    char *buf = malloc(totalLen);
    if (NULL == buf) {
      tscError("%p msg:%s malloc fail", pSql, taosMsg[pSql->cmd.msgType]);
      return TSDB_CODE_CLI_OUT_OF_MEMORY;
    }
    memcpy(buf, pSql->cmd.payload, totalLen);

    tscTrace("%p msg:%s is sent to server", pSql, taosMsg[pSql->cmd.msgType]);

    char *pStart = taosBuildReqHeader(pSql->thandle, pSql->cmd.msgType, buf);
    if (pStart) {
      /*
       * this SQL object may be released by other thread due to the completion of this query even before the log
       * is dumped to log file. So the signature needs to be kept in a local variable.
       */
      uint64_t signature = (uint64_t) pSql->signature;
      if (tscUpdateVnodeMsg[pSql->cmd.command]) (*tscUpdateVnodeMsg[pSql->cmd.command])(pSql, buf);
      
      int ret = taosSendMsgToPeerH(pSql->thandle, pStart, pSql->cmd.payloadLen, pSql);
      if (ret >= 0) {
        code = 0;
      }
      
      tscTrace("%p send msg ret:%d code:%d sig:%p", pSql, ret, code, signature);
    }
  }

  return code;
}

#ifdef CLUSTER
void tscProcessMgmtRedirect(SSqlObj *pSql, uint8_t *cont) {
  SIpList *pIpList = (SIpList *)(cont);
  tscMgmtIpList.numOfIps = pIpList->numOfIps;
  for (int i = 0; i < pIpList->numOfIps; ++i) {
    tinet_ntoa(tscMgmtIpList.ipstr[i], pIpList->ip[i]);
    tscMgmtIpList.ip[i] = pIpList->ip[i];
    tscTrace("Update mgmt Ip, index:%d ip:%s", i, tscMgmtIpList.ipstr[i]);
  }

  if (pSql->cmd.command < TSDB_SQL_READ) {
    tsMasterIndex = 0;
    pSql->index = 0;
  } else {
    pSql->index++;
  }

  tscPrintMgmtIp();
}
#endif

void *tscProcessMsgFromServer(char *msg, void *ahandle, void *thandle) {
  if (ahandle == NULL) return NULL;

  SIntMsg *pMsg = (SIntMsg *)msg;
  SSqlObj *pSql = (SSqlObj *)ahandle;
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;
  STscObj *pObj = pSql->pTscObj;
  int      code = TSDB_CODE_NETWORK_UNAVAIL;

  if (pSql->signature != pSql) {
    tscError("%p sql is already released, signature:%p", pSql, pSql->signature);
    return NULL;
  }

  if (pSql->thandle != thandle) {
    tscError("%p thandle:%p is different from received:%p", pSql, pSql->thandle, thandle);
    return NULL;
  }

  tscTrace("%p msg:%p is received from server, pConn:%p", pSql, msg, thandle);

  if (pSql->freed || pObj->signature != pObj) {
    tscTrace("%p sql is already released or DB connection is closed, freed:%d pObj:%p signature:%p", pSql, pSql->freed,
             pObj, pObj->signature);
    taosAddConnIntoCache(tscConnCache, pSql->thandle, pSql->ip, pSql->vnode, pObj->user);
    tscFreeSqlObj(pSql);
    return ahandle;
  }

  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);
  if (msg == NULL) {
    tscTrace("%p no response from ip:0x%x", pSql, pSql->ip);
	
#ifdef CLUSTER
    pSql->index++;
#else
    // for single node situation, do NOT try next index
#endif
    pSql->thandle = NULL;
    // todo taos_stop_query() in async model
    /*
     * in case of
     * 1. query cancelled(pRes->code != TSDB_CODE_QUERY_CANCELLED), do NOT re-issue the request to server.
     * 2. retrieve, do NOT re-issue the retrieve request since the qhandle may have been released by server
     */
    if (pCmd->command != TSDB_SQL_FETCH && pCmd->command != TSDB_SQL_RETRIEVE && pCmd->command != TSDB_SQL_KILL_QUERY &&
        pRes->code != TSDB_CODE_QUERY_CANCELLED) {
      code = tscSendMsgToServer(pSql);
      if (code == 0) return NULL;
    }

    // renew meter meta in case it is changed
    if (pCmd->command < TSDB_SQL_FETCH && pRes->code != TSDB_CODE_QUERY_CANCELLED) {
#ifdef CLUSTER
      pSql->maxRetry = TSDB_VNODES_SUPPORT * 2;
#else
      // for fetch, it shall not renew meter meta
      pSql->maxRetry = 2;
#endif
      code = tscRenewMeterMeta(pSql, pMeterMetaInfo->name);
      pRes->code = code;
      if (code == TSDB_CODE_ACTION_IN_PROGRESS) return pSql;

      if (pMeterMetaInfo->pMeterMeta) {
        code = tscSendMsgToServer(pSql);
        if (code == 0) return pSql;
      }
    }
  } else {
    uint16_t rspCode = pMsg->content[0];
    
#ifdef CLUSTER
    
    if (rspCode == TSDB_CODE_REDIRECT) {
      tscTrace("%p it shall be redirected!", pSql);
      taosAddConnIntoCache(tscConnCache, thandle, pSql->ip, pSql->vnode, pObj->user);
      pSql->thandle = NULL;

      if (pCmd->command > TSDB_SQL_MGMT) {
        tscProcessMgmtRedirect(pSql, pMsg->content + 1);
      } else if (pCmd->command == TSDB_SQL_INSERT){
        pSql->index++;
        pSql->maxRetry = TSDB_VNODES_SUPPORT * 2;
      } else {
        pSql->index++;
      }

      code = tscSendMsgToServer(pSql);
      if (code == 0) return pSql;
      msg = NULL;
    } else if (rspCode == TSDB_CODE_NOT_ACTIVE_TABLE || rspCode == TSDB_CODE_INVALID_TABLE_ID ||
        rspCode == TSDB_CODE_INVALID_VNODE_ID || rspCode == TSDB_CODE_NOT_ACTIVE_VNODE ||
        rspCode == TSDB_CODE_NETWORK_UNAVAIL) {
#else
     if (rspCode == TSDB_CODE_NOT_ACTIVE_TABLE || rspCode == TSDB_CODE_INVALID_TABLE_ID ||
        rspCode == TSDB_CODE_INVALID_VNODE_ID || rspCode == TSDB_CODE_NOT_ACTIVE_VNODE ||
        rspCode == TSDB_CODE_NETWORK_UNAVAIL) {
#endif
      pSql->thandle = NULL;
      taosAddConnIntoCache(tscConnCache, thandle, pSql->ip, pSql->vnode, pObj->user);
      
      if ((pCmd->command == TSDB_SQL_INSERT || pCmd->command == TSDB_SQL_SELECT) &&
          (rspCode == TSDB_CODE_INVALID_TABLE_ID || rspCode == TSDB_CODE_INVALID_VNODE_ID)) {
        /*
         * In case of the insert/select operations, the invalid table(vnode) id means
         * the submit/query msg is invalid, renew meter meta will not help to fix this problem,
         * so return the invalid_query_msg to client directly.
         */
        code = TSDB_CODE_INVALID_QUERY_MSG;
      } else if (pCmd->command == TSDB_SQL_CONNECT) {
        code = TSDB_CODE_NETWORK_UNAVAIL;
      } else if (pCmd->command == TSDB_SQL_HB) {
        code = TSDB_CODE_NOT_READY;
      } else {
        tscTrace("%p it shall renew meter meta, code:%d", pSql, rspCode);
        
        pSql->maxRetry = TSDB_VNODES_SUPPORT * 2;
        pSql->res.code = (uint8_t) rspCode;  // keep the previous error code
        
        code = tscRenewMeterMeta(pSql, pMeterMetaInfo->name);
        if (code == TSDB_CODE_ACTION_IN_PROGRESS) return pSql;

        if (pMeterMetaInfo->pMeterMeta) {
          code = tscSendMsgToServer(pSql);
          if (code == 0) return pSql;
        }
      }

      msg = NULL;
    } else {  // for other error set and return to invoker
      code = rspCode;
    }
  }

  pSql->retry = 0;

  if (msg) {
    if (pCmd->command < TSDB_SQL_MGMT) {
      if (UTIL_METER_IS_NOMRAL_METER(pMeterMetaInfo)) {
        if (pMeterMetaInfo->pMeterMeta)  // it may be deleted
          pMeterMetaInfo->pMeterMeta->index = pSql->index;
      } else {
        SVnodeSidList *pVnodeSidList = tscGetVnodeSidList(pMeterMetaInfo->pMetricMeta, pSql->cmd.vnodeIdx);
        pVnodeSidList->index = pSql->index;
      }
    } else {
      if (pCmd->command > TSDB_SQL_READ)
        tsSlaveIndex = pSql->index;
      else
        tsMasterIndex = pSql->index;
    }
  }

  if (pSql->fp == NULL) tsem_wait(&pSql->emptyRspSem);

  pRes->rspLen = 0;
  if (pRes->code != TSDB_CODE_QUERY_CANCELLED) {
    pRes->code = (code != TSDB_CODE_SUCCESS) ? code : TSDB_CODE_NETWORK_UNAVAIL;
  } else {
    tscTrace("%p query is cancelled, code:%d", pSql, pRes->code);
  }

  if (msg && pRes->code != TSDB_CODE_QUERY_CANCELLED) {
    assert(pMsg->msgType == pCmd->msgType + 1);
    pRes->code = pMsg->content[0];
    pRes->rspType = pMsg->msgType;
    pRes->rspLen = pMsg->msgLen - sizeof(SIntMsg);

    char *tmp = (char *)realloc(pRes->pRsp, pRes->rspLen);
    if (tmp == NULL) {
      pRes->code = TSDB_CODE_CLI_OUT_OF_MEMORY;
    } else {
      pRes->pRsp = tmp;
      if (pRes->rspLen) {
        memcpy(pRes->pRsp, pMsg->content + 1, pRes->rspLen - 1);
      }
    }

    // ignore the error information returned from mnode when set ignore flag in sql
    if (pRes->code == TSDB_CODE_DB_ALREADY_EXIST && pCmd->existsCheck && pRes->rspType == TSDB_MSG_TYPE_CREATE_DB_RSP) {
      pRes->code = TSDB_CODE_SUCCESS;
    }

    /*
     * There is not response callback function for submit response.
     * The actual inserted number of points is the first number.
     */
    if (pMsg->msgType == TSDB_MSG_TYPE_SUBMIT_RSP) {
      pRes->numOfRows += *(int32_t *)pRes->pRsp;

      tscTrace("%p cmd:%d code:%d, inserted rows:%d, rsp len:%d", pSql, pCmd->command, pRes->code,
               *(int32_t *)pRes->pRsp, pRes->rspLen);
    } else {
      tscTrace("%p cmd:%d code:%d rsp len:%d", pSql, pCmd->command, pRes->code, pRes->rspLen);
    }
  }

  if (tscKeepConn[pCmd->command] == 0 ||
      (pRes->code != TSDB_CODE_SUCCESS && pRes->code != TSDB_CODE_ACTION_IN_PROGRESS)) {
    if (pSql->thandle != NULL) {
      taosAddConnIntoCache(tscConnCache, pSql->thandle, pSql->ip, pSql->vnode, pObj->user);
      pSql->thandle = NULL;
    }
  }

  if (pSql->fp == NULL) {
    tsem_post(&pSql->rspSem);
  } else {
    if (pRes->code == TSDB_CODE_SUCCESS && tscProcessMsgRsp[pCmd->command])
      code = (*tscProcessMsgRsp[pCmd->command])(pSql);

    if (code != TSDB_CODE_ACTION_IN_PROGRESS) {
      int   command = pCmd->command;
      void *taosres = tscKeepConn[command] ? pSql : NULL;
      code = pRes->code ? -pRes->code : pRes->numOfRows;

      tscTrace("%p Async SQL result:%d res:%p", pSql, code, taosres);

      /*
       * Whether to free sqlObj or not should be decided before call the user defined function, since this SqlObj
       * may be freed in UDF, and reused by other threads before tscShouldFreeAsyncSqlObj called, in which case
       * tscShouldFreeAsyncSqlObj checks an object which is actually allocated by other threads.
       *
       * If this block of memory is re-allocated for an insert thread, in which tscKeepConn[command] equals to 0,
       * the tscShouldFreeAsyncSqlObj will success and tscFreeSqlObj free it immediately.
       */
      bool shouldFree = tscShouldFreeAsyncSqlObj(pSql);
      if (command == TSDB_SQL_INSERT) {  // handle multi-vnode insertion situation
        (*pSql->fp)(pSql, taosres, code);
      } else {
        (*pSql->fp)(pSql->param, taosres, code);
      }

      if (shouldFree) {
        // If it is failed, all objects allocated during execution taos_connect_a should be released
        if (command == TSDB_SQL_CONNECT) {
          taos_close(pObj);
          tscTrace("%p Async sql close failed connection", pSql);
        } else {
          tscFreeSqlObj(pSql);
          tscTrace("%p Async sql is automatically freed", pSql);
        }
      }
    }
  }

  return ahandle;
}

static SSqlObj *tscCreateSqlObjForSubquery(SSqlObj *pSql, SRetrieveSupport *trsupport, SSqlObj *prevSqlObj);
static int tscLaunchMetricSubQueries(SSqlObj *pSql);

// todo merge with callback
int32_t tscLaunchJoinSubquery(SSqlObj *pSql, int16_t tableIndex, int16_t vnodeIdx, SJoinSubquerySupporter *pSupporter) {
  SSqlCmd *pCmd = &pSql->cmd;

  pSql->res.qhandle = 0x1;
  pSql->res.numOfRows = 0;

  if (pSql->pSubs == NULL) {
    pSql->pSubs = malloc(POINTER_BYTES * pSupporter->pState->numOfTotal);
    if (pSql->pSubs == NULL) {
      return TSDB_CODE_CLI_OUT_OF_MEMORY;
    }
  }

  SSqlObj *pNew = createSubqueryObj(pSql, vnodeIdx, tableIndex, tscJoinQueryCallback, pSupporter, NULL);
  if (pNew == NULL) {
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }

  pSql->pSubs[pSql->numOfSubs++] = pNew;

  if (QUERY_IS_JOIN_QUERY(pCmd->type)) {
    addGroupInfoForSubquery(pSql, pNew, tableIndex);

    // refactor as one method
    tscColumnBaseInfoUpdateTableIndex(&pNew->cmd.colList, 0);
    tscColumnBaseInfoCopy(&pSupporter->colList, &pNew->cmd.colList, 0);

    tscSqlExprCopy(&pSupporter->exprsInfo, &pNew->cmd.exprsInfo, pSupporter->uid);

    tscFieldInfoCopyAll(&pNew->cmd.fieldsInfo, &pSupporter->fieldsInfo);
    tscTagCondCopy(&pSupporter->tagCond, &pNew->cmd.tagCond);
    pSupporter->groupbyExpr = pNew->cmd.groupbyExpr;

    pNew->cmd.numOfCols = 0;
    pNew->cmd.nAggTimeInterval = 0;
    memset(&pNew->cmd.limit, 0, sizeof(SLimitVal));
    memset(&pNew->cmd.groupbyExpr, 0, sizeof(SSqlGroupbyExpr));

    // set the ts,tags that involved in join, as the output column of intermediate result
    tscFreeSqlCmdData(&pNew->cmd);

    SSchema      colSchema = {.type = TSDB_DATA_TYPE_BINARY, .bytes = 1};
    SColumnIndex index = {0, PRIMARYKEY_TIMESTAMP_COL_INDEX};

    tscAddSpecialColumnForSelect(&pNew->cmd, 0, TSDB_FUNC_TS_COMP, &index, &colSchema, TSDB_COL_NORMAL);

    // set the tags value for ts_comp function
    SSqlExpr *pExpr = tscSqlExprGet(&pNew->cmd, 0);

    SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(&pNew->cmd, 0);
    int16_t         tagColIndex = tscGetJoinTagColIndexByUid(&pNew->cmd, pMeterMetaInfo->pMeterMeta->uid);

    pExpr->param->i64Key = tagColIndex;
    pExpr->numOfParams = 1;

    addRequiredTagColumn(pCmd, tagColIndex, 0);

    // add the filter tag column
    for (int32_t i = 0; i < pSupporter->colList.numOfCols; ++i) {
      SColumnBase *pColBase = &pSupporter->colList.pColList[i];
      if (pColBase->numOfFilters > 0) {  // copy to the pNew->cmd.colList if it is filtered.
        tscColumnBaseCopy(&pNew->cmd.colList.pColList[pNew->cmd.colList.numOfCols], pColBase);
        pNew->cmd.colList.numOfCols++;
      }
    }
  } else {
    pNew->cmd.type |= TSDB_QUERY_TYPE_SUBQUERY;
  }

  return tscProcessSql(pNew);
}

int doProcessSql(SSqlObj *pSql) {
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  int32_t code = TSDB_CODE_SUCCESS;

  void *asyncFp = pSql->fp;
  if (tscBuildMsg[pCmd->command](pSql) < 0) {  // build msg failed
    code = TSDB_CODE_APP_ERROR;
  } else {
    code = tscSendMsgToServer(pSql);
  }
  if (asyncFp) {
    if (code != 0) {
      pRes->code = code;
      tscQueueAsyncRes(pSql);
    }
    return 0;
  }

  if (code != 0) {
    pRes->code = code;
    return code;
  }

  tsem_wait(&pSql->rspSem);

  if (pRes->code == 0 && tscProcessMsgRsp[pCmd->command]) (*tscProcessMsgRsp[pCmd->command])(pSql);

  tsem_post(&pSql->emptyRspSem);

  return pRes->code;
}

int tscProcessSql(SSqlObj *pSql) {
  char *          name = NULL;
  SSqlRes *       pRes = &pSql->res;
  SSqlCmd *       pCmd = &pSql->cmd;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  if (pMeterMetaInfo != NULL) {
    name = pMeterMetaInfo->name;
  }

  tscTrace("%p SQL cmd:%d will be processed, name:%s, type:%d", pSql, pSql->cmd.command, name, pSql->cmd.type);
  pSql->retry = 0;
  if (pSql->cmd.command < TSDB_SQL_MGMT) {
#ifdef CLUSTER
    pSql->maxRetry = TSDB_VNODES_SUPPORT;
#else
    pSql->maxRetry = 2;
#endif
    
    // the pMeterMetaInfo cannot be NULL
    if (pMeterMetaInfo == NULL) {
      pSql->res.code = TSDB_CODE_OTHERS;
      return pSql->res.code;
    }
    
    if (UTIL_METER_IS_NOMRAL_METER(pMeterMetaInfo)) {
      pSql->index = pMeterMetaInfo->pMeterMeta->index;
    } else {  // it must be the parent SSqlObj for super table query
      if ((pSql->cmd.type & TSDB_QUERY_TYPE_SUBQUERY) != 0) {
        int32_t        idx = pSql->cmd.vnodeIdx;
        SVnodeSidList *pSidList = tscGetVnodeSidList(pMeterMetaInfo->pMetricMeta, idx);
        pSql->index = pSidList->index;
      }
    }
  } else if (pSql->cmd.command < TSDB_SQL_LOCAL) {
    pSql->index = pSql->cmd.command < TSDB_SQL_READ ? tsMasterIndex : tsSlaveIndex;
  } else {  // local handler
    return (*tscProcessMsgRsp[pCmd->command])(pSql);
  }

  // todo handle async situation
  if (QUERY_IS_JOIN_QUERY(pSql->cmd.type)) {
    if ((pSql->cmd.type & TSDB_QUERY_TYPE_SUBQUERY) == 0) {
      SSubqueryState *pState = calloc(1, sizeof(SSubqueryState));
      pState->numOfTotal = pSql->cmd.numOfTables;

      for (int32_t i = 0; i < pSql->cmd.numOfTables; ++i) {
        SJoinSubquerySupporter *pSupporter = tscCreateJoinSupporter(pSql, pState, i);

        if (pSupporter == NULL) {  // failed to create support struct, abort current query
          tscError("%p tableIndex:%d, failed to allocate join support object, abort further query", pSql, i);
          pState->numOfCompleted = pSql->cmd.numOfTables - i - 1;
          pSql->res.code = TSDB_CODE_CLI_OUT_OF_MEMORY;

          return pSql->res.code;
        }

        int32_t code = tscLaunchJoinSubquery(pSql, i, 0, pSupporter);
        if (code != TSDB_CODE_SUCCESS) {  // failed to create subquery object, quit query
          tscDestroyJoinSupporter(pSupporter);
          pSql->res.code = TSDB_CODE_CLI_OUT_OF_MEMORY;

          break;
        }
      }

      sem_post(&pSql->emptyRspSem);
      sem_wait(&pSql->rspSem);

      sem_post(&pSql->emptyRspSem);

      if (pSql->numOfSubs <= 0) {
        pSql->cmd.command = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
      } else {
        pSql->cmd.command = TSDB_SQL_METRIC_JOIN_RETRIEVE;
      }

      return TSDB_CODE_SUCCESS;
    } else {
      // for first stage sub query, iterate all vnodes to get all timestamp
      if ((pSql->cmd.type & TSDB_QUERY_TYPE_JOIN_SEC_STAGE) != TSDB_QUERY_TYPE_JOIN_SEC_STAGE) {
        return doProcessSql(pSql);
      }
    }
  }

  if (tscIsTwoStageMergeMetricQuery(pCmd)) {
    /*
     * (ref. line: 964)
     * Before this function returns from tscLaunchMetricSubQueries and continues, pSql may have been released at user
     * program context after retrieving all data from vnodes. User function is called at tscRetrieveFromVnodeCallBack.
     *
     * when pSql being released, pSql->fp == NULL, it may pass the check of pSql->fp == NULL,
     * which causes deadlock. So we keep it as local variable.
     */
    void *fp = pSql->fp;

    if (tscLaunchMetricSubQueries(pSql) != TSDB_CODE_SUCCESS) {
      return pRes->code;
    }

    if (fp == NULL) {
      sem_post(&pSql->emptyRspSem);
      sem_wait(&pSql->rspSem);
      sem_post(&pSql->emptyRspSem);

      // set the command flag must be after the semaphore been correctly set.
      pSql->cmd.command = TSDB_SQL_RETRIEVE_METRIC;
    }

    return pSql->res.code;
  }

  return doProcessSql(pSql);
}

static void doCleanupSubqueries(SSqlObj *pSql, int32_t vnodeIndex, int32_t numOfVnodes, SRetrieveSupport *pTrs,
                                tOrderDescriptor *pDesc, tColModel *pModel, tExtMemBuffer **pMemoryBuf,
                                SSubqueryState *pState) {
  pSql->cmd.command = TSDB_SQL_RETRIEVE_METRIC;
  pSql->res.code = TSDB_CODE_CLI_OUT_OF_MEMORY;

  /*
   * if i > 0, at least one sub query is issued, the allocated resource is
   * freed by it when subquery completed.
   */
  if (vnodeIndex == 0) {
    tscLocalReducerEnvDestroy(pMemoryBuf, pDesc, pModel, numOfVnodes);
    tfree(pState);

    if (pTrs != NULL) {
      tfree(pTrs->localBuffer);

      pthread_mutex_unlock(&pTrs->queryMutex);
      pthread_mutex_destroy(&pTrs->queryMutex);
      tfree(pTrs);
    }
  }
}

int tscLaunchMetricSubQueries(SSqlObj *pSql) {
  SSqlRes *pRes = &pSql->res;

  // pRes->code check only serves in launching metric sub-queries
  if (pRes->code == TSDB_CODE_QUERY_CANCELLED) {
    pSql->cmd.command = TSDB_SQL_RETRIEVE_METRIC;  // enable the abort of kill metric function.
    return pSql->res.code;
  }

  tExtMemBuffer **  pMemoryBuf = NULL;
  tOrderDescriptor *pDesc = NULL;
  tColModel *       pModel = NULL;

  pRes->qhandle = 1;  // hack the qhandle check

  const uint32_t  nBufferSize = (1 << 16);  // 64KB
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(&pSql->cmd, 0);
  int32_t         numOfVnodes = pMeterMetaInfo->pMetricMeta->numOfVnodes;
  assert(numOfVnodes > 0);

  int32_t ret = tscLocalReducerEnvCreate(pSql, &pMemoryBuf, &pDesc, &pModel, nBufferSize);
  if (ret != 0) {
    pRes->code = TSDB_CODE_CLI_OUT_OF_MEMORY;
    if (pSql->fp) {
      tscQueueAsyncRes(pSql);
    }
    return pRes->code;
  }

  pSql->pSubs = malloc(POINTER_BYTES * numOfVnodes);
  pSql->numOfSubs = numOfVnodes;

  tscTrace("%p retrieved query data from %d vnode(s)", pSql, numOfVnodes);
  SSubqueryState *pState = calloc(1, sizeof(SSubqueryState));
  pState->numOfTotal = numOfVnodes;
  pRes->code = TSDB_CODE_SUCCESS;

  for (int32_t i = 0; i < numOfVnodes; ++i) {
    if (pRes->code == TSDB_CODE_QUERY_CANCELLED || pRes->code == TSDB_CODE_CLI_OUT_OF_MEMORY) {
      /*
       * during launch sub queries, if the master query is cancelled. the remain is ignored and set the retrieveDoneRec
       * to the value of remaining not built sub-queries. So, the already issued sub queries can successfully free
       * allocated resources.
       */
      pState->numOfCompleted = (numOfVnodes - i);
      doCleanupSubqueries(pSql, i, numOfVnodes, NULL, pDesc, pModel, pMemoryBuf, pState);

      if (i == 0) {
        return pSql->res.code;
      }

      break;
    }

    SRetrieveSupport *trs = (SRetrieveSupport *)calloc(1, sizeof(SRetrieveSupport));
    trs->pExtMemBuffer = pMemoryBuf;
    trs->pOrderDescriptor = pDesc;
    trs->pState = pState;
    trs->localBuffer = (tFilePage *)calloc(1, nBufferSize + sizeof(tFilePage));
    trs->vnodeIdx = i;
    trs->pParentSqlObj = pSql;
    trs->pFinalColModel = pModel;

    pthread_mutexattr_t mutexattr = {0};
    pthread_mutexattr_settype(&mutexattr, PTHREAD_MUTEX_RECURSIVE_NP);
    pthread_mutex_init(&trs->queryMutex, &mutexattr);
    pthread_mutexattr_destroy(&mutexattr);

    SSqlObj *pNew = tscCreateSqlObjForSubquery(pSql, trs, NULL);

    if (pNew == NULL) {
      pState->numOfCompleted = (numOfVnodes - i);
      doCleanupSubqueries(pSql, i, numOfVnodes, trs, pDesc, pModel, pMemoryBuf, pState);

      if (i == 0) {
        return pSql->res.code;
      }

      break;
    }

    // todo handle multi-vnode situation
    if (pSql->cmd.tsBuf) {
      pNew->cmd.tsBuf = tsBufClone(pSql->cmd.tsBuf);
    }

    tscTrace("%p sub:%p launch subquery.orderOfSub:%d", pSql, pNew, pNew->cmd.vnodeIdx);
    tscProcessSql(pNew);
  }

  return TSDB_CODE_SUCCESS;
}

static void tscFreeSubSqlObj(SRetrieveSupport *trsupport, SSqlObj *pSql) {
  tscTrace("%p start to free subquery result", pSql);

  if (pSql->res.code == TSDB_CODE_SUCCESS) {
    taos_free_result(pSql);
  }

  tfree(trsupport->localBuffer);

  pthread_mutex_unlock(&trsupport->queryMutex);
  pthread_mutex_destroy(&trsupport->queryMutex);

  tfree(trsupport);
}

static void tscRetrieveFromVnodeCallBack(void *param, TAOS_RES *tres, int numOfRows);

static void tscAbortFurtherRetryRetrieval(SRetrieveSupport *trsupport, TAOS_RES *tres, int32_t errCode) {
// set no disk space error info
#ifdef WINDOWS
  LPVOID lpMsgBuf;
  FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, NULL,
                GetLastError(), MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),  // Default language
                (LPTSTR)&lpMsgBuf, 0, NULL);
  tscError("sub:%p failed to flush data to disk:reason:%s", tres, lpMsgBuf);
  LocalFree(lpMsgBuf);
#else
  char buf[256] = {0};
  strerror_r(errno, buf, 256);
  tscError("sub:%p failed to flush data to disk:reason:%s", tres, buf);
#endif

  trsupport->pState->code = -errCode;
  trsupport->numOfRetry = MAX_NUM_OF_SUBQUERY_RETRY;

  pthread_mutex_unlock(&trsupport->queryMutex);

  tscRetrieveFromVnodeCallBack(trsupport, tres, trsupport->pState->code);
}

static void tscHandleSubRetrievalError(SRetrieveSupport *trsupport, SSqlObj *pSql, int numOfRows) {
  SSqlObj *pPObj = trsupport->pParentSqlObj;
  int32_t  idx = trsupport->vnodeIdx;

  assert(pSql != NULL);

  /* retrieved in subquery failed. OR query cancelled in retrieve phase. */
  if (trsupport->pState->code == TSDB_CODE_SUCCESS && pPObj->res.code != TSDB_CODE_SUCCESS) {
    trsupport->pState->code = -(int)pPObj->res.code;

    /*
     * kill current sub-query connection, which may retrieve data from vnodes;
     * Here we get: pPObj->res.code == TSDB_CODE_QUERY_CANCELLED
     */
    pSql->res.numOfRows = 0;
    trsupport->numOfRetry = MAX_NUM_OF_SUBQUERY_RETRY;  // disable retry efforts
    tscTrace("%p query is cancelled, sub:%p, orderOfSub:%d abort retrieve, code:%d", trsupport->pParentSqlObj, pSql,
             trsupport->vnodeIdx, trsupport->pState->code);
  }

  if (numOfRows >= 0) {  // current query is successful, but other sub query failed, still abort current query.
    tscTrace("%p sub:%p retrieve numOfRows:%d,orderOfSub:%d", pPObj, pSql, numOfRows, idx);
    tscError("%p sub:%p abort further retrieval due to other queries failure,orderOfSub:%d,code:%d", pPObj, pSql, idx,
             trsupport->pState->code);
  } else {
    if (trsupport->numOfRetry++ < MAX_NUM_OF_SUBQUERY_RETRY && trsupport->pState->code == TSDB_CODE_SUCCESS) {
      /*
       * current query failed, and the retry count is less than the available
       * count, retry query clear previous retrieved data, then launch a new sub query
       */
      tExtMemBufferClear(trsupport->pExtMemBuffer[idx]);

      // clear local saved number of results
      trsupport->localBuffer->numOfElems = 0;
      pthread_mutex_unlock(&trsupport->queryMutex);

      tscTrace("%p sub:%p retrieve failed, code:%d, orderOfSub:%d, retry:%d", trsupport->pParentSqlObj, pSql, numOfRows,
               idx, trsupport->numOfRetry);

      SSqlObj *pNew = tscCreateSqlObjForSubquery(trsupport->pParentSqlObj, trsupport, pSql);
      if (pNew == NULL) {
        tscError("%p sub:%p failed to create new subquery sqlobj due to out of memory, abort retry",
                 trsupport->pParentSqlObj, pSql);

        trsupport->pState->code = TSDB_CODE_CLI_OUT_OF_MEMORY;
        trsupport->numOfRetry = MAX_NUM_OF_SUBQUERY_RETRY;
        return;
      }

      tscProcessSql(pNew);
      return;
    } else {  // reach the maximum retry count, abort
      atomic_val_compare_exchange_32(&trsupport->pState->code, TSDB_CODE_SUCCESS, numOfRows);
      tscError("%p sub:%p retrieve failed,code:%d,orderOfSub:%d failed.no more retry,set global code:%d", pPObj, pSql,
               numOfRows, idx, trsupport->pState->code);
    }
  }

  if (atomic_add_fetch_32(&trsupport->pState->numOfCompleted, 1) < trsupport->pState->numOfTotal) {
    return tscFreeSubSqlObj(trsupport, pSql);
  }

  // all subqueries are failed
  tscError("%p retrieve from %d vnode(s) completed,code:%d.FAILED.", pPObj, trsupport->pState->numOfTotal,
           trsupport->pState->code);
  pPObj->res.code = -(trsupport->pState->code);

  // release allocated resource
  tscLocalReducerEnvDestroy(trsupport->pExtMemBuffer, trsupport->pOrderDescriptor, trsupport->pFinalColModel,
                            trsupport->pState->numOfTotal);

  tfree(trsupport->pState);
  tscFreeSubSqlObj(trsupport, pSql);

  // sync query, wait for the master SSqlObj to proceed
  if (pPObj->fp == NULL) {
    // sync query, wait for the master SSqlObj to proceed
    tsem_wait(&pPObj->emptyRspSem);
    tsem_wait(&pPObj->emptyRspSem);

    tsem_post(&pPObj->rspSem);

    pPObj->cmd.command = TSDB_SQL_RETRIEVE_METRIC;
  } else {
    // in case of second stage join subquery, invoke its callback function instead of regular QueueAsyncRes
    if ((pPObj->cmd.type & TSDB_QUERY_TYPE_JOIN_SEC_STAGE) == TSDB_QUERY_TYPE_JOIN_SEC_STAGE) {
      (*pPObj->fp)(pPObj->param, pPObj, pPObj->res.code);
    } else {  // regular super table query
      if (pPObj->res.code != TSDB_CODE_SUCCESS) {
        tscQueueAsyncRes(pPObj);
      }
    }
  }
}

void tscRetrieveFromVnodeCallBack(void *param, TAOS_RES *tres, int numOfRows) {
  SRetrieveSupport *trsupport = (SRetrieveSupport *)param;
  int32_t           idx = trsupport->vnodeIdx;
  SSqlObj *         pPObj = trsupport->pParentSqlObj;
  tOrderDescriptor *pDesc = trsupport->pOrderDescriptor;

  SSqlObj *pSql = (SSqlObj *)tres;
  if (pSql == NULL) {
    /* sql object has been released in error process, return immediately */
    tscTrace("%p subquery has been released, idx:%d, abort", pPObj, idx);
    return;
  }

  // query process and cancel query process may execute at the same time
  pthread_mutex_lock(&trsupport->queryMutex);

  if (numOfRows < 0 || trsupport->pState->code < 0 || pPObj->res.code != TSDB_CODE_SUCCESS) {
    return tscHandleSubRetrievalError(trsupport, pSql, numOfRows);
  }

  SSqlRes *       pRes = &pSql->res;
  SSqlCmd *       pCmd = &pSql->cmd;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  SVnodeSidList *vnodeInfo = tscGetVnodeSidList(pMeterMetaInfo->pMetricMeta, idx);
  SVPeerDesc *   pSvd = &vnodeInfo->vpeerDesc[vnodeInfo->index];

  if (numOfRows > 0) {
    assert(pRes->numOfRows == numOfRows);
    atomic_add_fetch_64(&trsupport->pState->numOfRetrievedRows, numOfRows);

    tscTrace("%p sub:%p retrieve numOfRows:%d totalNumOfRows:%d from ip:%u,vid:%d,orderOfSub:%d", pPObj, pSql,
             pRes->numOfRows, trsupport->pState->numOfRetrievedRows, pSvd->ip, pSvd->vnode, idx);

#ifdef _DEBUG_VIEW
    printf("received data from vnode: %d rows\n", pRes->numOfRows);
    SSrcColumnInfo colInfo[256] = {0};
    tscGetSrcColumnInfo(colInfo, &pPObj->cmd);
    tColModelDisplayEx(pDesc->pSchema, pRes->data, pRes->numOfRows, pRes->numOfRows, colInfo);
#endif
    if (tsTotalTmpDirGB != 0 && tsAvailTmpDirGB < tsMinimalTmpDirGB) {
      tscError("%p sub:%p client disk space remain %.3f GB, need at least %.3f GB, stop query", pPObj, pSql,
               tsAvailTmpDirGB, tsMinimalTmpDirGB);
      tscAbortFurtherRetryRetrieval(trsupport, tres, TSDB_CODE_CLI_NO_DISKSPACE);
      return;
    }
    int32_t ret = saveToBuffer(trsupport->pExtMemBuffer[idx], pDesc, trsupport->localBuffer, pRes->data,
                               pRes->numOfRows, pCmd->groupbyExpr.orderType);
    if (ret < 0) {
      // set no disk space error info, and abort retry
      tscAbortFurtherRetryRetrieval(trsupport, tres, TSDB_CODE_CLI_NO_DISKSPACE);
    } else {
      pthread_mutex_unlock(&trsupport->queryMutex);
      taos_fetch_rows_a(tres, tscRetrieveFromVnodeCallBack, param);
    }

  } else {  // all data has been retrieved to client
    /* data in from current vnode is stored in cache and disk */
    uint32_t numOfRowsFromVnode =
        trsupport->pExtMemBuffer[pCmd->vnodeIdx]->numOfAllElems + trsupport->localBuffer->numOfElems;
    tscTrace("%p sub:%p all data retrieved from ip:%u,vid:%d, numOfRows:%d, orderOfSub:%d", pPObj, pSql, pSvd->ip,
             pSvd->vnode, numOfRowsFromVnode, idx);

    tColModelCompact(pDesc->pSchema, trsupport->localBuffer, pDesc->pSchema->maxCapacity);

#ifdef _DEBUG_VIEW
    printf("%ld rows data flushed to disk:\n", trsupport->localBuffer->numOfElems);
    SSrcColumnInfo colInfo[256] = {0};
    tscGetSrcColumnInfo(colInfo, &pPObj->cmd);
    tColModelDisplayEx(pDesc->pSchema, trsupport->localBuffer->data, trsupport->localBuffer->numOfElems,
                       trsupport->localBuffer->numOfElems, colInfo);
#endif
    if (tsTotalTmpDirGB != 0 && tsAvailTmpDirGB < tsMinimalTmpDirGB) {
      tscError("%p sub:%p client disk space remain %.3f GB, need at least %.3f GB, stop query", pPObj, pSql,
               tsAvailTmpDirGB, tsMinimalTmpDirGB);
      tscAbortFurtherRetryRetrieval(trsupport, tres, TSDB_CODE_CLI_NO_DISKSPACE);
      return;
    }

    // each result for a vnode is ordered as an independant list,
    // then used as an input of loser tree for disk-based merge routine
    int32_t ret =
        tscFlushTmpBuffer(trsupport->pExtMemBuffer[idx], pDesc, trsupport->localBuffer, pCmd->groupbyExpr.orderType);
    if (ret != 0) {
      /* set no disk space error info, and abort retry */
      return tscAbortFurtherRetryRetrieval(trsupport, tres, TSDB_CODE_CLI_NO_DISKSPACE);
    }

    if (atomic_add_fetch_32(&trsupport->pState->numOfCompleted, 1) < trsupport->pState->numOfTotal) {
      return tscFreeSubSqlObj(trsupport, pSql);
    }

    // all sub-queries are returned, start to local merge process
    pDesc->pSchema->maxCapacity = trsupport->pExtMemBuffer[idx]->numOfElemsPerPage;

    tscTrace("%p retrieve from %d vnodes completed.final NumOfRows:%d,start to build loser tree", pPObj,
             trsupport->pState->numOfTotal, trsupport->pState->numOfCompleted);

    tscClearInterpInfo(&pPObj->cmd);
    tscCreateLocalReducer(trsupport->pExtMemBuffer, trsupport->pState->numOfTotal, pDesc, trsupport->pFinalColModel,
                          &pPObj->cmd, &pPObj->res);
    tscTrace("%p build loser tree completed", pPObj);

    pPObj->res.precision = pSql->res.precision;
    pPObj->res.numOfRows = 0;
    pPObj->res.row = 0;

    // only free once
    free(trsupport->pState);
    tscFreeSubSqlObj(trsupport, pSql);

    if (pPObj->fp == NULL) {
      tsem_wait(&pPObj->emptyRspSem);
      tsem_wait(&pPObj->emptyRspSem);

      tsem_post(&pPObj->rspSem);
    } else {
      // set the command flag must be after the semaphore been correctly set.
      pPObj->cmd.command = TSDB_SQL_RETRIEVE_METRIC;
      if (pPObj->res.code == TSDB_CODE_SUCCESS) {
        (*pPObj->fp)(pPObj->param, pPObj, 0);
      } else {
        tscQueueAsyncRes(pPObj);
      }
    }
  }
}

void tscKillMetricQuery(SSqlObj *pSql) {
  if (!tscIsTwoStageMergeMetricQuery(&pSql->cmd)) {
    return;
  }

  for (int i = 0; i < pSql->numOfSubs; ++i) {
    SSqlObj *pSub = pSql->pSubs[i];

    if (pSub == NULL || pSub->thandle == NULL) {
      continue;
    }

    /*
     * here, we cannot set the command = TSDB_SQL_KILL_QUERY. Otherwise, it may cause
     * sub-queries not correctly released and master sql object of metric query reaches an abnormal state.
     */
    pSql->pSubs[i]->res.code = TSDB_CODE_QUERY_CANCELLED;
    taosStopRpcConn(pSql->pSubs[i]->thandle);
  }

  pSql->numOfSubs = 0;

  /*
   * 1. if the subqueries are not launched or partially launched, we need to waiting the launched
   * query return to successfully free allocated resources.
   * 2. if no any subqueries are launched yet, which means the metric query only in parse sql stage,
   * set the res.code, and return.
   */
  const int64_t MAX_WAITING_TIME = 10000;  // 10 Sec.
  int64_t       stime = taosGetTimestampMs();

  while (pSql->cmd.command != TSDB_SQL_RETRIEVE_METRIC && pSql->cmd.command != TSDB_SQL_RETRIEVE_EMPTY_RESULT) {
    taosMsleep(100);
    if (taosGetTimestampMs() - stime > MAX_WAITING_TIME) {
      break;
    }
  }

  tscTrace("%p metric query is cancelled", pSql);
}

static void tscRetrieveDataRes(void *param, TAOS_RES *tres, int retCode);

static SSqlObj *tscCreateSqlObjForSubquery(SSqlObj *pSql, SRetrieveSupport *trsupport, SSqlObj *prevSqlObj) {
  SSqlObj *pNew = createSubqueryObj(pSql, trsupport->vnodeIdx, 0, tscRetrieveDataRes, trsupport, prevSqlObj);
  if (pNew != NULL) {  // the sub query of two-stage super table query
    pNew->cmd.type |= TSDB_QUERY_TYPE_STABLE_SUBQUERY;
    pSql->pSubs[trsupport->vnodeIdx] = pNew;
  }

  return pNew;
}

void tscRetrieveDataRes(void *param, TAOS_RES *tres, int code) {
  SRetrieveSupport *trsupport = (SRetrieveSupport *)param;

  SSqlObj *       pSql = (SSqlObj *)tres;
  int32_t         idx = pSql->cmd.vnodeIdx;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(&pSql->cmd, 0);

  SVnodeSidList *vnodeInfo = NULL;
  SVPeerDesc *   pSvd = NULL;
  if (pMeterMetaInfo->pMetricMeta != NULL) {
    vnodeInfo = tscGetVnodeSidList(pMeterMetaInfo->pMetricMeta, idx);
    pSvd = &vnodeInfo->vpeerDesc[vnodeInfo->index];
  }

  if (trsupport->pParentSqlObj->res.code != TSDB_CODE_SUCCESS || trsupport->pState->code != TSDB_CODE_SUCCESS) {
    // metric query is killed, Note: code must be less than 0
    trsupport->numOfRetry = MAX_NUM_OF_SUBQUERY_RETRY;
    if (trsupport->pParentSqlObj->res.code != TSDB_CODE_SUCCESS) {
      code = -(int)(trsupport->pParentSqlObj->res.code);
    } else {
      code = trsupport->pState->code;
    }
    tscTrace("%p query cancelled or failed, sub:%p, orderOfSub:%d abort, code:%d", trsupport->pParentSqlObj, pSql,
             trsupport->vnodeIdx, code);
  }

  /*
   * if a query on a vnode is failed, all retrieve operations from vnode that occurs later
   * than this one are actually not necessary, we simply call the tscRetrieveFromVnodeCallBack
   * function to abort current and remain retrieve process.
   *
   * NOTE: threadsafe is required.
   */
  if (code != TSDB_CODE_SUCCESS) {
    if (trsupport->numOfRetry++ >= MAX_NUM_OF_SUBQUERY_RETRY) {
      tscTrace("%p sub:%p reach the max retry count,set global code:%d", trsupport->pParentSqlObj, pSql, code);
      atomic_val_compare_exchange_32(&trsupport->pState->code, 0, code);
    } else {  // does not reach the maximum retry count, go on
      tscTrace("%p sub:%p failed code:%d, retry:%d", trsupport->pParentSqlObj, pSql, code, trsupport->numOfRetry);

      SSqlObj *pNew = tscCreateSqlObjForSubquery(trsupport->pParentSqlObj, trsupport, pSql);
      if (pNew == NULL) {
        tscError("%p sub:%p failed to create new subquery due to out of memory, abort retry, vid:%d, orderOfSub:%d",
                 trsupport->pParentSqlObj, pSql, pSvd->vnode, trsupport->vnodeIdx);

        trsupport->pState->code = -TSDB_CODE_CLI_OUT_OF_MEMORY;
        trsupport->numOfRetry = MAX_NUM_OF_SUBQUERY_RETRY;
      } else {
        assert(pNew->cmd.pMeterInfo[0]->pMeterMeta != NULL && pNew->cmd.pMeterInfo[0]->pMetricMeta != NULL);
        tscProcessSql(pNew);
        return;
      }
    }
  }

  if (trsupport->pState->code != TSDB_CODE_SUCCESS) {  // failed, abort
    if (vnodeInfo != NULL) {
      tscTrace("%p sub:%p query failed,ip:%u,vid:%d,orderOfSub:%d,global code:%d", trsupport->pParentSqlObj, pSql,
               vnodeInfo->vpeerDesc[vnodeInfo->index].ip, vnodeInfo->vpeerDesc[vnodeInfo->index].vnode,
               trsupport->vnodeIdx, trsupport->pState->code);
    } else {
      tscTrace("%p sub:%p query failed,orderOfSub:%d,global code:%d", trsupport->pParentSqlObj, pSql,
               trsupport->vnodeIdx, trsupport->pState->code);
    }

    tscRetrieveFromVnodeCallBack(param, tres, trsupport->pState->code);
  } else {  // success, proceed to retrieve data from dnode
    tscTrace("%p sub:%p query complete,ip:%u,vid:%d,orderOfSub:%d,retrieve data", trsupport->pParentSqlObj, pSql,
             vnodeInfo->vpeerDesc[vnodeInfo->index].ip, vnodeInfo->vpeerDesc[vnodeInfo->index].vnode,
             trsupport->vnodeIdx);

    taos_fetch_rows_a(tres, tscRetrieveFromVnodeCallBack, param);
  }
}

int tscBuildRetrieveMsg(SSqlObj *pSql) {
  char *pMsg, *pStart;
  int   msgLen = 0;

  pStart = pSql->cmd.payload + tsRpcHeadSize;
  pMsg = pStart;

  *((uint64_t *)pMsg) = pSql->res.qhandle;
  pMsg += sizeof(pSql->res.qhandle);

  *((uint16_t*)pMsg) = htons(pSql->cmd.type);
  pMsg += sizeof(pSql->cmd.type);

  msgLen = pMsg - pStart;
  pSql->cmd.payloadLen = msgLen;
  pSql->cmd.msgType = TSDB_MSG_TYPE_RETRIEVE;

  return msgLen;
}

void tscUpdateVnodeInSubmitMsg(SSqlObj *pSql, char *buf) {
  SShellSubmitMsg *pShellMsg;
  char *           pMsg;
  SMeterMetaInfo * pMeterMetaInfo = tscGetMeterMetaInfo(&pSql->cmd, 0);

  SMeterMeta *pMeterMeta = pMeterMetaInfo->pMeterMeta;

  pMsg = buf + tsRpcHeadSize;

  pShellMsg = (SShellSubmitMsg *)pMsg;
  pShellMsg->vnode = htons(pMeterMeta->vpeerDesc[pSql->index].vnode);
  tscTrace("%p update submit msg vnode:%s:%d", pSql, taosIpStr(pMeterMeta->vpeerDesc[pSql->index].ip), htons(pShellMsg->vnode));
}

int tscBuildSubmitMsg(SSqlObj *pSql) {
  SShellSubmitMsg *pShellMsg;
  char *           pMsg, *pStart;
  int              msgLen = 0;

  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(&pSql->cmd, 0);
  SMeterMeta *    pMeterMeta = pMeterMetaInfo->pMeterMeta;

  pStart = pSql->cmd.payload + tsRpcHeadSize;
  pMsg = pStart;

  pShellMsg = (SShellSubmitMsg *)pMsg;
  pShellMsg->import = pSql->cmd.import;
  pShellMsg->vnode = htons(pMeterMeta->vpeerDesc[pMeterMeta->index].vnode);
  pShellMsg->numOfSid = htonl(pSql->cmd.count);  // number of meters to be inserted

  // pSql->cmd.payloadLen is set during parse sql routine, so we do not use it here
  pSql->cmd.msgType = TSDB_MSG_TYPE_SUBMIT;
  tscTrace("%p update submit msg vnode:%s:%d", pSql, taosIpStr(pMeterMeta->vpeerDesc[pMeterMeta->index].ip), htons(pShellMsg->vnode));

  return msgLen;
}

void tscUpdateVnodeInQueryMsg(SSqlObj *pSql, char *buf) {
  SSqlCmd *       pCmd = &pSql->cmd;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  char *          pStart = buf + tsRpcHeadSize;
  SQueryMeterMsg *pQueryMsg = (SQueryMeterMsg *)pStart;

  if (UTIL_METER_IS_NOMRAL_METER(pMeterMetaInfo)) {  // pSchema == NULL, query on meter
    SMeterMeta *pMeterMeta = pMeterMetaInfo->pMeterMeta;
    pQueryMsg->vnode = htons(pMeterMeta->vpeerDesc[pSql->index].vnode);
  } else {  // query on metric
    SMetricMeta *  pMetricMeta = pMeterMetaInfo->pMetricMeta;
    SVnodeSidList *pVnodeSidList = tscGetVnodeSidList(pMetricMeta, pCmd->vnodeIdx);
    pQueryMsg->vnode = htons(pVnodeSidList->vpeerDesc[pSql->index].vnode);
  }
}

/*
 * for meter query, simply return the size <= 1k
 * for metric query, estimate size according to meter tags
 */
static int32_t tscEstimateQueryMsgSize(SSqlCmd *pCmd) {
  const static int32_t MIN_QUERY_MSG_PKT_SIZE = TSDB_MAX_BYTES_PER_ROW * 5;
  int32_t              srcColListSize = pCmd->numOfCols * sizeof(SColumnInfo);

  int32_t         exprSize = sizeof(SSqlFuncExprMsg) * pCmd->fieldsInfo.numOfOutputCols;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  // meter query without tags values
  if (!UTIL_METER_IS_METRIC(pMeterMetaInfo)) {
    return MIN_QUERY_MSG_PKT_SIZE + minMsgSize() + sizeof(SQueryMeterMsg) + srcColListSize + exprSize;
  }

  SMetricMeta *pMetricMeta = pMeterMetaInfo->pMetricMeta;

  SVnodeSidList *pVnodeSidList = tscGetVnodeSidList(pMetricMeta, pCmd->vnodeIdx);

  int32_t meterInfoSize = (pMetricMeta->tagLen + sizeof(SMeterSidExtInfo)) * pVnodeSidList->numOfSids;
  int32_t outputColumnSize = pCmd->fieldsInfo.numOfOutputCols * sizeof(SSqlFuncExprMsg);

  int32_t size = meterInfoSize + outputColumnSize + srcColListSize + exprSize + MIN_QUERY_MSG_PKT_SIZE;
  if (pCmd->tsBuf != NULL) {
    size += pCmd->tsBuf->fileSize;
  }

  return size;
}

int tscBuildQueryMsg(SSqlObj *pSql) {
  SSqlCmd *pCmd = &pSql->cmd;

  int32_t size = tscEstimateQueryMsgSize(pCmd);

  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, size)) {
    tscError("%p failed to malloc for query msg", pSql);
    return -1;
  }

  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);
  char *          pStart = pCmd->payload + tsRpcHeadSize;

  SMeterMeta * pMeterMeta = pMeterMetaInfo->pMeterMeta;
  SMetricMeta *pMetricMeta = pMeterMetaInfo->pMetricMeta;

  SQueryMeterMsg *pQueryMsg = (SQueryMeterMsg *)pStart;

  int32_t msgLen = 0;
  int32_t numOfMeters = 0;

  if (UTIL_METER_IS_NOMRAL_METER(pMeterMetaInfo)) {
    numOfMeters = 1;

    tscTrace("%p query on vnode: %d, number of sid:%d, meter id: %s", pSql,
             pMeterMeta->vpeerDesc[pMeterMeta->index].vnode, 1, pMeterMetaInfo->name);

    pQueryMsg->vnode = htons(pMeterMeta->vpeerDesc[pMeterMeta->index].vnode);
    pQueryMsg->uid = pMeterMeta->uid;
    pQueryMsg->numOfTagsCols = 0;
  } else {  // query on metric
    SMetricMeta *pMetricMeta = pMeterMetaInfo->pMetricMeta;
    if (pCmd->vnodeIdx < 0) {
      tscError("%p error vnodeIdx:%d", pSql, pCmd->vnodeIdx);
      return -1;
    }

    SVnodeSidList *pVnodeSidList = tscGetVnodeSidList(pMetricMeta, pCmd->vnodeIdx);
    uint32_t       vnodeId = pVnodeSidList->vpeerDesc[pVnodeSidList->index].vnode;

    numOfMeters = pVnodeSidList->numOfSids;
    if (numOfMeters <= 0) {
      tscError("%p vid:%d,error numOfMeters in query message:%d", pSql, vnodeId, numOfMeters);
      return -1;  // error
    }

    tscTrace("%p query on vid:%d, number of sid:%d", pSql, vnodeId, numOfMeters);
    pQueryMsg->vnode = htons(vnodeId);
  }

  pQueryMsg->numOfSids = htonl(numOfMeters);
  pQueryMsg->numOfTagsCols = htons(pMeterMetaInfo->numOfTags);

  if (pCmd->order.order == TSQL_SO_ASC) {
    pQueryMsg->skey = htobe64(pCmd->stime);
    pQueryMsg->ekey = htobe64(pCmd->etime);
  } else {
    pQueryMsg->skey = htobe64(pCmd->etime);
    pQueryMsg->ekey = htobe64(pCmd->stime);
  }

  pQueryMsg->num = htonl(0);
  pQueryMsg->order = htons(pCmd->order.order);
  pQueryMsg->orderColId = htons(pCmd->order.orderColId);

  pQueryMsg->interpoType = htons(pCmd->interpoType);

  pQueryMsg->limit = htobe64(pCmd->limit.limit);
  pQueryMsg->offset = htobe64(pCmd->limit.offset);

  pQueryMsg->numOfCols = htons(pCmd->colList.numOfCols);

  if (pCmd->colList.numOfCols <= 0) {
    tscError("%p illegal value of numOfCols in query msg: %d", pSql, pMeterMeta->numOfColumns);
    return -1;
  }

  if (pMeterMeta->numOfTags < 0) {
    tscError("%p illegal value of numOfTagsCols in query msg: %d", pSql, pMeterMeta->numOfTags);
    return -1;
  }

  pQueryMsg->nAggTimeInterval = htobe64(pCmd->nAggTimeInterval);
  pQueryMsg->intervalTimeUnit = pCmd->intervalTimeUnit;
  if (pCmd->nAggTimeInterval < 0) {
    tscError("%p illegal value of aggregation time interval in query msg: %ld", pSql, pCmd->nAggTimeInterval);
    return -1;
  }

  if (pCmd->groupbyExpr.numOfGroupCols < 0) {
    tscError("%p illegal value of numOfGroupCols in query msg: %d", pSql, pCmd->groupbyExpr.numOfGroupCols);
    return -1;
  }

  pQueryMsg->numOfGroupCols = htons(pCmd->groupbyExpr.numOfGroupCols);

  if (UTIL_METER_IS_NOMRAL_METER(pMeterMetaInfo)) {  // query on meter
    pQueryMsg->tagLength = 0;
  } else {  // query on metric
    pQueryMsg->tagLength = htons(pMetricMeta->tagLen);
  }

  pQueryMsg->queryType = htons(pCmd->type);
  pQueryMsg->numOfOutputCols = htons(pCmd->exprsInfo.numOfExprs);

  if (pCmd->fieldsInfo.numOfOutputCols < 0) {
    tscError("%p illegal value of number of output columns in query msg: %d", pSql, pCmd->fieldsInfo.numOfOutputCols);
    return -1;
  }

  // set column list ids
  char *   pMsg = (char *)(pQueryMsg->colList) + pCmd->colList.numOfCols * sizeof(SColumnInfo);
  SSchema *pSchema = tsGetSchema(pMeterMeta);

  for (int32_t i = 0; i < pCmd->colList.numOfCols; ++i) {
    SColumnBase *pCol = tscColumnBaseInfoGet(&pCmd->colList, i);
    SSchema *    pColSchema = &pSchema[pCol->colIndex.columnIndex];

    if (pCol->colIndex.columnIndex >= pMeterMeta->numOfColumns || pColSchema->type < TSDB_DATA_TYPE_BOOL ||
        pColSchema->type > TSDB_DATA_TYPE_NCHAR) {
      tscError("%p vid:%d sid:%d id:%s, column index out of range, numOfColumns:%d, index:%d, column name:%s", pSql,
               htons(pQueryMsg->vnode), pMeterMeta->sid, pMeterMetaInfo->name, pMeterMeta->numOfColumns, pCol->colIndex,
               pColSchema->name);

      return -1;  // 0 means build msg failed
    }

    pQueryMsg->colList[i].colId = htons(pColSchema->colId);
    pQueryMsg->colList[i].bytes = htons(pColSchema->bytes);
    pQueryMsg->colList[i].type = htons(pColSchema->type);
    pQueryMsg->colList[i].numOfFilters = htons(pCol->numOfFilters);

    // append the filter information after the basic column information
    for (int32_t f = 0; f < pCol->numOfFilters; ++f) {
      SColumnFilterInfo *pColFilter = &pCol->filterInfo[f];

      SColumnFilterInfo *pFilterMsg = (SColumnFilterInfo *)pMsg;
      pFilterMsg->filterOnBinary = htons(pColFilter->filterOnBinary);

      pMsg += sizeof(SColumnFilterInfo);

      if (pColFilter->filterOnBinary) {
        pFilterMsg->len = htobe64(pColFilter->len);
        memcpy(pMsg, (void *)pColFilter->pz, pColFilter->len + 1);
        pMsg += (pColFilter->len + 1);  // append the additional filter binary info
      } else {
        pFilterMsg->lowerBndi = htobe64(pColFilter->lowerBndi);
        pFilterMsg->upperBndi = htobe64(pColFilter->upperBndi);
      }

      pFilterMsg->lowerRelOptr = htons(pColFilter->lowerRelOptr);
      pFilterMsg->upperRelOptr = htons(pColFilter->upperRelOptr);

      if (pColFilter->lowerRelOptr == TSDB_RELATION_INVALID && pColFilter->upperRelOptr == TSDB_RELATION_INVALID) {
        tscError("invalid filter info");
        return -1;
      }
    }
  }

  bool hasArithmeticFunction = false;

  SSqlFuncExprMsg *pSqlFuncExpr = (SSqlFuncExprMsg *)pMsg;

  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    SSqlExpr *pExpr = tscSqlExprGet(pCmd, i);

    if (pExpr->functionId == TSDB_FUNC_ARITHM) {
      hasArithmeticFunction = true;
    }

    if (!tscValidateColumnId(pCmd, pExpr->colInfo.colId)) {
      /* column id is not valid according to the cached metermeta, the meter meta is expired */
      tscError("%p table schema is not matched with parsed sql", pSql);
      return -1;
    }

    pSqlFuncExpr->colInfo.colId = htons(pExpr->colInfo.colId);
    pSqlFuncExpr->colInfo.colIdx = htons(pExpr->colInfo.colIdx);
    pSqlFuncExpr->colInfo.flag = htons(pExpr->colInfo.flag);

    pSqlFuncExpr->functionId = htons(pExpr->functionId);
    pSqlFuncExpr->numOfParams = htons(pExpr->numOfParams);
    pMsg += sizeof(SSqlFuncExprMsg);

    for (int32_t j = 0; j < pExpr->numOfParams; ++j) {
      pSqlFuncExpr->arg[j].argType = htons((uint16_t)pExpr->param[j].nType);
      pSqlFuncExpr->arg[j].argBytes = htons(pExpr->param[j].nLen);

      if (pExpr->param[j].nType == TSDB_DATA_TYPE_BINARY) {
        memcpy(pMsg, pExpr->param[j].pz, pExpr->param[j].nLen);

        // by plus one char to make the string null-terminated
        pMsg += pExpr->param[j].nLen + 1;
      } else {
        pSqlFuncExpr->arg[j].argValue.i64 = htobe64(pExpr->param[j].i64Key);
      }
    }

    pSqlFuncExpr = (SSqlFuncExprMsg *)pMsg;
  }

  int32_t len = 0;
  if (hasArithmeticFunction) {
    SColumnBase *pColBase = pCmd->colList.pColList;
    for (int32_t i = 0; i < pCmd->colList.numOfCols; ++i) {
      char *  name = pSchema[pColBase[i].colIndex.columnIndex].name;
      int32_t lenx = strlen(name);
      memcpy(pMsg, name, lenx);
      *(pMsg + lenx) = ',';

      len += (lenx + 1);  // one for comma
      pMsg += (lenx + 1);
    }
  }

  pQueryMsg->colNameLen = htonl(len);

  // set sids list
  tscTrace("%p vid:%d, query on %d meters", pSql, pSql->cmd.vnodeIdx, numOfMeters);
  if (UTIL_METER_IS_NOMRAL_METER(pMeterMetaInfo)) {
#ifdef _DEBUG_VIEW

    tscTrace("%p %d", pSql, pMeterMetaInfo->pMeterMeta->sid);
#endif
    SMeterSidExtInfo *pSMeterTagInfo = (SMeterSidExtInfo *)pMsg;
    pSMeterTagInfo->sid = htonl(pMeterMeta->sid);
    pMsg += sizeof(SMeterSidExtInfo);
  } else {
    SVnodeSidList *pVnodeSidList = tscGetVnodeSidList(pMetricMeta, pCmd->vnodeIdx);

    for (int32_t i = 0; i < numOfMeters; ++i) {
      SMeterSidExtInfo *pMeterTagInfo = (SMeterSidExtInfo *)pMsg;
      SMeterSidExtInfo *pQueryMeterInfo = tscGetMeterSidInfo(pVnodeSidList, i);

      pMeterTagInfo->sid = htonl(pQueryMeterInfo->sid);
      pMsg += sizeof(SMeterSidExtInfo);

#ifdef _DEBUG_VIEW
      tscTrace("%p %d", pSql, pQueryMeterInfo->sid);
#endif

      memcpy(pMsg, pQueryMeterInfo->tags, pMetricMeta->tagLen);
      pMsg += pMetricMeta->tagLen;
    }
  }

  // only include the required tag column schema. If a tag is not required, it won't be sent to vnode
  if (pMeterMetaInfo->numOfTags > 0) {
    // always transfer tag schema to vnode if exists
    SSchema *pTagSchema = tsGetTagSchema(pMeterMeta);

    for (int32_t j = 0; j < pMeterMetaInfo->numOfTags; ++j) {
      if (pMeterMetaInfo->tagColumnIndex[j] == TSDB_TBNAME_COLUMN_INDEX) {
        SSchema tbSchema = {
            .bytes = TSDB_METER_NAME_LEN, .colId = TSDB_TBNAME_COLUMN_INDEX, .type = TSDB_DATA_TYPE_BINARY};
        memcpy(pMsg, &tbSchema, sizeof(SSchema));
      } else {
        memcpy(pMsg, &pTagSchema[pMeterMetaInfo->tagColumnIndex[j]], sizeof(SSchema));
      }

      pMsg += sizeof(SSchema);
    }
  }

  SSqlGroupbyExpr *pGroupbyExpr = &pCmd->groupbyExpr;
  if (pGroupbyExpr->numOfGroupCols != 0) {
    pQueryMsg->orderByIdx = htons(pGroupbyExpr->orderIndex);
    pQueryMsg->orderType = htons(pGroupbyExpr->orderType);

    for (int32_t j = 0; j < pGroupbyExpr->numOfGroupCols; ++j) {
      SColIndexEx *pCol = &pGroupbyExpr->columnInfo[j];

      *((int16_t *)pMsg) = pCol->colId;
      pMsg += sizeof(pCol->colId);

      *((int16_t *)pMsg) += pCol->colIdx;
      pMsg += sizeof(pCol->colIdx);

      *((int16_t *)pMsg) += pCol->colIdxInBuf;
      pMsg += sizeof(pCol->colIdxInBuf);

      *((int16_t *)pMsg) += pCol->flag;
      pMsg += sizeof(pCol->flag);
    }
  }

  if (pCmd->interpoType != TSDB_INTERPO_NONE) {
    for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
      *((int64_t *)pMsg) = htobe64(pCmd->defaultVal[i]);
      pMsg += sizeof(pCmd->defaultVal[0]);
    }
  }

  // compressed ts block
  pQueryMsg->tsOffset = htonl(pMsg - pStart);
  int32_t tsLen = 0;
  int32_t numOfBlocks = 0;

  if (pCmd->tsBuf != NULL) {
    STSVnodeBlockInfo *pBlockInfo = tsBufGetVnodeBlockInfo(pCmd->tsBuf, pCmd->vnodeIdx);
    assert(QUERY_IS_JOIN_QUERY(pCmd->type) && pBlockInfo != NULL);  // this query should not be sent

    // todo refactor
    fseek(pCmd->tsBuf->f, pBlockInfo->offset, SEEK_SET);
    fread(pMsg, pBlockInfo->compLen, 1, pCmd->tsBuf->f);

    pMsg += pBlockInfo->compLen;
    tsLen = pBlockInfo->compLen;
    numOfBlocks = pBlockInfo->numOfBlocks;
  }

  pQueryMsg->tsLen = htonl(tsLen);
  pQueryMsg->tsNumOfBlocks = htonl(numOfBlocks);
  if (pCmd->tsBuf != NULL) {
    pQueryMsg->tsOrder = htonl(pCmd->tsBuf->tsOrder);
  }

  msgLen = pMsg - pStart;

  tscTrace("%p msg built success,len:%d bytes", pSql, msgLen);
  pCmd->payloadLen = msgLen;
  pSql->cmd.msgType = TSDB_MSG_TYPE_QUERY;

  assert(msgLen + minMsgSize() <= size);
  return msgLen;
}

int tscBuildCreateDbMsg(SSqlObj *pSql) {
  SCreateDbMsg *pCreateDbMsg;
  char *        pMsg, *pStart;
  int           msgLen = 0;

  SSqlCmd *       pCmd = &pSql->cmd;
  STscObj *       pObj = pSql->pTscObj;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  pStart = pCmd->payload + tsRpcHeadSize;
  pMsg = pStart;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  strcpy(pMgmt->db, pObj->db);
  pMsg += sizeof(SMgmtHead);

  pCreateDbMsg = (SCreateDbMsg *)pMsg;
  strncpy(pCreateDbMsg->db, pMeterMetaInfo->name, tListLen(pCreateDbMsg->db));
  pMsg += sizeof(SCreateDbMsg);

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_CREATE_DB;

  return msgLen;
}

int tscBuildCreateDnodeMsg(SSqlObj *pSql) {
  SCreateDnodeMsg *pCreate;
  char *           pMsg, *pStart;
  int              msgLen = 0;

  SSqlCmd *       pCmd = &pSql->cmd;
  STscObj *       pObj = pSql->pTscObj;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  strcpy(pMgmt->db, pObj->db);
  pMsg += sizeof(SMgmtHead);

  pCreate = (SCreateDnodeMsg *)pMsg;
  strcpy(pCreate->ip, pMeterMetaInfo->name);

  pMsg += sizeof(SCreateDnodeMsg);

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_CREATE_PNODE;

  return msgLen;
}

int tscBuildDropDnodeMsg(SSqlObj *pSql) {
  SDropDnodeMsg *pDrop;
  char *         pMsg, *pStart;
  int            msgLen = 0;

  SSqlCmd *       pCmd = &pSql->cmd;
  STscObj *       pObj = pSql->pTscObj;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  strcpy(pMgmt->db, pObj->db);
  pMsg += sizeof(SMgmtHead);

  pDrop = (SDropDnodeMsg *)pMsg;
  strcpy(pDrop->ip, pMeterMetaInfo->name);

  pMsg += sizeof(SDropDnodeMsg);

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_DROP_PNODE;

  return msgLen;
}

int tscBuildCreateUserMsg(SSqlObj *pSql) {
  SCreateUserMsg *pCreateMsg;
  char *          pMsg, *pStart;
  int             msgLen = 0;

  SSqlCmd *       pCmd = &pSql->cmd;
  STscObj *       pObj = pSql->pTscObj;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  strcpy(pMgmt->db, pObj->db);
  pMsg += sizeof(SMgmtHead);

  pCreateMsg = (SCreateUserMsg *)pMsg;
  strcpy(pCreateMsg->user, pMeterMetaInfo->name);
  strcpy(pCreateMsg->pass, pCmd->payload);

  pMsg += sizeof(SCreateUserMsg);

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_CREATE_USER;

  return msgLen;
}

static int tscBuildAcctMsgImpl(SSqlObj *pSql) {
  SCreateAcctMsg *pAlterMsg;
  char *          pMsg, *pStart;
  int             msgLen = 0;

  SSqlCmd *       pCmd = &pSql->cmd;
  STscObj *       pObj = pSql->pTscObj;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  strcpy(pMgmt->db, pObj->db);
  pMsg += sizeof(SMgmtHead);

  pAlterMsg = (SCreateAcctMsg *)pMsg;
  strcpy(pAlterMsg->user, pMeterMetaInfo->name);
  strcpy(pAlterMsg->pass, pCmd->payload);

  pMsg += sizeof(SCreateAcctMsg);

  pAlterMsg->cfg.maxUsers = htonl((int32_t)pCmd->defaultVal[0]);
  pAlterMsg->cfg.maxDbs = htonl((int32_t)pCmd->defaultVal[1]);
  pAlterMsg->cfg.maxTimeSeries = htonl((int32_t)pCmd->defaultVal[2]);
  pAlterMsg->cfg.maxStreams = htonl((int32_t)pCmd->defaultVal[3]);
  pAlterMsg->cfg.maxPointsPerSecond = htonl((int32_t)pCmd->defaultVal[4]);
  pAlterMsg->cfg.maxStorage = htobe64(pCmd->defaultVal[5]);
  pAlterMsg->cfg.maxQueryTime = htobe64(pCmd->defaultVal[6]);
  pAlterMsg->cfg.maxConnections = htonl((int32_t)pCmd->defaultVal[7]);
  pAlterMsg->cfg.accessState = (int8_t)pCmd->defaultVal[8];

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;

  return msgLen;
}

int tscBuildCreateAcctMsg(SSqlObj *pSql) {
  int msgLen = tscBuildAcctMsgImpl(pSql);
  pSql->cmd.msgType = TSDB_MSG_TYPE_CREATE_ACCT;
  return msgLen;
}

int tscBuildAlterAcctMsg(SSqlObj *pSql) {
  int msgLen = tscBuildAcctMsgImpl(pSql);
  pSql->cmd.msgType = TSDB_MSG_TYPE_ALTER_ACCT;
  return msgLen;
}

int tscBuildAlterUserMsg(SSqlObj *pSql) {
  SAlterUserMsg *pAlterMsg;
  char *         pMsg, *pStart;
  int            msgLen = 0;

  SSqlCmd *       pCmd = &pSql->cmd;
  STscObj *       pObj = pSql->pTscObj;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  strcpy(pMgmt->db, pObj->db);
  pMsg += sizeof(SMgmtHead);

  pAlterMsg = (SCreateUserMsg *)pMsg;
  strcpy(pAlterMsg->user, pMeterMetaInfo->name);
  strcpy(pAlterMsg->pass, pCmd->payload);
  pAlterMsg->flag = pCmd->order.order;
  pAlterMsg->privilege = (char)pCmd->count;

  pMsg += sizeof(SAlterUserMsg);

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_ALTER_USER;

  return msgLen;
}

int tscBuildCfgDnodeMsg(SSqlObj *pSql) {
  SCfgMsg *pCfg;
  char *   pMsg, *pStart;
  int      msgLen = 0;

  SSqlCmd *       pCmd = &pSql->cmd;
  STscObj *       pObj = pSql->pTscObj;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  strcpy(pMgmt->db, pObj->db);
  pMsg += sizeof(SMgmtHead);

  pCfg = (SCfgMsg *)pMsg;
  strcpy(pCfg->ip, pMeterMetaInfo->name);
  strcpy(pCfg->config, pCmd->payload);

  pMsg += sizeof(SCfgMsg);

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_CFG_PNODE;

  return msgLen;
}

int tscBuildDropDbMsg(SSqlObj *pSql) {
  SDropDbMsg *pDropDbMsg;
  char *      pMsg, *pStart;
  int         msgLen = 0;

  SSqlCmd *       pCmd = &pSql->cmd;
  STscObj *       pObj = pSql->pTscObj;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  strcpy(pMgmt->db, pObj->db);
  pMsg += sizeof(SMgmtHead);

  pDropDbMsg = (SDropDbMsg *)pMsg;
  strncpy(pDropDbMsg->db, pMeterMetaInfo->name, tListLen(pDropDbMsg->db));

  pDropDbMsg->ignoreNotExists = htons(pCmd->existsCheck ? 1 : 0);

  pMsg += sizeof(SDropDbMsg);

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_DROP_DB;

  return msgLen;
}

int tscBuildDropUserMsg(SSqlObj *pSql) {
  SDropUserMsg *pDropMsg;
  char *        pMsg, *pStart;
  int           msgLen = 0;

  SSqlCmd *       pCmd = &pSql->cmd;
  STscObj *       pObj = pSql->pTscObj;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  strcpy(pMgmt->db, pObj->db);
  pMsg += sizeof(SMgmtHead);

  pDropMsg = (SDropUserMsg *)pMsg;
  strcpy(pDropMsg->user, pMeterMetaInfo->name);

  pMsg += sizeof(SDropUserMsg);

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_DROP_USER;

  return msgLen;
}

int tscBuildDropAcctMsg(SSqlObj *pSql) {
  SDropAcctMsg *pDropMsg;
  char *        pMsg, *pStart;
  int           msgLen = 0;

  SSqlCmd *       pCmd = &pSql->cmd;
  STscObj *       pObj = pSql->pTscObj;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  strcpy(pMgmt->db, pObj->db);
  pMsg += sizeof(SMgmtHead);

  pDropMsg = (SDropAcctMsg *)pMsg;
  strcpy(pDropMsg->user, pMeterMetaInfo->name);

  pMsg += sizeof(SDropAcctMsg);

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_DROP_ACCT;

  return msgLen;
}

int tscBuildUseDbMsg(SSqlObj *pSql) {
  SUseDbMsg *pUseDbMsg;
  char *     pMsg, *pStart;
  int        msgLen = 0;

  SSqlCmd *       pCmd = &pSql->cmd;
  STscObj *       pObj = pSql->pTscObj;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  strcpy(pMgmt->db, pObj->db);
  pMsg += sizeof(SMgmtHead);

  pUseDbMsg = (SUseDbMsg *)pMsg;
  strcpy(pUseDbMsg->db, pMeterMetaInfo->name);

  pMsg += sizeof(SUseDbMsg);

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_USE_DB;

  return msgLen;
}

int tscBuildShowMsg(SSqlObj *pSql) {
  SShowMsg *pShowMsg;
  char *    pMsg, *pStart;
  int       msgLen = 0;

  SSqlCmd *pCmd = &pSql->cmd;
  STscObj *pObj = pSql->pTscObj;

  assert(pCmd->payloadLen < TSDB_SQLCMD_SIZE);
  char payload[TSDB_SQLCMD_SIZE] = {0};
  memcpy(payload, pCmd->payload, pCmd->payloadLen);

  int32_t size = minMsgSize() + sizeof(SMgmtHead) + sizeof(SShowTableMsg) + pCmd->payloadLen + TSDB_EXTRA_PAYLOAD_SIZE;
  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, size)) {
    tscError("%p failed to malloc for show msg", pSql);
    return -1;
  }

  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;

  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);
  size_t          nameLen = strlen(pMeterMetaInfo->name);

  if (nameLen > 0) {
    strcpy(pMgmt->db, pMeterMetaInfo->name);
  } else {
    strcpy(pMgmt->db, pObj->db);
  }

  pMsg += sizeof(SMgmtHead);

  pShowMsg = (SShowMsg *)pMsg;
  pShowMsg->type = pCmd->showType;

  if ((pShowMsg->type == TSDB_MGMT_TABLE_TABLE || pShowMsg->type == TSDB_MGMT_TABLE_METRIC || pShowMsg->type == TSDB_MGMT_TABLE_VNODES ) && pCmd->payloadLen != 0) {
    // only show tables support wildcard query
    pShowMsg->payloadLen = htons(pCmd->payloadLen);
    memcpy(pShowMsg->payload, payload, pCmd->payloadLen);
  }

  pMsg += (sizeof(SShowTableMsg) + pCmd->payloadLen);

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_SHOW;

  assert(msgLen + minMsgSize() <= size);
  return msgLen;
}

int tscBuildKillQueryMsg(SSqlObj *pSql) {
  SKillQuery *pKill;
  char *      pMsg, *pStart;
  int         msgLen = 0;

  SSqlCmd *pCmd = &pSql->cmd;
  STscObj *pObj = pSql->pTscObj;
  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  strcpy(pMgmt->db, pObj->db);
  pMsg += sizeof(SMgmtHead);

  pKill = (SKillQuery *)pMsg;
  pKill->handle = 0;
  strcpy(pKill->queryId, pCmd->payload);

  pMsg += sizeof(SKillQuery);

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_KILL_QUERY;

  return msgLen;
}

int tscBuildKillStreamMsg(SSqlObj *pSql) {
  SKillStream *pKill;
  char *       pMsg, *pStart;
  int          msgLen = 0;

  SSqlCmd *pCmd = &pSql->cmd;
  STscObj *pObj = pSql->pTscObj;
  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  strcpy(pMgmt->db, pObj->db);
  pMsg += sizeof(SMgmtHead);

  pKill = (SKillStream *)pMsg;
  pKill->handle = 0;
  strcpy(pKill->queryId, pCmd->payload);

  pMsg += sizeof(SKillStream);

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_KILL_STREAM;

  return msgLen;
}

int tscBuildKillConnectionMsg(SSqlObj *pSql) {
  SKillConnection *pKill;
  char *           pMsg, *pStart;
  int              msgLen = 0;

  SSqlCmd *pCmd = &pSql->cmd;
  STscObj *pObj = pSql->pTscObj;
  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  strcpy(pMgmt->db, pObj->db);
  pMsg += sizeof(SMgmtHead);

  pKill = (SKillStream *)pMsg;
  pKill->handle = 0;
  strcpy(pKill->queryId, pCmd->payload);

  pMsg += sizeof(SKillStream);

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_KILL_CONNECTION;

  return msgLen;
}

int tscEstimateCreateTableMsgLength(SSqlObj *pSql) {
  SSqlCmd *pCmd = &(pSql->cmd);

  int32_t size = minMsgSize() + sizeof(SMgmtHead) + sizeof(SCreateTableMsg);

  if (pCmd->numOfCols == 0 && pCmd->count == 0) {
    size += sizeof(STagData);
  } else {
    size += sizeof(SSchema) * (pCmd->numOfCols + pCmd->count);
  }

  if (strlen(pCmd->payload) > 0) size += strlen(pCmd->payload) + 1;

  return size + TSDB_EXTRA_PAYLOAD_SIZE;
}

int tscBuildCreateTableMsg(SSqlObj *pSql) {
  SCreateTableMsg *pCreateTableMsg;
  char *           pMsg, *pStart;
  int              msgLen = 0;
  SSchema *        pSchema;
  int              size = 0;

  // tmp variable to
  // 1. save tags data in order to avoid too long tag values overlapped by header
  // 2. save the selection clause, in create table as .. sql string
  char *tmpData = calloc(1, pSql->cmd.allocSize);

  // STagData is in binary format, strncpy is not available
  memcpy(tmpData, pSql->cmd.payload, pSql->cmd.allocSize);

  SSqlCmd *       pCmd = &pSql->cmd;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  // Reallocate the payload size
  size = tscEstimateCreateTableMsgLength(pSql);
  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, size)) {
    tscError("%p failed to malloc for create table msg", pSql);
    return -1;
  }

  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  // use dbinfo from meterid without modifying current db info
  tscGetDBInfoFromMeterId(pMeterMetaInfo->name, pMgmt->db);

  pMsg += sizeof(SMgmtHead);

  pCreateTableMsg = (SCreateTableMsg *)pMsg;
  strcpy(pCreateTableMsg->meterId, pMeterMetaInfo->name);

  pCreateTableMsg->igExists = pCmd->existsCheck ? 1 : 0;
  pCreateTableMsg->numOfColumns = htons(pCmd->numOfCols);
  pCreateTableMsg->numOfTags = htons(pCmd->count);
  pMsg = (char *)pCreateTableMsg->schema;

  pCreateTableMsg->sqlLen = 0;
  short sqlLen = (short)(strlen(tmpData) + 1);

  if (pCmd->numOfCols == 0 && pCmd->count == 0) {
    // create by using metric, tags value
    memcpy(pMsg, tmpData, sizeof(STagData));
    pMsg += sizeof(STagData);
  } else {
    // create metric/create normal meter
    pSchema = pCreateTableMsg->schema;
    for (int i = 0; i < pCmd->numOfCols + pCmd->count; ++i) {
      TAOS_FIELD *pField = tscFieldInfoGetField(pCmd, i);

      pSchema->type = pField->type;
      strcpy(pSchema->name, pField->name);
      pSchema->bytes = htons(pField->bytes);
      pSchema++;
    }

    pMsg = (char *)pSchema;

    // check if it is a stream sql
    if (sqlLen > 1) {
      memcpy(pMsg, tmpData, sqlLen);
      pMsg[sqlLen - 1] = 0;

      pCreateTableMsg->sqlLen = htons(sqlLen);
      pMsg += sqlLen;
    }
  }

  tfree(tmpData);
  tscClearFieldInfo(&pCmd->fieldsInfo);

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_CREATE_TABLE;

  assert(msgLen + minMsgSize() <= size);
  return msgLen;
}

int tscEstimateAlterTableMsgLength(SSqlCmd *pCmd) {
  return minMsgSize() + sizeof(SMgmtHead) + sizeof(SAlterTableMsg) + sizeof(SSchema) * pCmd->numOfCols +
         TSDB_EXTRA_PAYLOAD_SIZE;
}

int tscBuildAlterTableMsg(SSqlObj *pSql) {
  SAlterTableMsg *pAlterTableMsg;
  char *          pMsg, *pStart;
  int             msgLen = 0;
  int             size = 0;

  SSqlCmd *       pCmd = &pSql->cmd;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  char    buf[TSDB_MAX_TAGS_LEN] = {0};
  int32_t len = (TSDB_MAX_TAGS_LEN < pCmd->allocSize) ? TSDB_MAX_TAGS_LEN : pCmd->allocSize;
  memcpy(buf, pCmd->payload, len);

  size = tscEstimateAlterTableMsgLength(pCmd);
  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, size)) {
    tscError("%p failed to malloc for alter table msg", pSql);
    return -1;
  }

  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  tscGetDBInfoFromMeterId(pMeterMetaInfo->name, pMgmt->db);
  pMsg += sizeof(SMgmtHead);

  pAlterTableMsg = (SAlterTableMsg *)pMsg;
  strcpy(pAlterTableMsg->meterId, pMeterMetaInfo->name);
  pAlterTableMsg->type = htons(pCmd->count);
  pAlterTableMsg->numOfCols = htons(pCmd->numOfCols);
  memcpy(pAlterTableMsg->tagVal, buf, TSDB_MAX_TAGS_LEN);

  SSchema *pSchema = pAlterTableMsg->schema;
  for (int i = 0; i < pCmd->numOfCols; ++i) {
    TAOS_FIELD *pField = tscFieldInfoGetField(pCmd, i);

    pSchema->type = pField->type;
    strcpy(pSchema->name, pField->name);
    pSchema->bytes = htons(pField->bytes);
    pSchema++;
  }

  pMsg = (char *)pSchema;

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_ALTER_TABLE;

  assert(msgLen + minMsgSize() <= size);
  return msgLen;
}

int tscAlterDbMsg(SSqlObj *pSql) {
  SAlterDbMsg *pAlterDbMsg;
  char *       pMsg, *pStart;
  int          msgLen = 0;

  SSqlCmd *       pCmd = &pSql->cmd;
  STscObj *       pObj = pSql->pTscObj;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  pStart = pCmd->payload + tsRpcHeadSize;
  pMsg = pStart;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  strcpy(pMgmt->db, pObj->db);
  pMsg += sizeof(SMgmtHead);

  pAlterDbMsg = (SAlterDbMsg *)pMsg;
  strcpy(pAlterDbMsg->db, pMeterMetaInfo->name);

  pMsg += sizeof(SAlterDbMsg);

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_ALTER_DB;

  return msgLen;
}

int tscBuildDropTableMsg(SSqlObj *pSql) {
  SDropTableMsg *pDropTableMsg;
  char *         pMsg, *pStart;
  int            msgLen = 0;

  SSqlCmd *       pCmd = &pSql->cmd;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  tscGetDBInfoFromMeterId(pMeterMetaInfo->name, pMgmt->db);
  pMsg += sizeof(SMgmtHead);

  pDropTableMsg = (SDropTableMsg *)pMsg;
  strcpy(pDropTableMsg->meterId, pMeterMetaInfo->name);

  pDropTableMsg->igNotExists = pCmd->existsCheck ? 1 : 0;
  pMsg += sizeof(SDropTableMsg);

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_DROP_TABLE;

  return msgLen;
}

int tscBuildRetrieveFromMgmtMsg(SSqlObj *pSql) {
  char *pMsg, *pStart;
  int   msgLen = 0;

  SSqlCmd *pCmd = &pSql->cmd;
  STscObj *pObj = pSql->pTscObj;
  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;

  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);
  size_t          nameLen = strlen(pMeterMetaInfo->name);

  if (nameLen > 0) {
    strcpy(pMgmt->db, pMeterMetaInfo->name);
  } else {
    strcpy(pMgmt->db, pObj->db);
  }

  pMsg += sizeof(SMgmtHead);

  *((uint64_t *) pMsg) = pSql->res.qhandle;
  pMsg += sizeof(pSql->res.qhandle);

  *((uint16_t*) pMsg) = htons(pCmd->type);
  pMsg += sizeof(pCmd->type);

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_RETRIEVE;

  return msgLen;
}

static int tscSetResultPointer(SSqlCmd *pCmd, SSqlRes *pRes) {
  if (tscCreateResPointerInfo(pCmd, pRes) != TSDB_CODE_SUCCESS) {
    return pRes->code;
  }

  for (int i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    TAOS_FIELD *pField = tscFieldInfoGetField(pCmd, i);
    int16_t     offset = tscFieldInfoGetOffset(pCmd, i);

    pRes->bytes[i] = pField->bytes;
    if (pCmd->order.order == TSQL_SO_DESC) {
      pRes->bytes[i] = -pRes->bytes[i];
      pRes->tsrow[i] = ((pRes->data + offset * pRes->numOfRows) + (pRes->numOfRows - 1) * pField->bytes);
    } else {
      pRes->tsrow[i] = (pRes->data + offset * pRes->numOfRows);
    }
  }

  return 0;
}

/*
 * this function can only be called once.
 * by using pRes->rspType to denote its status
 *
 * if pRes->rspType is 1, no more result
 */
static int tscLocalResultCommonBuilder(SSqlObj *pSql, int32_t numOfRes) {
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

  pRes->code = TSDB_CODE_SUCCESS;

  if (pRes->rspType == 0) {
    pRes->numOfRows = numOfRes;
    pRes->row = 0;
    pRes->rspType = 1;

    tscSetResultPointer(pCmd, pRes);
    pRes->row = 0;

  } else {
    tscResetForNextRetrieve(pRes);
  }

  uint8_t code = pSql->res.code;
  if (pSql->fp) {
    if (code == TSDB_CODE_SUCCESS) {
      (*pSql->fp)(pSql->param, pSql, pSql->res.numOfRows);
    } else {
      tscQueueAsyncRes(pSql);
    }
  }

  return code;
}

int tscProcessDescribeTableRsp(SSqlObj *pSql) {
  SSqlCmd *       pCmd = &pSql->cmd;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  int32_t numOfRes = pMeterMetaInfo->pMeterMeta->numOfColumns + pMeterMetaInfo->pMeterMeta->numOfTags;

  return tscLocalResultCommonBuilder(pSql, numOfRes);
}

int tscProcessTagRetrieveRsp(SSqlObj *pSql) {
  SSqlCmd *       pCmd = &pSql->cmd;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  int32_t numOfRes = 0;
  if (tscSqlExprGet(pCmd, 0)->functionId == TSDB_FUNC_TAGPRJ) {
    numOfRes = pMeterMetaInfo->pMetricMeta->numOfMeters;
  } else {
    numOfRes = 1;  // for count function, there is only one output.
  }
  return tscLocalResultCommonBuilder(pSql, numOfRes);
}

int tscProcessRetrieveMetricRsp(SSqlObj *pSql) {
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

  pRes->code = tscLocalDoReduce(pSql);

  if (pRes->code == TSDB_CODE_SUCCESS && pRes->numOfRows > 0) {
    tscSetResultPointer(pCmd, pRes);
  }

  pRes->row = 0;

  uint8_t code = pSql->res.code;
  if (pSql->fp) {  // async retrieve metric data
    if (pSql->res.code == TSDB_CODE_SUCCESS) {
      (*pSql->fp)(pSql->param, pSql, pSql->res.numOfRows);
    } else {
      tscQueueAsyncRes(pSql);
    }
  }

  return code;
}

int tscProcessEmptyResultRsp(SSqlObj *pSql) { return tscLocalResultCommonBuilder(pSql, 0); }

int tscBuildConnectMsg(SSqlObj *pSql) {
  SConnectMsg *pConnect;
  char *       pMsg, *pStart;
  int          msgLen = 0;

  SSqlCmd *pCmd = &pSql->cmd;
  STscObj *pObj = pSql->pTscObj;
  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  pConnect = (SConnectMsg *)pMsg;

  char *db;  // ugly code to move the space
  db = strstr(pObj->db, TS_PATH_DELIMITER);
  db = (db == NULL) ? pObj->db : db + 1;
  strcpy(pConnect->db, db);

  pMsg += sizeof(SConnectMsg);

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_CONNECT;

  return msgLen;
}

int tscBuildMeterMetaMsg(SSqlObj *pSql) {
  SMeterInfoMsg *pInfoMsg;
  char *         pMsg, *pStart;
  int            msgLen = 0;

  char *tmpData = 0;
  if (pSql->cmd.allocSize > 0) {
    tmpData = calloc(1, pSql->cmd.allocSize);
    if (NULL == tmpData) return -1;
    // STagData is in binary format, strncpy is not available
    memcpy(tmpData, pSql->cmd.payload, pSql->cmd.allocSize);
  }

  SSqlCmd *       pCmd = &pSql->cmd;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  tscGetDBInfoFromMeterId(pMeterMetaInfo->name, pMgmt->db);

  pMsg += sizeof(SMgmtHead);

  pInfoMsg = (SMeterInfoMsg *)pMsg;
  strcpy(pInfoMsg->meterId, pMeterMetaInfo->name);
  pInfoMsg->createFlag = htons((uint16_t)pCmd->defaultVal[0]);
  pMsg += sizeof(SMeterInfoMsg);

  if (pCmd->defaultVal[0] != 0) {
    memcpy(pInfoMsg->tags, tmpData, sizeof(STagData));
    pMsg += sizeof(STagData);
  }

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_METERINFO;

  tfree(tmpData);

  assert(msgLen + minMsgSize() <= pCmd->allocSize);
  return msgLen;
}

/**
 *  multi meter meta req pkg format:
 *  | SMgmtHead | SMultiMeterInfoMsg | meterId0 | meterId1 | meterId2 | ......
 *      no used         4B
 **/
int tscBuildMultiMeterMetaMsg(SSqlObj *pSql) {
  SSqlCmd *pCmd = &pSql->cmd;

  // copy payload content to temp buff
  char *tmpData = 0;
  if (pCmd->payloadLen > 0) {
    tmpData = calloc(1, pCmd->payloadLen + 1);
    if (NULL == tmpData) return -1;
    memcpy(tmpData, pCmd->payload, pCmd->payloadLen);
  }

  // fill head info
  SMgmtHead *pMgmt = (SMgmtHead *)(pCmd->payload + tsRpcHeadSize);
  memset(pMgmt->db, 0, TSDB_METER_ID_LEN);  // server don't need the db

  SMultiMeterInfoMsg *pInfoMsg = (SMultiMeterInfoMsg *)(pCmd->payload + tsRpcHeadSize + sizeof(SMgmtHead));
  pInfoMsg->numOfMeters = htonl((int32_t)pCmd->count);

  if (pCmd->payloadLen > 0) {
    memcpy(pInfoMsg->meterId, tmpData, pCmd->payloadLen);
  }

  tfree(tmpData);

  pCmd->payloadLen += sizeof(SMgmtHead) + sizeof(SMultiMeterInfoMsg);
  pCmd->msgType = TSDB_MSG_TYPE_MULTI_METERINFO;

  assert(pCmd->payloadLen + minMsgSize() <= pCmd->allocSize);

  tscTrace("%p build load multi-metermeta msg completed, numOfMeters:%d, msg size:%d", pSql, pCmd->count,
           pCmd->payloadLen);

  return pCmd->payloadLen;
}

static int32_t tscEstimateMetricMetaMsgSize(SSqlCmd *pCmd) {
  const int32_t defaultSize =
      minMsgSize() + sizeof(SMetricMetaMsg) + sizeof(SMgmtHead) + sizeof(int16_t) * TSDB_MAX_TAGS;

  int32_t n = 0;
  for (int32_t i = 0; i < pCmd->tagCond.numOfTagCond; ++i) {
    n += pCmd->tagCond.cond[i].cond.n;
  }

  int32_t tagLen = n * TSDB_NCHAR_SIZE + pCmd->tagCond.tbnameCond.cond.n * TSDB_NCHAR_SIZE;
  int32_t joinCondLen = (TSDB_METER_ID_LEN + sizeof(int16_t)) * 2;
  int32_t elemSize = sizeof(SMetricMetaElemMsg) * pCmd->numOfTables;

  int32_t len = tagLen + joinCondLen + elemSize + defaultSize;

  return MAX(len, TSDB_DEFAULT_PAYLOAD_SIZE);
}

int tscBuildMetricMetaMsg(SSqlObj *pSql) {
  SMetricMetaMsg *pMetaMsg;
  char *          pMsg, *pStart;
  int             msgLen = 0;
  int             tableIndex = 0;

  SSqlCmd * pCmd = &pSql->cmd;
  STagCond *pTagCond = &pCmd->tagCond;

  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, tableIndex);

  int32_t size = tscEstimateMetricMetaMsgSize(pCmd);
  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, size)) {
    tscError("%p failed to malloc for metric meter msg", pSql);
    return -1;
  }

  pStart = pCmd->payload + tsRpcHeadSize;
  pMsg = pStart;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  tscGetDBInfoFromMeterId(pMeterMetaInfo->name, pMgmt->db);

  pMsg += sizeof(SMgmtHead);

  pMetaMsg = (SMetricMetaMsg *)pMsg;
  pMetaMsg->numOfMeters = htonl(pCmd->numOfTables);

  pMsg += sizeof(SMetricMetaMsg);

  int32_t offset = pMsg - (char *)pMetaMsg;
  pMetaMsg->join = htonl(offset);

  // todo refactor
  pMetaMsg->joinCondLen = htonl((TSDB_METER_ID_LEN + sizeof(int16_t)) * 2);

  memcpy(pMsg, pTagCond->joinInfo.left.meterId, TSDB_METER_ID_LEN);
  pMsg += TSDB_METER_ID_LEN;

  *(int16_t *)pMsg = pTagCond->joinInfo.left.tagCol;
  pMsg += sizeof(int16_t);

  memcpy(pMsg, pTagCond->joinInfo.right.meterId, TSDB_METER_ID_LEN);
  pMsg += TSDB_METER_ID_LEN;

  *(int16_t *)pMsg = pTagCond->joinInfo.right.tagCol;
  pMsg += sizeof(int16_t);

  for (int32_t i = 0; i < pCmd->numOfTables; ++i) {
    pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, i);
    uint64_t uid = pMeterMetaInfo->pMeterMeta->uid;

    offset = pMsg - (char *)pMetaMsg;
    pMetaMsg->metaElem[i] = htonl(offset);

    SMetricMetaElemMsg *pElem = (SMetricMetaElemMsg *)pMsg;
    pMsg += sizeof(SMetricMetaElemMsg);

    // convert to unicode before sending to mnode for metric query
    int32_t condLen = 0;
    if (pTagCond->numOfTagCond > 0) {
      SCond *pCond = tsGetMetricQueryCondPos(pTagCond, uid);
      if (pCond != NULL) {
        condLen = pCond->cond.n + 1;
        bool ret = taosMbsToUcs4(pCond->cond.z, pCond->cond.n, pMsg, pCond->cond.n * TSDB_NCHAR_SIZE);
        if (!ret) {
          tscError("%p mbs to ucs4 failed:%s", pSql, tsGetMetricQueryCondPos(pTagCond, uid));
          return 0;
        }
      }
    }

    pElem->condLen = htonl(condLen);

    offset = pMsg - (char *)pMetaMsg;
    pElem->cond = htonl(offset);
    pMsg += condLen * TSDB_NCHAR_SIZE;

    pElem->rel = htons(pTagCond->relType);
    if (pTagCond->tbnameCond.uid == uid) {
      offset = pMsg - (char *)pMetaMsg;

      pElem->tableCond = htonl(offset);
      pElem->tableCondLen = htonl(pTagCond->tbnameCond.cond.n);

      memcpy(pMsg, pTagCond->tbnameCond.cond.z, pTagCond->tbnameCond.cond.n);
      pMsg += pTagCond->tbnameCond.cond.n;
    }

    SSqlGroupbyExpr *pGroupby = &pCmd->groupbyExpr;

    if (pGroupby->tableIndex != i) {
      pElem->orderType = 0;
      pElem->orderIndex = 0;
      pElem->numOfGroupCols = 0;
    } else {
      pElem->numOfGroupCols = htons(pGroupby->numOfGroupCols);
      for (int32_t j = 0; j < pMeterMetaInfo->numOfTags; ++j) {
        pElem->tagCols[j] = htons(pMeterMetaInfo->tagColumnIndex[j]);
      }

      if (pGroupby->numOfGroupCols != 0) {
        pElem->orderIndex = htons(pGroupby->orderIndex);
        pElem->orderType = htons(pGroupby->orderType);
        offset = pMsg - (char *)pMetaMsg;

        pElem->groupbyTagColumnList = htonl(offset);
        for (int32_t j = 0; j < pCmd->groupbyExpr.numOfGroupCols; ++j) {
          SColIndexEx *pCol = &pCmd->groupbyExpr.columnInfo[j];

          *((int16_t *)pMsg) = pCol->colId;
          pMsg += sizeof(pCol->colId);

          *((int16_t *)pMsg) += pCol->colIdx;
          pMsg += sizeof(pCol->colIdx);

          *((int16_t *)pMsg) += pCol->flag;
          pMsg += sizeof(pCol->flag);
        }
      }
    }

    strcpy(pElem->meterId, pMeterMetaInfo->name);
    pElem->numOfTags = htons(pMeterMetaInfo->numOfTags);

    int16_t len = pMsg - (char *)pElem;
    pElem->elemLen = htons(len);  // redundant data for integrate check
  }

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_METRIC_META;
  assert(msgLen + minMsgSize() <= size);
  return msgLen;
}

int tscEstimateHeartBeatMsgLength(SSqlObj *pSql) {
  int      size = 0;
  STscObj *pObj = pSql->pTscObj;

  size += tsRpcHeadSize + sizeof(SMgmtHead);
  size += sizeof(SQList);

  SSqlObj *tpSql = pObj->sqlList;
  while (tpSql) {
    size += sizeof(SQDesc);
    tpSql = tpSql->next;
  }

  size += sizeof(SSList);
  SSqlStream *pStream = pObj->streamList;
  while (pStream) {
    size += sizeof(SSDesc);
    pStream = pStream->next;
  }

  return size + TSDB_EXTRA_PAYLOAD_SIZE;
}

int tscBuildHeartBeatMsg(SSqlObj *pSql) {
  char *pMsg, *pStart;
  int   msgLen = 0;
  int   size = 0;

  SSqlCmd *pCmd = &pSql->cmd;
  STscObj *pObj = pSql->pTscObj;

  pthread_mutex_lock(&pObj->mutex);

  size = tscEstimateHeartBeatMsgLength(pSql);
  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, size)) {
    tscError("%p failed to malloc for heartbeat msg", pSql);
    return -1;
  }

  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  strcpy(pMgmt->db, pObj->db);
  pMsg += sizeof(SMgmtHead);

  pMsg = tscBuildQueryStreamDesc(pMsg, pObj);
  pthread_mutex_unlock(&pObj->mutex);

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_HEARTBEAT;

  assert(msgLen + minMsgSize() <= size);
  return msgLen;
}

int tscProcessRetrieveRspFromMgmt(SSqlObj *pSql) {
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;
  STscObj *pObj = pSql->pTscObj;

  SRetrieveMeterRsp *pRetrieve = (SRetrieveMeterRsp *)(pRes->pRsp);
  pRes->numOfRows = htonl(pRetrieve->numOfRows);
  pRes->precision = htons(pRes->precision);

  pRes->data = pRetrieve->data;

  tscSetResultPointer(pCmd, pRes);

  if (pRes->numOfRows == 0) {
    taosAddConnIntoCache(tscConnCache, pSql->thandle, pSql->ip, pSql->vnode, pObj->user);
    pSql->thandle = NULL;
  }

  pRes->row = 0;
  return 0;
}

int tscProcessMeterMetaRsp(SSqlObj *pSql) {
  SMeterMeta *pMeta;
  SSchema *   pSchema;
  uint8_t     ieType;

  char *rsp = pSql->res.pRsp;

  ieType = *rsp;
  if (ieType != TSDB_IE_TYPE_META) {
    tscError("invalid ie type:%d", ieType);
    return TSDB_CODE_INVALID_IE;
  }

  rsp++;
  pMeta = (SMeterMeta *)rsp;

  pMeta->sid = htonl(pMeta->sid);
  pMeta->sversion = htons(pMeta->sversion);
  pMeta->vgid = htonl(pMeta->vgid);
  pMeta->uid = htobe64(pMeta->uid);

  if (pMeta->sid < 0 || pMeta->vgid < 0) {
    tscError("invalid meter vgid:%d, sid%d", pMeta->vgid, pMeta->sid);
    return TSDB_CODE_INVALID_VALUE;
  }

  pMeta->numOfColumns = htons(pMeta->numOfColumns);

  if (pMeta->numOfTags > TSDB_MAX_TAGS || pMeta->numOfTags < 0) {
    tscError("invalid tag value count:%d", pMeta->numOfTags);
    return TSDB_CODE_INVALID_VALUE;
  }

  if (pMeta->numOfTags > TSDB_MAX_TAGS || pMeta->numOfTags < 0) {
    tscError("invalid numOfTags:%d", pMeta->numOfTags);
    return TSDB_CODE_INVALID_VALUE;
  }

  if (pMeta->numOfColumns > TSDB_MAX_COLUMNS || pMeta->numOfColumns < 0) {
    tscError("invalid numOfColumns:%d", pMeta->numOfColumns);
    return TSDB_CODE_INVALID_VALUE;
  }

  for (int i = 0; i < TSDB_VNODES_SUPPORT; ++i) {
    pMeta->vpeerDesc[i].vnode = htonl(pMeta->vpeerDesc[i].vnode);
  }

  pMeta->rowSize = 0;
  rsp += sizeof(SMeterMeta);
  pSchema = (SSchema *)rsp;

  int32_t numOfTotalCols = pMeta->numOfColumns + pMeta->numOfTags;
  for (int i = 0; i < numOfTotalCols; ++i) {
    pSchema->bytes = htons(pSchema->bytes);
    pSchema->colId = htons(pSchema->colId);

    // ignore the tags length
    if (i < pMeta->numOfColumns) {
      pMeta->rowSize += pSchema->bytes;
    }
    pSchema++;
  }

  rsp += numOfTotalCols * sizeof(SSchema);

  int32_t  tagLen = 0;
  SSchema *pTagsSchema = tsGetTagSchema(pMeta);

  if (pMeta->meterType == TSDB_METER_MTABLE) {
    for (int32_t i = 0; i < pMeta->numOfTags; ++i) {
      tagLen += pTagsSchema[i].bytes;
    }
  }

  rsp += tagLen;
  int32_t size = (int32_t)(rsp - (char *)pMeta);

  // pMeta->index = rand() % TSDB_VNODES_SUPPORT;
  pMeta->index = 0;

  // todo add one more function: taosAddDataIfNotExists();
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(&pSql->cmd, 0);
  taosRemoveDataFromCache(tscCacheHandle, (void **)&(pMeterMetaInfo->pMeterMeta), false);

  pMeterMetaInfo->pMeterMeta = (SMeterMeta *)taosAddDataIntoCache(tscCacheHandle, pMeterMetaInfo->name, (char *)pMeta,
                                                                  size, tsMeterMetaKeepTimer);
  if (pMeterMetaInfo->pMeterMeta == NULL) return 0;

  return TSDB_CODE_OTHERS;
}

/**
 *  multi meter meta rsp pkg format:
 *  | STaosRsp | ieType | SMultiMeterInfoMsg | SMeterMeta0 | SSchema0 | SMeterMeta1 | SSchema1 | SMeterMeta2 | SSchema2
 *  |...... 1B        1B            4B
 **/
int tscProcessMultiMeterMetaRsp(SSqlObj *pSql) {
  SSchema *pSchema;
  uint8_t  ieType;
  int32_t  totalNum;
  int32_t  i;

  char *rsp = pSql->res.pRsp;

  ieType = *rsp;
  if (ieType != TSDB_IE_TYPE_META) {
    tscError("invalid ie type:%d", ieType);
    pSql->res.code = TSDB_CODE_INVALID_IE;
    pSql->res.numOfTotal = 0;
    return TSDB_CODE_OTHERS;
  }

  rsp++;

  SMultiMeterInfoMsg *pInfo = (SMultiMeterInfoMsg *)rsp;
  totalNum = htonl(pInfo->numOfMeters);
  rsp += sizeof(SMultiMeterInfoMsg);

  for (i = 0; i < totalNum; i++) {
    SMultiMeterMeta *pMultiMeta = (SMultiMeterMeta *)rsp;
    SMeterMeta *     pMeta = &pMultiMeta->meta;

    pMeta->sid = htonl(pMeta->sid);
    pMeta->sversion = htons(pMeta->sversion);
    pMeta->vgid = htonl(pMeta->vgid);
    pMeta->uid = htobe64(pMeta->uid);

    if (pMeta->sid <= 0 || pMeta->vgid < 0) {
      tscError("invalid meter vgid:%d, sid%d", pMeta->vgid, pMeta->sid);
      pSql->res.code = TSDB_CODE_INVALID_VALUE;
      pSql->res.numOfTotal = i;
      return TSDB_CODE_OTHERS;
    }

    pMeta->numOfColumns = htons(pMeta->numOfColumns);

    if (pMeta->numOfTags > TSDB_MAX_TAGS || pMeta->numOfTags < 0) {
      tscError("invalid tag value count:%d", pMeta->numOfTags);
      pSql->res.code = TSDB_CODE_INVALID_VALUE;
      pSql->res.numOfTotal = i;
      return TSDB_CODE_OTHERS;
    }

    if (pMeta->numOfTags > TSDB_MAX_TAGS || pMeta->numOfTags < 0) {
      tscError("invalid numOfTags:%d", pMeta->numOfTags);
      pSql->res.code = TSDB_CODE_INVALID_VALUE;
      pSql->res.numOfTotal = i;
      return TSDB_CODE_OTHERS;
    }

    if (pMeta->numOfColumns > TSDB_MAX_COLUMNS || pMeta->numOfColumns < 0) {
      tscError("invalid numOfColumns:%d", pMeta->numOfColumns);
      pSql->res.code = TSDB_CODE_INVALID_VALUE;
      pSql->res.numOfTotal = i;
      return TSDB_CODE_OTHERS;
    }

    for (int j = 0; j < TSDB_VNODES_SUPPORT; ++j) {
      pMeta->vpeerDesc[j].vnode = htonl(pMeta->vpeerDesc[j].vnode);
    }

    pMeta->rowSize = 0;
    rsp += sizeof(SMultiMeterMeta);
    pSchema = (SSchema *)rsp;

    int32_t numOfTotalCols = pMeta->numOfColumns + pMeta->numOfTags;
    for (int j = 0; j < numOfTotalCols; ++j) {
      pSchema->bytes = htons(pSchema->bytes);
      pSchema->colId = htons(pSchema->colId);

      // ignore the tags length
      if (j < pMeta->numOfColumns) {
        pMeta->rowSize += pSchema->bytes;
      }
      pSchema++;
    }

    rsp += numOfTotalCols * sizeof(SSchema);

    int32_t  tagLen = 0;
    SSchema *pTagsSchema = tsGetTagSchema(pMeta);

    if (pMeta->meterType == TSDB_METER_MTABLE) {
      for (int32_t j = 0; j < pMeta->numOfTags; ++j) {
        tagLen += pTagsSchema[j].bytes;
      }
    }

    rsp += tagLen;
    int32_t size = (int32_t)(rsp - ((char *)pMeta));  // Consistent with SMeterMeta in cache

    pMeta->index = 0;
    (void)taosAddDataIntoCache(tscCacheHandle, pMultiMeta->meterId, (char *)pMeta, size, tsMeterMetaKeepTimer);
  }

  pSql->res.code = TSDB_CODE_SUCCESS;
  pSql->res.numOfTotal = i;
  tscTrace("%p load multi-metermeta resp complete num:%d", pSql, pSql->res.numOfTotal);
  return TSDB_CODE_SUCCESS;
}

int tscProcessMetricMetaRsp(SSqlObj *pSql) {
  SMetricMeta *pMeta;
  uint8_t      ieType;
  void **      metricMetaList = NULL;
  int32_t *    sizes = NULL;

  char *rsp = pSql->res.pRsp;

  ieType = *rsp;
  if (ieType != TSDB_IE_TYPE_META) {
    tscError("invalid ie type:%d", ieType);
    return TSDB_CODE_INVALID_IE;
  }

  rsp++;

  int32_t num = htons(*(int16_t *)rsp);
  rsp += sizeof(int16_t);

  metricMetaList = calloc(1, POINTER_BYTES * num);
  sizes = calloc(1, sizeof(int32_t) * num);

  // return with error code
  if (metricMetaList == NULL || sizes == NULL) {
    tfree(metricMetaList);
    tfree(sizes);
    pSql->res.code = TSDB_CODE_CLI_OUT_OF_MEMORY;

    return pSql->res.code;
  }

  for (int32_t k = 0; k < num; ++k) {
    pMeta = (SMetricMeta *)rsp;

    size_t size = (size_t)pSql->res.rspLen - 1;
    rsp = rsp + sizeof(SMetricMeta);

    pMeta->numOfMeters = htonl(pMeta->numOfMeters);
    pMeta->numOfVnodes = htonl(pMeta->numOfVnodes);
    pMeta->tagLen = htons(pMeta->tagLen);

    size += pMeta->numOfVnodes * sizeof(SVnodeSidList *) + pMeta->numOfMeters * sizeof(SMeterSidExtInfo *);

    char *pStr = calloc(1, size);
    if (pStr == NULL) {
      pSql->res.code = TSDB_CODE_CLI_OUT_OF_MEMORY;
      goto _error_clean;
    }

    SMetricMeta *pNewMetricMeta = (SMetricMeta *)pStr;
    metricMetaList[k] = pNewMetricMeta;

    pNewMetricMeta->numOfMeters = pMeta->numOfMeters;
    pNewMetricMeta->numOfVnodes = pMeta->numOfVnodes;
    pNewMetricMeta->tagLen = pMeta->tagLen;

    pStr = pStr + sizeof(SMetricMeta) + pNewMetricMeta->numOfVnodes * sizeof(SVnodeSidList *);

    for (int32_t i = 0; i < pMeta->numOfVnodes; ++i) {
      SVnodeSidList *pSidLists = (SVnodeSidList *)rsp;
      memcpy(pStr, pSidLists, sizeof(SVnodeSidList));

      pNewMetricMeta->list[i] = pStr - (char *)pNewMetricMeta;  // offset value
      SVnodeSidList *pLists = (SVnodeSidList *)pStr;

      tscTrace("%p metricmeta:vid:%d,numOfMeters:%d", pSql, i, pLists->numOfSids);

      pStr += sizeof(SVnodeSidList) + sizeof(SMeterSidExtInfo *) * pSidLists->numOfSids;
      rsp += sizeof(SVnodeSidList);

      size_t sidSize = sizeof(SMeterSidExtInfo) + pNewMetricMeta->tagLen;
      for (int32_t j = 0; j < pSidLists->numOfSids; ++j) {
        pLists->pSidExtInfoList[j] = pStr - (char *)pLists;
        memcpy(pStr, rsp, sidSize);

        rsp += sidSize;
        pStr += sidSize;
      }
    }

    sizes[k] = pStr - (char *)pNewMetricMeta;
  }

  for (int32_t i = 0; i < num; ++i) {
    char name[TSDB_MAX_TAGS_LEN + 1] = {0};

    SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(&pSql->cmd, i);
    tscGetMetricMetaCacheKey(&pSql->cmd, name, pMeterMetaInfo->pMeterMeta->uid);

#ifdef _DEBUG_VIEW
    printf("generate the metric key:%s, index:%d\n", name, i);
#endif

    // release the used metricmeta
    taosRemoveDataFromCache(tscCacheHandle, (void **)&(pMeterMetaInfo->pMetricMeta), false);

    pMeterMetaInfo->pMetricMeta = (SMetricMeta *)taosAddDataIntoCache(tscCacheHandle, name, (char *)metricMetaList[i],
                                                                      sizes[i], tsMetricMetaKeepTimer);
    tfree(metricMetaList[i]);

    // failed to put into cache
    if (pMeterMetaInfo->pMetricMeta == NULL) {
      pSql->res.code = TSDB_CODE_CLI_OUT_OF_MEMORY;
      goto _error_clean;
    }
  }

_error_clean:
  // free allocated resource
  for (int32_t i = 0; i < num; ++i) {
    tfree(metricMetaList[i]);
  }

  free(sizes);
  free(metricMetaList);

  return pSql->res.code;
}

/*
 * current process do not use the cache at all
 */
int tscProcessShowRsp(SSqlObj *pSql) {
  SMeterMeta * pMeta;
  SShowRspMsg *pShow;
  SSchema *    pSchema;
  char         key[20];

  SSqlRes *       pRes = &pSql->res;
  SSqlCmd *       pCmd = &pSql->cmd;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  pShow = (SShowRspMsg *)pRes->pRsp;
  pRes->qhandle = pShow->qhandle;

  tscResetForNextRetrieve(pRes);
  pMeta = &(pShow->meterMeta);

  pMeta->numOfColumns = ntohs(pMeta->numOfColumns);

  pSchema = (SSchema *)((char *)pMeta + sizeof(SMeterMeta));
  pMeta->sid = ntohs(pMeta->sid);
  for (int i = 0; i < pMeta->numOfColumns; ++i) {
    pSchema->bytes = htons(pSchema->bytes);
    pSchema++;
  }

  key[0] = pCmd->showType + 'a';
  strcpy(key + 1, "showlist");

  taosRemoveDataFromCache(tscCacheHandle, (void *)&(pMeterMetaInfo->pMeterMeta), false);

  int32_t size = pMeta->numOfColumns * sizeof(SSchema) + sizeof(SMeterMeta);
  pMeterMetaInfo->pMeterMeta =
      (SMeterMeta *)taosAddDataIntoCache(tscCacheHandle, key, (char *)pMeta, size, tsMeterMetaKeepTimer);
  pCmd->numOfCols = pCmd->fieldsInfo.numOfOutputCols;
  SSchema *pMeterSchema = tsGetSchema(pMeterMetaInfo->pMeterMeta);

  tscColumnBaseInfoReserve(&pCmd->colList, pMeta->numOfColumns);
  SColumnIndex index = {0};

  for (int16_t i = 0; i < pMeta->numOfColumns; ++i) {
    index.columnIndex = i;
    tscColumnBaseInfoInsert(pCmd, &index);
    tscFieldInfoSetValFromSchema(&pCmd->fieldsInfo, i, &pMeterSchema[i]);
  }

  tscFieldInfoCalOffset(pCmd);
  return 0;
}

int tscProcessConnectRsp(SSqlObj *pSql) {
  char         temp[TSDB_METER_ID_LEN];
  SConnectRsp *pConnect;

  STscObj *pObj = pSql->pTscObj;
  SSqlRes *pRes = &pSql->res;

  pConnect = (SConnectRsp *)pRes->pRsp;
  strcpy(pObj->acctId, pConnect->acctId);  // copy acctId from response
  sprintf(temp, "%s%s%s", pObj->acctId, TS_PATH_DELIMITER, pObj->db);
  strcpy(pObj->db, temp);
#ifdef CLUSTER
  SIpList *    pIpList;
  char *rsp = pRes->pRsp + sizeof(SConnectRsp);
  pIpList = (SIpList *)rsp;
  tscMgmtIpList.numOfIps = pIpList->numOfIps;
  for (int i = 0; i < pIpList->numOfIps; ++i) {
    tinet_ntoa(tscMgmtIpList.ipstr[i], pIpList->ip[i]);
    tscMgmtIpList.ip[i] = pIpList->ip[i];
  }

  rsp += sizeof(SIpList) + sizeof(int32_t) * pIpList->numOfIps;

  tscPrintMgmtIp();
#endif
  strcpy(pObj->sversion, pConnect->version);
  pObj->writeAuth = pConnect->writeAuth;
  pObj->superAuth = pConnect->superAuth;
  taosTmrReset(tscProcessActivityTimer, tsShellActivityTimer * 500, pObj, tscTmr, &pObj->pTimer);

  return 0;
}

int tscProcessUseDbRsp(SSqlObj *pSql) {
  STscObj *       pObj = pSql->pTscObj;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(&pSql->cmd, 0);

  strcpy(pObj->db, pMeterMetaInfo->name);
  return 0;
}

int tscProcessDropDbRsp(SSqlObj *UNUSED_PARAM(pSql)) {
  taosClearDataCache(tscCacheHandle);
  return 0;
}

int tscProcessDropTableRsp(SSqlObj *pSql) {
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(&pSql->cmd, 0);

  SMeterMeta *pMeterMeta = taosGetDataFromCache(tscCacheHandle, pMeterMetaInfo->name);
  if (pMeterMeta == NULL) {
    /* not in cache, abort */
    return 0;
  }

  /*
   * 1. if a user drops one table, which is the only table in a vnode, remove operation will incur vnode to be removed.
   * 2. Then, a user creates a new metric followed by a table with identical name of removed table but different schema,
   * here the table will reside in a new vnode.
   * The cached information is expired, however, we may have lost the ref of original meter. So, clear whole cache
   * instead.
   */
  tscTrace("%p force release metermeta after drop table:%s", pSql, pMeterMetaInfo->name);
  taosRemoveDataFromCache(tscCacheHandle, (void **)&pMeterMeta, true);

  if (pMeterMetaInfo->pMeterMeta) {
    taosRemoveDataFromCache(tscCacheHandle, (void **)&(pMeterMetaInfo->pMeterMeta), true);
    taosRemoveDataFromCache(tscCacheHandle, (void **)&(pMeterMetaInfo->pMetricMeta), true);
  }

  return 0;
}

int tscProcessAlterTableMsgRsp(SSqlObj *pSql) {
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(&pSql->cmd, 0);

  SMeterMeta *pMeterMeta = taosGetDataFromCache(tscCacheHandle, pMeterMetaInfo->name);
  if (pMeterMeta == NULL) { /* not in cache, abort */
    return 0;
  }

  tscTrace("%p force release metermeta in cache after alter-table: %s", pSql, pMeterMetaInfo->name);
  taosRemoveDataFromCache(tscCacheHandle, (void **)&pMeterMeta, true);

  if (pMeterMetaInfo->pMeterMeta) {
    bool isMetric = UTIL_METER_IS_METRIC(pMeterMetaInfo);

    taosRemoveDataFromCache(tscCacheHandle, (void **)&(pMeterMetaInfo->pMeterMeta), true);
    taosRemoveDataFromCache(tscCacheHandle, (void **)&(pMeterMetaInfo->pMetricMeta), true);

    if (isMetric) {  // if it is a metric, reset whole query cache
      tscTrace("%p reset query cache since table:%s is stable", pSql, pMeterMetaInfo->name);
      taosClearDataCache(tscCacheHandle);
    }
  }

  return 0;
}

int tscProcessAlterDbMsgRsp(SSqlObj *pSql) {
  UNUSED(pSql);
  return 0;
}

int tscProcessQueryRsp(SSqlObj *pSql) {
  SSqlRes *pRes = &pSql->res;

  pRes->qhandle = *((uint64_t *)pRes->pRsp);
  pRes->data = NULL;
  tscResetForNextRetrieve(pRes);
  return 0;
}

int tscProcessRetrieveRspFromVnode(SSqlObj *pSql) {
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;
  STscObj *pObj = pSql->pTscObj;

  SRetrieveMeterRsp *pRetrieve = (SRetrieveMeterRsp *)pRes->pRsp;

  pRes->numOfRows = htonl(pRetrieve->numOfRows);
  pRes->precision = htons(pRetrieve->precision);
  pRes->offset = htobe64(pRetrieve->offset);

  pRes->useconds = htobe64(pRetrieve->useconds);
  pRes->data = pRetrieve->data;

  tscSetResultPointer(pCmd, pRes);
  pRes->row = 0;

  /**
   * If the query result is exhausted, or current query is to free resource at server side,
   * the connection will be recycled.
   */
  if ((pRes->numOfRows == 0 && !(tscProjectionQueryOnMetric(pCmd) && pRes->offset > 0)) ||
      ((pCmd->type & TSDB_QUERY_TYPE_FREE_RESOURCE) == TSDB_QUERY_TYPE_FREE_RESOURCE)) {
    tscTrace("%p no result or free resource, recycle connection", pSql);
    taosAddConnIntoCache(tscConnCache, pSql->thandle, pSql->ip, pSql->vnode, pObj->user);
    pSql->thandle = NULL;
  } else {
    tscTrace("%p numOfRows:%d, offset:%d, not recycle connection", pSql, pRes->numOfRows, pRes->offset);
  }

  return 0;
}

int tscProcessRetrieveRspFromLocal(SSqlObj *pSql) {
  SSqlRes *          pRes = &pSql->res;
  SSqlCmd *          pCmd = &pSql->cmd;
  SRetrieveMeterRsp *pRetrieve = (SRetrieveMeterRsp *)pRes->pRsp;

  pRes->numOfRows = htonl(pRetrieve->numOfRows);
  pRes->data = pRetrieve->data;

  tscSetResultPointer(pCmd, pRes);
  pRes->row = 0;
  return 0;
}

void tscMeterMetaCallBack(void *param, TAOS_RES *res, int code);

static int32_t tscDoGetMeterMeta(SSqlObj *pSql, char *meterId, int32_t index) {
  int32_t code = TSDB_CODE_SUCCESS;

  SSqlObj *pNew = calloc(1, sizeof(SSqlObj));
  if (NULL == pNew) {
    tscError("%p malloc failed for new sqlobj to get meter meta", pSql);
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }
  pNew->pTscObj = pSql->pTscObj;
  pNew->signature = pNew;
  pNew->cmd.command = TSDB_SQL_META;
  pNew->cmd.payload = NULL;
  pNew->cmd.allocSize = 0;

  pNew->cmd.defaultVal[0] = pSql->cmd.defaultVal[0];  // flag of create table if not exists
  if (TSDB_CODE_SUCCESS != tscAllocPayload(&pNew->cmd, TSDB_DEFAULT_PAYLOAD_SIZE)) {
    tscError("%p malloc failed for payload to get meter meta", pSql);
    free(pNew);
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }

  SMeterMetaInfo *pMeterMetaInfo = tscAddEmptyMeterMetaInfo(&pNew->cmd);

  strcpy(pMeterMetaInfo->name, meterId);
  memcpy(pNew->cmd.payload, pSql->cmd.payload, TSDB_DEFAULT_PAYLOAD_SIZE);
  tscTrace("%p new pSqlObj:%p to get meterMeta", pSql, pNew);

  if (pSql->fp == NULL) {
    tsem_init(&pNew->rspSem, 0, 0);
    tsem_init(&pNew->emptyRspSem, 0, 1);

    code = tscProcessSql(pNew);
    SMeterMetaInfo *pInfo = tscGetMeterMetaInfo(&pSql->cmd, index);

    // update cache only on success get metermeta
    if (code == TSDB_CODE_SUCCESS) {
      pInfo->pMeterMeta = (SMeterMeta *)taosGetDataFromCache(tscCacheHandle, meterId);
    }

    tscTrace("%p get meter meta complete, code:%d, pMeterMeta:%p", pSql, code, pInfo->pMeterMeta);
    tscFreeSqlObj(pNew);

  } else {
    pNew->fp = tscMeterMetaCallBack;
    pNew->param = pSql;
    pNew->sqlstr = strdup(pSql->sqlstr);

    code = tscProcessSql(pNew);
    if (code == TSDB_CODE_SUCCESS) {
      code = TSDB_CODE_ACTION_IN_PROGRESS;
    }
  }

  return code;
}

int tscGetMeterMeta(SSqlObj *pSql, char *meterId, int32_t index) {
  SSqlCmd *       pCmd = &pSql->cmd;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, index);

  // if the SSqlCmd owns a metermeta, release it first
  taosRemoveDataFromCache(tscCacheHandle, (void **)&(pMeterMetaInfo->pMeterMeta), false);
  pMeterMetaInfo->pMeterMeta = (SMeterMeta *)taosGetDataFromCache(tscCacheHandle, meterId);

  if (pMeterMetaInfo->pMeterMeta != NULL) {
    SMeterMeta *pMeterMeta = pMeterMetaInfo->pMeterMeta;

    tscTrace("%p retrieve meterMeta from cache, the number of columns:%d, numOfTags:%d", pSql, pMeterMeta->numOfColumns,
             pMeterMeta->numOfTags);

    return TSDB_CODE_SUCCESS;
  }

  /*
   * for async insert operation, release data block buffer before issue new object to get metermeta
   * because in metermeta callback function, the tscParse function will generate the submit data blocks
   */
  //if (pSql->fp != NULL && pSql->pStream == NULL) {
  //  tscFreeSqlCmdData(pCmd);
  //}

  return tscDoGetMeterMeta(pSql, meterId, index);
}

int tscGetMeterMetaEx(SSqlObj *pSql, char *meterId, bool createIfNotExists) {
  pSql->cmd.defaultVal[0] = createIfNotExists ? 1 : 0;
  return tscGetMeterMeta(pSql, meterId, 0);
}

/*
 * in handling the renew metermeta problem during insertion,
 *
 * If the meter is created on demand during insertion, the routine usually waits for a short
 * period to re-issue the getMeterMeta msg, in which makes a greater change that vnode has
 * successfully created the corresponding table.
 */
static void tscWaitingForCreateTable(SSqlCmd *pCmd) {
  if (pCmd->command == TSDB_SQL_INSERT) {
    taosMsleep(50);  // todo: global config
  }
}

/**
 * in renew metermeta, do not retrieve metadata in cache.
 * @param pSql          sql object
 * @param meterId       meter id
 * @return              status code
 */
int tscRenewMeterMeta(SSqlObj *pSql, char *meterId) {
  int             code = 0;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(&pSql->cmd, 0);

  // handle metric meta renew process
  SSqlCmd *pCmd = &pSql->cmd;

  // enforce the renew metermeta operation in async model
  if (pSql->fp == NULL) pSql->fp = (void *)0x1;

  /*
   * 1. only update the metermeta in force model metricmeta is not updated
   * 2. if get metermeta failed, still get the metermeta
   */
  if (pMeterMetaInfo->pMeterMeta == NULL || !tscQueryOnMetric(pCmd)) {
    if (pMeterMetaInfo->pMeterMeta) {
      tscTrace("%p update meter meta, old: numOfTags:%d, numOfCols:%d, uid:%lld, addr:%p", pSql,
               pMeterMetaInfo->numOfTags, pCmd->numOfCols, pMeterMetaInfo->pMeterMeta->uid, pMeterMetaInfo->pMeterMeta);
    }
    tscWaitingForCreateTable(&pSql->cmd);
    taosRemoveDataFromCache(tscCacheHandle, (void **)&(pMeterMetaInfo->pMeterMeta), true);

    code = tscDoGetMeterMeta(pSql, meterId, 0);  // todo ??
  } else {
    tscTrace("%p metric query not update metric meta, numOfTags:%d, numOfCols:%d, uid:%lld, addr:%p", pSql,
             pMeterMetaInfo->pMeterMeta->numOfTags, pCmd->numOfCols, pMeterMetaInfo->pMeterMeta->uid,
             pMeterMetaInfo->pMeterMeta);
  }

  if (code != TSDB_CODE_ACTION_IN_PROGRESS) {
    if (pSql->fp == (void *)0x1) {
      pSql->fp = NULL;
    }
  }

  return code;
}

int tscGetMetricMeta(SSqlObj *pSql) {
  int      code = TSDB_CODE_NETWORK_UNAVAIL;
  SSqlCmd *pCmd = &pSql->cmd;

  /*
   * the vnode query condition is serialized into pCmd->payload, we need to rebuild key for metricmeta info in cache.
   */
  bool reqMetricMeta = false;
  for (int32_t i = 0; i < pSql->cmd.numOfTables; ++i) {
    char tagstr[TSDB_MAX_TAGS_LEN + 1] = {0};

    SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, i);
    tscGetMetricMetaCacheKey(pCmd, tagstr, pMeterMetaInfo->pMeterMeta->uid);

    taosRemoveDataFromCache(tscCacheHandle, (void **)&(pMeterMetaInfo->pMetricMeta), false);

    SMetricMeta *ppMeta = (SMetricMeta *)taosGetDataFromCache(tscCacheHandle, tagstr);
    if (ppMeta == NULL) {
      reqMetricMeta = true;
      break;
    } else {
      pMeterMetaInfo->pMetricMeta = ppMeta;
    }
  }

  // all metricmeta are retrieved from cache, no need to query mgmt node
  if (!reqMetricMeta) {
    return TSDB_CODE_SUCCESS;
  }

  SSqlObj *pNew = calloc(1, sizeof(SSqlObj));
  pNew->pTscObj = pSql->pTscObj;
  pNew->signature = pNew;

  pNew->cmd.command = TSDB_SQL_METRIC;

  for (int32_t i = 0; i < pSql->cmd.numOfTables; ++i) {
    SMeterMetaInfo *pMMInfo = tscGetMeterMetaInfo(&pSql->cmd, i);

    SMeterMeta *pMeterMeta = taosGetDataFromCache(tscCacheHandle, pMMInfo->name);
    tscAddMeterMetaInfo(&pNew->cmd, pMMInfo->name, pMeterMeta, NULL, pMMInfo->numOfTags, pMMInfo->tagColumnIndex);
  }

  if ((code = tscAllocPayload(&pNew->cmd, TSDB_DEFAULT_PAYLOAD_SIZE)) != TSDB_CODE_SUCCESS) {
    tscFreeSqlObj(pNew);
    return code;
  }

  // the query condition on meter is serialized into payload
  tscTagCondCopy(&pNew->cmd.tagCond, &pSql->cmd.tagCond);

  pNew->cmd.groupbyExpr = pSql->cmd.groupbyExpr;
  pNew->cmd.numOfTables = pSql->cmd.numOfTables;

  pNew->cmd.slimit = pSql->cmd.slimit;
  pNew->cmd.order = pSql->cmd.order;

  if (pSql->fp != NULL && pSql->pStream == NULL) {
    tscFreeSqlCmdData(&pSql->cmd);
  }

  tscTrace("%p allocate new pSqlObj:%p to get metricMeta", pSql, pNew);
  if (pSql->fp == NULL) {
    tsem_init(&pNew->rspSem, 0, 0);
    tsem_init(&pNew->emptyRspSem, 0, 1);

    code = tscProcessSql(pNew);

    for (int32_t i = 0; i < pCmd->numOfTables; ++i) {
      char tagstr[TSDB_MAX_TAGS_LEN] = {0};

      SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, i);
      tscGetMetricMetaCacheKey(pCmd, tagstr, pMeterMetaInfo->pMeterMeta->uid);

#ifdef _DEBUG_VIEW
      printf("create metric key:%s, index:%d\n", tagstr, i);
#endif

      taosRemoveDataFromCache(tscCacheHandle, (void **)&(pMeterMetaInfo->pMetricMeta), false);
      pMeterMetaInfo->pMetricMeta = (SMetricMeta *)taosGetDataFromCache(tscCacheHandle, tagstr);
    }

    tscFreeSqlObj(pNew);
  } else {
    pNew->fp = tscMeterMetaCallBack;
    pNew->param = pSql;
    code = tscProcessSql(pNew);
    if (code == TSDB_CODE_SUCCESS) {
      code = TSDB_CODE_ACTION_IN_PROGRESS;
    }
  }

  return code;
}

void tscInitMsgs() {
  tscBuildMsg[TSDB_SQL_SELECT] = tscBuildQueryMsg;
  tscBuildMsg[TSDB_SQL_INSERT] = tscBuildSubmitMsg;
  tscBuildMsg[TSDB_SQL_FETCH] = tscBuildRetrieveMsg;

  tscBuildMsg[TSDB_SQL_CREATE_DB] = tscBuildCreateDbMsg;
  tscBuildMsg[TSDB_SQL_CREATE_USER] = tscBuildCreateUserMsg;

  tscBuildMsg[TSDB_SQL_CREATE_ACCT] = tscBuildCreateAcctMsg;
  tscBuildMsg[TSDB_SQL_ALTER_ACCT] = tscBuildAlterAcctMsg;

  tscBuildMsg[TSDB_SQL_CREATE_TABLE] = tscBuildCreateTableMsg;
  tscBuildMsg[TSDB_SQL_DROP_USER] = tscBuildDropUserMsg;
  tscBuildMsg[TSDB_SQL_DROP_ACCT] = tscBuildDropAcctMsg;
  tscBuildMsg[TSDB_SQL_DROP_DB] = tscBuildDropDbMsg;
  tscBuildMsg[TSDB_SQL_DROP_TABLE] = tscBuildDropTableMsg;
  tscBuildMsg[TSDB_SQL_ALTER_USER] = tscBuildAlterUserMsg;
  tscBuildMsg[TSDB_SQL_CREATE_DNODE] = tscBuildCreateDnodeMsg;
  tscBuildMsg[TSDB_SQL_DROP_DNODE] = tscBuildDropDnodeMsg;
  tscBuildMsg[TSDB_SQL_CFG_DNODE] = tscBuildCfgDnodeMsg;
  tscBuildMsg[TSDB_SQL_ALTER_TABLE] = tscBuildAlterTableMsg;
  tscBuildMsg[TSDB_SQL_ALTER_DB] = tscAlterDbMsg;

  tscBuildMsg[TSDB_SQL_CONNECT] = tscBuildConnectMsg;
  tscBuildMsg[TSDB_SQL_USE_DB] = tscBuildUseDbMsg;
  tscBuildMsg[TSDB_SQL_META] = tscBuildMeterMetaMsg;
  tscBuildMsg[TSDB_SQL_METRIC] = tscBuildMetricMetaMsg;
  tscBuildMsg[TSDB_SQL_MULTI_META] = tscBuildMultiMeterMetaMsg;

  tscBuildMsg[TSDB_SQL_HB] = tscBuildHeartBeatMsg;
  tscBuildMsg[TSDB_SQL_SHOW] = tscBuildShowMsg;
  tscBuildMsg[TSDB_SQL_RETRIEVE] = tscBuildRetrieveFromMgmtMsg;
  tscBuildMsg[TSDB_SQL_KILL_QUERY] = tscBuildKillQueryMsg;
  tscBuildMsg[TSDB_SQL_KILL_STREAM] = tscBuildKillStreamMsg;
  tscBuildMsg[TSDB_SQL_KILL_CONNECTION] = tscBuildKillConnectionMsg;

  tscProcessMsgRsp[TSDB_SQL_SELECT] = tscProcessQueryRsp;
  tscProcessMsgRsp[TSDB_SQL_FETCH] = tscProcessRetrieveRspFromVnode;

  tscProcessMsgRsp[TSDB_SQL_DROP_DB] = tscProcessDropDbRsp;
  tscProcessMsgRsp[TSDB_SQL_DROP_TABLE] = tscProcessDropTableRsp;
  tscProcessMsgRsp[TSDB_SQL_CONNECT] = tscProcessConnectRsp;
  tscProcessMsgRsp[TSDB_SQL_USE_DB] = tscProcessUseDbRsp;
  tscProcessMsgRsp[TSDB_SQL_META] = tscProcessMeterMetaRsp;
  tscProcessMsgRsp[TSDB_SQL_METRIC] = tscProcessMetricMetaRsp;
  tscProcessMsgRsp[TSDB_SQL_MULTI_META] = tscProcessMultiMeterMetaRsp;

  tscProcessMsgRsp[TSDB_SQL_SHOW] = tscProcessShowRsp;
  tscProcessMsgRsp[TSDB_SQL_RETRIEVE] = tscProcessRetrieveRspFromVnode;   // rsp handled by same function.
  tscProcessMsgRsp[TSDB_SQL_DESCRIBE_TABLE] = tscProcessDescribeTableRsp;
  
  tscProcessMsgRsp[TSDB_SQL_RETRIEVE_TAGS] = tscProcessTagRetrieveRsp;
  tscProcessMsgRsp[TSDB_SQL_CURRENT_DB] = tscProcessTagRetrieveRsp;
  tscProcessMsgRsp[TSDB_SQL_CURRENT_USER] = tscProcessTagRetrieveRsp;
  tscProcessMsgRsp[TSDB_SQL_SERV_VERSION] = tscProcessTagRetrieveRsp;
  tscProcessMsgRsp[TSDB_SQL_CLI_VERSION] = tscProcessTagRetrieveRsp;
  tscProcessMsgRsp[TSDB_SQL_SERV_STATUS] = tscProcessTagRetrieveRsp;
  
  tscProcessMsgRsp[TSDB_SQL_RETRIEVE_EMPTY_RESULT] = tscProcessEmptyResultRsp;

  tscProcessMsgRsp[TSDB_SQL_RETRIEVE_METRIC] = tscProcessRetrieveMetricRsp;

  tscProcessMsgRsp[TSDB_SQL_ALTER_TABLE] = tscProcessAlterTableMsgRsp;
  tscProcessMsgRsp[TSDB_SQL_ALTER_DB] = tscProcessAlterDbMsgRsp;

  tscKeepConn[TSDB_SQL_SHOW] = 1;
  tscKeepConn[TSDB_SQL_RETRIEVE] = 1;
  tscKeepConn[TSDB_SQL_SELECT] = 1;
  tscKeepConn[TSDB_SQL_FETCH] = 1;
  tscKeepConn[TSDB_SQL_HB] = 1;

  tscUpdateVnodeMsg[TSDB_SQL_SELECT] = tscUpdateVnodeInQueryMsg;
  tscUpdateVnodeMsg[TSDB_SQL_INSERT] = tscUpdateVnodeInSubmitMsg;
}
