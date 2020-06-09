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
#include "tscSQLParser.h"
#include "tscSecondaryMerge.h"
#include "tscUtil.h"
#include "tschemautil.h"
#include "tsclient.h"
#include "tscompression.h"
#include "tsocket.h"
#include "ttime.h"
#include "ttimer.h"
#include "tutil.h"

#define TSC_MGMT_VNODE 999

SIpStrList tscMgmtIpList;
int        tsMasterIndex = 0;
int        tsSlaveIndex = 1;

int (*tscBuildMsg[TSDB_SQL_MAX])(SSqlObj *pSql, SSqlInfo *pInfo) = {0};

int (*tscProcessMsgRsp[TSDB_SQL_MAX])(SSqlObj *pSql);
char *doBuildMsgHeader(SSqlObj *pSql, char **pStart);
void (*tscUpdateVnodeMsg[TSDB_SQL_MAX])(SSqlObj *pSql, char *buf);
void tscProcessActivityTimer(void *handle, void *tmrId);
int tscKeepConn[TSDB_SQL_MAX] = {0};
TSKEY tscGetSubscriptionProgress(void* sub, int64_t uid);
void tscUpdateSubscriptionProgress(void* sub, int64_t uid, TSKEY ts);
void tscSaveSubscriptionProgress(void* sub);

static int32_t minMsgSize() { return tsRpcHeadSize + sizeof(STaosDigest); }

void tscPrintMgmtIp() {
  if (tscMgmtIpList.numOfIps <= 0) {
    tscError("invalid mgmt IP list:%d", tscMgmtIpList.numOfIps);
  } else {
    for (int i = 0; i < tscMgmtIpList.numOfIps; ++i) {
      tscTrace("mgmt index:%d ip:%s", i, tscMgmtIpList.ipstr[i]);
    }
  }
}

void tscSetMgmtIpListFromCluster(SIpList *pIpList) {
  tscMgmtIpList.numOfIps = pIpList->numOfIps;
  if (memcmp(tscMgmtIpList.ip, pIpList->ip, pIpList->numOfIps * 4) != 0) {
    for (int i = 0; i < pIpList->numOfIps; ++i) {
      tinet_ntoa(tscMgmtIpList.ipstr[i], pIpList->ip[i]);
      tscMgmtIpList.ip[i] = pIpList->ip[i];
    }
    tscTrace("cluster mgmt IP list:");
    tscPrintMgmtIp();
  }
}

void tscSetMgmtIpListFromEdge() {
  if (tscMgmtIpList.numOfIps != 2) {
    tscMgmtIpList.numOfIps = 2;
    strcpy(tscMgmtIpList.ipstr[0], tsMasterIp);
    tscMgmtIpList.ip[0] = inet_addr(tsMasterIp);
    strcpy(tscMgmtIpList.ipstr[1], tsMasterIp);
    tscMgmtIpList.ip[1] = inet_addr(tsMasterIp);
    tscTrace("edge mgmt IP list:");
    tscPrintMgmtIp();
  }
}

void tscSetMgmtIpList(SIpList *pIpList) {
  /*
    * The iplist returned by the cluster edition is the current management nodes
    * and the iplist returned by the edge edition is empty
    */
  if (pIpList->numOfIps != 0) {
    tscSetMgmtIpListFromCluster(pIpList);
  } else {
    tscSetMgmtIpListFromEdge();
  }
}

/*
 * For each management node, try twice at least in case of poor network situation.
 * If the client start to connect to a non-management node from the client, and the first retry may fail due to
 * the poor network quality. And then, the second retry get the response with redirection command.
 * The retry will not be executed since only *two* retry is allowed in case of single management node in the cluster.
 * Therefore, we need to multiply the retry times by factor of 2 to fix this problem.
 */
static int32_t tscGetMgmtConnMaxRetryTimes() {
  int32_t factor = 2;
  return tscMgmtIpList.numOfIps * factor;
}

int32_t tscProcessHeartBeatRsp(void *param, TAOS_RES *tres, int code) {
  STscObj *pObj = (STscObj *)param;
  if (pObj == NULL) return TSDB_CODE_APP_ERROR;
  if (pObj != pObj->signature) {
    tscError("heart beat msg, pObj:%p, signature:%p invalid", pObj, pObj->signature);
    return TSDB_CODE_APP_ERROR;
  }

  SSqlObj *pSql = pObj->pHb;
  SSqlRes *pRes = &pSql->res;

  if (code == 0) {
    SHeartBeatRsp *pRsp = (SHeartBeatRsp *)pRes->pRsp;
    SIpList *      pIpList = &pRsp->ipList;
    tscSetMgmtIpList(pIpList);

    if (pRsp->killConnection) {
      tscKillConnection(pObj);
    } else {
      if (pRsp->queryId) tscKillQuery(pObj, pRsp->queryId);
      if (pRsp->streamId) tscKillStream(pObj, pRsp->streamId);
    }
    
    if (pRes->data == NULL) {
      pRes->data = calloc(2, sizeof(int32_t));
    }
    
    ((int32_t*)pRes->data)[0] = htonl(pRsp->totalDnodes);
    ((int32_t*)pRes->data)[1] = htonl(pRsp->onlineDnodes);
  } else {
    tscTrace("heart beat failed, code:%d", code);
  }

  taosTmrReset(tscProcessActivityTimer, tsShellActivityTimer * 500, pObj, tscTmr, &pObj->pTimer);
  return code;
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
    
    SQueryInfo *pQueryInfo = NULL;
    tscGetQueryInfoDetailSafely(&pSql->cmd, 0, &pQueryInfo);
    pQueryInfo->command = TSDB_SQL_HB;
    
    if (TSDB_CODE_SUCCESS != tscAllocPayload(&(pSql->cmd), TSDB_DEFAULT_PAYLOAD_SIZE)) {
      tfree(pSql);
      return;
    }

    pSql->cmd.command = TSDB_SQL_HB;
    pSql->param = pObj;
    pSql->pTscObj = pObj;
    pSql->signature = pSql;
    pObj->pHb = pSql;
    tscAddSubqueryInfo(&pObj->pHb->cmd);

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
  if (pSql->retry < tscGetMgmtConnMaxRetryTimes()) {
    *pCode = 0;
    pSql->retry++;
    pSql->index = pSql->index % tscMgmtIpList.numOfIps;
    if (pSql->cmd.command > TSDB_SQL_READ && pSql->index == 0) pSql->index = 1;
    void *thandle = taosGetConnFromCache(tscConnCache, tscMgmtIpList.ip[pSql->index], TSC_MGMT_VNODE, pTscObj->user);

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
      
      connInit.peerIp = tscMgmtIpList.ipstr[pSql->index];
      thandle = taosOpenRpcConn(&connInit, pCode);
    }

    pSql->thandle = thandle;
    pSql->ip = tscMgmtIpList.ip[pSql->index];
    pSql->vnode = TSC_MGMT_VNODE;
    tscTrace("%p mgmt index:%d ip:0x%x is picked up, pConn:%p", pSql, pSql->index, tscMgmtIpList.ip[pSql->index],
             pSql->thandle);
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
  SVPeerDesc *pVPeersDesc = NULL;
  static int  vidIndex = 0;
  STscObj *   pTscObj = pSql->pTscObj;

  pSql->thandle = NULL;

  SSqlCmd *       pCmd = &pSql->cmd;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, pCmd->clauseIndex, 0);
  
  if (UTIL_METER_IS_SUPERTABLE(pMeterMetaInfo)) {  // multiple vnode query
    SVnodeSidList *vnodeList = tscGetVnodeSidList(pMeterMetaInfo->pMetricMeta, pMeterMetaInfo->vnodeIndex);
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
    pSql->index = pSql->index%TSDB_VNODES_SUPPORT;
    
    char ipstr[40] = {0};
    if (pVPeersDesc[pSql->index].ip == 0) {
      /*
       * in the edge edition, ip is 0, and at this time we use masterIp instead
       * in the cluster edition, ip is vnode ip
       */
      pVPeersDesc[pSql->index].ip = tscMgmtIpList.ip[0];
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
      uint64_t signature = (uint64_t)pSql->signature;
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

void tscProcessMgmtRedirect(SSqlObj *pSql, uint8_t *cont) {
  SIpList *pIpList = (SIpList *)(cont);
  tscSetMgmtIpList(pIpList);

  if (pSql->cmd.command < TSDB_SQL_READ) {
    tsMasterIndex = 0;
    pSql->index = 0;
  } else {
    pSql->index++;
  }

  tscPrintMgmtIp();
}

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
    //taosAddConnIntoCache(tscConnCache, pSql->thandle, pSql->ip, pSql->vnode, pObj->user);
    tscFreeSqlObj(pSql);
    return NULL;
  }

  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, pCmd->clauseIndex, 0);
  if (msg == NULL) {
    tscTrace("%p no response from ip:%s", pSql, taosIpStr(pSql->ip));

    pSql->index++;
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
      pSql->maxRetry = TSDB_VNODES_SUPPORT * 2;
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
    
    if (rspCode == TSDB_CODE_REDIRECT) {
      tscTrace("%p it shall be redirected!", pSql);
      taosAddConnIntoCache(tscConnCache, thandle, pSql->ip, pSql->vnode, pObj->user);
      pSql->thandle = NULL;

      if (pCmd->command > TSDB_SQL_MGMT) {
        tscProcessMgmtRedirect(pSql, pMsg->content + 1);
      } else if (pCmd->command == TSDB_SQL_INSERT) {
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
        rspCode == TSDB_CODE_NETWORK_UNAVAIL || rspCode == TSDB_CODE_NOT_ACTIVE_SESSION ||
        rspCode == TSDB_CODE_TABLE_ID_MISMATCH) {
      /*
       * not_active_table: 1. the virtual node may fail to create table, since the procedure of create table is asynchronized,
       *                   the virtual node may have not create table till now, so try again by using the new metermeta.
       *                   2. this requested table may have been removed by other client, so we need to renew the
       *                   metermeta here.
       *
       * not_active_vnode: current vnode is move to other node due to node balance procedure or virtual node have been
       *                   removed. So, renew metermeta and try again.
       * not_active_session: db has been move to other node, the vnode does not exist on this dnode anymore.
       */
     pSql->thandle = NULL;
      taosAddConnIntoCache(tscConnCache, thandle, pSql->ip, pSql->vnode, pObj->user);

      if (pCmd->command == TSDB_SQL_CONNECT) {
        code = TSDB_CODE_NETWORK_UNAVAIL;
      } else if (pCmd->command == TSDB_SQL_HB) {
        code = TSDB_CODE_NOT_READY;
      } else {
        tscTrace("%p it shall renew meter meta, code:%d", pSql, rspCode);

        pSql->maxRetry = TSDB_VNODES_SUPPORT * 2;
        pSql->res.code = (uint8_t)rspCode;  // keep the previous error code

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
        SVnodeSidList *pVnodeSidList = tscGetVnodeSidList(pMeterMetaInfo->pMetricMeta, pMeterMetaInfo->vnodeIndex);
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
          tscTrace("%p Async sql is automatically freed", pSql);
          tscFreeSqlObj(pSql);
        }
      }
    }
  }

  return ahandle;
}

static SSqlObj *tscCreateSqlObjForSubquery(SSqlObj *pSql, SRetrieveSupport *trsupport, SSqlObj *prevSqlObj);
static int      tscLaunchSTableSubqueries(SSqlObj *pSql);

// todo merge with callback
int32_t tscLaunchJoinSubquery(SSqlObj *pSql, int16_t tableIndex, SJoinSubquerySupporter *pSupporter) {
  SSqlCmd *   pCmd = &pSql->cmd;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);

  pSql->res.qhandle = 0x1;
  pSql->res.numOfRows = 0;

  if (pSql->pSubs == NULL) {
    pSql->pSubs = calloc(pSupporter->pState->numOfTotal, POINTER_BYTES);
    if (pSql->pSubs == NULL) {
      return TSDB_CODE_CLI_OUT_OF_MEMORY;
    }
  }

  SSqlObj *pNew = createSubqueryObj(pSql, tableIndex, tscJoinQueryCallback, pSupporter, NULL);
  if (pNew == NULL) {
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }

  pSql->pSubs[pSql->numOfSubs++] = pNew;
  assert(pSql->numOfSubs <= pSupporter->pState->numOfTotal);
  
  if (QUERY_IS_JOIN_QUERY(pQueryInfo->type)) {
    addGroupInfoForSubquery(pSql, pNew, 0, tableIndex);

    // refactor as one method
    SQueryInfo *pNewQueryInfo = tscGetQueryInfoDetail(&pNew->cmd, 0);
    assert(pNewQueryInfo != NULL);

    tscColumnBaseInfoUpdateTableIndex(&pNewQueryInfo->colList, 0);
    tscColumnBaseInfoCopy(&pSupporter->colList, &pNewQueryInfo->colList, 0);

    tscSqlExprCopy(&pSupporter->exprsInfo, &pNewQueryInfo->exprsInfo, pSupporter->uid, false);
    tscFieldInfoCopyAll(&pSupporter->fieldsInfo, &pNewQueryInfo->fieldsInfo);
    
    tscTagCondCopy(&pSupporter->tagCond, &pNewQueryInfo->tagCond);

    pNew->cmd.numOfCols = 0;
    pNewQueryInfo->intervalTime = 0;
    memset(&pNewQueryInfo->limit, 0, sizeof(SLimitVal));

    // backup the data and clear it in the sqlcmd object
    pSupporter->groupbyExpr = pNewQueryInfo->groupbyExpr;
    memset(&pNewQueryInfo->groupbyExpr, 0, sizeof(SSqlGroupbyExpr));

    // this data needs to be transfer to support struct
    pNewQueryInfo->fieldsInfo.numOfOutputCols = 0;
    pNewQueryInfo->exprsInfo.numOfExprs = 0;
    
    // set the ts,tags that involved in join, as the output column of intermediate result
    tscClearSubqueryInfo(&pNew->cmd);

    SSchema      colSchema = {.type = TSDB_DATA_TYPE_BINARY, .bytes = 1};
    SColumnIndex index = {0, PRIMARYKEY_TIMESTAMP_COL_INDEX};

    tscAddSpecialColumnForSelect(pNewQueryInfo, 0, TSDB_FUNC_TS_COMP, &index, &colSchema, TSDB_COL_NORMAL);

    // set the tags value for ts_comp function
    SSqlExpr *pExpr = tscSqlExprGet(pNewQueryInfo, 0);

    SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfoFromQueryInfo(pNewQueryInfo, 0);
    int16_t         tagColIndex = tscGetJoinTagColIndexByUid(&pSupporter->tagCond, pMeterMetaInfo->pMeterMeta->uid);

    pExpr->param->i64Key = tagColIndex;
    pExpr->numOfParams = 1;

    // add the filter tag column
    for (int32_t i = 0; i < pSupporter->colList.numOfCols; ++i) {
      SColumnBase *pColBase = &pSupporter->colList.pColList[i];
      if (pColBase->numOfFilters > 0) {  // copy to the pNew->cmd.colList if it is filtered.
        tscColumnBaseCopy(&pNewQueryInfo->colList.pColList[pNewQueryInfo->colList.numOfCols], pColBase);
        pNewQueryInfo->colList.numOfCols++;
      }
    }
  
    tscTrace("%p subquery:%p tableIndex:%d, vnodeIdx:%d, type:%d, transfer to ts_comp query to retrieve timestamps, "
             "exprInfo:%d, colList:%d, fieldsInfo:%d, name:%s",
             pSql, pNew, tableIndex, pMeterMetaInfo->vnodeIndex, pNewQueryInfo->type,
             pNewQueryInfo->exprsInfo.numOfExprs, pNewQueryInfo->colList.numOfCols,
             pNewQueryInfo->fieldsInfo.numOfOutputCols, pNewQueryInfo->pMeterInfo[0]->name);
    tscPrintSelectClause(pNew, 0);
  } else {
    SQueryInfo *pNewQueryInfo = tscGetQueryInfoDetail(&pNew->cmd, 0);
    pNewQueryInfo->type |= TSDB_QUERY_TYPE_SUBQUERY;
  }

#ifdef _DEBUG_VIEW
  tscPrintSelectClause(pNew, 0);
#endif
  
  return tscProcessSql(pNew);
}

int doProcessSql(SSqlObj *pSql) {
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;
  int32_t  code = TSDB_CODE_SUCCESS;

  void *asyncFp = pSql->fp;
  if (pCmd->command == TSDB_SQL_SELECT || pCmd->command == TSDB_SQL_FETCH || pCmd->command == TSDB_SQL_RETRIEVE ||
      pCmd->command == TSDB_SQL_INSERT || pCmd->command == TSDB_SQL_CONNECT || pCmd->command == TSDB_SQL_HB ||
      pCmd->command == TSDB_SQL_META || pCmd->command == TSDB_SQL_METRIC) {
    code = tscBuildMsg[pCmd->command](pSql, NULL);
  }

  if (code != TSDB_CODE_SUCCESS) {
    pRes->code = code;
    return code;
  }

  code = tscSendMsgToServer(pSql);

  if (asyncFp) {
    if (code != TSDB_CODE_SUCCESS) {
      pRes->code = code;
      tscQueueAsyncRes(pSql);
    }
    return 0;
  }

  if (code != TSDB_CODE_SUCCESS) {
    pRes->code = code;
    return code;
  }

  tsem_wait(&pSql->rspSem);

  if (pRes->code == TSDB_CODE_SUCCESS && tscProcessMsgRsp[pCmd->command]) (*tscProcessMsgRsp[pCmd->command])(pSql);

  tsem_post(&pSql->emptyRspSem);

  return pRes->code;
}

int tscProcessSql(SSqlObj *pSql) {
  char *   name = NULL;
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;
  
  SQueryInfo *    pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  SMeterMetaInfo *pMeterMetaInfo = NULL;
  uint32_t         type = 0;

  if (pQueryInfo != NULL) {
    pMeterMetaInfo = tscGetMeterMetaInfoFromQueryInfo(pQueryInfo, 0);
    if (pMeterMetaInfo != NULL) {
      name = pMeterMetaInfo->name;
    }

    type = pQueryInfo->type;
  
    // for hearbeat, numOfTables == 0;
    assert((pQueryInfo->numOfTables == 0 && pQueryInfo->command == TSDB_SQL_HB) || pQueryInfo->numOfTables > 0);
  }

  tscTrace("%p SQL cmd:%d will be processed, name:%s, type:%d", pSql, pCmd->command, name, type);
  pSql->retry = 0;
  if (pSql->cmd.command < TSDB_SQL_MGMT) {
    pSql->maxRetry = TSDB_VNODES_SUPPORT;

    // the pMeterMetaInfo cannot be NULL
    if (pMeterMetaInfo == NULL) {
      pSql->res.code = TSDB_CODE_OTHERS;
      return pSql->res.code;
    }

    if (UTIL_METER_IS_NOMRAL_METER(pMeterMetaInfo)) {
      pSql->index = pMeterMetaInfo->pMeterMeta->index;
    } else {  // it must be the parent SSqlObj for super table query
      if ((pQueryInfo->type & TSDB_QUERY_TYPE_SUBQUERY) != 0) {
        int32_t idx = pMeterMetaInfo->vnodeIndex;

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
  if (QUERY_IS_JOIN_QUERY(type)) {
    if ((pQueryInfo->type & TSDB_QUERY_TYPE_SUBQUERY) == 0) {
      SSubqueryState *pState = calloc(1, sizeof(SSubqueryState));
      pState->numOfTotal = pQueryInfo->numOfTables;

      if ((pQueryInfo->type & TSDB_QUERY_TYPE_TS_NO_MATCH_JOIN_QUERY) != 0) {
        pSql->numOfSubs = pQueryInfo->numOfTables;
        if (pSql->pSubs == NULL) {
          pSql->pSubs = calloc(pSql->numOfSubs, POINTER_BYTES);
          if (pSql->pSubs == NULL) {
            return TSDB_CODE_CLI_OUT_OF_MEMORY;
          }
        }
        
        tscLaunchSecondPhaseDirectly(pSql, pState);
      } else {
        for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
          SJoinSubquerySupporter *pSupporter = tscCreateJoinSupporter(pSql, pState, i);
    
          if (pSupporter == NULL) {  // failed to create support struct, abort current query
            tscError("%p tableIndex:%d, failed to allocate join support object, abort further query", pSql, i);
            pState->numOfCompleted = pQueryInfo->numOfTables - i - 1;
            pSql->res.code = TSDB_CODE_CLI_OUT_OF_MEMORY;
      
            return pSql->res.code;
          }
    
          int32_t code = tscLaunchJoinSubquery(pSql, i, pSupporter);
          if (code != TSDB_CODE_SUCCESS) {  // failed to create subquery object, quit query
            tscDestroyJoinSupporter(pSupporter);
            pSql->res.code = TSDB_CODE_CLI_OUT_OF_MEMORY;
      
            break;
          }
        }
      }

      tsem_post(&pSql->emptyRspSem);
      tsem_wait(&pSql->rspSem);

      tsem_post(&pSql->emptyRspSem);

      if (pSql->numOfSubs <= 0) {
        pSql->cmd.command = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
      } else {
        pSql->cmd.command = TSDB_SQL_METRIC_JOIN_RETRIEVE;
      }

      return TSDB_CODE_SUCCESS;
    } else {
      // for first stage sub query, iterate all vnodes to get all timestamp
      if ((pQueryInfo->type & TSDB_QUERY_TYPE_JOIN_SEC_STAGE) != TSDB_QUERY_TYPE_JOIN_SEC_STAGE) {
        return doProcessSql(pSql);
      }
    }
  }

  if (tscIsTwoStageMergeMetricQuery(pQueryInfo, 0)) {
    /*
     * (ref. line: 964)
     * Before this function returns from tscLaunchSTableSubqueries and continues, pSql may have been released at user
     * program context after retrieving all data from vnodes. User function is called at tscRetrieveFromVnodeCallBack.
     *
     * when pSql being released, pSql->fp == NULL, it may pass the check of pSql->fp == NULL,
     * which causes deadlock. So we keep it as local variable.
     */
    void *fp = pSql->fp;

    if (tscLaunchSTableSubqueries(pSql) != TSDB_CODE_SUCCESS) {
      return pRes->code;
    }

    if (fp == NULL) {
      tsem_post(&pSql->emptyRspSem);
      tsem_wait(&pSql->rspSem);
      tsem_post(&pSql->emptyRspSem);

      // set the command flag must be after the semaphore been correctly set.
      pSql->cmd.command = TSDB_SQL_RETRIEVE_METRIC;
    }

    return pSql->res.code;
  }

  return doProcessSql(pSql);
}

static void doCleanupSubqueries(SSqlObj *pSql, int32_t numOfSubs, SSubqueryState* pState) {
  assert(numOfSubs <= pSql->numOfSubs && numOfSubs >= 0 && pState != NULL);
  
  for(int32_t i = 0; i < numOfSubs; ++i) {
    SSqlObj* pSub = pSql->pSubs[i];
    assert(pSub != NULL);
    
    SRetrieveSupport* pSupport = pSub->param;
  
    tfree(pSupport->localBuffer);
  
    pthread_mutex_unlock(&pSupport->queryMutex);
    pthread_mutex_destroy(&pSupport->queryMutex);
  
    tfree(pSupport);
  
    tscFreeSqlObj(pSub);
  }
  
  free(pState);
}

int tscLaunchSTableSubqueries(SSqlObj *pSql) {
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

  // pRes->code check only serves in launching metric sub-queries
  if (pRes->code == TSDB_CODE_QUERY_CANCELLED) {
    pCmd->command = TSDB_SQL_RETRIEVE_METRIC;  // enable the abort of kill metric function.
    return pRes->code;
  }

  tExtMemBuffer **  pMemoryBuf = NULL;
  tOrderDescriptor *pDesc = NULL;
  SColumnModel *    pModel = NULL;

  pRes->qhandle = 1;  // hack the qhandle check

  const uint32_t nBufferSize = (1 << 16);  // 64KB

  SQueryInfo *    pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfoFromQueryInfo(pQueryInfo, 0);
  int32_t         numOfSubQueries = pMeterMetaInfo->pMetricMeta->numOfVnodes;
  assert(numOfSubQueries > 0);

  int32_t ret = tscLocalReducerEnvCreate(pSql, &pMemoryBuf, &pDesc, &pModel, nBufferSize);
  if (ret != 0) {
    pRes->code = TSDB_CODE_CLI_OUT_OF_MEMORY;
    if (pSql->fp) {
      tscQueueAsyncRes(pSql);
    }
    return pRes->code;
  }

  pSql->pSubs = calloc(numOfSubQueries, POINTER_BYTES);
  pSql->numOfSubs = numOfSubQueries;

  tscTrace("%p retrieved query data from %d vnode(s)", pSql, numOfSubQueries);
  SSubqueryState *pState = calloc(1, sizeof(SSubqueryState));
  pState->numOfTotal = numOfSubQueries;
  pRes->code = TSDB_CODE_SUCCESS;

  int32_t i = 0;
  for (; i < numOfSubQueries; ++i) {
    SRetrieveSupport *trs = (SRetrieveSupport *)calloc(1, sizeof(SRetrieveSupport));
    if (trs == NULL) {
      tscError("%p failed to malloc buffer for SRetrieveSupport, orderOfSub:%d, reason:%s", pSql, i, strerror(errno));
      break;
    }
    
    trs->pExtMemBuffer = pMemoryBuf;
    trs->pOrderDescriptor = pDesc;
    trs->pState = pState;
    
    trs->localBuffer = (tFilePage *)calloc(1, nBufferSize + sizeof(tFilePage));
    if (trs->localBuffer == NULL) {
      tscError("%p failed to malloc buffer for local buffer, orderOfSub:%d, reason:%s", pSql, i, strerror(errno));
      tfree(trs);
      break;
    }
    
    trs->subqueryIndex = i;
    trs->pParentSqlObj = pSql;
    trs->pFinalColModel = pModel;

    pthread_mutexattr_t mutexattr = {0};
    pthread_mutexattr_settype(&mutexattr, PTHREAD_MUTEX_RECURSIVE_NP);
    pthread_mutex_init(&trs->queryMutex, &mutexattr);
    pthread_mutexattr_destroy(&mutexattr);

    SSqlObj *pNew = tscCreateSqlObjForSubquery(pSql, trs, NULL);
    if (pNew == NULL) {
      tscError("%p failed to malloc buffer for subObj, orderOfSub:%d, reason:%s", pSql, i, strerror(errno));
      tfree(trs->localBuffer);
      tfree(trs);
      break;
    }

    // todo handle multi-vnode situation
    if (pQueryInfo->tsBuf) {
      SQueryInfo *pNewQueryInfo = tscGetQueryInfoDetail(&pNew->cmd, 0);
      pNewQueryInfo->tsBuf = tsBufClone(pQueryInfo->tsBuf);
    }
    
    tscTrace("%p sub:%p create subquery success. orderOfSub:%d", pSql, pNew, trs->subqueryIndex);
  }
  
  if (i < numOfSubQueries) {
    tscError("%p failed to prepare subquery structure and launch subqueries", pSql);
    pRes->code = TSDB_CODE_CLI_OUT_OF_MEMORY;
  
    tscLocalReducerEnvDestroy(pMemoryBuf, pDesc, pModel, numOfSubQueries);
    doCleanupSubqueries(pSql, i, pState);
    return pRes->code;   // free all allocated resource
  }
  
  if (pRes->code == TSDB_CODE_QUERY_CANCELLED) {
    tscLocalReducerEnvDestroy(pMemoryBuf, pDesc, pModel, numOfSubQueries);
    doCleanupSubqueries(pSql, i, pState);
    return pRes->code;
  }
  
  for(int32_t j = 0; j < numOfSubQueries; ++j) {
    SSqlObj* pSub = pSql->pSubs[j];
    SRetrieveSupport* pSupport = pSub->param;
    
    tscTrace("%p sub:%p launch subquery, orderOfSub:%d.", pSql, pSub, pSupport->subqueryIndex);
    int code = tscProcessSql(pSub);
    if (code != TSDB_CODE_SUCCESS) {
      tscLocalReducerEnvDestroy(pMemoryBuf, pDesc, pModel, numOfSubQueries);
      doCleanupSubqueries(pSql, i, pState);
      pRes->code = code;
      return pRes->code;
    }
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
  int32_t  subqueryIndex = trsupport->subqueryIndex;

  assert(pSql != NULL);
  SSubqueryState* pState = trsupport->pState;
  assert(pState->numOfCompleted < pState->numOfTotal && pState->numOfCompleted >= 0 &&
         pPObj->numOfSubs == pState->numOfTotal);

  /* retrieved in subquery failed. OR query cancelled in retrieve phase. */
  if (pState->code == TSDB_CODE_SUCCESS && pPObj->res.code != TSDB_CODE_SUCCESS) {
    pState->code = -(int)pPObj->res.code;

    /*
     * kill current sub-query connection, which may retrieve data from vnodes;
     * Here we get: pPObj->res.code == TSDB_CODE_QUERY_CANCELLED
     */
    pSql->res.numOfRows = 0;
    trsupport->numOfRetry = MAX_NUM_OF_SUBQUERY_RETRY;  // disable retry efforts
    tscTrace("%p query is cancelled, sub:%p, orderOfSub:%d abort retrieve, code:%d", trsupport->pParentSqlObj, pSql,
             subqueryIndex, pState->code);
  }

  if (numOfRows >= 0) {  // current query is successful, but other sub query failed, still abort current query.
    tscTrace("%p sub:%p retrieve numOfRows:%d,orderOfSub:%d", pPObj, pSql, numOfRows, subqueryIndex);
    tscError("%p sub:%p abort further retrieval due to other queries failure,orderOfSub:%d,code:%d", pPObj, pSql,
        subqueryIndex, pState->code);
  } else {
    if (trsupport->numOfRetry++ < MAX_NUM_OF_SUBQUERY_RETRY && pState->code == TSDB_CODE_SUCCESS) {
      /*
       * current query failed, and the retry count is less than the available
       * count, retry query clear previous retrieved data, then launch a new sub query
       */
      tExtMemBufferClear(trsupport->pExtMemBuffer[subqueryIndex]);

      // clear local saved number of results
      trsupport->localBuffer->numOfElems = 0;
      pthread_mutex_unlock(&trsupport->queryMutex);

      tscTrace("%p sub:%p retrieve failed, code:%d, orderOfSub:%d, retry:%d", trsupport->pParentSqlObj, pSql, numOfRows,
               subqueryIndex, trsupport->numOfRetry);

      SSqlObj *pNew = tscCreateSqlObjForSubquery(trsupport->pParentSqlObj, trsupport, pSql);
      if (pNew == NULL) {
        tscError("%p sub:%p failed to create new subquery sqlobj due to out of memory, abort retry",
                 trsupport->pParentSqlObj, pSql);

        pState->code = TSDB_CODE_CLI_OUT_OF_MEMORY;
        trsupport->numOfRetry = MAX_NUM_OF_SUBQUERY_RETRY;
        return;
      }

      tscProcessSql(pNew);
      return;
    } else {  // reach the maximum retry count, abort
      atomic_val_compare_exchange_32(&pState->code, TSDB_CODE_SUCCESS, numOfRows);
      tscError("%p sub:%p retrieve failed,code:%d,orderOfSub:%d failed.no more retry,set global code:%d", pPObj, pSql,
               numOfRows, subqueryIndex, pState->code);
    }
  }

  int32_t numOfTotal = pState->numOfTotal;

  int32_t finished = atomic_add_fetch_32(&pState->numOfCompleted, 1);
  if (finished < numOfTotal) {
    tscTrace("%p sub:%p orderOfSub:%d freed, finished subqueries:%d", pPObj, pSql, trsupport->subqueryIndex, finished);
    return tscFreeSubSqlObj(trsupport, pSql);
  }

  // all subqueries are failed
  tscError("%p retrieve from %d vnode(s) completed,code:%d.FAILED.", pPObj, pState->numOfTotal, pState->code);
  pPObj->res.code = -(pState->code);

  // release allocated resource
  tscLocalReducerEnvDestroy(trsupport->pExtMemBuffer, trsupport->pOrderDescriptor, trsupport->pFinalColModel,
                            pState->numOfTotal);

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
    SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pPObj->cmd, 0);

    if ((pQueryInfo->type & TSDB_QUERY_TYPE_JOIN_SEC_STAGE) == TSDB_QUERY_TYPE_JOIN_SEC_STAGE) {
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
  int32_t           idx = trsupport->subqueryIndex;
  SSqlObj *         pPObj = trsupport->pParentSqlObj;
  tOrderDescriptor *pDesc = trsupport->pOrderDescriptor;

  SSqlObj *pSql = (SSqlObj *)tres;
  if (pSql == NULL) {  // sql object has been released in error process, return immediately
    tscTrace("%p subquery has been released, idx:%d, abort", pPObj, idx);
    return;
  }

  SSubqueryState* pState = trsupport->pState;
  assert(pState->numOfCompleted < pState->numOfTotal && pState->numOfCompleted >= 0 &&
      pPObj->numOfSubs == pState->numOfTotal);
  
  // query process and cancel query process may execute at the same time
  pthread_mutex_lock(&trsupport->queryMutex);

  if (numOfRows < 0 || pState->code < 0 || pPObj->res.code != TSDB_CODE_SUCCESS) {
    return tscHandleSubRetrievalError(trsupport, pSql, numOfRows);
  }

  SSqlRes *   pRes = &pSql->res;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);

  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfoFromQueryInfo(pQueryInfo, 0);

  SVnodeSidList *vnodeInfo = tscGetVnodeSidList(pMeterMetaInfo->pMetricMeta, idx);
  SVPeerDesc *   pSvd = &vnodeInfo->vpeerDesc[vnodeInfo->index];

  if (numOfRows > 0) {
    assert(pRes->numOfRows == numOfRows);
    int64_t num = atomic_add_fetch_64(&pState->numOfRetrievedRows, numOfRows);

    tscTrace("%p sub:%p retrieve numOfRows:%d totalNumOfRows:%d from ip:%u,vid:%d,orderOfSub:%d", pPObj, pSql,
             pRes->numOfRows, pState->numOfRetrievedRows, pSvd->ip, pSvd->vnode, idx);
    
    if (num > tsMaxNumOfOrderedResults && tscIsProjectionQueryOnSTable(pQueryInfo, 0)) {
      tscError("%p sub:%p num of OrderedRes is too many, max allowed:%" PRId64 " , current:%" PRId64,
          pPObj, pSql, tsMaxNumOfOrderedResults, num);
      tscAbortFurtherRetryRetrieval(trsupport, tres, TSDB_CODE_SORTED_RES_TOO_MANY);
      return;
    }
    

#ifdef _DEBUG_VIEW
    printf("received data from vnode: %d rows\n", pRes->numOfRows);
    SSrcColumnInfo colInfo[256] = {0};

    tscGetSrcColumnInfo(colInfo, pQueryInfo);
    tColModelDisplayEx(pDesc->pColumnModel, pRes->data, pRes->numOfRows, pRes->numOfRows, colInfo);
#endif
    if (tsTotalTmpDirGB != 0 && tsAvailTmpDirGB < tsMinimalTmpDirGB) {
      tscError("%p sub:%p client disk space remain %.3f GB, need at least %.3f GB, stop query", pPObj, pSql,
               tsAvailTmpDirGB, tsMinimalTmpDirGB);
      tscAbortFurtherRetryRetrieval(trsupport, tres, TSDB_CODE_CLI_NO_DISKSPACE);
      return;
    }
    
    int32_t ret = saveToBuffer(trsupport->pExtMemBuffer[idx], pDesc, trsupport->localBuffer, pRes->data,
                               pRes->numOfRows, pQueryInfo->groupbyExpr.orderType);
    if (ret < 0) {
      // set no disk space error info, and abort retry
      tscAbortFurtherRetryRetrieval(trsupport, tres, TSDB_CODE_CLI_NO_DISKSPACE);
    } else {
      pthread_mutex_unlock(&trsupport->queryMutex);
      taos_fetch_rows_a(tres, tscRetrieveFromVnodeCallBack, param);
    }

  } else {  // all data has been retrieved to client
    /* data in from current vnode is stored in cache and disk */
    uint32_t numOfRowsFromVnode = trsupport->pExtMemBuffer[idx]->numOfTotalElems + trsupport->localBuffer->numOfElems;
    tscTrace("%p sub:%p all data retrieved from ip:%u,vid:%d, numOfRows:%d, orderOfSub:%d", pPObj, pSql, pSvd->ip,
             pSvd->vnode, numOfRowsFromVnode, idx);

    tColModelCompact(pDesc->pColumnModel, trsupport->localBuffer, pDesc->pColumnModel->capacity);

#ifdef _DEBUG_VIEW
    printf("%" PRIu64 " rows data flushed to disk:\n", trsupport->localBuffer->numOfElems);
    SSrcColumnInfo colInfo[256] = {0};
    tscGetSrcColumnInfo(colInfo, pQueryInfo);
    tColModelDisplayEx(pDesc->pColumnModel, trsupport->localBuffer->data, trsupport->localBuffer->numOfElems,
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
    int32_t ret = tscFlushTmpBuffer(trsupport->pExtMemBuffer[idx], pDesc, trsupport->localBuffer,
                                    pQueryInfo->groupbyExpr.orderType);
    if (ret != 0) {
      /* set no disk space error info, and abort retry */
      return tscAbortFurtherRetryRetrieval(trsupport, tres, TSDB_CODE_CLI_NO_DISKSPACE);
    }
  
    // keep this value local variable, since the pState variable may be released by other threads, if atomic_add opertion
    // increases the finished value up to pState->numOfTotal value, which means all subqueries are completed.
    // In this case, the comparsion between finished value and released pState->numOfTotal is not safe.
    int32_t numOfTotal = pState->numOfTotal;

    int32_t finished = atomic_add_fetch_32(&pState->numOfCompleted, 1);
    if (finished < numOfTotal) {
      tscTrace("%p sub:%p orderOfSub:%d freed, finished subqueries:%d", pPObj, pSql, trsupport->subqueryIndex, finished);
      return tscFreeSubSqlObj(trsupport, pSql);
    }

    // all sub-queries are returned, start to local merge process
    pDesc->pColumnModel->capacity = trsupport->pExtMemBuffer[idx]->numOfElemsPerPage;

    tscTrace("%p retrieve from %d vnodes completed.final NumOfRows:%d,start to build loser tree", pPObj,
             pState->numOfTotal, pState->numOfRetrievedRows);
    
    SQueryInfo *pPQueryInfo = tscGetQueryInfoDetail(&pPObj->cmd, 0);
    tscClearInterpInfo(pPQueryInfo);

    tscCreateLocalReducer(trsupport->pExtMemBuffer, pState->numOfTotal, pDesc, trsupport->pFinalColModel,
                          &pPObj->cmd, &pPObj->res);
    tscTrace("%p build loser tree completed", pPObj);

    pPObj->res.precision = pSql->res.precision;
    pPObj->res.numOfRows = 0;
    pPObj->res.row = 0;

    // only free once
    tfree(trsupport->pState);
    
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
  SSqlCmd* pCmd = &pSql->cmd;
  
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  if (!tscIsTwoStageMergeMetricQuery(pQueryInfo, 0)) {
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
  const int32_t table_index = 0;
  
  SSqlObj *pNew = createSubqueryObj(pSql, table_index, tscRetrieveDataRes, trsupport, prevSqlObj);
  if (pNew != NULL) {  // the sub query of two-stage super table query
    SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pNew->cmd, 0);
    pQueryInfo->type |= TSDB_QUERY_TYPE_STABLE_SUBQUERY;
    
    assert(pQueryInfo->numOfTables == 1 && pNew->cmd.numOfClause == 1);

    // launch subquery for each vnode, so the subquery index equals to the vnodeIndex.
    SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfoFromQueryInfo(pQueryInfo, table_index);
    pMeterMetaInfo->vnodeIndex = trsupport->subqueryIndex;

    pSql->pSubs[trsupport->subqueryIndex] = pNew;
  }

  return pNew;
}

void tscRetrieveDataRes(void *param, TAOS_RES *tres, int code) {
  SRetrieveSupport *trsupport = (SRetrieveSupport *)param;
  
  SSqlObj*  pParentSql = trsupport->pParentSqlObj;
  SSqlObj*  pSql = (SSqlObj *)tres;
  
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(&pSql->cmd, 0, 0);
  assert(pSql->cmd.numOfClause == 1 && pSql->cmd.pQueryInfo[0]->numOfTables == 1);
  
  int32_t idx = pMeterMetaInfo->vnodeIndex;

  SVnodeSidList *vnodeInfo = NULL;
  SVPeerDesc *   pSvd = NULL;
  if (pMeterMetaInfo->pMetricMeta != NULL) {
    vnodeInfo = tscGetVnodeSidList(pMeterMetaInfo->pMetricMeta, idx);
    pSvd = &vnodeInfo->vpeerDesc[vnodeInfo->index];
  }

  SSubqueryState* pState = trsupport->pState;
  assert(pState->numOfCompleted < pState->numOfTotal && pState->numOfCompleted >= 0 &&
         pParentSql->numOfSubs == pState->numOfTotal);
  
  if (pParentSql->res.code != TSDB_CODE_SUCCESS || pState->code != TSDB_CODE_SUCCESS) {
    // metric query is killed, Note: code must be less than 0
    trsupport->numOfRetry = MAX_NUM_OF_SUBQUERY_RETRY;
    if (pParentSql->res.code != TSDB_CODE_SUCCESS) {
      code = -(int)(pParentSql->res.code);
    } else {
      code = pState->code;
    }
    tscTrace("%p query cancelled or failed, sub:%p, orderOfSub:%d abort, code:%d", pParentSql, pSql,
             trsupport->subqueryIndex, code);
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
      tscTrace("%p sub:%p reach the max retry count,set global code:%d", pParentSql, pSql, code);
      atomic_val_compare_exchange_32(&pState->code, 0, code);
    } else {  // does not reach the maximum retry count, go on
      tscTrace("%p sub:%p failed code:%d, retry:%d", pParentSql, pSql, code, trsupport->numOfRetry);

      SSqlObj *pNew = tscCreateSqlObjForSubquery(pParentSql, trsupport, pSql);
      if (pNew == NULL) {
        tscError("%p sub:%p failed to create new subquery due to out of memory, abort retry, vid:%d, orderOfSub:%d",
                 trsupport->pParentSqlObj, pSql, pSvd != NULL ? pSvd->vnode : -1, trsupport->subqueryIndex);

        pState->code = -TSDB_CODE_CLI_OUT_OF_MEMORY;
        trsupport->numOfRetry = MAX_NUM_OF_SUBQUERY_RETRY;
      } else {
        SQueryInfo *pNewQueryInfo = tscGetQueryInfoDetail(&pNew->cmd, 0);
        assert(pNewQueryInfo->pMeterInfo[0]->pMeterMeta != NULL && pNewQueryInfo->pMeterInfo[0]->pMetricMeta != NULL);
        tscProcessSql(pNew);
        return;
      }
    }
  }

  if (pState->code != TSDB_CODE_SUCCESS) {  // failed, abort
    if (vnodeInfo != NULL) {
      tscTrace("%p sub:%p query failed,ip:%u,vid:%d,orderOfSub:%d,global code:%d", pParentSql, pSql,
               vnodeInfo->vpeerDesc[vnodeInfo->index].ip, vnodeInfo->vpeerDesc[vnodeInfo->index].vnode,
               trsupport->subqueryIndex, pState->code);
    } else {
      tscTrace("%p sub:%p query failed,orderOfSub:%d,global code:%d", pParentSql, pSql,
               trsupport->subqueryIndex, pState->code);
    }

    tscRetrieveFromVnodeCallBack(param, tres, pState->code);
  } else {  // success, proceed to retrieve data from dnode
    if (vnodeInfo != NULL) {
      tscTrace("%p sub:%p query complete,ip:%u,vid:%d,orderOfSub:%d,retrieve data", trsupport->pParentSqlObj, pSql,
             vnodeInfo->vpeerDesc[vnodeInfo->index].ip, vnodeInfo->vpeerDesc[vnodeInfo->index].vnode,
             trsupport->subqueryIndex);
    } else {
      tscTrace("%p sub:%p query complete, orderOfSub:%d,retrieve data", trsupport->pParentSqlObj, pSql,
             trsupport->subqueryIndex);
    }

    taos_fetch_rows_a(tres, tscRetrieveFromVnodeCallBack, param);
  }
}

int tscBuildRetrieveMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  char *pMsg, *pStart;

  pStart = pSql->cmd.payload + tsRpcHeadSize;
  pMsg = pStart;

  *((uint64_t *)pMsg) = pSql->res.qhandle;
  pMsg += sizeof(pSql->res.qhandle);

  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  *((uint16_t *)pMsg) = htons(pQueryInfo->type);
  pMsg += sizeof(pQueryInfo->type);

  pSql->cmd.payloadLen = pMsg - pStart;
  pSql->cmd.msgType = TSDB_MSG_TYPE_RETRIEVE;

  return TSDB_CODE_SUCCESS;
}

void tscUpdateVnodeInSubmitMsg(SSqlObj *pSql, char *buf) {
  SShellSubmitMsg *pShellMsg;
  char *           pMsg;
  SMeterMetaInfo * pMeterMetaInfo = tscGetMeterMetaInfo(&pSql->cmd, pSql->cmd.clauseIndex, 0);

  SMeterMeta *pMeterMeta = pMeterMetaInfo->pMeterMeta;

  pMsg = buf + tsRpcHeadSize;

  pShellMsg = (SShellSubmitMsg *)pMsg;
  pShellMsg->vnode = htons(pMeterMeta->vpeerDesc[pSql->index].vnode);
  tscTrace("%p update submit msg vnode:%s:%d", pSql, taosIpStr(pMeterMeta->vpeerDesc[pSql->index].ip),
           htons(pShellMsg->vnode));
}

int tscBuildSubmitMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SShellSubmitMsg *pShellMsg;
  char *           pMsg, *pStart;

  SQueryInfo *    pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfoFromQueryInfo(pQueryInfo, 0);

  SMeterMeta *pMeterMeta = pMeterMetaInfo->pMeterMeta;

  pStart = pSql->cmd.payload + tsRpcHeadSize;
  pMsg = pStart;

  pShellMsg = (SShellSubmitMsg *)pMsg;

  pShellMsg->import = htons(TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_INSERT) ? 0 : 1);
  pShellMsg->vnode = htons(pMeterMeta->vpeerDesc[pMeterMeta->index].vnode);
  pShellMsg->numOfSid = htonl(pSql->cmd.numOfTablesInSubmit);  // number of meters to be inserted

  // pSql->cmd.payloadLen is set during parse sql routine, so we do not use it here
  pSql->cmd.msgType = TSDB_MSG_TYPE_SUBMIT;
  tscTrace("%p update submit msg vnode:%s:%d", pSql, taosIpStr(pMeterMeta->vpeerDesc[pMeterMeta->index].ip),
           htons(pShellMsg->vnode));
  
  return TSDB_CODE_SUCCESS;
}

void tscUpdateVnodeInQueryMsg(SSqlObj *pSql, char *buf) {
  SSqlCmd *       pCmd = &pSql->cmd;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, pCmd->clauseIndex, 0);

  char *          pStart = buf + tsRpcHeadSize;
  SQueryMeterMsg *pQueryMsg = (SQueryMeterMsg *)pStart;

  if (UTIL_METER_IS_NOMRAL_METER(pMeterMetaInfo)) {  // pColumnModel == NULL, query on meter
    SMeterMeta *pMeterMeta = pMeterMetaInfo->pMeterMeta;
    pQueryMsg->vnode = htons(pMeterMeta->vpeerDesc[pSql->index].vnode);
  } else {  // query on metric
    SMetricMeta *  pMetricMeta = pMeterMetaInfo->pMetricMeta;
    SVnodeSidList *pVnodeSidList = tscGetVnodeSidList(pMetricMeta, pMeterMetaInfo->vnodeIndex);
    pQueryMsg->vnode = htons(pVnodeSidList->vpeerDesc[pSql->index].vnode);
  }
}

/*
 * for meter query, simply return the size <= 1k
 * for metric query, estimate size according to meter tags
 */
static int32_t tscEstimateQueryMsgSize(SSqlCmd *pCmd, int32_t clauseIndex) {
  const static int32_t MIN_QUERY_MSG_PKT_SIZE = TSDB_MAX_BYTES_PER_ROW * 5;
  SQueryInfo *         pQueryInfo = tscGetQueryInfoDetail(pCmd, clauseIndex);

  int32_t srcColListSize = pQueryInfo->colList.numOfCols * sizeof(SColumnInfo);

  int32_t         exprSize = sizeof(SSqlFuncExprMsg) * pQueryInfo->exprsInfo.numOfExprs;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfoFromQueryInfo(pQueryInfo, 0);

  // meter query without tags values
  if (!UTIL_METER_IS_SUPERTABLE(pMeterMetaInfo)) {
    return MIN_QUERY_MSG_PKT_SIZE + minMsgSize() + sizeof(SQueryMeterMsg) + srcColListSize + exprSize;
  }

  SMetricMeta *pMetricMeta = pMeterMetaInfo->pMetricMeta;
  SVnodeSidList *pVnodeSidList = tscGetVnodeSidList(pMetricMeta, pMeterMetaInfo->vnodeIndex);

  int32_t meterInfoSize = (pMetricMeta->tagLen + sizeof(SMeterSidExtInfo)) * pVnodeSidList->numOfSids;
  int32_t outputColumnSize = pQueryInfo->exprsInfo.numOfExprs * sizeof(SSqlFuncExprMsg);

  int32_t size = meterInfoSize + outputColumnSize + srcColListSize + exprSize + MIN_QUERY_MSG_PKT_SIZE;
  if (pQueryInfo->tsBuf != NULL) {
    size += pQueryInfo->tsBuf->fileSize;
  }

  return size;
}

static char *doSerializeTableInfo(SSqlObj *pSql, int32_t numOfMeters, int32_t vnodeId, char *pMsg) {
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(&pSql->cmd, pSql->cmd.clauseIndex, 0);

  SMeterMeta * pMeterMeta = pMeterMetaInfo->pMeterMeta;
  SMetricMeta *pMetricMeta = pMeterMetaInfo->pMetricMeta;

  tscTrace("%p vid:%d, query on %d meters", pSql, vnodeId, numOfMeters);
  if (UTIL_METER_IS_NOMRAL_METER(pMeterMetaInfo)) {
#ifdef _DEBUG_VIEW
    tscTrace("%p sid:%d, uid:%" PRIu64, pSql, pMeterMetaInfo->pMeterMeta->sid, pMeterMetaInfo->pMeterMeta->uid);
#endif
    SMeterSidExtInfo *pMeterInfo = (SMeterSidExtInfo *)pMsg;
    pMeterInfo->sid = htonl(pMeterMeta->sid);
    pMeterInfo->uid = htobe64(pMeterMeta->uid);
    pMeterInfo->key = htobe64(tscGetSubscriptionProgress(pSql->pSubscription, pMeterMeta->uid));
    pMsg += sizeof(SMeterSidExtInfo);
  } else {
    SVnodeSidList *pVnodeSidList = tscGetVnodeSidList(pMetricMeta, pMeterMetaInfo->vnodeIndex);

    for (int32_t i = 0; i < numOfMeters; ++i) {
      SMeterSidExtInfo *pMeterInfo = (SMeterSidExtInfo *)pMsg;
      SMeterSidExtInfo *pQueryMeterInfo = tscGetMeterSidInfo(pVnodeSidList, i);

      pMeterInfo->sid = htonl(pQueryMeterInfo->sid);
      pMeterInfo->uid = htobe64(pQueryMeterInfo->uid);
      pMeterInfo->key = htobe64(tscGetSubscriptionProgress(pSql->pSubscription, pQueryMeterInfo->uid));
      
      pMsg += sizeof(SMeterSidExtInfo);

      memcpy(pMsg, pQueryMeterInfo->tags, pMetricMeta->tagLen);
      pMsg += pMetricMeta->tagLen;

#ifdef _DEBUG_VIEW
      tscTrace("%p sid:%d, uid:%" PRId64, pSql, pQueryMeterInfo->sid, pQueryMeterInfo->uid);
#endif
    }
  }

  return pMsg;
}

int tscBuildQueryMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;

  int32_t size = tscEstimateQueryMsgSize(pCmd, pCmd->clauseIndex);

  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, size)) {
    tscError("%p failed to malloc for query msg", pSql);
    return -1;
  }

  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfoFromQueryInfo(pQueryInfo, 0);
  
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
  } else {  // query on super table
    if (pMeterMetaInfo->vnodeIndex < 0) {
      tscError("%p error vnodeIdx:%d", pSql, pMeterMetaInfo->vnodeIndex);
      return -1;
    }

    SVnodeSidList *pVnodeSidList = tscGetVnodeSidList(pMetricMeta, pMeterMetaInfo->vnodeIndex);
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

  if (pQueryInfo->order.order == TSQL_SO_ASC) {
    pQueryMsg->skey = htobe64(pQueryInfo->stime);
    pQueryMsg->ekey = htobe64(pQueryInfo->etime);
  } else {
    pQueryMsg->skey = htobe64(pQueryInfo->etime);
    pQueryMsg->ekey = htobe64(pQueryInfo->stime);
  }

  pQueryMsg->order = htons(pQueryInfo->order.order);
  pQueryMsg->orderColId = htons(pQueryInfo->order.orderColId);

  pQueryMsg->interpoType = htons(pQueryInfo->interpoType);

  pQueryMsg->limit = htobe64(pQueryInfo->limit.limit);
  pQueryMsg->offset = htobe64(pQueryInfo->limit.offset);

  pQueryMsg->numOfCols = htons(pQueryInfo->colList.numOfCols);

  if (pQueryInfo->colList.numOfCols <= 0) {
    tscError("%p illegal value of numOfCols in query msg: %d", pSql, pMeterMeta->numOfColumns);
    return -1;
  }

  if (pMeterMeta->numOfTags < 0) {
    tscError("%p illegal value of numOfTagsCols in query msg: %d", pSql, pMeterMeta->numOfTags);
    return -1;
  }

  pQueryMsg->intervalTime = htobe64(pQueryInfo->intervalTime);
  pQueryMsg->slidingTimeUnit = pQueryInfo->slidingTimeUnit;
  pQueryMsg->slidingTime = htobe64(pQueryInfo->slidingTime);
  
  if (pQueryInfo->intervalTime < 0) {
    tscError("%p illegal value of aggregation time interval in query msg: %ld", pSql, pQueryInfo->intervalTime);
    return -1;
  }

  if (pQueryInfo->groupbyExpr.numOfGroupCols < 0) {
    tscError("%p illegal value of numOfGroupCols in query msg: %d", pSql, pQueryInfo->groupbyExpr.numOfGroupCols);
    return -1;
  }

  pQueryMsg->numOfGroupCols = htons(pQueryInfo->groupbyExpr.numOfGroupCols);

  if (UTIL_METER_IS_NOMRAL_METER(pMeterMetaInfo)) {  // query on meter
    pQueryMsg->tagLength = 0;
  } else {  // query on metric
    pQueryMsg->tagLength = htons(pMetricMeta->tagLen);
  }

  pQueryMsg->queryType = htons(pQueryInfo->type);
  pQueryMsg->numOfOutputCols = htons(pQueryInfo->exprsInfo.numOfExprs);

  if (pQueryInfo->fieldsInfo.numOfOutputCols < 0) {
    tscError("%p illegal value of number of output columns in query msg: %d", pSql,
             pQueryInfo->fieldsInfo.numOfOutputCols);
    return -1;
  }

  // set column list ids
  char *   pMsg = (char *)(pQueryMsg->colList) + pQueryInfo->colList.numOfCols * sizeof(SColumnInfo);
  SSchema *pSchema = tsGetSchema(pMeterMeta);

  for (int32_t i = 0; i < pQueryInfo->colList.numOfCols; ++i) {
    SColumnBase *pCol = tscColumnBaseInfoGet(&pQueryInfo->colList, i);
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

  for (int32_t i = 0; i < tscSqlExprNumOfExprs(pQueryInfo); ++i) {
    SSqlExpr *pExpr = tscSqlExprGet(pQueryInfo, i);

    if (pExpr->functionId == TSDB_FUNC_ARITHM) {
      hasArithmeticFunction = true;
    }

    if (!tscValidateColumnId(pMeterMetaInfo, pExpr->colInfo.colId)) {
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
    SColumnBase *pColBase = pQueryInfo->colList.pColList;
    for (int32_t i = 0; i < pQueryInfo->colList.numOfCols; ++i) {
      char *  name = pSchema[pColBase[i].colIndex.columnIndex].name;
      int32_t lenx = strlen(name);
      memcpy(pMsg, name, lenx);
      *(pMsg + lenx) = ',';

      len += (lenx + 1);  // one for comma
      pMsg += (lenx + 1);
    }
  }

  pQueryMsg->colNameLen = htonl(len);

  // serialize the table info (sid, uid, tags)
  pMsg = doSerializeTableInfo(pSql, numOfMeters, htons(pQueryMsg->vnode), pMsg);

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

  SSqlGroupbyExpr *pGroupbyExpr = &pQueryInfo->groupbyExpr;
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
      
      memcpy(pMsg, pCol->name, tListLen(pCol->name));
      pMsg += tListLen(pCol->name);
    }
  }

  if (pQueryInfo->interpoType != TSDB_INTERPO_NONE) {
    for (int32_t i = 0; i < pQueryInfo->fieldsInfo.numOfOutputCols; ++i) {
      *((int64_t *)pMsg) = htobe64(pQueryInfo->defaultVal[i]);
      pMsg += sizeof(pQueryInfo->defaultVal[0]);
    }
  }

  // compressed ts block
  pQueryMsg->tsOffset = htonl(pMsg - pStart);
  int32_t tsLen = 0;
  int32_t numOfBlocks = 0;

  if (pQueryInfo->tsBuf != NULL) {
    STSVnodeBlockInfo *pBlockInfo = tsBufGetVnodeBlockInfo(pQueryInfo->tsBuf, pMeterMetaInfo->vnodeIndex);
    assert(QUERY_IS_JOIN_QUERY(pQueryInfo->type) && pBlockInfo != NULL);  // this query should not be sent

    // todo refactor
    fseek(pQueryInfo->tsBuf->f, pBlockInfo->offset, SEEK_SET);
    fread(pMsg, pBlockInfo->compLen, 1, pQueryInfo->tsBuf->f);

    pMsg += pBlockInfo->compLen;
    tsLen = pBlockInfo->compLen;
    numOfBlocks = pBlockInfo->numOfBlocks;
  }

  pQueryMsg->tsLen = htonl(tsLen);
  pQueryMsg->tsNumOfBlocks = htonl(numOfBlocks);
  if (pQueryInfo->tsBuf != NULL) {
    pQueryMsg->tsOrder = htonl(pQueryInfo->tsBuf->tsOrder);
  }

  msgLen = pMsg - pStart;

  tscTrace("%p msg built success,len:%d bytes", pSql, msgLen);
  pCmd->payloadLen = msgLen;
  pSql->cmd.msgType = TSDB_MSG_TYPE_QUERY;

  assert(msgLen + minMsgSize() <= size);

  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildCreateDbMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SCreateDbMsg *pCreateDbMsg;
  char *        pMsg, *pStart;

  SSqlCmd *pCmd = &pSql->cmd;

  pMsg = doBuildMsgHeader(pSql, &pStart);
  pCreateDbMsg = (SCreateDbMsg *)pMsg;

  assert(pCmd->numOfClause == 1);
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, pCmd->clauseIndex, 0);
  
  strncpy(pCreateDbMsg->db, pMeterMetaInfo->name, tListLen(pCreateDbMsg->db));
  pMsg += sizeof(SCreateDbMsg);

  pCmd->payloadLen = pMsg - pStart;
  pCmd->msgType = TSDB_MSG_TYPE_CREATE_DB;

  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildCreateDnodeMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SCreateDnodeMsg *pCreate;

  char *pMsg, *pStart;

  SSqlCmd *pCmd = &pSql->cmd;

  pMsg = doBuildMsgHeader(pSql, &pStart);

  pCreate = (SCreateDnodeMsg *)pMsg;
  strncpy(pCreate->ip, pInfo->pDCLInfo->a[0].z, pInfo->pDCLInfo->a[0].n);

  pMsg += sizeof(SCreateDnodeMsg);

  pCmd->payloadLen = pMsg - pStart;
  pCmd->msgType = TSDB_MSG_TYPE_CREATE_DNODE;

  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildAcctMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SCreateAcctMsg *pAlterMsg;
  char *          pMsg, *pStart;
  int             msgLen = 0;

  SSqlCmd *pCmd = &pSql->cmd;

  pMsg = doBuildMsgHeader(pSql, &pStart);

  pAlterMsg = (SCreateAcctMsg *)pMsg;

  SSQLToken *pName = &pInfo->pDCLInfo->user.user;
  SSQLToken *pPwd = &pInfo->pDCLInfo->user.passwd;

  strncpy(pAlterMsg->user, pName->z, pName->n);
  strncpy(pAlterMsg->pass, pPwd->z, pPwd->n);

  pMsg += sizeof(SCreateAcctMsg);

  SCreateAcctSQL *pAcctOpt = &pInfo->pDCLInfo->acctOpt;

  pAlterMsg->cfg.maxUsers = htonl(pAcctOpt->maxUsers);
  pAlterMsg->cfg.maxDbs = htonl(pAcctOpt->maxDbs);
  pAlterMsg->cfg.maxTimeSeries = htonl(pAcctOpt->maxTimeSeries);
  pAlterMsg->cfg.maxStreams = htonl(pAcctOpt->maxStreams);
  pAlterMsg->cfg.maxPointsPerSecond = htonl(pAcctOpt->maxPointsPerSecond);
  pAlterMsg->cfg.maxStorage = htobe64(pAcctOpt->maxStorage);
  pAlterMsg->cfg.maxQueryTime = htobe64(pAcctOpt->maxQueryTime);
  pAlterMsg->cfg.maxConnections = htonl(pAcctOpt->maxConnections);

  if (pAcctOpt->stat.n == 0) {
    pAlterMsg->cfg.accessState = -1;
  } else {
    if (pAcctOpt->stat.z[0] == 'r' && pAcctOpt->stat.n == 1) {
      pAlterMsg->cfg.accessState = TSDB_VN_READ_ACCCESS;
    } else if (pAcctOpt->stat.z[0] == 'w' && pAcctOpt->stat.n == 1) {
      pAlterMsg->cfg.accessState = TSDB_VN_WRITE_ACCCESS;
    } else if (strncmp(pAcctOpt->stat.z, "all", 3) == 0 && pAcctOpt->stat.n == 3) {
      pAlterMsg->cfg.accessState = TSDB_VN_ALL_ACCCESS;
    } else if (strncmp(pAcctOpt->stat.z, "no", 2) == 0 && pAcctOpt->stat.n == 2) {
      pAlterMsg->cfg.accessState = 0;
    }
  }

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;

  pCmd->msgType = TSDB_MSG_TYPE_CREATE_ACCT;
  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildUserMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SCreateUserMsg *pAlterMsg;
  char *         pMsg, *pStart;

  SSqlCmd *pCmd = &pSql->cmd;

  pMsg = doBuildMsgHeader(pSql, &pStart);
  pAlterMsg = (SCreateUserMsg *)pMsg;

  SUserInfo *pUser = &pInfo->pDCLInfo->user;
  strncpy(pAlterMsg->user, pUser->user.z, pUser->user.n);
  
  pAlterMsg->flag = pUser->type;

  if (pUser->type == TSDB_ALTER_USER_PRIVILEGES) {
    pAlterMsg->privilege = (char)pCmd->count;
  } else if (pUser->type == TSDB_ALTER_USER_PASSWD) {
    strncpy(pAlterMsg->pass, pUser->passwd.z, pUser->passwd.n);
  } else { // create user password info
    strncpy(pAlterMsg->pass, pUser->passwd.z, pUser->passwd.n);
  }

  pMsg += sizeof(SCreateUserMsg);
  pCmd->payloadLen = pMsg - pStart;

  if (pUser->type == TSDB_ALTER_USER_PASSWD || pUser->type == TSDB_ALTER_USER_PRIVILEGES) {
    pCmd->msgType = TSDB_MSG_TYPE_ALTER_USER;
  } else {
    pCmd->msgType = TSDB_MSG_TYPE_CREATE_USER;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildCfgDnodeMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  char *   pStart = NULL;
  SSqlCmd *pCmd = &pSql->cmd;

  char *pMsg = doBuildMsgHeader(pSql, &pStart);
  pMsg += sizeof(SCfgMsg);

  pCmd->payloadLen = pMsg - pStart;
  pCmd->msgType = TSDB_MSG_TYPE_CFG_PNODE;

  return TSDB_CODE_SUCCESS;
}

char *doBuildMsgHeader(SSqlObj *pSql, char **pStart) {
  SSqlCmd *pCmd = &pSql->cmd;
  STscObj *pObj = pSql->pTscObj;

  char *pMsg = pCmd->payload + tsRpcHeadSize;
  *pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  strcpy(pMgmt->db, pObj->db);

  pMsg += sizeof(SMgmtHead);

  return pMsg;
}

int32_t tscBuildDropDbMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SDropDbMsg *pDropDbMsg;
  char *      pMsg, *pStart;

  SSqlCmd *pCmd = &pSql->cmd;

  pMsg = doBuildMsgHeader(pSql, &pStart);
  pDropDbMsg = (SDropDbMsg *)pMsg;

  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, pCmd->clauseIndex, 0);
  strncpy(pDropDbMsg->db, pMeterMetaInfo->name, tListLen(pDropDbMsg->db));
  pDropDbMsg->ignoreNotExists = pInfo->pDCLInfo->existsCheck ? 1 : 0;

  pMsg += sizeof(SDropDbMsg);

  pCmd->payloadLen = pMsg - pStart;
  pCmd->msgType = TSDB_MSG_TYPE_DROP_DB;

  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildDropTableMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SDropTableMsg *pDropTableMsg;
  char *         pMsg, *pStart;
  int            msgLen = 0;

  SSqlCmd *pCmd = &pSql->cmd;

  //pMsg = doBuildMsgHeader(pSql, &pStart);
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, pCmd->clauseIndex, 0);

  pMsg   = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  tscGetDBInfoFromMeterId(pMeterMetaInfo->name, pMgmt->db);
  pMsg += sizeof(SMgmtHead);

  pDropTableMsg = (SDropTableMsg *)pMsg;

  strcpy(pDropTableMsg->meterId, pMeterMetaInfo->name);

  pDropTableMsg->igNotExists = pInfo->pDCLInfo->existsCheck ? 1 : 0;
  pMsg += sizeof(SDropTableMsg);

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_DROP_TABLE;

  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildDropDnodeMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SDropDnodeMsg *pDrop;
  char *         pMsg, *pStart;

  SSqlCmd *       pCmd = &pSql->cmd;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, pCmd->clauseIndex, 0);

  pMsg = doBuildMsgHeader(pSql, &pStart);
  pDrop = (SDropDnodeMsg *)pMsg;

  strcpy(pDrop->ip, pMeterMetaInfo->name);

  pMsg += sizeof(SDropDnodeMsg);

  pCmd->payloadLen = pMsg - pStart;
  pCmd->msgType = TSDB_MSG_TYPE_DROP_DNODE;

  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildDropAcctMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SDropUserMsg *pDropMsg;
  char *        pMsg, *pStart;

  SSqlCmd *pCmd = &pSql->cmd;

  pMsg = doBuildMsgHeader(pSql, &pStart);
  pDropMsg = (SDropUserMsg *)pMsg;

  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, pCmd->clauseIndex, 0);
  strcpy(pDropMsg->user, pMeterMetaInfo->name);

  pMsg += sizeof(SDropUserMsg);

  pCmd->payloadLen = pMsg - pStart;

  if (pInfo->type == TSDB_SQL_DROP_ACCT) {
    pCmd->msgType = TSDB_MSG_TYPE_DROP_ACCT;
  } else {
    pCmd->msgType = TSDB_MSG_TYPE_DROP_USER;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildUseDbMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SUseDbMsg *pUseDbMsg;
  char *     pMsg, *pStart;

  SSqlCmd *pCmd = &pSql->cmd;

  pMsg = doBuildMsgHeader(pSql, &pStart);
  pUseDbMsg = (SUseDbMsg *)pMsg;

  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, pCmd->clauseIndex, 0);
  strcpy(pUseDbMsg->db, pMeterMetaInfo->name);

  pMsg += sizeof(SUseDbMsg);

  pCmd->payloadLen = pMsg - pStart;
  pCmd->msgType = TSDB_MSG_TYPE_USE_DB;

  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildShowMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SShowMsg *pShowMsg;
  char *    pMsg, *pStart;
  int       msgLen = 0;

  SSqlCmd *pCmd = &pSql->cmd;
  STscObj *pObj = pSql->pTscObj;

  int32_t size = minMsgSize() + sizeof(SMgmtHead) + sizeof(SShowTableMsg) + pCmd->payloadLen + TSDB_EXTRA_PAYLOAD_SIZE;
  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, size)) {
    tscError("%p failed to malloc for show msg", pSql);
    return -1;
  }

  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;

  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, pCmd->clauseIndex, 0);
  size_t          nameLen = strlen(pMeterMetaInfo->name);

  if (nameLen > 0) {
    strcpy(pMgmt->db, pMeterMetaInfo->name);  // prefix is set here
  } else {
    strcpy(pMgmt->db, pObj->db);
  }

  pMsg += sizeof(SMgmtHead);

  pShowMsg = (SShowMsg *)pMsg;
  SShowInfo *pShowInfo = &pInfo->pDCLInfo->showOpt;

  pShowMsg->type = pShowInfo->showType;

  if (pShowInfo->showType != TSDB_MGMT_TABLE_VNODES) {
    SSQLToken *pPattern = &pShowInfo->pattern;
    if (pPattern->type > 0) {  // only show tables support wildcard query
      strncpy(pShowMsg->payload, pPattern->z, pPattern->n);
      pShowMsg->payloadLen = htons(pPattern->n);
    }
    pMsg += (sizeof(SShowTableMsg) + pPattern->n);
  } else {
    SSQLToken *pIpAddr = &pShowInfo->prefix;
    assert(pIpAddr->n > 0 && pIpAddr->type > 0);

    strncpy(pShowMsg->payload, pIpAddr->z, pIpAddr->n);
    pShowMsg->payloadLen = htons(pIpAddr->n);

    pMsg += (sizeof(SShowTableMsg) + pIpAddr->n);
  }

  pCmd->payloadLen = pMsg - pStart;
  pCmd->msgType = TSDB_MSG_TYPE_SHOW;

  assert(msgLen + minMsgSize() <= size);

  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildKillMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SKillQuery *pKill;
  char *      pMsg, *pStart;

  SSqlCmd *pCmd = &pSql->cmd;

  pMsg = doBuildMsgHeader(pSql, &pStart);
  pKill = (SKillQuery *)pMsg;

  pKill->handle = 0;
  strncpy(pKill->queryId, pInfo->pDCLInfo->ip.z, pInfo->pDCLInfo->ip.n);

  pMsg += sizeof(SKillQuery);

  pCmd->payloadLen = pMsg - pStart;

  switch (pCmd->command) {
    case TSDB_SQL_KILL_QUERY:
      pCmd->msgType = TSDB_MSG_TYPE_KILL_QUERY;
      break;
    case TSDB_SQL_KILL_CONNECTION:
      pCmd->msgType = TSDB_MSG_TYPE_KILL_CONNECTION;
      break;
    case TSDB_SQL_KILL_STREAM:
      pCmd->msgType = TSDB_MSG_TYPE_KILL_STREAM;
      break;
  }
  return TSDB_CODE_SUCCESS;
}

int tscEstimateCreateTableMsgLength(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &(pSql->cmd);

  int32_t size = minMsgSize() + sizeof(SMgmtHead) + sizeof(SCreateTableMsg);

  SCreateTableSQL *pCreateTableInfo = pInfo->pCreateTableInfo;
  if (pCreateTableInfo->type == TSQL_CREATE_TABLE_FROM_STABLE) {
    size += sizeof(STagData);
  } else {
    size += sizeof(SSchema) * (pCmd->numOfCols + pCmd->count);
  }

  if (pCreateTableInfo->pSelect != NULL) {
    size += (pCreateTableInfo->pSelect->selectToken.n + 1);
  }

  return size + TSDB_EXTRA_PAYLOAD_SIZE;
}

int tscBuildCreateTableMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SCreateTableMsg *pCreateTableMsg;
  char *           pMsg, *pStart;
  int              msgLen = 0;
  SSchema *        pSchema;
  int              size = 0;

  SSqlCmd *pCmd = &pSql->cmd;

  SQueryInfo *    pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfoFromQueryInfo(pQueryInfo, 0);

  // Reallocate the payload size
  size = tscEstimateCreateTableMsgLength(pSql, pInfo);
  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, size)) {
    tscError("%p failed to malloc for create table msg", pSql);
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }

  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;

  // use dbinfo from table id without modifying current db info
  tscGetDBInfoFromMeterId(pMeterMetaInfo->name, pMgmt->db);

  pMsg += sizeof(SMgmtHead);

  pCreateTableMsg = (SCreateTableMsg *)pMsg;
  strcpy(pCreateTableMsg->meterId, pMeterMetaInfo->name);

  SCreateTableSQL *pCreateTable = pInfo->pCreateTableInfo;

  pCreateTableMsg->igExists = pCreateTable->existCheck ? 1 : 0;

  pCreateTableMsg->numOfColumns = htons(pCmd->numOfCols);
  pCreateTableMsg->numOfTags = htons(pCmd->count);

  pCreateTableMsg->sqlLen = 0;
  pMsg = (char *)pCreateTableMsg->schema;

  int8_t type = pInfo->pCreateTableInfo->type;
  if (type == TSQL_CREATE_TABLE_FROM_STABLE) {  // create by using super table, tags value
    memcpy(pMsg, &pInfo->pCreateTableInfo->usingInfo.tagdata, sizeof(STagData));
    pMsg += sizeof(STagData);
  } else {  // create (super) table
    pSchema = pCreateTableMsg->schema;

    for (int i = 0; i < pCmd->numOfCols + pCmd->count; ++i) {
      TAOS_FIELD *pField = tscFieldInfoGetField(pQueryInfo, i);

      pSchema->type = pField->type;
      strcpy(pSchema->name, pField->name);
      pSchema->bytes = htons(pField->bytes);

      pSchema++;
    }

    pMsg = (char *)pSchema;
    if (type == TSQL_CREATE_STREAM) {  // check if it is a stream sql
      SQuerySQL *pQuerySql = pInfo->pCreateTableInfo->pSelect;

      strncpy(pMsg, pQuerySql->selectToken.z, pQuerySql->selectToken.n + 1);
      pCreateTableMsg->sqlLen = htons(pQuerySql->selectToken.n + 1);
      pMsg += pQuerySql->selectToken.n + 1;
    }
  }

  tscClearFieldInfo(&pQueryInfo->fieldsInfo);

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_CREATE_TABLE;

  assert(msgLen + minMsgSize() <= size);
  return TSDB_CODE_SUCCESS;
}

int tscEstimateAlterTableMsgLength(SSqlCmd *pCmd) {
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);
  return minMsgSize() + sizeof(SMgmtHead) + sizeof(SAlterTableMsg) + sizeof(SSchema) * tscNumOfFields(pQueryInfo) +
         TSDB_EXTRA_PAYLOAD_SIZE;
}

int tscBuildAlterTableMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SAlterTableMsg *pAlterTableMsg;
  char *          pMsg, *pStart;
  int             msgLen = 0;
  int             size = 0;

  SSqlCmd *   pCmd = &pSql->cmd;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);

  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfoFromQueryInfo(pQueryInfo, 0);

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

  SAlterTableSQL *pAlterInfo = pInfo->pAlterInfo;

  pAlterTableMsg = (SAlterTableMsg *)pMsg;
  strcpy(pAlterTableMsg->meterId, pMeterMetaInfo->name);
  pAlterTableMsg->type = htons(pAlterInfo->type);

  pAlterTableMsg->numOfCols = htons(tscNumOfFields(pQueryInfo));
  memcpy(pAlterTableMsg->tagVal, pAlterInfo->tagData.data, TSDB_MAX_TAGS_LEN);

  SSchema *pSchema = pAlterTableMsg->schema;
  for (int i = 0; i < tscNumOfFields(pQueryInfo); ++i) {
    TAOS_FIELD *pField = tscFieldInfoGetField(pQueryInfo, i);

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

  return TSDB_CODE_SUCCESS;
}

int tscAlterDbMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SAlterDbMsg *pAlterDbMsg;
  char *       pMsg, *pStart;
  int          msgLen = 0;

  SSqlCmd *       pCmd = &pSql->cmd;
  STscObj *       pObj = pSql->pTscObj;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, pCmd->clauseIndex, 0);

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

  return TSDB_CODE_SUCCESS;
}

int tscBuildRetrieveFromMgmtMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  char *pMsg, *pStart;
  int   msgLen = 0;

  SSqlCmd *pCmd = &pSql->cmd;
  STscObj *pObj = pSql->pTscObj;
  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;

  SQueryInfo *    pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfoFromQueryInfo(pQueryInfo, 0);
  size_t          nameLen = strlen(pMeterMetaInfo->name);

  if (nameLen > 0) {
    strcpy(pMgmt->db, pMeterMetaInfo->name);
  } else {
    strcpy(pMgmt->db, pObj->db);
  }

  pMsg += sizeof(SMgmtHead);

  *((uint64_t *)pMsg) = pSql->res.qhandle;
  pMsg += sizeof(pSql->res.qhandle);

  *((uint16_t *)pMsg) = htons(pQueryInfo->type);
  pMsg += sizeof(pQueryInfo->type);

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_RETRIEVE;

  return TSDB_CODE_SUCCESS;
}

static int tscSetResultPointer(SQueryInfo *pQueryInfo, SSqlRes *pRes) {
  if (tscCreateResPointerInfo(pRes, pQueryInfo) != TSDB_CODE_SUCCESS) {
    return pRes->code;
  }

  for (int i = 0; i < pQueryInfo->fieldsInfo.numOfOutputCols; ++i) {
    int16_t offset = tscFieldInfoGetOffset(pQueryInfo, i);
    pRes->tsrow[i] = (pRes->data + offset * pRes->numOfRows);
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

  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);

  pRes->code = TSDB_CODE_SUCCESS;

  if (pRes->rspType == 0) {
    pRes->numOfRows = numOfRes;
    pRes->row = 0;
    pRes->rspType = 1;

    tscSetResultPointer(pQueryInfo, pRes);
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
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, pCmd->clauseIndex, 0);

  int32_t numOfRes = pMeterMetaInfo->pMeterMeta->numOfColumns + pMeterMetaInfo->pMeterMeta->numOfTags;

  return tscLocalResultCommonBuilder(pSql, numOfRes);
}

int tscProcessTagRetrieveRsp(SSqlObj *pSql) {
  SSqlCmd *pCmd = &pSql->cmd;

  SQueryInfo *    pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfoFromQueryInfo(pQueryInfo, 0);

  int32_t numOfRes = 0;
  if (tscSqlExprGet(pQueryInfo, 0)->functionId == TSDB_FUNC_TAGPRJ) {
    numOfRes = pMeterMetaInfo->pMetricMeta->numOfMeters;
  } else {
    numOfRes = 1;  // for count function, there is only one output.
  }
  return tscLocalResultCommonBuilder(pSql, numOfRes);
}

int tscProcessRetrieveMetricRsp(SSqlObj *pSql) {
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

  pRes->code = tscDoLocalreduce(pSql);
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);

  if (pRes->code == TSDB_CODE_SUCCESS && pRes->numOfRows > 0) {
    tscSetResultPointer(pQueryInfo, pRes);
  }

  pRes->row = 0;

  uint8_t code = pRes->code;
  if (pSql->fp) {  // async retrieve metric data
    if (pRes->code == TSDB_CODE_SUCCESS) {
      (*pSql->fp)(pSql->param, pSql, pRes->numOfRows);
    } else {
      tscQueueAsyncRes(pSql);
    }
  }

  return code;
}

int tscProcessEmptyResultRsp(SSqlObj *pSql) { return tscLocalResultCommonBuilder(pSql, 0); }

int tscBuildConnectMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SConnectMsg *pConnect;
  char *       pMsg, *pStart;

  SSqlCmd *pCmd = &pSql->cmd;
  STscObj *pObj = pSql->pTscObj;
  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  pConnect = (SConnectMsg *)pMsg;

  char *db;  // ugly code to move the space
  db = strstr(pObj->db, TS_PATH_DELIMITER);
  db = (db == NULL) ? pObj->db : db + 1;
  strcpy(pConnect->db, db);

  strcpy(pConnect->clientVersion, version);

  pMsg += sizeof(SConnectMsg);

  pCmd->payloadLen = pMsg - pStart;
  pCmd->msgType = TSDB_MSG_TYPE_CONNECT;

  return TSDB_CODE_SUCCESS;
}

int tscBuildMeterMetaMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SMeterInfoMsg *pInfoMsg;
  char *         pMsg, *pStart;
  int            msgLen = 0;

  char *tmpData = 0;
  if (pSql->cmd.allocSize > 0) {
    tmpData = calloc(1, pSql->cmd.allocSize);
    if (NULL == tmpData) {
      return TSDB_CODE_CLI_OUT_OF_MEMORY;
    }

    // STagData is in binary format, strncpy is not available
    memcpy(tmpData, pSql->cmd.payload, pSql->cmd.allocSize);
  }

  SSqlCmd *   pCmd = &pSql->cmd;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);

  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfoFromQueryInfo(pQueryInfo, 0);

  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  tscGetDBInfoFromMeterId(pMeterMetaInfo->name, pMgmt->db);

  pMsg += sizeof(SMgmtHead);

  pInfoMsg = (SMeterInfoMsg *)pMsg;
  strcpy(pInfoMsg->meterId, pMeterMetaInfo->name);
  pInfoMsg->createFlag = htons(pSql->cmd.createOnDemand ? 1 : 0);
  pMsg += sizeof(SMeterInfoMsg);

  if (pSql->cmd.createOnDemand) {
    memcpy(pInfoMsg->tags, tmpData, sizeof(STagData));
    pMsg += sizeof(STagData);
  }

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_METERINFO;

  tfree(tmpData);

  assert(msgLen + minMsgSize() <= pCmd->allocSize);
  return TSDB_CODE_SUCCESS;
}

/**
 *  multi meter meta req pkg format:
 *  | SMgmtHead | SMultiMeterInfoMsg | meterId0 | meterId1 | meterId2 | ......
 *      no used         4B
 **/
int tscBuildMultiMeterMetaMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
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

  return TSDB_CODE_SUCCESS;
}

static int32_t tscEstimateMetricMetaMsgSize(SSqlCmd *pCmd) {
  const int32_t defaultSize =
      minMsgSize() + sizeof(SMetricMetaMsg) + sizeof(SMgmtHead) + sizeof(int16_t) * TSDB_MAX_TAGS;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);

  int32_t n = 0;
  for (int32_t i = 0; i < pQueryInfo->tagCond.numOfTagCond; ++i) {
    n += strlen(pQueryInfo->tagCond.cond[i].cond);
  }

  int32_t tagLen = n * TSDB_NCHAR_SIZE;
  if (pQueryInfo->tagCond.tbnameCond.cond != NULL) {
    tagLen += strlen(pQueryInfo->tagCond.tbnameCond.cond) * TSDB_NCHAR_SIZE;
  }

  int32_t joinCondLen = (TSDB_METER_ID_LEN + sizeof(int16_t)) * 2;
  int32_t elemSize = sizeof(SMetricMetaElemMsg) * pQueryInfo->numOfTables;
  
  int32_t colSize = pQueryInfo->groupbyExpr.numOfGroupCols*sizeof(SColIndexEx);

  int32_t len = tagLen + joinCondLen + elemSize + colSize + defaultSize;

  return MAX(len, TSDB_DEFAULT_PAYLOAD_SIZE);
}

int tscBuildMetricMetaMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SMetricMetaMsg *pMetaMsg;
  char *          pMsg, *pStart;
  int             msgLen = 0;
  int             tableIndex = 0;

  SSqlCmd *   pCmd = &pSql->cmd;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);

  STagCond *pTagCond = &pQueryInfo->tagCond;

  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfoFromQueryInfo(pQueryInfo, tableIndex);

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
  pMetaMsg->numOfMeters = htonl(pQueryInfo->numOfTables);

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

  for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, pCmd->clauseIndex, i);
    uint64_t uid = pMeterMetaInfo->pMeterMeta->uid;

    offset = pMsg - (char *)pMetaMsg;
    pMetaMsg->metaElem[i] = htonl(offset);

    SMetricMetaElemMsg *pElem = (SMetricMetaElemMsg *)pMsg;
    pMsg += sizeof(SMetricMetaElemMsg);

    // convert to unicode before sending to mnode for metric query
    int32_t condLen = 0;
    if (pTagCond->numOfTagCond > 0) {
      SCond *pCond = tsGetMetricQueryCondPos(pTagCond, uid);
      if (pCond != NULL && pCond->cond != NULL) {
        condLen = strlen(pCond->cond) + 1;

        bool ret = taosMbsToUcs4(pCond->cond, condLen, pMsg, condLen * TSDB_NCHAR_SIZE);
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
      
      uint32_t len = 0;
      if (pTagCond->tbnameCond.cond != NULL) {
        len = strlen(pTagCond->tbnameCond.cond);
        memcpy(pMsg, pTagCond->tbnameCond.cond, len);
      }
      
      pElem->tableCondLen = htonl(len);
      pMsg += len;
    }

    SSqlGroupbyExpr *pGroupby = &pQueryInfo->groupbyExpr;

    if (pGroupby->tableIndex != i && pGroupby->numOfGroupCols > 0) {
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
        for (int32_t j = 0; j < pQueryInfo->groupbyExpr.numOfGroupCols; ++j) {
          SColIndexEx *pCol = &pQueryInfo->groupbyExpr.columnInfo[j];
          SColIndexEx *pDestCol = (SColIndexEx *)pMsg;

          pDestCol->colIdxInBuf = 0;
          pDestCol->colIdx = htons(pCol->colIdx);
          pDestCol->colId = htons(pDestCol->colId);
          pDestCol->flag = htons(pDestCol->flag);
          strncpy(pDestCol->name, pCol->name, tListLen(pCol->name));

          pMsg += sizeof(SColIndexEx);
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
  
  return TSDB_CODE_SUCCESS;
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

int tscBuildHeartBeatMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
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
  return TSDB_CODE_SUCCESS;
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
    tscError("invalid numOfTags:%d", pMeta->numOfTags);
    return TSDB_CODE_INVALID_VALUE;
  }

  if (pMeta->numOfColumns > TSDB_MAX_COLUMNS || pMeta->numOfColumns <= 0) {
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
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(&pSql->cmd, 0, 0);
  assert(pMeterMetaInfo->pMeterMeta == NULL);

  pMeterMetaInfo->pMeterMeta = (SMeterMeta *)taosAddDataIntoCache(tscCacheHandle, pMeterMetaInfo->name, (char *)pMeta,
                                                                  size, tsMeterMetaKeepTimer);
  // todo handle out of memory case
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

    char *pBuf = calloc(1, size);
    if (pBuf == NULL) {
      pSql->res.code = TSDB_CODE_CLI_OUT_OF_MEMORY;
      goto _error_clean;
    }

    SMetricMeta *pNewMetricMeta = (SMetricMeta *)pBuf;
    metricMetaList[k] = pNewMetricMeta;

    pNewMetricMeta->numOfMeters = pMeta->numOfMeters;
    pNewMetricMeta->numOfVnodes = pMeta->numOfVnodes;
    pNewMetricMeta->tagLen = pMeta->tagLen;

    pBuf = pBuf + sizeof(SMetricMeta) + pNewMetricMeta->numOfVnodes * sizeof(SVnodeSidList *);

    for (int32_t i = 0; i < pMeta->numOfVnodes; ++i) {
      SVnodeSidList *pSidLists = (SVnodeSidList *)rsp;
      memcpy(pBuf, pSidLists, sizeof(SVnodeSidList));

      pNewMetricMeta->list[i] = pBuf - (char *)pNewMetricMeta;  // offset value
      SVnodeSidList *pLists = (SVnodeSidList *)pBuf;

      tscTrace("%p metricmeta:vid:%d,numOfMeters:%d", pSql, i, pLists->numOfSids);

      pBuf += sizeof(SVnodeSidList) + sizeof(SMeterSidExtInfo *) * pSidLists->numOfSids;
      rsp += sizeof(SVnodeSidList);

      size_t elemSize = sizeof(SMeterSidExtInfo) + pNewMetricMeta->tagLen;
      for (int32_t j = 0; j < pSidLists->numOfSids; ++j) {
        pLists->pSidExtInfoList[j] = pBuf - (char *)pLists;
        memcpy(pBuf, rsp, elemSize);

        ((SMeterSidExtInfo *)pBuf)->uid = htobe64(((SMeterSidExtInfo *)pBuf)->uid);
        ((SMeterSidExtInfo *)pBuf)->sid = htonl(((SMeterSidExtInfo *)pBuf)->sid);

        rsp += elemSize;
        pBuf += elemSize;
      }
    }

    sizes[k] = pBuf - (char *)pNewMetricMeta;
  }

  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  for (int32_t i = 0; i < num; ++i) {
    char name[TSDB_MAX_TAGS_LEN + 1] = {0};

    SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfoFromQueryInfo(pQueryInfo, i);
    tscGetMetricMetaCacheKey(pQueryInfo, name, pMeterMetaInfo->pMeterMeta->uid);

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

  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);  //?

  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfoFromQueryInfo(pQueryInfo, 0);

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

  key[0] = pCmd->msgType + 'a';
  strcpy(key + 1, "showlist");

  taosRemoveDataFromCache(tscCacheHandle, (void *)&(pMeterMetaInfo->pMeterMeta), false);

  int32_t size = pMeta->numOfColumns * sizeof(SSchema) + sizeof(SMeterMeta);
  pMeterMetaInfo->pMeterMeta =
      (SMeterMeta *)taosAddDataIntoCache(tscCacheHandle, key, (char *)pMeta, size, tsMeterMetaKeepTimer);
  
  pCmd->numOfCols = pQueryInfo->fieldsInfo.numOfOutputCols;
  SSchema *pMeterSchema = tsGetSchema(pMeterMetaInfo->pMeterMeta);

  tscColumnBaseInfoReserve(&pQueryInfo->colList, pMeta->numOfColumns);
  SColumnIndex index = {0};

  for (int16_t i = 0; i < pMeta->numOfColumns; ++i) {
    index.columnIndex = i;
    tscColumnBaseInfoInsert(pQueryInfo, &index);
    tscFieldInfoSetValFromSchema(&pQueryInfo->fieldsInfo, i, &pMeterSchema[i]);
    
    pQueryInfo->fieldsInfo.pSqlExpr[i] = tscSqlExprInsert(pQueryInfo, i, TSDB_FUNC_TS_DUMMY, &index,
                     pMeterSchema[i].type, pMeterSchema[i].bytes, pMeterSchema[i].bytes);
  }

  tscFieldInfoCalOffset(pQueryInfo);
  return 0;
}

int tscProcessConnectRsp(SSqlObj *pSql) {
  char         temp[TSDB_METER_ID_LEN * 2];
  SConnectRsp *pConnect;

  STscObj *pObj = pSql->pTscObj;
  SSqlRes *pRes = &pSql->res;

  pConnect = (SConnectRsp *)pRes->pRsp;
  strcpy(pObj->acctId, pConnect->acctId);  // copy acctId from response
  int32_t len = sprintf(temp, "%s%s%s", pObj->acctId, TS_PATH_DELIMITER, pObj->db);

  assert(len <= tListLen(pObj->db));
  strncpy(pObj->db, temp, tListLen(pObj->db));
  
  SIpList *    pIpList;
  char *rsp = pRes->pRsp + sizeof(SConnectRsp);
  pIpList = (SIpList *)rsp;
  tscSetMgmtIpList(pIpList);

  strcpy(pObj->sversion, pConnect->version);
  pObj->writeAuth = pConnect->writeAuth;
  pObj->superAuth = pConnect->superAuth;
  taosTmrReset(tscProcessActivityTimer, tsShellActivityTimer * 500, pObj, tscTmr, &pObj->pTimer);

  return 0;
}

int tscProcessUseDbRsp(SSqlObj *pSql) {
  STscObj *       pObj = pSql->pTscObj;
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(&pSql->cmd, 0, 0);

  strcpy(pObj->db, pMeterMetaInfo->name);
  return 0;
}

int tscProcessDropDbRsp(SSqlObj *UNUSED_PARAM(pSql)) {
  taosClearDataCache(tscCacheHandle);
  return 0;
}

int tscProcessDropTableRsp(SSqlObj *pSql) {
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(&pSql->cmd, 0, 0);

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
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(&pSql->cmd, 0, 0);

  SMeterMeta *pMeterMeta = taosGetDataFromCache(tscCacheHandle, pMeterMetaInfo->name);
  if (pMeterMeta == NULL) { /* not in cache, abort */
    return 0;
  }

  tscTrace("%p force release metermeta in cache after alter-table: %s", pSql, pMeterMetaInfo->name);
  taosRemoveDataFromCache(tscCacheHandle, (void **)&pMeterMeta, true);

  if (pMeterMetaInfo->pMeterMeta) {
    bool isSuperTable = UTIL_METER_IS_SUPERTABLE(pMeterMetaInfo);

    taosRemoveDataFromCache(tscCacheHandle, (void **)&(pMeterMetaInfo->pMeterMeta), true);
    taosRemoveDataFromCache(tscCacheHandle, (void **)&(pMeterMetaInfo->pMetricMeta), true);

    if (isSuperTable) {  // if it is a super table, reset whole query cache
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
  
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  tscSetResultPointer(pQueryInfo, pRes);

  if (pSql->pSubscription != NULL) {
    int32_t numOfCols = pQueryInfo->fieldsInfo.numOfOutputCols;
    
    TAOS_FIELD *pField = tscFieldInfoGetField(pQueryInfo, numOfCols - 1);
    int16_t     offset = tscFieldInfoGetOffset(pQueryInfo, numOfCols - 1);
    
    char* p = pRes->data + (pField->bytes + offset) * pRes->numOfRows;

    int32_t numOfMeters = htonl(*(int32_t*)p);
    p += sizeof(int32_t);
    for (int i = 0; i < numOfMeters; i++) {
      int64_t uid = htobe64(*(int64_t*)p);
      p += sizeof(int64_t);
      TSKEY key = htobe64(*(TSKEY*)p);
      p += sizeof(TSKEY);
      tscUpdateSubscriptionProgress(pSql->pSubscription, uid, key);
    }
  }

  pRes->row = 0;

  /**
   * If the query result is exhausted, or current query is to free resource at server side,
   * the connection will be recycled.
   */
  if ((pRes->numOfRows == 0 && !(tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0) && pRes->offset > 0)) ||
      ((pQueryInfo->type & TSDB_QUERY_TYPE_FREE_RESOURCE) == TSDB_QUERY_TYPE_FREE_RESOURCE)) {
    tscTrace("%p no result or free resource, recycle connection", pSql);
    taosAddConnIntoCache(tscConnCache, pSql->thandle, pSql->ip, pSql->vnode, pObj->user);
    pSql->thandle = NULL;
  } else {
    tscTrace("%p numOfRows:%d, offset:%d, not recycle connection", pSql, pRes->numOfRows, pRes->offset);
  }

  return 0;
}

int tscProcessRetrieveRspFromLocal(SSqlObj *pSql) {
  SSqlRes *   pRes = &pSql->res;
  SSqlCmd *   pCmd = &pSql->cmd;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);

  SRetrieveMeterRsp *pRetrieve = (SRetrieveMeterRsp *)pRes->pRsp;

  pRes->numOfRows = htonl(pRetrieve->numOfRows);
  pRes->data = pRetrieve->data;

  tscSetResultPointer(pQueryInfo, pRes);
  pRes->row = 0;
  return 0;
}

void tscMeterMetaCallBack(void *param, TAOS_RES *res, int code);

static int32_t doGetMeterMetaFromServer(SSqlObj *pSql, SMeterMetaInfo *pMeterMetaInfo) {
  int32_t code = TSDB_CODE_SUCCESS;

  SSqlObj *pNew = calloc(1, sizeof(SSqlObj));
  if (NULL == pNew) {
    tscError("%p malloc failed for new sqlobj to get meter meta", pSql);
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }

  pNew->pTscObj = pSql->pTscObj;
  pNew->signature = pNew;
  pNew->cmd.command = TSDB_SQL_META;

  tscAddSubqueryInfo(&pNew->cmd);

  SQueryInfo *pNewQueryInfo = NULL;
  tscGetQueryInfoDetailSafely(&pNew->cmd, 0, &pNewQueryInfo);

  pNew->cmd.createOnDemand = pSql->cmd.createOnDemand;  // create table if not exists
  if (TSDB_CODE_SUCCESS != tscAllocPayload(&pNew->cmd, TSDB_DEFAULT_PAYLOAD_SIZE)) {
    tscError("%p malloc failed for payload to get meter meta", pSql);
    free(pNew);

    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }

  SMeterMetaInfo *pNewMeterMetaInfo = tscAddEmptyMeterMetaInfo(pNewQueryInfo);
  assert(pNew->cmd.numOfClause == 1 && pNewQueryInfo->numOfTables == 1);

  strcpy(pNewMeterMetaInfo->name, pMeterMetaInfo->name);
  memcpy(pNew->cmd.payload, pSql->cmd.payload, TSDB_DEFAULT_PAYLOAD_SIZE);  // tag information if table does not exists.
  tscTrace("%p new pSqlObj:%p to get meterMeta", pSql, pNew);

  if (pSql->fp == NULL) {
    tsem_init(&pNew->rspSem, 0, 0);
    tsem_init(&pNew->emptyRspSem, 0, 1);

    code = tscProcessSql(pNew);

    /*
     * Update cache only on succeeding in getting metermeta.
     * Transfer the ownership of metermeta to the new object, instead of invoking the release/acquire routine
     */
    if (code == TSDB_CODE_SUCCESS) {
      pMeterMetaInfo->pMeterMeta = taosTransferDataInCache(tscCacheHandle, (void**) &pNewMeterMetaInfo->pMeterMeta);
      assert(pMeterMetaInfo->pMeterMeta != NULL);
    }

    tscTrace("%p get meter meta complete, code:%d, pMeterMeta:%p", pSql, code, pMeterMetaInfo->pMeterMeta);
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

int tscGetMeterMeta(SSqlObj *pSql, SMeterMetaInfo *pMeterMetaInfo) {
  assert(strlen(pMeterMetaInfo->name) != 0);

  // If this SMeterMetaInfo owns a metermeta, release it first
  if (pMeterMetaInfo->pMeterMeta != NULL) {
    taosRemoveDataFromCache(tscCacheHandle, (void **)&(pMeterMetaInfo->pMeterMeta), false);
  }
  
  pMeterMetaInfo->pMeterMeta = (SMeterMeta *)taosGetDataFromCache(tscCacheHandle, pMeterMetaInfo->name);
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
  return doGetMeterMetaFromServer(pSql, pMeterMetaInfo);
}

int tscGetMeterMetaEx(SSqlObj *pSql, SMeterMetaInfo *pMeterMetaInfo, bool createIfNotExists) {
  pSql->cmd.createOnDemand = createIfNotExists;
  return tscGetMeterMeta(pSql, pMeterMetaInfo);
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
  int code = 0;

  // handle metric meta renew process
  SSqlCmd *pCmd = &pSql->cmd;

  SQueryInfo *    pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);
  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfoFromQueryInfo(pQueryInfo, 0);

  // enforce the renew metermeta operation in async model
  if (pSql->fp == NULL) pSql->fp = (void *)0x1;

  /*
   * 1. only update the metermeta in force model metricmeta is not updated
   * 2. if get metermeta failed, still get the metermeta
   */
  if (pMeterMetaInfo->pMeterMeta == NULL || !tscQueryOnMetric(pCmd)) {
    if (pMeterMetaInfo->pMeterMeta) {
      tscTrace("%p update meter meta, old: numOfTags:%d, numOfCols:%d, uid:%" PRId64 ", addr:%p", pSql,
               pMeterMetaInfo->numOfTags, pCmd->numOfCols, pMeterMetaInfo->pMeterMeta->uid, pMeterMetaInfo->pMeterMeta);
    }

    tscWaitingForCreateTable(pCmd);
    taosRemoveDataFromCache(tscCacheHandle, (void **)&(pMeterMetaInfo->pMeterMeta), true);

    code = doGetMeterMetaFromServer(pSql, pMeterMetaInfo);  // todo ??
  } else {
    tscTrace("%p metric query not update metric meta, numOfTags:%d, numOfCols:%d, uid:%" PRId64 ", addr:%p", pSql,
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

int tscGetMetricMeta(SSqlObj *pSql, int32_t clauseIndex) {
  int      code = TSDB_CODE_NETWORK_UNAVAIL;
  SSqlCmd *pCmd = &pSql->cmd;

  /*
   * the query condition is serialized into pCmd->payload, we need to rebuild key for metricmeta info in cache.
   */
  bool    required = false;

  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, clauseIndex);
  for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    char tagstr[TSDB_MAX_TAGS_LEN + 1] = {0};

    SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfoFromQueryInfo(pQueryInfo, i);
    tscGetMetricMetaCacheKey(pQueryInfo, tagstr, pMeterMetaInfo->pMeterMeta->uid);

    taosRemoveDataFromCache(tscCacheHandle, (void **)&(pMeterMetaInfo->pMetricMeta), false);

    SMetricMeta *ppMeta = (SMetricMeta *)taosGetDataFromCache(tscCacheHandle, tagstr);
    if (ppMeta == NULL) {
      required = true;
      break;
    } else {
      pMeterMetaInfo->pMetricMeta = ppMeta;
    }
  }

  // all metricmeta for one clause are retrieved from cache, no need to retrieve metricmeta from management node
  if (!required) {
    return TSDB_CODE_SUCCESS;
  }

  SSqlObj *pNew = calloc(1, sizeof(SSqlObj));
  pNew->pTscObj = pSql->pTscObj;
  pNew->signature = pNew;

  pNew->cmd.command = TSDB_SQL_METRIC;
  
  SQueryInfo *pNewQueryInfo = NULL;
  if ((code = tscGetQueryInfoDetailSafely(&pNew->cmd, 0, &pNewQueryInfo)) != TSDB_CODE_SUCCESS) {
    return code;
  }
  
  for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    SMeterMetaInfo *pMMInfo = tscGetMeterMetaInfoFromQueryInfo(pQueryInfo, i);

    SMeterMeta *pMeterMeta = (SMeterMeta *)taosGetDataFromExists(tscCacheHandle, pQueryInfo->pMeterInfo[i]->pMeterMeta);
    assert(pMeterMeta != NULL);
    tscAddMeterMetaInfo(pNewQueryInfo, pMMInfo->name, pMeterMeta, NULL, pMMInfo->numOfTags, pMMInfo->tagColumnIndex);
  }

  if ((code = tscAllocPayload(&pNew->cmd, TSDB_DEFAULT_PAYLOAD_SIZE)) != TSDB_CODE_SUCCESS) {
    tscFreeSqlObj(pNew);
    return code;
  }

  tscTagCondCopy(&pNewQueryInfo->tagCond, &pQueryInfo->tagCond);

  pNewQueryInfo->groupbyExpr = pQueryInfo->groupbyExpr;
  pNewQueryInfo->numOfTables = pQueryInfo->numOfTables;

  pNewQueryInfo->slimit = pQueryInfo->slimit;
  pNewQueryInfo->order = pQueryInfo->order;
  
  STagCond* pTagCond = &pNewQueryInfo->tagCond;
  tscTrace("%p new sqlobj:%p info, numOfTables:%d, slimit:%" PRId64 ", soffset:%" PRId64 ", order:%d, tbname cond:%s",
      pSql, pNew, pNewQueryInfo->numOfTables, pNewQueryInfo->slimit.limit, pNewQueryInfo->slimit.offset,
      pNewQueryInfo->order.order, pTagCond->tbnameCond.cond)

//  if (pSql->fp != NULL && pSql->pStream == NULL) {
//    pCmd->pDataBlocks = tscDestroyBlockArrayList(pCmd->pDataBlocks);
//    tscFreeSubqueryInfo(pCmd);
//  }

  tscTrace("%p allocate new pSqlObj:%p to get metricMeta", pSql, pNew);
  if (pSql->fp == NULL) {
    tsem_init(&pNew->rspSem, 0, 0);
    tsem_init(&pNew->emptyRspSem, 0, 1);

    code = tscProcessSql(pNew);

    if (code == TSDB_CODE_SUCCESS) {//todo optimize the performance
      for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
        char tagstr[TSDB_MAX_TAGS_LEN] = {0};
    
        SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfoFromQueryInfo(pQueryInfo, i);
        tscGetMetricMetaCacheKey(pQueryInfo, tagstr, pMeterMetaInfo->pMeterMeta->uid);

#ifdef _DEBUG_VIEW
        printf("create metric key:%s, index:%d\n", tagstr, i);
#endif
    
        taosRemoveDataFromCache(tscCacheHandle, (void **)&(pMeterMetaInfo->pMetricMeta), false);
        pMeterMetaInfo->pMetricMeta = (SMetricMeta *)taosGetDataFromCache(tscCacheHandle, tagstr);
      }
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
  tscBuildMsg[TSDB_SQL_CREATE_USER] = tscBuildUserMsg;

  tscBuildMsg[TSDB_SQL_CREATE_ACCT] = tscBuildAcctMsg;
  tscBuildMsg[TSDB_SQL_ALTER_ACCT] = tscBuildAcctMsg;

  tscBuildMsg[TSDB_SQL_CREATE_TABLE] = tscBuildCreateTableMsg;
  tscBuildMsg[TSDB_SQL_DROP_USER] = tscBuildDropAcctMsg;
  tscBuildMsg[TSDB_SQL_DROP_ACCT] = tscBuildDropAcctMsg;
  tscBuildMsg[TSDB_SQL_DROP_DB] = tscBuildDropDbMsg;
  tscBuildMsg[TSDB_SQL_DROP_TABLE] = tscBuildDropTableMsg;
  tscBuildMsg[TSDB_SQL_ALTER_USER] = tscBuildUserMsg;
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
  tscBuildMsg[TSDB_SQL_KILL_QUERY] = tscBuildKillMsg;
  tscBuildMsg[TSDB_SQL_KILL_STREAM] = tscBuildKillMsg;
  tscBuildMsg[TSDB_SQL_KILL_CONNECTION] = tscBuildKillMsg;

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
  tscProcessMsgRsp[TSDB_SQL_RETRIEVE] = tscProcessRetrieveRspFromVnode;  // rsp handled by same function.
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
