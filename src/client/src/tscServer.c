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

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <wchar.h>

#include "os.h"
#include "tcache.h"
#include "trpc.h"
#include "tscProfile.h"
#include "tscSecondaryMerge.h"
#include "tscUtil.h"
#include "tschemautil.h"
#include "tsclient.h"
#include "tsocket.h"
#include "tsql.h"
#include "ttime.h"
#include "ttimer.h"
#include "tutil.h"

#define TSC_MGMT_VNODE 999

int      tsMasterIndex = 0;
int      tsSlaveIndex = 1;
uint32_t tsServerIp;

int (*tscBuildMsg[TSDB_SQL_MAX])(SSqlObj *pSql);
int (*tscProcessMsgRsp[TSDB_SQL_MAX])(SSqlObj *pSql);
void (*tscUpdateVnodeMsg[TSDB_SQL_MAX])(SSqlObj *pSql, char* buf);
void tscProcessActivityTimer(void *handle, void *tmrId);
int tscKeepConn[TSDB_SQL_MAX] = {0};

static int32_t minMsgSize() { return tsRpcHeadSize + sizeof(STaosDigest); }

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
    SSqlObj *pSql = (SSqlObj *)malloc(sizeof(SSqlObj));
    memset(pSql, 0, sizeof(SSqlObj));
    pSql->fp = tscProcessHeartBeatRsp;
    pSql->cmd.command = TSDB_SQL_HB;
    tscAllocPayloadWithSize(&(pSql->cmd), TSDB_DEFAULT_PAYLOAD_SIZE);
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

  if (pSql->retry < 1) {
    *pCode = 0;
    pSql->retry++;
    void *thandle = taosGetConnFromCache(tscConnCache, tsServerIp, TSC_MGMT_VNODE, pTscObj->user);

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

      connInit.peerIp = tsServerIpStr;
      thandle = taosOpenRpcConn(&connInit, pCode);
    }

    pSql->thandle = thandle;
    pSql->ip = tsServerIp;
    pSql->vnode = TSC_MGMT_VNODE;
  }
}

void tscGetConnToVnode(SSqlObj *pSql, uint8_t *pCode) {
  SVPeerDesc *pVPeersDesc = NULL;
  static int  vidIndex = 0;
  STscObj *   pTscObj = pSql->pTscObj;

  pSql->thandle = NULL;

  SSqlCmd *pCmd = &pSql->cmd;
  if (UTIL_METER_IS_METRIC(pCmd)) {  // multiple vnode query
    int32_t        idx = (pCmd->vnodeIdx > 0) ? pCmd->vnodeIdx - 1 : 0;
    SVnodeSidList *vnodeList = tscGetVnodeSidList(pCmd->pMetricMeta, idx);
    if (vnodeList != NULL) {
      pVPeersDesc = vnodeList->vpeerDesc;
    }
  } else {
    SMeterMeta *pMeta = pSql->cmd.pMeterMeta;
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
    break;
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
    int32_t totalMsgLen = pSql->cmd.payloadLen + tsRpcHeadSize + sizeof(STaosDigest);

    // the memory will be released by taosProcessResponse, so no memory leak here
    char* buf = malloc(totalMsgLen);
    memcpy(buf, pSql->cmd.payload, totalMsgLen);

    tscTrace("%p msg:%s is sent to server", pSql, taosMsg[pSql->cmd.msgType]);
    char *pStart = taosBuildReqHeader(pSql->thandle, pSql->cmd.msgType, buf);
    if (pStart) {
      if (tscUpdateVnodeMsg[pSql->cmd.command]) (*tscUpdateVnodeMsg[pSql->cmd.command])(pSql, buf);
      int ret = taosSendMsgToPeerH(pSql->thandle, pStart, pSql->cmd.payloadLen, pSql);
      if (ret >= 0) code = 0;
      tscTrace("%p send msg ret:%d code:%d sig:%p", pSql, ret, code, pSql->signature);
    }
  }

  return code;
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
    tscTrace("%p sql is already released or DB connection is closed, freed:%d pObj:%p signature:%p",
             pSql, pSql->freed, pObj, pObj->signature);
    taosAddConnIntoCache(tscConnCache, pSql->thandle, pSql->ip, pSql->vnode, pObj->user);
    tscFreeSqlObj(pSql);
    return ahandle;
  }

  if (msg == NULL) {
    tscTrace("%p no response from ip:0x%x", pSql, pSql->ip);
    pSql->index++;
    pSql->thandle = NULL;

    // todo taos_stop_query() in async model
    /*
     * in case of
     * 1. query cancelled(pRes->code != TSDB_CODE_QUERY_CANCELLED), do NOT re-issue the
     *    request to server.
     * 2. retrieve, do NOT re-issue the retrieve request since the qhandle may
     *    have been released by server
     */
    if (pCmd->command != TSDB_SQL_FETCH && pCmd->command != TSDB_SQL_RETRIEVE && pCmd->command != TSDB_SQL_KILL_QUERY &&
        pRes->code != TSDB_CODE_QUERY_CANCELLED) {
      code = tscSendMsgToServer(pSql);
      if (code == 0) return NULL;
    }

    // renew meter meta in case it is changed
    if (pCmd->command < TSDB_SQL_FETCH && pRes->code != TSDB_CODE_QUERY_CANCELLED) {
      // for fetch, it shall not renew meter meta
      pSql->maxRetry = 2;
      code = tscRenewMeterMeta(pSql, pCmd->name);
      pRes->code = code;
      if (code == TSDB_CODE_ACTION_IN_PROGRESS) return pSql;

      if (pCmd->pMeterMeta) {
        code = tscSendMsgToServer(pSql);
        if (code == 0) return pSql;
      }
    }
  } else {
    if (pMsg->content[0] == TSDB_CODE_NOT_ACTIVE_SESSION || pMsg->content[0] == TSDB_CODE_NETWORK_UNAVAIL ||
        pMsg->content[0] == TSDB_CODE_INVALID_SESSION_ID) {
      pSql->thandle = NULL;
      taosAddConnIntoCache(tscConnCache, thandle, pSql->ip, pSql->vnode, pObj->user);

      if (UTIL_METER_IS_METRIC(pCmd) && pMsg->content[0] == TSDB_CODE_NOT_ACTIVE_SESSION) {
        /*
         * for metric query, in case of any meter missing during query, sub-query of metric query will failed,
         * causing metric query failed, and return TSDB_CODE_METRICMETA_EXPIRED code to app
         */
        tscTrace("%p invalid meters id cause metric query failed, code:%d", pSql, pMsg->content[0]);
        code = TSDB_CODE_METRICMETA_EXPIRED;
      } else if ((pCmd->command == TSDB_SQL_INSERT || pCmd->command == TSDB_SQL_SELECT) &&
          pMsg->content[0] == TSDB_CODE_INVALID_SESSION_ID) {
        /*
         * session id is invalid(e.g., less than 0 or larger than maximum session per
         * vnode) in submit/query msg, no retry
         */
         code = TSDB_CODE_INVALID_QUERY_MSG;
      } else if (pCmd->command == TSDB_SQL_CONNECT) {
        code = TSDB_CODE_NETWORK_UNAVAIL;
      } else if (pCmd->command == TSDB_SQL_HB) {
        code = TSDB_CODE_NOT_READY;
      } else {
        tscTrace("%p it shall renew meter meta, code:%d", pSql, pMsg->content[0]);
        pSql->maxRetry = TSDB_VNODES_SUPPORT * 2;

        code = tscRenewMeterMeta(pSql, pCmd->name);
        if (code == TSDB_CODE_ACTION_IN_PROGRESS) return pSql;

        if (pCmd->pMeterMeta) {
          code = tscSendMsgToServer(pSql);
          if (code == 0) return pSql;
        }
      }

      msg = NULL;
    }
  }

  pSql->retry = 0;

  if (msg) {
    if (pCmd->command < TSDB_SQL_MGMT) {
      if (UTIL_METER_IS_NOMRAL_METER(pCmd)) {
        if (pCmd->pMeterMeta)  // it may be deleted
          pCmd->pMeterMeta->index = pSql->index;
      } else {
        int32_t        idx = (pSql->cmd.vnodeIdx == 0) ? 0 : pSql->cmd.vnodeIdx - 1;
        SVnodeSidList *pVnodeSidList = tscGetVnodeSidList(pCmd->pMetricMeta, idx);
        pVnodeSidList->index = pSql->index;
      }
    } else {
      if (pCmd->command > TSDB_SQL_READ)
        tsSlaveIndex = pSql->index;
      else
        tsMasterIndex = pSql->index;
    }
  }

  if (pSql->fp == NULL) sem_wait(&pSql->emptyRspSem);

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
    pRes->pRsp = (char *)realloc(pRes->pRsp, pRes->rspLen);
    if (pRes->rspLen) memcpy(pRes->pRsp, pMsg->content + 1, pRes->rspLen - 1);

    if (pRes->code == TSDB_CODE_DB_ALREADY_EXIST && pCmd->existsCheck && pRes->rspType == TSDB_MSG_TYPE_CREATE_DB_RSP) {
      /* ignore the error information returned from mnode when set ignore flag in sql */
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
    sem_post(&pSql->rspSem);
  } else {
    if (pRes->code == TSDB_CODE_SUCCESS && tscProcessMsgRsp[pCmd->command])
      code = (*tscProcessMsgRsp[pCmd->command])(pSql);

    if (code != TSDB_CODE_ACTION_IN_PROGRESS) {
      int   command = pCmd->command;
      void *taosres = tscKeepConn[command] ? pSql : NULL;
      code = pRes->code ? -pRes->code : pRes->numOfRows;

      tscTrace("%p Async SQL result:%d taosres:%p", pSql, code, taosres);

      /*
       * Whether to free sqlObj or not should be decided before call the user defined function, since
       * this SqlObj may be freed in UDF, and reused by other threads before tscShouldFreeAsyncSqlObj
       * called, in which case tscShouldFreeAsyncSqlObj checks an object which is actually allocated by other threads.
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

static SSqlObj* tscCreateSqlObjForSubquery(SSqlObj *pSql, SRetrieveSupport *trsupport, SSqlObj* prevSqlObj);
static int tscLaunchMetricSubQueries(SSqlObj *pSql);

int tscProcessSql(SSqlObj *pSql) {
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

  tscTrace("%p SQL cmd:%d will be processed, name:%s", pSql, pSql->cmd.command, pSql->cmd.name);

  pSql->retry = 0;
  if (pSql->cmd.command < TSDB_SQL_MGMT) {
    pSql->maxRetry = 2;

    if (UTIL_METER_IS_NOMRAL_METER(pCmd)) {
      pSql->index = pCmd->pMeterMeta->index;
    } else {
      if (pSql->cmd.vnodeIdx == 0) {  // it must be the parent SSqlObj for metric query
        // do nothing
      } else {
        int32_t        idx = pSql->cmd.vnodeIdx - 1;
        SVnodeSidList *pSidList = tscGetVnodeSidList(pCmd->pMetricMeta, idx);
        pSql->index = pSidList->index;
      }
    }
  } else if (pSql->cmd.command < TSDB_SQL_LOCAL) {
    pSql->index = pSql->cmd.command < TSDB_SQL_READ ? tsMasterIndex : tsSlaveIndex;
  } else {  // local handler
    return (*tscProcessMsgRsp[pCmd->command])(pSql);
  }

  int code = 0;

  if (tscIsTwoStageMergeMetricQuery(pSql)) {  // query on metric
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

      assert(pSql->cmd.vnodeIdx == 0);
      sem_post(&pSql->emptyRspSem);

      // set the command flag must be after the semaphore been correctly set.
      pSql->cmd.command = TSDB_SQL_RETRIEVE_METRIC;
    }

    return pSql->res.code;
  } else {
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
  }

  if (code != 0) {
    pRes->code = code;
    return code;
  }

  sem_wait(&pSql->rspSem);

  if (pRes->code == 0 && tscProcessMsgRsp[pCmd->command]) (*tscProcessMsgRsp[pCmd->command])(pSql);

  sem_post(&pSql->emptyRspSem);

  return pRes->code;
}

int tscLaunchMetricSubQueries(SSqlObj *pSql) {
  SSqlRes *pRes = &pSql->res;

  /* pRes->code check only serves in launching metric sub-queries */
  if (pRes->code == TSDB_CODE_QUERY_CANCELLED) {
    pSql->cmd.command = TSDB_SQL_RETRIEVE_METRIC;  // enable the abort of kill metric function.
    return pSql->res.code;
  }

  tExtMemBuffer **  pMemoryBuf = NULL;
  tOrderDescriptor *pDesc = NULL;
  tColModel *       pModel = NULL;

  pRes->qhandle = 1;  // hack the qhandle check

  const uint32_t nBufferSize = (1 << 16);  // 64KB
  int32_t        numOfVnodes = pSql->cmd.pMetricMeta->numOfVnodes;
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
  int32_t * retrievedDoneRec = calloc(1, sizeof(int64_t) << 1);
  int32_t * subStatusCode = &retrievedDoneRec[1];
  uint64_t *numOfTotalRetrievedPoints = (uint64_t *)&retrievedDoneRec[2];

  pRes->code = TSDB_CODE_SUCCESS;

  for (int32_t i = 0; i < numOfVnodes; ++i) {
    if (pRes->code == TSDB_CODE_QUERY_CANCELLED || pRes->code == TSDB_CODE_CLI_OUT_OF_MEMORY) {
      /*
       * during launch sub queries, if the master query is cancelled.
       * the remain is ignored and set the retrieveDoneRec to the value of remaining
       * not built sub-queries. So, the already issued sub queries can successfully free allocated resources.
       */
      *retrievedDoneRec = (numOfVnodes - i);

      if (i == 0) {
        /*
         * if i > 0, at least one sub query is issued, the allocated resource is done by it when it completed.
         */
        tscLocalReducerEnvDestroy(pMemoryBuf, pDesc, pModel, nBufferSize);
        free(retrievedDoneRec);
        pSql->cmd.command = TSDB_SQL_RETRIEVE_METRIC;
        // enable the abort of kill metric function.
        return pSql->res.code;
      }
      break;
    }

    SRetrieveSupport *trs = (SRetrieveSupport *)calloc(1, sizeof(SRetrieveSupport));
    trs->pExtMemBuffer = pMemoryBuf;
    trs->pOrderDescriptor = pDesc;
    trs->numOfFinished = retrievedDoneRec;
    trs->code = subStatusCode;
    trs->localBuffer = (tFilePage *)calloc(1, nBufferSize + sizeof(tFilePage));
    trs->vnodeIdx = i + 1;
    trs->numOfVnodes = numOfVnodes;
    trs->pParentSqlObj = pSql;
    trs->pFinalColModel = pModel;
    trs->numOfTotalRetrievedPoints = numOfTotalRetrievedPoints;

    pthread_mutexattr_t mutexattr = {0};
    pthread_mutexattr_settype(&mutexattr, PTHREAD_MUTEX_RECURSIVE_NP);
    pthread_mutex_init(&trs->queryMutex, &mutexattr);
    pthread_mutexattr_destroy(&mutexattr);

    SSqlObj *pNew = tscCreateSqlObjForSubquery(pSql, trs, NULL);
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

  *(trsupport->code) = -errCode;
  trsupport->numOfRetry = MAX_NUM_OF_SUBQUERY_RETRY;

  pthread_mutex_unlock(&trsupport->queryMutex);

  tscRetrieveFromVnodeCallBack(trsupport, tres, *(trsupport->code));
}

static void tscHandleSubRetrievalError(SRetrieveSupport *trsupport, SSqlObj *pSql, int numOfRows) {
  SSqlObj *pPObj = trsupport->pParentSqlObj;
  int32_t  idx = trsupport->vnodeIdx;

  assert(pSql != NULL);

  /* retrieved in subquery failed. OR query cancelled in retrieve phase. */
  if (*trsupport->code == TSDB_CODE_SUCCESS && pPObj->res.code != TSDB_CODE_SUCCESS) {
    *trsupport->code = -(int)pPObj->res.code;

    /*
     * kill current sub-query connection, which may retrieve data from vnodes;
     * Here we get: pPObj->res.code == TSDB_CODE_QUERY_CANCELLED
     */
    pSql->res.numOfRows = 0;
    trsupport->numOfRetry = MAX_NUM_OF_SUBQUERY_RETRY;  // disable retry efforts
    tscTrace("%p query is cancelled, sub:%p, orderOfSub:%d abort retrieve, code:%d", trsupport->pParentSqlObj, pSql,
             trsupport->vnodeIdx, *trsupport->code);
  }

  if (numOfRows >= 0) {
    /* current query is successful, but other sub query failed, still abort current query. */
    tscTrace("%p sub:%p retrieve numOfRows:%d,orderOfSub:%d", pPObj, pSql, numOfRows, idx);
    tscError("%p sub:%p abort further retrieval due to other queries failure,orderOfSub:%d,code:%d",
        pPObj, pSql, idx, *trsupport->code);
  } else {
    if (trsupport->numOfRetry++ < MAX_NUM_OF_SUBQUERY_RETRY && *(trsupport->code) == TSDB_CODE_SUCCESS) {
      /*
       * current query failed, and the retry count is less than the available count,
       * retry query clear previous retrieved data, then launch a new sub query
       */
      tExtMemBufferClear(trsupport->pExtMemBuffer[idx - 1]);

      // clear local saved number of results
      trsupport->localBuffer->numOfElems = 0;

      pthread_mutex_unlock(&trsupport->queryMutex);

      SSqlObj *pNew = tscCreateSqlObjForSubquery(trsupport->pParentSqlObj, trsupport, pSql);
      tscTrace("%p sub:%p retrieve failed, code:%d, orderOfSub:%d, retry:%d, new SqlObj:%p",
          trsupport->pParentSqlObj, pSql, numOfRows, idx, trsupport->numOfRetry, pNew);

      tscProcessSql(pNew);
      return;
    } else {
      /* reach the maximum retry count, abort. */
      __sync_val_compare_and_swap_32(trsupport->code, TSDB_CODE_SUCCESS, numOfRows);
      tscError("%p sub:%p retrieve failed,code:%d,orderOfSub:%d failed.no more retry,set global code:%d",
               pPObj, pSql, numOfRows, idx, *trsupport->code);
    }
  }

  if (__sync_add_and_fetch_32(trsupport->numOfFinished, 1) < trsupport->numOfVnodes) {
    return tscFreeSubSqlObj(trsupport, pSql);
  }

  // all subqueries are failed
  tscError("%p retrieve from %d vnode(s) completed,code:%d.FAILED.", pPObj, trsupport->numOfVnodes, *trsupport->code);
  pPObj->res.code = -(*trsupport->code);

  // release allocated resource
  tscLocalReducerEnvDestroy(trsupport->pExtMemBuffer, trsupport->pOrderDescriptor, trsupport->pFinalColModel,
                            trsupport->numOfVnodes);

  tfree(trsupport->numOfFinished);
  tscFreeSubSqlObj(trsupport, pSql);

  if (pPObj->fp == NULL) {
    // sync query, wait for the master SSqlObj to proceed
    sem_wait(&pPObj->emptyRspSem);
    sem_wait(&pPObj->emptyRspSem);

    sem_post(&pPObj->rspSem);

    pPObj->cmd.command = TSDB_SQL_RETRIEVE_METRIC;
  } else {
    // in async query model, no need to sync operation
    if (pPObj->res.code != 0) {
      tscQueueAsyncRes(pPObj);
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

  if (numOfRows < 0 || *(trsupport->code) < 0 || pPObj->res.code != TSDB_CODE_SUCCESS) {
    return tscHandleSubRetrievalError(trsupport, pSql, numOfRows);
  }

  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

  SVnodeSidList *vnodeInfo = tscGetVnodeSidList(pCmd->pMetricMeta, idx - 1);
  SVPeerDesc *   pSvd = &vnodeInfo->vpeerDesc[vnodeInfo->index];

  if (numOfRows > 0) {
    assert(pRes->numOfRows == numOfRows);
    __sync_add_and_fetch_64(trsupport->numOfTotalRetrievedPoints, numOfRows);

    tscTrace("%p sub:%p retrieve numOfRows:%d totalNumOfRows:%d from ip:%u,vid:%d,orderOfSub:%d",
             pPObj, pSql, pRes->numOfRows, *trsupport->numOfTotalRetrievedPoints, pSvd->ip, pSvd->vnode, idx);

#ifdef _DEBUG_VIEW
    printf("received data from vnode: %d rows\n", pRes->numOfRows);
    SSrcColumnInfo colInfo[256] = {0};
    tscGetSrcColumnInfo(colInfo, &pPObj->cmd);
    tColModelDisplayEx(pDesc->pSchema, pRes->data, pRes->numOfRows, pRes->numOfRows, colInfo);
#endif
    int32_t ret = saveToBuffer(trsupport->pExtMemBuffer[idx - 1], pDesc, trsupport->localBuffer, pRes->data,
                               pRes->numOfRows, pCmd->groupbyExpr.orderType);
    if (ret < 0) {
      // set no disk space error info, and abort retry
      tscAbortFurtherRetryRetrieval(trsupport, tres, TSDB_CODE_CLI_NO_DISKSPACE);
    } else {
      pthread_mutex_unlock(&trsupport->queryMutex);
      taos_fetch_rows_a(tres, tscRetrieveFromVnodeCallBack, param);
    }

  } else {
    // all data has been retrieved to client data in from current vnode is stored in cache and disk
    uint32_t numOfRowsFromVnode = trsupport->pExtMemBuffer[idx - 1]->numOfAllElems + trsupport->localBuffer->numOfElems;
    tscTrace("%p sub:%p all data retrieved from ip:%u,vid:%d, numOfRows:%d, orderOfSub:%d",
             pPObj, pSql, pSvd->ip, pSvd->vnode, numOfRowsFromVnode, idx);

    tColModelCompact(pDesc->pSchema, trsupport->localBuffer, pDesc->pSchema->maxCapacity);

#ifdef _DEBUG_VIEW
    printf("%ld rows data flushed to disk:\n", trsupport->localBuffer->numOfElems);
    SSrcColumnInfo colInfo[256] = {0};
    tscGetSrcColumnInfo(colInfo, &pPObj->cmd);
    tColModelDisplayEx(pDesc->pSchema, trsupport->localBuffer->data, trsupport->localBuffer->numOfElems,
                       trsupport->localBuffer->numOfElems, colInfo);
#endif

    // each result for a vnode is ordered as an independant list,
    // then used as an input of loser tree for disk-based merge routine
    int32_t ret = tscFlushTmpBuffer(trsupport->pExtMemBuffer[idx - 1], pDesc, trsupport->localBuffer,
                                    pCmd->groupbyExpr.orderType);
    if (ret != 0) {
      /* set no disk space error info, and abort retry */
      return tscAbortFurtherRetryRetrieval(trsupport, tres, TSDB_CODE_CLI_NO_DISKSPACE);
    }

    if (__sync_add_and_fetch_32(trsupport->numOfFinished, 1) < trsupport->numOfVnodes) {
      return tscFreeSubSqlObj(trsupport, pSql);
    }

    // all sub-queries are returned, start to local merge process
    pDesc->pSchema->maxCapacity = trsupport->pExtMemBuffer[idx - 1]->numOfElemsPerPage;

    tscTrace("%p retrieve from %d vnodes completed.final NumOfRows:%d,start to build loser tree",
             pPObj, trsupport->numOfVnodes, *trsupport->numOfTotalRetrievedPoints);

    tscClearInterpInfo(&pPObj->cmd);
    tscCreateLocalReducer(trsupport->pExtMemBuffer, trsupport->numOfVnodes, pDesc, trsupport->pFinalColModel,
                          &pPObj->cmd, &pPObj->res);
    tscTrace("%p build loser tree completed", pPObj);

    pPObj->res.precision = pSql->res.precision;
    pPObj->res.numOfRows = 0;
    pPObj->res.row = 0;

    // only free once
    free(trsupport->numOfFinished);
    tscFreeSubSqlObj(trsupport, pSql);

    if (pPObj->fp == NULL) {
      sem_wait(&pPObj->emptyRspSem);
      sem_wait(&pPObj->emptyRspSem);

      sem_post(&pPObj->rspSem);
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
  if (!tscIsTwoStageMergeMetricQuery(pSql)) {
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

SSqlObj* tscCreateSqlObjForSubquery(SSqlObj *pSql, SRetrieveSupport *trsupport, SSqlObj* prevSqlObj) {
  SSqlCmd *pCmd = &pSql->cmd;

  SSqlObj *pNew = (SSqlObj *)calloc(1, sizeof(SSqlObj));
  if (pNew == NULL) {
    return NULL;
  }

  pSql->pSubs[trsupport->vnodeIdx - 1] = pNew;
  pNew->pTscObj = pSql->pTscObj;
  pNew->signature = pNew;
  pNew->sqlstr = strdup(pSql->sqlstr);

  memcpy(&pNew->cmd, pCmd, sizeof(SSqlCmd));
  pNew->cmd.command = TSDB_SQL_SELECT;
  pNew->cmd.payload = NULL;
  pNew->cmd.allocSize = 0;

  tscTagCondAssign(&pNew->cmd.tagCond, &pCmd->tagCond);

  tscAllocPayloadWithSize(&pNew->cmd, TSDB_DEFAULT_PAYLOAD_SIZE);
  tscColumnInfoClone(&pCmd->colList, &pNew->cmd.colList);
  tscFieldInfoClone(&pCmd->fieldsInfo, &pNew->cmd.fieldsInfo);
  tscSqlExprClone(&pCmd->exprsInfo, &pNew->cmd.exprsInfo);

  pNew->fp = tscRetrieveDataRes;

  pNew->param = trsupport;
  pNew->cmd.vnodeIdx = trsupport->vnodeIdx;

  if (prevSqlObj == NULL) {
    char key[TSDB_MAX_TAGS_LEN + 1] = {0};
    tscGetMetricMetaCacheKey(&pNew->cmd, key);
    pNew->cmd.pMetricMeta = taosGetDataFromCache(tscCacheHandle, key);
    pNew->cmd.pMeterMeta = taosGetDataFromCache(tscCacheHandle, pCmd->name);
  } else {
    pNew->cmd.pMeterMeta = prevSqlObj->cmd.pMeterMeta;
    pNew->cmd.pMetricMeta = prevSqlObj->cmd.pMetricMeta;

    prevSqlObj->cmd.pMetricMeta = NULL;
    prevSqlObj->cmd.pMeterMeta = NULL;
  }

  assert(pNew->cmd.pMeterMeta != NULL && pNew->cmd.pMetricMeta != NULL);
  return pNew;
}

void tscRetrieveDataRes(void *param, TAOS_RES *tres, int retCode) {
  SRetrieveSupport *trsupport = (SRetrieveSupport *)param;

  SSqlObj *pSql = (SSqlObj *)tres;
  int32_t  idx = pSql->cmd.vnodeIdx;

  SVnodeSidList *vnodeInfo = NULL;
  if (pSql->cmd.pMetricMeta != NULL) {
    vnodeInfo = tscGetVnodeSidList(pSql->cmd.pMetricMeta, idx - 1);
  }

  if (trsupport->pParentSqlObj->res.code != TSDB_CODE_SUCCESS || *trsupport->code != TSDB_CODE_SUCCESS) {
    // metric query is killed, Note: retCode must be less than 0
    trsupport->numOfRetry = MAX_NUM_OF_SUBQUERY_RETRY;
    if (trsupport->pParentSqlObj->res.code != TSDB_CODE_SUCCESS) {
      retCode = -(int)(trsupport->pParentSqlObj->res.code);
    } else {
      retCode = (*trsupport->code);
    }
    tscTrace("%p query cancelled or failed, sub:%p, orderOfSub:%d abort, code:%d", trsupport->pParentSqlObj, pSql,
             trsupport->vnodeIdx, retCode);
  }

  /*
   * if a query on vnode is failed, all retrieve operations from vnode that occurs later
   * than this one are actually not necessary, we simply call the tscRetrieveFromVnodeCallBack
   * function to abort current and remain retrieve process.
   * Note: threadsafe is required.
   */
  if (retCode != TSDB_CODE_SUCCESS) {
    if (trsupport->numOfRetry++ >= MAX_NUM_OF_SUBQUERY_RETRY) {
      tscTrace("%p sub:%p reach the max retry count,set global code:%d", trsupport->pParentSqlObj, pSql, retCode);
      __sync_val_compare_and_swap_32(trsupport->code, 0, retCode);
    } else {  // does not reach the maximum retry count, go on
      SSqlObj *pNew = tscCreateSqlObjForSubquery(trsupport->pParentSqlObj, trsupport, pSql);
      tscTrace("%p sub:%p failed code:%d, retry:%d, new SqlObj:%p", trsupport->pParentSqlObj, pSql, retCode,
               trsupport->numOfRetry, pNew);

      tscProcessSql(pNew);
      return;
    }
  }

  if (*(trsupport->code) != TSDB_CODE_SUCCESS) {  // failed, abort
    if (vnodeInfo != NULL) {
      tscTrace("%p sub:%p query failed,ip:%u,vid:%d,orderOfSub:%d,global code:%d", trsupport->pParentSqlObj, pSql,
               vnodeInfo->vpeerDesc[vnodeInfo->index].ip, vnodeInfo->vpeerDesc[vnodeInfo->index].vnode,
               trsupport->vnodeIdx, *(trsupport->code));
    } else {
      tscTrace("%p sub:%p query failed,orderOfSub:%d,global code:%d", trsupport->pParentSqlObj, pSql,
               trsupport->vnodeIdx, *(trsupport->code));
    }

    tscRetrieveFromVnodeCallBack(param, tres, *(trsupport->code));
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
  pMsg += 8;
  *pMsg = pSql->cmd.type;
  pMsg += 1;

  msgLen = pMsg - pStart;
  pSql->cmd.payloadLen = msgLen;
  pSql->cmd.msgType = TSDB_MSG_TYPE_RETRIEVE;

  return msgLen;
}

void tscUpdateVnodeInSubmitMsg(SSqlObj *pSql, char* buf) {
  SShellSubmitMsg *pShellMsg;
  char *           pMsg;
  SMeterMeta *     pMeterMeta = pSql->cmd.pMeterMeta;

  pMsg = buf + tsRpcHeadSize;

  pShellMsg = (SShellSubmitMsg *)pMsg;
  pShellMsg->vnode = htons(pMeterMeta->vpeerDesc[pSql->index].vnode);
  tscTrace("%p update submit msg vnode:%d", pSql, htons(pShellMsg->vnode));
}

int tscBuildSubmitMsg(SSqlObj *pSql) {
  SShellSubmitMsg *pShellMsg;
  char *           pMsg, *pStart;
  int              msgLen = 0;
  SMeterMeta *     pMeterMeta = pSql->cmd.pMeterMeta;

  pStart = pSql->cmd.payload + tsRpcHeadSize;
  pMsg = pStart;

  pShellMsg = (SShellSubmitMsg *)pMsg;
  pShellMsg->import = pSql->cmd.order.order;
  pShellMsg->vnode = htons(pMeterMeta->vpeerDesc[pMeterMeta->index].vnode);
  pShellMsg->numOfSid = htonl(pSql->cmd.count); /* number of meters to be inserted */

  /*
   * pSql->cmd.payloadLen is set during parse sql routine, so we do not use it here
   */
  pSql->cmd.msgType = TSDB_MSG_TYPE_SUBMIT;
  tscTrace("%p update submit msg vnode:%d", pSql, htons(pShellMsg->vnode));

  return msgLen;
}

void tscUpdateVnodeInQueryMsg(SSqlObj *pSql, char* buf) {
  SSqlCmd *pCmd = &pSql->cmd;
  char *   pStart = buf + tsRpcHeadSize;

  SQueryMeterMsg *pQueryMsg = (SQueryMeterMsg *)pStart;

  if (UTIL_METER_IS_NOMRAL_METER(pCmd)) {  // pSchema == NULL, query on meter
    SMeterMeta *pMeterMeta = pCmd->pMeterMeta;
    pQueryMsg->vnode = htons(pMeterMeta->vpeerDesc[pSql->index].vnode);
  } else {  // query on metric
    SMetricMeta *  pMetricMeta = pCmd->pMetricMeta;
    SVnodeSidList *pVnodeSidList = tscGetVnodeSidList(pMetricMeta, pCmd->vnodeIdx - 1);
    pQueryMsg->vnode = htons(pVnodeSidList->vpeerDesc[pSql->index].vnode);
  }
}

/*
 * for meter query, simply return the size <= 1k
 * for metric query, estimate size according to meter tags
 */
static int32_t tscEstimateQueryMsgSize(SSqlCmd *pCmd) {
  const static int32_t MIN_QUERY_MSG_PKT_SIZE = TSDB_MAX_BYTES_PER_ROW * 5;
  int32_t              srcColListSize = pCmd->numOfCols * sizeof(SColumnFilterMsg);

  int32_t exprSize = sizeof(SSqlFuncExprMsg) * pCmd->fieldsInfo.numOfOutputCols;

  // meter query without tags values
  if (!UTIL_METER_IS_METRIC(pCmd)) {
    return MIN_QUERY_MSG_PKT_SIZE + minMsgSize() + sizeof(SQueryMeterMsg) + srcColListSize + exprSize;
  }

  SMetricMeta *pMetricMeta = pCmd->pMetricMeta;

  SVnodeSidList *pVnodeSidList = tscGetVnodeSidList(pMetricMeta, pCmd->vnodeIdx - 1);

  int32_t meterInfoSize = (pMetricMeta->tagLen + sizeof(SMeterSidExtInfo)) * pVnodeSidList->numOfSids;
  int32_t outputColumnSize = pCmd->fieldsInfo.numOfOutputCols * sizeof(SSqlFuncExprMsg);

  return meterInfoSize + outputColumnSize + srcColListSize + exprSize + MIN_QUERY_MSG_PKT_SIZE;
}

int tscBuildQueryMsg(SSqlObj *pSql) {
  SSqlCmd *pCmd = &pSql->cmd;

  int32_t size = tscEstimateQueryMsgSize(pCmd);
  tscAllocPayloadWithSize(pCmd, size);

  char *pStart = pCmd->payload + tsRpcHeadSize;

  SMeterMeta * pMeterMeta = pCmd->pMeterMeta;
  SMetricMeta *pMetricMeta = pCmd->pMetricMeta;

  SQueryMeterMsg *pQueryMsg = (SQueryMeterMsg *)pStart;

  int32_t msgLen = 0;
  int32_t numOfMeters = 0;

  if (UTIL_METER_IS_NOMRAL_METER(pCmd)) {  // pSchema == NULL, query on meter
    numOfMeters = 1;

    tscTrace("%p query on vnode: %d, number of sid:%d, meter id: %s", pSql,
             pMeterMeta->vpeerDesc[pMeterMeta->index].vnode, 1, pCmd->name);

    pQueryMsg->vnode = htons(pMeterMeta->vpeerDesc[pMeterMeta->index].vnode);
    pQueryMsg->uid = pMeterMeta->uid;
    pQueryMsg->numOfTagsCols = 0;
  } else {  // query on metric
    SMetricMeta *pMetricMeta = pCmd->pMetricMeta;
    if (pCmd->vnodeIdx <= 0) {
      tscError("%p error vnodeIdx:%d", pSql, pCmd->vnodeIdx);
      return -1;
    }

    SVnodeSidList *pVnodeSidList = tscGetVnodeSidList(pMetricMeta, pCmd->vnodeIdx - 1);
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
  pQueryMsg->numOfTagsCols = htons(pCmd->numOfReqTags);

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

  if (UTIL_METER_IS_NOMRAL_METER(pCmd)) {  // query on meter
    assert(pCmd->groupbyExpr.numOfGroupbyCols == 0);
    pQueryMsg->tagLength = 0;
  } else {  // query on metric

    if (pCmd->groupbyExpr.numOfGroupbyCols > 0) {
      pQueryMsg->numOfGroupbyCols = htons(pCmd->groupbyExpr.numOfGroupbyCols);
      if (pCmd->groupbyExpr.numOfGroupbyCols < 0) {
        tscError("%p illegal value of numOfGroupbyCols in query msg: %d", pSql, pCmd->groupbyExpr.numOfGroupbyCols);
        return -1;
      }
    } else {  // no group by clause
      pQueryMsg->numOfGroupbyCols = 0;
    }
    pQueryMsg->tagLength = htons(pMetricMeta->tagLen);
  }

  pQueryMsg->metricQuery = htons(pCmd->metricQuery);
  pQueryMsg->numOfOutputCols = htons(pCmd->fieldsInfo.numOfOutputCols);

  if (pCmd->fieldsInfo.numOfOutputCols < 0) {
    tscError("%p illegal value of number of output columns in query msg: %d", pSql, pCmd->fieldsInfo.numOfOutputCols);
    return -1;
  }

  // set column list ids
  char *pMsg = (char *)(pQueryMsg->colList);
  char *pBinaryBuf = pMsg + sizeof(pQueryMsg->colList[0]) * pCmd->colList.numOfCols;

  SSchema *pSchema = tsGetSchema(pMeterMeta);

  for (int32_t i = 0; i < pCmd->colList.numOfCols; ++i) {
    SColumnBase *pCol = tscColumnInfoGet(pCmd, i);
    SSchema *    pColSchema = &pSchema[pCol->colIndex];

    if (pCol->colIndex >= pMeterMeta->numOfColumns || pColSchema->type < TSDB_DATA_TYPE_BOOL ||
        pColSchema->type > TSDB_DATA_TYPE_NCHAR) {
      tscError("%p vid:%d sid:%d id:%s, column index out of range, numOfColumns:%d, index:%d, column name:%s",
               pSql, htons(pQueryMsg->vnode), pMeterMeta->sid, pCmd->name, pMeterMeta->numOfColumns, pCol->colIndex,
               pColSchema->name);

      return 0;  // 0 means build msg failed
    }

    pQueryMsg->colList[i].colId = htons(pColSchema->colId);
    pQueryMsg->colList[i].bytes = htons(pColSchema->bytes);

    pQueryMsg->colList[i].type = htons(pColSchema->type);

    pQueryMsg->colList[i].filterOn = htons(pCol->filterOn);
    pQueryMsg->colList[i].filterOnBinary = htons(pCol->filterOnBinary);

    if (pCol->filterOn && pCol->filterOnBinary) {
      pQueryMsg->colList[i].len = htobe64(pCol->len);
      memcpy(pBinaryBuf, (void *)pCol->pz, pCol->len + 1);
      pBinaryBuf += pCol->len + 1;
    } else {
      pQueryMsg->colList[i].lowerBndi = htobe64(pCol->lowerBndi);
      pQueryMsg->colList[i].upperBndi = htobe64(pCol->upperBndi);
    }

    pQueryMsg->colList[i].lowerRelOptr = htons(pCol->lowerRelOptr);
    pQueryMsg->colList[i].upperRelOptr = htons(pCol->upperRelOptr);

    pMsg += sizeof(SColumnFilterMsg);
  }

  bool hasArithmeticFunction = false;

  pMsg = pBinaryBuf;
  SSqlFuncExprMsg *pSqlFuncExpr = (SSqlFuncExprMsg *)pBinaryBuf;

  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    SSqlExpr *pExpr = tscSqlExprGet(pCmd, i);

    if (pExpr->sqlFuncId == TSDB_FUNC_ARITHM) {
      hasArithmeticFunction = true;
    }

    if (!tscValidateColumnId(pCmd, pExpr->colInfo.colId)) {
      /* column id is not valid according to the cached metermeta, the meter meta is expired */
      tscError("%p table schema is not matched with parsed sql", pSql);
      return -1;
    }

    pSqlFuncExpr->colInfo.colId = htons(pExpr->colInfo.colId);
    pSqlFuncExpr->colInfo.colIdx = htons(pExpr->colInfo.colIdx);
    pSqlFuncExpr->colInfo.isTag = pExpr->colInfo.isTag;

    pSqlFuncExpr->functionId = htons(pExpr->sqlFuncId);
    pSqlFuncExpr->numOfParams = htons(pExpr->numOfParams);
    pMsg += sizeof(SSqlFuncExprMsg);

    for (int32_t j = 0; j < pExpr->numOfParams; ++j) {
      pSqlFuncExpr->arg[j].argType = htons((uint16_t)pExpr->param[j].nType);
      pSqlFuncExpr->arg[j].argBytes = htons(pExpr->param[j].nLen);

      if (pExpr->param[j].nType == TSDB_DATA_TYPE_BINARY) {
        memcpy(pMsg, pExpr->param[j].pz, pExpr->param[j].nLen);
        pMsg += pExpr->param[j].nLen + 1;  // by plus one char to make the string null-terminated
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
      char *  name = pSchema[pColBase[i].colIndex].name;
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
  if (UTIL_METER_IS_NOMRAL_METER(pCmd)) {
#ifdef _DEBUG_VIEW
    tscTrace("%p %d", pSql, pCmd->pMeterMeta->sid);
#endif
    SMeterSidExtInfo *pSMeterTagInfo = (SMeterSidExtInfo *)pMsg;
    pSMeterTagInfo->sid = htonl(pMeterMeta->sid);
    pMsg += sizeof(SMeterSidExtInfo);
  } else {
    SVnodeSidList *pVnodeSidList = tscGetVnodeSidList(pMetricMeta, pCmd->vnodeIdx - 1);

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

  /* only include the required tag column schema. If a tag is not required, it won't be sent to vnode */
  if (UTIL_METER_IS_METRIC(pCmd) && pCmd->numOfReqTags > 0) {  // always transfer tag schema to vnode if exists
    SSchema *pTagSchema = tsGetTagSchema(pMeterMeta);

    for (int32_t j = 0; j < pCmd->numOfReqTags; ++j) {
      if (pCmd->tagColumnIndex[j] == -1) {
        SSchema tbSchema = {.bytes = TSDB_METER_NAME_LEN, .colId = -1, .type = TSDB_DATA_TYPE_BINARY};
        memcpy(pMsg, &tbSchema, sizeof(SSchema));
      } else {
        memcpy(pMsg, &pTagSchema[pCmd->tagColumnIndex[j]], sizeof(SSchema));
      }

      pMsg += sizeof(SSchema);
    }
  }

  SSqlGroupbyExpr *pGroupbyExpr = &pCmd->groupbyExpr;
  if (pGroupbyExpr->numOfGroupbyCols != 0) {
    assert(pMeterMeta->numOfTags != 0);

    pQueryMsg->orderByIdx = htons(pGroupbyExpr->orderIdx);
    pQueryMsg->orderType = htons(pGroupbyExpr->orderType);

    for (int32_t j = 0; j < pGroupbyExpr->numOfGroupbyCols; ++j) {
      *((int16_t *)pMsg) = pGroupbyExpr->tagIndex[j];
      pMsg += sizeof(pGroupbyExpr->tagIndex[j]);
    }
  }

  if (pCmd->interpoType != TSDB_INTERPO_NONE) {
    for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
      *((int64_t *)pMsg) = htobe64(pCmd->defaultVal[i]);
      pMsg += sizeof(pCmd->defaultVal[0]);
    }
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

  SSqlCmd *pCmd = &pSql->cmd;
  STscObj *pObj = pSql->pTscObj;
  pStart = pCmd->payload + tsRpcHeadSize;
  pMsg = pStart;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  strcpy(pMgmt->db, pObj->db);
  pMsg += sizeof(SMgmtHead);

  pCreateDbMsg = (SCreateDbMsg *)pMsg;
  strcpy(pCreateDbMsg->db, pCmd->name);
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

  SSqlCmd *pCmd = &pSql->cmd;
  STscObj *pObj = pSql->pTscObj;
  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  strcpy(pMgmt->db, pObj->db);
  pMsg += sizeof(SMgmtHead);

  pCreate = (SCreateDnodeMsg *)pMsg;
  strcpy(pCreate->ip, pCmd->name);

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

  SSqlCmd *pCmd = &pSql->cmd;
  STscObj *pObj = pSql->pTscObj;
  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  strcpy(pMgmt->db, pObj->db);
  pMsg += sizeof(SMgmtHead);

  pDrop = (SDropDnodeMsg *)pMsg;
  strcpy(pDrop->ip, pCmd->name);

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

  SSqlCmd *pCmd = &pSql->cmd;
  STscObj *pObj = pSql->pTscObj;
  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  strcpy(pMgmt->db, pObj->db);
  pMsg += sizeof(SMgmtHead);

  pCreateMsg = (SCreateUserMsg *)pMsg;
  strcpy(pCreateMsg->user, pCmd->name);
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

  SSqlCmd *pCmd = &pSql->cmd;
  STscObj *pObj = pSql->pTscObj;
  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  strcpy(pMgmt->db, pObj->db);
  pMsg += sizeof(SMgmtHead);

  pAlterMsg = (SCreateAcctMsg *)pMsg;
  strcpy(pAlterMsg->user, pCmd->name);
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

  SSqlCmd *pCmd = &pSql->cmd;
  STscObj *pObj = pSql->pTscObj;
  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  strcpy(pMgmt->db, pObj->db);
  pMsg += sizeof(SMgmtHead);

  pAlterMsg = (SCreateUserMsg *)pMsg;
  strcpy(pAlterMsg->user, pCmd->name);
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

  SSqlCmd *pCmd = &pSql->cmd;
  STscObj *pObj = pSql->pTscObj;
  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  strcpy(pMgmt->db, pObj->db);
  pMsg += sizeof(SMgmtHead);

  pCfg = (SCfgMsg *)pMsg;
  strcpy(pCfg->ip, pCmd->name);
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

  SSqlCmd *pCmd = &pSql->cmd;
  STscObj *pObj = pSql->pTscObj;
  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  strcpy(pMgmt->db, pObj->db);
  pMsg += sizeof(SMgmtHead);

  pDropDbMsg = (SDropDbMsg *)pMsg;
  strcpy(pDropDbMsg->db, pCmd->name);

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

  SSqlCmd *pCmd = &pSql->cmd;
  STscObj *pObj = pSql->pTscObj;
  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  strcpy(pMgmt->db, pObj->db);
  pMsg += sizeof(SMgmtHead);

  pDropMsg = (SDropUserMsg *)pMsg;
  strcpy(pDropMsg->user, pCmd->name);

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

  SSqlCmd *pCmd = &pSql->cmd;
  STscObj *pObj = pSql->pTscObj;
  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  strcpy(pMgmt->db, pObj->db);
  pMsg += sizeof(SMgmtHead);

  pDropMsg = (SDropAcctMsg *)pMsg;
  strcpy(pDropMsg->user, pCmd->name);

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

  SSqlCmd *pCmd = &pSql->cmd;
  STscObj *pObj = pSql->pTscObj;
  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  strcpy(pMgmt->db, pObj->db);
  pMsg += sizeof(SMgmtHead);

  pUseDbMsg = (SUseDbMsg *)pMsg;
  strcpy(pUseDbMsg->db, pCmd->name);

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
  tscAllocPayloadWithSize(pCmd, size);

  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  if (pCmd->tagCond.len > 0) {
    strcpy(pMgmt->db, pCmd->tagCond.pData);
  } else {
    strcpy(pMgmt->db, pObj->db);
  }

  pMsg += sizeof(SMgmtHead);

  pShowMsg = (SShowMsg *)pMsg;
  pShowMsg->type = pCmd->type;

  if ((pShowMsg->type == TSDB_MGMT_TABLE_TABLE || pShowMsg->type == TSDB_MGMT_TABLE_METRIC) &&
      pCmd->payloadLen != 0) {
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

  SSqlCmd *pCmd = &pSql->cmd;

  // Reallocate the payload size
  size = tscEstimateCreateTableMsgLength(pSql);
  tscAllocPayloadWithSize(pCmd, size);

  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  // use dbinfo from meterid without modifying current db info
  tscGetDBInfoFromMeterId(pSql->cmd.name, pMgmt->db);

  pMsg += sizeof(SMgmtHead);

  pCreateTableMsg = (SCreateTableMsg *)pMsg;
  strcpy(pCreateTableMsg->meterId, pCmd->name);

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
  tscClearFieldInfo(pCmd);

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

  SSqlCmd *pCmd = &pSql->cmd;

  char buf[TSDB_MAX_TAGS_LEN] = {0};
  int32_t len = (TSDB_MAX_TAGS_LEN < pCmd->allocSize)? TSDB_MAX_TAGS_LEN:pCmd->allocSize;
  memcpy(buf, pCmd->payload, len);

  size = tscEstimateAlterTableMsgLength(pCmd);
  tscAllocPayloadWithSize(pCmd, size);

  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  tscGetDBInfoFromMeterId(pCmd->name, pMgmt->db);
  pMsg += sizeof(SMgmtHead);

  pAlterTableMsg = (SAlterTableMsg *)pMsg;
  strcpy(pAlterTableMsg->meterId, pCmd->name);
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

  SSqlCmd *pCmd = &pSql->cmd;
  STscObj *pObj = pSql->pTscObj;
  pStart = pCmd->payload + tsRpcHeadSize;
  pMsg = pStart;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  strcpy(pMgmt->db, pObj->db);
  pMsg += sizeof(SMgmtHead);

  pAlterDbMsg = (SAlterDbMsg *)pMsg;
  strcpy(pAlterDbMsg->db, pCmd->name);

  pAlterDbMsg->replications = pCmd->defaultVal[0];
  pAlterDbMsg->daysPerFile = htonl(pCmd->defaultVal[1]);
  pAlterDbMsg->daysToKeep = htonl(pCmd->defaultVal[2]);

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

  SSqlCmd *pCmd = &pSql->cmd;
  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  tscGetDBInfoFromMeterId(pCmd->name, pMgmt->db);
  pMsg += sizeof(SMgmtHead);

  pDropTableMsg = (SDropTableMsg *)pMsg;
  strcpy(pDropTableMsg->meterId, pCmd->name);

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
  if (pCmd->tagCond.len > 0) {
    strcpy(pMgmt->db, pCmd->tagCond.pData);
  } else {
    strcpy(pMgmt->db, pObj->db);
  }
  pMsg += sizeof(SMgmtHead);

  *((uint64_t *)pMsg) = pSql->res.qhandle;
  pMsg += 8;
  *pMsg = pCmd->type;
  pMsg += 1;

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
    pRes->numOfRows = 0;
    pRes->row = 0;
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
  SSqlCmd *pCmd = &pSql->cmd;
  int32_t  numOfRes = pCmd->pMeterMeta->numOfColumns + pCmd->pMeterMeta->numOfTags;

  return tscLocalResultCommonBuilder(pSql, numOfRes);
}

int tscProcessTagRetrieveRsp(SSqlObj *pSql) {
  SSqlCmd *pCmd = &pSql->cmd;
  int32_t  numOfRes = 0;
  if (tscSqlExprGet(pCmd, 0)->sqlFuncId == TSDB_FUNC_TAGPRJ) {
    numOfRes = pCmd->pMetricMeta->numOfMeters;
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

int tscProcessEmptyResultRsp(SSqlObj *pSql) {
  return tscLocalResultCommonBuilder(pSql, 0);
}

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
    // STagData is in binary format, strncpy is not available
    memcpy(tmpData, pSql->cmd.payload, pSql->cmd.allocSize);
  }

  SSqlCmd *pCmd = &pSql->cmd;
  pMsg = pCmd->payload + tsRpcHeadSize;
  pStart = pMsg;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  tscGetDBInfoFromMeterId(pCmd->name, pMgmt->db);

  pMsg += sizeof(SMgmtHead);

  pInfoMsg = (SMeterInfoMsg *)pMsg;
  strcpy(pInfoMsg->meterId, pCmd->name);
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

static int32_t tscEstimateMetricMetaMsgSize(SSqlCmd *pCmd) {
  const int32_t defaultSize =
      minMsgSize() + sizeof(SMetricMetaMsg) + sizeof(SMgmtHead) + sizeof(int16_t) * TSDB_MAX_TAGS;

  int32_t tagLen = pCmd->tagCond.len * TSDB_NCHAR_SIZE;
  if (tagLen + defaultSize > TSDB_DEFAULT_PAYLOAD_SIZE) {
    return tagLen + defaultSize;
  } else {
    return TSDB_DEFAULT_PAYLOAD_SIZE;
  }
}

int tscBuildMetricMetaMsg(SSqlObj *pSql) {
  SMetricMetaMsg *pMetaMsg;
  char *          pMsg, *pStart;
  int             msgLen = 0;

  SSqlCmd *pCmd = &pSql->cmd;

  int32_t size = tscEstimateMetricMetaMsgSize(pCmd);
  tscAllocPayloadWithSize(pCmd, size);

  pStart = pCmd->payload + tsRpcHeadSize;
  pMsg = pStart;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  tscGetDBInfoFromMeterId(pCmd->name, pMgmt->db);

  pMsg += sizeof(SMgmtHead);

  pMetaMsg = (SMetricMetaMsg *)pMsg;
  strcpy(pMetaMsg->meterId, pCmd->name);

  pMetaMsg->type = htons(pCmd->tagCond.type);
  pMetaMsg->condLength = htonl(pCmd->tagCond.len);

  if (pCmd->tagCond.len > 0) {
    /* convert to unicode before sending to mnode for metric query */
    bool ret = taosMbsToUcs4(tsGetMetricQueryCondPos(&pCmd->tagCond), pCmd->tagCond.len, (char *)pMetaMsg->tags,
                             pCmd->tagCond.len * TSDB_NCHAR_SIZE);
    if (!ret) {
      tscError("%p mbs to ucs4 failed:%s", pSql, tsGetMetricQueryCondPos(&pCmd->tagCond));
      return 0;
    }
  }

  pMsg += sizeof(SMetricMetaMsg);
  pMsg += pCmd->tagCond.len * TSDB_NCHAR_SIZE;

  SSqlGroupbyExpr *pGroupby = &pCmd->groupbyExpr;

  pMetaMsg->numOfTags = htons(pCmd->numOfReqTags);
  pMetaMsg->numOfGroupbyCols = htons(pGroupby->numOfGroupbyCols);

  for (int32_t j = 0; j < pCmd->numOfReqTags; ++j) {
    pMetaMsg->tagCols[j] = htons(pCmd->tagColumnIndex[j]);
  }

  if (pGroupby->numOfGroupbyCols != 0) {
    pMetaMsg->orderIndex = htons(pGroupby->orderIdx);
    pMetaMsg->orderType = htons(pGroupby->orderType);

    for (int32_t j = 0; j < pCmd->groupbyExpr.numOfGroupbyCols; ++j) {
      *((int16_t *)pMsg) = htons(pGroupby->tagIndex[j]);
      pMsg += sizeof(pCmd->groupbyExpr.tagIndex[j]);
    }
  }

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_METRIC_META;

  assert(msgLen + minMsgSize() <= size);
  return msgLen;
}

int tscEstimateBuildHeartBeatMsgLength(SSqlObj *pSql) {
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

  size = tscEstimateBuildHeartBeatMsgLength(pSql);
  tscAllocPayloadWithSize(pCmd, size);

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
  pMeta->sversion = htonl(pMeta->sversion);
  pMeta->vgid = htonl(pMeta->vgid);
  pMeta->uid = htobe64(pMeta->uid);

  if (pMeta->sid < 0 || pMeta->vgid < 0) {
    tscError("invalid meter vgid:%d, sid%d", pMeta->vgid, pMeta->sid);
    return TSDB_CODE_INVALID_VALUE;
  }

  pMeta->numOfColumns = htons(pMeta->numOfColumns);
  pMeta->numOfTags = htons(pMeta->numOfTags);
  pMeta->precision = htons(pMeta->precision);
  pMeta->meterType = htons(pMeta->meterType);

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
  SSchema *pTagsSchema = tsGetSchemaColIdx(pMeta, pMeta->numOfColumns);

  if (pMeta->meterType == TSDB_METER_MTABLE) {
    for (int32_t i = 0; i < pMeta->numOfTags; ++i) {
      tagLen += pTagsSchema[i].bytes;
    }
    pMeta->tags = sizeof(SMeterMeta) + numOfTotalCols * sizeof(SSchema);
  }

  rsp += tagLen;
  int32_t size = (int32_t)(rsp - (char *)pMeta);

  // pMeta->index = rand() % TSDB_VNODES_SUPPORT;
  pMeta->index = 0;

  // todo add one more function: taosAddDataIfNotExists();
  taosRemoveDataFromCache(tscCacheHandle, (void **)&(pSql->cmd.pMeterMeta), false);

  pSql->cmd.pMeterMeta =
      (SMeterMeta *)taosAddDataIntoCache(tscCacheHandle, pSql->cmd.name, (char *)pMeta, size, tsMeterMetaKeepTimer);
  if (pSql->cmd.pMeterMeta == NULL) return 0;

  return TSDB_CODE_OTHERS;
}

int tscProcessMetricMetaRsp(SSqlObj *pSql) {
  SMetricMeta *pMeta;
  uint8_t      ieType;
  char *       rsp = pSql->res.pRsp;

  ieType = *rsp;
  if (ieType != TSDB_IE_TYPE_META) {
    tscError("invalid ie type:%d", ieType);
    return TSDB_CODE_INVALID_IE;
  }

  rsp++;
  pMeta = (SMetricMeta *)rsp;
  size_t size = (size_t)pSql->res.rspLen - 1;
  rsp = rsp + sizeof(SMetricMeta);

  pMeta->numOfMeters = htonl(pMeta->numOfMeters);
  pMeta->numOfVnodes = htonl(pMeta->numOfVnodes);
  pMeta->tagLen = htons(pMeta->tagLen);

  size += pMeta->numOfVnodes * sizeof(SVnodeSidList *) + pMeta->numOfMeters * sizeof(SMeterSidExtInfo *);

  char *pStr = calloc(1, size);

  SMetricMeta *pNewMetricMeta = (SMetricMeta *)pStr;
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

  char name[TSDB_MAX_TAGS_LEN + 1] = {0};
  tscGetMetricMetaCacheKey(&pSql->cmd, name);

  /* release the used metricmeta */
  taosRemoveDataFromCache(tscCacheHandle, (void **)&(pSql->cmd.pMetricMeta), false);

  pSql->cmd.pMetricMeta =
      (SMetricMeta *)taosAddDataIntoCache(tscCacheHandle, name, (char *)pNewMetricMeta, size, tsMetricMetaKeepTimer);
  tfree(pNewMetricMeta);

  if (pSql->cmd.pMetricMeta == NULL) {
    return 0;
  }

  return TSDB_CODE_OTHERS;
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

  pShow = (SShowRspMsg *)pRes->pRsp;
  pRes->qhandle = pShow->qhandle;

  pRes->numOfRows = 0;
  pRes->row = 0;
  pMeta = &(pShow->meterMeta);

  pMeta->numOfColumns = ntohs(pMeta->numOfColumns);

  pSchema = (SSchema *)((char *)pMeta + sizeof(SMeterMeta));
  pMeta->sid = ntohs(pMeta->sid);
  for (int i = 0; i < pMeta->numOfColumns; ++i) {
    pSchema->bytes = htons(pSchema->bytes);
    pSchema++;
  }

  key[0] = pCmd->type + 'a';
  strcpy(key + 1, "showlist");

  taosRemoveDataFromCache(tscCacheHandle, (void *)&(pCmd->pMeterMeta), false);

  int32_t size = pMeta->numOfColumns * sizeof(SSchema) + sizeof(SMeterMeta);
  pCmd->pMeterMeta = (SMeterMeta *)taosAddDataIntoCache(tscCacheHandle, key, (char *)pMeta, size, tsMeterMetaKeepTimer);
  pCmd->numOfCols = pCmd->fieldsInfo.numOfOutputCols;
  SSchema *pMeterSchema = tsGetSchema(pCmd->pMeterMeta);

  tscColumnInfoReserve(pCmd, pMeta->numOfColumns);
  for (int i = 0; i < pMeta->numOfColumns; ++i) {
    tscColumnInfoInsert(pCmd, i);
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

  strcpy(pObj->sversion, pConnect->version);
  pObj->writeAuth = pConnect->writeAuth;
  pObj->superAuth = pConnect->superAuth;
  taosTmrReset(tscProcessActivityTimer, tsShellActivityTimer * 500, pObj, tscTmr, &pObj->pTimer);

  return 0;
}

int tscProcessUseDbRsp(SSqlObj *pSql) {
  STscObj *pObj = pSql->pTscObj;
  strcpy(pObj->db, pSql->cmd.name);
  return 0;
}

int tscProcessDropDbRsp(SSqlObj *UNUSED_PARAM(pSql)) {
  taosClearDataCache(tscCacheHandle);
  return 0;
}

int tscProcessDropTableRsp(SSqlObj *pSql) {
  SMeterMeta *pMeterMeta = taosGetDataFromCache(tscCacheHandle, pSql->cmd.name);
  if (pMeterMeta == NULL) {
    /* not in cache, abort */
    return 0;
  }

  /*
   * 1. if a user drops one table, which is the only table in a vnode, remove operation will incur vnode to be removed.
   * 2. Then, a user creates a new metric followed by a table with identical name of removed table but different schema,
   * here the table will reside in a new vnode.
   * The cached information is expired, however, we may have lost the ref of original meter. So, clear whole cache instead.
   */
  tscTrace("%p force release metermeta after drop table:%s", pSql, pSql->cmd.name);
  taosRemoveDataFromCache(tscCacheHandle, (void **)&pMeterMeta, true);

  if (pSql->cmd.pMeterMeta) {
    taosRemoveDataFromCache(tscCacheHandle, (void **)&(pSql->cmd.pMeterMeta), true);
    taosRemoveDataFromCache(tscCacheHandle, (void **)&(pSql->cmd.pMetricMeta), true);
  }

  return 0;
}

int tscProcessAlterTableMsgRsp(SSqlObj *pSql) {
  SMeterMeta *pMeterMeta = taosGetDataFromCache(tscCacheHandle, pSql->cmd.name);
  if (pMeterMeta == NULL) { /* not in cache, abort */
    return 0;
  }

  tscTrace("%p force release metermeta in cache after alter-table: %s", pSql, pSql->cmd.name);
  taosRemoveDataFromCache(tscCacheHandle, (void **)&pMeterMeta, true);

  if (pSql->cmd.pMeterMeta) {
    bool isMetric = UTIL_METER_IS_METRIC(&pSql->cmd);

    taosRemoveDataFromCache(tscCacheHandle, (void **)&(pSql->cmd.pMeterMeta), true);
    taosRemoveDataFromCache(tscCacheHandle, (void **)&(pSql->cmd.pMetricMeta), true);

    if (isMetric) {
      // here, the pCmd->pMeterMeta == NULL
      // if it is a metric, reset whole query cache
      tscTrace("%p reset query cache since table:%s is metric", pSql, pSql->cmd.name);
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
  pRes->numOfRows = 0;
  pRes->row = 0;
  pRes->data = NULL;
  return 0;
}

int tscProcessRetrieveRspFromVnode(SSqlObj *pSql) {
  SSqlRes *          pRes = &pSql->res;
  SSqlCmd *          pCmd = &pSql->cmd;
  STscObj *          pObj = pSql->pTscObj;
  SRetrieveMeterRsp *pRetrieve = (SRetrieveMeterRsp *)pRes->pRsp;

  pRes->numOfRows = htonl(pRetrieve->numOfRows);
  pRes->precision = htons(pRetrieve->precision);
  pRes->offset = htobe64(pRetrieve->offset);

  pRes->data = pRetrieve->data;
  pRes->useconds = pRetrieve->useconds;

  tscSetResultPointer(pCmd, pRes);
  pRes->row = 0;

  if (pRes->numOfRows == 0 && !(tscProjectionQueryOnMetric(pSql) && pRes->offset > 0)) {
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

static int32_t tscDoGetMeterMeta(SSqlObj *pSql, char *meterId) {
  int32_t code = TSDB_CODE_SUCCESS;

  SSqlObj *pNew = malloc(sizeof(SSqlObj));
  memset(pNew, 0, sizeof(SSqlObj));
  pNew->pTscObj = pSql->pTscObj;
  pNew->signature = pNew;
  pNew->cmd.command = TSDB_SQL_META;
  pNew->cmd.payload = NULL;
  pNew->cmd.allocSize = 0;
  pNew->cmd.defaultVal[0] = pSql->cmd.defaultVal[0];  // flag of create table if not exists
  tscAllocPayloadWithSize(&pNew->cmd, TSDB_DEFAULT_PAYLOAD_SIZE);

  strcpy(pNew->cmd.name, meterId);
  memcpy(pNew->cmd.payload, pSql->cmd.payload, TSDB_DEFAULT_PAYLOAD_SIZE);
  tscTrace("%p new pSqlObj:%p to get meterMeta", pSql, pNew);

  if (pSql->fp == NULL) {
    sem_init(&pNew->rspSem, 0, 0);
    sem_init(&pNew->emptyRspSem, 0, 1);

    code = tscProcessSql(pNew);
    if (code == TSDB_CODE_SUCCESS) {
      /* update cache only on success get metermeta */
      assert(pSql->cmd.pMeterMeta == NULL);
      pSql->cmd.pMeterMeta = (SMeterMeta *)taosGetDataFromCache(tscCacheHandle, meterId);
    }

    tscTrace("%p get meter meta complete, code:%d, pMeterMeta:%p", pSql, code, pSql->cmd.pMeterMeta);
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

int tscGetMeterMeta(SSqlObj *pSql, char *meterId) {
  SSqlCmd *pCmd = &pSql->cmd;

  /* if the SSqlCmd owns a metermeta, release it first */
  taosRemoveDataFromCache(tscCacheHandle, (void **)&(pCmd->pMeterMeta), false);
  pCmd->pMeterMeta = (SMeterMeta *)taosGetDataFromCache(tscCacheHandle, meterId);
  if (pCmd->pMeterMeta != NULL) {
    tscTrace("%p the number of columns:%d, numOfTags:%d, addr:%p", pSql, pCmd->pMeterMeta->numOfColumns,
             pCmd->pMeterMeta->numOfTags, pCmd->pMeterMeta);
    return TSDB_CODE_SUCCESS;
  }

  /*
   * for async insert operation, release data block buffer before issue new object to get metermeta
   * because in metermeta callback function, the tscParse function will generate the submit data blocks
   */
  if (pSql->fp != NULL && pSql->pStream == NULL) {
    tscfreeSqlCmdData(pCmd);
  }

  return tscDoGetMeterMeta(pSql, meterId);
}

int tscGetMeterMetaEx(SSqlObj *pSql, char *meterId, bool createIfNotExists) {
  pSql->cmd.defaultVal[0] = createIfNotExists ? 1 : 0;
  return tscGetMeterMeta(pSql, meterId);
}

/*
 * in handling the renew metermeta problem during insertion,
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

  // enforce the renew metermeta operation in async model
  if (pSql->fp == NULL) pSql->fp = (void *)0x1;

  /*
   * 1. nly update the metermeta in force model metricmeta is not updated
   * 2. if get metermeta failed, still get the metermeta
   */
  if (pCmd->pMeterMeta == NULL || !tscQueryOnMetric(pCmd)) {
    if (pCmd->pMeterMeta) {
      tscTrace("%p update meter meta, old: numOfTags:%d, numOfCols:%d, uid:%d, addr:%p",
               pSql, pCmd->pMeterMeta->numOfTags, pCmd->numOfCols, pCmd->pMeterMeta->uid, pCmd->pMeterMeta);
    }

    tscWaitingForCreateTable(&pSql->cmd);
    taosRemoveDataFromCache(tscCacheHandle, (void **)&(pSql->cmd.pMeterMeta), true);

    code = tscDoGetMeterMeta(pSql, meterId);
  } else {
    tscTrace("%p metric query not update metric meta, numOfTags:%d, numOfCols:%d, uid:%d, addr:%p",
             pSql, pCmd->pMeterMeta->numOfTags, pCmd->numOfCols, pCmd->pMeterMeta->uid, pCmd->pMeterMeta);
  }

  if (code != TSDB_CODE_ACTION_IN_PROGRESS) {
    if (pSql->fp == (void *)0x1) {
      pSql->fp = NULL;
    }
  }

  return code;
}

int tscGetMetricMeta(SSqlObj *pSql, char *meterId) {
  int  code = TSDB_CODE_NETWORK_UNAVAIL;
  char tagstr[TSDB_MAX_TAGS_LEN + 1] = {0};

  /*
   * the vnode query condition is serialized into pCmd->payload, we need to rebuild key for metricmeta info in cache.
   */
  tscGetMetricMetaCacheKey(&pSql->cmd, tagstr);
  taosRemoveDataFromCache(tscCacheHandle, (void **)&(pSql->cmd.pMetricMeta), false);

  SMetricMeta *ppMeta = (SMetricMeta *)taosGetDataFromCache(tscCacheHandle, tagstr);
  if (ppMeta != NULL) {
    pSql->cmd.pMetricMeta = ppMeta;
    return TSDB_CODE_SUCCESS;
  }

  SSqlObj *pNew = malloc(sizeof(SSqlObj));
  memset(pNew, 0, sizeof(SSqlObj));
  pNew->pTscObj = pSql->pTscObj;
  pNew->signature = pNew;

  pNew->cmd.command = TSDB_SQL_METRIC;
  strcpy(pNew->cmd.name, meterId);
  tscAllocPayloadWithSize(&pNew->cmd, TSDB_DEFAULT_PAYLOAD_SIZE);

  // the query condition on meter is serialized into payload
  tscTagCondAssign(&pNew->cmd.tagCond, &pSql->cmd.tagCond);

  pNew->cmd.groupbyExpr = pSql->cmd.groupbyExpr;

  pNew->cmd.glimit = pSql->cmd.glimit;
  pNew->cmd.order = pSql->cmd.order;
  pNew->cmd.numOfReqTags = pSql->cmd.numOfReqTags;

  memcpy(pNew->cmd.tagColumnIndex, pSql->cmd.tagColumnIndex, sizeof(pSql->cmd.tagColumnIndex));

  if (pSql->fp != NULL && pSql->pStream == NULL) {
    tscfreeSqlCmdData(&pSql->cmd);
  }

  tscTrace("%p allocate new pSqlObj:%p to get metricMeta", pSql, pNew);
  if (pSql->fp == NULL) {
    sem_init(&pNew->rspSem, 0, 0);
    sem_init(&pNew->emptyRspSem, 0, 1);

    code = tscProcessSql(pNew);
    pSql->cmd.pMetricMeta = taosGetDataFromCache(tscCacheHandle, tagstr);
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
  tscBuildMsg[TSDB_SQL_CREATE_PNODE] = tscBuildCreateDnodeMsg;
  tscBuildMsg[TSDB_SQL_DROP_PNODE] = tscBuildDropDnodeMsg;
  tscBuildMsg[TSDB_SQL_CFG_PNODE] = tscBuildCfgDnodeMsg;
  tscBuildMsg[TSDB_SQL_ALTER_TABLE] = tscBuildAlterTableMsg;
  tscBuildMsg[TSDB_SQL_ALTER_DB] = tscAlterDbMsg;

  tscBuildMsg[TSDB_SQL_CONNECT] = tscBuildConnectMsg;
  tscBuildMsg[TSDB_SQL_USE_DB] = tscBuildUseDbMsg;
  tscBuildMsg[TSDB_SQL_META] = tscBuildMeterMetaMsg;
  tscBuildMsg[TSDB_SQL_METRIC] = tscBuildMetricMetaMsg;

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

  tscProcessMsgRsp[TSDB_SQL_SHOW] = tscProcessShowRsp;
  tscProcessMsgRsp[TSDB_SQL_RETRIEVE] = tscProcessRetrieveRspFromMgmt;
  tscProcessMsgRsp[TSDB_SQL_DESCRIBE_TABLE] = tscProcessDescribeTableRsp;
  tscProcessMsgRsp[TSDB_SQL_RETRIEVE_TAGS] = tscProcessTagRetrieveRsp;
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
