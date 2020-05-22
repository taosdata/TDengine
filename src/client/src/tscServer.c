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
#include "tscProfile.h"
#include "tscSecondaryMerge.h"
#include "tscSubquery.h"
#include "tscUtil.h"
#include "tschemautil.h"
#include "tsclient.h"
#include "tsocket.h"
#include "ttime.h"
#include "ttimer.h"
#include "tutil.h"
#include "tscLog.h"
#include "qsqltype.h"

#define TSC_MGMT_VNODE 999

SRpcIpSet  tscMgmtIpSet;
SRpcIpSet  tscDnodeIpSet;

int (*tscBuildMsg[TSDB_SQL_MAX])(SSqlObj *pSql, SSqlInfo *pInfo) = {0};

int (*tscProcessMsgRsp[TSDB_SQL_MAX])(SSqlObj *pSql);
void tscProcessActivityTimer(void *handle, void *tmrId);
int tscKeepConn[TSDB_SQL_MAX] = {0};

TSKEY tscGetSubscriptionProgress(void* sub, int64_t uid, TSKEY dflt);
void tscUpdateSubscriptionProgress(void* sub, int64_t uid, TSKEY ts);
void tscSaveSubscriptionProgress(void* sub);

static int32_t minMsgSize() { return tsRpcHeadSize + 100; }

static void tscSetDnodeIpList(SSqlObj* pSql, SCMVgroupInfo* pVgroupInfo) {
  SRpcIpSet* pIpList = &pSql->ipList;
  
  pIpList->numOfIps = pVgroupInfo->numOfIps;
  pIpList->inUse    = 0;
  
  for(int32_t i = 0; i < pVgroupInfo->numOfIps; ++i) {
    strcpy(pIpList->fqdn[i], pVgroupInfo->ipAddr[i].fqdn);
    pIpList->port[i] = pVgroupInfo->ipAddr[i].port;
  }
}

void tscPrintMgmtIp() {
  if (tscMgmtIpSet.numOfIps <= 0) {
    tscError("invalid mgmt IP list:%d", tscMgmtIpSet.numOfIps);
  } else {
    for (int i = 0; i < tscMgmtIpSet.numOfIps; ++i) {
      tscTrace("mgmt index:%d %s:%d", i, tscMgmtIpSet.fqdn[i], tscMgmtIpSet.port[i]);
    }
  }
}

void tscSetMgmtIpList(SRpcIpSet *pIpList) {
  tscMgmtIpSet.numOfIps = pIpList->numOfIps;
  tscMgmtIpSet.inUse = pIpList->inUse;
  for (int32_t i = 0; i < tscMgmtIpSet.numOfIps; ++i) {
    tscMgmtIpSet.port[i] = htons(pIpList->port[i]);
  }
}

void tscUpdateIpSet(void *ahandle, SRpcIpSet *pIpSet) {
  tscMgmtIpSet = *pIpSet;
  tscTrace("mgmt IP list is changed for ufp is called, numOfIps:%d inUse:%d", tscMgmtIpSet.numOfIps, tscMgmtIpSet.inUse);
  for (int32_t i = 0; i < tscMgmtIpSet.numOfIps; ++i) {
    tscTrace("index:%d fqdn:%s port:%d", i, tscMgmtIpSet.fqdn[i], tscMgmtIpSet.port[i]);
  }
}

/*
 * For each management node, try twice at least in case of poor network situation.
 * If the client start to connect to a non-management node from the client, and the first retry may fail due to
 * the poor network quality. And then, the second retry get the response with redirection command.
 * The retry will not be executed since only *two* retry is allowed in case of single management node in the cluster.
 * Therefore, we need to multiply the retry times by factor of 2 to fix this problem.
 */
UNUSED_FUNC
static int32_t tscGetMgmtConnMaxRetryTimes() {
  int32_t factor = 2;
  return tscMgmtIpSet.numOfIps * factor;
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
    SCMHeartBeatRsp *pRsp = (SCMHeartBeatRsp *)pRes->pRsp;
    SRpcIpSet *      pIpList = &pRsp->ipList;
    if (pIpList->numOfIps > 0) 
      tscSetMgmtIpList(pIpList);

    if (pRsp->killConnection) {
      tscKillConnection(pObj);
    } else {
      if (pRsp->queryId) tscKillQuery(pObj, htonl(pRsp->queryId));
      if (pRsp->streamId) tscKillStream(pObj, htonl(pRsp->streamId));
    }
  } else {
    tscTrace("heart beat failed, code:%s", tstrerror(code));
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
    
    SQueryInfo *pQueryInfo = NULL;
    tscGetQueryInfoDetailSafely(&pSql->cmd, 0, &pQueryInfo);
    pQueryInfo->command = TSDB_SQL_HB;
    
    pSql->cmd.command = TSDB_SQL_HB;
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
    tscTrace("%p free HB object and release connection", pObj);
    tscFreeSqlObj(pObj->pHb);
    tscCloseTscObj(pObj);
    return;
  }

  tscProcessSql(pObj->pHb);
}

int tscSendMsgToServer(SSqlObj *pSql) {
  STscObj* pObj = pSql->pTscObj;
  SSqlCmd* pCmd = &pSql->cmd;
  
  char *pMsg = rpcMallocCont(pCmd->payloadLen);
  if (NULL == pMsg) {
    tscError("%p msg:%s malloc fail", pSql, taosMsg[pSql->cmd.msgType]);
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }

  if (pSql->cmd.command < TSDB_SQL_MGMT) {
    memcpy(pMsg, pSql->cmd.payload + tsRpcHeadSize, pSql->cmd.payloadLen);
  } else {
    pSql->ipList = tscMgmtIpSet;
    memcpy(pMsg, pSql->cmd.payload, pSql->cmd.payloadLen);
  }

  // tscTrace("%p msg:%s is sent to server", pSql, taosMsg[pSql->cmd.msgType]);

  SRpcMsg rpcMsg = {
      .msgType = pSql->cmd.msgType,
      .pCont   = pMsg,
      .contLen = pSql->cmd.payloadLen,
      .handle  = pSql,
      .code    = 0
  };
  rpcSendRequest(pObj->pDnodeConn, &pSql->ipList, &rpcMsg);

  return TSDB_CODE_SUCCESS;
}

void tscProcessMsgFromServer(SRpcMsg *rpcMsg, SRpcIpSet *pIpSet) {
  SSqlObj *pSql = (SSqlObj *)rpcMsg->handle;
  if (pSql == NULL) {
    tscError("%p sql is already released", pSql->signature);
    return;
  }
  if (pSql->signature != pSql) {
    tscError("%p sql is already released, signature:%p", pSql, pSql->signature);
    return;
  }

  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;
  STscObj *pObj = pSql->pTscObj;
  // tscTrace("%p msg:%s is received from server", pSql, taosMsg[rpcMsg->msgType]);

  if (pSql->freed || pObj->signature != pObj) {
    tscTrace("%p sql is already released or DB connection is closed, freed:%d pObj:%p signature:%p", pSql, pSql->freed,
             pObj, pObj->signature);
    tscFreeSqlObj(pSql);
    rpcFreeCont(rpcMsg->pCont);
    return;
  }

  if (pCmd->command < TSDB_SQL_MGMT) {
    if (pIpSet) pSql->ipList = *pIpSet;
  } else {
    if (pIpSet) tscMgmtIpSet = *pIpSet;
  }

  if (rpcMsg->pCont == NULL) {
    rpcMsg->code = TSDB_CODE_NETWORK_UNAVAIL;
  } else {
    STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
    if (rpcMsg->code == TSDB_CODE_NOT_ACTIVE_TABLE || rpcMsg->code == TSDB_CODE_INVALID_TABLE_ID ||
        rpcMsg->code == TSDB_CODE_INVALID_VNODE_ID || rpcMsg->code == TSDB_CODE_NOT_ACTIVE_VNODE ||
        rpcMsg->code == TSDB_CODE_NETWORK_UNAVAIL || rpcMsg->code == TSDB_CODE_NOT_ACTIVE_TABLE ||
        rpcMsg->code == TSDB_CODE_TABLE_ID_MISMATCH) {
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
      if (pCmd->command == TSDB_SQL_CONNECT) {
        rpcMsg->code = TSDB_CODE_NETWORK_UNAVAIL;
        rpcFreeCont(rpcMsg->pCont);
        return;
      } else if (pCmd->command == TSDB_SQL_HB) {
        rpcMsg->code = TSDB_CODE_NOT_READY;
        rpcFreeCont(rpcMsg->pCont);
        return;
      } else {
        tscWarn("%p it shall renew table meta, code:%s, retry:%d", pSql, tstrerror(rpcMsg->code), ++pSql->retry);
        
        pSql->res.code = rpcMsg->code;  // keep the previous error code
        if (pSql->retry > pSql->maxRetry) {
          tscError("%p max retry %d reached, give up", pSql, pSql->maxRetry);
        } else {
          rpcMsg->code = tscRenewMeterMeta(pSql, pTableMetaInfo->name);
          if (pTableMetaInfo->pTableMeta) {
            tscSendMsgToServer(pSql);
          }
  
          rpcFreeCont(rpcMsg->pCont);
          return;
        }
      }
    }
  }
  
  pRes->rspLen = 0;
  
  if (pRes->code != TSDB_CODE_QUERY_CANCELLED) {
    pRes->code = (rpcMsg->code != TSDB_CODE_SUCCESS) ? rpcMsg->code : TSDB_CODE_NETWORK_UNAVAIL;
  } else {
    tscTrace("%p query is cancelled, code:%d", pSql, tstrerror(pRes->code));
  }

  if (pRes->code == TSDB_CODE_SUCCESS) {
    tscTrace("%p reset retry counter to be 0 due to success rsp, old:%d", pSql, pSql->retry);
    pSql->retry = 0;
  }

  if (pRes->code != TSDB_CODE_QUERY_CANCELLED) {
    assert(rpcMsg->msgType == pCmd->msgType + 1);
    pRes->code    = rpcMsg->code;
    pRes->rspType = rpcMsg->msgType;
    pRes->rspLen  = rpcMsg->contLen;

    if (pRes->rspLen > 0 && rpcMsg->pCont) {
      char *tmp = (char *)realloc(pRes->pRsp, pRes->rspLen);
      if (tmp == NULL) {
        pRes->code = TSDB_CODE_CLI_OUT_OF_MEMORY;
      } else {
        pRes->pRsp = tmp;
        memcpy(pRes->pRsp, rpcMsg->pCont, pRes->rspLen);
      }
    } else {
      pRes->pRsp = NULL;
    }

    /*
     * There is not response callback function for submit response.
     * The actual inserted number of points is the first number.
     */
    if (rpcMsg->msgType == TSDB_MSG_TYPE_SUBMIT_RSP && pRes->pRsp != NULL) {
      SShellSubmitRspMsg *pMsg = (SShellSubmitRspMsg*)pRes->pRsp;
      pMsg->code = htonl(pMsg->code);
      pMsg->numOfRows = htonl(pMsg->numOfRows);
      pMsg->affectedRows = htonl(pMsg->affectedRows);
      pMsg->failedRows = htonl(pMsg->failedRows);
      pMsg->numOfFailedBlocks = htonl(pMsg->numOfFailedBlocks);

      pRes->numOfRows += pMsg->affectedRows;
      tscTrace("%p SQL cmd:%s, code:%s inserted rows:%d rspLen:%d", pSql, sqlCmd[pCmd->command], 
          tstrerror(pRes->code), pMsg->affectedRows, pRes->rspLen);
    } else {
      tscTrace("%p SQL cmd:%s, code:%s rspLen:%d", pSql, sqlCmd[pCmd->command], tstrerror(pRes->code), pRes->rspLen);
    }
  }
  
  if (pRes->code == TSDB_CODE_SUCCESS && tscProcessMsgRsp[pCmd->command])
    rpcMsg->code = (*tscProcessMsgRsp[pCmd->command])(pSql);
  
  if (rpcMsg->code != TSDB_CODE_ACTION_IN_PROGRESS) {
    void *taosres = tscKeepConn[pCmd->command] ? pSql : NULL;
    rpcMsg->code = pRes->code ? pRes->code : pRes->numOfRows;
    
    tscTrace("%p SQL result:%s res:%p", pSql, tstrerror(pRes->code), pSql);

    /*
     * Whether to free sqlObj or not should be decided before call the user defined function, since this SqlObj
     * may be freed in UDF, and reused by other threads before tscShouldBeFreed called, in which case
     * tscShouldBeFreed checks an object which is actually allocated by other threads.
     *
     * If this block of memory is re-allocated for an insert thread, in which tscKeepConn[command] equals to 0,
     * the tscShouldBeFreed will success and tscFreeSqlObj free it immediately.
     */
    bool shouldFree = tscShouldBeFreed(pSql);
    (*pSql->fp)(pSql->param, taosres, rpcMsg->code);

    if (shouldFree) {
      tscTrace("%p sqlObj is automatically freed", pSql);
      tscFreeSqlObj(pSql);
    }
  }

  rpcFreeCont(rpcMsg->pCont);
}

int doProcessSql(SSqlObj *pSql) {
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;
  int32_t code = TSDB_CODE_SUCCESS;
  
  if (pCmd->command == TSDB_SQL_SELECT ||
      pCmd->command == TSDB_SQL_FETCH ||
      pCmd->command == TSDB_SQL_RETRIEVE ||
      pCmd->command == TSDB_SQL_INSERT ||
      pCmd->command == TSDB_SQL_CONNECT ||
      pCmd->command == TSDB_SQL_HB ||
      pCmd->command == TSDB_SQL_META ||
      pCmd->command == TSDB_SQL_STABLEVGROUP) {
    pRes->code = tscBuildMsg[pCmd->command](pSql, NULL);
  }
  
  if (pRes->code != TSDB_CODE_SUCCESS) {
    tscQueueAsyncRes(pSql);
    return pRes->code;
  }

  code = tscSendMsgToServer(pSql);
  if (code != TSDB_CODE_SUCCESS) {
    pRes->code = code;
    tscQueueAsyncRes(pSql);
  }
  
  return TSDB_CODE_SUCCESS;
}

int tscProcessSql(SSqlObj *pSql) {
  char *   name = NULL;
  SSqlCmd *pCmd = &pSql->cmd;
  
  SQueryInfo *    pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  STableMetaInfo *pTableMetaInfo = NULL;
  uint16_t        type = 0;

  if (pQueryInfo != NULL) {
    pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
    if (pTableMetaInfo != NULL) {
      name = pTableMetaInfo->name;
    }

    type = pQueryInfo->type;
  
    // while numOfTables equals to 0, it must be Heartbeat
    assert((pQueryInfo->numOfTables == 0 && pQueryInfo->command == TSDB_SQL_HB) || pQueryInfo->numOfTables > 0);
  }

  tscTrace("%p SQL cmd:%s will be processed, name:%s, type:%d", pSql, sqlCmd[pCmd->command], name, type);
  if (pCmd->command < TSDB_SQL_MGMT) { // the pTableMetaInfo cannot be NULL
    if (pTableMetaInfo == NULL) {
      pSql->res.code = TSDB_CODE_OTHERS;
      return pSql->res.code;
    }
  } else if (pCmd->command < TSDB_SQL_LOCAL) {
    pSql->ipList = tscMgmtIpSet; //?
  } else {  // local handler
    return (*tscProcessMsgRsp[pCmd->command])(pSql);
  }
  
  return doProcessSql(pSql);
}

void tscKillSTableQuery(SSqlObj *pSql) {
  SSqlCmd* pCmd = &pSql->cmd;
  
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  if (!tscIsTwoStageSTableQuery(pQueryInfo, 0)) {
    return;
  }

  for (int i = 0; i < pSql->numOfSubs; ++i) {
    SSqlObj *pSub = pSql->pSubs[i];

    if (pSub == NULL) {
      continue;
    }

    /*
     * here, we cannot set the command = TSDB_SQL_KILL_QUERY. Otherwise, it may cause
     * sub-queries not correctly released and master sql object of metric query reaches an abnormal state.
     */
    pSql->pSubs[i]->res.code = TSDB_CODE_QUERY_CANCELLED;
    //taosStopRpcConn(pSql->pSubs[i]->thandle);
  }

  /*
   * 1. if the subqueries are not launched or partially launched, we need to waiting the launched
   * query return to successfully free allocated resources.
   * 2. if no any subqueries are launched yet, which means the metric query only in parse sql stage,
   * set the res.code, and return.
   */
  const int64_t MAX_WAITING_TIME = 10000;  // 10 Sec.
  int64_t       stime = taosGetTimestampMs();

  while (pCmd->command != TSDB_SQL_RETRIEVE_LOCALMERGE && pCmd->command != TSDB_SQL_RETRIEVE_EMPTY_RESULT) {
    taosMsleep(100);
    if (taosGetTimestampMs() - stime > MAX_WAITING_TIME) {
      break;
    }
  }

  tscTrace("%p super table query cancelled", pSql);
}

int tscBuildFetchMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  char *pMsg, *pStart;

  pStart = pSql->cmd.payload + tsRpcHeadSize;
  pMsg = pStart;

  SRetrieveTableMsg *pRetrieveMsg = (SRetrieveTableMsg *)pMsg;
  pRetrieveMsg->qhandle = htobe64(pSql->res.qhandle);
  pMsg += sizeof(pSql->res.qhandle);

  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  pRetrieveMsg->free = htons(pQueryInfo->type);
  pMsg += sizeof(pQueryInfo->type);

  // todo valid the vgroupId at the client side
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  
  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    int32_t vgIndex = pTableMetaInfo->vgroupIndex;
    
    SVgroupsInfo* pVgroupInfo = pTableMetaInfo->vgroupList;
    pRetrieveMsg->header.vgId = htonl(pVgroupInfo->vgroups[vgIndex].vgId);
  } else {
    STableMeta* pTableMeta = pTableMetaInfo->pTableMeta;
    pRetrieveMsg->header.vgId = htonl(pTableMeta->vgroupInfo.vgId);
  }
  
  pMsg += sizeof(SRetrieveTableMsg);
  
  pRetrieveMsg->header.contLen = htonl(pSql->cmd.payloadLen);
  
  pSql->cmd.msgType = TSDB_MSG_TYPE_FETCH;
  return TSDB_CODE_SUCCESS;
}

int tscBuildSubmitMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  STableMeta* pTableMeta = tscGetMetaInfo(pQueryInfo, 0)->pTableMeta;
  
  char* pMsg = pSql->cmd.payload + tsRpcHeadSize;
  
  // NOTE: shell message size should not include SMsgDesc
  int32_t size = pSql->cmd.payloadLen - sizeof(SMsgDesc);
  
  SMsgDesc* pMsgDesc = (SMsgDesc*) pMsg;
  
  pMsgDesc->numOfVnodes = htonl(1);       //todo set the right number of vnodes
  pMsg += sizeof(SMsgDesc);
  
  SSubmitMsg *pShellMsg = (SSubmitMsg *)pMsg;
  int32_t vgId = pTableMeta->vgroupInfo.vgId;
  
  pShellMsg->header.vgId = htonl(vgId);
  pShellMsg->header.contLen = htonl(size);
  pShellMsg->length = pShellMsg->header.contLen;
  
  pShellMsg->numOfBlocks = htonl(pSql->cmd.numOfTablesInSubmit);  // number of meters to be inserted

  // pSql->cmd.payloadLen is set during copying data into payload
  pSql->cmd.msgType = TSDB_MSG_TYPE_SUBMIT;
  tscSetDnodeIpList(pSql, &pTableMeta->vgroupInfo);
  
  tscTrace("%p build submit msg, vgId:%d numOfVgroup:%d numberOfIP:%d", pSql, vgId, htonl(pMsgDesc->numOfVnodes), pSql->ipList.numOfIps);
  return TSDB_CODE_SUCCESS;
}

/*
 * for table query, simply return the size <= 1k
 */
static int32_t tscEstimateQueryMsgSize(SSqlCmd *pCmd, int32_t clauseIndex) {
  const static int32_t MIN_QUERY_MSG_PKT_SIZE = TSDB_MAX_BYTES_PER_ROW * 5;
  SQueryInfo *         pQueryInfo = tscGetQueryInfoDetail(pCmd, clauseIndex);

  int32_t srcColListSize = taosArrayGetSize(pQueryInfo->colList) * sizeof(SColumnInfo);
  
  size_t numOfExprs = tscSqlExprNumOfExprs(pQueryInfo);
  int32_t exprSize = sizeof(SSqlFuncMsg) * numOfExprs;
  
  return MIN_QUERY_MSG_PKT_SIZE + minMsgSize() + sizeof(SQueryTableMsg) + srcColListSize + exprSize + 4096;
}

static char *doSerializeTableInfo(SQueryTableMsg* pQueryMsg, SSqlObj *pSql, char *pMsg) {
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pSql->cmd, pSql->cmd.clauseIndex, 0);
  TSKEY dfltKey = htobe64(pQueryMsg->window.skey);

  STableMeta * pTableMeta = pTableMetaInfo->pTableMeta;
  if (UTIL_TABLE_IS_NORMAL_TABLE(pTableMetaInfo) || pTableMetaInfo->pVgroupTables == NULL) {
    
    SCMVgroupInfo* pVgroupInfo = NULL;
    if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
      int32_t index = pTableMetaInfo->vgroupIndex;
      assert(index >= 0);
  
      pVgroupInfo = &pTableMetaInfo->vgroupList->vgroups[index];
      tscTrace("%p query on stable, vgIndex:%d, numOfVgroups:%d", pSql, index, pTableMetaInfo->vgroupList->numOfVgroups);
    } else {
      pVgroupInfo = &pTableMeta->vgroupInfo;
    }

    tscSetDnodeIpList(pSql, pVgroupInfo);
    pQueryMsg->head.vgId = htonl(pVgroupInfo->vgId);

    STableIdInfo *pTableIdInfo = (STableIdInfo *)pMsg;
    pTableIdInfo->tid = htonl(pTableMeta->sid);
    pTableIdInfo->uid = htobe64(pTableMeta->uid);
    pTableIdInfo->key = htobe64(tscGetSubscriptionProgress(pSql->pSubscription, pTableMeta->uid, dfltKey));

    pQueryMsg->numOfTables = htonl(1);  // set the number of tables

    pMsg += sizeof(STableIdInfo);
  } else {
    int32_t index = pTableMetaInfo->vgroupIndex;
    int32_t numOfVgroups = taosArrayGetSize(pTableMetaInfo->pVgroupTables);
    assert(index >= 0 && index < numOfVgroups);

    tscTrace("%p query on stable, vgIndex:%d, numOfVgroups:%d", pSql, index, numOfVgroups);

    SVgroupTableInfo* pTableIdList = taosArrayGet(pTableMetaInfo->pVgroupTables, index);
    
    // set the vgroup info
    tscSetDnodeIpList(pSql, &pTableIdList->vgInfo);
    pQueryMsg->head.vgId = htonl(pTableIdList->vgInfo.vgId);
    
    int32_t numOfTables = taosArrayGetSize(pTableIdList->itemList);
    pQueryMsg->numOfTables = htonl(numOfTables);  // set the number of tables
  
    // serialize each table id info
    for(int32_t i = 0; i < numOfTables; ++i) {
      STableIdInfo* pItem = taosArrayGet(pTableIdList->itemList, i);
      
      STableIdInfo *pTableIdInfo = (STableIdInfo *)pMsg;
      pTableIdInfo->tid = htonl(pItem->tid);
      pTableIdInfo->uid = htobe64(pItem->uid);
      pTableIdInfo->key = htobe64(tscGetSubscriptionProgress(pSql->pSubscription, pItem->uid, dfltKey));
      pMsg += sizeof(STableIdInfo);
    }
  }
  
  tscTrace("%p vgId:%d, query on table:%s, uid:%" PRIu64, pSql, htonl(pQueryMsg->head.vgId), pTableMetaInfo->name,
      pTableMeta->uid);
  
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
  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  STableMeta * pTableMeta = pTableMetaInfo->pTableMeta;
  
  if (taosArrayGetSize(pQueryInfo->colList) <= 0 && !tscQueryTags(pQueryInfo)) {
    tscError("%p illegal value of numOfCols in query msg: %d", pSql, tscGetNumOfColumns(pTableMeta));
    return -1;
  }
  
  if (pQueryInfo->intervalTime < 0) {
    tscError("%p illegal value of aggregation time interval in query msg: %ld", pSql, pQueryInfo->intervalTime);
    return -1;
  }
  
  if (pQueryInfo->groupbyExpr.numOfGroupCols < 0) {
    tscError("%p illegal value of numOfGroupCols in query msg: %d", pSql, pQueryInfo->groupbyExpr.numOfGroupCols);
    return -1;
  }

  char *pStart = pCmd->payload + tsRpcHeadSize;

  SQueryTableMsg *pQueryMsg = (SQueryTableMsg *)pStart;

  int32_t numOfTags = taosArrayGetSize(pTableMetaInfo->tagColList);
  
  if (pQueryInfo->order.order == TSDB_ORDER_ASC) {
    pQueryMsg->window.skey = htobe64(pQueryInfo->window.skey);
    pQueryMsg->window.ekey = htobe64(pQueryInfo->window.ekey);
  } else {
    pQueryMsg->window.skey = htobe64(pQueryInfo->window.ekey);
    pQueryMsg->window.ekey = htobe64(pQueryInfo->window.skey);
  }

  pQueryMsg->order          = htons(pQueryInfo->order.order);
  pQueryMsg->orderColId     = htons(pQueryInfo->order.orderColId);
  pQueryMsg->fillType    = htons(pQueryInfo->fillType);
  pQueryMsg->limit          = htobe64(pQueryInfo->limit.limit);
  pQueryMsg->offset         = htobe64(pQueryInfo->limit.offset);
  pQueryMsg->numOfCols      = htons(taosArrayGetSize(pQueryInfo->colList));
  pQueryMsg->intervalTime   = htobe64(pQueryInfo->intervalTime);
  pQueryMsg->slidingTime    = htobe64(pQueryInfo->slidingTime);
  pQueryMsg->slidingTimeUnit = pQueryInfo->slidingTimeUnit;
  pQueryMsg->numOfGroupCols = htons(pQueryInfo->groupbyExpr.numOfGroupCols);
  pQueryMsg->numOfTags      = htonl(numOfTags);
  pQueryMsg->tagNameRelType = htons(pQueryInfo->tagCond.relType);
  pQueryMsg->queryType      = htons(pQueryInfo->type);
  
  size_t numOfOutput = tscSqlExprNumOfExprs(pQueryInfo);
  pQueryMsg->numOfOutput = htons(numOfOutput);
  if (numOfOutput < 0) {
    tscError("%p illegal value of number of output columns in query msg: %d", pSql, numOfOutput);
    return -1;
  }

  // set column list ids
  size_t numOfCols = taosArrayGetSize(pQueryInfo->colList);
  char *pMsg = (char *)(pQueryMsg->colList) + numOfCols * sizeof(SColumnInfo);
  SSchema *pSchema = tscGetTableSchema(pTableMeta);
  
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumn *pCol = taosArrayGetP(pQueryInfo->colList, i);
    SSchema *pColSchema = &pSchema[pCol->colIndex.columnIndex];

    if (pCol->colIndex.columnIndex >= tscGetNumOfColumns(pTableMeta) || pColSchema->type < TSDB_DATA_TYPE_BOOL ||
        pColSchema->type > TSDB_DATA_TYPE_NCHAR) {
      tscError("%p sid:%d uid:%" PRIu64" id:%s, column index out of range, numOfColumns:%d, index:%d, column name:%s",
          pSql, pTableMeta->sid, pTableMeta->uid, pTableMetaInfo->name, tscGetNumOfColumns(pTableMeta), pCol->colIndex,
               pColSchema->name);

      return TSDB_CODE_INVALID_SQL;
    }

    pQueryMsg->colList[i].colId = htons(pColSchema->colId);
    pQueryMsg->colList[i].bytes = htons(pColSchema->bytes);
    pQueryMsg->colList[i].type  = htons(pColSchema->type);
    pQueryMsg->colList[i].numOfFilters = htons(pCol->numOfFilters);

    // append the filter information after the basic column information
    for (int32_t f = 0; f < pCol->numOfFilters; ++f) {
      SColumnFilterInfo *pColFilter = &pCol->filterInfo[f];

      SColumnFilterInfo *pFilterMsg = (SColumnFilterInfo *)pMsg;
      pFilterMsg->filterstr = htons(pColFilter->filterstr);

      pMsg += sizeof(SColumnFilterInfo);

      if (pColFilter->filterstr) {
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

  SSqlFuncMsg *pSqlFuncExpr = (SSqlFuncMsg *)pMsg;
  for (int32_t i = 0; i < tscSqlExprNumOfExprs(pQueryInfo); ++i) {
    SSqlExpr *pExpr = tscSqlExprGet(pQueryInfo, i);

    if (!tscValidateColumnId(pTableMetaInfo, pExpr->colInfo.colId)) {
      /* column id is not valid according to the cached table meta, the table meta is expired */
      tscError("%p table schema is not matched with parsed sql", pSql);
      return -1;
    }

    pSqlFuncExpr->colInfo.colId    = htons(pExpr->colInfo.colId);
    pSqlFuncExpr->colInfo.colIndex = htons(pExpr->colInfo.colIndex);
    pSqlFuncExpr->colInfo.flag     = htons(pExpr->colInfo.flag);

    pSqlFuncExpr->functionId  = htons(pExpr->functionId);
    pSqlFuncExpr->numOfParams = htons(pExpr->numOfParams);
    pMsg += sizeof(SSqlFuncMsg);

    for (int32_t j = 0; j < pExpr->numOfParams; ++j) {
      pSqlFuncExpr->arg[j].argType = htons((uint16_t)pExpr->param[j].nType);
      pSqlFuncExpr->arg[j].argBytes = htons(pExpr->param[j].nLen);

      if (pExpr->param[j].nType == TSDB_DATA_TYPE_BINARY) {
        memcpy(pMsg, pExpr->param[j].pz, pExpr->param[j].nLen);
        pMsg += pExpr->param[j].nLen;
      } else {
        pSqlFuncExpr->arg[j].argValue.i64 = htobe64(pExpr->param[j].i64Key);
      }
    }

    pSqlFuncExpr = (SSqlFuncMsg *)pMsg;
  }
  
  // serialize the table info (sid, uid, tags)
  pMsg = doSerializeTableInfo(pQueryMsg, pSql, pMsg);
  
  SSqlGroupbyExpr *pGroupbyExpr = &pQueryInfo->groupbyExpr;
  if (pGroupbyExpr->numOfGroupCols > 0) {
    pQueryMsg->orderByIdx = htons(pGroupbyExpr->orderIndex);
    pQueryMsg->orderType = htons(pGroupbyExpr->orderType);

    for (int32_t j = 0; j < pGroupbyExpr->numOfGroupCols; ++j) {
      SColIndex* pCol = taosArrayGet(pGroupbyExpr->columnInfo, j);
  
      *((int16_t *)pMsg) = pCol->colId;
      pMsg += sizeof(pCol->colId);

      *((int16_t *)pMsg) += pCol->colIndex;
      pMsg += sizeof(pCol->colIndex);

      *((int16_t *)pMsg) += pCol->flag;
      pMsg += sizeof(pCol->flag);
      
      memcpy(pMsg, pCol->name, tListLen(pCol->name));
      pMsg += tListLen(pCol->name);
    }
  }

  if (pQueryInfo->fillType != TSDB_FILL_NONE) {
    for (int32_t i = 0; i < pQueryInfo->fieldsInfo.numOfOutput; ++i) {
      *((int64_t *)pMsg) = htobe64(pQueryInfo->defaultVal[i]);
      pMsg += sizeof(pQueryInfo->defaultVal[0]);
    }
  }
  
  if (numOfTags != 0) {
    int32_t numOfColumns = tscGetNumOfColumns(pTableMeta);
    int32_t numOfTagColumns = tscGetNumOfTags(pTableMeta);
    int32_t total = numOfTagColumns + numOfColumns;
    
    pSchema = tscGetTableTagSchema(pTableMeta);
    
    for (int32_t i = 0; i < numOfTags; ++i) {
      SColumn *pCol = taosArrayGetP(pTableMetaInfo->tagColList, i);
      SSchema *pColSchema = &pSchema[pCol->colIndex.columnIndex];

      if ((pCol->colIndex.columnIndex >= numOfTagColumns || pCol->colIndex.columnIndex < -1) ||
          (pColSchema->type < TSDB_DATA_TYPE_BOOL || pColSchema->type > TSDB_DATA_TYPE_NCHAR)) {
        tscError("%p sid:%d uid:%" PRIu64 " id:%s, tag index out of range, totalCols:%d, numOfTags:%d, index:%d, column name:%s",
                 pSql, pTableMeta->sid, pTableMeta->uid, pTableMetaInfo->name, total, numOfTagColumns,
                 pCol->colIndex, pColSchema->name);

        return TSDB_CODE_INVALID_SQL;
      }
  
      SColumnInfo* pTagCol = (SColumnInfo*) pMsg;
  
      pTagCol->colId = htons(pColSchema->colId);
      pTagCol->bytes = htons(pColSchema->bytes);
      pTagCol->type  = htons(pColSchema->type);
      pTagCol->numOfFilters = 0;
      
      pMsg += sizeof(SColumnInfo);
    }
  }

  // compressed ts block
  pQueryMsg->tsOffset = htonl(pMsg - pStart);
  int32_t tsLen = 0;
  int32_t numOfBlocks = 0;

  if (pQueryInfo->tsBuf != NULL) {
    STSVnodeBlockInfo *pBlockInfo = tsBufGetVnodeBlockInfo(pQueryInfo->tsBuf, pTableMetaInfo->vgroupIndex);
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

  // serialize tag column query condition
  if (pQueryInfo->tagCond.pCond != NULL && taosArrayGetSize(pQueryInfo->tagCond.pCond) > 0) {
    STagCond* pTagCond = &pQueryInfo->tagCond;
    
    SCond *pCond = tsGetSTableQueryCond(pTagCond, pTableMeta->uid);
    if (pCond != NULL && pCond->cond != NULL) {
      pQueryMsg->tagCondLen = htons(pCond->len);
      memcpy(pMsg, pCond->cond, pCond->len);
      
      pMsg += pCond->len;
    }
  }
  
  if (pQueryInfo->tagCond.tbnameCond.cond == NULL) {
    *pMsg = 0;
    pMsg++;
  } else {
    strcpy(pMsg, pQueryInfo->tagCond.tbnameCond.cond);
    pMsg += strlen(pQueryInfo->tagCond.tbnameCond.cond) + 1;
  }

  int32_t msgLen = pMsg - pStart;

  tscTrace("%p msg built success,len:%d bytes", pSql, msgLen);
  pCmd->payloadLen = msgLen;
  pSql->cmd.msgType = TSDB_MSG_TYPE_QUERY;
  
  pQueryMsg->head.contLen = htonl(msgLen);
  assert(msgLen + minMsgSize() <= size);

  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildCreateDbMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->payloadLen = sizeof(SCMCreateDbMsg);
  pCmd->msgType = TSDB_MSG_TYPE_CM_CREATE_DB;

  SCMCreateDbMsg *pCreateDbMsg = (SCMCreateDbMsg*)pCmd->payload;

  assert(pCmd->numOfClause == 1);
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
  strncpy(pCreateDbMsg->db, pTableMetaInfo->name, tListLen(pCreateDbMsg->db));

  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildCreateDnodeMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->payloadLen = sizeof(SCMCreateDnodeMsg);
  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, pCmd->payloadLen)) {
    tscError("%p failed to malloc for query msg", pSql);
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }

  SCMCreateDnodeMsg *pCreate = (SCMCreateDnodeMsg *)pCmd->payload;
  strncpy(pCreate->ep, pInfo->pDCLInfo->a[0].z, pInfo->pDCLInfo->a[0].n);
  
  pCmd->msgType = TSDB_MSG_TYPE_CM_CREATE_DNODE;

  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildAcctMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->payloadLen = sizeof(SCMCreateAcctMsg);
  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, pCmd->payloadLen)) {
    tscError("%p failed to malloc for query msg", pSql);
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }

  SCMCreateAcctMsg *pAlterMsg = (SCMCreateAcctMsg *)pCmd->payload;

  SSQLToken *pName = &pInfo->pDCLInfo->user.user;
  SSQLToken *pPwd = &pInfo->pDCLInfo->user.passwd;

  strncpy(pAlterMsg->user, pName->z, pName->n);
  strncpy(pAlterMsg->pass, pPwd->z, pPwd->n);

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

  pCmd->msgType = TSDB_MSG_TYPE_CM_CREATE_ACCT;
  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildUserMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->payloadLen = sizeof(SCMCreateUserMsg);

  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, pCmd->payloadLen)) {
    tscError("%p failed to malloc for query msg", pSql);
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }

  SCMCreateUserMsg *pAlterMsg = (SCMCreateUserMsg*)pCmd->payload;

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

  if (pUser->type == TSDB_ALTER_USER_PASSWD || pUser->type == TSDB_ALTER_USER_PRIVILEGES) {
    pCmd->msgType = TSDB_MSG_TYPE_CM_ALTER_USER;
  } else {
    pCmd->msgType = TSDB_MSG_TYPE_CM_CREATE_USER;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildCfgDnodeMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->payloadLen = sizeof(SCMCfgDnodeMsg);
  pCmd->msgType = TSDB_MSG_TYPE_CM_CONFIG_DNODE;
  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildDropDbMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->payloadLen = sizeof(SCMDropDbMsg);

  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, pCmd->payloadLen)) {
    tscError("%p failed to malloc for query msg", pSql);
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }

  SCMDropDbMsg *pDropDbMsg = (SCMDropDbMsg*)pCmd->payload;

  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
  strncpy(pDropDbMsg->db, pTableMetaInfo->name, tListLen(pDropDbMsg->db));
  pDropDbMsg->ignoreNotExists = pInfo->pDCLInfo->existsCheck ? 1 : 0;

  pCmd->msgType = TSDB_MSG_TYPE_CM_DROP_DB;
  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildDropTableMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->payloadLen = sizeof(SCMDropTableMsg);

  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, pCmd->payloadLen)) {
    tscError("%p failed to malloc for query msg", pSql);
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }

  SCMDropTableMsg *pDropTableMsg = (SCMDropTableMsg*)pCmd->payload;
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
  strcpy(pDropTableMsg->tableId, pTableMetaInfo->name);
  pDropTableMsg->igNotExists = pInfo->pDCLInfo->existsCheck ? 1 : 0;

  pCmd->msgType = TSDB_MSG_TYPE_CM_DROP_TABLE;
  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildDropDnodeMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->payloadLen = sizeof(SCMDropDnodeMsg);
  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, pCmd->payloadLen)) {
    tscError("%p failed to malloc for query msg", pSql);
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }

  SCMDropDnodeMsg *pDrop = (SCMDropDnodeMsg *)pCmd->payload;
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
  strcpy(pDrop->ep, pTableMetaInfo->name);
  pCmd->msgType = TSDB_MSG_TYPE_CM_DROP_DNODE;

  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildDropUserMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->payloadLen = sizeof(SCMDropUserMsg);
  pCmd->msgType = TSDB_MSG_TYPE_CM_DROP_USER;

  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, pCmd->payloadLen)) {
    tscError("%p failed to malloc for query msg", pSql);
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }

  SCMDropUserMsg *pDropMsg = (SCMDropUserMsg*)pCmd->payload;
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
  strcpy(pDropMsg->user, pTableMetaInfo->name);

  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildDropAcctMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->payloadLen = sizeof(SCMDropUserMsg);
  pCmd->msgType = TSDB_MSG_TYPE_CM_DROP_ACCT;

  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, pCmd->payloadLen)) {
    tscError("%p failed to malloc for query msg", pSql);
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }

  SCMDropUserMsg *pDropMsg = (SCMDropUserMsg*)pCmd->payload;
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
  strcpy(pDropMsg->user, pTableMetaInfo->name);

  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildUseDbMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->payloadLen = sizeof(SCMUseDbMsg);

  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, pCmd->payloadLen)) {
    tscError("%p failed to malloc for query msg", pSql);
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }

  SCMUseDbMsg *pUseDbMsg = (SCMUseDbMsg*)pCmd->payload;
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
  strcpy(pUseDbMsg->db, pTableMetaInfo->name);
  pCmd->msgType = TSDB_MSG_TYPE_CM_USE_DB;

  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildShowMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  STscObj *pObj = pSql->pTscObj;
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->msgType = TSDB_MSG_TYPE_CM_SHOW;
  pCmd->payloadLen = sizeof(SCMShowMsg) + 100;

  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, pCmd->payloadLen)) {
    tscError("%p failed to malloc for query msg", pSql);
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }

  SCMShowMsg *pShowMsg = (SCMShowMsg*)pCmd->payload;

  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
  size_t nameLen = strlen(pTableMetaInfo->name);
  if (nameLen > 0) {
    strcpy(pShowMsg->db, pTableMetaInfo->name);  // prefix is set here
  } else {
    strcpy(pShowMsg->db, pObj->db);
  }

  SShowInfo *pShowInfo = &pInfo->pDCLInfo->showOpt;
  pShowMsg->type = pShowInfo->showType;

  if (pShowInfo->showType != TSDB_MGMT_TABLE_VNODES) {
    SSQLToken *pPattern = &pShowInfo->pattern;
    if (pPattern->type > 0) {  // only show tables support wildcard query
      strncpy(pShowMsg->payload, pPattern->z, pPattern->n);
      pShowMsg->payloadLen = htons(pPattern->n);
    }
  } else {
    SSQLToken *pIpAddr = &pShowInfo->prefix;
    assert(pIpAddr->n > 0 && pIpAddr->type > 0);

    strncpy(pShowMsg->payload, pIpAddr->z, pIpAddr->n);
    pShowMsg->payloadLen = htons(pIpAddr->n);
  }

  pCmd->payloadLen = sizeof(SCMShowMsg) + pShowMsg->payloadLen;
  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildKillMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->payloadLen = sizeof(SCMKillQueryMsg);

  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, pCmd->payloadLen)) {
    tscError("%p failed to malloc for query msg", pSql);
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }

  SCMKillQueryMsg *pKill = (SCMKillQueryMsg*)pCmd->payload;
  strncpy(pKill->queryId, pInfo->pDCLInfo->ip.z, pInfo->pDCLInfo->ip.n);
  switch (pCmd->command) {
    case TSDB_SQL_KILL_QUERY:
      pCmd->msgType = TSDB_MSG_TYPE_CM_KILL_QUERY;
      break;
    case TSDB_SQL_KILL_CONNECTION:
      pCmd->msgType = TSDB_MSG_TYPE_CM_KILL_CONN;
      break;
    case TSDB_SQL_KILL_STREAM:
      pCmd->msgType = TSDB_MSG_TYPE_CM_KILL_STREAM;
      break;
  }
  return TSDB_CODE_SUCCESS;
}

int tscEstimateCreateTableMsgLength(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &(pSql->cmd);

  int32_t size = minMsgSize() + sizeof(SCMCreateTableMsg);

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
  int              msgLen = 0;
  SSchema *        pSchema;
  int              size = 0;
  SSqlCmd *pCmd = &pSql->cmd;

  SQueryInfo *    pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);
  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  // Reallocate the payload size
  size = tscEstimateCreateTableMsgLength(pSql, pInfo);
  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, size)) {
    tscError("%p failed to malloc for create table msg", pSql);
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }


  SCMCreateTableMsg *pCreateTableMsg = (SCMCreateTableMsg *)pCmd->payload;
  strcpy(pCreateTableMsg->tableId, pTableMetaInfo->name);

  // use dbinfo from table id without modifying current db info
  tscGetDBInfoFromMeterId(pTableMetaInfo->name, pCreateTableMsg->db);

  SCreateTableSQL *pCreateTable = pInfo->pCreateTableInfo;

  pCreateTableMsg->igExists = pCreateTable->existCheck ? 1 : 0;
  pCreateTableMsg->numOfColumns = htons(pCmd->numOfCols);
  pCreateTableMsg->numOfTags = htons(pCmd->count);

  pCreateTableMsg->sqlLen = 0;
  char *pMsg = (char *)pCreateTableMsg->schema;

  int8_t type = pInfo->pCreateTableInfo->type;
  if (type == TSQL_CREATE_TABLE_FROM_STABLE) {  // create by using super table, tags value
    STagData* pTag = &pInfo->pCreateTableInfo->usingInfo.tagdata;
    *(int32_t*)pMsg = htonl(pTag->dataLen);
    pMsg += sizeof(int32_t);
    memcpy(pMsg, pTag->name, sizeof(pTag->name));
    pMsg += sizeof(pTag->name);
    memcpy(pMsg, pTag->data, pTag->dataLen);
    pMsg += pTag->dataLen;
  } else {  // create (super) table
    pSchema = (SSchema *)pCreateTableMsg->schema;

    for (int i = 0; i < pCmd->numOfCols + pCmd->count; ++i) {
      TAOS_FIELD *pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, i);

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

  tscFieldInfoClear(&pQueryInfo->fieldsInfo);

  msgLen = pMsg - (char*)pCreateTableMsg;
  pCreateTableMsg->contLen = htonl(msgLen);
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_CM_CREATE_TABLE;

  assert(msgLen + minMsgSize() <= size);
  return TSDB_CODE_SUCCESS;
}

int tscEstimateAlterTableMsgLength(SSqlCmd *pCmd) {
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);
  return minMsgSize() + sizeof(SCMAlterTableMsg) + sizeof(SSchema) * tscNumOfFields(pQueryInfo) +
         TSDB_EXTRA_PAYLOAD_SIZE;
}

int tscBuildAlterTableMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SCMAlterTableMsg *pAlterTableMsg;
  char *          pMsg;
  int             msgLen = 0;
  int             size = 0;

  SSqlCmd *   pCmd = &pSql->cmd;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);

  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  size = tscEstimateAlterTableMsgLength(pCmd);
  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, size)) {
    tscError("%p failed to malloc for alter table msg", pSql);
    return -1;
  }

  pAlterTableMsg = (SCMAlterTableMsg *)pCmd->payload;

  tscGetDBInfoFromMeterId(pTableMetaInfo->name, pAlterTableMsg->db);

  SAlterTableSQL *pAlterInfo = pInfo->pAlterInfo;

  strcpy(pAlterTableMsg->tableId, pTableMetaInfo->name);
  pAlterTableMsg->type = htons(pAlterInfo->type);

  pAlterTableMsg->numOfCols = htons(tscNumOfFields(pQueryInfo));
  SSchema *pSchema = pAlterTableMsg->schema;
  for (int i = 0; i < pAlterTableMsg->numOfCols; ++i) {
    TAOS_FIELD *pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, i);

    pSchema->type = pField->type;
    strcpy(pSchema->name, pField->name);
    pSchema->bytes = htons(pField->bytes);
    pSchema++;
  }

  pMsg = (char *)pSchema;
  pAlterTableMsg->tagValLen = htonl(pAlterInfo->tagData.dataLen);
  memcpy(pMsg, pAlterInfo->tagData.data, pAlterInfo->tagData.dataLen);
  pMsg += pAlterInfo->tagData.dataLen;

  msgLen = pMsg - (char*)pAlterTableMsg;
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_CM_ALTER_TABLE;

  assert(msgLen + minMsgSize() <= size);

  return TSDB_CODE_SUCCESS;
}

int tscAlterDbMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->payloadLen = sizeof(SCMAlterDbMsg);
  pCmd->msgType = TSDB_MSG_TYPE_CM_ALTER_DB;

  SCMAlterDbMsg *pAlterDbMsg = (SCMAlterDbMsg*)pCmd->payload;
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
  strcpy(pAlterDbMsg->db, pTableMetaInfo->name);

  return TSDB_CODE_SUCCESS;
}

int tscBuildRetrieveFromMgmtMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->msgType = TSDB_MSG_TYPE_CM_RETRIEVE;
  pCmd->payloadLen = sizeof(SRetrieveTableMsg);

  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, pCmd->payloadLen)) {
    tscError("%p failed to malloc for query msg", pSql);
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }

  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);
  SRetrieveTableMsg *pRetrieveMsg = (SRetrieveTableMsg*)pCmd->payload;
  pRetrieveMsg->qhandle = htobe64(pSql->res.qhandle);
  pRetrieveMsg->free = htons(pQueryInfo->type);

  return TSDB_CODE_SUCCESS;
}

static int tscSetResultPointer(SQueryInfo *pQueryInfo, SSqlRes *pRes) {
  if (tscCreateResPointerInfo(pRes, pQueryInfo) != TSDB_CODE_SUCCESS) {
    return pRes->code;
  }

  for (int i = 0; i < pQueryInfo->fieldsInfo.numOfOutput; ++i) {
    int16_t offset = tscFieldInfoGetOffset(pQueryInfo, i);
    pRes->tsrow[i] = ((char*) pRes->data + offset * pRes->numOfRows);
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
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);

  STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);
  
  int32_t numOfRes = tinfo.numOfColumns + tinfo.numOfTags;
  return tscLocalResultCommonBuilder(pSql, numOfRes);
}

int tscProcessTagRetrieveRsp(SSqlObj *pSql) {
//  SSqlCmd *pCmd = &pSql->cmd;

//  SQueryInfo *    pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
//  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  int32_t numOfRes = 0;
#if 0
  if (tscSqlExprGet(pQueryInfo, 0)->functionId == TSDB_FUNC_TAGPRJ) {
    numOfRes = pTableMetaInfo->pMetricMeta->numOfTables;
  } else {
    numOfRes = 1;  // for count function, there is only one output.
  }
  
#endif

  return tscLocalResultCommonBuilder(pSql, numOfRes);
}

int tscProcessRetrieveLocalMergeRsp(SSqlObj *pSql) {
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

  pRes->code = tscDoLocalMerge(pSql);
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);

  if (pRes->code == TSDB_CODE_SUCCESS && pRes->numOfRows > 0) {
    tscSetResultPointer(pQueryInfo, pRes);
  }

  pRes->row = 0;
  pRes->completed = (pRes->numOfRows == 0);

  int32_t code = pRes->code;
  if (pRes->code == TSDB_CODE_SUCCESS) {
    (*pSql->fp)(pSql->param, pSql, pRes->numOfRows);
  } else {
    tscQueueAsyncRes(pSql);
  }

  return code;
}

int tscProcessEmptyResultRsp(SSqlObj *pSql) { return tscLocalResultCommonBuilder(pSql, 0); }

int tscBuildConnectMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  STscObj *pObj = pSql->pTscObj;
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->msgType = TSDB_MSG_TYPE_CM_CONNECT;
  pCmd->payloadLen = sizeof(SCMConnectMsg);

  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, pCmd->payloadLen)) {
    tscError("%p failed to malloc for query msg", pSql);
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }

  SCMConnectMsg *pConnect = (SCMConnectMsg*)pCmd->payload;

  char *db;  // ugly code to move the space
  db = strstr(pObj->db, TS_PATH_DELIMITER);
  db = (db == NULL) ? pObj->db : db + 1;
  strcpy(pConnect->db, db);
  strcpy(pConnect->clientVersion, version);
  strcpy(pConnect->msgVersion, "");

  return TSDB_CODE_SUCCESS;
}

int tscBuildTableMetaMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SCMTableInfoMsg *pInfoMsg;
  char *         pMsg;
  int            msgLen = 0;

  char *tmpData = NULL;
  uint32_t len = pSql->cmd.payloadLen;
  if (len > 0) {
    tmpData = calloc(1, len);
    if (NULL == tmpData) {
      return TSDB_CODE_CLI_OUT_OF_MEMORY;
    }

    // STagData is in binary format, strncpy is not available
    memcpy(tmpData, pSql->cmd.payload, len);
  }

  SSqlCmd *   pCmd = &pSql->cmd;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);

  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  pInfoMsg = (SCMTableInfoMsg *)pCmd->payload;
  strcpy(pInfoMsg->tableId, pTableMetaInfo->name);
  pInfoMsg->createFlag = htons(pSql->cmd.autoCreated ? 1 : 0);

  pMsg = (char*)pInfoMsg + sizeof(SCMTableInfoMsg);

  if (pSql->cmd.autoCreated && len > 0) {
    memcpy(pInfoMsg->tags, tmpData, len);
    pMsg += len;
  }

  pCmd->payloadLen = pMsg - (char*)pInfoMsg;;
  pCmd->msgType = TSDB_MSG_TYPE_CM_TABLE_META;

  tfree(tmpData);

  assert(msgLen + minMsgSize() <= pCmd->allocSize);
  return TSDB_CODE_SUCCESS;
}

/**
 *  multi table meta req pkg format:
 *  | SMgmtHead | SCMMultiTableInfoMsg | tableId0 | tableId1 | tableId2 | ......
 *      no used         4B
 **/
int tscBuildMultiMeterMetaMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
#if 0
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
  memset(pMgmt->db, 0, TSDB_TABLE_ID_LEN);  // server don't need the db

  SCMMultiTableInfoMsg *pInfoMsg = (SCMMultiTableInfoMsg *)(pCmd->payload + tsRpcHeadSize + sizeof(SMgmtHead));
  pInfoMsg->numOfTables = htonl((int32_t)pCmd->count);

  if (pCmd->payloadLen > 0) {
    memcpy(pInfoMsg->tableIds, tmpData, pCmd->payloadLen);
  }

  tfree(tmpData);

  pCmd->payloadLen += sizeof(SMgmtHead) + sizeof(SCMMultiTableInfoMsg);
  pCmd->msgType = TSDB_MSG_TYPE_CM_TABLES_META;

  assert(pCmd->payloadLen + minMsgSize() <= pCmd->allocSize);

  tscTrace("%p build load multi-metermeta msg completed, numOfTables:%d, msg size:%d", pSql, pCmd->count,
           pCmd->payloadLen);

  return pCmd->payloadLen;
#endif
  return 0;  
}

//static UNUSED_FUNC int32_t tscEstimateMetricMetaMsgSize(SSqlCmd *pCmd) {
////  const int32_t defaultSize =
////      minMsgSize() + sizeof(SSuperTableMetaMsg) + sizeof(SMgmtHead) + sizeof(int16_t) * TSDB_MAX_TAGS;
////  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);
////
////  int32_t n = 0;
////  size_t size = taosArrayGetSize(pQueryInfo->tagCond.pCond);
////  for (int32_t i = 0; i < size; ++i) {
////    assert(0);
//////    n += strlen(pQueryInfo->tagCond.cond[i].cond);
////  }
////
////  int32_t tagLen = n * TSDB_NCHAR_SIZE;
////  if (pQueryInfo->tagCond.tbnameCond.cond != NULL) {
////    tagLen += strlen(pQueryInfo->tagCond.tbnameCond.cond) * TSDB_NCHAR_SIZE;
////  }
////
////  int32_t joinCondLen = (TSDB_TABLE_ID_LEN + sizeof(int16_t)) * 2;
////  int32_t elemSize = sizeof(SSuperTableMetaElemMsg) * pQueryInfo->numOfTables;
////
////  int32_t colSize = pQueryInfo->groupbyExpr.numOfGroupCols*sizeof(SColIndex);
////
////  int32_t len = tagLen + joinCondLen + elemSize + colSize + defaultSize;
////
////  return MAX(len, TSDB_DEFAULT_PAYLOAD_SIZE);
//}

int tscBuildSTableVgroupMsg(SSqlObj *pSql, SSqlInfo *pInfo) {

#if 0
  SSuperTableMetaMsg *pMetaMsg;
  char *          pMsg, *pStart;
  int             msgLen = 0;
  int             tableIndex = 0;

  SSqlCmd *   pCmd = &pSql->cmd;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);

  STagCond *pTagCond = &pQueryInfo->tagCond;

  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tableIndex);

  int32_t size = tscEstimateMetricMetaMsgSize(pCmd);
  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, size)) {
    tscError("%p failed to malloc for metric meter msg", pSql);
    return -1;
  }

  pStart = pCmd->payload + tsRpcHeadSize;
  pMsg = pStart;

  SMgmtHead *pMgmt = (SMgmtHead *)pMsg;
  tscGetDBInfoFromMeterId(pTableMetaInfo->name, pMgmt->db);

  pMsg += sizeof(SMgmtHead);

  pMetaMsg = (SSuperTableMetaMsg *)pMsg;
  pMetaMsg->numOfTables = htonl(pQueryInfo->numOfTables);

  pMsg += sizeof(SSuperTableMetaMsg);

  int32_t offset = pMsg - (char *)pMetaMsg;
  pMetaMsg->join = htonl(offset);

  // todo refactor
  pMetaMsg->joinCondLen = htonl((TSDB_TABLE_ID_LEN + sizeof(int16_t)) * 2);

  memcpy(pMsg, pTagCond->joinInfo.left.tableId, TSDB_TABLE_ID_LEN);
  pMsg += TSDB_TABLE_ID_LEN;

  *(int16_t *)pMsg = pTagCond->joinInfo.left.tagCol;
  pMsg += sizeof(int16_t);

  memcpy(pMsg, pTagCond->joinInfo.right.tableId, TSDB_TABLE_ID_LEN);
  pMsg += TSDB_TABLE_ID_LEN;

  *(int16_t *)pMsg = pTagCond->joinInfo.right.tagCol;
  pMsg += sizeof(int16_t);

  for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, i);
    uint64_t uid = pTableMetaInfo->pTableMeta->uid;

    offset = pMsg - (char *)pMetaMsg;
    pMetaMsg->metaElem[i] = htonl(offset);

    SSuperTableMetaElemMsg *pElem = (SSuperTableMetaElemMsg *)pMsg;
    pMsg += sizeof(SSuperTableMetaElemMsg);

    // convert to unicode before sending to mnode for metric query
    int32_t condLen = 0;
    if (pTagCond->numOfTagCond > 0) {
      SCond *pCond = tsGetSTableQueryCond(pTagCond, uid);
      if (pCond != NULL && pCond->cond != NULL) {
        condLen = strlen(pCond->cond) + 1;

        bool ret = taosMbsToUcs4(pCond->cond, condLen, pMsg, condLen * TSDB_NCHAR_SIZE);
        if (!ret) {
          tscError("%p mbs to ucs4 failed:%s", pSql, tsGetSTableQueryCond(pTagCond, uid));
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
      for (int32_t j = 0; j < pTableMetaInfo->numOfTags; ++j) {
        pElem->tagCols[j] = htons(pTableMetaInfo->tagColumnIndex[j]);
      }

      if (pGroupby->numOfGroupCols != 0) {
        pElem->orderIndex = htons(pGroupby->orderIndex);
        pElem->orderType = htons(pGroupby->orderType);
        offset = pMsg - (char *)pMetaMsg;

        pElem->groupbyTagColumnList = htonl(offset);
        for (int32_t j = 0; j < pQueryInfo->groupbyExpr.numOfGroupCols; ++j) {
          SColIndex *pCol = &pQueryInfo->groupbyExpr.columnInfo[j];
          SColIndex *pDestCol = (SColIndex *)pMsg;

          pDestCol->colIdxInBuf = 0;
          pDestCol->colIndex = htons(pCol->colIndex);
          pDestCol->colId = htons(pDestCol->colId);
          pDestCol->flag = htons(pDestCol->flag);
          strncpy(pDestCol->name, pCol->name, tListLen(pCol->name));

          pMsg += sizeof(SColIndex);
        }
      }
    }

    strcpy(pElem->tableId, pTableMetaInfo->name);
    pElem->numOfTags = htons(pTableMetaInfo->numOfTags);

    int16_t len = pMsg - (char *)pElem;
    pElem->elemLen = htons(len);  // redundant data for integrate check
  }

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_CM_STABLE_VGROUP;
  assert(msgLen + minMsgSize() <= size);
#endif
  
  SSqlCmd *pCmd = &pSql->cmd;
  
  char* pMsg = pCmd->payload;
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);
  
  SCMSTableVgroupMsg *pStableVgroupMsg = (SCMSTableVgroupMsg *) pMsg;
  pStableVgroupMsg->numOfTables = htonl(pQueryInfo->numOfTables);
  pMsg += sizeof(SCMSTableVgroupMsg);
  
  for(int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, i);
    strncpy(pMsg, pTableMetaInfo->name, TSDB_TABLE_ID_LEN);
    pMsg += TSDB_TABLE_ID_LEN;
  }

  pCmd->msgType = TSDB_MSG_TYPE_CM_STABLE_VGROUP;
  pCmd->payloadLen = (pMsg - pCmd->payload);

  return TSDB_CODE_SUCCESS;
}

int tscEstimateHeartBeatMsgLength(SSqlObj *pSql) {
  int      size = 0;
  STscObj *pObj = pSql->pTscObj;

  size += tsRpcHeadSize;
  size += sizeof(SQqueryList);

  SSqlObj *tpSql = pObj->sqlList;
  while (tpSql) {
    size += sizeof(SQueryDesc);
    tpSql = tpSql->next;
  }

  size += sizeof(SStreamList);
  SSqlStream *pStream = pObj->streamList;
  while (pStream) {
    size += sizeof(SStreamDesc);
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
    pthread_mutex_unlock(&pObj->mutex);
    tscError("%p failed to malloc for heartbeat msg", pSql);
    return -1;
  }

  pMsg = pCmd->payload;
  pStart = pMsg;

  pMsg = tscBuildQueryStreamDesc(pMsg, pObj);
  pthread_mutex_unlock(&pObj->mutex);

  msgLen = pMsg - pStart;
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_CM_HEARTBEAT;

  assert(msgLen + minMsgSize() <= size);
  return TSDB_CODE_SUCCESS;
}

int tscProcessTableMetaRsp(SSqlObj *pSql) {
  STableMetaMsg *pMetaMsg = (STableMetaMsg *)pSql->res.pRsp;

  pMetaMsg->sid = htonl(pMetaMsg->sid);
  pMetaMsg->sversion = htons(pMetaMsg->sversion);
  
  pMetaMsg->vgroup.vgId = htonl(pMetaMsg->vgroup.vgId);
  
  pMetaMsg->uid = htobe64(pMetaMsg->uid);
  pMetaMsg->contLen = htons(pMetaMsg->contLen);
  pMetaMsg->numOfColumns = htons(pMetaMsg->numOfColumns);

  if (pMetaMsg->sid < 0 || pMetaMsg->vgroup.numOfIps < 0) {
    tscError("invalid meter vgId:%d, sid%d", pMetaMsg->vgroup.numOfIps, pMetaMsg->sid);
    return TSDB_CODE_INVALID_VALUE;
  }

  if (pMetaMsg->numOfTags > TSDB_MAX_TAGS || pMetaMsg->numOfTags < 0) {
    tscError("invalid numOfTags:%d", pMetaMsg->numOfTags);
    return TSDB_CODE_INVALID_VALUE;
  }

  if (pMetaMsg->numOfColumns > TSDB_MAX_COLUMNS || pMetaMsg->numOfColumns <= 0) {
    tscError("invalid numOfColumns:%d", pMetaMsg->numOfColumns);
    return TSDB_CODE_INVALID_VALUE;
  }

  for (int i = 0; i < pMetaMsg->vgroup.numOfIps; ++i) {
    pMetaMsg->vgroup.ipAddr[i].port = htons(pMetaMsg->vgroup.ipAddr[i].port);
  }

  SSchema* pSchema = pMetaMsg->schema;

  int32_t numOfTotalCols = pMetaMsg->numOfColumns + pMetaMsg->numOfTags;
  for (int i = 0; i < numOfTotalCols; ++i) {
    pSchema->bytes = htons(pSchema->bytes);
    pSchema->colId = htons(pSchema->colId);
    
    assert(pSchema->type >= TSDB_DATA_TYPE_BOOL && pSchema->type <= TSDB_DATA_TYPE_NCHAR);
    pSchema++;
  }

  size_t size = 0;
  STableMeta* pTableMeta = tscCreateTableMetaFromMsg(pMetaMsg, &size);

#if 0
  // if current table is created according to super table, get the table meta of super table
  if (pTableMeta->tableType == TSDB_CHILD_TABLE) {
    char id[TSDB_TABLE_ID_LEN + 1] = {0};
    strncpy(id, pMetaMsg->stableId, TSDB_TABLE_ID_LEN);
  
    // NOTE: if the table meta of super table is not cached at client side yet, the pSTable is NULL
    pTableMeta->pSTable = taosCacheAcquireByName(tscCacheHandle, id);
  }
#endif
  
  // todo add one more function: taosAddDataIfNotExists();
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pSql->cmd, 0, 0);
  assert(pTableMetaInfo->pTableMeta == NULL);

  pTableMetaInfo->pTableMeta =
      (STableMeta *) taosCachePut(tscCacheHandle, pTableMetaInfo->name, pTableMeta, size, tsTableMetaKeepTimer);
  
  // todo handle out of memory case
  if (pTableMetaInfo->pTableMeta == NULL) {
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }

  free(pTableMeta);
  
  return TSDB_CODE_SUCCESS;
}

/**
 *  multi table meta rsp pkg format:
 *  | STaosRsp | ieType | SCMMultiTableInfoMsg | SMeterMeta0 | SSchema0 | SMeterMeta1 | SSchema1 | SMeterMeta2 | SSchema2
 *  |...... 1B        1B            4B
 **/
int tscProcessMultiMeterMetaRsp(SSqlObj *pSql) {
#if 0
  char *rsp = pSql->res.pRsp;

  ieType = *rsp;
  if (ieType != TSDB_IE_TYPE_META) {
    tscError("invalid ie type:%d", ieType);
    pSql->res.code = TSDB_CODE_INVALID_IE;
    pSql->res.numOfTotal = 0;
    return TSDB_CODE_OTHERS;
  }

  rsp++;

  SCMMultiTableInfoMsg *pInfo = (SCMMultiTableInfoMsg *)rsp;
  totalNum = htonl(pInfo->numOfTables);
  rsp += sizeof(SCMMultiTableInfoMsg);

  for (i = 0; i < totalNum; i++) {
    SMultiTableMeta *pMultiMeta = (SMultiTableMeta *)rsp;
    STableMeta *     pMeta = pMultiMeta->metas;

    pMeta->sid = htonl(pMeta->sid);
    pMeta->sversion = htons(pMeta->sversion);
    pMeta->vgId = htonl(pMeta->vgId);
    pMeta->uid = htobe64(pMeta->uid);

    if (pMeta->sid <= 0 || pMeta->vgId < 0) {
      tscError("invalid meter vgId:%d, sid%d", pMeta->vgId, pMeta->sid);
      pSql->res.code = TSDB_CODE_INVALID_VALUE;
      pSql->res.numOfTotal = i;
      return TSDB_CODE_OTHERS;
    }

    //    pMeta->numOfColumns = htons(pMeta->numOfColumns);
    //
    //    if (pMeta->numOfTags > TSDB_MAX_TAGS || pMeta->numOfTags < 0) {
    //      tscError("invalid tag value count:%d", pMeta->numOfTags);
    //      pSql->res.code = TSDB_CODE_INVALID_VALUE;
    //      pSql->res.numOfTotal = i;
    //      return TSDB_CODE_OTHERS;
    //    }
    //
    //    if (pMeta->numOfTags > TSDB_MAX_TAGS || pMeta->numOfTags < 0) {
    //      tscError("invalid numOfTags:%d", pMeta->numOfTags);
    //      pSql->res.code = TSDB_CODE_INVALID_VALUE;
    //      pSql->res.numOfTotal = i;
    //      return TSDB_CODE_OTHERS;
    //    }
    //
    //    if (pMeta->numOfColumns > TSDB_MAX_COLUMNS || pMeta->numOfColumns < 0) {
    //      tscError("invalid numOfColumns:%d", pMeta->numOfColumns);
    //      pSql->res.code = TSDB_CODE_INVALID_VALUE;
    //      pSql->res.numOfTotal = i;
    //      return TSDB_CODE_OTHERS;
    //    }
    //
    //    for (int j = 0; j < TSDB_REPLICA_MAX_NUM; ++j) {
    //      pMeta->vpeerDesc[j].vnode = htonl(pMeta->vpeerDesc[j].vnode);
    //    }
    //
    //    pMeta->rowSize = 0;
    //    rsp += sizeof(SMultiTableMeta);
    //    pSchema = (SSchema *)rsp;
    //
    //    int32_t numOfTotalCols = pMeta->numOfColumns + pMeta->numOfTags;
    //    for (int j = 0; j < numOfTotalCols; ++j) {
    //      pSchema->bytes = htons(pSchema->bytes);
    //      pSchema->colId = htons(pSchema->colId);
    //
    //      // ignore the tags length
    //      if (j < pMeta->numOfColumns) {
    //        pMeta->rowSize += pSchema->bytes;
    //      }
    //      pSchema++;
    //    }
    //
    //    rsp += numOfTotalCols * sizeof(SSchema);
    //
    //    int32_t  tagLen = 0;
    //    SSchema *pTagsSchema = tscGetTableTagSchema(pMeta);
    //
    //    if (pMeta->tableType == TSDB_CHILD_TABLE) {
    //      for (int32_t j = 0; j < pMeta->numOfTags; ++j) {
    //        tagLen += pTagsSchema[j].bytes;
    //      }
    //    }
    //
    //    rsp += tagLen;
    //    int32_t size = (int32_t)(rsp - ((char *)pMeta));  // Consistent with STableMeta in cache
    //
    //    pMeta->index = 0;
    //    (void)taosCachePut(tscCacheHandle, pMeta->tableId, (char *)pMeta, size, tsTableMetaKeepTimer);
    //  }
  }
  
  pSql->res.code = TSDB_CODE_SUCCESS;
  pSql->res.numOfTotal = i;
  tscTrace("%p load multi-metermeta resp complete num:%d", pSql, pSql->res.numOfTotal);
#endif
  
  return TSDB_CODE_SUCCESS;
}

int tscProcessSTableVgroupRsp(SSqlObj *pSql) {
#if 0
  void **      metricMetaList = NULL;
  int32_t *    sizes = NULL;
  
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
    pMeta = (SSuperTableMeta *)rsp;

    size_t size = (size_t)pSql->res.rspLen - 1;
    rsp = rsp + sizeof(SSuperTableMeta);

    pMeta->numOfTables = htonl(pMeta->numOfTables);
    pMeta->numOfVnodes = htonl(pMeta->numOfVnodes);
    pMeta->tagLen = htons(pMeta->tagLen);

    size += pMeta->numOfVnodes * sizeof(SVnodeSidList *) + pMeta->numOfTables * sizeof(STableIdInfo *);

    char *pBuf = calloc(1, size);
    if (pBuf == NULL) {
      pSql->res.code = TSDB_CODE_CLI_OUT_OF_MEMORY;
      goto _error_clean;
    }

    SSuperTableMeta *pNewMetricMeta = (SSuperTableMeta *)pBuf;
    metricMetaList[k] = pNewMetricMeta;

    pNewMetricMeta->numOfTables = pMeta->numOfTables;
    pNewMetricMeta->numOfVnodes = pMeta->numOfVnodes;
    pNewMetricMeta->tagLen = pMeta->tagLen;

    pBuf = pBuf + sizeof(SSuperTableMeta) + pNewMetricMeta->numOfVnodes * sizeof(SVnodeSidList *);

    for (int32_t i = 0; i < pMeta->numOfVnodes; ++i) {
      SVnodeSidList *pSidLists = (SVnodeSidList *)rsp;
      memcpy(pBuf, pSidLists, sizeof(SVnodeSidList));

      pNewMetricMeta->list[i] = pBuf - (char *)pNewMetricMeta;  // offset value
      SVnodeSidList *pLists = (SVnodeSidList *)pBuf;

      tscTrace("%p metricmeta:vid:%d,numOfTables:%d", pSql, i, pLists->numOfSids);

      pBuf += sizeof(SVnodeSidList) + sizeof(STableIdInfo *) * pSidLists->numOfSids;
      rsp += sizeof(SVnodeSidList);

      size_t elemSize = sizeof(STableIdInfo) + pNewMetricMeta->tagLen;
      for (int32_t j = 0; j < pSidLists->numOfSids; ++j) {
        pLists->pSidExtInfoList[j] = pBuf - (char *)pLists;
        memcpy(pBuf, rsp, elemSize);

        ((STableIdInfo *)pBuf)->uid = htobe64(((STableIdInfo *)pBuf)->uid);
        ((STableIdInfo *)pBuf)->sid = htonl(((STableIdInfo *)pBuf)->sid);

        rsp += elemSize;
        pBuf += elemSize;
      }
    }

    sizes[k] = pBuf - (char *)pNewMetricMeta;
  }

  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  for (int32_t i = 0; i < num; ++i) {
    char name[TSDB_MAX_TAGS_LEN + 1] = {0};

    STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, i);
    tscGetMetricMetaCacheKey(pQueryInfo, name, pTableMetaInfo->pTableMeta->uid);

#ifdef _DEBUG_VIEW
    printf("generate the metric key:%s, index:%d\n", name, i);
#endif

    // release the used metricmeta
    taosCacheRelease(tscCacheHandle, (void **)&(pTableMetaInfo->pMetricMeta), false);
    pTableMetaInfo->pMetricMeta = (SSuperTableMeta *)taosCachePut(tscCacheHandle, name, (char *)metricMetaList[i],
                                                                      sizes[i], tsMetricMetaKeepTimer);
    tfree(metricMetaList[i]);

    // failed to put into cache
    if (pTableMetaInfo->pMetricMeta == NULL) {
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
#endif
  SSqlRes* pRes = &pSql->res;
  
  // NOTE: the order of several table must be preserved.
  SCMSTableVgroupRspMsg *pStableVgroup = (SCMSTableVgroupRspMsg *)pRes->pRsp;
  pStableVgroup->numOfTables = htonl(pStableVgroup->numOfTables);
  char* pMsg = pRes->pRsp + sizeof(SCMSTableVgroupRspMsg);
  
  // master sqlObj locates in param
  SSqlObj* parent = pSql->param;
  assert(parent != NULL);
  
  SSqlCmd* pCmd = &parent->cmd;
  for(int32_t i = 0; i < pStableVgroup->numOfTables; ++i) {
    STableMetaInfo *pInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, i);
    SVgroupsInfo *  pVgroupInfo = (SVgroupsInfo *)pMsg;
    pVgroupInfo->numOfVgroups = htonl(pVgroupInfo->numOfVgroups);

    size_t size = sizeof(SCMVgroupInfo) * pVgroupInfo->numOfVgroups + sizeof(SVgroupsInfo);
    pInfo->vgroupList = calloc(1, size);
    assert(pInfo->vgroupList != NULL);

    memcpy(pInfo->vgroupList, pVgroupInfo, size);
    for (int32_t j = 0; j < pInfo->vgroupList->numOfVgroups; ++j) {
      SCMVgroupInfo *pVgroups = &pInfo->vgroupList->vgroups[j];

      pVgroups->vgId = htonl(pVgroups->vgId);
      assert(pVgroups->numOfIps >= 1);

      for (int32_t k = 0; k < pVgroups->numOfIps; ++k) {
        pVgroups->ipAddr[k].port = htons(pVgroups->ipAddr[k].port);
      }

      pMsg += size;
    }
  }
  
  return pSql->res.code;
}

/*
 * current process do not use the cache at all
 */
int tscProcessShowRsp(SSqlObj *pSql) {
  STableMetaMsg * pMetaMsg;
  SCMShowRsp *pShow;
  SSchema *    pSchema;
  char         key[20];

  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);

  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  pShow = (SCMShowRsp *)pRes->pRsp;
  pShow->qhandle = htobe64(pShow->qhandle);
  pRes->qhandle = pShow->qhandle;

  tscResetForNextRetrieve(pRes);
  pMetaMsg = &(pShow->tableMeta);

  pMetaMsg->numOfColumns = ntohs(pMetaMsg->numOfColumns);

  pSchema = pMetaMsg->schema;
  pMetaMsg->sid = ntohs(pMetaMsg->sid);
  for (int i = 0; i < pMetaMsg->numOfColumns; ++i) {
    pSchema->bytes = htons(pSchema->bytes);
    pSchema++;
  }

  key[0] = pCmd->msgType + 'a';
  strcpy(key + 1, "showlist");

  taosCacheRelease(tscCacheHandle, (void *)&(pTableMetaInfo->pTableMeta), false);
  
  size_t size = 0;
  STableMeta* pTableMeta = tscCreateTableMetaFromMsg(pMetaMsg, &size);
  
  pTableMetaInfo->pTableMeta = taosCachePut(tscCacheHandle, key, (char *)pTableMeta, size, tsTableMetaKeepTimer);
  SSchema *pTableSchema = tscGetTableSchema(pTableMetaInfo->pTableMeta);

  if (pQueryInfo->colList == NULL) {
    pQueryInfo->colList = taosArrayInit(4, POINTER_BYTES);
  }
  
  SFieldInfo* pFieldInfo = &pQueryInfo->fieldsInfo;
  
  SColumnIndex index = {0};
  pSchema = pMetaMsg->schema;
  
  for (int16_t i = 0; i < pMetaMsg->numOfColumns; ++i, ++pSchema) {
    index.columnIndex = i;
    tscColumnListInsert(pQueryInfo->colList, &index);
    
    TAOS_FIELD f = tscCreateField(pSchema->type, pSchema->name, pSchema->bytes);
    SFieldSupInfo* pInfo = tscFieldInfoAppend(pFieldInfo, &f);
    
    pInfo->pSqlExpr = tscSqlExprAppend(pQueryInfo, TSDB_FUNC_TS_DUMMY, &index,
                     pTableSchema[i].type, pTableSchema[i].bytes, pTableSchema[i].bytes, false);
  }
  
  pCmd->numOfCols = pQueryInfo->fieldsInfo.numOfOutput;
  tscFieldInfoUpdateOffset(pQueryInfo);
  
  tfree(pTableMeta);
  return 0;
}

int tscProcessConnectRsp(SSqlObj *pSql) {
  char temp[TSDB_TABLE_ID_LEN * 2];
  STscObj *pObj = pSql->pTscObj;
  SSqlRes *pRes = &pSql->res;

  SCMConnectRsp *pConnect = (SCMConnectRsp *)pRes->pRsp;
  strcpy(pObj->acctId, pConnect->acctId);  // copy acctId from response
  int32_t len = sprintf(temp, "%s%s%s", pObj->acctId, TS_PATH_DELIMITER, pObj->db);

  assert(len <= tListLen(pObj->db));
  strncpy(pObj->db, temp, tListLen(pObj->db));
  
  if (pConnect->ipList.numOfIps > 0) 
    tscSetMgmtIpList(&pConnect->ipList);

  strcpy(pObj->sversion, pConnect->serverVersion);
  pObj->writeAuth = pConnect->writeAuth;
  pObj->superAuth = pConnect->superAuth;
  taosTmrReset(tscProcessActivityTimer, tsShellActivityTimer * 500, pObj, tscTmr, &pObj->pTimer);

  return 0;
}

int tscProcessUseDbRsp(SSqlObj *pSql) {
  STscObj *       pObj = pSql->pTscObj;
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pSql->cmd, 0, 0);

  strcpy(pObj->db, pTableMetaInfo->name);
  return 0;
}

int tscProcessDropDbRsp(SSqlObj *UNUSED_PARAM(pSql)) {
  taosCacheEmpty(tscCacheHandle);
  return 0;
}

int tscProcessDropTableRsp(SSqlObj *pSql) {
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pSql->cmd, 0, 0);

  STableMeta *pTableMeta = taosCacheAcquireByName(tscCacheHandle, pTableMetaInfo->name);
  if (pTableMeta == NULL) {
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
  tscTrace("%p force release metermeta after drop table:%s", pSql, pTableMetaInfo->name);
  taosCacheRelease(tscCacheHandle, (void **)&pTableMeta, true);

  if (pTableMetaInfo->pTableMeta) {
    taosCacheRelease(tscCacheHandle, (void **)&(pTableMetaInfo->pTableMeta), true);
  }

  return 0;
}

int tscProcessAlterTableMsgRsp(SSqlObj *pSql) {
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pSql->cmd, 0, 0);

  STableMeta *pTableMeta = taosCacheAcquireByName(tscCacheHandle, pTableMetaInfo->name);
  if (pTableMeta == NULL) { /* not in cache, abort */
    return 0;
  }

  tscTrace("%p force release metermeta in cache after alter-table: %s", pSql, pTableMetaInfo->name);
  taosCacheRelease(tscCacheHandle, (void **)&pTableMeta, true);

  if (pTableMetaInfo->pTableMeta) {
    bool isSuperTable = UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo);

    taosCacheRelease(tscCacheHandle, (void **)&(pTableMetaInfo->pTableMeta), true);
//    taosCacheRelease(tscCacheHandle, (void **)&(pTableMetaInfo->pMetricMeta), true);

    if (isSuperTable) {  // if it is a super table, reset whole query cache
      tscTrace("%p reset query cache since table:%s is stable", pSql, pTableMetaInfo->name);
      taosCacheEmpty(tscCacheHandle);
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

  SQueryTableRsp *pQuery = (SQueryTableRsp *)pRes->pRsp;
  pQuery->qhandle = htobe64(pQuery->qhandle);
  pRes->qhandle = pQuery->qhandle;

  pRes->data = NULL;
  tscResetForNextRetrieve(pRes);
  return 0;
}

int tscProcessRetrieveRspFromNode(SSqlObj *pSql) {
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

  SRetrieveTableRsp *pRetrieve = (SRetrieveTableRsp *)pRes->pRsp;

  pRes->numOfRows = htonl(pRetrieve->numOfRows);
  pRes->precision = htons(pRetrieve->precision);
  pRes->offset    = htobe64(pRetrieve->offset);
  pRes->useconds  = htobe64(pRetrieve->useconds);
  pRes->completed = (pRetrieve->completed == 1);
  pRes->data      = pRetrieve->data;
  
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  if (tscCreateResPointerInfo(pRes, pQueryInfo) != TSDB_CODE_SUCCESS) {
    return pRes->code;
  }
  
  if (pSql->pSubscription != NULL) {
    int32_t numOfCols = pQueryInfo->fieldsInfo.numOfOutput;
    
    TAOS_FIELD *pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, numOfCols - 1);
    int16_t     offset = tscFieldInfoGetOffset(pQueryInfo, numOfCols - 1);
    
    char* p = pRes->data + (pField->bytes + offset) * pRes->numOfRows;

    int32_t numOfTables = htonl(*(int32_t*)p);
    p += sizeof(int32_t);
    for (int i = 0; i < numOfTables; i++) {
      int64_t uid = htobe64(*(int64_t*)p);
      p += sizeof(int64_t);
      p += sizeof(int32_t); // skip tid
      TSKEY key = htobe64(*(TSKEY*)p);
      p += sizeof(TSKEY);
      tscUpdateSubscriptionProgress(pSql->pSubscription, uid, key);
    }
  }

  pRes->row = 0;
  tscTrace("%p numOfRows:%d, offset:%d", pSql, pRes->numOfRows, pRes->offset);

  return 0;
}

int tscProcessRetrieveRspFromLocal(SSqlObj *pSql) {
  SSqlRes *   pRes = &pSql->res;
  SSqlCmd *   pCmd = &pSql->cmd;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);

  SRetrieveTableRsp *pRetrieve = (SRetrieveTableRsp *)pRes->pRsp;

  pRes->numOfRows = htonl(pRetrieve->numOfRows);
  pRes->data = pRetrieve->data;

  tscSetResultPointer(pQueryInfo, pRes);
  pRes->row = 0;
  return 0;
}

void tscTableMetaCallBack(void *param, TAOS_RES *res, int code);

static int32_t getTableMetaFromMgmt(SSqlObj *pSql, STableMetaInfo *pTableMetaInfo) {
  SSqlObj *pNew = calloc(1, sizeof(SSqlObj));
  if (NULL == pNew) {
    tscError("%p malloc failed for new sqlobj to get table meta", pSql);
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }

  pNew->pTscObj = pSql->pTscObj;
  pNew->signature = pNew;
  pNew->cmd.command = TSDB_SQL_META;

  tscAddSubqueryInfo(&pNew->cmd);

  SQueryInfo *pNewQueryInfo = NULL;
  tscGetQueryInfoDetailSafely(&pNew->cmd, 0, &pNewQueryInfo);

  pNew->cmd.autoCreated = pSql->cmd.autoCreated;  // create table if not exists
  if (TSDB_CODE_SUCCESS != tscAllocPayload(&pNew->cmd, TSDB_DEFAULT_PAYLOAD_SIZE + pSql->cmd.payloadLen)) {
    tscError("%p malloc failed for payload to get table meta", pSql);
    free(pNew);

    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }

  STableMetaInfo *pNewMeterMetaInfo = tscAddEmptyMetaInfo(pNewQueryInfo);
  assert(pNew->cmd.numOfClause == 1 && pNewQueryInfo->numOfTables == 1);

  strncpy(pNewMeterMetaInfo->name, pTableMetaInfo->name, tListLen(pNewMeterMetaInfo->name));
  memcpy(pNew->cmd.payload, pSql->cmd.payload, pSql->cmd.payloadLen);  // tag information if table does not exists.
  pNew->cmd.payloadLen = pSql->cmd.payloadLen;
  tscTrace("%p new pSqlObj:%p to get tableMeta, auto create:%d", pSql, pNew, pNew->cmd.autoCreated);

  pNew->fp = tscTableMetaCallBack;
  pNew->param = pSql;

  int32_t code = tscProcessSql(pNew);
  if (code == TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_ACTION_IN_PROGRESS;
  }

  return code;
}

int32_t tscGetTableMeta(SSqlObj *pSql, STableMetaInfo *pTableMetaInfo) {
  assert(strlen(pTableMetaInfo->name) != 0);

  // If this STableMetaInfo owns a table meta, release it first
  if (pTableMetaInfo->pTableMeta != NULL) {
    taosCacheRelease(tscCacheHandle, (void **)&(pTableMetaInfo->pTableMeta), false);
  }
  
  pTableMetaInfo->pTableMeta = (STableMeta *)taosCacheAcquireByName(tscCacheHandle, pTableMetaInfo->name);
  if (pTableMetaInfo->pTableMeta != NULL) {
    STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);
    tscTrace("%p retrieve table Meta from cache, the number of columns:%d, numOfTags:%d, %p", pSql, tinfo.numOfColumns,
             tinfo.numOfTags, pTableMetaInfo->pTableMeta);

    return TSDB_CODE_SUCCESS;
  }
  
  return getTableMetaFromMgmt(pSql, pTableMetaInfo);
}

int tscGetMeterMetaEx(SSqlObj *pSql, STableMetaInfo *pTableMetaInfo, bool createIfNotExists) {
  pSql->cmd.autoCreated = createIfNotExists;
  return tscGetTableMeta(pSql, pTableMetaInfo);
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
 * @param tableId       meter id
 * @return              status code
 */
int tscRenewMeterMeta(SSqlObj *pSql, char *tableId) {
  int code = 0;

  // handle table meta renew process
  SSqlCmd *pCmd = &pSql->cmd;

  SQueryInfo *    pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);
  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  /*
   * 1. only update the metermeta in force model metricmeta is not updated
   * 2. if get metermeta failed, still get the metermeta
   */
  if (pTableMetaInfo->pTableMeta == NULL || !tscQueryOnSTable(pCmd)) {
    STableMeta* pTableMeta = pTableMetaInfo->pTableMeta;
    if (pTableMetaInfo->pTableMeta) {
      tscTrace("%p update table meta, old: numOfTags:%d, numOfCols:%d, uid:%" PRId64 ", addr:%p", pSql,
               tscGetNumOfTags(pTableMeta), tscGetNumOfColumns(pTableMeta), pTableMeta->uid, pTableMeta);
    }

    tscWaitingForCreateTable(pCmd);
    taosCacheRelease(tscCacheHandle, (void **)&(pTableMetaInfo->pTableMeta), true);

    code = getTableMetaFromMgmt(pSql, pTableMetaInfo);  // todo ??
  } else {
    tscTrace("%p metric query not update metric meta, numOfTags:%d, numOfCols:%d, uid:%" PRId64 ", addr:%p", pSql,
             tscGetNumOfTags(pTableMetaInfo->pTableMeta), pCmd->numOfCols, pTableMetaInfo->pTableMeta->uid,
             pTableMetaInfo->pTableMeta);
  }

  return code;
}

static bool allVgroupInfoRetrieved(SSqlCmd* pCmd, int32_t clauseIndex) {
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, clauseIndex);
  for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, i);
    if (pTableMetaInfo->vgroupList == NULL) {
      return false;
    }
  }
  
  // all super tables vgroupinfo are retrieved, no need to retrieve vgroup info anymore
  return true;
}

int tscGetSTableVgroupInfo(SSqlObj *pSql, int32_t clauseIndex) {
  int      code = TSDB_CODE_NETWORK_UNAVAIL;
  SSqlCmd *pCmd = &pSql->cmd;
  
  if (allVgroupInfoRetrieved(pCmd, clauseIndex)) {
    return TSDB_CODE_SUCCESS;
  }

  SSqlObj *pNew = calloc(1, sizeof(SSqlObj));
  pNew->pTscObj = pSql->pTscObj;
  pNew->signature = pNew;

  pNew->cmd.command = TSDB_SQL_STABLEVGROUP;
  
  SQueryInfo *pNewQueryInfo = NULL;
  if ((code = tscGetQueryInfoDetailSafely(&pNew->cmd, 0, &pNewQueryInfo)) != TSDB_CODE_SUCCESS) {
    return code;
  }
  
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, clauseIndex);
  for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    STableMetaInfo *pMInfo = tscGetMetaInfo(pQueryInfo, i);
    STableMeta *pTableMeta = taosCacheAcquireByData(tscCacheHandle, pMInfo->pTableMeta);
    tscAddTableMetaInfo(pNewQueryInfo, pMInfo->name, pTableMeta, NULL, pMInfo->tagColList);
  }

  if ((code = tscAllocPayload(&pNew->cmd, TSDB_DEFAULT_PAYLOAD_SIZE)) != TSDB_CODE_SUCCESS) {
    tscFreeSqlObj(pNew);
    return code;
  }

  pNewQueryInfo->numOfTables = pQueryInfo->numOfTables;
  tscTrace("%p new sqlObj:%p to get vgroupInfo, numOfTables:%d", pSql, pNew, pNewQueryInfo->numOfTables);

  pNew->fp = tscTableMetaCallBack;
  pNew->param = pSql;
  code = tscProcessSql(pNew);
  if (code == TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_ACTION_IN_PROGRESS;
  }

  return code;
}

void tscInitMsgsFp() {
  tscBuildMsg[TSDB_SQL_SELECT] = tscBuildQueryMsg;
  tscBuildMsg[TSDB_SQL_INSERT] = tscBuildSubmitMsg;
  tscBuildMsg[TSDB_SQL_FETCH] = tscBuildFetchMsg;

  tscBuildMsg[TSDB_SQL_CREATE_DB] = tscBuildCreateDbMsg;
  tscBuildMsg[TSDB_SQL_CREATE_USER] = tscBuildUserMsg;

  tscBuildMsg[TSDB_SQL_CREATE_ACCT] = tscBuildAcctMsg;
  tscBuildMsg[TSDB_SQL_ALTER_ACCT] = tscBuildAcctMsg;

  tscBuildMsg[TSDB_SQL_CREATE_TABLE] = tscBuildCreateTableMsg;
  tscBuildMsg[TSDB_SQL_DROP_USER] = tscBuildDropUserMsg;
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
  tscBuildMsg[TSDB_SQL_META] = tscBuildTableMetaMsg;
  tscBuildMsg[TSDB_SQL_STABLEVGROUP] = tscBuildSTableVgroupMsg;
  tscBuildMsg[TSDB_SQL_MULTI_META] = tscBuildMultiMeterMetaMsg;

  tscBuildMsg[TSDB_SQL_HB] = tscBuildHeartBeatMsg;
  tscBuildMsg[TSDB_SQL_SHOW] = tscBuildShowMsg;
  tscBuildMsg[TSDB_SQL_RETRIEVE] = tscBuildRetrieveFromMgmtMsg;
  tscBuildMsg[TSDB_SQL_KILL_QUERY] = tscBuildKillMsg;
  tscBuildMsg[TSDB_SQL_KILL_STREAM] = tscBuildKillMsg;
  tscBuildMsg[TSDB_SQL_KILL_CONNECTION] = tscBuildKillMsg;

  tscProcessMsgRsp[TSDB_SQL_SELECT] = tscProcessQueryRsp;
  tscProcessMsgRsp[TSDB_SQL_FETCH] = tscProcessRetrieveRspFromNode;

  tscProcessMsgRsp[TSDB_SQL_DROP_DB] = tscProcessDropDbRsp;
  tscProcessMsgRsp[TSDB_SQL_DROP_TABLE] = tscProcessDropTableRsp;
  tscProcessMsgRsp[TSDB_SQL_CONNECT] = tscProcessConnectRsp;
  tscProcessMsgRsp[TSDB_SQL_USE_DB] = tscProcessUseDbRsp;
  tscProcessMsgRsp[TSDB_SQL_META] = tscProcessTableMetaRsp;
  tscProcessMsgRsp[TSDB_SQL_STABLEVGROUP] = tscProcessSTableVgroupRsp;
  tscProcessMsgRsp[TSDB_SQL_MULTI_META] = tscProcessMultiMeterMetaRsp;

  tscProcessMsgRsp[TSDB_SQL_SHOW] = tscProcessShowRsp;
  tscProcessMsgRsp[TSDB_SQL_RETRIEVE] = tscProcessRetrieveRspFromNode;  // rsp handled by same function.
  tscProcessMsgRsp[TSDB_SQL_DESCRIBE_TABLE] = tscProcessDescribeTableRsp;

  tscProcessMsgRsp[TSDB_SQL_CURRENT_DB] = tscProcessTagRetrieveRsp;
  tscProcessMsgRsp[TSDB_SQL_CURRENT_USER] = tscProcessTagRetrieveRsp;
  tscProcessMsgRsp[TSDB_SQL_SERV_VERSION] = tscProcessTagRetrieveRsp;
  tscProcessMsgRsp[TSDB_SQL_CLI_VERSION] = tscProcessTagRetrieveRsp;
  tscProcessMsgRsp[TSDB_SQL_SERV_STATUS] = tscProcessTagRetrieveRsp;

  tscProcessMsgRsp[TSDB_SQL_RETRIEVE_EMPTY_RESULT] = tscProcessEmptyResultRsp;

  tscProcessMsgRsp[TSDB_SQL_RETRIEVE_LOCALMERGE] = tscProcessRetrieveLocalMergeRsp;

  tscProcessMsgRsp[TSDB_SQL_ALTER_TABLE] = tscProcessAlterTableMsgRsp;
  tscProcessMsgRsp[TSDB_SQL_ALTER_DB] = tscProcessAlterDbMsgRsp;

  tscKeepConn[TSDB_SQL_SHOW] = 1;
  tscKeepConn[TSDB_SQL_RETRIEVE] = 1;
  tscKeepConn[TSDB_SQL_SELECT] = 1;
  tscKeepConn[TSDB_SQL_FETCH] = 1;
  tscKeepConn[TSDB_SQL_HB] = 1;
}
