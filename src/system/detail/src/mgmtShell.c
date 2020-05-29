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

#include "dnodeSystem.h"
#include "mgmt.h"
#include "mgmtProfile.h"
#include "taosmsg.h"
#include "tlog.h"
#include "vnodeStatus.h"

#define MAX_LEN_OF_METER_META (sizeof(SMultiMeterMeta) + sizeof(SSchema) * TSDB_MAX_COLUMNS + sizeof(SSchema) * TSDB_MAX_TAGS + TSDB_MAX_TAGS_LEN)

void *    pShellConn = NULL;
SConnObj *connList;
void *    mgmtProcessMsgFromShell(char *msg, void *ahandle, void *thandle);
int     (*mgmtProcessShellMsg[TSDB_MSG_TYPE_MAX])(char *, int, SConnObj *);
void      mgmtInitProcessShellMsg();
int       mgmtRedirectMsg(SConnObj *pConn, int msgType);
int       mgmtKillQuery(char *queryId, SConnObj *pConn);

int mgmtCheckRedirectMsg(SConnObj *pConn, int msgType);
int mgmtProcessAlterAcctMsg(char *pMsg, int msgLen, SConnObj *pConn);
int mgmtProcessCreateMnodeMsg(char *pMsg, int msgLen, SConnObj *pConn);
int mgmtProcessCreateDnodeMsg(char *pMsg, int msgLen, SConnObj *pConn);
int mgmtProcessCfgMnodeMsg(char *pMsg, int msgLen, SConnObj *pConn);
int mgmtProcessDropMnodeMsg(char *pMsg, int msgLen, SConnObj *pConn);
int mgmtProcessDropDnodeMsg(char *pMsg, int msgLen, SConnObj *pConn);
int mgmtProcessDropAcctMsg(char *pMsg, int msgLen, SConnObj *pConn);
int mgmtProcessCreateAcctMsg(char *pMsg, int msgLen, SConnObj *pConn);
int mgmtProcessCfgDnodeMsg(char *pMsg, int msgLen, SConnObj *pConn);

void mgmtProcessTranRequest(SSchedMsg *pSchedMsg) {
  SIntMsg * pMsg = (SIntMsg *)(pSchedMsg->msg);
  SConnObj *pConn = (SConnObj *)(pSchedMsg->thandle);

  char *cont = (char *)pMsg->content + sizeof(SMgmtHead);
  int   contLen = pMsg->msgLen - sizeof(SIntMsg) - sizeof(SMgmtHead);

  if (pConn->pAcct) (*mgmtProcessShellMsg[pMsg->msgType])(cont, contLen, pConn);

  if (pSchedMsg->msg) free(pSchedMsg->msg);
}

int mgmtInitShell() {
  SRpcInit rpcInit;

  mgmtInitProcessShellMsg();

  int size = sizeof(SConnObj) * tsMaxShellConns;
  connList = (SConnObj *)malloc(size);
  if (connList == NULL) {
    mError("failed to malloc for connList to shell");
    return -1;
  }
  memset(connList, 0, size);

  int numOfThreads = tsNumOfCores * tsNumOfThreadsPerCore / 4.0;
  if (numOfThreads < 1) numOfThreads = 1;

  memset(&rpcInit, 0, sizeof(rpcInit));

  rpcInit.localIp = tsAnyIp ? "0.0.0.0" : tsPrivateIp;;
  rpcInit.localPort = tsMgmtShellPort;
  rpcInit.label = "MND-shell";
  rpcInit.numOfThreads = numOfThreads;
  rpcInit.fp = mgmtProcessMsgFromShell;
  rpcInit.bits = 20;
  rpcInit.numOfChanns = 1;
  rpcInit.sessionsPerChann = tsMaxShellConns;
  rpcInit.idMgmt = TAOS_ID_FREE;
  rpcInit.connType = TAOS_CONN_SOCKET_TYPE_S();
  rpcInit.idleTime = tsShellActivityTimer * 2000;
  rpcInit.qhandle = mgmtQhandle;
  rpcInit.afp = mgmtRetriveUserAuthInfo;
  rpcInit.ufp = mgmtGetSetUserAuthFailInfo;

  pShellConn = taosOpenRpc(&rpcInit);
  if (pShellConn == NULL) {
    mError("failed to init tcp connection to shell");
    return -1;
  }

  return 0;
}

void mgmtCleanUpShell() {
  if (pShellConn) taosCloseRpc(pShellConn);
  pShellConn = NULL;
  tfree(connList);
}

static void mgmtSetSchemaFromMeters(SSchema *pSchema, STabObj *pMeterObj, uint32_t numOfCols) {
  SSchema *pMeterSchema = (SSchema *)(pMeterObj->schema);
  for (int i = 0; i < numOfCols; ++i) {
    pSchema->type = pMeterSchema[i].type;
    strcpy(pSchema->name, pMeterSchema[i].name);
    pSchema->bytes = htons(pMeterSchema[i].bytes);
    pSchema->colId = htons(pMeterSchema[i].colId);
    pSchema++;
  }
}

static uint32_t mgmtSetMeterTagValue(char *pTags, STabObj *pMetric, STabObj *pMeterObj) {
  SSchema *pTagSchema = (SSchema *)(pMetric->schema + pMetric->numOfColumns * sizeof(SSchema));

  char *tagVal = pMeterObj->pTagData + TSDB_METER_ID_LEN;  // tag start position

  uint32_t tagsLen = 0;
  for (int32_t i = 0; i < pMetric->numOfTags; ++i) {
    tagsLen += pTagSchema[i].bytes;
  }

  memcpy(pTags, tagVal, tagsLen);
  return tagsLen;
}

static char *mgmtAllocMsg(SConnObj *pConn, int32_t size, char **pMsg, STaosRsp **pRsp) {
  char *pStart = taosBuildRspMsgWithSize(pConn->thandle, TSDB_MSG_TYPE_METERINFO_RSP, size);
  if (pStart == NULL) return 0;
  *pMsg = pStart;
  *pRsp = (STaosRsp *)(*pMsg);

  return pStart;
}

static char *mgmtForMultiAllocMsg(SConnObj *pConn, int32_t size, char **pMsg, STaosRsp **pRsp) {
  char *pStart = taosBuildRspMsgWithSize(pConn->thandle, TSDB_MSG_TYPE_MULTI_METERINFO_RSP, size);
  if (pStart == NULL) return 0;
  *pMsg = pStart;
  *pRsp = (STaosRsp *)(*pMsg);

  return pStart;
}

/**
 * check if we need to add mgmtProcessMeterMetaMsg into tranQueue, which will be executed one-by-one.
 *
 * @param pMsg
 * @return
 */
bool mgmtCheckMeterMetaMsgType(char *pMsg) {
  SMeterInfoMsg *pInfo = (SMeterInfoMsg *)pMsg;

  int16_t  autoCreate = htons(pInfo->createFlag);
  STabObj *pMeterObj = mgmtGetMeter(pInfo->meterId);

  // If table does not exists and autoCreate flag is set, we add the handler into another task queue, namely tranQueue
  bool addIntoTranQueue = (pMeterObj == NULL && autoCreate == 1);
  if (addIntoTranQueue) {
    mTrace("meter:%s auto created task added", pInfo->meterId);
  }

  return addIntoTranQueue;
}

int mgmtProcessMeterMetaMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  SMeterInfoMsg *pInfo = (SMeterInfoMsg *)pMsg;
  STabObj *      pMeterObj = NULL;
  SVgObj *       pVgroup = NULL;
  SMeterMeta *   pMeta = NULL;
  SSchema *      pSchema = NULL;
  STaosRsp *     pRsp = NULL;
  char *         pStart = NULL;

  pInfo->createFlag = htons(pInfo->createFlag);

  int size = sizeof(STaosHeader) + sizeof(STaosRsp) + sizeof(SMeterMeta) + sizeof(SSchema) * TSDB_MAX_COLUMNS +
             sizeof(SSchema) * TSDB_MAX_TAGS + TSDB_MAX_TAGS_LEN + TSDB_EXTRA_PAYLOAD_SIZE;

  SDbObj *pDb = NULL;
  if (pConn->pDb != NULL) pDb = mgmtGetDb(pConn->pDb->name);

  // todo db check should be extracted
  if (pDb == NULL || (pDb != NULL && pDb->dropStatus != TSDB_DB_STATUS_READY)) {

    if ((pStart = mgmtAllocMsg(pConn, size, &pMsg, &pRsp)) == NULL) {
      taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_METERINFO_RSP, TSDB_CODE_SERV_OUT_OF_MEMORY);
	  return 0;
    }

    pRsp->code = TSDB_CODE_INVALID_DB;
    pMsg++;

    goto _exit_code;
  }

  pMeterObj = mgmtGetMeter(pInfo->meterId);

  // on demand create table from super table if meter does not exists
  if (pMeterObj == NULL && pInfo->createFlag == 1) {
    // write operation needs to redirect to master mnode
    if (mgmtCheckRedirectMsg(pConn, TSDB_MSG_TYPE_METERINFO_RSP) != 0) {
      return 0;
    }

    SCreateTableMsg *pCreateMsg = calloc(1, sizeof(SCreateTableMsg) + sizeof(STagData));
    if (pCreateMsg == NULL) {
      taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_METERINFO_RSP, TSDB_CODE_SERV_OUT_OF_MEMORY);
      return 0;
    }

    memcpy(pCreateMsg->schema, pInfo->tags, sizeof(STagData));
    strcpy(pCreateMsg->meterId, pInfo->meterId);

    SDbObj* pMeterDb = mgmtGetDbByMeterId(pCreateMsg->meterId);
    mTrace("meter:%s, pConnDb:%p, pConnDbName:%s, pMeterDb:%p, pMeterDbName:%s",
           pCreateMsg->meterId, pDb, pDb->name, pMeterDb, pMeterDb->name);
    assert(pDb == pMeterDb);

    int32_t code = mgmtCreateMeter(pDb, pCreateMsg);

    char stableName[TSDB_METER_ID_LEN] = {0};
    strncpy(stableName, pInfo->tags, TSDB_METER_ID_LEN);
    mTrace("meter:%s is automatically created by %s from %s, code:%d", pCreateMsg->meterId, pConn->pUser->user,
           stableName, code);

    tfree(pCreateMsg);

    if (code != TSDB_CODE_SUCCESS) {
      if ((pStart = mgmtAllocMsg(pConn, size, &pMsg, &pRsp)) == NULL) {
 	    taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_METERINFO_RSP, TSDB_CODE_SERV_OUT_OF_MEMORY);
 	    return 0;
      }

      pRsp->code = code;
      pMsg++;

      goto _exit_code;
    }

    pMeterObj = mgmtGetMeter(pInfo->meterId);
  }

  if ((pStart = mgmtAllocMsg(pConn, size, &pMsg, &pRsp)) == NULL) {
    taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_METERINFO_RSP, TSDB_CODE_SERV_OUT_OF_MEMORY);
    return 0;
  }

  if (pMeterObj == NULL) {
    if (pDb)
      pRsp->code = TSDB_CODE_INVALID_TABLE;
    else
      pRsp->code = TSDB_CODE_DB_NOT_SELECTED;
    pMsg++;
  } else {
    mTrace("%s, uid:%" PRIu64 " meter meta is retrieved", pInfo->meterId, pMeterObj->uid);
    pRsp->code = 0;
    pMsg += sizeof(STaosRsp);
    *pMsg = TSDB_IE_TYPE_META;
    pMsg++;

    pMeta = (SMeterMeta *)pMsg;
    pMeta->uid = htobe64(pMeterObj->uid);
    pMeta->sid = htonl(pMeterObj->gid.sid);
    pMeta->vgid = htonl(pMeterObj->gid.vgId);
    pMeta->sversion = htons(pMeterObj->sversion);

    pMeta->precision = pDb->cfg.precision;

    pMeta->numOfTags = pMeterObj->numOfTags;
    pMeta->numOfColumns = htons(pMeterObj->numOfColumns);
    pMeta->meterType = pMeterObj->meterType;

    pMsg += sizeof(SMeterMeta);
    pSchema = (SSchema *)pMsg;  // schema locates at the end of SMeterMeta struct

    if (mgmtMeterCreateFromMetric(pMeterObj)) {
      assert(pMeterObj->numOfTags == 0);

      STabObj *pMetric = mgmtGetMeter(pMeterObj->pTagData);
      uint32_t numOfTotalCols = (uint32_t)pMetric->numOfTags + pMetric->numOfColumns;

      pMeta->numOfTags = pMetric->numOfTags;  // update the numOfTags info
      mgmtSetSchemaFromMeters(pSchema, pMetric, numOfTotalCols);
      pMsg += numOfTotalCols * sizeof(SSchema);

      // for meters created from metric, we need the metric tag schema to parse the tag data
      int32_t tagsLen = mgmtSetMeterTagValue(pMsg, pMetric, pMeterObj);
      pMsg += tagsLen;
    } else {
      /*
       * for metrics, or meters that are not created from metric, set the schema directly
       * for meters created from metric, we use the schema of metric instead
       */
      uint32_t numOfTotalCols = (uint32_t)pMeterObj->numOfTags + pMeterObj->numOfColumns;
      mgmtSetSchemaFromMeters(pSchema, pMeterObj, numOfTotalCols);
      pMsg += numOfTotalCols * sizeof(SSchema);
    }

    if (mgmtIsNormalMeter(pMeterObj)) {
      pVgroup = mgmtGetVgroup(pMeterObj->gid.vgId);
      if (pVgroup == NULL) {
        pRsp->code = TSDB_CODE_INVALID_TABLE;
        goto _exit_code;
      }
      for (int i = 0; i < TSDB_VNODES_SUPPORT; ++i) {
        if (pConn->usePublicIp) {
          pMeta->vpeerDesc[i].ip = pVgroup->vnodeGid[i].publicIp;
          pMeta->vpeerDesc[i].vnode = htonl(pVgroup->vnodeGid[i].vnode);
        } else {
          pMeta->vpeerDesc[i].ip = pVgroup->vnodeGid[i].ip;
          pMeta->vpeerDesc[i].vnode = htonl(pVgroup->vnodeGid[i].vnode);
        }
      }
    }
  }

_exit_code:
  msgLen = pMsg - pStart;

  taosSendMsgToPeer(pConn->thandle, pStart, msgLen);

  return msgLen;
}

/**
 *  multi meter meta rsp pkg format:
 *  | STaosRsp | ieType | SMultiMeterInfoMsg | SMeterMeta0 | SSchema0 | SMeterMeta1 | SSchema1 | SMeterMeta2 | SSchema2
 *      1B         1B            4B
 *
 *  | STaosHeader | STaosRsp | ieType | SMultiMeterInfoMsg | SMeterMeta0 | SSchema0 | SMeterMeta1 | SSchema1 | ......................|
 *                ^                                                                                          ^                       ^
 *                |<--------------------------------------size-----------------------------------------------|---------------------->|
 *                |                                                                                          |                       |
 *              pStart                                                                                   pCurMeter                 pTail
 **/
int mgmtProcessMultiMeterMetaMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  SDbObj *          pDbObj    = NULL;
  STabObj *         pMeterObj = NULL;
  SVgObj *          pVgroup   = NULL;
  SMultiMeterMeta * pMeta     = NULL;
  SSchema *         pSchema   = NULL;
  STaosRsp *        pRsp      = NULL;
  char *            pStart    = NULL;

  SMultiMeterInfoMsg * pInfo = (SMultiMeterInfoMsg *)pMsg;
  char *                 str = pMsg + sizeof(SMultiMeterInfoMsg);
  pInfo->numOfMeters         = htonl(pInfo->numOfMeters);

  int size = 4*1024*1024; // first malloc 4 MB, subsequent reallocation as twice

  char *pNewMsg;
  if ((pStart = mgmtForMultiAllocMsg(pConn, size, &pNewMsg, &pRsp)) == NULL) {
    taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_MULTI_METERINFO_RSP, TSDB_CODE_SERV_OUT_OF_MEMORY);
    return 0;
  }

  int32_t totalNum = 0;
  char  tblName[TSDB_METER_ID_LEN];
  char* nextStr;

  char* pCurMeter  = pStart + sizeof(STaosRsp) + sizeof(SMultiMeterInfoMsg) + 1;  // 1: ie type byte
  char* pTail      = pStart + size;

  while (str - pMsg < msgLen) {
    nextStr = strchr(str, ',');
    if (nextStr == NULL) {
      break;
    }

    memcpy(tblName, str, nextStr - str);
    tblName[nextStr - str] = '\0';
    str = nextStr + 1;

    // judge whether the remaining memory is adequate
    if ((pTail - pCurMeter) < MAX_LEN_OF_METER_META) {
      char* pMsgHdr = pStart - sizeof(STaosHeader);
      size *= 2;
      pMsgHdr = (char*)realloc(pMsgHdr, size);
      if (NULL == pMsgHdr) {
        char* pTmp = pStart - sizeof(STaosHeader);
        tfree(pTmp);
        taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_MULTI_METERINFO_RSP, TSDB_CODE_SERV_OUT_OF_MEMORY);
        break;
      }

      pCurMeter = (char*)pMsgHdr + sizeof(STaosHeader) + (pCurMeter - pStart);
      pStart    = (char*)pMsgHdr + sizeof(STaosHeader);
      pNewMsg   = pStart;
      pRsp      = (STaosRsp *)pStart;
      pTail     = pMsgHdr + size;      
    }

    // get meter schema, and fill into resp payload
    pMeterObj = mgmtGetMeter(tblName);
    pDbObj = mgmtGetDbByMeterId(tblName);

    if (pMeterObj == NULL || (pDbObj == NULL)) {
      continue;
    } else {
      mTrace("%s, uid:%" PRIu64 " sversion:%d meter meta is retrieved", tblName, pMeterObj->uid, pMeterObj->sversion);
      pMeta = (SMultiMeterMeta *)pCurMeter;

      memcpy(pMeta->meterId, tblName, strlen(tblName));
      pMeta->meta.uid = htobe64(pMeterObj->uid);
      pMeta->meta.sid = htonl(pMeterObj->gid.sid);
      pMeta->meta.vgid = htonl(pMeterObj->gid.vgId);
      pMeta->meta.sversion = htons(pMeterObj->sversion);
      pMeta->meta.precision = pDbObj->cfg.precision;
      pMeta->meta.numOfTags = pMeterObj->numOfTags;
      pMeta->meta.numOfColumns = htons(pMeterObj->numOfColumns);
      pMeta->meta.meterType = pMeterObj->meterType;

      pCurMeter += sizeof(SMultiMeterMeta);
      pSchema = (SSchema *)pCurMeter;  // schema locates at the end of SMeterMeta struct

      if (mgmtMeterCreateFromMetric(pMeterObj)) {
        assert(pMeterObj->numOfTags == 0);

        STabObj *pMetric = mgmtGetMeter(pMeterObj->pTagData);
        uint32_t numOfTotalCols = (uint32_t)pMetric->numOfTags + pMetric->numOfColumns;

        pMeta->meta.numOfTags = pMetric->numOfTags;  // update the numOfTags info
        mgmtSetSchemaFromMeters(pSchema, pMetric, numOfTotalCols);
        pCurMeter += numOfTotalCols * sizeof(SSchema);

        // for meters created from metric, we need the metric tag schema to parse the tag data
        int32_t tagsLen = mgmtSetMeterTagValue(pCurMeter, pMetric, pMeterObj);
        pCurMeter += tagsLen;
      } else {
        /*
         * for metrics, or meters that are not created from metric, set the schema directly
         * for meters created from metric, we use the schema of metric instead
         */
        uint32_t numOfTotalCols = (uint32_t)pMeterObj->numOfTags + pMeterObj->numOfColumns;
        mgmtSetSchemaFromMeters(pSchema, pMeterObj, numOfTotalCols);
        pCurMeter += numOfTotalCols * sizeof(SSchema);
      }

      if (mgmtIsNormalMeter(pMeterObj)) {
        pVgroup = mgmtGetVgroup(pMeterObj->gid.vgId);
        if (pVgroup == NULL) {
          pRsp->code = TSDB_CODE_INVALID_TABLE;
          pNewMsg++;
          mError("%s, uid:%" PRIu64 " sversion:%d vgId:%d pVgroup is NULL", tblName, pMeterObj->uid, pMeterObj->sversion,
                 pMeterObj->gid.vgId);
          goto _error_exit_code;
        }

        for (int i = 0; i < TSDB_VNODES_SUPPORT; ++i) {
          if (pConn->usePublicIp) {
            pMeta->meta.vpeerDesc[i].ip = pVgroup->vnodeGid[i].publicIp;
            pMeta->meta.vpeerDesc[i].vnode = htonl(pVgroup->vnodeGid[i].vnode);
          } else {
            pMeta->meta.vpeerDesc[i].ip = pVgroup->vnodeGid[i].ip;
            pMeta->meta.vpeerDesc[i].vnode = htonl(pVgroup->vnodeGid[i].vnode);
          }
        }
      }
    }

    totalNum++;
    if (totalNum > pInfo->numOfMeters) {
      pNewMsg++;
      break;
    }
  }

  // fill rsp code, ieType
  msgLen = pCurMeter - pNewMsg;

  pRsp->code = 0;
  pNewMsg += sizeof(STaosRsp);
  *pNewMsg = TSDB_IE_TYPE_META;
  pNewMsg++;

  SMultiMeterInfoMsg *pRspInfo = (SMultiMeterInfoMsg *)pNewMsg;

  pRspInfo->numOfMeters = htonl(totalNum);
  goto _exit_code;

_error_exit_code:
  msgLen = pNewMsg - pStart;

_exit_code:
  taosSendMsgToPeer(pConn->thandle, pStart, msgLen);

  return msgLen;
}

int mgmtProcessMetricMetaMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  SMetricMetaMsg *pMetricMetaMsg = (SMetricMetaMsg *)pMsg;
  STabObj *       pMetric;
  STaosRsp *      pRsp;
  char *          pStart;

  pMetricMetaMsg->numOfMeters = htonl(pMetricMetaMsg->numOfMeters);

  pMetricMetaMsg->join = htonl(pMetricMetaMsg->join);
  pMetricMetaMsg->joinCondLen = htonl(pMetricMetaMsg->joinCondLen);

  for (int32_t i = 0; i < pMetricMetaMsg->numOfMeters; ++i) {
    pMetricMetaMsg->metaElem[i] = htonl(pMetricMetaMsg->metaElem[i]);
  }

  SMetricMetaElemMsg *pElem = (SMetricMetaElemMsg *)(((char *)pMetricMetaMsg) + pMetricMetaMsg->metaElem[0]);
  pMetric = mgmtGetMeter(pElem->meterId);

  SDbObj *pDb = NULL;
  if (pConn->pDb != NULL) pDb = mgmtGetDb(pConn->pDb->name);

  if (pMetric == NULL || (pDb != NULL && pDb->dropStatus != TSDB_DB_STATUS_READY)) {
    pStart = taosBuildRspMsg(pConn->thandle, TSDB_MSG_TYPE_METRIC_META_RSP);
    if (pStart == NULL) {
      taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_METRIC_META_RSP, TSDB_CODE_SERV_OUT_OF_MEMORY);
      return 0;
    }

    pMsg = pStart;
    pRsp = (STaosRsp *)pMsg;
    if (pDb)
      pRsp->code = TSDB_CODE_INVALID_TABLE;
    else
      pRsp->code = TSDB_CODE_DB_NOT_SELECTED;
    pMsg++;

    msgLen = pMsg - pStart;
  } else {
    msgLen = mgmtRetrieveMetricMeta(pConn, &pStart, pMetricMetaMsg);
    if (msgLen <= 0) {
      taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_METRIC_META_RSP, TSDB_CODE_SERV_OUT_OF_MEMORY);
      return 0;
    }
  }

  taosSendMsgToPeer(pConn->thandle, pStart, msgLen);

  return msgLen;
}

int mgmtProcessCreateDbMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  SCreateDbMsg *pCreate = (SCreateDbMsg *)pMsg;
  int           code = 0;

  if (mgmtCheckRedirectMsg(pConn, TSDB_MSG_TYPE_CREATE_DB_RSP) != 0) {
    return 0;
  }

  pCreate->maxSessions = htonl(pCreate->maxSessions);
  pCreate->cacheBlockSize = htonl(pCreate->cacheBlockSize);
  // pCreate->cacheNumOfBlocks = htonl(pCreate->cacheNumOfBlocks);
  pCreate->daysPerFile = htonl(pCreate->daysPerFile);
  pCreate->daysToKeep = htonl(pCreate->daysToKeep);
  pCreate->daysToKeep1 = htonl(pCreate->daysToKeep1);
  pCreate->daysToKeep2 = htonl(pCreate->daysToKeep2);
  pCreate->commitTime = htonl(pCreate->commitTime);
  pCreate->blocksPerMeter = htons(pCreate->blocksPerMeter);
  pCreate->rowsInFileBlock = htonl(pCreate->rowsInFileBlock);

  if (grantCheckExpired()) {
    code = TSDB_CODE_GRANT_EXPIRED;
  } else if (!pConn->writeAuth) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    code = mgmtCreateDb(pConn->pAcct, pCreate);
    if (code == TSDB_CODE_SUCCESS) {
      mLPrint("DB:%s is created by %s", pCreate->db, pConn->pUser->user);
    }
  }

  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_CREATE_DB_RSP, code);

  return 0;
}

int mgmtProcessCreateMnodeMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  return taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_CREATE_MNODE_RSP, TSDB_CODE_OPS_NOT_SUPPORT);
}

int mgmtProcessAlterDbMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  SAlterDbMsg *pAlter = (SAlterDbMsg *)pMsg;
  int          code = 0;

  if (mgmtCheckRedirectMsg(pConn, TSDB_MSG_TYPE_ALTER_DB_RSP) != 0) {
    return 0;
  }

  pAlter->daysPerFile = htonl(pAlter->daysPerFile);
  pAlter->daysToKeep = htonl(pAlter->daysToKeep);
  pAlter->maxSessions = htonl(pAlter->maxSessions) + 1;

  if (!pConn->writeAuth) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    code = mgmtAlterDb(pConn->pAcct, pAlter);
    if (code == TSDB_CODE_SUCCESS) {
      mLPrint("DB:%s is altered by %s", pAlter->db, pConn->pUser->user);
    }
  }

  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_ALTER_DB_RSP, code);

  return 0;
}

int mgmtProcessKillQueryMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  int         code = 0;
  SKillQuery *pKill = (SKillQuery *)pMsg;

  if (!pConn->writeAuth) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    code = mgmtKillQuery(pKill->queryId, pConn);
  }

  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_KILL_QUERY_RSP, code);

  return 0;
}

int mgmtProcessKillStreamMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  int          code = 0;
  SKillStream *pKill = (SKillStream *)pMsg;

  if (!pConn->writeAuth) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    code = mgmtKillStream(pKill->queryId, pConn);
  }

  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_KILL_STREAM_RSP, code);

  return 0;
}

int mgmtProcessKillConnectionMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  int              code = 0;
  SKillConnection *pKill = (SKillConnection *)pMsg;

  if (!pConn->superAuth) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    code = mgmtKillConnection(pKill->queryId, pConn);
  }

  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_KILL_CONNECTION_RSP, code);

  return 0;
}

int mgmtProcessCreateUserMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  SCreateUserMsg *pCreate = (SCreateUserMsg *)pMsg;
  int             code = 0;

  if (mgmtCheckRedirectMsg(pConn, TSDB_MSG_TYPE_CREATE_USER_RSP) != 0) {
    return 0;
  }

  if (pConn->superAuth) {
    code = mgmtCreateUser(pConn->pAcct, pCreate->user, pCreate->pass);
    if (code == TSDB_CODE_SUCCESS) {
      mLPrint("user:%s is created by %s", pCreate->user, pConn->pUser->user);
    }
  } else {
    code = TSDB_CODE_NO_RIGHTS;
  }

  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_CREATE_USER_RSP, code);

  return 0;
}

int mgmtProcessAlterUserMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  SAlterUserMsg *pAlter = (SAlterUserMsg *)pMsg;
  int            code = 0;
  SUserObj *     pUser;
  SUserObj *     pOperUser;

  if (mgmtCheckRedirectMsg(pConn, TSDB_MSG_TYPE_ALTER_USER_RSP) != 0) {
    return 0;
  }

  pUser = mgmtGetUser(pAlter->user);
  pOperUser = mgmtGetUser(pConn->pUser->user);

  if (pUser == NULL) {
    taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_ALTER_USER_RSP, TSDB_CODE_INVALID_USER);
    return 0;
  }

  if (pOperUser == NULL) {
    taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_ALTER_USER_RSP, TSDB_CODE_INVALID_USER);
    return 0;
  }

  if (strcmp(pUser->user, "monitor") == 0 || (strcmp(pUser->user + 1, pUser->acct) == 0 && pUser->user[0] == '_')) {
    code = TSDB_CODE_NO_RIGHTS;
    taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_ALTER_USER_RSP, code);
    return 0;
  }

  if ((pAlter->flag & TSDB_ALTER_USER_PASSWD) != 0) {
    bool hasRight = false;
    if (strcmp(pOperUser->user, "root") == 0) {
      hasRight = true;
    } else if (strcmp(pUser->user, pOperUser->user) == 0) {
      hasRight = true;
    } else if (pOperUser->superAuth) {
      if (strcmp(pUser->user, "root") == 0) {
        hasRight = false;
      } else if (strcmp(pOperUser->acct, pUser->acct) != 0) {
        hasRight = false;
      } else {
        hasRight = true;
      }
    }

    if (hasRight) {
      memset(pUser->pass, 0, sizeof(pUser->pass));
      taosEncryptPass((uint8_t*)pAlter->pass, strlen(pAlter->pass), pUser->pass);
      code = mgmtUpdateUser(pUser);
      mLPrint("user:%s password is altered by %s, code:%d", pAlter->user, pConn->pUser->user, code);
    } else {
      code = TSDB_CODE_NO_RIGHTS;
    }

    taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_ALTER_USER_RSP, code);
    return 0;
  }

  if ((pAlter->flag & TSDB_ALTER_USER_PRIVILEGES) != 0) {
    bool hasRight = false;

    if (strcmp(pUser->user, "root") == 0) {
      hasRight = false;
    } else if (strcmp(pUser->user, pUser->acct) == 0) {
      hasRight = false;
    } else if (strcmp(pOperUser->user, "root") == 0) {
      hasRight = true;
    } else if (strcmp(pUser->user, pOperUser->user) == 0) {
      hasRight = false;
    } else if (pOperUser->superAuth) {
      if (strcmp(pUser->user, "root") == 0) {
        hasRight = false;
      } else if (strcmp(pOperUser->acct, pUser->acct) != 0) {
        hasRight = false;
      } else {
        hasRight = true;
      }
    }

    if (pAlter->privilege == 1) { // super
      hasRight = false;
    }

    if (hasRight) {
      //if (pAlter->privilege == 1) {  // super
      //  pUser->superAuth = 1;
      //  pUser->writeAuth = 1;
      //}
      if (pAlter->privilege == 2) {  // read
        pUser->superAuth = 0;
        pUser->writeAuth = 0;
      }
      if (pAlter->privilege == 3) {  // write
        pUser->superAuth = 0;
        pUser->writeAuth = 1;
      }

      code = mgmtUpdateUser(pUser);
      mLPrint("user:%s privilege is altered by %s, code:%d", pAlter->user, pConn->pUser->user, code);
    } else {
      code = TSDB_CODE_NO_RIGHTS;
    }

    taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_ALTER_USER_RSP, code);
    return 0;
  }

  code = TSDB_CODE_NO_RIGHTS;
  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_ALTER_USER_RSP, code);
  return 0;
}

int mgmtProcessDropUserMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  SDropUserMsg *pDrop = (SDropUserMsg *)pMsg;
  int           code = 0;
  SUserObj *    pUser;
  SUserObj *    pOperUser;

  if (mgmtCheckRedirectMsg(pConn, TSDB_MSG_TYPE_DROP_USER_RSP) != 0) {
    return 0;
  }

  pUser = mgmtGetUser(pDrop->user);
  pOperUser = mgmtGetUser(pConn->pUser->user);

  if (pUser == NULL) {
    taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_DROP_USER_RSP, TSDB_CODE_INVALID_USER);
    return 0;
  }

  if (pOperUser == NULL) {
    taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_DROP_USER_RSP, TSDB_CODE_INVALID_USER);
    return 0;
  }

  if (strcmp(pUser->user, "monitor") == 0 || (strcmp(pUser->user + 1, pUser->acct) == 0 && pUser->user[0] == '_')) {
    code = TSDB_CODE_NO_RIGHTS;
    taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_DROP_USER_RSP, code);
    return 0;
  }

  bool hasRight = false;
  if (strcmp(pUser->user, "root") == 0) {
    hasRight = false;
  } else if (strcmp(pOperUser->user, "root") == 0) {
    hasRight = true;
  } else if (strcmp(pUser->user, pOperUser->user) == 0) {
    hasRight = false;
  } else if (pOperUser->superAuth) {
    if (strcmp(pUser->user, "root") == 0) {
      hasRight = false;
    } else if (strcmp(pOperUser->acct, pUser->acct) != 0) {
      hasRight = false;
    } else {
      hasRight = true;
    }
  }

  if (hasRight) {
    code = mgmtDropUser(pConn->pAcct, pDrop->user);
    if (code == 0) {
      mLPrint("user:%s is dropped by %s", pDrop->user, pConn->pUser->user);
    }
  } else {
    code = TSDB_CODE_NO_RIGHTS;
  }

  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_DROP_USER_RSP, code);
  return 0;
}

int mgmtProcessDropDbMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  SDropDbMsg *pDrop = (SDropDbMsg *)pMsg;
  int         code;

  if (mgmtCheckRedirectMsg(pConn, TSDB_MSG_TYPE_DROP_DB_RSP) != 0) {
    return 0;
  }

  if (!pConn->writeAuth) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    code = mgmtDropDbByName(pConn->pAcct, pDrop->db, pDrop->ignoreNotExists);
    if (code == 0) {
      mLPrint("DB:%s is dropped by %s", pDrop->db, pConn->pUser->user);
    }
  }
  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_DROP_DB_RSP, code);

  return 0;
}

int mgmtProcessUseDbMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  SUseDbMsg *pUse = (SUseDbMsg *)pMsg;
  int        code;

  code = mgmtUseDb(pConn, pUse->db);
  if (code == 0) mTrace("DB is change to:%s by %s", pUse->db, pConn->pUser->user);

  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_USE_DB_RSP, code);

  return 0;
}

int (*mgmtGetMetaFp[])(SMeterMeta *pMeta, SShowObj *pShow, SConnObj *pConn) = {
    mgmtGetAcctMeta,   mgmtGetUserMeta,   mgmtGetDbMeta,     mgmtGetMeterMeta,  mgmtGetDnodeMeta,
    mgmtGetMnodeMeta,  mgmtGetVgroupMeta, mgmtGetMetricMeta, mgmtGetModuleMeta, mgmtGetQueryMeta,
    mgmtGetStreamMeta, mgmtGetConfigMeta, mgmtGetConnsMeta,  mgmtGetScoresMeta, grantGetGrantsMeta,
    mgmtGetVnodeMeta,
};

int (*mgmtRetrieveFp[])(SShowObj *pShow, char *data, int rows, SConnObj *pConn) = {
    mgmtRetrieveAccts,   mgmtRetrieveUsers,   mgmtRetrieveDbs,     mgmtRetrieveMeters,  mgmtRetrieveDnodes,
    mgmtRetrieveMnodes,  mgmtRetrieveVgroups, mgmtRetrieveMetrics, mgmtRetrieveModules, mgmtRetrieveQueries,
    mgmtRetrieveStreams, mgmtRetrieveConfigs, mgmtRetrieveConns,   mgmtRetrieveScores,  grantRetrieveGrants,
    mgmtRetrieveVnodes,
};

int mgmtProcessShowMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  SShowMsg *   pShowMsg = (SShowMsg *)pMsg;
  STaosRsp *   pRsp;
  char *       pStart;
  int          code = 0;
  SShowRspMsg *pShowRsp;
  SShowObj *   pShow = NULL;

  if (pShowMsg->type == TSDB_MGMT_TABLE_DNODE || TSDB_MGMT_TABLE_GRANTS || TSDB_MGMT_TABLE_SCORES) {
    if (mgmtCheckRedirectMsg(pConn, TSDB_MSG_TYPE_SHOW_RSP) != 0) {
      return 0;
    }
  }
  
  int size = sizeof(STaosHeader) + sizeof(STaosRsp) + sizeof(SShowRspMsg) + sizeof(SSchema) * TSDB_MAX_COLUMNS +
             TSDB_EXTRA_PAYLOAD_SIZE;
  pStart = taosBuildRspMsgWithSize(pConn->thandle, TSDB_MSG_TYPE_SHOW_RSP, size);
  if (pStart == NULL) {
    taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_SHOW_RSP, TSDB_CODE_SERV_OUT_OF_MEMORY);
    return 0;
  }
  
  pMsg = pStart;
  pRsp = (STaosRsp *)pMsg;
  pMsg = (char *)pRsp->more;

  if (pShowMsg->type >= TSDB_MGMT_TABLE_MAX) {
    code = -1;
  } else {
    pShow = (SShowObj *)calloc(1, sizeof(SShowObj) + htons(pShowMsg->payloadLen));
    pShow->signature = pShow;
    pShow->type = pShowMsg->type;
    mTrace("pShow:%p is allocated", pShow);

    // set the table name query condition
    pShow->payloadLen = htons(pShowMsg->payloadLen);
    memcpy(pShow->payload, pShowMsg->payload, pShow->payloadLen);

    pShowRsp = (SShowRspMsg *)pMsg;
    pShowRsp->qhandle = (uint64_t)pShow;  // qhandle;
    pConn->qhandle = pShowRsp->qhandle;

    code = (*mgmtGetMetaFp[(uint8_t)pShowMsg->type])(&pShowRsp->meterMeta, pShow, pConn);
    if (code == 0) {
      pMsg += sizeof(SShowRspMsg) + sizeof(SSchema) * pShow->numOfColumns;
    } else {
      mError("pShow:%p, type:%d %s, failed to get Meta, code:%d", pShow, pShowMsg->type, taosMsg[(uint8_t)pShowMsg->type], code);
      free(pShow);
    }
  }

  pRsp->code = code;
  msgLen = pMsg - pStart;
  taosSendMsgToPeer(pConn->thandle, pStart, msgLen);

  return msgLen;
}

int mgmtProcessRetrieveMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  SRetrieveMeterMsg *pRetrieve;
  SRetrieveMeterRsp *pRsp;
  int                rowsToRead = 0, size = 0, rowsRead = 0;
  char *             pStart;
  int                code = 0;
  SShowObj *         pShow;

  pRetrieve = (SRetrieveMeterMsg *)pMsg;

  /*
   * in case of server restart, apps may hold qhandle created by server before
   * restart, which is actually invalid, therefore, signature check is required.
   */
  if (pRetrieve->qhandle != pConn->qhandle) {
    mError("retrieve:%p, qhandle:%p is not matched with saved:%p", pRetrieve, pRetrieve->qhandle, pConn->qhandle);
    taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_RETRIEVE_RSP, TSDB_CODE_MEMORY_CORRUPTED);
    return -1;
  }

  pShow = (SShowObj *)pRetrieve->qhandle;
  if (pShow->signature != (void *)pShow) {
    mError("pShow:%p, signature:%p, query memory is corrupted", pShow, pShow->signature);
    taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_RETRIEVE_RSP, TSDB_CODE_MEMORY_CORRUPTED);
    return -1;
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

  pStart = taosBuildRspMsgWithSize(pConn->thandle, TSDB_MSG_TYPE_RETRIEVE_RSP, size + 100);
  if (pStart == NULL) {
    taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_RETRIEVE_RSP, TSDB_CODE_SERV_OUT_OF_MEMORY);
    return 0;
  }
  
  pMsg = pStart;

  STaosRsp *pTaosRsp = (STaosRsp *)pStart;
  pTaosRsp->code = code;
  pMsg = pTaosRsp->more;

  if (code == 0) {
    pRsp = (SRetrieveMeterRsp *)pMsg;
    pMsg = pRsp->data;

    // if free flag is set, client wants to clean the resources
    if ((pRetrieve->free & TSDB_QUERY_TYPE_FREE_RESOURCE) != TSDB_QUERY_TYPE_FREE_RESOURCE)
      rowsRead = (*mgmtRetrieveFp[(uint8_t)pShow->type])(pShow, pRsp->data, rowsToRead, pConn);

    if (rowsRead < 0) {
      rowsRead = 0;
      pTaosRsp->code = TSDB_CODE_ACTION_IN_PROGRESS;
    }

    pRsp->numOfRows = htonl(rowsRead);
    pRsp->precision = htonl(TSDB_TIME_PRECISION_MILLI);  // millisecond time precision
    pMsg += size;
  }

  msgLen = pMsg - pStart;
  taosSendMsgToPeer(pConn->thandle, pStart, msgLen);

  if (rowsToRead == 0) {
    uintptr_t oldSign = (uintptr_t)atomic_val_compare_exchange_ptr(&pShow->signature, pShow, 0);
    if (oldSign != (uintptr_t)pShow) {
      return msgLen;
    }
    // pShow->signature = 0;
    mTrace("pShow:%p is released", pShow);
    tfree(pShow);
  }

  return msgLen;
}

int mgmtProcessCreateTableMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  SCreateTableMsg *pCreate = (SCreateTableMsg *)pMsg;
  int              code;
  SSchema *        pSchema;

  if (mgmtCheckRedirectMsg(pConn, TSDB_MSG_TYPE_CREATE_TABLE_RSP) != 0) {
    return 0;
  }

  if (!pConn->writeAuth) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    pCreate->numOfColumns = htons(pCreate->numOfColumns);
    pCreate->numOfTags = htons(pCreate->numOfTags);

    pCreate->sqlLen = htons(pCreate->sqlLen);
    pSchema = pCreate->schema;
    for (int i = 0; i < pCreate->numOfColumns + pCreate->numOfTags; ++i) {
      pSchema->bytes = htons(pSchema->bytes);
      pSchema->colId = i;
      pSchema++;
    }

    SDbObj *pDb = NULL;
    if (pConn->pDb != NULL) pDb = mgmtGetDb(pConn->pDb->name);

    if (pDb) {
      code = mgmtCreateMeter(pDb, pCreate);
    } else {
      code = TSDB_CODE_DB_NOT_SELECTED;
    }
  }

  if (code == 1) {
    //mTrace("table:%s, wait vgroup create finish", pCreate->meterId, code);
  } else if (code != TSDB_CODE_SUCCESS) {
    if (code == TSDB_CODE_TABLE_ALREADY_EXIST) {  // table already created when the second attempt to create table
      
      STabObj* pMeter = mgmtGetMeter(pCreate->meterId);
      assert(pMeter != NULL);
      
      mWarn("table:%s, table already created, failed to create table, ts:%" PRId64 ", code:%d", pCreate->meterId,
            pMeter->createdTime, code);
    } else {  // other errors
      mError("table:%s, failed to create table, code:%d", pCreate->meterId, code);
    }
  } else {
    mTrace("table:%s, table is created by %s", pCreate->meterId, pConn->pUser->user);
  }

  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_CREATE_TABLE_RSP, code);

  return 0;
}

int mgmtProcessDropTableMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  SDropTableMsg *pDrop = (SDropTableMsg *)pMsg;
  int            code;

  if (mgmtCheckRedirectMsg(pConn, TSDB_MSG_TYPE_DROP_TABLE_RSP) != 0) {
    return 0;
  }

  if (!pConn->writeAuth) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    SDbObj *pDb = NULL;
    if (pConn->pDb != NULL) pDb = mgmtGetDb(pConn->pDb->name);

    code = mgmtDropMeter(pDb, pDrop->meterId, pDrop->igNotExists);
    if (code == 0) {
      mTrace("meter:%s is dropped by user:%s", pDrop->meterId, pConn->pUser->user);
      // mLPrint("meter:%s is dropped by user:%s", pDrop->meterId, pConn->pUser->user);
    }

    taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_DROP_TABLE_RSP, code);
  }

  return 0;
}

int mgmtProcessAlterTableMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  SAlterTableMsg *pAlter = (SAlterTableMsg *)pMsg;
  int             code;

  if (mgmtCheckRedirectMsg(pConn, TSDB_MSG_TYPE_ALTER_TABLE_RSP) != 0) {
    return 0;
  }

  if (!pConn->writeAuth) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    pAlter->type = htons(pAlter->type);
    pAlter->numOfCols = htons(pAlter->numOfCols);

    if (pAlter->numOfCols > 2) {
      mError("meter:%s error numOfCols:%d in alter table", pAlter->meterId, pAlter->numOfCols);
      code = TSDB_CODE_APP_ERROR;
    } else {
      SDbObj *pDb = NULL;
      if (pConn->pDb != NULL) pDb = mgmtGetDb(pConn->pDb->name);

      if (pDb) {
        for (int32_t i = 0; i < pAlter->numOfCols; ++i) {
          pAlter->schema[i].bytes = htons(pAlter->schema[i].bytes);
        }

        code = mgmtAlterMeter(pDb, pAlter);
        if (code == 0) {
          mLPrint("meter:%s is altered by %s", pAlter->meterId, pConn->pUser->user);
        }
      } else {
        code = TSDB_CODE_DB_NOT_SELECTED;
      }
    }
  }

  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_ALTER_TABLE_RSP, code);

  return 0;
}

int mgmtProcessCfgDnodeMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  int      code = 0;
  SCfgMsg *pCfg = (SCfgMsg *)pMsg;

  if (mgmtCheckRedirectMsg(pConn, TSDB_MSG_TYPE_CFG_MNODE_RSP) != 0) {
    return 0;
  }

  if (strcmp(pConn->pAcct->user, "root") != 0) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    code = mgmtSendCfgDnodeMsg(pMsg);
  }

  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_CFG_PNODE_RSP, code);

  if (code == 0) mTrace("dnode:%s is configured by %s", pCfg->ip, pConn->pUser->user);

  return 0;
}

int mgmtProcessHeartBeatMsg(char *cont, int contLen, SConnObj *pConn) {
  char *    pStart, *pMsg;
  int       msgLen;
  STaosRsp *pRsp;
  
  mgmtSaveQueryStreamList(cont, contLen, pConn);

  pStart = taosBuildRspMsgWithSize(pConn->thandle, TSDB_MSG_TYPE_HEARTBEAT_RSP, 128);
  if (pStart == NULL) return 0;
  pMsg = pStart;
  pRsp = (STaosRsp *)pMsg;
  pRsp->code = 0;
  pMsg = (char *)pRsp->more;

  SHeartBeatRsp *pHBRsp = (SHeartBeatRsp *)pRsp->more;
  pHBRsp->queryId = pConn->queryId;
  pConn->queryId = 0;
  pHBRsp->streamId = pConn->streamId;
  pHBRsp->streamId = pConn->streamId;
  pConn->streamId = 0;
  pHBRsp->killConnection = pConn->killConnection;

  mgmtGetDnodeOnlineNum(&pHBRsp->totalDnodes, &pHBRsp->onlineDnodes);
  pHBRsp->totalDnodes = htonl(pHBRsp->totalDnodes);
  pHBRsp->onlineDnodes = htonl(pHBRsp->onlineDnodes);

  if (pConn->usePublicIp) {
    if (pSdbPublicIpList != NULL) {
      int size = pSdbPublicIpList->numOfIps * 4;
      pHBRsp->ipList.numOfIps = pSdbPublicIpList->numOfIps;
      memcpy(pHBRsp->ipList.ip, pSdbPublicIpList->ip, size);
      pMsg += sizeof(SHeartBeatRsp) + size;
    } else {
      pHBRsp->ipList.numOfIps = 0;
      pMsg += sizeof(SHeartBeatRsp);
    }

  } else {
    if (pSdbIpList != NULL) {
      int size = pSdbIpList->numOfIps * 4;
      pHBRsp->ipList.numOfIps = pSdbIpList->numOfIps;
      memcpy(pHBRsp->ipList.ip, pSdbIpList->ip, size);
      pMsg += sizeof(SHeartBeatRsp) + size;
    } else {
      pHBRsp->ipList.numOfIps = 0;
      pMsg += sizeof(SHeartBeatRsp);
    }
  }
  msgLen = pMsg - pStart;

  taosSendMsgToPeer(pConn->thandle, pStart, msgLen);

  return 0;
}

void mgmtEstablishConn(SConnObj *pConn) {
  atomic_fetch_add_32(&mgmtShellConns, 1);
  atomic_fetch_add_32(&sdbExtConns, 1);
  pConn->stime = taosGetTimestampMs();

  if (strcmp(pConn->pUser->user, "root") == 0) {
    pConn->superAuth = 1;
    pConn->writeAuth = 1;
  } else {
    pConn->superAuth = pConn->pUser->superAuth;
    pConn->writeAuth = pConn->pUser->writeAuth;
    if (pConn->superAuth) {
      pConn->writeAuth = 1;
    }
  }

  int32_t tempint32;
  uint32_t tempuint32;
  taosGetRpcConnInfo(pConn->thandle, &tempuint32, &pConn->ip, &pConn->port, &tempint32, &tempint32);
  mgmtAddConnIntoAcct(pConn);
}

int mgmtRetriveUserAuthInfo(char *user, char *spi, char *encrypt, uint8_t *secret, uint8_t *ckey) {
  SUserObj *pUser = NULL;

  *spi = 0;
  *encrypt = 0;
  secret[0] = 0;
  ckey[0] = 0;

  pUser = mgmtGetUser(user);
  if (pUser == NULL) return TSDB_CODE_INVALID_USER;

  *spi = 1;
  *encrypt = 0;
  memcpy(secret, pUser->pass, TSDB_KEY_LEN);

  return 0;
}

int mgmtGetSetUserAuthFailInfo(char *user, int32_t *failCount, int32_t *allowTime, bool opSet) {
  SUserObj *pUser = NULL;

  pUser = mgmtGetUser(user);
  if (pUser == NULL) return TSDB_CODE_INVALID_USER;
  if (opSet) {
    pUser->authAllowTime = *allowTime;
    pUser->authFailCount = *failCount;
  }else {
    *allowTime = pUser->authAllowTime;
    *failCount = pUser->authFailCount;
  }  
  return 0;
}

int mgmtProcessConnectMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  STaosRsp *   pRsp;
  SConnectRsp *pConnectRsp;
  SConnectMsg *pConnectMsg;
  char *       pStart;
  int          code = TSDB_CODE_INVALID_USER;
  SAcctObj *   pAcct = NULL;
  SUserObj *   pUser = NULL;
  SDbObj *     pDb = NULL;
  char         dbName[256] = {0};

  pConnectMsg = (SConnectMsg *)pMsg;

  pUser = mgmtGetUser(pConn->user);
  if (pUser == NULL) {
    code = TSDB_CODE_INVALID_USER;
    goto _rsp;
  }

  if (grantCheckExpired()) {
    code = TSDB_CODE_GRANT_EXPIRED;
    goto _rsp;
  }

  pAcct = mgmtGetAcct(pUser->acct);

  code = taosCheckVersion(pConnectMsg->clientVersion, version, 3);
  if (code != 0) {
    mError("invalid client version:%s", pConnectMsg->clientVersion);
    goto _rsp;
  }

  if (pConnectMsg->db[0]) {
    sprintf(dbName, "%x%s%s", pAcct->acctId, TS_PATH_DELIMITER, pConnectMsg->db);
    pDb = mgmtGetDb(dbName);
    if (pDb == NULL) {
      code = TSDB_CODE_INVALID_DB;
      goto _rsp;
    }
  }

  if (pConn->pAcct) {
    mgmtRemoveConnFromAcct(pConn);
    atomic_fetch_sub_32(&mgmtShellConns, 1);
    atomic_fetch_sub_32(&sdbExtConns, 1);
  }

  code = 0;
  pConn->pAcct = pAcct;
  pConn->pDb = pDb;
  pConn->pUser = pUser;
  mgmtEstablishConn(pConn);
  
_rsp:
  pStart = taosBuildRspMsgWithSize(pConn->thandle, TSDB_MSG_TYPE_CONNECT_RSP, 128);
  if (pStart == NULL) return 0;

  pMsg = pStart;
  pRsp = (STaosRsp *)pMsg;
  pRsp->code = code;
  pMsg += sizeof(STaosRsp);
  
  pConnectRsp = (SConnectRsp *)pRsp->more;

  if (code == 0) {
    sprintf(pConnectRsp->acctId, "%x", pConn->pAcct->acctId);
    strcpy(pConnectRsp->version, version);
    pConnectRsp->writeAuth = pConn->writeAuth;
    pConnectRsp->superAuth = pConn->superAuth;
    pMsg += sizeof(SConnectRsp);

    int size;
    if (pSdbPublicIpList != NULL && pSdbIpList != NULL) {
      size = pSdbPublicIpList->numOfIps * 4 + sizeof(SIpList);
      if (pConn->usePublicIp) {
        memcpy(pMsg, pSdbPublicIpList, size);
      } else {
        memcpy(pMsg, pSdbIpList, size);
      }
    } else {
      SIpList tmpIpList;
      tmpIpList.numOfIps = 0;
      size = tmpIpList.numOfIps * 4 + sizeof(SIpList);
      memcpy(pMsg, &tmpIpList, size);
    }

    pMsg += size;

    // set the time resolution: millisecond or microsecond
    *((uint32_t *)pMsg) = tsTimePrecision;
    pMsg += sizeof(uint32_t);
  } else {
    pConnectRsp->writeAuth = 0;
    pConnectRsp->superAuth = 0;
    pConn->pAcct = NULL;
    pConn->pUser = NULL;
  }

  msgLen = pMsg - pStart;
  taosSendMsgToPeer(pConn->thandle, pStart, msgLen);

  char ipstr[24];
  tinet_ntoa(ipstr, pConn->ip);
  mLPrint("user:%s login from %s, code:%d", pConn->user, ipstr, code);

  return code;
}

void *mgmtProcessMsgFromShell(char *msg, void *ahandle, void *thandle) {
  SIntMsg * pMsg = (SIntMsg *)msg;
  SConnObj *pConn = (SConnObj *)ahandle;

  if (msg == NULL) {
    if (pConn) {
      mgmtRemoveConnFromAcct(pConn);
      atomic_fetch_sub_32(&mgmtShellConns, 1);
      atomic_fetch_sub_32(&sdbExtConns, 1);
      mTrace("connection from %s is closed", pConn->pUser->user);
      memset(pConn, 0, sizeof(SConnObj));
    }

    return NULL;
  }

#ifdef CLUSTER
  if (sdbInited == NULL || sdbStatus != SDB_STATUS_SERVING) {
    taosSendSimpleRsp(thandle, pMsg->msgType + 1, TSDB_CODE_NOT_READY);
    mTrace("shell msg is ignored since SDB is not ready");
  }
#endif

  if (pConn == NULL) {
    pConn = connList + pMsg->destId;
    pConn->thandle = thandle;
    strcpy(pConn->user, pMsg->meterId);
    pConn->usePublicIp = (pMsg->destIp == tsPublicIpInt ? 1 : 0);
    mTrace("pConn:%p is rebuild, destIp:%s publicIp:%s usePublicIp:%u",
            pConn, taosIpStr(pMsg->destIp), taosIpStr(tsPublicIpInt), pConn->usePublicIp);
  }

  if (pMsg->msgType == TSDB_MSG_TYPE_CONNECT) {
    (*mgmtProcessShellMsg[pMsg->msgType])((char *)pMsg->content, pMsg->msgLen - sizeof(SIntMsg), pConn);
  } else {
    SMgmtHead *pHead = (SMgmtHead *)pMsg->content;
    if (pConn->pAcct == NULL) {
      pConn->pUser = mgmtGetUser(pConn->user);
      if (pConn->pUser) {
        pConn->pAcct = mgmtGetAcct(pConn->pUser->acct);
        mgmtEstablishConn(pConn);
        mTrace("login from:%x:%hu", pConn->ip, htons(pConn->port));
      }
    }

    if (pConn->pAcct) {
      if (pConn->pDb == NULL || strncmp(pConn->pDb->name, pHead->db, tListLen(pConn->pDb->name)) != 0) {
        pConn->pDb = mgmtGetDb(pHead->db);
      }

      char *cont = (char *)pMsg->content + sizeof(SMgmtHead);
      int   contLen = pMsg->msgLen - sizeof(SIntMsg) - sizeof(SMgmtHead);

      // read-only request can be executed concurrently
      if ((pMsg->msgType == TSDB_MSG_TYPE_METERINFO && (!mgmtCheckMeterMetaMsgType(cont))) ||
          pMsg->msgType == TSDB_MSG_TYPE_METRIC_META || pMsg->msgType == TSDB_MSG_TYPE_RETRIEVE ||
          pMsg->msgType == TSDB_MSG_TYPE_SHOW || pMsg->msgType == TSDB_MSG_TYPE_MULTI_METERINFO) {
        (*mgmtProcessShellMsg[pMsg->msgType])(cont, contLen, pConn);
      } else {
        if (mgmtProcessShellMsg[pMsg->msgType]) {
          SSchedMsg schedMsg;
          schedMsg.msg = malloc(pMsg->msgLen);  // Message to deal with
          memcpy(schedMsg.msg, pMsg, pMsg->msgLen);

          schedMsg.fp = mgmtProcessTranRequest;
          schedMsg.tfp = NULL;
          schedMsg.thandle = pConn;

          taosScheduleTask(mgmtTranQhandle, &schedMsg);
        } else {
          mError("%s from shell is not processed", taosMsg[pMsg->msgType]);
        }
      }
    } else {
      taosSendSimpleRsp(thandle, pMsg->msgType + 1, TSDB_CODE_DISCONNECTED);
    }
  }

  if (pConn->pAcct == NULL) {
    taosCloseRpcConn(pConn->thandle);
    memset(pConn, 0, sizeof(SConnObj));  // close the connection;
    pConn = NULL;
  }

  return pConn;
}

void mgmtInitProcessShellMsg() {
  mgmtProcessShellMsg[TSDB_MSG_TYPE_METERINFO] = mgmtProcessMeterMetaMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_METRIC_META] = mgmtProcessMetricMetaMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_MULTI_METERINFO] = mgmtProcessMultiMeterMetaMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_CREATE_DB] = mgmtProcessCreateDbMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_ALTER_DB] = mgmtProcessAlterDbMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_CREATE_USER] = mgmtProcessCreateUserMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_ALTER_USER] = mgmtProcessAlterUserMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_CREATE_ACCT] = mgmtProcessCreateAcctMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_DROP_DB] = mgmtProcessDropDbMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_DROP_USER] = mgmtProcessDropUserMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_DROP_ACCT] = mgmtProcessDropAcctMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_ALTER_ACCT] = mgmtProcessAlterAcctMsg;

  mgmtProcessShellMsg[TSDB_MSG_TYPE_CREATE_TABLE] = mgmtProcessCreateTableMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_DROP_TABLE] = mgmtProcessDropTableMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_ALTER_TABLE] = mgmtProcessAlterTableMsg;

  mgmtProcessShellMsg[TSDB_MSG_TYPE_USE_DB] = mgmtProcessUseDbMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_RETRIEVE] = mgmtProcessRetrieveMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_SHOW] = mgmtProcessShowMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_CONNECT] = mgmtProcessConnectMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_HEARTBEAT] = mgmtProcessHeartBeatMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_CREATE_DNODE] = mgmtProcessCreateDnodeMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_DROP_DNODE] = mgmtProcessDropDnodeMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_CREATE_MNODE] = mgmtProcessCreateMnodeMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_DROP_MNODE] = mgmtProcessDropMnodeMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_CFG_MNODE] = mgmtProcessCfgMnodeMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_CFG_PNODE] = mgmtProcessCfgDnodeMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_KILL_QUERY] = mgmtProcessKillQueryMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_KILL_STREAM] = mgmtProcessKillStreamMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_KILL_CONNECTION] = mgmtProcessKillConnectionMsg;
}
