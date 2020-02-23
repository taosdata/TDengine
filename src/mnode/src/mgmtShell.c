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
#include "tlog.h"
#include "trpc.h"
#include "tstatus.h"
#include "tsched.h"
#include "dnodeSystem.h"
#include "mnode.h"
#include "mgmtAcct.h"
#include "mgmtBalance.h"
#include "mgmtConn.h"
#include "mgmtDb.h"
#include "mgmtDnode.h"
#include "mgmtGrant.h"
#include "mgmtMnode.h"
#include "mgmtProfile.h"
#include "mgmtShell.h"
#include "mgmtTable.h"
#include "mgmtUser.h"
#include "mgmtVgroup.h"

#define MAX_LEN_OF_METER_META (sizeof(SMultiMeterMeta) + sizeof(SCMSchema) * TSDB_MAX_COLUMNS + sizeof(SCMSchema) * TSDB_MAX_TAGS + TSDB_MAX_TAGS_LEN)

typedef int32_t (*GetMateFp)(SMeterMeta *pMeta, SShowObj *pShow, void *pConn);
typedef int32_t (*RetrieveMetaFp)(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static GetMateFp* mgmtGetMetaFp;
static RetrieveMetaFp* mgmtRetrieveFp;
static void mgmtInitShowMsgFp();

void *tsShellConnServer = NULL;

static void mgmtInitProcessShellMsg();
static void mgmtProcessMsgFromShell(char type, void *pCont, int contLen, void *ahandle, int32_t code);
static int32_t (*mgmtProcessShellMsg[TSDB_MSG_TYPE_MAX])(void *pCont, int32_t contLen, void *ahandle);
static int32_t mgmtProcessUnSupportMsg(void *pCont, int32_t contLen, void *ahandle);
static int32_t mgmtRetriveUserAuthInfo(char *user, char *spi, char *encrypt, char *secret, char *ckey);

void mgmtProcessTranRequest(SSchedMsg *sched) {
  int8_t  msgType = *(int8_t *) (sched->msg);
  int32_t contLen = *(int32_t *) (sched->msg + sizeof(int8_t));
  int8_t  *pCont  = sched->msg + sizeof(int32_t) + sizeof(int8_t);
  void    *pConn  = sched->thandle;

  (*mgmtProcessShellMsg[msgType])(pCont, contLen, pConn);
  if (sched->msg) {
    free(sched->msg);
  }
}

void mgmtAddToTranRequest(int8_t type, void *pCont, int contLen, void *ahandle) {
  SSchedMsg schedMsg;
  schedMsg.msg     = malloc(contLen + sizeof(int32_t) + sizeof(int8_t));
  schedMsg.fp      = mgmtProcessTranRequest;
  schedMsg.tfp     = NULL;
  schedMsg.thandle = ahandle;
  *(int8_t *) (schedMsg.msg) = type;
  *(int32_t *) (schedMsg.msg + sizeof(int8_t)) = contLen;
  memcpy(schedMsg.msg + sizeof(int32_t) + sizeof(int8_t), pCont, contLen);

  taosScheduleTask(tsMgmtTranQhandle, &schedMsg);
}

int32_t mgmtInitShell() {
  SRpcInit rpcInit;
  mgmtInitProcessShellMsg();
  mgmtInitShowMsgFp();

  int32_t numOfThreads = tsNumOfCores * tsNumOfThreadsPerCore / 4.0;
  if (numOfThreads < 1) {
    numOfThreads = 1;
  }

  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localIp      = tsAnyIp ? "0.0.0.0" : tsPrivateIp;;
  rpcInit.localPort    = tsMgmtShellPort;
  rpcInit.label        = "MND-shell";
  rpcInit.numOfThreads = numOfThreads;
  rpcInit.cfp          = mgmtProcessMsgFromShell;
  rpcInit.sessions     = tsMaxShellConns;
  rpcInit.connType     = TAOS_CONN_SERVER;
  rpcInit.idleTime     = tsShellActivityTimer * 2000;
  rpcInit.afp          = mgmtRetriveUserAuthInfo;

  tsShellConnServer = rpcOpen(&rpcInit);
  if (tsShellConnServer == NULL) {
    mError("failed to init tcp connection to shell");
    return -1;
  }

  return 0;
}

void mgmtCleanUpShell() {
  if (tsShellConnServer) {
    rpcClose(tsShellConnServer);
    tsShellConnServer = NULL;
  }
}

static void mgmtSetSchemaFromMeters(SCMSchema *pSchema, STabObj *pMeterObj, uint32_t numOfCols) {
  SCMSchema *pMeterSchema = (SCMSchema *)(pMeterObj->schema);
  for (int32_t i = 0; i < numOfCols; ++i) {
    strcpy(pSchema->name, pMeterSchema[i].name);
    pSchema->type  = pMeterSchema[i].type;
    pSchema->bytes = htons(pMeterSchema[i].bytes);
    pSchema->colId = htons(pMeterSchema[i].colId);
    pSchema++;
  }
}

static uint32_t mgmtSetMeterTagValue(char *pTags, STabObj *pMetric, STabObj *pMeterObj) {
  SCMSchema *pTagSchema = (SCMSchema *)(pMetric->schema + pMetric->numOfColumns * sizeof(SCMSchema));

  char *tagVal = pMeterObj->pTagData + TSDB_TABLE_ID_LEN;  // tag start position

  uint32_t tagsLen = 0;
  for (int32_t i = 0; i < pMetric->numOfTags; ++i) {
    tagsLen += pTagSchema[i].bytes;
  }

  memcpy(pTags, tagVal, tagsLen);
  return tagsLen;
}

int32_t mgmtProcessMeterMetaMsg(void *pCont, int32_t contLen, void *ahandle) {
//  SMeterInfoMsg *pInfo = (SMeterInfoMsg *)pMsg;
//  STabObj *      pMeterObj = NULL;
//  SVgObj *       pVgroup = NULL;
//  SMeterMeta *   pMeta = NULL;
//  SCMSchema *      pSchema = NULL;
//  STaosRsp *     pRsp = NULL;
//  char *         pStart = NULL;
//
//  pInfo->createFlag = htons(pInfo->createFlag);
//
//  int32_t size = sizeof(STaosHeader) + sizeof(STaosRsp) + sizeof(SMeterMeta) + sizeof(SCMSchema) * TSDB_MAX_COLUMNS +
//             sizeof(SCMSchema) * TSDB_MAX_TAGS + TSDB_MAX_TAGS_LEN + TSDB_EXTRA_PAYLOAD_SIZE;
//
//  SDbObj *pDb = NULL;
//  if (pConn->pDb != NULL) pDb = mgmtGetDb(pConn->pDb->name);
//
//  // todo db check should be extracted
//  if (pDb == NULL || (pDb != NULL && pDb->dropStatus != TSDB_DB_STATUS_READY)) {
//
//    if ((pStart = mgmtAllocMsg(pConn, size, &pMsg, &pRsp)) == NULL) {
//      taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_TABLE_META_RSP, TSDB_CODE_SERV_OUT_OF_MEMORY);
//	  return 0;
//    }
//
//    pRsp->code = TSDB_CODE_INVALID_DB;
//    pMsg++;
//
//    goto _exit_code;
//  }
//
//  pMeterObj = mgmtGetTable(pInfo->meterId);
//
//  // on demand create table from super table if meter does not exists
//  if (pMeterObj == NULL && pInfo->createFlag == 1) {
//    // write operation needs to redirect to master mnode
//    if (mgmtCheckRedirectMsg(pConn, TSDB_MSG_TYPE_TABLE_META_RSP) != 0) {
//      return 0;
//    }
//
//    SCMCreateTableMsg *pCreateMsg = calloc(1, sizeof(SCMCreateTableMsg) + sizeof(STagData));
//    if (pCreateMsg == NULL) {
//      taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_TABLE_META_RSP, TSDB_CODE_SERV_OUT_OF_MEMORY);
//      return 0;
//    }
//
//    memcpy(pCreateMsg->schema, pInfo->tags, sizeof(STagData));
//    strcpy(pCreateMsg->meterId, pInfo->meterId);
//
//    SDbObj* pMeterDb = mgmtGetDbByTableId(pCreateMsg->meterId);
//    mTrace("table:%s, pConnDb:%p, pConnDbName:%s, pMeterDb:%p, pMeterDbName:%s",
//           pCreateMsg->meterId, pDb, pDb->name, pMeterDb, pMeterDb->name);
//    assert(pDb == pMeterDb);
//
//    int32_t code = mgmtCreateTable(pDb, pCreateMsg);
//
//    char stableName[TSDB_TABLE_ID_LEN] = {0};
//    strncpy(stableName, pInfo->tags, TSDB_TABLE_ID_LEN);
//    mTrace("table:%s is automatically created by %s from %s, code:%d", pCreateMsg->meterId, pConn->pUser->user,
//           stableName, code);
//
//    tfree(pCreateMsg);
//
//    if (code != TSDB_CODE_SUCCESS) {
//      if ((pStart = mgmtAllocMsg(pConn, size, &pMsg, &pRsp)) == NULL) {
// 	    taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_TABLE_META_RSP, TSDB_CODE_SERV_OUT_OF_MEMORY);
// 	    return 0;
//      }
//
//      pRsp->code = code;
//      pMsg++;
//
//      goto _exit_code;
//    }
//
//    pMeterObj = mgmtGetTable(pInfo->meterId);
//  }
//
//  if ((pStart = mgmtAllocMsg(pConn, size, &pMsg, &pRsp)) == NULL) {
//    taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_TABLE_META_RSP, TSDB_CODE_SERV_OUT_OF_MEMORY);
//    return 0;
//  }
//
//  if (pMeterObj == NULL) {
//    if (pDb)
//      pRsp->code = TSDB_CODE_INVALID_TABLE;
//    else
//      pRsp->code = TSDB_CODE_DB_NOT_SELECTED;
//    pMsg++;
//  } else {
//    mTrace("%s, uid:%" PRIu64 " meter meta is retrieved", pInfo->meterId, pMeterObj->uid);
//    pRsp->code = 0;
//    pMsg += sizeof(STaosRsp);
//    *pMsg = TSDB_IE_TYPE_META;
//    pMsg++;
//
//    pMeta = (SMeterMeta *)pMsg;
//    pMeta->uid = htobe64(pMeterObj->uid);
//    pMeta->sid = htonl(pMeterObj->gid.sid);
//    pMeta->vgid = htonl(pMeterObj->gid.vgId);
//    pMeta->sversion = htons(pMeterObj->sversion);
//
//    pMeta->precision = pDb->cfg.precision;
//
//    pMeta->numOfTags = pMeterObj->numOfTags;
//    pMeta->numOfColumns = htons(pMeterObj->numOfColumns);
//    pMeta->tableType = pMeterObj->tableType;
//
//    pMsg += sizeof(SMeterMeta);
//    pSchema = (SCMSchema *)pMsg;  // schema locates at the end of SMeterMeta struct
//
//    if (mgmtTableCreateFromSuperTable(pMeterObj)) {
//      assert(pMeterObj->numOfTags == 0);
//
//      STabObj *pMetric = mgmtGetTable(pMeterObj->pTagData);
//      uint32_t numOfTotalCols = (uint32_t)pMetric->numOfTags + pMetric->numOfColumns;
//
//      pMeta->numOfTags = pMetric->numOfTags;  // update the numOfTags info
//      mgmtSetSchemaFromMeters(pSchema, pMetric, numOfTotalCols);
//      pMsg += numOfTotalCols * sizeof(SCMSchema);
//
//      // for meters created from metric, we need the metric tag schema to parse the tag data
//      int32_t tagsLen = mgmtSetMeterTagValue(pMsg, pMetric, pMeterObj);
//      pMsg += tagsLen;
//    } else {
//      /*
//       * for metrics, or meters that are not created from metric, set the schema directly
//       * for meters created from metric, we use the schema of metric instead
//       */
//      uint32_t numOfTotalCols = (uint32_t)pMeterObj->numOfTags + pMeterObj->numOfColumns;
//      mgmtSetSchemaFromMeters(pSchema, pMeterObj, numOfTotalCols);
//      pMsg += numOfTotalCols * sizeof(SCMSchema);
//    }
//
//    if (mgmtIsNormalTable(pMeterObj)) {
//      pVgroup = mgmtGetVgroup(pMeterObj->gid.vgId);
//      if (pVgroup == NULL) {
//        pRsp->code = TSDB_CODE_INVALID_TABLE;
//        goto _exit_code;
//      }
//      for (int32_t i = 0; i < TSDB_VNODES_SUPPORT; ++i) {
//        if (pConn->usePublicIp) {
//          pMeta->vpeerDesc[i].ip = pVgroup->vnodeGid[i].publicIp;
//          pMeta->vpeerDesc[i].vnode = htonl(pVgroup->vnodeGid[i].vnode);
//        } else {
//          pMeta->vpeerDesc[i].ip = pVgroup->vnodeGid[i].ip;
//          pMeta->vpeerDesc[i].vnode = htonl(pVgroup->vnodeGid[i].vnode);
//        }
//      }
//    }
//  }
//
//_exit_code:
//  msgLen = pMsg - pStart;
//
//  taosSendMsgToPeer(pConn->thandle, pStart, msgLen);
//
//  return msgLen;
  return 0;
}

/**
 *  multi meter meta rsp pkg format:
 *  | STaosRsp | ieType | SMultiMeterInfoMsg | SMeterMeta0 | SCMSchema0 | SMeterMeta1 | SCMSchema1 | SMeterMeta2 | SCMSchema2
 *      1B         1B            4B
 *
 *  | STaosHeader | STaosRsp | ieType | SMultiMeterInfoMsg | SMeterMeta0 | SCMSchema0 | SMeterMeta1 | SCMSchema1 | ......................|
 *                ^                                                                                          ^                       ^
 *                |<--------------------------------------size-----------------------------------------------|---------------------->|
 *                |                                                                                          |                       |
 *              pStart                                                                                   pCurMeter                 pTail
 **/
int32_t mgmtProcessMultiMeterMetaMsg(void *pCont, int32_t contLen, void *ahandle) {
//  SDbObj *          pDbObj    = NULL;
//  STabObj *         pMeterObj = NULL;
//  SVgObj *          pVgroup   = NULL;
//  SMultiMeterMeta * pMeta     = NULL;
//  SCMSchema *         pSchema   = NULL;
//  STaosRsp *        pRsp      = NULL;
//  char *            pStart    = NULL;
//
//  SMultiMeterInfoMsg * pInfo = (SMultiMeterInfoMsg *)pMsg;
//  char *                 str = pMsg + sizeof(SMultiMeterInfoMsg);
//  pInfo->numOfMeters         = htonl(pInfo->numOfMeters);
//
//  int32_t size = 4*1024*1024; // first malloc 4 MB, subsequent reallocation as twice
//
//  char *pNewMsg;
//  if ((pStart = mgmtForMultiAllocMsg(pConn, size, &pNewMsg, &pRsp)) == NULL) {
//    taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_MULTI_TABLE_META_RSP, TSDB_CODE_SERV_OUT_OF_MEMORY);
//    return 0;
//  }
//
//  int32_t totalNum = 0;
//  char  tblName[TSDB_TABLE_ID_LEN];
//  char* nextStr;
//
//  char* pCurMeter  = pStart + sizeof(STaosRsp) + sizeof(SMultiMeterInfoMsg) + 1;  // 1: ie type byte
//  char* pTail      = pStart + size;
//
//  while (str - pMsg < msgLen) {
//    nextStr = strchr(str, ',');
//    if (nextStr == NULL) {
//      break;
//    }
//
//    memcpy(tblName, str, nextStr - str);
//    tblName[nextStr - str] = '\0';
//    str = nextStr + 1;
//
//    // judge whether the remaining memory is adequate
//    if ((pTail - pCurMeter) < MAX_LEN_OF_METER_META) {
//      char* pMsgHdr = pStart - sizeof(STaosHeader);
//      size *= 2;
//      pMsgHdr = (char*)realloc(pMsgHdr, size);
//      if (NULL == pMsgHdr) {
//        char* pTmp = pStart - sizeof(STaosHeader);
//        tfree(pTmp);
//        taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_MULTI_TABLE_META_RSP, TSDB_CODE_SERV_OUT_OF_MEMORY);
//        break;
//      }
//
//      pCurMeter = (char*)pMsgHdr + sizeof(STaosHeader) + (pCurMeter - pStart);
//      pStart    = (char*)pMsgHdr + sizeof(STaosHeader);
//      pNewMsg   = pStart;
//      pRsp      = (STaosRsp *)pStart;
//      pTail     = pMsgHdr + size;
//    }
//
//    // get meter schema, and fill into resp payload
//    pMeterObj = mgmtGetTable(tblName);
//    pDbObj = mgmtGetDbByTableId(tblName);
//
//    if (pMeterObj == NULL || (pDbObj == NULL)) {
//      continue;
//    } else {
//      mTrace("%s, uid:%" PRIu64 " sversion:%d meter meta is retrieved", tblName, pMeterObj->uid, pMeterObj->sversion);
//      pMeta = (SMultiMeterMeta *)pCurMeter;
//
//      memcpy(pMeta->meterId, tblName, strlen(tblName));
//      pMeta->meta.uid = htobe64(pMeterObj->uid);
//      pMeta->meta.sid = htonl(pMeterObj->gid.sid);
//      pMeta->meta.vgid = htonl(pMeterObj->gid.vgId);
//      pMeta->meta.sversion = htons(pMeterObj->sversion);
//      pMeta->meta.precision = pDbObj->cfg.precision;
//      pMeta->meta.numOfTags = pMeterObj->numOfTags;
//      pMeta->meta.numOfColumns = htons(pMeterObj->numOfColumns);
//      pMeta->meta.tableType = pMeterObj->tableType;
//
//      pCurMeter += sizeof(SMultiMeterMeta);
//      pSchema = (SCMSchema *)pCurMeter;  // schema locates at the end of SMeterMeta struct
//
//      if (mgmtTableCreateFromSuperTable(pMeterObj)) {
//        assert(pMeterObj->numOfTags == 0);
//
//        STabObj *pMetric = mgmtGetTable(pMeterObj->pTagData);
//        uint32_t numOfTotalCols = (uint32_t)pMetric->numOfTags + pMetric->numOfColumns;
//
//        pMeta->meta.numOfTags = pMetric->numOfTags;  // update the numOfTags info
//        mgmtSetSchemaFromMeters(pSchema, pMetric, numOfTotalCols);
//        pCurMeter += numOfTotalCols * sizeof(SCMSchema);
//
//        // for meters created from metric, we need the metric tag schema to parse the tag data
//        int32_t tagsLen = mgmtSetMeterTagValue(pCurMeter, pMetric, pMeterObj);
//        pCurMeter += tagsLen;
//      } else {
//        /*
//         * for metrics, or meters that are not created from metric, set the schema directly
//         * for meters created from metric, we use the schema of metric instead
//         */
//        uint32_t numOfTotalCols = (uint32_t)pMeterObj->numOfTags + pMeterObj->numOfColumns;
//        mgmtSetSchemaFromMeters(pSchema, pMeterObj, numOfTotalCols);
//        pCurMeter += numOfTotalCols * sizeof(SCMSchema);
//      }
//
//      if (mgmtIsNormalTable(pMeterObj)) {
//        pVgroup = mgmtGetVgroup(pMeterObj->gid.vgId);
//        if (pVgroup == NULL) {
//          pRsp->code = TSDB_CODE_INVALID_TABLE;
//          pNewMsg++;
//          mError("%s, uid:%" PRIu64 " sversion:%d vgId:%d pVgroup is NULL", tblName, pMeterObj->uid, pMeterObj->sversion,
//                 pMeterObj->gid.vgId);
//          goto _error_exit_code;
//        }
//
//        for (int32_t i = 0; i < TSDB_VNODES_SUPPORT; ++i) {
//          if (pConn->usePublicIp) {
//            pMeta->meta.vpeerDesc[i].ip = pVgroup->vnodeGid[i].publicIp;
//            pMeta->meta.vpeerDesc[i].vnode = htonl(pVgroup->vnodeGid[i].vnode);
//          } else {
//            pMeta->meta.vpeerDesc[i].ip = pVgroup->vnodeGid[i].ip;
//            pMeta->meta.vpeerDesc[i].vnode = htonl(pVgroup->vnodeGid[i].vnode);
//          }
//        }
//      }
//    }
//
//    totalNum++;
//    if (totalNum > pInfo->numOfMeters) {
//      pNewMsg++;
//      break;
//    }
//  }
//
//  // fill rsp code, ieType
//  msgLen = pCurMeter - pNewMsg;
//
//  pRsp->code = 0;
//  pNewMsg += sizeof(STaosRsp);
//  *pNewMsg = TSDB_IE_TYPE_META;
//  pNewMsg++;
//
//  SMultiMeterInfoMsg *pRspInfo = (SMultiMeterInfoMsg *)pNewMsg;
//
//  pRspInfo->numOfMeters = htonl(totalNum);
//  goto _exit_code;
//
//_error_exit_code:
//  msgLen = pNewMsg - pStart;
//
//_exit_code:
//  taosSendMsgToPeer(pConn->thandle, pStart, msgLen);
//
//  return msgLen;
  return 0;
}

int32_t mgmtProcessMetricMetaMsg(void *pCont, int32_t contLen, void *ahandle) {
//  SSuperTableMetaMsg *pSuperTableMetaMsg = (SSuperTableMetaMsg *)pMsg;
//  STabObj *       pMetric;
//  STaosRsp *      pRsp;
//  char *          pStart;
//
//  pSuperTableMetaMsg->numOfMeters = htonl(pSuperTableMetaMsg->numOfMeters);
//
//  pSuperTableMetaMsg->join = htonl(pSuperTableMetaMsg->join);
//  pSuperTableMetaMsg->joinCondLen = htonl(pSuperTableMetaMsg->joinCondLen);
//
//  for (int32_t i = 0; i < pSuperTableMetaMsg->numOfMeters; ++i) {
//    pSuperTableMetaMsg->metaElem[i] = htonl(pSuperTableMetaMsg->metaElem[i]);
//  }
//
//  SMetricMetaElemMsg *pElem = (SMetricMetaElemMsg *)(((char *)pSuperTableMetaMsg) + pSuperTableMetaMsg->metaElem[0]);
//  pMetric = mgmtGetTable(pElem->meterId);
//
//  SDbObj *pDb = NULL;
//  if (pConn->pDb != NULL) pDb = mgmtGetDb(pConn->pDb->name);
//
//  if (pMetric == NULL || (pDb != NULL && pDb->dropStatus != TSDB_DB_STATUS_READY)) {
//    pStart = taosBuildRspMsg(pConn->thandle, TSDB_MSG_TYPE_STABLE_META_RSP);
//    if (pStart == NULL) {
//      taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_STABLE_META_RSP, TSDB_CODE_SERV_OUT_OF_MEMORY);
//      return 0;
//    }
//
//    pMsg = pStart;
//    pRsp = (STaosRsp *)pMsg;
//    if (pDb)
//      pRsp->code = TSDB_CODE_INVALID_TABLE;
//    else
//      pRsp->code = TSDB_CODE_DB_NOT_SELECTED;
//    pMsg++;
//
//    msgLen = pMsg - pStart;
//  } else {
//    msgLen = mgmtRetrieveMetricMeta(pConn, &pStart, pSuperTableMetaMsg);
//    if (msgLen <= 0) {
//      taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_STABLE_META_RSP, TSDB_CODE_SERV_OUT_OF_MEMORY);
//      return 0;
//    }
//  }
//
//  taosSendMsgToPeer(pConn->thandle, pStart, msgLen);
//
//  return msgLen;
  return 0;
}

int32_t mgmtProcessCreateDbMsg(void *pCont, int32_t contLen, void *ahandle) {
  if (mgmtCheckRedirectMsg(ahandle) != 0) {
    return TSDB_CODE_REDIRECT;
  }

  SUserObj *pUser = mgmtGetUserFromConn(ahandle);
  if (pUser == NULL) {
    rpcSendResponse(ahandle, TSDB_CODE_INVALID_USER, NULL, 0);
    return TSDB_CODE_INVALID_USER;
  }

  SCMCreateDbMsg *pCreate = (SCMCreateDbMsg *) pCont;

  pCreate->maxSessions     = htonl(pCreate->maxSessions);
  pCreate->cacheBlockSize  = htonl(pCreate->cacheBlockSize);
  pCreate->daysPerFile     = htonl(pCreate->daysPerFile);
  pCreate->daysToKeep      = htonl(pCreate->daysToKeep);
  pCreate->daysToKeep1     = htonl(pCreate->daysToKeep1);
  pCreate->daysToKeep2     = htonl(pCreate->daysToKeep2);
  pCreate->commitTime      = htonl(pCreate->commitTime);
  pCreate->blocksPerMeter  = htons(pCreate->blocksPerMeter);
  pCreate->rowsInFileBlock = htonl(pCreate->rowsInFileBlock);
  // pCreate->cacheNumOfBlocks = htonl(pCreate->cacheNumOfBlocks);

  int32_t code;
  if (mgmtCheckExpired()) {
    code = TSDB_CODE_GRANT_EXPIRED;
  } else if (!pUser->writeAuth) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    code = mgmtCreateDb(pUser->pAcct, pCreate);
    if (code == TSDB_CODE_SUCCESS) {
      mLPrint("DB:%s is created by %s", pCreate->db, pUser->user);
    }
  }

  rpcSendResponse(ahandle, code, NULL, 0);
  return code;
}

int32_t mgmtProcessAlterDbMsg(void *pCont, int32_t contLen, void *ahandle) {
  if (mgmtCheckRedirectMsg(ahandle) != 0) {
    return TSDB_CODE_REDIRECT;
  }

  SUserObj *pUser = mgmtGetUserFromConn(ahandle);
  if (pUser == NULL) {
    rpcSendResponse(ahandle, TSDB_CODE_INVALID_USER, NULL, 0);
    return TSDB_CODE_INVALID_USER;
  }

  SCMAlterDbMsg *pAlter = (SCMAlterDbMsg *) pCont;
  pAlter->daysPerFile = htonl(pAlter->daysPerFile);
  pAlter->daysToKeep  = htonl(pAlter->daysToKeep);
  pAlter->maxSessions = htonl(pAlter->maxSessions) + 1;

  int32_t code;
  if (!pUser->writeAuth) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    code = mgmtAlterDb(pUser->pAcct, pAlter);
    if (code == TSDB_CODE_SUCCESS) {
      mLPrint("DB:%s is altered by %s", pAlter->db, pUser->user);
    }
  }

  rpcSendResponse(ahandle, code, NULL, 0);
  return code;
}

int32_t mgmtProcessKillQueryMsg(void *pCont, int32_t contLen, void *ahandle) {
  if (mgmtCheckRedirectMsg(ahandle) != 0) {
    return TSDB_CODE_REDIRECT;
  }

  SUserObj *pUser = mgmtGetUserFromConn(ahandle);
  if (pUser == NULL) {
    rpcSendResponse(ahandle, TSDB_CODE_INVALID_USER, NULL, 0);
    return TSDB_CODE_INVALID_USER;
  }

  SCMKillQueryMsg *pKill = (SCMKillQueryMsg *) pCont;
  int32_t code;

  if (!pUser->writeAuth) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    code = mgmtKillQuery(pKill->queryId, ahandle);
  }

  rpcSendResponse(ahandle, code, NULL, 0);
  return code;
}

int32_t mgmtProcessKillStreamMsg(void *pCont, int32_t contLen, void *ahandle) {
  if (mgmtCheckRedirectMsg(ahandle) != 0) {
    return TSDB_CODE_REDIRECT;
  }

  SUserObj *pUser = mgmtGetUserFromConn(ahandle);
  if (pUser == NULL) {
    rpcSendResponse(ahandle, TSDB_CODE_INVALID_USER, NULL, 0);
    return TSDB_CODE_INVALID_USER;
  }

  SCMKillStreamMsg *pKill = (SCMKillStreamMsg *) pCont;
  int32_t code;

  if (!pUser->writeAuth) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    code = mgmtKillStream(pKill->queryId, ahandle);
  }

  rpcSendResponse(ahandle, code, NULL, 0);
  return code;
}

int32_t mgmtProcessKillConnectionMsg(void *pCont, int32_t contLen, void *ahandle) {
  if (mgmtCheckRedirectMsg(ahandle) != 0) {
    return TSDB_CODE_REDIRECT;
  }

  SUserObj *pUser = mgmtGetUserFromConn(ahandle);
  if (pUser == NULL) {
    rpcSendResponse(ahandle, TSDB_CODE_INVALID_USER, NULL, 0);
    return TSDB_CODE_INVALID_USER;
  }

  SKillConnectionMsg *pKill = (SKillConnectionMsg *) pCont;
  int32_t code;

  if (!pUser->writeAuth) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    code = mgmtKillConnection(pKill->queryId, ahandle);
  }

  rpcSendResponse(ahandle, code, NULL, 0);
  return code;
}

int32_t mgmtProcessCreateUserMsg(void *pCont, int32_t contLen, void *ahandle) {
  if (mgmtCheckRedirectMsg(ahandle) != 0) {
    return TSDB_CODE_REDIRECT;
  }

  SUserObj *pUser = mgmtGetUserFromConn(ahandle);
  if (pUser == NULL) {
    rpcSendResponse(ahandle, TSDB_CODE_INVALID_USER, NULL, 0);
    return TSDB_CODE_INVALID_USER;
  }

  int32_t code;
  if (pUser->superAuth) {
    SCMCreateUserMsg *pCreate = pCont;
    code = mgmtCreateUser(pUser->pAcct, pCreate->user, pCreate->pass);
    if (code == TSDB_CODE_SUCCESS) {
      mLPrint("user:%s is created by %s", pCreate->user, pUser->user);
    }
  } else {
    code = TSDB_CODE_NO_RIGHTS;
  }

  rpcSendResponse(ahandle, code, NULL, 0);
  return code;
}

int32_t mgmtProcessAlterUserMsg(void *pCont, int32_t contLen, void *ahandle) {
  if (mgmtCheckRedirectMsg(ahandle) != 0) {
    return TSDB_CODE_REDIRECT;
  }

  SUserObj *pOperUser = mgmtGetUserFromConn(ahandle);
  if (pOperUser == NULL) {
    rpcSendResponse(ahandle, TSDB_CODE_INVALID_USER, NULL, 0);
    return TSDB_CODE_INVALID_USER;
  }

  SCMAlterUserMsg *pAlter = pCont;
  SUserObj *pUser = mgmtGetUser(pAlter->user);
  if (pUser == NULL) {
    rpcSendResponse(ahandle, TSDB_CODE_INVALID_USER, NULL, 0);
    return TSDB_CODE_INVALID_USER;
  }

  if (strcmp(pUser->user, "monitor") == 0 || (strcmp(pUser->user + 1, pUser->acct) == 0 && pUser->user[0] == '_')) {
    rpcSendResponse(ahandle, TSDB_CODE_NO_RIGHTS, NULL, 0);
    return TSDB_CODE_NO_RIGHTS;
  }

  int code;
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
      mLPrint("user:%s password is altered by %s, code:%d", pAlter->user, pUser->user, code);
    } else {
      code = TSDB_CODE_NO_RIGHTS;
    }

    rpcSendResponse(ahandle, code, NULL, 0);
    return code;
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
      mLPrint("user:%s privilege is altered by %s, code:%d", pAlter->user, pUser->user, code);
    } else {
      code = TSDB_CODE_NO_RIGHTS;
    }

    rpcSendResponse(ahandle, code, NULL, 0);
    return code;
  }

  code = TSDB_CODE_NO_RIGHTS;
  rpcSendResponse(ahandle, code, NULL, 0);
  return code;
}

int32_t mgmtProcessDropUserMsg(void *pCont, int32_t contLen, void *ahandle) {
  if (mgmtCheckRedirectMsg(ahandle) != 0) {
    return TSDB_CODE_REDIRECT;
  }

  SUserObj *pOperUser = mgmtGetUserFromConn(ahandle);
  if (pOperUser == NULL) {
    rpcSendResponse(ahandle, TSDB_CODE_INVALID_USER, NULL, 0);
    return TSDB_CODE_INVALID_USER;
  }

  SCMDropUserMsg *pDrop = pCont;
  SUserObj *pUser = mgmtGetUser(pDrop->user);
  if (pUser == NULL) {
    rpcSendResponse(ahandle, TSDB_CODE_INVALID_USER, NULL, 0);
    return TSDB_CODE_INVALID_USER;
  }

  if (strcmp(pUser->user, "monitor") == 0 || (strcmp(pUser->user + 1, pUser->acct) == 0 && pUser->user[0] == '_')) {
    rpcSendResponse(ahandle, TSDB_CODE_NO_RIGHTS, NULL, 0);
    return TSDB_CODE_NO_RIGHTS;
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

  int32_t code;
  if (hasRight) {
    code = mgmtDropUser(pUser->pAcct, pDrop->user);
    if (code == TSDB_CODE_SUCCESS) {
      mLPrint("user:%s is dropped by %s", pDrop->user, pUser->user);
    }
  } else {
    code = TSDB_CODE_NO_RIGHTS;
  }

  rpcSendResponse(ahandle, code, NULL, 0);
  return code;
}

int32_t mgmtProcessDropDbMsg(void *pCont, int32_t contLen, void *ahandle) {
  if (mgmtCheckRedirectMsg(ahandle) != 0) {
    return TSDB_CODE_REDIRECT;
  }

  SUserObj *pUser = mgmtGetUserFromConn(ahandle);
  if (pUser == NULL) {
    rpcSendResponse(ahandle, TSDB_CODE_INVALID_USER, NULL, 0);
    return TSDB_CODE_INVALID_USER;
  }

  int32_t code;
  if (pUser->superAuth) {
    SCMDropDbMsg *pDrop = pCont;
    code = mgmtDropDbByName(pUser->pAcct, pDrop->db, pDrop->ignoreNotExists);
    if (code == TSDB_CODE_SUCCESS) {
      mLPrint("DB:%s is dropped by %s", pDrop->db, pUser->user);
    }
  } else {
    code = TSDB_CODE_NO_RIGHTS;
  }

  rpcSendResponse(ahandle, code, NULL, 0);
  return code;
}

static void mgmtInitShowMsgFp() {
  mgmtGetMetaFp = (GetMateFp *)malloc(TSDB_MGMT_TABLE_MAX * sizeof(GetMateFp));
  mgmtGetMetaFp[TSDB_MGMT_TABLE_ACCT] = mgmtGetAcctMeta;
  mgmtGetMetaFp[TSDB_MGMT_TABLE_USER] = mgmtGetUserMeta;
  mgmtGetMetaFp[TSDB_MGMT_TABLE_DB] = mgmtGetDbMeta;
  mgmtGetMetaFp[TSDB_MGMT_TABLE_TABLE] = mgmtGetTableMeta;
  mgmtGetMetaFp[TSDB_MGMT_TABLE_DNODE] = mgmtGetDnodeMeta;
  mgmtGetMetaFp[TSDB_MGMT_TABLE_MNODE] = mgmtGetMnodeMeta;
  mgmtGetMetaFp[TSDB_MGMT_TABLE_VGROUP] = mgmtGetVgroupMeta;
  mgmtGetMetaFp[TSDB_MGMT_TABLE_METRIC] = mgmtGetSuperTableMeta;
  mgmtGetMetaFp[TSDB_MGMT_TABLE_MODULE] = mgmtGetModuleMeta;
  mgmtGetMetaFp[TSDB_MGMT_TABLE_QUERIES] = mgmtGetQueryMeta;
  mgmtGetMetaFp[TSDB_MGMT_TABLE_STREAMS] = mgmtGetStreamMeta;
  mgmtGetMetaFp[TSDB_MGMT_TABLE_CONFIGS] = mgmtGetConfigMeta;
  mgmtGetMetaFp[TSDB_MGMT_TABLE_CONNS] = mgmtGetConnsMeta;
  mgmtGetMetaFp[TSDB_MGMT_TABLE_SCORES] = mgmtGetScoresMeta;
  mgmtGetMetaFp[TSDB_MGMT_TABLE_GRANTS] = mgmtGetGrantsMeta;
  mgmtGetMetaFp[TSDB_MGMT_TABLE_VNODES] = mgmtGetVnodeMeta;

  mgmtRetrieveFp = (RetrieveMetaFp *)malloc(TSDB_MGMT_TABLE_MAX * sizeof(RetrieveMetaFp));
  mgmtRetrieveFp[TSDB_MGMT_TABLE_ACCT] = mgmtRetrieveAccts;
  mgmtRetrieveFp[TSDB_MGMT_TABLE_USER] = mgmtRetrieveUsers;
  mgmtRetrieveFp[TSDB_MGMT_TABLE_DB] = mgmtRetrieveDbs;
  mgmtRetrieveFp[TSDB_MGMT_TABLE_TABLE] = mgmtRetrieveTables;
  mgmtRetrieveFp[TSDB_MGMT_TABLE_DNODE] = mgmtRetrieveDnodes;
  mgmtRetrieveFp[TSDB_MGMT_TABLE_MNODE] = mgmtRetrieveMnodes;
  mgmtRetrieveFp[TSDB_MGMT_TABLE_VGROUP] = mgmtRetrieveVgroups;
  mgmtRetrieveFp[TSDB_MGMT_TABLE_METRIC] = mgmtRetrieveSuperTables;
  mgmtRetrieveFp[TSDB_MGMT_TABLE_MODULE] = mgmtRetrieveModules;
  mgmtRetrieveFp[TSDB_MGMT_TABLE_QUERIES] = mgmtRetrieveQueries;
  mgmtRetrieveFp[TSDB_MGMT_TABLE_STREAMS] = mgmtRetrieveStreams;
  mgmtRetrieveFp[TSDB_MGMT_TABLE_CONFIGS] = mgmtRetrieveConfigs;
  mgmtRetrieveFp[TSDB_MGMT_TABLE_CONNS] = mgmtRetrieveConns;
  mgmtRetrieveFp[TSDB_MGMT_TABLE_SCORES] = mgmtRetrieveScores;
  mgmtRetrieveFp[TSDB_MGMT_TABLE_GRANTS] = mgmtRetrieveGrants;
  mgmtRetrieveFp[TSDB_MGMT_TABLE_VNODES] = mgmtRetrieveVnodes;
}

int32_t mgmtProcessShowMsg(void *pCont, int32_t contLen, void *ahandle) {
//  SShowMsg *   pShowMsg = (SShowMsg *)pMsg;
//  STaosRsp *   pRsp;
//  char *       pStart;
//  int32_t          code = 0;
//  SShowRspMsg *pShowRsp;
//  SShowObj *   pShow = NULL;
//
//  if (pShowMsg->type == TSDB_MGMT_TABLE_DNODE || TSDB_MGMT_TABLE_GRANTS || TSDB_MGMT_TABLE_SCORES) {
//    if (mgmtCheckRedirectMsg(pConn, TSDB_MSG_TYPE_SHOW_RSP) != 0) {
//      return 0;
//    }
//  }
//
//  int32_t size = sizeof(STaosHeader) + sizeof(STaosRsp) + sizeof(SShowRspMsg) + sizeof(SCMSchema) * TSDB_MAX_COLUMNS +
//             TSDB_EXTRA_PAYLOAD_SIZE;
//  pStart = taosBuildRspMsgWithSize(pConn->thandle, TSDB_MSG_TYPE_SHOW_RSP, size);
//  if (pStart == NULL) {
//    taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_SHOW_RSP, TSDB_CODE_SERV_OUT_OF_MEMORY);
//    return 0;
//  }
//
//  pMsg = pStart;
//  pRsp = (STaosRsp *)pMsg;
//  pMsg = (char *)pRsp->more;
//
//  if (pShowMsg->type >= TSDB_MGMT_TABLE_MAX) {
//    code = -1;
//  } else {
//    pShow = (SShowObj *)calloc(1, sizeof(SShowObj) + htons(pShowMsg->payloadLen));
//    pShow->signature = pShow;
//    pShow->type = pShowMsg->type;
//    mTrace("pShow:%p is allocated", pShow);
//
//    // set the table name query condition
//    pShow->payloadLen = htons(pShowMsg->payloadLen);
//    memcpy(pShow->payload, pShowMsg->payload, pShow->payloadLen);
//
//    pShowRsp = (SShowRspMsg *)pMsg;
//    pShowRsp->qhandle = (uint64_t)pShow;  // qhandle;
//    pConn->qhandle = pShowRsp->qhandle;
//
//    code = (*mgmtGetMetaFp[(uint8_t)pShowMsg->type])(&pShowRsp->meterMeta, pShow, pConn);
//    if (code == 0) {
//      pMsg += sizeof(SShowRspMsg) + sizeof(SCMSchema) * pShow->numOfColumns;
//    } else {
//      mError("pShow:%p, type:%d %s, failed to get Meta, code:%d", pShow, pShowMsg->type, taosMsg[(uint8_t)pShowMsg->type], code);
//      free(pShow);
//    }
//  }
//
//  pRsp->code = code;
//  msgLen = pMsg - pStart;
//  taosSendMsgToPeer(pConn->thandle, pStart, msgLen);
//
//  return msgLen;
  return 0;
}

int32_t mgmtProcessRetrieveMsg(void *pCont, int32_t contLen, void *ahandle) {
//  SRetrieveMeterMsg *pRetrieve;
//  SRetrieveMeterRsp *pRsp;
//  int32_t                rowsToRead = 0, size = 0, rowsRead = 0;
//  char *             pStart;
//  int32_t                code = 0;
//  SShowObj *         pShow;
//
//  pRetrieve = (SRetrieveMeterMsg *)pMsg;
//
//  /*
//   * in case of server restart, apps may hold qhandle created by server before
//   * restart, which is actually invalid, therefore, signature check is required.
//   */
//  if (pRetrieve->qhandle != pConn->qhandle) {
//    mError("retrieve:%p, qhandle:%p is not matched with saved:%p", pRetrieve, pRetrieve->qhandle, pConn->qhandle);
//    taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_DNODE_RETRIEVE_RSP, TSDB_CODE_MEMORY_CORRUPTED);
//    return -1;
//  }
//
//  pShow = (SShowObj *)pRetrieve->qhandle;
//  if (pShow->signature != (void *)pShow) {
//    mError("pShow:%p, signature:%p, query memory is corrupted", pShow, pShow->signature);
//    taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_DNODE_RETRIEVE_RSP, TSDB_CODE_MEMORY_CORRUPTED);
//    return -1;
//  } else {
//    if ((pRetrieve->free & TSDB_QUERY_TYPE_FREE_RESOURCE) != TSDB_QUERY_TYPE_FREE_RESOURCE) {
//      rowsToRead = pShow->numOfRows - pShow->numOfReads;
//    }
//
//    /* return no more than 100 meters in one round trip */
//    if (rowsToRead > 100) rowsToRead = 100;
//
//    /*
//     * the actual number of table may be larger than the value of pShow->numOfRows, if a query is
//     * issued during a continuous create table operation. Therefore, rowToRead may be less than 0.
//     */
//    if (rowsToRead < 0) rowsToRead = 0;
//    size = pShow->rowSize * rowsToRead;
//  }
//
//  pStart = taosBuildRspMsgWithSize(pConn->thandle, TSDB_MSG_TYPE_DNODE_RETRIEVE_RSP, size + 100);
//  if (pStart == NULL) {
//    taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_DNODE_RETRIEVE_RSP, TSDB_CODE_SERV_OUT_OF_MEMORY);
//    return 0;
//  }
//
//  pMsg = pStart;
//
//  STaosRsp *pTaosRsp = (STaosRsp *)pStart;
//  pTaosRsp->code = code;
//  pMsg = pTaosRsp->more;
//
//  if (code == 0) {
//    pRsp = (SRetrieveMeterRsp *)pMsg;
//    pMsg = pRsp->data;
//
//    // if free flag is set, client wants to clean the resources
//    if ((pRetrieve->free & TSDB_QUERY_TYPE_FREE_RESOURCE) != TSDB_QUERY_TYPE_FREE_RESOURCE)
//      rowsRead = (*mgmtRetrieveFp[(uint8_t)pShow->type])(pShow, pRsp->data, rowsToRead, pConn);
//
//    if (rowsRead < 0) {
//      rowsRead = 0;
//      pTaosRsp->code = TSDB_CODE_ACTION_IN_PROGRESS;
//    }
//
//    pRsp->numOfRows = htonl(rowsRead);
//    pRsp->precision = htonl(TSDB_TIME_PRECISION_MILLI);  // millisecond time precision
//    pMsg += size;
//  }
//
//  msgLen = pMsg - pStart;
//  taosSendMsgToPeer(pConn->thandle, pStart, msgLen);
//
//  if (rowsToRead == 0) {
//    uintptr_t oldSign = (uintptr_t)atomic_val_compare_exchange_ptr(&pShow->signature, pShow, 0);
//    if (oldSign != (uintptr_t)pShow) {
//      return msgLen;
//    }
//    // pShow->signature = 0;
//    mTrace("pShow:%p is released", pShow);
//    tfree(pShow);
//  }
//
//  return msgLen;
  return 0;
}

int32_t mgmtProcessCreateTableMsg(void *pCont, int32_t contLen, void *ahandle) {
  if (mgmtCheckRedirectMsg(ahandle) != 0) {
    return TSDB_CODE_REDIRECT;
  }

  SUserObj *pUser = mgmtGetUserFromConn(ahandle);
  if (pUser == NULL) {
    rpcSendResponse(ahandle, TSDB_CODE_INVALID_USER, NULL, 0);
    return TSDB_CODE_INVALID_USER;
  }

  SCMCreateTableMsg *pCreate = (SCMCreateTableMsg *) pCont;
  SCMSchema *pSchema;
  int32_t code;

  if (!pUser->writeAuth) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    pCreate->numOfColumns = htons(pCreate->numOfColumns);
    pCreate->numOfTags = htons(pCreate->numOfTags);
    pCreate->sqlLen = htons(pCreate->sqlLen);
    pSchema = pCreate->schema;
    for (int32_t i = 0; i < pCreate->numOfColumns + pCreate->numOfTags; ++i) {
      pSchema->bytes = htons(pSchema->bytes);
      pSchema->colId = i;
      pSchema++;
    }

    SDbObj *pDb = mgmtGetDb(pCreate->db);
    if (pDb) {
      code = mgmtCreateTable(pDb, pCreate);
      if (code == TSDB_CODE_TABLE_ALREADY_EXIST) {
        if (pCreate->igExists) {
          code = TSDB_CODE_SUCCESS;
        }
      }
    } else {
      code = TSDB_CODE_DB_NOT_SELECTED;
    }
  }

  if (code != TSDB_CODE_SUCCESS) {
    rpcSendResponse(ahandle, TSDB_CODE_SUCCESS, NULL, 0);
  }

  return code;
}

int32_t mgmtProcessDropTableMsg(void *pCont, int32_t contLen, void *ahandle) {
  if (mgmtCheckRedirectMsg(ahandle) != 0) {
    return TSDB_CODE_REDIRECT;
  }

  SUserObj *pUser = mgmtGetUserFromConn(ahandle);
  if (pUser == NULL) {
    rpcSendResponse(ahandle, TSDB_CODE_INVALID_USER, NULL, 0);
    return TSDB_CODE_INVALID_USER;
  }

  SDropTableMsg *pDrop = (SDropTableMsg *) pCont;
  int32_t code;

  if (!pUser->writeAuth) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    SDbObj *pDb = mgmtGetDb(pDrop->db);
    if (pDb) {
      code = mgmtDropTable(pDb, pDrop->meterId, pDrop->igNotExists);
      if (code == TSDB_CODE_SUCCESS) {
        mTrace("table:%s is dropped by user:%s", pDrop->meterId, pUser->user);
      }
    } else {
      code = TSDB_CODE_DB_NOT_SELECTED;
    }
  }

  if (code != TSDB_CODE_SUCCESS) {
    rpcSendResponse(ahandle, code, NULL, 0);
  }
  return code;
}

int32_t mgmtProcessAlterTableMsg(void *pCont, int32_t contLen, void *ahandle) {
  if (mgmtCheckRedirectMsg(ahandle) != 0) {
    return TSDB_CODE_REDIRECT;
  }

  SUserObj *pUser = mgmtGetUserFromConn(ahandle);
  if (pUser == NULL) {
    rpcSendResponse(ahandle, TSDB_CODE_INVALID_USER, NULL, 0);
    return TSDB_CODE_INVALID_USER;
  }

  SAlterTableMsg *pAlter = (SAlterTableMsg *) pCont;
  int32_t code;

  if (!pUser->writeAuth) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    pAlter->type      = htons(pAlter->type);
    pAlter->numOfCols = htons(pAlter->numOfCols);

    if (pAlter->numOfCols > 2) {
      mError("table:%s error numOfCols:%d in alter table", pAlter->tableId, pAlter->numOfCols);
      code = TSDB_CODE_APP_ERROR;
    } else {
      SDbObj *pDb = mgmtGetDb(pAlter->db);
      if (pDb) {
        for (int32_t i = 0; i < pAlter->numOfCols; ++i) {
          pAlter->schema[i].bytes = htons(pAlter->schema[i].bytes);
        }

        code = mgmtAlterTable(pDb, pAlter);
        if (code == 0) {
          mLPrint("table:%s is altered by %s", pAlter->tableId, pUser->user);
        }
      } else {
        code = TSDB_CODE_DB_NOT_SELECTED;
      }
    }
  }

  if (code != TSDB_CODE_SUCCESS) {
    rpcSendResponse(ahandle, code, NULL, 0);
  }
  return code;
}

int32_t mgmtProcessCfgDnodeMsg(void *pCont, int32_t contLen, void *ahandle) {
  if (mgmtCheckRedirectMsg(ahandle) != 0) {
    return TSDB_CODE_REDIRECT;
  }

  SUserObj *pUser = mgmtGetUserFromConn(ahandle);
  if (pUser == NULL) {
    rpcSendResponse(ahandle, TSDB_CODE_INVALID_USER, NULL, 0);
    return TSDB_CODE_INVALID_USER;
  }

  SCMCfgDnodeMsg *pCfg = (SCMCfgDnodeMsg *)pCont;
  int32_t code;

  if (strcmp(pUser->pAcct->user, "root") != 0) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    code = mgmtSendCfgDnodeMsg(pCont);
  }

  if (code == TSDB_CODE_SUCCESS) {
    mTrace("dnode:%s is configured by %s", pCfg->ip, pUser->user);
  }

  rpcSendResponse(ahandle, code, NULL, 0);
  return code;
}

int32_t mgmtProcessHeartBeatMsg(void *pCont, int32_t contLen, void *ahandle) {
  SCMHeartBeatMsg *pHBMsg = (SCMHeartBeatMsg *) pCont;
  mgmtSaveQueryStreamList(pHBMsg);

  SCMHeartBeatRsp *pHBRsp = (SCMHeartBeatRsp *) rpcMallocCont(contLen);
  if (pHBRsp == NULL) {
    rpcSendResponse(ahandle, TSDB_CODE_SERV_OUT_OF_MEMORY, NULL, 0);
    rpcFreeCont(pCont);
    return TSDB_CODE_SERV_OUT_OF_MEMORY;
  }

  SRpcConnInfo connInfo;
  rpcGetConnInfo(ahandle, &connInfo);

  pHBRsp->ipList.index = 0;
  pHBRsp->ipList.port = htons(tsMgmtShellPort);
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

  rpcSendResponse(ahandle, TSDB_CODE_SUCCESS, pHBRsp, sizeof(SCMHeartBeatMsg));
  return TSDB_CODE_SUCCESS;
}

int32_t mgmtRetriveUserAuthInfo(char *user, char *spi, char *encrypt, char *secret, char *ckey) {
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

static int32_t mgmtProcessConnectMsg(void *pCont, int32_t contLen, void *thandle) {
  SCMConnectMsg *pConnectMsg = (SCMConnectMsg *) pCont;
  SRpcConnInfo connInfo;
  rpcGetConnInfo(thandle, &connInfo);
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
  pConnectRsp->ipList.index = 0;
  pConnectRsp->ipList.port = htons(tsMgmtShellPort);
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
  if (code != TSDB_CODE_SUCCESS) {
    mLError("user:%s login from %s, code:%d", connInfo.user, taosIpStr(connInfo.clientIp), code);
    rpcSendResponse(thandle, code, NULL, 0);
  } else {
    mLPrint("user:%s login from %s, code:%d", connInfo.user, taosIpStr(connInfo.clientIp), code);
    rpcSendResponse(thandle, code, pConnectRsp, sizeof(SCMConnectRsp));
  }

  return code;
}

/**
 * check if we need to add mgmtProcessMeterMetaMsg into tranQueue, which will be executed one-by-one.
 */
static bool mgmtCheckMeterMetaMsgType(void *pMsg) {
  SMeterInfoMsg *pInfo = (SMeterInfoMsg *) pMsg;
  int16_t autoCreate = htons(pInfo->createFlag);
  STableInfo *pTable = mgmtGetTable(pInfo->meterId);

  // If table does not exists and autoCreate flag is set, we add the handler into task queue
  bool addIntoTranQueue = (pTable == NULL && autoCreate == 1);
  if (addIntoTranQueue) {
    mTrace("table:%s auto created task added", pInfo->meterId);
  }

  return addIntoTranQueue;
}

static bool mgmtCheckMsgReadOnly(int8_t type, void *pCont) {
  if ((type == TSDB_MSG_TYPE_TABLE_META && (!mgmtCheckMeterMetaMsgType(pCont)))  ||
       type == TSDB_MSG_TYPE_STABLE_META || type == TSDB_MSG_TYPE_DNODE_RETRIEVE ||
       type == TSDB_MSG_TYPE_SHOW || type == TSDB_MSG_TYPE_MULTI_TABLE_META      ||
       type == TSDB_MSG_TYPE_CONNECT) {
    return true;
  }

  return false;
}

static void mgmtProcessMsgFromShell(char type, void *pCont, int contLen, void *ahandle, int32_t code) {
  if (sdbGetRunStatus() != SDB_STATUS_SERVING) {
    mTrace("shell msg is ignored since SDB is not ready");
    rpcSendResponse(ahandle, TSDB_CODE_NOT_READY, NULL, 0);
    rpcFreeCont(pCont);
    return;
  }

  if (mgmtCheckMsgReadOnly(type, pCont)) {
    (*mgmtProcessShellMsg[(int8_t)type])(pCont, contLen, ahandle);
  } else {
    if (mgmtProcessShellMsg[(int8_t)type]) {
      mgmtAddToTranRequest((int8_t)type, pCont, contLen, ahandle);
    } else {
      mError("%s from shell is not processed", taosMsg[(int8_t)type]);
    }
  }
  rpcFreeCont(pCont);
}

void mgmtInitProcessShellMsg() {
  mgmtProcessShellMsg[TSDB_MSG_TYPE_CONNECT]          = mgmtProcessConnectMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_HEARTBEAT]        = mgmtProcessHeartBeatMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_CREATE_DB]        = mgmtProcessCreateDbMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_ALTER_DB]         = mgmtProcessAlterDbMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_DROP_DB]          = mgmtProcessDropDbMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_USE_DB]           = mgmtProcessUnSupportMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_CREATE_USER]      = mgmtProcessCreateUserMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_ALTER_USER]       = mgmtProcessAlterUserMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_DROP_USER]        = mgmtProcessDropUserMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_CREATE_ACCT]      = mgmtProcessCreateAcctMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_DROP_ACCT]        = mgmtProcessDropAcctMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_ALTER_ACCT]       = mgmtProcessAlterAcctMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_CREATE_TABLE]     = mgmtProcessCreateTableMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_DROP_TABLE]       = mgmtProcessDropTableMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_ALTER_TABLE]      = mgmtProcessAlterTableMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_CREATE_DNODE]     = mgmtProcessCreateDnodeMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_DROP_DNODE]       = mgmtProcessDropDnodeMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_DNODE_CFG]        = mgmtProcessCfgDnodeMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_CREATE_MNODE]     = mgmtProcessUnSupportMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_DROP_MNODE]       = mgmtProcessDropMnodeMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_CFG_MNODE]        = mgmtProcessCfgMnodeMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_KILL_QUERY]       = mgmtProcessKillQueryMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_KILL_STREAM]      = mgmtProcessKillStreamMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_KILL_CONNECTION]  = mgmtProcessKillConnectionMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_SHOW]             = mgmtProcessShowMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_DNODE_RETRIEVE]   = mgmtProcessRetrieveMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_TABLE_META]       = mgmtProcessMeterMetaMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_STABLE_META]      = mgmtProcessMetricMetaMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_MULTI_TABLE_META] = mgmtProcessMultiMeterMetaMsg;
}

static int32_t mgmtCheckRedirectMsgImp(void *pConn) {
  return 0;
}

int32_t (*mgmtCheckRedirectMsg)(void *pConn) = mgmtCheckRedirectMsgImp;

static int32_t mgmtProcessUnSupportMsg(void *pCont, int32_t contLen, void *ahandle) {
  rpcSendResponse(ahandle, TSDB_CODE_OPS_NOT_SUPPORT, NULL, 0);
  return TSDB_CODE_OPS_NOT_SUPPORT;
}

int32_t (*mgmtProcessAlterAcctMsg)(void *pCont, int32_t contLen, void *ahandle)   = mgmtProcessUnSupportMsg;
int32_t (*mgmtProcessCreateDnodeMsg)(void *pCont, int32_t contLen, void *ahandle) = mgmtProcessUnSupportMsg;
int32_t (*mgmtProcessCfgMnodeMsg)(void *pCont, int32_t contLen, void *ahandle)    = mgmtProcessUnSupportMsg;
int32_t (*mgmtProcessDropMnodeMsg)(void *pCont, int32_t contLen, void *ahandle)   = mgmtProcessUnSupportMsg;
int32_t (*mgmtProcessDropDnodeMsg)(void *pCont, int32_t contLen, void *ahandle)   = mgmtProcessUnSupportMsg;
int32_t (*mgmtProcessDropAcctMsg)(void *pCont, int32_t contLen, void *ahandle)    = mgmtProcessUnSupportMsg;
int32_t (*mgmtProcessCreateAcctMsg)(void *pCont, int32_t contLen, void *ahandle)  = mgmtProcessUnSupportMsg;