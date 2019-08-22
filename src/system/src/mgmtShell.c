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

#include <arpa/inet.h>
#include <mgmt.h>
#include <taosmsg.h>
#include "taosmsg.h"

#include "dnodeSystem.h"
#include "mgmt.h"
#include "mgmtProfile.h"
#include "tlog.h"
#pragma GCC diagnostic ignored  "-Wint-conversion"
#pragma GCC diagnostic ignored  "-Wpointer-sign"

void *    pShellConn = NULL;
SConnObj *connList;
void *mgmtProcessMsgFromShell(char *msg, void *ahandle, void *thandle);
int (*mgmtProcessShellMsg[TSDB_MSG_TYPE_MAX])(char *, int, SConnObj *);
void mgmtInitProcessShellMsg();
int mgmtKillQuery(char *queryId, SConnObj *pConn);

void mgmtProcessTranRequest(SSchedMsg *pSchedMsg) {
  SIntMsg * pMsg = (SIntMsg *)(pSchedMsg->msg);
  SConnObj *pConn = (SConnObj *)(pSchedMsg->thandle);

  char *cont = (char *)pMsg->content + sizeof(SMgmtHead);
  int   contLen = pMsg->msgLen - sizeof(SIntMsg) - sizeof(SMgmtHead);
  (*mgmtProcessShellMsg[pMsg->msgType])(cont, contLen, pConn);

  if (pSchedMsg->msg) free(pSchedMsg->msg);
}

int mgmtInitShell() {
  SRpcInit rpcInit;

  mgmtInitProcessShellMsg();

  int size = sizeof(SConnObj) * tsMaxShellConns;
  connList = (SConnObj *)malloc(size);
  memset(connList, 0, size);

  int numOfThreads = tsNumOfCores * tsNumOfThreadsPerCore / 4.0;
  if (numOfThreads < 1) numOfThreads = 1;

  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localIp = "0.0.0.0";
  rpcInit.localPort = tsMgmtShellPort;
  rpcInit.label = "MND-shell";
  rpcInit.numOfThreads = numOfThreads;
  rpcInit.fp = mgmtProcessMsgFromShell;
  rpcInit.bits = 20;
  rpcInit.numOfChanns = 1;
  rpcInit.sessionsPerChann = tsMaxShellConns;
  rpcInit.idMgmt = TAOS_ID_FREE;
  rpcInit.connType = TAOS_CONN_UDPS;
  rpcInit.idleTime = tsShellActivityTimer * 2000;
  rpcInit.qhandle = mgmtQhandle;
  rpcInit.afp = mgmtRetriveUserAuthInfo;

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

  if ((pConn->pDb != NULL && pConn->pDb->dropStatus != TSDB_DB_STATUS_READY) || pConn->pDb == NULL) {
    if ((pStart = mgmtAllocMsg(pConn, size, &pMsg, &pRsp)) == NULL) {
      return 0;
    }

    pRsp->code = TSDB_CODE_INVALID_DB;
    pMsg++;

    goto _exit_code;
  }

  pMeterObj = mgmtGetMeter(pInfo->meterId);
  if (pMeterObj == NULL && pInfo->createFlag == 1) {
    // create the meter objects if not exists
    SCreateTableMsg *pCreateMsg = calloc(1, sizeof(SCreateTableMsg) + sizeof(STagData));
    memcpy(pCreateMsg->schema, pInfo->tags, sizeof(STagData));
    strcpy(pCreateMsg->meterId, pInfo->meterId);
    // todo handle if not master mnode
    int32_t code = mgmtCreateMeter(pConn->pDb, pCreateMsg);
    mTrace("meter:%s is automatically created by %s, code:%d", pCreateMsg->meterId, pConn->pUser->user, code);
    tfree(pCreateMsg);

    if (code != TSDB_CODE_SUCCESS) {
      if ((pStart = mgmtAllocMsg(pConn, size, &pMsg, &pRsp)) == NULL) {
        return 0;
      }

      pRsp->code = code;
      pMsg++;

      goto _exit_code;
    }

    pMeterObj = mgmtGetMeter(pInfo->meterId);
  }

  if ((pStart = mgmtAllocMsg(pConn, size, &pMsg, &pRsp)) == NULL) {
    return 0;
  }

  if (pMeterObj == NULL) {
    if (pConn->pDb)
      pRsp->code = TSDB_CODE_INVALID_TABLE;
    else
      pRsp->code = TSDB_CODE_DB_NOT_SELECTED;
    pMsg++;
  } else {
    mTrace("meter:%s, meta is retrieved from:%s", pInfo->meterId, pMeterObj->meterId);
    pRsp->code = 0;
    pMsg += sizeof(STaosRsp);
    *pMsg = TSDB_IE_TYPE_META;
    pMsg++;

    pMeta = (SMeterMeta *)pMsg;
    pMeta->uid = htobe64(pMeterObj->uid);
    pMeta->sid = htonl(pMeterObj->gid.sid);
    pMeta->vgid = htonl(pMeterObj->gid.vgId);
    pMeta->sversion = htonl(pMeterObj->sversion);

    pMeta->precision = htons(pConn->pDb->cfg.precision);

    pMeta->numOfTags = htons(pMeterObj->numOfTags);
    pMeta->numOfColumns = htons(pMeterObj->numOfColumns);
    pMeta->meterType = htons(pMeterObj->meterType);

    pMsg += sizeof(SMeterMeta);
    pSchema = (SSchema *)pMsg;  // schema locates at the end of SMeterMeta
                                // struct

    if (mgmtMeterCreateFromMetric(pMeterObj)) {
      assert(pMeterObj->numOfTags == 0);

      STabObj *pMetric = mgmtGetMeter(pMeterObj->pTagData);
      uint32_t numOfTotalCols = (uint32_t)pMetric->numOfTags + pMetric->numOfColumns;

      pMeta->numOfTags = htons(pMetric->numOfTags);  // update the numOfTags
                                                     // info
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
      pMeta->vpeerDesc[0].vnode = htonl(pVgroup->vnodeGid[0].vnode);
    }
  }

_exit_code:
  msgLen = pMsg - pStart;

  taosSendMsgToPeer(pConn->thandle, pStart, msgLen);

  return msgLen;
}

int mgmtProcessMetricMetaMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  SMetricMetaMsg *pMetricMetaMsg = (SMetricMetaMsg *)pMsg;
  STabObj *       pMetric;
  STaosRsp *      pRsp;
  char *          pStart;

  pMetric = mgmtGetMeter(pMetricMetaMsg->meterId);

  if (pMetric == NULL || (pConn->pDb != NULL && pConn->pDb->dropStatus != TSDB_DB_STATUS_READY)) {
    pStart = taosBuildRspMsg(pConn->thandle, TSDB_MSG_TYPE_METRIC_META_RSP);
    if (pStart == NULL) return 0;

    pMsg = pStart;
    pRsp = (STaosRsp *)pMsg;
    if (pConn->pDb)
      pRsp->code = TSDB_CODE_INVALID_TABLE;
    else
      pRsp->code = TSDB_CODE_DB_NOT_SELECTED;
    pMsg++;

    msgLen = pMsg - pStart;
  } else {
    pMetricMetaMsg->condLength = htonl(pMetricMetaMsg->condLength);
    pMetricMetaMsg->orderIndex = htons(pMetricMetaMsg->orderIndex);
    pMetricMetaMsg->orderType = htons(pMetricMetaMsg->orderType);
    pMetricMetaMsg->numOfTags = htons(pMetricMetaMsg->numOfTags);

    pMetricMetaMsg->type = htons(pMetricMetaMsg->type);
    pMetricMetaMsg->numOfGroupbyCols = htons(pMetricMetaMsg->numOfGroupbyCols);

    pMetricMetaMsg->groupbyTagIds = ((char *)pMetricMetaMsg->tags) + pMetricMetaMsg->condLength * TSDB_NCHAR_SIZE;
    int16_t *groupbyColIds = (int16_t *)pMetricMetaMsg->groupbyTagIds;
    for (int32_t i = 0; i < pMetricMetaMsg->numOfGroupbyCols; ++i) {
      groupbyColIds[i] = htons(groupbyColIds[i]);
    }

    for (int32_t i = 0; i < pMetricMetaMsg->numOfTags; ++i) {
      pMetricMetaMsg->tagCols[i] = htons(pMetricMetaMsg->tagCols[i]);
    }

    pMetricMetaMsg->limit = htobe64(pMetricMetaMsg->limit);
    pMetricMetaMsg->offset = htobe64(pMetricMetaMsg->offset);

    msgLen = mgmtRetrieveMetricMeta(pConn->thandle, &pStart, pMetric, pMetricMetaMsg);
  }

  taosSendMsgToPeer(pConn->thandle, pStart, msgLen);

  return msgLen;
}

int mgmtProcessCreateDbMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  SCreateDbMsg *pCreate = (SCreateDbMsg *)pMsg;
  int           code = 0;

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

  if (!pConn->writeAuth) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    SAcctObj *pAcct = &acctObj;
    code = mgmtCreateDb(pAcct, pCreate);
    if (code == TSDB_CODE_SUCCESS) {
      mLPrint("DB:%s is created by %s", pCreate->db, pConn->pUser->user);
    }
  }

  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_CREATE_DB_RSP, code);

  return 0;
}

int mgmtProcessAlterDbMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  SAlterDbMsg *pAlter = (SAlterDbMsg *)pMsg;
  int          code = 0;

  pAlter->daysPerFile = htonl(pAlter->daysPerFile);
  pAlter->daysToKeep = htonl(pAlter->daysToKeep);

  if (!pConn->writeAuth) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    code = mgmtAlterDb(&acctObj, pAlter);
    if (code == TSDB_CODE_SUCCESS) {
      mLPrint("DB:%s is altered by %s", pAlter->db, pConn->pUser->user);
    }
  }

  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_ALTER_DB_RSP, code);

  return 0;
}

int mgmtProcessCfgDnodeMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  SCfgMsg *pCfg = (SCfgMsg *)pMsg;

  int code = tsCfgDynamicOptions(pCfg->config);

  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_CFG_PNODE_RSP, code);

  if (code == 0) mTrace("dnode is configured by %s", pConn->pUser->user);

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

  if (pConn->superAuth) {
    code = mgmtCreateUser(&acctObj, pCreate->user, pCreate->pass);
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

  pUser = mgmtGetUser(pAlter->user);
  if (pUser == NULL) {
    taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_ALTER_USER_RSP, TSDB_CODE_INVALID_USER);
    return 0;
  }

  if (strcmp(pUser->user, "monitor") == 0 || strcmp(pUser->user, "stream") == 0) {
    code = TSDB_CODE_NO_RIGHTS;
  } else if ((strcmp(pUser->user, pConn->pUser->user) == 0) ||
             ((strcmp(pUser->acct, acctObj.user) == 0) && pConn->superAuth) ||
             (strcmp(pConn->pUser->user, "root") == 0)) {
    if ((pAlter->flag & TSDB_ALTER_USER_PASSWD) != 0) {
      memset(pUser->pass, 0, sizeof(pUser->pass));
      taosEncryptPass((uint8_t *)(pAlter->pass), strlen(pAlter->pass), pUser->pass);
    }
    if ((pAlter->flag & TSDB_ALTER_USER_PRIVILEGES) != 0) {
      if (pAlter->privilege == 1) {  // super
        pUser->superAuth = 1;
        pUser->writeAuth = 1;
      }
      if (pAlter->privilege == 2) {  // read
        pUser->superAuth = 0;
        pUser->writeAuth = 0;
      }
      if (pAlter->privilege == 3) {  // write
        pUser->superAuth = 0;
        pUser->writeAuth = 1;
      }
    }

    code = mgmtUpdateUser(pUser);
    mLPrint("user:%s is altered by %s", pAlter->user, pConn->pUser->user);
  } else {
    code = TSDB_CODE_NO_RIGHTS;
  }

  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_ALTER_USER_RSP, code);

  return 0;
}

int mgmtProcessDropUserMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  SDropUserMsg *pDrop = (SDropUserMsg *)pMsg;
  int           code = 0;

  if (strcmp(pConn->pUser->user, pDrop->user) == 0) {
    code = TSDB_CODE_NO_RIGHTS;
  } else if (strcmp(pDrop->user, "monitor") == 0 || strcmp(pDrop->user, "stream") == 0) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    if (pConn->superAuth) {
      code = mgmtDropUser(&acctObj, pDrop->user);
      if (code == 0) {
        mLPrint("user:%s is dropped by %s", pDrop->user, pConn->pUser->user);
      }
    } else {
      code = TSDB_CODE_NO_RIGHTS;
    }
  }

  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_DROP_USER_RSP, code);

  return 0;
}

int mgmtProcessDropDbMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  SDropDbMsg *pDrop = (SDropDbMsg *)pMsg;
  int         code;

  if (!pConn->writeAuth) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    code = mgmtDropDbByName(&acctObj, pDrop->db);
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
    mgmtGetUserMeta,   mgmtGetDbMeta,    mgmtGetMeterMeta,  mgmtGetDnodeMeta, mgmtGetVgroupMeta,
    mgmtGetMetricMeta, mgmtGetQueryMeta, mgmtGetStreamMeta, mgmtGetConnsMeta,
};

int (*mgmtRetrieveFp[])(SShowObj *pShow, char *data, int rows, SConnObj *pConn) = {
    mgmtRetrieveUsers,   mgmtRetrieveDbs,     mgmtRetrieveMeters,  mgmtRetrieveDnodes, mgmtRetrieveVgroups,
    mgmtRetrieveMetrics, mgmtRetrieveQueries, mgmtRetrieveStreams, mgmtRetrieveConns,
};

int mgmtProcessShowMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  SShowMsg *   pShowMsg = (SShowMsg *)pMsg;
  STaosRsp *   pRsp;
  char *       pStart;
  int          code = 0;
  SShowRspMsg *pShowRsp;
  SShowObj *   pShow = NULL;

  int size = sizeof(STaosHeader) + sizeof(STaosRsp) + sizeof(SShowRspMsg) + sizeof(SSchema) * TSDB_MAX_COLUMNS +
             TSDB_EXTRA_PAYLOAD_SIZE;
  pStart = taosBuildRspMsgWithSize(pConn->thandle, TSDB_MSG_TYPE_SHOW_RSP, size);
  if (pStart == NULL) return 0;
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

    code = (*mgmtGetMetaFp[pShowMsg->type])(&pShowRsp->meterMeta, pShow, pConn);
    if (code == 0) {
      pMsg += sizeof(SShowRspMsg) + sizeof(SSchema) * pShow->numOfColumns;
    } else {
      mError("pShow:%p, failed to get Meta, code:%d", pShow, code);
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
  * in case of server restart, apps may hold qhandle created by server before restart,
  * which is actually invalid, therefore, signature check is required.
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
    if (pRetrieve->free == 0) rowsToRead = pShow->numOfRows - pShow->numOfReads;

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
  if (pStart == NULL) return 0;
  pMsg = pStart;

  STaosRsp *pTaosRsp = (STaosRsp *)pStart;
  pTaosRsp->code = code;
  pMsg = pTaosRsp->more;

  if (code == 0) {
    pRsp = (SRetrieveMeterRsp *)pMsg;
    pMsg = pRsp->data;

    // if free flag is set, client wants to clean the resources
    if (pRetrieve->free == 0) rowsRead = (*mgmtRetrieveFp[pShow->type])(pShow, pRsp->data, rowsToRead, pConn);

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
    int64_t oldSign = __sync_val_compare_and_swap(&pShow->signature, (uint64_t)pShow, 0);
    if (oldSign != (uint64_t)pShow) {
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

    if (pConn->pDb) {
      code = mgmtCreateMeter(pConn->pDb, pCreate);
      if (code == 0) {
        mTrace("meter:%s is created by %s", pCreate->meterId, pConn->pUser->user);
        // mLPrint("meter:%s is created by %s", pCreate->meterId, pConn->pUser->user);
      }
    } else {
      code = TSDB_CODE_DB_NOT_SELECTED;
    }
  }

  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_CREATE_TABLE_RSP, code);

  return 0;
}

int mgmtProcessDropTableMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  SDropTableMsg *pDrop = (SDropTableMsg *)pMsg;
  int            code;

  if (!pConn->writeAuth) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    code = mgmtDropMeter(pConn->pDb, pDrop->meterId, pDrop->igNotExists);
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

  if (!pConn->writeAuth) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    pAlter->type = htons(pAlter->type);
    pAlter->numOfCols = htons(pAlter->numOfCols);

    if (pAlter->numOfCols > 2) {
      mError("meter:%s error numOfCols:%d in alter table", pAlter->meterId, pAlter->numOfCols);
      code = TSDB_CODE_APP_ERROR;
    } else {
      if (pConn->pDb) {
        for (int32_t i = 0; i < pAlter->numOfCols; ++i) {
          pAlter->schema[i].bytes = htons(pAlter->schema[i].bytes);
        }

        code = mgmtAlterMeter(pConn->pDb, pAlter);
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

  pMsg += sizeof(SHeartBeatRsp);

  msgLen = pMsg - pStart;

  taosSendMsgToPeer(pConn->thandle, pStart, msgLen);

  return 0;
}

void mgmtEstablishConn(SConnObj *pConn) {
  __sync_fetch_and_add(&mgmtShellConns, 1);
  pConn->stime = taosGetTimestampMs();

  if (strcmp(pConn->pUser->user, "root") == 0 || strcmp(pConn->pUser->user, acctObj.user) == 0) {
    pConn->superAuth = 1;
    pConn->writeAuth = 1;
  } else {
    pConn->superAuth = pConn->pUser->superAuth;
    pConn->writeAuth = pConn->pUser->writeAuth;
    if (pConn->superAuth) {
      pConn->writeAuth = 1;
    }
  }

  uint32_t temp;
  taosGetRpcConnInfo(pConn->thandle, &temp, &pConn->ip, &pConn->port, &temp, &temp);
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

int mgmtProcessConnectMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  STaosRsp *   pRsp;
  SConnectRsp *pConnectRsp;
  SConnectMsg *pConnectMsg;
  char *       pStart;
  int          code = TSDB_CODE_INVALID_USER;
  SAcctObj *   pAcct = NULL;
  SUserObj *   pUser = NULL;
  SDbObj *     pDb = NULL;
  char         dbName[TSDB_METER_ID_LEN];

  pConnectMsg = (SConnectMsg *)pMsg;

  pUser = mgmtGetUser(pConn->user);
  if (pUser == NULL) {
    code = TSDB_CODE_INVALID_USER;
    goto _rsp;
  }

  pAcct = &acctObj;

  if (pConnectMsg->db[0]) {
    memset(dbName, 0, sizeof(dbName));
    sprintf(dbName, "%x%s%s", pAcct->acctId, TS_PATH_DELIMITER, pConnectMsg->db);
    pDb = mgmtGetDb(dbName);
    if (pDb == NULL) {
      code = TSDB_CODE_INVALID_DB;
      goto _rsp;
    }
    strcpy(pConn->db, dbName);
  }

  if (pConn->pAcct) {
    mgmtRemoveConnFromAcct(pConn);
    __sync_fetch_and_sub(&mgmtShellConns, 1);
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

  if (code == 0) {
    pConnectRsp = (SConnectRsp *)pRsp->more;
    sprintf(pConnectRsp->acctId, "%x", pConn->pAcct->acctId);
    strcpy(pConnectRsp->version, version);
    pConnectRsp->writeAuth = pConn->writeAuth;
    pConnectRsp->superAuth = pConn->superAuth;
    pMsg += sizeof(SConnectRsp);

    // set the time resolution: millisecond or microsecond
    *((uint32_t *)pMsg) = tsTimePrecision;
    pMsg += sizeof(uint32_t);

  } else {
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
      __sync_fetch_and_sub(&mgmtShellConns, 1);
      mTrace("connection from %s is closed", pConn->pUser->user);
      memset(pConn, 0, sizeof(SConnObj));
    }

    return NULL;
  }

  if (pConn == NULL) {
    pConn = connList + pMsg->destId;
    pConn->thandle = thandle;
    strcpy(pConn->user, pMsg->meterId);
  }

  if (pMsg->msgType == TSDB_MSG_TYPE_CONNECT) {
    (*mgmtProcessShellMsg[pMsg->msgType])((char *)pMsg->content, pMsg->msgLen - sizeof(SIntMsg), pConn);
  } else {
    SMgmtHead *pHead = (SMgmtHead *)pMsg->content;
    if (pConn->pAcct == NULL) {
      pConn->pUser = mgmtGetUser(pConn->user);
      if (pConn->pUser) {
        pConn->pAcct = &acctObj;
        mgmtEstablishConn(pConn);
        mTrace("login from:%x:%d", pConn->ip, htons(pConn->port));
      }
    }

    if (pConn->pAcct) {
      if (strcmp(pConn->db, pHead->db) != 0) pConn->pDb = mgmtGetDb(pHead->db);

      char *cont = (char *)pMsg->content + sizeof(SMgmtHead);
      int   contLen = pMsg->msgLen - sizeof(SIntMsg) - sizeof(SMgmtHead);
      if (pMsg->msgType == TSDB_MSG_TYPE_METERINFO || pMsg->msgType == TSDB_MSG_TYPE_METRIC_META ||
          pMsg->msgType == TSDB_MSG_TYPE_RETRIEVE || pMsg->msgType == TSDB_MSG_TYPE_SHOW) {
        (*mgmtProcessShellMsg[pMsg->msgType])(cont, contLen, pConn);
      } else {
        if (mgmtProcessShellMsg[pMsg->msgType]) {
          // TODO : put the msg in tran queue
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
  mgmtProcessShellMsg[TSDB_MSG_TYPE_CREATE_DB] = mgmtProcessCreateDbMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_ALTER_DB] = mgmtProcessAlterDbMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_CREATE_USER] = mgmtProcessCreateUserMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_ALTER_USER] = mgmtProcessAlterUserMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_DROP_DB] = mgmtProcessDropDbMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_DROP_USER] = mgmtProcessDropUserMsg;

  mgmtProcessShellMsg[TSDB_MSG_TYPE_CREATE_TABLE] = mgmtProcessCreateTableMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_DROP_TABLE] = mgmtProcessDropTableMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_ALTER_TABLE] = mgmtProcessAlterTableMsg;

  mgmtProcessShellMsg[TSDB_MSG_TYPE_USE_DB] = mgmtProcessUseDbMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_RETRIEVE] = mgmtProcessRetrieveMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_SHOW] = mgmtProcessShowMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_CONNECT] = mgmtProcessConnectMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_HEARTBEAT] = mgmtProcessHeartBeatMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_CFG_PNODE] = mgmtProcessCfgDnodeMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_KILL_QUERY] = mgmtProcessKillQueryMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_KILL_STREAM] = mgmtProcessKillStreamMsg;
  mgmtProcessShellMsg[TSDB_MSG_TYPE_KILL_CONNECTION] = mgmtProcessKillConnectionMsg;
}
