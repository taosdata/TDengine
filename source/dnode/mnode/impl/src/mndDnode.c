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
#include "mndDnode.h"
#include <stdio.h>
#include "audit.h"
#include "mndCluster.h"
#include "mndDb.h"
#include "mndMnode.h"
#include "mndPrivilege.h"
#include "mndQnode.h"
#include "mndShow.h"
#include "mndSnode.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"
#include "taos_monitor.h"
#include "tjson.h"
#include "tmisce.h"
#include "tunit.h"

#define TSDB_DNODE_VER_NUMBER   2
#define TSDB_DNODE_RESERVE_SIZE 40

static const char *offlineReason[] = {
    "",
    "status msg timeout",
    "status not received",
    "version not match",
    "dnodeId not match",
    "clusterId not match",
    "statusInterval not match",
    "timezone not match",
    "locale not match",
    "charset not match",
    "ttlChangeOnWrite not match",
    "enableWhiteList not match",
    "encryptionKey not match",
    "monitor not match",
    "monitor switch not match",
    "monitor interval not match",
    "monitor slow log threshold not match",
    "monitor slow log sql max len not match",
    "monitor slow log scope not match",
    "unknown",
};

enum {
  DND_ACTIVE_CODE,
  DND_CONN_ACTIVE_CODE,
};

enum {
  DND_CREATE,
  DND_ADD,
  DND_DROP,
};

static int32_t  mndCreateDefaultDnode(SMnode *pMnode);
static SSdbRaw *mndDnodeActionEncode(SDnodeObj *pDnode);
static SSdbRow *mndDnodeActionDecode(SSdbRaw *pRaw);
static int32_t  mndDnodeActionInsert(SSdb *pSdb, SDnodeObj *pDnode);
static int32_t  mndDnodeActionDelete(SSdb *pSdb, SDnodeObj *pDnode);
static int32_t  mndDnodeActionUpdate(SSdb *pSdb, SDnodeObj *pOld, SDnodeObj *pNew);
static int32_t  mndProcessDnodeListReq(SRpcMsg *pReq);
static int32_t  mndProcessShowVariablesReq(SRpcMsg *pReq);

static int32_t mndProcessCreateDnodeReq(SRpcMsg *pReq);
static int32_t mndProcessDropDnodeReq(SRpcMsg *pReq);
static int32_t mndProcessConfigDnodeReq(SRpcMsg *pReq);
static int32_t mndProcessConfigDnodeRsp(SRpcMsg *pRsp);
static int32_t mndProcessStatusReq(SRpcMsg *pReq);
static int32_t mndProcessNotifyReq(SRpcMsg *pReq);
static int32_t mndProcessRestoreDnodeReq(SRpcMsg *pReq);
static int32_t mndProcessStatisReq(SRpcMsg *pReq);
static int32_t mndProcessUpdateDnodeInfoReq(SRpcMsg *pReq);
static int32_t mndProcessCreateEncryptKeyReq(SRpcMsg *pRsp);
static int32_t mndProcessCreateEncryptKeyRsp(SRpcMsg *pRsp);

static int32_t mndRetrieveConfigs(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextConfig(SMnode *pMnode, void *pIter);
static int32_t mndRetrieveDnodes(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextDnode(SMnode *pMnode, void *pIter);

static int32_t mndMCfgGetValInt32(SMCfgDnodeReq *pInMCfgReq, int32_t optLen, int32_t *pOutValue);

#ifdef _GRANT
int32_t mndUpdClusterInfo(SRpcMsg *pReq);
#else
static int32_t mndUpdClusterInfo(SRpcMsg *pReq) { return 0; }
#endif

int32_t mndInitDnode(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_DNODE,
      .keyType = SDB_KEY_INT32,
      .deployFp = (SdbDeployFp)mndCreateDefaultDnode,
      .encodeFp = (SdbEncodeFp)mndDnodeActionEncode,
      .decodeFp = (SdbDecodeFp)mndDnodeActionDecode,
      .insertFp = (SdbInsertFp)mndDnodeActionInsert,
      .updateFp = (SdbUpdateFp)mndDnodeActionUpdate,
      .deleteFp = (SdbDeleteFp)mndDnodeActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_DNODE, mndProcessCreateDnodeReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_DNODE, mndProcessDropDnodeReq);
  mndSetMsgHandle(pMnode, TDMT_MND_CONFIG_DNODE, mndProcessConfigDnodeReq);
  mndSetMsgHandle(pMnode, TDMT_DND_CONFIG_DNODE_RSP, mndProcessConfigDnodeRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_STATUS, mndProcessStatusReq);
  mndSetMsgHandle(pMnode, TDMT_MND_NOTIFY, mndProcessNotifyReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DNODE_LIST, mndProcessDnodeListReq);
  mndSetMsgHandle(pMnode, TDMT_MND_SHOW_VARIABLES, mndProcessShowVariablesReq);
  mndSetMsgHandle(pMnode, TDMT_MND_RESTORE_DNODE, mndProcessRestoreDnodeReq);
  mndSetMsgHandle(pMnode, TDMT_MND_STATIS, mndProcessStatisReq);
  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_ENCRYPT_KEY, mndProcessCreateEncryptKeyReq);
  mndSetMsgHandle(pMnode, TDMT_DND_CREATE_ENCRYPT_KEY_RSP, mndProcessCreateEncryptKeyRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_UPDATE_DNODE_INFO, mndProcessUpdateDnodeInfoReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_CONFIGS, mndRetrieveConfigs);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_CONFIGS, mndCancelGetNextConfig);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_DNODE, mndRetrieveDnodes);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_DNODE, mndCancelGetNextDnode);

  return sdbSetTable(pMnode->pSdb, table);
}

SIpWhiteList *mndCreateIpWhiteOfDnode(SMnode *pMnode);
SIpWhiteList *mndAddIpWhiteOfDnode(SIpWhiteList *pIpWhiteList, char *fqdn);
SIpWhiteList *mndRmIpWhiteOfDnode(SIpWhiteList *pIpWhiteList, char *fqdn);
void          mndCleanupDnode(SMnode *pMnode) {}

static int32_t mndCreateDefaultDnode(SMnode *pMnode) {
  int32_t  code = -1;
  SSdbRaw *pRaw = NULL;
  STrans  *pTrans = NULL;

  SDnodeObj dnodeObj = {0};
  dnodeObj.id = 1;
  dnodeObj.createdTime = taosGetTimestampMs();
  dnodeObj.updateTime = dnodeObj.createdTime;
  dnodeObj.port = tsServerPort;
  tstrncpy(dnodeObj.fqdn, tsLocalFqdn, TSDB_FQDN_LEN);
  dnodeObj.fqdn[TSDB_FQDN_LEN - 1] = 0;
  (void)snprintf(dnodeObj.ep, TSDB_EP_LEN - 1, "%s:%u", tsLocalFqdn, tsServerPort);
  char *machineId = NULL;
  code = tGetMachineId(&machineId);
  if (machineId) {
    (void)memcpy(dnodeObj.machineId, machineId, TSDB_MACHINE_ID_LEN);
    taosMemoryFreeClear(machineId);
  } else {
#if defined(TD_ENTERPRISE) && !defined(GRANTS_CFG)
    terrno = TSDB_CODE_DNODE_NO_MACHINE_CODE;
    goto _OVER;
#endif
  }

  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_GLOBAL, NULL, "create-dnode");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }
  mInfo("trans:%d, used to create dnode:%s on first deploy", pTrans->id, dnodeObj.ep);

  pRaw = mndDnodeActionEncode(&dnodeObj);
  if (pRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }
  TAOS_CHECK_GOTO(mndTransAppendCommitlog(pTrans, pRaw), NULL, _OVER);
  (void)sdbSetRawStatus(pRaw, SDB_STATUS_READY);
  pRaw = NULL;

  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);
  code = 0;
  (void)mndUpdateIpWhiteForAllUser(pMnode, TSDB_DEFAULT_USER, dnodeObj.fqdn, IP_WHITE_ADD,
                                   1);  // TODO: check the return value

_OVER:
  mndTransDrop(pTrans);
  sdbFreeRaw(pRaw);
  return code;
}

static SSdbRaw *mndDnodeActionEncode(SDnodeObj *pDnode) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_DNODE, TSDB_DNODE_VER_NUMBER, sizeof(SDnodeObj) + TSDB_DNODE_RESERVE_SIZE);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pDnode->id, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pDnode->createdTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pDnode->updateTime, _OVER)
  SDB_SET_INT16(pRaw, dataPos, pDnode->port, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pDnode->fqdn, TSDB_FQDN_LEN, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pDnode->machineId, TSDB_MACHINE_ID_LEN, _OVER)
  SDB_SET_RESERVE(pRaw, dataPos, TSDB_DNODE_RESERVE_SIZE, _OVER)
  SDB_SET_INT16(pRaw, dataPos, 0, _OVER)  // forward/backward compatible
  SDB_SET_INT16(pRaw, dataPos, 0, _OVER)  // forward/backward compatible
  SDB_SET_DATALEN(pRaw, dataPos, _OVER);

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("dnode:%d, failed to encode to raw:%p since %s", pDnode->id, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("dnode:%d, encode to raw:%p, row:%p", pDnode->id, pRaw, pDnode);
  return pRaw;
}

static SSdbRow *mndDnodeActionDecode(SSdbRaw *pRaw) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  SSdbRow   *pRow = NULL;
  SDnodeObj *pDnode = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;
  if (sver < 1 || sver > TSDB_DNODE_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  pRow = sdbAllocRow(sizeof(SDnodeObj));
  if (pRow == NULL) goto _OVER;

  pDnode = sdbGetRowObj(pRow);
  if (pDnode == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &pDnode->id, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pDnode->createdTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pDnode->updateTime, _OVER)
  SDB_GET_INT16(pRaw, dataPos, &pDnode->port, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pDnode->fqdn, TSDB_FQDN_LEN, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pDnode->machineId, TSDB_MACHINE_ID_LEN, _OVER)
  SDB_GET_RESERVE(pRaw, dataPos, TSDB_DNODE_RESERVE_SIZE, _OVER)
  if (sver > 1) {
    int16_t keyLen = 0;
    SDB_GET_INT16(pRaw, dataPos, &keyLen, _OVER)
    SDB_GET_BINARY(pRaw, dataPos, NULL, keyLen, _OVER)
    SDB_GET_INT16(pRaw, dataPos, &keyLen, _OVER)
    SDB_GET_BINARY(pRaw, dataPos, NULL, keyLen, _OVER)
  }

  terrno = 0;
  if (tmsgUpdateDnodeInfo(&pDnode->id, NULL, pDnode->fqdn, &pDnode->port)) {
    mInfo("dnode:%d, endpoint changed", pDnode->id);
  }

_OVER:
  if (terrno != 0) {
    mError("dnode:%d, failed to decode from raw:%p since %s", pDnode == NULL ? 0 : pDnode->id, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("dnode:%d, decode from raw:%p, row:%p ep:%s:%u", pDnode->id, pRaw, pDnode, pDnode->fqdn, pDnode->port);
  return pRow;
}

static int32_t mndDnodeActionInsert(SSdb *pSdb, SDnodeObj *pDnode) {
  mTrace("dnode:%d, perform insert action, row:%p", pDnode->id, pDnode);
  pDnode->offlineReason = DND_REASON_STATUS_NOT_RECEIVED;

  char ep[TSDB_EP_LEN] = {0};
  (void)snprintf(ep, TSDB_EP_LEN - 1, "%s:%u", pDnode->fqdn, pDnode->port);
  tstrncpy(pDnode->ep, ep, TSDB_EP_LEN);
  return 0;
}

static int32_t mndDnodeActionDelete(SSdb *pSdb, SDnodeObj *pDnode) {
  mTrace("dnode:%d, perform delete action, row:%p", pDnode->id, pDnode);
  return 0;
}

static int32_t mndDnodeActionUpdate(SSdb *pSdb, SDnodeObj *pOld, SDnodeObj *pNew) {
  mTrace("dnode:%d, perform update action, old row:%p new row:%p", pOld->id, pOld, pNew);
  pOld->updateTime = pNew->updateTime;
#ifdef TD_ENTERPRISE
  tstrncpy(pOld->machineId, pNew->machineId, TSDB_MACHINE_ID_LEN + 1);
#endif
  return 0;
}

SDnodeObj *mndAcquireDnode(SMnode *pMnode, int32_t dnodeId) {
  SSdb      *pSdb = pMnode->pSdb;
  SDnodeObj *pDnode = sdbAcquire(pSdb, SDB_DNODE, &dnodeId);
  if (pDnode == NULL) {
    if (terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
      terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    } else if (terrno == TSDB_CODE_SDB_OBJ_CREATING) {
      terrno = TSDB_CODE_MND_DNODE_IN_CREATING;
    } else if (terrno == TSDB_CODE_SDB_OBJ_DROPPING) {
      terrno = TSDB_CODE_MND_DNODE_IN_DROPPING;
    } else {
      terrno = TSDB_CODE_APP_ERROR;
      mFatal("dnode:%d, failed to acquire db since %s", dnodeId, terrstr());
    }
  }

  return pDnode;
}

void mndReleaseDnode(SMnode *pMnode, SDnodeObj *pDnode) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pDnode);
}

SEpSet mndGetDnodeEpset(SDnodeObj *pDnode) {
  SEpSet epSet = {0};
  terrno = addEpIntoEpSet(&epSet, pDnode->fqdn, pDnode->port);
  return epSet;
}

SEpSet mndGetDnodeEpsetById(SMnode *pMnode, int32_t dnodeId) {
  SEpSet     epSet = {0};
  SDnodeObj *pDnode = mndAcquireDnode(pMnode, dnodeId);
  if (!pDnode) return epSet;

  epSet = mndGetDnodeEpset(pDnode);

  mndReleaseDnode(pMnode, pDnode);
  return epSet;
}

static SDnodeObj *mndAcquireDnodeByEp(SMnode *pMnode, char *pEpStr) {
  SSdb *pSdb = pMnode->pSdb;

  void *pIter = NULL;
  while (1) {
    SDnodeObj *pDnode = NULL;
    pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pDnode);
    if (pIter == NULL) break;

    if (strncasecmp(pEpStr, pDnode->ep, TSDB_EP_LEN) == 0) {
      sdbCancelFetch(pSdb, pIter);
      return pDnode;
    }

    sdbRelease(pSdb, pDnode);
  }

  terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
  return NULL;
}

static SDnodeObj *mndAcquireDnodeAllStatusByEp(SMnode *pMnode, char *pEpStr) {
  SSdb *pSdb = pMnode->pSdb;

  void *pIter = NULL;
  while (1) {
    SDnodeObj *pDnode = NULL;
    ESdbStatus objStatus = 0;
    pIter = sdbFetchAll(pSdb, SDB_DNODE, pIter, (void **)&pDnode, &objStatus, true);
    if (pIter == NULL) break;

    if (strncasecmp(pEpStr, pDnode->ep, TSDB_EP_LEN) == 0) {
      sdbCancelFetch(pSdb, pIter);
      return pDnode;
    }

    sdbRelease(pSdb, pDnode);
  }

  return NULL;
}

int32_t mndGetDnodeSize(SMnode *pMnode) {
  SSdb *pSdb = pMnode->pSdb;
  return sdbGetSize(pSdb, SDB_DNODE);
}

int32_t mndGetDbSize(SMnode *pMnode) {
  SSdb *pSdb = pMnode->pSdb;
  return sdbGetSize(pSdb, SDB_DB);
}

bool mndIsDnodeOnline(SDnodeObj *pDnode, int64_t curMs) {
  int64_t interval = TABS(pDnode->lastAccessTime - curMs);
  if (interval > 5000 * (int64_t)tsStatusInterval) {
    if (pDnode->rebootTime > 0 && pDnode->offlineReason == DND_REASON_ONLINE) {
      pDnode->offlineReason = DND_REASON_STATUS_MSG_TIMEOUT;
    }
    return false;
  }
  return true;
}

static void mndGetDnodeEps(SMnode *pMnode, SArray *pDnodeEps) {
  SSdb *pSdb = pMnode->pSdb;

  int32_t numOfEps = 0;
  void   *pIter = NULL;
  while (1) {
    SDnodeObj *pDnode = NULL;
    ESdbStatus objStatus = 0;
    pIter = sdbFetchAll(pSdb, SDB_DNODE, pIter, (void **)&pDnode, &objStatus, true);
    if (pIter == NULL) break;

    SDnodeEp dnodeEp = {0};
    dnodeEp.id = pDnode->id;
    dnodeEp.ep.port = pDnode->port;
    tstrncpy(dnodeEp.ep.fqdn, pDnode->fqdn, TSDB_FQDN_LEN);
    sdbRelease(pSdb, pDnode);

    dnodeEp.isMnode = 0;
    if (mndIsMnode(pMnode, pDnode->id)) {
      dnodeEp.isMnode = 1;
    }
    if (taosArrayPush(pDnodeEps, &dnodeEp) == NULL) {
      mError("failed to put ep into array, but continue at this call");
    }
  }
}

int32_t mndGetDnodeData(SMnode *pMnode, SArray *pDnodeInfo) {
  SSdb   *pSdb = pMnode->pSdb;
  int32_t code = 0;

  int32_t numOfEps = 0;
  void   *pIter = NULL;
  while (1) {
    SDnodeObj *pDnode = NULL;
    ESdbStatus objStatus = 0;
    pIter = sdbFetchAll(pSdb, SDB_DNODE, pIter, (void **)&pDnode, &objStatus, true);
    if (pIter == NULL) break;

    SDnodeInfo dInfo;
    dInfo.id = pDnode->id;
    dInfo.ep.port = pDnode->port;
    dInfo.offlineReason = pDnode->offlineReason;
    tstrncpy(dInfo.ep.fqdn, pDnode->fqdn, TSDB_FQDN_LEN);
    sdbRelease(pSdb, pDnode);
    if (mndIsMnode(pMnode, pDnode->id)) {
      dInfo.isMnode = 1;
    } else {
      dInfo.isMnode = 0;
    }

    if(taosArrayPush(pDnodeInfo, &dInfo) == NULL){
      code = terrno;
      sdbCancelFetch(pSdb, pIter);
      break;
    }
  }
  TAOS_RETURN(code);
}

#define CHECK_MONITOR_PARA(para,err) \
if (pCfg->monitorParas.para != para) { \
  mError("dnode:%d, para:%d inconsistent with cluster:%d", pDnode->id, pCfg->monitorParas.para, para); \
  terrno = err; \
  return err;\
}

static int32_t mndCheckClusterCfgPara(SMnode *pMnode, SDnodeObj *pDnode, const SClusterCfg *pCfg) {
  CHECK_MONITOR_PARA(tsEnableMonitor, DND_REASON_STATUS_MONITOR_SWITCH_NOT_MATCH);
  CHECK_MONITOR_PARA(tsMonitorInterval, DND_REASON_STATUS_MONITOR_INTERVAL_NOT_MATCH);
  CHECK_MONITOR_PARA(tsSlowLogThreshold, DND_REASON_STATUS_MONITOR_SLOW_LOG_THRESHOLD_NOT_MATCH);
  CHECK_MONITOR_PARA(tsSlowLogThresholdTest, DND_REASON_STATUS_MONITOR_NOT_MATCH);
  CHECK_MONITOR_PARA(tsSlowLogMaxLen, DND_REASON_STATUS_MONITOR_SLOW_LOG_SQL_MAX_LEN_NOT_MATCH);
  CHECK_MONITOR_PARA(tsSlowLogScope, DND_REASON_STATUS_MONITOR_SLOW_LOG_SCOPE_NOT_MATCH);

  if (0 != strcasecmp(pCfg->monitorParas.tsSlowLogExceptDb, tsSlowLogExceptDb)) {
    mError("dnode:%d, tsSlowLogExceptDb:%s inconsistent with cluster:%s", pDnode->id, pCfg->monitorParas.tsSlowLogExceptDb, tsSlowLogExceptDb);
    terrno = TSDB_CODE_DNODE_INVALID_MONITOR_PARAS;
    return DND_REASON_STATUS_MONITOR_NOT_MATCH;
  }

  if (pCfg->statusInterval != tsStatusInterval) {
    mError("dnode:%d, statusInterval:%d inconsistent with cluster:%d", pDnode->id, pCfg->statusInterval,
           tsStatusInterval);
    terrno = TSDB_CODE_DNODE_INVALID_STATUS_INTERVAL;
    return DND_REASON_STATUS_INTERVAL_NOT_MATCH;
  }

  if ((0 != strcasecmp(pCfg->timezone, tsTimezoneStr)) && (pMnode->checkTime != pCfg->checkTime)) {
    mError("dnode:%d, timezone:%s checkTime:%" PRId64 " inconsistent with cluster %s %" PRId64, pDnode->id,
           pCfg->timezone, pCfg->checkTime, tsTimezoneStr, pMnode->checkTime);
    terrno = TSDB_CODE_DNODE_INVALID_TIMEZONE;
    return DND_REASON_TIME_ZONE_NOT_MATCH;
  }

  if (0 != strcasecmp(pCfg->locale, tsLocale)) {
    mError("dnode:%d, locale:%s inconsistent with cluster:%s", pDnode->id, pCfg->locale, tsLocale);
    terrno = TSDB_CODE_DNODE_INVALID_LOCALE;
    return DND_REASON_LOCALE_NOT_MATCH;
  }

  if (0 != strcasecmp(pCfg->charset, tsCharset)) {
    mError("dnode:%d, charset:%s inconsistent with cluster:%s", pDnode->id, pCfg->charset, tsCharset);
    terrno = TSDB_CODE_DNODE_INVALID_CHARSET;
    return DND_REASON_CHARSET_NOT_MATCH;
  }

  if (pCfg->ttlChangeOnWrite != tsTtlChangeOnWrite) {
    mError("dnode:%d, ttlChangeOnWrite:%d inconsistent with cluster:%d", pDnode->id, pCfg->ttlChangeOnWrite,
           tsTtlChangeOnWrite);
    terrno = TSDB_CODE_DNODE_INVALID_TTL_CHG_ON_WR;
    return DND_REASON_TTL_CHANGE_ON_WRITE_NOT_MATCH;
  }
  int8_t enable = tsEnableWhiteList ? 1 : 0;
  if (pCfg->enableWhiteList != enable) {
    mError("dnode:%d, enableWhiteList:%d inconsistent with cluster:%d", pDnode->id, pCfg->enableWhiteList, enable);
    terrno = TSDB_CODE_DNODE_INVALID_EN_WHITELIST;
    return DND_REASON_ENABLE_WHITELIST_NOT_MATCH;
  }

  if (!atomic_load_8(&pMnode->encryptMgmt.encrypting) &&
      (pCfg->encryptionKeyStat != tsEncryptionKeyStat || pCfg->encryptionKeyChksum != tsEncryptionKeyChksum)) {
    mError("dnode:%d, encryptionKey:%" PRIi8 "-%u inconsistent with cluster:%" PRIi8 "-%u", pDnode->id,
           pCfg->encryptionKeyStat, pCfg->encryptionKeyChksum, tsEncryptionKeyStat, tsEncryptionKeyChksum);
    terrno = pCfg->encryptionKeyChksum ? TSDB_CODE_DNODE_INVALID_ENCRYPTKEY : TSDB_CODE_DNODE_NO_ENCRYPT_KEY;
    return DND_REASON_ENCRYPTION_KEY_NOT_MATCH;
  }

  return DND_REASON_ONLINE;
}

static bool mndUpdateVnodeState(int32_t vgId, SVnodeGid *pGid, SVnodeLoad *pVload) {
  bool stateChanged = false;
  bool roleChanged = pGid->syncState != pVload->syncState ||
                     (pVload->syncTerm != -1 && pGid->syncTerm != pVload->syncTerm) ||
                     pGid->roleTimeMs != pVload->roleTimeMs;
  if (roleChanged || pGid->syncRestore != pVload->syncRestore || pGid->syncCanRead != pVload->syncCanRead ||
      pGid->startTimeMs != pVload->startTimeMs) {
    mInfo(
        "vgId:%d, state changed by status msg, old state:%s restored:%d canRead:%d new state:%s restored:%d "
        "canRead:%d, dnode:%d",
        vgId, syncStr(pGid->syncState), pGid->syncRestore, pGid->syncCanRead, syncStr(pVload->syncState),
        pVload->syncRestore, pVload->syncCanRead, pGid->dnodeId);
    pGid->syncState = pVload->syncState;
    pGid->syncTerm = pVload->syncTerm;
    pGid->syncRestore = pVload->syncRestore;
    pGid->syncCanRead = pVload->syncCanRead;
    pGid->startTimeMs = pVload->startTimeMs;
    pGid->roleTimeMs = pVload->roleTimeMs;
    stateChanged = true;
  }
  return stateChanged;
}

static bool mndUpdateMnodeState(SMnodeObj *pObj, SMnodeLoad *pMload) {
  bool stateChanged = false;
  bool roleChanged = pObj->syncState != pMload->syncState ||
                     (pMload->syncTerm != -1 && pObj->syncTerm != pMload->syncTerm) ||
                     pObj->roleTimeMs != pMload->roleTimeMs;
  if (roleChanged || pObj->syncRestore != pMload->syncRestore) {
    mInfo("dnode:%d, mnode syncState from %s to %s, restoreState from %d to %d, syncTerm from %" PRId64 " to %" PRId64,
          pObj->id, syncStr(pObj->syncState), syncStr(pMload->syncState), pObj->syncRestore, pMload->syncRestore,
          pObj->syncTerm, pMload->syncTerm);
    pObj->syncState = pMload->syncState;
    pObj->syncTerm = pMload->syncTerm;
    pObj->syncRestore = pMload->syncRestore;
    pObj->roleTimeMs = pMload->roleTimeMs;
    stateChanged = true;
  }
  return stateChanged;
}

extern char* tsMonFwUri;
extern char* tsMonSlowLogUri;
static int32_t mndProcessStatisReq(SRpcMsg *pReq) {
  SMnode    *pMnode = pReq->info.node;
  SStatisReq statisReq = {0};
  int32_t    code = -1;

  TAOS_CHECK_RETURN(tDeserializeSStatisReq(pReq->pCont, pReq->contLen, &statisReq));

  if (tsMonitorLogProtocol) {
    mInfo("process statis req,\n %s", statisReq.pCont);
  }

  if (statisReq.type == MONITOR_TYPE_COUNTER){
    monSendContent(statisReq.pCont, tsMonFwUri);
  }else if(statisReq.type == MONITOR_TYPE_SLOW_LOG){
    monSendContent(statisReq.pCont, tsMonSlowLogUri);
  }

  tFreeSStatisReq(&statisReq);
  return 0;
}

static int32_t mndUpdateDnodeObj(SMnode *pMnode, SDnodeObj *pDnode) {
  int32_t       code = 0, lino = 0;
  SDnodeInfoReq infoReq = {0};
  int32_t       contLen = 0;
  void         *pReq = NULL;

  infoReq.dnodeId = pDnode->id;
  tstrncpy(infoReq.machineId, pDnode->machineId, TSDB_MACHINE_ID_LEN + 1);

  if ((contLen = tSerializeSDnodeInfoReq(NULL, 0, &infoReq)) <= 0) {
    TAOS_RETURN(contLen ? contLen : TSDB_CODE_OUT_OF_MEMORY);
  }
  pReq = rpcMallocCont(contLen);
  if (pReq == NULL) {
    TAOS_RETURN(terrno);
  }

  (void)tSerializeSDnodeInfoReq(pReq, contLen, &infoReq);

  SRpcMsg rpcMsg = {.msgType = TDMT_MND_UPDATE_DNODE_INFO, .pCont = pReq, .contLen = contLen};
  TAOS_CHECK_EXIT(tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg));
_exit:
  if (code < 0) {
    mError("dnode:%d, failed to update dnode info since %s", pDnode->id, tstrerror(code));
  }
  TAOS_RETURN(code);
}

static int32_t mndProcessUpdateDnodeInfoReq(SRpcMsg *pReq) {
  int32_t       code = 0, lino = 0;
  SMnode       *pMnode = pReq->info.node;
  SDnodeInfoReq infoReq = {0};
  SDnodeObj    *pDnode = NULL;
  STrans       *pTrans = NULL;
  SSdbRaw      *pCommitRaw = NULL;

  TAOS_CHECK_EXIT(tDeserializeSDnodeInfoReq(pReq->pCont, pReq->contLen, &infoReq));

  pDnode = mndAcquireDnode(pMnode, infoReq.dnodeId);
  if (pDnode == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, NULL, "update-dnode-obj");
  if (pTrans == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  pDnode->updateTime = taosGetTimestampMs();

  if ((pCommitRaw = mndDnodeActionEncode(pDnode)) == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  if ((code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, tstrerror(code));
    TAOS_CHECK_EXIT(code);
  }
  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);
  pCommitRaw = NULL;

  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, tstrerror(code));
    TAOS_CHECK_EXIT(code);
  }

_exit:
  mndReleaseDnode(pMnode, pDnode);
  if (code != 0) {
    mError("dnode:%d, failed to update dnode info at line %d since %s", infoReq.dnodeId, lino, tstrerror(code));
  }
  mndTransDrop(pTrans);
  sdbFreeRaw(pCommitRaw);
  TAOS_RETURN(code);
}

static int32_t mndProcessStatusReq(SRpcMsg *pReq) {
  SMnode    *pMnode = pReq->info.node;
  SStatusReq statusReq = {0};
  SDnodeObj *pDnode = NULL;
  int32_t    code = -1;

  TAOS_CHECK_GOTO(tDeserializeSStatusReq(pReq->pCont, pReq->contLen, &statusReq), NULL, _OVER);

  int64_t clusterid = mndGetClusterId(pMnode);
  if (statusReq.clusterId != 0 && statusReq.clusterId != clusterid) {
    code = TSDB_CODE_MND_DNODE_DIFF_CLUSTER;
    mWarn("dnode:%d, %s, its clusterid:%" PRId64 " differ from current cluster:%" PRId64 ", code:0x%x",
          statusReq.dnodeId, statusReq.dnodeEp, statusReq.clusterId, clusterid, code);
    goto _OVER;
  }

  if (statusReq.dnodeId == 0) {
    pDnode = mndAcquireDnodeByEp(pMnode, statusReq.dnodeEp);
    if (pDnode == NULL) {
      mInfo("dnode:%s, not created yet", statusReq.dnodeEp);
      code = TSDB_CODE_MND_RETURN_VALUE_NULL;
      if (terrno != 0) code = terrno;
      goto _OVER;
    }
  } else {
    pDnode = mndAcquireDnode(pMnode, statusReq.dnodeId);
    if (pDnode == NULL) {
      int32_t err = terrno;
      pDnode = mndAcquireDnodeByEp(pMnode, statusReq.dnodeEp);
      if (pDnode != NULL) {
        pDnode->offlineReason = DND_REASON_DNODE_ID_NOT_MATCH;
        terrno = err;
        goto _OVER;
      }

      mError("dnode:%d, %s not exist, code:0x%x", statusReq.dnodeId, statusReq.dnodeEp, err);
      if (err == TSDB_CODE_MND_DNODE_NOT_EXIST) {
        terrno = err;
        goto _OVER;
      } else {
        pDnode = mndAcquireDnodeAllStatusByEp(pMnode, statusReq.dnodeEp);
        if (pDnode == NULL) goto _OVER;
      }
    }
  }

  pMnode->ipWhiteVer = mndGetIpWhiteVer(pMnode);

  int64_t dnodeVer = sdbGetTableVer(pMnode->pSdb, SDB_DNODE) + sdbGetTableVer(pMnode->pSdb, SDB_MNODE);
  int64_t curMs = taosGetTimestampMs();
  bool    online = mndIsDnodeOnline(pDnode, curMs);
  bool    dnodeChanged = (statusReq.dnodeVer == 0) || (statusReq.dnodeVer != dnodeVer);
  bool    reboot = (pDnode->rebootTime != statusReq.rebootTime);
  bool    supportVnodesChanged = pDnode->numOfSupportVnodes != statusReq.numOfSupportVnodes;
  bool    encryptKeyChanged = pDnode->encryptionKeyChksum != statusReq.clusterCfg.encryptionKeyChksum;
  bool    enableWhiteListChanged = statusReq.clusterCfg.enableWhiteList != (tsEnableWhiteList ? 1 : 0);
  bool    needCheck = !online || dnodeChanged || reboot || supportVnodesChanged ||
                   pMnode->ipWhiteVer != statusReq.ipWhiteVer || encryptKeyChanged || enableWhiteListChanged;

  const STraceId *trace = &pReq->info.traceId;
  mGTrace("dnode:%d, status received, accessTimes:%d check:%d online:%d reboot:%d changed:%d statusSeq:%d", pDnode->id,
          pDnode->accessTimes, needCheck, online, reboot, dnodeChanged, statusReq.statusSeq);

  if (reboot) {
    tsGrantHBInterval = GRANT_HEART_BEAT_MIN;
  }

  for (int32_t v = 0; v < taosArrayGetSize(statusReq.pVloads); ++v) {
    SVnodeLoad *pVload = taosArrayGet(statusReq.pVloads, v);

    SVgObj *pVgroup = mndAcquireVgroup(pMnode, pVload->vgId);
    if (pVgroup != NULL) {
      if (pVload->syncState == TAOS_SYNC_STATE_LEADER || pVload->syncState == TAOS_SYNC_STATE_ASSIGNED_LEADER) {
        pVgroup->cacheUsage = pVload->cacheUsage;
        pVgroup->numOfCachedTables = pVload->numOfCachedTables;
        pVgroup->numOfTables = pVload->numOfTables;
        pVgroup->numOfTimeSeries = pVload->numOfTimeSeries;
        pVgroup->totalStorage = pVload->totalStorage;
        pVgroup->compStorage = pVload->compStorage;
        pVgroup->pointsWritten = pVload->pointsWritten;
      }
      bool stateChanged = false;
      for (int32_t vg = 0; vg < pVgroup->replica; ++vg) {
        SVnodeGid *pGid = &pVgroup->vnodeGid[vg];
        if (pGid->dnodeId == statusReq.dnodeId) {
          if (pVload->startTimeMs == 0) {
            pVload->startTimeMs = statusReq.rebootTime;
          }
          if (pVload->roleTimeMs == 0) {
            pVload->roleTimeMs = statusReq.rebootTime;
          }
          stateChanged = mndUpdateVnodeState(pVgroup->vgId, pGid, pVload);
          break;
        }
      }
      if (stateChanged) {
        SDbObj *pDb = mndAcquireDb(pMnode, pVgroup->dbName);
        if (pDb != NULL && pDb->stateTs != curMs) {
          mInfo("db:%s, stateTs changed by status msg, old stateTs:%" PRId64 " new stateTs:%" PRId64, pDb->name,
                pDb->stateTs, curMs);
          pDb->stateTs = curMs;
        }
        mndReleaseDb(pMnode, pDb);
      }
    }

    mndReleaseVgroup(pMnode, pVgroup);
  }

  SMnodeObj *pObj = mndAcquireMnode(pMnode, pDnode->id);
  if (pObj != NULL) {
    if (statusReq.mload.roleTimeMs == 0) {
      statusReq.mload.roleTimeMs = statusReq.rebootTime;
    }
    (void)mndUpdateMnodeState(pObj, &statusReq.mload);
    mndReleaseMnode(pMnode, pObj);
  }

  SQnodeObj *pQnode = mndAcquireQnode(pMnode, statusReq.qload.dnodeId);
  if (pQnode != NULL) {
    pQnode->load = statusReq.qload;
    mndReleaseQnode(pMnode, pQnode);
  }

  if (needCheck) {
    if (statusReq.sver != tsVersion) {
      if (pDnode != NULL) {
        pDnode->offlineReason = DND_REASON_VERSION_NOT_MATCH;
      }
      mError("dnode:%d, status msg version:%d not match cluster:%d", statusReq.dnodeId, statusReq.sver, tsVersion);
      terrno = TSDB_CODE_VERSION_NOT_COMPATIBLE;
      goto _OVER;
    }

    if (statusReq.dnodeId == 0) {
      mInfo("dnode:%d, %s first access, clusterId:%" PRId64, pDnode->id, pDnode->ep, pMnode->clusterId);
    } else {
      if (statusReq.clusterId != pMnode->clusterId) {
        if (pDnode != NULL) {
          pDnode->offlineReason = DND_REASON_CLUSTER_ID_NOT_MATCH;
        }
        mError("dnode:%d, clusterId %" PRId64 " not match exist %" PRId64, pDnode->id, statusReq.clusterId,
               pMnode->clusterId);
        terrno = TSDB_CODE_MND_INVALID_CLUSTER_ID;
        goto _OVER;
      }
    }

    // Verify whether the cluster parameters are consistent when status change from offline to ready
    pDnode->offlineReason = mndCheckClusterCfgPara(pMnode, pDnode, &statusReq.clusterCfg);
    if (pDnode->offlineReason != 0) {
      mError("dnode:%d, cluster cfg inconsistent since:%s", pDnode->id, offlineReason[pDnode->offlineReason]);
      if (terrno == 0) terrno = TSDB_CODE_MND_INVALID_CLUSTER_CFG;
      goto _OVER;
    }

    if (!online) {
      mInfo("dnode:%d, from offline to online, memory avail:%" PRId64 " total:%" PRId64 " cores:%.2f", pDnode->id,
            statusReq.memAvail, statusReq.memTotal, statusReq.numOfCores);
    } else {
      mInfo("dnode:%d, send dnode epset, online:%d dnodeVer:%" PRId64 ":%" PRId64 " reboot:%d", pDnode->id, online,
            statusReq.dnodeVer, dnodeVer, reboot);
    }

    pDnode->rebootTime = statusReq.rebootTime;
    pDnode->numOfCores = statusReq.numOfCores;
    pDnode->numOfSupportVnodes = statusReq.numOfSupportVnodes;
    pDnode->numOfDiskCfg = statusReq.numOfDiskCfg;
    pDnode->memAvail = statusReq.memAvail;
    pDnode->memTotal = statusReq.memTotal;
    pDnode->encryptionKeyStat = statusReq.clusterCfg.encryptionKeyStat;
    pDnode->encryptionKeyChksum = statusReq.clusterCfg.encryptionKeyChksum;
    if (memcmp(pDnode->machineId, statusReq.machineId, TSDB_MACHINE_ID_LEN) != 0) {
      tstrncpy(pDnode->machineId, statusReq.machineId, TSDB_MACHINE_ID_LEN + 1);
      if ((terrno = mndUpdateDnodeObj(pMnode, pDnode)) != 0) {
        goto _OVER;
      }
    }

    SStatusRsp statusRsp = {0};
    statusRsp.statusSeq++;
    statusRsp.dnodeVer = dnodeVer;
    statusRsp.dnodeCfg.dnodeId = pDnode->id;
    statusRsp.dnodeCfg.clusterId = pMnode->clusterId;
    statusRsp.pDnodeEps = taosArrayInit(mndGetDnodeSize(pMnode), sizeof(SDnodeEp));
    if (statusRsp.pDnodeEps == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto _OVER;
    }

    mndGetDnodeEps(pMnode, statusRsp.pDnodeEps);
    statusRsp.ipWhiteVer = pMnode->ipWhiteVer;

    int32_t contLen = tSerializeSStatusRsp(NULL, 0, &statusRsp);
    void   *pHead = rpcMallocCont(contLen);
    (void)tSerializeSStatusRsp(pHead, contLen, &statusRsp);
    taosArrayDestroy(statusRsp.pDnodeEps);

    pReq->info.rspLen = contLen;
    pReq->info.rsp = pHead;
  }

  pDnode->accessTimes++;
  pDnode->lastAccessTime = curMs;
  code = 0;

_OVER:
  mndReleaseDnode(pMnode, pDnode);
  taosArrayDestroy(statusReq.pVloads);
  (void)mndUpdClusterInfo(pReq);
  return code;
}

static int32_t mndProcessNotifyReq(SRpcMsg *pReq) {
  SMnode    *pMnode = pReq->info.node;
  SNotifyReq notifyReq = {0};
  int32_t    code = 0;

  if ((code = tDeserializeSNotifyReq(pReq->pCont, pReq->contLen, &notifyReq)) != 0) {
    terrno = code;
    goto _OVER;
  }

  int64_t clusterid = mndGetClusterId(pMnode);
  if (notifyReq.clusterId != 0 && notifyReq.clusterId != clusterid) {
    code = TSDB_CODE_MND_DNODE_DIFF_CLUSTER;
    mWarn("dnode:%d, its clusterid:%" PRId64 " differ from current cluster:%" PRId64 " since %s", notifyReq.dnodeId,
          notifyReq.clusterId, clusterid, tstrerror(code));
    goto _OVER;
  }

  int32_t nVgroup = taosArrayGetSize(notifyReq.pVloads);
  for (int32_t v = 0; v < nVgroup; ++v) {
    SVnodeLoadLite *pVload = taosArrayGet(notifyReq.pVloads, v);

    SVgObj *pVgroup = mndAcquireVgroup(pMnode, pVload->vgId);
    if (pVgroup != NULL) {
      pVgroup->numOfTimeSeries = pVload->nTimeSeries;
      mndReleaseVgroup(pMnode, pVgroup);
    }
  }
  code = mndUpdClusterInfo(pReq);
_OVER:
  tFreeSNotifyReq(&notifyReq);
  return code;
}

static int32_t mndCreateDnode(SMnode *pMnode, SRpcMsg *pReq, SCreateDnodeReq *pCreate) {
  int32_t  code = -1;
  SSdbRaw *pRaw = NULL;
  STrans  *pTrans = NULL;

  SDnodeObj dnodeObj = {0};
  dnodeObj.id = sdbGetMaxId(pMnode->pSdb, SDB_DNODE);
  dnodeObj.createdTime = taosGetTimestampMs();
  dnodeObj.updateTime = dnodeObj.createdTime;
  dnodeObj.port = pCreate->port;
  tstrncpy(dnodeObj.fqdn, pCreate->fqdn, TSDB_FQDN_LEN);
  (void)snprintf(dnodeObj.ep, TSDB_EP_LEN - 1, "%s:%u", pCreate->fqdn, pCreate->port);

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_GLOBAL, pReq, "create-dnode");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }
  mInfo("trans:%d, used to create dnode:%s", pTrans->id, dnodeObj.ep);
  TAOS_CHECK_GOTO(mndTransCheckConflict(pMnode, pTrans), NULL, _OVER);

  pRaw = mndDnodeActionEncode(&dnodeObj);
  if (pRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }
  TAOS_CHECK_GOTO(mndTransAppendCommitlog(pTrans, pRaw), NULL, _OVER);
  (void)sdbSetRawStatus(pRaw, SDB_STATUS_READY);
  pRaw = NULL;

  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);
  code = 0;

  (void)mndUpdateIpWhiteForAllUser(pMnode, TSDB_DEFAULT_USER, dnodeObj.fqdn, IP_WHITE_ADD,
                                   1);  // TODO: check the return value
_OVER:
  mndTransDrop(pTrans);
  sdbFreeRaw(pRaw);
  return code;
}

static int32_t mndProcessDnodeListReq(SRpcMsg *pReq) {
  SMnode       *pMnode = pReq->info.node;
  SSdb         *pSdb = pMnode->pSdb;
  SDnodeObj    *pObj = NULL;
  void         *pIter = NULL;
  SDnodeListRsp rsp = {0};
  int32_t       code = -1;

  rsp.dnodeList = taosArrayInit(5, sizeof(SEpSet));
  if (NULL == rsp.dnodeList) {
    mError("failed to alloc epSet while process dnode list req");
    code = terrno;
    goto _OVER;
  }

  while (1) {
    pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SEpSet epSet = {0};
    epSet.numOfEps = 1;
    tstrncpy(epSet.eps[0].fqdn, pObj->fqdn, TSDB_FQDN_LEN);
    epSet.eps[0].port = pObj->port;

    if (taosArrayPush(rsp.dnodeList, &epSet) == NULL) {
      if (terrno != 0) code = terrno;
      sdbRelease(pSdb, pObj);
      sdbCancelFetch(pSdb, pIter);
      goto _OVER;
    }

    sdbRelease(pSdb, pObj);
  }

  int32_t rspLen = tSerializeSDnodeListRsp(NULL, 0, &rsp);
  void   *pRsp = rpcMallocCont(rspLen);
  if (pRsp == NULL) {
    code = terrno;
    goto _OVER;
  }

  (void)tSerializeSDnodeListRsp(pRsp, rspLen, &rsp);

  pReq->info.rspLen = rspLen;
  pReq->info.rsp = pRsp;
  code = 0;

_OVER:

  if (code != 0) {
    mError("failed to get dnode list since %s", tstrerror(code));
  }

  tFreeSDnodeListRsp(&rsp);

  TAOS_RETURN(code);
}

static void getSlowLogScopeString(int32_t scope, char* result){
  if(scope == SLOW_LOG_TYPE_NULL) {
    (void)strcat(result, "NONE");
    return;
  }
  while(scope > 0){
    if(scope & SLOW_LOG_TYPE_QUERY) {
      (void)strcat(result, "QUERY");
      scope &= ~SLOW_LOG_TYPE_QUERY;
    } else if(scope & SLOW_LOG_TYPE_INSERT) {
      (void)strcat(result, "INSERT");
      scope &= ~SLOW_LOG_TYPE_INSERT;
    } else if(scope & SLOW_LOG_TYPE_OTHERS) {
      (void)strcat(result, "OTHERS");
      scope &= ~SLOW_LOG_TYPE_OTHERS;
    } else{
      (void)printf("invalid slow log scope:%d", scope);
      return;
    }

    if(scope > 0) {
      (void)strcat(result, "|");
    }
  }
}

static int32_t mndProcessShowVariablesReq(SRpcMsg *pReq) {
  SShowVariablesRsp rsp = {0};
  int32_t           code = -1;

  if (mndCheckOperPrivilege(pReq->info.node, pReq->info.conn.user, MND_OPER_SHOW_VARIBALES) != 0) {
    goto _OVER;
  }

  rsp.variables = taosArrayInit(16, sizeof(SVariablesInfo));
  if (NULL == rsp.variables) {
    mError("failed to alloc SVariablesInfo array while process show variables req");
    code = terrno;
    goto _OVER;
  }

  SVariablesInfo info = {0};

  (void)strcpy(info.name, "statusInterval");
  (void)snprintf(info.value, TSDB_CONFIG_VALUE_LEN, "%d", tsStatusInterval);
  (void)strcpy(info.scope, "server");
  if (taosArrayPush(rsp.variables, &info) == NULL) {
    code = terrno;
    goto _OVER;
  }

  (void)strcpy(info.name, "timezone");
  (void)snprintf(info.value, TSDB_CONFIG_VALUE_LEN, "%s", tsTimezoneStr);
  (void)strcpy(info.scope, "both");
  if (taosArrayPush(rsp.variables, &info) == NULL) {
    code = terrno;
    goto _OVER;
  }

  (void)strcpy(info.name, "locale");
  (void)snprintf(info.value, TSDB_CONFIG_VALUE_LEN, "%s", tsLocale);
  (void)strcpy(info.scope, "both");
  if (taosArrayPush(rsp.variables, &info) == NULL) {
    code = terrno;
    goto _OVER;
  }

  (void)strcpy(info.name, "charset");
  (void)snprintf(info.value, TSDB_CONFIG_VALUE_LEN, "%s", tsCharset);
  (void)strcpy(info.scope, "both");
  if (taosArrayPush(rsp.variables, &info) == NULL) {
    code = terrno;
    goto _OVER;
  }

  (void)strcpy(info.name, "monitor");
  (void)snprintf(info.value, TSDB_CONFIG_VALUE_LEN, "%d", tsEnableMonitor);
  (void)strcpy(info.scope, "server");
  if (taosArrayPush(rsp.variables, &info) == NULL) {
    code = terrno;
    goto _OVER;
  }

  (void)strcpy(info.name, "monitorInterval");
  (void)snprintf(info.value, TSDB_CONFIG_VALUE_LEN, "%d", tsMonitorInterval);
  (void)strcpy(info.scope, "server");
  if (taosArrayPush(rsp.variables, &info) == NULL) {
    code = terrno;
    goto _OVER;
  }

  (void)strcpy(info.name, "slowLogThreshold");
  (void)snprintf(info.value, TSDB_CONFIG_VALUE_LEN, "%d", tsSlowLogThreshold);
  (void)strcpy(info.scope, "server");
  if (taosArrayPush(rsp.variables, &info) == NULL) {
    code = terrno;
    goto _OVER;
  }

  (void)strcpy(info.name, "slowLogMaxLen");
  (void)snprintf(info.value, TSDB_CONFIG_VALUE_LEN, "%d", tsSlowLogMaxLen);
  (void)strcpy(info.scope, "server");
  if (taosArrayPush(rsp.variables, &info) == NULL) {
    code = terrno;
    goto _OVER;
  }

  char scopeStr[64] = {0};
  getSlowLogScopeString(tsSlowLogScope, scopeStr);
  (void)strcpy(info.name, "slowLogScope");
  (void)snprintf(info.value, TSDB_CONFIG_VALUE_LEN, "%s", scopeStr);
  (void)strcpy(info.scope, "server");
  if (taosArrayPush(rsp.variables, &info) == NULL) {
    code = terrno;
    goto _OVER;
  }

  int32_t rspLen = tSerializeSShowVariablesRsp(NULL, 0, &rsp);
  void   *pRsp = rpcMallocCont(rspLen);
  if (pRsp == NULL) {
    code = terrno;
    goto _OVER;
  }

  (void)tSerializeSShowVariablesRsp(pRsp, rspLen, &rsp);

  pReq->info.rspLen = rspLen;
  pReq->info.rsp = pRsp;
  code = 0;

_OVER:

  if (code != 0) {
    mError("failed to get show variables info since %s", tstrerror(code));
  }

  tFreeSShowVariablesRsp(&rsp);
  TAOS_RETURN(code);
}

static int32_t mndProcessCreateDnodeReq(SRpcMsg *pReq) {
  SMnode         *pMnode = pReq->info.node;
  int32_t         code = -1;
  SDnodeObj      *pDnode = NULL;
  SCreateDnodeReq createReq = {0};

  if ((code = grantCheck(TSDB_GRANT_DNODE)) != 0 || (code = grantCheck(TSDB_GRANT_CPU_CORES)) != 0) {
    goto _OVER;
  }

  TAOS_CHECK_GOTO(tDeserializeSCreateDnodeReq(pReq->pCont, pReq->contLen, &createReq), NULL, _OVER);

  mInfo("dnode:%s:%d, start to create", createReq.fqdn, createReq.port);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CREATE_DNODE), NULL, _OVER);

  if (createReq.fqdn[0] == 0 || createReq.port <= 0 || createReq.port > UINT16_MAX) {
    code = TSDB_CODE_MND_INVALID_DNODE_EP;
    goto _OVER;
  }

  char ep[TSDB_EP_LEN];
  (void)snprintf(ep, TSDB_EP_LEN, "%s:%d", createReq.fqdn, createReq.port);
  pDnode = mndAcquireDnodeByEp(pMnode, ep);
  if (pDnode != NULL) {
    code = TSDB_CODE_MND_DNODE_ALREADY_EXIST;
    goto _OVER;
  }

  code = mndCreateDnode(pMnode, pReq, &createReq);
  if (code == 0) {
    code = TSDB_CODE_ACTION_IN_PROGRESS;
    tsGrantHBInterval = 5;
  }

  char obj[200] = {0};
  (void)sprintf(obj, "%s:%d", createReq.fqdn, createReq.port);

  auditRecord(pReq, pMnode->clusterId, "createDnode", "", obj, createReq.sql, createReq.sqlLen);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("dnode:%s:%d, failed to create since %s", createReq.fqdn, createReq.port, tstrerror(code));
  }

  mndReleaseDnode(pMnode, pDnode);
  tFreeSCreateDnodeReq(&createReq);
  TAOS_RETURN(code);
}

extern int32_t mndProcessRestoreDnodeReqImpl(SRpcMsg *pReq);

int32_t mndProcessRestoreDnodeReq(SRpcMsg *pReq) { return mndProcessRestoreDnodeReqImpl(pReq); }

#ifndef TD_ENTERPRISE
int32_t mndProcessRestoreDnodeReqImpl(SRpcMsg *pReq) { return 0; }
#endif

static int32_t mndDropDnode(SMnode *pMnode, SRpcMsg *pReq, SDnodeObj *pDnode, SMnodeObj *pMObj, SQnodeObj *pQObj,
                            SSnodeObj *pSObj, int32_t numOfVnodes, bool force, bool unsafe) {
  int32_t  code = -1;
  SSdbRaw *pRaw = NULL;
  STrans  *pTrans = NULL;

  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_GLOBAL, pReq, "drop-dnode");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }
  mndTransSetSerial(pTrans);
  mInfo("trans:%d, used to drop dnode:%d, force:%d", pTrans->id, pDnode->id, force);
  TAOS_CHECK_GOTO(mndTransCheckConflict(pMnode, pTrans), NULL, _OVER);

  pRaw = mndDnodeActionEncode(pDnode);
  if (pRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }
  TAOS_CHECK_GOTO(mndTransAppendRedolog(pTrans, pRaw), NULL, _OVER);
  (void)sdbSetRawStatus(pRaw, SDB_STATUS_DROPPING);
  pRaw = NULL;

  pRaw = mndDnodeActionEncode(pDnode);
  if (pRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }
  TAOS_CHECK_GOTO(mndTransAppendCommitlog(pTrans, pRaw), NULL, _OVER);
  (void)sdbSetRawStatus(pRaw, SDB_STATUS_DROPPED);
  pRaw = NULL;

  if (pMObj != NULL) {
    mInfo("trans:%d, mnode on dnode:%d will be dropped", pTrans->id, pDnode->id);
    TAOS_CHECK_GOTO(mndSetDropMnodeInfoToTrans(pMnode, pTrans, pMObj, force), NULL, _OVER);
  }

  if (pQObj != NULL) {
    mInfo("trans:%d, qnode on dnode:%d will be dropped", pTrans->id, pDnode->id);
    TAOS_CHECK_GOTO(mndSetDropQnodeInfoToTrans(pMnode, pTrans, pQObj, force), NULL, _OVER);
  }

  if (pSObj != NULL) {
    mInfo("trans:%d, snode on dnode:%d will be dropped", pTrans->id, pDnode->id);
    TAOS_CHECK_GOTO(mndSetDropSnodeInfoToTrans(pMnode, pTrans, pSObj, force), NULL, _OVER);
  }

  if (numOfVnodes > 0) {
    mInfo("trans:%d, %d vnodes on dnode:%d will be dropped", pTrans->id, numOfVnodes, pDnode->id);
    TAOS_CHECK_GOTO(mndSetMoveVgroupsInfoToTrans(pMnode, pTrans, pDnode->id, force, unsafe), NULL, _OVER);
  }

  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);

  (void)mndUpdateIpWhiteForAllUser(pMnode, TSDB_DEFAULT_USER, pDnode->fqdn, IP_WHITE_DROP,
                                   1);  // TODO: check the return value
  code = 0;

_OVER:
  mndTransDrop(pTrans);
  sdbFreeRaw(pRaw);
  TAOS_RETURN(code);
}

static bool mndIsEmptyDnode(SMnode *pMnode, int32_t dnodeId) {
  bool       isEmpty = false;
  SMnodeObj *pMObj = NULL;
  SQnodeObj *pQObj = NULL;
  SSnodeObj *pSObj = NULL;

  pQObj = mndAcquireQnode(pMnode, dnodeId);
  if (pQObj) goto _OVER;

  pSObj = mndAcquireSnode(pMnode, dnodeId);
  if (pSObj) goto _OVER;

  pMObj = mndAcquireMnode(pMnode, dnodeId);
  if (pMObj) goto _OVER;

  int32_t numOfVnodes = mndGetVnodesNum(pMnode, dnodeId);
  if (numOfVnodes > 0) goto _OVER;

  isEmpty = true;
_OVER:
  mndReleaseMnode(pMnode, pMObj);
  mndReleaseQnode(pMnode, pQObj);
  mndReleaseSnode(pMnode, pSObj);
  return isEmpty;
}

static int32_t mndProcessDropDnodeReq(SRpcMsg *pReq) {
  SMnode       *pMnode = pReq->info.node;
  int32_t       code = -1;
  SDnodeObj    *pDnode = NULL;
  SMnodeObj    *pMObj = NULL;
  SQnodeObj    *pQObj = NULL;
  SSnodeObj    *pSObj = NULL;
  SDropDnodeReq dropReq = {0};

  TAOS_CHECK_GOTO(tDeserializeSDropDnodeReq(pReq->pCont, pReq->contLen, &dropReq), NULL, _OVER);

  mInfo("dnode:%d, start to drop, ep:%s:%d, force:%s, unsafe:%s", dropReq.dnodeId, dropReq.fqdn, dropReq.port,
        dropReq.force ? "true" : "false", dropReq.unsafe ? "true" : "false");
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_DROP_MNODE), NULL, _OVER);

  bool force = dropReq.force;
  if (dropReq.unsafe) {
    force = true;
  }

  pDnode = mndAcquireDnode(pMnode, dropReq.dnodeId);
  if (pDnode == NULL) {
    int32_t err = terrno;
    char    ep[TSDB_EP_LEN + 1] = {0};
    (void)snprintf(ep, sizeof(ep), dropReq.fqdn, dropReq.port);
    pDnode = mndAcquireDnodeByEp(pMnode, ep);
    if (pDnode == NULL) {
      code = err;
      goto _OVER;
    }
  }

  pQObj = mndAcquireQnode(pMnode, dropReq.dnodeId);
  pSObj = mndAcquireSnode(pMnode, dropReq.dnodeId);
  pMObj = mndAcquireMnode(pMnode, dropReq.dnodeId);
  if (pMObj != NULL) {
    if (sdbGetSize(pMnode->pSdb, SDB_MNODE) <= 1) {
      code = TSDB_CODE_MND_TOO_FEW_MNODES;
      goto _OVER;
    }
    if (pMnode->selfDnodeId == dropReq.dnodeId) {
      code = TSDB_CODE_MND_CANT_DROP_LEADER;
      goto _OVER;
    }
  }

  int32_t numOfVnodes = mndGetVnodesNum(pMnode, pDnode->id);
  bool    isonline = mndIsDnodeOnline(pDnode, taosGetTimestampMs());

  if (isonline && force) {
    code = TSDB_CODE_DNODE_ONLY_USE_WHEN_OFFLINE;
    mError("dnode:%d, failed to drop since %s, vnodes:%d mnode:%d qnode:%d snode:%d", pDnode->id, tstrerror(code),
           numOfVnodes, pMObj != NULL, pQObj != NULL, pSObj != NULL);
    goto _OVER;
  }

  bool isEmpty = mndIsEmptyDnode(pMnode, pDnode->id);
  if (!isonline && !force && !isEmpty) {
    code = TSDB_CODE_DNODE_OFFLINE;
    mError("dnode:%d, failed to drop since %s, vnodes:%d mnode:%d qnode:%d snode:%d", pDnode->id, tstrerror(code),
           numOfVnodes, pMObj != NULL, pQObj != NULL, pSObj != NULL);
    goto _OVER;
  }

  code = mndDropDnode(pMnode, pReq, pDnode, pMObj, pQObj, pSObj, numOfVnodes, force, dropReq.unsafe);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  char obj1[30] = {0};
  (void)sprintf(obj1, "%d", dropReq.dnodeId);

  auditRecord(pReq, pMnode->clusterId, "dropDnode", "", obj1, dropReq.sql, dropReq.sqlLen);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("dnode:%d, failed to drop since %s", dropReq.dnodeId, tstrerror(code));
  }

  mndReleaseDnode(pMnode, pDnode);
  mndReleaseMnode(pMnode, pMObj);
  mndReleaseQnode(pMnode, pQObj);
  mndReleaseSnode(pMnode, pSObj);
  tFreeSDropDnodeReq(&dropReq);
  TAOS_RETURN(code);
}

static int32_t mndMCfg2DCfg(SMCfgDnodeReq *pMCfgReq, SDCfgDnodeReq *pDCfgReq) {
  int32_t code = 0;
  char *p = pMCfgReq->config;
  while (*p) {
    if (*p == ' ') {
      break;
    }
    p++;
  }

  size_t optLen = p - pMCfgReq->config;
  (void)strncpy(pDCfgReq->config, pMCfgReq->config, optLen);
  pDCfgReq->config[optLen] = 0;

  if (' ' == pMCfgReq->config[optLen]) {
    // 'key value'
    if (strlen(pMCfgReq->value) != 0) goto _err;
    (void)strcpy(pDCfgReq->value, p + 1);
  } else {
    // 'key' 'value'
    if (strlen(pMCfgReq->value) == 0) goto _err;
    (void)strcpy(pDCfgReq->value, pMCfgReq->value);
  }

  TAOS_RETURN(code);

_err:
  mError("dnode:%d, failed to config since invalid conf:%s", pMCfgReq->dnodeId, pMCfgReq->config);
  code = TSDB_CODE_INVALID_CFG;
  TAOS_RETURN(code);
}

static int32_t mndSendCfgDnodeReq(SMnode *pMnode, int32_t dnodeId, SDCfgDnodeReq *pDcfgReq) {
  int32_t code = -1;
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
  while (1) {
    SDnodeObj *pDnode = NULL;
    pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pDnode);
    if (pIter == NULL) break;

    if (pDnode->id == dnodeId || dnodeId == -1 || dnodeId == 0) {
      SEpSet  epSet = mndGetDnodeEpset(pDnode);
      int32_t bufLen = tSerializeSDCfgDnodeReq(NULL, 0, pDcfgReq);
      void   *pBuf = rpcMallocCont(bufLen);

      if (pBuf != NULL) {
        (void)tSerializeSDCfgDnodeReq(pBuf, bufLen, pDcfgReq);
        mInfo("dnode:%d, send config req to dnode, config:%s value:%s", dnodeId, pDcfgReq->config, pDcfgReq->value);
        SRpcMsg rpcMsg = {.msgType = TDMT_DND_CONFIG_DNODE, .pCont = pBuf, .contLen = bufLen};
        code = tmsgSendReq(&epSet, &rpcMsg);
      }
    }

    sdbRelease(pSdb, pDnode);
  }

  if (code == -1) {
    code = TSDB_CODE_MND_DNODE_NOT_EXIST;
  }
  TAOS_RETURN(code);
}

static int32_t mndProcessConfigDnodeReq(SRpcMsg *pReq) {
  int32_t       code = 0;
  SMnode       *pMnode = pReq->info.node;
  SMCfgDnodeReq cfgReq = {0};
  TAOS_CHECK_RETURN(tDeserializeSMCfgDnodeReq(pReq->pCont, pReq->contLen, &cfgReq));
  int8_t updateIpWhiteList = 0;
  mInfo("dnode:%d, start to config, option:%s, value:%s", cfgReq.dnodeId, cfgReq.config, cfgReq.value);
  if ((code = mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CONFIG_DNODE)) != 0) {
    tFreeSMCfgDnodeReq(&cfgReq);
    TAOS_RETURN(code);
  }

  SDCfgDnodeReq dcfgReq = {0};
  if (strcasecmp(cfgReq.config, "resetlog") == 0) {
    (void)strcpy(dcfgReq.config, "resetlog");
#ifdef TD_ENTERPRISE
  } else if (strncasecmp(cfgReq.config, "s3blocksize", 11) == 0) {
    int32_t optLen = strlen("s3blocksize");
    int32_t flag = -1;
    int32_t code = mndMCfgGetValInt32(&cfgReq, optLen, &flag);
    if (code < 0) return code;

    if (flag > 1024 * 1024 || (flag > -1 && flag < 1024) || flag < -1) {
      mError("dnode:%d, failed to config s3blocksize since value:%d. Valid range: -1 or [1024, 1024 * 1024]",
             cfgReq.dnodeId, flag);
      code = TSDB_CODE_INVALID_CFG;
      tFreeSMCfgDnodeReq(&cfgReq);
      TAOS_RETURN(code);
    }

    strcpy(dcfgReq.config, "s3blocksize");
    snprintf(dcfgReq.value, TSDB_DNODE_VALUE_LEN, "%d", flag);
#endif
  } else {
    TAOS_CHECK_GOTO (mndMCfg2DCfg(&cfgReq, &dcfgReq), NULL, _err_out);
    if (strlen(dcfgReq.config) > TSDB_DNODE_CONFIG_LEN) {
      mError("dnode:%d, failed to config since config is too long", cfgReq.dnodeId);
      code = TSDB_CODE_INVALID_CFG;
      goto _err_out;
    }
    if (strncasecmp(dcfgReq.config, "enableWhiteList", strlen("enableWhiteList")) == 0) {
      updateIpWhiteList = 1;
    }

    TAOS_CHECK_GOTO(cfgCheckRangeForDynUpdate(taosGetCfg(), dcfgReq.config, dcfgReq.value, true), NULL, _err_out);
  }

  {  // audit
    char obj[50] = {0};
    (void)sprintf(obj, "%d", cfgReq.dnodeId);

    auditRecord(pReq, pMnode->clusterId, "alterDnode", obj, "", cfgReq.sql, cfgReq.sqlLen);
  }

  tFreeSMCfgDnodeReq(&cfgReq);

  code = mndSendCfgDnodeReq(pMnode, cfgReq.dnodeId, &dcfgReq);

  // dont care suss or succ;
  if (updateIpWhiteList) mndRefreshUserIpWhiteList(pMnode);
  TAOS_RETURN(code);

_err_out:
  tFreeSMCfgDnodeReq(&cfgReq);
  TAOS_RETURN(code);
}

static int32_t mndProcessConfigDnodeRsp(SRpcMsg *pRsp) {
  mInfo("config rsp from dnode");
  return 0;
}

static int32_t mndProcessCreateEncryptKeyReqImpl(SRpcMsg *pReq, int32_t dnodeId, SDCfgDnodeReq *pDcfgReq) {
  int32_t code = 0;
  SMnode *pMnode = pReq->info.node;
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
  int8_t  encrypting = 0;

  const STraceId *trace = &pReq->info.traceId;

  int32_t klen = strlen(pDcfgReq->value);
  if (klen > ENCRYPT_KEY_LEN || klen < ENCRYPT_KEY_LEN_MIN) {
    code = TSDB_CODE_DNODE_INVALID_ENCRYPT_KLEN;
    mGError("msg:%p, failed to create encrypt_key since invalid key length:%d, valid range:[%d, %d]", pReq, klen,
            ENCRYPT_KEY_LEN_MIN, ENCRYPT_KEY_LEN);
    goto _exit;
  }

  if (0 != (encrypting = atomic_val_compare_exchange_8(&pMnode->encryptMgmt.encrypting, 0, 1))) {
    code = TSDB_CODE_QRY_DUPLICATED_OPERATION;
    mGWarn("msg:%p, failed to create encrypt key since %s, encrypting:%" PRIi8, pReq, tstrerror(code), encrypting);
    goto _exit;
  }

  if (tsEncryptionKeyStat == ENCRYPT_KEY_STAT_SET || tsEncryptionKeyStat == ENCRYPT_KEY_STAT_LOADED) {
    atomic_store_8(&pMnode->encryptMgmt.encrypting, 0);
    code = TSDB_CODE_QRY_DUPLICATED_OPERATION;
    mGWarn("msg:%p, failed to create encrypt key since %s, stat:%" PRIi8 ", checksum:%u", pReq, tstrerror(code),
           tsEncryptionKeyStat, tsEncryptionKeyChksum);
    goto _exit;
  }

  atomic_store_16(&pMnode->encryptMgmt.nEncrypt, 0);
  atomic_store_16(&pMnode->encryptMgmt.nSuccess, 0);
  atomic_store_16(&pMnode->encryptMgmt.nFailed, 0);

  while (1) {
    SDnodeObj *pDnode = NULL;
    pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pDnode);
    if (pIter == NULL) break;
    if (pDnode->offlineReason != DND_REASON_ONLINE) {
      mGWarn("msg:%p, don't send create encrypt_key req since dnode:%d in offline state:%s", pReq, pDnode->id,
             offlineReason[pDnode->offlineReason]);
      sdbRelease(pSdb, pDnode);
      continue;
    }

    if (dnodeId == -1 || pDnode->id == dnodeId || dnodeId == 0) {
      SEpSet  epSet = mndGetDnodeEpset(pDnode);
      int32_t bufLen = tSerializeSDCfgDnodeReq(NULL, 0, pDcfgReq);
      void   *pBuf = rpcMallocCont(bufLen);

      if (pBuf != NULL) {
        (void)tSerializeSDCfgDnodeReq(pBuf, bufLen, pDcfgReq);
        SRpcMsg rpcMsg = {.msgType = TDMT_DND_CREATE_ENCRYPT_KEY, .pCont = pBuf, .contLen = bufLen};
        if (0 == tmsgSendReq(&epSet, &rpcMsg)) {
          (void)atomic_add_fetch_16(&pMnode->encryptMgmt.nEncrypt, 1);
        }
      }
    }

    sdbRelease(pSdb, pDnode);
  }

  if (atomic_load_16(&pMnode->encryptMgmt.nEncrypt) <= 0) {
    atomic_store_8(&pMnode->encryptMgmt.encrypting, 0);
  }

_exit:
  if (code != 0) {
    if (terrno == 0) terrno = code;
  }
  return code;
}

static int32_t mndProcessCreateEncryptKeyReq(SRpcMsg *pReq) {
  int32_t code = 0;

#ifdef TD_ENTERPRISE
  SMnode       *pMnode = pReq->info.node;
  SMCfgDnodeReq cfgReq = {0};
  TAOS_CHECK_RETURN(tDeserializeSMCfgDnodeReq(pReq->pCont, pReq->contLen, &cfgReq));

  if ((code = mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CONFIG_DNODE)) != 0) {
    tFreeSMCfgDnodeReq(&cfgReq);
    TAOS_RETURN(code);
  }
  const STraceId *trace = &pReq->info.traceId;
  SDCfgDnodeReq   dcfgReq = {0};
  if (strncasecmp(cfgReq.config, "encrypt_key", 12) == 0) {
    strcpy(dcfgReq.config, cfgReq.config);
    strcpy(dcfgReq.value, cfgReq.value);
    tFreeSMCfgDnodeReq(&cfgReq);
    return mndProcessCreateEncryptKeyReqImpl(pReq, cfgReq.dnodeId, &dcfgReq);
  } else {
    code = TSDB_CODE_PAR_INTERNAL_ERROR;
    tFreeSMCfgDnodeReq(&cfgReq);
    TAOS_RETURN(code);
  }

#else
  TAOS_RETURN(code);
#endif
}

static int32_t mndProcessCreateEncryptKeyRsp(SRpcMsg *pRsp) {
  SMnode *pMnode = pRsp->info.node;
  int16_t nSuccess = 0;
  int16_t nFailed = 0;

  if (0 == pRsp->code) {
    nSuccess = atomic_add_fetch_16(&pMnode->encryptMgmt.nSuccess, 1);
  } else {
    nFailed = atomic_add_fetch_16(&pMnode->encryptMgmt.nFailed, 1);
  }

  int16_t nReq = atomic_load_16(&pMnode->encryptMgmt.nEncrypt);
  bool    finished = nSuccess + nFailed >= nReq;

  if (finished) {
    atomic_store_8(&pMnode->encryptMgmt.encrypting, 0);
  }

  const STraceId *trace = &pRsp->info.traceId;
  mGInfo("msg:%p, create encrypt key rsp, nReq:%" PRIi16 ", nSucess:%" PRIi16 ", nFailed:%" PRIi16 ", %s", pRsp, nReq,
         nSuccess, nFailed, finished ? "encrypt done" : "in encrypting");

  return 0;
}

static int32_t mndRetrieveConfigs(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode *pMnode = pReq->info.node;
  int32_t totalRows = 0;
  int32_t numOfRows = 0;
  char   *cfgOpts[TSDB_CONFIG_NUMBER] = {0};
  char    cfgVals[TSDB_CONFIG_NUMBER][TSDB_CONFIG_VALUE_LEN + 1] = {0};
  char   *pWrite = NULL;
  int32_t cols = 0;

  cfgOpts[totalRows] = "statusInterval";
  (void)snprintf(cfgVals[totalRows], TSDB_CONFIG_VALUE_LEN, "%d", tsStatusInterval);
  totalRows++;

  cfgOpts[totalRows] = "timezone";
  (void)snprintf(cfgVals[totalRows], TSDB_CONFIG_VALUE_LEN, "%s", tsTimezoneStr);
  totalRows++;

  cfgOpts[totalRows] = "locale";
  (void)snprintf(cfgVals[totalRows], TSDB_CONFIG_VALUE_LEN, "%s", tsLocale);
  totalRows++;

  cfgOpts[totalRows] = "charset";
  (void)snprintf(cfgVals[totalRows], TSDB_CONFIG_VALUE_LEN, "%s", tsCharset);
  totalRows++;

  cfgOpts[totalRows] = "monitor";
  (void)snprintf(cfgVals[totalRows], TSDB_CONFIG_VALUE_LEN, "%d", tsEnableMonitor);
  totalRows++;

  cfgOpts[totalRows] = "monitorInterval";
  (void)snprintf(cfgVals[totalRows], TSDB_CONFIG_VALUE_LEN, "%d", tsMonitorInterval);
  totalRows++;

  cfgOpts[totalRows] = "slowLogThreshold";
  (void)snprintf(cfgVals[totalRows], TSDB_CONFIG_VALUE_LEN, "%d", tsSlowLogThreshold);
  totalRows++;

  cfgOpts[totalRows] = "slowLogMaxLen";
  (void)snprintf(cfgVals[totalRows], TSDB_CONFIG_VALUE_LEN, "%d", tsSlowLogMaxLen);
  totalRows++;

  char scopeStr[64] = {0};
  getSlowLogScopeString(tsSlowLogScope, scopeStr);
  cfgOpts[totalRows] = "slowLogScope";
  (void)snprintf(cfgVals[totalRows], TSDB_CONFIG_VALUE_LEN, "%s", scopeStr);
  totalRows++;

  char buf[TSDB_CONFIG_OPTION_LEN + VARSTR_HEADER_SIZE] = {0};
  char bufVal[TSDB_CONFIG_VALUE_LEN + VARSTR_HEADER_SIZE] = {0};

  for (int32_t i = 0; i < totalRows; i++) {
    cols = 0;

    STR_WITH_MAXSIZE_TO_VARSTR(buf, cfgOpts[i], TSDB_CONFIG_OPTION_LEN);
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    (void)colDataSetVal(pColInfo, numOfRows, (const char *)buf, false);

    STR_WITH_MAXSIZE_TO_VARSTR(bufVal, cfgVals[i], TSDB_CONFIG_VALUE_LEN);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    (void)colDataSetVal(pColInfo, numOfRows, (const char *)bufVal, false);

    numOfRows++;
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextConfig(SMnode *pMnode, void *pIter) {}

static int32_t mndRetrieveDnodes(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode    *pMnode = pReq->info.node;
  SSdb      *pSdb = pMnode->pSdb;
  int32_t    numOfRows = 0;
  int32_t    cols = 0;
  ESdbStatus objStatus = 0;
  SDnodeObj *pDnode = NULL;
  int64_t    curMs = taosGetTimestampMs();
  char       buf[TSDB_EP_LEN + VARSTR_HEADER_SIZE];

  while (numOfRows < rows) {
    pShow->pIter = sdbFetchAll(pSdb, SDB_DNODE, pShow->pIter, (void **)&pDnode, &objStatus, true);
    if (pShow->pIter == NULL) break;
    bool online = mndIsDnodeOnline(pDnode, curMs);

    cols = 0;

    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    (void)colDataSetVal(pColInfo, numOfRows, (const char *)&pDnode->id, false);

    STR_WITH_MAXSIZE_TO_VARSTR(buf, pDnode->ep, pShow->pMeta->pSchemas[cols].bytes);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    (void)colDataSetVal(pColInfo, numOfRows, buf, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    int16_t id = mndGetVnodesNum(pMnode, pDnode->id);
    (void)colDataSetVal(pColInfo, numOfRows, (const char *)&id, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    (void)colDataSetVal(pColInfo, numOfRows, (const char *)&pDnode->numOfSupportVnodes, false);

    const char *status = "ready";
    if (objStatus == SDB_STATUS_CREATING) status = "creating";
    if (objStatus == SDB_STATUS_DROPPING) status = "dropping";
    if (!online) {
      if (objStatus == SDB_STATUS_CREATING)
        status = "creating*";
      else if (objStatus == SDB_STATUS_DROPPING)
        status = "dropping*";
      else
        status = "offline";
    }

    STR_TO_VARSTR(buf, status);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    (void)colDataSetVal(pColInfo, numOfRows, buf, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    (void)colDataSetVal(pColInfo, numOfRows, (const char *)&pDnode->createdTime, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    (void)colDataSetVal(pColInfo, numOfRows, (const char *)&pDnode->rebootTime, false);

    char *b = taosMemoryCalloc(VARSTR_HEADER_SIZE + strlen(offlineReason[pDnode->offlineReason]) + 1, 1);
    STR_TO_VARSTR(b, online ? "" : offlineReason[pDnode->offlineReason]);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    (void)colDataSetVal(pColInfo, numOfRows, b, false);
    taosMemoryFreeClear(b);

#ifdef TD_ENTERPRISE
    STR_TO_VARSTR(buf, pDnode->machineId);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    (void)colDataSetVal(pColInfo, numOfRows, buf, false);
#endif

    numOfRows++;
    sdbRelease(pSdb, pDnode);
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextDnode(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_DNODE);
}

// get int32_t value from 'SMCfgDnodeReq'
static int32_t mndMCfgGetValInt32(SMCfgDnodeReq *pMCfgReq, int32_t optLen, int32_t *pOutValue) {
  int32_t code = 0;
  if (' ' != pMCfgReq->config[optLen] && 0 != pMCfgReq->config[optLen]) {
    goto _err;
  }

  if (' ' == pMCfgReq->config[optLen]) {
    // 'key value'
    if (strlen(pMCfgReq->value) != 0) goto _err;
    *pOutValue = atoi(pMCfgReq->config + optLen + 1);
  } else {
    // 'key' 'value'
    if (strlen(pMCfgReq->value) == 0) goto _err;
    *pOutValue = atoi(pMCfgReq->value);
  }

  TAOS_RETURN(code);

_err:
  mError("dnode:%d, failed to config since invalid conf:%s", pMCfgReq->dnodeId, pMCfgReq->config);
  code = TSDB_CODE_INVALID_CFG;
  TAOS_RETURN(code);
}

SArray *mndGetAllDnodeFqdns(SMnode *pMnode) {
  SDnodeObj *pObj = NULL;
  void      *pIter = NULL;
  SSdb      *pSdb = pMnode->pSdb;
  SArray    *fqdns = taosArrayInit(4, sizeof(void *));
  while (1) {
    pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    char *fqdn = taosStrdup(pObj->fqdn);
    if (taosArrayPush(fqdns, &fqdn) == NULL) {
      mError("failed to fqdn into array, but continue at this time");
    }
    sdbRelease(pSdb, pObj);
  }
  return fqdns;
}
