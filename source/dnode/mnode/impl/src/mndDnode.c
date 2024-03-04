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
#include <stdio.h>
#include "tjson.h"
#include "mndDnode.h"
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
#include "tmisce.h"
#include "tunit.h"
#include "taos_monitor.h"

#define TSDB_DNODE_VER_NUMBER   2
#define TSDB_DNODE_RESERVE_SIZE 64

static const char *offlineReason[] = {
    "",
    "status msg timeout",
    "status not received",
    "version not match",
    "dnodeId not match",
    "clusterId not match",
    "interval not match",
    "timezone not match",
    "locale not match",
    "charset not match",
    "ttlChangeOnWrite not match",
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
  snprintf(dnodeObj.ep, TSDB_EP_LEN - 1, "%s:%u", tsLocalFqdn, tsServerPort);
  char *machineId = tGetMachineId();
  if (machineId) {
    memcpy(dnodeObj.machineId, machineId, TSDB_MACHINE_ID_LEN);
    taosMemoryFreeClear(machineId);
  } else {
#if defined(TD_ENTERPRISE) && !defined(GRANTS_CFG)
    terrno = TSDB_CODE_DNODE_NO_MACHINE_CODE;
    goto _OVER;
#endif
  }

  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_GLOBAL, NULL, "create-dnode");
  if (pTrans == NULL) goto _OVER;
  mInfo("trans:%d, used to create dnode:%s on first deploy", pTrans->id, dnodeObj.ep);

  pRaw = mndDnodeActionEncode(&dnodeObj);
  if (pRaw == NULL || mndTransAppendCommitlog(pTrans, pRaw) != 0) goto _OVER;
  (void)sdbSetRawStatus(pRaw, SDB_STATUS_READY);
  pRaw = NULL;

  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;
  code = 0;
  mndUpdateIpWhiteForAllUser(pMnode, TSDB_DEFAULT_USER, dnodeObj.fqdn, IP_WHITE_ADD, 1);

_OVER:
  mndTransDrop(pTrans);
  sdbFreeRaw(pRaw);
  return code;
}

static SSdbRaw *mndDnodeActionEncode(SDnodeObj *pDnode) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_DNODE, TSDB_DNODE_VER_NUMBER, sizeof(SDnodeObj) + TSDB_DNODE_RESERVE_SIZE);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pDnode->id, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pDnode->createdTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pDnode->updateTime, _OVER)
  SDB_SET_INT16(pRaw, dataPos, pDnode->port, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pDnode->fqdn, TSDB_FQDN_LEN, _OVER)
  SDB_SET_RESERVE(pRaw, dataPos, TSDB_DNODE_RESERVE_SIZE, _OVER)
  SDB_SET_INT16(pRaw, dataPos, TSDB_ACTIVE_KEY_LEN, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pDnode->active, TSDB_ACTIVE_KEY_LEN, _OVER)
  SDB_SET_INT16(pRaw, dataPos, TSDB_CONN_ACTIVE_KEY_LEN, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pDnode->connActive, TSDB_CONN_ACTIVE_KEY_LEN, _OVER)
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
  SDB_GET_RESERVE(pRaw, dataPos, TSDB_DNODE_RESERVE_SIZE, _OVER)
  if (sver > 1) {
    int16_t keyLen = 0;
    SDB_GET_INT16(pRaw, dataPos, &keyLen, _OVER)
    SDB_GET_BINARY(pRaw, dataPos, pDnode->active, keyLen, _OVER)
    SDB_GET_INT16(pRaw, dataPos, &keyLen, _OVER)
    SDB_GET_BINARY(pRaw, dataPos, pDnode->connActive, keyLen, _OVER)
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
  snprintf(ep, TSDB_EP_LEN - 1, "%s:%u", pDnode->fqdn, pDnode->port);
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
  if (strncmp(pOld->active, pNew->active, TSDB_ACTIVE_KEY_LEN) != 0) {
    strncpy(pOld->active, pNew->active, TSDB_ACTIVE_KEY_LEN);
  }
  if (strncmp(pOld->connActive, pNew->connActive, TSDB_CONN_ACTIVE_KEY_LEN) != 0) {
    strncpy(pOld->connActive, pNew->connActive, TSDB_CONN_ACTIVE_KEY_LEN);
  }
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
  addEpIntoEpSet(&epSet, pDnode->fqdn, pDnode->port);
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
    if (pDnode->rebootTime > 0) {
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
    taosArrayPush(pDnodeEps, &dnodeEp);
  }
}

void mndGetDnodeData(SMnode *pMnode, SArray *pDnodeInfo) {
  SSdb *pSdb = pMnode->pSdb;

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

    taosArrayPush(pDnodeInfo, &dInfo);
  }
}

static int32_t mndCheckClusterCfgPara(SMnode *pMnode, SDnodeObj *pDnode, const SClusterCfg *pCfg) {
  if (pCfg->statusInterval != tsStatusInterval) {
    mError("dnode:%d, statusInterval:%d inconsistent with cluster:%d", pDnode->id, pCfg->statusInterval,
           tsStatusInterval);
    return DND_REASON_STATUS_INTERVAL_NOT_MATCH;
  }

  if ((0 != strcasecmp(pCfg->timezone, tsTimezoneStr)) && (pMnode->checkTime != pCfg->checkTime)) {
    mError("dnode:%d, timezone:%s checkTime:%" PRId64 " inconsistent with cluster %s %" PRId64, pDnode->id,
           pCfg->timezone, pCfg->checkTime, tsTimezoneStr, pMnode->checkTime);
    return DND_REASON_TIME_ZONE_NOT_MATCH;
  }

  if (0 != strcasecmp(pCfg->locale, tsLocale)) {
    mError("dnode:%d, locale:%s inconsistent with cluster:%s", pDnode->id, pCfg->locale, tsLocale);
    return DND_REASON_LOCALE_NOT_MATCH;
  }

  if (0 != strcasecmp(pCfg->charset, tsCharset)) {
    mError("dnode:%d, charset:%s inconsistent with cluster:%s", pDnode->id, pCfg->charset, tsCharset);
    return DND_REASON_CHARSET_NOT_MATCH;
  }

  if (pCfg->ttlChangeOnWrite != tsTtlChangeOnWrite) {
    mError("dnode:%d, ttlChangeOnWrite:%d inconsistent with cluster:%d", pDnode->id, pCfg->ttlChangeOnWrite,
           tsTtlChangeOnWrite);
    return DND_REASON_TTL_CHANGE_ON_WRITE_NOT_MATCH;
  }
  int8_t enable = tsEnableWhiteList ? 1 : 0;
  if (pCfg->enableWhiteList != enable) {
    mError("dnode:%d, enable :%d inconsistent with cluster:%d", pDnode->id, pCfg->enableWhiteList, enable);
    return DND_REASON_ENABLE_WHITELIST_NOT_MATCH;
  }

  return 0;
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

static int32_t mndProcessStatisReq(SRpcMsg *pReq) {
  SMnode    *pMnode = pReq->info.node;
  SStatisReq statisReq = {0};
  int32_t    code = -1;

  char strClusterId[TSDB_CLUSTER_ID_LEN] = {0};
  sprintf(strClusterId, "%"PRId64, pMnode->clusterId);

  if (tDeserializeSStatisReq(pReq->pCont, pReq->contLen, &statisReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return code;
  }

  if(tsMonitorLogProtocol){
    mInfo("process statis req,\n %s", statisReq.pCont);
  }

  SJson* pJson = tjsonParse(statisReq.pCont);

  int32_t ts_size = tjsonGetArraySize(pJson);

  for(int32_t i = 0; i < ts_size; i++){
    SJson* item = tjsonGetArrayItem(pJson, i);

    SJson* tables = tjsonGetObjectItem(item, "tables");

    int32_t tableSize = tjsonGetArraySize(tables);
    for(int32_t i = 0; i < tableSize; i++){
      SJson* table = tjsonGetArrayItem(tables, i);

      char tableName[MONITOR_TABLENAME_LEN] = {0};
      tjsonGetStringValue(table, "name", tableName);

      SJson* metricGroups = tjsonGetObjectItem(table, "metric_groups");

      int32_t size = tjsonGetArraySize(metricGroups);
      for(int32_t i = 0; i < size; i++){
        SJson* item = tjsonGetArrayItem(metricGroups, i);

        SJson* arrayTag = tjsonGetObjectItem(item, "tags");

        int32_t tagSize = tjsonGetArraySize(arrayTag);
        for(int32_t j = 0; j < tagSize; j++){
          SJson* item = tjsonGetArrayItem(arrayTag, j);

          char tagName[MONITOR_TAG_NAME_LEN] = {0};
          tjsonGetStringValue(item, "name", tagName);

          if(strncmp(tagName, "cluster_id", MONITOR_TAG_NAME_LEN) == 0) {
            tjsonDeleteItemFromObject(item, "value");
            tjsonAddStringToObject(item, "value", strClusterId);
          }
        }
      }
    }
  }

  char *pCont = tjsonToString(pJson);
  monSendContent(pCont);

  if(pJson != NULL){
    tjsonDelete(pJson);
    pJson = NULL;
  }

  if(pCont != NULL){
    taosMemoryFree(pCont);
    pCont = NULL;
  }

  tFreeSStatisReq(&statisReq);
  return 0;

/*
  SJson* pJson = tjsonParse(statisReq.pCont);

  int32_t ts_size = tjsonGetArraySize(pJson);

  for(int32_t i = 0; i < ts_size; i++){
    SJson* item = tjsonGetArrayItem(pJson, i);

    SJson* tables = tjsonGetObjectItem(item, "tables");

    int32_t tableSize = tjsonGetArraySize(tables);
    for(int32_t i = 0; i < tableSize; i++){
      SJson* table = tjsonGetArrayItem(tables, i);

      char tableName[MONITOR_TABLENAME_LEN] = {0};
      tjsonGetStringValue(table, "name", tableName);

      SJson* metricGroups = tjsonGetObjectItem(table, "metric_groups");

      int32_t size = tjsonGetArraySize(metricGroups);
      for(int32_t i = 0; i < size; i++){
        SJson* item = tjsonGetArrayItem(metricGroups, i);

        SJson* arrayTag = tjsonGetObjectItem(item, "tags");

        int32_t tagSize = tjsonGetArraySize(arrayTag);

        char** labels = taosMemoryMalloc(sizeof(char*) * tagSize);
        char** sample_labels = taosMemoryMalloc(sizeof(char*) * tagSize);

        for(int32_t j = 0; j < tagSize; j++){
          SJson* item = tjsonGetArrayItem(arrayTag, j);

          *(labels + j) = taosMemoryMalloc(MONITOR_TAG_NAME_LEN); 
          tjsonGetStringValue(item, "name", *(labels + j));

          *(sample_labels + j) = taosMemoryMalloc(MONITOR_TAG_VALUE_LEN);
          tjsonGetStringValue(item, "value", *(sample_labels + j));
          if(strncmp(*(labels + j), "cluster_id", MONITOR_TAG_NAME_LEN) == 0) {
            strncpy(*(sample_labels + j), strClusterId, MONITOR_TAG_VALUE_LEN);
          }
        }

        SJson* metrics = tjsonGetObjectItem(item, "metrics");

        int32_t metricLen = tjsonGetArraySize(metrics);
        for(int32_t j = 0; j < metricLen; j++){
          SJson *item = tjsonGetArrayItem(metrics, j);

          char name[MONITOR_METRIC_NAME_LEN] = {0}; 
          tjsonGetStringValue(item, "name", name);

          double value = 0;
          tjsonGetDoubleValue(item, "value", &value);

          double type = 0;
          tjsonGetDoubleValue(item, "type", &type);

          int32_t metricNameLen = strlen(name) + strlen(tableName) + 2;
          char* metricName = taosMemoryMalloc(metricNameLen); 
          memset(metricName, 0, metricNameLen);
          sprintf(metricName, "%s:%s", tableName, name);

          taos_metric_t* metric = taos_collector_registry_get_metric(metricName);
          if(metric == NULL){
            if(type == 0){
              metric = taos_counter_new(metricName, "",  tagSize, (const char**)labels);
            }
            if(type == 1){
              metric = taos_gauge_new(metricName, "",  tagSize, (const char**)labels);
            }
            mTrace("fail to get metric from registry, new one metric:%p", metric);

            if(taos_collector_registry_register_metric(metric) == 1){
              if(type == 0){
                taos_counter_destroy(metric);
              }
              if(type == 1){
                taos_gauge_destroy(metric);
              }

              metric = taos_collector_registry_get_metric(metricName);

              mTrace("fail to register metric, get metric from registry:%p", metric);
            }
            else{
              mTrace("succeed to register metric:%p", metric);
            }
          }
          else{
            mTrace("get metric from registry:%p", metric);
          }
          
          if(type == 0){
            taos_counter_add(metric, value, (const char**)sample_labels);
          }
          if(type == 1){
            taos_gauge_set(metric, value, (const char**)sample_labels);
          }

          taosMemoryFreeClear(metricName);
        }

        for(int32_t j = 0; j < tagSize; j++){
          taosMemoryFreeClear(*(labels + j));
          taosMemoryFreeClear(*(sample_labels + j));
        }

        taosMemoryFreeClear(sample_labels);
        taosMemoryFreeClear(labels);
      }
    }
    
  }

  code = 0;

_OVER:
  if(pJson != NULL){
    tjsonDelete(pJson);
    pJson = NULL;
  }

  tFreeSStatisReq(&statisReq);
  return code;
  */
}

static int32_t mndProcessStatusReq(SRpcMsg *pReq) {
  SMnode    *pMnode = pReq->info.node;
  SStatusReq statusReq = {0};
  SDnodeObj *pDnode = NULL;
  int32_t    code = -1;

  if (tDeserializeSStatusReq(pReq->pCont, pReq->contLen, &statusReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

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
  bool    needCheck =
      !online || dnodeChanged || reboot || supportVnodesChanged || pMnode->ipWhiteVer != statusReq.ipWhiteVer;

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
      if (pVload->syncState == TAOS_SYNC_STATE_LEADER) {
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
    mndUpdateMnodeState(pObj, &statusReq.mload);
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
      terrno = TSDB_CODE_MND_INVALID_CLUSTER_CFG;
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
    if (pDnode->machineId[0] == 0 && statusReq.machineId[0] != 0) {
      tstrncpy(pDnode->machineId, statusReq.machineId, TSDB_MACHINE_ID_LEN + 1);
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
    tSerializeSStatusRsp(pHead, contLen, &statusRsp);
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
  mndUpdClusterInfo(pReq);
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
  mndUpdClusterInfo(pReq);
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
  snprintf(dnodeObj.ep, TSDB_EP_LEN - 1, "%s:%u", pCreate->fqdn, pCreate->port);

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_GLOBAL, pReq, "create-dnode");
  if (pTrans == NULL) goto _OVER;
  mInfo("trans:%d, used to create dnode:%s", pTrans->id, dnodeObj.ep);
  if (mndTransCheckConflict(pMnode, pTrans) != 0) goto _OVER;

  pRaw = mndDnodeActionEncode(&dnodeObj);
  if (pRaw == NULL || mndTransAppendCommitlog(pTrans, pRaw) != 0) goto _OVER;
  (void)sdbSetRawStatus(pRaw, SDB_STATUS_READY);
  pRaw = NULL;

  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;
  code = 0;

  mndUpdateIpWhiteForAllUser(pMnode, TSDB_DEFAULT_USER, dnodeObj.fqdn, IP_WHITE_ADD, 1);
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
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  while (1) {
    pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SEpSet epSet = {0};
    epSet.numOfEps = 1;
    tstrncpy(epSet.eps[0].fqdn, pObj->fqdn, TSDB_FQDN_LEN);
    epSet.eps[0].port = pObj->port;

    (void)taosArrayPush(rsp.dnodeList, &epSet);

    sdbRelease(pSdb, pObj);
  }

  int32_t rspLen = tSerializeSDnodeListRsp(NULL, 0, &rsp);
  void   *pRsp = rpcMallocCont(rspLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  tSerializeSDnodeListRsp(pRsp, rspLen, &rsp);

  pReq->info.rspLen = rspLen;
  pReq->info.rsp = pRsp;
  code = 0;

_OVER:

  if (code != 0) {
    mError("failed to get dnode list since %s", terrstr());
  }

  tFreeSDnodeListRsp(&rsp);

  return code;
}

static int32_t mndProcessShowVariablesReq(SRpcMsg *pReq) {
  SShowVariablesRsp rsp = {0};
  int32_t           code = -1;

  if (mndCheckOperPrivilege(pReq->info.node, pReq->info.conn.user, MND_OPER_SHOW_VARIBALES) != 0) {
    goto _OVER;
  }

  rsp.variables = taosArrayInit(4, sizeof(SVariablesInfo));
  if (NULL == rsp.variables) {
    mError("failed to alloc SVariablesInfo array while process show variables req");
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  SVariablesInfo info = {0};

  strcpy(info.name, "statusInterval");
  snprintf(info.value, TSDB_CONFIG_VALUE_LEN, "%d", tsStatusInterval);
  strcpy(info.scope, "server");
  taosArrayPush(rsp.variables, &info);

  strcpy(info.name, "timezone");
  snprintf(info.value, TSDB_CONFIG_VALUE_LEN, "%s", tsTimezoneStr);
  strcpy(info.scope, "both");
  taosArrayPush(rsp.variables, &info);

  strcpy(info.name, "locale");
  snprintf(info.value, TSDB_CONFIG_VALUE_LEN, "%s", tsLocale);
  strcpy(info.scope, "both");
  taosArrayPush(rsp.variables, &info);

  strcpy(info.name, "charset");
  snprintf(info.value, TSDB_CONFIG_VALUE_LEN, "%s", tsCharset);
  strcpy(info.scope, "both");
  taosArrayPush(rsp.variables, &info);

  int32_t rspLen = tSerializeSShowVariablesRsp(NULL, 0, &rsp);
  void   *pRsp = rpcMallocCont(rspLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  tSerializeSShowVariablesRsp(pRsp, rspLen, &rsp);

  pReq->info.rspLen = rspLen;
  pReq->info.rsp = pRsp;
  code = 0;

_OVER:

  if (code != 0) {
    mError("failed to get show variables info since %s", terrstr());
  }

  tFreeSShowVariablesRsp(&rsp);

  return code;
}

static int32_t mndProcessCreateDnodeReq(SRpcMsg *pReq) {
  SMnode         *pMnode = pReq->info.node;
  int32_t         code = -1;
  SDnodeObj      *pDnode = NULL;
  SCreateDnodeReq createReq = {0};

  if ((terrno = grantCheck(TSDB_GRANT_DNODE)) != 0 || (terrno = grantCheck(TSDB_GRANT_CPU_CORES)) != 0) {
    code = terrno;
    goto _OVER;
  }

  if (tDeserializeSCreateDnodeReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("dnode:%s:%d, start to create", createReq.fqdn, createReq.port);
  if (mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CREATE_DNODE) != 0) {
    goto _OVER;
  }

  if (createReq.fqdn[0] == 0 || createReq.port <= 0 || createReq.port > UINT16_MAX) {
    terrno = TSDB_CODE_MND_INVALID_DNODE_EP;
    goto _OVER;
  }

  char ep[TSDB_EP_LEN];
  snprintf(ep, TSDB_EP_LEN, "%s:%d", createReq.fqdn, createReq.port);
  pDnode = mndAcquireDnodeByEp(pMnode, ep);
  if (pDnode != NULL) {
    terrno = TSDB_CODE_MND_DNODE_ALREADY_EXIST;
    goto _OVER;
  }

  code = mndCreateDnode(pMnode, pReq, &createReq);
  if (code == 0) {
    code = TSDB_CODE_ACTION_IN_PROGRESS;
    tsGrantHBInterval = 5;
  }

  char obj[200] = {0};
  sprintf(obj, "%s:%d", createReq.fqdn, createReq.port);

  auditRecord(pReq, pMnode->clusterId, "createDnode", "", obj, createReq.sql, createReq.sqlLen);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("dnode:%s:%d, failed to create since %s", createReq.fqdn, createReq.port, terrstr());
  }

  mndReleaseDnode(pMnode, pDnode);
  tFreeSCreateDnodeReq(&createReq);
  return code;
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
  if (pTrans == NULL) goto _OVER;
  mndTransSetSerial(pTrans);
  mInfo("trans:%d, used to drop dnode:%d, force:%d", pTrans->id, pDnode->id, force);
  if (mndTransCheckConflict(pMnode, pTrans) != 0) goto _OVER;

  pRaw = mndDnodeActionEncode(pDnode);
  if (pRaw == NULL) goto _OVER;
  if (mndTransAppendRedolog(pTrans, pRaw) != 0) goto _OVER;
  (void)sdbSetRawStatus(pRaw, SDB_STATUS_DROPPING);
  pRaw = NULL;

  pRaw = mndDnodeActionEncode(pDnode);
  if (pRaw == NULL) goto _OVER;
  if (mndTransAppendCommitlog(pTrans, pRaw) != 0) goto _OVER;
  (void)sdbSetRawStatus(pRaw, SDB_STATUS_DROPPED);
  pRaw = NULL;

  if (pMObj != NULL) {
    mInfo("trans:%d, mnode on dnode:%d will be dropped", pTrans->id, pDnode->id);
    if (mndSetDropMnodeInfoToTrans(pMnode, pTrans, pMObj, force) != 0) goto _OVER;
  }

  if (pQObj != NULL) {
    mInfo("trans:%d, qnode on dnode:%d will be dropped", pTrans->id, pDnode->id);
    if (mndSetDropQnodeInfoToTrans(pMnode, pTrans, pQObj, force) != 0) goto _OVER;
  }

  if (pSObj != NULL) {
    mInfo("trans:%d, snode on dnode:%d will be dropped", pTrans->id, pDnode->id);
    if (mndSetDropSnodeInfoToTrans(pMnode, pTrans, pSObj, force) != 0) goto _OVER;
  }

  if (numOfVnodes > 0) {
    mInfo("trans:%d, %d vnodes on dnode:%d will be dropped", pTrans->id, numOfVnodes, pDnode->id);
    if (mndSetMoveVgroupsInfoToTrans(pMnode, pTrans, pDnode->id, force, unsafe) != 0) goto _OVER;
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;

  mndUpdateIpWhiteForAllUser(pMnode, TSDB_DEFAULT_USER, pDnode->fqdn, IP_WHITE_DROP, 1);
  code = 0;

_OVER:
  mndTransDrop(pTrans);
  sdbFreeRaw(pRaw);
  return code;
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

  if (tDeserializeSDropDnodeReq(pReq->pCont, pReq->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("dnode:%d, start to drop, ep:%s:%d, force:%s, unsafe:%s", dropReq.dnodeId, dropReq.fqdn, dropReq.port,
        dropReq.force ? "true" : "false", dropReq.unsafe ? "true" : "false");
  if (mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_DROP_MNODE) != 0) {
    goto _OVER;
  }

  bool force = dropReq.force;
  if (dropReq.unsafe) {
    force = true;
  }

  pDnode = mndAcquireDnode(pMnode, dropReq.dnodeId);
  if (pDnode == NULL) {
    int32_t err = terrno;
    char    ep[TSDB_EP_LEN + 1] = {0};
    snprintf(ep, sizeof(ep), dropReq.fqdn, dropReq.port);
    pDnode = mndAcquireDnodeByEp(pMnode, ep);
    if (pDnode == NULL) {
      terrno = err;
      goto _OVER;
    }
  }

  pQObj = mndAcquireQnode(pMnode, dropReq.dnodeId);
  pSObj = mndAcquireSnode(pMnode, dropReq.dnodeId);
  pMObj = mndAcquireMnode(pMnode, dropReq.dnodeId);
  if (pMObj != NULL) {
    if (sdbGetSize(pMnode->pSdb, SDB_MNODE) <= 1) {
      terrno = TSDB_CODE_MND_TOO_FEW_MNODES;
      goto _OVER;
    }
    if (pMnode->selfDnodeId == dropReq.dnodeId) {
      terrno = TSDB_CODE_MND_CANT_DROP_LEADER;
      goto _OVER;
    }
  }

  int32_t numOfVnodes = mndGetVnodesNum(pMnode, pDnode->id);
  bool    isonline = mndIsDnodeOnline(pDnode, taosGetTimestampMs());

  if (isonline && force) {
    terrno = TSDB_CODE_DNODE_ONLY_USE_WHEN_OFFLINE;
    mError("dnode:%d, failed to drop since %s, vnodes:%d mnode:%d qnode:%d snode:%d", pDnode->id, terrstr(),
           numOfVnodes, pMObj != NULL, pQObj != NULL, pSObj != NULL);
    goto _OVER;
  }

  bool isEmpty = mndIsEmptyDnode(pMnode, pDnode->id);
  if (!isonline && !force && !isEmpty) {
    terrno = TSDB_CODE_DNODE_OFFLINE;
    mError("dnode:%d, failed to drop since %s, vnodes:%d mnode:%d qnode:%d snode:%d", pDnode->id, terrstr(),
           numOfVnodes, pMObj != NULL, pQObj != NULL, pSObj != NULL);
    goto _OVER;
  }

  code = mndDropDnode(pMnode, pReq, pDnode, pMObj, pQObj, pSObj, numOfVnodes, force, dropReq.unsafe);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  char obj1[30] = {0};
  sprintf(obj1, "%d", dropReq.dnodeId);

  auditRecord(pReq, pMnode->clusterId, "dropDnode", "", obj1, dropReq.sql, dropReq.sqlLen);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("dnode:%d, failed to drop since %s", dropReq.dnodeId, terrstr());
  }

  mndReleaseDnode(pMnode, pDnode);
  mndReleaseMnode(pMnode, pMObj);
  mndReleaseQnode(pMnode, pQObj);
  mndReleaseSnode(pMnode, pSObj);
  tFreeSDropDnodeReq(&dropReq);
  return code;
}

static int32_t mndMCfg2DCfg(SMCfgDnodeReq *pMCfgReq, SDCfgDnodeReq *pDCfgReq) {
  terrno = 0;
  char *p = pMCfgReq->config;
  while (*p) {
    if (*p == ' ') {
      break;
    }
    p++;
  }

  size_t optLen = p - pMCfgReq->config;
  strncpy(pDCfgReq->config, pMCfgReq->config, optLen);
  pDCfgReq->config[optLen] = 0;

  if (' ' == pMCfgReq->config[optLen]) {
    // 'key value'
    if (strlen(pMCfgReq->value) != 0) goto _err;
    strcpy(pDCfgReq->value, p + 1);
  } else {
    // 'key' 'value'
    if (strlen(pMCfgReq->value) == 0) goto _err;
    strcpy(pDCfgReq->value, pMCfgReq->value);
  }

  return 0;

_err:
  mError("dnode:%d, failed to config since invalid conf:%s", pMCfgReq->dnodeId, pMCfgReq->config);
  terrno = TSDB_CODE_INVALID_CFG;
  return -1;
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
        tSerializeSDCfgDnodeReq(pBuf, bufLen, pDcfgReq);
        mInfo("dnode:%d, send config req to dnode, config:%s value:%s", dnodeId, pDcfgReq->config, pDcfgReq->value);
        SRpcMsg rpcMsg = {.msgType = TDMT_DND_CONFIG_DNODE, .pCont = pBuf, .contLen = bufLen};
        tmsgSendReq(&epSet, &rpcMsg);
        code = 0;
      }
    }

    sdbRelease(pSdb, pDnode);
  }

  if (code == -1) {
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
  }
  return code;
}

static int32_t mndProcessConfigDnodeReq(SRpcMsg *pReq) {
  SMnode       *pMnode = pReq->info.node;
  SMCfgDnodeReq cfgReq = {0};
  if (tDeserializeSMCfgDnodeReq(pReq->pCont, pReq->contLen, &cfgReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  mInfo("dnode:%d, start to config, option:%s, value:%s", cfgReq.dnodeId, cfgReq.config, cfgReq.value);
  if (mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CONFIG_DNODE) != 0) {
    tFreeSMCfgDnodeReq(&cfgReq);
    return -1;
  }

  SDCfgDnodeReq dcfgReq = {0};
  if (strcasecmp(cfgReq.config, "resetlog") == 0) {
    strcpy(dcfgReq.config, "resetlog");
#ifdef TD_ENTERPRISE
  } else if (strncasecmp(cfgReq.config, "supportvnodes", 13) == 0) {
    int32_t optLen = strlen("supportvnodes");
    int32_t flag = -1;
    int32_t code = mndMCfgGetValInt32(&cfgReq, optLen, &flag);
    if (code < 0) return code;

    if (flag < 0 || flag > 4096) {
      mError("dnode:%d, failed to config supportVnodes since value:%d. Valid range: [0, 4096]", cfgReq.dnodeId, flag);
      terrno = TSDB_CODE_OUT_OF_RANGE;
      goto _err_out;
    }
    if (flag == 0) {
      flag = tsNumOfCores * 2;
    }
    flag = TMAX(flag, 2);

    strcpy(dcfgReq.config, "supportvnodes");
    snprintf(dcfgReq.value, TSDB_DNODE_VALUE_LEN, "%d", flag);
  } else if (strncasecmp(cfgReq.config, "s3blocksize", 11) == 0) {
    int32_t optLen = strlen("s3blocksize");
    int32_t flag = -1;
    int32_t code = mndMCfgGetValInt32(&cfgReq, optLen, &flag);
    if (code < 0) return code;

    if (flag > 1024 * 1024 || (flag > -1 && flag < 1024) || flag < -1) {
      mError("dnode:%d, failed to config s3blocksize since value:%d. Valid range: -1 or [1024, 1024 * 1024]",
             cfgReq.dnodeId, flag);
      terrno = TSDB_CODE_INVALID_CFG;
      tFreeSMCfgDnodeReq(&cfgReq);
      return -1;
    }

    strcpy(dcfgReq.config, "s3blocksize");
    snprintf(dcfgReq.value, TSDB_DNODE_VALUE_LEN, "%d", flag);
#endif
  } else {
    if (mndMCfg2DCfg(&cfgReq, &dcfgReq)) goto _err_out;
    if (strlen(dcfgReq.config) > TSDB_DNODE_CONFIG_LEN) {
      mError("dnode:%d, failed to config since config is too long", cfgReq.dnodeId);
      terrno = TSDB_CODE_INVALID_CFG;
      goto _err_out;
    }

    if (cfgCheckRangeForDynUpdate(taosGetCfg(), dcfgReq.config, dcfgReq.value, true) != 0) goto _err_out;
  }

  {  // audit
    char obj[50] = {0};
    sprintf(obj, "%d", cfgReq.dnodeId);

    auditRecord(pReq, pMnode->clusterId, "alterDnode", obj, "", cfgReq.sql, cfgReq.sqlLen);
  }

  tFreeSMCfgDnodeReq(&cfgReq);

  return mndSendCfgDnodeReq(pMnode, cfgReq.dnodeId, &dcfgReq);

_err_out:
  tFreeSMCfgDnodeReq(&cfgReq);
  return -1;
}

static int32_t mndProcessConfigDnodeRsp(SRpcMsg *pRsp) {
  mInfo("config rsp from dnode");
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
  snprintf(cfgVals[totalRows], TSDB_CONFIG_VALUE_LEN, "%d", tsStatusInterval);
  totalRows++;

  cfgOpts[totalRows] = "timezone";
  snprintf(cfgVals[totalRows], TSDB_CONFIG_VALUE_LEN, "%s", tsTimezoneStr);
  totalRows++;

  cfgOpts[totalRows] = "locale";
  snprintf(cfgVals[totalRows], TSDB_CONFIG_VALUE_LEN, "%s", tsLocale);
  totalRows++;

  cfgOpts[totalRows] = "charset";
  snprintf(cfgVals[totalRows], TSDB_CONFIG_VALUE_LEN, "%s", tsCharset);
  totalRows++;

  char buf[TSDB_CONFIG_OPTION_LEN + VARSTR_HEADER_SIZE] = {0};
  char bufVal[TSDB_CONFIG_VALUE_LEN + VARSTR_HEADER_SIZE] = {0};

  for (int32_t i = 0; i < totalRows; i++) {
    cols = 0;

    STR_WITH_MAXSIZE_TO_VARSTR(buf, cfgOpts[i], TSDB_CONFIG_OPTION_LEN);
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)buf, false);

    STR_WITH_MAXSIZE_TO_VARSTR(bufVal, cfgVals[i], TSDB_CONFIG_VALUE_LEN);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)bufVal, false);

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
  char       buf[TSDB_CONN_ACTIVE_KEY_LEN + VARSTR_HEADER_SIZE];  // make sure TSDB_CONN_ACTIVE_KEY_LEN >= TSDB_EP_LEN

  while (numOfRows < rows) {
    pShow->pIter = sdbFetchAll(pSdb, SDB_DNODE, pShow->pIter, (void **)&pDnode, &objStatus, true);
    if (pShow->pIter == NULL) break;
    bool online = mndIsDnodeOnline(pDnode, curMs);

    cols = 0;

    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pDnode->id, false);

    STR_WITH_MAXSIZE_TO_VARSTR(buf, pDnode->ep, pShow->pMeta->pSchemas[cols].bytes);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, buf, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    int16_t id = mndGetVnodesNum(pMnode, pDnode->id);
    colDataSetVal(pColInfo, numOfRows, (const char *)&id, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pDnode->numOfSupportVnodes, false);

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
    colDataSetVal(pColInfo, numOfRows, buf, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pDnode->createdTime, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pDnode->rebootTime, false);

    char *b = taosMemoryCalloc(VARSTR_HEADER_SIZE + strlen(offlineReason[pDnode->offlineReason]) + 1, 1);
    STR_TO_VARSTR(b, online ? "" : offlineReason[pDnode->offlineReason]);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, b, false);
    taosMemoryFreeClear(b);

#ifdef TD_ENTERPRISE
    STR_TO_VARSTR(buf, pDnode->machineId);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, buf, false);
#endif

    numOfRows++;
    sdbRelease(pSdb, pDnode);
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextDnode(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}

// get int32_t value from 'SMCfgDnodeReq'
static int32_t mndMCfgGetValInt32(SMCfgDnodeReq *pMCfgReq, int32_t optLen, int32_t *pOutValue) {
  terrno = 0;
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

  return 0;

_err:
  mError("dnode:%d, failed to config since invalid conf:%s", pMCfgReq->dnodeId, pMCfgReq->config);
  terrno = TSDB_CODE_INVALID_CFG;
  return -1;
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
    taosArrayPush(fqdns, &fqdn);
    sdbRelease(pSdb, pObj);
  }
  return fqdns;
}
