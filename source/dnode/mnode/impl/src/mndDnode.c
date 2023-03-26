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

#define TSDB_DNODE_VER_NUMBER   1
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
    "unknown",
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

static int32_t mndRetrieveConfigs(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextConfig(SMnode *pMnode, void *pIter);
static int32_t mndRetrieveDnodes(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextDnode(SMnode *pMnode, void *pIter);

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
  mndSetMsgHandle(pMnode, TDMT_MND_DNODE_LIST, mndProcessDnodeListReq);
  mndSetMsgHandle(pMnode, TDMT_MND_SHOW_VARIABLES, mndProcessShowVariablesReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_CONFIGS, mndRetrieveConfigs);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_CONFIGS, mndCancelGetNextConfig);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_DNODE, mndRetrieveDnodes);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_DNODE, mndCancelGetNextDnode);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupDnode(SMnode *pMnode) {}

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

  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_GLOBAL, NULL, "create-dnode");
  if (pTrans == NULL) goto _OVER;
  mInfo("trans:%d, used to create dnode:%s on first deploy", pTrans->id, dnodeObj.ep);

  pRaw = mndDnodeActionEncode(&dnodeObj);
  if (pRaw == NULL || mndTransAppendCommitlog(pTrans, pRaw) != 0) goto _OVER;
  (void)sdbSetRawStatus(pRaw, SDB_STATUS_READY);
  pRaw = NULL;

  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;
  code = 0;

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
  if (sver != TSDB_DNODE_VER_NUMBER) {
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

void mndGetDnodeData(SMnode *pMnode, SArray *pDnodeEps) {
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

  return 0;
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

  int64_t dnodeVer = sdbGetTableVer(pMnode->pSdb, SDB_DNODE) + sdbGetTableVer(pMnode->pSdb, SDB_MNODE);
  int64_t curMs = taosGetTimestampMs();
  bool    online = mndIsDnodeOnline(pDnode, curMs);
  bool    dnodeChanged = (statusReq.dnodeVer == 0) || (statusReq.dnodeVer != dnodeVer);
  bool    reboot = (pDnode->rebootTime != statusReq.rebootTime);
  bool    needCheck = !online || dnodeChanged || reboot;

  const STraceId *trace = &pReq->info.traceId;
  mGTrace("dnode:%d, status received, accessTimes:%d check:%d online:%d reboot:%d changed:%d statusSeq:%d", pDnode->id,
          pDnode->accessTimes, needCheck, online, reboot, dnodeChanged, statusReq.statusSeq);

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
      bool roleChanged = false;
      for (int32_t vg = 0; vg < pVgroup->replica; ++vg) {
        SVnodeGid *pGid = &pVgroup->vnodeGid[vg];
        if (pGid->dnodeId == statusReq.dnodeId) {
          if (pGid->syncState != pVload->syncState || pGid->syncRestore != pVload->syncRestore ||
              pGid->syncCanRead != pVload->syncCanRead) {
            mInfo(
                "vgId:%d, state changed by status msg, old state:%s restored:%d canRead:%d new state:%s restored:%d "
                "canRead:%d, dnode:%d",
                pVgroup->vgId, syncStr(pGid->syncState), pGid->syncRestore, pGid->syncCanRead,
                syncStr(pVload->syncState), pVload->syncRestore, pVload->syncCanRead, pDnode->id);
            pGid->syncState = pVload->syncState;
            pGid->syncRestore = pVload->syncRestore;
            pGid->syncCanRead = pVload->syncCanRead;
            roleChanged = true;
          }
          break;
        }
      }
      if (roleChanged) {
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
    if (pObj->syncState != statusReq.mload.syncState || pObj->syncRestore != statusReq.mload.syncRestore) {
      mInfo("dnode:%d, mnode syncState from %s to %s, restoreState from %d to %d", pObj->id, syncStr(pObj->syncState),
            syncStr(statusReq.mload.syncState), pObj->syncRestore, statusReq.mload.syncRestore);
      pObj->syncState = statusReq.mload.syncState;
      pObj->syncRestore = statusReq.mload.syncRestore;
      pObj->stateStartTime = taosGetTimestampMs();
    }
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
    pDnode->memAvail = statusReq.memAvail;
    pDnode->memTotal = statusReq.memTotal;

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

    mndGetDnodeData(pMnode, statusRsp.pDnodeEps);

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
  if (mndTrancCheckConflict(pMnode, pTrans) != 0) goto _OVER;

  pRaw = mndDnodeActionEncode(&dnodeObj);
  if (pRaw == NULL || mndTransAppendCommitlog(pTrans, pRaw) != 0) goto _OVER;
  (void)sdbSetRawStatus(pRaw, SDB_STATUS_READY);
  pRaw = NULL;

  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;
  code = 0;

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
  taosArrayPush(rsp.variables, &info);

  strcpy(info.name, "timezone");
  snprintf(info.value, TSDB_CONFIG_VALUE_LEN, "%s", tsTimezoneStr);
  taosArrayPush(rsp.variables, &info);

  strcpy(info.name, "locale");
  snprintf(info.value, TSDB_CONFIG_VALUE_LEN, "%s", tsLocale);
  taosArrayPush(rsp.variables, &info);

  strcpy(info.name, "charset");
  snprintf(info.value, TSDB_CONFIG_VALUE_LEN, "%s", tsCharset);
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

  if ((terrno = grantCheck(TSDB_GRANT_DNODE)) != 0) {
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
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("dnode:%s:%d, failed to create since %s", createReq.fqdn, createReq.port, terrstr());
  }

  mndReleaseDnode(pMnode, pDnode);
  return code;
}

static int32_t mndDropDnode(SMnode *pMnode, SRpcMsg *pReq, SDnodeObj *pDnode, SMnodeObj *pMObj, SQnodeObj *pQObj,
                            SSnodeObj *pSObj, int32_t numOfVnodes, bool force) {
  int32_t  code = -1;
  SSdbRaw *pRaw = NULL;
  STrans  *pTrans = NULL;

  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_GLOBAL, pReq, "drop-dnode");
  if (pTrans == NULL) goto _OVER;
  mndTransSetSerial(pTrans);
  mInfo("trans:%d, used to drop dnode:%d, force:%d", pTrans->id, pDnode->id, force);
  if (mndTrancCheckConflict(pMnode, pTrans) != 0) goto _OVER;

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
    if (mndSetMoveVgroupsInfoToTrans(pMnode, pTrans, pDnode->id, force) != 0) goto _OVER;
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  sdbFreeRaw(pRaw);
  return code;
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

  mInfo("dnode:%d, start to drop, ep:%s:%d", dropReq.dnodeId, dropReq.fqdn, dropReq.port);
  if (mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_DROP_MNODE) != 0) {
    goto _OVER;
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
  if ((numOfVnodes > 0 || pMObj != NULL || pSObj != NULL || pQObj != NULL) && !dropReq.force) {
    if (!mndIsDnodeOnline(pDnode, taosGetTimestampMs())) {
      terrno = TSDB_CODE_DNODE_OFFLINE;
      mError("dnode:%d, failed to drop since %s, vnodes:%d mnode:%d qnode:%d snode:%d", pDnode->id, terrstr(),
             numOfVnodes, pMObj != NULL, pQObj != NULL, pSObj != NULL);
      goto _OVER;
    }
  }

  code = mndDropDnode(pMnode, pReq, pDnode, pMObj, pQObj, pSObj, numOfVnodes, dropReq.force);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("dnode:%d, failed to drop since %s", dropReq.dnodeId, terrstr());
  }

  mndReleaseDnode(pMnode, pDnode);
  mndReleaseMnode(pMnode, pMObj);
  mndReleaseQnode(pMnode, pQObj);
  mndReleaseSnode(pMnode, pSObj);
  return code;
}

static int32_t mndProcessConfigDnodeReq(SRpcMsg *pReq) {
  SMnode     *pMnode = pReq->info.node;
  const char *options[] = {
      "debugFlag",   "dDebugFlag",   "vDebugFlag",   "mDebugFlag",   "wDebugFlag",    "sDebugFlag",   "tsdbDebugFlag",
      "tqDebugFlag", "fsDebugFlag",  "udfDebugFlag", "smaDebugFlag", "idxDebugFlag",  "tdbDebugFlag", "tmrDebugFlag",
      "uDebugFlag",  "smaDebugFlag", "rpcDebugFlag", "qDebugFlag",   "metaDebugFlag",
  };
  int32_t optionSize = tListLen(options);

  SMCfgDnodeReq cfgReq = {0};
  if (tDeserializeSMCfgDnodeReq(pReq->pCont, pReq->contLen, &cfgReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  mInfo("dnode:%d, start to config, option:%s, value:%s", cfgReq.dnodeId, cfgReq.config, cfgReq.value);
  if (mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CONFIG_DNODE) != 0) {
    return -1;
  }

  SDCfgDnodeReq dcfgReq = {0};
  if (strcasecmp(cfgReq.config, "resetlog") == 0) {
    strcpy(dcfgReq.config, "resetlog");
  } else if (strncasecmp(cfgReq.config, "monitor", 7) == 0) {
    if (' ' != cfgReq.config[7] && 0 != cfgReq.config[7]) {
      mError("dnode:%d, failed to config monitor since invalid conf:%s", cfgReq.dnodeId, cfgReq.config);
      terrno = TSDB_CODE_INVALID_CFG;
      return -1;
    }

    const char *value = cfgReq.value;
    int32_t     flag = atoi(value);
    if (flag <= 0) {
      flag = atoi(cfgReq.config + 8);
    }
    if (flag < 0 || flag > 2) {
      mError("dnode:%d, failed to config monitor since value:%d", cfgReq.dnodeId, flag);
      terrno = TSDB_CODE_INVALID_CFG;
      return -1;
    }

    strcpy(dcfgReq.config, "monitor");
    snprintf(dcfgReq.value, TSDB_DNODE_VALUE_LEN, "%d", flag);
  } else {
    bool findOpt = false;
    for (int32_t d = 0; d < optionSize; ++d) {
      const char *optName = options[d];
      int32_t     optLen = strlen(optName);
      if (strncasecmp(cfgReq.config, optName, optLen) != 0) continue;

      if (' ' != cfgReq.config[optLen] && 0 != cfgReq.config[optLen]) {
        mError("dnode:%d, failed to config since invalid conf:%s", cfgReq.dnodeId, cfgReq.config);
        terrno = TSDB_CODE_INVALID_CFG;
        return -1;
      }

      const char *value = cfgReq.value;
      int32_t     flag = atoi(value);
      if (flag <= 0) {
        flag = atoi(cfgReq.config + optLen + 1);
      }
      if (flag < 0 || flag > 255) {
        mError("dnode:%d, failed to config %s since value:%d", cfgReq.dnodeId, optName, flag);
        terrno = TSDB_CODE_INVALID_CFG;
        return -1;
      }

      tstrncpy(dcfgReq.config, optName, optLen + 1);
      snprintf(dcfgReq.value, TSDB_DNODE_VALUE_LEN, "%d", flag);
      findOpt = true;
    }

    if (!findOpt) {
      terrno = TSDB_CODE_INVALID_CFG;
      mError("dnode:%d, failed to config since %s", cfgReq.dnodeId, terrstr());
      return -1;
    }
  }

  int32_t code = -1;
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
  while (1) {
    SDnodeObj *pDnode = NULL;
    pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pDnode);
    if (pIter == NULL) break;

    if (pDnode->id == cfgReq.dnodeId || cfgReq.dnodeId == -1 || cfgReq.dnodeId == 0) {
      SEpSet  epSet = mndGetDnodeEpset(pDnode);
      int32_t bufLen = tSerializeSDCfgDnodeReq(NULL, 0, &dcfgReq);
      void   *pBuf = rpcMallocCont(bufLen);

      if (pBuf != NULL) {
        tSerializeSDCfgDnodeReq(pBuf, bufLen, &dcfgReq);
        mInfo("dnode:%d, send config req to dnode, app:%p config:%s value:%s", cfgReq.dnodeId, pReq->info.ahandle,
              dcfgReq.config, dcfgReq.value);
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

  while (numOfRows < rows) {
    pShow->pIter = sdbFetchAll(pSdb, SDB_DNODE, pShow->pIter, (void **)&pDnode, &objStatus, true);
    if (pShow->pIter == NULL) break;
    bool online = mndIsDnodeOnline(pDnode, curMs);

    cols = 0;

    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pDnode->id, false);

    char buf[tListLen(pDnode->ep) + VARSTR_HEADER_SIZE] = {0};
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

    char b1[16] = {0};
    STR_TO_VARSTR(b1, status);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, b1, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pDnode->createdTime, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pDnode->rebootTime, false);

    char *b = taosMemoryCalloc(VARSTR_HEADER_SIZE + strlen(offlineReason[pDnode->offlineReason]) + 1, 1);
    STR_TO_VARSTR(b, online ? "" : offlineReason[pDnode->offlineReason]);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, b, false);
    taosMemoryFreeClear(b);

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
