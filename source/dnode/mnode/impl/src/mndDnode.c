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
#include "mndAuth.h"
#include "mndMnode.h"
#include "mndQnode.h"
#include "mndShow.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"

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
  memcpy(&dnodeObj.fqdn, tsLocalFqdn, TSDB_FQDN_LEN);
  snprintf(dnodeObj.ep, TSDB_EP_LEN, "%s:%u", dnodeObj.fqdn, dnodeObj.port);

  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_GLOBAL, NULL);
  if (pTrans == NULL) goto _OVER;
  mDebug("trans:%d, used to create dnode:%s on first deploy", pTrans->id, dnodeObj.ep);

  pRaw = mndDnodeActionEncode(&dnodeObj);
  if (pRaw == NULL || mndTransAppendCommitlog(pTrans, pRaw) != 0) goto _OVER;
  sdbSetRawStatus(pRaw, SDB_STATUS_READY);
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
  SSdbRow *pRow = NULL;
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;
  if (sver != TSDB_DNODE_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  pRow = sdbAllocRow(sizeof(SDnodeObj));
  if (pRow == NULL) goto _OVER;
  SDnodeObj *pDnode = sdbGetRowObj(pRow);
  if (pDnode == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &pDnode->id, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pDnode->createdTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pDnode->updateTime, _OVER)
  SDB_GET_INT16(pRaw, dataPos, &pDnode->port, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pDnode->fqdn, TSDB_FQDN_LEN, _OVER)
  SDB_GET_RESERVE(pRaw, dataPos, TSDB_DNODE_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("dnode:%d, failed to decode from raw:%p since %s", pDnode->id, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("dnode:%d, decode from raw:%p, row:%p", pDnode->id, pRaw, pDnode);
  return pRow;
}

static int32_t mndDnodeActionInsert(SSdb *pSdb, SDnodeObj *pDnode) {
  mTrace("dnode:%d, perform insert action, row:%p", pDnode->id, pDnode);
  pDnode->offlineReason = DND_REASON_STATUS_NOT_RECEIVED;
  snprintf(pDnode->ep, TSDB_EP_LEN, "%s:%u", pDnode->fqdn, pDnode->port);
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
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
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

int32_t mndGetDnodeSize(SMnode *pMnode) {
  SSdb *pSdb = pMnode->pSdb;
  return sdbGetSize(pSdb, SDB_DNODE);
}

bool mndIsDnodeOnline(SDnodeObj *pDnode, int64_t curMs) {
  int64_t interval = TABS(pDnode->lastAccessTime - curMs);
  if (interval > 5000 * tsStatusInterval) {
    if (pDnode->rebootTime > 0) {
      pDnode->offlineReason = DND_REASON_STATUS_MSG_TIMEOUT;
    }
    return false;
  }
  return true;
}

static void mndGetDnodeData(SMnode *pMnode, SArray *pDnodeEps) {
  SSdb *pSdb = pMnode->pSdb;

  int32_t numOfEps = 0;
  void   *pIter = NULL;
  while (1) {
    SDnodeObj *pDnode = NULL;
    pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pDnode);
    if (pIter == NULL) break;

    SDnodeEp dnodeEp = {0};
    dnodeEp.id = pDnode->id;
    dnodeEp.isMnode = 0;
    dnodeEp.ep.port = pDnode->port;
    memcpy(dnodeEp.ep.fqdn, pDnode->fqdn, TSDB_FQDN_LEN);

    if (mndIsMnode(pMnode, pDnode->id)) {
      dnodeEp.isMnode = 1;
    }

    sdbRelease(pSdb, pDnode);
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
      mDebug("dnode:%s, not created yet", statusReq.dnodeEp);
      goto _OVER;
    }
  } else {
    pDnode = mndAcquireDnode(pMnode, statusReq.dnodeId);
    if (pDnode == NULL) {
      pDnode = mndAcquireDnodeByEp(pMnode, statusReq.dnodeEp);
      if (pDnode != NULL) {
        pDnode->offlineReason = DND_REASON_DNODE_ID_NOT_MATCH;
      }
      mError("dnode:%d, %s not exist", statusReq.dnodeId, statusReq.dnodeEp);
      goto _OVER;
    }
  }

  for (int32_t v = 0; v < taosArrayGetSize(statusReq.pVloads); ++v) {
    SVnodeLoad *pVload = taosArrayGet(statusReq.pVloads, v);

    SVgObj *pVgroup = mndAcquireVgroup(pMnode, pVload->vgId);
    if (pVgroup != NULL) {
      if (pVload->syncState == TAOS_SYNC_STATE_LEADER) {
        pVgroup->numOfTables = pVload->numOfTables;
        pVgroup->numOfTimeSeries = pVload->numOfTimeSeries;
        pVgroup->totalStorage = pVload->totalStorage;
        pVgroup->compStorage = pVload->compStorage;
        pVgroup->pointsWritten = pVload->pointsWritten;
      }
      bool roleChanged = false;
      for (int32_t vg = 0; vg < pVgroup->replica; ++vg) {
        if (pVgroup->vnodeGid[vg].dnodeId == statusReq.dnodeId) {
          if (pVgroup->vnodeGid[vg].role != pVload->syncState) {
            roleChanged = true;
          }
          pVgroup->vnodeGid[vg].role = pVload->syncState;
          break;
        }
      }
      if (roleChanged) {
        // notify scheduler role has changed
      }
    }

    mndReleaseVgroup(pMnode, pVgroup);
  }

  SMnodeObj *pObj = mndAcquireMnode(pMnode, pDnode->id);
  if (pObj != NULL) {
    if (pObj->state != statusReq.mload.syncState) {
      mInfo("dnode:%d, mnode syncstate from %s to %s", pObj->id, syncStr(pObj->state), syncStr(statusReq.mload.syncState));
      pObj->state = statusReq.mload.syncState;
      pObj->stateStartTime = taosGetTimestampMs();
    }
    mndReleaseMnode(pMnode, pObj);
  }

  SQnodeObj *pQnode = mndAcquireQnode(pMnode, statusReq.qload.dnodeId);
  if (pQnode != NULL) {
    pQnode->load = statusReq.qload;
    mndReleaseQnode(pMnode, pQnode);
  }

  int64_t dnodeVer = sdbGetTableVer(pMnode->pSdb, SDB_DNODE) + sdbGetTableVer(pMnode->pSdb, SDB_MNODE);
  int64_t curMs = taosGetTimestampMs();
  bool    online = mndIsDnodeOnline(pDnode, curMs);
  bool    dnodeChanged = (statusReq.dnodeVer == 0) || (statusReq.dnodeVer != dnodeVer);
  bool    reboot = (pDnode->rebootTime != statusReq.rebootTime);
  bool    needCheck = !online || dnodeChanged || reboot;

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
      mInfo("dnode:%d, %s first access, set clusterId %" PRId64, pDnode->id, pDnode->ep, pMnode->clusterId);
    } else {
      if (statusReq.clusterId != pMnode->clusterId) {
        if (pDnode != NULL) {
          pDnode->offlineReason = DND_REASON_CLUSTER_ID_NOT_MATCH;
        }
        mError("dnode:%d, clusterId %" PRId64 " not match exist %" PRId64, pDnode->id, statusReq.clusterId,
               pMnode->clusterId);
        terrno = TSDB_CODE_MND_INVALID_CLUSTER_ID;
        goto _OVER;
      } else {
        pDnode->accessTimes++;
        mTrace("dnode:%d, status received, access times %d", pDnode->id, pDnode->accessTimes);
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
      mInfo("dnode:%d, from offline to online", pDnode->id);
    } else {
      mDebug("dnode:%d, send dnode epset, online:%d dnodeVer:%" PRId64 ":%" PRId64 " reboot:%d", pDnode->id, online,
             statusReq.dnodeVer, dnodeVer, reboot);
    }

    pDnode->rebootTime = statusReq.rebootTime;
    pDnode->numOfCores = statusReq.numOfCores;
    pDnode->numOfSupportVnodes = statusReq.numOfSupportVnodes;

    SStatusRsp statusRsp = {0};
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
  memcpy(dnodeObj.fqdn, pCreate->fqdn, TSDB_FQDN_LEN);
  snprintf(dnodeObj.ep, TSDB_EP_LEN, "%s:%u", dnodeObj.fqdn, dnodeObj.port);

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_GLOBAL, pReq);
  if (pTrans == NULL) goto _OVER;
  mDebug("trans:%d, used to create dnode:%s", pTrans->id, dnodeObj.ep);

  pRaw = mndDnodeActionEncode(&dnodeObj);
  if (pRaw == NULL || mndTransAppendCommitlog(pTrans, pRaw) != 0) goto _OVER;
  sdbSetRawStatus(pRaw, SDB_STATUS_READY);
  pRaw = NULL;

  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;
  code = 0;

_OVER:
  mndTransDrop(pTrans);
  sdbFreeRaw(pRaw);
  return code;
}

static int32_t mndProcessCreateDnodeReq(SRpcMsg *pReq) {
  SMnode         *pMnode = pReq->info.node;
  int32_t         code = -1;
  SDnodeObj      *pDnode = NULL;
  SCreateDnodeReq createReq = {0};

  if (tDeserializeSCreateDnodeReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mDebug("dnode:%s:%d, start to create", createReq.fqdn, createReq.port);

  if (createReq.fqdn[0] == 0 || createReq.port <= 0 || createReq.port > UINT16_MAX) {
    terrno = TSDB_CODE_MND_INVALID_DNODE_EP;
    goto _OVER;
  }

  char ep[TSDB_EP_LEN];
  snprintf(ep, TSDB_EP_LEN, "%s:%d", createReq.fqdn, createReq.port);
  pDnode = mndAcquireDnodeByEp(pMnode, ep);
  if (pDnode != NULL) {
    goto _OVER;
  }

  if (mndCheckOperAuth(pMnode, pReq->info.conn.user, MND_OPER_CREATE_DNODE) != 0) {
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

static int32_t mndDropDnode(SMnode *pMnode, SRpcMsg *pReq, SDnodeObj *pDnode, SMnodeObj *pMObj, int32_t numOfVnodes) {
  int32_t  code = -1;
  SSdbRaw *pRaw = NULL;
  STrans  *pTrans = NULL;

  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_GLOBAL, pReq);
  if (pTrans == NULL) goto _OVER;
  mndTransSetSerial(pTrans);
  mDebug("trans:%d, used to drop dnode:%d", pTrans->id, pDnode->id);

  pRaw = mndDnodeActionEncode(pDnode);
  if (pRaw == NULL || mndTransAppendRedolog(pTrans, pRaw) != 0) goto _OVER;
  sdbSetRawStatus(pRaw, SDB_STATUS_DROPPING);
  pRaw = NULL;

  pRaw = mndDnodeActionEncode(pDnode);
  if (pRaw == NULL || mndTransAppendCommitlog(pTrans, pRaw) != 0) goto _OVER;
  sdbSetRawStatus(pRaw, SDB_STATUS_DROPPED);
  pRaw = NULL;

  if (pMObj != NULL) {
    mDebug("trans:%d, mnode on dnode:%d will be dropped", pTrans->id, pDnode->id);
    if (mndSetDropMnodeInfoToTrans(pMnode, pTrans, pMObj) != 0) goto _OVER;
  }
  if (numOfVnodes > 0) {
    mDebug("trans:%d, %d vnodes on dnode:%d will be dropped", pTrans->id, numOfVnodes, pDnode->id);
    if (mndSetMoveVgroupsInfoToTrans(pMnode, pTrans, pDnode->id) != 0) goto _OVER;
  }
  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  sdbFreeRaw(pRaw);
  return code;
}

static int32_t mndProcessDropDnodeReq(SRpcMsg *pReq) {
  SMnode        *pMnode = pReq->info.node;
  int32_t        code = -1;
  SDnodeObj     *pDnode = NULL;
  SMnodeObj     *pMObj = NULL;
  SMDropMnodeReq dropReq = {0};

  if (tDeserializeSCreateDropMQSBNodeReq(pReq->pCont, pReq->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mDebug("dnode:%d, start to drop", dropReq.dnodeId);

  if (dropReq.dnodeId <= 0) {
    terrno = TSDB_CODE_MND_INVALID_DNODE_ID;
    goto _OVER;
  }

  pDnode = mndAcquireDnode(pMnode, dropReq.dnodeId);
  if (pDnode == NULL) {
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    goto _OVER;
  }

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
  if (numOfVnodes > 0 || pMObj != NULL) {
    if (!mndIsDnodeOnline(pDnode, taosGetTimestampMs())) {
      terrno = TSDB_CODE_NODE_OFFLINE;
      mError("dnode:%d, failed to drop since %s, has_mnode:%d numOfVnodes:%d", pDnode->id, terrstr(), pMObj != NULL,
             numOfVnodes);
      goto _OVER;
    }
  }

  if (mndCheckOperAuth(pMnode, pReq->info.conn.user, MND_OPER_DROP_MNODE) != 0) {
    goto _OVER;
  }

  code = mndDropDnode(pMnode, pReq, pDnode, pMObj, numOfVnodes);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("dnode:%d, failed to drop since %s", dropReq.dnodeId, terrstr());
  }

  mndReleaseDnode(pMnode, pDnode);
  mndReleaseMnode(pMnode, pMObj);
  return code;
}

static int32_t mndProcessConfigDnodeReq(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;

  SMCfgDnodeReq cfgReq = {0};
  if (tDeserializeSMCfgDnodeReq(pReq->pCont, pReq->contLen, &cfgReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  SDnodeObj *pDnode = mndAcquireDnode(pMnode, cfgReq.dnodeId);
  if (pDnode == NULL) {
    mError("dnode:%d, failed to config since %s ", cfgReq.dnodeId, terrstr());
    return -1;
  }

  SEpSet epSet = mndGetDnodeEpset(pDnode);
  mndReleaseDnode(pMnode, pDnode);

  int32_t bufLen = tSerializeSMCfgDnodeReq(NULL, 0, &cfgReq);
  void   *pBuf = rpcMallocCont(bufLen);

  if (pBuf == NULL) return -1;
  tSerializeSMCfgDnodeReq(pBuf, bufLen, &cfgReq);

  mDebug("dnode:%d, send config req to dnode, app:%p", cfgReq.dnodeId, pReq->info.ahandle);
  SRpcMsg rpcMsg = {.msgType = TDMT_DND_CONFIG_DNODE, .pCont = pBuf, .contLen = bufLen, .info = pReq->info};
  return tmsgSendReq(&epSet, &rpcMsg);
}

static int32_t mndProcessConfigDnodeRsp(SRpcMsg *pRsp) {
  mDebug("config rsp from dnode, app:%p", pRsp->info.ahandle);
  return 0;
}

static int32_t mndRetrieveConfigs(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode *pMnode = pReq->info.node;
  int32_t totalRows = 0;
  int32_t numOfRows = 0;
  char   *cfgOpts[TSDB_CONFIG_NUMBER] = {0};
  char    cfgVals[TSDB_CONFIG_NUMBER][TSDB_CONIIG_VALUE_LEN + 1] = {0};
  char   *pWrite = NULL;
  int32_t cols = 0;

  cfgOpts[totalRows] = "statusInterval";
  snprintf(cfgVals[totalRows], TSDB_CONIIG_VALUE_LEN, "%d", tsStatusInterval);
  totalRows++;

  cfgOpts[totalRows] = "timezone";
  snprintf(cfgVals[totalRows], TSDB_CONIIG_VALUE_LEN, "%s", tsTimezoneStr);
  totalRows++;

  cfgOpts[totalRows] = "locale";
  snprintf(cfgVals[totalRows], TSDB_CONIIG_VALUE_LEN, "%s", tsLocale);
  totalRows++;

  cfgOpts[totalRows] = "charset";
  snprintf(cfgVals[totalRows], TSDB_CONIIG_VALUE_LEN, "%s", tsCharset);
  totalRows++;

  char buf[TSDB_CONFIG_OPTION_LEN + VARSTR_HEADER_SIZE] = {0};
  char bufVal[TSDB_CONIIG_VALUE_LEN + VARSTR_HEADER_SIZE] = {0};

  for (int32_t i = 0; i < totalRows; i++) {
    cols = 0;

    STR_WITH_MAXSIZE_TO_VARSTR(buf, cfgOpts[i], TSDB_CONFIG_OPTION_LEN);
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)buf, false);

    STR_WITH_MAXSIZE_TO_VARSTR(bufVal, cfgVals[i], TSDB_CONIIG_VALUE_LEN);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)bufVal, false);

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
  SDnodeObj *pDnode = NULL;
  int64_t    curMs = taosGetTimestampMs();

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_DNODE, pShow->pIter, (void **)&pDnode);
    if (pShow->pIter == NULL) break;
    bool online = mndIsDnodeOnline(pDnode, curMs);

    cols = 0;

    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)&pDnode->id, false);

    char buf[tListLen(pDnode->ep) + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(buf, pDnode->ep, pShow->pMeta->pSchemas[cols].bytes);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, buf, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    int16_t id = mndGetVnodesNum(pMnode, pDnode->id);
    colDataAppend(pColInfo, numOfRows, (const char *)&id, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)&pDnode->numOfSupportVnodes, false);

    char b1[9] = {0};
    STR_TO_VARSTR(b1, online ? "ready" : "offline");
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, b1, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)&pDnode->createdTime, false);

    char *b = taosMemoryCalloc(VARSTR_HEADER_SIZE + strlen(offlineReason[pDnode->offlineReason]) + 1, 1);
    STR_TO_VARSTR(b, online ? "" : offlineReason[pDnode->offlineReason]);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, b, false);
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
