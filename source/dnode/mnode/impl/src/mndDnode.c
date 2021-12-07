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
#include "mndMnode.h"
#include "mndTrans.h"
#include "ttime.h"

#define SDB_DNODE_VER 1

static char *offlineReason[] = {
    "",
    "status msg timeout",
    "status not received",
    "version not match",
    "dnodeId not match",
    "clusterId not match",
    "mnEqualVn not match",
    "interval not match",
    "timezone not match",
    "locale not match",
    "charset not match",
    "unknown",
};

static SSdbRaw *mndDnodeActionEncode(SDnodeObj *pDnode) {
  SSdbRaw *pRaw = sdbAllocRaw(SDB_DNODE, SDB_DNODE_VER, sizeof(SDnodeObj));
  if (pRaw == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pDnode->id);
  SDB_SET_INT64(pRaw, dataPos, pDnode->createdTime)
  SDB_SET_INT64(pRaw, dataPos, pDnode->updateTime)
  SDB_SET_INT16(pRaw, dataPos, pDnode->port)
  SDB_SET_BINARY(pRaw, dataPos, pDnode->fqdn, TSDB_FQDN_LEN)
  SDB_SET_DATALEN(pRaw, dataPos);

  return pRaw;
}

static SSdbRow *mndDnodeActionDecode(SSdbRaw *pRaw) {
  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) return NULL;

  if (sver != SDB_DNODE_VER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("failed to decode dnode since %s", terrstr());
    return NULL;
  }

  SSdbRow   *pRow = sdbAllocRow(sizeof(SDnodeObj));
  SDnodeObj *pDnode = sdbGetRowObj(pRow);
  if (pDnode == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDnode->id)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pDnode->createdTime)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pDnode->updateTime)
  SDB_GET_INT16(pRaw, pRow, dataPos, &pDnode->port)
  SDB_GET_BINARY(pRaw, pRow, dataPos, pDnode->fqdn, TSDB_FQDN_LEN)

  return pRow;
}

static void mnodeResetDnode(SDnodeObj *pDnode) {
  pDnode->rebootTime = 0;
  pDnode->accessTimes = 0;
  pDnode->numOfCores = 0;
  pDnode->numOfMnodes = 0;
  pDnode->numOfVnodes = 0;
  pDnode->numOfQnodes = 0;
  pDnode->numOfSupportMnodes = 0;
  pDnode->numOfSupportVnodes = 0;
  pDnode->numOfSupportQnodes = 0;
  pDnode->status = DND_STATUS_OFFLINE;
  pDnode->offlineReason = DND_REASON_STATUS_NOT_RECEIVED;
  snprintf(pDnode->ep, TSDB_EP_LEN, "%s:%u", pDnode->fqdn, pDnode->port);
}

static int32_t mndDnodeActionInsert(SSdb *pSdb, SDnodeObj *pDnode) {
  mTrace("dnode:%d, perform insert action", pDnode->id);
  mnodeResetDnode(pDnode);
  return 0;
}

static int32_t mndDnodeActionDelete(SSdb *pSdb, SDnodeObj *pDnode) {
  mTrace("dnode:%d, perform delete action", pDnode->id);
  return 0;
}

static int32_t mndDnodeActionUpdate(SSdb *pSdb, SDnodeObj *pOldDnode, SDnodeObj *pNewDnode) {
  mTrace("dnode:%d, perform update action", pOldDnode->id);
  pOldDnode->id = pNewDnode->id;
  pOldDnode->createdTime = pNewDnode->createdTime;
  pOldDnode->updateTime = pNewDnode->updateTime;
  pOldDnode->port = pNewDnode->port;
  memcpy(pOldDnode->fqdn, pNewDnode->fqdn, TSDB_FQDN_LEN); 
  return 0;
}

static int32_t mndCreateDefaultDnode(SMnode *pMnode) {
  SDnodeObj dnodeObj = {0};
  dnodeObj.id = 1;
  dnodeObj.createdTime = taosGetTimestampMs();
  dnodeObj.updateTime = dnodeObj.createdTime;
  dnodeObj.port = pMnode->replicas[0].port;
  memcpy(&dnodeObj.fqdn, pMnode->replicas[0].fqdn, TSDB_FQDN_LEN);

  SSdbRaw *pRaw = mndDnodeActionEncode(&dnodeObj);
  if (pRaw == NULL) return -1;
  sdbSetRawStatus(pRaw, SDB_STATUS_READY);

  mDebug("dnode:%d, will be created while deploy sdb", dnodeObj.id);
  return sdbWrite(pMnode->pSdb, pRaw);
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
  }

  return NULL;
}

static int32_t mndGetDnodeSize(SMnode *pMnode) {
  SSdb *pSdb = pMnode->pSdb;
  return sdbGetSize(pSdb, SDB_DNODE);
}

static void mndGetDnodeData(SMnode *pMnode, SDnodeEps *pEps, int32_t numOfEps) {
  SSdb *pSdb = pMnode->pSdb;

  int32_t i = 0;
  void   *pIter = NULL;
  while (1) {
    SDnodeObj *pDnode = NULL;
    pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pDnode);
    if (pIter == NULL) break;
    if (i >= numOfEps) {
      sdbCancelFetch(pSdb, pIter);
      break;
    }

    SDnodeEp *pEp = &pEps->eps[i];
    pEp->id = htonl(pDnode->id);
    pEp->port = htons(pDnode->port);
    memcpy(pEp->fqdn, pDnode->fqdn, TSDB_FQDN_LEN);
    pEp->isMnode = 0;
    if (mndIsMnode(pMnode, pDnode->id)) {
      pEp->isMnode = 1;
    }
    i++;
  }

  pEps->num = htonl(i);
}

static int32_t mndCheckClusterCfgPara(SMnode *pMnode, const SClusterCfg *pCfg) {
  if (pCfg->mnodeEqualVnodeNum != pMnode->cfg.mnodeEqualVnodeNum) {
    mError("\"mnodeEqualVnodeNum\"[%d - %d] cfg inconsistent", pCfg->mnodeEqualVnodeNum,
           pMnode->cfg.mnodeEqualVnodeNum);
    return DND_REASON_MN_EQUAL_VN_NOT_MATCH;
  }

  if (pCfg->statusInterval != pMnode->cfg.statusInterval) {
    mError("\"statusInterval\"[%d - %d] cfg inconsistent", pCfg->statusInterval, pMnode->cfg.statusInterval);
    return DND_REASON_STATUS_INTERVAL_NOT_MATCH;
  }

  int64_t checkTime = 0;
  char    timestr[32] = "1970-01-01 00:00:00.00";
  (void)taosParseTime(timestr, &checkTime, (int32_t)strlen(timestr), TSDB_TIME_PRECISION_MILLI, 0);
  if ((0 != strcasecmp(pCfg->timezone, pMnode->cfg.timezone)) && (checkTime != pCfg->checkTime)) {
    mError("\"timezone\"[%s - %s] [%" PRId64 " - %" PRId64 "] cfg inconsistent", pCfg->timezone, pMnode->cfg.timezone,
           pCfg->checkTime, checkTime);
    return DND_REASON_TIME_ZONE_NOT_MATCH;
  }

  if (0 != strcasecmp(pCfg->locale, pMnode->cfg.locale)) {
    mError("\"locale\"[%s - %s]  cfg parameters inconsistent", pCfg->locale, pMnode->cfg.locale);
    return DND_REASON_LOCALE_NOT_MATCH;
  }

  if (0 != strcasecmp(pCfg->charset, pMnode->cfg.charset)) {
    mError("\"charset\"[%s - %s] cfg parameters inconsistent.", pCfg->charset, pMnode->cfg.charset);
    return DND_REASON_CHARSET_NOT_MATCH;
  }

  return 0;
}

static void mndParseStatusMsg(SStatusMsg *pStatus) {
  pStatus->sver = htonl(pStatus->sver);
  pStatus->dnodeId = htonl(pStatus->dnodeId);
  pStatus->clusterId = htonl(pStatus->clusterId);
  pStatus->rebootTime = htonl(pStatus->rebootTime);
  pStatus->numOfCores = htons(pStatus->numOfCores);
  pStatus->numOfSupportMnodes = htons(pStatus->numOfSupportMnodes);
  pStatus->numOfSupportVnodes = htons(pStatus->numOfSupportVnodes);
  pStatus->numOfSupportQnodes = htons(pStatus->numOfSupportQnodes);

  pStatus->clusterCfg.statusInterval = htonl(pStatus->clusterCfg.statusInterval);
  pStatus->clusterCfg.mnodeEqualVnodeNum = htonl(pStatus->clusterCfg.mnodeEqualVnodeNum);
  pStatus->clusterCfg.checkTime = htobe64(pStatus->clusterCfg.checkTime);
}

static int32_t mndProcessStatusMsg(SMnodeMsg *pMsg) {
  SMnode     *pMnode = pMsg->pMnode;
  SStatusMsg *pStatus = pMsg->rpcMsg.pCont;
  mndParseStatusMsg(pStatus);

  SDnodeObj *pDnode = NULL;
  if (pStatus->dnodeId == 0) {
    pDnode = mndAcquireDnodeByEp(pMnode, pStatus->dnodeEp);
    if (pDnode == NULL) {
      mDebug("dnode:%s, not created yet", pStatus->dnodeEp);
      return TSDB_CODE_MND_DNODE_NOT_EXIST;
    }
  } else {
    pDnode = mndAcquireDnode(pMnode, pStatus->dnodeId);
    if (pDnode == NULL) {
      pDnode = mndAcquireDnodeByEp(pMnode, pStatus->dnodeEp);
      if (pDnode != NULL && pDnode->status != DND_STATUS_READY) {
        pDnode->offlineReason = DND_REASON_DNODE_ID_NOT_MATCH;
      }
      mError("dnode:%d, %s not exist", pStatus->dnodeId, pStatus->dnodeEp);
      mndReleaseDnode(pMnode, pDnode);
      return TSDB_CODE_MND_DNODE_NOT_EXIST;
    }
  }

  if (pStatus->sver != pMnode->cfg.sver) {
    if (pDnode != NULL && pDnode->status != DND_STATUS_READY) {
      pDnode->offlineReason = DND_REASON_VERSION_NOT_MATCH;
    }
    mndReleaseDnode(pMnode, pDnode);
    mError("dnode:%d, status msg version:%d not match cluster:%d", pStatus->dnodeId, pStatus->sver, pMnode->cfg.sver);
    return TSDB_CODE_MND_INVALID_MSG_VERSION;
  }

  if (pStatus->dnodeId == 0) {
    mDebug("dnode:%d %s, first access, set clusterId %d", pDnode->id, pDnode->ep, pMnode->clusterId);
  } else {
    if (pStatus->clusterId != pMnode->clusterId) {
      if (pDnode != NULL && pDnode->status != DND_STATUS_READY) {
        pDnode->offlineReason = DND_REASON_CLUSTER_ID_NOT_MATCH;
      }
      mError("dnode:%d, clusterId %d not match exist %d", pDnode->id, pStatus->clusterId, pMnode->clusterId);
      mndReleaseDnode(pMnode, pDnode);
      return TSDB_CODE_MND_INVALID_CLUSTER_ID;
    } else {
      pDnode->accessTimes++;
      mTrace("dnode:%d, status received, access times %d", pDnode->id, pDnode->accessTimes);
    }
  }

  if (pDnode->status == DND_STATUS_OFFLINE) {
    // Verify whether the cluster parameters are consistent when status change from offline to ready
    int32_t ret = mndCheckClusterCfgPara(pMnode, &pStatus->clusterCfg);
    if (0 != ret) {
      pDnode->offlineReason = ret;
      mError("dnode:%d, cluster cfg inconsistent since:%s", pDnode->id, offlineReason[ret]);
      mndReleaseDnode(pMnode, pDnode);
      return TSDB_CODE_MND_CLUSTER_CFG_INCONSISTENT;
    }

    mInfo("dnode:%d, from offline to online", pDnode->id);
  }

  pDnode->rebootTime = pStatus->rebootTime;
  pDnode->numOfCores = pStatus->numOfCores;
  pDnode->numOfSupportMnodes = pStatus->numOfSupportMnodes;
  pDnode->numOfSupportVnodes = pStatus->numOfSupportVnodes;
  pDnode->numOfSupportQnodes = pStatus->numOfSupportQnodes;
  pDnode->status = DND_STATUS_READY;

  int32_t     numOfEps = mndGetDnodeSize(pMnode);
  int32_t     contLen = sizeof(SStatusRsp) + numOfEps * sizeof(SDnodeEp);
  SStatusRsp *pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    mndReleaseDnode(pMnode, pDnode);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pRsp->dnodeCfg.dnodeId = htonl(pDnode->id);
  pRsp->dnodeCfg.dropped = 0;
  pRsp->dnodeCfg.clusterId = htonl(pMnode->clusterId);
  mndGetDnodeData(pMnode, &pRsp->dnodeEps, numOfEps);

  pMsg->contLen = contLen;
  pMsg->pCont = pRsp;
  mndReleaseDnode(pMnode, pDnode);

  return 0;
}

static int32_t mndProcessCreateDnodeMsg(SMnodeMsg *pMsg) { return 0; }

static int32_t mndProcessDropDnodeMsg(SMnodeMsg *pMsg) { return 0; }

static int32_t mndProcessConfigDnodeMsg(SMnodeMsg *pMsg) { return 0; }

int32_t mndInitDnode(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_DNODE,
                     .keyType = SDB_KEY_INT32,
                     .deployFp = (SdbDeployFp)mndCreateDefaultDnode,
                     .encodeFp = (SdbEncodeFp)mndDnodeActionEncode,
                     .decodeFp = (SdbDecodeFp)mndDnodeActionDecode,
                     .insertFp = (SdbInsertFp)mndDnodeActionInsert,
                     .updateFp = (SdbUpdateFp)mndDnodeActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndDnodeActionDelete};

  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_CREATE_DNODE, mndProcessCreateDnodeMsg);
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_DROP_DNODE, mndProcessDropDnodeMsg);
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_CONFIG_DNODE, mndProcessConfigDnodeMsg);
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_STATUS, mndProcessStatusMsg);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupDnode(SMnode *pMnode) {}

SDnodeObj *mndAcquireDnode(SMnode *pMnode, int32_t dnodeId) {
  SSdb *pSdb = pMnode->pSdb;
  return sdbAcquire(pSdb, SDB_DNODE, &dnodeId);
}

void mndReleaseDnode(SMnode *pMnode, SDnodeObj *pDnode) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pDnode);
}
