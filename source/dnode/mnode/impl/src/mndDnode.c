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
#include "mndShow.h"
#include "mndTrans.h"
#include "mndVgroup.h"

#define TSDB_DNODE_VER_NUMBER 1
#define TSDB_DNODE_RESERVE_SIZE 64
#define TSDB_CONFIG_OPTION_LEN 16
#define TSDB_CONIIG_VALUE_LEN 48
#define TSDB_CONFIG_NUMBER 8

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

static int32_t mndProcessCreateDnodeReq(SMnodeMsg *pReq);
static int32_t mndProcessDropDnodeReq(SMnodeMsg *pReq);
static int32_t mndProcessConfigDnodeReq(SMnodeMsg *pReq);
static int32_t mndProcessConfigDnodeRsp(SMnodeMsg *pRsp);
static int32_t mndProcessStatusReq(SMnodeMsg *pReq);

static int32_t mndGetConfigMeta(SMnodeMsg *pReq, SShowObj *pShow, STableMetaRsp *pMeta);
static int32_t mndRetrieveConfigs(SMnodeMsg *pReq, SShowObj *pShow, char *data, int32_t rows);
static void    mndCancelGetNextConfig(SMnode *pMnode, void *pIter);
static int32_t mndGetDnodeMeta(SMnodeMsg *pReq, SShowObj *pShow, STableMetaRsp *pMeta);
static int32_t mndRetrieveDnodes(SMnodeMsg *pReq, SShowObj *pShow, char *data, int32_t rows);
static void    mndCancelGetNextDnode(SMnode *pMnode, void *pIter);

int32_t mndInitDnode(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_DNODE,
                     .keyType = SDB_KEY_INT32,
                     .deployFp = (SdbDeployFp)mndCreateDefaultDnode,
                     .encodeFp = (SdbEncodeFp)mndDnodeActionEncode,
                     .decodeFp = (SdbDecodeFp)mndDnodeActionDecode,
                     .insertFp = (SdbInsertFp)mndDnodeActionInsert,
                     .updateFp = (SdbUpdateFp)mndDnodeActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndDnodeActionDelete};

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_DNODE, mndProcessCreateDnodeReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_DNODE, mndProcessDropDnodeReq);
  mndSetMsgHandle(pMnode, TDMT_MND_CONFIG_DNODE, mndProcessConfigDnodeReq);
  mndSetMsgHandle(pMnode, TDMT_DND_CONFIG_DNODE_RSP, mndProcessConfigDnodeRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_STATUS, mndProcessStatusReq);

  mndAddShowMetaHandle(pMnode, TSDB_MGMT_TABLE_VARIABLES, mndGetConfigMeta);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_VARIABLES, mndRetrieveConfigs);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_VARIABLES, mndCancelGetNextConfig);
  mndAddShowMetaHandle(pMnode, TSDB_MGMT_TABLE_DNODE, mndGetDnodeMeta);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_DNODE, mndRetrieveDnodes);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_DNODE, mndCancelGetNextDnode);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupDnode(SMnode *pMnode) {}

static int32_t mndCreateDefaultDnode(SMnode *pMnode) {
  SDnodeObj dnodeObj = {0};
  dnodeObj.id = 1;
  dnodeObj.createdTime = taosGetTimestampMs();
  dnodeObj.updateTime = dnodeObj.createdTime;
  dnodeObj.port = pMnode->replicas[0].port;
  memcpy(&dnodeObj.fqdn, pMnode->replicas[0].fqdn, TSDB_FQDN_LEN);

  SSdbRaw *pRaw = mndDnodeActionEncode(&dnodeObj);
  if (pRaw == NULL) return -1;
  if (sdbSetRawStatus(pRaw, SDB_STATUS_READY) != 0) return -1;

  mDebug("dnode:%d, will be created while deploy sdb, raw:%p", dnodeObj.id, pRaw);
  return sdbWrite(pMnode->pSdb, pRaw);
}

static SSdbRaw *mndDnodeActionEncode(SDnodeObj *pDnode) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_DNODE, TSDB_DNODE_VER_NUMBER, sizeof(SDnodeObj) + TSDB_DNODE_RESERVE_SIZE);
  if (pRaw == NULL) goto DNODE_ENCODE_OVER;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pDnode->id, DNODE_ENCODE_OVER)
  SDB_SET_INT64(pRaw, dataPos, pDnode->createdTime, DNODE_ENCODE_OVER)
  SDB_SET_INT64(pRaw, dataPos, pDnode->updateTime, DNODE_ENCODE_OVER)
  SDB_SET_INT16(pRaw, dataPos, pDnode->port, DNODE_ENCODE_OVER)
  SDB_SET_BINARY(pRaw, dataPos, pDnode->fqdn, TSDB_FQDN_LEN, DNODE_ENCODE_OVER)
  SDB_SET_RESERVE(pRaw, dataPos, TSDB_DNODE_RESERVE_SIZE, DNODE_ENCODE_OVER)
  SDB_SET_DATALEN(pRaw, dataPos, DNODE_ENCODE_OVER);

  terrno = 0;

DNODE_ENCODE_OVER:
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

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto DNODE_DECODE_OVER;

  if (sver != TSDB_DNODE_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto DNODE_DECODE_OVER;
  }

  SSdbRow *pRow = sdbAllocRow(sizeof(SDnodeObj));
  if (pRow == NULL) goto DNODE_DECODE_OVER;

  SDnodeObj *pDnode = sdbGetRowObj(pRow);
  if (pDnode == NULL) goto DNODE_DECODE_OVER;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &pDnode->id, DNODE_DECODE_OVER)
  SDB_GET_INT64(pRaw, dataPos, &pDnode->createdTime, DNODE_DECODE_OVER)
  SDB_GET_INT64(pRaw, dataPos, &pDnode->updateTime, DNODE_DECODE_OVER)
  SDB_GET_INT16(pRaw, dataPos, &pDnode->port, DNODE_DECODE_OVER)
  SDB_GET_BINARY(pRaw, dataPos, pDnode->fqdn, TSDB_FQDN_LEN, DNODE_DECODE_OVER)
  SDB_GET_RESERVE(pRaw, dataPos, TSDB_DNODE_RESERVE_SIZE, DNODE_DECODE_OVER)

  terrno = 0;

DNODE_DECODE_OVER:
  if (terrno != 0) {
    mError("dnode:%d, failed to decode from raw:%p since %s", pDnode->id, pRaw, terrstr());
    tfree(pRow);
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

  return NULL;
}

int32_t mndGetDnodeSize(SMnode *pMnode) {
  SSdb *pSdb = pMnode->pSdb;
  return sdbGetSize(pSdb, SDB_DNODE);
}

bool mndIsDnodeOnline(SMnode *pMnode, SDnodeObj *pDnode, int64_t curMs) {
  int64_t interval = TABS(pDnode->lastAccessTime - curMs);
  if (interval > 3500 * pMnode->cfg.statusInterval) {
    if (pDnode->rebootTime > 0) {
      pDnode->offlineReason = DND_REASON_STATUS_MSG_TIMEOUT;
    }
    return false;
  }
  return true;
}

static void mndGetDnodeData(SMnode *pMnode, SDnodeEps *pEps, int32_t maxEps) {
  SSdb *pSdb = pMnode->pSdb;

  int32_t numOfEps = 0;
  void   *pIter = NULL;
  while (1) {
    SDnodeObj *pDnode = NULL;
    pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pDnode);
    if (pIter == NULL) break;
    if (numOfEps >= maxEps) {
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pDnode);
      break;
    }

    SDnodeEp *pEp = &pEps->eps[numOfEps];
    pEp->id = htonl(pDnode->id);
    pEp->ep.port = htons(pDnode->port);
    memcpy(pEp->ep.fqdn, pDnode->fqdn, TSDB_FQDN_LEN);
    pEp->isMnode = 0;
    if (mndIsMnode(pMnode, pDnode->id)) {
      pEp->isMnode = 1;
    }
    numOfEps++;
    sdbRelease(pSdb, pDnode);
  }

  pEps->num = htonl(numOfEps);
}

static int32_t mndCheckClusterCfgPara(SMnode *pMnode, const SClusterCfg *pCfg) {
  if (pCfg->statusInterval != pMnode->cfg.statusInterval) {
    mError("statusInterval [%d - %d] cfg inconsistent", pCfg->statusInterval, pMnode->cfg.statusInterval);
    return DND_REASON_STATUS_INTERVAL_NOT_MATCH;
  }

  if ((0 != strcasecmp(pCfg->timezone, pMnode->cfg.timezone)) && (pMnode->checkTime != pCfg->checkTime)) {
    mError("timezone [%s - %s] [%" PRId64 " - %" PRId64 "] cfg inconsistent", pCfg->timezone, pMnode->cfg.timezone,
           pCfg->checkTime, pMnode->checkTime);
    return DND_REASON_TIME_ZONE_NOT_MATCH;
  }

  if (0 != strcasecmp(pCfg->locale, pMnode->cfg.locale)) {
    mError("locale [%s - %s]  cfg inconsistent", pCfg->locale, pMnode->cfg.locale);
    return DND_REASON_LOCALE_NOT_MATCH;
  }

  if (0 != strcasecmp(pCfg->charset, pMnode->cfg.charset)) {
    mError("charset [%s - %s] cfg inconsistent.", pCfg->charset, pMnode->cfg.charset);
    return DND_REASON_CHARSET_NOT_MATCH;
  }

  return 0;
}

static void mndParseStatusMsg(SStatusReq *pStatus) {
  pStatus->sver = htonl(pStatus->sver);
  pStatus->dver = htobe64(pStatus->dver);
  pStatus->dnodeId = htonl(pStatus->dnodeId);
  pStatus->clusterId = htobe64(pStatus->clusterId);
  pStatus->rebootTime = htobe64(pStatus->rebootTime);
  pStatus->updateTime = htobe64(pStatus->updateTime);
  pStatus->numOfCores = htonl(pStatus->numOfCores);
  pStatus->numOfSupportVnodes = htonl(pStatus->numOfSupportVnodes);
  pStatus->clusterCfg.statusInterval = htonl(pStatus->clusterCfg.statusInterval);
  pStatus->clusterCfg.checkTime = htobe64(pStatus->clusterCfg.checkTime);
  for (int32_t v = 0; v < pStatus->vnodeLoads.num; ++v) {
    SVnodeLoad *pVload = &pStatus->vnodeLoads.data[v];
    pVload->vgId = htonl(pVload->vgId);
    pVload->totalStorage = htobe64(pVload->totalStorage);
    pVload->compStorage = htobe64(pVload->compStorage);
    pVload->pointsWritten = htobe64(pVload->pointsWritten);
    pVload->tablesNum = htobe64(pVload->tablesNum);
  }
}

static int32_t mndProcessStatusReq(SMnodeMsg *pReq) {
  SMnode     *pMnode = pReq->pMnode;
  SStatusReq *pStatus = pReq->rpcMsg.pCont;
  SDnodeObj  *pDnode = NULL;
  int32_t     code = -1;

  mndParseStatusMsg(pStatus);

  if (pStatus->dnodeId == 0) {
    pDnode = mndAcquireDnodeByEp(pMnode, pStatus->dnodeEp);
    if (pDnode == NULL) {
      mDebug("dnode:%s, not created yet", pStatus->dnodeEp);
      terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
      goto PROCESS_STATUS_MSG_OVER;
    }
  } else {
    pDnode = mndAcquireDnode(pMnode, pStatus->dnodeId);
    if (pDnode == NULL) {
      pDnode = mndAcquireDnodeByEp(pMnode, pStatus->dnodeEp);
      if (pDnode != NULL) {
        pDnode->offlineReason = DND_REASON_DNODE_ID_NOT_MATCH;
      }
      mError("dnode:%d, %s not exist", pStatus->dnodeId, pStatus->dnodeEp);
      terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
      goto PROCESS_STATUS_MSG_OVER;
    }
  }

  int64_t curMs = taosGetTimestampMs();
  bool    online = mndIsDnodeOnline(pMnode, pDnode, curMs);
  bool    dnodeChanged = (pStatus->dver != sdbGetTableVer(pMnode->pSdb, SDB_DNODE));
  bool    reboot = (pDnode->rebootTime != pStatus->rebootTime);
  bool    needCheck = !online || dnodeChanged || reboot;

  if (needCheck) {
    if (pStatus->sver != pMnode->cfg.sver) {
      if (pDnode != NULL) {
        pDnode->offlineReason = DND_REASON_VERSION_NOT_MATCH;
      }
      mError("dnode:%d, status msg version:%d not match cluster:%d", pStatus->dnodeId, pStatus->sver, pMnode->cfg.sver);
      terrno = TSDB_CODE_MND_INVALID_MSG_VERSION;
      goto PROCESS_STATUS_MSG_OVER;
    }

    if (pStatus->dnodeId == 0) {
      mDebug("dnode:%d, %s first access, set clusterId %" PRId64, pDnode->id, pDnode->ep, pMnode->clusterId);
    } else {
      if (pStatus->clusterId != pMnode->clusterId) {
        if (pDnode != NULL) {
          pDnode->offlineReason = DND_REASON_CLUSTER_ID_NOT_MATCH;
        }
        mError("dnode:%d, clusterId %" PRId64 " not match exist %" PRId64, pDnode->id, pStatus->clusterId,
               pMnode->clusterId);
        terrno = TSDB_CODE_MND_INVALID_CLUSTER_ID;
        goto PROCESS_STATUS_MSG_OVER;
      } else {
        pDnode->accessTimes++;
        mTrace("dnode:%d, status received, access times %d", pDnode->id, pDnode->accessTimes);
      }
    }

    // Verify whether the cluster parameters are consistent when status change from offline to ready
    int32_t ret = mndCheckClusterCfgPara(pMnode, &pStatus->clusterCfg);
    if (0 != ret) {
      pDnode->offlineReason = ret;
      mError("dnode:%d, cluster cfg inconsistent since:%s", pDnode->id, offlineReason[ret]);
      terrno = TSDB_CODE_MND_INVALID_CLUSTER_CFG;
      goto PROCESS_STATUS_MSG_OVER;
    }

    if (!online) {
      mInfo("dnode:%d, from offline to online", pDnode->id);
    } else {
      mDebug("dnode:%d, send dnode eps", pDnode->id);
    }

    pDnode->rebootTime = pStatus->rebootTime;
    pDnode->numOfCores = pStatus->numOfCores;
    pDnode->numOfSupportVnodes = pStatus->numOfSupportVnodes;

    int32_t     numOfEps = mndGetDnodeSize(pMnode);
    int32_t     contLen = sizeof(SStatusRsp) + numOfEps * sizeof(SDnodeEp);
    SStatusRsp *pRsp = rpcMallocCont(contLen);
    if (pRsp == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto PROCESS_STATUS_MSG_OVER;
    }

    pRsp->dver = htobe64(sdbGetTableVer(pMnode->pSdb, SDB_DNODE));
    pRsp->dnodeCfg.dnodeId = htonl(pDnode->id);
    pRsp->dnodeCfg.clusterId = htobe64(pMnode->clusterId);
    mndGetDnodeData(pMnode, &pRsp->dnodeEps, numOfEps);

    pReq->contLen = contLen;
    pReq->pCont = pRsp;
  }

  pDnode->lastAccessTime = curMs;
  code = 0;

PROCESS_STATUS_MSG_OVER:
  mndReleaseDnode(pMnode, pDnode);
  return code;
}

static int32_t mndCreateDnode(SMnode *pMnode, SMnodeMsg *pReq, SCreateDnodeReq *pCreate) {
  SDnodeObj dnodeObj = {0};
  dnodeObj.id = sdbGetMaxId(pMnode->pSdb, SDB_DNODE);
  dnodeObj.createdTime = taosGetTimestampMs();
  dnodeObj.updateTime = dnodeObj.createdTime;
  dnodeObj.port = pCreate->port;
  memcpy(dnodeObj.fqdn, pCreate->fqdn, TSDB_FQDN_LEN);
  snprintf(dnodeObj.ep, TSDB_EP_LEN, "%s:%u", dnodeObj.fqdn, dnodeObj.port);

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, &pReq->rpcMsg);
  if (pTrans == NULL) {
    mError("dnode:%s, failed to create since %s", dnodeObj.ep, terrstr());
    return -1;
  }
  mDebug("trans:%d, used to create dnode:%s", pTrans->id, dnodeObj.ep);

  SSdbRaw *pRedoRaw = mndDnodeActionEncode(&dnodeObj);
  if (pRedoRaw == NULL || mndTransAppendRedolog(pTrans, pRedoRaw) != 0) {
    mError("trans:%d, failed to append redo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pRedoRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  return 0;
}

static int32_t mndProcessCreateDnodeReq(SMnodeMsg *pReq) {
  SMnode          *pMnode = pReq->pMnode;
  SCreateDnodeReq *pCreate = pReq->rpcMsg.pCont;
  pCreate->port = htonl(pCreate->port);
  mDebug("dnode:%s:%d, start to create", pCreate->fqdn, pCreate->port);

  if (pCreate->fqdn[0] == 0 || pCreate->port <= 0 || pCreate->port > UINT16_MAX) {
    terrno = TSDB_CODE_MND_INVALID_DNODE_EP;
    mError("dnode:%s:%d, failed to create since %s", pCreate->fqdn, pCreate->port, terrstr());
    return -1;
  }

  char ep[TSDB_EP_LEN];
  snprintf(ep, TSDB_EP_LEN, "%s:%d", pCreate->fqdn, pCreate->port);
  SDnodeObj *pDnode = mndAcquireDnodeByEp(pMnode, ep);
  if (pDnode != NULL) {
    mError("dnode:%d, already exist, %s:%u", pDnode->id, pCreate->fqdn, pCreate->port);
    mndReleaseDnode(pMnode, pDnode);
    terrno = TSDB_CODE_MND_DNODE_ALREADY_EXIST;
    return -1;
  }

  int32_t code = mndCreateDnode(pMnode, pReq, pCreate);

  if (code != 0) {
    mError("dnode:%s:%d, failed to create since %s", pCreate->fqdn, pCreate->port, terrstr());
    return -1;
  }

  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static int32_t mndDropDnode(SMnode *pMnode, SMnodeMsg *pReq, SDnodeObj *pDnode) {
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, &pReq->rpcMsg);
  if (pTrans == NULL) {
    mError("dnode:%d, failed to drop since %s", pDnode->id, terrstr());
    return -1;
  }
  mDebug("trans:%d, used to drop dnode:%d", pTrans->id, pDnode->id);

  SSdbRaw *pRedoRaw = mndDnodeActionEncode(pDnode);
  if (pRedoRaw == NULL || mndTransAppendRedolog(pTrans, pRedoRaw) != 0) {
    mError("trans:%d, failed to append redo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPED);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  return 0;
}

static int32_t mndProcessDropDnodeReq(SMnodeMsg *pReq) {
  SMnode        *pMnode = pReq->pMnode;
  SDropDnodeReq *pDrop = pReq->rpcMsg.pCont;
  pDrop->dnodeId = htonl(pDrop->dnodeId);

  mDebug("dnode:%d, start to drop", pDrop->dnodeId);

  if (pDrop->dnodeId <= 0) {
    terrno = TSDB_CODE_MND_INVALID_DNODE_ID;
    mError("dnode:%d, failed to drop since %s", pDrop->dnodeId, terrstr());
    return -1;
  }

  SDnodeObj *pDnode = mndAcquireDnode(pMnode, pDrop->dnodeId);
  if (pDnode == NULL) {
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    mError("dnode:%d, failed to drop since %s", pDrop->dnodeId, terrstr());
    return -1;
  }

  int32_t code = mndDropDnode(pMnode, pReq, pDnode);
  if (code != 0) {
    mndReleaseDnode(pMnode, pDnode);
    mError("dnode:%d, failed to drop since %s", pDrop->dnodeId, terrstr());
    return -1;
  }

  mndReleaseDnode(pMnode, pDnode);
  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static int32_t mndProcessConfigDnodeReq(SMnodeMsg *pReq) {
  SMnode        *pMnode = pReq->pMnode;
  SMCfgDnodeReq *pCfg = pReq->rpcMsg.pCont;
  pCfg->dnodeId = htonl(pCfg->dnodeId);

  SDnodeObj *pDnode = mndAcquireDnode(pMnode, pCfg->dnodeId);
  if (pDnode == NULL) {
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    mError("dnode:%d, failed to config since %s ", pCfg->dnodeId, terrstr());
    return -1;
  }

  SEpSet epSet = mndGetDnodeEpset(pDnode);
  mndReleaseDnode(pMnode, pDnode);

  SDCfgDnodeReq *pCfgDnode = rpcMallocCont(sizeof(SDCfgDnodeReq));
  pCfgDnode->dnodeId = htonl(pCfg->dnodeId);
  memcpy(pCfgDnode->config, pCfg->config, TSDB_DNODE_CONFIG_LEN);

  SRpcMsg rpcMsg = {.msgType = TDMT_DND_CONFIG_DNODE,
                    .pCont = pCfgDnode,
                    .contLen = sizeof(SDCfgDnodeReq),
                    .ahandle = pReq->rpcMsg.ahandle};

  mInfo("dnode:%d, app:%p config:%s req send to dnode", pCfg->dnodeId, rpcMsg.ahandle, pCfg->config);
  mndSendReqToDnode(pMnode, &epSet, &rpcMsg);

  return 0;
}

static int32_t mndProcessConfigDnodeRsp(SMnodeMsg *pRsp) {
  mInfo("app:%p config rsp from dnode", pRsp->rpcMsg.ahandle);
}

static int32_t mndGetConfigMeta(SMnodeMsg *pReq, SShowObj *pShow, STableMetaRsp *pMeta) {
  int32_t  cols = 0;
  SSchema *pSchema = pMeta->pSchema;

  pShow->bytes[cols] = TSDB_CONFIG_OPTION_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  tstrncpy(pSchema[cols].name, "name", sizeof(pSchema[cols].name));
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = TSDB_CONIIG_VALUE_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  tstrncpy(pSchema[cols].name, "value", sizeof(pSchema[cols].name));
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htonl(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = TSDB_CONFIG_NUMBER;
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  strcpy(pMeta->tbFname, mndShowStr(pShow->type));

  return 0;
}

static int32_t mndRetrieveConfigs(SMnodeMsg *pReq, SShowObj *pShow, char *data, int32_t rows) {
  SMnode *pMnode = pReq->pMnode;
  int32_t numOfRows = 0;
  char   *cfgOpts[TSDB_CONFIG_NUMBER] = {0};
  char    cfgVals[TSDB_CONFIG_NUMBER][TSDB_CONIIG_VALUE_LEN + 1] = {0};
  char   *pWrite;
  int32_t cols = 0;

  cfgOpts[numOfRows] = "statusInterval";
  snprintf(cfgVals[numOfRows], TSDB_CONIIG_VALUE_LEN, "%d", pMnode->cfg.statusInterval);
  numOfRows++;

  cfgOpts[numOfRows] = "timezone";
  snprintf(cfgVals[numOfRows], TSDB_CONIIG_VALUE_LEN, "%s", pMnode->cfg.timezone);
  numOfRows++;

  cfgOpts[numOfRows] = "locale";
  snprintf(cfgVals[numOfRows], TSDB_CONIIG_VALUE_LEN, "%s", pMnode->cfg.locale);
  numOfRows++;

  cfgOpts[numOfRows] = "charset";
  snprintf(cfgVals[numOfRows], TSDB_CONIIG_VALUE_LEN, "%s", pMnode->cfg.charset);
  numOfRows++;

  for (int32_t i = 0; i < numOfRows; i++) {
    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, cfgOpts[i], TSDB_CONFIG_OPTION_LEN);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, cfgVals[i], TSDB_CONIIG_VALUE_LEN);
    cols++;
  }

  mndVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  pShow->numOfReads += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextConfig(SMnode *pMnode, void *pIter) {}

static int32_t mndGetDnodeMeta(SMnodeMsg *pReq, SShowObj *pShow, STableMetaRsp *pMeta) {
  SMnode *pMnode = pReq->pMnode;
  SSdb   *pSdb = pMnode->pSdb;

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->pSchema;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "id");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = TSDB_EP_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "endpoint");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "vnodes");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "support_vnodes");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 10 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "status");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create_time");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 24 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "offline_reason");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htonl(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = sdbGetSize(pSdb, SDB_DNODE);
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  strcpy(pMeta->tbFname, mndShowStr(pShow->type));

  return 0;
}

static int32_t mndRetrieveDnodes(SMnodeMsg *pReq, SShowObj *pShow, char *data, int32_t rows) {
  SMnode    *pMnode = pReq->pMnode;
  SSdb      *pSdb = pMnode->pSdb;
  int32_t    numOfRows = 0;
  int32_t    cols = 0;
  SDnodeObj *pDnode = NULL;
  char      *pWrite;
  int64_t    curMs = taosGetTimestampMs();

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_DNODE, pShow->pIter, (void **)&pDnode);
    if (pShow->pIter == NULL) break;
    bool online = mndIsDnodeOnline(pMnode, pDnode, curMs);

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pDnode->id;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pDnode->ep, pShow->bytes[cols]);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = mndGetVnodesNum(pMnode, pDnode->id);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pDnode->numOfSupportVnodes;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_TO_VARSTR(pWrite, online ? "ready" : "offline");
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pDnode->createdTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_TO_VARSTR(pWrite, online ? "" : offlineReason[pDnode->offlineReason]);
    cols++;

    numOfRows++;
    sdbRelease(pSdb, pDnode);
  }

  mndVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  pShow->numOfReads += numOfRows;

  return numOfRows;
}

static void mndCancelGetNextDnode(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}