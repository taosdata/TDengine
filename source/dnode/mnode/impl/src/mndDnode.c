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
#include "mndShow.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"

#define TSDB_DNODE_VER_NUMBER   1
#define TSDB_DNODE_RESERVE_SIZE 64
#define TSDB_CONFIG_OPTION_LEN  16
#define TSDB_CONIIG_VALUE_LEN   48
#define TSDB_CONFIG_NUMBER      8

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

static int32_t mndProcessCreateDnodeReq(SNodeMsg *pReq);
static int32_t mndProcessDropDnodeReq(SNodeMsg *pReq);
static int32_t mndProcessConfigDnodeReq(SNodeMsg *pReq);
static int32_t mndProcessConfigDnodeRsp(SNodeMsg *pRsp);
static int32_t mndProcessStatusReq(SNodeMsg *pReq);

static int32_t mndGetConfigMeta(SNodeMsg *pReq, SShowObj *pShow, STableMetaRsp *pMeta);
static int32_t mndRetrieveConfigs(SNodeMsg *pReq, SShowObj *pShow, char *data, int32_t rows);
static void    mndCancelGetNextConfig(SMnode *pMnode, void *pIter);
static int32_t mndRetrieveDnodes(SNodeMsg *pReq, SShowObj *pShow, char *data, int32_t rows);
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

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_VARIABLES, mndRetrieveConfigs);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_VARIABLES, mndCancelGetNextConfig);
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

  return NULL;
}

int32_t mndGetDnodeSize(SMnode *pMnode) {
  SSdb *pSdb = pMnode->pSdb;
  return sdbGetSize(pSdb, SDB_DNODE);
}

bool mndIsDnodeOnline(SMnode *pMnode, SDnodeObj *pDnode, int64_t curMs) {
  int64_t interval = TABS(pDnode->lastAccessTime - curMs);
  if (interval > 30000 * tsStatusInterval) {
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

static int32_t mndCheckClusterCfgPara(SMnode *pMnode, const SClusterCfg *pCfg) {
  if (pCfg->statusInterval != tsStatusInterval) {
    mError("statusInterval [%d - %d] cfg inconsistent", pCfg->statusInterval, tsStatusInterval);
    return DND_REASON_STATUS_INTERVAL_NOT_MATCH;
  }

  if ((0 != strcasecmp(pCfg->timezone, tsTimezoneStr)) && (pMnode->checkTime != pCfg->checkTime)) {
    mError("timezone [%s - %s] [%" PRId64 " - %" PRId64 "] cfg inconsistent", pCfg->timezone, tsTimezoneStr,
           pCfg->checkTime, pMnode->checkTime);
    return DND_REASON_TIME_ZONE_NOT_MATCH;
  }

  if (0 != strcasecmp(pCfg->locale, tsLocale)) {
    mError("locale [%s - %s]  cfg inconsistent", pCfg->locale, tsLocale);
    return DND_REASON_LOCALE_NOT_MATCH;
  }

  if (0 != strcasecmp(pCfg->charset, tsCharset)) {
    mError("charset [%s - %s] cfg inconsistent.", pCfg->charset, tsCharset);
    return DND_REASON_CHARSET_NOT_MATCH;
  }

  return 0;
}

static int32_t mndProcessStatusReq(SNodeMsg *pReq) {
  SMnode    *pMnode = pReq->pNode;
  SStatusReq statusReq = {0};
  SDnodeObj *pDnode = NULL;
  int32_t    code = -1;

  if (tDeserializeSStatusReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &statusReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto PROCESS_STATUS_MSG_OVER;
  }

  if (statusReq.dnodeId == 0) {
    pDnode = mndAcquireDnodeByEp(pMnode, statusReq.dnodeEp);
    if (pDnode == NULL) {
      mDebug("dnode:%s, not created yet", statusReq.dnodeEp);
      terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
      goto PROCESS_STATUS_MSG_OVER;
    }
  } else {
    pDnode = mndAcquireDnode(pMnode, statusReq.dnodeId);
    if (pDnode == NULL) {
      pDnode = mndAcquireDnodeByEp(pMnode, statusReq.dnodeEp);
      if (pDnode != NULL) {
        pDnode->offlineReason = DND_REASON_DNODE_ID_NOT_MATCH;
      }
      mError("dnode:%d, %s not exist", statusReq.dnodeId, statusReq.dnodeEp);
      terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
      goto PROCESS_STATUS_MSG_OVER;
    }
  }

  int32_t numOfVloads = (int32_t)taosArrayGetSize(statusReq.pVloads);
  for (int32_t v = 0; v < numOfVloads; ++v) {
    SVnodeLoad *pVload = taosArrayGet(statusReq.pVloads, v);

    SVgObj *pVgroup = mndAcquireVgroup(pMnode, pVload->vgId);
    if (pVgroup != NULL) {
      if (pVload->role == TAOS_SYNC_STATE_LEADER) {
        pVgroup->numOfTables = pVload->numOfTables;
        pVgroup->numOfTimeSeries = pVload->numOfTimeSeries;
        pVgroup->totalStorage = pVload->totalStorage;
        pVgroup->compStorage = pVload->compStorage;
        pVgroup->pointsWritten = pVload->pointsWritten;
      }
      bool roleChanged = false;
      for (int32_t vg = 0; vg < pVgroup->replica; ++vg) {
        if (pVgroup->vnodeGid[vg].role != pVload->role) {
          roleChanged = true;
        }
        pVgroup->vnodeGid[vg].role = pVload->role;
      }
      if (roleChanged) {
        // notify scheduler role has changed
      }
    }

    mndReleaseVgroup(pMnode, pVgroup);
  }

  int64_t curMs = taosGetTimestampMs();
  bool    online = mndIsDnodeOnline(pMnode, pDnode, curMs);
  bool    dnodeChanged = (statusReq.dver != sdbGetTableVer(pMnode->pSdb, SDB_DNODE));
  bool    reboot = (pDnode->rebootTime != statusReq.rebootTime);
  bool    needCheck = !online || dnodeChanged || reboot;

  if (needCheck) {
    if (statusReq.sver != tsVersion) {
      if (pDnode != NULL) {
        pDnode->offlineReason = DND_REASON_VERSION_NOT_MATCH;
      }
      mError("dnode:%d, status msg version:%d not match cluster:%d", statusReq.dnodeId, statusReq.sver, tsVersion);
      terrno = TSDB_CODE_MND_INVALID_MSG_VERSION;
      goto PROCESS_STATUS_MSG_OVER;
    }

    if (statusReq.dnodeId == 0) {
      mDebug("dnode:%d, %s first access, set clusterId %" PRId64, pDnode->id, pDnode->ep, pMnode->clusterId);
    } else {
      if (statusReq.clusterId != pMnode->clusterId) {
        if (pDnode != NULL) {
          pDnode->offlineReason = DND_REASON_CLUSTER_ID_NOT_MATCH;
        }
        mError("dnode:%d, clusterId %" PRId64 " not match exist %" PRId64, pDnode->id, statusReq.clusterId,
               pMnode->clusterId);
        terrno = TSDB_CODE_MND_INVALID_CLUSTER_ID;
        goto PROCESS_STATUS_MSG_OVER;
      } else {
        pDnode->accessTimes++;
        mTrace("dnode:%d, status received, access times %d", pDnode->id, pDnode->accessTimes);
      }
    }

    // Verify whether the cluster parameters are consistent when status change from offline to ready
    int32_t ret = mndCheckClusterCfgPara(pMnode, &statusReq.clusterCfg);
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

    pDnode->rebootTime = statusReq.rebootTime;
    pDnode->numOfCores = statusReq.numOfCores;
    pDnode->numOfSupportVnodes = statusReq.numOfSupportVnodes;

    SStatusRsp statusRsp = {0};
    statusRsp.dver = sdbGetTableVer(pMnode->pSdb, SDB_DNODE);
    statusRsp.dnodeCfg.dnodeId = pDnode->id;
    statusRsp.dnodeCfg.clusterId = pMnode->clusterId;
    statusRsp.pDnodeEps = taosArrayInit(mndGetDnodeSize(pMnode), sizeof(SDnodeEp));
    if (statusRsp.pDnodeEps == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto PROCESS_STATUS_MSG_OVER;
    }

    mndGetDnodeData(pMnode, statusRsp.pDnodeEps);

    int32_t contLen = tSerializeSStatusRsp(NULL, 0, &statusRsp);
    void   *pHead = rpcMallocCont(contLen);
    tSerializeSStatusRsp(pHead, contLen, &statusRsp);
    taosArrayDestroy(statusRsp.pDnodeEps);

    pReq->rspLen = contLen;
    pReq->pRsp = pHead;
  }

  pDnode->lastAccessTime = curMs;
  code = 0;

PROCESS_STATUS_MSG_OVER:
  mndReleaseDnode(pMnode, pDnode);
  taosArrayDestroy(statusReq.pVloads);
  return code;
}

static int32_t mndCreateDnode(SMnode *pMnode, SNodeMsg *pReq, SCreateDnodeReq *pCreate) {
  SDnodeObj dnodeObj = {0};
  dnodeObj.id = sdbGetMaxId(pMnode->pSdb, SDB_DNODE);
  dnodeObj.createdTime = taosGetTimestampMs();
  dnodeObj.updateTime = dnodeObj.createdTime;
  dnodeObj.port = pCreate->port;
  memcpy(dnodeObj.fqdn, pCreate->fqdn, TSDB_FQDN_LEN);
  snprintf(dnodeObj.ep, TSDB_EP_LEN, "%s:%u", dnodeObj.fqdn, dnodeObj.port);

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_TYPE_CREATE_DNODE, &pReq->rpcMsg);
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

static int32_t mndProcessCreateDnodeReq(SNodeMsg *pReq) {
  SMnode         *pMnode = pReq->pNode;
  int32_t         code = -1;
  SUserObj       *pUser = NULL;
  SDnodeObj      *pDnode = NULL;
  SCreateDnodeReq createReq = {0};

  if (tDeserializeSCreateDnodeReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto CREATE_DNODE_OVER;
  }

  mDebug("dnode:%s:%d, start to create", createReq.fqdn, createReq.port);

  if (createReq.fqdn[0] == 0 || createReq.port <= 0 || createReq.port > UINT16_MAX) {
    terrno = TSDB_CODE_MND_INVALID_DNODE_EP;
    goto CREATE_DNODE_OVER;
  }

  char ep[TSDB_EP_LEN];
  snprintf(ep, TSDB_EP_LEN, "%s:%d", createReq.fqdn, createReq.port);
  pDnode = mndAcquireDnodeByEp(pMnode, ep);
  if (pDnode != NULL) {
    terrno = TSDB_CODE_MND_DNODE_ALREADY_EXIST;
    goto CREATE_DNODE_OVER;
  }

  pUser = mndAcquireUser(pMnode, pReq->user);
  if (pUser == NULL) {
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
    goto CREATE_DNODE_OVER;
  }

  if (mndCheckNodeAuth(pUser)) {
    goto CREATE_DNODE_OVER;
  }

  code = mndCreateDnode(pMnode, pReq, &createReq);
  if (code == 0) code = TSDB_CODE_MND_ACTION_IN_PROGRESS;

CREATE_DNODE_OVER:
  if (code != 0 && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("dnode:%s:%d, failed to create since %s", createReq.fqdn, createReq.port, terrstr());
  }

  mndReleaseDnode(pMnode, pDnode);
  mndReleaseUser(pMnode, pUser);
  return code;
}

static int32_t mndDropDnode(SMnode *pMnode, SNodeMsg *pReq, SDnodeObj *pDnode) {
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_TYPE_DROP_DNODE, &pReq->rpcMsg);
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

static int32_t mndProcessDropDnodeReq(SNodeMsg *pReq) {
  SMnode        *pMnode = pReq->pNode;
  int32_t        code = -1;
  SUserObj      *pUser = NULL;
  SDnodeObj     *pDnode = NULL;
  SMnodeObj     *pMObj = NULL;
  SMDropMnodeReq dropReq = {0};

  if (tDeserializeSCreateDropMQSBNodeReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto DROP_DNODE_OVER;
  }

  mDebug("dnode:%d, start to drop", dropReq.dnodeId);

  if (dropReq.dnodeId <= 0) {
    terrno = TSDB_CODE_MND_INVALID_DNODE_ID;
    goto DROP_DNODE_OVER;
  }

  pDnode = mndAcquireDnode(pMnode, dropReq.dnodeId);
  if (pDnode == NULL) {
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    goto DROP_DNODE_OVER;
  }

  pMObj = mndAcquireMnode(pMnode, dropReq.dnodeId);
  if (pMObj != NULL) {
    terrno = TSDB_CODE_MND_MNODE_DEPLOYED;
    goto DROP_DNODE_OVER;
  }

  pUser = mndAcquireUser(pMnode, pReq->user);
  if (pUser == NULL) {
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
    goto DROP_DNODE_OVER;
  }

  if (mndCheckNodeAuth(pUser)) {
    goto DROP_DNODE_OVER;
  }

  code = mndDropDnode(pMnode, pReq, pDnode);
  if (code == 0) code = TSDB_CODE_MND_ACTION_IN_PROGRESS;

DROP_DNODE_OVER:
  if (code != 0 && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("dnode:%d, failed to drop since %s", dropReq.dnodeId, terrstr());
  }

  mndReleaseDnode(pMnode, pDnode);
  mndReleaseUser(pMnode, pUser);
  mndReleaseMnode(pMnode, pMObj);

  return code;
}

static int32_t mndProcessConfigDnodeReq(SNodeMsg *pReq) {
  SMnode *pMnode = pReq->pNode;

  SMCfgDnodeReq cfgReq = {0};
  if (tDeserializeSMCfgDnodeReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &cfgReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  SDnodeObj *pDnode = mndAcquireDnode(pMnode, cfgReq.dnodeId);
  if (pDnode == NULL) {
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    mError("dnode:%d, failed to config since %s ", cfgReq.dnodeId, terrstr());
    return -1;
  }

  SEpSet epSet = mndGetDnodeEpset(pDnode);
  mndReleaseDnode(pMnode, pDnode);

  int32_t bufLen = tSerializeSMCfgDnodeReq(NULL, 0, &cfgReq);
  void   *pBuf = rpcMallocCont(bufLen);
  tSerializeSMCfgDnodeReq(pBuf, bufLen, &cfgReq);

  SRpcMsg rpcMsg = {
      .msgType = TDMT_DND_CONFIG_DNODE, .pCont = pBuf, .contLen = bufLen, .ahandle = pReq->rpcMsg.ahandle};

  mInfo("dnode:%d, app:%p config:%s req send to dnode", cfgReq.dnodeId, rpcMsg.ahandle, cfgReq.config);
  tmsgSendReq(&pMnode->msgCb, &epSet, &rpcMsg);

  return 0;
}

static int32_t mndProcessConfigDnodeRsp(SNodeMsg *pRsp) {
  mInfo("app:%p config rsp from dnode", pRsp->rpcMsg.ahandle);
  return TSDB_CODE_SUCCESS;
}

static int32_t mndGetConfigMeta(SNodeMsg *pReq, SShowObj *pShow, STableMetaRsp *pMeta) {
  int32_t  cols = 0;
  SSchema *pSchema = pMeta->pSchemas;

  pShow->bytes[cols] = TSDB_CONFIG_OPTION_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "name");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = TSDB_CONIIG_VALUE_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "value");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pMeta->numOfColumns = cols;
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = TSDB_CONFIG_NUMBER;
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  strcpy(pMeta->tbName, mndShowStr(pShow->type));

  return 0;
}

static int32_t mndRetrieveConfigs(SNodeMsg *pReq, SShowObj *pShow, char *data, int32_t rows) {
  SMnode *pMnode = pReq->pNode;
  int32_t totalRows = 0;
  int32_t numOfRows = 0;
  char   *cfgOpts[TSDB_CONFIG_NUMBER] = {0};
  char    cfgVals[TSDB_CONFIG_NUMBER][TSDB_CONIIG_VALUE_LEN + 1] = {0};
  char   *pWrite;
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

  for (int32_t i = 0; i < totalRows; i++) {
    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, cfgOpts[i], TSDB_CONFIG_OPTION_LEN);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, cfgVals[i], TSDB_CONIIG_VALUE_LEN);
    cols++;

    numOfRows++;
  }

  mndVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  pShow->numOfReads += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextConfig(SMnode *pMnode, void *pIter) {}

static int32_t mndRetrieveDnodes(SNodeMsg *pReq, SShowObj *pShow, char *data, int32_t rows) {
  SMnode    *pMnode = pReq->pNode;
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
