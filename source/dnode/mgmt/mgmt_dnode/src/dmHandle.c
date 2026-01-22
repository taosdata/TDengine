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
 * along with this program. If not, see <http:www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "audit.h"
#include "dmInt.h"
// #include "dmMgmt.h"
#include "crypt.h"
#include "monitor.h"
#include "stream.h"
#include "systable.h"
#include "tanalytics.h"
#include "tchecksum.h"
#include "tencrypt.h"
#include "tutil.h"

#if defined(TD_ENTERPRISE) && defined(TD_HAS_TAOSK)
#include "taoskInt.h"
#endif

extern SConfig *tsCfg;
extern void setAuditDbNameToken(char *pDb, char *pToken);

#ifndef TD_ENTERPRISE
void setAuditDbNameToken(char *pDb, char *pToken) {}
#endif

extern void getAuditDbNameToken(char *pDb, char *pToken);

#ifndef TD_ENTERPRISE
void getAuditDbNameToken(char *pDb, char *pToken) {}
#endif

SMonVloadInfo tsVinfo = {0};
SMnodeLoad    tsMLoad = {0};
SDnodeData    tsDnodeData = {0};

static void dmUpdateDnodeCfg(SDnodeMgmt *pMgmt, SDnodeCfg *pCfg) {
  int32_t code = 0;
  if (pMgmt->pData->dnodeId == 0 || pMgmt->pData->clusterId == 0) {
    dInfo("set local info, dnodeId:%d clusterId:%" PRId64, pCfg->dnodeId, pCfg->clusterId);
    (void)taosThreadRwlockWrlock(&pMgmt->pData->lock);
    pMgmt->pData->dnodeId = pCfg->dnodeId;
    pMgmt->pData->clusterId = pCfg->clusterId;
    monSetDnodeId(pCfg->dnodeId);
    auditSetDnodeId(pCfg->dnodeId);
    code = dmWriteEps(pMgmt->pData);
    if (code != 0) {
      dInfo("failed to set local info, dnodeId:%d clusterId:0x%" PRIx64 " reason:%s", pCfg->dnodeId, pCfg->clusterId,
            tstrerror(code));
    }
    (void)taosThreadRwlockUnlock(&pMgmt->pData->lock);
  }
}

static void dmMayShouldUpdateIpWhiteList(SDnodeMgmt *pMgmt, int64_t ver) {
  int32_t code = 0;
  dDebug("ip-white-list on dnode ver: %" PRId64 ", status ver: %" PRId64, pMgmt->pData->ipWhiteVer, ver);
  if (pMgmt->pData->ipWhiteVer == ver) {
    if (ver == 0) {
      dDebug("disable ip-white-list on dnode ver: %" PRId64 ", status ver: %" PRId64, pMgmt->pData->ipWhiteVer, ver);
      if (rpcSetIpWhite(pMgmt->msgCb.serverRpc, NULL) != 0) {
        dError("failed to disable ip white list on dnode");
      }
    }
    return;
  }
  int64_t oldVer = pMgmt->pData->ipWhiteVer;

  SRetrieveWhiteListReq req = {.ver = oldVer};
  int32_t             contLen = tSerializeRetrieveWhiteListReq(NULL, 0, &req);
  if (contLen < 0) {
    dError("failed to serialize ip white list request since: %s", tstrerror(contLen));
    return;
  }
  void *pHead = rpcMallocCont(contLen);
  contLen = tSerializeRetrieveWhiteListReq(pHead, contLen, &req);
  if (contLen < 0) {
    rpcFreeCont(pHead);
    dError("failed to serialize ip white list request since:%s", tstrerror(contLen));
    return;
  }

  SRpcMsg rpcMsg = {.pCont = pHead,
                    .contLen = contLen,
                    .msgType = TDMT_MND_RETRIEVE_IP_WHITELIST_DUAL,
                    .info.ahandle = 0,
                    .info.notFreeAhandle = 1,
                    .info.refId = 0,
                    .info.noResp = 0,
                    .info.handle = 0};
  SEpSet  epset = {0};

  (void)dmGetMnodeEpSet(pMgmt->pData, &epset);

  code = rpcSendRequest(pMgmt->msgCb.clientRpc, &epset, &rpcMsg, NULL);
  if (code != 0) {
    dError("failed to send retrieve ip white list request since:%s", tstrerror(code));
  }
}



static void dmMayShouldUpdateTimeWhiteList(SDnodeMgmt *pMgmt, int64_t ver) {
  int32_t code = 0;
  dDebug("time-white-list on dnode ver: %" PRId64 ", status ver: %" PRId64, pMgmt->pData->timeWhiteVer, ver);
  if (pMgmt->pData->timeWhiteVer == ver) {
    if (ver == 0) {
      dDebug("disable time-white-list on dnode ver: %" PRId64 ", status ver: %" PRId64, pMgmt->pData->timeWhiteVer, ver);
      if (rpcSetIpWhite(pMgmt->msgCb.serverRpc, NULL) != 0) {
        dError("failed to disable time white list on dnode");
      }
    }
    return;
  }
  int64_t oldVer = pMgmt->pData->timeWhiteVer;

  SRetrieveWhiteListReq req = {.ver = oldVer};
  int32_t             contLen = tSerializeRetrieveWhiteListReq(NULL, 0, &req);
  if (contLen < 0) {
    dError("failed to serialize datetime white list request since: %s", tstrerror(contLen));
    return;
  }
  void *pHead = rpcMallocCont(contLen);
  contLen = tSerializeRetrieveWhiteListReq(pHead, contLen, &req);
  if (contLen < 0) {
    rpcFreeCont(pHead);
    dError("failed to serialize datetime white list request since:%s", tstrerror(contLen));
    return;
  }

  SRpcMsg rpcMsg = {.pCont = pHead,
                    .contLen = contLen,
                    .msgType = TDMT_MND_RETRIEVE_DATETIME_WHITELIST,
                    .info.ahandle = 0,
                    .info.notFreeAhandle = 1,
                    .info.refId = 0,
                    .info.noResp = 0,
                    .info.handle = 0};
  SEpSet  epset = {0};

  (void)dmGetMnodeEpSet(pMgmt->pData, &epset);

  code = rpcSendRequest(pMgmt->msgCb.clientRpc, &epset, &rpcMsg, NULL);
  if (code != 0) {
    dError("failed to send retrieve datetime white list request since:%s", tstrerror(code));
  }
}



static void dmMayShouldUpdateAnalyticsFunc(SDnodeMgmt *pMgmt, int64_t newVer) {
  int32_t code = 0;
  int64_t oldVer = taosAnalyGetVersion();
  if (oldVer == newVer) return;
  dDebug("analysis on dnode ver:%" PRId64 ", status ver:%" PRId64, oldVer, newVer);

  SRetrieveAnalyticsAlgoReq req = {.dnodeId = pMgmt->pData->dnodeId, .analVer = oldVer};
  int32_t              contLen = tSerializeRetrieveAnalyticAlgoReq(NULL, 0, &req);
  if (contLen < 0) {
    dError("failed to serialize analysis function ver request since %s", tstrerror(contLen));
    return;
  }

  void *pHead = rpcMallocCont(contLen);
  contLen = tSerializeRetrieveAnalyticAlgoReq(pHead, contLen, &req);
  if (contLen < 0) {
    rpcFreeCont(pHead);
    dError("failed to serialize analysis function ver request since %s", tstrerror(contLen));
    return;
  }

  SRpcMsg rpcMsg = {
      .pCont = pHead,
      .contLen = contLen,
      .msgType = TDMT_MND_RETRIEVE_ANAL_ALGO,
      .info.ahandle = 0,
      .info.refId = 0,
      .info.noResp = 0,
      .info.handle = 0,
  };
  SEpSet epset = {0};

  (void)dmGetMnodeEpSet(pMgmt->pData, &epset);

  code = rpcSendRequest(pMgmt->msgCb.clientRpc, &epset, &rpcMsg, NULL);
  if (code != 0) {
    dError("failed to send retrieve analysis func ver request since %s", tstrerror(code));
  }
}

static void dmProcessStatusRsp(SDnodeMgmt *pMgmt, SRpcMsg *pRsp) {
  const STraceId *trace = &pRsp->info.traceId;
  dGTrace("status rsp received from mnode, statusSeq:%d code:0x%x", pMgmt->statusSeq, pRsp->code);

  if (pRsp->code != 0) {
    if (pRsp->code == TSDB_CODE_MND_DNODE_NOT_EXIST && !pMgmt->pData->dropped && pMgmt->pData->dnodeId > 0) {
      dGInfo("dnode:%d, set to dropped since not exist in mnode, statusSeq:%d", pMgmt->pData->dnodeId,
             pMgmt->statusSeq);
      pMgmt->pData->dropped = 1;
      if (dmWriteEps(pMgmt->pData) != 0) {
        dError("failed to write dnode file");
      }
      dInfo("dnode will exit since it is in the dropped state");
      (void)raise(SIGINT);
    }
  } else {
    SStatusRsp statusRsp = {0};
    if (pRsp->pCont != NULL && pRsp->contLen > 0 &&
        tDeserializeSStatusRsp(pRsp->pCont, pRsp->contLen, &statusRsp) == 0) {
      if (pMgmt->pData->dnodeVer != statusRsp.dnodeVer) {
        dGInfo("status rsp received from mnode, statusSeq:%d:%d dnodeVer:%" PRId64 ":%" PRId64, pMgmt->statusSeq,
               statusRsp.statusSeq, pMgmt->pData->dnodeVer, statusRsp.dnodeVer);
        pMgmt->pData->dnodeVer = statusRsp.dnodeVer;
        dmUpdateDnodeCfg(pMgmt, &statusRsp.dnodeCfg);
        dmUpdateEps(pMgmt->pData, statusRsp.pDnodeEps);
      }
      setAuditDbNameToken(statusRsp.auditDB, statusRsp.auditToken);
      dmMayShouldUpdateIpWhiteList(pMgmt, statusRsp.ipWhiteVer);
      dmMayShouldUpdateTimeWhiteList(pMgmt, statusRsp.timeWhiteVer);
      dmMayShouldUpdateAnalyticsFunc(pMgmt, statusRsp.analVer);
    }
    tFreeSStatusRsp(&statusRsp);
  }
  rpcFreeCont(pRsp->pCont);
}

void dmSendStatusReq(SDnodeMgmt *pMgmt) {
  int32_t    code = 0;
  SStatusReq req = {0};
  req.timestamp = taosGetTimestampMs();
  pMgmt->statusSeq++;

  dTrace("send status req to mnode, begin to mgnt statusInfolock, statusSeq:%d", pMgmt->statusSeq);
  if (taosThreadMutexLock(&pMgmt->pData->statusInfolock) != 0) {
    dError("failed to lock status info lock");
    return;
  }

  dTrace("send status req to mnode, begin to get dnode info, statusSeq:%d", pMgmt->statusSeq);
  req.sver = tsVersion;
  req.dnodeVer = tsDnodeData.dnodeVer;
  req.dnodeId = tsDnodeData.dnodeId;
  req.clusterId = tsDnodeData.clusterId;
  if (req.clusterId == 0) req.dnodeId = 0;
  req.rebootTime = tsDnodeData.rebootTime;
  req.updateTime = tsDnodeData.updateTime;
  req.numOfCores = tsNumOfCores;
  req.numOfSupportVnodes = tsNumOfSupportVnodes;
  req.numOfDiskCfg = tsDiskCfgNum;
  req.memTotal = tsTotalMemoryKB * 1024;
  req.memAvail = req.memTotal - tsQueueMemoryAllowed - tsApplyMemoryAllowed - 16 * 1024 * 1024;
  tstrncpy(req.dnodeEp, tsLocalEp, TSDB_EP_LEN);
  tstrncpy(req.machineId, tsDnodeData.machineId, TSDB_MACHINE_ID_LEN + 1);

  req.clusterCfg.statusInterval = tsStatusInterval;
  req.clusterCfg.statusIntervalMs = tsStatusIntervalMs;
  req.clusterCfg.checkTime = 0;
  req.clusterCfg.ttlChangeOnWrite = tsTtlChangeOnWrite;
  req.clusterCfg.enableWhiteList = tsEnableWhiteList ? 1 : 0;
  req.clusterCfg.encryptionKeyStat = tsEncryptionKeyStat;
  req.clusterCfg.encryptionKeyChksum = tsEncryptionKeyChksum;
  req.clusterCfg.monitorParas.tsEnableMonitor = tsEnableMonitor;
  req.clusterCfg.monitorParas.tsMonitorInterval = tsMonitorInterval;
  req.clusterCfg.monitorParas.tsSlowLogScope = tsSlowLogScope;
  req.clusterCfg.monitorParas.tsSlowLogMaxLen = tsSlowLogMaxLen;
  req.clusterCfg.monitorParas.tsSlowLogThreshold = tsSlowLogThreshold;
  tstrncpy(req.clusterCfg.monitorParas.tsSlowLogExceptDb, tsSlowLogExceptDb, TSDB_DB_NAME_LEN);
  char timestr[32] = "1970-01-01 00:00:00.00";
  if (taosParseTime(timestr, &req.clusterCfg.checkTime, (int32_t)strlen(timestr), TSDB_TIME_PRECISION_MILLI, NULL) !=
      0) {
    dError("failed to parse time since %s", tstrerror(code));
  }
  memcpy(req.clusterCfg.timezone, tsTimezoneStr, TD_TIMEZONE_LEN);
  memcpy(req.clusterCfg.locale, tsLocale, TD_LOCALE_LEN);
  memcpy(req.clusterCfg.charset, tsCharset, TD_LOCALE_LEN);

  dTrace("send status req to mnode, begin to get vnode loads, statusSeq:%d", pMgmt->statusSeq);

  req.pVloads = tsVinfo.pVloads;
  tsVinfo.pVloads = NULL;

  dTrace("send status req to mnode, begin to get mnode loads, statusSeq:%d", pMgmt->statusSeq);
  req.mload = tsMLoad;

  if (taosThreadMutexUnlock(&pMgmt->pData->statusInfolock) != 0) {
    dError("failed to unlock status info lock");
    return;
  }

  dTrace("send status req to mnode, begin to get qnode loads, statusSeq:%d", pMgmt->statusSeq);
  (*pMgmt->getQnodeLoadsFp)(&req.qload);

  req.statusSeq = pMgmt->statusSeq;
  req.ipWhiteVer = pMgmt->pData->ipWhiteVer;
  req.analVer = taosAnalyGetVersion();
  req.timeWhiteVer = pMgmt->pData->timeWhiteVer;

  if (tsAuditUseToken) {
    getAuditDbNameToken(req.auditDB, req.auditToken);
  }

  int32_t contLen = tSerializeSStatusReq(NULL, 0, &req);
  if (contLen < 0) {
    dError("failed to serialize status req since %s", tstrerror(contLen));
    return;
  }

  void *pHead = rpcMallocCont(contLen);
  contLen = tSerializeSStatusReq(pHead, contLen, &req);
  if (contLen < 0) {
    rpcFreeCont(pHead);
    dError("failed to serialize status req since %s", tstrerror(contLen));
    return;
  }
  tFreeSStatusReq(&req);

  SRpcMsg rpcMsg = {.pCont = pHead,
                    .contLen = contLen,
                    .msgType = TDMT_MND_STATUS,
                    .info.ahandle = 0,
                    .info.notFreeAhandle = 1,
                    .info.refId = 0,
                    .info.noResp = 0,
                    .info.handle = 0};
  SRpcMsg rpcRsp = {0};

  dDebug("send status req to mnode, dnodeVer:%" PRId64 " statusSeq:%d", req.dnodeVer, req.statusSeq);

  SEpSet epSet = {0};
  int8_t epUpdated = 0;
  (void)dmGetMnodeEpSet(pMgmt->pData, &epSet);

  if (dDebugFlag & DEBUG_TRACE) {
    char tbuf[512];
    dmEpSetToStr(tbuf, sizeof(tbuf), &epSet);
    dTrace("send status req to mnode, begin to send rpc msg, statusSeq:%d to %s", pMgmt->statusSeq, tbuf);
  }
  code = rpcSendRecvWithTimeout(pMgmt->msgCb.statusRpc, &epSet, &rpcMsg, &rpcRsp, &epUpdated, tsStatusSRTimeoutMs);
  if (code != 0) {
    dError("failed to SendRecv status req with timeout %d since %s", tsStatusSRTimeoutMs, tstrerror(code));
    if (code == TSDB_CODE_TIMEOUT_ERROR) {
      dmRotateMnodeEpSet(pMgmt->pData);
      char tbuf[512];
      dmEpSetToStr(tbuf, sizeof(tbuf), &epSet);
      dInfo("Rotate mnode ep set since failed to SendRecv status req %s, epSet:%s, inUse:%d", tstrerror(rpcRsp.code),
            tbuf, epSet.inUse);
    }
    return;
  }

  if (rpcRsp.code != 0) {
    dmRotateMnodeEpSet(pMgmt->pData);
    char tbuf[512];
    dmEpSetToStr(tbuf, sizeof(tbuf), &epSet);
    dInfo("Rotate mnode ep set since failed to SendRecv status req %s, epSet:%s, inUse:%d", tstrerror(rpcRsp.code),
          tbuf, epSet.inUse);
  } else {
    if (epUpdated == 1) {
      dmSetMnodeEpSet(pMgmt->pData, &epSet);
    }
  }
  dmProcessStatusRsp(pMgmt, &rpcRsp);
}

static void dmProcessConfigRsp(SDnodeMgmt *pMgmt, SRpcMsg *pRsp) {
  const STraceId *trace = &pRsp->info.traceId;
  int32_t         code = 0;
  SConfigRsp      configRsp = {0};
  bool            needStop = false;

  if (pRsp->code != 0) {
    if (pRsp->code == TSDB_CODE_MND_DNODE_NOT_EXIST && !pMgmt->pData->dropped && pMgmt->pData->dnodeId > 0) {
      dGInfo("dnode:%d, set to dropped since not exist in mnode", pMgmt->pData->dnodeId);
      pMgmt->pData->dropped = 1;
      if (dmWriteEps(pMgmt->pData) != 0) {
        dError("failed to write dnode file");
      }
      dInfo("dnode will exit since it is in the dropped state");
      (void)raise(SIGINT);
    }
  } else {
    bool needUpdate = false;
    if (pRsp->pCont != NULL && pRsp->contLen > 0 &&
        tDeserializeSConfigRsp(pRsp->pCont, pRsp->contLen, &configRsp) == 0) {
      // Try to use cfg from mnode sdb.
      if (!configRsp.isVersionVerified) {
        uInfo("config version not verified, update config");
        needUpdate = true;
        code = taosPersistGlobalConfig(configRsp.array, pMgmt->path, configRsp.cver);
        if (code != TSDB_CODE_SUCCESS) {
          dError("failed to persist global config since %s", tstrerror(code));
          goto _exit;
        }
      }
    }
    if (needUpdate) {
      code = cfgUpdateFromArray(tsCfg, configRsp.array);
      if (code != TSDB_CODE_SUCCESS) {
        dError("failed to update config since %s", tstrerror(code));
        goto _exit;
      }
      code = setAllConfigs(tsCfg);
      if (code != TSDB_CODE_SUCCESS) {
        dError("failed to set all configs since %s", tstrerror(code));
        goto _exit;
      }
    }
    code = taosPersistLocalConfig(pMgmt->path);
    if (code != TSDB_CODE_SUCCESS) {
      dError("failed to persist local config since %s", tstrerror(code));
    }
    tsConfigInited = 1;
  }
_exit:
  tFreeSConfigRsp(&configRsp);
  rpcFreeCont(pRsp->pCont);
  if (needStop) {
    dmStop();
  }
}

int32_t dmProcessKeySyncRsp(SDnodeMgmt *pMgmt, SRpcMsg *pRsp) {
  const STraceId *trace = &pRsp->info.traceId;
  int32_t         code = 0;
  SKeySyncRsp     keySyncRsp = {0};

  if (pRsp->code != 0) {
    dError("failed to sync keys from mnode since %s", tstrerror(pRsp->code));
    code = pRsp->code;
    goto _exit;
  }

  if (pRsp->pCont == NULL || pRsp->contLen <= 0) {
    dError("invalid key sync response, empty content");
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  code = tDeserializeSKeySyncRsp(pRsp->pCont, pRsp->contLen, &keySyncRsp);
  if (code != 0) {
    dError("failed to deserialize key sync response since %s", tstrerror(code));
    goto _exit;
  }

  dInfo("received key sync response, mnode keyVersion:%d, local keyVersion:%d, needUpdate:%d", keySyncRsp.keyVersion,
        tsLocalKeyVersion, keySyncRsp.needUpdate);
  tsEncryptKeysStatus = keySyncRsp.encryptionKeyStatus;
  if (keySyncRsp.needUpdate) {
#if defined(TD_ENTERPRISE) && defined(TD_HAS_TAOSK)
    // Get encrypt file path from tsDataDir
    char masterKeyFile[PATH_MAX] = {0};
    char derivedKeyFile[PATH_MAX] = {0};
    snprintf(masterKeyFile, sizeof(masterKeyFile), "%s%sdnode%sconfig%smaster.bin", tsDataDir, TD_DIRSEP, TD_DIRSEP,
             TD_DIRSEP);
    snprintf(derivedKeyFile, sizeof(derivedKeyFile), "%s%sdnode%sconfig%sderived.bin", tsDataDir, TD_DIRSEP, TD_DIRSEP,
             TD_DIRSEP);

    dInfo("updating local encryption keys from mnode, key file is saved in %s and %s, keyVersion:%d -> %d",
          masterKeyFile, derivedKeyFile, tsLocalKeyVersion, keySyncRsp.keyVersion);

    // Save keys to master.bin and derived.bin
    // Use the same algorithm for cfg and meta keys (backward compatible)
    code = taoskSaveEncryptKeys(masterKeyFile, derivedKeyFile, keySyncRsp.svrKey, keySyncRsp.dbKey, keySyncRsp.cfgKey, keySyncRsp.metaKey,
                                keySyncRsp.dataKey, keySyncRsp.algorithm, keySyncRsp.algorithm, keySyncRsp.algorithm,
                                keySyncRsp.keyVersion, keySyncRsp.createTime,
                                keySyncRsp.svrKeyUpdateTime, keySyncRsp.dbKeyUpdateTime);
    if (code != 0) {
      dError("failed to save encryption keys since %s", tstrerror(code));
      goto _exit;
    }

    // Update global variables with synced keys
    tstrncpy(tsSvrKey, keySyncRsp.svrKey, sizeof(tsSvrKey));
    tstrncpy(tsDbKey, keySyncRsp.dbKey, sizeof(tsDbKey));
    tstrncpy(tsCfgKey, keySyncRsp.cfgKey, sizeof(tsCfgKey));
    tstrncpy(tsMetaKey, keySyncRsp.metaKey, sizeof(tsMetaKey));
    tstrncpy(tsDataKey, keySyncRsp.dataKey, sizeof(tsDataKey));
    tsEncryptAlgorithmType = keySyncRsp.algorithm;
    tsEncryptKeyVersion = keySyncRsp.keyVersion;
    tsEncryptKeyCreateTime = keySyncRsp.createTime;
    tsSvrKeyUpdateTime = keySyncRsp.svrKeyUpdateTime;
    tsDbKeyUpdateTime = keySyncRsp.dbKeyUpdateTime;

    // Update local key version
    tsLocalKeyVersion = keySyncRsp.keyVersion;
    dInfo("successfully updated local encryption keys to version:%d", tsLocalKeyVersion);

    // Encrypt existing plaintext config files
    code = taosEncryptExistingCfgFiles(tsDataDir);
    if (code != 0) {
      dWarn("failed to encrypt existing config files since %s, will retry on next write", tstrerror(code));
      // Don't fail the key sync, files will be encrypted on next write
      code = 0;
    }
#else
    dWarn("enterprise features not enabled, skipping key sync");
#endif
  } else {
    dDebug("local keys are up to date, version:%d", tsLocalKeyVersion);
  }
  
  code = TSDB_CODE_SUCCESS;

_exit:
  rpcFreeCont(pRsp->pCont);
  return code;
}

void dmSendKeySyncReq(SDnodeMgmt *pMgmt) {
  int32_t     code = 0;
  SKeySyncReq req = {0};

  req.dnodeId = pMgmt->pData->dnodeId;
  req.keyVersion = tsLocalKeyVersion;
  dDebug("send key sync req to mnode, dnodeId:%d keyVersion:%d", req.dnodeId, req.keyVersion);

  int32_t contLen = tSerializeSKeySyncReq(NULL, 0, &req);
  if (contLen < 0) {
    dError("failed to serialize key sync req since %s", tstrerror(contLen));
    return;
  }

  void *pHead = rpcMallocCont(contLen);
  if (pHead == NULL) {
    dError("failed to malloc cont since %s", tstrerror(contLen));
    return;
  }
  contLen = tSerializeSKeySyncReq(pHead, contLen, &req);
  if (contLen < 0) {
    rpcFreeCont(pHead);
    dError("failed to serialize key sync req since %s", tstrerror(contLen));
    return;
  }

  SRpcMsg rpcMsg = {.pCont = pHead,
                    .contLen = contLen,
                    .msgType = TDMT_MND_KEY_SYNC,
                    .info.ahandle = 0,
                    .info.notFreeAhandle = 1,
                    .info.refId = 0,
                    .info.noResp = 0,
                    .info.handle = 0};
  SRpcMsg rpcRsp = {0};

  SEpSet epSet = {0};
  int8_t epUpdated = 0;
  (void)dmGetMnodeEpSet(pMgmt->pData, &epSet);

  dDebug("send key sync req to mnode, dnodeId:%d keyVersion:%d, begin to send rpc msg", req.dnodeId, req.keyVersion);
  code = rpcSendRecvWithTimeout(pMgmt->msgCb.statusRpc, &epSet, &rpcMsg, &rpcRsp, &epUpdated, tsStatusSRTimeoutMs);
  if (code != 0) {
    dError("failed to SendRecv key sync req with timeout %d since %s", tsStatusSRTimeoutMs, tstrerror(code));
    return;
  }
  if (rpcRsp.code != 0) {
    dError("failed to send key sync req since %s", tstrerror(rpcRsp.code));
    return;
  }
  code = dmProcessKeySyncRsp(pMgmt, &rpcRsp);
  if (code != 0) {
    dError("failed to process key sync rsp since %s", tstrerror(code));
    return;
  }
}

void dmSendConfigReq(SDnodeMgmt *pMgmt) {
  int32_t    code = 0;
  SConfigReq req = {0};

  req.cver = tsdmConfigVersion;
  req.forceReadConfig = tsForceReadConfig;
  req.array = taosGetGlobalCfg(tsCfg);
  dDebug("send config req to mnode, configVersion:%d", req.cver);

  int32_t contLen = tSerializeSConfigReq(NULL, 0, &req);
  if (contLen < 0) {
    dError("failed to serialize status req since %s", tstrerror(contLen));
    return;
  }

  void *pHead = rpcMallocCont(contLen);
  if (pHead == NULL) {
    dError("failed to malloc cont since %s", tstrerror(contLen));
    return;
  }
  contLen = tSerializeSConfigReq(pHead, contLen, &req);
  if (contLen < 0) {
    rpcFreeCont(pHead);
    dError("failed to serialize status req since %s", tstrerror(contLen));
    return;
  }

  SRpcMsg rpcMsg = {.pCont = pHead,
                    .contLen = contLen,
                    .msgType = TDMT_MND_CONFIG,
                    .info.ahandle = 0,
                    .info.notFreeAhandle = 1,
                    .info.refId = 0,
                    .info.noResp = 0,
                    .info.handle = 0};
  SRpcMsg rpcRsp = {0};

  SEpSet epSet = {0};
  int8_t epUpdated = 0;
  (void)dmGetMnodeEpSet(pMgmt->pData, &epSet);

  dDebug("send config req to mnode, configSeq:%d, begin to send rpc msg", pMgmt->statusSeq);
  code = rpcSendRecvWithTimeout(pMgmt->msgCb.statusRpc, &epSet, &rpcMsg, &rpcRsp, &epUpdated, tsStatusSRTimeoutMs);
  if (code != 0) {
    dError("failed to SendRecv config req with timeout %d since %s", tsStatusSRTimeoutMs, tstrerror(code));
    return;
  }
  if (rpcRsp.code != 0) {
    dError("failed to send config req since %s", tstrerror(rpcRsp.code));
    return;
  }
  dmProcessConfigRsp(pMgmt, &rpcRsp);
}

void dmUpdateStatusInfo(SDnodeMgmt *pMgmt) {
  dDebug("begin to get dnode info");
  SDnodeData dnodeData = {0};
  (void)taosThreadRwlockRdlock(&pMgmt->pData->lock);
  dnodeData.dnodeVer = pMgmt->pData->dnodeVer;
  dnodeData.dnodeId = pMgmt->pData->dnodeId;
  dnodeData.clusterId = pMgmt->pData->clusterId;
  dnodeData.rebootTime = pMgmt->pData->rebootTime;
  dnodeData.updateTime = pMgmt->pData->updateTime;
  tstrncpy(dnodeData.machineId, pMgmt->pData->machineId, TSDB_MACHINE_ID_LEN + 1);
  (void)taosThreadRwlockUnlock(&pMgmt->pData->lock);

  dDebug("begin to get vnode loads");
  SMonVloadInfo vinfo = {0};
  (*pMgmt->getVnodeLoadsFp)(&vinfo);  // dmGetVnodeLoads

  dDebug("begin to get mnode loads");
  SMonMloadInfo minfo = {0};
  (*pMgmt->getMnodeLoadsFp)(&minfo);  // dmGetMnodeLoads

  dDebug("begin to lock status info");
  if (taosThreadMutexLock(&pMgmt->pData->statusInfolock) != 0) {
    dError("failed to lock status info lock");
    return;
  }
  tsDnodeData.dnodeVer = dnodeData.dnodeVer;
  tsDnodeData.dnodeId = dnodeData.dnodeId;
  tsDnodeData.clusterId = dnodeData.clusterId;
  tsDnodeData.rebootTime = dnodeData.rebootTime;
  tsDnodeData.updateTime = dnodeData.updateTime;
  tstrncpy(tsDnodeData.machineId, dnodeData.machineId, TSDB_MACHINE_ID_LEN + 1);

  if (tsVinfo.pVloads == NULL) {
    tsVinfo.pVloads = vinfo.pVloads;
    vinfo.pVloads = NULL;
  } else {
    taosArrayDestroy(vinfo.pVloads);
    vinfo.pVloads = NULL;
  }

  tsMLoad = minfo.load;

  if (taosThreadMutexUnlock(&pMgmt->pData->statusInfolock) != 0) {
    dError("failed to unlock status info lock");
    return;
  }
}

void dmSendNotifyReq(SDnodeMgmt *pMgmt, SNotifyReq *pReq) {
  int32_t contLen = tSerializeSNotifyReq(NULL, 0, pReq);
  if (contLen < 0) {
    dError("failed to serialize notify req since %s", tstrerror(contLen));
    return;
  }
  void *pHead = rpcMallocCont(contLen);
  contLen = tSerializeSNotifyReq(pHead, contLen, pReq);
  if (contLen < 0) {
    rpcFreeCont(pHead);
    dError("failed to serialize notify req since %s", tstrerror(contLen));
    return;
  }

  SRpcMsg rpcMsg = {.pCont = pHead,
                    .contLen = contLen,
                    .msgType = TDMT_MND_NOTIFY,
                    .info.ahandle = 0,
                    .info.notFreeAhandle = 1,
                    .info.refId = 0,
                    .info.noResp = 1,
                    .info.handle = 0};

  SEpSet epSet = {0};
  dmGetMnodeEpSet(pMgmt->pData, &epSet);
  if (rpcSendRequest(pMgmt->msgCb.clientRpc, &epSet, &rpcMsg, NULL) != 0) {
    dError("failed to send notify req");
  }
}

int32_t dmProcessAuthRsp(SDnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  dError("auth rsp is received, but not supported yet");
  return 0;
}

int32_t dmProcessGrantRsp(SDnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  dError("grant rsp is received, but not supported yet");
  return 0;
}

int32_t dmProcessConfigReq(SDnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  int32_t       code = 0;
  SDCfgDnodeReq cfgReq = {0};
  SConfig      *pCfg = taosGetCfg();
  SConfigItem  *pItem = NULL;

  if (tDeserializeSDCfgDnodeReq(pMsg->pCont, pMsg->contLen, &cfgReq) != 0) {
    return TSDB_CODE_INVALID_MSG;
  }
  if (strcasecmp(cfgReq.config, "dataDir") == 0) {
    return taosUpdateTfsItemDisable(pCfg, cfgReq.value, pMgmt->pTfs);
  }

  dInfo("start to config, option:%s, value:%s", cfgReq.config, cfgReq.value);

  code = cfgGetAndSetItem(pCfg, &pItem, cfgReq.config, cfgReq.value, CFG_STYPE_ALTER_SERVER_CMD, true);
  if (code != 0) {
    if (strncasecmp(cfgReq.config, "resetlog", strlen("resetlog")) == 0) {
      TAOS_CHECK_RETURN(taosCfgDynamicOptions(pCfg, cfgReq.config, true));
      return TSDB_CODE_SUCCESS;
    } else {
      return code;
    }
  }
  if (pItem == NULL) {
    return TSDB_CODE_CFG_NOT_FOUND;
  }

  if (taosStrncasecmp(cfgReq.config, "syncTimeout", 128) == 0) {
    char value[10] = {0};
    if (sscanf(cfgReq.value, "%d", &tsSyncTimeout) != 1) {
      tsSyncTimeout = 0;
    }

    if (tsSyncTimeout > 0) {
      SConfigItem *pItemTmp = NULL;
      char         tmp[10] = {0};

      sprintf(tmp, "%d", tsSyncTimeout);
      TAOS_CHECK_RETURN(
          cfgGetAndSetItem(pCfg, &pItemTmp, "arbSetAssignedTimeoutMs", tmp, CFG_STYPE_ALTER_SERVER_CMD, true));
      if (pItemTmp == NULL) {
        return TSDB_CODE_CFG_NOT_FOUND;
      }

      sprintf(tmp, "%d", tsSyncTimeout / 4);
      TAOS_CHECK_RETURN(
          cfgGetAndSetItem(pCfg, &pItemTmp, "arbHeartBeatIntervalMs", tmp, CFG_STYPE_ALTER_SERVER_CMD, true));
      if (pItemTmp == NULL) {
        return TSDB_CODE_CFG_NOT_FOUND;
      }
      TAOS_CHECK_RETURN(
          cfgGetAndSetItem(pCfg, &pItemTmp, "arbCheckSyncIntervalMs", tmp, CFG_STYPE_ALTER_SERVER_CMD, true));
      if (pItemTmp == NULL) {
        return TSDB_CODE_CFG_NOT_FOUND;
      }

      sprintf(tmp, "%d", (tsSyncTimeout - tsSyncTimeout / 4) / 2);
      TAOS_CHECK_RETURN(
          cfgGetAndSetItem(pCfg, &pItemTmp, "syncVnodeElectIntervalMs", tmp, CFG_STYPE_ALTER_SERVER_CMD, true));
      if (pItemTmp == NULL) {
        return TSDB_CODE_CFG_NOT_FOUND;
      }
      TAOS_CHECK_RETURN(
          cfgGetAndSetItem(pCfg, &pItemTmp, "syncMnodeElectIntervalMs", tmp, CFG_STYPE_ALTER_SERVER_CMD, true));
      if (pItemTmp == NULL) {
        return TSDB_CODE_CFG_NOT_FOUND;
      }
      TAOS_CHECK_RETURN(cfgGetAndSetItem(pCfg, &pItemTmp, "statusTimeoutMs", tmp, CFG_STYPE_ALTER_SERVER_CMD, true));
      if (pItemTmp == NULL) {
        return TSDB_CODE_CFG_NOT_FOUND;
      }

      sprintf(tmp, "%d", (tsSyncTimeout - tsSyncTimeout / 4) / 4);
      TAOS_CHECK_RETURN(cfgGetAndSetItem(pCfg, &pItemTmp, "statusSRTimeoutMs", tmp, CFG_STYPE_ALTER_SERVER_CMD, true));
      if (pItemTmp == NULL) {
        return TSDB_CODE_CFG_NOT_FOUND;
      }

      sprintf(tmp, "%d", (tsSyncTimeout - tsSyncTimeout / 4) / 8);
      TAOS_CHECK_RETURN(
          cfgGetAndSetItem(pCfg, &pItemTmp, "syncVnodeHeartbeatIntervalMs", tmp, CFG_STYPE_ALTER_SERVER_CMD, true));
      if (pItemTmp == NULL) {
        return TSDB_CODE_CFG_NOT_FOUND;
      }
      TAOS_CHECK_RETURN(
          cfgGetAndSetItem(pCfg, &pItemTmp, "syncMnodeHeartbeatIntervalMs", tmp, CFG_STYPE_ALTER_SERVER_CMD, true));
      if (pItemTmp == NULL) {
        return TSDB_CODE_CFG_NOT_FOUND;
      }
      TAOS_CHECK_RETURN(cfgGetAndSetItem(pCfg, &pItemTmp, "statusIntervalMs", tmp, CFG_STYPE_ALTER_SERVER_CMD, true));
      if (pItemTmp == NULL) {
        return TSDB_CODE_CFG_NOT_FOUND;
      }

      dInfo("change syncTimeout, GetAndSetItem, option:%s, value:%s, tsSyncTimeout:%d", cfgReq.config, cfgReq.value,
            tsSyncTimeout);
    }
  }

  if (!isConifgItemLazyMode(pItem)) {
    TAOS_CHECK_RETURN(taosCfgDynamicOptions(pCfg, cfgReq.config, true));

    if (taosStrncasecmp(cfgReq.config, "syncTimeout", 128) == 0) {
      TAOS_CHECK_RETURN(taosCfgDynamicOptions(pCfg, "arbSetAssignedTimeoutMs", true));
      TAOS_CHECK_RETURN(taosCfgDynamicOptions(pCfg, "arbHeartBeatIntervalMs", true));
      TAOS_CHECK_RETURN(taosCfgDynamicOptions(pCfg, "arbCheckSyncIntervalMs", true));

      TAOS_CHECK_RETURN(taosCfgDynamicOptions(pCfg, "syncVnodeElectIntervalMs", true));
      TAOS_CHECK_RETURN(taosCfgDynamicOptions(pCfg, "syncMnodeElectIntervalMs", true));
      TAOS_CHECK_RETURN(taosCfgDynamicOptions(pCfg, "syncVnodeHeartbeatIntervalMs", true));
      TAOS_CHECK_RETURN(taosCfgDynamicOptions(pCfg, "syncMnodeHeartbeatIntervalMs", true));

      TAOS_CHECK_RETURN(taosCfgDynamicOptions(pCfg, "statusTimeoutMs", true));
      TAOS_CHECK_RETURN(taosCfgDynamicOptions(pCfg, "statusSRTimeoutMs", true));
      TAOS_CHECK_RETURN(taosCfgDynamicOptions(pCfg, "statusIntervalMs", true));

      dInfo("change syncTimeout, DynamicOptions, option:%s, value:%s, tsSyncTimeout:%d", cfgReq.config, cfgReq.value,
            tsSyncTimeout);
    }
  }

  if (pItem->category == CFG_CATEGORY_GLOBAL) {
    code = taosPersistGlobalConfig(taosGetGlobalCfg(pCfg), pMgmt->path, tsdmConfigVersion);
    if (code != TSDB_CODE_SUCCESS) {
      dError("failed to persist global config since %s", tstrerror(code));
    }
  } else {
    code = taosPersistLocalConfig(pMgmt->path);
    if (code != TSDB_CODE_SUCCESS) {
      dError("failed to persist local config since %s", tstrerror(code));
    }
  }

  if (taosStrncasecmp(cfgReq.config, "syncTimeout", 128) == 0) {
    dInfo("finished change syncTimeout, option:%s, value:%s", cfgReq.config, cfgReq.value);

    (*pMgmt->setMnodeSyncTimeoutFp)();
    (*pMgmt->setVnodeSyncTimeoutFp)();
  }

  if (taosStrncasecmp(cfgReq.config, "syncVnodeElectIntervalMs", 128) == 0 ||
      taosStrncasecmp(cfgReq.config, "syncVnodeHeartbeatIntervalMs", 128) == 0) {
    (*pMgmt->setVnodeSyncTimeoutFp)();
  }

  if (taosStrncasecmp(cfgReq.config, "syncMnodeElectIntervalMs", 128) == 0 ||
      taosStrncasecmp(cfgReq.config, "syncMnodeHeartbeatIntervalMs", 128) == 0) {
    (*pMgmt->setMnodeSyncTimeoutFp)();
  }

  if (cfgReq.version > 0) {
    tsdmConfigVersion = cfgReq.version;
  }
  return code;
}

int32_t dmProcessCreateEncryptKeyReq(SDnodeMgmt *pMgmt, SRpcMsg *pMsg) {
#ifdef TD_ENTERPRISE
  int32_t       code = 0;
  SDCfgDnodeReq cfgReq = {0};
  if (tDeserializeSDCfgDnodeReq(pMsg->pCont, pMsg->contLen, &cfgReq) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  code = dmUpdateEncryptKey(cfgReq.value, true);
  if (code == 0) {
    tsEncryptionKeyChksum = taosCalcChecksum(0, cfgReq.value, strlen(cfgReq.value));
    tsEncryptionKeyStat = ENCRYPT_KEY_STAT_LOADED;
    tstrncpy(tsEncryptKey, cfgReq.value, ENCRYPT_KEY_LEN + 1);
  }

_exit:
  pMsg->code = code;
  pMsg->info.rsp = NULL;
  pMsg->info.rspLen = 0;
  return code;
#else
  return 0;
#endif
}

#if defined(TD_ENTERPRISE) && defined(TD_HAS_TAOSK)
// Verification plaintext used to validate encryption keys
#define KEY_VERIFY_PLAINTEXT "TDengine_Encryption_Key_Verification_v1.0"

// Save key verification file with encrypted plaintext for each key
static int32_t dmSaveKeyVerification(const char *svrKey, const char *dbKey, const char *cfgKey, const char *metaKey,
                                     const char *dataKey, int32_t algorithm, int32_t cfgAlgorithm,
                                     int32_t metaAlgorithm) {
  char    verifyFile[PATH_MAX] = {0};
  int32_t nBytes = snprintf(verifyFile, sizeof(verifyFile), "%s%sdnode%sconfig%skey_verify.dat", tsDataDir, TD_DIRSEP,
                            TD_DIRSEP, TD_DIRSEP);
  if (nBytes <= 0 || nBytes >= sizeof(verifyFile)) {
    dError("failed to build key verification file path");
    return TSDB_CODE_OUT_OF_BUFFER;
  }

  int32_t     code = 0;
  const char *plaintext = KEY_VERIFY_PLAINTEXT;
  int32_t     plaintextLen = strlen(plaintext);

  // Array of keys and their algorithms
  const char *keys[] = {svrKey, dbKey, cfgKey, metaKey, dataKey};
  int32_t     algorithms[] = {algorithm, algorithm, cfgAlgorithm, metaAlgorithm, algorithm};
  const char *keyNames[] = {"SVR_KEY", "DB_KEY", "CFG_KEY", "META_KEY", "DATA_KEY"};

  // Calculate total buffer size
  int32_t encryptedLen = ((plaintextLen + 15) / 16) * 16;                 // Padded length for CBC
  int32_t headerSize = sizeof(uint32_t) + sizeof(uint16_t);               // magic + version
  int32_t perKeySize = sizeof(int32_t) + sizeof(int32_t) + encryptedLen;  // algo + len + encrypted data
  int32_t totalSize = headerSize + perKeySize * 5;

  // Allocate buffer for all data
  char *buffer = taosMemoryMalloc(totalSize);
  if (buffer == NULL) {
    dError("failed to allocate memory for key verification buffer");
    return terrno;
  }

  char *ptr = buffer;

  // Write magic number and version to buffer
  uint32_t magic = 0x544B5659;  // "TKVY" in hex
  uint16_t version = 1;
  memcpy(ptr, &magic, sizeof(magic));
  ptr += sizeof(magic);
  memcpy(ptr, &version, sizeof(version));
  ptr += sizeof(version);

  // Encrypt all keys and write to buffer
  char paddedPlaintext[512] = {0};
  memcpy(paddedPlaintext, plaintext, plaintextLen);

  for (int i = 0; i < 5; i++) {
    char encrypted[512] = {0};

    // Encrypt the verification plaintext with this key using CBC
    SCryptOpts opts = {0};
    opts.len = encryptedLen;
    opts.source = paddedPlaintext;
    opts.result = encrypted;
    opts.unitLen = 16;
    opts.pOsslAlgrName =
        (algorithms[i] == TSDB_ENCRYPT_ALGO_SM4) ? TSDB_ENCRYPT_ALGO_SM4_STR : TSDB_ENCRYPT_ALGO_NONE_STR;
    tstrncpy(opts.key, keys[i], sizeof(opts.key));

    int32_t count = CBC_Encrypt(&opts);
    if (count != opts.len) {
      code = terrno ? terrno : TSDB_CODE_FAILED;
      dError("failed to encrypt verification for %s, count=%d, expected=%d, since %s", keyNames[i], count, opts.len,
             tstrerror(code));
      taosMemoryFree(buffer);
      return code;
    }

    // Write to buffer: algorithm + encrypted length + encrypted data
    memcpy(ptr, &algorithms[i], sizeof(int32_t));
    ptr += sizeof(int32_t);
    memcpy(ptr, &encryptedLen, sizeof(int32_t));
    ptr += sizeof(int32_t);
    memcpy(ptr, encrypted, encryptedLen);
    ptr += encryptedLen;

    dDebug("prepared verification for %s: algorithm=%d, encLen=%d", keyNames[i], algorithms[i], encryptedLen);
  }

  // Write all data to file in one operation
  TdFilePtr pFile = taosOpenFile(verifyFile, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pFile == NULL) {
    dError("failed to create key verification file:%s, errno:%d", verifyFile, errno);
    taosMemoryFree(buffer);
    return TSDB_CODE_FILE_CORRUPTED;
  }

  int64_t written = taosWriteFile(pFile, buffer, totalSize);
  (void)taosCloseFile(&pFile);
  taosMemoryFree(buffer);

  if (written != totalSize) {
    dError("failed to write key verification file, written=%" PRId64 ", expected=%d", written, totalSize);
    return TSDB_CODE_FILE_CORRUPTED;
  }

  dInfo("successfully saved key verification file:%s, size=%d", verifyFile, totalSize);
  return 0;
}

// Verify all encryption keys by decrypting and comparing with original plaintext
static int32_t dmVerifyEncryptionKeys(const char *svrKey, const char *dbKey, const char *cfgKey, const char *metaKey,
                                      const char *dataKey, int32_t algorithm, int32_t cfgAlgorithm,
                                      int32_t metaAlgorithm) {
  char    verifyFile[PATH_MAX] = {0};
  int32_t nBytes = snprintf(verifyFile, sizeof(verifyFile), "%s%sdnode%sconfig%skey_verify.dat", tsDataDir, TD_DIRSEP,
                            TD_DIRSEP, TD_DIRSEP);
  if (nBytes <= 0 || nBytes >= sizeof(verifyFile)) {
    dError("failed to build key verification file path");
    return TSDB_CODE_OUT_OF_BUFFER;
  }

  // Get file size
  int64_t fileSize = 0;
  if (taosStatFile(verifyFile, &fileSize, NULL, NULL) < 0) {
    // File doesn't exist, create it with current keys
    dInfo("key verification file not found, creating new one");
    return dmSaveKeyVerification(svrKey, dbKey, cfgKey, metaKey, dataKey, algorithm, cfgAlgorithm, metaAlgorithm);
  }

  if (fileSize <= 0 || fileSize > 10240) {  // Max 10KB
    dError("invalid key verification file size: %" PRId64, fileSize);
    return TSDB_CODE_FILE_CORRUPTED;
  }

  // Allocate buffer and read entire file
  char *buffer = taosMemoryMalloc(fileSize);
  if (buffer == NULL) {
    dError("failed to allocate memory for key verification buffer");
    return terrno;
  }

  TdFilePtr pFile = taosOpenFile(verifyFile, TD_FILE_READ);
  if (pFile == NULL) {
    dError("failed to open key verification file:%s", verifyFile);
    taosMemoryFree(buffer);
    return TSDB_CODE_FILE_CORRUPTED;
  }

  int64_t bytesRead = taosReadFile(pFile, buffer, fileSize);
  (void)taosCloseFile(&pFile);

  if (bytesRead != fileSize) {
    dError("failed to read key verification file, read=%" PRId64 ", expected=%" PRId64, bytesRead, fileSize);
    taosMemoryFree(buffer);
    return TSDB_CODE_FILE_CORRUPTED;
  }

  int32_t     code = 0;
  const char *plaintext = KEY_VERIFY_PLAINTEXT;
  int32_t     plaintextLen = strlen(plaintext);
  const char *ptr = buffer;

  // Parse and verify header
  uint32_t magic = 0;
  uint16_t version = 0;
  memcpy(&magic, ptr, sizeof(magic));
  ptr += sizeof(magic);
  memcpy(&version, ptr, sizeof(version));
  ptr += sizeof(version);

  if (magic != 0x544B5659) {
    dError("invalid magic number in key verification file: 0x%x", magic);
    taosMemoryFree(buffer);
    return TSDB_CODE_FILE_CORRUPTED;
  }

  // Array of keys and their algorithms
  const char *keys[] = {svrKey, dbKey, cfgKey, metaKey, dataKey};
  int32_t     expectedAlgos[] = {algorithm, algorithm, cfgAlgorithm, metaAlgorithm, algorithm};
  const char *keyNames[] = {"SVR_KEY", "DB_KEY", "CFG_KEY", "META_KEY", "DATA_KEY"};

  // Verify each key from buffer
  for (int i = 0; i < 5; i++) {
    // Check if we have enough data remaining
    if (ptr - buffer + sizeof(int32_t) * 2 > fileSize) {
      dError("unexpected end of file while reading %s metadata", keyNames[i]);
      taosMemoryFree(buffer);
      return TSDB_CODE_FILE_CORRUPTED;
    }

    int32_t savedAlgo = 0;
    int32_t encryptedLen = 0;

    memcpy(&savedAlgo, ptr, sizeof(int32_t));
    ptr += sizeof(int32_t);
    memcpy(&encryptedLen, ptr, sizeof(int32_t));
    ptr += sizeof(int32_t);

    if (encryptedLen <= 0 || encryptedLen > 512) {
      dError("invalid encrypted length %d for %s", encryptedLen, keyNames[i]);
      taosMemoryFree(buffer);
      return TSDB_CODE_FILE_CORRUPTED;
    }

    if (ptr - buffer + encryptedLen > fileSize) {
      dError("unexpected end of file while reading %s encrypted data", keyNames[i]);
      taosMemoryFree(buffer);
      return TSDB_CODE_FILE_CORRUPTED;
    }

    uint8_t encrypted[512] = {0};
    memcpy(encrypted, ptr, encryptedLen);
    ptr += encryptedLen;

    // Decrypt with current key using CBC
    char decrypted[512] = {0};

    SCryptOpts opts = {0};
    opts.len = encryptedLen;
    opts.source = (char *)encrypted;
    opts.result = decrypted;
    opts.unitLen = 16;
    opts.pOsslAlgrName = (savedAlgo == TSDB_ENCRYPT_ALGO_SM4) ? TSDB_ENCRYPT_ALGO_SM4_STR : TSDB_ENCRYPT_ALGO_NONE_STR;
    tstrncpy(opts.key, keys[i], sizeof(opts.key));

    int32_t count = CBC_Decrypt(&opts);
    if (count != opts.len) {
      code = terrno ? terrno : TSDB_CODE_DNODE_INVALID_ENCRYPTKEY;
      dError("failed to decrypt verification for %s, count=%d, expected=%d, since %s - KEY IS INCORRECT", keyNames[i],
             count, opts.len, tstrerror(code));
      taosMemoryFree(buffer);
      return TSDB_CODE_DNODE_INVALID_ENCRYPTKEY;
    }

    // Verify decrypted data matches original plaintext (compare only the plaintext length)
    if (memcmp(decrypted, plaintext, plaintextLen) != 0) {
      dError("%s verification FAILED: decrypted text does not match - KEY IS INCORRECT", keyNames[i]);
      taosMemoryFree(buffer);
      return TSDB_CODE_DNODE_INVALID_ENCRYPTKEY;
    }

    dInfo("%s verification passed (algorithm=%d)", keyNames[i], savedAlgo);
  }

  taosMemoryFree(buffer);
  dInfo("all encryption keys verified successfully");
  return 0;
}

// Public API: Verify and initialize encryption keys at startup
int32_t dmVerifyAndInitEncryptionKeys(void) {
  // Skip verification in dump sdb mode (taosd -s)
  if (tsSkipKeyCheckMode) {
    dInfo("skip encryption key verification in some special check mode");
    return 0;
  }

  // Check if encryption keys are loaded
  if (tsEncryptKeysStatus != TSDB_ENCRYPT_KEY_STAT_LOADED) {
    dDebug("encryption keys not loaded, skipping verification");
    return 0;
  }

  // Get key file paths
  char    masterKeyFile[PATH_MAX] = {0};
  char    derivedKeyFile[PATH_MAX] = {0};
  int32_t nBytes = snprintf(masterKeyFile, sizeof(masterKeyFile), "%s%sdnode%sconfig%smaster.bin", tsDataDir, TD_DIRSEP,
                            TD_DIRSEP, TD_DIRSEP);
  if (nBytes <= 0 || nBytes >= sizeof(masterKeyFile)) {
    dError("failed to build master key file path");
    return TSDB_CODE_OUT_OF_BUFFER;
  }

  nBytes = snprintf(derivedKeyFile, sizeof(derivedKeyFile), "%s%sdnode%sconfig%sderived.bin", tsDataDir, TD_DIRSEP,
                    TD_DIRSEP, TD_DIRSEP);
  if (nBytes <= 0 || nBytes >= sizeof(derivedKeyFile)) {
    dError("failed to build derived key file path");
    return TSDB_CODE_OUT_OF_BUFFER;
  }

  // Load encryption keys
  char    svrKey[ENCRYPT_KEY_LEN + 1] = {0};
  char    dbKey[ENCRYPT_KEY_LEN + 1] = {0};
  char    cfgKey[ENCRYPT_KEY_LEN + 1] = {0};
  char    metaKey[ENCRYPT_KEY_LEN + 1] = {0};
  char    dataKey[ENCRYPT_KEY_LEN + 1] = {0};
  int32_t algorithm = 0;
  int32_t cfgAlgorithm = 0;
  int32_t metaAlgorithm = 0;
  int32_t fileVersion = 0;
  int32_t keyVersion = 0;
  int64_t createTime = 0;
  int64_t svrKeyUpdateTime = 0;
  int64_t dbKeyUpdateTime = 0;

  int32_t code = taoskLoadEncryptKeys(masterKeyFile, derivedKeyFile, svrKey, dbKey, cfgKey, metaKey, dataKey,
                                      &algorithm, &cfgAlgorithm, &metaAlgorithm, &fileVersion, &keyVersion, &createTime,
                                      &svrKeyUpdateTime, &dbKeyUpdateTime);
  if (code != 0) {
    dError("failed to load encryption keys, since %s", tstrerror(code));
    return code;
  }

  // Verify all keys
  code = dmVerifyEncryptionKeys(svrKey, dbKey, cfgKey, metaKey, dataKey, algorithm, cfgAlgorithm, metaAlgorithm);
  if (code != 0) {
    dError("encryption key verification failed, since %s", tstrerror(code));
    return code;
  }

  dInfo("encryption keys verified and initialized successfully");
  return 0;
}
#else
int32_t dmVerifyAndInitEncryptionKeys(void) {
  // Community edition or no TaosK support
  return 0;
}
#endif

#if defined(TD_ENTERPRISE) && defined(TD_HAS_TAOSK)
static int32_t dmUpdateSvrKey(const char *newKey) {
  if (newKey == NULL || newKey[0] == '\0') {
    dError("invalid new SVR_KEY, key is empty");
    return TSDB_CODE_INVALID_PARA;
  }

  char masterKeyFile[PATH_MAX] = {0};
  char derivedKeyFile[PATH_MAX] = {0};

  // Build path to key files
  int32_t nBytes = snprintf(masterKeyFile, sizeof(masterKeyFile), "%s%sdnode%sconfig%smaster.bin", tsDataDir, TD_DIRSEP,
                            TD_DIRSEP, TD_DIRSEP);
  if (nBytes <= 0 || nBytes >= sizeof(masterKeyFile)) {
    dError("failed to build master key file path");
    return TSDB_CODE_OUT_OF_BUFFER;
  }

  nBytes = snprintf(derivedKeyFile, sizeof(derivedKeyFile), "%s%sdnode%sconfig%sderived.bin", tsDataDir, TD_DIRSEP,
                    TD_DIRSEP, TD_DIRSEP);
  if (nBytes <= 0 || nBytes >= sizeof(derivedKeyFile)) {
    dError("failed to build derived key file path");
    return TSDB_CODE_OUT_OF_BUFFER;
  }

  // Load current keys
  char    svrKey[ENCRYPT_KEY_LEN + 1] = {0};
  char    dbKey[ENCRYPT_KEY_LEN + 1] = {0};
  char    cfgKey[ENCRYPT_KEY_LEN + 1] = {0};
  char    metaKey[ENCRYPT_KEY_LEN + 1] = {0};
  char    dataKey[ENCRYPT_KEY_LEN + 1] = {0};
  int32_t algorithm = 0;
  int32_t cfgAlgorithm = 0;
  int32_t metaAlgorithm = 0;
  int32_t fileVersion = 0;
  int32_t keyVersion = 0;
  int64_t createTime = 0;
  int64_t svrKeyUpdateTime = 0;
  int64_t dbKeyUpdateTime = 0;

  int32_t code =
      taoskLoadEncryptKeys(masterKeyFile, derivedKeyFile, svrKey, dbKey, cfgKey, metaKey, dataKey, &algorithm,
                           &cfgAlgorithm, &metaAlgorithm, &fileVersion, &keyVersion, &createTime, 
                           &svrKeyUpdateTime, &dbKeyUpdateTime);
  if (code != 0) {
    dError("failed to load encryption keys, since %s", tstrerror(code));
    return code;
  }

  // Update SVR_KEY
  int64_t now = taosGetTimestampMs();
  int32_t newKeyVersion = keyVersion + 1;

  dInfo("updating SVR_KEY, old version:%d, new version:%d", keyVersion, newKeyVersion);
  tstrncpy(svrKey, newKey, sizeof(svrKey));
  svrKeyUpdateTime = now;

  // Save updated keys (use algorithm for all keys for backward compatibility)
  code = taoskSaveEncryptKeys(masterKeyFile, derivedKeyFile, svrKey, dbKey, cfgKey, metaKey, dataKey, algorithm,
                              algorithm, algorithm, newKeyVersion, createTime, svrKeyUpdateTime, dbKeyUpdateTime);
  if (code != 0) {
    dError("failed to save updated encryption keys, since %s", tstrerror(code));
    return code;
  }

  // Update key verification file with new SVR_KEY
  code = dmSaveKeyVerification(svrKey, dbKey, cfgKey, metaKey, dataKey, algorithm, algorithm, algorithm);
  if (code != 0) {
    dWarn("failed to update key verification file, since %s", tstrerror(code));
    // Don't fail the operation if verification file update fails
  }

  // Update global variables
  tstrncpy(tsSvrKey, svrKey, sizeof(tsSvrKey));
  tstrncpy(tsDbKey, dbKey, sizeof(tsDbKey));
  tstrncpy(tsCfgKey, cfgKey, sizeof(tsCfgKey));
  tstrncpy(tsMetaKey, metaKey, sizeof(tsMetaKey));
  tstrncpy(tsDataKey, dataKey, sizeof(tsDataKey));
  tsEncryptAlgorithmType = algorithm;
  tsEncryptFileVersion = fileVersion;
  tsEncryptKeyVersion = newKeyVersion;
  tsEncryptKeyCreateTime = createTime;
  tsSvrKeyUpdateTime = svrKeyUpdateTime;
  tsDbKeyUpdateTime = dbKeyUpdateTime;

  // Update encryption key status for backward compatibility
  int keyLen = strlen(tsDataKey);
  if (keyLen > ENCRYPT_KEY_LEN) {
    keyLen = ENCRYPT_KEY_LEN;
  }
  memset(tsEncryptKey, 0, ENCRYPT_KEY_LEN + 1);
  memcpy(tsEncryptKey, tsDataKey, keyLen);
  tsEncryptKey[ENCRYPT_KEY_LEN] = '\0';
  tsEncryptionKeyChksum = taosCalcChecksum(0, (const uint8_t *)tsEncryptKey, strlen(tsEncryptKey));

  dInfo("successfully updated SVR_KEY to version:%d", newKeyVersion);
  return 0;
}

static int32_t dmUpdateDbKey(const char *newKey) {
  if (newKey == NULL || newKey[0] == '\0') {
    dError("invalid new DB_KEY, key is empty");
    return TSDB_CODE_INVALID_PARA;
  }

  char masterKeyFile[PATH_MAX] = {0};
  char derivedKeyFile[PATH_MAX] = {0};

  // Build path to key files
  int32_t nBytes = snprintf(masterKeyFile, sizeof(masterKeyFile), "%s%sdnode%sconfig%smaster.bin", tsDataDir, TD_DIRSEP,
                            TD_DIRSEP, TD_DIRSEP);
  if (nBytes <= 0 || nBytes >= sizeof(masterKeyFile)) {
    dError("failed to build master key file path");
    return TSDB_CODE_OUT_OF_BUFFER;
  }

  nBytes = snprintf(derivedKeyFile, sizeof(derivedKeyFile), "%s%sdnode%sconfig%sderived.bin", tsDataDir, TD_DIRSEP,
                    TD_DIRSEP, TD_DIRSEP);
  if (nBytes <= 0 || nBytes >= sizeof(derivedKeyFile)) {
    dError("failed to build derived key file path");
    return TSDB_CODE_OUT_OF_BUFFER;
  }

  // Load current keys
  char    svrKey[ENCRYPT_KEY_LEN + 1] = {0};
  char    dbKey[ENCRYPT_KEY_LEN + 1] = {0};
  char    cfgKey[ENCRYPT_KEY_LEN + 1] = {0};
  char    metaKey[ENCRYPT_KEY_LEN + 1] = {0};
  char    dataKey[ENCRYPT_KEY_LEN + 1] = {0};
  int32_t algorithm = 0;
  int32_t cfgAlgorithm = 0;
  int32_t metaAlgorithm = 0;
  int32_t fileVersion = 0;
  int32_t keyVersion = 0;
  int64_t createTime = 0;
  int64_t svrKeyUpdateTime = 0;
  int64_t dbKeyUpdateTime = 0;

  int32_t code =
      taoskLoadEncryptKeys(masterKeyFile, derivedKeyFile, svrKey, dbKey, cfgKey, metaKey, dataKey, &algorithm,
                           &cfgAlgorithm, &metaAlgorithm, &fileVersion, &keyVersion, &createTime, 
                           &svrKeyUpdateTime, &dbKeyUpdateTime);
  if (code != 0) {
    dError("failed to load encryption keys, since %s", tstrerror(code));
    return code;
  }

  // Update DB_KEY
  int64_t now = taosGetTimestampMs();
  int32_t newKeyVersion = keyVersion + 1;

  dInfo("updating DB_KEY, old version:%d, new version:%d", keyVersion, newKeyVersion);
  tstrncpy(dbKey, newKey, sizeof(dbKey));
  dbKeyUpdateTime = now;

  // Save updated keys (use algorithm for all keys for backward compatibility)
  code = taoskSaveEncryptKeys(masterKeyFile, derivedKeyFile, svrKey, dbKey, cfgKey, metaKey, dataKey, algorithm,
                              algorithm, algorithm, newKeyVersion, createTime, svrKeyUpdateTime, dbKeyUpdateTime);
  if (code != 0) {
    dError("failed to save updated encryption keys, since %s", tstrerror(code));
    return code;
  }

  // Update key verification file with new DB_KEY
  code = dmSaveKeyVerification(svrKey, dbKey, cfgKey, metaKey, dataKey, algorithm, algorithm, algorithm);
  if (code != 0) {
    dWarn("failed to update key verification file, since %s", tstrerror(code));
    // Don't fail the operation if verification file update fails
  }

  // Update global variables
  tstrncpy(tsSvrKey, svrKey, sizeof(tsSvrKey));
  tstrncpy(tsDbKey, dbKey, sizeof(tsDbKey));
  tstrncpy(tsCfgKey, cfgKey, sizeof(tsCfgKey));
  tstrncpy(tsMetaKey, metaKey, sizeof(tsMetaKey));
  tstrncpy(tsDataKey, dataKey, sizeof(tsDataKey));
  tsEncryptAlgorithmType = algorithm;
  tsEncryptFileVersion = fileVersion;
  tsEncryptKeyVersion = newKeyVersion;
  tsEncryptKeyCreateTime = createTime;
  tsSvrKeyUpdateTime = svrKeyUpdateTime;
  tsDbKeyUpdateTime = dbKeyUpdateTime;

  // Update encryption key status for backward compatibility
  int keyLen = strlen(tsDataKey);
  if (keyLen > ENCRYPT_KEY_LEN) {
    keyLen = ENCRYPT_KEY_LEN;
  }
  memset(tsEncryptKey, 0, ENCRYPT_KEY_LEN + 1);
  memcpy(tsEncryptKey, tsDataKey, keyLen);
  tsEncryptKey[ENCRYPT_KEY_LEN] = '\0';
  tsEncryptionKeyChksum = taosCalcChecksum(0, (const uint8_t *)tsEncryptKey, strlen(tsEncryptKey));

  dInfo("successfully updated DB_KEY to version:%d", newKeyVersion);
  return 0;
}
#endif

int32_t dmProcessAlterEncryptKeyReq(SDnodeMgmt *pMgmt, SRpcMsg *pMsg) {
#if defined(TD_ENTERPRISE) && defined(TD_HAS_TAOSK)
  int32_t              code = 0;
  SMAlterEncryptKeyReq alterKeyReq = {0};
  if (tDeserializeSMAlterEncryptKeyReq(pMsg->pCont, pMsg->contLen, &alterKeyReq) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    dError("failed to deserialize alter encrypt key req, since %s", tstrerror(code));
    goto _exit;
  }

  dInfo("received alter encrypt key req, keyType:%d", alterKeyReq.keyType);

  // Update the specified key (svr_key or db_key)
  if (alterKeyReq.keyType == 0) {
    // Update SVR_KEY
    code = dmUpdateSvrKey(alterKeyReq.newKey);
    if (code == 0) {
      dInfo("successfully updated SVR_KEY");
    } else {
      dError("failed to update SVR_KEY, since %s", tstrerror(code));
    }
  } else if (alterKeyReq.keyType == 1) {
    // Update DB_KEY
    code = dmUpdateDbKey(alterKeyReq.newKey);
    if (code == 0) {
      dInfo("successfully updated DB_KEY");
    } else {
      dError("failed to update DB_KEY, since %s", tstrerror(code));
    }
  } else {
    dError("invalid keyType:%d, must be 0 (SVR_KEY) or 1 (DB_KEY)", alterKeyReq.keyType);
    code = TSDB_CODE_INVALID_PARA;
  }

_exit:
  tFreeSMAlterEncryptKeyReq(&alterKeyReq);
  pMsg->code = code;
  pMsg->info.rsp = NULL;
  pMsg->info.rspLen = 0;
  return code;
#else
  dError("encryption key management is only available in enterprise edition");
  pMsg->code = TSDB_CODE_OPS_NOT_SUPPORT;
  pMsg->info.rsp = NULL;
  pMsg->info.rspLen = 0;
  return TSDB_CODE_OPS_NOT_SUPPORT;
#endif
}

int32_t dmProcessReloadTlsConfig(SDnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  SMsgCb *msgCb = &pMgmt->msgCb;
  void *pTransCli = msgCb->clientRpc;
  void *pTransStatus = msgCb->statusRpc;  
  void *pTransSync = msgCb->syncRpc; 
  void *pTransServer = msgCb->serverRpc;

  code = rpcReloadTlsConfig(pTransServer, TAOS_CONN_SERVER);
  if (code != 0) {
    dError("failed to reload tls config for transport %s since %s", "server", tstrerror(code));
    goto _error;
  }

  code = rpcReloadTlsConfig(pTransCli, TAOS_CONN_CLIENT);
  if (code != 0) {
    dError("failed to reload tls config for transport %s since %s", "cli", tstrerror(code));
    goto _error;
  }

  code = rpcReloadTlsConfig(pTransStatus, TAOS_CONN_CLIENT);
  if (code != 0) {
    dError("failed to reload tls config for transport %s since %s", "status-cli", tstrerror(code));
    goto _error;
  }

  code = rpcReloadTlsConfig(pTransSync, TAOS_CONN_CLIENT);
  if (code != 0) {
    dError("failed to reload tls config for transport %s since %s", "sync-cli", tstrerror(code));
    goto _error;
  }

_error:
  
  return code;
}

static void dmGetServerRunStatus(SDnodeMgmt *pMgmt, SServerStatusRsp *pStatus) {
  pStatus->statusCode = TSDB_SRV_STATUS_SERVICE_OK;
  pStatus->details[0] = 0;

  SMonMloadInfo minfo = {0};
  (*pMgmt->getMnodeLoadsFp)(&minfo);
  if (minfo.isMnode &&
      (minfo.load.syncState == TAOS_SYNC_STATE_ERROR || minfo.load.syncState == TAOS_SYNC_STATE_OFFLINE)) {
    pStatus->statusCode = TSDB_SRV_STATUS_SERVICE_DEGRADED;
    snprintf(pStatus->details, sizeof(pStatus->details), "mnode sync state is %s", syncStr(minfo.load.syncState));
    return;
  }

  SMonVloadInfo vinfo = {0};
  (*pMgmt->getVnodeLoadsFp)(&vinfo);
  for (int32_t i = 0; i < taosArrayGetSize(vinfo.pVloads); ++i) {
    SVnodeLoad *pLoad = taosArrayGet(vinfo.pVloads, i);
    if (pLoad->syncState == TAOS_SYNC_STATE_ERROR || pLoad->syncState == TAOS_SYNC_STATE_OFFLINE) {
      pStatus->statusCode = TSDB_SRV_STATUS_SERVICE_DEGRADED;
      snprintf(pStatus->details, sizeof(pStatus->details), "vnode:%d sync state is %s", pLoad->vgId,
               syncStr(pLoad->syncState));
      break;
    }
  }

  taosArrayDestroy(vinfo.pVloads);
}

int32_t dmProcessServerRunStatus(SDnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  int32_t code = 0;
  dDebug("server run status req is received");
  SServerStatusRsp statusRsp = {0};
  dmGetServerRunStatus(pMgmt, &statusRsp);

  pMsg->info.rsp = NULL;
  pMsg->info.rspLen = 0;

  SRpcMsg rspMsg = {.info = pMsg->info};
  int32_t rspLen = tSerializeSServerStatusRsp(NULL, 0, &statusRsp);
  if (rspLen < 0) {
    return TSDB_CODE_OUT_OF_MEMORY;
    // rspMsg.code = TSDB_CODE_OUT_OF_MEMORY;
    // return rspMsg.code;
  }

  void *pRsp = rpcMallocCont(rspLen);
  if (pRsp == NULL) {
    return terrno;
    // rspMsg.code = TSDB_CODE_OUT_OF_MEMORY;
    // return rspMsg.code;
  }

  rspLen = tSerializeSServerStatusRsp(pRsp, rspLen, &statusRsp);
  if (rspLen < 0) {
    return TSDB_CODE_INVALID_MSG;
  }

  pMsg->info.rsp = pRsp;
  pMsg->info.rspLen = rspLen;
  return 0;
}

int32_t dmBuildVariablesBlock(SSDataBlock **ppBlock) {
  int32_t code = 0;

  SSDataBlock *pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  if (pBlock == NULL) {
    return terrno;
  }

  size_t size = 0;

  const SSysTableMeta *pMeta = NULL;
  getInfosDbMeta(&pMeta, &size);

  int32_t index = 0;
  for (int32_t i = 0; i < size; ++i) {
    if (strcmp(pMeta[i].name, TSDB_INS_TABLE_DNODE_VARIABLES) == 0) {
      index = i;
      break;
    }
  }

  pBlock->pDataBlock = taosArrayInit(pMeta[index].colNum, sizeof(SColumnInfoData));
  if (pBlock->pDataBlock == NULL) {
    code = terrno;
    goto _exit;
  }

  for (int32_t i = 0; i < pMeta[index].colNum; ++i) {
    SColumnInfoData colInfoData = {0};
    colInfoData.info.colId = i + 1;
    colInfoData.info.type = pMeta[index].schema[i].type;
    colInfoData.info.bytes = pMeta[index].schema[i].bytes;
    if (taosArrayPush(pBlock->pDataBlock, &colInfoData) == NULL) {
      code = terrno;
      goto _exit;
    }
  }

  pBlock->info.hasVarCol = true;
_exit:
  if (code != 0) {
    blockDataDestroy(pBlock);
  } else {
    *ppBlock = pBlock;
  }
  return code;
}

int32_t dmAppendVariablesToBlock(SSDataBlock *pBlock, int32_t dnodeId) {
  int32_t code = dumpConfToDataBlock(pBlock, 1, NULL);
  if (code != 0) {
    return code;
  }

  SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, 0);
  if (pColInfo == NULL) {
    return TSDB_CODE_OUT_OF_RANGE;
  }

  return colDataSetNItems(pColInfo, 0, (const char *)&dnodeId, pBlock->info.rows, 1, false);
}

int32_t dmProcessRetrieve(SDnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  int32_t           size = 0;
  int32_t           rowsRead = 0;
  int32_t           code = 0;
  SRetrieveTableReq retrieveReq = {0};
  if (tDeserializeSRetrieveTableReq(pMsg->pCont, pMsg->contLen, &retrieveReq) != 0) {
    return TSDB_CODE_INVALID_MSG;
  }
  dInfo("retrieve table:%s, user:%s, compactId:%" PRId64, retrieveReq.tb, retrieveReq.user, retrieveReq.compactId);
#if 0
  if (strcmp(retrieveReq.user, TSDB_DEFAULT_USER) != 0) {
    code = TSDB_CODE_MND_NO_RIGHTS;
    return code;
  }
#endif
  if (strcasecmp(retrieveReq.tb, TSDB_INS_TABLE_DNODE_VARIABLES)) {
    return TSDB_CODE_INVALID_MSG;
  }

  SSDataBlock *pBlock = NULL;
  if ((code = dmBuildVariablesBlock(&pBlock)) != 0) {
    return code;
  }

  code = dmAppendVariablesToBlock(pBlock, pMgmt->pData->dnodeId);
  if (code != 0) {
    blockDataDestroy(pBlock);
    return code;
  }

  size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  size_t dataEncodeBufSize = blockGetEncodeSize(pBlock);
  size = sizeof(SRetrieveMetaTableRsp) + sizeof(int32_t) + sizeof(SSysTableSchema) * numOfCols + dataEncodeBufSize;

  SRetrieveMetaTableRsp *pRsp = rpcMallocCont(size);
  if (pRsp == NULL) {
    code = terrno;
    dError("failed to retrieve data since %s", tstrerror(code));
    blockDataDestroy(pBlock);
    return code;
  }

  char *pStart = pRsp->data;
  *(int32_t *)pStart = htonl(numOfCols);
  pStart += sizeof(int32_t);  // number of columns

  for (int32_t i = 0; i < numOfCols; ++i) {
    SSysTableSchema *pSchema = (SSysTableSchema *)pStart;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, i);

    pSchema->bytes = htonl(pColInfo->info.bytes);
    pSchema->colId = htons(pColInfo->info.colId);
    pSchema->type = pColInfo->info.type;

    pStart += sizeof(SSysTableSchema);
  }

  int32_t len = blockEncode(pBlock, pStart, dataEncodeBufSize, numOfCols);
  if (len < 0) {
    dError("failed to retrieve data since %s", tstrerror(code));
    blockDataDestroy(pBlock);
    rpcFreeCont(pRsp);
    return terrno;
  }

  pRsp->numOfRows = htonl(pBlock->info.rows);
  pRsp->precision = TSDB_TIME_PRECISION_MILLI;  // millisecond time precision
  pRsp->completed = 1;
  pMsg->info.rsp = pRsp;
  pMsg->info.rspLen = size;
  dDebug("dnode variables retrieve completed");

  blockDataDestroy(pBlock);
  return TSDB_CODE_SUCCESS;
}

int32_t dmProcessStreamHbRsp(SDnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SMStreamHbRspMsg rsp = {0};
  int32_t          code = 0;
  SDecoder         decoder;
  char*            msg = POINTER_SHIFT(pMsg->pCont, sizeof(SStreamMsgGrpHeader));
  int32_t          len = pMsg->contLen - sizeof(SStreamMsgGrpHeader);
  int64_t          currTs = taosGetTimestampMs();

  if (pMsg->code) {
    return streamHbHandleRspErr(pMsg->code, currTs);
  }

  tDecoderInit(&decoder, (uint8_t*)msg, len);
  code = tDecodeStreamHbRsp(&decoder, &rsp);
  if (code < 0) {
    code = TSDB_CODE_INVALID_MSG;
    tDeepFreeSMStreamHbRspMsg(&rsp);
    tDecoderClear(&decoder);
    dError("fail to decode stream hb rsp msg, error:%s", tstrerror(code));
    return streamHbHandleRspErr(code, currTs);
  }

  tDecoderClear(&decoder);

  return streamHbProcessRspMsg(&rsp);
}


SArray *dmGetMsgHandles() {
  int32_t code = -1;
  SArray *pArray = taosArrayInit(16, sizeof(SMgmtHandle));
  if (pArray == NULL) {
    return NULL;
  }

  // Requests handled by DNODE
  if (dmSetMgmtHandle(pArray, TDMT_DND_CREATE_MNODE, dmPutNodeMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_DROP_MNODE, dmPutNodeMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_CREATE_QNODE, dmPutNodeMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_DROP_QNODE, dmPutNodeMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_CREATE_SNODE, dmPutNodeMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_ALTER_SNODE, dmPutNodeMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_DROP_SNODE, dmPutNodeMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_CREATE_BNODE, dmPutNodeMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_DROP_BNODE, dmPutNodeMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_CONFIG_DNODE, dmPutNodeMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_SERVER_STATUS, dmPutNodeMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_SYSTABLE_RETRIEVE, dmPutNodeMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_ALTER_MNODE_TYPE, dmPutNodeMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_CREATE_ENCRYPT_KEY, dmPutNodeMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_ALTER_ENCRYPT_KEY, dmPutNodeMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_STREAM_HEARTBEAT_RSP, dmPutMsgToStreamMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_RELOAD_DNODE_TLS, dmPutNodeMsgToMgmtQueue, 0) == NULL) goto _OVER;

  // Requests handled by MNODE
  if (dmSetMgmtHandle(pArray, TDMT_MND_GRANT, dmPutNodeMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_GRANT_NOTIFY, dmPutNodeMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_AUTH_RSP, dmPutNodeMsgToMgmtQueue, 0) == NULL) goto _OVER;

  code = 0;

_OVER:
  if (code != 0) {
    taosArrayDestroy(pArray);
    return NULL;
  } else {
    return pArray;
  }
}
