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
#include "monitor.h"
#include "systable.h"
#include "tanalytics.h"
#include "tchecksum.h"
#include "tutil.h"
#include "stream.h"

extern SConfig *tsCfg;

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

  SRetrieveIpWhiteReq req = {.ipWhiteVer = oldVer};
  int32_t             contLen = tSerializeRetrieveIpWhite(NULL, 0, &req);
  if (contLen < 0) {
    dError("failed to serialize ip white list request since: %s", tstrerror(contLen));
    return;
  }
  void *pHead = rpcMallocCont(contLen);
  contLen = tSerializeRetrieveIpWhite(pHead, contLen, &req);
  if (contLen < 0) {
    rpcFreeCont(pHead);
    dError("failed to serialize ip white list request since:%s", tstrerror(contLen));
    return;
  }

  SRpcMsg rpcMsg = {.pCont = pHead,
                    .contLen = contLen,
                    .msgType = TDMT_MND_RETRIEVE_IP_WHITE_DUAL,
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
      dmMayShouldUpdateIpWhiteList(pMgmt, statusRsp.ipWhiteVer);
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

  dDebug("send status req to mnode, statusSeq:%d, begin to mgnt statusInfolock", pMgmt->statusSeq);
  if (taosThreadMutexLock(&pMgmt->pData->statusInfolock) != 0) {
    dError("failed to lock status info lock");
    return;
  }

  dDebug("send status req to mnode, statusSeq:%d, begin to get dnode info", pMgmt->statusSeq);
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

  dDebug("send status req to mnode, statusSeq:%d, begin to get vnode loads", pMgmt->statusSeq);

  req.pVloads = tsVinfo.pVloads;
  tsVinfo.pVloads = NULL;

  dDebug("send status req to mnode, statusSeq:%d, begin to get mnode loads", pMgmt->statusSeq);
  req.mload = tsMLoad;

  if (taosThreadMutexUnlock(&pMgmt->pData->statusInfolock) != 0) {
    dError("failed to unlock status info lock");
    return;
  }

  dDebug("send status req to mnode, statusSeq:%d, begin to get qnode loads", pMgmt->statusSeq);
  (*pMgmt->getQnodeLoadsFp)(&req.qload);

  req.statusSeq = pMgmt->statusSeq;
  req.ipWhiteVer = pMgmt->pData->ipWhiteVer;
  req.analVer = taosAnalyGetVersion();

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

  dTrace("send status req to mnode, dnodeVer:%" PRId64 " statusSeq:%d", req.dnodeVer, req.statusSeq);

  SEpSet epSet = {0};
  int8_t epUpdated = 0;
  (void)dmGetMnodeEpSet(pMgmt->pData, &epSet);

  dDebug("send status req to mnode, statusSeq:%d, begin to send rpc msg", pMgmt->statusSeq);
  code =
      rpcSendRecvWithTimeout(pMgmt->msgCb.statusRpc, &epSet, &rpcMsg, &rpcRsp, &epUpdated, tsStatusInterval * 5 * 1000);
  if (code != 0) {
    dError("failed to SendRecv status req with timeout %d since %s", tsStatusInterval * 5 * 1000, tstrerror(code));
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

  dDebug("send status req to mnode, statusSeq:%d, begin to send rpc msg", pMgmt->statusSeq);
  code =
      rpcSendRecvWithTimeout(pMgmt->msgCb.statusRpc, &epSet, &rpcMsg, &rpcRsp, &epUpdated, tsStatusInterval * 5 * 1000);
  if (code != 0) {
    dError("failed to SendRecv config req with timeout %d since %s", tsStatusInterval * 5 * 1000, tstrerror(code));
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
  if (!isConifgItemLazyMode(pItem)) {
    TAOS_CHECK_RETURN(taosCfgDynamicOptions(pCfg, cfgReq.config, true));
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
  if (dmSetMgmtHandle(pArray, TDMT_MND_STREAM_HEARTBEAT_RSP, dmPutMsgToStreamMgmtQueue, 0) == NULL) goto _OVER;

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
