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
#include "tchecksum.h"
#include "tanal.h"

extern SConfig *tsCfg;

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
      dInfo("failed to set local info, dnodeId:%d clusterId:%" PRId64 " reason:%s", pCfg->dnodeId, pCfg->clusterId,
            tstrerror(code));
    }
    (void)taosThreadRwlockUnlock(&pMgmt->pData->lock);
  }
}

static void dmMayShouldUpdateIpWhiteList(SDnodeMgmt *pMgmt, int64_t ver) {
  int32_t code = 0;
  dDebug("ip-white-list on dnode ver: %" PRId64 ", status ver: %" PRId64 "", pMgmt->pData->ipWhiteVer, ver);
  if (pMgmt->pData->ipWhiteVer == ver) {
    if (ver == 0) {
      dDebug("disable ip-white-list on dnode ver: %" PRId64 ", status ver: %" PRId64 "", pMgmt->pData->ipWhiteVer, ver);
      (void)rpcSetIpWhite(pMgmt->msgCb.serverRpc, NULL);
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
                    .msgType = TDMT_MND_RETRIEVE_IP_WHITE,
                    .info.ahandle = (void *)0x9527,
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

static void dmMayShouldUpdateAnalFunc(SDnodeMgmt *pMgmt, int64_t newVer) {
  int32_t code = 0;
  int64_t oldVer = taosFuncGetVersion();
  if (oldVer == newVer) return;
  dDebug("dnode analysis func ver: %" PRId64 ", status ver: %" PRId64 "", oldVer, newVer);

  SRetrieveAnalFuncReq req = {.dnodeId = pMgmt->pData->dnodeId, .analFuncVer = oldVer};
  int32_t              contLen = tSerializeRetrieveAnalFuncReq(NULL, 0, &req);
  if (contLen < 0) {
    dError("failed to serialize analysis func ver request since: %s", tstrerror(contLen));
    return;
  }

  void *pHead = rpcMallocCont(contLen);
  contLen = tSerializeRetrieveAnalFuncReq(pHead, contLen, &req);
  if (contLen < 0) {
    rpcFreeCont(pHead);
    dError("failed to serialize analysis func ver request since:%s", tstrerror(contLen));
    return;
  }

  SRpcMsg rpcMsg = {
      .pCont = pHead,
      .contLen = contLen,
      .msgType = TDMT_MND_RETRIEVE_ANAL_FUNC,
      .info.ahandle = (void *)0x9527,
      .info.refId = 0,
      .info.noResp = 0,
      .info.handle = 0,
  };
  SEpSet  epset = {0};

  (void)dmGetMnodeEpSet(pMgmt->pData, &epset);

  code = rpcSendRequest(pMgmt->msgCb.clientRpc, &epset, &rpcMsg, NULL);
  if (code != 0) {
    dError("failed to send retrieve analysis func ver request since:%s", tstrerror(code));
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
      (void)dmWriteEps(pMgmt->pData);
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
      dmMayShouldUpdateAnalFunc(pMgmt, statusRsp.analFuncVer);
    }
    tFreeSStatusRsp(&statusRsp);
  }
  rpcFreeCont(pRsp->pCont);
}

void dmSendStatusReq(SDnodeMgmt *pMgmt) {
  int32_t    code = 0;
  SStatusReq req = {0};

  (void)taosThreadRwlockRdlock(&pMgmt->pData->lock);
  req.sver = tsVersion;
  req.dnodeVer = pMgmt->pData->dnodeVer;
  req.dnodeId = pMgmt->pData->dnodeId;
  req.clusterId = pMgmt->pData->clusterId;
  if (req.clusterId == 0) req.dnodeId = 0;
  req.rebootTime = pMgmt->pData->rebootTime;
  req.updateTime = pMgmt->pData->updateTime;
  req.numOfCores = tsNumOfCores;
  req.numOfSupportVnodes = tsNumOfSupportVnodes;
  req.numOfDiskCfg = tsDiskCfgNum;
  req.memTotal = tsTotalMemoryKB * 1024;
  req.memAvail = req.memTotal - tsQueueMemoryAllowed - 16 * 1024 * 1024;
  tstrncpy(req.dnodeEp, tsLocalEp, TSDB_EP_LEN);
  tstrncpy(req.machineId, pMgmt->pData->machineId, TSDB_MACHINE_ID_LEN + 1);

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
  req.clusterCfg.monitorParas.tsSlowLogThresholdTest = tsSlowLogThresholdTest;
  tstrncpy(req.clusterCfg.monitorParas.tsSlowLogExceptDb, tsSlowLogExceptDb, TSDB_DB_NAME_LEN);
  char timestr[32] = "1970-01-01 00:00:00.00";
  (void)taosParseTime(timestr, &req.clusterCfg.checkTime, (int32_t)strlen(timestr), TSDB_TIME_PRECISION_MILLI, 0);
  memcpy(req.clusterCfg.timezone, tsTimezoneStr, TD_TIMEZONE_LEN);
  memcpy(req.clusterCfg.locale, tsLocale, TD_LOCALE_LEN);
  memcpy(req.clusterCfg.charset, tsCharset, TD_LOCALE_LEN);
  (void)taosThreadRwlockUnlock(&pMgmt->pData->lock);

  SMonVloadInfo vinfo = {0};
  (*pMgmt->getVnodeLoadsFp)(&vinfo);
  req.pVloads = vinfo.pVloads;

  SMonMloadInfo minfo = {0};
  (*pMgmt->getMnodeLoadsFp)(&minfo);
  req.mload = minfo.load;

  (*pMgmt->getQnodeLoadsFp)(&req.qload);

  pMgmt->statusSeq++;
  req.statusSeq = pMgmt->statusSeq;
  req.ipWhiteVer = pMgmt->pData->ipWhiteVer;
  req.analFuncVer = taosFuncGetVersion();

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
                    .info.ahandle = (void *)0x9527,
                    .info.refId = 0,
                    .info.noResp = 0,
                    .info.handle = 0};
  SRpcMsg rpcRsp = {0};

  dTrace("send status req to mnode, dnodeVer:%" PRId64 " statusSeq:%d", req.dnodeVer, req.statusSeq);

  SEpSet epSet = {0};
  int8_t epUpdated = 0;
  (void)dmGetMnodeEpSet(pMgmt->pData, &epSet);

  code =
      rpcSendRecvWithTimeout(pMgmt->msgCb.statusRpc, &epSet, &rpcMsg, &rpcRsp, &epUpdated, tsStatusInterval * 5 * 1000);
  if (code != 0) {
    dError("failed to send status req since %s", tstrerror(code));
    return;
  }

  if (rpcRsp.code != 0) {
    dmRotateMnodeEpSet(pMgmt->pData);
    char tbuf[512];
    dmEpSetToStr(tbuf, sizeof(tbuf), &epSet);
    dError("failed to send status req since %s, epSet:%s, inUse:%d", tstrerror(rpcRsp.code), tbuf, epSet.inUse);
  } else {
    if (epUpdated == 1) {
      dmSetMnodeEpSet(pMgmt->pData, &epSet);
    }
  }
  dmProcessStatusRsp(pMgmt, &rpcRsp);
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
                    .info.ahandle = (void *)0x9527,
                    .info.refId = 0,
                    .info.noResp = 1,
                    .info.handle = 0};

  SEpSet epSet = {0};
  dmGetMnodeEpSet(pMgmt->pData, &epSet);
  (void)rpcSendRequest(pMgmt->msgCb.clientRpc, &epSet, &rpcMsg, NULL);
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
  if (tDeserializeSDCfgDnodeReq(pMsg->pCont, pMsg->contLen, &cfgReq) != 0) {
    return TSDB_CODE_INVALID_MSG;
  }

  dInfo("start to config, option:%s, value:%s", cfgReq.config, cfgReq.value);

  SConfig *pCfg = taosGetCfg();

  code = cfgSetItem(pCfg, cfgReq.config, cfgReq.value, CFG_STYPE_ALTER_CMD, true);
  if (code != 0) {
    if (strncasecmp(cfgReq.config, "resetlog", strlen("resetlog")) == 0) {
      code = 0;
    } else {
      return code;
    }
  }

  return taosCfgDynamicOptions(pCfg, cfgReq.config, true);
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
  int32_t code = dumpConfToDataBlock(pBlock, 1);
  if (code != 0) {
    return code;
  }

  SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, 0);
  if (pColInfo == NULL) {
    return TSDB_CODE_OUT_OF_RANGE;
  }

  return colDataSetNItems(pColInfo, 0, (const char *)&dnodeId, pBlock->info.rows, false);
}

int32_t dmProcessRetrieve(SDnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  int32_t           size = 0;
  int32_t           rowsRead = 0;
  int32_t           code = 0;
  SRetrieveTableReq retrieveReq = {0};
  if (tDeserializeSRetrieveTableReq(pMsg->pCont, pMsg->contLen, &retrieveReq) != 0) {
    return TSDB_CODE_INVALID_MSG;
  }
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
  size = sizeof(SRetrieveMetaTableRsp) + sizeof(int32_t) + sizeof(SSysTableSchema) * numOfCols +
         blockDataGetSize(pBlock) + blockDataGetSerialMetaSize(numOfCols);

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

  int32_t len = blockEncode(pBlock, pStart, numOfCols);
  if(len < 0) {
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
  if (dmSetMgmtHandle(pArray, TDMT_DND_DROP_SNODE, dmPutNodeMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_CONFIG_DNODE, dmPutNodeMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_SERVER_STATUS, dmPutNodeMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_SYSTABLE_RETRIEVE, dmPutNodeMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_ALTER_MNODE_TYPE, dmPutNodeMsgToMgmtQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_CREATE_ENCRYPT_KEY, dmPutNodeMsgToMgmtQueue, 0) == NULL) goto _OVER;

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
