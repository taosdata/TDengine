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
#include "dmInt.h"
#include "systable.h"

extern SConfig *tsCfg;

static void dmUpdateDnodeCfg(SDnodeMgmt *pMgmt, SDnodeCfg *pCfg) {
  if (pMgmt->pData->dnodeId == 0 || pMgmt->pData->clusterId == 0) {
    dInfo("set local info, dnodeId:%d clusterId:%" PRId64, pCfg->dnodeId, pCfg->clusterId);
    taosThreadRwlockWrlock(&pMgmt->pData->lock);
    pMgmt->pData->dnodeId = pCfg->dnodeId;
    pMgmt->pData->clusterId = pCfg->clusterId;
    dmWriteEps(pMgmt->pData);
    taosThreadRwlockUnlock(&pMgmt->pData->lock);
  }
}
static void dmMayShouldUpdateIpWhiteList(SDnodeMgmt *pMgmt, int64_t ver) {
  dDebug("ip-white-list on dnode ver: %" PRId64 ", status ver: %" PRId64 "", pMgmt->pData->ipWhiteVer, ver);
  if (pMgmt->pData->ipWhiteVer == ver) {
    if (ver == 0) {
      dDebug("disable ip-white-list on dnode ver: %" PRId64 ", status ver: %" PRId64 "", pMgmt->pData->ipWhiteVer, ver);
      rpcSetIpWhite(pMgmt->msgCb.serverRpc, NULL);
      // pMgmt->ipWhiteVer = ver;
    }
    return;
  }
  int64_t oldVer = pMgmt->pData->ipWhiteVer;
  // pMgmt->ipWhiteVer = ver;

  SRetrieveIpWhiteReq req = {.ipWhiteVer = oldVer};
  int32_t             contLen = tSerializeRetrieveIpWhite(NULL, 0, &req);
  void *              pHead = rpcMallocCont(contLen);
  tSerializeRetrieveIpWhite(pHead, contLen, &req);

  SRpcMsg rpcMsg = {.pCont = pHead,
                    .contLen = contLen,
                    .msgType = TDMT_MND_RETRIEVE_IP_WHITE,
                    .info.ahandle = (void *)0x9527,
                    .info.refId = 0,
                    .info.noResp = 0,
                    .info.handle = 0};
  SEpSet  epset = {0};

  dmGetMnodeEpSet(pMgmt->pData, &epset);

  rpcSendRequest(pMgmt->msgCb.clientRpc, &epset, &rpcMsg, NULL);
}
static void dmProcessStatusRsp(SDnodeMgmt *pMgmt, SRpcMsg *pRsp) {
  const STraceId *trace = &pRsp->info.traceId;
  dGTrace("status rsp received from mnode, statusSeq:%d code:0x%x", pMgmt->statusSeq, pRsp->code);

  if (pRsp->code != 0) {
    if (pRsp->code == TSDB_CODE_MND_DNODE_NOT_EXIST && !pMgmt->pData->dropped && pMgmt->pData->dnodeId > 0) {
      dGInfo("dnode:%d, set to dropped since not exist in mnode, statusSeq:%d", pMgmt->pData->dnodeId,
             pMgmt->statusSeq);
      pMgmt->pData->dropped = 1;
      dmWriteEps(pMgmt->pData);
      dInfo("dnode will exit since it is in the dropped state");
      raise(SIGINT);
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
    }
    tFreeSStatusRsp(&statusRsp);
  }
  rpcFreeCont(pRsp->pCont);
}

void dmEpSetToStr(char *buf, int32_t len, SEpSet *epSet) {
  int32_t n = 0;
  n += snprintf(buf + n, len - n, "%s", "{");
  for (int i = 0; i < epSet->numOfEps; i++) {
    n += snprintf(buf + n, len - n, "%s:%d%s", epSet->eps[i].fqdn, epSet->eps[i].port,
                  (i + 1 < epSet->numOfEps ? ", " : ""));
  }
  n += snprintf(buf + n, len - n, "%s", "}");
}

void dmSendStatusReq(SDnodeMgmt *pMgmt) {
  SStatusReq req = {0};

  taosThreadRwlockRdlock(&pMgmt->pData->lock);
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
  req.memAvail = req.memTotal - tsRpcQueueMemoryAllowed - 16 * 1024 * 1024;
  tstrncpy(req.dnodeEp, tsLocalEp, TSDB_EP_LEN);
  tstrncpy(req.machineId, pMgmt->pData->machineId, TSDB_MACHINE_ID_LEN + 1);

  req.clusterCfg.statusInterval = tsStatusInterval;
  req.clusterCfg.checkTime = 0;
  req.clusterCfg.ttlChangeOnWrite = tsTtlChangeOnWrite;
  req.clusterCfg.enableWhiteList = tsEnableWhiteList ? 1 : 0;
  char timestr[32] = "1970-01-01 00:00:00.00";
  (void)taosParseTime(timestr, &req.clusterCfg.checkTime, (int32_t)strlen(timestr), TSDB_TIME_PRECISION_MILLI, 0);
  memcpy(req.clusterCfg.timezone, tsTimezoneStr, TD_TIMEZONE_LEN);
  memcpy(req.clusterCfg.locale, tsLocale, TD_LOCALE_LEN);
  memcpy(req.clusterCfg.charset, tsCharset, TD_LOCALE_LEN);
  taosThreadRwlockUnlock(&pMgmt->pData->lock);

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

  int32_t contLen = tSerializeSStatusReq(NULL, 0, &req);
  void *  pHead = rpcMallocCont(contLen);
  tSerializeSStatusReq(pHead, contLen, &req);
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
  dmGetMnodeEpSet(pMgmt->pData, &epSet);
  rpcSendRecvWithTimeout(pMgmt->msgCb.statusRpc, &epSet, &rpcMsg, &rpcRsp, &epUpdated, tsStatusInterval * 5 * 1000);
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

void dmSendNotifyReq(SDnodeMgmt *pMgmt) {
  SNotifyReq req = {0};

  taosThreadRwlockRdlock(&pMgmt->pData->lock);
  req.dnodeId = pMgmt->pData->dnodeId;
  taosThreadRwlockUnlock(&pMgmt->pData->lock);

  req.clusterId = pMgmt->pData->clusterId;

  SMonVloadInfo vinfo = {0};
  (*pMgmt->getVnodeLoadsLiteFp)(&vinfo);
  req.pVloads = vinfo.pVloads;

  int32_t contLen = tSerializeSNotifyReq(NULL, 0, &req);
  void *  pHead = rpcMallocCont(contLen);
  tSerializeSNotifyReq(pHead, contLen, &req);
  tFreeSNotifyReq(&req);

  SRpcMsg rpcMsg = {.pCont = pHead,
                    .contLen = contLen,
                    .msgType = TDMT_MND_NOTIFY,
                    .info.ahandle = (void *)0x9527,
                    .info.refId = 0,
                    .info.noResp = 1,
                    .info.handle = 0};

  SEpSet epSet = {0};
  dmGetMnodeEpSet(pMgmt->pData, &epSet);
  rpcSendRequest(pMgmt->msgCb.clientRpc, &epSet, &rpcMsg, NULL);
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
  SDCfgDnodeReq cfgReq = {0};
  if (tDeserializeSDCfgDnodeReq(pMsg->pCont, pMsg->contLen, &cfgReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  dInfo("start to config, option:%s, value:%s", cfgReq.config, cfgReq.value);

  SConfig *pCfg = taosGetCfg();
  cfgSetItem(pCfg, cfgReq.config, cfgReq.value, CFG_STYPE_ALTER_CMD);
  taosCfgDynamicOptions(pCfg, cfgReq.config, true);
  return 0;
}

static void dmGetServerRunStatus(SDnodeMgmt *pMgmt, SServerStatusRsp *pStatus) {
  pStatus->statusCode = TSDB_SRV_STATUS_SERVICE_OK;
  pStatus->details[0] = 0;

  SServerStatusRsp statusRsp = {0};
  SMonMloadInfo    minfo = {0};
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
  dDebug("server run status req is received");
  SServerStatusRsp statusRsp = {0};
  dmGetServerRunStatus(pMgmt, &statusRsp);

  SRpcMsg rspMsg = {.info = pMsg->info};
  int32_t rspLen = tSerializeSServerStatusRsp(NULL, 0, &statusRsp);
  if (rspLen < 0) {
    rspMsg.code = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  void *pRsp = rpcMallocCont(rspLen);
  if (pRsp == NULL) {
    rspMsg.code = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  tSerializeSServerStatusRsp(pRsp, rspLen, &statusRsp);
  pMsg->info.rsp = pRsp;
  pMsg->info.rspLen = rspLen;
  return 0;
}

SSDataBlock *dmBuildVariablesBlock(void) {
  SSDataBlock *        pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  size_t               size = 0;
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

  for (int32_t i = 0; i < pMeta[index].colNum; ++i) {
    SColumnInfoData colInfoData = {0};
    colInfoData.info.colId = i + 1;
    colInfoData.info.type = pMeta[index].schema[i].type;
    colInfoData.info.bytes = pMeta[index].schema[i].bytes;
    taosArrayPush(pBlock->pDataBlock, &colInfoData);
  }

  pBlock->info.hasVarCol = true;

  return pBlock;
}

int32_t dmAppendVariablesToBlock(SSDataBlock *pBlock, int32_t dnodeId) {
  int32_t numOfCfg = taosArrayGetSize(tsCfg->array);
  int32_t numOfRows = 0;
  blockDataEnsureCapacity(pBlock, numOfCfg);

  for (int32_t i = 0, c = 0; i < numOfCfg; ++i, c = 0) {
    SConfigItem *pItem = taosArrayGet(tsCfg->array, i);
    // GRANT_CFG_SKIP;

    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, c++);
    colDataSetVal(pColInfo, i, (const char *)&dnodeId, false);

    char name[TSDB_CONFIG_OPTION_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(name, pItem->name, TSDB_CONFIG_OPTION_LEN + VARSTR_HEADER_SIZE);
    pColInfo = taosArrayGet(pBlock->pDataBlock, c++);
    colDataSetVal(pColInfo, i, name, false);

    char    value[TSDB_CONFIG_VALUE_LEN + VARSTR_HEADER_SIZE] = {0};
    int32_t valueLen = 0;
    cfgDumpItemValue(pItem, &value[VARSTR_HEADER_SIZE], TSDB_CONFIG_VALUE_LEN, &valueLen);
    varDataSetLen(value, valueLen);
    pColInfo = taosArrayGet(pBlock->pDataBlock, c++);
    colDataSetVal(pColInfo, i, value, false);

    char scope[TSDB_CONFIG_SCOPE_LEN + VARSTR_HEADER_SIZE] = {0};
    cfgDumpItemScope(pItem, &scope[VARSTR_HEADER_SIZE], TSDB_CONFIG_SCOPE_LEN, &valueLen);
    varDataSetLen(scope, valueLen);
    pColInfo = taosArrayGet(pBlock->pDataBlock, c++);
    colDataSetVal(pColInfo, i, scope, false);

    numOfRows++;
  }

  pBlock->info.rows = numOfRows;

  return TSDB_CODE_SUCCESS;
}

int32_t dmProcessRetrieve(SDnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  int32_t size = 0;
  int32_t rowsRead = 0;

  SRetrieveTableReq retrieveReq = {0};
  if (tDeserializeSRetrieveTableReq(pMsg->pCont, pMsg->contLen, &retrieveReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }
#if 0
  if (strcmp(retrieveReq.user, TSDB_DEFAULT_USER) != 0) {
    terrno = TSDB_CODE_MND_NO_RIGHTS;
    return -1;
  }
#endif
  if (strcasecmp(retrieveReq.tb, TSDB_INS_TABLE_DNODE_VARIABLES)) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  SSDataBlock *pBlock = dmBuildVariablesBlock();

  dmAppendVariablesToBlock(pBlock, pMgmt->pData->dnodeId);

  size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  size = sizeof(SRetrieveMetaTableRsp) + sizeof(int32_t) + sizeof(SSysTableSchema) * numOfCols +
         blockDataGetSize(pBlock) + blockDataGetSerialMetaSize(numOfCols);

  SRetrieveMetaTableRsp *pRsp = rpcMallocCont(size);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    dError("failed to retrieve data since %s", terrstr());
    blockDataDestroy(pBlock);
    return -1;
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
  if (pArray == NULL) goto _OVER;

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
