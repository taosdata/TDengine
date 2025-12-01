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

#include <stdint.h>
#include "audit.h"
#include "cos.h"
#include "libs/new-stream/stream.h"
#include "monitor.h"
#include "taoserror.h"
#include "tencode.h"
#include "tglobal.h"
#include "tmsg.h"
#include "tmsgcb.h"
#include "tstrbuild.h"
#include "tutil.h"
#include "vnd.h"
#include "vnode.h"
#include "vnodeInt.h"

extern taos_counter_t *tsInsertCounter;

extern int32_t vnodeProcessScanVnodeReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
extern int32_t vnodeQueryScanProgress(SVnode *pVnode, SRpcMsg *pMsg);

static int32_t vnodeProcessCreateStbReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessAlterStbReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessDropStbReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp, SRpcMsg *pOriginRpc);
static int32_t vnodeProcessCreateTbReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp,
                                       SRpcMsg *pOriginRpc);
static int32_t vnodeProcessAlterTbReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessDropTbReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp,
                                     SRpcMsg *pOriginRpc);
static int32_t vnodeProcessSubmitReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp,
                                     SRpcMsg *pOriginalMsg);
static int32_t vnodeProcessAlterConfirmReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessAlterConfigReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessDropTtlTbReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessTrimReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessTrimWalReq(SVnode *pVnode, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessDeleteReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp,
                                     SRpcMsg *pOriginalMsg);
static int32_t vnodeProcessBatchDeleteReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessCreateIndexReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessDropIndexReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessCompactVnodeReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessConfigChangeReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessArbCheckSyncReq(SVnode *pVnode, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessDropTSmaCtbReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp,
                                          SRpcMsg *pOriginRpc);
static int32_t vnodeProcessCreateRsmaReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessDropRsmaReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessAlterRsmaReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);

static int32_t vnodeCheckState(SVnode *pVnode);
static int32_t vnodeCheckToken(SVnode *pVnode, char *member0Token, char *member1Token);
static int32_t vnodeCheckSyncd(SVnode *pVnode, char *member0Token, char *member1Token);
static int32_t vnodeProcessFetchTtlExpiredTbs(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);

extern int32_t vnodeProcessKillCompactReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
extern int32_t vnodeQueryCompactProgress(SVnode *pVnode, SRpcMsg *pMsg);

extern int32_t vnodeProcessKillRetentionReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
extern int32_t vnodeQueryRetentionProgress(SVnode *pVnode, SRpcMsg *pMsg);

extern int32_t vnodeProcessKillScanReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);

extern int32_t vnodeListSsMigrateFileSets(SVnode *pVnode, SRpcMsg *pMsg);
static int32_t vnodeProcessSsMigrateFileSetReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
extern int32_t vnodeQuerySsMigrateProgress(SVnode *pVnode, SRpcMsg *pMsg);
static int32_t vnodeProcessFollowerSsMigrateReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessKillSsMigrateReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);

static int32_t vnodePreprocessCreateTableReq(SVnode *pVnode, SDecoder *pCoder, int64_t btime, int64_t *pUid) {
  int32_t code = 0;
  int32_t lino = 0;

  if (tStartDecode(pCoder) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // flags
  if (tDecodeI32v(pCoder, NULL) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // name
  char *name = NULL;
  if (tDecodeCStr(pCoder, &name) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // uid
  int64_t uid = metaGetTableEntryUidByName(pVnode->pMeta, name);
  if (uid == 0) {
    uid = tGenIdPI64();
  }
  taosSetInt64Aligned((int64_t *)(pCoder->data + pCoder->pos), uid);

  // btime
  taosSetInt64Aligned((int64_t *)(pCoder->data + pCoder->pos + 8), btime);

  tEndDecode(pCoder);

_exit:
  if (code) {
    vError("vgId:%d %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  } else {
    vTrace("vgId:%d %s done, table:%s uid generated:%" PRId64, TD_VID(pVnode), __func__, name, uid);
    if (pUid) *pUid = uid;
  }
  return code;
}
static int32_t vnodePreProcessCreateTableMsg(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;
  int32_t lino = 0;

  int64_t  btime = taosGetTimestampMs();
  SDecoder dc = {0};
  int32_t  nReqs;

  tDecoderInit(&dc, (uint8_t *)pMsg->pCont + sizeof(SMsgHead), pMsg->contLen - sizeof(SMsgHead));
  if (tStartDecode(&dc) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    return code;
  }

  if (tDecodeI32v(&dc, &nReqs) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  for (int32_t iReq = 0; iReq < nReqs; iReq++) {
    code = vnodePreprocessCreateTableReq(pVnode, &dc, btime, NULL);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  tEndDecode(&dc);

_exit:
  tDecoderClear(&dc);
  if (code) {
    vError("vgId:%d, %s:%d failed to preprocess submit request since %s, msg type:%s", TD_VID(pVnode), __func__, lino,
           tstrerror(code), TMSG_INFO(pMsg->msgType));
  }
  return code;
}

static int32_t vnodePreProcessAlterTableMsg(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = TSDB_CODE_INVALID_MSG;
  int32_t lino = 0;

  SDecoder dc = {0};
  tDecoderInit(&dc, (uint8_t *)pMsg->pCont + sizeof(SMsgHead), pMsg->contLen - sizeof(SMsgHead));

  SVAlterTbReq vAlterTbReq = {0};
  int64_t      ctimeMs = taosGetTimestampMs();
  if (tDecodeSVAlterTbReqSetCtime(&dc, &vAlterTbReq, ctimeMs) < 0) {
    taosArrayDestroy(vAlterTbReq.pMultiTag);
    vAlterTbReq.pMultiTag = NULL;
    goto _exit;
  }
  taosArrayDestroy(vAlterTbReq.pMultiTag);
  vAlterTbReq.pMultiTag = NULL;

  code = 0;

_exit:
  tDecoderClear(&dc);
  if (code) {
    vError("vgId:%d %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  } else {
    vTrace("vgId:%d %s done, table:%s ctimeMs generated:%" PRId64, TD_VID(pVnode), __func__, vAlterTbReq.tbName,
           ctimeMs);
  }
  return code;
}

static int32_t vnodePreProcessDropTtlMsg(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = TSDB_CODE_INVALID_MSG;
  int32_t lino = 0;

  SMsgHead *pContOld = pMsg->pCont;
  int32_t   reqLenOld = pMsg->contLen - sizeof(SMsgHead);

  SArray *tbUids = NULL;
  int64_t timestampMs = 0;

  SVDropTtlTableReq ttlReq = {0};
  if (tDeserializeSVDropTtlTableReq((char *)pContOld + sizeof(SMsgHead), reqLenOld, &ttlReq) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  {  // find expired uids
    tbUids = taosArrayInit(8, sizeof(tb_uid_t));
    if (tbUids == NULL) {
      code = terrno;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    timestampMs = (int64_t)ttlReq.timestampSec * 1000;
    code = metaTtlFindExpired(pVnode->pMeta, timestampMs, tbUids, ttlReq.ttlDropMaxCount);
    if (code != 0) {
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    ttlReq.nUids = taosArrayGetSize(tbUids);
    ttlReq.pTbUids = tbUids;
  }

  if (ttlReq.nUids == 0) {
    code = TSDB_CODE_MSG_PREPROCESSED;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  {  // prepare new content
    int32_t reqLenNew = tSerializeSVDropTtlTableReq(NULL, 0, &ttlReq);
    int32_t contLenNew = reqLenNew + sizeof(SMsgHead);

    SMsgHead *pContNew = rpcMallocCont(contLenNew);
    if (pContNew == NULL) {
      code = terrno;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    if (tSerializeSVDropTtlTableReq((char *)pContNew + sizeof(SMsgHead), reqLenNew, &ttlReq) != 0) {
      vError("vgId:%d %s:%d failed to serialize drop ttl request", TD_VID(pVnode), __func__, lino);
    }
    pContNew->contLen = htonl(reqLenNew);
    pContNew->vgId = pContOld->vgId;

    rpcFreeCont(pContOld);
    pMsg->pCont = pContNew;
    pMsg->contLen = contLenNew;
  }

  code = 0;

_exit:
  taosArrayDestroy(tbUids);

  if (code && code != TSDB_CODE_MSG_PREPROCESSED) {
    vError("vgId:%d, %s:%d failed to preprocess drop ttl request since %s, msg type:%s", TD_VID(pVnode), __func__, lino,
           tstrerror(code), TMSG_INFO(pMsg->msgType));
  } else {
    vTrace("vgId:%d, %s done, timestampSec:%d, nUids:%d", TD_VID(pVnode), __func__, ttlReq.timestampSec, ttlReq.nUids);
  }

  return code;
}

extern int64_t tsMaxKeyByPrecision[];
static int32_t vnodePreProcessSubmitTbData(SVnode *pVnode, SDecoder *pCoder, int64_t btimeMs, int64_t ctimeMs) {
  int32_t code = 0;
  int32_t lino = 0;

  if (tStartDecode(pCoder) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  SSubmitTbData submitTbData;
  uint8_t       version;
  if (tDecodeI32v(pCoder, &submitTbData.flags) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  version = (submitTbData.flags >> 8) & 0xff;
  submitTbData.flags = submitTbData.flags & 0xff;

  int64_t uid;
  if (submitTbData.flags & SUBMIT_REQ_AUTO_CREATE_TABLE) {
    code = vnodePreprocessCreateTableReq(pVnode, pCoder, btimeMs, &uid);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // submit data
  if (tDecodeI64(pCoder, &submitTbData.suid) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (submitTbData.flags & SUBMIT_REQ_AUTO_CREATE_TABLE) {
    taosSetInt64Aligned((int64_t *)(pCoder->data + pCoder->pos), uid);
    pCoder->pos += sizeof(int64_t);
  } else {
    if (tDecodeI64(pCoder, &submitTbData.uid) < 0) {
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  if (tDecodeI32v(pCoder, &submitTbData.sver) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // scan and check
  TSKEY now = btimeMs;
  if (pVnode->config.tsdbCfg.precision == TSDB_TIME_PRECISION_MICRO) {
    now *= 1000;
  } else if (pVnode->config.tsdbCfg.precision == TSDB_TIME_PRECISION_NANO) {
    now *= 1000000;
  }

  int32_t keep = pVnode->config.tsdbCfg.keep2;

  TSKEY minKey = now - tsTickPerMin[pVnode->config.tsdbCfg.precision] * keep;
  TSKEY maxKey = tsMaxKeyByPrecision[pVnode->config.tsdbCfg.precision];
  if (submitTbData.flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
    uint64_t nColData;
    if (tDecodeU64v(pCoder, &nColData) < 0) {
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    SColData colData = {0};
    code = tDecodeColData(version, pCoder, &colData, false);
    if (code) {
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    if (colData.flag != HAS_VALUE) {
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    for (int32_t iRow = 0; iRow < colData.nVal; iRow++) {
      if (((TSKEY *)colData.pData)[iRow] < minKey || ((TSKEY *)colData.pData)[iRow] > maxKey) {
        code = TSDB_CODE_TDB_TIMESTAMP_OUT_OF_RANGE;
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    }

    for (uint64_t i = 1; i < nColData; i++) {
      code = tDecodeColData(version, pCoder, &colData, true);
      if (code) {
        code = TSDB_CODE_INVALID_MSG;
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    }
  } else {
    uint64_t nRow;
    if (tDecodeU64v(pCoder, &nRow) < 0) {
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    for (int32_t iRow = 0; iRow < nRow; ++iRow) {
      SRow *pRow = (SRow *)(pCoder->data + pCoder->pos);
      pCoder->pos += pRow->len;
#ifndef NO_UNALIGNED_ACCESS
      if (pRow->ts < minKey || pRow->ts > maxKey) {
#else
      TSKEY ts = taosGetInt64Aligned(&pRow->ts);
      if (ts < minKey || ts > maxKey) {
#endif
        code = TSDB_CODE_TDB_TIMESTAMP_OUT_OF_RANGE;
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      // Check pRow->sver
      if (pRow->sver != submitTbData.sver) {
        code = TSDB_CODE_INVALID_DATA_FMT;
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    }
  }

  if (!tDecodeIsEnd(pCoder)) {
    taosSetInt64Aligned((int64_t *)(pCoder->data + pCoder->pos), ctimeMs);
    pCoder->pos += sizeof(int64_t);
  }

  tEndDecode(pCoder);

_exit:
  if (code) {
    vError("vgId:%d, %s:%d failed to vnodePreProcessSubmitTbData submit request since %s", TD_VID(pVnode), __func__,
           lino, tstrerror(code));
  }
  return code;
}
static int32_t vnodePreProcessSubmitMsg(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;
  int32_t lino = 0;

  if (tsBypassFlag & TSDB_BYPASS_RA_RPC_RECV_SUBMIT) {
    return TSDB_CODE_MSG_PREPROCESSED;
  }

  SDecoder *pCoder = &(SDecoder){0};

  if (taosHton64(((SSubmitReq2Msg *)pMsg->pCont)->version) != 1) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  tDecoderInit(pCoder, (uint8_t *)pMsg->pCont + sizeof(SSubmitReq2Msg), pMsg->contLen - sizeof(SSubmitReq2Msg));

  if (tStartDecode(pCoder) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  uint64_t nSubmitTbData;
  if (tDecodeU64v(pCoder, &nSubmitTbData) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  int64_t btimeMs = taosGetTimestampMs();
  int64_t ctimeMs = btimeMs;
  for (int32_t i = 0; i < nSubmitTbData; i++) {
    code = vnodePreProcessSubmitTbData(pVnode, pCoder, btimeMs, ctimeMs);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  tEndDecode(pCoder);

_exit:
  tDecoderClear(pCoder);
  if (code) {
    vError("vgId:%d, %s:%d failed to preprocess submit request since %s, msg type:%s", TD_VID(pVnode), __func__, lino,
           tstrerror(code), TMSG_INFO(pMsg->msgType));
  }
  return code;
}

static int32_t vnodePreProcessDeleteMsg(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;

  int32_t    size;
  int32_t    ret;
  uint8_t   *pCont;
  SEncoder  *pCoder = &(SEncoder){0};
  SDeleteRes res = {0};

  SReadHandle handle = {0};
  handle.vnode = pVnode;
  handle.pMsgCb = &pVnode->msgCb;
  handle.skipRollup = 1;
  initStorageAPI(&handle.api);

  code = qWorkerProcessDeleteMsg(&handle, pVnode->pQuery, pMsg, &res);
  if (code) goto _exit;

  res.ctimeMs = taosGetTimestampMs();
  // malloc and encode
  tEncodeSize(tEncodeDeleteRes, &res, size, ret);
  pCont = rpcMallocCont(size + sizeof(SMsgHead));

  ((SMsgHead *)pCont)->contLen = size + sizeof(SMsgHead);
  ((SMsgHead *)pCont)->vgId = TD_VID(pVnode);

  tEncoderInit(pCoder, pCont + sizeof(SMsgHead), size);
  if (tEncodeDeleteRes(pCoder, &res) != 0) {
    vError("vgId:%d %s failed to encode delete response", TD_VID(pVnode), __func__);
  }
  tEncoderClear(pCoder);

  rpcFreeCont(pMsg->pCont);
  pMsg->pCont = pCont;
  pMsg->contLen = size + sizeof(SMsgHead);

  taosArrayDestroy(res.uidList);

_exit:
  return code;
}

static int32_t vnodePreProcessBatchDeleteMsg(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;
  int32_t lino = 0;

  int64_t         ctimeMs = taosGetTimestampMs();
  SBatchDeleteReq pReq = {0};
  SDecoder       *pCoder = &(SDecoder){0};

  tDecoderInit(pCoder, (uint8_t *)pMsg->pCont + sizeof(SMsgHead), pMsg->contLen - sizeof(SMsgHead));

  if (tDecodeSBatchDeleteReqSetCtime(pCoder, &pReq, ctimeMs) < 0) {
    code = TSDB_CODE_INVALID_MSG;
  }

  tDecoderClear(pCoder);
  taosArrayDestroy(pReq.deleteReqs);

  if (code) {
    vError("vgId:%d %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  } else {
    vTrace("vgId:%d %s done, ctimeMs generated:%" PRId64, TD_VID(pVnode), __func__, ctimeMs);
  }
  return code;
}

static int32_t vnodePreProcessArbCheckSyncMsg(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t ret = 0;
  if ((ret = vnodeCheckState(pVnode)) != 0) {
    vDebug("vgId:%d, failed to preprocess vnode-arb-check-sync request since %s", TD_VID(pVnode), tstrerror(ret));
    return 0;
  }

  SVArbCheckSyncReq syncReq = {0};

  if (tDeserializeSVArbCheckSyncReq((char *)pMsg->pCont + sizeof(SMsgHead), pMsg->contLen - sizeof(SMsgHead),
                                    &syncReq) != 0) {
    return TSDB_CODE_INVALID_MSG;
  }

  ret = vnodeCheckToken(pVnode, syncReq.member0Token, syncReq.member1Token);
  if (ret != 0) {
    vError("vgId:%d, failed to preprocess vnode-arb-check-sync request since %s", TD_VID(pVnode), tstrerror(ret));
  }

  int32_t code = terrno;
  tFreeSVArbCheckSyncReq(&syncReq);

  return code;
}

int32_t vnodePreProcessDropTbMsg(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  int32_t          size = 0;
  SDecoder         dc = {0};
  SEncoder         ec = {0};
  SVDropTbBatchReq receivedBatchReqs = {0};
  SVDropTbBatchReq sentBatchReqs = {0};

  tDecoderInit(&dc, POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead)), pMsg->contLen - sizeof(SMsgHead));

  code = tDecodeSVDropTbBatchReq(&dc, &receivedBatchReqs);
  if (code < 0) {
    terrno = code;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  sentBatchReqs.pArray = taosArrayInit(receivedBatchReqs.nReqs, sizeof(SVDropTbReq));
  if (!sentBatchReqs.pArray) {
    code = terrno;
    goto _exit;
  }

  for (int32_t i = 0; i < receivedBatchReqs.nReqs; ++i) {
    SVDropTbReq *pReq = receivedBatchReqs.pReqs + i;
    tb_uid_t     uid = metaGetTableEntryUidByName(pVnode->pMeta, pReq->name);
    if (uid == 0) {
      vWarn("vgId:%d, preprocess drop ctb: %s not found", TD_VID(pVnode), pReq->name);
      continue;
    }
    pReq->uid = uid;
    vDebug("vgId:%d %s for: %s, uid: %" PRId64, TD_VID(pVnode), __func__, pReq->name, pReq->uid);
    if (taosArrayPush(sentBatchReqs.pArray, pReq) == NULL) {
      code = terrno;
      goto _exit;
    }
  }
  sentBatchReqs.nReqs = sentBatchReqs.pArray->size;

  tEncodeSize(tEncodeSVDropTbBatchReq, &sentBatchReqs, size, code);
  tEncoderInit(&ec, POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead)), size);
  code = tEncodeSVDropTbBatchReq(&ec, &sentBatchReqs);
  tEncoderClear(&ec);
  if (code != TSDB_CODE_SUCCESS) {
    vError("vgId:%d %s failed to encode drop tb batch req: %s", TD_VID(pVnode), __func__, tstrerror(code));
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  tDecoderClear(&dc);
  if (sentBatchReqs.pArray) {
    taosArrayDestroy(sentBatchReqs.pArray);
  }
  return code;
}

int32_t vnodePreProcessSsMigrateFileSetReq(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t              code = TSDB_CODE_SUCCESS, lino = 0;
  SSsMigrateFileSetReq req = {0};

  code = tDeserializeSSsMigrateFileSetReq(POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead)),
                                          pMsg->contLen - sizeof(SMsgHead), &req);
  if (code < 0) {
    terrno = code;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  req.nodeId = vnodeNodeId(pVnode);
  TAOS_UNUSED(tSerializeSSsMigrateFileSetReq(POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead)),
                                             pMsg->contLen - sizeof(SMsgHead), &req));

_exit:
  return code;
}

int32_t vnodePreProcessWriteMsg(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;

  if (pVnode->mounted) {
    vError("vgId:%d, mountVgId:%d, skip write msg:%s", TD_VID(pVnode), pVnode->config.mountVgId,
           TMSG_INFO(pMsg->msgType));
    return TSDB_CODE_OPS_NOT_SUPPORT;
  }

  switch (pMsg->msgType) {
    case TDMT_VND_CREATE_TABLE: {
      code = vnodePreProcessCreateTableMsg(pVnode, pMsg);
    } break;
    case TDMT_VND_ALTER_TABLE: {
      code = vnodePreProcessAlterTableMsg(pVnode, pMsg);
    } break;
    case TDMT_VND_FETCH_TTL_EXPIRED_TBS:
    case TDMT_VND_DROP_TTL_TABLE: {
      code = vnodePreProcessDropTtlMsg(pVnode, pMsg);
    } break;
    case TDMT_VND_SUBMIT: {
      METRICS_TIMING_BLOCK(pVnode->writeMetrics.preprocess_time, METRIC_LEVEL_LOW,
                           { code = vnodePreProcessSubmitMsg(pVnode, pMsg); });
    } break;
    case TDMT_VND_DELETE: {
      code = vnodePreProcessDeleteMsg(pVnode, pMsg);
    } break;
    case TDMT_VND_BATCH_DEL: {
      code = vnodePreProcessBatchDeleteMsg(pVnode, pMsg);
    } break;
    case TDMT_VND_ARB_CHECK_SYNC: {
      code = vnodePreProcessArbCheckSyncMsg(pVnode, pMsg);
    } break;
    case TDMT_VND_DROP_TABLE: {
      code = vnodePreProcessDropTbMsg(pVnode, pMsg);
    } break;
#ifdef TD_ENTERPRISE
    case TDMT_VND_SSMIGRATE_FILESET: {
      code = vnodePreProcessSsMigrateFileSetReq(pVnode, pMsg);
    } break;
#endif
    default:
      break;
  }

  if (code && code != TSDB_CODE_MSG_PREPROCESSED) {
    vError("vgId:%d, failed to preprocess write request since %s, msg type:%s", TD_VID(pVnode), tstrerror(code),
           TMSG_INFO(pMsg->msgType));
  }
  return code;
}

static int32_t inline vnodeSubmitSubRowBlobData(SVnode *pVnode, SSubmitTbData *pSubmitTbData) {
  int32_t code = 0;
  int32_t lino = 0;

  int64_t   st = taosGetTimestampUs();
  SBlobSet *pBlobSet = pSubmitTbData->pBlobSet;
  int32_t   sz = taosArrayGetSize(pBlobSet->pSeqTable);

  SBseBatch *pBatch = NULL;

  code = bseBatchInit(pVnode->pBse, &pBatch, sz);
  TSDB_CHECK_CODE(code, lino, _exit);

  SRow  **pRow = (SRow **)TARRAY_DATA(pSubmitTbData->aRowP);
  int32_t rowIdx = -1;
  for (int32_t i = 0; i < sz; i++) {
    int64_t     seq = 0;
    SBlobValue *p = taosArrayGet(pBlobSet->pSeqTable, i);
    if (p->type == TSDB_DATA_BLOB_EMPTY_VALUE || p->type == TSDB_DATA_BLOB_NULL_VALUE) {
      // skip empty or null blob
      continue;
    }

    code = bseBatchPut(pBatch, &seq, pBlobSet->data + p->offset, p->len);
    TSDB_CHECK_CODE(code, lino, _exit);

    SRow *row = taosArrayGetP(pSubmitTbData->aRowP, i);
    if (row == NULL) {
      int32_t tlen = taosArrayGetSize(pBlobSet->pSeqTable);
      uTrace("blob invalid row index:%d, sz:%d, pBlobSet size:%d", rowIdx, sz, tlen);
      break;
    }

    if (tPutU64(row->data + p->dataOffset, seq) < 0) {
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  code = bseCommitBatch(pVnode->pBse, pBatch);
  TSDB_CHECK_CODE(code, lino, _exit);

  int64_t cost = taosGetTimestampUs() - st;
  if (cost >= 500) {
    vDebug("vgId:%d, %s, cost:%" PRId64 "us, rows:%d, size:%" PRId64 "", TD_VID(pVnode), __func__, cost, sz,
           pBlobSet->len);
  }
_exit:
  if (code != 0) {
    vError("vgId:%d %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t inline vnodeSubmitSubColBlobData(SVnode *pVnode, SSubmitTbData *pSubmitTbData) {
  int32_t code = 0;
  int32_t lino = 0;

  int32_t   blobColIdx = 0;
  SColData *pBlobCol = NULL;
  int64_t   st = taosGetTimestampUs();
  SBlobSet *pBlobSet = pSubmitTbData->pBlobSet;
  int32_t   sz = taosArrayGetSize(pBlobSet->pSeqTable);

  SBseBatch *pBatch = NULL;

  code = bseBatchInit(pVnode->pBse, &pBatch, sz);
  TSDB_CHECK_CODE(code, lino, _exit);

  SColData *p = (SColData *)TARRAY_DATA(pSubmitTbData->aCol);
  for (int32_t i = 0; i < TARRAY_SIZE(pSubmitTbData->aCol); i++) {
    if (IS_STR_DATA_BLOB(p[i].type)) {
      pBlobCol = &p[i];
      break;
    }
  }
  if (pBlobCol == NULL) {
    vError("vgId:%d %s failed to find blob column in submit data", TD_VID(pVnode), __func__);
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  int32_t offset = 0;
  // int32_t   rowIdx = -1;
  for (int32_t i = 0; i < sz; i++) {
    int64_t     seq = 0;
    SBlobValue *p = taosArrayGet(pBlobSet->pSeqTable, i);
    if (p->type == TSDB_DATA_BLOB_EMPTY_VALUE || p->type == TSDB_DATA_BLOB_NULL_VALUE) {
      // skip empty or null blob
      continue;
    }
    code = bseBatchPut(pBatch, &seq, pBlobSet->data + p->offset, p->len);
    TSDB_CHECK_CODE(code, lino, _exit);

    memcpy(pBlobCol->pData + offset, (void *)&seq, BSE_SEQUECE_SIZE);
    offset += BSE_SEQUECE_SIZE;
  }

  code = bseCommitBatch(pVnode->pBse, pBatch);
  TSDB_CHECK_CODE(code, lino, _exit);

  int64_t cost = taosGetTimestampUs() - st;
  if (cost >= 500) {
    vDebug("vgId:%d, %s, cost:%" PRId64 "us, rows:%d, size:%" PRId64 "", TD_VID(pVnode), __func__, cost, sz,
           pBlobSet->len);
  }
_exit:
  if (code != 0) {
    vError("vgId:%d %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}
static int32_t inline vnodeSubmitBlobData(SVnode *pVnode, SSubmitTbData *pData) {
  int32_t code = 0;
  if (pData->flags & SUBMIT_REQ_WITH_BLOB) {
    if (pData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
      code = vnodeSubmitSubColBlobData(pVnode, pData);
    } else {
      code = vnodeSubmitSubRowBlobData(pVnode, pData);
    }
  }

  return code;
}

int32_t vnodeProcessWriteMsg(SVnode *pVnode, SRpcMsg *pMsg, int64_t ver, SRpcMsg *pRsp) {
  int32_t code = 0;
  int32_t lino = 0;
  void   *ptr = NULL;
  void   *pReq;
  int32_t len;

  (void)taosThreadMutexLock(&pVnode->mutex);
  if (pVnode->disableWrite) {
    (void)taosThreadMutexUnlock(&pVnode->mutex);
    vError("vgId:%d write is disabled for snapshot, version:%" PRId64, TD_VID(pVnode), ver);
    return TSDB_CODE_VND_WRITE_DISABLED;
  }
  (void)taosThreadMutexUnlock(&pVnode->mutex);

  if (ver <= pVnode->state.applied) {
    vError("vgId:%d, duplicate write request. ver: %" PRId64 ", applied: %" PRId64, TD_VID(pVnode), ver,
           pVnode->state.applied);
    return terrno = TSDB_CODE_VND_DUP_REQUEST;
  }

  vGDebug(&pMsg->info.traceId,
          "vgId:%d, index:%" PRId64 ", process write request:%s, applied:%" PRId64 ", state.applyTerm:%" PRId64
          ", conn.applyTerm:%" PRId64 ", contLen:%d",
          TD_VID(pVnode), ver, TMSG_INFO(pMsg->msgType), pVnode->state.applied, pVnode->state.applyTerm,
          pMsg->info.conn.applyTerm, pMsg->contLen);

  if (!(pVnode->state.applyTerm <= pMsg->info.conn.applyTerm)) {
    vError("vgId:%d, applyTerm mismatch, expected: %" PRId64 ", received: %" PRId64, TD_VID(pVnode),
           pVnode->state.applyTerm, pMsg->info.conn.applyTerm);
    return terrno = TSDB_CODE_INTERNAL_ERROR;
  }

  if (!(pVnode->state.applied + 1 == ver)) {
    vError("vgId:%d, mountVgId:%d, ver mismatch, expected: %" PRId64 ", received: %" PRId64, TD_VID(pVnode),
           pVnode->config.mountVgId, pVnode->state.applied + 1, ver);
    return terrno = TSDB_CODE_INTERNAL_ERROR;
  }

  atomic_store_64(&pVnode->state.applied, ver);
  atomic_store_64(&pVnode->state.applyTerm, pMsg->info.conn.applyTerm);

  if (!syncUtilUserCommit(pMsg->msgType)) goto _exit;

  // skip header
  pReq = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  len = pMsg->contLen - sizeof(SMsgHead);
  bool needCommit = false;

  switch (pMsg->msgType) {
    /* META */
    case TDMT_VND_CREATE_STB:
      code = vnodeProcessCreateStbReq(pVnode, ver, pReq, len, pRsp);
      TSDB_CHECK_CODE(code, lino, _err);
      break;
    case TDMT_VND_ALTER_STB:
      code = vnodeProcessAlterStbReq(pVnode, ver, pReq, len, pRsp);
      TSDB_CHECK_CODE(code, lino, _err);
      break;
    case TDMT_VND_DROP_STB:
      code = vnodeProcessDropStbReq(pVnode, ver, pReq, len, pRsp, pMsg);
      TSDB_CHECK_CODE(code, lino, _err);
      break;
    case TDMT_VND_CREATE_TABLE:
      code = vnodeProcessCreateTbReq(pVnode, ver, pReq, len, pRsp, pMsg);
      TSDB_CHECK_CODE(code, lino, _err);
      break;
    case TDMT_VND_ALTER_TABLE:
      code = vnodeProcessAlterTbReq(pVnode, ver, pReq, len, pRsp);
      TSDB_CHECK_CODE(code, lino, _err);
      break;
    case TDMT_VND_DROP_TABLE:
    case TDMT_VND_SNODE_DROP_TABLE:
      code = vnodeProcessDropTbReq(pVnode, ver, pReq, len, pRsp, pMsg);
      TSDB_CHECK_CODE(code, lino, _err);
      break;
    case TDMT_VND_CREATE_RSMA:
      code = vnodeProcessCreateRsmaReq(pVnode, ver, pReq, len, pRsp);
      TSDB_CHECK_CODE(code, lino, _err);
      break;
    case TDMT_VND_DROP_RSMA:
      code = vnodeProcessDropRsmaReq(pVnode, ver, pReq, len, pRsp);
      TSDB_CHECK_CODE(code, lino, _err);
      break;
    case TDMT_VND_ALTER_RSMA:
      code = vnodeProcessAlterRsmaReq(pVnode, ver, pReq, len, pRsp);
      TSDB_CHECK_CODE(code, lino, _err);
      break;
    case TDMT_VND_DROP_TTL_TABLE:
      code = vnodeProcessDropTtlTbReq(pVnode, ver, pReq, len, pRsp);
      TSDB_CHECK_CODE(code, lino, _err);
      break;
    case TDMT_VND_FETCH_TTL_EXPIRED_TBS:
      code = vnodeProcessFetchTtlExpiredTbs(pVnode, ver, pReq, len, pRsp);
      TSDB_CHECK_CODE(code, lino, _err);
      break;
    case TDMT_VND_TRIM:
      code = vnodeProcessTrimReq(pVnode, ver, pReq, len, pRsp);
      TSDB_CHECK_CODE(code, lino, _err);
      break;
      case TDMT_VND_TRIM_WAL:
      if (vnodeProcessTrimWalReq(pVnode, pReq, len, pRsp) < 0) goto _err;
      break;
#ifdef TD_ENTERPRISE
    case TDMT_VND_SSMIGRATE_FILESET:
      if (vnodeProcessSsMigrateFileSetReq(pVnode, ver, pReq, len, pRsp) < 0) goto _err;
      break;
    case TDMT_VND_FOLLOWER_SSMIGRATE:
      if (vnodeProcessFollowerSsMigrateReq(pVnode, ver, pReq, len, pRsp) < 0) goto _err;
      break;
    case TDMT_VND_KILL_SSMIGRATE:
      if (vnodeProcessKillSsMigrateReq(pVnode, ver, pReq, len, pRsp) < 0) goto _err;
      break;
#endif

    /* TSDB */
    case TDMT_VND_SUBMIT: {
      METRICS_TIMING_BLOCK(pVnode->writeMetrics.apply_time, METRIC_LEVEL_LOW, {
        if (vnodeProcessSubmitReq(pVnode, ver, pMsg->pCont, pMsg->contLen, pRsp, pMsg) < 0) goto _err;
      });
      METRICS_UPDATE(pVnode->writeMetrics.apply_bytes, METRIC_LEVEL_LOW, (int64_t)pMsg->contLen);
      break;
    }
    case TDMT_VND_DELETE:
      code = vnodeProcessDeleteReq(pVnode, ver, pReq, len, pRsp, pMsg);
      TSDB_CHECK_CODE(code, lino, _err);
      break;
    case TDMT_VND_BATCH_DEL:
      code = vnodeProcessBatchDeleteReq(pVnode, ver, pReq, len, pRsp);
      TSDB_CHECK_CODE(code, lino, _err);
      break;
      /* TQ */
#if defined(USE_TQ) || defined(USE_STREAM)
    case TDMT_VND_TMQ_SUBSCRIBE:
      code = tqProcessSubscribeReq(pVnode->pTq, ver, pReq, len);
      TSDB_CHECK_CODE(code, lino, _err);
      break;
    case TDMT_VND_TMQ_DELETE_SUB:
      code = tqProcessDeleteSubReq(pVnode->pTq, ver, pMsg->pCont, pMsg->contLen);
      TSDB_CHECK_CODE(code, lino, _err);
      break;
    case TDMT_VND_TMQ_COMMIT_OFFSET:
      code = tqProcessOffsetCommitReq(pVnode->pTq, ver, pReq, len);
      TSDB_CHECK_CODE(code, lino, _err);
      break;
    case TDMT_VND_TMQ_ADD_CHECKINFO:
      code = tqProcessAddCheckInfoReq(pVnode->pTq, ver, pReq, len);
      TSDB_CHECK_CODE(code, lino, _err);
      break;
    case TDMT_VND_TMQ_DEL_CHECKINFO:
      code = tqProcessDelCheckInfoReq(pVnode->pTq, ver, pReq, len);
      TSDB_CHECK_CODE(code, lino, _err);
      break;
#endif
    case TDMT_VND_ALTER_CONFIRM:
      needCommit = pVnode->config.hashChange;
      code = vnodeProcessAlterConfirmReq(pVnode, ver, pReq, len, pRsp);
      TSDB_CHECK_CODE(code, lino, _err);
      break;
    case TDMT_VND_ALTER_CONFIG:
      vnodeProcessAlterConfigReq(pVnode, ver, pReq, len, pRsp);
      break;
    case TDMT_VND_COMMIT:
      needCommit = true;
      break;
    case TDMT_VND_CREATE_INDEX:
      vnodeProcessCreateIndexReq(pVnode, ver, pReq, len, pRsp);
      break;
    case TDMT_VND_DROP_INDEX:
      vnodeProcessDropIndexReq(pVnode, ver, pReq, len, pRsp);
      break;
    case TDMT_VND_COMPACT:
      vnodeProcessCompactVnodeReq(pVnode, ver, pReq, len, pRsp);
      goto _exit;
    case TDMT_VND_SCAN:
      vnodeProcessScanVnodeReq(pVnode, ver, pReq, len, pRsp);
      goto _exit;
    case TDMT_SYNC_CONFIG_CHANGE:
      vnodeProcessConfigChangeReq(pVnode, ver, pReq, len, pRsp);
      break;
#ifdef TD_ENTERPRISE
    case TDMT_VND_KILL_COMPACT:
      vnodeProcessKillCompactReq(pVnode, ver, pReq, len, pRsp);
      break;
#endif
    case TDMT_VND_KILL_SCAN:
      vnodeProcessKillScanReq(pVnode, ver, pReq, len, pRsp);
      break;
    case TDMT_VND_KILL_TRIM:
      vnodeProcessKillRetentionReq(pVnode, ver, pReq, len, pRsp);
      break;
    /* ARB */
    case TDMT_VND_ARB_CHECK_SYNC:
      vnodeProcessArbCheckSyncReq(pVnode, pReq, len, pRsp);
      break;
    default:
      vError("vgId:%d, unprocessed msg, %d", TD_VID(pVnode), pMsg->msgType);
      return TSDB_CODE_INVALID_MSG;
  }

  vGDebug(&pMsg->info.traceId, "vgId:%d, index:%" PRId64 ", msg processed, code:0x%x", TD_VID(pVnode), ver, pRsp->code);

  walApplyVer(pVnode->pWal, ver);

  if (pVnode->pTq) {
    code = tqPushMsg(pVnode->pTq, pMsg->msgType);
    if (code) {
      vError("vgId:%d, failed to push msg to TQ since %s", TD_VID(pVnode), tstrerror(terrno));
      return code;
    }
  }

  // commit if need
  if (needCommit) {
    vInfo("vgId:%d, commit at version %" PRId64, TD_VID(pVnode), ver);
    code = vnodeAsyncCommit(pVnode);
    if (code) {
      vError("vgId:%d, failed to vnode async commit since %s.", TD_VID(pVnode), tstrerror(terrno));
      goto _err;
    }

    // start a new one
    METRICS_TIMING_BLOCK(pVnode->writeMetrics.memtable_wait_time, METRIC_LEVEL_LOW, {
      code = vnodeBegin(pVnode);
      if (code) {
        vError("vgId:%d, failed to begin vnode since %s.", TD_VID(pVnode), tstrerror(terrno));
        goto _err;
      }
    });
  }

_exit:
  return 0;

_err:
  if (code == TSDB_CODE_TDB_TABLE_ALREADY_EXIST) {
    vInfo("vgId:%d, process %s request failed since %s, lino 1:%d ver:%" PRId64, TD_VID(pVnode),
          TMSG_INFO(pMsg->msgType), tstrerror(code), lino, ver);
  } else if (code) {
    vError("vgId:%d, process %s request failed since %s, lino 2:%d ver:%" PRId64, TD_VID(pVnode),
           TMSG_INFO(pMsg->msgType), tstrerror(code), lino, ver);
  }

  return code;
}

int32_t vnodePreprocessQueryMsg(SVnode *pVnode, SRpcMsg *pMsg) {
  if (TDMT_SCH_QUERY != pMsg->msgType && TDMT_SCH_MERGE_QUERY != pMsg->msgType) {
    return 0;
  }

  int32_t qType = 0;
  return qWorkerPreprocessQueryMsg(pVnode->pQuery, pMsg, (TDMT_SCH_QUERY == pMsg->msgType), &qType);
}

int32_t vnodeProcessQueryMsg(SVnode *pVnode, SRpcMsg *pMsg, SQueueInfo *pInfo) {
  vTrace("message in vnode query queue is processing");
  if (pMsg->msgType == TDMT_VND_TMQ_CONSUME && !syncIsReadyForRead(pVnode->sync)) {
    vnodeRedirectRpcMsg(pVnode, pMsg, terrno);
    return 0;
  }

  if (pMsg->msgType == TDMT_VND_TMQ_CONSUME && !pVnode->restored) {
    vnodeRedirectRpcMsg(pVnode, pMsg, TSDB_CODE_SYN_RESTORING);
    return 0;
  }

  SReadHandle handle = {0};
  handle.vnode = pVnode;
  handle.pMsgCb = &pVnode->msgCb;
  handle.pWorkerCb = pInfo->workerCb;
  initStorageAPI(&handle.api);
  int32_t code = TSDB_CODE_SUCCESS;
  bool    redirected = false;

  switch (pMsg->msgType) {
    case TDMT_SCH_QUERY:
      if (!syncIsReadyForRead(pVnode->sync)) {
        pMsg->code = (terrno) ? terrno : TSDB_CODE_SYN_NOT_LEADER;
        redirected = true;
      }
      code = qWorkerProcessQueryMsg(&handle, pVnode->pQuery, pMsg, 0);
      if (redirected) {
        vnodeRedirectRpcMsg(pVnode, pMsg, pMsg->code);
        return 0;
      }

      return code;
    case TDMT_SCH_MERGE_QUERY:
      return qWorkerProcessQueryMsg(&handle, pVnode->pQuery, pMsg, 0);
    case TDMT_SCH_QUERY_CONTINUE:
      return qWorkerProcessCQueryMsg(&handle, pVnode->pQuery, pMsg, 0);
    case TDMT_VND_TMQ_CONSUME:
      return tqProcessPollReq(pVnode->pTq, pMsg);
    case TDMT_VND_TMQ_CONSUME_PUSH:
      return tqProcessPollPush(pVnode->pTq);
    default:
      vError("unknown msg type:%d in query queue", pMsg->msgType);
      return TSDB_CODE_APP_ERROR;
  }
}

int32_t vnodeProcessFetchMsg(SVnode *pVnode, SRpcMsg *pMsg, SQueueInfo *pInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  vTrace("vgId:%d, msg:%p in fetch queue is processing", pVnode->config.vgId, pMsg);
  if ((pMsg->msgType == TDMT_SCH_FETCH || pMsg->msgType == TDMT_VND_TABLE_META || pMsg->msgType == TDMT_VND_TABLE_CFG ||
       pMsg->msgType == TDMT_VND_BATCH_META || pMsg->msgType == TDMT_VND_TABLE_NAME ||
       pMsg->msgType == TDMT_VND_VSUBTABLES_META || pMsg->msgType == TDMT_VND_VSTB_REF_DBS) &&
      !syncIsReadyForRead(pVnode->sync)) {
    vnodeRedirectRpcMsg(pVnode, pMsg, terrno);
    return 0;
  }

  switch (pMsg->msgType) {
    case TDMT_SCH_FETCH:
    case TDMT_SCH_MERGE_FETCH:
      return qWorkerProcessFetchMsg(pVnode, pVnode->pQuery, pMsg, 0);
    case TDMT_SCH_FETCH_RSP:
      return qWorkerProcessRspMsg(pVnode, pVnode->pQuery, pMsg, 0);
    // case TDMT_SCH_CANCEL_TASK:
    //   return qWorkerProcessCancelMsg(pVnode, pVnode->pQuery, pMsg, 0);
    case TDMT_SCH_DROP_TASK:
      return qWorkerProcessDropMsg(pVnode, pVnode->pQuery, pMsg, 0);
    case TDMT_SCH_TASK_NOTIFY:
      return qWorkerProcessNotifyMsg(pVnode, pVnode->pQuery, pMsg, 0);
    case TDMT_SCH_QUERY_HEARTBEAT:
      return qWorkerProcessHbMsg(pVnode, pVnode->pQuery, pMsg, 0);
    case TDMT_VND_TABLE_META:
    case TDMT_VND_TABLE_NAME:
      return vnodeGetTableMeta(pVnode, pMsg, true);
    case TDMT_VND_TABLE_CFG:
      return vnodeGetTableCfg(pVnode, pMsg, true);
    case TDMT_VND_BATCH_META: {
      METRICS_TIMING_BLOCK(pVnode->writeMetrics.fetch_batch_meta_time, METRIC_LEVEL_LOW,
                           { code = vnodeGetBatchMeta(pVnode, pMsg); });
      METRICS_UPDATE(pVnode->writeMetrics.fetch_batch_meta_count, METRIC_LEVEL_LOW, 1);
      return code;
    }
    case TDMT_VND_VSUBTABLES_META:
      return vnodeGetVSubtablesMeta(pVnode, pMsg);
    case TDMT_VND_VSTB_REF_DBS:
      return vnodeGetVStbRefDbs(pVnode, pMsg);
    case TDMT_VND_QUERY_SCAN_PROGRESS:
      return vnodeQueryScanProgress(pVnode, pMsg);
#ifdef TD_ENTERPRISE
    case TDMT_VND_QUERY_COMPACT_PROGRESS:
      return vnodeQueryCompactProgress(pVnode, pMsg);

    case TDMT_VND_LIST_SSMIGRATE_FILESETS:
      return vnodeListSsMigrateFileSets(pVnode, pMsg);

    case TDMT_VND_QUERY_SSMIGRATE_PROGRESS:
      return vnodeQuerySsMigrateProgress(pVnode, pMsg);
#endif
    case TDMT_VND_QUERY_TRIM_PROGRESS:
      return vnodeQueryRetentionProgress(pVnode, pMsg);
      //    case TDMT_VND_TMQ_CONSUME:
      //      return tqProcessPollReq(pVnode->pTq, pMsg);
#ifdef USE_TQ
    case TDMT_VND_TMQ_VG_WALINFO:
      return tqProcessVgWalInfoReq(pVnode->pTq, pMsg);
    case TDMT_VND_TMQ_VG_COMMITTEDINFO:
      return tqProcessVgCommittedInfoReq(pVnode->pTq, pMsg);
    case TDMT_VND_TMQ_SEEK:
      return tqProcessSeekReq(pVnode->pTq, pMsg);
#endif
    default:
      vError("unknown msg type:%d in fetch queue", pMsg->msgType);
      return TSDB_CODE_APP_ERROR;
  }
}

void vnodeUpdateMetaRsp(SVnode *pVnode, STableMetaRsp *pMetaRsp) {
  if (NULL == pMetaRsp) {
    return;
  }

  tstrncpy(pMetaRsp->dbFName, pVnode->config.dbname, TSDB_DB_FNAME_LEN);
  pMetaRsp->dbId = pVnode->config.dbId;
  pMetaRsp->vgId = TD_VID(pVnode);
  pMetaRsp->precision = pVnode->config.tsdbCfg.precision;
}

extern int32_t vnodeAsyncRetention(SVnode *pVnode, STimeWindow tw, int8_t optrType, int8_t triggerType);

static int32_t vnodeProcessTrimReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  if (!pVnode->restored) {
    vInfo("vgId:%d, ignore trim req during restoring. ver:%" PRId64, TD_VID(pVnode), ver);
    return 0;
  }

  int32_t     code = 0;
  SVTrimDbReq trimReq = {0};

  // decode
  if (tDeserializeSVTrimDbReq(pReq, len, &trimReq) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  vInfo("vgId:%d, process trim vnode request, time:%" PRIi64 ",%" PRIi64 ", optr:%d, trigger:%d", pVnode->config.vgId,
        trimReq.tw.skey, trimReq.tw.ekey, (int32_t)trimReq.optrType, (int32_t)trimReq.triggerType);

  code = vnodeAsyncRetention(pVnode, trimReq.tw, (int8_t)trimReq.optrType, (int8_t)trimReq.triggerType);

_exit:
  return code;
}

extern int32_t vnodeAsyncSsMigrateFileSet(SVnode *pVnode, SSsMigrateFileSetReq *pReq);

static int32_t vnodeProcessTrimWalReq(SVnode *pVnode, void *pReq, int32_t len, SRpcMsg *pRsp) {
  int32_t code = 0;

  vInfo("vgId:%d, process trim wal request, force clean expired WAL files by triggering commit with forceTrim",
        pVnode->config.vgId);

  // Trigger a commit with forceTrim flag
  // This will properly calculate ver through sync layer and apply forceTrim during snapshot
  code = vnodeAsyncCommitEx(pVnode, true);
  if (code != TSDB_CODE_SUCCESS) {
    vError("vgId:%d, failed to trigger trim wal commit since %s", pVnode->config.vgId, tstrerror(code));
  } else {
    vInfo("vgId:%d, successfully triggered trim wal commit", pVnode->config.vgId);
  }

  return code;
}

extern int32_t vnodeAsyncS3Migrate(SVnode *pVnode, int64_t now);

static int32_t vnodeProcessSsMigrateFileSetReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  int32_t code = 0;

  SSsMigrateFileSetReq req = {0};
  SSsMigrateFileSetRsp rsp = {0};
  pRsp->msgType = TDMT_VND_SSMIGRATE_FILESET_RSP;
  pRsp->code = 0;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  if (tDeserializeSSsMigrateFileSetReq(pReq, len, &req) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  vInfo("vgId:%d, ssmigrate:%d, fid:%d, process ssmigrate fileset request, time:%" PRId64, pVnode->config.vgId,
        req.ssMigrateId, req.fid, req.startTimeSec);
  rsp.ssMigrateId = req.ssMigrateId;
  rsp.vgId = TD_VID(pVnode);
  rsp.nodeId = req.nodeId;
  rsp.fid = req.fid;

  code = vnodeAsyncSsMigrateFileSet(pVnode, &req);
  if (code != TSDB_CODE_SUCCESS) {
    vError("vgId:%d, failed to async ssmigrate since %s", TD_VID(pVnode), tstrerror(code));
    pRsp->code = code;
    goto _exit;
  }

  pRsp->code = TSDB_CODE_SUCCESS;
  pRsp->contLen = tSerializeSSsMigrateFileSetRsp(NULL, 0, &rsp);
  pRsp->pCont = rpcMallocCont(pRsp->contLen);
  if (pRsp->pCont == NULL) {
    vError("vgId:%d, failed to allocate memory for ssmigrate fileset response", TD_VID(pVnode));
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  TAOS_UNUSED(tSerializeSSsMigrateFileSetRsp(pRsp->pCont, pRsp->contLen, &rsp));

_exit:
  pRsp->code = code;
  return code;
}

extern int32_t vnodeFollowerSsMigrate(SVnode *pVnode, SSsMigrateProgress *pReq);

static int32_t vnodeProcessFollowerSsMigrateReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  int32_t            code = 0;
  SSsMigrateProgress req = {0};

  // decode
  if (tDeserializeSSsMigrateProgress(pReq, len, &req) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  code = vnodeFollowerSsMigrate(pVnode, &req);

_exit:
  pRsp->code = code;
  return code;
}

extern int32_t vnodeKillSsMigrate(SVnode *pVnode, SVnodeKillSsMigrateReq *pReq);

static int32_t vnodeProcessKillSsMigrateReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  int32_t          code = 0;
  SVnodeKillSsMigrateReq req = {0};

  // decode
  if (tDeserializeSVnodeKillSsMigrateReq(pReq, len, &req) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  code = vnodeKillSsMigrate(pVnode, &req);

_exit:
  pRsp->code = code;
  return code;
}

static int32_t vnodeProcessDropTtlTbReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  int               ret = 0;
  SVDropTtlTableReq ttlReq = {0};
  if (tDeserializeSVDropTtlTableReq(pReq, len, &ttlReq) != 0) {
    ret = TSDB_CODE_INVALID_MSG;
    goto end;
  }

  if (ttlReq.nUids != taosArrayGetSize(ttlReq.pTbUids)) {
    ret = TSDB_CODE_INVALID_MSG;
    goto end;
  }

  if (ttlReq.nUids != 0) {
    vInfo("vgId:%d, process drop ttl table request, time:%d, ntbUids:%d", pVnode->config.vgId, ttlReq.timestampSec,
          ttlReq.nUids);
  }

  if (ttlReq.nUids > 0) {
    int32_t code = metaDropMultipleTables(pVnode->pMeta, ver, ttlReq.pTbUids);
    if (code) return code;

    code = tqUpdateTbUidList(pVnode->pTq, ttlReq.pTbUids, false);
    if (code) {
      vError("vgId:%d, failed to update tbUid list since %s", TD_VID(pVnode), tstrerror(code));
    }
  }

end:
  taosArrayDestroy(ttlReq.pTbUids);
  return ret;
}

static int32_t vnodeProcessFetchTtlExpiredTbs(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  int32_t                 code = -1;
  SMetaReader             mr = {0};
  SVDropTtlTableReq       ttlReq = {0};
  SVFetchTtlExpiredTbsRsp rsp = {0};
  SEncoder                encoder = {0};
  SArray                 *pNames = NULL;
  pRsp->msgType = TDMT_VND_FETCH_TTL_EXPIRED_TBS_RSP;
  pRsp->code = TSDB_CODE_SUCCESS;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  if (tDeserializeSVDropTtlTableReq(pReq, len, &ttlReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _end;
  }

  if (!(ttlReq.nUids == taosArrayGetSize(ttlReq.pTbUids))) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _end;
  }

  tb_uid_t    suid;
  char        ctbName[TSDB_TABLE_NAME_LEN];
  SVDropTbReq expiredTb = {.igNotExists = true};
  metaReaderDoInit(&mr, pVnode->pMeta, 0);
  rsp.vgId = TD_VID(pVnode);
  rsp.pExpiredTbs = taosArrayInit(ttlReq.nUids, sizeof(SVDropTbReq));
  if (!rsp.pExpiredTbs) goto _end;

  pNames = taosArrayInit(ttlReq.nUids, TSDB_TABLE_NAME_LEN);
  if (!pNames) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _end;
  }
  char buf[TSDB_TABLE_NAME_LEN];
  for (int32_t i = 0; i < ttlReq.nUids; ++i) {
    tb_uid_t *uid = taosArrayGet(ttlReq.pTbUids, i);
    expiredTb.suid = *uid;
    terrno = metaReaderGetTableEntryByUid(&mr, *uid);
    if (terrno < 0) goto _end;
    tstrncpy(buf, mr.me.name, TSDB_TABLE_NAME_LEN);
    void *p = taosArrayPush(pNames, buf);
    if (p == NULL) {
      goto _end;
    }

    expiredTb.name = p;
    if (mr.me.type == TSDB_CHILD_TABLE) {
      expiredTb.suid = mr.me.ctbEntry.suid;
    }

    if (taosArrayPush(rsp.pExpiredTbs, &expiredTb) == NULL) {
      goto _end;
    }
  }

  int32_t ret = 0;
  tEncodeSize(tEncodeVFetchTtlExpiredTbsRsp, &rsp, pRsp->contLen, ret);
  pRsp->pCont = rpcMallocCont(pRsp->contLen);
  if (pRsp->pCont == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    code = -1;
    goto _end;
  }
  tEncoderInit(&encoder, pRsp->pCont, pRsp->contLen);
  terrno = tEncodeVFetchTtlExpiredTbsRsp(&encoder, &rsp);
  tEncoderClear(&encoder);

  if (terrno == 0) code = 0;
_end:
  metaReaderClear(&mr);
  tFreeFetchTtlExpiredTbsRsp(&rsp);
  taosArrayDestroy(ttlReq.pTbUids);
  if (pNames) taosArrayDestroy(pNames);
  pRsp->code = terrno;
  return code;
}

static int32_t vnodeProcessCreateStbReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  int32_t        code = 0;
  SVCreateStbReq req = {0};
  SDecoder       coder;

  pRsp->msgType = TDMT_VND_CREATE_STB_RSP;
  pRsp->code = TSDB_CODE_SUCCESS;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  // decode and process req
  tDecoderInit(&coder, pReq, len);

  code = tDecodeSVCreateStbReq(&coder, &req);
  if (code) {
    pRsp->code = code;
    goto _err;
  }

  code = metaCreateSuperTable(pVnode->pMeta, ver, &req);
  if (code) {
    pRsp->code = code;
    goto _err;
  }

  tDecoderClear(&coder);
  return 0;

_err:
  tDecoderClear(&coder);
  return code;
}

static int32_t vnodeProcessCreateTbReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp,
                                       SRpcMsg *pOriginRpc) {
  SDecoder           decoder = {0};
  SEncoder           encoder = {0};
  int32_t            rcode = 0;
  SVCreateTbBatchReq req = {0};
  SVCreateTbReq     *pCreateReq;
  SVCreateTbBatchRsp rsp = {0};
  SVCreateTbRsp      cRsp = {0};
  char               tbName[TSDB_TABLE_FNAME_LEN];
  SArray            *tbUids = NULL;
  SArray            *tbNames = NULL;
  pRsp->msgType = TDMT_VND_CREATE_TABLE_RSP;
  pRsp->code = TSDB_CODE_SUCCESS;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  // decode
  tDecoderInit(&decoder, pReq, len);
  if (tDecodeSVCreateTbBatchReq(&decoder, &req) < 0) {
    rcode = -1;
    terrno = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  rsp.pArray = taosArrayInit(req.nReqs, sizeof(cRsp));
  tbUids = taosArrayInit(req.nReqs, sizeof(int64_t));
  tbNames = taosArrayInit(req.nReqs, sizeof(char *));
  if (rsp.pArray == NULL || tbUids == NULL || tbNames == NULL) {
    rcode = -1;
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  // loop to create table
  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    pCreateReq = req.pReqs + iReq;
    memset(&cRsp, 0, sizeof(cRsp));

    if (tsEnableAudit && tsEnableAuditCreateTable) {
      char *str = taosMemoryCalloc(1, TSDB_TABLE_FNAME_LEN);
      if (str == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        rcode = -1;
        goto _exit;
      }
      tstrncpy(str, pCreateReq->name, TSDB_TABLE_FNAME_LEN);
      if (taosArrayPush(tbNames, &str) == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        rcode = -1;
        goto _exit;
      }
    }

    // validate hash
    (void)tsnprintf(tbName, TSDB_TABLE_FNAME_LEN, "%s.%s", pVnode->config.dbname, pCreateReq->name);
    if (vnodeValidateTableHash(pVnode, tbName) < 0) {
      cRsp.code = TSDB_CODE_VND_HASH_MISMATCH;
      if (taosArrayPush(rsp.pArray, &cRsp) == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        rcode = -1;
        goto _exit;
      }
      vError("vgId:%d create-table:%s failed due to hash value mismatch", TD_VID(pVnode), tbName);
      continue;
    }

    // do create table
    if (metaCreateTable2(pVnode->pMeta, ver, pCreateReq, &cRsp.pMeta) < 0) {
      if (pCreateReq->flags & TD_CREATE_IF_NOT_EXISTS && terrno == TSDB_CODE_TDB_TABLE_ALREADY_EXIST) {
        cRsp.code = TSDB_CODE_SUCCESS;
      } else {
        cRsp.code = terrno;
      }
    } else {
      cRsp.code = TSDB_CODE_SUCCESS;
      if (taosArrayPush(tbUids, &pCreateReq->uid) == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        rcode = -1;
        goto _exit;
      }
      vnodeUpdateMetaRsp(pVnode, cRsp.pMeta);
    }

    if (taosArrayPush(rsp.pArray, &cRsp) == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      rcode = -1;
      goto _exit;
    }
  }

  vTrace("vgId:%d, add %d new created tables into query table list", TD_VID(pVnode), (int32_t)taosArrayGetSize(tbUids));
  if (tqUpdateTbUidList(pVnode->pTq, tbUids, true) < 0) {
    vError("vgId:%d, failed to update tbUid list since %s", TD_VID(pVnode), tstrerror(terrno));
  }

  // prepare rsp
  int32_t ret = 0;
  tEncodeSize(tEncodeSVCreateTbBatchRsp, &rsp, pRsp->contLen, ret);
  pRsp->pCont = rpcMallocCont(pRsp->contLen);
  if (pRsp->pCont == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    rcode = -1;
    goto _exit;
  }
  tEncoderInit(&encoder, pRsp->pCont, pRsp->contLen);
  if (tEncodeSVCreateTbBatchRsp(&encoder, &rsp) != 0) {
    vError("vgId:%d, failed to encode create table batch response", TD_VID(pVnode));
  }

  if (tsEnableAudit && tsEnableAuditCreateTable) {
    int64_t clusterId = pVnode->config.syncCfg.nodeInfo[0].clusterId;

    SName name = {0};
    if (tNameFromString(&name, pVnode->config.dbname, T_NAME_ACCT | T_NAME_DB) < 0) {
      vError("vgId:%d, failed to get name from string", TD_VID(pVnode));
    }

    SStringBuilder sb = {0};
    for (int32_t i = 0; i < tbNames->size; i++) {
      char **key = (char **)taosArrayGet(tbNames, i);
      taosStringBuilderAppendStringLen(&sb, *key, strlen(*key));
      if (i < tbNames->size - 1) {
        taosStringBuilderAppendChar(&sb, ',');
      }
      // taosMemoryFreeClear(*key);
    }

    size_t len = 0;
    char  *keyJoined = taosStringBuilderGetResult(&sb, &len);

    if (pOriginRpc->info.conn.user != NULL && strlen(pOriginRpc->info.conn.user) > 0) {
      auditAddRecord(pOriginRpc, clusterId, "createTable", name.dbname, "", keyJoined, len);
    }

    taosStringBuilderDestroy(&sb);
  }

_exit:
  tDeleteSVCreateTbBatchReq(&req);
  taosArrayDestroyEx(rsp.pArray, tFreeSVCreateTbRsp);
  taosArrayDestroy(tbUids);
  tDecoderClear(&decoder);
  tEncoderClear(&encoder);
  taosArrayDestroyP(tbNames, NULL);
  return rcode;
}

static int32_t vnodeProcessAlterStbReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  int32_t        code = 0;
  SVCreateStbReq req = {0};
  SDecoder       dc = {0};

  pRsp->msgType = TDMT_VND_ALTER_STB_RSP;
  pRsp->code = TSDB_CODE_SUCCESS;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  tDecoderInit(&dc, pReq, len);

  // decode req
  code = tDecodeSVCreateStbReq(&dc, &req);
  if (code) {
    tDecoderClear(&dc);
    return code;
  }

  code = metaAlterSuperTable(pVnode->pMeta, ver, &req);
  if (code) {
    pRsp->code = code;
    tDecoderClear(&dc);
    return code;
  }

  tDecoderClear(&dc);

  return 0;
}

static int32_t vnodeProcessDropStbReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp, SRpcMsg *pOriginRpc) {
  SVDropStbReq req = {0};
  int32_t      rcode = TSDB_CODE_SUCCESS;
  SDecoder     decoder = {0};
  SArray      *tbUidList = NULL;

  pRsp->msgType = TDMT_VND_CREATE_STB_RSP;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  // decode request
  tDecoderInit(&decoder, pReq, len);
  if (tDecodeSVDropStbReq(&decoder, &req) < 0) {
    rcode = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  STraceId* trace = &(pOriginRpc->info.traceId);

  vInfo("vgId:%d, start to process vnode-drop-stb, QID:0x%" PRIx64 ":0x%" PRIx64 ", drop stb:%s", TD_VID(pVnode), trace ? trace->rootId : 0, 
              trace ? trace->msgId : 0, req.name);

  // process request
  tbUidList = taosArrayInit(8, sizeof(int64_t));
  if (tbUidList == NULL) goto _exit;
  if (metaDropSuperTable(pVnode->pMeta, ver, &req) < 0) {
    rcode = terrno;
    goto _exit;
  }

  if (tqUpdateTbUidList(pVnode->pTq, tbUidList, false) < 0) {
    rcode = terrno;
    goto _exit;
  }

  // return rsp
_exit:
  vInfo("vgId:%d, finished to process vnode-drop-stb, QID:0x%" PRIx64 ":0x%" PRIx64 ", drop stb:%s", TD_VID(pVnode), trace ? trace->rootId : 0, 
              trace ? trace->msgId : 0, req.name);
  if (tbUidList) taosArrayDestroy(tbUidList);
  pRsp->code = rcode;
  tDecoderClear(&decoder);
  return 0;
}

static int32_t vnodeProcessAlterTbReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  SVAlterTbReq  vAlterTbReq = {0};
  SVAlterTbRsp  vAlterTbRsp = {0};
  SDecoder      dc = {0};
  int32_t       code = 0;
  int32_t       lino = 0;
  int32_t       ret;
  SEncoder      ec = {0};
  STableMetaRsp vMetaRsp = {0};

  pRsp->msgType = TDMT_VND_ALTER_TABLE_RSP;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;
  pRsp->code = TSDB_CODE_SUCCESS;

  tDecoderInit(&dc, pReq, len);

  // decode
  if (tDecodeSVAlterTbReq(&dc, &vAlterTbReq) < 0) {
    vAlterTbRsp.code = TSDB_CODE_INVALID_MSG;
    tDecoderClear(&dc);
    goto _exit;
  }

  // process
  if (metaAlterTable(pVnode->pMeta, ver, &vAlterTbReq, &vMetaRsp) < 0) {
    vAlterTbRsp.code = terrno;
    tDecoderClear(&dc);
    goto _exit;
  }
  tDecoderClear(&dc);

  if (NULL != vMetaRsp.pSchemas) {
    vnodeUpdateMetaRsp(pVnode, &vMetaRsp);
    vAlterTbRsp.pMeta = &vMetaRsp;
  }

  if (vAlterTbReq.action == TSDB_ALTER_TABLE_UPDATE_TAG_VAL ||
      vAlterTbReq.action == TSDB_ALTER_TABLE_UPDATE_MULTI_TAG_VAL) {
    int64_t uid = metaGetTableEntryUidByName(pVnode->pMeta, vAlterTbReq.tbName);
    if (uid == 0) {
      vError("vgId:%d, %s failed at %s:%d since table %s not found", TD_VID(pVnode), __func__, __FILE__, __LINE__,
             vAlterTbReq.tbName);
      goto _exit;
    }

    SArray *tbUids = taosArrayInit(4, sizeof(int64_t));
    void   *p = taosArrayPush(tbUids, &uid);
    TSDB_CHECK_NULL(p, code, lino, _exit, terrno);

    vDebug("vgId:%d, remove tags value altered table:%s from query table list", TD_VID(pVnode), vAlterTbReq.tbName);
    if ((code = tqUpdateTbUidList(pVnode->pTq, tbUids, false)) < 0) {
      vError("vgId:%d, failed to remove tbUid list since %s", TD_VID(pVnode), tstrerror(code));
    }

    vDebug("vgId:%d, try to add table:%s in query table list", TD_VID(pVnode), vAlterTbReq.tbName);
    if ((code = tqUpdateTbUidList(pVnode->pTq, tbUids, true)) < 0) {
      vError("vgId:%d, failed to add tbUid list since %s", TD_VID(pVnode), tstrerror(code));
    }

    taosArrayDestroy(tbUids);
  }

_exit:
  taosArrayDestroy(vAlterTbReq.pMultiTag);
  tEncodeSize(tEncodeSVAlterTbRsp, &vAlterTbRsp, pRsp->contLen, ret);
  pRsp->pCont = rpcMallocCont(pRsp->contLen);
  tEncoderInit(&ec, pRsp->pCont, pRsp->contLen);
  if (tEncodeSVAlterTbRsp(&ec, &vAlterTbRsp) != 0) {
    vError("vgId:%d, failed to encode alter table response", TD_VID(pVnode));
  }

  tEncoderClear(&ec);
  if (vMetaRsp.pSchemas) {
    taosMemoryFree(vMetaRsp.pSchemas);
    taosMemoryFree(vMetaRsp.pSchemaExt);
  }
  if (vMetaRsp.pColRefs) {
    taosMemoryFree(vMetaRsp.pColRefs);
  }
  return 0;
}

static int32_t vnodeProcessDropTbReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp,
                                     SRpcMsg *pOriginRpc) {
  SVDropTbBatchReq req = {0};
  SVDropTbBatchRsp rsp = {0};
  SDecoder         decoder = {0};
  SEncoder         encoder = {0};
  int32_t          ret;
  SArray          *tbUids = NULL;
  SArray          *tbNames = NULL;

  pRsp->msgType = ((SRpcMsg *)pReq)->msgType + 1;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;
  pRsp->code = TSDB_CODE_SUCCESS;

  // decode req
  tDecoderInit(&decoder, pReq, len);
  ret = tDecodeSVDropTbBatchReq(&decoder, &req);
  if (ret < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    pRsp->code = terrno;
    goto _exit;
  }

  // process req
  tbUids = taosArrayInit(req.nReqs, sizeof(int64_t));
  rsp.pArray = taosArrayInit(req.nReqs, sizeof(SVDropTbRsp));
  tbNames = taosArrayInit(req.nReqs, sizeof(char *));
  if (tbUids == NULL || rsp.pArray == NULL || tbNames == NULL) goto _exit;

  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    SVDropTbReq *pDropTbReq = req.pReqs + iReq;
    SVDropTbRsp  dropTbRsp = {0};
    tb_uid_t     tbUid = 0;

    /* code */
    ret = metaDropTable2(pVnode->pMeta, ver, pDropTbReq);
    if (ret < 0) {
      if (pDropTbReq->igNotExists && terrno == TSDB_CODE_TDB_TABLE_NOT_EXIST) {
        dropTbRsp.code = TSDB_CODE_SUCCESS;
      } else {
        dropTbRsp.code = terrno;
      }
    } else {
      dropTbRsp.code = TSDB_CODE_SUCCESS;
    }

    if (taosArrayPush(rsp.pArray, &dropTbRsp) == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      pRsp->code = terrno;
      goto _exit;
    }

    if (tsEnableAuditCreateTable) {
      char *str = taosMemoryCalloc(1, TSDB_TABLE_FNAME_LEN);
      if (str == NULL) {
        pRsp->code = terrno;
        goto _exit;
      }
      tstrncpy(str, pDropTbReq->name, TSDB_TABLE_FNAME_LEN);
      if (taosArrayPush(tbNames, &str) == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        pRsp->code = terrno;
        goto _exit;
      }
    }
  }

  if (tqUpdateTbUidList(pVnode->pTq, tbUids, false) < 0) {
    vError("vgId:%d, failed to update tbUid list since %s", TD_VID(pVnode), tstrerror(terrno));
  }

  if (tsEnableAuditCreateTable) {
    int64_t clusterId = pVnode->config.syncCfg.nodeInfo[0].clusterId;

    SName name = {0};
    if (tNameFromString(&name, pVnode->config.dbname, T_NAME_ACCT | T_NAME_DB) != 0) {
      vError("vgId:%d, failed to get name from string", TD_VID(pVnode));
    }

    SStringBuilder sb = {0};
    for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
      char **key = (char **)taosArrayGet(tbNames, iReq);
      taosStringBuilderAppendStringLen(&sb, *key, strlen(*key));
      if (iReq < req.nReqs - 1) {
        taosStringBuilderAppendChar(&sb, ',');
      }
      taosMemoryFreeClear(*key);
    }

    size_t len = 0;
    char  *keyJoined = taosStringBuilderGetResult(&sb, &len);

    if (pOriginRpc->info.conn.user != NULL && strlen(pOriginRpc->info.conn.user) > 0) {
      auditAddRecord(pOriginRpc, clusterId, "dropTable", name.dbname, "", keyJoined, len);
    }

    taosStringBuilderDestroy(&sb);
  }

_exit:
  taosArrayDestroy(tbUids);
  tDecoderClear(&decoder);
  tEncodeSize(tEncodeSVDropTbBatchRsp, &rsp, pRsp->contLen, ret);
  pRsp->pCont = rpcMallocCont(pRsp->contLen);
  tEncoderInit(&encoder, pRsp->pCont, pRsp->contLen);
  if (tEncodeSVDropTbBatchRsp(&encoder, &rsp) != 0) {
    vError("vgId:%d, failed to encode drop table batch response", TD_VID(pVnode));
  }
  tEncoderClear(&encoder);
  taosArrayDestroy(rsp.pArray);
  taosArrayDestroy(tbNames);
  return 0;
}

static int32_t vnodeProcessCreateRsmaReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  int32_t         code = 0, lino = 0;
  SVCreateRsmaReq req = {0};
  TAOS_CHECK_EXIT(tDeserializeSVCreateRsmaReq(pReq, len, &req));
  TAOS_CHECK_EXIT(metaCreateRsma(pVnode->pMeta, ver, &req));
_exit:
  pRsp->msgType = TDMT_VND_CREATE_RSMA_RSP;
  pRsp->pCont = NULL;
  pRsp->code = code;
  pRsp->contLen = 0;
  tFreeSVCreateRsmaReq(&req);
  return code;
}

static int32_t vnodeProcessDropRsmaReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  int32_t       code = 0, lino = 0;
  SVDropRsmaReq req = {0};
  TAOS_CHECK_EXIT(tDeserializeSVDropRsmaReq(pReq, len, &req));
  TAOS_CHECK_EXIT(metaDropRsma(pVnode->pMeta, ver, &req));
_exit:
  pRsp->msgType = TDMT_VND_DROP_RSMA_RSP;
  pRsp->pCont = NULL;
  pRsp->code = code;
  pRsp->contLen = 0;
  return 0;
}

static int32_t vnodeProcessAlterRsmaReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  int32_t         code = 0, lino = 0;
  SVAlterRsmaReq req = {0};
  TAOS_CHECK_EXIT(tDeserializeSVAlterRsmaReq(pReq, len, &req));
  TAOS_CHECK_EXIT(metaAlterRsma(pVnode->pMeta, ver, &req));
_exit:
  pRsp->msgType = TDMT_VND_ALTER_RSMA_RSP;
  pRsp->pCont = NULL;
  pRsp->code = code;
  pRsp->contLen = 0;
  tFreeSVAlterRsmaReq(&req);
  return code;
}

#ifdef BUILD_NO_CALL
static int32_t vnodeDebugPrintSingleSubmitMsg(SMeta *pMeta, SSubmitBlk *pBlock, SSubmitMsgIter *msgIter,
                                              const char *tags) {
  SSubmitBlkIter blkIter = {0};
  STSchema      *pSchema = NULL;
  tb_uid_t       suid = 0;
  STSRow        *row = NULL;
  int32_t        rv = -1;

  tInitSubmitBlkIter(msgIter, pBlock, &blkIter);
  if (blkIter.row == NULL) return 0;

  int32_t code = metaGetTbTSchemaNotNull(pMeta, msgIter->suid, TD_ROW_SVER(blkIter.row), 1,
                                         &pSchema);  // TODO: use the real schema
  if (TSDB_CODE_SUCCESS != code) {
    printf("%s:%d no valid schema\n", tags, __LINE__);
    return code;
  }

  suid = msgIter->suid;
  rv = TD_ROW_SVER(blkIter.row);

  char __tags[128] = {0};
  snprintf(__tags, 128, "%s: uid %" PRIi64 " ", tags, msgIter->uid);
  while ((row = tGetSubmitBlkNext(&blkIter))) {
    tdSRowPrint(row, pSchema, __tags);
  }

  taosMemoryFreeClear(pSchema);

  return TSDB_CODE_SUCCESS;
}
#endif
typedef struct SSubmitReqConvertCxt {
  SSubmitMsgIter msgIter;
  SSubmitBlk    *pBlock;
  SSubmitBlkIter blkIter;
  STSRow        *pRow;
  STSRowIter     rowIter;
  SSubmitTbData *pTbData;
  STSchema      *pTbSchema;
  SArray        *pColValues;
} SSubmitReqConvertCxt;

static int32_t vnodeResetTableCxt(SMeta *pMeta, SSubmitReqConvertCxt *pCxt) {
  taosMemoryFreeClear(pCxt->pTbSchema);
  int32_t code = metaGetTbTSchemaNotNull(pMeta, pCxt->msgIter.suid > 0 ? pCxt->msgIter.suid : pCxt->msgIter.uid,
                                         pCxt->msgIter.sversion, 1, &pCxt->pTbSchema);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  tdSTSRowIterInit(&pCxt->rowIter, pCxt->pTbSchema);

  tDestroySubmitTbData(pCxt->pTbData, TSDB_MSG_FLG_ENCODE);
  if (NULL == pCxt->pTbData) {
    pCxt->pTbData = taosMemoryCalloc(1, sizeof(SSubmitTbData));
    if (NULL == pCxt->pTbData) {
      return terrno;
    }
  }
  pCxt->pTbData->flags = 0;
  pCxt->pTbData->suid = pCxt->msgIter.suid;
  pCxt->pTbData->uid = pCxt->msgIter.uid;
  pCxt->pTbData->sver = pCxt->msgIter.sversion;
  pCxt->pTbData->pCreateTbReq = NULL;
  pCxt->pTbData->aRowP = taosArrayInit(128, POINTER_BYTES);
  if (NULL == pCxt->pTbData->aRowP) {
    return terrno;
  }

  taosArrayDestroy(pCxt->pColValues);
  pCxt->pColValues = taosArrayInit(pCxt->pTbSchema->numOfCols, sizeof(SColVal));
  if (NULL == pCxt->pColValues) {
    return terrno;
  }
  for (int32_t i = 0; i < pCxt->pTbSchema->numOfCols; ++i) {
    SColVal val = COL_VAL_NONE(pCxt->pTbSchema->columns[i].colId, pCxt->pTbSchema->columns[i].type);
    if (taosArrayPush(pCxt->pColValues, &val) == NULL) {
      return terrno;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static void vnodeDestroySubmitReqConvertCxt(SSubmitReqConvertCxt *pCxt) {
  taosMemoryFreeClear(pCxt->pTbSchema);
  tDestroySubmitTbData(pCxt->pTbData, TSDB_MSG_FLG_ENCODE);
  taosMemoryFreeClear(pCxt->pTbData);
  taosArrayDestroy(pCxt->pColValues);
}

static int32_t vnodeCellValConvertToColVal(STColumn *pCol, SCellVal *pCellVal, SColVal *pColVal) {
  if (tdValTypeIsNone(pCellVal->valType)) {
    pColVal->flag = CV_FLAG_NONE;
    return TSDB_CODE_SUCCESS;
  }

  if (tdValTypeIsNull(pCellVal->valType)) {
    pColVal->flag = CV_FLAG_NULL;
    return TSDB_CODE_SUCCESS;
  }

  if (IS_VAR_DATA_TYPE(pCol->type)) {
    if (IS_STR_DATA_BLOB(pCol->type)) {
      pColVal->value.nData = blobDataLen(pCellVal->val);
      pColVal->value.pData = (uint8_t *)blobDataVal(pCellVal->val);

    } else {
      pColVal->value.nData = varDataLen(pCellVal->val);
      pColVal->value.pData = (uint8_t *)varDataVal(pCellVal->val);
    }
  } else if (TSDB_DATA_TYPE_FLOAT == pCol->type) {
    float f = GET_FLOAT_VAL(pCellVal->val);
    valueSetDatum(&pColVal->value, pCol->type, &f, sizeof(f));
  } else if (TSDB_DATA_TYPE_DOUBLE == pCol->type) {
    taosSetPInt64Aligned(&pColVal->value.val, (int64_t *)pCellVal->val);
  } else {
    valueSetDatum(&pColVal->value, pCol->type, pCellVal->val, tDataTypes[pCol->type].bytes);
  }

  pColVal->flag = CV_FLAG_VALUE;
  return TSDB_CODE_SUCCESS;
}

static int32_t vnodeTSRowConvertToColValArray(SSubmitReqConvertCxt *pCxt) {
  int32_t code = TSDB_CODE_SUCCESS;
  tdSTSRowIterReset(&pCxt->rowIter, pCxt->pRow);
  for (int32_t i = 0; TSDB_CODE_SUCCESS == code && i < pCxt->pTbSchema->numOfCols; ++i) {
    STColumn *pCol = pCxt->pTbSchema->columns + i;
    SCellVal  cellVal = {0};
    if (!tdSTSRowIterFetch(&pCxt->rowIter, pCol->colId, pCol->type, &cellVal)) {
      break;
    }
    code = vnodeCellValConvertToColVal(pCol, &cellVal, (SColVal *)taosArrayGet(pCxt->pColValues, i));
  }
  return code;
}

static int32_t vnodeDecodeCreateTbReq(SSubmitReqConvertCxt *pCxt) {
  if (pCxt->msgIter.schemaLen <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  pCxt->pTbData->pCreateTbReq = taosMemoryCalloc(1, sizeof(SVCreateTbReq));
  if (NULL == pCxt->pTbData->pCreateTbReq) {
    return terrno;
  }

  SDecoder decoder = {0};
  tDecoderInit(&decoder, (uint8_t *)pCxt->pBlock->data, pCxt->msgIter.schemaLen);
  int32_t code = tDecodeSVCreateTbReq(&decoder, pCxt->pTbData->pCreateTbReq);
  tDecoderClear(&decoder);

  return code;
}

static int32_t vnodeSubmitReqConvertToSubmitReq2(SVnode *pVnode, SSubmitReq *pReq, SSubmitReq2 *pReq2) {
  pReq2->aSubmitTbData = taosArrayInit(128, sizeof(SSubmitTbData));
  if (NULL == pReq2->aSubmitTbData) {
    return terrno;
  }

  SSubmitReqConvertCxt cxt = {0};

  int32_t code = tInitSubmitMsgIter(pReq, &cxt.msgIter);
  while (TSDB_CODE_SUCCESS == code) {
    code = tGetSubmitMsgNext(&cxt.msgIter, &cxt.pBlock);
    if (TSDB_CODE_SUCCESS == code) {
      if (NULL == cxt.pBlock) {
        break;
      }
      code = vnodeResetTableCxt(pVnode->pMeta, &cxt);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = tInitSubmitBlkIter(&cxt.msgIter, cxt.pBlock, &cxt.blkIter);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = vnodeDecodeCreateTbReq(&cxt);
    }
    while (TSDB_CODE_SUCCESS == code && (cxt.pRow = tGetSubmitBlkNext(&cxt.blkIter)) != NULL) {
      code = vnodeTSRowConvertToColValArray(&cxt);
      if (TSDB_CODE_SUCCESS == code) {
        SRow **pNewRow = taosArrayReserve(cxt.pTbData->aRowP, 1);

        SRowBuildScanInfo sinfo = {0};
        code = tRowBuild(cxt.pColValues, cxt.pTbSchema, pNewRow, &sinfo);
      }
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = (NULL == taosArrayPush(pReq2->aSubmitTbData, cxt.pTbData) ? terrno : TSDB_CODE_SUCCESS);
    }
    if (TSDB_CODE_SUCCESS == code) {
      taosMemoryFreeClear(cxt.pTbData);
    }
  }

  vnodeDestroySubmitReqConvertCxt(&cxt);
  return code;
}

static int32_t vnodeRebuildSubmitReqMsg(SSubmitReq2 *pSubmitReq, void **ppMsg) {
  int32_t  code = TSDB_CODE_SUCCESS;
  char    *pMsg = NULL;
  uint32_t msglen = 0;
  tEncodeSize(tEncodeSubmitReq, pSubmitReq, msglen, code);
  if (TSDB_CODE_SUCCESS == code) {
    pMsg = taosMemoryMalloc(msglen);
    if (NULL == pMsg) {
      code = terrno;
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    SEncoder encoder;
    tEncoderInit(&encoder, (uint8_t *)pMsg, msglen);
    code = tEncodeSubmitReq(&encoder, pSubmitReq);
    tEncoderClear(&encoder);
  }
  if (TSDB_CODE_SUCCESS == code) {
    *ppMsg = pMsg;
  }
  return code;
}

static int32_t buildExistSubTalbeRsp(SVnode *pVnode, SSubmitTbData *pSubmitTbData, STableMetaRsp **ppRsp) {
  int32_t code = 0;
  int32_t lino = 0;

  SMetaEntry *pEntry = NULL;
  code = metaFetchEntryByUid(pVnode->pMeta, pSubmitTbData->suid, &pEntry);
  if (code) {
    vError("vgId:%d, table uid:%" PRId64 " not exists, line:%d", TD_VID(pVnode), pSubmitTbData->uid, __LINE__);
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  if (pEntry->type != TSDB_SUPER_TABLE) {
    vError("vgId:%d, table uid:%" PRId64 " exists, but is not super table, line:%d", TD_VID(pVnode), pSubmitTbData->uid,
           __LINE__);
    code = TSDB_CODE_STREAM_INSERT_SCHEMA_NOT_MATCH;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  *ppRsp = taosMemoryCalloc(1, sizeof(STableMetaRsp));
  if (NULL == *ppRsp) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }
  (*ppRsp)->suid = pSubmitTbData->suid;
  (*ppRsp)->tuid = pSubmitTbData->uid;
  (*ppRsp)->sversion = pEntry->stbEntry.schemaRow.version;
  (*ppRsp)->vgId = pVnode->config.vgId;
  (*ppRsp)->numOfColumns = pEntry->stbEntry.schemaRow.nCols;
  (*ppRsp)->numOfTags = pEntry->stbEntry.schemaTag.nCols;
  (*ppRsp)->pSchemas =
      taosMemoryCalloc(pEntry->stbEntry.schemaRow.nCols + pEntry->stbEntry.schemaTag.nCols, sizeof(SSchema));
  if (NULL == (*ppRsp)->pSchemas) {
    taosMemoryFree(*ppRsp);
    *ppRsp = NULL;
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }
  memcpy((*ppRsp)->pSchemas, pEntry->stbEntry.schemaRow.pSchema, pEntry->stbEntry.schemaRow.nCols * sizeof(SSchema));
  memcpy((*ppRsp)->pSchemas + pEntry->stbEntry.schemaRow.nCols, pEntry->stbEntry.schemaTag.pSchema,
         pEntry->stbEntry.schemaTag.nCols * sizeof(SSchema));
  if (pEntry->pExtSchemas != NULL) {
    (*ppRsp)->pSchemaExt = taosMemoryCalloc(pEntry->colCmpr.nCols, sizeof(SExtSchema));
    if (NULL == (*ppRsp)->pSchemaExt) {
      taosMemoryFree((*ppRsp)->pSchemas);
      taosMemoryFree(*ppRsp);
      *ppRsp = NULL;
      TSDB_CHECK_CODE(code = terrno, lino, _exit);
    }
    memcpy((*ppRsp)->pSchemaExt, pEntry->pExtSchemas, pEntry->colCmpr.nCols * sizeof(SExtSchema));
  }

  if (pEntry->stbEntry.schemaRow.version != pSubmitTbData->sver) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_TABLE_ALREADY_EXIST, lino, _exit);
  }
_exit:
  metaFetchEntryFree(&pEntry);
  if (code != TSDB_CODE_SUCCESS) {
    vError("vgId:%d, failed to build exist sub table response, code:0x%0x, line:%d", TD_VID(pVnode), code, lino);
  }
  return code;
}

static int32_t buildExistNormalTalbeRsp(SVnode *pVnode, int64_t uid, STableMetaRsp **ppRsp) {
  int32_t code = 0;
  int32_t lino = 0;

  SMetaEntry *pEntry = NULL;
  code = metaFetchEntryByUid(pVnode->pMeta, uid, &pEntry);
  if (code) {
    vError("vgId:%d, table uid:%" PRId64 " not exists, line:%d", TD_VID(pVnode), uid, __LINE__);
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  if (pEntry->type != TSDB_NORMAL_TABLE) {
    vError("vgId:%d, table uid:%" PRId64 " exists, but is not normal table, line:%d", TD_VID(pVnode), uid, __LINE__);
    code = TSDB_CODE_STREAM_INSERT_SCHEMA_NOT_MATCH;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  *ppRsp = taosMemoryCalloc(1, sizeof(STableMetaRsp));
  if (NULL == *ppRsp) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }

  (*ppRsp)->tuid = pEntry->uid;
  (*ppRsp)->vgId = pVnode->config.vgId;
  (*ppRsp)->sversion = pEntry->ntbEntry.schemaRow.version;
  (*ppRsp)->numOfColumns = pEntry->ntbEntry.schemaRow.nCols;
  (*ppRsp)->pSchemas = taosMemoryCalloc(pEntry->ntbEntry.schemaRow.nCols, sizeof(SSchema));
  if (NULL == (*ppRsp)->pSchemas) {
    taosMemoryFree(*ppRsp);
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }
  memcpy((*ppRsp)->pSchemas, pEntry->ntbEntry.schemaRow.pSchema, pEntry->ntbEntry.schemaRow.nCols * sizeof(SSchema));
  if (pEntry->pExtSchemas != NULL) {
    (*ppRsp)->pSchemaExt = taosMemoryCalloc(pEntry->ntbEntry.schemaRow.nCols, sizeof(SSchemaExt));
    if (NULL == (*ppRsp)->pSchemaExt) {
      taosMemoryFree((*ppRsp)->pSchemas);
      taosMemoryFree(*ppRsp);
      TSDB_CHECK_CODE(code = terrno, lino, _exit);
    }
    memcpy((*ppRsp)->pSchemaExt, pEntry->pExtSchemas, pEntry->ntbEntry.schemaRow.nCols * sizeof(SSchemaExt));
  }

_exit:
  metaFetchEntryFree(&pEntry);
  if (code != TSDB_CODE_SUCCESS) {
    vError("vgId:%d, failed to build exist normal table response, code:%d, line:%d", TD_VID(pVnode), code, lino);
  }
  return code;
}

static int32_t buildExistTableInStreamRsp(SVnode *pVnode, SSubmitTbData *pSubmitTbData, STableMetaRsp **ppRsp) {
  if (pSubmitTbData->pCreateTbReq->flags & TD_CREATE_NORMAL_TB_IN_STREAM) {
    int32_t code = buildExistNormalTalbeRsp(pVnode, pSubmitTbData->uid, ppRsp);
    if (code) {
      vError("vgId:%d, table uid:%" PRId64 " not exists, line:%d", TD_VID(pVnode), pSubmitTbData->uid, __LINE__);
      return code;
    }
    return TSDB_CODE_TDB_TABLE_ALREADY_EXIST;
  } else if (pSubmitTbData->pCreateTbReq->flags & TD_CREATE_SUB_TB_IN_STREAM) {
    return buildExistSubTalbeRsp(pVnode, pSubmitTbData, ppRsp);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t vnodeHandleAutoCreateTable(SVnode      *pVnode,    // vnode
                                          int64_t      version,   // version
                                          SSubmitReq2 *pRequest,  // request
                                          SSubmitRsp2 *pResponse  // response
) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t numTbData = taosArrayGetSize(pRequest->aSubmitTbData);
  SArray *newTbUids = NULL;

  for (int32_t i = 0; i < numTbData; ++i) {
    SSubmitTbData *pTbData = taosArrayGet(pRequest->aSubmitTbData, i);

    if (pTbData->pCreateTbReq == NULL) {
      continue;
    }

    pTbData->uid = pTbData->pCreateTbReq->uid;

    // Alloc necessary resources
    if (pResponse->aCreateTbRsp == NULL) {
      pResponse->aCreateTbRsp = taosArrayInit(numTbData, sizeof(SVCreateTbRsp));
      if (pResponse->aCreateTbRsp == NULL) {
        code = terrno;
        vError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64, TD_VID(pVnode), __func__, __FILE__, __LINE__,
               tstrerror(code), version);
        taosArrayDestroy(newTbUids);
        return code;
      }
    }

    // Do create table
    vDebug("vgId:%d start to handle auto create table, version:%" PRId64, TD_VID(pVnode), version);

    SVCreateTbRsp *pCreateTbRsp = taosArrayReserve(pResponse->aCreateTbRsp, 1);
    code = metaCreateTable2(pVnode->pMeta, version, pTbData->pCreateTbReq, &pCreateTbRsp->pMeta);
    if (code == TSDB_CODE_SUCCESS) {
      // Allocate necessary resources
      if (newTbUids == NULL) {
        newTbUids = taosArrayInit(numTbData, sizeof(int64_t));
        if (newTbUids == NULL) {
          code = terrno;
          vError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64, TD_VID(pVnode), __func__, __FILE__, __LINE__,
                 tstrerror(code), version);
          return code;
        }
      }

      if (taosArrayPush(newTbUids, &pTbData->uid) == NULL) {
        code = terrno;
        vError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64, TD_VID(pVnode), __func__, __FILE__, __LINE__,
               tstrerror(code), version);
        taosArrayDestroy(newTbUids);
        return code;
      }

      if (pCreateTbRsp->pMeta) {
        vnodeUpdateMetaRsp(pVnode, pCreateTbRsp->pMeta);
      }
    } else if (code == TSDB_CODE_TDB_TABLE_ALREADY_EXIST) {
      code = terrno = 0;
      pTbData->uid = pTbData->pCreateTbReq->uid;  // update uid if table exist for using below

      // stream: get sver from meta, write to pCreateTbRsp, and need to check crateTbReq is same as meta.
      if (i == 0) {
        // In the streaming scenario, multiple grouped req requests will only operate on the same write table, and
        // only the first one needs to be processed.
        code = buildExistTableInStreamRsp(pVnode, pTbData, &pCreateTbRsp->pMeta);
        if (code) {
          vInfo("vgId:%d failed to create table in stream:%s, code(0x%0x):%s", TD_VID(pVnode),
                pTbData->pCreateTbReq->name, code, tstrerror(code));
          taosArrayDestroy(newTbUids);
          return code;
        }
      }
    } else {
      code = terrno;
      vError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64, TD_VID(pVnode), __func__, __FILE__, __LINE__,
             tstrerror(code), version);
      taosArrayDestroy(newTbUids);
      return code;
    }
  }

  // Update the affected table uid list
  if (taosArrayGetSize(newTbUids) > 0) {
    vDebug("vgId:%d, add %d table into query table list in handling submit", TD_VID(pVnode),
           (int32_t)taosArrayGetSize(newTbUids));
    if (tqUpdateTbUidList(pVnode->pTq, newTbUids, true) != 0) {
      vError("vgId:%d, failed to update tbUid list", TD_VID(pVnode));
    }
  }

  vDebug("vgId:%d, handle auto create table done, version:%" PRId64, TD_VID(pVnode), version);

  taosArrayDestroy(newTbUids);
  return code;
}

static void addExistTableInfoIntoRes(SVnode *pVnode, SSubmitReq2 *pRequest, SSubmitRsp2 *pResponse,
                                     SSubmitTbData *pTbData, int32_t numTbData) {
  int32_t code = 0;
  int32_t lino = 0;
  if ((pTbData->flags & SUBMIT_REQ_SCHEMA_RES) == 0) {
    return;
  }
  if (pResponse->aCreateTbRsp) {  // If aSubmitTbData is not NULL, it means that the request is a create table request,
                                  // so table info has exitst and we do not need to add again.
    return;
  }
  pResponse->aCreateTbRsp = taosArrayInit(numTbData, sizeof(SVCreateTbRsp));
  if (pResponse->aCreateTbRsp == NULL) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  SVCreateTbRsp *pCreateTbRsp = taosArrayReserve(pResponse->aCreateTbRsp, 1);
  if (pCreateTbRsp == NULL) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  if (pTbData->suid == 0) {
    code = buildExistNormalTalbeRsp(pVnode, pTbData->uid, &pCreateTbRsp->pMeta);
    if (code) {
      vError("vgId:%d, table uid:%" PRId64 " not exists, line:%d", TD_VID(pVnode), pTbData->uid, __LINE__);
    }
  } else {
    code = buildExistSubTalbeRsp(pVnode, pTbData, &pCreateTbRsp->pMeta);
  }

  TSDB_CHECK_CODE(code, lino, _exit);
_exit:
  if (code != TSDB_CODE_SUCCESS) {
    vError("vgId:%d, failed to add exist table info into response, code:0x%0x, line:%d", TD_VID(pVnode), code, lino);
  }
  return;
}

static int32_t vnodeHandleDataWrite(SVnode *pVnode, int64_t version, SSubmitReq2 *pRequest, SSubmitRsp2 *pResponse) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t numTbData = taosArrayGetSize(pRequest->aSubmitTbData);
  int8_t  hasBlob = 0;

  // Scan submit data
  for (int32_t i = 0; i < numTbData; ++i) {
    SMetaInfo      info = {0};
    SSubmitTbData *pTbData = taosArrayGet(pRequest->aSubmitTbData, i);

    if (pTbData->flags & SUBMIT_REQ_WITH_BLOB) {
      hasBlob = 1;
    }
    if (pTbData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
      continue;  // skip column data format
    }
    if (pTbData->flags & SUBMIT_REQ_ONLY_CREATE_TABLE) {
      continue;  // skip only crate table request
    }

    code = metaGetInfo(pVnode->pMeta, pTbData->uid, &info, NULL);
    if (code) {
      code = TSDB_CODE_TDB_TABLE_NOT_EXIST;
      vWarn("vgId:%d, error occurred at %s:%d since %s, version:%" PRId64 " uid:%" PRId64, TD_VID(pVnode), __FILE__,
            __LINE__, tstrerror(code), version, pTbData->uid);
      return code;
    }

    if (info.suid != pTbData->suid) {
      code = TSDB_CODE_INVALID_MSG;
      vError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64 " uid:%" PRId64 " suid:%" PRId64
             " info.suid:%" PRId64,
             TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code), version, pTbData->uid, pTbData->suid,
             info.suid);
      return code;
    }

    if (info.suid) {
      code = metaGetInfo(pVnode->pMeta, info.suid, &info, NULL);
      if (code) {
        code = TSDB_CODE_INTERNAL_ERROR;
        vError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64 " suid:%" PRId64, TD_VID(pVnode), __func__,
               __FILE__, __LINE__, tstrerror(code), version, info.suid);
        return code;
      }
    }

    if (pTbData->sver != info.skmVer) {
      code = TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER;
      addExistTableInfoIntoRes(pVnode, pRequest, pResponse, pTbData, numTbData);
      vDebug("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64 " uid:%" PRId64
             " sver:%d"
             " info.skmVer:%d",
             TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code), version, pTbData->uid, pTbData->sver,
             info.skmVer);
      return code;
    }
  }

  // Do write data
  vDebug("vgId:%d start to handle data write, version:%" PRId64, TD_VID(pVnode), version);

  for (int32_t i = 0; i < numTbData; ++i) {
    int32_t        affectedRows = 0;
    SSubmitTbData *pTbData = taosArrayGet(pRequest->aSubmitTbData, i);

    if (pTbData->flags & SUBMIT_REQ_ONLY_CREATE_TABLE) {
      continue;
    }

    if (hasBlob) {
      code = vnodeSubmitBlobData(pVnode, pTbData);
      if (code) {
        vError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64 " uid:%" PRId64, TD_VID(pVnode), __func__,
               __FILE__, __LINE__, tstrerror(code), version, pTbData->uid);
        return code;
      }
    }

    code = tsdbInsertTableData(pVnode->pTsdb, version, pTbData, &affectedRows);
    if (code) {
      vError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64 " uid:%" PRId64, TD_VID(pVnode), __func__,
             __FILE__, __LINE__, tstrerror(code), version, pTbData->uid);
      return code;
    }

    code = metaUpdateChangeTimeWithLock(pVnode->pMeta, pTbData->uid, pTbData->ctimeMs);
    if (code) {
      vError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64 " uid:%" PRId64, TD_VID(pVnode), __func__,
             __FILE__, __LINE__, tstrerror(code), version, pTbData->uid);
      return code;
    }
    pResponse->affectedRows += affectedRows;
  }

  vDebug("vgId:%d, handle data write done, version:%" PRId64 ", affectedRows:%d", TD_VID(pVnode), version,
         pResponse->affectedRows);
  return code;
}

static int32_t vnodeScanColumnData(SVnode *pVnode, SSubmitTbData *pTbData, TSKEY minKey, TSKEY maxKey) {
  int32_t code = 0;

  int32_t   numCols = taosArrayGetSize(pTbData->aCol);
  SColData *aColData = (SColData *)TARRAY_DATA(pTbData->aCol);

  if (numCols <= 0) {
    code = TSDB_CODE_INVALID_MSG;
    vError("vgId:%d, %s failed at %s:%d since %s, version:%d uid:%" PRId64 " numCols:%d", TD_VID(pVnode), __func__,
           __FILE__, __LINE__, tstrerror(code), pTbData->sver, pTbData->uid, numCols);
    return code;
  }

  if (aColData[0].cid != PRIMARYKEY_TIMESTAMP_COL_ID || aColData[0].type != TSDB_DATA_TYPE_TIMESTAMP ||
      aColData[0].nVal <= 0) {
    code = TSDB_CODE_INVALID_MSG;
    vError("vgId:%d, %s failed at %s:%d since %s, version:%d uid:%" PRId64
           " first column is not primary key timestamp, cid:%d type:%d nVal:%d",
           TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code), pTbData->sver, pTbData->uid, aColData[0].cid,
           aColData[0].type, aColData[0].nVal);
    return code;
  }

  for (int32_t i = 1; i < numCols; ++i) {
    if (aColData[i].nVal != aColData[0].nVal) {
      code = TSDB_CODE_INVALID_MSG;
      vError("vgId:%d, %s failed at %s:%d since %s, version:%d uid:%" PRId64
             " column cid:%d type:%d nVal:%d is not equal to primary key timestamp nVal:%d",
             TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code), pTbData->sver, pTbData->uid,
             aColData[i].cid, aColData[i].type, aColData[i].nVal, aColData[0].nVal);
      return code;
    }
  }

  SRowKey *pLastKey = NULL;
  SRowKey  lastKey = {0};
  for (int32_t i = 0; i < aColData[0].nVal; ++i) {
    SRowKey key = {0};

    tColDataArrGetRowKey(aColData, numCols, i, &key);

    if (key.ts < minKey || key.ts > maxKey) {
      code = TSDB_CODE_INVALID_MSG;
      vError("vgId:%d, %s failed at %s:%d since %s, version:%d uid:%" PRId64 " row[%d] key:%" PRId64
             " is out of range [%" PRId64 ", %" PRId64 "]",
             TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code), pTbData->sver, pTbData->uid, i, key.ts,
             minKey, maxKey);
      return code;
    }

    if (pLastKey && tRowKeyCompare(pLastKey, &key) >= 0) {
      code = TSDB_CODE_INVALID_MSG;
      vError("vgId:%d, %s failed at %s:%d since %s, version:%d uid:%" PRId64 " row[%d] key:%" PRId64
             " is not in order, lastKey:%" PRId64,
             TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code), pTbData->sver, pTbData->uid, i, key.ts,
             pLastKey->ts);
      return code;
    } else if (pLastKey == NULL) {
      pLastKey = &lastKey;
    }

    *pLastKey = key;
  }

  return code;
}

static int32_t vnodeScanSubmitRowData(SVnode *pVnode, SSubmitTbData *pTbData, TSKEY minKey, TSKEY maxKey) {
  int32_t code = 0;

  int32_t numRows = taosArrayGetSize(pTbData->aRowP);
  SRow  **aRow = (SRow **)TARRAY_DATA(pTbData->aRowP);

  if (numRows <= 0 && (pTbData->flags & SUBMIT_REQ_ONLY_CREATE_TABLE) == 0) {
    code = TSDB_CODE_INVALID_MSG;
    vError("vgId:%d, %s failed at %s:%d since %s, version:%d uid:%" PRId64 " numRows:%d", TD_VID(pVnode), __func__,
           __FILE__, __LINE__, tstrerror(code), pTbData->sver, pTbData->uid, numRows);
    return code;
  }

  SRowKey *pLastKey = NULL;
  SRowKey  lastKey = {0};
  for (int32_t i = 0; i < numRows; ++i) {
    SRow *pRow = aRow[i];
    if (pRow->sver != pTbData->sver) {
      code = TSDB_CODE_INVALID_MSG;
      vError("vgId:%d, %s failed at %s:%d since %s, version:%d uid:%" PRId64 " row[%d] sver:%d pTbData->sver:%d",
             TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code), pTbData->sver, pTbData->uid, i, pRow->sver,
             pTbData->sver);
      return code;
    }

    SRowKey key = {0};
    tRowGetKey(pRow, &key);
    if (key.ts < minKey || key.ts > maxKey) {
      code = TSDB_CODE_INVALID_MSG;
      vError("vgId:%d, %s failed at %s:%d since %s, version:%d uid:%" PRId64 " row[%d] key:%" PRId64
             " is out of range [%" PRId64 ", %" PRId64 "]",
             TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code), pTbData->sver, pTbData->uid, i, key.ts,
             minKey, maxKey);
      return code;
    }

    if (pLastKey && tRowKeyCompare(pLastKey, &key) >= 0) {
      code = TSDB_CODE_INVALID_MSG;
      vError("vgId:%d, %s failed at %s:%d since %s, version:%d uid:%" PRId64 " row[%d] key:%" PRId64
             " is not in order, lastKey:%" PRId64,
             TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code), pTbData->sver, pTbData->uid, i, key.ts,
             pLastKey->ts);
      return code;
    } else if (pLastKey == NULL) {
      pLastKey = &lastKey;
    }

    *pLastKey = key;
  }

  return code;
}

static int32_t vnodeScanSubmitReq(SVnode *pVnode, int64_t version, SSubmitReq2 *pRequest, SSubmitRsp2 *pResponse) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t numTbData = taosArrayGetSize(pRequest->aSubmitTbData);

  TSKEY now = taosGetTimestamp(pVnode->config.tsdbCfg.precision);
  TSKEY minKey = now - tsTickPerMin[pVnode->config.tsdbCfg.precision] * pVnode->config.tsdbCfg.keep2;
  TSKEY maxKey = tsMaxKeyByPrecision[pVnode->config.tsdbCfg.precision];
  for (int32_t i = 0; i < numTbData; i++) {
    SSubmitTbData *pTbData = taosArrayGet(pRequest->aSubmitTbData, i);

    if (pTbData->pCreateTbReq && pTbData->pCreateTbReq->uid == 0) {
      code = TSDB_CODE_INVALID_MSG;
      vError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64, TD_VID(pVnode), __func__, __FILE__, __LINE__,
             tstrerror(code), version);
      return code;
    }

    if (pTbData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
      code = vnodeScanColumnData(pVnode, pTbData, minKey, maxKey);
      if (code) {
        vError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64 " uid:%" PRId64, TD_VID(pVnode), __func__,
               __FILE__, __LINE__, tstrerror(code), version, pTbData->uid);
        return code;
      }
    } else {
      code = vnodeScanSubmitRowData(pVnode, pTbData, minKey, maxKey);
      if (code) {
        vError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64 " uid:%" PRId64, TD_VID(pVnode), __func__,
               __FILE__, __LINE__, tstrerror(code), version, pTbData->uid);
        return code;
      }
    }
  }

  return code;
}

static int32_t vnodeProcessSubmitReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp,
                                     SRpcMsg *pOriginalMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = 0;

  SSubmitReq2 *pSubmitReq = &(SSubmitReq2){0};
  SSubmitRsp2 *pSubmitRsp = &(SSubmitRsp2){0};
  int32_t      ret;
  SEncoder     ec = {0};

  pRsp->code = TSDB_CODE_SUCCESS;

  void           *pAllocMsg = NULL;
  SSubmitReq2Msg *pMsg = (SSubmitReq2Msg *)pReq;
  SDecoder        dc = {0};
  if (0 == taosHton64(pMsg->version)) {
    code = vnodeSubmitReqConvertToSubmitReq2(pVnode, (SSubmitReq *)pMsg, pSubmitReq);
    if (TSDB_CODE_SUCCESS == code) {
      code = vnodeRebuildSubmitReqMsg(pSubmitReq, &pReq);
    }
    if (TSDB_CODE_SUCCESS == code) {
      pAllocMsg = pReq;
    }
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    // decode
    pReq = POINTER_SHIFT(pReq, sizeof(SSubmitReq2Msg));
    len -= sizeof(SSubmitReq2Msg);

    tDecoderInit(&dc, pReq, len);
    if (tDecodeSubmitReq(&dc, pSubmitReq, NULL) < 0) {
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  // Scan the request
  code = vnodeScanSubmitReq(pVnode, ver, pSubmitReq, pSubmitRsp);
  if (code) {
    vError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64, TD_VID(pVnode), __func__, __FILE__, __LINE__,
           tstrerror(code), ver);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  vGDebug(pOriginalMsg ? &pOriginalMsg->info.traceId : NULL, "vgId:%d, index:%" PRId64 ", submit block, rows:%d",
          TD_VID(pVnode), ver, (int32_t)taosArrayGetSize(pSubmitReq->aSubmitTbData));

  // Handle auto create table
  code = vnodeHandleAutoCreateTable(pVnode, ver, pSubmitReq, pSubmitRsp);
  if (code) {
    vError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64, TD_VID(pVnode), __func__, __FILE__, __LINE__,
           tstrerror(code), ver);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // Handle data write
  code = vnodeHandleDataWrite(pVnode, ver, pSubmitReq, pSubmitRsp);
  if (code) {
    vError("vgId:%d, %s failed at %s:%d since %s, version:%" PRId64, TD_VID(pVnode), __func__, __FILE__, __LINE__,
           tstrerror(code), ver);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  // message
  pRsp->code = code;
  tEncodeSize(tEncodeSSubmitRsp2, pSubmitRsp, pRsp->contLen, ret);
  pRsp->pCont = rpcMallocCont(pRsp->contLen);
  tEncoderInit(&ec, pRsp->pCont, pRsp->contLen);
  if (tEncodeSSubmitRsp2(&ec, pSubmitRsp) < 0) {
    vError("vgId:%d, failed to encode submit response", TD_VID(pVnode));
  }
  tEncoderClear(&ec);

  // update statistics
  (void)atomic_add_fetch_64(&pVnode->statis.nInsert, pSubmitRsp->affectedRows);
  (void)atomic_add_fetch_64(&pVnode->statis.nInsertSuccess, pSubmitRsp->affectedRows);
  (void)atomic_add_fetch_64(&pVnode->statis.nBatchInsert, 1);

  // update metrics
  METRICS_UPDATE(pVnode->writeMetrics.total_requests, METRIC_LEVEL_LOW, 1);
  METRICS_UPDATE(pVnode->writeMetrics.total_rows, METRIC_LEVEL_HIGH, pSubmitRsp->affectedRows);
  METRICS_UPDATE(pVnode->writeMetrics.total_bytes, METRIC_LEVEL_LOW, pMsg->header.contLen);

  if (tsEnableMonitor && tsMonitorFqdn[0] != 0 && tsMonitorPort != 0 && pSubmitRsp->affectedRows > 0 &&
      strlen(pOriginalMsg->info.conn.user) > 0 && tsInsertCounter != NULL) {
    const char *sample_labels[] = {VNODE_METRIC_TAG_VALUE_INSERT_AFFECTED_ROWS,
                                   pVnode->monitor.strClusterId,
                                   pVnode->monitor.strDnodeId,
                                   tsLocalEp,
                                   pVnode->monitor.strVgId,
                                   pOriginalMsg->info.conn.user,
                                   "Success"};
    int         tv = taos_counter_add(tsInsertCounter, pSubmitRsp->affectedRows, sample_labels);
  }

  if (code == 0) {
    (void)atomic_add_fetch_64(&pVnode->statis.nBatchInsertSuccess, 1);
  }

  // clear
  tDestroySubmitReq(pSubmitReq, 0 == taosHton64(pMsg->version) ? TSDB_MSG_FLG_CMPT : TSDB_MSG_FLG_DECODE);
  tDestroySSubmitRsp2(pSubmitRsp, TSDB_MSG_FLG_ENCODE);

  if (code) {
    terrno = code;
    if (code == TSDB_CODE_TDB_TABLE_ALREADY_EXIST) {
      vInfo("vgId:%d, failed to process submit request since %s, lino:%d, version:%" PRId64, TD_VID(pVnode),
            tstrerror(code), lino, ver);

    } else {
      vError("vgId:%d, failed to process submit request since %s, lino:%d, version:%" PRId64, TD_VID(pVnode),
             tstrerror(code), lino, ver);
    }
  }

  taosMemoryFree(pAllocMsg);
  tDecoderClear(&dc);

  return code;
}

static int32_t vnodeConsolidateAlterHashRange(SVnode *pVnode, int64_t ver) {
  int32_t code = TSDB_CODE_SUCCESS;

  vInfo("vgId:%d, trim meta of tables per hash range [%" PRIu32 ", %" PRIu32 "]. apply-index:%" PRId64, TD_VID(pVnode),
        pVnode->config.hashBegin, pVnode->config.hashEnd, ver);

  // TODO: trim meta of tables from TDB per hash range [pVnode->config.hashBegin, pVnode->config.hashEnd]
  code = metaTrimTables(pVnode->pMeta, ver);

  return code;
}

static int32_t vnodeProcessAlterConfirmReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  vInfo("vgId:%d, vnode handle msgType:alter-confirm, alter confirm msg is processed", TD_VID(pVnode));
  int32_t code = TSDB_CODE_SUCCESS;
  if (!pVnode->config.hashChange) {
    goto _exit;
  }

  code = vnodeConsolidateAlterHashRange(pVnode, ver);
  if (code < 0) {
    vError("vgId:%d, failed to consolidate alter hashrange since %s. version:%" PRId64, TD_VID(pVnode), terrstr(), ver);
    goto _exit;
  }
  pVnode->config.hashChange = false;

_exit:
  pRsp->msgType = TDMT_VND_ALTER_CONFIRM_RSP;
  pRsp->code = code;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  return code;
}

extern int32_t tsdbDisableAndCancelAllBgTask(STsdb *pTsdb);
extern void    tsdbEnableBgTask(STsdb *pTsdb);

static int32_t vnodeProcessAlterConfigReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  bool walChanged = false;
  bool tsdbChanged = false;

  SAlterVnodeConfigReq req = {0};
  if (tDeserializeSAlterVnodeConfigReq(pReq, len, &req) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return TSDB_CODE_INVALID_MSG;
  }

  vInfo("vgId:%d, start to alter vnode config, page:%d pageSize:%d buffer:%d szPage:%d szBuf:%" PRIu64
        " cacheLast:%d cacheLastSize:%d days:%d keep0:%d keep1:%d keep2:%d keepTimeOffset:%d ssKeepLocal:%d "
        "ssCompact:%d fsync:%d level:%d "
        "walRetentionPeriod:%d walRetentionSize:%d",
        TD_VID(pVnode), req.pages, req.pageSize, req.buffer, req.pageSize * 1024, (uint64_t)req.buffer * 1024 * 1024,
        req.cacheLast, req.cacheLastSize, req.daysPerFile, req.daysToKeep0, req.daysToKeep1, req.daysToKeep2,
        req.keepTimeOffset, req.ssKeepLocal, req.ssCompact, req.walFsyncPeriod, req.walLevel, req.walRetentionPeriod,
        req.walRetentionSize);

  if (pVnode->config.cacheLastSize != req.cacheLastSize) {
    pVnode->config.cacheLastSize = req.cacheLastSize;
    tsdbCacheSetCapacity(pVnode, (size_t)pVnode->config.cacheLastSize * 1024 * 1024);
  }

  if (pVnode->config.szBuf != req.buffer * 1024LL * 1024LL) {
    vInfo("vgId:%d, vnode buffer is changed from %" PRId64 " to %" PRId64, TD_VID(pVnode), pVnode->config.szBuf,
          (uint64_t)(req.buffer * 1024LL * 1024LL));
    pVnode->config.szBuf = req.buffer * 1024LL * 1024LL;
  }

  if (pVnode->config.szCache != req.pages) {
    if ((terrno = metaAlterCache(pVnode->pMeta, req.pages)) < 0) {
      vError("vgId:%d, failed to change vnode pages from %d to %d failed since %s", TD_VID(pVnode),
             pVnode->config.szCache, req.pages, tstrerror(terrno));
      return terrno;
    } else {
      vInfo("vgId:%d, vnode pages is changed from %d to %d", TD_VID(pVnode), pVnode->config.szCache, req.pages);
      pVnode->config.szCache = req.pages;
    }
  }

  if (pVnode->config.cacheLast != req.cacheLast) {
    pVnode->config.cacheLast = req.cacheLast;
  }

  if (pVnode->config.walCfg.fsyncPeriod != req.walFsyncPeriod) {
    pVnode->config.walCfg.fsyncPeriod = req.walFsyncPeriod;
    walChanged = true;
  }

  if (pVnode->config.walCfg.level != req.walLevel) {
    if (pVnode->config.walCfg.level == 0) {
      pVnode->config.walCfg.clearFiles = 1;
    }
    pVnode->config.walCfg.level = req.walLevel;
    walChanged = true;
  }

  if (pVnode->config.walCfg.retentionPeriod != req.walRetentionPeriod) {
    pVnode->config.walCfg.retentionPeriod = req.walRetentionPeriod;
    walChanged = true;
  }

  if (pVnode->config.walCfg.retentionSize != req.walRetentionSize) {
    pVnode->config.walCfg.retentionSize = req.walRetentionSize;
    walChanged = true;
  }

  if (pVnode->config.tsdbCfg.keep0 != req.daysToKeep0) {
    pVnode->config.tsdbCfg.keep0 = req.daysToKeep0;
    tsdbChanged = true;
  }

  if (pVnode->config.tsdbCfg.keep1 != req.daysToKeep1) {
    pVnode->config.tsdbCfg.keep1 = req.daysToKeep1;
    tsdbChanged = true;
  }

  if (pVnode->config.tsdbCfg.keep2 != req.daysToKeep2) {
    pVnode->config.tsdbCfg.keep2 = req.daysToKeep2;
    tsdbChanged = true;
  }

  if (pVnode->config.tsdbCfg.keepTimeOffset != req.keepTimeOffset) {
    pVnode->config.tsdbCfg.keepTimeOffset = req.keepTimeOffset;
    tsdbChanged = true;
  }

  if (req.sttTrigger != -1 && req.sttTrigger != pVnode->config.sttTrigger) {
    if (req.sttTrigger > 1 && pVnode->config.sttTrigger > 1) {
      pVnode->config.sttTrigger = req.sttTrigger;
    } else {
      vnodeAWait(&pVnode->commitTask);

      int32_t ret = tsdbDisableAndCancelAllBgTask(pVnode->pTsdb);
      if (ret != 0) {
        vError("vgId:%d, failed to disable bg task since %s", TD_VID(pVnode), tstrerror(ERRNO));
      }

      pVnode->config.sttTrigger = req.sttTrigger;
      tsdbEnableBgTask(pVnode->pTsdb);
    }
  }

  if (req.minRows != -1 && req.minRows != pVnode->config.tsdbCfg.minRows) {
    pVnode->config.tsdbCfg.minRows = req.minRows;
  }

  if (req.ssKeepLocal != -1 && req.ssKeepLocal != pVnode->config.ssKeepLocal) {
    pVnode->config.ssKeepLocal = req.ssKeepLocal;
  }
  if (req.ssCompact != -1 && req.ssCompact != pVnode->config.ssCompact) {
    pVnode->config.ssCompact = req.ssCompact;
  }

  if (walChanged) {
    if (walAlter(pVnode->pWal, &pVnode->config.walCfg) != 0) {
      vError("vgId:%d, failed to alter wal config since %s", TD_VID(pVnode), tstrerror(ERRNO));
    }
  }

  if (tsdbChanged) {
    tsdbSetKeepCfg(pVnode->pTsdb, &pVnode->config.tsdbCfg);
  }

  return 0;
}

static int32_t vnodeProcessBatchDeleteReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  SBatchDeleteReq deleteReq;
  SDecoder        decoder;
  tDecoderInit(&decoder, pReq, len);
  if (tDecodeSBatchDeleteReq(&decoder, &deleteReq) < 0) {
    tDecoderClear(&decoder);
    return terrno = TSDB_CODE_INVALID_MSG;
  }

  SMetaReader mr = {0};
  metaReaderDoInit(&mr, pVnode->pMeta, META_READER_NOLOCK);
  STsdb *pTsdb = pVnode->pTsdb;

  int32_t sz = taosArrayGetSize(deleteReq.deleteReqs);
  for (int32_t i = 0; i < sz; i++) {
    SSingleDeleteReq *pOneReq = taosArrayGet(deleteReq.deleteReqs, i);
    char             *name = pOneReq->tbname;
    if (metaGetTableEntryByName(&mr, name) < 0) {
      vDebug("vgId:%d, stream delete msg, skip since no table: %s", pVnode->config.vgId, name);
      continue;
    }

    int64_t uid = mr.me.uid;

    int32_t code = tsdbDeleteTableData(pTsdb, ver, deleteReq.suid, uid, pOneReq->startTs, pOneReq->endTs);
    if (code < 0) {
      terrno = code;
      vError("vgId:%d, delete error since %s, suid:%" PRId64 ", uid:%" PRId64 ", start ts:%" PRId64 ", end ts:%" PRId64,
             TD_VID(pVnode), terrstr(), deleteReq.suid, uid, pOneReq->startTs, pOneReq->endTs);
    }

    if (deleteReq.level == 0) {
      code = metaUpdateChangeTimeWithLock(pVnode->pMeta, uid, deleteReq.ctimeMs);
      if (code < 0) {
        terrno = code;
        vError("vgId:%d, update change time error since %s, suid:%" PRId64 ", uid:%" PRId64 ", start ts:%" PRId64
               ", end ts:%" PRId64,
               TD_VID(pVnode), terrstr(), deleteReq.suid, uid, pOneReq->startTs, pOneReq->endTs);
      }
    }
    tDecoderClear(&mr.coder);
  }
  metaReaderClear(&mr);
  taosArrayDestroy(deleteReq.deleteReqs);
  return 0;
}

static int32_t vnodeProcessDeleteReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp,
                                     SRpcMsg *pOriginalMsg) {
  int32_t     code = 0;
  SDecoder   *pCoder = &(SDecoder){0};
  SDeleteRes *pRes = &(SDeleteRes){0};

  pRsp->msgType = TDMT_VND_DELETE_RSP;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;
  pRsp->code = TSDB_CODE_SUCCESS;

  pRes->uidList = taosArrayInit(0, sizeof(tb_uid_t));
  if (pRes->uidList == NULL) {
    code = terrno;
    goto _err;
  }

  tDecoderInit(pCoder, pReq, len);
  code = tDecodeDeleteRes(pCoder, pRes);
  if (code) goto _err;

  if (pRes->affectedRows > 0) {
    for (int32_t iUid = 0; iUid < taosArrayGetSize(pRes->uidList); iUid++) {
      uint64_t uid = *(uint64_t *)taosArrayGet(pRes->uidList, iUid);
      code = tsdbDeleteTableData(pVnode->pTsdb, ver, pRes->suid, uid, pRes->skey, pRes->ekey);
      if (code) goto _err;
      code = metaUpdateChangeTimeWithLock(pVnode->pMeta, uid, pRes->ctimeMs);
      if (code) goto _err;
    }
  }

  tDecoderClear(pCoder);
  taosArrayDestroy(pRes->uidList);

  SVDeleteRsp rsp = {.affectedRows = pRes->affectedRows};
  int32_t     ret = 0;
  tEncodeSize(tEncodeSVDeleteRsp, &rsp, pRsp->contLen, ret);
  pRsp->pCont = rpcMallocCont(pRsp->contLen);
  SEncoder ec = {0};
  tEncoderInit(&ec, pRsp->pCont, pRsp->contLen);
  code = tEncodeSVDeleteRsp(&ec, &rsp);
  if (code) goto _err;
  tEncoderClear(&ec);
  return code;

_err:
  /*
  if(code == TSDB_CODE_SUCCESS){
    const char *batch_sample_labels[] = {VNODE_METRIC_TAG_VALUE_DELETE, pVnode->monitor.strClusterId,
                                        pVnode->monitor.strDnodeId, tsLocalEp, pVnode->monitor.strVgId,
                                        pOriginalMsg->info.conn.user, "Success"};
    taos_counter_inc(pVnode->monitor.insertCounter, batch_sample_labels);
  }
  else{
    const char *batch_sample_labels[] = {VNODE_METRIC_TAG_VALUE_DELETE, pVnode->monitor.strClusterId,
                                        pVnode->monitor.strDnodeId, tsLocalEp, pVnode->monitor.strVgId,
                                        pOriginalMsg->info.conn.user, "Failed"};
    taos_counter_inc(pVnode->monitor.insertCounter, batch_sample_labels);
  }
  */

  return code;
}
static int32_t vnodeProcessCreateIndexReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  SVCreateStbReq req = {0};
  SDecoder       dc = {0};
  int32_t        code = 0;

  pRsp->msgType = TDMT_VND_CREATE_INDEX_RSP;
  pRsp->code = TSDB_CODE_SUCCESS;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  tDecoderInit(&dc, pReq, len);
  // decode req
  if (tDecodeSVCreateStbReq(&dc, &req) < 0) {
    tDecoderClear(&dc);
    return terrno = TSDB_CODE_INVALID_MSG;
  }

  code = metaAddIndexToSuperTable(pVnode->pMeta, ver, &req);
  if (code) {
    pRsp->code = code;
    goto _err;
  }
  tDecoderClear(&dc);
  return 0;

_err:
  tDecoderClear(&dc);
  return code;
}
static int32_t vnodeProcessDropIndexReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  SDropIndexReq req = {0};
  int32_t       code = 0;
  pRsp->msgType = TDMT_VND_DROP_INDEX_RSP;
  pRsp->code = TSDB_CODE_SUCCESS;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  if ((code = tDeserializeSDropIdxReq(pReq, len, &req))) {
    pRsp->code = code;
    return code;
  }

  code = metaDropIndexFromSuperTable(pVnode->pMeta, ver, &req);
  if (code) {
    pRsp->code = code;
    return code;
  }
  return TSDB_CODE_SUCCESS;
}

extern int32_t vnodeAsyncCompact(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);

static int32_t vnodeProcessCompactVnodeReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  if (!pVnode->restored) {
    vInfo("vgId:%d, ignore compact req during restoring. ver:%" PRId64, TD_VID(pVnode), ver);
    return 0;
  }
  return vnodeAsyncCompact(pVnode, ver, pReq, len, pRsp);
}

static int32_t vnodeProcessConfigChangeReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  if (syncCheckMember(pVnode->sync) != 0) {
    vError("vgId:%d, failed to check member", TD_VID(pVnode));
  }

  pRsp->msgType = TDMT_SYNC_CONFIG_CHANGE_RSP;
  pRsp->code = TSDB_CODE_SUCCESS;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  return 0;
}

static int32_t vnodeCheckState(SVnode *pVnode) {
  SSyncState syncState = syncGetState(pVnode->sync);
  if (syncState.state != TAOS_SYNC_STATE_LEADER) {
    return terrno = TSDB_CODE_SYN_NOT_LEADER;
  }
  return 0;
}

static int32_t vnodeCheckToken(SVnode *pVnode, char *member0Token, char *member1Token) {
  char token[TSDB_ARB_TOKEN_SIZE] = {0};
  if (vnodeGetArbToken(pVnode, token) != 0) {
    return terrno = TSDB_CODE_NOT_FOUND;
  }

  if (strncmp(token, member0Token, TSDB_ARB_TOKEN_SIZE) != 0 &&
      strncmp(token, member1Token, TSDB_ARB_TOKEN_SIZE) != 0) {
    return terrno = TSDB_CODE_MND_ARB_TOKEN_MISMATCH;
  }

  terrno = TSDB_CODE_SUCCESS;
  return 0;
}

static int32_t vnodeCheckSyncd(SVnode *pVnode, char *member0Token, char *member1Token) {
  int32_t code = vnodeCheckToken(pVnode, member0Token, member1Token);
  if (code != 0) {
    return code;
  }

  return syncCheckSynced(pVnode->sync);
}

static int32_t vnodeProcessArbCheckSyncReq(SVnode *pVnode, void *pReq, int32_t len, SRpcMsg *pRsp) {
  int32_t code = 0;

  if ((code = vnodeCheckState(pVnode)) != 0) {
    vDebug("vgId:%d, failed to preprocess vnode-arb-check-sync request since %s", TD_VID(pVnode), tstrerror(code));
    return 0;
  }

  SVArbCheckSyncReq syncReq = {0};

  code = tDeserializeSVArbCheckSyncReq(pReq, len, &syncReq);
  if (code) {
    return code;
  }

  vInfo("vgId:%d, start to process vnode-arb-check-sync req QID:0x%" PRIx64 ":0x%" PRIx64 ", seqNum:%" PRIx64
        ", arbToken:%s, member0Token:%s, member1Token:%s, "
        "arbTerm:%" PRId64,
        TD_VID(pVnode), pRsp->info.traceId.rootId, pRsp->info.traceId.msgId, pRsp->info.seqNum, syncReq.arbToken,
        syncReq.member0Token, syncReq.member1Token, syncReq.arbTerm);

  pRsp->msgType = TDMT_VND_ARB_CHECK_SYNC_RSP;
  pRsp->code = TSDB_CODE_SUCCESS;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  SVArbCheckSyncRsp syncRsp = {0};
  syncRsp.arbToken = syncReq.arbToken;
  syncRsp.member0Token = syncReq.member0Token;
  syncRsp.member1Token = syncReq.member1Token;
  syncRsp.vgId = TD_VID(pVnode);

  if ((syncRsp.errCode = vnodeCheckSyncd(pVnode, syncReq.member0Token, syncReq.member1Token)) != 0) {
    vError("vgId:%d, failed to check assigned log syncd since %s", TD_VID(pVnode), tstrerror(syncRsp.errCode));
  }

  if ((code = vnodeUpdateArbTerm(pVnode, syncReq.arbTerm)) != 0) {
    vError("vgId:%d, failed to update arb term since %s", TD_VID(pVnode), tstrerror(code));
    goto _OVER;
  }

  int32_t contLen = tSerializeSVArbCheckSyncRsp(NULL, 0, &syncRsp);
  if (contLen <= 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    code = -1;
    goto _OVER;
  }
  void *pHead = rpcMallocCont(contLen);
  if (!pHead) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    code = -1;
    goto _OVER;
  }

  if (tSerializeSVArbCheckSyncRsp(pHead, contLen, &syncRsp) <= 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    rpcFreeCont(pHead);
    code = -1;
    goto _OVER;
  }

  pRsp->pCont = pHead;
  pRsp->contLen = contLen;

  vInfo(
      "vgId:%d, suceed to process vnode-arb-check-sync req rsp.code:%s, arbToken:%s, member0Token:%s, "
      "member1Token:%s",
      TD_VID(pVnode), tstrerror(syncRsp.errCode), syncRsp.arbToken, syncRsp.member0Token, syncRsp.member1Token);

  code = TSDB_CODE_SUCCESS;

_OVER:
  if (code != 0) {
    vError("vgId:%d, failed to process arb check req rsp.code:%s since %s", TD_VID(pVnode), tstrerror(syncRsp.errCode),
           tstrerror(code));
  }
  tFreeSVArbCheckSyncReq(&syncReq);
  return code;
}

#ifndef TD_ENTERPRISE
int32_t vnodeAsyncCompact(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) { return 0; }
int32_t tsdbAsyncCompact(STsdb *tsdb, const STimeWindow *tw, ETsdbOpType type) { return 0; }
#endif
