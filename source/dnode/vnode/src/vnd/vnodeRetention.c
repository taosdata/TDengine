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

#include "vnd.h"

extern int32_t tsdbAsyncRetention(STsdb *tsdb, int64_t now);
extern int32_t tsdbListSsMigrateFileSets(STsdb* tsdb, SArray* fidArr);
extern int32_t tsdbAsyncSsMigrateFileSet(STsdb *tsdb, SSsMigrateFileSetReq *pReq);
extern int32_t tsdbQuerySsMigrateProgress(STsdb *tsdb, SSsMigrateProgress *pProgress);
extern int32_t tsdbUpdateSsMigrateProgress(STsdb* tsdb, SSsMigrateProgress* pProgress);



int32_t vnodeAsyncRetention(SVnode *pVnode, int64_t now) {
  // async retention
  return tsdbAsyncRetention(pVnode->pTsdb, now);
}



int32_t vnodeQuerySsMigrateProgress(SVnode *pVnode, SRpcMsg *pMsg) {
#ifdef USE_SHARED_STORAGE

  int32_t code = 0;

  SSsMigrateProgress req = {0};

  int32_t                  rspSize = 0;
  SRpcMsg                  rspMsg = {0};
  void                    *pRsp = NULL;

  char* buf = (char*)pMsg->pCont + sizeof(SMsgHead);
  code = tDeserializeSSsMigrateProgress(buf, pMsg->contLen - sizeof(SMsgHead), &req);
  if (code) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  vDebug("vgId:%d, ssMigrateId:%d, processing query ss migrate progress request", req.vgId, req.ssMigrateId);
  SSsMigrateProgress rsp = req;
  code = tsdbQuerySsMigrateProgress(pVnode->pTsdb, &rsp);
  if (code) {
    goto _exit;
  }

  rspSize = tSerializeSSsMigrateProgress(NULL, 0, &rsp);
  pRsp = rpcMallocCont(rspSize);
  if (pRsp == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    vError("vgId:%d, ssMigrateId:%d, failed to allocate response buffer since %s", req.vgId, req.ssMigrateId, tstrerror(code));
    rspSize = 0;
    goto _exit;
  }
  TAOS_UNUSED(tSerializeSSsMigrateProgress(pRsp, rspSize, &rsp));

_exit:
  rspMsg.info = pMsg->info;
  rspMsg.pCont = pRsp;
  rspMsg.contLen = rspSize;
  rspMsg.code = code;
  rspMsg.msgType = TDMT_VND_QUERY_SSMIGRATE_PROGRESS_RSP;

  tmsgSendRsp(&rspMsg);
  return 0;

#else
  return TSDB_CODE_OPS_NOT_SUPPORT;
#endif
}



int32_t vnodeListSsMigrateFileSets(SVnode *pVnode, SRpcMsg *pMsg) {
#ifdef USE_SHARED_STORAGE

  int32_t code = 0, vgId = TD_VID(pVnode);
  SListSsMigrateFileSetsReq req = {0};
  SArray* fidArr = NULL;

  int32_t                  rspSize = 0;
  SRpcMsg                  rspMsg = {0};
  void                    *pRsp = NULL;
  SListSsMigrateFileSetsRsp rsp = {0};

  // deserialize request
  char* buf = (char*)pMsg->pCont + sizeof(SMsgHead);
  code = tDeserializeSListSsMigrateFileSetsReq(buf, pMsg->contLen - sizeof(SMsgHead), &req);
  if (code) {
    vError("vgId:%d, failed to deserialize ss migrate query file sets request since %s", vgId, tstrerror(code));
    goto _exit;
  }

  fidArr = taosArrayInit(10, sizeof(int32_t));
  if (fidArr == NULL) {
    code = terrno;
    vError("vgId:%d, failed to initialize file set id array since %s", TD_VID(pVnode), tstrerror(code));
    goto _exit;
  }

  code = tsdbListSsMigrateFileSets(pVnode->pTsdb, fidArr);
  if (code != TSDB_CODE_SUCCESS) {
    vError("vgId:%d, %s failed since %s", TD_VID(pVnode), __func__, tstrerror(code));
    goto _exit;
  }

  rsp.ssMigrateId = req.ssMigrateId;
  rsp.vgId = vgId;
  rsp.pFileSets = fidArr;
  rspSize = tSerializeSListSsMigrateFileSetsRsp(NULL, 0, &rsp);
  pRsp = rpcMallocCont(rspSize);
  if (pRsp == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    vError("vgId:%d, failed to allocate response buffer of size %d since %s", vgId, rspSize, tstrerror(code));
    rspSize = 0;
    goto _exit;
  }
  TAOS_UNUSED(tSerializeSListSsMigrateFileSetsRsp(pRsp, rspSize, &rsp));

_exit:
  taosArrayDestroy(fidArr);
  rspMsg.info = pMsg->info;
  rspMsg.pCont = pRsp;
  rspMsg.contLen = rspSize;
  rspMsg.code = code;
  rspMsg.msgType = TDMT_VND_LIST_SSMIGRATE_FILESETS_RSP;

  tmsgSendRsp(&rspMsg);
  return 0;

#else
  return TSDB_CODE_OPS_NOT_SUPPORT;
#endif
}



int32_t vnodeAsyncSsMigrateFileSet(SVnode *pVnode, SSsMigrateFileSetReq *pReq) {
  // async migration
#ifdef USE_SHARED_STORAGE
  if (tsSsEnabled) {
    return tsdbAsyncSsMigrateFileSet(pVnode->pTsdb, pReq);
  }
#endif
  return TSDB_CODE_OPS_NOT_SUPPORT;
}



int32_t vnodeFollowerSsMigrate(SVnode *pVnode, SSsMigrateProgress *pReq) {
#ifdef USE_SHARED_STORAGE
  return tsdbUpdateSsMigrateProgress(pVnode->pTsdb, pReq);
#else
  return TSDB_CODE_OPS_NOT_SUPPORT;
#endif
}
