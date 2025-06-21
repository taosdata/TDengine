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
extern int32_t tsdbAsyncSsMigrate(STsdb *tsdb, SSsMigrateVgroupReq *pReq);
extern int32_t tsdbQuerySsMigrateProgress(STsdb *tsdb, int32_t ssMigrateId, int32_t *rspSize, void** ppRsp);
extern int32_t tsdbUpdateSsMigrateState(STsdb* tsdb, SVnodeSsMigrateState* pState);


int32_t vnodeAsyncRetention(SVnode *pVnode, int64_t now) {
  // async retention
  return tsdbAsyncRetention(pVnode->pTsdb, now);
}

int32_t vnodeAsyncSsMigrate(SVnode *pVnode, SSsMigrateVgroupReq *pReq) {
  // async migration
#ifdef USE_SHARED_STORAGE
  if (tsSsEnabled) {
    return tsdbAsyncSsMigrate(pVnode->pTsdb, pReq);
  }
#endif
  return TSDB_CODE_INTERNAL_ERROR;
}

int32_t vnodeQuerySsMigrateProgress(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;

  SQuerySsMigrateProgressReq req = {0};

  int32_t                  rspSize = 0;
  SRpcMsg                  rspMsg = {0};
  void                    *pRsp = NULL;
  SQuerySsMigrateProgressRsp rsp = {0};

  // deserialize request
  char* buf = (char*)pMsg->pCont + sizeof(SMsgHead);
  code = tDeserializeSQuerySsMigrateProgressReq(buf, pMsg->contLen - sizeof(SMsgHead), &req);
  if (code) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  vDebug("vgId:%d, ssMigrateId:%d, processing query ss migrate progress request", req.vgId, req.ssMigrateId);
  code = tsdbQuerySsMigrateProgress(pVnode->pTsdb, req.ssMigrateId, &rspSize, &pRsp);

_exit:
  rspMsg.info = pMsg->info;
  rspMsg.pCont = pRsp;
  rspMsg.contLen = rspSize;
  rspMsg.code = code;
  rspMsg.msgType = TDMT_VND_QUERY_SSMIGRATE_PROGRESS_RSP;

  tmsgSendRsp(&rspMsg);
  return 0;
}

int32_t vnodeFollowerSsMigrate(SVnode *pVnode, SVnodeSsMigrateState *pState) {
  return tsdbUpdateSsMigrateState(pVnode->pTsdb, pState);
}
