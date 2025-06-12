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
extern int32_t tsdbAsyncS3Migrate(STsdb *tsdb, SS3MigrateVgroupReq *pReq);
extern int32_t tsdbQueryS3MigrateProgress(STsdb *tsdb, int32_t s3MigrateId, int32_t *rspSize, void** ppRsp);
extern int32_t tsdbUpdateS3MigrateState(STsdb* tsdb, SVnodeS3MigrateState* pState);


int32_t vnodeAsyncRetention(SVnode *pVnode, int64_t now) {
  // async retention
  return tsdbAsyncRetention(pVnode->pTsdb, now);
}

int32_t vnodeAsyncS3Migrate(SVnode *pVnode, SS3MigrateVgroupReq *pReq) {
  // async migration
#ifdef USE_S3
  return tsdbAsyncS3Migrate(pVnode->pTsdb, pReq);
#else
  return TSDB_CODE_INTERNAL_ERROR;
#endif
}

int32_t vnodeQueryS3MigrateProgress(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;

  SQueryS3MigrateProgressReq req = {0};

  int32_t                  rspSize = 0;
  SRpcMsg                  rspMsg = {0};
  void                    *pRsp = NULL;
  SQueryS3MigrateProgressRsp rsp = {0};

  // deserialize request
  code = tDeserializeSQueryS3MigrateProgressReq(pMsg->pCont + sizeof(SMsgHead), pMsg->contLen - sizeof(SMsgHead), &req);
  if (code) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  code = tsdbQueryS3MigrateProgress(pVnode->pTsdb, req.s3MigrateId, &rspSize, &pRsp);

_exit:
  rspMsg.info = pMsg->info;
  rspMsg.pCont = pRsp;
  rspMsg.contLen = rspSize;
  rspMsg.code = code;
  rspMsg.msgType = TDMT_VND_QUERY_S3MIGRATE_PROGRESS_RSP;

  tmsgSendRsp(&rspMsg);
  return 0;
}

int32_t vnodeFollowerS3Migrate(SVnode *pVnode, SVnodeS3MigrateState *pState) {
  return tsdbUpdateS3MigrateState(pVnode->pTsdb, pState);
}
