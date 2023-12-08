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

extern int32_t tsdbStopAllCompTask(STsdb *tsdb);
extern int32_t tsdbAsyncCompact(STsdb *tsdb, const STimeWindow *tw, bool sync);
extern int32_t tsdbCompMonitorGetInfo(STsdb *tsdb, SQueryCompactProgressRsp *rsp);

int32_t vnodeProcessCompactVnodeReqImpl(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp) {
  SCompactVnodeReq req = {0};
  if (tDeserializeSCompactVnodeReq(pReq, len, &req) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return TSDB_CODE_INVALID_MSG;
  }
  vInfo("vgId:%d, compact msg will be processed, db:%s dbUid:%" PRId64 " compactStartTime:%" PRId64, TD_VID(pVnode),
        req.db, req.dbUid, req.compactStartTime);

  return tsdbAsyncCompact(pVnode->pTsdb, &req.tw, pVnode->config.sttTrigger == 1);
}

int32_t vnodeProcessKillCompactReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  tsdbStopAllCompTask(pVnode->pTsdb);
  // TODO: send response
  return 0;
}

int32_t vnodeQueryCompactProgress(SVnode *pVnode, SRpcMsg *pMsg) {
  SQueryCompactProgressReq req = {0};
  SQueryCompactProgressRsp rsp = {0};

  // deserialize request
  if (tDeserializeSQueryCompactProgressReq(pMsg->pCont, pMsg->contLen, &req)) {
    terrno = TSDB_CODE_INVALID_MSG;
    // TODO
  }

  // query compact progress
  rsp.dnodeId = req.dnodeId;
  tsdbCompMonitorGetInfo(pVnode->pTsdb, &rsp);

  // serialize response
  // TODO
  // tSerializeSQueryCompactProgressRsp(void *buf, int32_t bufLen, SQueryCompactProgressRsp *pReq);

  return 0;
}
