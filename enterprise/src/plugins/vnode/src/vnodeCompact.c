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

extern int32_t tsdbAsyncCompact(STsdb *tsdb, const STimeWindow *tw, bool sync);

int32_t vnodeProcessCompactVnodeReqImpl(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp) {
  SCompactVnodeReq req = {0};
  if (tDeserializeSCompactVnodeReq(pReq, len, &req) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return TSDB_CODE_INVALID_MSG;
  }
  vInfo("vgId:%d, compact msg will be processed, db:%s dbUid:%" PRId64 " compactStartTime:%" PRId64, TD_VID(pVnode),
        req.db, req.dbUid, req.compactStartTime);

  if (pVnode->config.sttTrigger == 1) {
    return tsdbAsyncCompact(pVnode->pTsdb, &req.tw, true);
  } else {
    return tsdbAsyncCompact(pVnode->pTsdb, &req.tw, false);
  }
}