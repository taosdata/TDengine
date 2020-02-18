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

#define _DEFAULT_SOURCE
#include "os.h"
#include "taoserror.h"
#include "tlog.h"
#include "dnodeWrite.h"
#include "dnodeVnodeMgmt.h"

void dnodeWriteData(SShellSubmitMsg *pSubmit, void *pConn, void (*callback)(SShellSubmitRspMsg *rsp, void *pConn)) {
  SShellSubmitRspMsg result = {0};

  int32_t numOfSid = htonl(pSubmit->numOfSid);
  if (numOfSid <= 0) {
    dError("invalid num of tables:%d", numOfSid);
    result.code = TSDB_CODE_INVALID_QUERY_MSG;
    callback(&result, pConn);
  }

  //TODO: submit implementation
}

int32_t dnodeCreateTable(SDCreateTableMsg *table) {
  return TSDB_CODE_SUCCESS;
}


/*
 * Remove table from local repository
 */
int32_t dnodeDropTable(int32_t vnode, int32_t sid, uint64_t uid) {
  return TSDB_CODE_SUCCESS;
}

/*
 * Create stream
 * if stream already exist, update it
 */
int32_t dnodeCreateStream(SAlterStreamMsg *stream) {
  int32_t vnode = htonl(stream->vnode);
  int32_t sid = htonl(stream->sid);
  uint64_t uid = htobe64(stream->uid);

  if (!dnodeCheckTableExist(vnode, sid, uid)) {
    return TSDB_CODE_INVALID_TABLE;
  }

  //TODO create or remove stream
}

/*
 * Remove all child tables of supertable from local repository
 */
int32_t dnodeDropSuperTable(uint64_t stableUid) {
  return TSDB_CODE_SUCCESS;
}

