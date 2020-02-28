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
#include "tutil.h"
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

int32_t dnodeCreateTable(SDCreateTableMsg *pTable) {
  if (pTable->tableType == TSDB_TABLE_TYPE_CHILD_TABLE) {
    dTrace("table:%s, start to create child table, stable:%s", pTable->tableId, pTable->superTableId);
  } else if (pTable->tableType == TSDB_TABLE_TYPE_NORMAL_TABLE){
    dTrace("table:%s, start to create normal table", pTable->tableId);
  } else if (pTable->tableType == TSDB_TABLE_TYPE_STREAM_TABLE){
    dTrace("table:%s, start to create stream table", pTable->tableId);
  } else {
    dError("table:%s, invalid table type:%d", pTable->tableType);
  }

  for (int i = 0; i < pTable->numOfVPeers; ++i) {
    dTrace("table:%s ip:%s vnode:%d sid:%d", pTable->tableId, taosIpStr(pTable->vpeerDesc[i].ip),
           pTable->vpeerDesc[i].vnode, pTable->sid);
  }

  SSchema *pSchema = (SSchema *) pTable->data;
  for (int32_t col = 0; col < pTable->numOfColumns; ++col) {
    dTrace("table:%s col index:%d colId:%d bytes:%d type:%d name:%s",
           pTable->tableId, col, pSchema->colId, pSchema->bytes, pSchema->type, pSchema->name);
    pSchema++;
  }
  for (int32_t col = 0; col < pTable->numOfTags; ++col) {
    dTrace("table:%s tag index:%d colId:%d bytes:%d type:%d name:%s",
           pTable->tableId, col, pSchema->colId, pSchema->bytes, pSchema->type, pSchema->name);
    pSchema++;
  }

  return TSDB_CODE_SUCCESS;
}


/*
 * Remove table from local repository
 */
int32_t dnodeDropTable(SDRemoveTableMsg *pTable) {
  dPrint("table:%s, sid:%d is removed", pTable->tableId, pTable->sid);
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

  return 0;
}

/*
 * Remove all child tables of supertable from local repository
 */
int32_t dnodeDropSuperTable(uint64_t stableUid) {
  return TSDB_CODE_SUCCESS;
}

