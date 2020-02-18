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

int32_t dnodeCreateNormalTable(SCreateNormalTableMsg *table) {
  return 0;
}

int32_t dnodeCreateStreamTable(SCreateStreamTableMsg *table) {
  return 0;
}

int32_t dnodeCreateChildTable(SCreateChildTableMsg *table) {
  return 0;
}

int32_t dnodeAlterNormalTable(SCreateNormalTableMsg *table) {
  return 0;
}

int32_t dnodeAlterStreamTable(SCreateStreamTableMsg *table) {
  return 0;
}

int32_t dnodeAlterChildTable(SCreateChildTableMsg *table) {
  return 0;
}

int32_t dnodeDropSuperTable(int vid, int sid, int64_t uid) {
  return 0;
}

int32_t dnodeDropTable(int vid, int sid, int64_t uid) {
  return 0;
}

