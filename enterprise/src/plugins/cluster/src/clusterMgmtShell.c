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

#include <arpa/inet.h>

#include "dnodeSystem.h"
#include "mnode.h"
#include "mgmtProfile.h"
#include "taosmsg.h"
#include "tlog.h"
#include "dnodeModule.h"

#define MAX_LEN_OF_METER_META (sizeof(SMultiTableMeta) + sizeof(SSchema) * TSDB_MAX_COLUMNS + sizeof(SSchema) * TSDB_MAX_TAGS + TSDB_MAX_TAGS_LEN)

void *mgmtProcessMsgFromShell(char *msg, void *ahandle, void *thandle);
int (*mgmtProcessShellMsg[TSDB_MSG_TYPE_MAX])(char *, int, void *);
void  mgmtInitProcessShellMsg();



int mgmtCheckRedirectMsg(void *pConn) {
  // if is running
  if (!sdbMaster) {
    rpcSendRedirectRsp(pConn, NULL);
    return 1;
  } else {
    return 0;
  }

  //if not running

  //  if (pConn->usePublicIp) {
  //    size = sizeof(SIpList) + pSdbPublicIpList->numOfIps * 4;
  //    memcpy(pMsg, pSdbPublicIpList, size);
  //    pMsg += size;
  //  } else {
  //    size = sizeof(SIpList) + pSdbIpList->numOfIps * 4;
  //    memcpy(pMsg, pSdbIpList, size);
  //    pMsg += size;
  //  }
  //

  return 0;
}

