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
#include "mgmt.h"

int mgmtCheckRedirectMsg(SConnObj *pConn, int msgType) { return 0; }

int mgmtProcessAlterAcctMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  return taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_ALTER_ACCT_RSP, TSDB_CODE_OPS_NOT_SUPPORT);
}

int mgmtProcessCreateDnodeMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  return taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_CREATE_DNODE_RSP, TSDB_CODE_OPS_NOT_SUPPORT);
}

int mgmtProcessCfgMnodeMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  return taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_CFG_MNODE_RSP, TSDB_CODE_OPS_NOT_SUPPORT);
}

int mgmtProcessDropMnodeMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  return taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_DROP_MNODE_RSP, TSDB_CODE_OPS_NOT_SUPPORT);
}

int mgmtProcessDropDnodeMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  return taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_DROP_DNODE_RSP, TSDB_CODE_OPS_NOT_SUPPORT);
}

int mgmtProcessDropAcctMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  return taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_DROP_ACCT_RSP, TSDB_CODE_OPS_NOT_SUPPORT);
}

int mgmtProcessCreateAcctMsg(char *pMsg, int msgLen, SConnObj *pConn) {
  return taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_CREATE_ACCT_RSP, TSDB_CODE_OPS_NOT_SUPPORT);
}

void mgmtGetDnodeOnlineNum(int32_t *totalDnodes, int32_t *onlineDnodes) {
  *totalDnodes = 1;
  *onlineDnodes = 1;
}