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

#include "qndInt.h"

SQnode *qndOpen(const SQnodeOpt *pOption) {
  SQnode *pQnode = calloc(1, sizeof(SQnode));
  return pQnode;
}

void qndClose(SQnode *pQnode) { free(pQnode); }

int32_t qndGetLoad(SQnode *pQnode, SQnodeLoad *pLoad) { return 0; }

int32_t qndProcessQueryReq(SQnode *pQnode, SRpcMsg *pMsg, SRpcMsg **pRsp) {
  *pRsp = NULL;
  return 0;
}

int32_t qndProcessFetchReq(SQnode *pQnode, SRpcMsg *pMsg, SRpcMsg **pRsp) {
  *pRsp = NULL;
  return 0;
}
