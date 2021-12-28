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

#include "sndInt.h"

SSnode *sndOpen(const char *path, const SSnodeOpt *pOption) {
  SSnode *pSnode = calloc(1, sizeof(SSnode));
  return pSnode;
}

void sndClose(SSnode *pSnode) { free(pSnode); }

int32_t sndGetLoad(SSnode *pSnode, SSnodeLoad *pLoad) { return 0; }

int32_t sndProcessWriteMsg(SSnode *pSnode, SRpcMsg *pMsg, SRpcMsg **pRsp) {
  *pRsp = NULL;
  return 0;
}

void sndDestroy(const char *path) {}