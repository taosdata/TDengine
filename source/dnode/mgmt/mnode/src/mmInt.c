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
 * along with this program. If not, see <http:www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "mmInt.h"
#include "mmMsg.h"

bool mmRequireNode(SMgmtWrapper *pWrapper) { return false; }

void mmGetMgmtFp(SMgmtWrapper *pWrapper) {
  SMgmtFp mgmtFp = {0};
  mgmtFp.openFp = NULL;
  mgmtFp.closeFp = NULL;
  mgmtFp.requiredFp = mmRequireNode;

  mmInitMsgHandles(pWrapper);
  pWrapper->name = "mnode";
  pWrapper->fp = mgmtFp;
}

int32_t mmGetUserAuth(SMgmtWrapper *pWrapper, char *user, char *spi, char *encrypt, char *secret, char *ckey) {
  return 0;
}

int32_t mmGetMonitorInfo(SDnode *pDnode, SMonClusterInfo *pClusterInfo, SMonVgroupInfo *pVgroupInfo,
                         SMonGrantInfo *pGrantInfo) {
  return 0;
}