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
#include "balance.h"
#include "mpeer.h"

int32_t replicaInit() {
  int32_t code = balanceInit();
  if (code != 0) return code;

  return mpeerInit();
}

void replicaCleanUp() {
  balanceCleanUp();
  mpeerCleanUp();
}

void replicaNotify() {
  mpeerNotify();
  balanceNotify();
}

void replicaReset() { balanceReset(); }
int32_t replicaAllocVnodes(struct SVgObj *pVgroup) { return balanceAllocVnodes(pVgroup); }
int32_t replicaForwardReqToPeer(void *pHead) { return mpeerForwardReqToPeer(pHead); }
int32_t replicaDropDnode(struct SDnodeObj *pDnode) { return balanceDropDnode(pDnode); }