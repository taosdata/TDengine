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

#include "libs/txnode/txnode.h"
#include "xndInt.h"

static SXnode xnodeInstance = {0};
SXnode       *xndInstance() { return &xnodeInstance; }

int32_t xndOpen(const SXnodeOpt *pOption, SXnode **pXnode) {
  int32_t code = 0;

  // *pXnode = taosMemoryCalloc(1, sizeof(SXnode));
  // if (NULL == *pXnode) {
  //   xndError("calloc SXnode failed");
  //   code = terrno;
  //   TAOS_RETURN(code);
  // }

  // (*pXnode)->msgCb = pOption->msgCb;
  // (*pXnode)->dnodeId = pOption->dnodeId;
  // (*pXnode)->protocol = (int8_t)pOption->proto;
  *pXnode = &xnodeInstance;
  (*pXnode)->protocol = (int8_t)pOption->proto;

  if (TSDB_XNODE_OPT_PROTO == (*pXnode)->protocol) {
    // if ((code = xnodeMgmtStartXnoded((*pXnode)->dnodeId)) != 0) {
    if ((code = xnodeMgmtStartXnoded(*pXnode)) != 0) {
      xndError("failed to start xnoded since %s", tstrerror(code));

      taosMemoryFree(*pXnode);
      TAOS_RETURN(code);
    }
  } else {
    xndError("Unknown xnode proto: %hhd.", (*pXnode)->protocol);

    taosMemoryFree(*pXnode);
    TAOS_RETURN(code);
  }

  xndInfo("Xnode opened.");

  return TSDB_CODE_SUCCESS;
}

void xndClose(SXnode *pXnode) {
  xnodeMgmtStopXnoded();

  taosMemoryFree(pXnode);

  xndInfo("Xnode closed.");
}

int32_t mndOpenXnd(const SXnodeOpt *pOption) {
  int32_t code = 0;
  SXnode *pXnode = xndInstance();
  pXnode->dnodeId = pOption->dnodeId;
  pXnode->clusterId = pOption->clusterId;
  pXnode->upLen = pOption->upLen;
  pXnode->ep = pOption->ep;
  memset(pXnode->userPass, 0, XNODE_USER_PASS_LEN);
  memcpy(pXnode->userPass, pOption->userPass, pOption->upLen);

  if ((code = xnodeMgmtStartXnoded(pXnode)) != 0) {
    xndError("failed to start xnoded since %s", tstrerror(code));

    TAOS_RETURN(code);
  }
  return code;
}

void mndCloseXnd() { xnodeMgmtStopXnoded(); }

void getXnodedPipeName(char *pipeName, int32_t size) {
#ifdef _WIN32
  snprintf(pipeName, size, "%s.%x", XNODED_MGMT_LISTEN_PIPE_NAME_PREFIX, MurmurHash3_32(tsDataDir, strlen(tsDataDir)));
#else
  snprintf(pipeName, size, "%s/%s", tsDataDir, XNODED_MGMT_LISTEN_PIPE_NAME_PREFIX);
#endif
  xndInfo("get unix socket pipe path:%s", pipeName);
}
