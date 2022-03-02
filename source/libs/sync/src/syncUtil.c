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

#include "syncUtil.h"
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

// ---- encode / decode
uint64_t syncUtilAddr2U64(const char* host, uint16_t port) {
  uint64_t u64;
  uint32_t hostU32 = (uint32_t)inet_addr(host);
  // assert(hostU32 != (uint32_t)-1);
  u64 = (((uint64_t)hostU32) << 32) | (((uint32_t)port) << 16);
  return u64;
}

void syncUtilU642Addr(uint64_t u64, char* host, size_t len, uint16_t* port) {
  uint32_t hostU32 = (uint32_t)((u64 >> 32) & 0x00000000FFFFFFFF);

  struct in_addr addr;
  addr.s_addr = hostU32;
  snprintf(host, len, "%s", inet_ntoa(addr));
  *port = (uint16_t)((u64 & 0x00000000FFFF0000) >> 16);
}

void syncUtilnodeInfo2EpSet(const SNodeInfo* pNodeInfo, SEpSet* pEpSet) {
  pEpSet->inUse = 0;
  pEpSet->numOfEps = 0;
  addEpIntoEpSet(pEpSet, pNodeInfo->nodeFqdn, pNodeInfo->nodePort);
}

void syncUtilraftId2EpSet(const SRaftId* raftId, SEpSet* pEpSet) {
  char     host[TSDB_FQDN_LEN];
  uint16_t port;

  syncUtilU642Addr(raftId->addr, host, sizeof(host), &port);

  /*
    pEpSet->numOfEps = 1;
    pEpSet->inUse = 0;
    pEpSet->eps[0].port = port;
    snprintf(pEpSet->eps[0].fqdn, sizeof(pEpSet->eps[0].fqdn), "%s", host);
  */
  pEpSet->inUse = 0;
  pEpSet->numOfEps = 0;
  addEpIntoEpSet(pEpSet, host, port);
}

void syncUtilnodeInfo2raftId(const SNodeInfo* pNodeInfo, SyncGroupId vgId, SRaftId* raftId) {
  uint32_t ipv4 = taosGetIpv4FromFqdn(pNodeInfo->nodeFqdn);
  assert(ipv4 != 0xFFFFFFFF);
  char ipbuf[128];
  tinet_ntoa(ipbuf, ipv4);
  raftId->addr = syncUtilAddr2U64(ipbuf, pNodeInfo->nodePort);
  raftId->vgId = vgId;
}

// ---- SSyncBuffer -----
#if 0
void syncUtilbufBuild(SSyncBuffer* syncBuf, size_t len) {
  syncBuf->len = len;
  syncBuf->data = malloc(syncBuf->len);
}

void syncUtilbufDestroy(SSyncBuffer* syncBuf) { free(syncBuf->data); }

void syncUtilbufCopy(const SSyncBuffer* src, SSyncBuffer* dest) {
  dest->len = src->len;
  dest->data = src->data;
}

void syncUtilbufCopyDeep(const SSyncBuffer* src, SSyncBuffer* dest) {
  dest->len = src->len;
  dest->data = malloc(dest->len);
  memcpy(dest->data, src->data, dest->len);
}
#endif