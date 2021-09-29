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
#include "tglobal.h"
#include "dnodeCheck.h"

#define MIN_AVAIL_MEMORY_MB 32

static int32_t dnodeBindTcpPort(uint16_t port) {
#if 0
  SOCKET             serverSocket;
  struct sockaddr_in server_addr;

  if ((serverSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
    dError("failed to create tcp socket since %s", strerror(errno));
    return -1;
  }

  bzero(&server_addr, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);

  if (bind(serverSocket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
    dError("failed to bind tcp port:%d since %s", port, strerror(errno));
    taosCloseSocket(serverSocket);
    return -1;
  }

  if (listen(serverSocket, 5) < 0) {
    dError("failed to listen tcp port:%d since %s", port, strerror(errno));
    taosCloseSocket(serverSocket);
    return -1;
  }

  taosCloseSocket(serverSocket);
#endif
  return 0;
}

static int32_t dnodeBindUdpPort(int16_t port) {
#if 0
  SOCKET             serverSocket;
  struct sockaddr_in server_addr;

  if ((serverSocket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) < 0) {
    dError("failed to create udp socket since %s", strerror(errno));
    return -1;
  }

  bzero(&server_addr, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);

  if (bind(serverSocket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
    dError("failed to bind udp port:%d since %s", port, strerror(errno));
    taosCloseSocket(serverSocket);
    return -1;
  }

  taosCloseSocket(serverSocket);
#endif
  return 0;
}

static int32_t dnodeCheckNetwork() {
  int32_t  ret;
  uint16_t startPort = tsServerPort;

  for (uint16_t port = startPort; port < startPort + 12; port++) {
    ret = dnodeBindTcpPort(port);
    if (0 != ret) {
      dError("failed to bind tcp port:%d", port);
      return -1;
    }
    ret = dnodeBindUdpPort(port);
    if (0 != ret) {
      dError("failed to bind udp port:%d", port);
      return -1;
    }
  }

  return 0;
}

static int32_t dnodeCheckMem() {
  float memoryUsedMB;
  float memoryAvailMB;
  if (true != taosGetSysMemory(&memoryUsedMB)) {
    dError("failed to get system memory since %s, errno:%u,", strerror(errno), errno);
    return -1;
  }

  memoryAvailMB = (float)tsTotalMemoryMB - memoryUsedMB;

  if (memoryAvailMB < MIN_AVAIL_MEMORY_MB) {
    dError("available memory %fMB less than the threshold %dMB", memoryAvailMB, MIN_AVAIL_MEMORY_MB);
    return -1;
  }

  return 0;
}

static int32_t dnodeCheckDisk() {
  taosGetDisk();

  if (tsAvailDataDirGB < tsMinimalDataDirGB) {
    dError("dataDir disk size:%fGB less than threshold %fGB ", tsAvailDataDirGB, tsMinimalDataDirGB);
    return -1;
  }

  if (tsAvailLogDirGB < tsMinimalLogDirGB) {
    dError("logDir disk size:%fGB less than threshold %fGB", tsAvailLogDirGB, tsMinimalLogDirGB);
    return -1;
  }

  if (tsAvailTmpDirectorySpace < tsReservedTmpDirectorySpace) {
    dError("tmpDir disk size:%fGB less than threshold %fGB", tsAvailTmpDirectorySpace, tsReservedTmpDirectorySpace);
    return -1;
  }

  return 0;
}

static int32_t dnodeCheckCpu() { return 0; }
static int32_t dnodeCheckOs() { return 0; }
static int32_t dnodeCheckAccess() { return 0; }
static int32_t dnodeCheckVersion() { return 0; }
static int32_t dnodeCheckDatafile() { return 0; }

int32_t dnodeInitCheck(Dnode *dnode, DnCheck **out) {
  DnCheck *check = calloc(1, sizeof(DnCheck));
  if (check == NULL) return -1;

  check->dnode = dnode;
  *out = check;

  if (dnodeCheckNetwork() != 0) {
    dError("failed to check network");
    return -1;
  }

  if (dnodeCheckMem() != 0) {
    dError("failed to check memory");
    return -1;
  }

  if (dnodeCheckCpu() != 0) {
    dError("failed to check cpu");
    return -1;
  }

  if (dnodeCheckDisk() != 0) {
    dError("failed to check disk");
    return -1;
  }

  if (dnodeCheckOs() != 0) {
    dError("failed to check os");
    return -1;
  }

  if (dnodeCheckAccess() != 0) {
    dError("failed to check access");
    return -1;
  }

  if (dnodeCheckVersion() != 0) {
    dError("failed to check version");
    return -1;
  }

  if (dnodeCheckDatafile() != 0) {
    dError("failed to check datafile");
    return -1;
  }

  dInfo("dnode check is finished");
  return 0;
}

void dnodeCleanupCheck(DnCheck **out) {
  DnCheck *check = *out;
  *out = NULL;

  free(check);
}