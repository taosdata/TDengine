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
#include "dnodeCheck.h"

typedef struct {
  bool      enable;
  char *    name;
  int32_t (*initFp)();
  int32_t (*startFp)();
  void    (*cleanUpFp)();
  void    (*stopFp)();
} SCheckItem;

static  SCheckItem  tsCheckItem[TSDB_CHECK_ITEM_MAX] = {{0}};
int64_t tsMinFreeMemSizeForStart = 0;

static int32_t bindTcpPort(int16_t port) {
  SOCKET serverSocket;
  struct sockaddr_in server_addr;

  if ((serverSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
    dError("socket() fail: %s", strerror(errno));
    return -1;
  }

  bzero(&server_addr, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);

  if (bind(serverSocket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
    dError("port:%d tcp bind() fail: %s", port, strerror(errno));
    taosCloseSocket(serverSocket);
    return -1;
  }

  if (listen(serverSocket, 5) < 0) {
    dError("port:%d listen() fail: %s", port, strerror(errno));
    taosCloseSocket(serverSocket);
    return -1;
  }

  taosCloseSocket(serverSocket);
  return 0;
}

static int32_t bindUdpPort(int16_t port) {
  SOCKET serverSocket;
  struct sockaddr_in server_addr;
    
  if ((serverSocket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) < 0) {
    dError("socket() fail: %s", strerror(errno));
    return -1;
  }

  bzero(&server_addr, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);

  if (bind(serverSocket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
    dError("port:%d udp bind() fail: %s", port, strerror(errno));
    taosCloseSocket(serverSocket);
    return -1;
  }

  taosCloseSocket(serverSocket);
  return 0;
}

static int32_t dnodeCheckNetwork() {
  int32_t ret;
  int16_t startPort = tsServerPort;

  for (int16_t port = startPort; port < startPort + 12; port++) {
    ret = bindTcpPort(port);
    if (0 != ret) {
      dError("failed to tcp bind port %d, quit", port);
      return -1;
    }
    ret = bindUdpPort(port);
    if (0 != ret) {
      dError("failed to udp bind port %d, quit", port);
      return -1;
    }    
  } 

  return 0;
}

static int32_t dnodeCheckMem() {
  float  memoryUsedMB;
  float  memoryAvailMB;
  if (true != taosGetSysMemory(&memoryUsedMB)) {
    dError("failed to get system mem infomation, errno:%u, reason:%s", errno, strerror(errno));
    return -1;
  }

  memoryAvailMB = (float)tsTotalMemoryMB - memoryUsedMB;

  if (memoryAvailMB < tsMinFreeMemSizeForStart) {
    dError("free mem %f too little, quit", memoryAvailMB);
    return -1;
  }

  return 0;  
}

static int32_t dnodeCheckCpu() {
  // TODO:  
  return 0;
}

static int32_t dnodeCheckDisk() {
  taosGetDisk();

  if (tsAvailDataDirGB < tsMinimalDataDirGB) {
    dError("free disk size: %f GB, too little, quit", tsAvailDataDirGB);
    return -1;
  }

  if (tsAvailLogDirGB < tsMinimalLogDirGB) {
    dError("free disk size: %f GB, too little, quit", tsAvailLogDirGB);
    return -1;
  }

  if (tsAvailTmpDirectorySpace < tsReservedTmpDirectorySpace) {
    dError("free disk size: %f GB, too little, quit", tsAvailTmpDirectorySpace);
    return -1;
  }

  return 0;
}

static int32_t dnodeCheckOs() {
  // TODO:

  return 0;
}
static int32_t dnodeCheckAccess() {
  // TODO:

  return 0;
}

static int32_t dnodeCheckVersion() {
  // TODO:

  return 0;
}

static int32_t dnodeCheckDatafile() {
  // TODO:

  return 0;
}

static void dnodeAllocCheckItem() {
  tsCheckItem[TSDB_CHECK_ITEM_NETWORK].enable       = false;
  tsCheckItem[TSDB_CHECK_ITEM_NETWORK].name         = "network";
  tsCheckItem[TSDB_CHECK_ITEM_NETWORK].initFp       = NULL;
  tsCheckItem[TSDB_CHECK_ITEM_NETWORK].cleanUpFp    = NULL;
  tsCheckItem[TSDB_CHECK_ITEM_NETWORK].startFp      = dnodeCheckNetwork;
  tsCheckItem[TSDB_CHECK_ITEM_NETWORK].stopFp       = NULL;

  tsCheckItem[TSDB_CHECK_ITEM_MEM].enable       = true;
  tsCheckItem[TSDB_CHECK_ITEM_MEM].name         = "mem";
  tsCheckItem[TSDB_CHECK_ITEM_MEM].initFp       = NULL;
  tsCheckItem[TSDB_CHECK_ITEM_MEM].cleanUpFp    = NULL;
  tsCheckItem[TSDB_CHECK_ITEM_MEM].startFp      = dnodeCheckMem;
  tsCheckItem[TSDB_CHECK_ITEM_MEM].stopFp       = NULL;

  tsCheckItem[TSDB_CHECK_ITEM_CPU].enable       = true;
  tsCheckItem[TSDB_CHECK_ITEM_CPU].name         = "cpu";
  tsCheckItem[TSDB_CHECK_ITEM_CPU].initFp       = NULL;
  tsCheckItem[TSDB_CHECK_ITEM_CPU].cleanUpFp    = NULL;
  tsCheckItem[TSDB_CHECK_ITEM_CPU].startFp      = dnodeCheckCpu;
  tsCheckItem[TSDB_CHECK_ITEM_CPU].stopFp       = NULL;

  tsCheckItem[TSDB_CHECK_ITEM_DISK].enable       = true;
  tsCheckItem[TSDB_CHECK_ITEM_DISK].name         = "disk";
  tsCheckItem[TSDB_CHECK_ITEM_DISK].initFp       = NULL;
  tsCheckItem[TSDB_CHECK_ITEM_DISK].cleanUpFp    = NULL;
  tsCheckItem[TSDB_CHECK_ITEM_DISK].startFp      = dnodeCheckDisk;
  tsCheckItem[TSDB_CHECK_ITEM_DISK].stopFp       = NULL;

  tsCheckItem[TSDB_CHECK_ITEM_OS].enable       = true;
  tsCheckItem[TSDB_CHECK_ITEM_OS].name         = "os";
  tsCheckItem[TSDB_CHECK_ITEM_OS].initFp       = NULL;
  tsCheckItem[TSDB_CHECK_ITEM_OS].cleanUpFp    = NULL;
  tsCheckItem[TSDB_CHECK_ITEM_OS].startFp      = dnodeCheckOs;
  tsCheckItem[TSDB_CHECK_ITEM_OS].stopFp       = NULL;

  tsCheckItem[TSDB_CHECK_ITEM_ACCESS].enable       = true;
  tsCheckItem[TSDB_CHECK_ITEM_ACCESS].name         = "access";
  tsCheckItem[TSDB_CHECK_ITEM_ACCESS].initFp       = NULL;
  tsCheckItem[TSDB_CHECK_ITEM_ACCESS].cleanUpFp    = NULL;
  tsCheckItem[TSDB_CHECK_ITEM_ACCESS].startFp      = dnodeCheckAccess;
  tsCheckItem[TSDB_CHECK_ITEM_ACCESS].stopFp       = NULL;

  tsCheckItem[TSDB_CHECK_ITEM_VERSION].enable       = true;
  tsCheckItem[TSDB_CHECK_ITEM_VERSION].name         = "version";
  tsCheckItem[TSDB_CHECK_ITEM_VERSION].initFp       = NULL;
  tsCheckItem[TSDB_CHECK_ITEM_VERSION].cleanUpFp    = NULL;
  tsCheckItem[TSDB_CHECK_ITEM_VERSION].startFp      = dnodeCheckVersion;
  tsCheckItem[TSDB_CHECK_ITEM_VERSION].stopFp       = NULL;

  tsCheckItem[TSDB_CHECK_ITEM_DATAFILE].enable       = true;
  tsCheckItem[TSDB_CHECK_ITEM_DATAFILE].name         = "datafile";
  tsCheckItem[TSDB_CHECK_ITEM_DATAFILE].initFp       = NULL;
  tsCheckItem[TSDB_CHECK_ITEM_DATAFILE].cleanUpFp    = NULL;
  tsCheckItem[TSDB_CHECK_ITEM_DATAFILE].startFp      = dnodeCheckDatafile;
  tsCheckItem[TSDB_CHECK_ITEM_DATAFILE].stopFp       = NULL;
}

void dnodeCleanupCheck() {
  for (ECheckItemType index = 0; index < TSDB_CHECK_ITEM_MAX; ++index) {
    if (tsCheckItem[index].enable && tsCheckItem[index].stopFp) {
      (*tsCheckItem[index].stopFp)();
    }
    if (tsCheckItem[index].cleanUpFp) {
      (*tsCheckItem[index].cleanUpFp)();
    }
  }
}

int32_t dnodeInitCheck() {
  dnodeAllocCheckItem();

  for (ECheckItemType index = 0; index < TSDB_CHECK_ITEM_MAX; ++index) {
    if (tsCheckItem[index].initFp) {
      if ((*tsCheckItem[index].initFp)() != 0) {
        dError("failed to init check item:%s", tsCheckItem[index].name);
        return -1;
      }
    }
  }

  for (ECheckItemType index = 0; index < TSDB_CHECK_ITEM_MAX; ++index) {
    if (tsCheckItem[index].enable && tsCheckItem[index].startFp) {
      if ((*tsCheckItem[index].startFp)() != 0) {
        dError("failed to check item:%s", tsCheckItem[index].name);
        exit(-1);
      }
    }
  }

  dInfo("dnode check is initialized");
  return 0;
}
