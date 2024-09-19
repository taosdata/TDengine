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
#include "dmUtil.h"

const char *dmStatStr(EDndRunStatus stype) {
  switch (stype) {
    case DND_STAT_INIT:
      return "init";
    case DND_STAT_RUNNING:
      return "running";
    case DND_STAT_STOPPED:
      return "stopped";
    default:
      return "UNKNOWN";
  }
}

const char *dmNodeName(EDndNodeType ntype) {
  switch (ntype) {
    case VNODE:
      return "vnode";
    case QNODE:
      return "qnode";
    case SNODE:
      return "snode";
    case MNODE:
      return "mnode";
    default:
      return "dnode";
  }
}

void *dmSetMgmtHandle(SArray *pArray, tmsg_t msgType, void *nodeMsgFp, bool needCheckVgId) {
  SMgmtHandle handle = {
      .msgType = msgType,
      .msgFp = (NodeMsgFp)nodeMsgFp,
      .needCheckVgId = needCheckVgId,
  };

  return taosArrayPush(pArray, &handle);
}

void dmGetMonitorSystemInfo(SMonSysInfo *pInfo) {
  int32_t code = 0;
  code = taosGetCpuUsage(&pInfo->cpu_system, &pInfo->cpu_engine);
  if (code != 0) {
    dError("failed to get cpu usage since %s", tstrerror(code));
  }
  code = taosGetCpuCores(&pInfo->cpu_cores, false);
  if (code != 0) {
    dError("failed to get cpu cores since %s", tstrerror(code));
  }
  code = taosGetProcMemory(&pInfo->mem_engine);
  if (code != 0) {
    dError("failed to get proc memory since %s", tstrerror(code));
  }
  code = taosGetSysMemory(&pInfo->mem_system);
  if (code != 0) {
    dError("failed to get sys memory since %s", tstrerror(code));
  }
  pInfo->mem_total = tsTotalMemoryKB;
  pInfo->disk_engine = 0;
  pInfo->disk_used = tsDataSpace.size.used;
  pInfo->disk_total = tsDataSpace.size.total;
  code = taosGetCardInfoDelta(&pInfo->net_in, &pInfo->net_out);
  if (code != 0) {
    dError("failed to get card info since %s", tstrerror(code));
    taosSetDefaultCardInfoDelta(&pInfo->net_in, &pInfo->net_out);
  }
  code = taosGetProcIODelta(&pInfo->io_read, &pInfo->io_write, &pInfo->io_read_disk, &pInfo->io_write_disk);
  if (code != 0) {
    dError("failed to get proc io delta since %s", tstrerror(code));
    taosSetDefaultProcIODelta(&pInfo->io_read, &pInfo->io_write, &pInfo->io_read_disk, &pInfo->io_write_disk);
  }
  return;
}

int32_t dmGetDnodeId(SDnodeData *pData) { return pData->dnodeId; }