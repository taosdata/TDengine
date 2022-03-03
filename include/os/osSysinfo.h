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

#ifndef _TD_OS_SYSINFO_H_
#define _TD_OS_SYSINFO_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  int64_t total;
  int64_t used;
  int64_t avail;
} SDiskSize;

typedef struct {
  int64_t   reserved;
  SDiskSize size;
} SDiskSpace;

void    taosGetSystemInfo();
bool    taosGetEmail(char *email, int32_t maxLen);
bool    taosGetOsReleaseName(char *releaseName, int32_t maxLen);
bool    taosGetCpuInfo(char *cpuModel, int32_t maxLen, int32_t *numOfCores);
int32_t taosGetCpuCores();
bool    taosGetCpuUsage(float *sysCpuUsage, float *procCpuUsage);
bool    taosGetTotalSysMemoryKB(uint64_t *kb);
bool    taosGetProcMemory(float *memoryUsedMB);  //
bool    taosGetSysMemory(float *memoryUsedMB);   //
int32_t taosGetDiskSize(char *dataDir, SDiskSize *diskSize);
bool    taosReadProcIO(int64_t *rchars, int64_t *wchars);
bool    taosGetProcIO(float *readKB, float *writeKB);
bool    taosGetCardInfo(int64_t *bytes, int64_t *rbytes, int64_t *tbytes);
bool    taosGetBandSpeed(float *bandSpeedKb);

int32_t taosSystem(const char *cmd);
void    taosKillSystem();
int32_t taosGetSystemUUID(char *uid, int32_t uidlen);
char   *taosGetCmdlineByPID(int32_t pid);
void    taosSetCoreDump(bool enable);

typedef struct {
  char sysname[_UTSNAME_MACHINE_LENGTH];
  char nodename[_UTSNAME_MACHINE_LENGTH];
  char release[_UTSNAME_MACHINE_LENGTH];
  char version[_UTSNAME_MACHINE_LENGTH];
  char machine[_UTSNAME_MACHINE_LENGTH];
} SysNameInfo;

SysNameInfo taosGetSysNameInfo();

#ifdef __cplusplus
}
#endif

#endif
