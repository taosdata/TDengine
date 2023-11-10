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

bool    taosCheckSystemIsLittleEnd();
void    taosGetSystemInfo();
int64_t taosGetOsUptime();
int32_t taosGetEmail(char *email, int32_t maxLen);
int32_t taosGetOsReleaseName(char *releaseName, char* sName, char* ver, int32_t maxLen);
int32_t taosGetCpuInfo(char *cpuModel, int32_t maxLen, float *numOfCores);
int32_t taosGetCpuCores(float *numOfCores, bool physical);
void    taosGetCpuUsage(double *cpu_system, double *cpu_engine);
int32_t taosGetCpuInstructions(char* sse42, char* avx, char* avx2, char* fma, char* avx512);
int32_t taosGetTotalMemory(int64_t *totalKB);
int32_t taosGetProcMemory(int64_t *usedKB);
int32_t taosGetSysMemory(int64_t *usedKB);
int32_t taosGetDiskSize(char *dataDir, SDiskSize *diskSize);
void    taosGetProcIODelta(int64_t *rchars, int64_t *wchars, int64_t *read_bytes, int64_t *write_bytes);
void    taosGetCardInfoDelta(int64_t *receive_bytes, int64_t *transmit_bytes);

void    taosKillSystem();
int32_t taosGetSystemUUID(char *uid, int32_t uidlen);
char   *taosGetCmdlineByPID(int32_t pid);
void    taosSetCoreDump(bool enable);

#if !defined(LINUX)

#define _UTSNAME_LENGTH         65
#define _UTSNAME_MACHINE_LENGTH _UTSNAME_LENGTH

#endif  // WINDOWS

#if defined(_ALPINE)

#define _UTSNAME_LENGTH         65
#define _UTSNAME_MACHINE_LENGTH _UTSNAME_LENGTH

#endif

typedef struct {
  char sysname[_UTSNAME_MACHINE_LENGTH];
  char nodename[_UTSNAME_MACHINE_LENGTH];
  char release[_UTSNAME_MACHINE_LENGTH];
  char version[_UTSNAME_MACHINE_LENGTH];
  char machine[_UTSNAME_MACHINE_LENGTH];
} SysNameInfo;

SysNameInfo taosGetSysNameInfo();
bool        taosCheckCurrentInDll();
int         taosGetlocalhostname(char *hostname, size_t maxLen);

#ifdef __cplusplus
}
#endif

#endif
