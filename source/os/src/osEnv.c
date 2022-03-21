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
#include "osEnv.h"

extern void taosWinSocketInit();

char       configDir[PATH_MAX] = {0};
char       tsDataDir[PATH_MAX] = {0};
char       tsLogDir[PATH_MAX] = {0};
char       tsTempDir[PATH_MAX] = {0};
SDiskSpace tsDataSpace = {0};
SDiskSpace tsLogSpace = {0};
SDiskSpace tsTempSpace = {0};
char       tsOsName[16] = {0};
char       tsTimezone[TD_TIMEZONE_LEN] = {0};
char       tsLocale[TD_LOCALE_LEN] = {0};
char       tsCharset[TD_CHARSET_LEN] = {0};
int8_t     tsDaylight = 0;
bool       tsEnableCoreFile = 0;
int64_t    tsPageSizeKB = 0;
int64_t    tsOpenMax = 0;
int64_t    tsStreamMax = 0;
float      tsNumOfCores = 0;
int64_t    tsTotalMemoryKB = 0;

void osInit() {
  taosSeedRand(taosSafeRand());
  taosGetSystemLocale(tsLocale, tsCharset);
  taosGetSystemTimezone(tsTimezone);
  taosSetSystemTimezone(tsTimezone, tsTimezone, &tsDaylight);
  taosGetSystemInfo();

  // deadlock in query
  if (tsNumOfCores < 2) {
    tsNumOfCores = 2;
  }

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  taosWinSocketInit();

  const char *tmpDir = getenv("tmp");
  if (tmpDir == NULL) {
    tmpDir = getenv("temp");
  }
  if (tmpDir != NULL) {
    strcpy(tsTempDir, tmpDir);
  }

  if (configDir[0] == 0) {
    strcpy(configDir, "C:\\TDengine\\cfg");
  }
  strcpy(tsDataDir, "C:\\TDengine\\data");
  strcpy(tsLogDir, "C:\\TDengine\\log");
  strcpy(tsTempDir, "C:\\Windows\\Temp");
  strcpy(tsOsName, "Windows");

#elif defined(_TD_DARWIN_64)
  if (configDir[0] == 0) {
    strcpy(configDir, "/tmp/taosd");
  }
  strcpy(tsDataDir, "/usr/local/var/lib/taos");
  strcpy(tsLogDir, "/usr/local/var/log/taos");
  strcpy(tsTempDir, "/usr/local/etc/taos");
  strcpy(tsOsName, "Darwin");

#else
  if (configDir[0] == 0) {
    strcpy(configDir, "/etc/taos");
  }
  strcpy(tsDataDir, "/var/lib/taos");
  strcpy(tsLogDir, "/var/log/taos");
  strcpy(tsTempDir, "/tmp");
  strcpy(tsOsName, "Linux");

#endif
}

void osUpdate() {
  if (tsLogDir[0] != 0) {
    taosGetDiskSize(tsLogDir, &tsLogSpace.size);
  }
  if (tsDataDir[0] != 0) {
    taosGetDiskSize(tsDataDir, &tsDataSpace.size);
  }
  if (tsTempDir[0] != 0) {
    taosGetDiskSize(tsTempDir, &tsTempSpace.size);
  }
}

void osCleanup() {}

bool osLogSpaceAvailable() { return tsLogSpace.reserved <= tsLogSpace.size.avail; }

void osSetTimezone(const char *timezone) { taosSetSystemTimezone(tsTimezone, tsTimezone, &tsDaylight); }
