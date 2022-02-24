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

SOsEnv env = {0};
char   configDir[PATH_MAX] = {0};

SOsEnv *osEnv() { return &env; }

void osInitImp() {
  osGetSystemTimezone(env.timezone);
  osSetTimezone(env.timezone);
}

void osUpdate() {
  if (env.logDir[0] != 0) {
    taosGetDiskSize(env.logDir, &env.logSpace.size);
  }
  if (env.dataDir[0] != 0) {
    taosGetDiskSize(env.dataDir, &env.dataSpace.size);
  }
  if (env.tempDir[0] != 0) {
    taosGetDiskSize(env.tempDir, &env.tempSpace.size);
  }
}

bool osLogSpaceAvailable() { return env.logSpace.reserved < env.logSpace.size.avail; }

char *osLogDir() { return env.logDir; }
char *osTempDir() { return env.tempDir; }
char *osDataDir() { return env.dataDir; }
char *osName() { return env.osName; }
char *osTimezone() { return env.timezone; }

int8_t osDaylight() { return env.daylight; }

void osSetTimezone(const char *timezone) { osSetSystemTimezone(timezone, env.timezone, &env.daylight); }

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)

extern taosWinSocketInit();

void osInit() {
  srand(taosSafeRand());
  taosWinSocketInit();

  const char *tmpDir = getenv("tmp");
  if (tmpDir == NULL) {
    tmpDir = getenv("temp");
  }
  if (tmpDir != NULL) {
    strcpy(env.tempDir, tmpDir);
  }

  strcpy(configDir, "C:\\TDengine\\cfg");
  strcpy(env.dataDir, "C:\\TDengine\\data");
  strcpy(env.logDir, "C:\\TDengine\\log");
  strcpy(env.tempDir, "C:\\Windows\\Temp");
  strcpy(env.osName, "Windows");
}

#elif defined(_TD_DARWIN_64)

void osInit() {
  srand(taosSafeRand());
  strcpy(configDir, "/tmp/taosd");
  strcpy(env.dataDir, "/usr/local/var/lib/taos");
  strcpy(env.logDir, "/usr/local/var/log/taos");
  strcpy(env.tempDir, "/usr/local/etc/taos");
  strcpy(env.osName, "Darwin");
}
#else

void osInit() {
  srand(taosSafeRand());
  strcpy(configDir, "/etc/taos");
  strcpy(env.dataDir, "/var/lib/taos");
  strcpy(env.logDir, "/var/log/taos");
  strcpy(env.tempDir, "/tmp");
  strcpy(env.osName, "Linux");
}

#endif