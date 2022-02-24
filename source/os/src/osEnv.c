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
char configDir[PATH_MAX] = {0};

typedef struct SOsEnv {
  char       dataDir[PATH_MAX];
  char       logDir[PATH_MAX];
  char       tempDir[PATH_MAX];
  SDiskSpace dataSpace;
  SDiskSpace logSpace;
  SDiskSpace tempSpace;
  char       osName[16];
  char       timezone[TD_TIMEZONE_LEN];
  char       locale[TD_LOCALE_LEN];
  char       charset[TD_CHARSET_LEN];
  int8_t     daylight;
  bool       enableCoreFile;
} SOsEnv;

static SOsEnv env = {0};

SOsEnv *osEnv() { return &env; }

void osInitImp() {
  taosGetSystemLocale(env.locale, env.charset);
  taosGetSystemTimezone(env.timezone);
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

bool   osLogSpaceAvailable() { return env.logSpace.reserved <= env.logSpace.size.avail; }
int8_t osDaylight() { return env.daylight; }

const char *osLogDir() { return env.logDir; }
const char *osTempDir() { return env.tempDir; }
const char *osDataDir() { return env.dataDir; }
const char *osName() { return env.osName; }
const char *osTimezone() { return env.timezone; }
const char *osLocale() { return env.locale; }
const char *osCharset() { return env.charset; }

void osSetLogDir(const char *logDir) { tstrncpy(env.logDir, logDir, PATH_MAX); }
void osSetTempDir(const char *tempDir) { tstrncpy(env.tempDir, tempDir, PATH_MAX); }
void osSetDataDir(const char *dataDir) { tstrncpy(env.dataDir, dataDir, PATH_MAX); }
void osSetLogReservedSpace(float sizeInGB) { env.logSpace.reserved = sizeInGB; }
void osSetTempReservedSpace(float sizeInGB) { env.tempSpace.reserved = sizeInGB; }
void osSetDataReservedSpace(float sizeInGB) { env.dataSpace.reserved = sizeInGB; }
void osSetTimezone(const char *timezone) { taosSetSystemTimezone(timezone, env.timezone, &env.daylight); }
void osSetLocale(const char *locale, const char *charset) { taosSetSystemLocale(locale, charset); }
bool osSetEnableCore(bool enable) { env.enableCoreFile = enable; }

void osInit() {
  srand(taosSafeRand());
  taosGetSystemInfo();

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  taosWinSocketInit();

  const char *tmpDir = getenv("tmp");
  if (tmpDir == NULL) {
    tmpDir = getenv("temp");
  }
  if (tmpDir != NULL) {
    strcpy(env.tempDir, tmpDir);
  }

  if (configDir[0] == 0) {
    strcpy(configDir, "C:\\TDengine\\cfg");
  }
  strcpy(env.dataDir, "C:\\TDengine\\data");
  strcpy(env.logDir, "C:\\TDengine\\log");
  strcpy(env.tempDir, "C:\\Windows\\Temp");
  strcpy(env.osName, "Windows");

#elif defined(_TD_DARWIN_64)
  if (configDir[0] == 0) {
    strcpy(configDir, "/tmp/taosd");
  }
  strcpy(env.dataDir, "/usr/local/var/lib/taos");
  strcpy(env.logDir, "/usr/local/var/log/taos");
  strcpy(env.tempDir, "/usr/local/etc/taos");
  strcpy(env.osName, "Darwin");

#else
  if (configDir[0] == 0) {
    strcpy(configDir, "/etc/taos");
  }
  strcpy(env.dataDir, "/var/lib/taos");
  strcpy(env.logDir, "/var/log/taos");
  strcpy(env.tempDir, "/tmp");
  strcpy(env.osName, "Linux");

#endif
}