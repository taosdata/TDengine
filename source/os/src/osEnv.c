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

char            configDir[PATH_MAX] = {0};
char            tsDataDir[PATH_MAX] = {0};
char            tsLogDir[PATH_MAX] = {0};
char            tsTempDir[PATH_MAX] = {0};
SDiskSpace      tsDataSpace = {0};
SDiskSpace      tsLogSpace = {0};
SDiskSpace      tsTempSpace = {0};
char            tsOsName[16] = {0};
char            tsTimezoneStr[TD_TIMEZONE_LEN] = {0};
enum TdTimezone tsTimezone = TdZeroZone;
char            tsLocale[TD_LOCALE_LEN] = {0};
char            tsCharset[TD_CHARSET_LEN] = {0};
int8_t          tsDaylight = 0;
bool            tsEnableCoreFile = 1;
int64_t         tsPageSizeKB = 0;
int64_t         tsOpenMax = 0;
int64_t         tsStreamMax = 0;
float           tsNumOfCores = 0;
int64_t         tsTotalMemoryKB = 0;
char           *tsProcPath = NULL;

char tsSIMDEnable = 0;
char tsAVX512Enable = 0;
char tsSSE42Supported = 0;
char tsAVXSupported = 0;
char tsAVX2Supported = 0;
char tsFMASupported = 0;
char tsAVX512Supported = 0;

int32_t osDefaultInit() {
  int32_t code = TSDB_CODE_SUCCESS;

  taosSeedRand(taosSafeRand());
  taosGetSystemLocale(tsLocale, tsCharset);
  taosGetSystemTimezone(tsTimezoneStr, &tsTimezone);
  if (strlen(tsTimezoneStr) > 0) { // ignore empty timezone
    if ((code = taosSetSystemTimezone(tsTimezoneStr, tsTimezoneStr, &tsDaylight, &tsTimezone)) != TSDB_CODE_SUCCESS)
      return code;
  }

  taosGetSystemInfo();

  // deadlock in query
  if (tsNumOfCores < 2) {
    tsNumOfCores = 2;
  }

#ifdef WINDOWS
  taosWinSocketInit();

  const char *tmpDir = getenv("tmp");
  if (tmpDir == NULL) {
    tmpDir = getenv("temp");
  }
  if (tmpDir != NULL) {
    (void)strcpy(tsTempDir, tmpDir);
  }
  (void)strcpy(tsOsName, "Windows");
#elif defined(_TD_DARWIN_64)
  (void)strcpy(tsOsName, "Darwin");
#else
  (void)strcpy(tsOsName, "Linux");
#endif
  if (configDir[0] == 0) {
    (void)strcpy(configDir, TD_CFG_DIR_PATH);
  }
  (void)strcpy(tsDataDir, TD_DATA_DIR_PATH);
  (void)strcpy(tsLogDir, TD_LOG_DIR_PATH);
  if(strlen(tsTempDir) == 0){
    (void)strcpy(tsTempDir, TD_TMP_DIR_PATH);
  }

  return code;
}

void osUpdate() {
  if (tsLogDir[0] != 0) {
    (void)taosGetDiskSize(tsLogDir, &tsLogSpace.size);
  }
  if (tsDataDir[0] != 0) {
    (void)taosGetDiskSize(tsDataDir, &tsDataSpace.size);
  }
  if (tsTempDir[0] != 0) {
    (void)taosGetDiskSize(tsTempDir, &tsTempSpace.size);
  }
}

void osCleanup() {}

bool osLogSpaceAvailable() { return tsLogSpace.size.avail > 0; }

bool osTempSpaceAvailable() { return tsTempSpace.size.avail > 0; }

bool osDataSpaceAvailable() { return tsDataSpace.size.avail > 0; }

bool osLogSpaceSufficient() { return tsLogSpace.size.avail > tsLogSpace.reserved; }

bool osDataSpaceSufficient() { return tsDataSpace.size.avail > tsDataSpace.reserved; }

bool osTempSpaceSufficient() { return tsTempSpace.size.avail > tsTempSpace.reserved; }

int32_t osSetTimezone(const char *tz) { return taosSetSystemTimezone(tz, tsTimezoneStr, &tsDaylight, &tsTimezone); }

void osSetSystemLocale(const char *inLocale, const char *inCharSet) {
  (void)memcpy(tsLocale, inLocale, strlen(inLocale) + 1);
  (void)memcpy(tsCharset, inCharSet, strlen(inCharSet) + 1);
}

void osSetProcPath(int32_t argc, char **argv) { tsProcPath = argv[0]; }
