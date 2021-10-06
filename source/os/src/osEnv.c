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

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)

char tsOsName[10] = "Windows";
char configDir[PATH_MAX] = "C:/TDengine/cfg";
char tsDataDir[PATH_MAX] = "C:/TDengine/data";
char tsLogDir[PATH_MAX] = "C:/TDengine/log";
char tsScriptDir[PATH_MAX] = "C:/TDengine/script";
char tsTempDir[PATH_MAX] = "C:\\Windows\\Temp";

extern taosWinSocketInit();

void osInit() {
  taosWinSocketInit();

  const char *tmpDir = getenv("tmp");
  if (tmpDir == NULL) {
    tmpDir = getenv("temp");
  }

  if (tmpDir != NULL) {
    strcpy(tsTempDir, tmpDir);
  }
}

#elif defined(_TD_DARWIN_64)

char tsOsName[10] = "Darwin";
char configDir[PATH_MAX] = "/usr/local/etc/taos";
char tsDataDir[PATH_MAX] = "/usr/local/var/lib/taos";
char tsLogDir[PATH_MAX] = "/usr/local/var/log/taos";
char tsScriptDir[PATH_MAX] = "/usr/local/etc/taos";
char tsTempDir[PATH_MAX] = "/tmp/taosd";

void osInit() {}

#else

char tsOsName[10] = "Linux";
char configDir[PATH_MAX] = "/etc/taos";
char tsDataDir[PATH_MAX] = "/var/lib/taos";
char tsLogDir[PATH_MAX] = "/var/log/taos";
char tsScriptDir[PATH_MAX] = "/etc/taos";
char tsTempDir[PATH_MAX] = "/tmp/";

void osInit() {}

#endif
