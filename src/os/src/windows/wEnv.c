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
#include "tulog.h"

extern void taosWinSocketInit();

void osInit() {
  taosSetCoreDump();
#ifdef _TD_POWER_
  if (configDir[0] == 0) {
    strcpy(configDir, "C:/PowerDB/cfg");
  }

  strcpy(tsVnodeDir, "C:/PowerDB/data");
  strcpy(tsDataDir, "C:/PowerDB/data");
  strcpy(tsLogDir, "C:/PowerDB/log");
  strcpy(tsScriptDir, "C:/PowerDB/script");
#elif (_TD_TQ_ == true)
  if (configDir[0] == 0) {
    strcpy(configDir, "C:/TQueue/cfg");
  }
  strcpy(tsVnodeDir, "C:/TQueue/data");
  strcpy(tsDataDir, "C:/TQueue/data");
  strcpy(tsLogDir, "C:/TQueue/log");
  strcpy(tsScriptDir, "C:/TQueue/script");
#elif (_TD_PRO_ == true)
  if (configDir[0] == 0) {
    strcpy(configDir, "C:/ProDB/cfg");
  }
  strcpy(tsVnodeDir, "C:/ProDB/data");
  strcpy(tsDataDir, "C:/ProDB/data");
  strcpy(tsLogDir, "C:/ProDB/log");
  strcpy(tsScriptDir, "C:/ProDB/script");
#elif (_TD_KH_ == true)
  if (configDir[0] == 0) {
    strcpy(configDir, "C:/KingHistorian/cfg");
  }
  strcpy(tsVnodeDir, "C:/KingHistorian/data");
  strcpy(tsDataDir, "C:/KingHistorian/data");
  strcpy(tsLogDir, "C:/KingHistorian/log");
  strcpy(tsScriptDir, "C:/KingHistorian/script");
#elif (_TD_JH_ == true)
  if (configDir[0] == 0) {
    strcpy(configDir, "C:/jh_iot/cfg");
  }
  strcpy(tsVnodeDir, "C:/jh_iot/data");
  strcpy(tsDataDir, "C:/jh_iot/data");
  strcpy(tsLogDir, "C:/jh_iot/log");
  strcpy(tsScriptDir, "C:/jh_iot/script");
#else
  if (configDir[0] == 0) {
    strcpy(configDir, "C:/TDengine/cfg");
  }

  strcpy(tsVnodeDir, "C:/TDengine/data");
  strcpy(tsDataDir, "C:/TDengine/data");
  strcpy(tsLogDir, "C:/TDengine/log");
  strcpy(tsScriptDir, "C:/TDengine/script");
#endif

  strcpy(tsDnodeDir, "");
  strcpy(tsMnodeDir, "");  
  strcpy(tsOsName, "Windows");

  const char *tmpDir = getenv("tmp");
  if (tmpDir == NULL) {
    tmpDir = getenv("temp");
  }

  if (tmpDir != NULL) {
    strcpy(tsTempDir, tmpDir);
  } else {
    strcpy(tsTempDir, "C:\\Windows\\Temp");
  }
  
  taosWinSocketInit();
}
