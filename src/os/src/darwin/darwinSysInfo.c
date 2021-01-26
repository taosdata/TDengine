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
#include "tconfig.h"
#include "tglobal.h"
#include "tulog.h"

static void taosGetSystemTimezone() {
  // get and set default timezone
  SGlobalCfg *cfg_timezone = taosGetConfigOption("timezone");
  if (cfg_timezone && cfg_timezone->cfgStatus < TAOS_CFG_CSTATUS_DEFAULT) {
    char *tz = getenv("TZ");
    if (tz == NULL || strlen(tz) == 0) {
      strcpy(tsTimezone, "not configured");
    }
    else {
      strcpy(tsTimezone, tz);
    }
    cfg_timezone->cfgStatus = TAOS_CFG_CSTATUS_DEFAULT;
    uInfo("timezone not configured, use default");
  }
}

static void taosGetSystemLocale() {
  // get and set default locale
  SGlobalCfg *cfg_locale = taosGetConfigOption("locale");
  if (cfg_locale && cfg_locale->cfgStatus < TAOS_CFG_CSTATUS_DEFAULT) {
    char *locale = setlocale(LC_CTYPE, "chs");
    if (locale != NULL) {
      tstrncpy(tsLocale, locale, TSDB_LOCALE_LEN);
      cfg_locale->cfgStatus = TAOS_CFG_CSTATUS_DEFAULT;
      uInfo("locale not configured, set to default:%s", tsLocale);
    }
  }

  SGlobalCfg *cfg_charset = taosGetConfigOption("charset");
  if (cfg_charset && cfg_charset->cfgStatus < TAOS_CFG_CSTATUS_DEFAULT) {
    strcpy(tsCharset, "cp936");
    cfg_charset->cfgStatus = TAOS_CFG_CSTATUS_DEFAULT;
    uInfo("charset not configured, set to default:%s", tsCharset);
  }
}

void taosPrintOsInfo() {}

void taosKillSystem() {
  uError("function taosKillSystem, exit!");
  exit(0);
}

void taosGetSystemInfo() {
  taosGetSystemTimezone();
  taosGetSystemLocale();
}

bool taosGetDisk() { return true; }

bool taosGetProcIO(float *readKB, float *writeKB) {
  *readKB = 0;
  *writeKB = 0;
  return true;
}

bool taosGetBandSpeed(float *bandSpeedKb) {
  *bandSpeedKb = 0;
  return true;
}

bool taosGetCpuUsage(float *sysCpuUsage, float *procCpuUsage) {
  *sysCpuUsage = 0;
  *procCpuUsage = 0;
  return true;
}

bool taosGetProcMemory(float *memoryUsedMB) {
  *memoryUsedMB = 0;
  return true;
}

bool taosGetSysMemory(float *memoryUsedMB) {
  *memoryUsedMB = 0;
  return true;
}

int taosSystem(const char *cmd) {
  uError("un support funtion");
  return -1;
}

void taosSetCoreDump() {}

char *taosGetCmdlineByPID(int pid) {
  return "[not supported yet]";
}

bool taosGetSystemUid(char *uid) {
  uuid_t uuid = {0};
  uuid_generate(uuid);
  // it's caller's responsibility to make enough space for `uid`, that's 36-char + 1-null
  uuid_unparse(uuid, uid);
  return true;
}

