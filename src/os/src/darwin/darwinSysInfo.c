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
#include "taoserror.h"
#include <errno.h>
#include <libproc.h>


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

int32_t taosGetDiskSize(char *dataDir, SysDiskSize *diskSize) {
  struct statvfs info;
  if (statvfs(tsDataDir, &info)) {
    uError("failed to get disk size, dataDir:%s errno:%s", tsDataDir, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  } else {
    diskSize->tsize = info.f_blocks * info.f_frsize;
    diskSize->avail = info.f_bavail * info.f_frsize;
    return 0;
  }
}

char cmdline[1024];

char *taosGetCmdlineByPID(int pid) {

  errno = 0;

  if (proc_pidpath(pid, cmdline, sizeof(cmdline)) <= 0) {
    fprintf(stderr, "PID is %d, %s", pid, strerror(errno));
    return strerror(errno);
  }

  return cmdline;
}

bool taosGetSystemUid(char *uid) {
  uuid_t uuid = {0};
  uuid_generate(uuid);
  // it's caller's responsibility to make enough space for `uid`, that's 36-char + 1-null
  uuid_unparse(uuid, uid);
  return true;
}
