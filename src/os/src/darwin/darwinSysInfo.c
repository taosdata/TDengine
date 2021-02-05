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
  SGlobalCfg *cfg_timezone = taosGetConfigOption("timezone");
  if (cfg_timezone == NULL) return;
  if (cfg_timezone->cfgStatus >= TAOS_CFG_CSTATUS_DEFAULT) {
    return;
  }

  /* load time zone string from /etc/localtime */
  char buf[4096];
  char *tz = NULL; {
    int n = readlink("/etc/localtime", buf, sizeof(buf));
    if (n<0) {
      uError("read /etc/localtime error, reason:%s", strerror(errno));
      return;
    }
    buf[n] = '\0';
    for(int i=n-1; i>=0; --i) {
      if (buf[i]=='/') {
        if (tz) {
          tz = buf + i + 1;
          break;
        }
        tz = buf + i + 1;
      }
    }
    if (!tz || 0==strchr(tz, '/')) {
      uError("parsing /etc/localtime failed");
      return;
    }

    setenv("TZ", tz, 1);
    tzset();
  }

  /*
   * NOTE: do not remove it.
   * Enforce set the correct daylight saving time(DST) flag according
   * to current time
   */
  time_t    tx1 = time(NULL);
  struct tm tm1;
  localtime_r(&tx1, &tm1);

  /*
   * format example:
   *
   * Asia/Shanghai   (CST, +0800)
   * Europe/London   (BST, +0100)
   */
  snprintf(tsTimezone, TSDB_TIMEZONE_LEN, "%s (%s, %+03ld00)",
           tz, tm1.tm_isdst ? tzname[daylight] : tzname[0], -timezone/3600);

  // cfg_timezone->cfgStatus = TAOS_CFG_CSTATUS_DEFAULT;
  uWarn("timezone not configured, set to system default:%s", tsTimezone);
}

/*
 * originally from src/os/src/detail/osSysinfo.c
 * POSIX format locale string:
 * (Language Strings)_(Country/Region Strings).(code_page)
 *
 * example: en_US.UTF-8, zh_CN.GB18030, zh_CN.UTF-8,
 *
 * if user does not specify the locale in taos.cfg the program use default LC_CTYPE as system locale.
 *
 * In case of some CentOS systems, their default locale is "en_US.utf8", which is not valid code_page
 * for libiconv that is employed to convert string in this system. This program will automatically use
 * UTF-8 instead as the charset.
 *
 * In case of windows client, the locale string is not valid POSIX format, user needs to set the
 * correct code_page for libiconv. Usually, the code_page of windows system with simple chinese is
 * CP936, CP437 for English charset.
 *
 */
static void taosGetSystemLocale() {  // get and set default locale
  char  sep = '.';
  char *locale = NULL;

  SGlobalCfg *cfg_locale = taosGetConfigOption("locale");
  if (cfg_locale && cfg_locale->cfgStatus < TAOS_CFG_CSTATUS_DEFAULT) {
    locale = setlocale(LC_CTYPE, "");
    if (locale == NULL) {
      uError("can't get locale from system, set it to en_US.UTF-8 since error:%d:%s", errno, strerror(errno));
      strcpy(tsLocale, "en_US.UTF-8");
    } else {
      tstrncpy(tsLocale, locale, TSDB_LOCALE_LEN);
      uWarn("locale not configured, set to system default:%s", tsLocale);
    }
  }

  /* if user does not specify the charset, extract it from locale */
  SGlobalCfg *cfg_charset = taosGetConfigOption("charset");
  if (cfg_charset && cfg_charset->cfgStatus < TAOS_CFG_CSTATUS_DEFAULT) {
    char *str = strrchr(tsLocale, sep);
    if (str != NULL) {
      str++;

      char *revisedCharset = taosCharsetReplace(str);
      tstrncpy(tsCharset, revisedCharset, TSDB_LOCALE_LEN);

      free(revisedCharset);
      uWarn("charset not configured, set to system default:%s", tsCharset);
    } else {
      strcpy(tsCharset, "UTF-8");
      uWarn("can't get locale and charset from system, set it to UTF-8");
    }
  }
}

void taosPrintOsInfo() {
  uInfo(" os pageSize:            %" PRId64 "(KB)", tsPageSize / 1024);
  // uInfo(" os openMax:             %" PRId64, tsOpenMax);
  // uInfo(" os streamMax:           %" PRId64, tsStreamMax);
  uInfo(" os numOfCores:          %d", tsNumOfCores);
  uInfo(" os totalDisk:           %f(GB)", tsTotalDataDirGB);
  uInfo(" os totalMemory:         %d(MB)", tsTotalMemoryMB);

  struct utsname buf;
  if (uname(&buf)) {
    uInfo(" can't fetch os info");
    return;
  }
  uInfo(" os sysname:             %s", buf.sysname);
  uInfo(" os nodename:            %s", buf.nodename);
  uInfo(" os release:             %s", buf.release);
  uInfo(" os version:             %s", buf.version);
  uInfo(" os machine:             %s", buf.machine);
  uInfo("==================================");
}

void taosKillSystem() {
  uError("function taosKillSystem, exit!");
  exit(0);
}

void taosGetSystemInfo() {
  // taosGetProcInfos();

  tsNumOfCores        = sysconf(_SC_NPROCESSORS_ONLN);
  long physical_pages = sysconf(_SC_PHYS_PAGES);
  long page_size      = sysconf(_SC_PAGESIZE);
  tsTotalMemoryMB     = physical_pages * page_size / (1024 * 1024);
  tsPageSize          = page_size;

  // float tmp1, tmp2;
  // taosGetSysMemory(&tmp1);
  // taosGetProcMemory(&tmp2);
  // taosGetDisk();
  // taosGetBandSpeed(&tmp1);
  // taosGetCpuUsage(&tmp1, &tmp2);
  // taosGetProcIO(&tmp1, &tmp2);

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
  uuid_unparse_lower(uuid, uid);
  return true;
}

