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

#include <ifaddrs.h>
#include <locale.h>
#include <netdb.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdbool.h>
#include <sys/statvfs.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/utsname.h>
#include <unistd.h>

#include "tglobalcfg.h"
#include "tlog.h"
#include "tsystem.h"
#include "tutil.h"

#define PROCESS_ITEM 12

typedef struct {
  uint64_t user;
  uint64_t nice;
  uint64_t system;
  uint64_t idle;
} SysCpuInfo;

typedef struct {
  uint64_t utime;   // user time
  uint64_t stime;   // kernel time
  uint64_t cutime;  // all user time
  uint64_t cstime;  // all dead time
} ProcCpuInfo;

static pid_t tsProcId;
static char  tsSysNetFile[] = "/proc/net/dev";
static char  tsSysCpuFile[] = "/proc/stat";
static char  tsProcCpuFile[25] = {0};
static char  tsProcMemFile[25] = {0};
static char  tsProcIOFile[25] = {0};
static float tsPageSizeKB = 0;

bool taosGetSysMemory(float *memoryUsedMB) {
  float memoryAvailMB = (float)sysconf(_SC_AVPHYS_PAGES) * tsPageSizeKB / 1024;
  *memoryUsedMB = (float)tsTotalMemoryMB - memoryAvailMB;
  return true;
}

bool taosGetProcMemory(float *memoryUsedMB) {
  FILE *fp = fopen(tsProcMemFile, "r");
  if (fp == NULL) {
    pError("open file:%s failed", tsProcMemFile);
    return false;
  }

  size_t len;
  char * line = NULL;
  while (!feof(fp)) {
    tfree(line);
    getline(&line, &len, fp);
    if (line == NULL) {
      break;
    }
    if (strstr(line, "VmRSS:") != NULL) {
      break;
    }
  }

  if (line == NULL) {
    pError("read file:%s failed", tsProcMemFile);
    fclose(fp);
    return false;
  }

  int64_t memKB = 0;
  char    tmp[10];
  sscanf(line, "%s %ld", tmp, &memKB);
  *memoryUsedMB = (float)((double)memKB / 1024);

  tfree(line);
  fclose(fp);
  return true;
}

bool taosGetSysCpuInfo(SysCpuInfo *cpuInfo) {
  FILE *fp = fopen(tsSysCpuFile, "r");
  if (fp == NULL) {
    pError("open file:%s failed", tsSysCpuFile);
    return false;
  }

  size_t len;
  char * line = NULL;
  getline(&line, &len, fp);
  if (line == NULL) {
    pError("read file:%s failed", tsSysCpuFile);
    fclose(fp);
    return false;
  }

  char cpu[10] = {0};
  sscanf(line, "%s %ld %ld %ld %ld", cpu, &cpuInfo->user, &cpuInfo->nice, &cpuInfo->system, &cpuInfo->idle);

  tfree(line);
  fclose(fp);
  return true;
}

bool taosGetProcCpuInfo(ProcCpuInfo *cpuInfo) {
  FILE *fp = fopen(tsProcCpuFile, "r");
  if (fp == NULL) {
    pError("open file:%s failed", tsProcCpuFile);
    return false;
  }

  size_t len;
  char * line = NULL;
  getline(&line, &len, fp);
  if (line == NULL) {
    pError("read file:%s failed", tsProcCpuFile);
    fclose(fp);
    return false;
  }

  for (int i = 0, blank = 0; line[i] != 0; ++i) {
    if (line[i] == ' ') blank++;
    if (blank == PROCESS_ITEM) {
      sscanf(line + i + 1, "%ld %ld %ld %ld", &cpuInfo->utime, &cpuInfo->stime, &cpuInfo->cutime, &cpuInfo->cstime);
      break;
    }
  }

  tfree(line);
  fclose(fp);
  return true;
}

void taosGetSystemTimezone() {
  SGlobalConfig *cfg_timezone = tsGetConfigOption("timezone");
  if (cfg_timezone == NULL) return;
  if (cfg_timezone->cfgStatus >= TSDB_CFG_CSTATUS_DEFAULT) {
    return;
  }

  /*
   * NOTE: do not remove it.
   * Enforce set the correct daylight saving time(DST) flag according
   * to current time
   */
  time_t    tx1 = time(NULL);
  struct tm tm1;
  localtime_r(&tx1, &tm1);

  /* load time zone string from /etc/timezone */
  FILE *f = fopen("/etc/timezone", "r");
  char  buf[64] = {0};
  if (f != NULL) {
    fread(buf, 64, 1, f);
    fclose(f);
  }

  char *lineEnd = strstr(buf, "\n");
  if (lineEnd != NULL) {
    *lineEnd = 0;
  }

  // for CentOS system, /etc/timezone does not exist. Ignore the TZ environment
  // variables
  if (strlen(buf) > 0) {
    setenv("TZ", buf, 1);
  }

  // get and set default timezone
  tzset();

  /*
   * get CURRENT time zone.
   * system current time zone is affected by daylight saving time(DST)
   *
   * e.g., the local time zone of London in DST is GMT+01:00,
   * otherwise is GMT+00:00
   */
  int32_t tz = (-timezone * MILLISECOND_PER_SECOND) / MILLISECOND_PER_HOUR;
  tz += daylight;

  /*
   * format example:
   *
   * Asia/Shanghai   (CST, +0800)
   * Europe/London   (BST, +0100)
   */
  sprintf(tsTimezone, "%s (%s, %s%02d00)", buf, tzname[daylight], tz >= 0 ? "+" : "-", abs(tz));

  cfg_timezone->cfgStatus = TSDB_CFG_CSTATUS_DEFAULT;
  pPrint("timezone not configured, set to system default:%s", tsTimezone);
}

typedef struct CharsetPair {
  char *oldCharset;
  char *newCharset;
} CharsetPair;

char *taosCharsetReplace(char *charsetstr) {
  CharsetPair charsetRep[] = {
      {"utf8", "UTF-8"}, {"936", "CP936"},
  };

  for (int32_t i = 0; i < tListLen(charsetRep); ++i) {
    if (strcasecmp(charsetRep[i].oldCharset, charsetstr) == 0) {
      return strdup(charsetRep[i].newCharset);
    }
  }

  return strdup(charsetstr);
}

void taosGetSystemLocale() {  // get and set default locale
                              /*
                               * POSIX format locale string:
                               * (Language Strings)_(Country/Region Strings).(code_page)
                               *
                               * example: en_US.UTF-8, zh_CN.GB18030, zh_CN.UTF-8,
                               *
                               * if user does not specify the locale in taos.cfg
                               * the program use default LC_CTYPE as system locale.
                               *
                               * In case of some CentOS systems, their default locale is "en_US.utf8", which
                               * is not
                               * valid code_page for libiconv that is employed to convert string in this
                               * system.
                               * User needs to specify the locale explicitly
                               * in config file in the correct format: en_US.UTF-8
                               *
                               * In case of windows client, the locale string is not legal POSIX format,
                               * user needs to
                               * set the correct code_page for libiconv. Usually, the code_page of windows
                               * system
                               * with simple chinese is CP936, CP437 for English locale.
                               *
                               */
  char  sep = '.';
  char *locale = NULL;

  SGlobalConfig *cfg_locale = tsGetConfigOption("locale");
  if (cfg_locale && cfg_locale->cfgStatus < TSDB_CFG_CSTATUS_DEFAULT) {
    locale = setlocale(LC_CTYPE, "");
    if (locale == NULL) {
      pError("can't get locale from system");
    } else {
      strncpy(tsLocale, locale, sizeof(tsLocale) / sizeof(tsLocale[0]));
      pPrint("locale not configured, set to system default:%s", tsLocale);
    }
  }

  /* if user does not specify the charset, extract it from locale */
  SGlobalConfig *cfg_charset = tsGetConfigOption("charset");
  if (cfg_charset && cfg_charset->cfgStatus < TSDB_CFG_CSTATUS_DEFAULT) {
    char *str = strrchr(tsLocale, sep);
    if (str != NULL) {
      str++;

      char *revisedCharset = taosCharsetReplace(str);
      strncpy(tsCharset, revisedCharset, sizeof(tsCharset) / sizeof(tsCharset[0]));

      free(revisedCharset);
      pPrint("charset not configured, set to system default:%s", tsCharset);
    }
  }
}

bool taosGetCpuUsage(float *sysCpuUsage, float *procCpuUsage) {
  static uint64_t lastSysUsed = 0;
  static uint64_t lastSysTotal = 0;
  static uint64_t lastProcTotal = 0;

  SysCpuInfo  sysCpu;
  ProcCpuInfo procCpu;
  if (!taosGetSysCpuInfo(&sysCpu)) {
    return false;
  }
  if (!taosGetProcCpuInfo(&procCpu)) {
    return false;
  }

  uint64_t curSysUsed = sysCpu.user + sysCpu.nice + sysCpu.system;
  uint64_t curSysTotal = curSysUsed + sysCpu.idle;
  uint64_t curProcTotal = procCpu.utime + procCpu.stime + procCpu.cutime + procCpu.cstime;

  if (lastSysUsed == 0 || lastSysTotal == 0 || lastProcTotal == 0) {
    lastSysUsed = curSysUsed > 1 ? curSysUsed : 1;
    lastSysTotal = curSysTotal > 1 ? curSysTotal : 1;
    lastProcTotal = curProcTotal > 1 ? curProcTotal : 1;
    return false;
  }

  if (curSysTotal == lastSysTotal) {
    return false;
  }

  *sysCpuUsage = (float)((double)(curSysUsed - lastSysUsed) / (double)(curSysTotal - lastSysTotal) * 100);
  *procCpuUsage = (float)((double)(curProcTotal - lastProcTotal) / (double)(curSysTotal - lastSysTotal) * 100);

  lastSysUsed = curSysUsed;
  lastSysTotal = curSysTotal;
  lastProcTotal = curProcTotal;

  return true;
}

bool taosGetDisk(float *diskUsedGB) {
  struct statvfs info;
  const double   unit = 1024 * 1024 * 1024;

  if (statvfs(tsDirectory, &info)) {
    *diskUsedGB = 0;
    tsTotalDiskGB = 0;
    return false;
  }

  float diskAvail = (float)((double)info.f_bavail * (double)info.f_frsize / unit);
  tsTotalDiskGB = (int32_t)((double)info.f_blocks * (double)info.f_frsize / unit);
  *diskUsedGB = (float)tsTotalDiskGB - diskAvail;

  return true;
}

bool taosGetCardName(char *ip, char *name) {
  struct ifaddrs *ifaddr, *ifa;
  int             family, s;
  char            host[NI_MAXHOST];
  bool            ret = false;

  if (getifaddrs(&ifaddr) == -1) {
    return false;
  }

  /* Walk through linked list, maintaining head pointer so we can free list
   * later */
  for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
    if (ifa->ifa_addr == NULL) continue;

    family = ifa->ifa_addr->sa_family;
    if (family != AF_INET) {
      continue;
    }

    s = getnameinfo(ifa->ifa_addr, (family == AF_INET) ? sizeof(struct sockaddr_in) : sizeof(struct sockaddr_in6), host,
                    NI_MAXHOST, NULL, 0, NI_NUMERICHOST);
    if (s != 0) {
      break;
    }

    if (strcmp(host, ip) == 0) {
      strcpy(name, ifa->ifa_name);
      ret = true;
    }
  }

  freeifaddrs(ifaddr);
  return ret;
}

bool taosGetCardInfo(int64_t *bytes) {
  static char tsPublicCard[1000] = {0};
  if (tsPublicCard[0] == 0) {
    if (!taosGetCardName(tsInternalIp, tsPublicCard)) {
      pError("can't get card name from ip:%s", tsInternalIp);
      return false;
    }
    int cardNameLen = (int)strlen(tsPublicCard);
    for (int i = 0; i < cardNameLen; ++i) {
      if (tsPublicCard[i] == ':') {
        tsPublicCard[i] = 0;
        break;
      }
    }
    // pTrace("card name of public ip:%s is %s", tsPublicIp, tsPublicCard);
  }

  FILE *fp = fopen(tsSysNetFile, "r");
  if (fp == NULL) {
    pError("open file:%s failed", tsSysNetFile);
    return false;
  }

  int64_t rbytes, rpackts, tbytes, tpackets;
  int64_t nouse1, nouse2, nouse3, nouse4, nouse5, nouse6;
  char    nouse0[200] = {0};

  size_t len;
  char * line = NULL;

  while (!feof(fp)) {
    tfree(line);
    getline(&line, &len, fp);
    if (line == NULL) {
      break;
    }
    if (strstr(line, tsPublicCard) != NULL) {
      break;
    }
  }
  if (line != NULL) {
    sscanf(line, "%s %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld", nouse0, &rbytes, &rpackts, &nouse1, &nouse2, &nouse3,
           &nouse4, &nouse5, &nouse6, &tbytes, &tpackets);
    *bytes = rbytes + tbytes;
    tfree(line);
    fclose(fp);
    return true;
  } else {
    pWarn("can't get card:%s info from device:%s", tsPublicCard, tsSysNetFile);
    *bytes = 0;
    fclose(fp);
    return false;
  }
}

bool taosGetBandSpeed(float *bandSpeedKb) {
  static int64_t lastBytes = 0;
  static time_t  lastTime = 0;
  int64_t        curBytes = 0;
  time_t         curTime = time(NULL);

  if (!taosGetCardInfo(&curBytes)) {
    return false;
  }

  if (lastTime == 0 || lastBytes == 0) {
    lastTime = curTime;
    lastBytes = curBytes;
    return false;
  }

  if (lastTime >= curTime || lastBytes > curBytes) {
    lastTime = curTime;
    lastBytes = curBytes;
    return false;
  }

  double totalBytes = (double)(curBytes - lastBytes) / 1024 * 8;  // Kb
  *bandSpeedKb = (float)(totalBytes / (double)(curTime - lastTime));

  // pPrint("bandwidth lastBytes:%ld, lastTime:%ld, curBytes:%ld, curTime:%ld,
  // speed:%f", lastBytes, lastTime, curBytes, curTime, *bandSpeed);

  lastTime = curTime;
  lastBytes = curBytes;

  return true;
}

bool taosReadProcIO(int64_t *readbyte, int64_t *writebyte) {
  FILE *fp = fopen(tsProcIOFile, "r");
  if (fp == NULL) {
    pError("open file:%s failed", tsProcIOFile);
    return false;
  }

  size_t len;
  char * line = NULL;
  char   tmp[10];
  int    readIndex = 0;

  while (!feof(fp)) {
    tfree(line);
    getline(&line, &len, fp);
    if (line == NULL) {
      break;
    }
    if (strstr(line, "rchar:") != NULL) {
      sscanf(line, "%s %ld", tmp, readbyte);
      readIndex++;
    } else if (strstr(line, "wchar:") != NULL) {
      sscanf(line, "%s %ld", tmp, writebyte);
      readIndex++;
    } else {
    }

    if (readIndex >= 2) break;
  }

  tfree(line);
  fclose(fp);

  if (readIndex < 2) {
    pError("read file:%s failed", tsProcIOFile);
    return false;
  }

  return true;
}

bool taosGetProcIO(float *readKB, float *writeKB) {
  static int64_t lastReadbyte = -1;
  static int64_t lastWritebyte = -1;

  int64_t curReadbyte, curWritebyte;

  if (!taosReadProcIO(&curReadbyte, &curWritebyte)) {
    return false;
  }

  if (lastReadbyte == -1 || lastWritebyte == -1) {
    lastReadbyte = curReadbyte;
    lastWritebyte = curWritebyte;
    return false;
  }

  *readKB = (float)((double)(curReadbyte - lastReadbyte) / 1024);
  *writeKB = (float)((double)(curWritebyte - lastWritebyte) / 1024);
  if (*readKB < 0) *readKB = 0;
  if (*writeKB < 0) *writeKB = 0;

  lastReadbyte = curReadbyte;
  lastWritebyte = curWritebyte;

  return true;
}

void taosGetSystemInfo() {
  tsNumOfCores = (int32_t)sysconf(_SC_NPROCESSORS_ONLN);
  tsPageSize = sysconf(_SC_PAGESIZE);
  tsOpenMax = sysconf(_SC_OPEN_MAX);
  tsStreamMax = sysconf(_SC_STREAM_MAX);

  tsProcId = (pid_t)syscall(SYS_gettid);
  tsPageSizeKB = (float)(sysconf(_SC_PAGESIZE)) / 1024;
  tsTotalMemoryMB = (int32_t)((float)sysconf(_SC_PHYS_PAGES) * tsPageSizeKB / 1024);

  snprintf(tsProcMemFile, 25, "/proc/%d/status", tsProcId);
  snprintf(tsProcCpuFile, 25, "/proc/%d/stat", tsProcId);
  snprintf(tsProcIOFile, 25, "/proc/%d/io", tsProcId);

  float tmp1, tmp2;
  taosGetSysMemory(&tmp1);
  taosGetProcMemory(&tmp2);
  taosGetDisk(&tmp1);
  taosGetBandSpeed(&tmp1);
  taosGetCpuUsage(&tmp1, &tmp2);
  taosGetProcIO(&tmp1, &tmp2);

  taosGetSystemTimezone();
  taosGetSystemLocale();
}

void tsPrintOsInfo() {
  pPrint(" os pageSize:            %ld(KB)", tsPageSize);
  pPrint(" os openMax:             %ld", tsOpenMax);
  pPrint(" os streamMax:           %ld", tsStreamMax);
  pPrint(" os numOfCores:          %d", tsNumOfCores);
  pPrint(" os totalDisk:           %d(GB)", tsTotalDiskGB);
  pPrint(" os totalMemory:         %d(MB)", tsTotalMemoryMB);

  struct utsname buf;
  if (uname(&buf)) {
    pPrint(" can't fetch os info");
    return;
  }
  pPrint(" os sysname:             %s", buf.sysname);
  pPrint(" os nodename:            %s", buf.nodename);
  pPrint(" os release:             %s", buf.release);
  pPrint(" os version:             %s", buf.version);
  pPrint(" os machine:             %s", buf.machine);
}

void taosKillSystem() {
  // SIGINT
  pPrint("taosd will shut down soon");
  kill(tsProcId, 2);
}