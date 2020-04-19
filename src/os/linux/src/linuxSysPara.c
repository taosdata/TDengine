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
#include "tutil.h"
#include "tsystem.h"

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
    uError("open file:%s failed", tsProcMemFile);
    return false;
  }

  size_t len;
  char * line = NULL;
  while (!feof(fp)) {
    tfree(line);
    len = 0;
    getline(&line, &len, fp);
    if (line == NULL) {
      break;
    }
    if (strstr(line, "VmRSS:") != NULL) {
      break;
    }
  }

  if (line == NULL) {
    uError("read file:%s failed", tsProcMemFile);
    fclose(fp);
    return false;
  }

  int64_t memKB = 0;
  char    tmp[10];
  sscanf(line, "%s %" PRId64, tmp, &memKB);
  *memoryUsedMB = (float)((double)memKB / 1024);

  tfree(line);
  fclose(fp);
  return true;
}

static bool taosGetSysCpuInfo(SysCpuInfo *cpuInfo) {
  FILE *fp = fopen(tsSysCpuFile, "r");
  if (fp == NULL) {
    uError("open file:%s failed", tsSysCpuFile);
    return false;
  }

  size_t len;
  char * line = NULL;
  getline(&line, &len, fp);
  if (line == NULL) {
    uError("read file:%s failed", tsSysCpuFile);
    fclose(fp);
    return false;
  }

  char cpu[10] = {0};
  sscanf(line, "%s %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64, cpu, &cpuInfo->user, &cpuInfo->nice, &cpuInfo->system, &cpuInfo->idle);

  tfree(line);
  fclose(fp);
  return true;
}

static bool taosGetProcCpuInfo(ProcCpuInfo *cpuInfo) {
  FILE *fp = fopen(tsProcCpuFile, "r");
  if (fp == NULL) {
    uError("open file:%s failed", tsProcCpuFile);
    return false;
  }

  size_t len = 0;
  char * line = NULL;
  getline(&line, &len, fp);
  if (line == NULL) {
    uError("read file:%s failed", tsProcCpuFile);
    fclose(fp);
    return false;
  }

  for (int i = 0, blank = 0; line[i] != 0; ++i) {
    if (line[i] == ' ') blank++;
    if (blank == PROCESS_ITEM) {
      sscanf(line + i + 1, "%" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64, &cpuInfo->utime, &cpuInfo->stime, &cpuInfo->cutime, &cpuInfo->cstime);
      break;
    }
  }

  tfree(line);
  fclose(fp);
  return true;
}

static void taosGetSystemTimezone() {
  SGlobalCfg *cfg_timezone = taosGetConfigOption("timezone");
  if (cfg_timezone == NULL) return;
  if (cfg_timezone->cfgStatus >= TAOS_CFG_CSTATUS_DEFAULT) {
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

  // for CentOS system, /etc/timezone does not exist. Ignore the TZ environment variables
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

  // cfg_timezone->cfgStatus = TAOS_CFG_CSTATUS_DEFAULT;
  uPrint("timezone not configured, set to system default:%s", tsTimezone);
}

/*
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
      uError("can't get locale from system");
    } else {
      strncpy(tsLocale, locale, tListLen(tsLocale));
      uPrint("locale not configured, set to system default:%s", tsLocale);
    }
  }

  /* if user does not specify the charset, extract it from locale */
  SGlobalCfg *cfg_charset = taosGetConfigOption("charset");
  if (cfg_charset && cfg_charset->cfgStatus < TAOS_CFG_CSTATUS_DEFAULT) {
    char *str = strrchr(tsLocale, sep);
    if (str != NULL) {
      str++;

      char *revisedCharset = taosCharsetReplace(str);
      strncpy(tsCharset, revisedCharset, tListLen(tsCharset));

      free(revisedCharset);
      uPrint("charset not configured, set to system default:%s", tsCharset);
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

bool taosGetDisk() {
  struct statvfs info;
  const double   unit = 1024 * 1024 * 1024;

  if (tscEmbedded) {
    if (statvfs(dataDir, &info)) {
      tsTotalDataDirGB = 0;
      tsAvailDataDirGB = 0;
      return false;
    } else {
      tsTotalDataDirGB = (float)((double)info.f_blocks * (double)info.f_frsize / unit);
      tsAvailDataDirGB = (float)((double)info.f_bavail * (double)info.f_frsize / unit);
    }
  }

  if (statvfs(logDir, &info)) {
    tsTotalLogDirGB = 0;
    tsAvailLogDirGB = 0;
    return false;
  } else {
    tsTotalLogDirGB = (float)((double)info.f_blocks * (double)info.f_frsize / unit);
    tsAvailLogDirGB = (float)((double)info.f_bavail * (double)info.f_frsize / unit);
  }

  if (statvfs("/tmp", &info)) {
    tsTotalTmpDirGB = 0;
    tsAvailTmpDirGB = 0;
    return false;
  } else {
    tsTotalTmpDirGB = (float)((double)info.f_blocks * (double)info.f_frsize / unit);
    tsAvailTmpDirGB = (float)((double)info.f_bavail * (double)info.f_frsize / unit);
  }

  return true;
}

static bool taosGetCardName(char *ip, char *name) {
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

static bool taosGetCardInfo(int64_t *bytes) {
  static char tsPublicCard[1000] = {0};
  if (tsPublicCard[0] == 0) {
    if (!taosGetCardName(tsPrivateIp, tsPublicCard)) {
      uError("can't get card name from ip:%s", tsPrivateIp);
      return false;
    }
    int cardNameLen = (int)strlen(tsPublicCard);
    for (int i = 0; i < cardNameLen; ++i) {
      if (tsPublicCard[i] == ':') {
        tsPublicCard[i] = 0;
        break;
      }
    }
    // uTrace("card name of public ip:%s is %s", tsPublicIp, tsPublicCard);
  }

  FILE *fp = fopen(tsSysNetFile, "r");
  if (fp == NULL) {
    uError("open file:%s failed", tsSysNetFile);
    return false;
  }

  int64_t rbytes, rpackts, tbytes, tpackets;
  int64_t nouse1, nouse2, nouse3, nouse4, nouse5, nouse6;
  char    nouse0[200] = {0};

  size_t len;
  char * line = NULL;

  while (!feof(fp)) {
    tfree(line);
    len = 0;
    getline(&line, &len, fp);
    if (line == NULL) {
      break;
    }
    if (strstr(line, tsPublicCard) != NULL) {
      break;
    }
  }
  if (line != NULL) {
    sscanf(line, "%s %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64, nouse0, &rbytes, &rpackts, &nouse1, &nouse2, &nouse3,
           &nouse4, &nouse5, &nouse6, &tbytes, &tpackets);
    *bytes = rbytes + tbytes;
    tfree(line);
    fclose(fp);
    return true;
  } else {
    uWarn("can't get card:%s info from device:%s", tsPublicCard, tsSysNetFile);
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

  // uPrint("bandwidth lastBytes:%ld, lastTime:%ld, curBytes:%ld, curTime:%ld,
  // speed:%f", lastBytes, lastTime, curBytes, curTime, *bandSpeed);

  lastTime = curTime;
  lastBytes = curBytes;

  return true;
}

static bool taosReadProcIO(int64_t *readbyte, int64_t *writebyte) {
  FILE *fp = fopen(tsProcIOFile, "r");
  if (fp == NULL) {
    uError("open file:%s failed", tsProcIOFile);
    return false;
  }

  size_t len;
  char * line = NULL;
  char   tmp[10];
  int    readIndex = 0;

  while (!feof(fp)) {
    tfree(line);
    len = 0;
    getline(&line, &len, fp);
    if (line == NULL) {
      break;
    }
    if (strstr(line, "rchar:") != NULL) {
      sscanf(line, "%s %" PRId64, tmp, readbyte);
      readIndex++;
    } else if (strstr(line, "wchar:") != NULL) {
      sscanf(line, "%s %" PRId64, tmp, writebyte);
      readIndex++;
    } else {
    }

    if (readIndex >= 2) break;
  }

  tfree(line);
  fclose(fp);

  if (readIndex < 2) {
    uError("read file:%s failed", tsProcIOFile);
    return false;
  }

  return true;
}

bool taosGetProcIO(float *readKB, float *writeKB) {
  static int64_t lastReadbyte = -1;
  static int64_t lastWritebyte = -1;

  int64_t curReadbyte  = 0;
  int64_t curWritebyte = 0;

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
  taosGetDisk();
  taosGetBandSpeed(&tmp1);
  taosGetCpuUsage(&tmp1, &tmp2);
  taosGetProcIO(&tmp1, &tmp2);

  taosGetSystemTimezone();
  taosGetSystemLocale();
}

void taosPrintOsInfo() {
  uPrint(" os pageSize:            %" PRId64 "(KB)", tsPageSize);
  uPrint(" os openMax:             %" PRId64, tsOpenMax);
  uPrint(" os streamMax:           %" PRId64, tsStreamMax);
  uPrint(" os numOfCores:          %d", tsNumOfCores);
  uPrint(" os totalDisk:           %f(GB)", tsTotalDataDirGB);
  uPrint(" os totalMemory:         %d(MB)", tsTotalMemoryMB);

  struct utsname buf;
  if (uname(&buf)) {
    uPrint(" can't fetch os info");
    return;
  }
  uPrint(" os sysname:             %s", buf.sysname);
  uPrint(" os nodename:            %s", buf.nodename);
  uPrint(" os release:             %s", buf.release);
  uPrint(" os version:             %s", buf.version);
  uPrint(" os machine:             %s", buf.machine);
  uPrint("==================================");
}

void taosKillSystem() {
  // SIGINT
  uPrint("taosd will shut down soon");
  kill(tsProcId, 2);
}

int _sysctl(struct __sysctl_args *args );
void taosSetCoreDump() {
  if (0 == tsEnableCoreFile) {
    return;
  }
  
  // 1. set ulimit -c unlimited
  struct rlimit rlim;
  struct rlimit rlim_new;
  if (getrlimit(RLIMIT_CORE, &rlim) == 0) {
    uPrint("the old unlimited para: rlim_cur=%d, rlim_max=%d", rlim.rlim_cur, rlim.rlim_max);
    rlim_new.rlim_cur = RLIM_INFINITY;
    rlim_new.rlim_max = RLIM_INFINITY;
    if (setrlimit(RLIMIT_CORE, &rlim_new) != 0) {
      uPrint("set unlimited fail, error: %s", strerror(errno));
      rlim_new.rlim_cur = rlim.rlim_max;
      rlim_new.rlim_max = rlim.rlim_max;
      (void)setrlimit(RLIMIT_CORE, &rlim_new);
    }
  }

  if (getrlimit(RLIMIT_CORE, &rlim) == 0) {
    uPrint("the new unlimited para: rlim_cur=%d, rlim_max=%d", rlim.rlim_cur, rlim.rlim_max);
  }

#ifndef _TD_ARM_
  // 2. set the path for saving core file
  struct __sysctl_args args;
  int     old_usespid = 0;
  size_t  old_len     = 0;
  int     new_usespid = 1;
  size_t  new_len     = sizeof(new_usespid);
  
  int name[] = {CTL_KERN, KERN_CORE_USES_PID};
  
  memset(&args, 0, sizeof(struct __sysctl_args));
  args.name    = name;
  args.nlen    = sizeof(name)/sizeof(name[0]);
  args.oldval  = &old_usespid;
  args.oldlenp = &old_len;
  args.newval  = &new_usespid;
  args.newlen  = new_len;
  
  old_len = sizeof(old_usespid);
  
  if (syscall(SYS__sysctl, &args) == -1) {
      uPrint("_sysctl(kern_core_uses_pid) set fail: %s", strerror(errno));
  }
  
  uPrint("The old core_uses_pid[%d]: %d", old_len, old_usespid);


  old_usespid = 0;
  old_len     = 0;
  memset(&args, 0, sizeof(struct __sysctl_args));
  args.name    = name;
  args.nlen    = sizeof(name)/sizeof(name[0]);
  args.oldval  = &old_usespid;
  args.oldlenp = &old_len;
  
  old_len = sizeof(old_usespid);
  
  if (syscall(SYS__sysctl, &args) == -1) {
      uPrint("_sysctl(kern_core_uses_pid) get fail: %s", strerror(errno));
  }
  
  uPrint("The new core_uses_pid[%d]: %d", old_len, old_usespid);
#endif
  
#if 0
  // 3. create the path for saving core file
  int status; 
  char coredump_dir[32] = "/var/log/taosdump";
  if (opendir(coredump_dir) == NULL) {
    status = mkdir(coredump_dir, S_IRWXU | S_IRWXG | S_IRWXO); 
    if (status) {
      uPrint("mkdir fail, error: %s\n", strerror(errno));
    }
  }

  // 4. set kernel.core_pattern
   struct __sysctl_args args;
   char    old_corefile[128];
   size_t  old_len;
   char    new_corefile[128] = "/var/log/taosdump/core-%e-%p";
   size_t  new_len = sizeof(new_corefile);
   
   int name[] = {CTL_KERN, KERN_CORE_PATTERN};

   memset(&args, 0, sizeof(struct __sysctl_args));
   args.name    = name;
   args.nlen    = sizeof(name)/sizeof(name[0]);
   args.oldval  = old_corefile;
   args.oldlenp = &old_len;
   args.newval  = new_corefile;
   args.newlen  = new_len;

   old_len = sizeof(old_corefile);

   if (syscall(SYS__sysctl, &args) == -1) {
       uPrint("_sysctl(kern_core_pattern) set fail: %s", strerror(errno));
   }
   
   uPrint("The old kern_core_pattern: %*s\n", old_len, old_corefile);


   memset(&args, 0, sizeof(struct __sysctl_args));
   args.name    = name;
   args.nlen    = sizeof(name)/sizeof(name[0]);
   args.oldval  = old_corefile;
   args.oldlenp = &old_len;
   
   old_len = sizeof(old_corefile);

   if (syscall(SYS__sysctl, &args) == -1) {
       uPrint("_sysctl(kern_core_pattern) get fail: %s", strerror(errno));
   }
   
   uPrint("The new kern_core_pattern: %*s\n", old_len, old_corefile);
#endif
  
}

