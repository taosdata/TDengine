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

#ifndef TAOS_OS_FUNC_SYSINFO

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
  char  buf[68] = {0};
  if (f != NULL) {
    int len = fread(buf, 64, 1, f);
    if(len < 64 && ferror(f)) {
      fclose(f);
      uError("read /etc/timezone error, reason:%s", strerror(errno));
      return;
    }
    
    fclose(f);

    buf[sizeof(buf) - 1] = 0;
    char *lineEnd = strstr(buf, "\n");
    if (lineEnd != NULL) {
      *lineEnd = 0;
    }

    // for CentOS system, /etc/timezone does not exist. Ignore the TZ environment variables
    if (strlen(buf) > 0) {
      setenv("TZ", buf, 1);
    }
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
  snprintf(tsTimezone, TSDB_TIMEZONE_LEN, "%s (%s, %s%02d00)", buf, tzname[daylight], tz >= 0 ? "+" : "-", abs(tz));

  // cfg_timezone->cfgStatus = TAOS_CFG_CSTATUS_DEFAULT;
  uWarn("timezone not configured, set to system default:%s", tsTimezone);
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
      uError("can't get locale from system, set it to en_US.UTF-8");
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
    if (statvfs(tsDataDir, &info)) {
      //tsTotalDataDirGB = 0;
      //tsAvailDataDirGB = 0;
      uError("failed to get disk size, dataDir:%s errno:%s", tsDataDir, strerror(errno));
      return false;
    } else {
      tsTotalDataDirGB = (float)((double)info.f_blocks * (double)info.f_frsize / unit);
      tsAvailDataDirGB = (float)((double)info.f_bavail * (double)info.f_frsize / unit);
    }
  }

  if (statvfs(tsLogDir, &info)) {
    //tsTotalLogDirGB = 0;
    //tsAvailLogDirGB = 0;
    uError("failed to get disk size, logDir:%s errno:%s", tsLogDir, strerror(errno));
    return false;
  } else {
    tsTotalLogDirGB = (float)((double)info.f_blocks * (double)info.f_frsize / unit);
    tsAvailLogDirGB = (float)((double)info.f_bavail * (double)info.f_frsize / unit);
  }

  if (statvfs("/tmp", &info)) {
    //tsTotalTmpDirGB = 0;
    //tsAvailTmpDirectorySpace = 0;
    uError("failed to get disk size, tmpDir:/tmp errno:%s", strerror(errno));
    return false;
  } else {
    tsTotalTmpDirGB = (float)((double)info.f_blocks * (double)info.f_frsize / unit);
    tsAvailTmpDirectorySpace = (float)((double)info.f_bavail * (double)info.f_frsize / unit);
  }

  return true;
}

static bool taosGetCardInfo(int64_t *bytes) {
  *bytes = 0;
  FILE *fp = fopen(tsSysNetFile, "r");
  if (fp == NULL) {
    uError("open file:%s failed", tsSysNetFile);
    return false;
  }


  size_t len = 2048;
  char * line = calloc(1, len);

  while (!feof(fp)) {
    memset(line, 0, len);
    
    int64_t rbytes = 0;
    int64_t rpackts = 0;
    int64_t tbytes = 0;
    int64_t tpackets = 0;
    int64_t nouse1 = 0;
    int64_t nouse2 = 0;
    int64_t nouse3 = 0;
    int64_t nouse4 = 0;
    int64_t nouse5 = 0;
    int64_t nouse6 = 0;
    char    nouse0[200] = {0};

    getline(&line, &len, fp);
    line[len - 1] = 0;

    if (strstr(line, "lo:") != NULL) {
      continue;
    }

    sscanf(line,
           "%s %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64
           " %" PRId64,
           nouse0, &rbytes, &rpackts, &nouse1, &nouse2, &nouse3, &nouse4, &nouse5, &nouse6, &tbytes, &tpackets);
    *bytes += (rbytes + tbytes);
  }

  tfree(line);
  fclose(fp);

  return true;
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
    *bandSpeedKb = 0;
    return true;
  }

  if (lastTime >= curTime || lastBytes > curBytes) {
    lastTime = curTime;
    lastBytes = curBytes;
    *bandSpeedKb = 0;
    return true;
  }

  double totalBytes = (double)(curBytes - lastBytes) / 1024 * 8;  // Kb
  *bandSpeedKb = (float)(totalBytes / (double)(curTime - lastTime));

  // uInfo("bandwidth lastBytes:%ld, lastTime:%ld, curBytes:%ld, curTime:%ld,
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
  uInfo(" os pageSize:            %" PRId64 "(KB)", tsPageSize);
  uInfo(" os openMax:             %" PRId64, tsOpenMax);
  uInfo(" os streamMax:           %" PRId64, tsStreamMax);
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
  // SIGINT
  uInfo("taosd will shut down soon");
  kill(tsProcId, 2);
}

int taosSystem(const char *cmd) {
  FILE *fp;
  int   res;
  char  buf[1024];
  if (cmd == NULL) {
    uError("taosSystem cmd is NULL!");
    return -1;
  }

  if ((fp = popen(cmd, "r")) == NULL) {
    uError("popen cmd:%s error: %s", cmd, strerror(errno));
    return -1;
  } else {
    while (fgets(buf, sizeof(buf), fp)) {
      uDebug("popen result:%s", buf);
    }

    if ((res = pclose(fp)) == -1) {
      uError("close popen file pointer fp error!");
    } else {
      uDebug("popen res is :%d", res);
    }

    return res;
  }
}

void taosSetCoreDump() {
  if (0 == tsEnableCoreFile) {
    return;
  }
  
  // 1. set ulimit -c unlimited
  struct rlimit rlim;
  struct rlimit rlim_new;
  if (getrlimit(RLIMIT_CORE, &rlim) == 0) {
    #ifndef _ALPINE
    uInfo("the old unlimited para: rlim_cur=%" PRIu64 ", rlim_max=%" PRIu64, rlim.rlim_cur, rlim.rlim_max);
    #else
    uInfo("the old unlimited para: rlim_cur=%llu, rlim_max=%llu", rlim.rlim_cur, rlim.rlim_max);
    #endif
    rlim_new.rlim_cur = RLIM_INFINITY;
    rlim_new.rlim_max = RLIM_INFINITY;
    if (setrlimit(RLIMIT_CORE, &rlim_new) != 0) {
      uInfo("set unlimited fail, error: %s", strerror(errno));
      rlim_new.rlim_cur = rlim.rlim_max;
      rlim_new.rlim_max = rlim.rlim_max;
      (void)setrlimit(RLIMIT_CORE, &rlim_new);
    }
  }

  if (getrlimit(RLIMIT_CORE, &rlim) == 0) {
    #ifndef _ALPINE
    uInfo("the new unlimited para: rlim_cur=%" PRIu64 ", rlim_max=%" PRIu64, rlim.rlim_cur, rlim.rlim_max);
    #else
    uInfo("the new unlimited para: rlim_cur=%llu, rlim_max=%llu", rlim.rlim_cur, rlim.rlim_max);
    #endif
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
      uInfo("_sysctl(kern_core_uses_pid) set fail: %s", strerror(errno));
  }
  
  uInfo("The old core_uses_pid[%" PRIu64 "]: %d", old_len, old_usespid);


  old_usespid = 0;
  old_len     = 0;
  memset(&args, 0, sizeof(struct __sysctl_args));
  args.name    = name;
  args.nlen    = sizeof(name)/sizeof(name[0]);
  args.oldval  = &old_usespid;
  args.oldlenp = &old_len;
  
  old_len = sizeof(old_usespid);
  
  if (syscall(SYS__sysctl, &args) == -1) {
      uInfo("_sysctl(kern_core_uses_pid) get fail: %s", strerror(errno));
  }
  
  uInfo("The new core_uses_pid[%" PRIu64 "]: %d", old_len, old_usespid);
#endif

}

bool taosGetSystemUid(char *uid) {
  int fd;
  int len = 0;

  fd = open("/proc/sys/kernel/random/uuid", 0);
  if (fd < 0) {
    return false;
  } else {
    len = read(fd, uid, TSDB_CLUSTER_ID_LEN);
    close(fd);
  }

  if (len >= 36) {
    uid[36] = 0;
    return true;
  }
  return false;
}

#endif
