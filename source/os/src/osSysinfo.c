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

int32_t tsTotalMemoryMB = 0;
int64_t tsPageSize = 0;
int64_t tsOpenMax = 0;
int64_t tsStreamMax = 0;
int32_t tsNumOfCores = 1;
char    tsTimezone[TSDB_TIMEZONE_LEN] = {0};
char    tsLocale[TSDB_LOCALE_LEN] = {0};
char    tsCharset[TSDB_LOCALE_LEN] = {0};  // default encode string

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)

/*
 * windows implementation
 */

#if (_WIN64)
#include <iphlpapi.h>
#include <mswsock.h>
#include <psapi.h>
#include <stdio.h>
#include <windows.h>
#include <ws2tcpip.h>
#pragma comment(lib, "Mswsock.lib ")
#endif

#include <objbase.h>

#pragma warning(push)
#pragma warning(disable : 4091)
#include <DbgHelp.h>
#pragma warning(pop)

static int32_t taosGetTotalMemory() {
  MEMORYSTATUSEX memsStat;
  memsStat.dwLength = sizeof(memsStat);
  if (!GlobalMemoryStatusEx(&memsStat)) {
    return 0;
  }

  float nMemTotal = memsStat.ullTotalPhys / (1024.0f * 1024.0f);
  return (int32_t)nMemTotal;
}

bool taosGetSysMemory(float *memoryUsedMB) {
  MEMORYSTATUSEX memsStat;
  memsStat.dwLength = sizeof(memsStat);
  if (!GlobalMemoryStatusEx(&memsStat)) {
    return false;
  }

  float nMemFree = memsStat.ullAvailPhys / (1024.0f * 1024.0f);
  float nMemTotal = memsStat.ullTotalPhys / (1024.0f * 1024.0f);

  *memoryUsedMB = nMemTotal - nMemFree;
  return true;
}

bool taosGetProcMemory(float *memoryUsedMB) {
  unsigned bytes_used = 0;

#if defined(_WIN64) && defined(_MSC_VER)
  PROCESS_MEMORY_COUNTERS pmc;
  HANDLE                  cur_proc = GetCurrentProcess();

  if (GetProcessMemoryInfo(cur_proc, &pmc, sizeof(pmc))) {
    bytes_used = (unsigned)(pmc.WorkingSetSize + pmc.PagefileUsage);
  }
#endif

  *memoryUsedMB = (float)bytes_used / 1024 / 1024;
  return true;
}

static void taosGetSystemTimezone() {
  // get and set default timezone
  char *tz = getenv("TZ");
  if (tz == NULL || strlen(tz) == 0) {
    strcpy(tsTimezone, "not configured");
  } else {
    strcpy(tsTimezone, tz);
  }
}

static void taosGetSystemLocale() {
  // get and set default locale
  char *locale = setlocale(LC_CTYPE, "chs");
  if (locale != NULL) {
    tstrncpy(tsLocale, locale, TSDB_LOCALE_LEN);
  }

  strcpy(tsCharset, "cp936");
}

int32_t taosGetCpuCores() {
  SYSTEM_INFO info;
  GetSystemInfo(&info);
  return (int32_t)info.dwNumberOfProcessors;
}

bool taosGetCpuUsage(float *sysCpuUsage, float *procCpuUsage) {
  *sysCpuUsage = 0;
  *procCpuUsage = 0;
  return true;
}

int32_t taosGetDiskSize(char *dataDir, SysDiskSize *diskSize) {
  unsigned _int64 i64FreeBytesToCaller;
  unsigned _int64 i64TotalBytes;
  unsigned _int64 i64FreeBytes;

  BOOL fResult = GetDiskFreeSpaceExA(dataDir, (PULARGE_INTEGER)&i64FreeBytesToCaller, (PULARGE_INTEGER)&i64TotalBytes,
                                     (PULARGE_INTEGER)&i64FreeBytes);
  if (fResult) {
    diskSize->tsize = (int64_t)(i64TotalBytes);
    diskSize->avail = (int64_t)(i64FreeBytesToCaller);
    diskSize->used = (int64_t)(i64TotalBytes - i64FreeBytes);
    return 0;
  } else {
    printf("failed to get disk size, dataDir:%s errno:%s", tsDataDir, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }
}

bool taosGetCardInfo(int64_t *bytes, int64_t *rbytes, int64_t *tbytes) {
  if (bytes) *bytes = 0;
  if (rbytes) *rbytes = 0;
  if (tbytes) *tbytes = 0;
  return true;
}

bool taosGetBandSpeed(float *bandSpeedKb) {
  *bandSpeedKb = 0;
  return true;
}

bool taosReadProcIO(int64_t *readbyte, int64_t *writebyte) {
  IO_COUNTERS io_counter;
  if (GetProcessIoCounters(GetCurrentProcess(), &io_counter)) {
    if (readbyte) *readbyte = io_counter.ReadTransferCount;
    if (writebyte) *writebyte = io_counter.WriteTransferCount;
    return true;
  }
  return false;
}

bool taosGetProcIO(float *readKB, float *writeKB) {
  static int64_t lastReadbyte = -1;
  static int64_t lastWritebyte = -1;

  int64_t curReadbyte = 0;
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
  tsNumOfCores = taosGetCpuCores();
  tsTotalMemoryMB = taosGetTotalMemory();

  float tmp1, tmp2;
  // taosGetDisk();
  taosGetBandSpeed(&tmp1);
  taosGetCpuUsage(&tmp1, &tmp2);
  taosGetProcIO(&tmp1, &tmp2);

  taosGetSystemTimezone();
  taosGetSystemLocale();
}

void taosKillSystem() {
  printf("function taosKillSystem, exit!");
  exit(0);
}

int taosSystem(const char *cmd) {
  printf("taosSystem not support");
  return -1;
}

LONG WINAPI FlCrashDump(PEXCEPTION_POINTERS ep) {
  typedef BOOL(WINAPI * FxMiniDumpWriteDump)(IN HANDLE hProcess, IN DWORD ProcessId, IN HANDLE hFile,
                                             IN MINIDUMP_TYPE                           DumpType,
                                             IN CONST PMINIDUMP_EXCEPTION_INFORMATION   ExceptionParam,
                                             IN CONST PMINIDUMP_USER_STREAM_INFORMATION UserStreamParam,
                                             IN CONST PMINIDUMP_CALLBACK_INFORMATION    CallbackParam);

  HMODULE dll = LoadLibrary("dbghelp.dll");
  if (dll == NULL) return EXCEPTION_CONTINUE_SEARCH;
  FxMiniDumpWriteDump mdwd = (FxMiniDumpWriteDump)(GetProcAddress(dll, "MiniDumpWriteDump"));
  if (mdwd == NULL) {
    FreeLibrary(dll);
    return EXCEPTION_CONTINUE_SEARCH;
  }

  TCHAR path[MAX_PATH];
  DWORD len = GetModuleFileName(NULL, path, _countof(path));
  path[len - 3] = 'd';
  path[len - 2] = 'm';
  path[len - 1] = 'p';

  HANDLE file = CreateFile(path, GENERIC_WRITE, 0, NULL, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);

  MINIDUMP_EXCEPTION_INFORMATION mei;
  mei.ThreadId = GetCurrentThreadId();
  mei.ExceptionPointers = ep;
  mei.ClientPointers = FALSE;

  (*mdwd)(GetCurrentProcess(), GetCurrentProcessId(), file, MiniDumpWithHandleData, &mei, NULL, NULL);

  CloseHandle(file);
  FreeLibrary(dll);

  return EXCEPTION_CONTINUE_SEARCH;
}

void taosSetCoreDump() { SetUnhandledExceptionFilter(&FlCrashDump); }

bool taosGetSystemUid(char *uid) {
  GUID guid;
  CoCreateGuid(&guid);

  sprintf(uid, "%08X-%04X-%04X-%02X%02X-%02X%02X%02X%02X%02X%02X", guid.Data1, guid.Data2, guid.Data3, guid.Data4[0],
          guid.Data4[1], guid.Data4[2], guid.Data4[3], guid.Data4[4], guid.Data4[5], guid.Data4[6], guid.Data4[7]);

  return true;
}

char *taosGetCmdlineByPID(int pid) { return ""; }

#elif defined(_TD_DARWIN_64)

/*
 * darwin implementation
 */

#include <errno.h>
#include <libproc.h>

static void taosGetSystemTimezone() {
  /* load time zone string from /etc/localtime */
  char  buf[4096];
  char *tz = NULL;
  {
    int n = readlink("/etc/localtime", buf, sizeof(buf));
    if (n < 0) {
      printf("read /etc/localtime error, reason:%s", strerror(errno));
      return;
    }
    buf[n] = '\0';
    for (int i = n - 1; i >= 0; --i) {
      if (buf[i] == '/') {
        if (tz) {
          tz = buf + i + 1;
          break;
        }
        tz = buf + i + 1;
      }
    }
    if (!tz || 0 == strchr(tz, '/')) {
      printf("parsing /etc/localtime failed");
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
  snprintf(tsTimezone, TSDB_TIMEZONE_LEN, "%s (%s, %+03ld00)", tz, tm1.tm_isdst ? tzname[daylight] : tzname[0],
           -timezone / 3600);

  // cfg_timezone->cfgStatus = TAOS_CFG_CSTATUS_DEFAULT;
  printf("timezone not configured, set to system default:%s", tsTimezone);
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

  locale = setlocale(LC_CTYPE, "");
  if (locale == NULL) {
    printf("can't get locale from system, set it to en_US.UTF-8 since error:%d:%s", errno, strerror(errno));
    strcpy(tsLocale, "en_US.UTF-8");
  } else {
    tstrncpy(tsLocale, locale, TSDB_LOCALE_LEN);
    printf("locale not configured, set to system default:%s", tsLocale);
  }

  /* if user does not specify the charset, extract it from locale */
  char *str = strrchr(tsLocale, sep);
  if (str != NULL) {
    str++;

    char *revisedCharset = taosCharsetReplace(str);
    tstrncpy(tsCharset, revisedCharset, TSDB_LOCALE_LEN);

    free(revisedCharset);
    printf("charset not configured, set to system default:%s", tsCharset);
  } else {
    strcpy(tsCharset, "UTF-8");
    printf("can't get locale and charset from system, set it to UTF-8");
  }
}

void taosKillSystem() {
  printf("function taosKillSystem, exit!");
  exit(0);
}

int32_t taosGetCpuCores() { return sysconf(_SC_NPROCESSORS_ONLN); }

void taosGetSystemInfo() {
  // taosGetProcInfos();

  tsNumOfCores = sysconf(_SC_NPROCESSORS_ONLN);
  long physical_pages = sysconf(_SC_PHYS_PAGES);
  long page_size = sysconf(_SC_PAGESIZE);
  tsTotalMemoryMB = physical_pages * page_size / (1024 * 1024);
  tsPageSize = page_size;

  taosGetSystemTimezone();
  taosGetSystemLocale();
}

bool taosReadProcIO(int64_t *rchars, int64_t *wchars) {
  if (rchars) *rchars = 0;
  if (wchars) *wchars = 0;
  return true;
}

bool taosGetProcIO(float *readKB, float *writeKB) {
  *readKB = 0;
  *writeKB = 0;
  return true;
}

bool taosGetCardInfo(int64_t *bytes, int64_t *rbytes, int64_t *tbytes) {
  if (bytes) *bytes = 0;
  if (rbytes) *rbytes = 0;
  if (tbytes) *tbytes = 0;
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
  printf("un support funtion");
  return -1;
}

void taosSetCoreDump() {}

int32_t taosGetDiskSize(char *dataDir, SysDiskSize *diskSize) {
  struct statvfs info;
  if (statvfs(dataDir, &info)) {
    printf("failed to get disk size, dataDir:%s errno:%s", tsDataDir, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  } else {
    diskSize->tsize = info.f_blocks * info.f_frsize;
    diskSize->avail = info.f_bavail * info.f_frsize;
    diskSize->used = (info.f_blocks - info.f_bfree) * info.f_frsize;
    return 0;
  }
}

bool taosGetSystemUid(char *uid) {
  uuid_t uuid = {0};
  uuid_generate(uuid);
  // it's caller's responsibility to make enough space for `uid`, that's 36-char + 1-null
  uuid_unparse_lower(uuid, uid);
  return true;
}

char *taosGetCmdlineByPID(int pid) {
  static char cmdline[1024];
  errno = 0;

  if (proc_pidpath(pid, cmdline, sizeof(cmdline)) <= 0) {
    fprintf(stderr, "PID is %d, %s", pid, strerror(errno));
    return strerror(errno);
  }

  return cmdline;
}

#else

/*
 * linux implementation
 */

#include <argp.h>
#include <linux/sysctl.h>
#include <sys/file.h>
#include <sys/resource.h>
#include <sys/statvfs.h>
#include <sys/syscall.h>
#include <sys/utsname.h>

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

static void taosGetProcInfos() {
  tsPageSize = sysconf(_SC_PAGESIZE);
  tsOpenMax = sysconf(_SC_OPEN_MAX);
  tsStreamMax = sysconf(_SC_STREAM_MAX);

  tsProcId = (pid_t)syscall(SYS_gettid);
  tsPageSizeKB = (float)(sysconf(_SC_PAGESIZE)) / 1024;

  snprintf(tsProcMemFile, 25, "/proc/%d/status", tsProcId);
  snprintf(tsProcCpuFile, 25, "/proc/%d/stat", tsProcId);
  snprintf(tsProcIOFile, 25, "/proc/%d/io", tsProcId);
}

static int32_t taosGetTotalMemory() { return (int32_t)((float)sysconf(_SC_PHYS_PAGES) * tsPageSizeKB / 1024); }

bool taosGetSysMemory(float *memoryUsedMB) {
  float memoryAvailMB = (float)sysconf(_SC_AVPHYS_PAGES) * tsPageSizeKB / 1024;
  *memoryUsedMB = (float)tsTotalMemoryMB - memoryAvailMB;
  return true;
}

bool taosGetProcMemory(float *memoryUsedMB) {
  FILE *fp = fopen(tsProcMemFile, "r");
  if (fp == NULL) {
    printf("open file:%s failed", tsProcMemFile);
    return false;
  }

  ssize_t _bytes = 0;
  size_t  len;
  char *  line = NULL;
  while (!feof(fp)) {
    tfree(line);
    len = 0;
    _bytes = getline(&line, &len, fp);
    if ((_bytes < 0) || (line == NULL)) {
      break;
    }
    if (strstr(line, "VmRSS:") != NULL) {
      break;
    }
  }

  if (line == NULL) {
    printf("read file:%s failed", tsProcMemFile);
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
    printf("open file:%s failed", tsSysCpuFile);
    return false;
  }

  size_t  len;
  char *  line = NULL;
  ssize_t _bytes = getline(&line, &len, fp);
  if ((_bytes < 0) || (line == NULL)) {
    printf("read file:%s failed", tsSysCpuFile);
    fclose(fp);
    return false;
  }

  char cpu[10] = {0};
  sscanf(line, "%s %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64, cpu, &cpuInfo->user, &cpuInfo->nice, &cpuInfo->system,
         &cpuInfo->idle);

  tfree(line);
  fclose(fp);
  return true;
}

static bool taosGetProcCpuInfo(ProcCpuInfo *cpuInfo) {
  FILE *fp = fopen(tsProcCpuFile, "r");
  if (fp == NULL) {
    printf("open file:%s failed", tsProcCpuFile);
    return false;
  }

  size_t  len = 0;
  char *  line = NULL;
  ssize_t _bytes = getline(&line, &len, fp);
  if ((_bytes < 0) || (line == NULL)) {
    printf("read file:%s failed", tsProcCpuFile);
    fclose(fp);
    return false;
  }

  for (int i = 0, blank = 0; line[i] != 0; ++i) {
    if (line[i] == ' ') blank++;
    if (blank == PROCESS_ITEM) {
      sscanf(line + i + 1, "%" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64, &cpuInfo->utime, &cpuInfo->stime,
             &cpuInfo->cutime, &cpuInfo->cstime);
      break;
    }
  }

  tfree(line);
  fclose(fp);
  return true;
}

static void taosGetSystemTimezone() {
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
    if (len < 64 && ferror(f)) {
      fclose(f);
      printf("read /etc/timezone error, reason:%s", strerror(errno));
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
  printf("timezone not configured, set to system default:%s", tsTimezone);
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

  locale = setlocale(LC_CTYPE, "");
  if (locale == NULL) {
    printf("can't get locale from system, set it to en_US.UTF-8 since error:%d:%s", errno, strerror(errno));
    strcpy(tsLocale, "en_US.UTF-8");
  } else {
    tstrncpy(tsLocale, locale, TSDB_LOCALE_LEN);
    printf("locale not configured, set to system default:%s", tsLocale);
  }

  /* if user does not specify the charset, extract it from locale */
  char *str = strrchr(tsLocale, sep);
  if (str != NULL) {
    str++;

    char *revisedCharset = taosCharsetReplace(str);
    tstrncpy(tsCharset, revisedCharset, TSDB_LOCALE_LEN);

    free(revisedCharset);
    printf("charset not configured, set to system default:%s", tsCharset);
  } else {
    strcpy(tsCharset, "UTF-8");
    printf("can't get locale and charset from system, set it to UTF-8");
  }
}

int32_t taosGetCpuCores() { return (int32_t)sysconf(_SC_NPROCESSORS_ONLN); }

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

int32_t taosGetDiskSize(char *dataDir, SysDiskSize *diskSize) {
  struct statvfs info;
  if (statvfs(dataDir, &info)) {
    printf("failed to get disk size, dataDir:%s errno:%s", dataDir, strerror(errno));
    return -1;
  } else {
    diskSize->tsize = info.f_blocks * info.f_frsize;
    diskSize->avail = info.f_bavail * info.f_frsize;
    diskSize->used = (info.f_blocks - info.f_bfree) * info.f_frsize;
    return 0;
  }
}

bool taosGetCardInfo(int64_t *bytes, int64_t *rbytes, int64_t *tbytes) {
  *bytes = 0;
  FILE *fp = fopen(tsSysNetFile, "r");
  if (fp == NULL) {
    printf("open file:%s failed", tsSysNetFile);
    return false;
  }

  ssize_t _bytes = 0;
  size_t  len = 2048;
  char *  line = calloc(1, len);

  while (!feof(fp)) {
    memset(line, 0, len);

    int64_t o_rbytes = 0;
    int64_t rpackts = 0;
    int64_t o_tbytes = 0;
    int64_t tpackets = 0;
    int64_t nouse1 = 0;
    int64_t nouse2 = 0;
    int64_t nouse3 = 0;
    int64_t nouse4 = 0;
    int64_t nouse5 = 0;
    int64_t nouse6 = 0;
    char    nouse0[200] = {0};

    _bytes = getline(&line, &len, fp);
    if (_bytes < 0) {
      break;
    }

    line[len - 1] = 0;

    if (strstr(line, "lo:") != NULL) {
      continue;
    }

    sscanf(line,
           "%s %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64
           " %" PRId64,
           nouse0, &o_rbytes, &rpackts, &nouse1, &nouse2, &nouse3, &nouse4, &nouse5, &nouse6, &o_tbytes, &tpackets);
    if (rbytes) *rbytes = o_rbytes;
    if (tbytes) *tbytes = o_tbytes;
    *bytes += (o_rbytes + o_tbytes);
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

  if (!taosGetCardInfo(&curBytes, NULL, NULL)) {
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

  // printf("bandwidth lastBytes:%ld, lastTime:%ld, curBytes:%ld, curTime:%ld,
  // speed:%f", lastBytes, lastTime, curBytes, curTime, *bandSpeed);

  lastTime = curTime;
  lastBytes = curBytes;

  return true;
}

bool taosReadProcIO(int64_t *rchars, int64_t *wchars) {
  FILE *fp = fopen(tsProcIOFile, "r");
  if (fp == NULL) {
    printf("open file:%s failed", tsProcIOFile);
    return false;
  }

  ssize_t _bytes = 0;
  size_t  len;
  char *  line = NULL;
  char    tmp[10];
  int     readIndex = 0;

  while (!feof(fp)) {
    tfree(line);
    len = 0;
    _bytes = getline(&line, &len, fp);
    if ((_bytes < 0) || (line == NULL)) {
      break;
    }
    if (strstr(line, "rchar:") != NULL) {
      sscanf(line, "%s %" PRId64, tmp, rchars);
      readIndex++;
    } else if (strstr(line, "wchar:") != NULL) {
      sscanf(line, "%s %" PRId64, tmp, wchars);
      readIndex++;
    } else {
    }

    if (readIndex >= 2) break;
  }

  tfree(line);
  fclose(fp);

  if (readIndex < 2) {
    printf("read file:%s failed", tsProcIOFile);
    return false;
  }

  return true;
}

bool taosGetProcIO(float *readKB, float *writeKB) {
  static int64_t lastReadbyte = -1;
  static int64_t lastWritebyte = -1;

  int64_t curReadbyte = 0;
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
  taosGetProcInfos();

  tsNumOfCores = taosGetCpuCores();
  tsTotalMemoryMB = taosGetTotalMemory();

  float tmp1, tmp2;
  taosGetSysMemory(&tmp1);
  taosGetProcMemory(&tmp2);
  // taosGetDisk();
  taosGetBandSpeed(&tmp1);
  taosGetCpuUsage(&tmp1, &tmp2);
  taosGetProcIO(&tmp1, &tmp2);

  taosGetSystemTimezone();
  taosGetSystemLocale();
}

void taosKillSystem() {
  // SIGINT
  printf("taosd will shut down soon");
  kill(tsProcId, 2);
}

int taosSystem(const char *cmd) {
  FILE *fp;
  int   res;
  char  buf[1024];
  if (cmd == NULL) {
    printf("taosSystem cmd is NULL!");
    return -1;
  }

  if ((fp = popen(cmd, "r")) == NULL) {
    printf("popen cmd:%s error: %s", cmd, strerror(errno));
    return -1;
  } else {
    while (fgets(buf, sizeof(buf), fp)) {
      printf("popen result:%s", buf);
    }

    if ((res = pclose(fp)) == -1) {
      printf("close popen file pointer fp error!");
    } else {
      printf("popen res is :%d", res);
    }

    return res;
  }
}

void taosSetCoreDump(bool enable) {
  if (!enable) return;

  // 1. set ulimit -c unlimited
  struct rlimit rlim;
  struct rlimit rlim_new;
  if (getrlimit(RLIMIT_CORE, &rlim) == 0) {
#ifndef _ALPINE
    printf("the old unlimited para: rlim_cur=%" PRIu64 ", rlim_max=%" PRIu64, rlim.rlim_cur, rlim.rlim_max);
#else
    printf("the old unlimited para: rlim_cur=%llu, rlim_max=%llu", rlim.rlim_cur, rlim.rlim_max);
#endif
    rlim_new.rlim_cur = RLIM_INFINITY;
    rlim_new.rlim_max = RLIM_INFINITY;
    if (setrlimit(RLIMIT_CORE, &rlim_new) != 0) {
      printf("set unlimited fail, error: %s", strerror(errno));
      rlim_new.rlim_cur = rlim.rlim_max;
      rlim_new.rlim_max = rlim.rlim_max;
      (void)setrlimit(RLIMIT_CORE, &rlim_new);
    }
  }

  if (getrlimit(RLIMIT_CORE, &rlim) == 0) {
#ifndef _ALPINE
    printf("the new unlimited para: rlim_cur=%" PRIu64 ", rlim_max=%" PRIu64, rlim.rlim_cur, rlim.rlim_max);
#else
    printf("the new unlimited para: rlim_cur=%llu, rlim_max=%llu", rlim.rlim_cur, rlim.rlim_max);
#endif
  }

#ifndef _TD_ARM_
  // 2. set the path for saving core file
  struct __sysctl_args args;

  int    old_usespid = 0;
  size_t old_len = 0;
  int    new_usespid = 1;
  size_t new_len = sizeof(new_usespid);

  int name[] = {CTL_KERN, KERN_CORE_USES_PID};

  memset(&args, 0, sizeof(struct __sysctl_args));
  args.name = name;
  args.nlen = sizeof(name) / sizeof(name[0]);
  args.oldval = &old_usespid;
  args.oldlenp = &old_len;
  args.newval = &new_usespid;
  args.newlen = new_len;

  old_len = sizeof(old_usespid);

  if (syscall(SYS__sysctl, &args) == -1) {
    printf("_sysctl(kern_core_uses_pid) set fail: %s", strerror(errno));
  }

  printf("The old core_uses_pid[%" PRIu64 "]: %d", old_len, old_usespid);

  old_usespid = 0;
  old_len = 0;
  memset(&args, 0, sizeof(struct __sysctl_args));
  args.name = name;
  args.nlen = sizeof(name) / sizeof(name[0]);
  args.oldval = &old_usespid;
  args.oldlenp = &old_len;

  old_len = sizeof(old_usespid);

  if (syscall(SYS__sysctl, &args) == -1) {
    printf("_sysctl(kern_core_uses_pid) get fail: %s", strerror(errno));
  }

  printf("The new core_uses_pid[%" PRIu64 "]: %d", old_len, old_usespid);
#endif
}

bool taosGetSystemUid(char *uid, int32_t uidlen) {
  int fd;
  int len = 0;

  fd = open("/proc/sys/kernel/random/uuid", 0);
  if (fd < 0) {
    return false;
  } else {
    len = read(fd, uid, uidlen);
    close(fd);
  }

  if (len >= 36) {
    uid[36] = 0;
    return true;
  }
  return false;
}

char *taosGetCmdlineByPID(int pid) {
  static char cmdline[1024];
  sprintf(cmdline, "/proc/%d/cmdline", pid);

  int fd = open(cmdline, O_RDONLY);
  if (fd >= 0) {
    int n = read(fd, cmdline, sizeof(cmdline) - 1);
    if (n < 0) n = 0;

    if (n > 0 && cmdline[n - 1] == '\n') --n;

    cmdline[n] = 0;

    close(fd);
  } else {
    cmdline[0] = 0;
  }

  return cmdline;
}

#endif

#if !(defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32))

SysNameInfo taosGetSysNameInfo() {
  SysNameInfo info = {0};

  struct utsname buf;
  if (!uname(&buf)) {
    info.sysname = buf.sysname;
    info.sysname == buf.nodename;
    info.sysname = buf.release;
    info.sysname = buf.version;
    info.sysname = buf.machine;
  }

  return info;
}
#endif