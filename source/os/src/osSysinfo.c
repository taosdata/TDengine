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
#include "taoserror.h"

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

#elif defined(_TD_DARWIN_64)

#include <errno.h>
#include <libproc.h>

#else

#include <argp.h>
#include <linux/sysctl.h>
#include <sys/file.h>
#include <sys/resource.h>
#include <sys/statvfs.h>
#include <sys/syscall.h>
#include <sys/utsname.h>
#include <unistd.h>

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

static void taosGetProcIOnfos() {
  tsPageSizeKB = sysconf(_SC_PAGESIZE) / 1024;
  tsOpenMax = sysconf(_SC_OPEN_MAX);
  tsStreamMax = sysconf(_SC_STREAM_MAX);
  tsProcId = (pid_t)syscall(SYS_gettid);

  snprintf(tsProcMemFile, sizeof(tsProcMemFile), "/proc/%d/status", tsProcId);
  snprintf(tsProcCpuFile, sizeof(tsProcCpuFile), "/proc/%d/stat", tsProcId);
  snprintf(tsProcIOFile, sizeof(tsProcIOFile), "/proc/%d/io", tsProcId);
}

static int32_t taosGetSysCpuInfo(SysCpuInfo *cpuInfo) {
  TdFilePtr pFile = taosOpenFile(tsSysCpuFile, TD_FILE_READ | TD_FILE_STREAM);
  if (pFile == NULL) {
    return -1;
  }

  char   *line = NULL;
  ssize_t _bytes = taosGetLineFile(pFile, &line);
  if ((_bytes < 0) || (line == NULL)) {
    taosCloseFile(&pFile);
    return -1;
  }

  char cpu[10] = {0};
  sscanf(line, "%s %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64, cpu, &cpuInfo->user, &cpuInfo->nice, &cpuInfo->system,
         &cpuInfo->idle);

  if (line != NULL) taosMemoryFreeClear(line);
  taosCloseFile(&pFile);
  return 0;
}

static int32_t taosGetProcCpuInfo(ProcCpuInfo *cpuInfo) {
  TdFilePtr pFile = taosOpenFile(tsProcCpuFile, TD_FILE_READ | TD_FILE_STREAM);
  if (pFile == NULL) {
    return -1;
  }

  char   *line = NULL;
  ssize_t _bytes = taosGetLineFile(pFile, &line);
  if ((_bytes < 0) || (line == NULL)) {
    taosCloseFile(&pFile);
    return -1;
  }

  for (int i = 0, blank = 0; line[i] != 0; ++i) {
    if (line[i] == ' ') blank++;
    if (blank == PROCESS_ITEM) {
      sscanf(line + i + 1, "%" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64, &cpuInfo->utime, &cpuInfo->stime,
             &cpuInfo->cutime, &cpuInfo->cstime);
      break;
    }
  }

  if (line != NULL) taosMemoryFreeClear(line);
  taosCloseFile(&pFile);
  return 0;
}

#endif

bool taosCheckSystemIsSmallEnd() {
  union check {
    int16_t i;
    char    ch[2];
  } c;
  c.i = 1;
  return c.ch[0] == 1;
}

void taosGetSystemInfo() {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  taosGetCpuCores(&tsNumOfCores);
  taosGetTotalMemory(&tsTotalMemoryKB);

  double tmp1, tmp2, tmp3, tmp4;
  taosGetCpuUsage(&tmp1, &tmp2);
#elif defined(_TD_DARWIN_64)
  long physical_pages = sysconf(_SC_PHYS_PAGES);
  long page_size = sysconf(_SC_PAGESIZE);
  tsTotalMemoryKB = physical_pages * page_size / 1024;
  tsPageSizeKB = page_size / 1024;
  tsNumOfCores = sysconf(_SC_NPROCESSORS_ONLN);
#else
  taosGetProcIOnfos();
  taosGetCpuCores(&tsNumOfCores);
  taosGetTotalMemory(&tsTotalMemoryKB);

  double tmp1, tmp2, tmp3, tmp4;
  taosGetCpuUsage(&tmp1, &tmp2);
#endif
}

int32_t taosGetEmail(char *email, int32_t maxLen) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
#elif defined(_TD_DARWIN_64)
  const char *filepath = "/usr/local/taos/email";

  TdFilePtr pFile = taosOpenFile(filepath, TD_FILE_READ);
  if (pFile == NULL) return false;

  if (taosReadFile(pFile, (void *)email, maxLen) < 0) {
    taosCloseFile(&pFile);
    return -1;
  }

  taosCloseFile(&pFile);
  return 0;
#else
  const char *filepath = "/usr/local/taos/email";

  TdFilePtr pFile = taosOpenFile(filepath, TD_FILE_READ);
  if (pFile == NULL) return false;

  if (taosReadFile(pFile, (void *)email, maxLen) < 0) {
    taosCloseFile(&pFile);
    return -1;
  }

  taosCloseFile(&pFile);
  return 0;
#endif
}

int32_t taosGetOsReleaseName(char *releaseName, int32_t maxLen) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
#elif defined(_TD_DARWIN_64)
  char   *line = NULL;
  size_t  size = 0;
  int32_t code = -1;

  TdFilePtr pFile = taosOpenFile("/etc/os-release", TD_FILE_READ | TD_FILE_STREAM);
  if (pFile == NULL) return false;

  while ((size = taosGetLineFile(pFile, &line)) != -1) {
    line[size - 1] = '\0';
    if (strncmp(line, "PRETTY_NAME", 11) == 0) {
      const char *p = strchr(line, '=') + 1;
      if (*p == '"') {
        p++;
        line[size - 2] = 0;
      }
      tstrncpy(releaseName, p, maxLen);
      code = 0;
      break;
    }
  }

  if (line != NULL) taosMemoryFree(line);
  taosCloseFile(&pFile);
  return code;
#else
  char   *line = NULL;
  size_t  size = 0;
  int32_t code = -1;

  TdFilePtr pFile = taosOpenFile("/etc/os-release", TD_FILE_READ | TD_FILE_STREAM);
  if (pFile == NULL) return false;

  while ((size = taosGetLineFile(pFile, &line)) != -1) {
    line[size - 1] = '\0';
    if (strncmp(line, "PRETTY_NAME", 11) == 0) {
      const char *p = strchr(line, '=') + 1;
      if (*p == '"') {
        p++;
        line[size - 2] = 0;
      }
      tstrncpy(releaseName, p, maxLen);
      code = 0;
      break;
    }
  }

  if (line != NULL) taosMemoryFree(line);
  taosCloseFile(&pFile);
  return code;
#endif
}

int32_t taosGetCpuInfo(char *cpuModel, int32_t maxLen, float *numOfCores) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
#elif defined(_TD_DARWIN_64)
  char   *line = NULL;
  size_t  size = 0;
  int32_t done = 0;
  int32_t code = -1;

  TdFilePtr pFile = taosOpenFile("/proc/cpuinfo", TD_FILE_READ | TD_FILE_STREAM);
  if (pFile == NULL) return false;

  while (done != 3 && (size = taosGetLineFile(pFile, &line)) != -1) {
    line[size - 1] = '\0';
    if (((done & 1) == 0) && strncmp(line, "model name", 10) == 0) {
      const char *v = strchr(line, ':') + 2;
      tstrncpy(cpuModel, v, maxLen);
      code = 0;
      done |= 1;
    } else if (((done & 2) == 0) && strncmp(line, "cpu cores", 9) == 0) {
      const char *v = strchr(line, ':') + 2;
      *numOfCores = atof(v);
      done |= 2;
    }
  }

  if (line != NULL) taosMemoryFree(line);
  taosCloseFile(&pFile);

  return code;
#else
  char   *line = NULL;
  size_t  size = 0;
  int32_t done = 0;
  int32_t code = -1;

  TdFilePtr pFile = taosOpenFile("/proc/cpuinfo", TD_FILE_READ | TD_FILE_STREAM);
  if (pFile == NULL) return false;

  while (done != 3 && (size = taosGetLineFile(pFile, &line)) != -1) {
    line[size - 1] = '\0';
    if (((done & 1) == 0) && strncmp(line, "model name", 10) == 0) {
      const char *v = strchr(line, ':') + 2;
      tstrncpy(cpuModel, v, maxLen);
      code = 0;
      done |= 1;
    } else if (((done & 2) == 0) && strncmp(line, "cpu cores", 9) == 0) {
      const char *v = strchr(line, ':') + 2;
      *numOfCores = atof(v);
      done |= 2;
    }
  }

  if (line != NULL) taosMemoryFree(line);
  taosCloseFile(&pFile);

  return code;
#endif
}

int32_t taosGetCpuCores(float *numOfCores) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  SYSTEM_INFO info;
  GetSystemInfo(&info);
  *numOfCores = info.dwNumberOfProcessors;
  return 0;
#elif defined(_TD_DARWIN_64)
  *numOfCores = sysconf(_SC_NPROCESSORS_ONLN);
  return 0;
#else
  *numOfCores = sysconf(_SC_NPROCESSORS_ONLN);
  return 0;
#endif
}

void taosGetCpuUsage(double *cpu_system, double *cpu_engine) {
  static int64_t lastSysUsed = 0;
  static int64_t lastSysTotal = 0;
  static int64_t lastProcTotal = 0;
  static int64_t curSysUsed = 0;
  static int64_t curSysTotal = 0;
  static int64_t curProcTotal = 0;

  *cpu_system = 0;
  *cpu_engine = 0;

  SysCpuInfo  sysCpu = {0};
  ProcCpuInfo procCpu = {0};
  if (taosGetSysCpuInfo(&sysCpu) == 0 && taosGetProcCpuInfo(&procCpu) == 0) {
    curSysUsed = sysCpu.user + sysCpu.nice + sysCpu.system;
    curSysTotal = curSysUsed + sysCpu.idle;
    curProcTotal = procCpu.utime + procCpu.stime + procCpu.cutime + procCpu.cstime;

    if (curSysTotal > lastSysTotal && curSysUsed >= lastSysUsed && curProcTotal >= lastProcTotal) {
      *cpu_engine = (curSysUsed - lastSysUsed) / (double)(curSysTotal - lastSysTotal) * 100;
      *cpu_system = (curProcTotal - lastProcTotal) / (double)(curSysTotal - lastSysTotal) * 100;
    }

    lastSysUsed = curSysUsed;
    lastSysTotal = curSysTotal;
    lastProcTotal = curProcTotal;
  }
}

int32_t taosGetTotalMemory(int64_t *totalKB) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  MEMORYSTATUSEX memsStat;
  memsStat.dwLength = sizeof(memsStat);
  if (!GlobalMemoryStatusEx(&memsStat)) {
    return -1;
  }

  *totalKB = memsStat.ullTotalPhys / 1024;
  return 0;
#elif defined(_TD_DARWIN_64)
  return 0;
#else
  *totalKB = (int64_t)(sysconf(_SC_PHYS_PAGES) * tsPageSizeKB);
  return 0;
#endif
}

int32_t taosGetProcMemory(int64_t *usedKB) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  unsigned bytes_used = 0;

#if defined(_WIN64) && defined(_MSC_VER)
  PROCESS_MEMORY_COUNTERS pmc;
  HANDLE                  cur_proc = GetCurrentProcess();

  if (GetProcessMemoryInfo(cur_proc, &pmc, sizeof(pmc))) {
    bytes_used = (unsigned)(pmc.WorkingSetSize + pmc.PagefileUsage);
  }
#endif

  *usedKB = bytes_used / 1024;
  return 0;
#elif defined(_TD_DARWIN_64)
  *usedKB = 0;
  return 0;
#else
  TdFilePtr pFile = taosOpenFile(tsProcMemFile, TD_FILE_READ | TD_FILE_STREAM);
  if (pFile == NULL) {
    // printf("open file:%s failed", tsProcMemFile);
    return -1;
  }

  ssize_t _bytes = 0;
  char   *line = NULL;
  while (!taosEOFFile(pFile)) {
    _bytes = taosGetLineFile(pFile, &line);
    if ((_bytes < 0) || (line == NULL)) {
      break;
    }
    if (strstr(line, "VmRSS:") != NULL) {
      break;
    }
  }

  if (line == NULL) {
    // printf("read file:%s failed", tsProcMemFile);
    taosCloseFile(&pFile);
    return -1;
  }

  char tmp[10];
  sscanf(line, "%s %" PRId64, tmp, usedKB);

  if (line != NULL) taosMemoryFreeClear(line);
  taosCloseFile(&pFile);
  return 0;
#endif
}

int32_t taosGetSysMemory(int64_t *usedKB) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  MEMORYSTATUSEX memsStat;
  memsStat.dwLength = sizeof(memsStat);
  if (!GlobalMemoryStatusEx(&memsStat)) {
    return -1;
  }

  int64_t nMemFree = memsStat.ullAvailPhys / 1024;
  int64_t nMemTotal = memsStat.ullTotalPhys / 1024.0;

  *usedKB = nMemTotal - nMemFree;
  return 0;
#elif defined(_TD_DARWIN_64)
  *usedKB = 0;
  return 0;
#else
  *usedKB = sysconf(_SC_AVPHYS_PAGES) * tsPageSizeKB;
  return 0;
#endif
}

int32_t taosGetDiskSize(char *dataDir, SDiskSize *diskSize) {
#if defined(WINDOWS)
  unsigned _int64 i64FreeBytesToCaller;
  unsigned _int64 i64TotalBytes;
  unsigned _int64 i64FreeBytes;

  BOOL fResult = GetDiskFreeSpaceExA(dataDir, (PULARGE_INTEGER)&i64FreeBytesToCaller, (PULARGE_INTEGER)&i64TotalBytes,
                                     (PULARGE_INTEGER)&i64FreeBytes);
  if (fResult) {
    diskSize->total = (int64_t)(i64TotalBytes);
    diskSize->avail = (int64_t)(i64FreeBytesToCaller);
    diskSize->used = (int64_t)(i64TotalBytes - i64FreeBytes);
    return 0;
  } else {
    // printf("failed to get disk size, dataDir:%s errno:%s", tsDataDir, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }
#elif defined(_TD_DARWIN_64)
  struct statvfs info;
  if (statvfs(dataDir, &info)) {
    // printf("failed to get disk size, dataDir:%s errno:%s", tsDataDir, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  } else {
    diskSize->total = info.f_blocks * info.f_frsize;
    diskSize->avail = info.f_bavail * info.f_frsize;
    diskSize->used = (info.f_blocks - info.f_bfree) * info.f_frsize;
    return 0;
  }
#else
  struct statvfs info;
  if (statvfs(dataDir, &info)) {
    return -1;
  } else {
    diskSize->total = info.f_blocks * info.f_frsize;
    diskSize->avail = info.f_bavail * info.f_frsize;
    diskSize->used = diskSize->total - diskSize->avail;
    return 0;
  }
#endif
}

int32_t taosGetProcIO(int64_t *rchars, int64_t *wchars, int64_t *read_bytes, int64_t *write_bytes) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  IO_COUNTERS io_counter;
  if (GetProcessIoCounters(GetCurrentProcess(), &io_counter)) {
    if (rchars) *rchars = io_counter.ReadTransferCount;
    if (wchars) *wchars = io_counter.WriteTransferCount;
    if (read_bytes) *read_bytes = 0;
    if (write_bytes) *write_bytes = 0;
    return 0;
  }
  return -1;
#elif defined(_TD_DARWIN_64)
  if (rchars) *rchars = 0;
  if (wchars) *wchars = 0;
  if (read_bytes) *read_bytes = 0;
  if (write_bytes) *write_bytes = 0;
  return 0;
#else
  TdFilePtr pFile = taosOpenFile(tsProcIOFile, TD_FILE_READ | TD_FILE_STREAM);
  if (pFile == NULL) return -1;

  ssize_t _bytes = 0;
  char   *line = NULL;
  char    tmp[24];
  int     readIndex = 0;

  while (!taosEOFFile(pFile)) {
    _bytes = taosGetLineFile(pFile, &line);
    if (_bytes < 10 || line == NULL) {
      break;
    }
    if (strstr(line, "rchar:") != NULL) {
      sscanf(line, "%s %" PRId64, tmp, rchars);
      readIndex++;
    } else if (strstr(line, "wchar:") != NULL) {
      sscanf(line, "%s %" PRId64, tmp, wchars);
      readIndex++;
    } else if (strstr(line, "read_bytes:") != NULL) {  // read_bytes
      sscanf(line, "%s %" PRId64, tmp, read_bytes);
      readIndex++;
    } else if (strstr(line, "write_bytes:") != NULL) {  // write_bytes
      sscanf(line, "%s %" PRId64, tmp, write_bytes);
      readIndex++;
    } else {
    }

    if (readIndex >= 4) break;
  }

  if (line != NULL) taosMemoryFreeClear(line);
  taosCloseFile(&pFile);

  if (readIndex < 4) {
    return -1;
  }

  return 0;
#endif
}

void taosGetProcIODelta(int64_t *rchars, int64_t *wchars, int64_t *read_bytes, int64_t *write_bytes) {
  static int64_t last_rchars = 0;
  static int64_t last_wchars = 0;
  static int64_t last_read_bytes = 0;
  static int64_t last_write_bytes = 0;
  static int64_t cur_rchars = 0;
  static int64_t cur_wchars = 0;
  static int64_t cur_read_bytes = 0;
  static int64_t cur_write_bytes = 0;
  if (taosGetProcIO(&cur_rchars, &cur_wchars, &cur_read_bytes, &cur_write_bytes) == 0) {
    *rchars = cur_rchars - last_rchars;
    *wchars = cur_wchars - last_wchars;
    *read_bytes = cur_read_bytes - last_read_bytes;
    *write_bytes = cur_write_bytes - last_write_bytes;
    last_rchars = cur_rchars;
    last_wchars = cur_wchars;
    last_read_bytes = cur_read_bytes;
    last_write_bytes = cur_write_bytes;
  } else {
    *rchars = 0;
    *wchars = 0;
    *read_bytes = 0;
    *write_bytes = 0;
  }
}

int32_t taosGetCardInfo(int64_t *receive_bytes, int64_t *transmit_bytes) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  *receive_bytes = 0;
  *transmit_bytes = 0;
  return 0;
#elif defined(_TD_DARWIN_64)
  *receive_bytes = 0;
  *transmit_bytes = 0;
  return 0;
#else
  TdFilePtr pFile = taosOpenFile(tsSysNetFile, TD_FILE_READ | TD_FILE_STREAM);
  if (pFile == NULL) return -1;

  ssize_t _bytes = 0;
  char   *line = NULL;

  while (!taosEOFFile(pFile)) {
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

    _bytes = taosGetLineFile(pFile, &line);
    if (_bytes < 0) {
      break;
    }

    line[_bytes - 1] = 0;

    if (strstr(line, "lo:") != NULL) {
      continue;
    }

    sscanf(line,
           "%s %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64
           " %" PRId64,
           nouse0, &o_rbytes, &rpackts, &nouse1, &nouse2, &nouse3, &nouse4, &nouse5, &nouse6, &o_tbytes, &tpackets);
    *receive_bytes = o_rbytes;
    *transmit_bytes = o_tbytes;
  }

  if (line != NULL) taosMemoryFreeClear(line);
  taosCloseFile(&pFile);

  return 0;
#endif
}

void taosGetCardInfoDelta(int64_t *receive_bytes, int64_t *transmit_bytes) {
  static int64_t last_receive_bytes = 0;
  static int64_t last_transmit_bytes = 0;
  static int64_t cur_receive_bytes = 0;
  static int64_t cur_transmit_bytes = 0;
  if (taosGetCardInfo(&cur_receive_bytes, &cur_transmit_bytes) == 0) {
    *receive_bytes = cur_receive_bytes - last_receive_bytes;
    *transmit_bytes = cur_transmit_bytes - last_transmit_bytes;
    last_receive_bytes = cur_receive_bytes;
    last_transmit_bytes = cur_transmit_bytes;
  } else {
    *receive_bytes = 0;
    *transmit_bytes = 0;
  }
}

void taosKillSystem() {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  printf("function taosKillSystem, exit!");
  exit(0);
#elif defined(_TD_DARWIN_64)
  printf("function taosKillSystem, exit!");
  exit(0);
#else
  // SIGINT
  printf("taosd will shut down soon");
  kill(tsProcId, 2);
#endif
}

int32_t taosGetSystemUUID(char *uid, int32_t uidlen) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  GUID guid;
  CoCreateGuid(&guid);

  sprintf(uid, "%08X-%04X-%04X-%02X%02X-%02X%02X%02X%02X%02X%02X", guid.Data1, guid.Data2, guid.Data3, guid.Data4[0],
          guid.Data4[1], guid.Data4[2], guid.Data4[3], guid.Data4[4], guid.Data4[5], guid.Data4[6], guid.Data4[7]);

  return 0;
#elif defined(_TD_DARWIN_64)
  uuid_t uuid = {0};
  uuid_generate(uuid);
  // it's caller's responsibility to make enough space for `uid`, that's 36-char + 1-null
  uuid_unparse_lower(uuid, uid);
  return 0;
#else
  int len = 0;

  // fd = open("/proc/sys/kernel/random/uuid", 0);
  TdFilePtr pFile = taosOpenFile("/proc/sys/kernel/random/uuid", TD_FILE_READ);
  if (pFile == NULL) {
    return -1;
  } else {
    len = taosReadFile(pFile, uid, uidlen);
    taosCloseFile(&pFile);
  }

  if (len >= 36) {
    uid[36] = 0;
    return 0;
  }

  return 0;
#endif
}

char *taosGetCmdlineByPID(int pid) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return "";
#elif defined(_TD_DARWIN_64)
  static char cmdline[1024];
  errno = 0;

  if (proc_pidpath(pid, cmdline, sizeof(cmdline)) <= 0) {
    fprintf(stderr, "PID is %d, %s", pid, strerror(errno));
    return strerror(errno);
  }

  return cmdline;
#else
  static char cmdline[1024];
  sprintf(cmdline, "/proc/%d/cmdline", pid);

  // int fd = open(cmdline, O_RDONLY);
  TdFilePtr pFile = taosOpenFile(cmdline, TD_FILE_READ);
  if (pFile != NULL) {
    int n = taosReadFile(pFile, cmdline, sizeof(cmdline) - 1);
    if (n < 0) n = 0;

    if (n > 0 && cmdline[n - 1] == '\n') --n;

    cmdline[n] = 0;

    taosCloseFile(&pFile);
  } else {
    cmdline[0] = 0;
  }

  return cmdline;
#endif
}

void taosSetCoreDump(bool enable) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  SetUnhandledExceptionFilter(&FlCrashDump);
#elif defined(_TD_DARWIN_64)
#else
  if (!enable) return;

  // 1. set ulimit -c unlimited
  struct rlimit rlim;
  struct rlimit rlim_new;
  if (getrlimit(RLIMIT_CORE, &rlim) == 0) {
#ifndef _ALPINE
    // printf("the old unlimited para: rlim_cur=%" PRIu64 ", rlim_max=%" PRIu64, rlim.rlim_cur, rlim.rlim_max);
#else
    // printf("the old unlimited para: rlim_cur=%llu, rlim_max=%llu", rlim.rlim_cur, rlim.rlim_max);
#endif
    rlim_new.rlim_cur = RLIM_INFINITY;
    rlim_new.rlim_max = RLIM_INFINITY;
    if (setrlimit(RLIMIT_CORE, &rlim_new) != 0) {
      // printf("set unlimited fail, error: %s", strerror(errno));
      rlim_new.rlim_cur = rlim.rlim_max;
      rlim_new.rlim_max = rlim.rlim_max;
      (void)setrlimit(RLIMIT_CORE, &rlim_new);
    }
  }

  if (getrlimit(RLIMIT_CORE, &rlim) == 0) {
#ifndef _ALPINE
    // printf("the new unlimited para: rlim_cur=%" PRIu64 ", rlim_max=%" PRIu64, rlim.rlim_cur, rlim.rlim_max);
#else
    // printf("the new unlimited para: rlim_cur=%llu, rlim_max=%llu", rlim.rlim_cur, rlim.rlim_max);
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
    // printf("_sysctl(kern_core_uses_pid) set fail: %s", strerror(errno));
  }

  // printf("The old core_uses_pid[%" PRIu64 "]: %d", old_len, old_usespid);

  old_usespid = 0;
  old_len = 0;
  memset(&args, 0, sizeof(struct __sysctl_args));
  args.name = name;
  args.nlen = sizeof(name) / sizeof(name[0]);
  args.oldval = &old_usespid;
  args.oldlenp = &old_len;

  old_len = sizeof(old_usespid);

  if (syscall(SYS__sysctl, &args) == -1) {
    // printf("_sysctl(kern_core_uses_pid) get fail: %s", strerror(errno));
  }

  // printf("The new core_uses_pid[%" PRIu64 "]: %d", old_len, old_usespid);
#endif
#endif
}

SysNameInfo taosGetSysNameInfo() {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
#elif defined(_TD_DARWIN_64)
  SysNameInfo info = {0};

  struct utsname uts;
  if (!uname(&uts)) {
    tstrncpy(info.sysname, uts.sysname, sizeof(info.sysname));
    tstrncpy(info.nodename, uts.nodename, sizeof(info.nodename));
    tstrncpy(info.release, uts.release, sizeof(info.release));
    tstrncpy(info.version, uts.version, sizeof(info.version));
    tstrncpy(info.machine, uts.machine, sizeof(info.machine));
  }

  return info;
#else
  SysNameInfo info = {0};

  struct utsname uts;
  if (!uname(&uts)) {
    tstrncpy(info.sysname, uts.sysname, sizeof(info.sysname));
    tstrncpy(info.nodename, uts.nodename, sizeof(info.nodename));
    tstrncpy(info.release, uts.release, sizeof(info.release));
    tstrncpy(info.version, uts.version, sizeof(info.version));
    tstrncpy(info.machine, uts.machine, sizeof(info.machine));
  }

  return info;
#endif
}
