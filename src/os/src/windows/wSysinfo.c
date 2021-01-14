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
#include "taosdef.h"
#include "tconfig.h"
#include "tglobal.h"
#include "ttimer.h"
#include "tulog.h"
#include "tutil.h"
#if (_WIN64)
#include <iphlpapi.h>
#include <mswsock.h>
#include <psapi.h>
#include <stdio.h>
#include <windows.h>
#include <ws2tcpip.h>
#pragma comment(lib, "Mswsock.lib ")
#endif

#pragma warning(push)
#pragma warning(disable:4091)
#include <DbgHelp.h>
#pragma warning(pop)

static void taosGetSystemTimezone() {
  // get and set default timezone
  SGlobalCfg *cfg_timezone = taosGetConfigOption("timezone");
  if (cfg_timezone && cfg_timezone->cfgStatus < TAOS_CFG_CSTATUS_DEFAULT) {
    char *tz = getenv("TZ");
    if (tz == NULL || strlen(tz) == 0) {
      strcpy(tsTimezone, "not configured");
    } else {
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

bool taosGetDisk() {
  const double    unit = 1024 * 1024 * 1024;
  BOOL            fResult;
  unsigned _int64 i64FreeBytesToCaller;
  unsigned _int64 i64TotalBytes;
  unsigned _int64 i64FreeBytes;
  char            dir[4] = {'C', ':', '\\', '\0'};
  int             drive_type;

  if (tscEmbedded) {
    drive_type = GetDriveTypeA(dir);
    if (drive_type == DRIVE_FIXED) {
      fResult = GetDiskFreeSpaceExA(dir, (PULARGE_INTEGER)&i64FreeBytesToCaller, (PULARGE_INTEGER)&i64TotalBytes,
                                    (PULARGE_INTEGER)&i64FreeBytes);
      if (fResult) {
        tsTotalDataDirGB = tsTotalLogDirGB = tsTotalTmpDirGB = (float)(i64TotalBytes / unit);
        tsAvailDataDirGB = tsAvailLogDirGB = tsAvailTmpDirectorySpace = (float)(i64FreeBytes / unit);
      }
    }
  }
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
  unsigned bytes_used = 0;
#if 0
#if defined(_WIN32) && defined(_MSC_VER)
   PROCESS_MEMORY_COUNTERS pmc;
   HANDLE                  cur_proc = GetCurrentProcess();

   if (GetProcessMemoryInfo(cur_proc, &pmc, sizeof(pmc))) {
     bytes_used = (unsigned)(pmc.WorkingSetSize + pmc.PagefileUsage);
   }
#endif
#endif

  *memoryUsedMB = (float)bytes_used / 1024 / 1024;

  return true;
}

bool taosGetSysMemory(float *memoryUsedMB) {
  MEMORYSTATUSEX memsStat;
  float          nMemFree;
  float          nMemTotal;

  memsStat.dwLength = sizeof(memsStat);
  if (!GlobalMemoryStatusEx(&memsStat)) {
    return false;
  }
  nMemFree = memsStat.ullAvailPhys / (1024.0f * 1024.0f);
  nMemTotal = memsStat.ullTotalPhys / (1024.0f * 1024.0f);
  *memoryUsedMB = nMemTotal - nMemFree;
  return true;
}

int taosSystem(const char *cmd) {
  uError("taosSystem not support");
  return -1;
}

int flock(int fd, int option) { return 0; }

int fsync(int filedes) { return 0; }

int sigaction(int sig, struct sigaction *d, void *p) { return 0; }

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
  sprintf(uid, "uid_not_implemented_yet");
  return true;
}