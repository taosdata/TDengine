/*
 * MIT License
 *
 * Copyright (c) 2022-2023 freemine <freemine@yeah.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "os_port.h"

#include <stdint.h>
#include <stdio.h>
#ifdef _WIN32
#include <psapi.h>
#include <windows.h>
#pragma comment(lib, "psapi.lib")
#endif

struct tm* localtime_r(const time_t *clock, struct tm *result)
{
  errno_t err = localtime_s(result, clock);
  if (err) {
    errno = err;
    return NULL;
  }

  return result;
}

struct tm* gmtime_r(const time_t *clock, struct tm *result)
{
  errno_t err = gmtime_s(result, clock);
  if (err) {
    errno = err;
    return NULL;
  }

  return result;
}

int gettimeofday(struct timeval *tp, void *tzp)
{
  static const uint64_t EPOCH = ((uint64_t) 116444736000000000ULL);
  LARGE_INTEGER li;
  FILETIME ft;
  GetSystemTimeAsFileTime(&ft);

  li.QuadPart   = ft.dwHighDateTime;
  li.QuadPart <<= 32;
  li.QuadPart  += ft.dwLowDateTime;
  li.QuadPart  -= EPOCH;

  tp->tv_sec    = (long)(li.QuadPart / 10000000);
  tp->tv_usec   = (li.QuadPart % 10000000) / 10;

  return 0;
}

char* strndup(const char *s, size_t n)
{
  size_t len = strlen(s);
  if (len < n) n = len;

  char *p = (char*)malloc(n + 1);
  if (!p) return NULL;

  memcpy(p, s, n);
  p[n] = '\0';

  return p;
}

char* tod_getenv(const char *name)
{
  return getenv(name);
}

int tod_setenv(const char *name, const char *value, int overwrite)
{
  if (!overwrite && getenv(name)) {
    return -1;
  }

  if (SetEnvironmentVariable(name, value) == 0) {
    errno = GetLastError();
    return -1;
  }

  return 0;
}

static BOOL CALLBACK InitHandleFunction(PINIT_ONCE InitOnce, PVOID Parameter, PVOID *lpContext)
{
  (void)InitOnce;
  void (*init_routine)(void) = (void (*)(void))Parameter;
  init_routine();
  *lpContext = NULL;
  return TRUE;
}

int pthread_once(pthread_once_t *once_control, void (*init_routine)(void))
{
  PVOID lpContext = NULL;

  if (InitOnceExecuteOnce(once_control, InitHandleFunction, (PVOID)init_routine, &lpContext)) {
    return 0;
  }

  return -1;
}

static char dl_err[1024] = {0};

void* dlopen(const char* path, int mode)
{
  dl_err[0] = '\0';
  if (mode != RTLD_NOW) {
    errno = EINVAL;
    snprintf(dl_err, sizeof(dl_err), "only `RTLD_NOW` is supported");
    return NULL;
  }
  HMODULE hDll = LoadLibrary(path);
  if (!hDll) {
    errno = GetLastError();
    snprintf(dl_err, sizeof(dl_err), "LoadLibrary(`%s`) failed", path);
    return NULL;
  }

  return hDll;
}

int dlclose(void* handle)
{
  BOOL ok;
  dl_err[0] = '\0';
  ok = FreeLibrary(handle);
  if (!ok) {
    errno = GetLastError();
    snprintf(dl_err, sizeof(dl_err), "FreeLibrary failed");
    return -1;
  }
  return 0;
}

static void* _dlsym_recursive(const char *symbol)
{
  HMODULE hMods[1024];
  HANDLE hProcess;
  DWORD cbNeeded;
  unsigned int i;

  hProcess = GetCurrentProcess();
  if (NULL == hProcess) {
    dl_err[0] = '\0';
    errno = GetLastError();
    snprintf(dl_err, sizeof(dl_err), "GetProcAddress(`%s`) failed:[%d]%s", symbol, errno, strerror(errno));
    return NULL;
  }

  if (EnumProcessModules(hProcess, hMods, sizeof(hMods), &cbNeeded)) {
    for (i = 0; i < (cbNeeded / sizeof(HMODULE)); i++) {
      FARPROC proc = GetProcAddress(hMods[i], symbol);
      if (proc) return proc;

      char szModName[MAX_PATH];
      szModName[0] = '\0';

      GetModuleFileNameEx(hProcess, hMods[i], szModName, sizeof(szModName) / sizeof(szModName[0]));

      dl_err[0] = '\0';
      errno = GetLastError();
      int e = errno;
      snprintf(dl_err, sizeof(dl_err), "GetProcAddress(`%s`) from `%s` failed:[%d]%s", symbol, szModName, e, strerror(e));
    }
  }

  return NULL;
}

void* dlsym(void *handle, const char *symbol)
{
  dl_err[0] = '\0';
  if (handle == RTLD_DEFAULT) {
    return _dlsym_recursive(symbol);
  }
  FARPROC proc = GetProcAddress(handle, symbol);
  if (!proc) {
    errno = GetLastError();
    int e = errno;
    snprintf(dl_err, sizeof(dl_err), "GetProcAddress(`%s`) failed:[%d]%s", symbol, e, strerror(e));
    return NULL;
  }

  return proc;
}

const char * dlerror(void)
{
  if (!dl_err[0]) return NULL;
  return dl_err;
}

char *tod_strerror_x(int errnum, char *buf, size_t buflen)
{
  strerror_s(buf, buflen, errnum);
  return buf;
}

