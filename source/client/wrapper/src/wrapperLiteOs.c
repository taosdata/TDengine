/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

/*
 * The stateless leaves of libtaos.so's self-contained implementation, plus the
 * binding of the stateful symbols to the driver.
 *
 * Everything replaced here is a stateless helper of the engine's os/util (atomics,
 * dlopen, memory, sleep, paths), matched one by one against the engine's semantics
 * (the reference implementation is named on each function).  Built with
 * -fvisibility=hidden: not exported, so the driver's same-named symbols are never
 * interposed.
 *
 * configDir / tsCharset are state the dispatcher owns and hands over explicitly:
 * nothing here looks a data symbol up in the driver (Windows exports none, and the
 * WebSocket driver has none), which is what keeps the behaviour identical on every
 * platform and every driver.  configDir is written by the JNI layer and by
 * taos_options() before the driver exists, so wrapperDriver.c pushes this copy into
 * the engine with the engine's own option once the driver is loaded; the charset is
 * only ever read back (getTsCharset()), so the copy records what the engine
 * accepted (taos_options(TSDB_OPTION_CHARSET)) or what taos.cfg configured.
 */

#define _DEFAULT_SOURCE

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(WINDOWS)
#include <intrin.h> /* _InterlockedCompareExchange (the engine gets it through os.h) */
#include <io.h>     /* _access */
#include <windows.h>
#else
#include <dlfcn.h>
#include <unistd.h>
#if defined(_TD_DARWIN_64)
#include <mach-o/dyld.h>
#endif
#endif

#include <taos.h> /* only for public constants such as TSDB_DATA_TYPE_MAX */

#include "wrapperLite.h"

/* ===================================================================== *
 * 1. configDir / charset: state the dispatcher owns
 * ===================================================================== */

/* Passed in by CMake (see wrapper/CMakeLists.txt) and built from the same
 * variables as the engine's version.c.in; the unit test fails if it is missing. */
#ifndef TD_LITE_VERSION
#define TD_LITE_VERSION "unknown"
#endif

static char           gLiteConfigDir[PATH_MAX] = {0};      /* canonical form, for the engine */
static char           gLiteConfigDirGiven[PATH_MAX] = {0}; /* as the caller wrote it, for the driver */
static char           gLiteCharset[TD_LITE_CHARSET_LEN] = {0};
static pthread_once_t gLiteStateOnce = PTHREAD_ONCE_INIT;

/* Per-type widths, entry by entry the same values as the engine's tDataTypes[].bytes
 * (same order as TSDB_DATA_TYPE_*).  Only the JNI uses them, to size the buffer of a
 * fixed-length column; the unit test compares them with the engine's table. */
static const int32_t gLiteTypeBytes[TSDB_DATA_TYPE_MAX] = {
    2,     /* TSDB_DATA_TYPE_NULL */
    1,     /* TSDB_DATA_TYPE_BOOL        = CHAR_BYTES */
    1,     /* TSDB_DATA_TYPE_TINYINT     = CHAR_BYTES */
    2,     /* TSDB_DATA_TYPE_SMALLINT    = SHORT_BYTES */
    4,     /* TSDB_DATA_TYPE_INT         = INT_BYTES */
    8,     /* TSDB_DATA_TYPE_BIGINT      = LONG_BYTES */
    4,     /* TSDB_DATA_TYPE_FLOAT       = FLOAT_BYTES */
    8,     /* TSDB_DATA_TYPE_DOUBLE      = DOUBLE_BYTES */
    1,     /* TSDB_DATA_TYPE_VARCHAR     (variable length; the engine's table says 1 too) */
    8,     /* TSDB_DATA_TYPE_TIMESTAMP   = LONG_BYTES */
    1,     /* TSDB_DATA_TYPE_NCHAR       */
    1,     /* TSDB_DATA_TYPE_UTINYINT    */
    2,     /* TSDB_DATA_TYPE_USMALLINT   */
    4,     /* TSDB_DATA_TYPE_UINT        */
    8,     /* TSDB_DATA_TYPE_UBIGINT     */
    16384, /* TSDB_DATA_TYPE_JSON        = TSDB_MAX_JSON_TAG_LEN */
    1,     /* TSDB_DATA_TYPE_VARBINARY   */
    16,    /* TSDB_DATA_TYPE_DECIMAL     = DECIMAL128_BYTES */
    1,     /* TSDB_DATA_TYPE_BLOB        */
    1,     /* TSDB_DATA_TYPE_MEDIUMBLOB  */
    1,     /* TSDB_DATA_TYPE_GEOMETRY    */
    8,     /* TSDB_DATA_TYPE_DECIMAL64   = DECIMAL64_BYTES */
};

/* configDir default, same as os/osEnv.c's osDefaultInit().  Nothing else is derived
 * here: the charset is not guessed from the process locale (that call is a
 * process-wide side effect, and the value would not follow a configured charset),
 * it is recorded where the dispatcher sees the engine accept one. */
static void liteInitLocalState(void) {
  if (gLiteConfigDir[0] == 0) {
    snprintf(gLiteConfigDir, sizeof(gLiteConfigDir), "%s", TD_LITE_CFG_DIR_DEFAULT);
  }
}

char *tdLiteConfigDir(void) {
  (void)pthread_once(&gLiteStateOnce, liteInitLocalState);
  return gLiteConfigDir;
}

const char *tdLiteConfigDirGiven(void) {
  (void)pthread_once(&gLiteStateOnce, liteInitLocalState);
  return gLiteConfigDirGiven;
}

/* The canonical form the engine stores configDir in (clientEnv.c's
 * taos_options_imp): on non-Windows a path of more than one character that does not
 * already start with a quote gets '"' on both ends.  It has to be that form rather
 * than the raw path, because the engine reads this global through cfgAddDir() ->
 * taosExpandDir(), which is wordexp(3): an unquoted "/opt/taos cfg" is two words
 * there, the second one is dropped, and the configured directory is never seen.
 *
 * Returns `dir` for the inputs the engine leaves alone, `quoted` otherwise, and NULL
 * when the quoted result would not fit -- the engine reports "Too long path" and
 * keeps the previous value in that case. */
static const char *liteQuoteConfigDir(const char *dir, char *quoted, int32_t quotedLen) {
#if defined(WINDOWS)
  /* The engine's quoting block is #ifndef WINDOWS: a Windows path is stored as it
   * arrives. */
  (void)quoted;
  (void)quotedLen;
  return dir;
#else
  size_t len = strlen(dir);
  if (len <= 1 || dir[0] == '"' || dir[0] == '\'') {
    return dir;
  }
  if (len + 2 >= (size_t)quotedLen) {
    return NULL;
  }
  quoted[0] = '"';
  (void)memcpy(quoted + 1, dir, len);
  quoted[len + 1] = '"';
  quoted[len + 2] = 0;
  return quoted;
#endif
}

int32_t tdLiteSetConfigDir(const char *dir) {
  if (dir == NULL) {
    /* A missing value is an error, not "no change": the engine rejects it with
     * TSDB_CODE_INVALID_PARA, and answering -1 + terrno here keeps this function and
     * taos_options(CONFIGDIR, NULL) saying the same thing. */
    terrno = TSDB_CODE_INVALID_PARA;
    return -1;
  }

  char        quoted[PATH_MAX] = {0};
  const char *value = liteQuoteConfigDir(dir, quoted, (int32_t)sizeof(quoted));
  if (value == NULL) {
    /* Goes to the log that is open now, i.e. the old logDir: the rejected directory is
     * exactly the one whose log could not be written. */
    tdLiteLogMsg(1, "WRP ERROR ", "configDir is too long (%d bytes at most): %s", PATH_MAX - 2, dir);
    return -1;
  }

  /* tstrncpy() is an engine header macro and this TU includes no engine headers, so
   * this is what it does: bounded copy plus an explicit terminator.  Two forms are
   * kept: the canonical one (quoted for the engine's wordexp()) and the caller's own
   * string, which is what a driver that takes the option as a plain path needs -- the
   * WebSocket driver stores the string verbatim, so quoted input would become part of
   * the directory name there. */
  strncpy(tdLiteConfigDir(), value, PATH_MAX - 1);
  tdLiteConfigDir()[PATH_MAX - 1] = 0;

  strncpy(gLiteConfigDirGiven, dir, PATH_MAX - 1);
  gLiteConfigDirGiven[PATH_MAX - 1] = 0;
  return 0;
}

/* The charset the engine starts with when nothing is configured, i.e. what
 * osLocale.c's taosGetSystemLocale() answers: "UTF-8" on Windows, and on POSIX the
 * code page part of LC_CTYPE ("zh_CN.GB18030" -> "GB18030", "utf8" -> "UTF-8"),
 * "UTF-8" when the locale has none.  Read from the environment instead of asking
 * setlocale(): that call changes the process locale of everything in the process, and
 * answering a question must not do that (it is exactly the side effect this file no
 * longer needs). */
static const char *liteSystemCharset(void) {
#if defined(WINDOWS)
  return "UTF-8";
#else
  static char cs[TD_LITE_CHARSET_LEN] = {0};
  if (cs[0] != 0) {
    return cs;
  }
  const char *env = getenv("LC_ALL");
  if (env == NULL || env[0] == 0) {
    env = getenv("LC_CTYPE");
  }
  if (env == NULL || env[0] == 0) {
    env = getenv("LANG");
  }
  const char *dot = (env != NULL) ? strrchr(env, '.') : NULL;
  const char *value = (dot != NULL && dot[1] != 0) ? dot + 1 : "UTF-8";
  if (strcasecmp(value, "utf8") == 0) {
    value = "UTF-8";
  } else if (strcmp(value, "936") == 0) {
    value = "CP936";
  }
  snprintf(cs, sizeof(cs), "%s", value);
  return cs;
#endif
}

/* Recorded value first, the engine's default second: the JNI layer passes this on as
 * a charset name, so it must never be empty. */
const char *tdLiteCharset(void) {
  return (gLiteCharset[0] != 0) ? gLiteCharset : liteSystemCharset();
}

void tdLiteSetCharset(const char *charset) {
  if (charset == NULL) {
    return;
  }
  snprintf(gLiteCharset, sizeof(gLiteCharset), "%s", charset);
}

const char *tdLiteVersionString(void) { return TD_LITE_VERSION; }

int32_t tdLiteDataTypeBytes(int32_t type) {
  if (type < 0 || type >= TSDB_DATA_TYPE_MAX) {
    return 0;
  }
  return gLiteTypeBytes[type];
}

/* Nothing to bind or unbind: the state above belongs to this library alone, and the
 * engine receives configDir through its own taos_options() when the driver is
 * loaded (see wrapperDriver.c). */

/* ===================================================================== *
 * 2. Atomics (engine: os/src/osAtomic.c)
 * ===================================================================== */

int32_t atomic_load_32(int32_t volatile *ptr) {
#if defined(WINDOWS)
  return (*(int32_t volatile *)ptr);
#else
  return __atomic_load_n(ptr, __ATOMIC_SEQ_CST);
#endif
}

void atomic_store_32(int32_t volatile *ptr, int32_t val) {
#if defined(WINDOWS)
  *(int32_t volatile *)ptr = val;
#else
  __atomic_store_n(ptr, val, __ATOMIC_SEQ_CST);
#endif
}

int32_t atomic_val_compare_exchange_32(int32_t volatile *ptr, int32_t oldval, int32_t newval) {
#if defined(WINDOWS)
  return _InterlockedCompareExchange((int32_t volatile *)ptr, (int32_t)(newval), (int32_t)(oldval));
#else
  return __sync_val_compare_and_swap(ptr, oldval, newval);
#endif
}

/* ===================================================================== *
 * 3. Time (engine: os/src/osSleep.c)
 * ===================================================================== */

void taosMsleep(int32_t ms) {
  if (ms < 0) {
    return;
  }
#if defined(WINDOWS)
  Sleep((DWORD)ms);
#else
  usleep((useconds_t)ms * 1000);
#endif
}

/* Same as the engine's osThread.c:588.  TdThreadOnce is pthread_once_t (Windows uses
 * pthread-win32), so declaring it with pthread_once_t matches osThread.h. */
int32_t taosThreadOnce(pthread_once_t *onceControl, void (*initRoutine)(void)) {
  int32_t code = pthread_once(onceControl, initRoutine);
  if (code) {
    return (terrno = TAOS_SYSTEM_ERROR(code));
  }
  return code;
}

/* ===================================================================== *
 * 4. Dynamic libraries (engine: os/src/osSystem.c)
 * ===================================================================== */

void *taosLoadDll(const char *fileName) {
#if defined(WINDOWS)
  void *handle = (void *)LoadLibraryA(fileName);
#else
  void *handle = dlopen(fileName, RTLD_LAZY);
#endif

  if (handle == NULL) {
    if (errno != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
    } else {
      terrno = TSDB_CODE_DLL_NOT_LOAD;
    }
  }

  return handle;
}

void *taosLoadDllFunc(void *handle, const char *funcName) {
  if (handle == NULL) {
    return NULL;
  }

#if defined(WINDOWS)
  void *fptr = (void *)GetProcAddress((HMODULE)handle, funcName);
#else
  void *fptr = dlsym(handle, funcName);
#endif

  if (fptr == NULL) {
    terrno = TSDB_CODE_DLL_FUNC_NOT_LOAD;
  }

  return fptr;
}

void taosCloseDll(void *handle) {
  if (handle == NULL) {
    return;
  }
#if defined(WINDOWS)
  (void)FreeLibrary((HMODULE)handle);
#else
  if (dlclose(handle) != 0 && errno != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
  }
#endif
}

/* ===================================================================== *
 * 5. Paths (engine: os/src/osDir.c -- taosDirName/taosAppPath/taosRealPath)
 * ===================================================================== */

/* Strip the last component, keeping the directory -- same as the engine */
static char *liteDirName(char *name) {
  if (name == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }
#if defined(WINDOWS)
  char   drive[MAX_PATH] = {0};
  char   dir[MAX_PATH] = {0};
  size_t len;
  _splitpath(name, drive, dir, NULL, NULL);
  len = strlen(drive) + strlen(dir);
  if (len > 0) {
    if (name[len - 1] == '/' || name[len - 1] == '\\') {
      name[len - 1] = 0;
    } else {
      name[len] = 0;
    }
  } else {
    name[0] = 0;
  }
#else
  char *end = strrchr(name, '/');
  if (end != NULL) {
    *end = '\0';
  } else {
    name[0] = '\0';
  }
#endif
  return name;
}

int32_t taosAppPath(char *path, int32_t maxLen) {
  if (path == NULL || maxLen <= 1) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  int32_t ret = 0;
#if defined(WINDOWS)
  ret = (int32_t)GetModuleFileNameA(NULL, path, (DWORD)(maxLen - 1));
#elif defined(_TD_DARWIN_64)
  uint32_t len = (uint32_t)(maxLen - 1);
  ret = _NSGetExecutablePath(path, &len);
#else
  ret = (int32_t)readlink("/proc/self/exe", path, (size_t)(maxLen - 1));
#endif

  if (ret >= 0) {
    path[maxLen - 1] = '\0';  /* readlink does not terminate; do it explicitly */
    ret = (liteDirName(path) == NULL) ? -1 : 0;
  }

  return ret;
}

int32_t taosRealPath(char *dirname, char *realPath, int32_t maxlen) {
  if (dirname == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  /* Must be libc's PATH_MAX: FORTIFY's __realpath_chk aborts the process when the
   * destination is smaller than 4096 (a caller's maxlen can be only 256). */
  char tmp[TD_LITE_REALPATH_MAX] = {0};
#if defined(WINDOWS)
  if (_fullpath(tmp, dirname, maxlen) != NULL) {
#else
  if (realpath(dirname, tmp) != NULL) {
#endif
    if ((int32_t)strlen(tmp) < maxlen) {
      if (realPath == NULL) {
        snprintf(dirname, maxlen, "%s", tmp);
      } else {
        snprintf(realPath, maxlen, "%s", tmp);
      }
      return 0;
    }
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  terrno = TAOS_SYSTEM_ERROR(errno);
  return terrno;
}

/* Engine: os/src/osFile.c -- taosCheckExistFile() is taosCheckAccessFile(...,
 * TD_FILE_ACCESS_EXIST_OK), i.e. "does the path exist"; osFile.h would rename it
 * to the wide variant on Windows, but TAOS_OS_WIDE_PATH is not defined by any
 * build in this repo, so the plain name is what the wrapper links against. */
bool taosCheckExistFile(const char *pathname) {
  if (pathname == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return false;
  }
#if defined(WINDOWS)
  /* MSVC has no F_OK: _access() with mode 0 is the "exists" test there. */
  return _access(pathname, 0) == 0;
#else
  return access(pathname, F_OK) == 0;
#endif
}

/* ===================================================================== *
 * 6. Memory (engine: os/src/osMemory.c; macro redirection in wrapperLiteMem.h)
 *
 * Line by line the engine's *non* USE_TD_MEMORY branch, including terrno on failure:
 * the engine's `taosMemoryMalloc(_size)` macro only takes the pool when
 * (threadPoolEnabled && threadPoolSession) hold, and threadPoolSession is NULL on an
 * ordinary application thread -- i.e. plain malloc/calloc/free, which is what this
 * is.  (USE_TD_MEMORY is not enabled by any build in this repo.)
 * ===================================================================== */

void *tdLiteMemMalloc(int64_t size) {
  void *p = malloc((size_t)size);
  if (NULL == p) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
  }
  return p;
}

void *tdLiteMemCalloc(int64_t num, int64_t size) {
  void *p = calloc((size_t)num, (size_t)size);
  if (NULL == p) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
  }
  return p;
}

void *tdLiteMemRealloc(void *ptr, int64_t size) {
  void *p = realloc(ptr, (size_t)size);
  if (size > 0 && NULL == p) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
  }
  return p;
}

void tdLiteMemFree(void *ptr) {
  if (NULL == ptr) {
    return;
  }
  free(ptr);
}

char *tdLiteStrdup(const char *ptr) {
  if (ptr == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }
#if defined(WINDOWS)
  return _strdup(ptr); /* the engine's tstrdup takes the same branch on Windows */
#else
  char *p = strdup(ptr);
  if (NULL == p) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
  }
  return p;
#endif
}

char *tdLiteStrndup(const char *ptr, int32_t size) {
  if (ptr == NULL) {
    return NULL;
  }
#if defined(WINDOWS)
  /* Windows has no strndup in the engine either: malloc and truncate by hand */
  size_t l = strlen(ptr);
  if (l > (size_t)size) {
    l = (size_t)size;
  }
  char *p = (char *)malloc(l + 1);
  if (p == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  memcpy(p, ptr, l);
  p[l] = '\0';
  return p;
#else
  char *p = strndup(ptr, (size_t)size);
  if (NULL == p) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
  }
  return p;
#endif
}
