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
 * Self-contained helpers for libtaos.so (the driver dispatcher).
 *
 * libtaos.so used to take 27 engine helpers (logging, error strings, memory,
 * dlopen, paths, configDir/tsCharset/tDataTypes, ...) from libtaosnative.so over
 * DT_NEEDED.  That made the dependency a load-time one -- a WebSocket-only
 * deployment still had to ship libtaosnative.so, and eight of those symbols are
 * data relocations (GLOB_DAT) that must resolve before the process can start.
 *
 * The 27 symbols are handled in three groups:
 *
 *   1. Stateless leaves, implemented in wrapperLiteOs.c / wrapperLiteErr.c /
 *      wrapperLiteLog.c: atomics, dlopen, memory, sleep, paths, version string,
 *      logging, error strings.  They are built with -fvisibility=hidden and are
 *      not exported, so they cannot interpose the driver's same-named symbols.
 *      (Two copies of one data symbol split the engine state once already -- see
 *      the ai-proj/package-size notes.)
 *
 *   2. State (configDir / tsCharset): owned here and handed to the engine
 *      explicitly instead of being shared through a bound data symbol --
 *      configDir goes through the engine's own taos_options(CONFIGDIR) when the
 *      driver is loaded, and the charset is recorded where the wrapper sees the
 *      engine accept one.  Nothing in this library reads a data symbol out of the
 *      driver, which is what makes native, WebSocket and Windows behave the same
 *      (Windows exports no data symbols, and the WebSocket driver has no engine
 *      globals at all).
 *
 *   3. Memory: the taosMemory* / taosStrdup* macros are re-pointed at libc
 *      wrappers, which is what removes the five tmempool globals (gMemPoolHandle
 *      / threadPoolSession / threadPoolEnabled / taosMemPoolCalloc /
 *      taosMemPoolFree) from this library's imports.
 *
 * WARNING: this header may only include "safe" headers such as <taoserror.h> or
 * <taos.h>.  Never include <os.h> (nor its osTime.h / osFile.h / osMemory.h):
 * those #define libc names (time, fopen, malloc, ...) to *_TAOS_FORBID and would
 * break the libc calls in the local implementations.  The memory macro overrides
 * live in wrapperLiteMem.h, which only the wrapper's own TUs include.
 *
 * Every constant here (type widths, default directories, log level bits) is
 * cross-checked against the engine's own definitions by
 * source/client/test/liteConsistencyTest.cpp.
 */

#ifndef _TD_WRAPPER_TAOS_LITE_H_
#define _TD_WRAPPER_TAOS_LITE_H_

#include <limits.h>
#include <pthread.h> /* only for the taosThreadOnce declaration: TdThreadOnce == pthread_once_t (engine osThread.h:57/69) */
#include <stdbool.h>
#include <stdint.h>

#include <taoserror.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------------------------------------------------ *
 * Constants kept in sync with the engine (the unit test cross-checks them;
 * changing one here means changing the engine too)
 * ------------------------------------------------------------------ */

#define TD_LITE_CHARSET_LEN 64 /* = osDef.h:TD_CHARSET_LEN */

/* Path buffer limit = the PATH_MAX the engine really uses.  osDef.h pins it to 256
 * on Windows only; Linux/macOS take libc's (4096/1024).  That is why the engine's
 * configDir[] is 4096 bytes here and 256 on Windows -- the local copy and every
 * write bound must use the same value. */
#if defined(WINDOWS)
#define TD_LITE_PATH_MAX 256 /* = osDef.h:PATH_MAX */
#else
#define TD_LITE_PATH_MAX PATH_MAX /* = libc's, which is what the engine uses too */
#endif

/* MSVC's <limits.h> has no PATH_MAX (it is POSIX; the engine pins 256 through
 * osDef.h on Windows).  The files that include this header without the engine
 * headers still use PATH_MAX for their path buffers, so define the same value.
 * Engine headers are included first elsewhere, which makes this a no-op there. */
#if defined(WINDOWS) && !defined(PATH_MAX)
#define PATH_MAX TD_LITE_PATH_MAX
#endif

/* realpath(3) needs a destination of at least libc's PATH_MAX: the glibc FORTIFY
 * wrapper (__realpath_chk, on by default from -O2) calls __chk_fail() and aborts
 * the process for anything smaller.  A caller's maxlen can be smaller than this,
 * the internal buffer cannot. */
#if defined(WINDOWS)
#define TD_LITE_REALPATH_MAX 260 /* MAX_PATH */
#else
#define TD_LITE_REALPATH_MAX 4096
#endif

/* Thread local (= osDef.h's threadlocal) */
#if defined(WINDOWS)
#define TD_LITE_TLS __declspec(thread)
#else
#define TD_LITE_TLS __thread
#endif

#if defined(WINDOWS)
#define TD_LITE_DIRSEP           "\\"
#define TD_LITE_DIRSEP_CHAR      '\\'
#define TD_LITE_CFG_DIR_DEFAULT  "C:\\TDengine\\cfg\\" /* = osDir.h:TD_CFG_DIR_PATH */
#define TD_LITE_LOG_DIR_DEFAULT  "C:\\TDengine\\log\\" /* = osDir.h:TD_LOG_DIR_PATH */
#elif defined(_TD_DARWIN_64)
#define TD_LITE_DIRSEP           "/"
#define TD_LITE_DIRSEP_CHAR      '/'
#define TD_LITE_CFG_DIR_DEFAULT  "/etc/taos/"     /* = osDir.h:TD_CFG_DIR_PATH */
#define TD_LITE_LOG_DIR_DEFAULT  "/var/log/taos/" /* = osDir.h:TD_LOG_DIR_PATH */
#else
#define TD_LITE_DIRSEP           "/"
#define TD_LITE_DIRSEP_CHAR      '/'
#define TD_LITE_CFG_DIR_DEFAULT  "/etc/taos/"     /* = osDir.h:TD_CFG_DIR_PATH */
#define TD_LITE_LOG_DIR_DEFAULT  "/var/log/taos/" /* = osDir.h:TD_LOG_DIR_PATH */
#endif

/* Driver file names: the dispatcher dlopens exactly one of them, chosen by
 * TDENGINE_DRIVER, and the isolation test looks for the same names in the loaded
 * module list, so both take them from here. */
#if defined(WINDOWS)
#define TD_LITE_DRIVER_NATIVE_FILE "taosnative.dll"
#define TD_LITE_DRIVER_WS_FILE     "taosws.dll"
#elif defined(_TD_DARWIN_64)
#define TD_LITE_DRIVER_NATIVE_FILE "libtaosnative.dylib"
#define TD_LITE_DRIVER_WS_FILE     "libtaosws.dylib"
#else
#define TD_LITE_DRIVER_NATIVE_FILE "libtaosnative.so"
#define TD_LITE_DRIVER_WS_FILE     "libtaosws.so"
#endif

/* Log filter / level bits (= tlog.h's ELogLevel) */
#define TD_LITE_DEBUG_ERROR  1
#define TD_LITE_DEBUG_WARN   2
#define TD_LITE_DEBUG_DEBUG  4
#define TD_LITE_DEBUG_TRACE  8
#define TD_LITE_DEBUG_DUMP   16
#define TD_LITE_DEBUG_SCREEN 64
#define TD_LITE_DEBUG_FILE   128

/* ------------------------------------------------------------------ *
 * configDir / charset: state the dispatcher owns and hands to the engine
 * explicitly.  Nothing is bound out of the driver (Windows exports no data symbols
 * and the WebSocket driver has none), so native, WebSocket and Windows all behave
 * the same way.
 * ------------------------------------------------------------------ */

/* The effective configDir (TD_LITE_PATH_MAX capacity, writable -- the JNI initImp
 * writes into it).  Never NULL.  This is the canonical form described below, which is
 * what the engine wants and what the wrapper itself parses for <configDir>/taos.cfg. */
char *tdLiteConfigDir(void);
/* The same directory as the caller wrote it (empty when nobody set one, never NULL).
 * This is the form to hand to a driver: the WebSocket driver stores the option string
 * verbatim as a path, so the engine's '"' quoting would become part of the directory
 * name there.  wrapperDriver.c passes this one when the driver is loaded. */
const char *tdLiteConfigDirGiven(void);
/* Write the effective configDir in the engine's canonical form: exactly what the
 * engine's taos_options_imp() stores for TSDB_OPTION_CONFIGDIR, i.e. on non-Windows
 * a path of more than one character that is not already quoted gets '"' on both
 * ends, because the engine parses this value with wordexp() (taosExpandDir()
 * through cfgAddDir()) and an unquoted space would cut the path short there.
 *
 * Every writer of the global goes through this, and wrapperDriver.c forwards
 * tdLiteConfigDirGiven() to the driver through taos_options() as soon as the driver is
 * loaded, so a directory set before taos_init() -- by the JNI layer or through
 * taos_options() -- reaches the engine in the same form one set through the engine's
 * own option path does.  Returns 0; NULL is rejected with -1 and terrno ==
 * TSDB_CODE_INVALID_PARA (a missing value is what the engine's taos_options()
 * refuses), and -1 when the path cannot be quoted within TD_LITE_PATH_MAX (nothing is
 * written then, as in the engine). */
int32_t tdLiteSetConfigDir(const char *dir);
/* The charset the engine works with (read only, never empty): what was recorded --
 * see tdLiteSetCharset() -- or, until the engine has one, the default the engine's
 * own osLocale.c would pick.  The JNI layer reports it through getTsCharset() as a
 * charset name. */
const char *tdLiteCharset(void);
/* Record the charset: called after the engine's taos_options(TSDB_OPTION_CHARSET)
 * succeeded and when taos.cfg is read, i.e. exactly where the engine's tsCharset
 * gets its value.  NULL is ignored, "" clears the record. */
void tdLiteSetCharset(const char *charset);
/* Per-type width for the JNI layer; same values as the engine's tDataTypes[].bytes,
 * cross-checked by the unit test. */
int32_t tdLiteDataTypeBytes(int32_t type);

/* ------------------------------------------------------------------ *
 * Stateless leaves: local implementations, signatures copied verbatim from the
 * engine headers (a changed engine signature makes the wrapper fail to compile)
 * ------------------------------------------------------------------ */
int32_t atomic_load_32(int32_t volatile *ptr);                                                 /* osAtomic.h */
void    atomic_store_32(int32_t volatile *ptr, int32_t val);                                   /* osAtomic.h */
int32_t atomic_val_compare_exchange_32(int32_t volatile *ptr, int32_t oldval, int32_t newval); /* osAtomic.h */
void    taosMsleep(int32_t ms);                                                                /* osSleep.h */
int32_t taosThreadOnce(pthread_once_t *onceControl, void (*initRoutine)(void));                 /* osThread.h (TdThreadOnce == pthread_once_t) */
void   *taosLoadDll(const char *fileName);                                                     /* osSystem.h */
void   *taosLoadDllFunc(void *handle, const char *funcName);                                   /* osSystem.h */
void    taosCloseDll(void *handle);                                                            /* osSystem.h */
int32_t taosAppPath(char *path, int32_t maxLen);                                               /* osDir.h */
int32_t taosRealPath(char *dirname, char *realPath, int32_t maxlen);                           /* osDir.h */
bool    taosCheckExistFile(const char *pathname);                                              /* osFile.h */

/* Version string: TD_LITE_VERSION, built from the same CMake variables as the
 * engine's version.c.in, so it equals the engine's td_version by construction. */
const char *tdLiteVersionString(void);

/* JNI log level (cfg item jniDebugFlag, default 131); the local implementation
 * keeps its own copy, see wrapperLiteLog.c. */
extern int32_t jniDebugFlag; /* tlog.h */

/* ------------------------------------------------------------------ *
 * Logging: independent implementation, see wrapperLiteLog.c
 * ------------------------------------------------------------------ */

/* Same name and signature as the engine's, and the same dflag filtering: DEBUG_FILE
 * writes the log file, DEBUG_SCREEN writes the same line to fd 1 (tlog.c's
 * taosPrintLogImp() runs both branches independently; `level` only feeds its line
 * counters).  The format attribute is the engine's too, so every JNI/tool call site
 * built on this header keeps its printf checking. */
void taosPrintLog(const char *flags, int32_t level, int32_t dflag, const char *format, ...)
#ifdef __GNUC__
    __attribute__((format(printf, 4, 5)))
#endif
    ;
/* Initialise logging (idempotent, takes the lock internally): reads logDir and
 * jniDebugFlag from taos.cfg and opens the file.  The wrapper's taos_init() path
 * calls it explicitly so jniDebugFlag is effective before the first JNI message. */
void tdLiteLogInit(void);
/* Wrapper diagnostics: the terminal and the log file both get it. */
void tdLiteLogMsg(int32_t level, const char *flags, const char *format, ...)
#ifdef __GNUC__
    __attribute__((format(printf, 3, 4)))
#endif
    ;
/* Log file only, no terminal output (wrapperHint.c has printed its hint already). */
void tdLiteLogAppend(int32_t level, const char *flags, const char *format, ...)
#ifdef __GNUC__
    __attribute__((format(printf, 3, 4)))
#endif
    ;
/* Flush and close the log file (called from taos_cleanup). */
void tdLiteLogClose(void);
/* Copy the absolute path of the current log file into `out` (unit test /
 * diagnostics).  The copy is taken under the log lock, so a concurrent rotation cannot
 * hand the caller a half-rewritten buffer -- which is why this is not a
 * `const char *` accessor.  Returns 0, or -1 when no file is open (then `out` is
 * left empty). */
int32_t tdLiteLogFilePath(char *out, size_t len);

/* ------------------------------------------------------------------ *
 * Memory (libc wrappers; the macro redirection lives in wrapperLiteMem.h)
 * ------------------------------------------------------------------ */

void *tdLiteMemMalloc(int64_t size);
void *tdLiteMemCalloc(int64_t num, int64_t size);
void *tdLiteMemRealloc(void *ptr, int64_t size);
void  tdLiteMemFree(void *ptr);
char *tdLiteStrdup(const char *ptr);
char *tdLiteStrndup(const char *ptr, int32_t size);

#ifdef __cplusplus
}
#endif

#endif  // _TD_WRAPPER_TAOS_LITE_H_
