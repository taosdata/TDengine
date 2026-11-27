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
 * The wrapper's own log implementation (replaces util/tlog.c's taosPrintLog and
 * jniDebugFlag for this library).
 *
 * Why it exists: the wrapper's own diagnostics (driver loading, missing entry
 * points, the JNI/TMQ connectors) need a log, but that path cannot depend on
 * libtaosnative.so -- the engine's logger needs the engine's config init, which a
 * WebSocket-only deployment does not have.  So this reads taos.cfg and writes the
 * file itself.
 *
 * Design (from review):
 *   - only `logDir` is read from taos.cfg (not debugFlag/numOfLogLines/...);
 *   - same directory as the engine's (the configured logDir, TD_LOG_DIR_PATH by
 *     default), same line head, and a file name that keeps the engine's shape:
 *     `<logDir>/taosloglite<idx>.<flag>` (flag = 0/1).  Collection rules that match
 *     `taoslog*` therefore need no change, yet the file is recognisably the
 *     wrapper's;
 *   - it must *not* share a base name with the engine: in the same process and
 *     directory the engine uses `taoslog<idx>.0/.1` too, and the same name would
 *     truncate/rename each other's files;
 *   - rotation by size (20 MB by default), alternating .0/.1, the replaced file is
 *     archived as `<base>.<flag>.<unix seconds>`; at most 3 archives and 7 days are
 *     kept, cleaned up at startup, so the disk usage has a bound;
 *   - one lock, flushed per line; when the file cannot be opened it falls back to
 *     stderr rather than dropping messages silently.
 *
 * Environment variables (debugging / unit test): TDENGINE_LITE_LOG_DIR / _MAX_MB /
 * _KEEP / _KEEP_DAYS
 */

#define _DEFAULT_SOURCE

#include <ctype.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#if defined(WINDOWS)
#include <direct.h>
#include <windows.h>
#else
#include <dirent.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#include "wrapperLite.h"

/* Same "edition flag" as the engine's tlog.c (C = community, E = enterprise) */
#ifdef TD_ENTERPRISE
#define LITE_LOG_EDITION_FLG ("E")
#else
#define LITE_LOG_EDITION_FLG ("C")
#endif

#define LITE_LOG_MAX_LINE   4096
/* File name: keeps the engine's prefix and shape (`taosloglite<idx>.<flag>`) so the
 * collection rules that match `taoslog*` need no change, while the file is still
 * recognisably the wrapper's.  It must never share a base name with the engine
 * (same process, same directory: they would rename/truncate each other). */
#define LITE_LOG_BASE_NAME  "taosloglite"
#define LITE_LOG_IDX_MAX    10                   /* how many idx values to probe */
#define LITE_LOG_ARCH_MAX   64                   /* archive scan limit */

/* Same default as the engine's tlog.c (FILE|WARN/INFO|ERROR/FATAL) */
int32_t jniDebugFlag = 131;

typedef struct SLiteLogName {
  char base[PATH_MAX]; /* <logDir>/taosloglite<idx> */
  char dir[PATH_MAX];  /* <logDir> */
} SLiteLogName;

typedef struct SLiteLogObj {
  pthread_mutex_t lock;
  bool            inited;
  bool            fileOk;
  FILE           *fp;
  char            path[PATH_MAX]; /* full path of the current file */
  SLiteLogName    name;
  int32_t         flag;           /* 0/1 */
  int64_t         written;
  int64_t         maxBytes;
  int32_t         keepFiles;
  int32_t         keepDays;
} SLiteLogObj;

static SLiteLogObj tsLiteLog = {.lock = PTHREAD_MUTEX_INITIALIZER};

/* ------------------------------------------------------------------ *
 * Small helpers
 * ------------------------------------------------------------------ */

static int64_t liteNowSec(void) { return (int64_t)time(NULL); }

static void liteLogHead(char *buf, int32_t len, const char *flags) {
  struct tm      tmNow;
  int32_t        usec = 0;
#if defined(WINDOWS)
  SYSTEMTIME st;
  GetLocalTime(&st);
  memset(&tmNow, 0, sizeof(tmNow));
  tmNow.tm_mon = st.wMonth - 1;
  tmNow.tm_mday = st.wDay;
  tmNow.tm_hour = st.wHour;
  tmNow.tm_min = st.wMinute;
  tmNow.tm_sec = st.wSecond;
  usec = st.wMilliseconds * 1000;
#else
  struct timeval tv;
  (void)gettimeofday(&tv, NULL);
  time_t sec = tv.tv_sec;
  (void)localtime_r(&sec, &tmNow);
  usec = (int32_t)tv.tv_usec;
#endif

  /* pthread_t is an integer on Linux/macOS but a struct with pthread-win32, so the
   * cast only holds on the former; Windows takes the engine's own answer
   * (osSemaphore.c: taosGetSelfPthreadId). */
#if defined(WINDOWS)
  int64_t tid = (int64_t)GetCurrentThreadId();
#else
  int64_t tid = (int64_t)(uintptr_t)pthread_self();
#endif
  snprintf(buf, (size_t)len, "%02d/%02d %02d:%02d:%02d.%06d %08" PRId64 " %s %s", tmNow.tm_mon + 1, tmNow.tm_mday,
           tmNow.tm_hour, tmNow.tm_min, tmNow.tm_sec, usec, tid, LITE_LOG_EDITION_FLG, flags);
}

/* Minimal directory expansion: ~ / $VAR / ${VAR} (and %VAR% on Windows) -- the
 * subset of taosExpandDir this needs */
static void liteExpandDir(const char *in, char *out, int32_t outLen) {
  char tmp[PATH_MAX] = {0};
  int32_t n = 0;

  if (in == NULL) {
    out[0] = 0;
    return;
  }

  for (const char *p = in; *p != 0 && n < (int32_t)sizeof(tmp) - 1;) {
    if (*p == '~' && n == 0) {
      const char *home = getenv("HOME");
#if defined(WINDOWS)
      if (home == NULL) home = getenv("USERPROFILE");
#endif
      if (home != NULL) {
        n += snprintf(tmp + n, sizeof(tmp) - (size_t)n, "%s", home);
        ++p;
        continue;
      }
    }
    if (*p == '$' || *p == '%') {
      char        close = (*p == '$') ? '}' : '%';
      char        name[128] = {0};
      const char *q = p + 1;
      bool        braced = false;
      if (*p == '$' && *q == '{') {
        braced = true;
        ++q;
      }
      int32_t k = 0;
      while (*q != 0 && k < (int32_t)sizeof(name) - 1) {
        if (braced) {
          if (*q == close) break;
        } else if (*q == '%' || *q == '/' || *q == '\\' || *q == ' ') {
          break;
        }
        name[k++] = *q++;
      }
      const char *val = (k > 0) ? getenv(name) : NULL;
      if (val != NULL) {
        n += snprintf(tmp + n, sizeof(tmp) - (size_t)n, "%s", val);
        p = (*q == close) ? q + 1 : q;
        continue;
      }
    }
    tmp[n++] = *p++;
  }
  tmp[sizeof(tmp) - 1] = 0;
  snprintf(out, (size_t)outLen, "%s", tmp);
}

/* taos.cfg tokenizer: skips blanks and `#` comments, honours `@..@` quoting (same as
 * the engine's paGetToken) */
static const char *liteNextToken(const char *s, char *out, int32_t outLen) {
  while (*s == ' ' || *s == '\t') ++s;
  if (*s == '#') return NULL;

  bool   quote = false;
  int32_t n = 0;
  if (*s == '@') {
    quote = true;
    ++s;
  }
  while (*s != 0) {
    if (quote && *s == '@') {
      ++s;
      break;
    }
    if (!quote && (*s == '#' || *s == ' ' || *s == '\t' || *s == '\r' || *s == '\n')) {
      break;
    }
    if (n < outLen - 1) {
      out[n++] = *s;
    }
    ++s;
  }
  out[n] = 0;
  while (*s == ' ' || *s == '\t') ++s;
  return s;
}

/* Case-insensitive key compare: MSVC has no strcasecmp (osDef.h maps it to _stricmp
 * in the engine, which this file cannot include). */
static bool liteKeyIs(const char *name, const char *key) {
#if defined(WINDOWS)
  return _stricmp(name, key) == 0;
#else
  return strcasecmp(name, key) == 0;
#endif
}

/* tdLiteConfigDir() holds the engine's canonical form, which quotes a path that
 * contains spaces (taos_options_imp: the engine parses the global with wordexp()).
 * That is a cfg string, not a directory name -- opening a file has to drop the
 * quotes again, or a directory like "/opt/taos cfg" is looked up as
 * "\"/opt/taos cfg\"". */
static void liteUnquotedConfigDir(char *out, size_t outLen) {
  const char *dir = tdLiteConfigDir();
  size_t      len = strlen(dir);
  if (len > 1 && (dir[0] == '"' || dir[0] == '\'') && dir[len - 1] == dir[0]) {
    ++dir;
    len -= 2;
  }
  if (len > outLen - 1) {
    len = outLen - 1;
  }
  (void)memcpy(out, dir, len);
  out[len] = 0;
}

/* Reads logDir, jniDebugFlag and charset from <configDir>/taos.cfg (the engine reads
 * the same keys via tglobal.c's cfgAddDir/cfgAddInt32/cfgAddCharset).  Returns 0 when
 * logDir was found; *pDebugFlag is only written when jniDebugFlag is present (range as
 * in the engine: 0..255); the charset is recorded so tdLiteCharset() reports the same
 * value the engine gets, see tdLiteSetCharset(). */
static int32_t liteReadCfg(char *logDirOut, int32_t outLen, int32_t *pDebugFlag) {
  char path[PATH_MAX] = {0};
  char cfgDir[PATH_MAX] = {0};
  liteUnquotedConfigDir(cfgDir, sizeof(cfgDir));
  snprintf(path, sizeof(path), "%s%s%s", cfgDir, TD_LITE_DIRSEP, "taos.cfg");

  FILE *fp = fopen(path, "r");
  if (fp == NULL) {
    return -1;
  }

  /* One line of taos.cfg is "key value": a value can be a path, so the buffer has to
   * hold more than PATH_MAX.  A line that does not fit is dropped as a whole instead
   * of being parsed in pieces -- its tail would otherwise look like a new key, and a
   * long logDir would silently resolve to a wrong directory. */
  char    line[PATH_MAX + 128];
  int32_t ret = -1;
  while (fgets(line, sizeof(line), fp) != NULL) {
    size_t n = strlen(line);
    if (n == sizeof(line) - 1 && line[n - 1] != '\n') {
      int32_t ch = 0;
      while ((ch = fgetc(fp)) != EOF && ch != '\n') {
      }
      continue;
    }

    char        name[256] = {0};
    char        value[PATH_MAX] = {0};
    const char *p = liteNextToken(line, name, sizeof(name));
    if (p == NULL || name[0] == 0) {
      continue;
    }
    (void)liteNextToken(p, value, sizeof(value));
    if (value[0] == 0) {
      continue;
    }
    if (liteKeyIs(name, "logDir")) {
      snprintf(logDirOut, (size_t)outLen, "%s", value);
      ret = 0;
    } else if (pDebugFlag != NULL && liteKeyIs(name, "jniDebugFlag")) {
      char   *end = NULL;
      int32_t v = (int32_t)strtol(value, &end, 10);
      /* Trailing junk ("131abc") is not a value the engine would accept either. */
      if (end != value && (*end == 0 || isspace((unsigned char)*end)) && v >= 0 && v <= 255) {
        *pDebugFlag = v;
      }
    } else if (liteKeyIs(name, "charset")) {
      tdLiteSetCharset(value);
    }
  }

  (void)fclose(fp);
  return ret;
}

/* ------------------------------------------------------------------ *
 * Directory / file helpers (POSIX + Windows)
 * ------------------------------------------------------------------ */

static bool litePathExists(const char *path) {
#if defined(WINDOWS)
  DWORD attrs = GetFileAttributesA(path);
  return attrs != INVALID_FILE_ATTRIBUTES;
#else
  struct stat st;
  return stat(path, &st) == 0;
#endif
}

/* mkdir -p (an existing directory is not an error) */
static void liteMakeDirs(const char *dir) {
  if (dir == NULL || dir[0] == 0) {
    return;
  }
  char tmp[PATH_MAX] = {0};
  snprintf(tmp, sizeof(tmp), "%s", dir);

  for (char *p = tmp + 1; *p != 0; ++p) {
    if (*p == '/' || *p == '\\') {
      char saved = *p;
      *p = 0;
#if defined(WINDOWS)
      (void)_mkdir(tmp);
#else
      (void)mkdir(tmp, 0755);
#endif
      *p = saved;
    }
  }
#if defined(WINDOWS)
  (void)_mkdir(tmp);
#else
  (void)mkdir(tmp, 0755);
#endif
}

static int64_t liteFileSize(const char *path) {
#if defined(WINDOWS)
  WIN32_FILE_ATTRIBUTE_DATA info;
  if (GetFileAttributesExA(path, GetFileExInfoStandard, &info) == 0) {
    return 0;
  }
  return ((int64_t)info.nFileSizeHigh << 32) | (int64_t)info.nFileSizeLow;
#else
  struct stat st;
  if (stat(path, &st) != 0) {
    return 0;
  }
  return (int64_t)st.st_size;
#endif
}

/* ------------------------------------------------------------------ *
 * Archive cleanup: drop <base>.<flag>.<unix seconds> beyond the kept count or age
 * ------------------------------------------------------------------ */

/* One archive file: <leaf>.<flag>.<unix seconds>.  The match must be strictly three
 * fields -- a live file <leaf>.<flag> has only two, and counting it as an archive
 * mixes its bogus "timestamp" (0 or 1) into the sort order, which then deletes the
 * real archives as surplus (caught by the unit test).  Fills flag/ts when true. */
static bool liteArchiveInfo(const char *name, const char *leaf, int32_t *flag, int64_t *ts) {
  size_t leafLen = strlen(leaf);
  if (strncmp(name, leaf, leafLen) != 0 || name[leafLen] != '.') {
    return false;
  }
  const char *p = name + leafLen + 1;
  if ((*p != '0' && *p != '1') || p[1] != '.') {
    return false;
  }
  int32_t f = (*p == '0') ? 0 : 1;
  p += 2;
  if (*p == 0) {
    return false;
  }
  for (const char *q = p; *q != 0; ++q) {
    if (*q < '0' || *q > '9') {
      return false;
    }
  }
  int64_t v = (int64_t)strtoll(p, NULL, 10);
  if (v <= 0) {
    return false;
  }

  *flag = f;
  *ts = v;
  return true;
}

static void litePruneArchives(const char *basePath, int32_t keep, int32_t keepDays) {
  const char *leaf = strrchr(basePath, TD_LITE_DIRSEP_CHAR);
  leaf = (leaf != NULL) ? leaf + 1 : basePath;

  /* Archives are counted per file: .0 and .1 can each have one in the same second
   * (identical timestamps), so the flag is tracked alongside and the victim name is
   * built as <base>.<flag>.<ts>. */
  int32_t flags[LITE_LOG_ARCH_MAX] = {0};
  int64_t stamps[LITE_LOG_ARCH_MAX] = {0};
  int32_t count = 0;
  int64_t now = liteNowSec();

#if defined(WINDOWS)
  char pattern[PATH_MAX] = {0};
  snprintf(pattern, sizeof(pattern), "%s*", basePath);
  WIN32_FIND_DATAA fd;
  HANDLE h = FindFirstFileA(pattern, &fd);
  if (h == INVALID_HANDLE_VALUE) {
    return;
  }
  do {
    int32_t flag = 0;
    int64_t ts = 0;
    if (liteArchiveInfo(fd.cFileName, leaf, &flag, &ts) && count < LITE_LOG_ARCH_MAX) {
      flags[count] = flag;
      stamps[count] = ts;
      ++count;
    }
  } while (FindNextFileA(h, &fd));
  FindClose(h);
#else
  char dirCopy[PATH_MAX] = {0};
  snprintf(dirCopy, sizeof(dirCopy), "%s", basePath);
  char *slash = strrchr(dirCopy, TD_LITE_DIRSEP_CHAR);
  if (slash != NULL) {
    *slash = 0;
  }

  DIR *dp = opendir((slash != NULL) ? dirCopy : ".");
  if (dp == NULL) {
    return;
  }
  struct dirent *de = NULL;
  while ((de = readdir(dp)) != NULL) {
    int32_t flag = 0;
    int64_t ts = 0;
    if (liteArchiveInfo(de->d_name, leaf, &flag, &ts) && count < LITE_LOG_ARCH_MAX) {
      flags[count] = flag;
      stamps[count] = ts;
      ++count;
    }
  }
  (void)closedir(dp);
#endif

  /* oldest first (insertion sort; the list is tiny); same second ordered by flag so
   * the result is deterministic */
  for (int32_t i = 1; i < count; ++i) {
    int32_t keyFlag = flags[i];
    int64_t keyTs = stamps[i];
    int32_t j = i - 1;
    while (j >= 0 && (stamps[j] > keyTs || (stamps[j] == keyTs && flags[j] > keyFlag))) {
      flags[j + 1] = flags[j];
      stamps[j + 1] = stamps[j];
      --j;
    }
    flags[j + 1] = keyFlag;
    stamps[j + 1] = keyTs;
  }

  /* delete what is too old (keepDays) or beyond the kept count (keep) */
  int32_t aliveFrom = count - keep;
  if (aliveFrom < 0) {
    aliveFrom = 0;
  }
  for (int32_t i = 0; i < count; ++i) {
    bool tooOld = (keepDays > 0) && (stamps[i] < now - (int64_t)keepDays * 86400);
    bool tooMany = (i < aliveFrom);
    if (!tooOld && !tooMany) {
      continue;
    }
    char victim[PATH_MAX] = {0};
    snprintf(victim, sizeof(victim), "%s.%d.%" PRId64, basePath, flags[i], stamps[i]);
    (void)remove(victim);
  }
}

/* ------------------------------------------------------------------ *
 * Initialisation / rotation
 * ------------------------------------------------------------------ */

/* Picks a base name no other process holds (same shape as the engine's
 * decideLogFileName); falls back to 0 */
static void litePickBaseName(SLiteLogName *name, const char *dir) {
  snprintf(name->dir, sizeof(name->dir), "%s", dir);

  for (int32_t i = 0; i < LITE_LOG_IDX_MAX; ++i) {
    char cand[PATH_MAX] = {0};
    snprintf(cand, sizeof(cand), "%s%s%s%d", dir, TD_LITE_DIRSEP, LITE_LOG_BASE_NAME, i);

    char f0[PATH_MAX] = {0};
    char f1[PATH_MAX] = {0};
    snprintf(f0, sizeof(f0), "%s.0", cand);
    snprintf(f1, sizeof(f1), "%s.1", cand);

    /* Close to the engine's semantics: free when neither file exists, otherwise try
     * the next index */
    if (!litePathExists(f0) && !litePathExists(f1)) {
      snprintf(name->base, sizeof(name->base), "%s", cand);
      return;
    }
  }

  /* All taken (should not happen): fall back to index 0, still usable (append) */
  snprintf(name->base, sizeof(name->base), "%s%s%s%d", dir, TD_LITE_DIRSEP, LITE_LOG_BASE_NAME, 0);
}

/* Opens <base>.<flag> in append mode and records how many bytes it already holds */
static bool liteOpenFileLocked(int32_t flag) {
  char path[PATH_MAX] = {0};
  snprintf(path, sizeof(path), "%s.%d", tsLiteLog.name.base, flag);

  FILE *fp = fopen(path, "a");
  if (fp == NULL) {
    tsLiteLog.fileOk = false;
    tsLiteLog.fp = NULL;
    return false;
  }

  tsLiteLog.fp = fp;
  tsLiteLog.flag = flag;
  tsLiteLog.written = liteFileSize(path);
  tsLiteLog.fileOk = true;
  snprintf(tsLiteLog.path, sizeof(tsLiteLog.path), "%s", path);
  return true;
}

/* Rotation: archive the other file, then continue on it */
static void liteRotateLocked(void) {
  if (tsLiteLog.fp != NULL) {
    fflush(tsLiteLog.fp);
    (void)fclose(tsLiteLog.fp);
    tsLiteLog.fp = NULL;
  }

  int32_t next = tsLiteLog.flag ^ 1;
  char    oldFile[PATH_MAX] = {0};
  snprintf(oldFile, sizeof(oldFile), "%s.%d", tsLiteLog.name.base, next);

  if (litePathExists(oldFile)) {
    int64_t ts = liteNowSec();
    char    arch[PATH_MAX] = {0};
    snprintf(arch, sizeof(arch), "%s.%" PRId64, oldFile, ts);
    (void)remove(arch);
    (void)rename(oldFile, arch);
    litePruneArchives(tsLiteLog.name.base, tsLiteLog.keepFiles, tsLiteLog.keepDays);
  }

  (void)liteOpenFileLocked(next);
}

static int64_t liteEnvInt64(const char *name, int64_t defVal) {
  const char *v = getenv(name);
  if (v == NULL || v[0] == 0) {
    return defVal;
  }
  int64_t parsed = (int64_t)strtoll(v, NULL, 10);
  return (parsed > 0) ? parsed : defVal;
}

/* Idempotent initialisation; it takes the lock itself, so callers must not hold it.
 *
 * `inited` is set before the file is opened on purpose: a directory that cannot be
 * written (permissions, a full disk) degrades the whole process to the stderr
 * fallback in liteWriteLocked() instead of retrying the open on every line, which
 * would be the slower and noisier failure. */
void tdLiteLogInit(void) {
  (void)pthread_mutex_lock(&tsLiteLog.lock);
  if (tsLiteLog.inited) {
    (void)pthread_mutex_unlock(&tsLiteLog.lock);
    return;
  }
  tsLiteLog.inited = true;
  tsLiteLog.maxBytes = liteEnvInt64("TDENGINE_LITE_LOG_MAX_MB", 20) * 1024 * 1024;
  tsLiteLog.keepFiles = (int32_t)liteEnvInt64("TDENGINE_LITE_LOG_KEEP", 3);
  tsLiteLog.keepDays = (int32_t)liteEnvInt64("TDENGINE_LITE_LOG_KEEP_DAYS", 7);

  /* Log directory: environment variable > taos.cfg's logDir > platform default.
   * jniDebugFlag from taos.cfg is read into the local copy along the way, otherwise
   * the wrapper's log level could not be controlled. */
  char    dir[PATH_MAX] = {0};
  int32_t cfgFlag = jniDebugFlag;
  const char *envDir = getenv("TDENGINE_LITE_LOG_DIR");
  if (envDir != NULL && envDir[0] != 0) {
    liteExpandDir(envDir, dir, sizeof(dir));
  } else {
    char cfgDir[PATH_MAX] = {0};
    if (liteReadCfg(cfgDir, sizeof(cfgDir), &cfgFlag) == 0) {
      liteExpandDir(cfgDir, dir, sizeof(dir));
    } else {
      snprintf(dir, sizeof(dir), "%s", TD_LITE_LOG_DIR_DEFAULT);
    }
  }
  jniDebugFlag = cfgFlag;

  liteMakeDirs(dir);
  litePickBaseName(&tsLiteLog.name, dir);

  /* Clear the archives once at startup (repeated starts, several pids) */
  litePruneArchives(tsLiteLog.name.base, tsLiteLog.keepFiles, tsLiteLog.keepDays);

  /* Continue on the newer of .0/.1 when both exist, otherwise open .0 */
  char f0[PATH_MAX] = {0};
  char f1[PATH_MAX] = {0};
  snprintf(f0, sizeof(f0), "%s.0", tsLiteLog.name.base);
  snprintf(f1, sizeof(f1), "%s.1", tsLiteLog.name.base);
  int32_t flag = 0;
  if (litePathExists(f0) && litePathExists(f1)) {
    flag = (liteFileSize(f0) >= liteFileSize(f1)) ? 0 : 1;
  } else if (!litePathExists(f0) && litePathExists(f1)) {
    flag = 1;
  }

  tsLiteLog.flag = flag;
  (void)liteOpenFileLocked(flag);
  (void)pthread_mutex_unlock(&tsLiteLog.lock);
}

/* ------------------------------------------------------------------ *
 * Public entry points
 * ------------------------------------------------------------------ */

static void liteWriteLocked(const char *flags, int32_t level, const char *msg) {
  char head[512] = {0};
  liteLogHead(head, sizeof(head), flags);

  /* byte count of the line, used for the rotation test */
  int64_t lineBytes = (int64_t)strlen(head) + (int64_t)strlen(msg) + 2;
  if (tsLiteLog.fileOk && tsLiteLog.fp != NULL && tsLiteLog.maxBytes > 0 &&
      tsLiteLog.written + lineBytes > tsLiteLog.maxBytes) {
    liteRotateLocked();
  }

  if (tsLiteLog.fileOk && tsLiteLog.fp != NULL) {
    (void)fprintf(tsLiteLog.fp, "%s%s\n", head, msg);
    fflush(tsLiteLog.fp);
    tsLiteLog.written += lineBytes;
    return;
  }

  /* Fall back to stderr: a file that cannot be created (permissions, disk) must not
   * make the log disappear silently */
  (void)fprintf(stderr, "%s%s\n", head, msg);
  fflush(stderr);
  (void)level;
}

void taosPrintLog(const char *flags, int32_t level, int32_t dflag, const char *format, ...) {
  if (!(dflag & (TD_LITE_DEBUG_FILE | TD_LITE_DEBUG_SCREEN))) {
    return;
  }

  char    msg[LITE_LOG_MAX_LINE] = {0};
  va_list ap;
  va_start(ap, format);
  (void)vsnprintf(msg, sizeof(msg), format, ap);
  va_end(ap);

  const char *f = (flags != NULL) ? flags : "";

  /* Engine's taosPrintLogImp(): the two bits are independent branches -- DEBUG_FILE
   * appends to the log file, DEBUG_SCREEN writes the same line to fd 1, and a line
   * can be both.  `level` only feeds the engine's line counters, which is why the
   * file-local helpers below (tdLiteLogMsg/tdLiteLogAppend) take it as well. */
  if (dflag & TD_LITE_DEBUG_SCREEN) {
    (void)fprintf(stdout, "%s%s\n", f, msg);
    fflush(stdout);
  }

  if (dflag & TD_LITE_DEBUG_FILE) {
    tdLiteLogInit(); /* idempotent and takes its own lock: this must run before the lock below */
    (void)pthread_mutex_lock(&tsLiteLog.lock);
    liteWriteLocked(f, level, msg);
    (void)pthread_mutex_unlock(&tsLiteLog.lock);
  }
}

void tdLiteLogAppend(int32_t level, const char *flags, const char *format, ...) {
  char    msg[LITE_LOG_MAX_LINE] = {0};
  va_list ap;
  va_start(ap, format);
  (void)vsnprintf(msg, sizeof(msg), format, ap);
  va_end(ap);

  tdLiteLogInit(); /* idempotent and takes its own lock: this must run before the lock below */
  (void)pthread_mutex_lock(&tsLiteLog.lock);
  liteWriteLocked((flags != NULL) ? flags : "", level, msg);
  (void)pthread_mutex_unlock(&tsLiteLog.lock);
}

void tdLiteLogMsg(int32_t level, const char *flags, const char *format, ...) {
  char    msg[LITE_LOG_MAX_LINE] = {0};
  va_list ap;
  va_start(ap, format);
  (void)vsnprintf(msg, sizeof(msg), format, ap);
  va_end(ap);

  /* keep the terminal output (tools and users read it), and write the log file too */
  (void)fprintf(stdout, "%s%s\n", (flags != NULL) ? flags : "", msg);
  fflush(stdout);

  tdLiteLogInit(); /* idempotent and takes its own lock: this must run before the lock below */
  (void)pthread_mutex_lock(&tsLiteLog.lock);
  liteWriteLocked((flags != NULL) ? flags : "", level, msg);
  (void)pthread_mutex_unlock(&tsLiteLog.lock);
}

int32_t tdLiteLogFilePath(char *out, size_t len) {
  if (out == NULL || len == 0) {
    return -1;
  }
  out[0] = 0;

  tdLiteLogInit();
  (void)pthread_mutex_lock(&tsLiteLog.lock);
  if (tsLiteLog.fileOk) {
    /* Copied while the lock is held: a rotation in another thread rewrites
     * tsLiteLog.path, and handing out a pointer to it would race. */
    (void)snprintf(out, len, "%s", tsLiteLog.path);
  }
  int32_t code = tsLiteLog.fileOk ? 0 : -1;
  (void)pthread_mutex_unlock(&tsLiteLog.lock);
  return code;
}

void tdLiteLogClose(void) {
  (void)pthread_mutex_lock(&tsLiteLog.lock);
  if (tsLiteLog.fp != NULL) {
    fflush(tsLiteLog.fp);
    (void)fclose(tsLiteLog.fp);
    tsLiteLog.fp = NULL;
    tsLiteLog.fileOk = false;
  }
  /* Closing is not the failure path above: it is taos_cleanup(), after which a new
   * taos_init() in the same process has to be able to log again (and to pick up a
   * configDir that changed in the meantime), so the next call re-initialises. */
  tsLiteLog.inited = false;
  (void)pthread_mutex_unlock(&tsLiteLog.lock);
}
