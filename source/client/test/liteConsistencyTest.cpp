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
 * Consistency test for libtaos.so's self-contained helpers
 * (source/client/wrapper/src/wrapperLite*.c).
 *
 * Those files re-implement engine helpers so that the dispatcher can run without
 * libtaosnative.so, which means every constant, table and log-file name in them
 * is a second source of truth.  This test is the guard against that duplication
 * drifting:
 *
 *   - constants and log-level bits are compared with the engine's own macros
 *     (osDef.h / osDir.h / tlog.h) from the same build;
 *   - the per-type width table is compared with tdef.h's *_BYTES macros, and --
 *     when the native driver built in this tree can be dlopen()ed -- with the
 *     engine's real tDataTypes[] table, entry by entry;
 *   - the log subsystem is exercised on the real file system: taos.cfg parsing
 *     (from a configDir that contains a space), file naming (taosloglite<idx>.<flag>),
 *     size rotation, archive naming and pruning of old archives.
 *
 * The three wrapperLite*.c files are compiled into this test binary instead of being
 * taken from libtaos.so: they are built with -fvisibility=hidden and are not
 * exported, and they define the same names as os/util (taosMemFree, taosPrintLog,
 * taosRealPath, ...), so linking the static engine archives next to them would be
 * a duplicate-symbol error.
 */

#include <gtest/gtest.h>

#include <atomic>
#include <thread>

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#if defined(WINDOWS)
#include <direct.h> /* _mkdir */
#include <io.h>     /* _findfirst/_findnext */
#else
#include <dirent.h>
#include <unistd.h>
#endif

#include "osDir.h"     /* TD_CFG_DIR_PATH, TD_LOG_DIR_PATH */
#include "ttypes.h"    /* tDataTypeDescriptor (the engine's table type) */
#include "taos.h"      /* TSDB_DATA_TYPE_* */
#include "tdef.h"      /* *_BYTES, TSDB_MAX_JSON_TAG_LEN */
#include "tlog.h"      /* ELogLevel, jniDebugFlag */
#include "wrapperLite.h"
#include "taosNativeApi.h" /* tdNativeApiTable: how the dispatcher reaches the driver's entry points */

/* The engine's osDef.h only defines PATH_MAX on Windows (256); on Linux/macOS both
 * the engine and this test take libc's value, which is why the engine's configDir[]
 * is 4096 bytes here and 256 on Windows.  Reading it after the engine headers gives
 * the value the engine really uses on each platform. */
static const int kEnginePathMax = PATH_MAX;

/* The engine headers mask the libc names they want the engine to wrap (see
 * osFile.h/osMemory.h/osTime.h).  This test drives the file system itself, so put
 * the real names back -- the declarations above were taken before the mask was
 * installed, which is what makes the calls below mean the libc functions.  Nothing
 * here relies on the wrappers. */
#undef open
#undef fopen
#undef access
#undef stat
#undef lstat
#undef fstat
#undef close
#undef fclose
#undef fsync
#undef getline
#undef mkdir
#undef opendir
#undef readdir
#undef closedir
#undef dirname
#undef basename
#undef malloc
#undef calloc
#undef realloc
#undef free
#undef strdup
#undef time
#undef gettimeofday
#undef localtime
#undef localtime_r
#undef mktime
#undef strftime
#undef strptime

#include <string>
#include <vector>

namespace {

const char* kLogBaseName = "taosloglite";

/* Path join with the platform separator; a separator already at the end of the left
 * side is not doubled. */
std::string joinPath(const std::string& left, const std::string& right) {
  if (left.empty() || right.empty()) {
    return left + right;
  }
  char last = left[left.size() - 1];
  if (last == '/' || last == '\\') {
    return left + right;
  }
  return left + TD_LITE_DIRSEP + right;
}

int liteMkdir(const std::string& path) {
  std::string p = path;
  while (!p.empty() && (p[p.size() - 1] == '/' || p[p.size() - 1] == '\\')) {
    p.erase(p.size() - 1);
  }
#if defined(WINDOWS)
  return _mkdir(p.c_str());
#else
  return mkdir(p.c_str(), 0755);
#endif
}

void setEnv(const char* name, const char* value) {
#if defined(WINDOWS)
  ASSERT_EQ(_putenv_s(name, value), 0);
#else
  ASSERT_EQ(setenv(name, value, 1), 0);
#endif
}

void unsetEnv(const char* name) {
#if defined(WINDOWS)
  (void)_putenv_s(name, "");
#else
  (void)unsetenv(name);
#endif
}

/* A unique, empty directory for one test.  The log test needs a real tree it can
 * create under; /tmp does not exist on Windows. */
std::string tempDir() {
#if defined(WINDOWS)
  static int32_t seq = 0;
  const char*    base = getenv("TEMP");
  if (base == nullptr || base[0] == '\0') {
    base = ".";
  }
  std::string dir =
      joinPath(base, "taosLiteUT_" + std::to_string((long long)time(nullptr)) + "_" + std::to_string(++seq));
  EXPECT_EQ(liteMkdir(dir), 0) << dir;
  return dir;
#else
  char        tmpl[] = "/tmp/taosLiteConsistencyXXXXXX";
  const char* dir = mkdtemp(tmpl);
  EXPECT_NE(dir, nullptr);
  return std::string(dir == nullptr ? "/tmp" : dir);
#endif
}

void writeFile(const std::string& path, const std::string& content) {
  FILE* fp = fopen(path.c_str(), "w");
  ASSERT_NE(fp, nullptr) << path;
  (void)fputs(content.c_str(), fp);
  (void)fclose(fp);
}

bool pathExists(const std::string& path) {
  struct stat st;
  return stat(path.c_str(), &st) == 0;
}

int64_t fileSize(const std::string& path) {
  struct stat st;
  return (stat(path.c_str(), &st) == 0) ? (int64_t)st.st_size : -1;
}

/* Number of lines of a file that hold `marker`, and how many of those lines hold it
 * twice.  One taosPrintLog() call writes exactly one marker, so a line with two of
 * them is a line that two threads interleaved. */
int32_t countMarkerLines(const std::string& path, const char* marker, int32_t* doubleHits) {
  *doubleHits = 0;
  FILE* fp = fopen(path.c_str(), "rb");
  if (fp == nullptr) {
    return -1;
  }
  std::string text;
  char        buf[4096];
  size_t      got = 0;
  while ((got = fread(buf, 1, sizeof(buf), fp)) > 0) {
    text.append(buf, got);
  }
  (void)fclose(fp);

  const std::string needle(marker);
  int32_t           lines = 0;
  size_t            pos = 0;
  while (pos < text.size()) {
    size_t end = text.find('\n', pos);
    if (end == std::string::npos) {
      end = text.size();
    }
    const std::string line = text.substr(pos, end - pos);
    const size_t      first = line.find(needle);
    if (first != std::string::npos) {
      lines++;
      if (line.find(needle, first + needle.size()) != std::string::npos) {
        (*doubleHits)++;
      }
    }
    pos = end + 1;
  }
  return lines;
}

/* An archive name: <base><idx>.<flag>.<unixts>, i.e. at least three dot separated
 * fields.  A live file (<base><idx>.<flag>) has only two. */
bool isArchiveName(const std::string& name) {
  if (name.rfind(std::string(kLogBaseName) + "0.", 0) != 0 &&
      name.rfind(std::string(kLogBaseName) + "1.", 0) != 0) {
    return false;
  }
  size_t lastDot = name.rfind('.');
  if (lastDot == std::string::npos) {
    return false;
  }
  size_t baseDot = name.find('.', strlen(kLogBaseName));
  return (baseDot != std::string::npos && baseDot != lastDot);
}

/* Every entry in an archive directory whose name starts with <base>.0. / <base>.1.
 * (the suffix is the rotation timestamp, so those are the prunable archives). */
std::vector<std::string> archivesOf(const std::string& dir) {
  std::vector<std::string> out;
#if defined(WINDOWS)
  struct _finddata_t fd;
  intptr_t           handle = _findfirst(joinPath(dir, "*").c_str(), &fd);
  if (handle == -1) {
    return out;
  }
  do {
    if (isArchiveName(fd.name)) {
      out.push_back(fd.name);
    }
  } while (_findnext(handle, &fd) == 0);
  (void)_findclose(handle);
#else
  DIR* d = opendir(dir.c_str());
  if (d == nullptr) {
    return out;
  }
  for (struct dirent* e = readdir(d); e != nullptr; e = readdir(d)) {
    if (isArchiveName(e->d_name)) {
      out.push_back(e->d_name);
    }
  }
  (void)closedir(d);
#endif
  return out;
}

}  // namespace

/* ------------------------------------------------------------------ *
 * Constants: the engine's macros are the reference
 * ------------------------------------------------------------------ */

TEST(taosLite, constantsMatchEngineDefinitions) {
  EXPECT_EQ(TD_LITE_CHARSET_LEN, TD_CHARSET_LEN);
  EXPECT_STREQ(TD_LITE_DIRSEP, TD_DIRSEP);
  EXPECT_EQ(TD_LITE_DIRSEP_CHAR, TD_DIRSEP_CHAR);
  EXPECT_STREQ(TD_LITE_CFG_DIR_DEFAULT, TD_CFG_DIR_PATH);
  EXPECT_STREQ(TD_LITE_LOG_DIR_DEFAULT, TD_LOG_DIR_PATH);

  /* osDef.h only pins PATH_MAX on Windows; on Linux/macOS the engine takes libc's
   * value (both 4096) -- this catches a hardcoded 256 on the lite side. */
  EXPECT_EQ(TD_LITE_PATH_MAX, kEnginePathMax);

  /* tlog.h:ELogLevel -- taosPrintLog() filters on these bits, so they have to be
   * the same ones the engine's logging macros test. */
  EXPECT_EQ(TD_LITE_DEBUG_ERROR, (int32_t)DEBUG_ERROR);
  EXPECT_EQ(TD_LITE_DEBUG_WARN, (int32_t)DEBUG_WARN);
  EXPECT_EQ(TD_LITE_DEBUG_DEBUG, (int32_t)DEBUG_DEBUG);
  EXPECT_EQ(TD_LITE_DEBUG_TRACE, (int32_t)DEBUG_TRACE);
  EXPECT_EQ(TD_LITE_DEBUG_DUMP, (int32_t)DEBUG_DUMP);
  EXPECT_EQ(TD_LITE_DEBUG_SCREEN, (int32_t)DEBUG_SCREEN);
  EXPECT_EQ(TD_LITE_DEBUG_FILE, (int32_t)DEBUG_FILE);
}

TEST(taosLite, versionStringIsWired) {
  /* The local fallback must never be the "unknown" placeholder: that define comes
   * from wrapper/CMakeLists.txt (TD_LITE_VERSION) and silently going missing would
   * otherwise only show up as a bogus taos_get_client_info() answer. */
  const char* v = tdLiteVersionString();
  ASSERT_NE(v, nullptr);
  EXPECT_STRNE(v, "unknown");
  EXPECT_GT(strlen(v), 0u);
  /* Looks like a version: at least three dot-separated numeric fields. */
  int dots = 0;
  for (const char* p = v; *p != '\0'; ++p) {
    if (*p == '.') {
      ++dots;
    }
  }
  EXPECT_GE(dots, 3) << v;
}

/* ------------------------------------------------------------------ *
 * Type widths: tdef.h macros, and the engine's own table when reachable
 * ------------------------------------------------------------------ */

TEST(taosLite, dataTypeBytesMatchEngineMacros) {
  EXPECT_EQ(tdLiteDataTypeBytes(TSDB_DATA_TYPE_NULL), 2);

  EXPECT_EQ(tdLiteDataTypeBytes(TSDB_DATA_TYPE_BOOL), (int32_t)CHAR_BYTES);
  EXPECT_EQ(tdLiteDataTypeBytes(TSDB_DATA_TYPE_TINYINT), (int32_t)CHAR_BYTES);
  EXPECT_EQ(tdLiteDataTypeBytes(TSDB_DATA_TYPE_SMALLINT), (int32_t)SHORT_BYTES);
  EXPECT_EQ(tdLiteDataTypeBytes(TSDB_DATA_TYPE_INT), (int32_t)INT_BYTES);
  EXPECT_EQ(tdLiteDataTypeBytes(TSDB_DATA_TYPE_BIGINT), (int32_t)LONG_BYTES);
  EXPECT_EQ(tdLiteDataTypeBytes(TSDB_DATA_TYPE_FLOAT), (int32_t)FLOAT_BYTES);
  EXPECT_EQ(tdLiteDataTypeBytes(TSDB_DATA_TYPE_DOUBLE), (int32_t)DOUBLE_BYTES);
  EXPECT_EQ(tdLiteDataTypeBytes(TSDB_DATA_TYPE_TIMESTAMP), (int32_t)LONG_BYTES);

  EXPECT_EQ(tdLiteDataTypeBytes(TSDB_DATA_TYPE_UTINYINT), (int32_t)CHAR_BYTES);
  EXPECT_EQ(tdLiteDataTypeBytes(TSDB_DATA_TYPE_USMALLINT), (int32_t)SHORT_BYTES);
  EXPECT_EQ(tdLiteDataTypeBytes(TSDB_DATA_TYPE_UINT), (int32_t)INT_BYTES);
  EXPECT_EQ(tdLiteDataTypeBytes(TSDB_DATA_TYPE_UBIGINT), (int32_t)LONG_BYTES);

  EXPECT_EQ(tdLiteDataTypeBytes(TSDB_DATA_TYPE_JSON), TSDB_MAX_JSON_TAG_LEN);
  EXPECT_EQ(tdLiteDataTypeBytes(TSDB_DATA_TYPE_DECIMAL), DECIMAL128_BYTES);
  EXPECT_EQ(tdLiteDataTypeBytes(TSDB_DATA_TYPE_DECIMAL64), DECIMAL64_BYTES);

  /* Variable length types carry 1 in the engine table; only the JNI's fixed-width
   * branch reads it, but the value is still part of the contract. */
  EXPECT_EQ(tdLiteDataTypeBytes(TSDB_DATA_TYPE_VARCHAR), 1);
  EXPECT_EQ(tdLiteDataTypeBytes(TSDB_DATA_TYPE_NCHAR), 1);
  EXPECT_EQ(tdLiteDataTypeBytes(TSDB_DATA_TYPE_VARBINARY), 1);
  EXPECT_EQ(tdLiteDataTypeBytes(TSDB_DATA_TYPE_BLOB), 1);
  EXPECT_EQ(tdLiteDataTypeBytes(TSDB_DATA_TYPE_MEDIUMBLOB), 1);
  EXPECT_EQ(tdLiteDataTypeBytes(TSDB_DATA_TYPE_GEOMETRY), 1);

  /* Out of range answers 0 instead of reading past the table (the JNI passes a
   * type byte straight from Java). */
  EXPECT_EQ(tdLiteDataTypeBytes(-1), 0);
  EXPECT_EQ(tdLiteDataTypeBytes(TSDB_DATA_TYPE_MAX), 0);
  EXPECT_EQ(tdLiteDataTypeBytes(4096), 0);
}

/* Entry-by-entry comparison with the engine's tDataTypes[] when this tree's driver
 * can be loaded.  The table is not in a header, and the test must not link the
 * engine (duplicate symbols), so dlsym() it: libtaosnative.so exports it on
 * Linux/macOS.  Windows exports no data symbols and its driver path is passed as a
 * bare name there (see CMakeLists.txt); a driver that cannot be loaded (missing
 * third-party libs) is an environment problem, not a product one, so every one of
 * those cases is reported and skipped rather than failed. */
TEST(taosLite, dataTypeBytesMatchEngineTable) {
  const char* driverPath = TD_LITE_NATIVE_LIB_PATH;
  void* handle = taosLoadDll(driverPath);
  if (handle == nullptr) {
    GTEST_SKIP() << "cannot load " << driverPath << " (terrno=" << terrno << ")";
  }

  const tDataTypeDescriptor* table =
      (const tDataTypeDescriptor*)taosLoadDllFunc(handle, "tDataTypes");
  if (table == nullptr) {
    taosCloseDll(handle);
    GTEST_SKIP() << "driver does not export tDataTypes (Windows?)";
  }

  for (int32_t type = 0; type < TSDB_DATA_TYPE_MAX; ++type) {
    EXPECT_EQ(tdLiteDataTypeBytes(type), table[type].bytes)
        << "TSDB_DATA_TYPE_" << type << ": local table disagrees with the engine's tDataTypes[]";
  }

  taosCloseDll(handle);
}

/* ------------------------------------------------------------------ *
 * Log subsystem (stateful singleton: keep the scenarios in one test)
 * ------------------------------------------------------------------ */

TEST(taosLite, logFileNamingRotationAndCfg) {
  const std::string root = tempDir();
  /* A directory with a space in it: the wrapper stores configDir in the engine's
   * canonical form (quoted, see tdLiteSetConfigDir) and has to drop those quotes
   * again before opening <configDir>/taos.cfg. */
  const std::string cfgDir = joinPath(root, "cfg with space") + TD_LITE_DIRSEP;
  const std::string logDir = joinPath(root, "logs");

  /* taos.cfg: the wrapper reads logDir and jniDebugFlag, nothing else.  The last line
   * is a long one whose tail would parse as `logDir /tmp/wrong` if the parser split
   * it at the 1024-byte boundary, i.e. the log directory below is the regression
   * check for that. */
  ASSERT_EQ(liteMkdir(cfgDir), 0);
  writeFile(cfgDir + "taos.cfg",
            "# comment line\n"
            "  logDir   " + logDir + "   # trailing comment\n"
            "jniDebugFlag 135\n"
            "someOtherOption 42\n" +
                std::string(1022, 'x') + " logDir " + joinPath(root, "wrong") + "\n");
  ASSERT_EQ(strlen(cfgDir.c_str()) < (size_t)TD_LITE_PATH_MAX, true);

  /* tdLiteConfigDir() is where the wrapper looks for taos.cfg (and where the JNI
   * layer writes the config dir it was given), so this is also the path a consumer
   * takes.  No driver is bound here, so it is the local buffer. */
  ASSERT_EQ(tdLiteSetConfigDir(cfgDir.c_str()), 0);

  /* Keep the environment out of the way so the cfg path is the one taken, and make
   * rotation cheap: the smallest observable limit is 1 MB. */
  unsetEnv("TDENGINE_LITE_LOG_DIR");
  setEnv("TDENGINE_LITE_LOG_MAX_MB", "1");
  setEnv("TDENGINE_LITE_LOG_KEEP", "1");
  setEnv("TDENGINE_LITE_LOG_KEEP_DAYS", "7");

  tdLiteLogInit();

  /* logDir came from taos.cfg (spaces and the trailing comment are stripped), and
   * the file is named like the engine's, only with the lite marker in it. */
  char path[TD_LITE_PATH_MAX] = {0};
  ASSERT_EQ(tdLiteLogFilePath(path, sizeof(path)), 0);
  const std::string first = path;
  EXPECT_EQ(first, joinPath(logDir, std::string(kLogBaseName) + "0.0"));
  EXPECT_TRUE(pathExists(first));

  /* jniDebugFlag from taos.cfg: without it the wrapper's own logging could not be
   * turned on in a WebSocket-only deployment.  135 = FILE | WARN/INFO | ERROR. */
  EXPECT_EQ(jniDebugFlag, 135);
  EXPECT_EQ(jniDebugFlag & TD_LITE_DEBUG_FILE, TD_LITE_DEBUG_FILE);

  /* taosPrintLog() must honour the same dflag bits the engine's macros test: a
   * call whose bits are not set writes nothing. */
  const int64_t before = fileSize(first);
  taosPrintLog("JNI TEST  ", TD_LITE_DEBUG_TRACE, 0, "must not be written");
  EXPECT_EQ(fileSize(first), before);

  /* Write until the first rotation shows up.  The byte boundaries depend on the
   * message length and the timestamp prefix, the invariants below do not: they are
   * what operators and log collection rely on. */
  std::string filler(1200, 'x');
  size_t archivesAfterFirstRotation = 0;
  for (int i = 0; i < 4000 && archivesAfterFirstRotation == 0; ++i) {
    taosPrintLog("JNI TEST  ", TD_LITE_DEBUG_ERROR, jniDebugFlag, "line %d %s", i, filler.c_str());
    archivesAfterFirstRotation = archivesOf(logDir).size();
  }

  EXPECT_TRUE(pathExists(joinPath(logDir, std::string(kLogBaseName) + "0.0")));
  EXPECT_TRUE(pathExists(joinPath(logDir, std::string(kLogBaseName) + "0.1"))) << "rotation must have happened";
  /* The archive appears when the *other* file is rotated into it, so the very first
   * rotation has nothing to archive yet. */
  EXPECT_LE(archivesAfterFirstRotation, 1u);

  /* Keep writing: rotations alternate .0/.1 and pruning must hold the archive count
   * at TDENGINE_LITE_LOG_KEEP. */
  for (int i = 0; i < 6000; ++i) {
    taosPrintLog("JNI TEST  ", TD_LITE_DEBUG_ERROR, jniDebugFlag, "round2 %d %s", i, filler.c_str());
  }

  std::vector<std::string> archives = archivesOf(logDir);
  ASSERT_EQ(archives.size(), 1u) << "TDENGINE_LITE_LOG_KEEP=1 keeps a single archive";

  /* <base><idx>.<flag>.<unixts> -- the shape the engine's collection rules expect. */
  const std::string& a = archives[0];
  const size_t       baseLen = strlen(kLogBaseName);
  ASSERT_TRUE(a.rfind(std::string(kLogBaseName), 0) == 0u) << a;
  ASSERT_GT(a.size(), baseLen + 4) << a;
  EXPECT_TRUE(isdigit((unsigned char)a[baseLen])) << a;
  EXPECT_EQ(a[baseLen + 1], '.') << a;
  EXPECT_TRUE(isdigit((unsigned char)a[baseLen + 2])) << a;
  EXPECT_EQ(a[baseLen + 3], '.') << a;
  EXPECT_EQ(a.substr(baseLen + 4).find_first_not_of("0123456789"), std::string::npos) << a;
  EXPECT_GT(a.substr(baseLen + 4).size(), 4u) << "the suffix is a unix timestamp: " << a;

  /* The file being written alternates between .0 and .1 and stays around the limit,
   * i.e. neither side grows without bound. */
  char currentPath[TD_LITE_PATH_MAX] = {0};
  ASSERT_EQ(tdLiteLogFilePath(currentPath, sizeof(currentPath)), 0);
  const std::string cur = currentPath;
  EXPECT_TRUE(cur == joinPath(logDir, std::string(kLogBaseName) + "0.0") ||
              cur == joinPath(logDir, std::string(kLogBaseName) + "0.1"))
      << cur;
  EXPECT_LT(fileSize(cur), 2 * 1024 * 1024) << "the active file must rotate near the limit";

  /* Multi-threaded burst.  The engine's logger formats the line before taking the
   * per-file lock and then writes it with a single fprintf, so calls from different
   * threads must not interleave and none may be lost.  The volume is small on
   * purpose: TDENGINE_LITE_LOG_KEEP is 1 in this test, so a second rotation inside
   * the burst could prune a file that is still being counted. */
  const int32_t kMtThreads = 8;
  const int32_t kMtLines = 50;
  {
    std::atomic<int32_t>     staged{0};
    std::vector<std::thread> threads;
    for (int32_t t = 0; t < kMtThreads; ++t) {
      threads.emplace_back([t, kMtThreads, kMtLines, &staged] {
        staged.fetch_add(1);
        while (staged.load() < kMtThreads) {
        }
        for (int32_t i = 0; i < kMtLines; ++i) {
          taosPrintLog("MT TEST   ", TD_LITE_DEBUG_ERROR, jniDebugFlag, "MT-MARK t=%d i=%d", (int)t, (int)i);
        }
      });
    }
    for (std::thread& th : threads) {
      th.join();
    }
  }

  int32_t     mtLines = 0;
  int32_t     mtTorn = 0;
  char        livePath[TD_LITE_PATH_MAX] = {0};
  ASSERT_EQ(tdLiteLogFilePath(livePath, sizeof(livePath)), 0);
  int32_t seen = countMarkerLines(std::string(livePath), "MT-MARK", &mtTorn);
  ASSERT_GE(seen, 0) << livePath;
  mtLines += seen;
  for (const std::string& name : archivesOf(logDir)) {
    int32_t tornHere = 0;
    seen = countMarkerLines(joinPath(logDir, name), "MT-MARK", &tornHere);
    if (seen > 0) {
      mtLines += seen;
    }
    mtTorn += tornHere;
  }
  EXPECT_EQ(mtLines, kMtThreads * kMtLines) << "no line may be lost when threads log at once";
  EXPECT_EQ(mtTorn, 0) << "two markers on one line means the write was not atomic";

  tdLiteLogClose();
}

/* ------------------------------------------------------------------ *
 * configDir: one canonical form, whichever writer got there first
 * ------------------------------------------------------------------ */

/* The engine stores this global in one form (clientEnv.c's taos_options_imp): on
 * non-Windows the path is wrapped in '"', because the engine parses it with
 * wordexp() (taosExpandDir() through cfgAddDir()) and an unquoted space would cut
 * the path short there.  The wrapper has to write the same form, or a directory set
 * before taos_init() reaches the engine in a different form than one set afterwards.
 */
TEST(taosLite, configDirUsesEngineCanonicalForm) {
  /* A plain path is quoted too: the value is a cfg string, not a directory name. */
  ASSERT_EQ(tdLiteSetConfigDir("/tmp/taos lite cfg"), 0);
#if defined(WINDOWS)
  EXPECT_STREQ(tdLiteConfigDir(), "/tmp/taos lite cfg"); /* the engine leaves Windows paths alone */
#else
  EXPECT_STREQ(tdLiteConfigDir(), "\"/tmp/taos lite cfg\"");
#endif
  /* The driver gets the caller's own string: the WebSocket driver stores what it is
   * given as a path, so the engine's quoting must not travel with it. */
  EXPECT_STREQ(tdLiteConfigDirGiven(), "/tmp/taos lite cfg");

  /* Already quoted, either style: taken as is, never quoted twice. */
  ASSERT_EQ(tdLiteSetConfigDir("\"/tmp/x y\""), 0);
  EXPECT_STREQ(tdLiteConfigDir(), "\"/tmp/x y\"");
  EXPECT_STREQ(tdLiteConfigDirGiven(), "\"/tmp/x y\""); /* given as it came */
  ASSERT_EQ(tdLiteSetConfigDir("'/tmp/x y'"), 0);
  EXPECT_STREQ(tdLiteConfigDir(), "'/tmp/x y'");
  EXPECT_STREQ(tdLiteConfigDirGiven(), "'/tmp/x y'");

  /* One character: the engine does not quote a single-character path (len > 1). */
  ASSERT_EQ(tdLiteSetConfigDir("a"), 0);
  EXPECT_STREQ(tdLiteConfigDir(), "a");

#if !defined(WINDOWS)
  /* Longer than the buffer the engine stores it in: rejected with the value left
   * untouched, exactly like the engine, instead of a silent truncation. */
  ASSERT_EQ(tdLiteSetConfigDir("/tmp/taos lite keep"), 0);
  const std::string tooLong((size_t)TD_LITE_PATH_MAX + 8, 'x');
  EXPECT_EQ(tdLiteSetConfigDir(tooLong.c_str()), -1);
  EXPECT_STREQ(tdLiteConfigDir(), "\"/tmp/taos lite keep\"");
#endif

  /* Leave the default behind for the tests that come after. */
  ASSERT_EQ(tdLiteSetConfigDir(TD_LITE_CFG_DIR_DEFAULT), 0);

  /* NULL is a missing value, not "no change": the engine's option path answers
   * TSDB_CODE_INVALID_PARA and keeps the previous value. */
  const std::string kept = tdLiteConfigDir();
  terrno = 0;
  EXPECT_EQ(tdLiteSetConfigDir(nullptr), -1);
  EXPECT_EQ(terrno, TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(std::string(tdLiteConfigDir()), kept);
}

/* The driver's entry points are not looked up by name any more: the native driver
 * keeps them behind the API table that wrapperDriver.c reads via
 * taosNativeApiLookup() (Windows exports that table and nothing else).  Repeat the
 * lookup here instead of calling dlsym("taos_options"), which answers NULL for the
 * native driver on every platform. */
static void* driverEntry(void* handle, const char* name) {
  const TdNativeApiEntry* table =
      reinterpret_cast<const TdNativeApiEntry*>(taosLoadDllFunc(handle, "tdNativeApiTable"));
  if (table != nullptr) {
    for (const TdNativeApiEntry* entry = table; entry->name != nullptr; ++entry) {
      if (strcmp(entry->name, name) == 0) {
        return entry->fn;
      }
    }
    return nullptr;
  }
  /* A driver that exports the public names directly (the WebSocket one). */
  return taosLoadDllFunc(handle, name);
}

/* The wrapper keeps configDir itself and hands it to the engine through the engine's
 * own option once the driver is loaded (see wrapperDriver.c).  Nothing depends on the
 * driver exporting its configDir array any more, so this is the one path that works on
 * every platform -- including Windows and the WebSocket driver, where that array is
 * not reachable at all.  What is verified here is the hand-over: the same call, the
 * same value, and (where the array happens to be exported) the engine holding exactly
 * what the wrapper reports. */
TEST(taosLite, configDirIsHandedToTheDriver) {
  const char* driverPath = TD_LITE_NATIVE_LIB_PATH;
  void*       handle = taosLoadDll(driverPath);
  if (handle == nullptr) {
    GTEST_SKIP() << "cannot load " << driverPath << " (terrno=" << terrno << ")";
  }

  auto opt = reinterpret_cast<int (*)(TSDB_OPTION, const void*, ...)>(driverEntry(handle, "taos_options"));
  if (opt == nullptr) {
    taosCloseDll(handle);
    GTEST_SKIP() << "the driver has no taos_options";
  }

  const std::string dir = joinPath(tempDir(), "cfg dir handed over");
  ASSERT_EQ(tdLiteSetConfigDir(dir.c_str()), 0);
  /* The caller's own string goes to the driver, exactly as wrapperDriver.c does it. */
  EXPECT_EQ(opt(TSDB_OPTION_CONFIGDIR, tdLiteConfigDirGiven()), 0);

#if !defined(WINDOWS)
  /* The native driver exports the array on Linux/macOS, which makes the result
   * observable from the outside: the engine normalizes the value it receives, so a
   * directory with a space has to end up in the canonical form the wrapper reports. */
  auto engineDir = reinterpret_cast<char (*)[TD_LITE_PATH_MAX]>(taosLoadDllFunc(handle, "configDir"));
  ASSERT_NE(engineDir, nullptr) << "the native driver should export configDir here";
  EXPECT_STREQ(reinterpret_cast<const char*>(*engineDir), tdLiteConfigDir());
#endif

  ASSERT_EQ(tdLiteSetConfigDir(TD_LITE_CFG_DIR_DEFAULT), 0);
  taosCloseDll(handle);
}

/* The charset is recorded where the wrapper sees the engine accept one -- the
 * TSDB_OPTION_CHARSET option, or the charset key of taos.cfg -- and is never derived
 * from the process locale: that call is a process-wide side effect and answers the
 * wrong thing on Windows and with the WebSocket driver. */
TEST(taosLite, charsetIsRecordedFromTheOption) {
  tdLiteSetCharset("GB18030");
  EXPECT_STREQ(tdLiteCharset(), "GB18030");

  /* A value longer than the engine's array holds is cut, never overflowed. */
  const std::string tooLong(4 * (size_t)TD_LITE_CHARSET_LEN, 'x');
  tdLiteSetCharset(tooLong.c_str());
  EXPECT_EQ(strlen(tdLiteCharset()), (size_t)TD_LITE_CHARSET_LEN - 1);

  /* NULL is not a value: the recorded one stays. */
  tdLiteSetCharset("UTF-8");
  tdLiteSetCharset(nullptr);
  EXPECT_STREQ(tdLiteCharset(), "UTF-8");

  /* Clearing falls back to what the engine itself would start with, never to an
   * empty string: getTsCharset() hands this to the JVM as a charset name. */
  tdLiteSetCharset("");
  const char* fallback = tdLiteCharset();
  ASSERT_NE(fallback, nullptr);
  EXPECT_GT(strlen(fallback), 0u);
#if defined(WINDOWS)
  /* osLocale.c hardcodes UTF-8 there. */
  EXPECT_STREQ(fallback, "UTF-8");
#endif

  tdLiteSetCharset("UTF-8");
}

/* ------------------------------------------------------------------ *
 * Small helpers
 * ------------------------------------------------------------------ */

TEST(taosLite, pathHelpersMatchEngineSemantics) {
  /* The engine's taosRealPath() calls realpath(3) on Linux/macOS -- which resolves
   * symlinks (/tmp is /private/tmp on macOS) -- and _fullpath() on Windows, so the
   * expected value is whatever the platform answers, not a hard-coded path. */
  const std::string base = tempDir();
  char              expected[TD_LITE_REALPATH_MAX] = {0};
#if defined(WINDOWS)
  ASSERT_NE(_fullpath(expected, base.c_str(), sizeof(expected)), nullptr);
#else
  ASSERT_NE(realpath(base.c_str(), expected), nullptr);
#endif
  EXPECT_NE(expected[0], '\0');

  char buf[TD_LITE_REALPATH_MAX] = {0};
  EXPECT_EQ(taosRealPath((char*)base.c_str(), buf, (int32_t)sizeof(buf)), 0);
  EXPECT_STREQ(buf, expected);

  /* Same call with the engine's 256 byte limit: the internal realpath() buffer must
   * not be smaller than libc's PATH_MAX, or FORTIFY aborts the process. */
  char small[TD_LITE_PATH_MAX] = {0};
  EXPECT_EQ(taosRealPath((char*)base.c_str(), small, (int32_t)sizeof(small)), 0);
  EXPECT_STREQ(small, expected);

#if !defined(WINDOWS)
  /* _fullpath() answers for a path that does not exist, so the error path is only
   * reachable through realpath() (same as the engine's on that platform). */
  char missing[TD_LITE_REALPATH_MAX] = {0};
  EXPECT_NE(taosRealPath((char*)"/nonexistent-taos-lite-path", missing, (int32_t)sizeof(missing)), 0);
  EXPECT_EQ(terrno & (int32_t)0xFFFF0000, (int32_t)0x80FF0000) << "system error code expected";
#endif

  /* taosAppPath() answers with the directory holding the running image: the file
   * name is stripped, the directory separators are not (same as the engine's). */
  char app[TD_LITE_REALPATH_MAX] = {0};
  EXPECT_EQ(taosAppPath(app, (int32_t)sizeof(app)), 0);
  EXPECT_GT(strlen(app), 0u);
  EXPECT_EQ(strstr(app, "liteConsistencyTest"), nullptr) << app;
}

TEST(taosLite, memoryStringAndAtomicHelpers) {
  /* taosMemMalloc(0) follows the engine (plain malloc(0)), NULL means out of
   * memory and sets terrno. */
  terrno = 0;
  void* p = tdLiteMemMalloc(8);
  ASSERT_NE(p, nullptr);
  EXPECT_EQ(terrno, 0);
  p = tdLiteMemRealloc(p, 16);
  ASSERT_NE(p, nullptr);
  tdLiteMemFree(p);
  tdLiteMemFree(nullptr); /* must be a no-op */

  /* strdup family: NULL maps to INVALID_PARA, the size variant truncates. */
  terrno = 0;
  EXPECT_EQ(tdLiteStrdup(nullptr), nullptr);
  EXPECT_EQ(terrno, TSDB_CODE_INVALID_PARA);

  char* copy = tdLiteStrdup("abcdef");
  ASSERT_NE(copy, nullptr);
  EXPECT_STREQ(copy, "abcdef");
  tdLiteMemFree(copy);

  char* cut = tdLiteStrndup("abcdef", 3);
  ASSERT_NE(cut, nullptr);
  EXPECT_STREQ(cut, "abc");
  tdLiteMemFree(cut);

  int32_t value = 0;
  atomic_store_32(&value, 5);
  EXPECT_EQ(atomic_load_32(&value), 5);
  /* The compare-exchange returns the value it replaced, like the engine's. */
  EXPECT_EQ(atomic_val_compare_exchange_32(&value, 5, 9), 5);
  EXPECT_EQ(atomic_load_32(&value), 9);
  EXPECT_EQ(atomic_val_compare_exchange_32(&value, 5, 11), 9);
  EXPECT_EQ(atomic_load_32(&value), 9);

  taosMsleep(0); /* must not hang or crash */
}

TEST(taosLite, errorStringDegradesGracefully) {
  /* System errors are handed to strerror(); there is no copy of the engine's 1133
   * entry table, and that is deliberate: the public taos_errno()/taos_errstr() are
   * still answered by the driver. */
  const char* sys = tstrerror(TAOS_SYSTEM_ERROR(ENOENT));
  ASSERT_NE(sys, nullptr);
  EXPECT_NE(strstr(sys, "No such file"), nullptr) << sys;

  EXPECT_STREQ(tstrerror(TSDB_CODE_SUCCESS), "");

  /* terrno is thread local and terrstr() reports it. */
  terrno = TAOS_SYSTEM_ERROR(ENOENT);
  EXPECT_NE(strstr(terrstr(), "No such file"), nullptr);
  terrno = 0;
}

/* ------------------------------------------------------------------ *
 * Concurrency: the local helpers take the place of engine code that is called from
 * many threads at once, so they have to keep the engine's guarantees.
 * ------------------------------------------------------------------ */

TEST(taosLite, helpersAreThreadSafe) {
  const int32_t kNrThreads = 8;

  /* terrno is thread local in the engine (threadlocal int32_t + taosGetErrno()) and
   * the copy must be too: a value written by one thread may never show up in
   * another one, which is exactly the failure mode of sharing one global slot. */
  {
    const int32_t            kRounds = 2000;
    std::vector<int32_t*>    slots(kNrThreads, nullptr);
    std::atomic<int32_t>     staged{0};
    std::atomic<int32_t>     crosstalk{0};
    std::vector<std::thread> threads;
    for (int32_t t = 1; t <= kNrThreads; ++t) {
      threads.emplace_back([t, kRounds, kNrThreads, &slots, &staged, &crosstalk] {
        slots[t - 1] = taosGetErrno();
        terrno = t * 1000000;
        staged.fetch_add(1);
        while (staged.load() < kNrThreads) {
        }
        /* every thread has written by now: a shared slot would have been overwritten */
        if (*taosGetErrno() != t * 1000000) {
          crosstalk.fetch_add(1);
        }
        for (int32_t i = 1; i <= kRounds; ++i) {
          terrno = t * 1000000 + i;
          if (*taosGetErrno() != t * 1000000 + i) {
            crosstalk.fetch_add(1);
          }
        }
      });
    }
    for (std::thread& th : threads) {
      th.join();
    }
    EXPECT_EQ(crosstalk.load(), 0) << "terrno must be per thread";
    for (int32_t a = 0; a < kNrThreads; ++a) {
      ASSERT_NE(slots[a], nullptr);
      for (int32_t b = a + 1; b < kNrThreads; ++b) {
        EXPECT_NE(slots[a], slots[b]) << "taosGetErrno() returned the same slot for two threads";
      }
    }
  }

  /* atomic_val_compare_exchange_32 is the engine's CAS: exactly one thread wins each
   * round, so the total is deterministic even though the threads spin. */
  {
    const int32_t            kRounds = 3000;
    int32_t                  value = 0;
    std::atomic<int32_t>     staged{0};
    std::vector<std::thread> threads;
    for (int32_t t = 0; t < kNrThreads; ++t) {
      threads.emplace_back([&value, &staged, kNrThreads, kRounds] {
        staged.fetch_add(1);
        while (staged.load() < kNrThreads) {
        }
        for (int32_t i = 0; i < kRounds; ++i) {
          int32_t seen = atomic_load_32(&value);
          while (atomic_val_compare_exchange_32(&value, seen, seen + 1) != seen) {
            seen = atomic_load_32(&value);
          }
        }
      });
    }
    for (std::thread& th : threads) {
      th.join();
    }
    EXPECT_EQ(atomic_load_32(&value), kNrThreads * kRounds)
        << "every successful compare-exchange must be counted exactly once";
  }

  /* The allocator path is plain libc (the engine's pool is only used by the code
   * that lived in the driver), so it has no shared state of ours to corrupt. */
  {
    const int32_t            kRounds = 500;
    std::atomic<int32_t>     bad{0};
    std::atomic<int32_t>     staged{0};
    std::vector<std::thread> threads;
    for (int32_t t = 1; t <= kNrThreads; ++t) {
      threads.emplace_back([t, kRounds, kNrThreads, &bad, &staged] {
        staged.fetch_add(1);
        while (staged.load() < kNrThreads) {
        }
        for (int32_t i = 0; i < kRounds; ++i) {
          char* raw = (char*)tdLiteMemMalloc(64);
          if (raw == nullptr) {
            bad.fetch_add(1);
            continue;
          }
          (void)snprintf(raw, 64, "%d-%d", (int)t, (int)i);
          char* grow = (char*)tdLiteMemRealloc(raw, 128);
          if (grow == nullptr) {
            tdLiteMemFree(raw);
            bad.fetch_add(1);
            continue;
          }
          char* copy = tdLiteStrdup(grow);
          if (copy == nullptr || strcmp(copy, grow) != 0) {
            bad.fetch_add(1);
          }
          tdLiteMemFree(copy);
          tdLiteMemFree(grow);
        }
      });
    }
    for (std::thread& th : threads) {
      th.join();
    }
    EXPECT_EQ(bad.load(), 0) << "malloc/realloc/strdup churn must stay intact";
  }

  /* configDir is a single buffer that every thread reads while one of them may be
   * replacing it through tdLiteSetConfigDir(); it is never empty once the default has
   * been filled in, so concurrent readers must all get something usable. */
  {
    std::atomic<int32_t>     mismatched{0};
    std::atomic<int32_t>     staged{0};
    std::vector<std::thread> threads;
    for (int32_t t = 0; t < kNrThreads; ++t) {
      threads.emplace_back([kNrThreads, &mismatched, &staged] {
        staged.fetch_add(1);
        while (staged.load() < kNrThreads) {
        }
        for (int32_t i = 0; i < 1000; ++i) {
          const char* dir = tdLiteConfigDir();
          if (dir == nullptr || *dir == '\0') {
            mismatched.fetch_add(1);
          }
        }
      });
    }
    for (std::thread& th : threads) {
      th.join();
    }
    EXPECT_EQ(mismatched.load(), 0);
  }
}

/* ------------------------------------------------------------------ *
 * Log API surface: the flag semantics and the locking contract the engine's
 * counterparts have.
 * ------------------------------------------------------------------ */

TEST(taosLite, printLogHonoursFileAndScreenFlags) {
  /* The engine's taosPrintLogImp() runs two independent branches: DEBUG_FILE appends
   * to the log file, DEBUG_SCREEN writes the same line to fd 1.  Only the file half is
   * observable from a test -- the screen half goes to this binary's own stdout. */
  const std::string dir = joinPath(tempDir(), "print flags");
  /* Start from a closed log: initialisation happens once per process and the log
   * scenario above has already used it, so this test has to own the directory it logs
   * into instead of inheriting whatever log is (or is not) open at this point. */
  tdLiteLogClose();
  setEnv("TDENGINE_LITE_LOG_DIR", dir.c_str());
  char path[TD_LITE_PATH_MAX] = {0};
  ASSERT_EQ(tdLiteLogFilePath(path, sizeof(path)), 0) << "cannot open a log in " << dir;

  const int64_t idle = fileSize(path);

  /* Neither bit: no output anywhere. */
  taosPrintLog("FLAG TEST ", TD_LITE_DEBUG_ERROR, 0, "NO-FLAG-MARK");
  EXPECT_EQ(fileSize(path), idle) << "a dflag without FILE or SCREEN must write nothing";

  /* SCREEN only: the line goes to the terminal, the file must not grow. */
  taosPrintLog("FLAG TEST ", TD_LITE_DEBUG_ERROR, TD_LITE_DEBUG_SCREEN, "SCREEN-ONLY-MARK");
  EXPECT_EQ(fileSize(path), idle) << "DEBUG_SCREEN must not touch the file";

  /* FILE only: it has to be in the log now.  Rotation can switch the file, so the
   * marker is looked for in the active file first and then in the archives. */
  taosPrintLog("FLAG TEST ", TD_LITE_DEBUG_ERROR, TD_LITE_DEBUG_FILE, "FILE-ONLY-MARK");
  char now[TD_LITE_PATH_MAX] = {0};
  ASSERT_EQ(tdLiteLogFilePath(now, sizeof(now)), 0);
  int32_t doubled = 0;
  int32_t hits = countMarkerLines(now, "FILE-ONLY-MARK", &doubled);
  EXPECT_GE(hits, 0) << now;
  std::string logRoot = now;
  const size_t sep = logRoot.find_last_of("/\\");
  if (sep != std::string::npos) {
    logRoot.erase(sep);
  }
  for (const std::string& name : archivesOf(logRoot)) {
    if (hits > 0) {
      break;
    }
    hits += countMarkerLines(joinPath(logRoot, name), "FILE-ONLY-MARK", &doubled);
  }
  EXPECT_GT(hits, 0) << "a DEBUG_FILE line must reach the log file";
  EXPECT_EQ(doubled, 0) << "one call writes one line";

  /* Both bits: still exactly one line in the file, plus the screen copy. */
  taosPrintLog("FLAG TEST ", TD_LITE_DEBUG_ERROR, TD_LITE_DEBUG_FILE | TD_LITE_DEBUG_SCREEN, "BOTH-MARK");
  char both[TD_LITE_PATH_MAX] = {0};
  ASSERT_EQ(tdLiteLogFilePath(both, sizeof(both)), 0);
  int32_t dualDoubled = 0;
  int32_t dual = countMarkerLines(both, "BOTH-MARK", &dualDoubled);
  for (const std::string& name : archivesOf(logRoot)) {
    if (dual > 0) {
      break;
    }
    dual += countMarkerLines(joinPath(logRoot, name), "BOTH-MARK", &dualDoubled);
  }
  EXPECT_EQ(dual, 1) << "FILE|SCREEN writes the line to each channel once";
}

TEST(taosLite, logFilePathIsCopiedUnderLock) {
  /* The path buffer belongs to the log singleton and a rotation rewrites it, so the
   * accessor copies instead of handing out a pointer. */
  EXPECT_EQ(tdLiteLogFilePath(nullptr, 32), -1);
  EXPECT_EQ(tdLiteLogFilePath(nullptr, 0), -1);

  const std::string dir = joinPath(tempDir(), "path copy");
  tdLiteLogClose(); /* same reason as in printLogHonoursFileAndScreenFlags() */
  setEnv("TDENGINE_LITE_LOG_DIR", dir.c_str());
  char full[TD_LITE_PATH_MAX] = {0};
  ASSERT_EQ(tdLiteLogFilePath(full, sizeof(full)), 0) << "cannot open a log in " << dir;
  EXPECT_GT(strlen(full), 0u);
  EXPECT_TRUE(pathExists(full)) << full;

  /* A short destination is truncated but still terminated -- never overrun. */
  char tiny[8] = {0};
  memset(tiny, 'Z', sizeof(tiny));
  EXPECT_EQ(tdLiteLogFilePath(tiny, sizeof(tiny)), 0);
  EXPECT_EQ(strlen(tiny), sizeof(tiny) - 1);
  EXPECT_EQ(tiny[sizeof(tiny) - 1], '\0');
  EXPECT_EQ(std::string(tiny), std::string(full).substr(0, sizeof(tiny) - 1));

  /* And the caller's buffer is what it owns: overwriting it must not disturb the
   * log's own state. */
  memset(tiny, '?', sizeof(tiny));
  char again[TD_LITE_PATH_MAX] = {0};
  EXPECT_EQ(tdLiteLogFilePath(again, sizeof(again)), 0);
  EXPECT_EQ(std::string(again), std::string(full));
}

