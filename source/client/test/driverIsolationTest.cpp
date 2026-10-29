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
 * Exactly one driver per process.
 *
 * libtaos.so is a dispatcher: it dlopen()s either the native engine or the
 * WebSocket connector at the first taos_init(), depending on TDENGINE_DRIVER.
 * The two drivers are alternatives, not layers, so whichever mode is selected
 * the loader must never pull in the other one -- a WebSocket-only deployment
 * ships without libtaosnative.so, and loading it by accident would fail there
 * (or, worse, silently run another protocol).
 *
 * The choice is per process and resolved once, so ctest runs this binary twice,
 * once per mode (see CMakeLists.txt).  The binary links the dispatcher only:
 * naming the driver on the link line would put it in DT_NEEDED/imports and the
 * test could not tell a load-time dependency from a run-time decision.
 */

#include <gtest/gtest.h>

/* Before the engine headers: osSleep.h masks nanosleep() (and friends) for the engine,
 * and <thread> -- pulled in by <chrono> -- asks for the real one. */
#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#if defined(WINDOWS)
#include <direct.h> /* _mkdir */
#include <io.h>     /* _findfirst/_findnext */
#else
#include <dirent.h> /* opendir/readdir */
#endif

#include "taos.h"     /* taos_init/taos_cleanup/taos_get_client_info/taos_errno */
#include "wrapperLite.h" /* TD_LITE_DRIVER_*_FILE, the names the dispatcher dlopens */

#if defined(WINDOWS)
#include <windows.h>
#elif defined(_TD_DARWIN_64)
#include <mach-o/dyld.h>
#endif

namespace {

/* Is <name> mapped into this process right now?  Each platform answers with its
 * own loader introspection: the module table, dyld's image list, /proc/self/maps.
 * A substring test is enough: the drivers are dlopen()ed under these exact names
 * (that is what TD_LITE_DRIVER_*_FILE is for). */
bool isModuleLoaded(const char* name) {
#if defined(WINDOWS)
  return GetModuleHandleA(name) != NULL;
#elif defined(_TD_DARWIN_64)
  uint32_t count = _dyld_image_count();
  for (uint32_t i = 0; i < count; ++i) {
    const char* image = _dyld_get_image_name(i);
    if (image != NULL && strstr(image, name) != NULL) {
      return true;
    }
  }
  return false;
#else
  FILE* fp = fopen("/proc/self/maps", "r");
  if (fp == NULL) {
    return false;
  }
  char line[4096];
  bool found = false;
  while (!found && fgets(line, sizeof(line), fp) != NULL) {
    found = (strstr(line, name) != NULL);
  }
  (void)fclose(fp);
  return found;
#endif
}

bool modeIsWebsocket() {
  const char* driver = getenv("TDENGINE_DRIVER");
  return (driver != NULL && strcmp(driver, "websocket") == 0);
}

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

/* A private, empty directory for one test. */
std::string tempDir() {
#if defined(WINDOWS)
  static int32_t seq = 0;
  const char*    base = getenv("TEMP");
  if (base == nullptr || base[0] == '\0') {
    base = ".";
  }
  /* mkdtemp() has no Windows equivalent, so the pid keeps two ctest entries
   * running at the same time out of the same name. */
  std::string dir = joinPath(base, "tdDriverIsolation_" + std::to_string((long long)GetCurrentProcessId()) + "_" +
                                       std::to_string(++seq));
  (void)liteMkdir(dir);
  return dir;
#else
  char        tmpl[] = "/tmp/tdDriverIsolationXXXXXX";
  const char* dir = mkdtemp(tmpl);
  return std::string(dir == nullptr ? "" : dir);
#endif
}

/* The driver the dispatcher loads is the engine, and the engine opens its log under
 * logDir -- /var/log/taos by default, with /etc/taos as configDir.  Both belong to
 * whoever installed the client, which on a shared CI machine is not the user running
 * the tests, and then taos_init() fails with EACCES before any driver question is
 * asked (seen on the macOS runner, which has an installed /etc/taos/taos.cfg).  The
 * property under test is *which* driver gets loaded, not where the engine writes its
 * log, so the run brings its own directory -- same setup as liteConsistencyTest and
 * jniSurfaceTest.  Returns "" when it could not be prepared, and the caller then
 * leaves the machine's own configDir alone instead of pointing the engine at a
 * half-built one. */
std::string prepareConfigDir() {
  const std::string cfgDir = tempDir();
  if (cfgDir.empty()) {
    return cfgDir;
  }

  const std::string logDir = joinPath(cfgDir, "logs");
  (void)liteMkdir(logDir);

  const std::string cfgFile = joinPath(cfgDir, "taos.cfg");
  FILE*             fp = fopen(cfgFile.c_str(), "w");
  if (fp == nullptr) {
    return std::string();
  }
  (void)fprintf(fp, "logDir %s\n", logDir.c_str());
  (void)fclose(fp);
  return cfgDir;
}

/* taos_options(CONFIGDIR) is the public way to hand the directory over; the
 * dispatcher answers it itself (and a later-loaded driver is told as well), so this
 * loads nothing and the "nothing is loaded yet" assertions still hold. */
std::string usePrivateConfigDir() {
  const std::string cfgDir = prepareConfigDir();
  if (!cfgDir.empty()) {
    EXPECT_EQ(taos_options(TSDB_OPTION_CONFIGDIR, cfgDir.c_str()), 0) << cfgDir;
  }
  return cfgDir;
}

/* Does `dir` hold a file whose name starts with `prefix`?  Used to prove that a driver
 * wrote its log into the logDir of the taos.cfg the test prepared. */
bool dirHasFileStartingWith(const std::string& dir, const char* prefix) {
#if defined(WINDOWS)
  struct _finddata_t fd;
  intptr_t           handle = _findfirst(joinPath(dir, "*").c_str(), &fd);
  if (handle == -1) {
    return false;
  }
  bool found = false;
  do {
    found = (strncmp(fd.name, prefix, strlen(prefix)) == 0);
  } while (!found && _findnext(handle, &fd) == 0);
  (void)_findclose(handle);
  return found;
#else
  DIR* d = opendir(dir.c_str());
  if (d == nullptr) {
    return false;
  }
  bool found = false;
  for (struct dirent* e = readdir(d); e != nullptr; e = readdir(d)) {
    if (strncmp(e->d_name, prefix, strlen(prefix)) == 0) {
      found = true;
      break;
    }
  }
  (void)closedir(d);
  return found;
#endif
}

}  // namespace

/* Registered twice by ctest: TDENGINE_DRIVER=native and TDENGINE_DRIVER=websocket. */
TEST(driverIsolation, onlyTheSelectedDriverIsLoaded) {
  const bool  ws = modeIsWebsocket();
  const char* selected = ws ? TD_LITE_DRIVER_WS_FILE : TD_LITE_DRIVER_NATIVE_FILE;
  const char* other = ws ? TD_LITE_DRIVER_NATIVE_FILE : TD_LITE_DRIVER_WS_FILE;

  /* Nothing is loaded before the first initialisation: this binary links the
   * dispatcher only, and no platform puts a driver in the dispatcher's load
   * commands any more, so a driver that is already here would mean the wrapper
   * had dragged it in. */
  EXPECT_FALSE(isModuleLoaded(selected)) << selected << " was loaded before taos_init()";
  EXPECT_FALSE(isModuleLoaded(other)) << other << " was loaded before taos_init()";

  const std::string cfgDir = usePrivateConfigDir();
  (void)cfgDir;

  int32_t code = taos_init();

  /* The property a WebSocket-only (or native-only) deployment depends on: the
   * driver of the other mode must not be loaded, whatever happens below. */
  EXPECT_FALSE(isModuleLoaded(other)) << "TDENGINE_DRIVER=" << (ws ? "websocket" : "native")
                                      << " must not load " << other;

  if (code != 0) {
    if (ws) {
      /* The WebSocket connector comes from taos-connector-rust, a separate
       * component that is staged into the client tree, so a build that does not
       * ship it cannot be asked to run this mode.  The isolation assertion above
       * still ran. */
      GTEST_SKIP() << "websocket driver unavailable (errno=" << taos_errno(NULL) << ")";
    }
    ASSERT_EQ(code, 0) << "the native driver is always built next to the dispatcher"
                       << " (errno=" << taos_errno(NULL) << ")";
  }

  /* The selected mode really engaged: its driver is loaded and it answers the
   * public API the dispatcher forwards to it. */
  EXPECT_TRUE(isModuleLoaded(selected)) << selected << " should have been loaded by taos_init()";

  /* The config directory handed over *before* taos_init() has to be the one the driver
   * uses.  The WebSocket driver builds its rolling log appender in the logDir of that
   * taos.cfg (taoswslog*), which makes this observable without a server -- and it is
   * exactly the case the dispatcher used to break: it passed the engine's quoted form
   * of the path, which is not a directory any driver can open. */
  if (ws && !cfgDir.empty()) {
    const std::string logDir = joinPath(cfgDir, "logs");
    EXPECT_TRUE(dirHasFileStartingWith(logDir, "taoswslog"))
        << "the websocket driver did not use " << logDir
        << " from the config dir given to taos_options(TSDB_OPTION_CONFIGDIR)";
  }

  const char* info = taos_get_client_info();
  ASSERT_NE(info, nullptr);
  EXPECT_GT(strlen(info), 0u) << "the driver did not answer taos_get_client_info()";

  taos_cleanup();
}

/* taos_init() is the entry point every thread reaches (taos_connect() calls it), and
 * it serialises its three steps -- driver environment, driver load, wrapper state --
 * with pthread_once, so a burst of threads has to end up with one driver loaded and
 * identical answers.  ctest runs this case in a process of its own
 * (--gtest_filter=...): that is the only way to make the *first* taos_init() of the
 * process concurrent, which is the lock-free path production code uses. */
TEST(driverIsolation, concurrentInitLoadsTheDriverOnce) {
  const int32_t kThreads = 16;

  const bool  ws = modeIsWebsocket();
  const char* selected = ws ? TD_LITE_DRIVER_WS_FILE : TD_LITE_DRIVER_NATIVE_FILE;
  const char* other = ws ? TD_LITE_DRIVER_NATIVE_FILE : TD_LITE_DRIVER_WS_FILE;

  /* Before the threads, so none of them races into a configDir that the machine may
   * not let this user write to. */
  const std::string cfgDir = usePrivateConfigDir();
  (void)cfgDir;

  std::atomic<int32_t>     failed{0};
  std::atomic<int32_t>     unanswered{0};
  std::vector<int32_t>     codes(kThreads, -1);
  std::vector<std::thread> threads;
  for (int32_t t = 0; t < kThreads; ++t) {
    threads.emplace_back([t, &failed, &unanswered, &codes] {
      const int32_t code = taos_init();
      codes[t] = code;
      if (code != 0) {
        failed.fetch_add(1);
      }
      const char* info = taos_get_client_info();
      if (info == nullptr || info[0] == '\0') {
        unanswered.fetch_add(1);
      }
    });
  }
  for (std::thread& th : threads) {
    th.join();
  }

  /* The property the deployment depends on, checked before the availability skip:
   * sixteen threads racing into init must not drag in the other driver. */
  EXPECT_FALSE(isModuleLoaded(other)) << "TDENGINE_DRIVER=" << (ws ? "websocket" : "native")
                                      << " must not load " << other;

  if (failed.load() != 0) {
    if (ws) {
      GTEST_SKIP() << "websocket driver unavailable (errno=" << taos_errno(NULL) << ")";
    }
    ASSERT_EQ(failed.load(), 0) << "the native driver is always built next to the dispatcher"
                                << " (errno=" << taos_errno(NULL) << ")";
  }

  for (int32_t t = 1; t < kThreads; ++t) {
    EXPECT_EQ(codes[t], codes[0]) << "taos_init() answered differently to thread " << t;
  }
  EXPECT_EQ(unanswered.load(), 0) << "every thread must get the driver's client info";
  EXPECT_TRUE(isModuleLoaded(selected)) << selected << " should be loaded once, not once per thread";

  taos_cleanup();
}
