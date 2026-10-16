/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

#define ALLOW_FORBID_FUNC

#include <gtest/gtest.h>

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#ifndef WINDOWS
#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#endif
#ifdef __linux__
#include <cstdlib>
#include <spawn.h>
#include <sys/mount.h>

extern char **environ;

// constructor(101) runs before the default constructors in linked libraries.
// Those constructors can start threads; fork() afterwards may never reach exec.
// A copy of this binary, named xnoded, stops here so /proc/pid/exe stays xnoded
// even when /bin/sleep is a script or re-execs another path.
static void txnodeTestPauseIfSleeper(void) __attribute__((constructor(101)));
static void txnodeTestPauseIfSleeper(void) {
  char    buf[512] = {0};
  int     fd = open("/proc/self/cmdline", O_RDONLY | O_CLOEXEC);
  if (fd < 0) return;
  ssize_t n = read(fd, buf, sizeof(buf) - 1);
  (void)close(fd);
  if (n <= 0) return;
  for (ssize_t offset = 0; offset < n;) {
    if (std::strcmp(buf + offset, "--txnode-sleeper") == 0) {
      for (;;) pause();
    }
    offset += (ssize_t)std::strlen(buf + offset) + 1;
  }
}
#endif

#include "txnodeInt.h"

namespace {

bool containsEnvironmentValue(char **environment, const char *expected) {
  for (int32_t index = 0; environment[index] != nullptr; ++index) {
    if (std::strcmp(environment[index], expected) == 0) {
      return true;
    }
  }
  return false;
}

}  // namespace

TEST(TxnodeEnvironmentTest, InheritsPrivateKeyWhenConfigurationIsEmpty) {
  const char *inherited[] = {"PATH=/bin", "XNODED_XNODE_DSN_PRIVATE_KEY_FILE=/inherited/key"};
  const char *overrides[] = {"XNODED_CFG_DIR=/etc/taos", "", nullptr};
  char **environment = nullptr;

  ASSERT_EQ(txnodeBuildXnodedEnvironment(inherited, 2, overrides, 2, &environment), 0);
  ASSERT_NE(environment, nullptr);
  EXPECT_TRUE(containsEnvironmentValue(environment, "PATH=/bin"));
  EXPECT_TRUE(containsEnvironmentValue(environment, "XNODED_CFG_DIR=/etc/taos"));
  EXPECT_TRUE(containsEnvironmentValue(environment, "XNODED_XNODE_DSN_PRIVATE_KEY_FILE=/inherited/key"));
  txnodeFreeEnvironment(environment);
}

TEST(TxnodeEnvironmentTest, ConfiguredPrivateKeyOverridesInheritedValue) {
  const char *inherited[] = {"XNODED_XNODE_DSN_PRIVATE_KEY_FILE=/old/key"};
  const char *overrides[] = {"XNODED_XNODE_DSN_PRIVATE_KEY_FILE=/configured/key", nullptr};
  char **environment = nullptr;

  ASSERT_EQ(txnodeBuildXnodedEnvironment(inherited, 1, overrides, 1, &environment), 0);
  ASSERT_NE(environment, nullptr);
  EXPECT_TRUE(containsEnvironmentValue(environment, "XNODED_XNODE_DSN_PRIVATE_KEY_FILE=/configured/key"));
  EXPECT_FALSE(containsEnvironmentValue(environment, "XNODED_XNODE_DSN_PRIVATE_KEY_FILE=/old/key"));
  txnodeFreeEnvironment(environment);
}

TEST(TxnodeEnvironmentTest, MatchesEnvironmentNamesUsingPlatformSemantics) {
  const char *inherited[] = {"xnoded_cfg_dir=/inherited/cfg"};
  const char *overrides[] = {"XNODED_CFG_DIR=/etc/taos", nullptr};
  char **environment = nullptr;

  ASSERT_EQ(txnodeBuildXnodedEnvironment(inherited, 1, overrides, 1, &environment), 0);
  ASSERT_NE(environment, nullptr);
  EXPECT_TRUE(containsEnvironmentValue(environment, "XNODED_CFG_DIR=/etc/taos"));
#ifdef WINDOWS
  EXPECT_FALSE(containsEnvironmentValue(environment, "xnoded_cfg_dir=/inherited/cfg"));
#else
  EXPECT_TRUE(containsEnvironmentValue(environment, "xnoded_cfg_dir=/inherited/cfg"));
#endif
  txnodeFreeEnvironment(environment);
}

TEST(TxnodeSupervisionTest, DoesNotRestartForPrivateKeyConfigurationFailure) {
  EXPECT_FALSE(txnodeShouldRestartXnoded(XNODED_PRIVATE_KEY_CONFIG_EXIT_CODE, 0, false));
}

TEST(TxnodeSupervisionTest, RestartsForOtherUnexpectedExit) {
  EXPECT_TRUE(txnodeShouldRestartXnoded(1, 0, false));
}

TEST(TxnodeSupervisionTest, DoesNotRestartAfterNormalExitOrStop) {
  EXPECT_FALSE(txnodeShouldRestartXnoded(0, 0, false));
  EXPECT_FALSE(txnodeShouldRestartXnoded(1, 0, true));
}

TEST(TxnodePidFileTest, ParsesOnlyPositiveProcessIds) {
  int32_t pid = 0;
  EXPECT_EQ(txnodeParseXnodedPid("1234", 4, &pid), 0);
  EXPECT_EQ(pid, 1234);
  EXPECT_EQ(txnodeParseXnodedPid("4321\n", 5, &pid), 0);
  EXPECT_EQ(pid, 4321);

  const char *invalid[] = {"0", "1", "-1", "-1234", " 12", "12abc", "abc", "99999999999"};
  for (const char *value : invalid) {
    pid = 7;
    EXPECT_NE(txnodeParseXnodedPid(value, (int64_t)std::strlen(value), &pid), 0) << value;
    EXPECT_EQ(pid, 7) << value;
  }
  EXPECT_NE(txnodeParseXnodedPid("", 0, &pid), 0);
}

#ifndef WINDOWS
namespace {

class TxnodeTempDir {
 public:
  explicit TxnodeTempDir(const char *parent = "/tmp") {
    std::string pattern = std::string(parent) + "/txnodeTestXXXXXX";
    std::vector<char> storage(pattern.begin(), pattern.end());
    storage.push_back('\0');
    char *dir = mkdtemp(storage.data());
    if (dir != nullptr) {
      path_ = dir;
      (void)chmod(dir, 0755);
    }
  }
  ~TxnodeTempDir() {
    if (!path_.empty()) {
      std::string command = "rm -rf '" + path_ + "'";
      (void)system(command.c_str());
    }
  }
  bool        ok() const { return !path_.empty(); }
  std::string file(const char *name) const { return path_ + "/" + name; }

 private:
  std::string path_;
};

void writeText(const std::string &path, const char *content) {
  int fd = open(path.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(write(fd, content, std::strlen(content)), (ssize_t)std::strlen(content));
  (void)close(fd);
}

std::string readText(const std::string &path) {
  char buf[64] = {0};
  int  fd = open(path.c_str(), O_RDONLY);
  if (fd < 0) return "";
  ssize_t n = read(fd, buf, sizeof(buf) - 1);
  (void)close(fd);
  return n > 0 ? std::string(buf, n) : "";
}

}  // namespace

TEST(TxnodePidFileTest, RoundTripsPrivatePidFile) {
  TxnodeTempDir dir;
  ASSERT_TRUE(dir.ok());
  std::string pidPath = dir.file(".xnoded.pid");

  ASSERT_EQ(txnodeWriteXnodedPidFile(pidPath.c_str(), 1234), 0);
  struct stat info = {};
  ASSERT_EQ(lstat(pidPath.c_str(), &info), 0);
  EXPECT_TRUE(S_ISREG(info.st_mode));
  EXPECT_EQ(info.st_mode & 0777, 0600u);

  int32_t pid = 0;
  ASSERT_EQ(txnodeReadXnodedPidFile(pidPath.c_str(), &pid), 0);
  EXPECT_EQ(pid, 1234);

  ASSERT_EQ(txnodeWriteXnodedPidFile(pidPath.c_str(), 5678), 0);
  ASSERT_EQ(txnodeReadXnodedPidFile(pidPath.c_str(), &pid), 0);
  EXPECT_EQ(pid, 5678);
}

TEST(TxnodePidFileTest, WriteDoesNotFollowPlantedSymlink) {
  TxnodeTempDir dir;
  ASSERT_TRUE(dir.ok());
  std::string victim = dir.file("victim");
  std::string pidPath = dir.file(".xnoded.pid");
  writeText(victim, "do-not-truncate");
  ASSERT_EQ(symlink(victim.c_str(), pidPath.c_str()), 0);

  ASSERT_EQ(txnodeWriteXnodedPidFile(pidPath.c_str(), 1234), 0);
  EXPECT_EQ(readText(victim), "do-not-truncate");

  struct stat info = {};
  ASSERT_EQ(lstat(pidPath.c_str(), &info), 0);
  EXPECT_TRUE(S_ISREG(info.st_mode));
  EXPECT_EQ(readText(pidPath), "1234");
}

TEST(TxnodePidFileTest, ReadRejectsSymlinkAndInvalidContent) {
  TxnodeTempDir dir;
  ASSERT_TRUE(dir.ok());
  std::string target = dir.file("target");
  std::string pidPath = dir.file(".xnoded.pid");
  writeText(target, "1234");
  ASSERT_EQ(symlink(target.c_str(), pidPath.c_str()), 0);

  int32_t pid = 0;
  EXPECT_NE(txnodeReadXnodedPidFile(pidPath.c_str(), &pid), 0);
  EXPECT_EQ(pid, 0);

  ASSERT_EQ(unlink(pidPath.c_str()), 0);
  writeText(pidPath, "0");
  EXPECT_NE(txnodeReadXnodedPidFile(pidPath.c_str(), &pid), 0);
  writeText(pidPath, "-1");
  EXPECT_NE(txnodeReadXnodedPidFile(pidPath.c_str(), &pid), 0);
  EXPECT_EQ(pid, 0);
}

TEST(TxnodePidFileTest, ReadRejectsFileOwnedByOtherUser) {
  if (geteuid() != 0) {
    GTEST_SKIP() << "requires root to chown";
  }
  TxnodeTempDir dir;
  ASSERT_TRUE(dir.ok());
  std::string pidPath = dir.file(".xnoded.pid");
  writeText(pidPath, "1234");
  ASSERT_EQ(chown(pidPath.c_str(), 65534, 65534), 0);

  int32_t pid = 0;
  EXPECT_NE(txnodeReadXnodedPidFile(pidPath.c_str(), &pid), 0);
  EXPECT_EQ(pid, 0);
}

#endif

#ifdef __linux__
namespace {

std::string gMountedExecDir;
bool        gOtherUserCanExec = false;

void reap(pid_t pid) {
  (void)kill(pid, SIGKILL);
  (void)waitpid(pid, nullptr, 0);
}

bool mountAllowsExec(const char *path) {
  FILE *fp = fopen("/proc/self/mountinfo", "r");
  if (fp == nullptr) return true;
  char   line[4096];
  size_t bestLen = 0;
  bool   bestExec = true;
  while (fgets(line, sizeof(line), fp) != nullptr) {
    int  id = 0;
    int  parent = 0;
    char root[PATH_MAX] = {0};
    char mountpoint[PATH_MAX] = {0};
    char options[PATH_MAX] = {0};
    if (std::sscanf(line, "%d %d %*s %s %s %s", &id, &parent, root, mountpoint, options) != 5) continue;
    size_t len = std::strlen(mountpoint);
    if (len < bestLen) continue;
    if (std::strncmp(path, mountpoint, len) != 0) continue;
    if (len > 1 && path[len] != '\0' && path[len] != '/') continue;
    bestLen = len;
    bestExec = std::strstr(options, "noexec") == nullptr;
  }
  (void)fclose(fp);
  return bestExec;
}

bool installXnodedSleeper(const std::string &dest) {
  int in = open("/proc/self/exe", O_RDONLY | O_CLOEXEC);
  if (in < 0) return false;
  int out = open(dest.c_str(), O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0755);
  if (out < 0) {
    (void)close(in);
    return false;
  }
  char buf[1 << 16];
  bool ok = true;
  for (;;) {
    ssize_t n = read(in, buf, sizeof(buf));
    if (n == 0) break;
    if (n < 0) {
      ok = false;
      break;
    }
    ssize_t off = 0;
    while (off < n) {
      ssize_t wrote = write(out, buf + off, (size_t)(n - off));
      if (wrote < 0) {
        ok = false;
        break;
      }
      off += wrote;
    }
    if (!ok) break;
  }
  if (ok && fchmod(out, 0755) != 0) ok = false;
  (void)close(in);
  (void)close(out);
  if (!ok) (void)unlink(dest.c_str());
  return ok;
}

std::string executableBaseName(pid_t pid) {
  std::string procExe = "/proc/" + std::to_string(pid) + "/exe";
  char        buf[PATH_MAX] = {0};
  ssize_t     n = readlink(procExe.c_str(), buf, sizeof(buf) - 1);
  if (n <= 0) return "";
  std::string path(buf, n);
  const char  deleted[] = " (deleted)";
  size_t      deletedLen = sizeof(deleted) - 1;
  if (path.size() > deletedLen && path.compare(path.size() - deletedLen, deletedLen, deleted) == 0) {
    path.resize(path.size() - deletedLen);
  }
  size_t slash = path.find_last_of('/');
  return slash == std::string::npos ? path : path.substr(slash + 1);
}

pid_t spawnSleeper(const std::string &exe, uid_t uid) {
  char  arg0[] = "xnoded";
  char  arg1[] = "--txnode-sleeper";
  char *argv[] = {arg0, arg1, nullptr};
  pid_t pid = 0;
  if (uid == geteuid()) {
    // posix_spawn does not run between fork and exec, so locks held by other
    // threads in this process cannot stall the child.
    if (posix_spawn(&pid, exe.c_str(), nullptr, nullptr, argv, environ) != 0) return -1;
  } else {
    // glibc 2.28 has no POSIX_SPAWN_SETUID. vfork + execve stays async-signal-safe.
    pid = vfork();
    if (pid < 0) return -1;
    if (pid == 0) {
      if (setuid(uid) != 0) _exit(126);
      execve(exe.c_str(), argv, environ);
      _exit(127);
    }
  }
  if (pid <= 0) return -1;
  bool ready = false;
  for (int i = 0; i < 50; ++i) {
    int status = 0;
    if (waitpid(pid, &status, WNOHANG) != 0) break;
    if (executableBaseName(pid) == "xnoded") {
      ready = true;
      break;
    }
    (void)usleep(10 * 1000);
  }
  if (!ready) {
    reap(pid);
    return -1;
  }
  return pid;
}

bool directoryCanRunCompanion(const char *parent) {
  if (parent == nullptr || parent[0] == '\0' || !mountAllowsExec(parent)) return false;
  TxnodeTempDir probe(parent);
  if (!probe.ok()) return false;
  std::string exe = probe.file("xnoded");
  if (!installXnodedSleeper(exe)) return false;
  pid_t own = spawnSleeper(exe, geteuid());
  if (own <= 0) return false;
  reap(own);
  if (geteuid() == 0 && !gOtherUserCanExec) {
    pid_t other = spawnSleeper(exe, 65534);
    if (other > 0) {
      gOtherUserCanExec = true;
      reap(other);
    }
  }
  return true;
}

}  // namespace

extern "C" void txnodeTestReleaseExecMount(void) {
  if (gMountedExecDir.empty()) return;
  (void)umount(gMountedExecDir.c_str());
  (void)rmdir(gMountedExecDir.c_str());
  gMountedExecDir.clear();
}

TEST(TxnodeProcessTest, RejectsNonXnodedProcesses) {
  EXPECT_FALSE(txnodeIsXnodedProcess(0));
  EXPECT_FALSE(txnodeIsXnodedProcess(1));
  EXPECT_FALSE(txnodeIsXnodedProcess(-1));
  EXPECT_FALSE(txnodeIsXnodedProcess((int32_t)getpid()));
  EXPECT_FALSE(txnodeIsXnodedProcess(INT32_MAX));
}

std::string mountPrivateExecDirectory() {
  if (!gMountedExecDir.empty()) return gMountedExecDir;
  // The unit-test container is privileged, but its /tmp and /dev/shm are often
  // noexec, and the bind-mounted workspace is not traversable by uid 65534.
  // A private tmpfs keeps its own exec flag even when the parent mount is noexec.
  const char *parents[] = {"/tmp", "/var/tmp", "/run", "/dev/shm"};
  for (const char *parent : parents) {
    std::string       pattern = std::string(parent) + "/txnodeExecXXXXXX";
    std::vector<char> storage(pattern.begin(), pattern.end());
    storage.push_back('\0');
    char *dir = mkdtemp(storage.data());
    if (dir == nullptr) continue;
    if (mount("tmpfs", dir, "tmpfs", MS_NOSUID | MS_NODEV, "mode=1777") != 0) {
      (void)rmdir(dir);
      continue;
    }
    gMountedExecDir = dir;
    (void)atexit(txnodeTestReleaseExecMount);
    return gMountedExecDir;
  }
  return "";
}

std::string executableParentDirectory() {
  char        selfPath[PATH_MAX] = {0};
  std::string selfParentStorage;
  const char *selfParent = nullptr;
  ssize_t     selfLen = readlink("/proc/self/exe", selfPath, sizeof(selfPath) - 1);
  if (selfLen > 0) {
    selfParentStorage.assign(selfPath, selfLen);
    size_t slash = selfParentStorage.find_last_of('/');
    if (slash != std::string::npos && slash > 0) {
      selfParentStorage.resize(slash);
      selfParent = selfParentStorage.c_str();
    }
  }
  char        cwd[PATH_MAX] = {0};
  const char *cwdParent = getcwd(cwd, sizeof(cwd)) == nullptr ? nullptr : cwd;
  // The test binary's directory is already executable. /tmp and /dev/shm in the
  // unit-test image are often noexec, so they are only used when exec is allowed.
  const char *candidates[] = {selfParent, "/dev/shm", "/run/shm", "/tmp", "/var/tmp", cwdParent};
  for (const char *candidate : candidates) {
    if (directoryCanRunCompanion(candidate)) return candidate;
  }
  std::string mounted = mountPrivateExecDirectory();
  if (!mounted.empty() && directoryCanRunCompanion(mounted.c_str())) return mounted;
  return "";
}

TEST(TxnodeProcessTest, AcceptsOnlyXnodedOwnedByCurrentUser) {
  std::string parent = executableParentDirectory();
  ASSERT_FALSE(parent.empty());
  TxnodeTempDir dir(parent.c_str());
  ASSERT_TRUE(dir.ok());
  std::string exe = dir.file("xnoded");
  ASSERT_TRUE(installXnodedSleeper(exe));

  pid_t own = spawnSleeper(exe, geteuid());
  ASSERT_GT(own, 0);
  EXPECT_TRUE(txnodeIsXnodedProcess((int32_t)own));
  reap(own);

  if (geteuid() == 0 && gOtherUserCanExec) {
    pid_t other = spawnSleeper(exe, 65534);
    ASSERT_GT(other, 0);
    EXPECT_FALSE(txnodeIsXnodedProcess((int32_t)other));
    reap(other);
  }
}
#endif
