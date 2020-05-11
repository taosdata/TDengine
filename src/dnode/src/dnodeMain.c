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
#include "taos.h"
#include "tglobal.h"
#include "trpc.h"
#include "tutil.h"
#include "tconfig.h"
#include "tglobal.h"
#include "dnode.h"
#include "dnodeInt.h"
#include "dnodeMgmt.h"
#include "dnodePeer.h"
#include "dnodeModule.h"
#include "dnodeVRead.h"
#include "dnodeShell.h"
#include "dnodeVWrite.h"
#include "tgrant.h"

static int32_t dnodeInitSystem();
static int32_t dnodeInitStorage();
extern void grantParseParameter();
static void dnodeCleanupStorage();
static void dnodeCleanUpSystem();
static void dnodeSetRunStatus(SDnodeRunStatus status);
static void signal_handler(int32_t signum, siginfo_t *sigInfo, void *context);
static void dnodeCheckDataDirOpenned(char *dir);
static SDnodeRunStatus tsDnodeRunStatus = TSDB_DNODE_RUN_STATUS_STOPPED;

int32_t main(int32_t argc, char *argv[]) {
  // Set global configuration file
  for (int32_t i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "-c") == 0) {
      if (i < argc - 1) {
        strcpy(configDir, argv[++i]);
      } else {
        printf("'-c' requires a parameter, default:%s\n", configDir);
        exit(EXIT_FAILURE);
      }
    } else if (strcmp(argv[i], "-V") == 0) {
#ifdef _SYNC      
      char *versionStr = "enterprise";
#else      
      char *versionStr = "community";
#endif      
      printf("%s version: %s compatible_version: %s\n", versionStr, version, compatible_version);
      printf("gitinfo: %s\n", gitinfo);
      printf("gitinfoI: %s\n", gitinfoOfInternal);
      printf("buildinfo: %s\n", buildinfo);
      exit(EXIT_SUCCESS);
    } else if (strcmp(argv[i], "-k") == 0) {
      grantParseParameter();
      exit(EXIT_SUCCESS);
    }
#ifdef TAOS_MEM_CHECK
    else if (strcmp(argv[i], "--alloc-random-fail") == 0) {
      if ((i < argc - 1) && (argv[i + 1][0] != '-')) {
        taosSetAllocMode(TAOS_ALLOC_MODE_RANDOM_FAIL, argv[++i], true);
      } else {
        taosSetAllocMode(TAOS_ALLOC_MODE_RANDOM_FAIL, NULL, true);
      }
    } else if (strcmp(argv[i], "--detect-mem-leak") == 0) {
      if ((i < argc - 1) && (argv[i + 1][0] != '-')) {
        taosSetAllocMode(TAOS_ALLOC_MODE_DETECT_LEAK, argv[++i], true);
      } else {
        taosSetAllocMode(TAOS_ALLOC_MODE_DETECT_LEAK, NULL, true);
      }
    }
#endif
  }

  /* Set termination handler. */
  struct sigaction act = {0};
  act.sa_flags = SA_SIGINFO;
  act.sa_sigaction = signal_handler;
  sigaction(SIGTERM, &act, NULL);
  sigaction(SIGHUP, &act, NULL);
  sigaction(SIGINT, &act, NULL);
  sigaction(SIGUSR1, &act, NULL);
  sigaction(SIGUSR2, &act, NULL);

  // Open /var/log/syslog file to record information.
  openlog("TDengine:", LOG_PID | LOG_CONS | LOG_NDELAY, LOG_LOCAL1);
  syslog(LOG_INFO, "Starting TDengine service...");

  // Initialize the system
  if (dnodeInitSystem() < 0) {
    syslog(LOG_ERR, "Error initialize TDengine system");
    closelog();

    dnodeCleanUpSystem();
    exit(EXIT_FAILURE);
  }

  syslog(LOG_INFO, "Started TDengine service successfully.");

  while (1) {
    sleep(1000);
  }
}

static void signal_handler(int32_t signum, siginfo_t *sigInfo, void *context) {
  if (signum == SIGUSR1) {
    taosCfgDynamicOptions("debugFlag 135");
    return;
  }
  if (signum == SIGUSR2) {
    taosCfgDynamicOptions("resetlog");
    return;
  }
  syslog(LOG_INFO, "Shut down signal is %d", signum);
  syslog(LOG_INFO, "Shutting down TDengine service...");
  // clean the system.
  dPrint("shut down signal is %d, sender PID:%d", signum, sigInfo->si_pid);
  dnodeCleanUpSystem();
  // close the syslog
  syslog(LOG_INFO, "Shut down TDengine service successfully");
  dPrint("TDengine is shut down!");
  closelog();
  exit(EXIT_SUCCESS);
}

static int32_t dnodeInitSystem() {
  dnodeSetRunStatus(TSDB_DNODE_RUN_STATUS_INITIALIZE);
  tscEmbedded  = 1;
  taosResolveCRC();
  taosInitGlobalCfg();
  taosReadGlobalLogCfg();
  taosSetCoreDump();
  signal(SIGPIPE, SIG_IGN);

  struct stat dirstat;
  if (stat(tsLogDir, &dirstat) < 0) {
    mkdir(tsLogDir, 0755);
  }

  char temp[TSDB_FILENAME_LEN];
  sprintf(temp, "%s/taosdlog", tsLogDir);
  if (taosInitLog(temp, tsNumOfLogLines, 1) < 0) {
    printf("failed to init log file\n");
  }

  if (!taosReadGlobalCfg() || !taosCheckGlobalCfg()) {
    taosPrintGlobalCfg();
    dError("TDengine read global config failed");
    return -1;
  }
  taosPrintGlobalCfg();

  dPrint("start to initialize TDengine on %s", tsLocalEp);

  if (dnodeInitStorage() != 0) return -1;
  if (dnodeInitRead() != 0) return -1;
  if (dnodeInitWrite() != 0) return -1;
  if (dnodeInitClient() != 0) return -1;
  if (dnodeInitServer() != 0) return -1;
  if (dnodeInitMgmt() != 0) return -1;
  if (dnodeInitModules() != 0) return -1;
  if (dnodeInitShell() != 0) return -1;

  dnodeStartModules();
  dnodeSetRunStatus(TSDB_DNODE_RUN_STATUS_RUNING);

  dPrint("TDengine is initialized successfully");

  return 0;
}

static void dnodeCleanUpSystem() {
  if (dnodeGetRunStatus() != TSDB_DNODE_RUN_STATUS_STOPPED) {
    dnodeSetRunStatus(TSDB_DNODE_RUN_STATUS_STOPPED);
    dnodeCleanupShell();
    dnodeCleanUpModules();
    dnodeCleanupMgmt();
    dnodeCleanupServer();
    dnodeCleanupClient();
    dnodeCleanupWrite();
    dnodeCleanupRead();
    dnodeCleanupStorage();
    taos_cleanup();
    taosCloseLog();
  }
}

SDnodeRunStatus dnodeGetRunStatus() {
  return tsDnodeRunStatus;
}

static void dnodeSetRunStatus(SDnodeRunStatus status) {
  tsDnodeRunStatus = status;
}

static void dnodeCheckDataDirOpenned(char *dir) {
  char filepath[256] = {0};
  sprintf(filepath, "%s/.running", dir);

  int32_t fd  = open(filepath, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
  int32_t ret = flock(fd, LOCK_EX | LOCK_NB);
  if (ret != 0) {
    dError("failed to lock file:%s ret:%d, database may be running, quit", filepath, ret);
    close(fd);
    exit(0);
  }
}

static int32_t dnodeInitStorage() {
  struct stat dirstat;
  if (stat(tsDataDir, &dirstat) < 0) {
    mkdir(tsDataDir, 0755);
  }

  sprintf(tsMnodeDir, "%s/mnode", tsDataDir);
  sprintf(tsVnodeDir, "%s/vnode", tsDataDir);
  sprintf(tsDnodeDir, "%s/dnode", tsDataDir);
  mkdir(tsVnodeDir, 0755);
  mkdir(tsDnodeDir, 0755);

  dnodeCheckDataDirOpenned(tsDnodeDir);

  dPrint("storage directory is initialized");
  return 0;
}

static void dnodeCleanupStorage() {}

bool  dnodeIsFirstDeploy() {
  return strcmp(tsFirst, tsLocalEp) == 0;
}
