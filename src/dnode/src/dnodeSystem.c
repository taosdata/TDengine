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
#include "tgrant.h"
#include "tconfig.h"
#include "dnodeMain.h"

static void signal_handler(int32_t signum, siginfo_t *sigInfo, void *context);
static tsem_t exitSem;

int32_t main(int32_t argc, char *argv[]) {
  int dump_config = 0;

  // Set global configuration file
  for (int32_t i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "-c") == 0) {
      if (i < argc - 1) {        
        if (strlen(argv[++i]) >= TSDB_FILENAME_LEN) {
          printf("config file path overflow");
          exit(EXIT_FAILURE);
        }
        tstrncpy(configDir, argv[i], TSDB_FILENAME_LEN);
      } else {
        printf("'-c' requires a parameter, default:%s\n", configDir);
        exit(EXIT_FAILURE);
      }
    } else if (strcmp(argv[i], "-C") == 0) {
      dump_config = 1;
    } else if (strcmp(argv[i], "-V") == 0) {
#ifdef _ACCT
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
    } else if (strcmp(argv[i], "-A") == 0) {
      tsPrintAuth = 1;
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
#ifdef TAOS_RANDOM_FILE_FAIL
    else if (strcmp(argv[i], "--random-file-fail-output") == 0) {
      if ((i < argc - 1) && (argv[i + 1][0] != '-')) {
        taosSetRandomFileFailOutput(argv[++i]);
      } else {
        taosSetRandomFileFailOutput(NULL);
      }
    } else if (strcmp(argv[i], "--random-file-fail-factor") == 0) {
      if ( (i+1) < argc ) {
        int factor = atoi(argv[i+1]);
        printf("The factor of random failure is %d\n", factor);
        taosSetRandomFileFailFactor(factor);
      } else {
        printf("Please specify a number for random failure factor!");
        exit(EXIT_FAILURE);
      }
    }
#endif
  }

  if (0 != dump_config) {
    tscEmbedded  = 1;
    taosInitGlobalCfg();
    taosReadGlobalLogCfg();

    if (!taosReadGlobalCfg()) {
      printf("TDengine read global config failed");
      exit(EXIT_FAILURE);
    }

    taosDumpGlobalCfg();
    exit(EXIT_SUCCESS);
  }

  if (tsem_init(&exitSem, 0, 0) != 0) {
    printf("failed to create exit semphore\n");
    exit(EXIT_FAILURE);
  }

  /* Set termination handler. */
  struct sigaction act = {{0}};
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
    dInfo("Failed to start TDengine, please check the log at:%s", tsLogDir);
    closelog();
    exit(EXIT_FAILURE);
  }

  syslog(LOG_INFO, "Started TDengine service successfully.");

  if (tsem_wait(&exitSem) != 0) {
    syslog(LOG_ERR, "failed to wait exit semphore: %s", strerror(errno));
  }

  dnodeCleanUpSystem();
  // close the syslog
  syslog(LOG_INFO, "Shut down TDengine service successfully");
  dInfo("TDengine is shut down!");
  closelog();
  return EXIT_SUCCESS;
}

static void signal_handler(int32_t signum, siginfo_t *sigInfo, void *context) {
  if (signum == SIGUSR1) {
    taosCfgDynamicOptions("debugFlag 143");
    return;
  }
  if (signum == SIGUSR2) {
    taosCfgDynamicOptions("resetlog");
    return;
  }

  syslog(LOG_INFO, "Shut down signal is %d", signum);
  syslog(LOG_INFO, "Shutting down TDengine service...");
  // clean the system.
#ifdef __APPLE__
  dInfo("shut down signal is %d, sender PID:%d", signum, sigInfo->si_pid);
#else // __APPLE__
  dInfo("shut down signal is %d, sender PID:%d cmdline:%s", signum, sigInfo->si_pid, taosGetCmdlineByPID(sigInfo->si_pid));
#endif // __APPLE__

  // protect the application from receive another signal
  struct sigaction act = {{0}};
  act.sa_handler = SIG_IGN;
  sigaction(SIGTERM, &act, NULL);
  sigaction(SIGHUP, &act, NULL);
  sigaction(SIGINT, &act, NULL);
  sigaction(SIGUSR1, &act, NULL);
  sigaction(SIGUSR2, &act, NULL);

  // inform main thread to exit
  tsem_post(&exitSem);
}
