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
#include "dmMgmt.h"
#include "mnode.h"
#include "osFile.h"
#include "tconfig.h"
#include "tglobal.h"
#include "version.h"
#include "tconv.h"
#include "dmUtil.h"
#include "qworker.h"
#include "tss.h"

#ifdef TD_JEMALLOC_ENABLED
#define ALLOW_FORBID_FUNC
#include "jemalloc/jemalloc.h"
#endif

#include "cus_name.h"

// clang-format off
#define DM_APOLLO_URL    "The apollo string to use when configuring the server, such as: -a 'jsonFile:./tests/cfg.json', cfg.json text can be '{\"fqdn\":\"td1\"}'."
#define DM_CFG_DIR       "Configuration directory."
#define DM_DMP_CFG       "Dump configuration."
#define DM_SDB_INFO      "Dump sdb info."
#define DM_ENV_CMD       "The env cmd variable string to use when configuring the server, such as: -e 'TAOS_FQDN=td1'."
#define DM_ENV_FILE      "The env variable file path to use when configuring the server, default is './.env', .env text can be 'TAOS_FQDN=td1'."
#define DM_MACHINE_CODE  "Get machine code."
#define DM_LOG_OUTPUT    "Specify log output. Options:\n\r\t\t\t   stdout, stderr, /dev/null, <directory>, <directory>/<filename>, <filename>\n\r\t\t\t   * If OUTPUT contains an absolute directory, logs will be stored in that directory instead of logDir.\n\r\t\t\t   * If OUTPUT contains a relative directory, logs will be stored in the directory combined with logDir and the relative directory."
#define DM_VERSION       "Print program version."
#define DM_EMAIL         "<support@taosdata.com>"
#define DM_MEM_DBG       "Enable memory debug"
#define DM_SET_ENCRYPTKEY  "Set encrypt key. such as: -y 1234567890abcdef, the length should be less or equal to 16."

// clang-format on
static struct {
#ifdef WINDOWS
  bool winServiceMode;
#endif
  bool         dumpConfig;
  bool         dumpSdb;
  bool         deleteTrans;
  bool         modifySdb;
  char         sdbJsonFile[PATH_MAX];
  bool         generateGrant;
  bool         memDbg;

#ifdef USE_SHARED_STORAGE
  bool         checkSs;
#endif

  bool         printAuth;
  bool         printVersion;
  bool         printHelp;
  char         envFile[PATH_MAX];
  char         apolloUrl[PATH_MAX];
  const char **envCmd;
  SArray      *pArgs;  // SConfigPair
  int64_t      startTime;
  bool         generateCode;
  char         encryptKey[ENCRYPT_KEY_LEN + 1];
} global = {0};

static void dmSetDebugFlag(int32_t signum, void *sigInfo, void *context) { (void)taosSetGlobalDebugFlag(143); }
static void dmSetAssert(int32_t signum, void *sigInfo, void *context) { tsAssert = 1; }

static void dmStopDnode(int signum, void *sigInfo, void *context) {
  // taosIgnSignal(SIGUSR1);
  // taosIgnSignal(SIGUSR2);
#ifndef TD_ASTRA
  if (taosIgnSignal(SIGTERM) != 0) {
    dWarn("failed to ignore signal SIGTERM");
  }
  if (taosIgnSignal(SIGHUP) != 0) {
    dWarn("failed to ignore signal SIGHUP");
  }
  if (taosIgnSignal(SIGINT) != 0) {
    dWarn("failed to ignore signal SIGINT");
  }
  if (taosIgnSignal(SIGABRT) != 0) {
    dWarn("failed to ignore signal SIGABRT");
  }
  if (taosIgnSignal(SIGBREAK) != 0) {
    dWarn("failed to ignore signal SIGBREAK");
  }
#endif
  dInfo("shut down signal is %d", signum);
#if !defined(WINDOWS) && !defined(TD_ASTRA)
  if (sigInfo != NULL) {
    dInfo("sender PID:%d cmdline:%s", ((siginfo_t *)sigInfo)->si_pid,
        taosGetCmdlineByPID(((siginfo_t *)sigInfo)->si_pid));
  }
#endif

  dmStop();
}

void dmStopDaemon() { dmStopDnode(SIGTERM, NULL, NULL); }

void dmLogCrash(int signum, void *sigInfo, void *context) {
  // taosIgnSignal(SIGTERM);
  // taosIgnSignal(SIGHUP);
  // taosIgnSignal(SIGINT);
  // taosIgnSignal(SIGBREAK);

#ifndef WINDOWS
  if (taosIgnSignal(SIGBUS) != 0) {
    dWarn("failed to ignore signal SIGBUS");
  }
#endif
  if (taosIgnSignal(SIGABRT) != 0) {
    dWarn("failed to ignore signal SIGABRT");
  }
  if (taosIgnSignal(SIGFPE) != 0) {
    dWarn("failed to ignore signal SIGABRT");
  }
  if (taosIgnSignal(SIGSEGV) != 0) {
    dWarn("failed to ignore signal SIGABRT");
  }
#ifdef USE_REPORT
  writeCrashLogToFile(signum, sigInfo, CUS_PROMPT "d", dmGetClusterId(), global.startTime);
#endif
#ifdef _TD_DARWIN_64
  exit(signum);
#elif defined(WINDOWS)
  exit(signum);
#endif
}

static void dmSetSignalHandle() {
  if (taosSetSignal(SIGUSR1, dmSetDebugFlag) != 0) {
    dWarn("failed to set signal SIGUSR1");
  }
  if (taosSetSignal(SIGUSR2, dmSetAssert) != 0) {
    dWarn("failed to set signal SIGUSR1");
  }
  if (taosSetSignal(SIGTERM, dmStopDnode) != 0) {
    dWarn("failed to set signal SIGUSR1");
  }
  if (taosSetSignal(SIGHUP, dmStopDnode) != 0) {
    dWarn("failed to set signal SIGUSR1");
  }
  if (taosSetSignal(SIGINT, dmStopDnode) != 0) {
    dWarn("failed to set signal SIGUSR1");
  }
  if (taosSetSignal(SIGBREAK, dmStopDnode) != 0) {
    dWarn("failed to set signal SIGUSR1");
  }
  if (taosSetSignal(SIGABRT, dmLogCrash) != 0) {
    dWarn("failed to set signal SIGUSR1");
  }
  if (taosSetSignal(SIGFPE, dmLogCrash) != 0) {
    dWarn("failed to set signal SIGUSR1");
  }
  if (taosSetSignal(SIGSEGV, dmLogCrash) != 0) {
    dWarn("failed to set signal SIGUSR1");
  }
#ifndef WINDOWS
  if (taosSetSignal(SIGTSTP, dmStopDnode) != 0) {
    dWarn("failed to set signal SIGUSR1");
  }
  if (taosSetSignal(SIGQUIT, dmStopDnode) != 0) {
    dWarn("failed to set signal SIGUSR1");
  }
  if (taosSetSignal(SIGBUS, dmLogCrash) != 0) {
    dWarn("failed to set signal SIGUSR1");
  }
#endif
}

extern bool generateNewMeta;

static int32_t dmParseArgs(int32_t argc, char const *argv[]) {
  global.startTime = taosGetTimestampMs();

  int32_t cmdEnvIndex = 0;
  if (argc < 2) return 0;

  global.envCmd = taosMemoryMalloc((argc - 1) * sizeof(char *));
  if (global.envCmd == NULL) {
    return terrno;
  }
  memset(global.envCmd, 0, (argc - 1) * sizeof(char *));
  for (int32_t i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "-c") == 0) {
      if (i < argc - 1) {
        if (strlen(argv[++i]) >= PATH_MAX) {
          printf("config file path overflow");
          return TSDB_CODE_INVALID_CFG;
        }
        tstrncpy(configDir, argv[i], PATH_MAX);
      } else {
        printf("'-c' requires a parameter, default is %s\n", configDir);
        return TSDB_CODE_INVALID_CFG;
      }
    } else if (strcmp(argv[i], "-a") == 0) {
      if (i < argc - 1) {
        if (strlen(argv[++i]) >= PATH_MAX) {
          printf("apollo url overflow");
          return TSDB_CODE_INVALID_CFG;
        }
        tstrncpy(global.apolloUrl, argv[i], PATH_MAX);
      } else {
        printf("'-a' requires a parameter\n");
        return TSDB_CODE_INVALID_CFG;
      }
    } else if (strcmp(argv[i], "-s") == 0) {
      global.dumpSdb = true;
    } else if (strcmp(argv[i], "-dTxn") == 0) {
      global.deleteTrans = true;
    } else if (strcmp(argv[i], "-mSdb") == 0) {
      global.modifySdb = true;
      if (i < argc - 1) {
        i++;
        if (strlen(argv[i]) >= PATH_MAX) {
          printf("sdb.json file path is too long\n");
          return TSDB_CODE_INVALID_CFG;
        }
        tstrncpy(global.sdbJsonFile, argv[i], PATH_MAX);
      } else {
        printf("'-mSdb' requires sdb.json file path\n");
        return TSDB_CODE_INVALID_CFG;
      }
    } else if (strcmp(argv[i], "-r") == 0) {
      generateNewMeta = true;
    } else if (strcmp(argv[i], "-E") == 0) {
      if (i < argc - 1) {
        if (strlen(argv[++i]) >= PATH_MAX) {
          printf("env file path overflow");
          return TSDB_CODE_INVALID_CFG;
        }
        tstrncpy(global.envFile, argv[i], PATH_MAX);
      } else {
        printf("'-E' requires a parameter\n");
        return TSDB_CODE_INVALID_CFG;
      }
    } else if (strcmp(argv[i], "-k") == 0) {
      global.generateGrant = true;
#if defined(LINUX)
    } else if (strcmp(argv[i], "-o") == 0 || strcmp(argv[i], "--log-output") == 0 ||
               strncmp(argv[i], "--log-output=", 13) == 0) {
      if ((i < argc - 1) || ((i == argc - 1) && strncmp(argv[i], "--log-output=", 13) == 0)) {
        int32_t     klen = strlen(argv[i]);
        int32_t     vlen = klen < 13 ? strlen(argv[++i]) : klen - 13;
        const char *val = argv[i];
        if (klen >= 13) val += 13;
        if (vlen <= 0 || vlen >= PATH_MAX) {
          printf("failed to set log output since invalid vlen:%d, valid range: [1, %d)\n", vlen, PATH_MAX);
          return TSDB_CODE_INVALID_CFG;
        }
        tsLogOutput = taosMemoryMalloc(PATH_MAX);
        if (!tsLogOutput) {
          printf("failed to set log output: '%s' since %s\n", val, tstrerror(terrno));
          return terrno;
        }
        if (taosExpandDir(val, tsLogOutput, PATH_MAX) != 0) {
          printf("failed to expand log output: '%s' since %s\n", val, tstrerror(terrno));
          return terrno;
        }
      } else {
        printf("'%s' requires a parameter\n", argv[i]);
        return TSDB_CODE_INVALID_CFG;
      }
#endif
    } else if (strcmp(argv[i], "-y") == 0) {
      global.generateCode = true;
      if (i < argc - 1) {
        int32_t len = strlen(argv[++i]);
        if (len < ENCRYPT_KEY_LEN_MIN) {
          printf("ERROR: Encrypt key should be at least %d characters\n", ENCRYPT_KEY_LEN_MIN);
          return TSDB_CODE_INVALID_CFG;
        }
        if (len > ENCRYPT_KEY_LEN) {
          printf("ERROR: Encrypt key overflow, it should be at most %d characters\n", ENCRYPT_KEY_LEN);
          return TSDB_CODE_INVALID_CFG;
        }
        tstrncpy(global.encryptKey, argv[i], ENCRYPT_KEY_LEN + 1);
      } else {
        printf("'-y' requires a parameter\n");
        return TSDB_CODE_INVALID_CFG;
      }
    } else if (strcmp(argv[i], "-C") == 0) {
      global.dumpConfig = true;
    } else if (strcmp(argv[i], "-V") == 0 || strcmp(argv[i], "--version") == 0) {
      global.printVersion = true;
#ifdef WINDOWS
    } else if (strcmp(argv[i], "--win_service") == 0) {
      global.winServiceMode = true;
#endif
    } else if (strcmp(argv[i], "-e") == 0) {
      global.envCmd[cmdEnvIndex] = argv[++i];
      cmdEnvIndex++;
    } else if (strcmp(argv[i], "-dm") == 0) {
      global.memDbg = true;
#ifdef USE_SHARED_STORAGE
    } else if (strcmp(argv[i], "--checkss") == 0) {
      global.checkSs = true;
#endif
    } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "--usage") == 0 ||
               strcmp(argv[i], "-?") == 0) {
      global.printHelp = true;
    } else {
      printf("taosd: invalid option: %s\n", argv[i]);
      printf("Try `taosd --help' or `taosd --usage' for more information.\n");
      return TSDB_CODE_INVALID_CFG;
    }
  }

  return 0;
}

static void dmPrintArgs(int32_t argc, char const *argv[]) {
  char path[1024] = {0};
  taosGetCwd(path, sizeof(path));

  char args[1024] = {0};
  if (argc > 0) {
    int32_t arglen = tsnprintf(args, sizeof(args), "%s", argv[0]);
    for (int32_t i = 1; i < argc; ++i) {
      arglen = arglen + tsnprintf(args + arglen, sizeof(args) - arglen, " %s", argv[i]);
    }
  }

  dInfo("startup path:%s args:%s", path, args);
}

static void dmGenerateGrant() { mndGenerateMachineCode(); }

static void dmPrintVersion() {
  printf("%s\n%sd version: %s compatible_version: %s\n", TD_PRODUCT_NAME, CUS_PROMPT, td_version,
         td_compatible_version);
  printf("git: %s\n", td_gitinfo);
#ifdef TD_ENTERPRISE
  printf("gitOfInternal: %s\n", td_gitinfoOfInternal);
#endif
  printf("build: %s\n", td_buildinfo);
}

static void dmPrintHelp() {
  char indent[] = "  ";
  printf("Usage: %sd [OPTION...] \n\n", CUS_PROMPT);
  printf("%s%s%s%s\n", indent, "-a,", indent, DM_APOLLO_URL);
  printf("%s%s%s%s\n", indent, "-c,", indent, DM_CFG_DIR);
  printf("%s%s%s%s\n", indent, "-s,", indent, DM_SDB_INFO);
  printf("%s%s%s%s\n", indent, "-C,", indent, DM_DMP_CFG);
  printf("%s%s%s%s\n", indent, "-e,", indent, DM_ENV_CMD);
  printf("%s%s%s%s\n", indent, "-E,", indent, DM_ENV_FILE);
  printf("%s%s%s%s\n", indent, "-k,", indent, DM_MACHINE_CODE);
#if defined(LINUX)
  printf("%s%s%s%s\n", indent, "-o, --log-output=OUTPUT", indent, DM_LOG_OUTPUT);
#endif
  printf("%s%s%s%s\n", indent, "-y,", indent, DM_SET_ENCRYPTKEY);
  printf("%s%s%s%s\n", indent, "-dm,", indent, DM_MEM_DBG);
  printf("%s%s%s%s\n", indent, "-V,", indent, DM_VERSION);

  printf("\n\nReport bugs to %s.\n", DM_EMAIL);
}

static void dmDumpCfg() {
  SConfig *pCfg = taosGetCfg();
  cfgDumpCfg(pCfg, 0, true);
}


#ifdef USE_SHARED_STORAGE
static int32_t dmCheckSs() {
  int32_t  code = 0;
  (void)printf("\n");
  
  if (!tsSsEnabled) {
    printf("shared storage is disabled (ssEnabled is 0), please enable it and try again.\n");
    return TSDB_CODE_OPS_NOT_SUPPORT;
  }

  code = tssInit();
  if (code != 0) {
    printf("failed to initialize shared storage, error code=%d.\n", code);
    return code;
  }

  code = tssCreateDefaultInstance();
  if (code != 0) {
    printf("failed to create default shared storage instance, error code=%d.\n", code);
    (void)tssUninit();
    return code;
  }

  (void)printf("shared storage configuration\n");
  (void)printf("=================================================================\n");
  tssPrintDefaultConfig();
  (void)printf("=================================================================\n");
  code = tssCheckDefaultInstance(0);
  (void)printf("=================================================================\n");

  if (code == TSDB_CODE_SUCCESS){
    printf("shared storage configuration check finished successfully.\n");
  } else {
    printf("shared storage configuration check finished with error.\n");
  }

  (void)tssCloseDefaultInstance();
  (void)tssUninit();

  return code;
}
#endif

static int32_t dmInitLog() {
  const char *logName = CUS_PROMPT "dlog";

  TAOS_CHECK_RETURN(taosInitLogOutput(&logName));

  return taosCreateLog(logName, 1, configDir, global.envCmd, global.envFile, global.apolloUrl, global.pArgs, 0);
}

static void taosCleanupArgs() {
  if (global.envCmd != NULL) taosMemoryFreeClear(global.envCmd);
}

#ifdef TAOSD_INTEGRATED
int dmStartDaemon(int argc, char const *argv[]) {
#else
int main(int argc, char const *argv[]) {
#endif
  int32_t code = 0;
#ifdef TD_JEMALLOC_ENABLED
  bool jeBackgroundThread = true;
  mallctl("background_thread", NULL, NULL, &jeBackgroundThread, sizeof(bool));
#endif
  if (!taosCheckSystemIsLittleEnd()) {
    printf("failed to start since on non-little-end machines\n");
    return -1;
  }

  if ((code = dmParseArgs(argc, argv)) != 0) {
    // printf("failed to start since parse args error\n");
    taosCleanupArgs();
    return code;
  }

#ifdef WINDOWS
  int mainWindows(int argc, char **argv);
  if (global.winServiceMode) {
    stratWindowsService(mainWindows);
  } else {
    return mainWindows(argc, argv);
  }
  return 0;
}
int mainWindows(int argc, char **argv) {
  int32_t code = 0;
#endif

  if (global.generateGrant) {
    dmGenerateGrant();
    taosCleanupArgs();
    return 0;
  }

  if (global.printHelp) {
    dmPrintHelp();
    taosCleanupArgs();
    return 0;
  }

  if (global.printVersion) {
    dmPrintVersion();
    taosCleanupArgs();
    return 0;
  }

#if defined(LINUX)
  if (global.memDbg) {
    code = taosMemoryDbgInit();
    if (code) {
      printf("failed to init memory dbg, error:%s\n", tstrerror(code));
      return code;
    }
    tsAsyncLog = false;
    printf("memory dbg enabled\n");
  }
#endif
  if (global.generateCode) {
    bool toLogFile = false;
    if ((code = taosReadDataFolder(configDir, global.envCmd, global.envFile, global.apolloUrl, global.pArgs)) != 0) {
      encryptError("failed to generate encrypt code since dataDir can not be set from cfg file,reason:%s",
                   tstrerror(code));
      return code;
    };
    TdFilePtr pFile;
    if ((code = dmCheckRunning(tsDataDir, &pFile)) != 0) {
      encryptError("failed to generate encrypt code since taosd is running, please stop it first, reason:%s",
                   tstrerror(code));
      return code;
    }
    int ret = dmUpdateEncryptKey(global.encryptKey, toLogFile);
    if (taosCloseFile(&pFile) != 0) {
      encryptError("failed to close file:%p", pFile);
    }
    taosCloseLog();
    taosCleanupArgs();
    return ret;
  }

  if ((code = dmInitLog()) != 0) {
    printf("failed to start since init log error\n");
    taosCleanupArgs();
    return code;
  }

  dmPrintArgs(argc, argv);

  if ((code = taosInitCfg(configDir, global.envCmd, global.envFile, global.apolloUrl, global.pArgs, 0)) != 0) {
    dError("failed to start since read config error");
    taosCloseLog();
    taosCleanupArgs();
    return code;
  }

  if ((code = taosMemoryPoolInit(qWorkerRetireJobs, qWorkerRetireJob)) != 0) {
    dError("failed to init memPool, error:0x%x", code);
    taosCloseLog();
    taosCleanupArgs();
    return code;
  }

#ifndef DISALLOW_NCHAR_WITHOUT_ICONV
  if ((tsCharsetCxt = taosConvInit(tsCharset)) == NULL) {
    dError("failed to init conv");
    taosCloseLog();
    taosCleanupArgs();
    return code;
  }
#endif

#ifdef USE_SHARED_STORAGE
  if (global.checkSs) {
    code = dmCheckSs();
    taosCleanupCfg();
    taosCloseLog();
    taosCleanupArgs();
    taosConvDestroy();
    return code;
  }
#endif

  if (global.dumpConfig) {
    dmDumpCfg();
    taosCleanupCfg();
    taosCloseLog();
    taosCleanupArgs();
    taosConvDestroy();
    return 0;
  }

  if (global.dumpSdb) {
    int32_t code = 0;
    TAOS_CHECK_RETURN(mndDumpSdb());
    taosCleanupCfg();
    taosCloseLog();
    taosCleanupArgs();
    taosConvDestroy();
    return 0;
  }

  if (global.deleteTrans) {
    int32_t   code = 0;
    TdFilePtr pFile;
    if ((code = dmCheckRunning(tsDataDir, &pFile)) != 0) {
      printf("failed to generate encrypt code since taosd is running, please stop it first, reason:%s",
             tstrerror(code));
      return code;
    }

    TAOS_CHECK_RETURN(mndDeleteTrans());
    taosCleanupCfg();
    taosCloseLog();
    taosCleanupArgs();
    taosConvDestroy();
    return 0;
  }

  if (global.modifySdb) {
    int32_t   code = 0;
    TdFilePtr pFile;
    if ((code = dmCheckRunning(tsDataDir, &pFile)) != 0) {
      printf("failed to modify sdb since taosd is running, please stop it first, reason:%s", tstrerror(code));
      return code;
    }

    TAOS_CHECK_RETURN(mndModifySdb(global.sdbJsonFile));
    taosCleanupCfg();
    taosCloseLog();
    taosCleanupArgs();
    taosConvDestroy();
    return 0;
  }

  osSetProcPath(argc, (char **)argv);
  taosCleanupArgs();

  if ((code = dmGetEncryptKey()) != 0) {
    dError("failed to start since failed to get encrypt key");
    taosCloseLog();
    taosCleanupArgs();
    return code;
  };

  if ((code = dmInit()) != 0) {
    if (code == TSDB_CODE_NOT_FOUND) {
      dError("Initialization of dnode failed because your current operating system is not supported. For more information and supported platforms, please visit https://docs.taosdata.com/reference/supported/.");
    } else {
      dError("failed to init dnode since %s", tstrerror(code));
    }

    taosCleanupCfg();
    taosCloseLog();
    taosConvDestroy();
    return code;
  }

  dInfo("start to init service");
  dmSetSignalHandle();

  code = dmRun();
  dInfo("shutting down the service");

  dmCleanup();
  return code;
}
