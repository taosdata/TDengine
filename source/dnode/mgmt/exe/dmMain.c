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
#include "tconfig.h"
#include "tglobal.h"
#include "version.h"
#ifdef TD_JEMALLOC_ENABLED
#include "jemalloc/jemalloc.h"
#endif
#include "dmUtil.h"

#if defined(CUS_NAME) || defined(CUS_PROMPT) || defined(CUS_EMAIL)
#include "cus_name.h"
#else
#ifndef CUS_NAME
#define CUS_NAME "TDengine"
#endif

#ifndef CUS_PROMPT
#define CUS_PROMPT "taos"
#endif

#ifndef CUS_EMAIL
#define CUS_EMAIL "<support@taosdata.com>"
#endif
#endif
// clang-format off
#define DM_APOLLO_URL    "The apollo string to use when configuring the server, such as: -a 'jsonFile:./tests/cfg.json', cfg.json text can be '{\"fqdn\":\"td1\"}'."
#define DM_CFG_DIR       "Configuration directory."
#define DM_DMP_CFG       "Dump configuration."
#define DM_SDB_INFO      "Dump sdb info."
#define DM_ENV_CMD       "The env cmd variable string to use when configuring the server, such as: -e 'TAOS_FQDN=td1'."
#define DM_ENV_FILE      "The env variable file path to use when configuring the server, default is './.env', .env text can be 'TAOS_FQDN=td1'."
#define DM_MACHINE_CODE  "Get machine code."
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
  bool         generateGrant;
  bool         memDbg;
  bool         checkS3;
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
  (void)taosIgnSignal(SIGTERM);
  (void)taosIgnSignal(SIGHUP);
  (void)taosIgnSignal(SIGINT);
  (void)taosIgnSignal(SIGABRT);
  (void)taosIgnSignal(SIGBREAK);

  dInfo("shut down signal is %d", signum);
#ifndef WINDOWS
  dInfo("sender PID:%d cmdline:%s", ((siginfo_t *)sigInfo)->si_pid,
        taosGetCmdlineByPID(((siginfo_t *)sigInfo)->si_pid));
#endif

  dmStop();
}

void dmLogCrash(int signum, void *sigInfo, void *context) {
  // taosIgnSignal(SIGTERM);
  // taosIgnSignal(SIGHUP);
  // taosIgnSignal(SIGINT);
  // taosIgnSignal(SIGBREAK);

#ifndef WINDOWS
  (void)taosIgnSignal(SIGBUS);
#endif
  (void)taosIgnSignal(SIGABRT);
  (void)taosIgnSignal(SIGFPE);
  (void)taosIgnSignal(SIGSEGV);

  char       *pMsg = NULL;
  const char *flags = "UTL FATAL ";
  ELogLevel   level = DEBUG_FATAL;
  int32_t     dflag = 255;
  int64_t     msgLen = -1;

  if (tsEnableCrashReport) {
    if (taosGenCrashJsonMsg(signum, &pMsg, dmGetClusterId(), global.startTime)) {
      taosPrintLog(flags, level, dflag, "failed to generate crash json msg");
      goto _return;
    } else {
      msgLen = strlen(pMsg);
    }
  }

_return:

  taosLogCrashInfo(CUS_PROMPT "d", pMsg, msgLen, signum, sigInfo);

#ifdef _TD_DARWIN_64
  exit(signum);
#elif defined(WINDOWS)
  exit(signum);
#endif
}

static void dmSetSignalHandle() {
  (void)taosSetSignal(SIGUSR1, dmSetDebugFlag);
  (void)taosSetSignal(SIGUSR2, dmSetAssert);
  (void)taosSetSignal(SIGTERM, dmStopDnode);
  (void)taosSetSignal(SIGHUP, dmStopDnode);
  (void)taosSetSignal(SIGINT, dmStopDnode);
  (void)taosSetSignal(SIGBREAK, dmStopDnode);
#ifndef WINDOWS
  (void)taosSetSignal(SIGTSTP, dmStopDnode);
  (void)taosSetSignal(SIGQUIT, dmStopDnode);
#endif

#if 0
#ifndef WINDOWS
  (void)taosSetSignal(SIGBUS, dmLogCrash);
#endif
  (void)taosSetSignal(SIGABRT, dmLogCrash);
  (void)taosSetSignal(SIGFPE, dmLogCrash);
  (void)taosSetSignal(SIGSEGV, dmLogCrash);
#endif
}

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
        tstrncpy(global.encryptKey, argv[i], ENCRYPT_KEY_LEN);
      } else {
        printf("'-y' requires a parameter\n");
        return TSDB_CODE_INVALID_CFG;
      }
    } else if (strcmp(argv[i], "-C") == 0) {
      global.dumpConfig = true;
    } else if (strcmp(argv[i], "-V") == 0) {
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
    } else if (strcmp(argv[i], "--checks3") == 0) {
      global.checkS3 = true;
    } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "--usage") == 0 ||
               strcmp(argv[i], "-?") == 0) {
      global.printHelp = true;
    } else {
    }
  }

  return 0;
}

static void dmPrintArgs(int32_t argc, char const *argv[]) {
  char path[1024] = {0};
  taosGetCwd(path, sizeof(path));

  char    args[1024] = {0};
  int32_t arglen = snprintf(args, sizeof(args), "%s", argv[0]);
  for (int32_t i = 1; i < argc; ++i) {
    arglen = arglen + snprintf(args + arglen, sizeof(args) - arglen, " %s", argv[i]);
  }

  dInfo("startup path:%s args:%s", path, args);
}

static void dmGenerateGrant() { mndGenerateMachineCode(); }

static void dmPrintVersion() {
  printf("%s\n%sd version: %s compatible_version: %s\n", TD_PRODUCT_NAME, CUS_PROMPT, version, compatible_version);
  printf("git: %s\n", gitinfo);
#ifdef TD_ENTERPRISE
  printf("gitOfInternal: %s\n", gitinfoOfInternal);
#endif
  printf("build: %s\n", buildinfo);
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
  printf("%s%s%s%s\n", indent, "-y,", indent, DM_SET_ENCRYPTKEY);
  printf("%s%s%s%s\n", indent, "-dm,", indent, DM_MEM_DBG);
  printf("%s%s%s%s\n", indent, "-V,", indent, DM_VERSION);

  printf("\n\nReport bugs to %s.\n", DM_EMAIL);
}

static void dmDumpCfg() {
  SConfig *pCfg = taosGetCfg();
  cfgDumpCfg(pCfg, 0, true);
}

static int32_t dmCheckS3() {
  int32_t  code = 0;
  SConfig *pCfg = taosGetCfg();
  cfgDumpCfgS3(pCfg, 0, true);
#if defined(USE_S3)
  extern int32_t s3CheckCfg();

  code = s3CheckCfg();
#endif
  return code;
}

static int32_t dmInitLog() {
  return taosCreateLog(CUS_PROMPT "dlog", 1, configDir, global.envCmd, global.envFile, global.apolloUrl, global.pArgs,
                       0);
}

static void taosCleanupArgs() {
  if (global.envCmd != NULL) taosMemoryFreeClear(global.envCmd);
}

int main(int argc, char const *argv[]) {
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

  if ((code = taosConvInit()) != 0) {
    dError("failed to init conv");
    taosCloseLog();
    taosCleanupArgs();
    return code;
  }

  if (global.checkS3) {
    code = dmCheckS3();
    taosCleanupCfg();
    taosCloseLog();
    taosCleanupArgs();
    taosConvDestroy();
    return code;
  }

  if (global.dumpConfig) {
    dmDumpCfg();
    taosCleanupCfg();
    taosCloseLog();
    taosCleanupArgs();
    taosConvDestroy();
    return 0;
  }

  if (global.dumpSdb) {
    mndDumpSdb();
    taosCleanupCfg();
    taosCloseLog();
    taosCleanupArgs();
    taosConvDestroy();
    return 0;
  }

  if (global.deleteTrans) {
    TdFilePtr pFile;
    if ((code = dmCheckRunning(tsDataDir, &pFile)) != 0) {
      printf("failed to generate encrypt code since taosd is running, please stop it first, reason:%s",
             tstrerror(code));
      return code;
    }

    mndDeleteTrans();
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
      dError("failed to init dnode since unsupported platform, please visit https://www.taosdata.com for support");
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
  tsDndStart = taosGetTimestampMs();
  tsDndStartOsUptime = taosGetOsUptime();

  code = dmRun();
  dInfo("shutting down the service");

  dmCleanup();
  return code;
}
