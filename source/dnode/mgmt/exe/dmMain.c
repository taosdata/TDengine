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
#include "dmUtil.h"
#include "mnode.h"
#include "osEnv.h"
#include "osFile.h"
#include "qworker.h"
#include "tconfig.h"
#include "tconv.h"
#include "tglobal.h"
#include "trepair.h"
#include "tss.h"
#include "version.h"
#include "wal.h"

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
#define DM_REPAIR        "Enable repair mode. Works with --node-type/--file-type/--mode and other repair options."
#define DM_REPAIR_NODE_TYPE "Repair target node type. Options: vnode, mnode, dnode, snode."
#define DM_REPAIR_FILE_TYPE "Repair target file type. Examples: vnode->wal|meta|tsdb; mnode->wal|data; dnode->config; snode->checkpoint."
#define DM_REPAIR_VNODE_ID  "Target vnode id list, separated by comma (required when --node-type=vnode)."
#define DM_REPAIR_BACKUP_PATH "Backup path for corrupted files before repair."
#define DM_REPAIR_MODE       "Repair mode. Options: force, replica, copy."
#define DM_REPAIR_REPLICA_NODE "Replica node endpoint for copy mode. Format: <ip>:<dataDir>, required when --mode=copy."

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
  SRepairCliArgs repairCliArgs;
  SRepairCtx   repairCtx;
  char         repairSessionDir[PATH_MAX];
  char         repairLogPath[PATH_MAX];
  char         repairStatePath[PATH_MAX];
  char         envFile[PATH_MAX];
  char         apolloUrl[PATH_MAX];
  const char **envCmd;
  SArray      *pArgs;  // SConfigPair
  int64_t      startTime;
  bool         generateCode;
  char         encryptKey[ENCRYPT_KEY_LEN + 1];
} global = {0};

extern int32_t cryptLoadProviders();
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

static bool dmHasRepairCliOption(const SRepairCliArgs *pCliArgs) {
  if (pCliArgs == NULL) {
    return false;
  }

  return pCliArgs->hasNodeType || pCliArgs->hasFileType || pCliArgs->hasVnodeIdList || pCliArgs->hasBackupPath ||
         pCliArgs->hasMode || pCliArgs->hasReplicaNode;
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
    } else if (strcmp(argv[i], "--node-type") == 0 || strncmp(argv[i], "--node-type=", 12) == 0) {
      if ((i < argc - 1) || ((i == argc - 1) && strncmp(argv[i], "--node-type=", 12) == 0)) {
        int32_t     klen = strlen(argv[i]);
        int32_t     vlen = klen < 12 ? strlen(argv[++i]) : klen - 12;
        const char *val = argv[i];
        if (klen >= 12) val += 12;
        if (vlen <= 0) {
          printf("invalid value of '--node-type'\n");
          return TSDB_CODE_INVALID_CFG;
        }
        int32_t code = tRepairParseCliOption(&global.repairCliArgs, "node-type", val);
        if (code != TSDB_CODE_SUCCESS) {
          printf("invalid value of '--node-type': %s\n", val);
          return TSDB_CODE_INVALID_CFG;
        }
      } else {
        printf("'--node-type' requires a parameter\n");
        return TSDB_CODE_INVALID_CFG;
      }
    } else if (strcmp(argv[i], "--file-type") == 0 || strncmp(argv[i], "--file-type=", 12) == 0) {
      if ((i < argc - 1) || ((i == argc - 1) && strncmp(argv[i], "--file-type=", 12) == 0)) {
        int32_t     klen = strlen(argv[i]);
        int32_t     vlen = klen < 12 ? strlen(argv[++i]) : klen - 12;
        const char *val = argv[i];
        if (klen >= 12) val += 12;
        if (vlen <= 0) {
          printf("invalid value of '--file-type'\n");
          return TSDB_CODE_INVALID_CFG;
        }
        int32_t code = tRepairParseCliOption(&global.repairCliArgs, "file-type", val);
        if (code != TSDB_CODE_SUCCESS) {
          printf("invalid value of '--file-type': %s\n", val);
          return TSDB_CODE_INVALID_CFG;
        }
      } else {
        printf("'--file-type' requires a parameter\n");
        return TSDB_CODE_INVALID_CFG;
      }
    } else if (strcmp(argv[i], "--vnode-id") == 0 || strncmp(argv[i], "--vnode-id=", 11) == 0) {
      if ((i < argc - 1) || ((i == argc - 1) && strncmp(argv[i], "--vnode-id=", 11) == 0)) {
        int32_t     klen = strlen(argv[i]);
        int32_t     vlen = klen < 11 ? strlen(argv[++i]) : klen - 11;
        const char *val = argv[i];
        if (klen >= 11) val += 11;
        if (vlen <= 0) {
          printf("invalid value of '--vnode-id'\n");
          return TSDB_CODE_INVALID_CFG;
        }
        int32_t code = tRepairParseCliOption(&global.repairCliArgs, "vnode-id", val);
        if (code != TSDB_CODE_SUCCESS) {
          printf("invalid value of '--vnode-id': %s\n", val);
          return TSDB_CODE_INVALID_CFG;
        }
      } else {
        printf("'--vnode-id' requires a parameter\n");
        return TSDB_CODE_INVALID_CFG;
      }
    } else if (strcmp(argv[i], "--backup-path") == 0 || strncmp(argv[i], "--backup-path=", 14) == 0) {
      if ((i < argc - 1) || ((i == argc - 1) && strncmp(argv[i], "--backup-path=", 14) == 0)) {
        int32_t     klen = strlen(argv[i]);
        int32_t     vlen = klen < 14 ? strlen(argv[++i]) : klen - 14;
        const char *val = argv[i];
        if (klen >= 14) val += 14;
        if (vlen <= 0) {
          printf("invalid value of '--backup-path'\n");
          return TSDB_CODE_INVALID_CFG;
        }
        int32_t code = tRepairParseCliOption(&global.repairCliArgs, "backup-path", val);
        if (code != TSDB_CODE_SUCCESS) {
          printf("invalid value of '--backup-path': %s\n", val);
          return TSDB_CODE_INVALID_CFG;
        }
      } else {
        printf("'--backup-path' requires a parameter\n");
        return TSDB_CODE_INVALID_CFG;
      }
    } else if (strcmp(argv[i], "--mode") == 0 || strncmp(argv[i], "--mode=", 7) == 0) {
      if ((i < argc - 1) || ((i == argc - 1) && strncmp(argv[i], "--mode=", 7) == 0)) {
        int32_t     klen = strlen(argv[i]);
        int32_t     vlen = klen < 7 ? strlen(argv[++i]) : klen - 7;
        const char *val = argv[i];
        if (klen >= 7) val += 7;
        if (vlen <= 0) {
          printf("invalid value of '--mode'\n");
          return TSDB_CODE_INVALID_CFG;
        }
        int32_t code = tRepairParseCliOption(&global.repairCliArgs, "mode", val);
        if (code != TSDB_CODE_SUCCESS) {
          printf("invalid value of '--mode': %s\n", val);
          return TSDB_CODE_INVALID_CFG;
        }
      } else {
        printf("'--mode' requires a parameter\n");
        return TSDB_CODE_INVALID_CFG;
      }
    } else if (strcmp(argv[i], "--replica-node") == 0 || strncmp(argv[i], "--replica-node=", 15) == 0) {
      if ((i < argc - 1) || ((i == argc - 1) && strncmp(argv[i], "--replica-node=", 15) == 0)) {
        int32_t     klen = strlen(argv[i]);
        int32_t     vlen = klen < 15 ? strlen(argv[++i]) : klen - 15;
        const char *val = argv[i];
        if (klen >= 15) val += 15;
        if (vlen <= 0) {
          printf("invalid value of '--replica-node'\n");
          return TSDB_CODE_INVALID_CFG;
        }
        int32_t code = tRepairParseCliOption(&global.repairCliArgs, "replica-node", val);
        if (code != TSDB_CODE_SUCCESS) {
          printf("invalid value of '--replica-node': %s\n", val);
          return TSDB_CODE_INVALID_CFG;
        }
      } else {
        printf("'--replica-node' requires a parameter\n");
        return TSDB_CODE_INVALID_CFG;
      }
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

  if (dmHasRepairCliOption(&global.repairCliArgs)) {
    if (!generateNewMeta) {
      printf("repair options require '-r'\n");
      return TSDB_CODE_INVALID_CFG;
    }

    int32_t code = tRepairValidateCliArgs(&global.repairCliArgs);
    if (code != TSDB_CODE_SUCCESS) {
      printf("invalid repair option combination\n");
      return TSDB_CODE_INVALID_CFG;
    }

    code = tRepairInitCtx(&global.repairCliArgs, global.startTime, &global.repairCtx);
    if (code != TSDB_CODE_SUCCESS) {
      printf("failed to initialize repair context\n");
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
  printf("%s%s%s%s\n", indent, "-r,", indent, DM_REPAIR);
  printf("%s%s%s%s\n", indent, "--node-type=NODE_TYPE,", indent, DM_REPAIR_NODE_TYPE);
  printf("%s%s%s%s\n", indent, "--file-type=FILE_TYPE,", indent, DM_REPAIR_FILE_TYPE);
  printf("%s%s%s%s\n", indent, "--vnode-id=VNODE_IDS,", indent, DM_REPAIR_VNODE_ID);
  printf("%s%s%s%s\n", indent, "--backup-path=PATH,", indent, DM_REPAIR_BACKUP_PATH);
  printf("%s%s%s%s\n", indent, "--mode=MODE,", indent, DM_REPAIR_MODE);
  printf("%s%s%s%s\n", indent, "--replica-node=NODE,", indent, DM_REPAIR_REPLICA_NODE);

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

static int32_t dmRunForceWalRepair(int32_t totalVnodes, int64_t repairProgressIntervalMs, int64_t *pLastProgressReportMs,
                                   bool *pNeedReport, char *progressLine, int32_t progressLineSize) {
  if (pLastProgressReportMs == NULL || pNeedReport == NULL || progressLine == NULL || progressLineSize <= 0) {
    return TSDB_CODE_INVALID_CFG;
  }

  int32_t code = 0;
  bool    needRunWalForceRepair = false;
  code = tRepairNeedRunWalForceRepair(&global.repairCtx, &needRunWalForceRepair);
  if (code != TSDB_CODE_SUCCESS) {
    dError("failed to determine wal force repair schedule since %s", tstrerror(code));
    printf("failed repair session preparation: %s\n", tstrerror(code));
    return TSDB_CODE_INVALID_CFG;
  }

  if (!needRunWalForceRepair) {
    return TSDB_CODE_SUCCESS;
  }

  code = walInit(dmStopDaemon);
  if (code != TSDB_CODE_SUCCESS) {
    dError("failed to initialize wal module for repair since %s", tstrerror(code));
    printf("failed repair wal scheduling: %s\n", tstrerror(code));
    return TSDB_CODE_INVALID_CFG;
  }

  int32_t walDoneVnodes = 0;
  for (int32_t i = 0; i < global.repairCtx.vnodeIdNum; ++i) {
    int32_t vnodeId = global.repairCtx.vnodeIds[i];
    char    walPath[PATH_MAX] = {0};
    char    walBackupDir[PATH_MAX] = {0};
    code = tRepairBuildVnodeTargetPath(tsDataDir, vnodeId, REPAIR_FILE_TYPE_WAL, walPath, sizeof(walPath));
    if (code != TSDB_CODE_SUCCESS) {
      dError("failed to build wal path for vnode:%d since %s", vnodeId, tstrerror(code));
      printf("failed repair wal scheduling: %s\n", tstrerror(code));
      return TSDB_CODE_INVALID_CFG;
    }

    code = tRepairBackupVnodeTarget(&global.repairCtx, tsDataDir, vnodeId, walBackupDir, sizeof(walBackupDir));
    if (code != TSDB_CODE_SUCCESS) {
      dError("failed to backup wal target for vnode:%d since %s", vnodeId, tstrerror(code));
      printf("failed repair wal scheduling: %s\n", tstrerror(code));
      return TSDB_CODE_INVALID_CFG;
    }

    char backupLog[PATH_MAX] = {0};
    int32_t backupLogLen =
        tsnprintf(backupLog, sizeof(backupLog), "prepared wal backup for vnode:%d path:%s", vnodeId, walBackupDir);
    if (backupLogLen <= 0 || backupLogLen >= (int32_t)sizeof(backupLog)) {
      tstrncpy(backupLog, "prepared wal backup", sizeof(backupLog));
    }
    code = tRepairAppendSessionLog(global.repairLogPath, backupLog);
    if (code != TSDB_CODE_SUCCESS) {
      dError("failed to append wal backup log for vnode:%d since %s", vnodeId, tstrerror(code));
      printf("failed repair wal scheduling: %s\n", tstrerror(code));
      return TSDB_CODE_INVALID_CFG;
    }

    code = tRepairWriteSessionState(&global.repairCtx, global.repairStatePath, "wal", "running", walDoneVnodes,
                                    totalVnodes);
    if (code != TSDB_CODE_SUCCESS) {
      dError("failed to write wal repair state for vnode:%d since %s", vnodeId, tstrerror(code));
      printf("failed repair wal scheduling: %s\n", tstrerror(code));
      return TSDB_CODE_INVALID_CFG;
    }

    SWalCfg walCfg = {0};
    walCfg.vgId = vnodeId;
    walCfg.fsyncPeriod = 0;
    walCfg.retentionPeriod = -1;
    walCfg.retentionSize = -1;
    walCfg.level = TAOS_WAL_WRITE;

    SWal *pWal = walOpen(walPath, &walCfg);
    if (pWal == NULL) {
      int32_t walCode = terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
      dError("failed to repair wal for vnode:%d path:%s since %s", vnodeId, walPath, tstrerror(walCode));
      int32_t rollbackCode = tRepairRollbackVnodeTarget(&global.repairCtx, tsDataDir, vnodeId);
      if (rollbackCode != TSDB_CODE_SUCCESS) {
        dError("failed to rollback wal repair for vnode:%d since %s", vnodeId, tstrerror(rollbackCode));
        (void)tRepairAppendSessionLog(global.repairLogPath, "wal repair rollback failed");
      } else {
        char rollbackLog[128] = {0};
        int32_t rollbackLogLen = tsnprintf(rollbackLog, sizeof(rollbackLog), "rolled back wal for vnode:%d", vnodeId);
        if (rollbackLogLen <= 0 || rollbackLogLen >= (int32_t)sizeof(rollbackLog)) {
          tstrncpy(rollbackLog, "rolled back wal repair", sizeof(rollbackLog));
        }
        (void)tRepairAppendSessionLog(global.repairLogPath, rollbackLog);
      }
      (void)tRepairWriteSessionState(&global.repairCtx, global.repairStatePath, "wal", "failed", walDoneVnodes,
                                     totalVnodes);
      printf("failed repair wal scheduling: %s\n", tstrerror(walCode));
      return TSDB_CODE_INVALID_CFG;
    }

    SWalRepairStats repairStats = {0};
    code = walGetRepairStats(pWal, &repairStats);
    if (code != TSDB_CODE_SUCCESS) {
      walClose(pWal);
      dError("failed to query wal repair stats for vnode:%d since %s", vnodeId, tstrerror(code));
      printf("failed repair wal scheduling: %s\n", tstrerror(code));
      return TSDB_CODE_INVALID_CFG;
    }

    walClose(pWal);

    char walDetailLog[192] = {0};
    int32_t walDetailLen =
        tsnprintf(walDetailLog, sizeof(walDetailLog),
                  "wal repair detail: vnode=%d corruptedSegments=%d rebuiltIdxEntries=%" PRId64, vnodeId,
                  repairStats.corruptedSegments, repairStats.rebuiltIdxEntries);
    if (walDetailLen <= 0 || walDetailLen >= (int32_t)sizeof(walDetailLog)) {
      tstrncpy(walDetailLog, "wal repair detail unavailable", sizeof(walDetailLog));
    }
    code = tRepairAppendSessionLog(global.repairLogPath, walDetailLog);
    if (code != TSDB_CODE_SUCCESS) {
      dError("failed to append wal repair detail log for vnode:%d since %s", vnodeId, tstrerror(code));
      printf("failed repair wal scheduling: %s\n", tstrerror(code));
      return TSDB_CODE_INVALID_CFG;
    }

    ++walDoneVnodes;

    code = tRepairWriteSessionState(&global.repairCtx, global.repairStatePath, "wal", "running", walDoneVnodes,
                                    totalVnodes);
    if (code != TSDB_CODE_SUCCESS) {
      dError("failed to update wal repair state for vnode:%d since %s", vnodeId, tstrerror(code));
      printf("failed repair wal scheduling: %s\n", tstrerror(code));
      return TSDB_CODE_INVALID_CFG;
    }

    char walLog[128] = {0};
    int32_t walLogLen = tsnprintf(walLog, sizeof(walLog), "finished force wal repair for vnode:%d", vnodeId);
    if (walLogLen <= 0 || walLogLen >= (int32_t)sizeof(walLog)) {
      tstrncpy(walLog, "finished force wal repair", sizeof(walLog));
    }
    code = tRepairAppendSessionLog(global.repairLogPath, walLog);
    if (code != TSDB_CODE_SUCCESS) {
      dError("failed to append wal repair log for vnode:%d since %s", vnodeId, tstrerror(code));
      printf("failed repair wal scheduling: %s\n", tstrerror(code));
      return TSDB_CODE_INVALID_CFG;
    }

    code = tRepairNeedReportProgress(taosGetTimestampMs(), repairProgressIntervalMs, pLastProgressReportMs, pNeedReport);
    if (code != TSDB_CODE_SUCCESS) {
      dError("failed to update wal repair progress interval for vnode:%d since %s", vnodeId, tstrerror(code));
      printf("failed repair wal scheduling: %s\n", tstrerror(code));
      return TSDB_CODE_INVALID_CFG;
    }

    if (*pNeedReport || walDoneVnodes == totalVnodes) {
      code = tRepairBuildProgressLine(&global.repairCtx, "wal", walDoneVnodes, totalVnodes, progressLine,
                                      progressLineSize);
      if (code != TSDB_CODE_SUCCESS) {
        dError("failed to build wal repair progress line for vnode:%d since %s", vnodeId, tstrerror(code));
        printf("failed repair wal scheduling: %s\n", tstrerror(code));
        return TSDB_CODE_INVALID_CFG;
      }

      dInfo("%s", progressLine);
      printf("%s\n", progressLine);
      code = tRepairAppendSessionLog(global.repairLogPath, progressLine);
      if (code != TSDB_CODE_SUCCESS) {
        dError("failed to append wal repair progress log for vnode:%d since %s", vnodeId, tstrerror(code));
        printf("failed repair wal scheduling: %s\n", tstrerror(code));
        return TSDB_CODE_INVALID_CFG;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static void dmHandleTsdbRepairRollback(int32_t vnodeId, int32_t doneVnodes, int32_t totalVnodes) {
  int32_t rollbackCode = tRepairRollbackVnodeTarget(&global.repairCtx, tsDataDir, vnodeId);
  if (rollbackCode != TSDB_CODE_SUCCESS) {
    dError("failed to rollback tsdb repair for vnode:%d since %s", vnodeId, tstrerror(rollbackCode));
    (void)tRepairAppendSessionLog(global.repairLogPath, "tsdb repair rollback failed");
  } else {
    char rollbackLog[128] = {0};
    int32_t rollbackLogLen = tsnprintf(rollbackLog, sizeof(rollbackLog), "rolled back tsdb for vnode:%d", vnodeId);
    if (rollbackLogLen <= 0 || rollbackLogLen >= (int32_t)sizeof(rollbackLog)) {
      tstrncpy(rollbackLog, "rolled back tsdb repair", sizeof(rollbackLog));
    }
    (void)tRepairAppendSessionLog(global.repairLogPath, rollbackLog);
  }

  (void)tRepairWriteSessionState(&global.repairCtx, global.repairStatePath, "tsdb", "failed", doneVnodes, totalVnodes);
}

static int32_t dmRunForceTsdbRepair(int32_t totalVnodes, int64_t repairProgressIntervalMs, int64_t *pLastProgressReportMs,
                                    bool *pNeedReport, char *progressLine, int32_t progressLineSize) {
  if (pLastProgressReportMs == NULL || pNeedReport == NULL || progressLine == NULL || progressLineSize <= 0) {
    return TSDB_CODE_INVALID_CFG;
  }

  int32_t code = 0;
  bool    needRunTsdbForceRepair = false;
  code = tRepairNeedRunTsdbForceRepair(&global.repairCtx, &needRunTsdbForceRepair);
  if (code != TSDB_CODE_SUCCESS) {
    dError("failed to determine tsdb force repair schedule since %s", tstrerror(code));
    printf("failed repair tsdb scheduling: %s\n", tstrerror(code));
    return TSDB_CODE_INVALID_CFG;
  }

  if (!needRunTsdbForceRepair) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t tsdbDoneVnodes = 0;
  for (int32_t i = 0; i < global.repairCtx.vnodeIdNum; ++i) {
    int32_t vnodeId = global.repairCtx.vnodeIds[i];
    char    tsdbPath[PATH_MAX] = {0};
    char    tsdbBackupDir[PATH_MAX] = {0};
    char    rebuildDir[PATH_MAX] = {0};

    code = tRepairBuildVnodeTargetPath(tsDataDir, vnodeId, REPAIR_FILE_TYPE_TSDB, tsdbPath, sizeof(tsdbPath));
    if (code != TSDB_CODE_SUCCESS) {
      dError("failed to build tsdb path for vnode:%d since %s", vnodeId, tstrerror(code));
      printf("failed repair tsdb scheduling: %s\n", tstrerror(code));
      return TSDB_CODE_INVALID_CFG;
    }

    code = tRepairBackupVnodeTarget(&global.repairCtx, tsDataDir, vnodeId, tsdbBackupDir, sizeof(tsdbBackupDir));
    if (code != TSDB_CODE_SUCCESS) {
      dError("failed to backup tsdb target for vnode:%d since %s", vnodeId, tstrerror(code));
      printf("failed repair tsdb scheduling: %s\n", tstrerror(code));
      return TSDB_CODE_INVALID_CFG;
    }

    char backupLog[PATH_MAX] = {0};
    int32_t backupLogLen =
        tsnprintf(backupLog, sizeof(backupLog), "prepared tsdb backup for vnode:%d path:%s", vnodeId, tsdbBackupDir);
    if (backupLogLen <= 0 || backupLogLen >= (int32_t)sizeof(backupLog)) {
      tstrncpy(backupLog, "prepared tsdb backup", sizeof(backupLog));
    }
    code = tRepairAppendSessionLog(global.repairLogPath, backupLog);
    if (code != TSDB_CODE_SUCCESS) {
      dError("failed to append tsdb backup log for vnode:%d since %s", vnodeId, tstrerror(code));
      printf("failed repair tsdb scheduling: %s\n", tstrerror(code));
      return TSDB_CODE_INVALID_CFG;
    }

    code = tRepairWriteSessionState(&global.repairCtx, global.repairStatePath, "tsdb", "running", tsdbDoneVnodes,
                                    totalVnodes);
    if (code != TSDB_CODE_SUCCESS) {
      dError("failed to write tsdb repair state for vnode:%d since %s", vnodeId, tstrerror(code));
      printf("failed repair tsdb scheduling: %s\n", tstrerror(code));
      return TSDB_CODE_INVALID_CFG;
    }

    SRepairTsdbBlockReport analyzeReport = {0};
    code = tRepairAnalyzeTsdbBlocks(&global.repairCtx, tsDataDir, vnodeId, &analyzeReport);
    if (code != TSDB_CODE_SUCCESS) {
      dError("failed to analyze tsdb blocks for vnode:%d since %s", vnodeId, tstrerror(code));
      printf("failed repair tsdb scheduling: %s\n", tstrerror(code));
      return TSDB_CODE_INVALID_CFG;
    }

    char analyzeLog[192] = {0};
    int32_t analyzeLogLen = tsnprintf(analyzeLog, sizeof(analyzeLog),
                                      "tsdb analyze detail: vnode=%d totalBlocks=%d recoverableBlocks=%d "
                                      "corruptedBlocks=%d unknownFiles=%d",
                                      vnodeId, analyzeReport.totalBlocks, analyzeReport.recoverableBlocks,
                                      analyzeReport.corruptedBlocks, analyzeReport.unknownFiles);
    if (analyzeLogLen <= 0 || analyzeLogLen >= (int32_t)sizeof(analyzeLog)) {
      tstrncpy(analyzeLog, "tsdb analyze detail unavailable", sizeof(analyzeLog));
    }
    code = tRepairAppendSessionLog(global.repairLogPath, analyzeLog);
    if (code != TSDB_CODE_SUCCESS) {
      dError("failed to append tsdb analyze log for vnode:%d since %s", vnodeId, tstrerror(code));
      printf("failed repair tsdb scheduling: %s\n", tstrerror(code));
      return TSDB_CODE_INVALID_CFG;
    }

    int32_t rebuildDirLen = tsnprintf(rebuildDir, sizeof(rebuildDir), "%s.rebuild", tsdbPath);
    if (rebuildDirLen <= 0 || rebuildDirLen >= (int32_t)sizeof(rebuildDir)) {
      dError("failed to build tsdb rebuild path for vnode:%d", vnodeId);
      printf("failed repair tsdb scheduling: %s\n", tstrerror(TSDB_CODE_INVALID_PARA));
      return TSDB_CODE_INVALID_CFG;
    }

    SRepairTsdbBlockReport rebuildReport = {0};
    code = tRepairRebuildTsdbBlocks(&global.repairCtx, tsDataDir, vnodeId, rebuildDir, &rebuildReport);
    if (code != TSDB_CODE_SUCCESS) {
      dError("failed to rebuild tsdb blocks for vnode:%d since %s", vnodeId, tstrerror(code));
      printf("failed repair tsdb scheduling: %s\n", tstrerror(code));
      return TSDB_CODE_INVALID_CFG;
    }

    if (taosDirExist(tsdbPath)) {
      taosRemoveDir(tsdbPath);
    }

    if (taosRenameFile(rebuildDir, tsdbPath) != 0) {
      int32_t renameCode = terrno != 0 ? terrno : TSDB_CODE_INVALID_PARA;
      dError("failed to switch tsdb rebuild output for vnode:%d since %s", vnodeId, tstrerror(renameCode));
      dmHandleTsdbRepairRollback(vnodeId, tsdbDoneVnodes, totalVnodes);
      printf("failed repair tsdb scheduling: %s\n", tstrerror(renameCode));
      return TSDB_CODE_INVALID_CFG;
    }

    char tsdbDetailLog[192] = {0};
    int32_t tsdbDetailLen = tsnprintf(tsdbDetailLog, sizeof(tsdbDetailLog),
                                      "tsdb rebuild detail: vnode=%d recoverableBlocks=%d corruptedBlocks=%d "
                                      "reportedCorrupted=%d",
                                      vnodeId, rebuildReport.recoverableBlocks, rebuildReport.corruptedBlocks,
                                      rebuildReport.reportedCorruptedBlocks);
    if (tsdbDetailLen <= 0 || tsdbDetailLen >= (int32_t)sizeof(tsdbDetailLog)) {
      tstrncpy(tsdbDetailLog, "tsdb rebuild detail unavailable", sizeof(tsdbDetailLog));
    }
    code = tRepairAppendSessionLog(global.repairLogPath, tsdbDetailLog);
    if (code != TSDB_CODE_SUCCESS) {
      dError("failed to append tsdb rebuild detail log for vnode:%d since %s", vnodeId, tstrerror(code));
      printf("failed repair tsdb scheduling: %s\n", tstrerror(code));
      return TSDB_CODE_INVALID_CFG;
    }

    ++tsdbDoneVnodes;

    code = tRepairWriteSessionState(&global.repairCtx, global.repairStatePath, "tsdb", "running", tsdbDoneVnodes,
                                    totalVnodes);
    if (code != TSDB_CODE_SUCCESS) {
      dError("failed to update tsdb repair state for vnode:%d since %s", vnodeId, tstrerror(code));
      printf("failed repair tsdb scheduling: %s\n", tstrerror(code));
      return TSDB_CODE_INVALID_CFG;
    }

    char tsdbLog[128] = {0};
    int32_t tsdbLogLen = tsnprintf(tsdbLog, sizeof(tsdbLog), "finished force tsdb repair for vnode:%d", vnodeId);
    if (tsdbLogLen <= 0 || tsdbLogLen >= (int32_t)sizeof(tsdbLog)) {
      tstrncpy(tsdbLog, "finished force tsdb repair", sizeof(tsdbLog));
    }
    code = tRepairAppendSessionLog(global.repairLogPath, tsdbLog);
    if (code != TSDB_CODE_SUCCESS) {
      dError("failed to append tsdb repair log for vnode:%d since %s", vnodeId, tstrerror(code));
      printf("failed repair tsdb scheduling: %s\n", tstrerror(code));
      return TSDB_CODE_INVALID_CFG;
    }

    code = tRepairNeedReportProgress(taosGetTimestampMs(), repairProgressIntervalMs, pLastProgressReportMs, pNeedReport);
    if (code != TSDB_CODE_SUCCESS) {
      dError("failed to update tsdb repair progress interval for vnode:%d since %s", vnodeId, tstrerror(code));
      printf("failed repair tsdb scheduling: %s\n", tstrerror(code));
      return TSDB_CODE_INVALID_CFG;
    }

    if (*pNeedReport || tsdbDoneVnodes == totalVnodes) {
      code = tRepairBuildProgressLine(&global.repairCtx, "tsdb", tsdbDoneVnodes, totalVnodes, progressLine,
                                      progressLineSize);
      if (code != TSDB_CODE_SUCCESS) {
        dError("failed to build tsdb repair progress line for vnode:%d since %s", vnodeId, tstrerror(code));
        printf("failed repair tsdb scheduling: %s\n", tstrerror(code));
        return TSDB_CODE_INVALID_CFG;
      }

      dInfo("%s", progressLine);
      printf("%s\n", progressLine);
      code = tRepairAppendSessionLog(global.repairLogPath, progressLine);
      if (code != TSDB_CODE_SUCCESS) {
        dError("failed to append tsdb repair progress log for vnode:%d since %s", vnodeId, tstrerror(code));
        printf("failed repair tsdb scheduling: %s\n", tstrerror(code));
        return TSDB_CODE_INVALID_CFG;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t dmRunForceMetaRepair(int32_t totalVnodes, int64_t repairProgressIntervalMs, int64_t *pLastProgressReportMs,
                                    bool *pNeedReport, char *progressLine, int32_t progressLineSize) {
  if (pLastProgressReportMs == NULL || pNeedReport == NULL || progressLine == NULL || progressLineSize <= 0) {
    return TSDB_CODE_INVALID_CFG;
  }

  int32_t code = 0;
  bool    needRunMetaForceRepair = false;
  code = tRepairNeedRunMetaForceRepair(&global.repairCtx, &needRunMetaForceRepair);
  if (code != TSDB_CODE_SUCCESS) {
    dError("failed to determine meta force repair schedule since %s", tstrerror(code));
    printf("failed repair meta scheduling: %s\n", tstrerror(code));
    return TSDB_CODE_INVALID_CFG;
  }

  if (!needRunMetaForceRepair) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t metaDoneVnodes = 0;
  for (int32_t i = 0; i < global.repairCtx.vnodeIdNum; ++i) {
    int32_t vnodeId = global.repairCtx.vnodeIds[i];
    char    metaBackupDir[PATH_MAX] = {0};

    code = tRepairBackupVnodeTarget(&global.repairCtx, tsDataDir, vnodeId, metaBackupDir, sizeof(metaBackupDir));
    if (code != TSDB_CODE_SUCCESS) {
      dError("failed to backup meta target for vnode:%d since %s", vnodeId, tstrerror(code));
      printf("failed repair meta scheduling: %s\n", tstrerror(code));
      return TSDB_CODE_INVALID_CFG;
    }

    char backupLog[PATH_MAX] = {0};
    int32_t backupLogLen =
        tsnprintf(backupLog, sizeof(backupLog), "prepared meta backup for vnode:%d path:%s", vnodeId, metaBackupDir);
    if (backupLogLen <= 0 || backupLogLen >= (int32_t)sizeof(backupLog)) {
      tstrncpy(backupLog, "prepared meta backup", sizeof(backupLog));
    }
    code = tRepairAppendSessionLog(global.repairLogPath, backupLog);
    if (code != TSDB_CODE_SUCCESS) {
      dError("failed to append meta backup log for vnode:%d since %s", vnodeId, tstrerror(code));
      printf("failed repair meta scheduling: %s\n", tstrerror(code));
      return TSDB_CODE_INVALID_CFG;
    }

    code = tRepairWriteSessionState(&global.repairCtx, global.repairStatePath, "meta", "running", metaDoneVnodes,
                                    totalVnodes);
    if (code != TSDB_CODE_SUCCESS) {
      dError("failed to write meta repair state for vnode:%d since %s", vnodeId, tstrerror(code));
      printf("failed repair meta scheduling: %s\n", tstrerror(code));
      return TSDB_CODE_INVALID_CFG;
    }

    SRepairMetaScanResult scanResult = {0};
    code = tRepairScanMetaFiles(&global.repairCtx, tsDataDir, vnodeId, &scanResult);
    if (code != TSDB_CODE_SUCCESS) {
      (void)tRepairWriteSessionState(&global.repairCtx, global.repairStatePath, "meta", "failed", metaDoneVnodes,
                                     totalVnodes);
      dError("failed to scan meta files for vnode:%d since %s", vnodeId, tstrerror(code));
      printf("failed repair meta scheduling: %s\n", tstrerror(code));
      return TSDB_CODE_INVALID_CFG;
    }

    char metaDetailLog[192] = {0};
    int32_t metaDetailLen =
        tsnprintf(metaDetailLog, sizeof(metaDetailLog),
                  "meta scan detail: vnode=%d required=%d presentRequired=%d optional=%d missingRequired=%d", vnodeId,
                  scanResult.requiredFiles, scanResult.presentRequiredFiles, scanResult.optionalIndexFiles,
                  scanResult.missingRequiredFiles);
    if (metaDetailLen <= 0 || metaDetailLen >= (int32_t)sizeof(metaDetailLog)) {
      tstrncpy(metaDetailLog, "meta scan detail unavailable", sizeof(metaDetailLog));
    }
    code = tRepairAppendSessionLog(global.repairLogPath, metaDetailLog);
    if (code != TSDB_CODE_SUCCESS) {
      dError("failed to append meta scan detail log for vnode:%d since %s", vnodeId, tstrerror(code));
      printf("failed repair meta scheduling: %s\n", tstrerror(code));
      return TSDB_CODE_INVALID_CFG;
    }

    ++metaDoneVnodes;

    code = tRepairWriteSessionState(&global.repairCtx, global.repairStatePath, "meta", "running", metaDoneVnodes,
                                    totalVnodes);
    if (code != TSDB_CODE_SUCCESS) {
      dError("failed to update meta repair state for vnode:%d since %s", vnodeId, tstrerror(code));
      printf("failed repair meta scheduling: %s\n", tstrerror(code));
      return TSDB_CODE_INVALID_CFG;
    }

    char metaLog[128] = {0};
    int32_t metaLogLen = tsnprintf(metaLog, sizeof(metaLog), "finished force meta scan for vnode:%d", vnodeId);
    if (metaLogLen <= 0 || metaLogLen >= (int32_t)sizeof(metaLog)) {
      tstrncpy(metaLog, "finished force meta scan", sizeof(metaLog));
    }
    code = tRepairAppendSessionLog(global.repairLogPath, metaLog);
    if (code != TSDB_CODE_SUCCESS) {
      dError("failed to append meta repair log for vnode:%d since %s", vnodeId, tstrerror(code));
      printf("failed repair meta scheduling: %s\n", tstrerror(code));
      return TSDB_CODE_INVALID_CFG;
    }

    code = tRepairNeedReportProgress(taosGetTimestampMs(), repairProgressIntervalMs, pLastProgressReportMs, pNeedReport);
    if (code != TSDB_CODE_SUCCESS) {
      dError("failed to update meta repair progress interval for vnode:%d since %s", vnodeId, tstrerror(code));
      printf("failed repair meta scheduling: %s\n", tstrerror(code));
      return TSDB_CODE_INVALID_CFG;
    }

    if (*pNeedReport || metaDoneVnodes == totalVnodes) {
      code = tRepairBuildProgressLine(&global.repairCtx, "meta", metaDoneVnodes, totalVnodes, progressLine,
                                      progressLineSize);
      if (code != TSDB_CODE_SUCCESS) {
        dError("failed to build meta repair progress line for vnode:%d since %s", vnodeId, tstrerror(code));
        printf("failed repair meta scheduling: %s\n", tstrerror(code));
        return TSDB_CODE_INVALID_CFG;
      }

      dInfo("%s", progressLine);
      printf("%s\n", progressLine);
      code = tRepairAppendSessionLog(global.repairLogPath, progressLine);
      if (code != TSDB_CODE_SUCCESS) {
        dError("failed to append meta repair progress log for vnode:%d since %s", vnodeId, tstrerror(code));
        printf("failed repair meta scheduling: %s\n", tstrerror(code));
        return TSDB_CODE_INVALID_CFG;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t dmRunRepairWorkflow(void) {
  const int64_t kRepairProgressIntervalMs = 3000;
  int64_t       lastProgressReportMs = 0;
  int32_t       totalVnodes = global.repairCtx.nodeType == REPAIR_NODE_TYPE_VNODE ? global.repairCtx.vnodeIdNum : 0;
  int32_t       doneVnodes = 0;
  bool          resumed = false;
  bool          needReport = false;
  int64_t       minDiskAvailBytes = tsDataSpace.reserved > 0 ? tsDataSpace.reserved : 0;
  char          progressLine[PATH_MAX] = {0};
  int32_t       code = tRepairPrecheck(&global.repairCtx, tsDataDir, minDiskAvailBytes);
  if (code != TSDB_CODE_SUCCESS) {
    dError("failed to run repair precheck since %s", tstrerror(code));
    printf("failed repair precheck: %s\n", tstrerror(code));
    return TSDB_CODE_INVALID_CFG;
  }

  code = tRepairTryResumeSession(&global.repairCtx, tsDataDir, global.repairSessionDir, sizeof(global.repairSessionDir),
                                 global.repairLogPath, sizeof(global.repairLogPath), global.repairStatePath,
                                 sizeof(global.repairStatePath), &doneVnodes, &totalVnodes, &resumed);
  if (code != TSDB_CODE_SUCCESS) {
    dError("failed to load repair resume session since %s", tstrerror(code));
    printf("failed repair session preparation: %s\n", tstrerror(code));
    return TSDB_CODE_INVALID_CFG;
  }

  if (global.repairCtx.nodeType == REPAIR_NODE_TYPE_VNODE &&
      (totalVnodes != global.repairCtx.vnodeIdNum || doneVnodes < 0 || doneVnodes > totalVnodes)) {
    dError("invalid repair resume state, doneVnodes:%d totalVnodes:%d vnodeIdNum:%d", doneVnodes, totalVnodes,
           global.repairCtx.vnodeIdNum);
    printf("failed repair session preparation: invalid resume state\n");
    return TSDB_CODE_INVALID_CFG;
  }

  if (!resumed) {
    code = tRepairPrepareSessionFiles(&global.repairCtx, tsDataDir, global.repairSessionDir, sizeof(global.repairSessionDir),
                                      global.repairLogPath, sizeof(global.repairLogPath), global.repairStatePath,
                                      sizeof(global.repairStatePath));
    if (code != TSDB_CODE_SUCCESS) {
      dError("failed to prepare repair session files since %s", tstrerror(code));
      printf("failed repair session preparation: %s\n", tstrerror(code));
      return TSDB_CODE_INVALID_CFG;
    }
  } else {
    char resumeMessage[128] = {0};
    int32_t resumeLen = tsnprintf(resumeMessage, sizeof(resumeMessage), "repair session resumed: session=%s vnode=%d/%d",
                                  global.repairCtx.sessionId, doneVnodes, totalVnodes);
    if (resumeLen <= 0 || resumeLen >= (int32_t)sizeof(resumeMessage)) {
      tstrncpy(resumeMessage, "repair session resumed", sizeof(resumeMessage));
    }

    dInfo("%s", resumeMessage);
    printf("%s\n", resumeMessage);
    code = tRepairAppendSessionLog(global.repairLogPath, resumeMessage);
    if (code != TSDB_CODE_SUCCESS) {
      dError("failed to append repair resume log since %s", tstrerror(code));
      printf("failed repair session preparation: %s\n", tstrerror(code));
      return TSDB_CODE_INVALID_CFG;
    }
  }

  code = tRepairWriteSessionState(&global.repairCtx, global.repairStatePath, "precheck", "running", doneVnodes,
                                  totalVnodes);
  if (code != TSDB_CODE_SUCCESS) {
    dError("failed to write repair precheck state since %s", tstrerror(code));
    printf("failed repair session preparation: %s\n", tstrerror(code));
    return TSDB_CODE_INVALID_CFG;
  }

  code = tRepairAppendSessionLog(global.repairLogPath, "repair precheck passed");
  if (code != TSDB_CODE_SUCCESS) {
    dError("failed to append repair precheck log since %s", tstrerror(code));
    printf("failed repair session preparation: %s\n", tstrerror(code));
    return TSDB_CODE_INVALID_CFG;
  }

  code = tRepairBuildProgressLine(&global.repairCtx, "precheck", doneVnodes, totalVnodes, progressLine,
                                  sizeof(progressLine));
  if (code != TSDB_CODE_SUCCESS) {
    dError("failed to build repair progress line since %s", tstrerror(code));
    printf("failed repair session preparation: %s\n", tstrerror(code));
    return TSDB_CODE_INVALID_CFG;
  }
  dInfo("%s", progressLine);
  printf("%s\n", progressLine);

  code = tRepairAppendSessionLog(global.repairLogPath, progressLine);
  if (code != TSDB_CODE_SUCCESS) {
    dError("failed to append repair progress log since %s", tstrerror(code));
    printf("failed repair session preparation: %s\n", tstrerror(code));
    return TSDB_CODE_INVALID_CFG;
  }

  code = tRepairNeedReportProgress(taosGetTimestampMs(), kRepairProgressIntervalMs, &lastProgressReportMs, &needReport);
  if (code != TSDB_CODE_SUCCESS) {
    dError("failed to initialize repair progress interval since %s", tstrerror(code));
    printf("failed repair session preparation: %s\n", tstrerror(code));
    return TSDB_CODE_INVALID_CFG;
  }

  if (global.repairCtx.nodeType == REPAIR_NODE_TYPE_VNODE) {
    int32_t startVnodeIndex = doneVnodes;
    if (startVnodeIndex < 0 || startVnodeIndex > global.repairCtx.vnodeIdNum) {
      dError("invalid repair resume vnode index:%d vnodeIdNum:%d", startVnodeIndex, global.repairCtx.vnodeIdNum);
      printf("failed repair backup preparation: invalid resume state\n");
      return TSDB_CODE_INVALID_CFG;
    }

    for (int32_t i = startVnodeIndex; i < global.repairCtx.vnodeIdNum; ++i) {
      char backupDir[PATH_MAX] = {0};
      code = tRepairPrepareBackupDir(&global.repairCtx, tsDataDir, global.repairCtx.vnodeIds[i], backupDir,
                                     sizeof(backupDir));
      if (code != TSDB_CODE_SUCCESS) {
        dError("failed to prepare repair backup dir for vnode:%d since %s", global.repairCtx.vnodeIds[i],
               tstrerror(code));
        printf("failed repair backup preparation: %s\n", tstrerror(code));
        return TSDB_CODE_INVALID_CFG;
      }

      ++doneVnodes;
      code = tRepairWriteSessionState(&global.repairCtx, global.repairStatePath, "backup", "running", doneVnodes,
                                      totalVnodes);
      if (code != TSDB_CODE_SUCCESS) {
        dError("failed to write repair backup state for vnode:%d since %s", global.repairCtx.vnodeIds[i],
               tstrerror(code));
        printf("failed repair backup preparation: %s\n", tstrerror(code));
        return TSDB_CODE_INVALID_CFG;
      }

      char logMessage[128] = {0};
      int32_t logLen =
          tsnprintf(logMessage, sizeof(logMessage), "prepared backup dir for vnode:%d", global.repairCtx.vnodeIds[i]);
      if (logLen <= 0 || logLen >= (int32_t)sizeof(logMessage)) {
        tstrncpy(logMessage, "prepared backup dir", sizeof(logMessage));
      }

      code = tRepairAppendSessionLog(global.repairLogPath, logMessage);
      if (code != TSDB_CODE_SUCCESS) {
        dError("failed to append repair backup log for vnode:%d since %s", global.repairCtx.vnodeIds[i], tstrerror(code));
        printf("failed repair backup preparation: %s\n", tstrerror(code));
        return TSDB_CODE_INVALID_CFG;
      }

      code = tRepairNeedReportProgress(taosGetTimestampMs(), kRepairProgressIntervalMs, &lastProgressReportMs, &needReport);
      if (code != TSDB_CODE_SUCCESS) {
        dError("failed to update repair progress interval for vnode:%d since %s", global.repairCtx.vnodeIds[i],
               tstrerror(code));
        printf("failed repair backup preparation: %s\n", tstrerror(code));
        return TSDB_CODE_INVALID_CFG;
      }

      if (needReport || doneVnodes == totalVnodes) {
        code = tRepairBuildProgressLine(&global.repairCtx, "backup", doneVnodes, totalVnodes, progressLine,
                                        sizeof(progressLine));
        if (code != TSDB_CODE_SUCCESS) {
          dError("failed to build repair backup progress line for vnode:%d since %s", global.repairCtx.vnodeIds[i],
                 tstrerror(code));
          printf("failed repair backup preparation: %s\n", tstrerror(code));
          return TSDB_CODE_INVALID_CFG;
        }

        dInfo("%s", progressLine);
        printf("%s\n", progressLine);
        code = tRepairAppendSessionLog(global.repairLogPath, progressLine);
        if (code != TSDB_CODE_SUCCESS) {
          dError("failed to append repair backup progress log for vnode:%d since %s", global.repairCtx.vnodeIds[i],
                 tstrerror(code));
          printf("failed repair backup preparation: %s\n", tstrerror(code));
          return TSDB_CODE_INVALID_CFG;
        }
      }
    }
  }

  code = dmRunForceWalRepair(totalVnodes, kRepairProgressIntervalMs, &lastProgressReportMs, &needReport, progressLine,
                             sizeof(progressLine));
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = dmRunForceTsdbRepair(totalVnodes, kRepairProgressIntervalMs, &lastProgressReportMs, &needReport, progressLine,
                              sizeof(progressLine));
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = dmRunForceMetaRepair(totalVnodes, kRepairProgressIntervalMs, &lastProgressReportMs, &needReport, progressLine,
                              sizeof(progressLine));
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = tRepairWriteSessionState(&global.repairCtx, global.repairStatePath, "preflight", "ready", doneVnodes,
                                  totalVnodes);
  if (code != TSDB_CODE_SUCCESS) {
    dError("failed to write repair session state since %s", tstrerror(code));
    printf("failed repair session preparation: %s\n", tstrerror(code));
    return TSDB_CODE_INVALID_CFG;
  }

  int32_t failedVnodes = totalVnodes >= doneVnodes ? (totalVnodes - doneVnodes) : 0;
  int64_t elapsedMs = taosGetTimestampMs() - global.repairCtx.startTimeMs;
  char    summaryLine[PATH_MAX] = {0};
  code =
      tRepairBuildSummaryLine(&global.repairCtx, doneVnodes, failedVnodes, elapsedMs, summaryLine, sizeof(summaryLine));
  if (code != TSDB_CODE_SUCCESS) {
    dError("failed to build repair summary line since %s", tstrerror(code));
    printf("failed repair session preparation: %s\n", tstrerror(code));
    return TSDB_CODE_INVALID_CFG;
  }

  dInfo("%s", summaryLine);
  printf("%s\n", summaryLine);
  code = tRepairAppendSessionLog(global.repairLogPath, summaryLine);
  if (code != TSDB_CODE_SUCCESS) {
    dError("failed to append repair summary log since %s", tstrerror(code));
    printf("failed repair session preparation: %s\n", tstrerror(code));
    return TSDB_CODE_INVALID_CFG;
  }

  return TSDB_CODE_SUCCESS;
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
  if ((code = taosPreLoadCfg(configDir, global.envCmd, global.envFile, global.apolloUrl, global.pArgs, 0)) != 0) {
    dError("failed to start since pre load config error");
    taosCloseLog();
    taosCleanupArgs();
    return code;
  }

  if ((code = dmGetEncryptKey()) != 0) {
    dError("failed to start since failed to get encrypt key");
    taosCloseLog();
    taosCleanupArgs();
    return code;
  };

  if ((code = tryLoadCfgFromDataDir(tsCfg)) != 0) {
    dError("failed to start since try load config from data dir error");
    taosCloseLog();
    taosCleanupArgs();
    return code;
  }

  if ((code = taosApplyCfg(configDir, global.envCmd, global.envFile, global.apolloUrl, global.pArgs, 0)) != 0) {
    dError("failed to start since apply config error");
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
    tsSkipKeyCheckMode = true;  // Set global flag to skip key check mode
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

  if (global.repairCtx.enabled) {
    code = dmRunRepairWorkflow();
    if (code != TSDB_CODE_SUCCESS) {
      taosCleanupCfg();
      taosCloseLog();
      taosCleanupArgs();
      taosConvDestroy();
      return code;
    }
  }

  osSetProcPath(argc, (char **)argv);
  taosCleanupArgs();

  if (tsEncryptExtDir[0] != '\0') {
#if defined(TD_ENTERPRISE) && defined(LINUX)
    if ((code = cryptLoadProviders()) != 0) {
      dError("failed to load encrypt providers since %s", tstrerror(code));
      taosCloseLog();
      taosCleanupArgs();
      return code;
    }
#endif
  }

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
