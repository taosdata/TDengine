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

#include <ctype.h>

#include "dmMgmt.h"
#include "dmUtil.h"
#include "mnode.h"
#include "osEnv.h"
#include "osFile.h"
#include "qworker.h"
#include "tconfig.h"
#include "tconv.h"
#include "tglobal.h"
#include "tss.h"
#include "version.h"

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
#define DM_REPAIR_MODE   "Start repair mode."

// clang-format on
typedef struct {
  bool withR;                 // -r
  bool hasRepairArgs;         //
  bool hasNodeType;           // --node-type
  bool hasFileType;           // --file-type
  bool hasVnodeId;            // --vnode-id
  bool hasBackupPath;         // --backup-path
  bool hasMode;               // --mode
  bool hasReplicaNode;        // --replica-node
  char nodeType[32];          // --node-type(vnode, mnode, dnode, snode)
  char fileType[32];          // --file-type
                              //   vnode: wal, tsdb, meta
                              //   mnode: wal, data
                              //   dnode: config
                              //   snode: checkpoint
  char vnodeId[PATH_MAX];     // --vnode-id: 如 1,2,36
  char backupPath[PATH_MAX];  // --backup-path
  char mode[32];              // --mode
                              //   force: single node recovery mode. (Recovery as mush data as possible with local info)
                              //   replica: replica recovery mode
                              //   copy: Copy from backup data
  char replicaNode[PATH_MAX];  // --replica-node:
} SDmRepairOption;

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
  bool         printRepairHelp;
  char         envFile[PATH_MAX];
  char         apolloUrl[PATH_MAX];
  const char **envCmd;
  SArray      *pArgs;  // SConfigPair
  int64_t      startTime;
  bool         generateCode;
  bool         runRepairFlow;
  char         encryptKey[ENCRYPT_KEY_LEN + 1];
  SDmRepairOption repairOpt;
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

static bool dmMatchLongOption(const char *arg, const char *opt, const char **pVal) {
  int32_t optLen = (int32_t)strlen(opt);
  if (strncmp(arg, opt, optLen) != 0) {
    return false;
  }

  if (arg[optLen] == '\0') {
    *pVal = NULL;
    return true;
  }

  if (arg[optLen] == '=') {
    *pVal = arg + optLen + 1;
    return true;
  }

  return false;
}

static int32_t dmParseLongOptionValue(int32_t argc, char const *argv[], int32_t *pIndex, const char *opt, char *buf,
                                      int32_t bufLen, bool *pMatched) {
  const char *val = NULL;
  *pMatched = false;
  if (!dmMatchLongOption(argv[*pIndex], opt, &val)) {
    return TSDB_CODE_SUCCESS;
  }
  *pMatched = true;

  if (val == NULL) {
    if (*pIndex >= argc - 1 || argv[*pIndex + 1][0] == '-') {
      printf("'%s' requires a parameter\n", opt);
      return TSDB_CODE_INVALID_PARA;
    }
    val = argv[++(*pIndex)];
  }

  int32_t vLen = (int32_t)strlen(val);
  if (vLen <= 0 || vLen >= bufLen) {
    printf("invalid value for '%s'\n", opt);
    return TSDB_CODE_INVALID_PARA;
  }

  tstrncpy(buf, val, bufLen);
  return TSDB_CODE_SUCCESS;
}

static bool dmIsValidVnodeId(const char *vnodeId) {
  if (vnodeId == NULL || vnodeId[0] == '\0') {
    return false;
  }

  bool expectDigit = true;
  for (const unsigned char *p = (const unsigned char *)vnodeId; *p != '\0'; ++p) {
    if (isdigit(*p)) {
      expectDigit = false;
      continue;
    }

    if (*p == ',') {
      if (expectDigit) {
        return false;
      }
      expectDigit = true;
      continue;
    }

    return false;
  }

  return !expectDigit;
}

static int32_t dmValidateRepairOption() {
  SDmRepairOption *pOpt = &global.repairOpt;

  if (!pOpt->hasNodeType) {
    printf("missing '--node-type' in repair mode\n");
    return TSDB_CODE_INVALID_PARA;
  }
  if (strcmp(pOpt->nodeType, "vnode") != 0) {
    printf("'--node-type %s' is not supported in this phase\n", pOpt->nodeType);
    return TSDB_CODE_OPS_NOT_SUPPORT;
  }

  if (!pOpt->hasFileType) {
    printf("missing '--file-type' in repair mode\n");
    return TSDB_CODE_INVALID_PARA;
  }
  if (strcmp(pOpt->fileType, "wal") != 0 && strcmp(pOpt->fileType, "tsdb") != 0 && strcmp(pOpt->fileType, "meta") != 0) {
    printf("'--file-type %s' is not supported in this phase\n", pOpt->fileType);
    return TSDB_CODE_OPS_NOT_SUPPORT;
  }

  if (!pOpt->hasMode) {
    printf("missing '--mode' in repair mode\n");
    return TSDB_CODE_INVALID_PARA;
  }

  bool isForce = strcmp(pOpt->mode, "force") == 0;
  bool isReplica = strcmp(pOpt->mode, "replica") == 0;
  bool isCopy = strcmp(pOpt->mode, "copy") == 0;
  if (!isForce && !isReplica && !isCopy) {
    printf("invalid '--mode %s', valid options are force|replica|copy\n", pOpt->mode);
    return TSDB_CODE_INVALID_PARA;
  }

  if (!isCopy && pOpt->hasReplicaNode) {
    printf("'--replica-node' is only valid when '--mode copy' is used\n");
    return TSDB_CODE_INVALID_PARA;
  }

  if (isReplica || isCopy) {
    printf("'--mode %s' is reserved and not supported in this phase\n", pOpt->mode);
    return TSDB_CODE_OPS_NOT_SUPPORT;
  }

  bool allowAllVnodes = strcmp(pOpt->fileType, "meta") == 0 && isForce;
  if (!pOpt->hasVnodeId && !allowAllVnodes) {
    printf("missing '--vnode-id' in repair mode\n");
    return TSDB_CODE_INVALID_PARA;
  }
  if (pOpt->hasVnodeId && !dmIsValidVnodeId(pOpt->vnodeId)) {
    printf("invalid '--vnode-id' format, only digits and comma-separated digits are allowed\n");
    return TSDB_CODE_INVALID_PARA;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t dmParseRepairOption(int32_t argc, char const *argv[], int32_t *pIndex, bool *pParsed) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  index = *pIndex;
  const char *arg = argv[index];
  bool        matched = false;
  bool        optMatched = false;
  SDmRepairOption *pOpt = &global.repairOpt;

  *pParsed = false;

  code = dmParseLongOptionValue(argc, argv, &index, "--node-type", pOpt->nodeType, sizeof(pOpt->nodeType),
                                &optMatched);
  if (code != 0) return code;
  if (optMatched) {
    pOpt->hasRepairArgs = true;
    pOpt->hasNodeType = true;
    matched = true;
  }

  if (!matched) {
    code = dmParseLongOptionValue(argc, argv, &index, "--file-type", pOpt->fileType, sizeof(pOpt->fileType),
                                  &optMatched);
    if (code != 0) return code;
    if (optMatched) {
      pOpt->hasRepairArgs = true;
      pOpt->hasFileType = true;
      matched = true;
    }
  }

  if (!matched) {
    code = dmParseLongOptionValue(argc, argv, &index, "--vnode-id", pOpt->vnodeId, sizeof(pOpt->vnodeId), &optMatched);
    if (code != 0) return code;
    if (optMatched) {
      pOpt->hasRepairArgs = true;
      pOpt->hasVnodeId = true;
      matched = true;
    }
  }

  if (!matched) {
    code = dmParseLongOptionValue(argc, argv, &index, "--backup-path", pOpt->backupPath, sizeof(pOpt->backupPath),
                                  &optMatched);
    if (code != 0) return code;
    if (optMatched) {
      pOpt->hasRepairArgs = true;
      pOpt->hasBackupPath = true;
      matched = true;
    }
  }

  if (!matched) {
    code = dmParseLongOptionValue(argc, argv, &index, "--mode", pOpt->mode, sizeof(pOpt->mode), &optMatched);
    if (code != 0) return code;
    if (optMatched) {
      pOpt->hasRepairArgs = true;
      pOpt->hasMode = true;
      matched = true;
    }
  }

  if (!matched) {
    code = dmParseLongOptionValue(argc, argv, &index, "--replica-node", pOpt->replicaNode, sizeof(pOpt->replicaNode),
                                  &optMatched);
    if (code != 0) return code;
    if (optMatched) {
      pOpt->hasRepairArgs = true;
      pOpt->hasReplicaNode = true;
      matched = true;
    }
  }

  if (matched) {
    *pParsed = true;
    *pIndex = index;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t dmFinalizeRepairOption() {
  SDmRepairOption *pOpt = &global.repairOpt;
  global.runRepairFlow = false;

  if (pOpt->hasRepairArgs && !pOpt->withR) {
    printf("repair options must be used with '-r'\n");
    return TSDB_CODE_INVALID_PARA;
  }

  if (global.printHelp && pOpt->withR) {
    global.printRepairHelp = true;
    generateNewMeta = false;
    return TSDB_CODE_SUCCESS;
  }

  if (!pOpt->withR) {
    return TSDB_CODE_SUCCESS;
  }

  if (!pOpt->hasRepairArgs) {
    generateNewMeta = true;
    return TSDB_CODE_SUCCESS;
  }

  generateNewMeta = false;
  int32_t code = dmValidateRepairOption();
  if (code == TSDB_CODE_SUCCESS) {
    global.runRepairFlow = true;
    return TSDB_CODE_SUCCESS;
  }

  if (code == TSDB_CODE_OPS_NOT_SUPPORT) {
    return 1;
  }

  return code;
}

bool dmRepairFlowEnabled() { return global.runRepairFlow; }

const char *dmRepairNodeType() { return global.repairOpt.nodeType; }

const char *dmRepairFileType() { return global.repairOpt.fileType; }

const char *dmRepairMode() { return global.repairOpt.mode; }

bool dmRepairHasVnodeId() { return global.repairOpt.hasVnodeId; }

const char *dmRepairVnodeId() { return global.repairOpt.vnodeId; }

bool dmRepairHasBackupPath() { return global.repairOpt.hasBackupPath; }

const char *dmRepairBackupPath() { return global.repairOpt.backupPath; }

static int32_t dmParseArgs(int32_t argc, char const *argv[]) {
  global.startTime = taosGetTimestampMs();
  generateNewMeta = false;
  memset(&global.repairOpt, 0, sizeof(global.repairOpt));

  int32_t cmdEnvIndex = 0;
  if (argc < 2) return 0;

  global.envCmd = taosMemoryMalloc((argc - 1) * sizeof(char *));
  if (global.envCmd == NULL) {
    return terrno;
  }
  memset(global.envCmd, 0, (argc - 1) * sizeof(char *));
  for (int32_t i = 1; i < argc; ++i) {
    bool    parsedRepairOpt = false;
    int32_t code = dmParseRepairOption(argc, argv, &i, &parsedRepairOpt);
    if (code != 0) {
      return code;
    }
    if (parsedRepairOpt) {
      continue;
    }

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
      global.repairOpt.withR = true;
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

  return dmFinalizeRepairOption();
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
  printf("%s%s%s%s\n", indent, "-r,", indent, DM_REPAIR_MODE);
  printf("%s%s%s%s\n", indent, "-k,", indent, DM_MACHINE_CODE);
#if defined(LINUX)
  printf("%s%s%s%s\n", indent, "-o, --log-output=OUTPUT", indent, DM_LOG_OUTPUT);
#endif
  printf("%s%s%s%s\n", indent, "-y,", indent, DM_SET_ENCRYPTKEY);
  printf("%s%s%s%s\n", indent, "-dm,", indent, DM_MEM_DBG);
  printf("%s%s%s%s\n", indent, "-V,", indent, DM_VERSION);

  printf("\n\nReport bugs to %s.\n", DM_EMAIL);
}

static void dmPrintRepairHelp() {
  printf("Usage: %sd -r [--node-type NODE_TYPE] [--file-type FILE_TYPE] [--vnode-id IDS]\n", CUS_PROMPT);
  printf("              [--backup-path PATH] [--mode MODE] [--replica-node NODE]\n\n");

  printf("Compatibility\n");
  printf("  1) '%sd -r' keeps legacy metadata rebuild when no new repair option is provided.\n", CUS_PROMPT);
  printf("  2) Any new repair option switches to the new repair parameter flow.\n");

  printf("Phase1 scope\n");
  printf("  --node-type: vnode (only)\n");
  printf("  --file-type: wal | tsdb | meta\n");
  printf("  --mode: force (replica/copy are not supported in this phase)\n");
  printf("  --vnode-id: comma-separated numeric ids, e.g. 2,3\n");
  printf("  --backup-path: path or 'none'\n");
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

  if (global.printRepairHelp) {
    dmPrintRepairHelp();
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
