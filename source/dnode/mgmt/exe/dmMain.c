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
#include "crypt.h"

#if defined(CUS_NAME) || defined(CUS_PROMPT) || defined(CUS_EMAIL)
#include "cus_name.h"
#else
#ifndef CUS_NAME
    #define CUS_NAME      "TDengine"
#endif

#ifndef CUS_PROMPT
    #define CUS_PROMPT    "taos"
#endif

#ifndef CUS_EMAIL
    #define CUS_EMAIL     "<support@taosdata.com>"
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
#define DM_SET_ENCRYPTKEY  "Set encrypt key. such as: -e 1234567890abcdef, the length should be less or equal to 16."
#define DM_KEY_INDICATOR "this indicator!"
#define DM_ENCRYPT_CODE_FILE  "encryptCode.cfg"
#define DM_CHECK_CODE_FILE    "checkCode.bin"
// clang-format on
static struct {
#ifdef WINDOWS
  bool winServiceMode;
#endif
  bool         dumpConfig;
  bool         dumpSdb;
  bool         generateGrant;
  bool         memDbg;
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

static void dmSetDebugFlag(int32_t signum, void *sigInfo, void *context) { taosSetGlobalDebugFlag(143); }
static void dmSetAssert(int32_t signum, void *sigInfo, void *context) { tsAssert = 1; }

static void dmStopDnode(int signum, void *sigInfo, void *context) {
  // taosIgnSignal(SIGUSR1);
  // taosIgnSignal(SIGUSR2);
  taosIgnSignal(SIGTERM);
  taosIgnSignal(SIGHUP);
  taosIgnSignal(SIGINT);
  taosIgnSignal(SIGABRT);
  taosIgnSignal(SIGBREAK);

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
  taosIgnSignal(SIGBUS);
#endif
  taosIgnSignal(SIGABRT);
  taosIgnSignal(SIGFPE);
  taosIgnSignal(SIGSEGV);

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

  taosLogCrashInfo("taosd", pMsg, msgLen, signum, sigInfo);

#ifdef _TD_DARWIN_64
  exit(signum);
#elif defined(WINDOWS)
  exit(signum);
#endif
}

static void dmSetSignalHandle() {
  taosSetSignal(SIGUSR1, dmSetDebugFlag);
  taosSetSignal(SIGUSR2, dmSetAssert);
  taosSetSignal(SIGTERM, dmStopDnode);
  taosSetSignal(SIGHUP, dmStopDnode);
  taosSetSignal(SIGINT, dmStopDnode);
  taosSetSignal(SIGBREAK, dmStopDnode);
#ifndef WINDOWS
  taosSetSignal(SIGTSTP, dmStopDnode);
  taosSetSignal(SIGQUIT, dmStopDnode);
#endif

#ifndef WINDOWS
  taosSetSignal(SIGBUS, dmLogCrash);
#endif
  taosSetSignal(SIGABRT, dmLogCrash);
  taosSetSignal(SIGFPE, dmLogCrash);
  taosSetSignal(SIGSEGV, dmLogCrash);
}

static int32_t dmParseArgs(int32_t argc, char const *argv[]) {
  global.startTime = taosGetTimestampMs();

  int32_t cmdEnvIndex = 0;
  if (argc < 2) return 0;

  global.envCmd = taosMemoryMalloc((argc - 1) * sizeof(char *));
  memset(global.envCmd, 0, (argc - 1) * sizeof(char *));
  for (int32_t i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "-c") == 0) {
      if (i < argc - 1) {
        if (strlen(argv[++i]) >= PATH_MAX) {
          printf("config file path overflow");
          return -1;
        }
        tstrncpy(configDir, argv[i], PATH_MAX);
      } else {
        printf("'-c' requires a parameter, default is %s\n", configDir);
        return -1;
      }
    } else if (strcmp(argv[i], "-a") == 0) {
      if(i < argc - 1) {
        if (strlen(argv[++i]) >= PATH_MAX) {
          printf("apollo url overflow");
          return -1;
        }
        tstrncpy(global.apolloUrl, argv[i], PATH_MAX);
      } else {
        printf("'-a' requires a parameter\n");
        return -1;
      }
    } else if (strcmp(argv[i], "-s") == 0) {
      global.dumpSdb = true;
    } else if (strcmp(argv[i], "-E") == 0) {
      if(i < argc - 1) {
        if (strlen(argv[++i]) >= PATH_MAX) {
          printf("env file path overflow");
          return -1;
        }
        tstrncpy(global.envFile, argv[i], PATH_MAX);
      } else {
        printf("'-E' requires a parameter\n");
        return -1;
      }
    } else if (strcmp(argv[i], "-k") == 0) {
      global.generateGrant = true;
    } else if (strcmp(argv[i], "-y") == 0) {
      global.generateCode = true;
      if(i < argc - 1) {
        if (strlen(argv[++i]) > ENCRYPT_KEY_LEN) {
          printf("encrypt key overflow, it should be less or equal to %d\n", ENCRYPT_KEY_LEN);
          return -1;
        }
        tstrncpy(global.encryptKey, argv[i], ENCRYPT_KEY_LEN);
      } else {
        printf("'-y' requires a parameter\n");
        return -1;
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
#ifdef TD_ENTERPRISE
  char *releaseName = "enterprise";
#else
  char *releaseName = "community";
#endif
  printf("%s version: %s compatible_version: %s\n", releaseName, version, compatible_version);
  printf("gitinfo: %s\n", gitinfo);
#ifdef TD_ENTERPRISE
  printf("gitinfoOfInternal: %s\n", gitinfoOfInternal);
#endif
  printf("buildInfo: %s\n", buildinfo);
}

static void dmPrintHelp() {
  char indent[] = "  ";
  printf("Usage: taosd [OPTION...] \n\n");
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

static int32_t dmInitLog() {
  return taosCreateLog(CUS_PROMPT"dlog", 1, configDir, global.envCmd, global.envFile, global.apolloUrl, global.pArgs, 0);
}

static void taosCleanupArgs() {
  if (global.envCmd != NULL) taosMemoryFreeClear(global.envCmd);
}

static int32_t compareCheckCode(char* file, char* key){
  char     *content = NULL;
  int64_t   size = 0;
  TdFilePtr pFile = NULL;
  char     *result = NULL;
  int32_t   code = -1;

  pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to open dnode file:%s since %s", file, terrstr());
    goto _OVER;
  }

  if (taosFStatFile(pFile, &size, NULL) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to fstat dnode file:%s since %s", file, terrstr());
    goto _OVER;
  }

  content = taosMemoryMalloc(size);
  if (content == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  if (taosReadFile(pFile, content, size) != size) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to read dnode file:%s since %s", file, terrstr());
    goto _OVER;
  }

  dInfo("succeed to read checkCode file:%s", file);
  
  int len = ENCRYPTED_LEN(size);
  result = taosMemoryMalloc(len);

  SCryptOpts opts = {0};
  strncpy(opts.key, key, ENCRYPT_KEY_LEN);
  opts.len = len;
  opts.source = content;
  opts.result = result;
  opts.unitLen = 16;
  CBC_Decrypt(&opts);

  if(strcmp(opts.result, DM_KEY_INDICATOR) != 0) {
    terrno = TSDB_CODE_DNODE_ENCRYPTKEY_CHANGED;
    dError("failed to compare decrypted result");
    goto _OVER;
  }

  dInfo("succeed to compare checkCode file:%s", file);
  code = 0;
_OVER:
  if(result != NULL) taosMemoryFree(result);
  if(content != NULL) taosMemoryFree(content);
  if(pFile != NULL) taosCloseFile(&pFile);

  return code;
}

static int32_t writeCheckCode(char* file, char* realfile, char* key){
  TdFilePtr pFile = NULL;
  char     *result = NULL;
  int32_t   code = -1;

  int32_t len = ENCRYPTED_LEN(sizeof(DM_KEY_INDICATOR));
  result = taosMemoryMalloc(len);

  SCryptOpts opts;
  strncpy(opts.key, key, ENCRYPT_KEY_LEN);
  opts.len = len;
  opts.source = DM_KEY_INDICATOR;
  opts.result = result;
  opts.unitLen = 16;
  CBC_Encrypt(&opts);

  pFile = taosOpenFile(file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pFile == NULL) goto _OVER;

  if (taosWriteFile(pFile, opts.result, len) <= 0) goto _OVER;
  if (taosFsyncFile(pFile) < 0) goto _OVER;

  taosCloseFile(&pFile);
  if (taosRenameFile(file, realfile) != 0) goto _OVER;

  dInfo("succeed to write checkCode file:%s", realfile);
  code = 0;
_OVER:
  if(pFile != NULL) taosCloseFile(&pFile);
  if(result != NULL) taosMemoryFree(result);

  return code;
}

static int32_t readEncryptCode(char* file, char** output){
  TdFilePtr pFile = NULL;
  int32_t   code = -1;
  char     *content = NULL;

  pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to open dnode file:%s since %s", file, terrstr());
    goto _OVER;
  }

  int64_t size = 0;
  if (taosFStatFile(pFile, &size, NULL) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to fstat dnode file:%s since %s", file, terrstr());
    goto _OVER;
  }

  content = taosMemoryMalloc(size + 1);
  if (content == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  if (taosReadFile(pFile, content, size) != size) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to read dnode file:%s since %s", file, terrstr());
    goto _OVER;
  }

  content[size] = '\0';

  *output = content;

  dInfo("succeed to read encryptCode file:%s", file);
  code = 0;
_OVER:
  if(pFile != NULL) taosCloseFile(&pFile);

  return code;
}

static int32_t writeEncryptCode(char* file, char* realfile, char* encryptCode){
  TdFilePtr pFile = NULL;
  int32_t   code = -1;

  pFile = taosOpenFile(file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pFile == NULL) goto _OVER;

  int32_t len = strlen(encryptCode);
  if (taosWriteFile(pFile, encryptCode, len) <= 0) goto _OVER;
  if (taosFsyncFile(pFile) < 0) goto _OVER;

  taosCloseFile(&pFile);
  if (taosRenameFile(file, realfile) != 0) goto _OVER;

  dInfo("succeed to write encryptCode file:%s", realfile);

  code = 0;
_OVER:
  if(pFile != NULL) taosCloseFile(&pFile);

  return code;
}

static int32_t getEncryptKey(){
  int32_t   code = -1;
  char      encryptFile[PATH_MAX] = {0};
  char      checkFile[PATH_MAX] = {0};

  snprintf(encryptFile, sizeof(encryptFile), "%s%sdnode%s%s", tsDataDir, TD_DIRSEP, TD_DIRSEP, DM_ENCRYPT_CODE_FILE);
  snprintf(checkFile, sizeof(checkFile), "%s%sdnode%s%s", tsDataDir, TD_DIRSEP, TD_DIRSEP, DM_CHECK_CODE_FILE);

  if(!taosCheckExistFile(encryptFile)){
    dInfo("no exist, checkCode file:%s", encryptFile);
    return 0;
  }

  char     *content = NULL;
  if(readEncryptCode(encryptFile, &content) != 0){
    goto _OVER;
  }

  //TODO: dmchen parse key from code

  strncpy(tsEncryptKey, content, ENCRYPT_KEY_LEN);

  taosMemoryFreeClear(content);

  if(tsEncryptKey[0] == '\0'){
    terrno = TSDB_CODE_DNODE_INVALID_ENCRYPTKEY;
    dError("failed to read key since %s", terrstr());
    goto _OVER;
  }

  if(compareCheckCode(checkFile, tsEncryptKey) != 0){
    goto _OVER;
  }

  code = 0;
_OVER:
  if (content != NULL) taosMemoryFree(content);
  if (code != 0) {
    if (terrno == 0) terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to get encrypt key since %s", terrstr());
  }
  return code;
}

int32_t updateEncryptKey(char *key) {
  int32_t   code = -1;

  char      folder[PATH_MAX] = {0};

  char      encryptFile[PATH_MAX] = {0};
  char      realEncryptFile[PATH_MAX] = {0};

  char      checkFile[PATH_MAX] = {0};
  char      realCheckFile[PATH_MAX] = {0};

  snprintf(folder, sizeof(folder), "%s%sdnode", tsDataDir, TD_DIRSEP);
  snprintf(encryptFile, sizeof(realEncryptFile), "%s%s%s.bak", folder, TD_DIRSEP, DM_ENCRYPT_CODE_FILE);
  snprintf(realEncryptFile, sizeof(realEncryptFile), "%s%s%s", folder, TD_DIRSEP, DM_ENCRYPT_CODE_FILE);
  snprintf(checkFile, sizeof(checkFile), "%s%s%s.bak", folder, TD_DIRSEP, DM_CHECK_CODE_FILE);
  snprintf(realCheckFile, sizeof(realCheckFile), "%s%s%s", folder, TD_DIRSEP, DM_CHECK_CODE_FILE);

  terrno = 0;

  if (taosMkDir(folder) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to create dir:%s since %s", folder, terrstr());
    goto _OVER;
  }

  if(taosCheckExistFile(realCheckFile)){
    if(compareCheckCode(realCheckFile, key) != 0){
      goto _OVER;
    }
  }

  if(writeEncryptCode(encryptFile, realEncryptFile, key) != 0){
    goto _OVER;
  }

  if(writeCheckCode(checkFile, realCheckFile, key) != 0){
    goto _OVER;
  } 

  code = 0;
_OVER:

  if (code != 0) {
    if (terrno == 0) terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to update encrypt key since %s", terrstr());
  }
  return code;
}

int main(int argc, char const *argv[]) {
#ifdef TD_JEMALLOC_ENABLED
  bool jeBackgroundThread = true;
  mallctl("background_thread", NULL, NULL, &jeBackgroundThread, sizeof(bool));
#endif
  if (!taosCheckSystemIsLittleEnd()) {
    printf("failed to start since on non-little-end machines\n");
    return -1;
  }

  if (dmParseArgs(argc, argv) != 0) {
    printf("failed to start since parse args error\n");
    taosCleanupArgs();
    return -1;
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
    int32_t code = taosMemoryDbgInit();
    if (code) {
      printf("failed to init memory dbg, error:%s\n", tstrerror(code));
      return code;
    }
    tsAsyncLog = false;
    printf("memory dbg enabled\n");
  }
#endif

  if (dmInitLog() != 0) {
    printf("failed to start since init log error\n");
    taosCleanupArgs();
    return -1;
  }

  dmPrintArgs(argc, argv);

  if (taosInitCfg(configDir, global.envCmd, global.envFile, global.apolloUrl, global.pArgs, 0) != 0) {
    dError("failed to start since read config error");
    taosCloseLog();
    taosCleanupArgs();
    return -1;
  }

  if(global.generateCode) {
    updateEncryptKey(global.encryptKey);
    taosCloseLog();
    taosCleanupArgs();
    return 0;
  }

  if(getEncryptKey() != 0){
    dError("failed to start since failed to get encrypt key");
    taosCloseLog();
    taosCleanupArgs();
    return -1;
  };

  if (taosConvInit() != 0) {
    dError("failed to init conv");
    taosCloseLog();
    taosCleanupArgs();
    return -1;
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

  osSetProcPath(argc, (char **)argv);
  taosCleanupArgs();

  if (dmInit() != 0) {
    if (terrno == TSDB_CODE_NOT_FOUND) {
      dError("failed to init dnode since unsupported platform, please visit https://www.taosdata.com for support");
    } else {
      dError("failed to init dnode since %s", terrstr());
    }

    taosCleanupCfg();
    taosCloseLog();
    taosConvDestroy();
    return -1;
  }

  dInfo("start to init service");
  dmSetSignalHandle();
  tsDndStart = taosGetTimestampMs();
  tsDndStartOsUptime = taosGetOsUptime();
  int32_t code = dmRun();
  dInfo("shutting down the service");

  dmCleanup();
  return code;
}
