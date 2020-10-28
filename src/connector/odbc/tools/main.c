#include "../src/todbc_log.h"

#ifdef _MSC_VER
#include <winsock2.h>
#include <windows.h>
#include "os.h"
#endif
#include <odbcinst.h>

#include <stdio.h>
#include <string.h>

static void usage(const char *arg0);
static int do_install(int i, int argc, char *argv[]);
static int do_uninstall(int i, int argc, char *argv[]);

int main(int argc, char *argv[]) {
  for (int i = 1; i < argc; i++) {
    const char *arg = argv[i];
    if (strcmp(arg, "-h") == 0) {
      usage(argv[0]);
      return 0;
    } else if (strcmp(arg, "-i") == 0 ) {
      i = do_install(i + 1, argc, argv);
      if (i > 0) continue;
      return i == 0 ? 0 : 1;
    } else if (strcmp(arg, "-u") == 0 ) {
      i = do_uninstall(i + 1, argc, argv);
      if (i > 0) continue;
      return i == 0 ? 0 : 1;
    } else {
      fprintf(stderr, "unknown argument: [%s]\n", arg);
      return 1;
    }
  }
}

static void usage(const char *arg0) {
  fprintf(stderr, "%s -h | -i -n [TaosDriverName] -p [TaosDriverPath] | -u [-f] [TaosDriverName]\n", arg0);
  return;
}

static int do_install(int i, int argc, char *argv[]) {
  int forceful = 0;
  const char* driverName = NULL;
  const char* driverFile = "todbc.dll";
  const char* driverPath = NULL;
  for (; i < argc; ++i) {
    const char *arg = argv[i];
    if (strcmp(arg, "-n") == 0) {
      i += 1;
      if (i >= argc) {
        fprintf(stderr, "expecting TaosDriverName, but got nothing\n");
        return -1;
      }
      arg = argv[i];
      if (strstr(arg, "TAOS") != arg) {
        fprintf(stderr, "TaosDriverName shall begin with 'TAOS': [%s]\n", arg);
        return -1;
      }
      driverName = arg;
    } else if (strcmp(arg, "-p") == 0) {
      i += 1;
      if (i >= argc) {
        fprintf(stderr, "expecting TaosDriverPath, but got nothing\n");
        return -1;
      }
      driverPath = argv[i];
    } else {
      fprintf(stderr, "unknown argument: [%s]\n", arg);
      return -1;
    }
  }
  if (!driverName) {
    fprintf(stderr, "TaosDriverName not specified\n");
    return -1;
  }
  if (!driverPath) {
    fprintf(stderr, "TaosDriverPath not specified\n");
    return -1;
  }
  char buf[8192];
  snprintf(buf, sizeof(buf), "%s%cDriver=%s%cFileUage=0%cConnectFunctions=YYN%c",
    driverName, 0, driverFile, 0, 0, 0);
  BOOL ok = TRUE;
  DWORD usageCount = 1;
  char installed[PATH_MAX + 1];
  WORD len = 0;
  ok = SQLInstallDriverEx(buf, driverPath, installed, sizeof(installed), &len, ODBC_INSTALL_INQUIRY, &usageCount);
  if (!ok) {
    fprintf(stderr, "failed to query TaosDriverName: [%s]\n", driverName);
    return -1;
  }
  if (stricmp(driverPath, installed)) {
    fprintf(stderr, "previously installed TaosDriver [%s] has different target path [%s]\n"
      "it shall be uninstalled before you can install it to different path [%s]\n",
      driverName, installed, driverPath);
    return -1;
  }
  ok = SQLInstallDriverEx(buf, driverPath, installed, sizeof(installed), &len, ODBC_INSTALL_COMPLETE, &usageCount);
  if (!ok) {
    fprintf(stderr, "failed to install TaosDriverName: [%s][%s]\n", driverName, driverPath);
    return -1;
  }

  return argc;
}

static int do_uninstall(int i, int argc, char *argv[]) {
  int forceful = 0;
  const char* driverName = NULL;
  for (; i < argc; ++i) {
    const char *arg = argv[i];
    if (strcmp(arg, "-f") == 0) {
      forceful = 1;
    } else if (strcmp(arg, "-n") == 0) {
      i += 1;
      if (i >= argc) {
        fprintf(stderr, "expecting TaosDriverName, but got nothing\n");
        return -1;
      }
      arg = argv[i];
      if (strstr(arg, "TAOS") != arg) {
        fprintf(stderr, "TaosDriverName shall begin with 'TAOS': [%s]\n", arg);
        return -1;
      }
      driverName = arg;
    } else {
      fprintf(stderr, "unknown argument: [%s]\n", arg);
      return -1;
    }
  }
  if (!driverName) {
    fprintf(stderr, "TaosDriverName not specified\n");
    return -1;
  }
  BOOL ok = TRUE;
  DWORD usageCount = 1;
  do {
    ok = SQLRemoveDriver(driverName, FALSE, &usageCount);
    if (!ok) {
      fprintf(stderr, "failed to remove driver [%s]\n", driverName);
      return -1;
    }
    if (!forceful) return argc;
  } while (usageCount > 0);
  return argc;
}

