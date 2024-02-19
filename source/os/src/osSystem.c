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

#define ALLOW_FORBID_FUNC
#define _DEFAULT_SOURCE
#include "os.h"

#if defined(WINDOWS)
typedef void (*MainWindows)(int argc, char** argv);
MainWindows mainWindowsFunc = NULL;

SERVICE_STATUS        ServiceStatus;
SERVICE_STATUS_HANDLE hServiceStatusHandle;
void WINAPI           windowsServiceCtrlHandle(DWORD request) {
            switch (request) {
              case SERVICE_CONTROL_STOP:
              case SERVICE_CONTROL_SHUTDOWN:
      raise(SIGINT);
      ServiceStatus.dwCurrentState = SERVICE_STOP_PENDING;
      if (!SetServiceStatus(hServiceStatusHandle, &ServiceStatus)) {
                  DWORD nError = GetLastError();
                  printf("failed to send stopped status to windows service: %d", nError);
      }
      break;
              default:
      return;
  }
}
void WINAPI mainWindowsService(int argc, char** argv) {
  int ret = 0;
  ServiceStatus.dwServiceType = SERVICE_WIN32;
  ServiceStatus.dwControlsAccepted = SERVICE_ACCEPT_PAUSE_CONTINUE | SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_SHUTDOWN;
  ServiceStatus.dwCurrentState = SERVICE_START_PENDING;
  ServiceStatus.dwWin32ExitCode = 0;
  ServiceStatus.dwCheckPoint = 0;
  ServiceStatus.dwWaitHint = 0;
  ServiceStatus.dwServiceSpecificExitCode = 0;
  hServiceStatusHandle = RegisterServiceCtrlHandler("taosd", &windowsServiceCtrlHandle);
  if (hServiceStatusHandle == 0) {
    DWORD nError = GetLastError();
    printf("failed to register windows service ctrl handler: %d", nError);
  }

  ServiceStatus.dwCurrentState = SERVICE_RUNNING;
  if (SetServiceStatus(hServiceStatusHandle, &ServiceStatus)) {
    DWORD nError = GetLastError();
    printf("failed to send running status to windows service: %d", nError);
  }
  if (mainWindowsFunc != NULL) mainWindowsFunc(argc, argv);
  ServiceStatus.dwCurrentState = SERVICE_STOPPED;
  if (!SetServiceStatus(hServiceStatusHandle, &ServiceStatus)) {
    DWORD nError = GetLastError();
    printf("failed to send stopped status to windows service: %d", nError);
  }
}
void stratWindowsService(MainWindows mainWindows) {
  mainWindowsFunc = mainWindows;
  SERVICE_TABLE_ENTRY ServiceTable[2];
  ServiceTable[0].lpServiceName = "taosd";
  ServiceTable[0].lpServiceProc = (LPSERVICE_MAIN_FUNCTION)mainWindowsService;
  ServiceTable[1].lpServiceName = NULL;
  ServiceTable[1].lpServiceProc = NULL;
  StartServiceCtrlDispatcher(ServiceTable);
}

#elif defined(_TD_DARWIN_64)
#else
#include <dlfcn.h>
#include <termios.h>
#include <unistd.h>
#endif

#if !defined(WINDOWS)
struct termios oldtio;
#endif

typedef struct FILE TdCmd;

#ifdef BUILD_NO_CALL
void* taosLoadDll(const char* filename) {
#if defined(WINDOWS)
  ASSERT(0);
  return NULL;
#elif defined(_TD_DARWIN_64)
  return NULL;
#else
  void* handle = dlopen(filename, RTLD_LAZY);
  if (!handle) {
    // printf("load dll:%s failed, error:%s", filename, dlerror());
    return NULL;
  }

  // printf("dll %s loaded", filename);

  return handle;
#endif
}

void* taosLoadSym(void* handle, char* name) {
#if defined(WINDOWS)
  ASSERT(0);
  return NULL;
#elif defined(_TD_DARWIN_64)
  return NULL;
#else
  void* sym = dlsym(handle, name);
  char* error = NULL;

  if ((error = dlerror()) != NULL) {
    // printf("load sym:%s failed, error:%s", name, dlerror());
    return NULL;
  }

  // printf("sym %s loaded", name);

  return sym;
#endif
}

void taosCloseDll(void* handle) {
#if defined(WINDOWS)
  ASSERT(0);
  return;
#elif defined(_TD_DARWIN_64)
  return;
#else
  if (handle) {
    dlclose(handle);
  }
#endif
}
#endif

int taosSetConsoleEcho(bool on) {
#if defined(WINDOWS)
  HANDLE hStdin = GetStdHandle(STD_INPUT_HANDLE);
  DWORD  mode = 0;
  GetConsoleMode(hStdin, &mode);
  if (on) {
    mode |= ENABLE_ECHO_INPUT;
  } else {
    mode &= ~ENABLE_ECHO_INPUT;
  }
  SetConsoleMode(hStdin, mode);

  return 0;
#else
#define ECHOFLAGS (ECHO | ECHOE | ECHOK | ECHONL)
  int            err;
  struct termios term;

  if (tcgetattr(STDIN_FILENO, &term) == -1) {
    /*perror("Cannot get the attribution of the terminal");*/
    return -1;
  }

  if (on)
    term.c_lflag |= ECHOFLAGS;
  else
    term.c_lflag &= ~ECHOFLAGS;

  err = tcsetattr(STDIN_FILENO, TCSAFLUSH, &term);
  if (err == -1 || err == EINTR) {
    /*printf("Cannot set the attribution of the terminal");*/
    return -1;
  }

  return 0;
#endif
}

void taosSetTerminalMode() {
#if defined(WINDOWS)

#else
  struct termios newtio;

  /* if (atexit() != 0) { */
  /*     fprintf(stderr, "Error register exit function!\n"); */
  /*     exit(EXIT_FAILURE); */
  /* } */

  memcpy(&newtio, &oldtio, sizeof(oldtio));

  // Set new terminal attributes.
  newtio.c_iflag &= ~(IXON | IXOFF | ICRNL | INLCR | IGNCR | IMAXBEL | ISTRIP);
  newtio.c_iflag |= IGNBRK;

  // newtio.c_oflag &= ~(OPOST|ONLCR|OCRNL|ONLRET);
  newtio.c_oflag |= OPOST;
  newtio.c_oflag |= ONLCR;
  newtio.c_oflag &= ~(OCRNL | ONLRET);

  newtio.c_lflag &= ~(IEXTEN | ICANON | ECHO | ECHOE | ECHONL | ECHOCTL | ECHOPRT | ECHOKE | ISIG);
  newtio.c_cc[VMIN] = 1;
  newtio.c_cc[VTIME] = 0;

  if (tcsetattr(0, TCSANOW, &newtio) != 0) {
    fprintf(stderr, "Fail to set terminal properties!\n");
    exit(EXIT_FAILURE);
  }
#endif
}

int32_t taosGetOldTerminalMode() {
#if defined(WINDOWS)
#else
  /* Make sure stdin is a terminal. */
  if (!isatty(STDIN_FILENO)) {
    return -1;
  }

  // Get the parameter of current terminal
  if (tcgetattr(0, &oldtio) != 0) {
    return -1;
  }

  return 1;
#endif
}

void taosResetTerminalMode() {
#if defined(WINDOWS)
#else
  if (tcsetattr(0, TCSANOW, &oldtio) != 0) {
    fprintf(stderr, "Fail to reset the terminal properties!\n");
    exit(EXIT_FAILURE);
  }
#endif
}

TdCmdPtr taosOpenCmd(const char* cmd) {
  if (cmd == NULL) return NULL;
#ifdef WINDOWS
  return (TdCmdPtr)_popen(cmd, "r");
#else
  return (TdCmdPtr)popen(cmd, "r");
#endif
}

int64_t taosGetsCmd(TdCmdPtr pCmd, int32_t maxSize, char* __restrict buf) {
  if (pCmd == NULL || buf == NULL) {
    return -1;
  }
  if (fgets(buf, maxSize, (FILE*)pCmd) == NULL) {
    return -1;
  }
  return strlen(buf);
}

int64_t taosGetLineCmd(TdCmdPtr pCmd, char** __restrict ptrBuf) {
  if (pCmd == NULL || ptrBuf == NULL) {
    return -1;
  }
  if (*ptrBuf != NULL) {
    taosMemoryFreeClear(*ptrBuf);
  }
#ifdef WINDOWS
  *ptrBuf = taosMemoryMalloc(1024);
  if (*ptrBuf == NULL) return -1;
  if (fgets(*ptrBuf, 1023, (FILE*)pCmd) == NULL) {
    taosMemoryFreeClear(*ptrBuf);
    return -1;
  }
  (*ptrBuf)[1023] = 0;
  return strlen(*ptrBuf);
#else
  size_t len = 0;
  return getline(ptrBuf, &len, (FILE*)pCmd);
#endif
}

int32_t taosEOFCmd(TdCmdPtr pCmd) {
  if (pCmd == NULL) {
    return 0;
  }
  return feof((FILE*)pCmd);
}

int64_t taosCloseCmd(TdCmdPtr* ppCmd) {
  if (ppCmd == NULL || *ppCmd == NULL) {
    return 0;
  }
#ifdef WINDOWS
  _pclose((FILE*)(*ppCmd));
#else
  pclose((FILE*)(*ppCmd));
#endif
  *ppCmd = NULL;
  return 0;
}
