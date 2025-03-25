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
                  fprintf(stderr, "failed to send stopped status to windows service: %d", nError);
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
    fprintf(stderr, "failed to register windows service ctrl handler: %d", nError);
  }

  ServiceStatus.dwCurrentState = SERVICE_RUNNING;
  if (SetServiceStatus(hServiceStatusHandle, &ServiceStatus)) {
    DWORD nError = GetLastError();
    fprintf(stderr, "failed to send running status to windows service: %d", nError);
  }
  if (mainWindowsFunc != NULL) mainWindowsFunc(argc, argv);
  ServiceStatus.dwCurrentState = SERVICE_STOPPED;
  if (!SetServiceStatus(hServiceStatusHandle, &ServiceStatus)) {
    DWORD nError = GetLastError();
    fprintf(stderr, "failed to send stopped status to windows service: %d", nError);
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

#elif defined(_TD_DARWIN_64) || defined(TD_ASTRA)
#else
#include <dlfcn.h>
#include <termios.h>
#include <unistd.h>
#endif

#if !defined(WINDOWS) && !defined(TD_ASTRA)
struct termios oldtio;
#endif

typedef struct FILE TdCmd;

int32_t taosSetConsoleEcho(bool on) {
#if defined(WINDOWS)
  HANDLE hStdin = GetStdHandle(STD_INPUT_HANDLE);
  if (hStdin == INVALID_HANDLE_VALUE) {
    terrno = TAOS_SYSTEM_WINAPI_ERROR(GetLastError());
    return terrno;
  }
  DWORD  mode = 0;
  if(!GetConsoleMode(hStdin, &mode)){
    terrno = TAOS_SYSTEM_WINAPI_ERROR(GetLastError());
    return terrno;
  }
  if (on) {
    mode |= ENABLE_ECHO_INPUT;
  } else {
    mode &= ~ENABLE_ECHO_INPUT;
  }
  if(!SetConsoleMode(hStdin, mode)) {
    terrno = TAOS_SYSTEM_WINAPI_ERROR(GetLastError());
    return terrno;
  }

  return 0;
#elif defined(TD_ASTRA) // TD_ASTRA_TODO
  return 0;
#else
#define ECHOFLAGS (ECHO | ECHOE | ECHOK | ECHONL)
  int            err;
  struct termios term;

  if (tcgetattr(STDIN_FILENO, &term) == -1) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    return terrno;
  }

  if (on)
    term.c_lflag |= ECHOFLAGS;
  else
    term.c_lflag &= ~ECHOFLAGS;

  err = tcsetattr(STDIN_FILENO, TCSAFLUSH, &term);
  if (err == -1) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    return terrno;
  }

  return 0;
#endif
}

int32_t taosSetTerminalMode() {
#if defined(WINDOWS) || defined(TD_ASTRA) // TD_ASTRA_TODO
  return 0;
#else
  struct termios newtio;

  /* if (atexit() != 0) { */
  /*     fprintf(stderr, "Error register exit function!\n"); */
  /*     exit(EXIT_FAILURE); */
  /* } */

  (void)memcpy(&newtio, &oldtio, sizeof(oldtio));

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

  if (-1 == tcsetattr(0, TCSANOW, &newtio)) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    (void)fprintf(stderr, "Fail to set terminal properties!\n");
    return terrno;
  }

  return 0;
#endif
}

int32_t taosGetOldTerminalMode() {
#if defined(WINDOWS) || defined(TD_ASTRA) // TD_ASTRA_TODO
  return 0;
#else
  /* Make sure stdin is a terminal. */
  if (!isatty(STDIN_FILENO)) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    return terrno;
  }

  // Get the parameter of current terminal
  if (-1 == tcgetattr(0, &oldtio)) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    return terrno;
  }

  return 0;
#endif
}

int32_t taosResetTerminalMode() {
#if defined(WINDOWS) || defined(TD_ASTRA) // TD_ASTRA_TODO
  return 0;
#else
  if (-1 == tcsetattr(0, TCSANOW, &oldtio)) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    (void)fprintf(stderr, "Fail to reset the terminal properties!\n");
    return terrno;
  }
#endif
  return 0;
}

TdCmdPtr taosOpenCmd(const char* cmd) {
  if (cmd == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }
  
#ifdef WINDOWS
  return (TdCmdPtr)_popen(cmd, "r");
#elif defined(TD_ASTRA)
  return NULL;
#else
  TdCmdPtr p = (TdCmdPtr)popen(cmd, "r");
  if (NULL == p) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
  }
  return p;
#endif
}

int64_t taosGetsCmd(TdCmdPtr pCmd, int32_t maxSize, char* __restrict buf) {
  if (pCmd == NULL || buf == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  if (fgets(buf, maxSize, (FILE*)pCmd) == NULL) {
    if (feof((FILE*)pCmd)) {
      return 0;
    }

    terrno = TAOS_SYSTEM_ERROR(ferror((FILE*)pCmd));
    return terrno;
  }
  
  return strlen(buf);
}

int64_t taosGetLineCmd(TdCmdPtr pCmd, char** __restrict ptrBuf) {
  if (pCmd == NULL || ptrBuf == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
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
#elif defined(TD_ASTRA)
  size_t bufsize = 128;
  size_t pos = 0;
  int    c;
  if (*ptrBuf == NULL) {
    *ptrBuf = (char*)taosMemoryMalloc(bufsize);
    if (*ptrBuf == NULL) {
      return terrno;
    }
  }
  while ((c = fgetc((FILE*)pCmd)) != EOF) {
    if (pos + 1 >= bufsize) {
      size_t new_size = bufsize << 1;
      char*  new_line = (char*)taosMemoryRealloc(*ptrBuf, new_size);
      if (new_line == NULL) {
        return terrno;
      }
      *ptrBuf = new_line;
      bufsize = new_size;
    }
    (*ptrBuf)[pos++] = (char)c;
    if (c == '\n') {
      break;
    }
  }
  if (pos == 0 && c == EOF) {
    return TSDB_CODE_INVALID_PARA;
  }
  (*ptrBuf)[pos] = '\0';
  return (ssize_t)pos;
#else
  ssize_t len = 0;
  len = getline(ptrBuf, (size_t*)&len, (FILE*)pCmd);
  if (-1 == len) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    return terrno;
  }
  return len;
#endif
}

int32_t taosEOFCmd(TdCmdPtr pCmd) {
  if (pCmd == NULL) {
    return 0;
  }
  return feof((FILE*)pCmd);
}

void taosCloseCmd(TdCmdPtr* ppCmd) {
  if (ppCmd == NULL || *ppCmd == NULL) {
    return;
  }
#ifdef WINDOWS
  _pclose((FILE*)(*ppCmd));
#elif defined(TD_ASTRA) // TD_ASTRA_TODO
#else
  (void)pclose((FILE*)(*ppCmd));
#endif
  *ppCmd = NULL;
}
