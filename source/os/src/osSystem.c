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

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)

/*
 * windows implementation
 */

void* taosLoadDll(const char* filename) { return NULL; }
void* taosLoadSym(void* handle, char* name) { return NULL; }
void  taosCloseDll(void* handle) {}

int taosSetConsoleEcho(bool on) {
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
}

#elif defined(_TD_DARWIN_64)

/*
 * darwin implementation
 */

void* taosLoadDll(const char* filename) { return NULL; }
void* taosLoadSym(void* handle, char* name) { return NULL; }
void  taosCloseDll(void* handle) {}

int taosSetConsoleEcho(bool on) {
#define ECHOFLAGS (ECHO | ECHOE | ECHOK | ECHONL)
  int            err;
  struct termios term;

  if (tcgetattr(STDIN_FILENO, &term) == -1) {
    perror("Cannot get the attribution of the terminal");
    return -1;
  }

  if (on)
    term.c_lflag |= ECHOFLAGS;
  else
    term.c_lflag &= ~ECHOFLAGS;

  err = tcsetattr(STDIN_FILENO, TCSAFLUSH, &term);
  if (err == -1 && err == EINTR) {
    perror("Cannot set the attribution of the terminal");
    return -1;
  }

  return 0;
}

#else

/*
 * linux implementation
 */

#include <dlfcn.h>
#include <termios.h>
#include <unistd.h>

void* taosLoadDll(const char* filename) {
  void* handle = dlopen(filename, RTLD_LAZY);
  if (!handle) {
    printf("load dll:%s failed, error:%s", filename, dlerror());
    return NULL;
  }

  printf("dll %s loaded", filename);

  return handle;
}

void* taosLoadSym(void* handle, char* name) {
  void* sym = dlsym(handle, name);
  char* error = NULL;

  if ((error = dlerror()) != NULL) {
    printf("load sym:%s failed, error:%s", name, dlerror());
    return NULL;
  }

  printf("sym %s loaded", name);

  return sym;
}

void taosCloseDll(void* handle) {
  if (handle) {
    dlclose(handle);
  }
}

int taosSetConsoleEcho(bool on) {
#define ECHOFLAGS (ECHO | ECHOE | ECHOK | ECHONL)
  int            err;
  struct termios term;

  if (tcgetattr(STDIN_FILENO, &term) == -1) {
    perror("Cannot get the attribution of the terminal");
    return -1;
  }

  if (on)
    term.c_lflag |= ECHOFLAGS;
  else
    term.c_lflag &= ~ECHOFLAGS;

  err = tcsetattr(STDIN_FILENO, TCSAFLUSH, &term);
  if (err == -1 || err == EINTR) {
    printf("Cannot set the attribution of the terminal");
    return -1;
  }

  return 0;
}

#endif
