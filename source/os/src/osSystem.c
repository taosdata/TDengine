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
#elif defined(_TD_DARWIN_64)
#else
#include <dlfcn.h>
#include <termios.h>
#include <unistd.h>
#endif

#if !defined(WINDOWS)
struct termios oldtio;
#endif

void* taosLoadDll(const char* filename) {
#if defined(WINDOWS)
  return NULL;
#elif defined(_TD_DARWIN_64)
  return NULL;
#else
  void* handle = dlopen(filename, RTLD_LAZY);
  if (!handle) {
    //printf("load dll:%s failed, error:%s", filename, dlerror());
    return NULL;
  }

  //printf("dll %s loaded", filename);

  return handle;
#endif
}

void* taosLoadSym(void* handle, char* name) {
#if defined(WINDOWS)
  return NULL;
#elif defined(_TD_DARWIN_64)
  return NULL;
#else
  void* sym = dlsym(handle, name);
  char* error = NULL;

  if ((error = dlerror()) != NULL) {
    //printf("load sym:%s failed, error:%s", name, dlerror());
    return NULL;
  }

  //printf("sym %s loaded", name);

  return sym;
#endif
}

void  taosCloseDll(void* handle) {
#if defined(WINDOWS)
  return;
#elif defined(_TD_DARWIN_64)
  return;
#else
  if (handle) {
    dlclose(handle);
  }
#endif
}

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
#endif
}

void setTerminalMode() {
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

int32_t getOldTerminalMode() {
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

void resetTerminalMode() {
#if defined(WINDOWS)

#else
  if (tcsetattr(0, TCSANOW, &oldtio) != 0) {
    fprintf(stderr, "Fail to reset the terminal properties!\n");
    exit(EXIT_FAILURE);
  }
#endif
}
