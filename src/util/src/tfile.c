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

#include <stdio.h>
#include <stdlib.h>
#include <error.h>
#include <errno.h>
#include <stdarg.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "os.h"

#ifdef TAOS_RANDOM_FILE_FAIL

static int random_file_fail_factor = 20;
static FILE *fpRandomFileFailOutput = NULL;

void taosSetRandomFileFailFactor(int factor)
{
  random_file_fail_factor = factor;
}

static void close_random_file_fail_output()
{
  if (fpRandomFileFailOutput != NULL) {
    if (fpRandomFileFailOutput != stdout) {
      fclose(fpRandomFileFailOutput);
    }
    fpRandomFileFailOutput = NULL;
  }
}

static void random_file_fail_output_sig(int sig)
{
  fprintf(fpRandomFileFailOutput, "signal %d received.\n", sig);

  struct sigaction act = {0};
  act.sa_handler = SIG_DFL;
  sigaction(sig, &act, NULL);

  close_random_file_fail_output();
  exit(EXIT_FAILURE);
}

void taosSetRandomFileFailOutput(const char *path)
{
  if (path == NULL) {
    fpRandomFileFailOutput = stdout;
  } else if ((fpRandomFileFailOutput = fopen(path, "w")) != NULL) {
    atexit(close_random_file_fail_output);
  } else {
    printf("failed to open random file fail log file '%s', errno=%d\n", path, errno);
    return;
  }

  struct sigaction act = {0};
  act.sa_handler = random_file_fail_output_sig;
  sigaction(SIGFPE, &act, NULL);
  sigaction(SIGSEGV, &act, NULL);
  sigaction(SIGILL, &act, NULL);
}
#endif

ssize_t taos_tread(int fd, void *buf, size_t count, const char *file, uint32_t line)
{
#ifdef TAOS_RANDOM_FILE_FAIL
  if (random_file_fail_factor > 0) {
    if (rand() % random_file_fail_factor == 0) {
      errno = EIO;
      return -1;
    }
  }
#endif
  return tread(fd, buf, count);
}

ssize_t taos_twrite(int fd, void *buf, size_t count, const char *file, uint32_t line)
{
#ifdef TAOS_RANDOM_FILE_FAIL
  if (random_file_fail_factor > 0) {
    if (rand() % random_file_fail_factor == 0) {
      errno = EIO;
      return -1;
    }
  }
#endif
  return twrite(fd, buf, count);
}

off_t taos_lseek(int fd, off_t offset, int whence, const char *file, uint32_t line)
{
#ifdef TAOS_RANDOM_FILE_FAIL
  if (random_file_fail_factor > 0) {
    if (rand() % random_file_fail_factor == 0) {
      errno = EIO;
      return -1;
    }
  }
#endif
  return lseek(fd, offset, whence);
}
