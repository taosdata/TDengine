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

#define RANDOM_NETWORK_FAIL_FACTOR  20

#ifdef TAOS_RANDOM_NETWORK_FAIL

int64_t taosSendRandomFail(int32_t sockfd, const void *buf, size_t len, int32_t flags) {
  if (rand() % RANDOM_NETWORK_FAIL_FACTOR == 0) {
    errno = ECONNRESET;
    return -1;
  }

  return send(sockfd, buf, len, flags);
}

int64_t taosSendToRandomFail(int32_t sockfd, const void *buf, size_t len, int32_t flags,
                             const struct sockaddr *dest_addr, socklen_t addrlen) {
  if (rand() % RANDOM_NETWORK_FAIL_FACTOR == 0) {
    errno = ECONNRESET;
    return -1;
  }

  return sendto(sockfd, buf, len, flags, dest_addr, addrlen);
}

int64_t taosReadSocketRandomFail(int32_t fd, void *buf, size_t count) {
  if (rand() % RANDOM_NETWORK_FAIL_FACTOR == 0) {
    errno = ECONNRESET;
    return -1;
  }

  return read(fd, buf, count);
}

int64_t taosWriteSocketRandomFail(int32_t fd, const void *buf, size_t count) {
  if (rand() % RANDOM_NETWORK_FAIL_FACTOR == 0) {
    errno = EINTR;
    return -1;
  }

  return write(fd, buf, count);
}

#endif //TAOS_RANDOM_NETWORK_FAIL

#ifdef TAOS_RANDOM_FILE_FAIL

static int32_t random_file_fail_factor = 20;
static FILE *fpRandomFileFailOutput = NULL;

void taosSetRandomFileFailFactor(int32_t factor) { 
  random_file_fail_factor = factor;
}

static void close_random_file_fail_output() {
  if (fpRandomFileFailOutput != NULL) {
    if (fpRandomFileFailOutput != stdout) {
      fclose(fpRandomFileFailOutput);
    }
    fpRandomFileFailOutput = NULL;
  }
}

static void random_file_fail_output_sig(int32_t sig) {
  fprintf(fpRandomFileFailOutput, "signal %d received.\n", sig);

  struct sigaction act = {0};
  act.sa_handler = SIG_DFL;
  sigaction(sig, &act, NULL);

  close_random_file_fail_output();
  exit(EXIT_FAILURE);
}

void taosSetRandomFileFailOutput(const char *path) {
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

int64_t taosReadFileRandomFail(int32_t fd, void *buf, int32_t count, const char *file, uint32_t line) {
  if (random_file_fail_factor > 0) {
    if (rand() % random_file_fail_factor == 0) {
      errno = EIO;
      return -1;
    }
  }

  return taosReadImp(fd, buf, count);
}

int64_t taosWriteFileRandomFail(int32_t fd, void *buf, int32_t count, const char *file, uint32_t line) {
  if (random_file_fail_factor > 0) {
    if (rand() % random_file_fail_factor == 0) {
      errno = EIO;
      return -1;
    }
  }

  return taosWriteImp(fd, buf, count);
}

int64_t taosLSeekRandomFail(int32_t fd, int64_t offset, int32_t whence, const char *file, uint32_t line) {
  if (random_file_fail_factor > 0) {
    if (rand() % random_file_fail_factor == 0) {
      errno = EIO;
      return -1;
    }
  }

  return taosLSeekImp(fd, offset, whence);
}

#endif //TAOS_RANDOM_FILE_FAIL
