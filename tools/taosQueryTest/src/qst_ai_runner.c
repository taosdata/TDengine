/*
 * Copyright (c) 2026 TAOS Data, Inc.
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 */

#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "qst_ai_runner.h"

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "qst_ai.h"
#include "qst_block.h"
#include "qst_process.h"
#include "qst_result_store.h"
#include "qst_resource.h"
#include "qst_sha256.h"
#include "qst_shm.h"
#include "toolscJson.h"

enum {
  QST_AI_DEFAULT_TIMEOUT_MILLISECONDS = 900000,
  QST_AI_POLL_MILLISECONDS = 100,
  QST_AI_MAX_ANALYSIS_BYTES = 65536,
};

static EQstCode qstAiMakePath(char *out_path, uint64_t out_bytes,
                              const char *parent, const char *name) {
  int32_t written = 0;

  if (NULL == out_path || out_bytes == 0 || NULL == parent || NULL == name) {
    return QST_E_INVALID_ARGUMENT;
  }
  written = snprintf(out_path, (size_t)out_bytes, "%s/%s", parent, name);
  if (written < 0 || (uint64_t)written >= out_bytes) {
    return QST_E_RESOURCE;
  }
  return QST_OK;
}

static uint64_t qstAiMonotonicMilliseconds(void) {
  struct timespec now = {0};

  if (clock_gettime(CLOCK_MONOTONIC, &now) != 0 || now.tv_sec < 0 ||
      (uint64_t)now.tv_sec > UINT64_MAX / UINT64_C(1000)) {
    return 0;
  }
  return (uint64_t)now.tv_sec * UINT64_C(1000) +
         (uint64_t)now.tv_nsec / UINT64_C(1000000);
}

static int32_t qstAiExecutableAvailable(const char *command) {
  char path_copy[QST_PATH_BYTES] = {0};
  char *save = NULL;
  char *directory = NULL;
  const char *path = NULL;

  if (NULL == command || command[0] == '\0') {
    return 0;
  }
  if (strchr(command, '/') != NULL) {
    return access(command, X_OK) == 0;
  }
  path = getenv("PATH");
  if (NULL == path || strlen(path) >= sizeof(path_copy)) {
    return 0;
  }
  memcpy(path_copy, path, strlen(path) + 1);
  for (directory = strtok_r(path_copy, ":", &save); directory != NULL;
       directory = strtok_r(NULL, ":", &save)) {
    char candidate[QST_PATH_BYTES] = {0};
    const int32_t written = snprintf(candidate, sizeof(candidate), "%s/%s",
                                     directory[0] == '\0' ? "." : directory,
                                     command);
    if (written >= 0 && (uint64_t)written < sizeof(candidate) &&
        access(candidate, X_OK) == 0) {
      return 1;
    }
  }
  return 0;
}

static EQstCode qstAiWriteBytes(int32_t fd, const void *bytes, uint64_t length);

static const char *qstAiPromptName(uint32_t entry) {
  switch ((EQstAiEntry)entry) {
    case QST_AI_ENTRY_AUTO_VERDICT:
    case QST_AI_ENTRY_MANUAL_UNVERIFIED:
    case QST_AI_ENTRY_MANUAL_ALL:
      return "result_judgment.md";
    default:
      return NULL;
  }
}

static EQstCode qstAiReadFile(const char *path, uint8_t **out_bytes,
                              uint32_t *out_length) {
  FILE *file = NULL;
  long length = 0;
  uint8_t *bytes = NULL;

  if (NULL == path || NULL == out_bytes || NULL == out_length) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_bytes = NULL;
  *out_length = 0;
  file = fopen(path, "rb");
  if (NULL == file || fseek(file, 0, SEEK_END) != 0 ||
      (length = ftell(file)) <= 0 || (uint64_t)length > QST_AI_MAX_ANALYSIS_BYTES ||
      fseek(file, 0, SEEK_SET) != 0) {
    if (file != NULL) {
      (void)fclose(file);
    }
    return QST_E_RESOURCE;
  }
  bytes = (uint8_t *)calloc((size_t)length + 1, 1);
  if (NULL == bytes || fread(bytes, 1, (size_t)length, file) != (size_t)length) {
    (void)fclose(file);
    free(bytes);
    return QST_E_RESOURCE;
  }
  if (fclose(file) != 0) {
    free(bytes);
    return QST_E_RESOURCE;
  }
  *out_bytes = bytes;
  *out_length = (uint32_t)length;
  return QST_OK;
}

static EQstCode qstAiWriteBytes(int32_t fd, const void *bytes, uint64_t length) {
  const uint8_t *current = (const uint8_t *)bytes;

  if (fd < 0 || (length != 0 && NULL == bytes)) {
    return QST_E_INVALID_ARGUMENT;
  }
  while (length != 0) {
    const ssize_t written = write(fd, current, (size_t)length);
    if (written > 0) {
      current += written;
      length -= (uint64_t)written;
      continue;
    }
    if (written < 0 && errno == EINTR) {
      continue;
    }
    return QST_E_RESOURCE;
  }
  return QST_OK;
}

static void qstAiHexDigest(SQstDigest256 digest, char output[65]) {
  static const char kHex[] = "0123456789abcdef";

  for (uint32_t index = 0; index < sizeof(digest.bytes); ++index) {
    output[index * 2U] = kHex[digest.bytes[index] >> 4U];
    output[index * 2U + 1U] = kHex[digest.bytes[index] & 0x0fU];
  }
  output[64] = '\0';
}

static char *qstAiJsonEscape(const char *text) {
  uint64_t bytes = 0;
  uint64_t escaped_bytes = 0;
  char *output = NULL;

  if (NULL == text) {
    return NULL;
  }
  bytes = strlen(text);
  for (uint64_t index = 0; index < bytes; ++index) {
    const unsigned char value = (unsigned char)text[index];
    const uint64_t extra = value == '"' || value == '\\' ? 2U :
                           value == '\n' || value == '\r' || value == '\t' ? 2U : 1U;
    if (escaped_bytes > UINT64_MAX - extra) {
      return NULL;
    }
    escaped_bytes += extra;
  }
  if (escaped_bytes >= SIZE_MAX) {
    return NULL;
  }
  output = (char *)calloc((size_t)escaped_bytes + 1U, 1);
  if (NULL == output) {
    return NULL;
  }
  for (uint64_t source = 0, target = 0; source < bytes; ++source) {
    const unsigned char value = (unsigned char)text[source];
    if (value == '"' || value == '\\') {
      output[target++] = '\\';
      output[target++] = (char)value;
    } else if (value == '\n' || value == '\r' || value == '\t') {
      output[target++] = '\\';
      output[target++] = value == '\n' ? 'n' : value == '\r' ? 'r' : 't';
    } else if (value < 0x20U) {
      free(output);
      return NULL;
    } else {
      output[target++] = (char)value;
    }
  }
  return output;
}

static EQstCode qstAiWriteActionRecord(
    const char *run_dir, uint32_t entry, uint64_t gid,
    SQstDigest256 action_id, const uint8_t *analysis, uint32_t analysis_bytes,
    const char *remote_issue_id, EQstCode code, const char *state) {
  char path[QST_PATH_BYTES] = {0};
  char action_hex[65] = {0};
  char analysis_hex[65] = {0};
  char *escaped_remote = NULL;
  char *line = NULL;
  uint64_t line_capacity = 0;
  int32_t fd = -1;
  int32_t line_bytes = 0;
  SQstSha256 analysis_hash = {0};
  SQstDigest256 analysis_digest = {0};
  const char *remote = remote_issue_id == NULL ? "" : remote_issue_id;
  EQstCode result = QST_OK;

  if (qstAiMakePath(path, sizeof(path), run_dir, "ai-actions.jsonl") != QST_OK ||
      NULL == state) {
    return QST_E_INVALID_ARGUMENT;
  }
  escaped_remote = qstAiJsonEscape(remote);
  if (NULL == escaped_remote) {
    return QST_E_RESOURCE;
  }
  qstAiHexDigest(action_id, action_hex);
  qst_sha256_init(&analysis_hash);
  if (analysis_bytes != 0 && analysis != NULL) {
    qst_sha256_update(&analysis_hash, analysis, analysis_bytes);
  }
  qst_sha256_final(&analysis_hash, analysis_digest.bytes);
  qstAiHexDigest(analysis_digest, analysis_hex);
  line_capacity = strlen(escaped_remote) + 256U;
  line = (char *)calloc((size_t)line_capacity, 1);
  if (NULL == line) {
    free(escaped_remote);
    return QST_E_RESOURCE;
  }
  line_bytes = snprintf(
      line, (size_t)line_capacity,
      "{\"entry\":%u,\"gid\":%llu,\"action_id\":\"%s\","
      "\"state\":\"%s\",\"code\":%d,\"analysis_sha256\":\"%s\","
      "\"analysis_bytes\":%u,\"remote_issue_id\":\"%s\"}\n",
      entry, (unsigned long long)gid, action_hex, state, code, analysis_hex,
      analysis_bytes, escaped_remote);
  if (line_bytes < 0 || (uint64_t)line_bytes >= line_capacity) {
    free(line);
    free(escaped_remote);
    return QST_E_RESOURCE;
  }
  fd = open(path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0640);
  if (fd < 0) {
    free(line);
    free(escaped_remote);
    return QST_E_RESOURCE;
  }
  if (flock(fd, LOCK_EX) != 0 ||
      qstAiWriteBytes(fd, line, (uint64_t)line_bytes) != QST_OK ||
      fsync(fd) != 0 || flock(fd, LOCK_UN) != 0) {
    result = QST_E_RESOURCE;
  }
  if (close(fd) != 0 && result == QST_OK) {
    result = QST_E_RESOURCE;
  }
  free(line);
  free(escaped_remote);
  return result;
}

static int32_t qstAiLedgerHasSucceeded(const char *run_dir,
                                       SQstDigest256 action_id) {
  char path[QST_PATH_BYTES] = {0};
  char action_hex[65] = {0};
  char line[4096] = {0};
  FILE *file = NULL;

  if (qstAiMakePath(path, sizeof(path), run_dir, "ai-actions.jsonl") != QST_OK) {
    return 0;
  }
  file = fopen(path, "r");
  if (NULL == file) {
    return 0;
  }
  qstAiHexDigest(action_id, action_hex);
  while (fgets(line, sizeof(line), file) != NULL) {
    if (strstr(line, "\"state\":\"succeeded\"") != NULL &&
        strstr(line, action_hex) != NULL) {
      (void)fclose(file);
      return 1;
    }
  }
  (void)fclose(file);
  return 0;
}

static EQstCode qstAiPrecheck(const SQstCliConfig *config, char *run_dir,
                              uint64_t run_dir_bytes, char *prompt_path,
                              uint64_t prompt_path_bytes) {
  char report_path[QST_PATH_BYTES] = {0};
  char config_path[QST_PATH_BYTES] = {0};
  char prompts_dir[QST_PATH_BYTES] = {0};
  const char *prompt_name = NULL;
  struct stat st = {0};

  if (NULL == config || NULL == run_dir || NULL == prompt_path ||
      config->existing_run_id == 0 || config->ai_entry == 0 ||
      config->tool_root[0] == '\0') {
    return QST_E_INVALID_ARGUMENT;
  }
  if (snprintf(run_dir, (size_t)run_dir_bytes, "%s/results/%llu",
               config->tool_root,
               (unsigned long long)config->existing_run_id) < 0 ||
      strlen(run_dir) >= run_dir_bytes ||
      qstAiMakePath(report_path, sizeof(report_path), run_dir, "report.json") != QST_OK ||
      qstAiMakePath(config_path, sizeof(config_path), run_dir, "run-config.txt") != QST_OK ||
      stat(run_dir, &st) != 0 || !S_ISDIR(st.st_mode) ||
      access(report_path, R_OK) != 0 || access(config_path, R_OK) != 0) {
    return QST_E_RESOURCE;
  }
  prompt_name = qstAiPromptName(config->ai_entry);
  if (NULL == prompt_name ||
      qstAiMakePath(prompts_dir, sizeof(prompts_dir), config->tool_root,
                    "prompts") != QST_OK ||
      qstAiMakePath(prompt_path, prompt_path_bytes, prompts_dir, prompt_name) != QST_OK ||
      access(prompt_path, R_OK) != 0 || !qstAiExecutableAvailable(config->ai_command)) {
    return QST_E_RESOURCE;
  }
  if (config->ai_working_directory[0] != '\0' &&
      access(config->ai_working_directory, R_OK | X_OK) != 0) {
    return QST_E_RESOURCE;
  }
  return QST_OK;
}

static EQstCode qstAiSeal(SQstShmMapping *mapping, const uint8_t *bytes,
                          uint32_t length, uint64_t owner_gid,
                          SQstRef *out_ref) {
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == bytes || length == 0 || NULL == out_ref) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qst_block_alloc(mapping, owner_gid, 1, length, out_ref);
  if (code == QST_OK) {
    code = qst_block_write_seal(mapping, out_ref, bytes, length);
  }
  if (code != QST_OK && !qstRefIsEmpty(*out_ref)) {
    (void)qst_block_abort(mapping, *out_ref, owner_gid);
    memset(out_ref, 0, sizeof(*out_ref));
  }
  return code;
}

static int32_t qstAiOpenPidfd(pid_t pid) {
#ifdef SYS_pidfd_open
  const int32_t fd = (int32_t)syscall(SYS_pidfd_open, pid, 0);
  return fd;
#else
  (void)pid;
  return -1;
#endif
}

/* Test-only worker crash hook; inert unless explicitly enabled by the caller. */
static void qstAiTestMaybeKillWorker(uint32_t generation) {
  const char *target = getenv("QST_TEST_KILL_ROLE");
  const char *fifo_path = NULL;
  const char *claim_path = NULL;

  if (NULL == target || strcmp(target, "ai_worker") != 0) {
    return;
  }
  claim_path = getenv("QST_TEST_KILL_ONCE");
  if (NULL != claim_path && claim_path[0] != '\0') {
    const int32_t claim_fd = open(claim_path, O_WRONLY | O_CREAT | O_EXCL |
                                      O_CLOEXEC, 0600);
    if (claim_fd < 0) {
      return;
    }
    (void)close(claim_fd);
  }
  fifo_path = getenv("QST_TEST_KILL_FIFO");
  if (NULL != fifo_path && fifo_path[0] != '\0') {
    int32_t fd = open(fifo_path, O_WRONLY | O_CLOEXEC);
    if (fd >= 0) {
      char line[128] = {0};
      const int32_t written = snprintf(
          line, sizeof(line), "role=ai_worker pid=%d generation=%u\n",
          (int32_t)getpid(), generation);
      if (written > 0 && (uint64_t)written < sizeof(line)) {
        (void)write(fd, line, (size_t)written);
      }
      (void)close(fd);
    }
  }
  (void)kill(getpid(), SIGKILL);
  _exit(128 + SIGKILL);
}

static EQstCode qstAiCaptureCommand(const SQstCliConfig *config,
                                    const char *input_path,
                                    uint32_t timeout_milliseconds,
                                    uint8_t **out_analysis,
                                    uint32_t *out_analysis_bytes) {
  int32_t pipe_fds[2] = {-1, -1};
  int32_t pidfd = -1;
  pid_t pid = -1;
  uint8_t *analysis = NULL;
  uint32_t analysis_bytes = 0;
  uint64_t deadline = 0;
  int32_t status = 0;
  int32_t exited = 0;

  if (NULL == config || NULL == input_path || NULL == out_analysis ||
      NULL == out_analysis_bytes ||
      timeout_milliseconds == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_analysis = NULL;
  *out_analysis_bytes = 0;
  if (pipe2(pipe_fds, O_CLOEXEC | O_NONBLOCK) != 0) {
    return QST_E_RESOURCE;
  }
  pid = fork();
  if (pid < 0) {
    (void)close(pipe_fds[0]);
    (void)close(pipe_fds[1]);
    return QST_E_RESOURCE;
  }
  if (pid == 0) {
    const char *working_directory = config->ai_working_directory;
    int32_t input_fd = open(input_path, O_RDONLY | O_CLOEXEC);
    if (input_fd < 0 || setpgid(0, 0) != 0 ||
        dup2(input_fd, STDIN_FILENO) < 0 || dup2(pipe_fds[1], STDOUT_FILENO) < 0 ||
        dup2(pipe_fds[1], STDERR_FILENO) < 0 || close(pipe_fds[0]) != 0 ||
        close(pipe_fds[1]) != 0 || close(input_fd) != 0) {
      _exit(127);
    }
    if (working_directory[0] != '\0' && chdir(working_directory) != 0) {
      _exit(127);
    }
    execlp(config->ai_command, config->ai_command, (char *)NULL);
    _exit(127);
  }
  (void)close(pipe_fds[1]);
  pipe_fds[1] = -1;
  pidfd = qstAiOpenPidfd(pid);
  if (pidfd < 0 || fcntl(pipe_fds[0], F_SETFL, O_NONBLOCK) != 0 ||
      qstAiMonotonicMilliseconds() == 0) {
    (void)kill(-pid, SIGKILL);
    (void)waitpid(pid, &status, 0);
    (void)close(pipe_fds[0]);
    if (pidfd >= 0) {
      (void)close(pidfd);
    }
    return QST_E_RESOURCE;
  }
  deadline = qstAiMonotonicMilliseconds() + timeout_milliseconds;
  analysis = (uint8_t *)calloc(QST_AI_MAX_ANALYSIS_BYTES + 1, 1);
  if (NULL == analysis) {
    (void)kill(-pid, SIGKILL);
    (void)waitpid(pid, &status, 0);
    (void)close(pipe_fds[0]);
    (void)close(pidfd);
    return QST_E_RESOURCE;
  }
  while (exited == 0) {
    struct pollfd events[2] = {{.fd = pipe_fds[0], .events = POLLIN},
                               {.fd = pidfd, .events = POLLIN}};
    const uint64_t now = qstAiMonotonicMilliseconds();
    int32_t poll_timeout = QST_AI_POLL_MILLISECONDS;
    const int32_t poll_result = now == 0 ? -1 : poll(events, 2, poll_timeout);

    if (poll_result < 0 && errno != EINTR) {
      (void)kill(-pid, SIGKILL);
      free(analysis);
      (void)waitpid(pid, &status, 0);
      (void)close(pipe_fds[0]);
      (void)close(pidfd);
      return QST_E_RESOURCE;
    }
    for (;;) {
      const ssize_t read_bytes = read(pipe_fds[0], analysis + analysis_bytes,
                                      QST_AI_MAX_ANALYSIS_BYTES - analysis_bytes);
      if (read_bytes > 0) {
        analysis_bytes += (uint32_t)read_bytes;
        if (analysis_bytes == QST_AI_MAX_ANALYSIS_BYTES) {
          (void)kill(-pid, SIGKILL);
          free(analysis);
          (void)waitpid(pid, &status, 0);
          (void)close(pipe_fds[0]);
          (void)close(pidfd);
          return QST_E_RESOURCE;
        }
      } else if (read_bytes == 0 || (read_bytes < 0 && errno == EAGAIN)) {
        break;
      } else if (errno != EINTR) {
        (void)kill(-pid, SIGKILL);
        free(analysis);
        (void)waitpid(pid, &status, 0);
        (void)close(pipe_fds[0]);
        (void)close(pidfd);
        return QST_E_RESOURCE;
      }
    }
    if (waitpid(pid, &status, WNOHANG) == pid) {
      exited = 1;
      break;
    }
    if (qstAiMonotonicMilliseconds() >= deadline) {
      (void)kill(-pid, SIGTERM);
      (void)kill(-pid, SIGKILL);
      (void)waitpid(pid, &status, 0);
      free(analysis);
      (void)close(pipe_fds[0]);
      (void)close(pidfd);
      return QST_E_DEADLINE;
    }
  }
  (void)close(pipe_fds[0]);
  (void)close(pidfd);
  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
    free(analysis);
    return QST_E_RESOURCE;
  }
  *out_analysis = analysis;
  *out_analysis_bytes = analysis_bytes;
  return QST_OK;
}

static EQstCode qstAiWorkerRun(SQstShmMapping *mapping,
                               const SQstCliConfig *config,
                               const char *input_path,
                               SQstDigest256 action_id,
                               uint32_t generation,
                               uint32_t timeout_milliseconds) {
  uint8_t *analysis = NULL;
  uint32_t analysis_bytes = 0;
  SQstRef analysis_ref = {0};
  uint64_t action_gid = 0;
  EQstCode code = qst_ai_run_action(mapping, action_id, generation);

  if (code == QST_OK) {
    code = qst_ai_get_action_gid(mapping, action_id, &action_gid);
  }
  if (code == QST_OK) {
    code = qstAiCaptureCommand(config, input_path, timeout_milliseconds, &analysis,
                               &analysis_bytes);
  }
  if (code == QST_OK && analysis_bytes != 0) {
    code = qstAiSeal(mapping, analysis, analysis_bytes,
                     action_gid, &analysis_ref);
  }
  if (qst_ai_publish_result(mapping, action_id, analysis_ref, code, generation) !=
      QST_OK) {
    if (!qstRefIsEmpty(analysis_ref)) {
      (void)qst_block_release(mapping, analysis_ref, action_gid);
    }
    free(analysis);
    return QST_E_RESOURCE;
  }
  free(analysis);
  return code;
}

static EQstCode qstAiGetActionCode(const SQstShmMapping *mapping,
                                   SQstDigest256 action_id,
                                   EQstCode *out_code) {
  const SQstRegion *region = NULL;
  SQstAiActionEntry *entries = NULL;

  if (NULL == mapping || NULL == mapping->header || NULL == mapping->base ||
      NULL == out_code) {
    return QST_E_INVALID_ARGUMENT;
  }
  region = &mapping->header->ai_action_region;
  if (region->element_bytes != sizeof(SQstAiActionEntry) ||
      region->region_bytes != (uint64_t)region->element_count * region->element_bytes ||
      region->region_offset > mapping->bytes ||
      region->region_bytes > mapping->bytes - region->region_offset) {
    return QST_E_SHM_CORRUPT;
  }
  entries = (SQstAiActionEntry *)((uint8_t *)mapping->base + region->region_offset);
  for (uint32_t index = 0; index < region->element_count; ++index) {
    const uint32_t state = atomic_load_explicit(&entries[index].state,
                                                memory_order_acquire);
    if (memcmp(entries[index].action_id.bytes, action_id.bytes,
               sizeof(action_id.bytes)) == 0 &&
        (state == QST_AI_SUCCEEDED || state == QST_AI_FAILED ||
         state == QST_AI_ABANDONED)) {
      *out_code = entries[index].action_code;
      return QST_OK;
    }
  }
  return QST_E_QUEUE_EMPTY;
}

static uint32_t qstAiReadTargetBits(const char *path, uint32_t fallback) {
  FILE *file = NULL;
  char line[256] = {0};
  uint32_t bits = fallback;

  if (NULL == path || (file = fopen(path, "r")) == NULL) {
    return bits;
  }
  while (fgets(line, sizeof(line), file) != NULL) {
    unsigned long parsed = 0;
    if (sscanf(line, "target_bits=%lu", &parsed) == 1 && parsed != 0 &&
        parsed <= UINT32_MAX) {
      bits = (uint32_t)parsed;
      break;
    }
  }
  (void)fclose(file);
  return bits;
}

static EQstCode qstAiParseVerdict(const uint8_t *analysis,
                                  uint32_t analysis_bytes, int32_t *out_verdict,
                                  char **out_diff, char **out_content) {
  char *text = NULL;
  tools_cJSON *root = NULL;
  tools_cJSON *verdict = NULL;
  tools_cJSON *diff = NULL;
  tools_cJSON *content = NULL;

  if (NULL == analysis || analysis_bytes == 0 || NULL == out_verdict ||
      NULL == out_diff || NULL == out_content || analysis_bytes >= SIZE_MAX) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_verdict = 2;
  *out_diff = NULL;
  *out_content = NULL;
  text = (char *)calloc((size_t)analysis_bytes + 1U, 1);
  if (NULL == text) {
    return QST_E_RESOURCE;
  }
  memcpy(text, analysis, analysis_bytes);
  root = tools_cJSON_ParseWithOpts(text, NULL, 1);
  verdict = tools_cJSON_GetObjectItemCaseSensitive(root, "verdict");
  diff = tools_cJSON_GetObjectItemCaseSensitive(root, "diff_summary");
  content = tools_cJSON_GetObjectItemCaseSensitive(root, "analysis_content");
  if (!tools_cJSON_IsObject(root) || !tools_cJSON_IsNumber(verdict) ||
      verdict->valueint < 0 || verdict->valueint > 2 ||
      (diff != NULL && !tools_cJSON_IsString(diff)) ||
      (content != NULL && !tools_cJSON_IsString(content))) {
    tools_cJSON_Delete(root);
    free(text);
    return QST_E_RESULT_STORE;
  }
  *out_verdict = verdict->valueint;
  *out_diff = strdup(diff == NULL ? "AI verdict parsed" : diff->valuestring);
  *out_content = strdup(content == NULL ? text : content->valuestring);
  tools_cJSON_Delete(root);
  free(text);
  if (NULL == *out_diff || NULL == *out_content) {
    free(*out_diff);
    free(*out_content);
    *out_diff = NULL;
    *out_content = NULL;
    return QST_E_RESOURCE;
  }
  return QST_OK;
}

static EQstCode qstAiRunManualOne(const SQstCliConfig *config,
                                  const SQstStoredResult *stored,
                                  SQstResultStore *store,
                                  const SQstPendingStore *pending_store,
                                  uint64_t record_seq,
                                  uint32_t timeout_milliseconds) {
  SQstRunConfig run_config = {0};
  SQstResourceInputs inputs = {0};
  SQstResourceProfile profile = {0};
  SQstShmMapping mapping = {0};
  SQstRef prompt_ref = {0};
  SQstRef manifest_ref = {0};
  SQstDigest256 action_id = {0};
  SQstManualAiResult manual = {0};
  char run_dir[QST_PATH_BYTES] = {0};
  char prompt_path[QST_PATH_BYTES] = {0};
  char logs_dir[QST_PATH_BYTES] = {0};
  char input_path[QST_PATH_BYTES] = {0};
  char material[QST_PATH_BYTES * 2] = {0};
  uint8_t *prompt = NULL;
  uint32_t prompt_bytes = 0;
  uint32_t material_bytes = 0;
  uint32_t main_generation = 0;
  uint32_t worker_generation = 0;
  uint8_t *analysis = NULL;
  uint32_t analysis_bytes = 0;
  char *diff = NULL;
  char *analysis_content = NULL;
  pid_t worker_pid = -1;
  int32_t worker_status = 0;
  uint32_t durable_source = 0;
  int32_t action_skipped = 0;
  EQstCode action_code = QST_E_RESOURCE;
  EQstCode code = QST_OK;

  if (NULL == config || NULL == stored || NULL == store || stored->gid == 0 ||
      stored->ds_fingerprint == NULL || strlen(stored->ds_fingerprint) != 64U) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (snprintf(run_dir, sizeof(run_dir), "%s/results/%llu", config->tool_root,
               (unsigned long long)config->existing_run_id) < 0 ||
      qstAiMakePath(logs_dir, sizeof(logs_dir), run_dir, "logs") != QST_OK ||
      qstAiMakePath(input_path, sizeof(input_path), run_dir,
                    "ai-input.txt") != QST_OK ||
      qstAiMakePath(prompt_path, sizeof(prompt_path), config->tool_root,
                    "prompts/result_judgment.md") != QST_OK ||
      qstAiReadFile(prompt_path, &prompt, &prompt_bytes) != QST_OK) {
    free(prompt);
    return QST_E_RESOURCE;
  }
  material_bytes = (uint32_t)snprintf(
      material, sizeof(material),
      "run_id=%llu\ngid=%llu\ntarget=%u\nds_fingerprint=%s\n"
      "result_content_bytes=%llu\nuser_prompt=%s\n",
      (unsigned long long)config->existing_run_id,
      (unsigned long long)stored->gid, stored->target, stored->ds_fingerprint,
      (unsigned long long)stored->result_content.bytes, config->prompt);
  if (material_bytes == 0 || material_bytes >= sizeof(material) ||
      qst_resource_capture(run_dir, logs_dir, &inputs) != QST_OK) {
    free(prompt);
    return QST_E_RESOURCE;
  }
  {
    int32_t fd = open(input_path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0640);
    if (fd < 0 || qstAiWriteBytes(fd, prompt, prompt_bytes) != QST_OK ||
        qstAiWriteBytes(fd, "\n\n", 2) != QST_OK ||
        qstAiWriteBytes(fd, config->prompt, strlen(config->prompt)) != QST_OK ||
        qstAiWriteBytes(fd, "\n\n", 2) != QST_OK ||
        qstAiWriteBytes(fd, material, material_bytes) != QST_OK ||
        qstAiWriteBytes(fd, "\nresult_content=\n", strlen("\nresult_content=\n")) != QST_OK ||
        qstAiWriteBytes(fd, stored->result_content.data,
                        stored->result_content.bytes) != QST_OK ||
        fsync(fd) != 0) {
      if (fd >= 0) (void)close(fd);
      free(prompt);
      return QST_E_RESOURCE;
    }
    if (close(fd) != 0) {
      free(prompt);
      return QST_E_RESOURCE;
    }
  }
  run_config = config->run_config;
  run_config.run_id = config->existing_run_id;
  run_config.tool_root = config->tool_root;
  run_config.result_dir = run_dir;
  run_config.ai_action_entry_count = 1;
  code = qst_resource_calculate(&inputs, &run_config, &profile);
  if (code == QST_OK) code = qst_shm_create(&run_config, &mapping);
  if (code == QST_OK) {
    code = qst_process_register(&mapping, QST_ROLE_LAUNCHER, 1, QST_ROLE_MAIN,
                                (int32_t)getpid(), (int32_t)getpgrp(),
                                &main_generation);
  }
  if (code == QST_OK) code = qstAiSeal(&mapping, prompt, prompt_bytes,
                                       stored->gid, &prompt_ref);
  if (code == QST_OK) code = qstAiSeal(&mapping, (const uint8_t *)material,
                                       material_bytes, stored->gid, &manifest_ref);
  if (code == QST_OK) {
    code = qst_ai_precheck(&mapping, stored->gid,
                           (EQstAiEntry)config->ai_entry, prompt_ref,
                           manifest_ref, &action_id);
  }
  free(prompt);
  prompt = NULL;
  if (code != QST_OK) goto _cleanup;
  if (qstAiLedgerHasSucceeded(run_dir, action_id) != 0) {
    action_skipped = 1;
    code = QST_OK;
    goto _cleanup;
  }
  worker_pid = fork();
  if (worker_pid < 0) {
    code = QST_E_RESOURCE;
    goto _cleanup;
  }
  if (worker_pid == 0) {
    uint32_t child_generation = 0;
    EQstCode child_code = qst_process_register(
        &mapping, QST_ROLE_MAIN, main_generation, QST_ROLE_AI_WORKER,
        (int32_t)getpid(), (int32_t)getpgrp(), &child_generation);
    if (child_code == QST_OK) {
      qstAiTestMaybeKillWorker(child_generation);
      child_code = qstAiWorkerRun(
          &mapping, config, input_path, action_id, child_generation,
          timeout_milliseconds == 0 ? QST_AI_DEFAULT_TIMEOUT_MILLISECONDS :
                                       timeout_milliseconds);
    }
    _exit(child_code == QST_OK ? 0 : 1);
  }
  while (waitpid(worker_pid, &worker_status, 0) < 0 && errno == EINTR) {
  }
  (void)qst_process_find_generation(&mapping, QST_ROLE_AI_WORKER,
                                    (int32_t)worker_pid, &worker_generation);
  if (worker_generation != 0) {
    (void)qst_process_reap_generation(&mapping, QST_ROLE_MAIN, main_generation,
                                      QST_ROLE_AI_WORKER, worker_generation,
                                      worker_status);
  }
  if (qstAiGetActionCode(&mapping, action_id, &action_code) == QST_OK) {
    code = action_code;
  } else {
    code = QST_E_RESOURCE;
  }
  if (code == QST_OK) {
    code = qst_ai_copy_action_analysis(&mapping, action_id, &analysis,
                                       &analysis_bytes);
  }
  if (code == QST_OK) {
    code = qstAiParseVerdict(analysis, analysis_bytes, &manual.verdict,
                             &diff, &analysis_content);
  }
  if (code == QST_OK) {
    manual.gid = stored->gid;
    manual.target = stored->target;
    manual.ds_fingerprint = stored->ds_fingerprint;
    manual.result_content = manual.verdict == 2 ? &stored->result_content : NULL;
    manual.validator = "ai";
    manual.diff_summary = diff;
    manual.analysis_content = analysis_content;
    memcpy(&manual.action_id_hash, action_id.bytes,
           sizeof(manual.action_id_hash));
    code = qst_result_store_write_manual_ai_result(
        &mapping, store, pending_store, &manual, record_seq, &durable_source);
  }
_cleanup:
  if (action_skipped == 0) {
    (void)qstAiWriteActionRecord(
        run_dir, config->ai_entry, stored->gid, action_id, analysis,
        analysis_bytes, NULL, code, code == QST_OK ? "succeeded" : "failed");
  }
  free(analysis);
  free(diff);
  free(analysis_content);
  if (!qstRefIsEmpty(prompt_ref)) (void)qst_block_release(&mapping, prompt_ref, stored->gid);
  if (!qstRefIsEmpty(manifest_ref)) (void)qst_block_release(&mapping, manifest_ref, stored->gid);
  if (mapping.base != NULL) (void)qst_shm_unmap(&mapping);
  (void)unlink(input_path);
  return code;
}

static EQstCode qstAiRunManualCli(const SQstCliConfig *config,
                                  uint32_t timeout_milliseconds) {
  char run_dir[QST_PATH_BYTES] = {0};
  char config_path[QST_PATH_BYTES] = {0};
  char pending_dir[QST_PATH_BYTES] = {0};
  char quarantine_dir[QST_PATH_BYTES] = {0};
  char overflow_path[QST_PATH_BYTES] = {0};
  SQstResultStoreConfig store_config = {0};
  SQstResultStore store = {0};
  SQstPendingStore pending = {0};
  EQstCode code = QST_OK;
  EQstCode first_error = QST_OK;
  uint64_t record_seq = 1;
  uint32_t target_bits = 0;

  if (NULL == config || (config->ai_entry != QST_AI_ENTRY_MANUAL_UNVERIFIED &&
                         config->ai_entry != QST_AI_ENTRY_MANUAL_ALL)) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (snprintf(run_dir, sizeof(run_dir), "%s/results/%llu", config->tool_root,
               (unsigned long long)config->existing_run_id) < 0 ||
      snprintf(config_path, sizeof(config_path), "%s/run-config.txt", run_dir) < 0 ||
      snprintf(pending_dir, sizeof(pending_dir), "%s/pending-results", run_dir) < 0 ||
      snprintf(quarantine_dir, sizeof(quarantine_dir), "%s/quarantine", pending_dir) < 0 ||
      snprintf(overflow_path, sizeof(overflow_path), "%s/overflow_content.ndjson",
               config->tool_root) < 0) {
    return QST_E_RESOURCE;
  }
  target_bits = qstAiReadTargetBits(config_path, config->run_config.target_selection_bits);
  store_config.run_id = config->existing_run_id;
  store_config.pending_dir = pending_dir;
  store_config.quarantine_dir = quarantine_dir;
  store_config.overflow_path = overflow_path;
  code = qst_result_store_open(&store_config, &store);
  if (code != QST_OK) {
    memset(&store, 0, sizeof(store));
    store.run_id = config->existing_run_id;
    (void)snprintf(store.pending_dir, sizeof(store.pending_dir), "%s", pending_dir);
    (void)snprintf(store.quarantine_dir, sizeof(store.quarantine_dir), "%s", quarantine_dir);
    (void)snprintf(store.overflow_path, sizeof(store.overflow_path), "%s", overflow_path);
  }
  pending.run_id = config->existing_run_id;
  pending.pending_dir = store.pending_dir;
  pending.quarantine_dir = store.quarantine_dir;
  pending.quota_bytes = 0;
  for (uint32_t target = QST_TARGET_TD; target <= QST_TARGET_INFLUX; ++target) {
    SQstStoredResult *records = NULL;
    uint32_t count = 0;
    uint64_t seen[1024] = {0};
    uint32_t seen_count = 0;
    if ((target_bits & (UINT32_C(1) << (target - QST_TARGET_TD))) == 0) continue;
    if (store.native_taos == NULL ||
        (code = qst_result_store_read_results(&store, target, &records, &count)) != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: manual result read failed run_id=%llu target=%u code=%d\n",
                    (unsigned long long)config->existing_run_id, target, code);
      if (first_error == QST_OK) first_error = code == QST_OK ? QST_E_RESULT_STORE : code;
      continue;
    }
    for (uint32_t index = 0; index < count; ++index) {
      int32_t duplicate = 0;
      for (uint32_t seen_index = 0; seen_index < seen_count; ++seen_index) {
        if (seen[seen_index] == records[index].gid) duplicate = 1;
      }
      if (duplicate != 0) continue;
      if (seen_count < sizeof(seen) / sizeof(seen[0])) seen[seen_count++] = records[index].gid;
      if (config->ai_entry == QST_AI_ENTRY_MANUAL_UNVERIFIED && records[index].verdict != 2) continue;
      code = qstAiRunManualOne(config, &records[index], &store, &pending,
                               record_seq++, timeout_milliseconds);
      if (code != QST_OK && first_error == QST_OK) first_error = code;
    }
    qst_result_store_free_results(records, count);
  }
  (void)qst_result_store_close(&store);
  return first_error;
}

EQstCode qst_ai_run_cli(const SQstCliConfig *config,
                        uint32_t timeout_milliseconds) {
  if (NULL == config || config->ai_entry == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (config->ai_entry == QST_AI_ENTRY_MANUAL_UNVERIFIED ||
      config->ai_entry == QST_AI_ENTRY_MANUAL_ALL) {
    return qstAiRunManualCli(config, timeout_milliseconds);
  }
  return QST_E_INVALID_ARGUMENT;
}
