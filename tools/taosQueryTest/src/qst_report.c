/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

/* Durable, rebuildable run-summary artifacts. */

#define _POSIX_C_SOURCE 200809L

#include "qst_report.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "qst_task.h"
#include "qst_provider.h"

enum {
  QST_REPORT_PATH_BYTES = 4096,
  /* Random mode retains one runtime fact per pair generation. */
  QST_REPORT_TEXT_BYTES = 1024 * 1024,
};

typedef struct {
  uint64_t generated;
  uint64_t executing;
  uint64_t final;
  uint64_t final_published;
  uint64_t persisting;
  uint64_t persisted;
  uint64_t isolated;
  uint64_t result_memory;
  uint64_t result_unavailable;
  uint64_t ai_prechecked;
  uint64_t ai_running;
  uint64_t ai_succeeded;
  uint64_t ai_failed;
  uint64_t ai_abandoned;
  uint64_t process_registered;
  uint64_t process_running;
  uint64_t process_reaped;
  uint64_t process_failed;
  uint64_t execution_success;
  uint64_t execution_error;
  uint64_t execution_timeout;
  uint64_t execution_cancelled;
  uint64_t verdict_passed;
  uint64_t verdict_failed;
  uint64_t verdict_unverified;
} SQstReportCounts;

/* Reads the immutable readiness artifact produced during dataset preparation. */
static EQstCode qstReportReadSnapshot(const char *path, char *out_text,
                                      uint64_t out_bytes) {
  struct stat status = {0};
  int32_t fd = -1;
  uint64_t read_bytes = 0;

  if (NULL == path || NULL == out_text || out_bytes < 2U ||
      stat(path, &status) != 0 || !S_ISREG(status.st_mode) ||
      status.st_size <= 0 || (uint64_t)status.st_size >= out_bytes) {
    return QST_E_RESOURCE;
  }
  fd = open(path, O_RDONLY | O_CLOEXEC);
  if (fd < 0) return QST_E_RESOURCE;
  while (read_bytes < (uint64_t)status.st_size) {
    const ssize_t result = read(fd, out_text + read_bytes,
                                (size_t)((uint64_t)status.st_size - read_bytes));
    if (result < 0 && errno == EINTR) continue;
    if (result <= 0) {
      (void)close(fd);
      return QST_E_RESOURCE;
    }
    read_bytes += (uint64_t)result;
  }
  if (close(fd) != 0 || memchr(out_text, '\0', read_bytes) != NULL) {
    return QST_E_RESOURCE;
  }
  out_text[read_bytes] = '\0';
  return out_text[0] == '{' && out_text[read_bytes - 1U] == '}' ? QST_OK :
                                                                    QST_E_RESOURCE;
}

static void qstReportDigestHex(SQstDigest256 digest, char out_hex[65]) {
  static const char kHex[] = "0123456789abcdef";

  for (uint32_t index = 0; index < sizeof(digest.bytes); ++index) {
    out_hex[index * 2U] = kHex[digest.bytes[index] >> 4U];
    out_hex[index * 2U + 1U] = kHex[digest.bytes[index] & 0x0fU];
  }
  out_hex[64] = '\0';
}

static const char *qstReportRunStatus(uint32_t run_state) {
  switch (run_state) {
    case QST_RUN_PREPARING:
      return "preparing";
    case QST_RUN_RUNNING:
      return "running";
    case QST_RUN_DRAINING:
      return "draining";
    case QST_RUN_COMPLETED:
      return "completed";
    case QST_RUN_FAILED:
      return "failed";
    case QST_RUN_CANCELLED:
      return "cancelled";
    default:
      return "invalid";
  }
}

static const char *qstReportStopReason(uint32_t stop_reason) {
  switch (stop_reason) {
    case QST_STOP_NONE:
      return "none";
    case QST_STOP_FULL_COMPLETE:
      return "full_coverage_complete";
    case QST_STOP_STATEMENT_LIMIT:
      return "statement_limit";
    case QST_STOP_DURATION_LIMIT:
      return "duration_limit";
    case QST_STOP_USER_REQUEST:
      return "user_request";
    case QST_STOP_PREPARE_FAILED:
      return "prepare_failed";
    case QST_STOP_INFRASTRUCTURE_FAILED:
      return "infrastructure_failed";
    case QST_STOP_SHM_CORRUPT:
      return "shared_memory_corrupt";
    default:
      return NULL;
  }
}

static EQstCode qstReportWriteAll(int32_t fd, const char *text,
                                  uint64_t text_bytes) {
  uint64_t written = 0;

  if (fd < 0 || NULL == text) {
    return QST_E_INVALID_ARGUMENT;
  }
  while (written < text_bytes) {
    const ssize_t bytes = write(fd, text + written, (size_t)(text_bytes - written));

    if (bytes < 0) {
      if (errno == EINTR) {
        continue;
      }
      return QST_E_RESOURCE;
    }
    if (bytes == 0) {
      return QST_E_RESOURCE;
    }
    written += (uint64_t)bytes;
  }
  return QST_OK;
}

static EQstCode qstReportReplaceFile(const char *path, const char *text) {
  char temporary[QST_REPORT_PATH_BYTES] = {0};
  char directory[QST_REPORT_PATH_BYTES] = {0};
  char *separator = NULL;
  int32_t directory_fd = -1;
  int32_t fd = -1;
  int32_t formatted = 0;
  EQstCode code = QST_OK;

  if (NULL == path || NULL == text) {
    return QST_E_INVALID_ARGUMENT;
  }
  formatted = snprintf(temporary, sizeof(temporary), "%s.tmp", path);
  if (formatted < 0 || (uint64_t)formatted >= sizeof(temporary)) {
    return QST_E_RESOURCE;
  }
  fd = open(temporary, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0640);
  if (fd < 0) {
    return QST_E_RESOURCE;
  }
  code = qstReportWriteAll(fd, text, strlen(text));
  if (code == QST_OK && fsync(fd) != 0) {
    code = QST_E_RESOURCE;
  }
  if (close(fd) != 0 && code == QST_OK) {
    code = QST_E_RESOURCE;
  }
  if (code != QST_OK) {
    (void)unlink(temporary);
    return code;
  }
  if (rename(temporary, path) != 0) {
    (void)unlink(temporary);
    return QST_E_RESOURCE;
  }
  formatted = snprintf(directory, sizeof(directory), "%s", path);
  if (formatted < 0 || (uint64_t)formatted >= sizeof(directory)) {
    return QST_E_RESOURCE;
  }
  separator = strrchr(directory, '/');
  if (NULL == separator) {
    return QST_E_INVALID_ARGUMENT;
  }
  *separator = '\0';
  directory_fd = open(directory, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (directory_fd >= 0) {
    (void)fsync(directory_fd);
    (void)close(directory_fd);
  }
  return QST_OK;
}

static EQstCode qstReportCountTasks(const SQstShmMapping *mapping,
                                    SQstReportCounts *out_counts) {
  const SQstRegion *region = NULL;
  SQstTaskEntry *entries = NULL;

  if (NULL == mapping || NULL == mapping->header || NULL == mapping->base ||
      NULL == out_counts) {
    return QST_E_INVALID_ARGUMENT;
  }
  region = &mapping->header->task_region;
  if (region->element_bytes != sizeof(SQstTaskEntry) ||
      region->element_count == 0 || region->region_offset > mapping->bytes ||
      region->region_bytes != (uint64_t)region->element_count * region->element_bytes ||
      region->region_bytes > mapping->bytes - region->region_offset) {
    return QST_E_SHM_CORRUPT;
  }
  memset(out_counts, 0, sizeof(*out_counts));
  entries = (SQstTaskEntry *)((uint8_t *)mapping->base + region->region_offset);
  for (uint32_t index = 0; index < region->element_count; ++index) {
    const uint32_t phase = atomic_load_explicit(&entries[index].phase,
                                                memory_order_acquire);
    const uint32_t storage = atomic_load_explicit(
        &entries[index].result_storage_status, memory_order_acquire);

    if (entries[index].gid != 0) {
      ++out_counts->generated;
    }

    switch (phase) {
      case QST_TASK_GENERATED:
        break;
      case QST_TASK_EXECUTING:
        ++out_counts->executing;
        break;
      case QST_TASK_FINAL:
        ++out_counts->final;
        break;
      case QST_TASK_FINAL_PUBLISHED:
        ++out_counts->final_published;
        break;
      case QST_TASK_PERSISTING:
        ++out_counts->persisting;
        break;
      case QST_TASK_PERSISTED:
        ++out_counts->persisted;
        break;
      case QST_TASK_ISOLATED:
        ++out_counts->isolated;
        break;
      case 0:
      case QST_TASK_REGISTERING:
        break;
      default:
        return QST_E_SHM_CORRUPT;
    }
    if (storage == QST_RESULT_MEMORY) {
      ++out_counts->result_memory;
    } else if (storage == QST_RESULT_UNAVAILABLE) {
      ++out_counts->result_unavailable;
    } else if (storage != 0 && (storage < QST_RESULT_NOT_REQUESTED ||
                                storage > QST_RESULT_UNAVAILABLE)) {
      return QST_E_SHM_CORRUPT;
    }
  }
  {
    const uint64_t generated = atomic_load_explicit(
        &mapping->header->generated_task_count, memory_order_acquire);
    const uint64_t persisted = atomic_load_explicit(
        &mapping->header->persisted_task_count, memory_order_acquire);
    const uint64_t isolated = atomic_load_explicit(
        &mapping->header->isolated_task_count, memory_order_acquire);
    const uint64_t result_memory = atomic_load_explicit(
        &mapping->header->result_memory_count, memory_order_acquire);
    const uint64_t result_unavailable = atomic_load_explicit(
        &mapping->header->result_unavailable_count, memory_order_acquire);

    /* Current terminal slots remain observable until main reclaims them. */
    if (generated > out_counts->generated) {
      out_counts->generated = generated;
    }
    if (persisted > out_counts->persisted) {
      out_counts->persisted = persisted;
    }
    if (isolated > out_counts->isolated) {
      out_counts->isolated = isolated;
    }
    if (result_memory > out_counts->result_memory) {
      out_counts->result_memory = result_memory;
    }
    if (result_unavailable > out_counts->result_unavailable) {
      out_counts->result_unavailable = result_unavailable;
    }
  }
  return QST_OK;
}

static EQstCode qstReportCountAiActions(const SQstShmMapping *mapping,
                                        SQstReportCounts *inout_counts) {
  const SQstRegion *region = NULL;
  SQstAiActionEntry *entries = NULL;

  if (NULL == mapping || NULL == mapping->header || NULL == mapping->base ||
      NULL == inout_counts) {
    return QST_E_INVALID_ARGUMENT;
  }
  region = &mapping->header->ai_action_region;
  if (region->element_bytes != sizeof(SQstAiActionEntry) ||
      region->element_count == 0 || region->region_offset > mapping->bytes ||
      region->region_bytes != (uint64_t)region->element_count * region->element_bytes ||
      region->region_bytes > mapping->bytes - region->region_offset) {
    return QST_E_SHM_CORRUPT;
  }
  entries = (SQstAiActionEntry *)((uint8_t *)mapping->base + region->region_offset);
  for (uint32_t index = 0; index < region->element_count; ++index) {
    switch (atomic_load_explicit(&entries[index].state, memory_order_acquire)) {
      case 0:
      case QST_AI_REGISTERING:
        break;
      case QST_AI_PRECHECKED:
        ++inout_counts->ai_prechecked;
        break;
      case QST_AI_RUNNING:
        ++inout_counts->ai_running;
        break;
      case QST_AI_SUCCEEDED:
        ++inout_counts->ai_succeeded;
        break;
      case QST_AI_FAILED:
        ++inout_counts->ai_failed;
        break;
      case QST_AI_ABANDONED:
        ++inout_counts->ai_abandoned;
        break;
      default:
        return QST_E_SHM_CORRUPT;
    }
  }
  return QST_OK;
}

static EQstCode qstReportCountProcesses(const SQstShmMapping *mapping,
                                        SQstReportCounts *inout_counts) {
  const SQstRegion *region = NULL;
  SQstProcessSlot *slots = NULL;

  if (NULL == mapping || NULL == mapping->header || NULL == mapping->base ||
      NULL == inout_counts) {
    return QST_E_INVALID_ARGUMENT;
  }
  region = &mapping->header->process_region;
  if (region->element_bytes != sizeof(SQstProcessSlot) ||
      region->region_offset > mapping->bytes ||
      region->region_bytes != (uint64_t)region->element_count * region->element_bytes ||
      region->region_bytes > mapping->bytes - region->region_offset) {
    return QST_E_SHM_CORRUPT;
  }
  slots = (SQstProcessSlot *)((uint8_t *)mapping->base + region->region_offset);
  for (uint32_t index = 0; index < region->element_count; ++index) {
    const uint32_t role = atomic_load_explicit(&slots[index].role, memory_order_acquire);
    const uint32_t state = atomic_load_explicit(&slots[index].exit_state,
                                                memory_order_acquire);

    if (role == 0) {
      continue;
    }
    ++inout_counts->process_registered;
    if (state == QST_PROCESS_RUNNING) {
      ++inout_counts->process_running;
    } else if (state == QST_PROCESS_REAPED) {
      ++inout_counts->process_reaped;
    } else if (state == QST_PROCESS_EXITED) {
      ++inout_counts->process_failed;
    } else if (state != QST_PROCESS_STOP_REQUESTED &&
               state != QST_PROCESS_STOP_ACKED) {
      return QST_E_SHM_CORRUPT;
    }
  }
  return QST_OK;
}

/* Reads pair runtime facts so crash/rebuild evidence is present in reports. */
static EQstCode qstReportBuildPairInfo(const SQstRunConfig *config,
                                       char *out_json, uint64_t out_bytes) {
  char pairs_path[QST_REPORT_PATH_BYTES] = {0};
  DIR *directory = NULL;
  struct dirent *entry = NULL;
  uint64_t used = 0;
  uint32_t pair_count = 0;

  if (NULL == config || NULL == config->result_dir || NULL == out_json ||
      out_bytes == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (snprintf(pairs_path, sizeof(pairs_path), "%s/pairs", config->result_dir) <
      0) {
    return QST_E_RESOURCE;
  }
  directory = opendir(pairs_path);
  if (NULL == directory) {
    if (errno == ENOENT) {
      return snprintf(out_json, (size_t)out_bytes,
                      "\"parallel_pair_count\":0,\"pairs\":[]") >= 0
                 ? QST_OK
                 : QST_E_RESOURCE;
    }
    return QST_E_RESOURCE;
  }
  out_json[0] = '\0';
  while ((entry = readdir(directory)) != NULL) {
    char runtime_path[QST_REPORT_PATH_BYTES] = {0};
    char fact[2048] = {0};
    FILE *file = NULL;
    size_t fact_bytes = 0;
    int32_t written = 0;

    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    if (snprintf(runtime_path, sizeof(runtime_path), "%s/%s/runtime.json",
                 pairs_path, entry->d_name) < 0) {
      (void)closedir(directory);
      return QST_E_RESOURCE;
    }
    file = fopen(runtime_path, "r");
    if (NULL == file) {
      (void)closedir(directory);
      return QST_E_RESOURCE;
    }
    fact_bytes = fread(fact, 1, sizeof(fact) - 1U, file);
    if (ferror(file) != 0 || fclose(file) != 0 || fact_bytes == 0 ||
        fact_bytes >= sizeof(fact) - 1U) {
      (void)closedir(directory);
      return QST_E_RESOURCE;
    }
    fact[fact_bytes] = '\0';
    while (fact_bytes > 0 && (fact[fact_bytes - 1U] == '\n' ||
                              fact[fact_bytes - 1U] == '\r' ||
                              fact[fact_bytes - 1U] == ' ' ||
                              fact[fact_bytes - 1U] == '\t')) {
      fact[--fact_bytes] = '\0';
    }
    if (fact_bytes == 0 || fact[0] != '{' || fact[fact_bytes - 1U] != '}') {
      (void)closedir(directory);
      return QST_E_RESOURCE;
    }
    written = snprintf(out_json + used, (size_t)(out_bytes - used), "%s%s",
                       pair_count == 0 ? "" : ",", fact);
    if (written < 0 || (uint64_t)written >= out_bytes - used) {
      (void)closedir(directory);
      return QST_E_RESOURCE;
    }
    used += (uint64_t)written;
    if (pair_count == UINT32_MAX) {
      (void)closedir(directory);
      return QST_E_RESOURCE;
    }
    ++pair_count;
  }
  if (closedir(directory) != 0) {
    return QST_E_RESOURCE;
  }
  {
    char facts[QST_REPORT_TEXT_BYTES] = {0};
    const int32_t copied = snprintf(facts, sizeof(facts), "%s", out_json);
    const int32_t written = snprintf(
        out_json, (size_t)out_bytes,
        "\"parallel_pair_count\":%u,\"pairs\":[%s]", pair_count,
        copied < 0 ? "" : facts);

    return written >= 0 && (uint64_t)written < out_bytes ? QST_OK :
                                                            QST_E_RESOURCE;
  }
}

/* Reads the generator-owned FULL round counter without exposing a mutable state. */
static EQstCode qstReportFullCoverageRoundCount(
    const SQstShmMapping *mapping, uint64_t *out_round_count) {
  const SQstRegion *region = NULL;
  const SQstGenerationState *state = NULL;

  if (NULL == mapping || NULL == mapping->base || NULL == mapping->header ||
      NULL == out_round_count) {
    return QST_E_INVALID_ARGUMENT;
  }
  region = &mapping->header->generation_state_region;
  if (region->region_offset == 0 && region->element_bytes == 0 &&
      region->element_count == 0 && region->region_bytes == 0) {
    *out_round_count = 0;
    return QST_E_STOPPING;
  }
  if (region->element_bytes != sizeof(SQstGenerationState) ||
      region->element_count != 1 || region->region_bytes != sizeof(*state) ||
      region->region_offset > mapping->bytes ||
      region->region_bytes > mapping->bytes - region->region_offset) {
    return QST_E_SHM_CORRUPT;
  }
  state = (const SQstGenerationState *)((const uint8_t *)mapping->base +
                                        region->region_offset);
  if (state->format_version == 0) {
    *out_round_count = 0;
    return QST_E_STOPPING;
  }
  if (state->format_version != QST_GENERATION_STATE_FORMAT_VERSION ||
      state->catalog_version == 0 ||
      state->catalog_version != mapping->header->dimension_catalog_version ||
      memcmp(state->catalog_digest.bytes,
             mapping->header->dimension_catalog_digest.bytes,
             sizeof(state->catalog_digest.bytes)) != 0) {
    return QST_E_SHM_CORRUPT;
  }
  *out_round_count = atomic_load_explicit(&state->full_coverage_round_count,
                                          memory_order_acquire);
  return QST_OK;
}

EQstCode qst_report_refresh(const SQstShmMapping *mapping,
                            const SQstRunConfig *config) {
  SQstReportCounts counts = {0};
  char json_path[QST_REPORT_PATH_BYTES] = {0};
  char markdown_path[QST_REPORT_PATH_BYTES] = {0};
  char json[QST_REPORT_TEXT_BYTES] = {0};
  char markdown[QST_REPORT_TEXT_BYTES] = {0};
  char pair_info[QST_REPORT_TEXT_BYTES] = {0};
  int32_t formatted = 0;
  uint32_t run_state = 0;
  uint32_t stop_reason = 0;
  char provider_json[QST_REPORT_TEXT_BYTES] = {0};
  const char *provider_text = "null";
  char tdengine_json[8192] = {0};
  const char *tdengine_text = "null";
  char result_store_json[8192] = {0};
  const char *result_store_text = "null";
  char tdengine_path[QST_REPORT_PATH_BYTES] = {0};
  char tdengine_dir[QST_REPORT_PATH_BYTES] = {0};
  char resource_profile_digest[65] = {0};
  char dependency_lock_digest[65] = {0};
  char tdengine_version_digest[65] = {0};
  uint64_t full_coverage_round_count = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == config || NULL == config->result_dir ||
      config->run_id == 0 || config->run_id != mapping->header->run_id) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qstReportCountTasks(mapping, &counts);
  if (code != QST_OK) {
    return code;
  }
  code = qstReportCountAiActions(mapping, &counts);
  if (code != QST_OK) {
    return code;
  }
  code = qstReportCountProcesses(mapping, &counts);
  if (code != QST_OK) {
    return code;
  }
  counts.execution_success = atomic_load_explicit(
      &mapping->header->execution_success_count, memory_order_acquire);
  counts.execution_error = atomic_load_explicit(
      &mapping->header->execution_error_count, memory_order_acquire);
  counts.execution_timeout = atomic_load_explicit(
      &mapping->header->execution_timeout_count, memory_order_acquire);
  counts.execution_cancelled = atomic_load_explicit(
      &mapping->header->execution_cancelled_count, memory_order_acquire);
  counts.verdict_passed = atomic_load_explicit(
      &mapping->header->verdict_passed_count, memory_order_acquire);
  counts.verdict_failed = atomic_load_explicit(
      &mapping->header->verdict_failed_count, memory_order_acquire);
  counts.verdict_unverified = atomic_load_explicit(
      &mapping->header->verdict_unverified_count, memory_order_acquire);
  code = qstReportBuildPairInfo(config, pair_info, sizeof(pair_info));
  if (code != QST_OK) {
    return code;
  }
  code = qstReportFullCoverageRoundCount(mapping, &full_coverage_round_count);
  if (code == QST_E_STOPPING) {
    code = QST_OK;
  }
  if (code != QST_OK) {
    return code;
  }
  run_state = atomic_load_explicit(&mapping->header->run_state,
                                   memory_order_acquire);
  stop_reason = atomic_load_explicit(&mapping->header->stop_reason,
                                     memory_order_acquire);
  if (NULL == qstReportRunStatus(run_state) ||
      strcmp(qstReportRunStatus(run_state), "invalid") == 0 ||
      NULL == qstReportStopReason(stop_reason)) {
    return QST_E_SHM_CORRUPT;
  }
  if (qst_provider_readiness_json(config, provider_json, sizeof(provider_json)) ==
      QST_OK) {
    provider_text = provider_json;
  }
  if (snprintf(tdengine_dir, sizeof(tdengine_dir), "%s/dataset/tdengine-readiness.json",
               config->result_dir) >= 0 &&
      qstReportReadSnapshot(tdengine_dir, tdengine_json,
                            sizeof(tdengine_json)) == QST_OK) {
    tdengine_text = tdengine_json;
  }
  if (snprintf(tdengine_dir, sizeof(tdengine_dir),
               "%s/dataset/result-store-readiness.json", config->result_dir) >= 0 &&
      qstReportReadSnapshot(tdengine_dir, result_store_json,
                            sizeof(result_store_json)) == QST_OK) {
    result_store_text = result_store_json;
  }
  qstReportDigestHex(mapping->header->resource_profile_digest,
                     resource_profile_digest);
  qstReportDigestHex(mapping->header->dependency_lock_digest,
                     dependency_lock_digest);
  qstReportDigestHex(mapping->header->tdengine_version_digest,
                     tdengine_version_digest);
  formatted = snprintf(json_path, sizeof(json_path), "%s/report.json",
                       config->result_dir);
  if (formatted < 0 || (uint64_t)formatted >= sizeof(json_path)) {
    return QST_E_RESOURCE;
  }
  formatted = snprintf(markdown_path, sizeof(markdown_path), "%s/report.md",
                       config->result_dir);
  if (formatted < 0 || (uint64_t)formatted >= sizeof(markdown_path)) {
    return QST_E_RESOURCE;
  }
  formatted = snprintf(
      json, sizeof(json),
      "{\"run_id\":%llu,\"status\":\"%s\",\"run_state\":%u,"
      "\"run_control\":{\"mode\":%u,\"focus\":%u,\"random_seed\":%llu,"
      "\"has_random_seed\":%s,\"statement_limit\":%llu,"
      "\"duration_limit_minutes\":%llu,\"full_coverage_rounds\":%llu,"
      "\"stop_reason\":\"%s\"},"
      "\"shared_memory_bytes\":%llu,\"execution_pair_count\":%u,"
      "\"judge_worker_count\":%u,\"pair_runtime\":{%s},"
      "\"tasks\":{\"generated\":%llu,"
      "\"executing\":%llu,\"final\":%llu,\"final_published\":%llu,"
      "\"persisting\":%llu,\"persisted\":%llu,\"isolated\":%llu},"
      "\"processes\":{\"registered\":%llu,\"running\":%llu,"
      "\"reaped\":%llu,\"failed\":%llu},"
      "\"phases\":{\"run\":\"%s\",\"persistence\":\"%s\"},"
      "\"statistics\":{\"tasks\":{\"generated\":%llu,\"persisted\":%llu,"
      "\"isolated\":%llu},\"execution\":{\"success\":%llu,"
      "\"error\":%llu,\"timeout\":%llu,\"cancelled\":%llu},"
      "\"verdict\":{\"passed\":%llu,\"failed\":%llu,"
      "\"unverified\":%llu},\"result_storage\":{\"memory\":%llu,"
      "\"unavailable\":%llu}},"
      "\"errors\":{\"isolated_tasks\":%llu,\"failed_processes\":%llu},"
      "\"artifacts\":{\"logs\":\"logs\",\"pending_results\":\"pending-results\","
      "\"report_markdown\":\"report.md\",\"provider_readiness\":\"dataset/provider-readiness.json\","
      "\"tdengine_readiness\":\"dataset/tdengine-readiness.json\","
      "\"result_store_readiness\":\"dataset/result-store-readiness.json\","
      "\"tdengine_startup\":\"dataset/tdengine-startup.json\"},"
      "\"startup_digests\":{\"resource_profile_sha256\":\"%s\","
      "\"dependency_lock_sha256\":\"%s\",\"tdengine_version_sha256\":\"%s\"},"
      "\"provider_readiness\":%s,"
      "\"tdengine_readiness\":%s,"
      "\"result_store_readiness\":%s,"
      "\"result_storage\":{\"memory\":%llu,\"unavailable\":%llu},"
      "\"ai_actions\":{\"prechecked\":%llu,\"running\":%llu,"
      "\"succeeded\":%llu,\"failed\":%llu,\"abandoned\":%llu}}\n",
      (unsigned long long)config->run_id,
      qstReportRunStatus(run_state), run_state,
      config->mode, config->focus, (unsigned long long)config->random_seed,
      config->mode == QST_MODE_RANDOM ? "true" : "false",
      (unsigned long long)config->statement_limit,
      (unsigned long long)config->duration_limit_minutes,
      (unsigned long long)full_coverage_round_count,
      qstReportStopReason(stop_reason),
      (unsigned long long)mapping->header->total_bytes,
      mapping->header->execution_pair_count, mapping->header->judge_worker_count,
      pair_info,
      (unsigned long long)counts.generated, (unsigned long long)counts.executing,
      (unsigned long long)counts.final, (unsigned long long)counts.final_published,
      (unsigned long long)counts.persisting, (unsigned long long)counts.persisted,
      (unsigned long long)counts.isolated,
      (unsigned long long)counts.process_registered,
      (unsigned long long)counts.process_running,
      (unsigned long long)counts.process_reaped,
      (unsigned long long)counts.process_failed,
      qstReportRunStatus(run_state), counts.persisted == counts.generated ?
          "durable" : "draining",
      (unsigned long long)counts.generated, (unsigned long long)counts.persisted,
      (unsigned long long)counts.isolated,
      (unsigned long long)counts.execution_success,
      (unsigned long long)counts.execution_error,
      (unsigned long long)counts.execution_timeout,
      (unsigned long long)counts.execution_cancelled,
      (unsigned long long)counts.verdict_passed,
      (unsigned long long)counts.verdict_failed,
      (unsigned long long)counts.verdict_unverified,
      (unsigned long long)counts.result_memory,
      (unsigned long long)counts.result_unavailable,
      (unsigned long long)counts.isolated,
      (unsigned long long)counts.process_failed,
      resource_profile_digest, dependency_lock_digest, tdengine_version_digest,
      provider_text,
      tdengine_text,
      result_store_text,
      (unsigned long long)counts.result_memory,
      (unsigned long long)counts.result_unavailable,
      (unsigned long long)counts.ai_prechecked, (unsigned long long)counts.ai_running,
      (unsigned long long)counts.ai_succeeded, (unsigned long long)counts.ai_failed,
      (unsigned long long)counts.ai_abandoned);
  if (formatted < 0 || (uint64_t)formatted >= sizeof(json)) {
    return QST_E_RESOURCE;
  }
  formatted = snprintf(
      markdown, sizeof(markdown),
      "# taosQueryTest Run\n\nrun_id: %llu\n\nstatus: %s\n\nrun_state: %u\n\n"
      "mode: %u\n\nfocus: %u\n\nrandom_seed: %llu\n\n"
      "statement_limit: %llu\n\nduration_limit_minutes: %llu\n\n"
      "full_coverage_rounds: %llu\n\n"
      "stop_reason: %s\n\n"
      "shared_memory_bytes: %llu\n\nexecution_pairs: %u\n\njudge_workers: %u\n\n"
      "pair_runtime_json:\n\n```json\n{%s}\n```\n\n"
      "| task state | count |\n| --- | ---: |\n| generated | %llu |\n"
      "| executing | %llu |\n| final | %llu |\n| final_published | %llu |\n"
      "| persisting | %llu |\n| persisted | %llu |\n| isolated | %llu |\n"
      "\n| execution result | count |\n| --- | ---: |\n| success | %llu |\n"
      "| error | %llu |\n| timeout | %llu |\n| cancelled | %llu |\n"
      "\n| verdict | count |\n| --- | ---: |\n| passed | %llu |\n"
      "| failed | %llu |\n| unverified | %llu |\n\n"
      "| result_memory | %llu |\n| result_unavailable | %llu |\n\n"
      "startup_digests:\n\n"
      "resource_profile_sha256: %s\n\n"
      "dependency_lock_sha256: %s\n\n"
      "tdengine_version_sha256: %s\n\n"
      "provider_readiness_json:\n\n```json\n%s\n```\n\n"
      "tdengine_readiness_json:\n\n```json\n%s\n```\n\n"
      "result_store_readiness_json:\n\n```json\n%s\n```\n\n"
      "| AI action state | count |\n| --- | ---: |\n| prechecked | %llu |\n"
      "| running | %llu |\n| succeeded | %llu |\n| failed | %llu |\n"
      "| abandoned | %llu |\n",
      (unsigned long long)config->run_id,
      qstReportRunStatus(run_state), run_state,
      config->mode, config->focus, (unsigned long long)config->random_seed,
      (unsigned long long)config->statement_limit,
      (unsigned long long)config->duration_limit_minutes,
      (unsigned long long)full_coverage_round_count,
      qstReportStopReason(stop_reason),
      (unsigned long long)mapping->header->total_bytes,
      mapping->header->execution_pair_count, mapping->header->judge_worker_count,
      pair_info,
      (unsigned long long)counts.generated, (unsigned long long)counts.executing,
      (unsigned long long)counts.final, (unsigned long long)counts.final_published,
      (unsigned long long)counts.persisting, (unsigned long long)counts.persisted,
      (unsigned long long)counts.isolated,
      (unsigned long long)counts.execution_success,
      (unsigned long long)counts.execution_error,
      (unsigned long long)counts.execution_timeout,
      (unsigned long long)counts.execution_cancelled,
      (unsigned long long)counts.verdict_passed,
      (unsigned long long)counts.verdict_failed,
      (unsigned long long)counts.verdict_unverified,
      (unsigned long long)counts.result_memory,
      (unsigned long long)counts.result_unavailable,
      resource_profile_digest, dependency_lock_digest, tdengine_version_digest,
      provider_text,
      tdengine_text,
      result_store_text,
      (unsigned long long)counts.ai_prechecked, (unsigned long long)counts.ai_running,
      (unsigned long long)counts.ai_succeeded, (unsigned long long)counts.ai_failed,
      (unsigned long long)counts.ai_abandoned);
  if (formatted < 0 || (uint64_t)formatted >= sizeof(markdown)) {
    return QST_E_RESOURCE;
  }
  formatted = snprintf(tdengine_path, sizeof(tdengine_path),
                       "%s/dataset/tdengine-readiness.json", config->result_dir);
  if (formatted < 0 || (uint64_t)formatted >= sizeof(tdengine_path)) {
    return QST_E_RESOURCE;
  }
  formatted = snprintf(tdengine_dir, sizeof(tdengine_dir), "%s/dataset",
                       config->result_dir);
  if (formatted < 0 || (uint64_t)formatted >= sizeof(tdengine_dir)) {
    return QST_E_RESOURCE;
  }
  if (tdengine_text != NULL && strcmp(tdengine_text, "null") != 0 &&
      access(tdengine_dir, F_OK) == 0) {
    code = qstReportReplaceFile(tdengine_path, tdengine_text);
    if (code != QST_OK) {
      return code;
    }
  }
  code = qstReportReplaceFile(json_path, json);
  return code == QST_OK ? qstReportReplaceFile(markdown_path, markdown) : code;
}

EQstCode qst_log_event(const SQstProcessSlot *self, EQstCode code,
                       const char *event_name, SQstRef diagnostic_ref) {
  if (NULL == self || NULL == event_name || event_name[0] == '\0') {
    return QST_E_INVALID_ARGUMENT;
  }
  if (fprintf(stderr,
              "event=%s role=%u slot=%u generation=%u code=%d diagnostic="
              "%llu:%u:%u\n",
              event_name, atomic_load_explicit(&self->role, memory_order_acquire),
              self->slot_id,
              atomic_load_explicit(&self->generation, memory_order_acquire),
              code, (unsigned long long)diagnostic_ref.offset,
              diagnostic_ref.length, diagnostic_ref.crc32c) < 0) {
    return QST_E_RESOURCE;
  }
  return QST_OK;
}
