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

#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "qst_runner.h"

#include <errno.h>
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/eventfd.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <netinet/in.h>
#include <time.h>
#include <unistd.h>

#include "qst_process.h"
#include "qst_block.h"
#include "qst_queue.h"
#include "qst_resource.h"
#include "qst_sha256.h"
#include "toolscJson.h"
#include "qst_report.h"
#include "qst_result_store.h"
#include "qst_dataset.h"
#include "qst_dataset_fixture.h"
#include "qst_provider.h"
#include "qst_native.h"
#include "qst_sql_dimension_config.h"
#include "qst_supervisor.h"

enum {
  QST_MAIN_FLOW_POLL_MILLISECONDS = 1000,
  /* Pair tasks are assigned directly in the shared ledger instead of a
   * per-pair queue. One native connection owns the bounded async batch, and
   * this poll observes its callback and control events. */
  QST_PAIR_QUERY_POLL_MILLISECONDS = 1,
  QST_PAIR_ASSIGN_POLL_MILLISECONDS = 1,
  /* Multiple judges can observe one execute-queue wakeup after another
   * consumer has claimed the only message. Back off before rechecking. */
  QST_JUDGE_EMPTY_CLAIM_BACKOFF_MILLISECONDS = 1,
  /* Startup and sparse workloads need enough time for the generator to fill
   * the bounded native batch. Saturated runs do not wait because the direct
   * task claim loop reaches QST_PAIR_MAX_INFLIGHT immediately. */
  QST_PAIR_BATCH_FILL_MILLISECONDS = 20,
  QST_PAIR_RESULT_CAPTURE_WORKER_COUNT = 3,
  QST_PAIR_CORE_CALLBACK_GRACE_MILLISECONDS = 10000,
  QST_QUERY_CANCEL_GRACE_MILLISECONDS = 10000,
  QST_PAIR_PORT_MIN = 20000,
  QST_PAIR_PORT_MAX = 60000,
  QST_PAIR_PORT_BLOCK_BYTES = 16,
  QST_PAIR_STARTUP_ATTEMPTS = 8,
  QST_PAIR_STARTUP_SETTLE_MILLISECONDS = 20,
  QST_PAIR_STARTUP_MILLISECONDS = 30000,
  /* TDengine's public native API returns this code while a private dnode has
   * accepted a connection but has not joined its one-node cluster yet. */
  QST_NATIVE_CODE_NO_ENOUGH_DNODES = -2147482828,
  QST_LOG_ROTATION_SEGMENT_COUNT = 4,
  QST_PAIR_HEALTH_SCAN_MILLISECONDS = 100,
  /* Main clears its completed persistence-task flow state on this cadence.
   * This is far below the 30-second watchdog lease while avoiding a complete
   * shared-memory flow update for every durable record. */
  QST_MAIN_PERSIST_FLOW_CONFIRM_MILLISECONDS = 10,
  /* Power-of-two SPSC capacities keep the RANDOM pipeline index arithmetic cheap. */
  QST_RANDOM_PIPELINE_RING_CAPACITY = 256,
  /* Builders own independent input/output SPSC pairs; the publisher reorders. */
  QST_RANDOM_PIPELINE_BUILDER_COUNT = 4,
  QST_RANDOM_PIPELINE_MAX_BUILDER_COUNT = QST_GENERATOR_MAX_RANDOM_BUILDERS,
  /* Keep shared block preparation near ordered publication so the four
   * builders cannot consume the downstream block reserve a full ring ahead. */
  QST_RANDOM_RESOURCE_PREPARE_WINDOW = 128,
  /* Stage metrics are diagnostic data. Sampling queue depth more often than
   * this does not improve a one-second QPS measurement, but it does add
   * shared-memory traffic to every persisted task. */
  QST_STAGE_PROFILE_OBSERVATION_INTERVAL_NS = 10000000U,
  QST_STAGE_PROFILE_QPS_SAMPLE_WINDOW_NS = 1000000000U,
  QST_STAGE_PROFILE_QPS_WARMUP_WINDOW_NS = 10000000000U,
  QST_CPU_EFFICIENCY_SAMPLE_INTERVAL_NS = 1000000000U,
  /* Pair facts are persisted through the result-store overflow journal when
   * they exceed the runs.parallel_test_info BINARY(8192) column. Keep the
   * in-process construction buffer large enough for long random/crash runs. */
  QST_PARALLEL_INFO_BUFFER_BYTES = 1024 * 1024,
};

static volatile sig_atomic_t gQstStopSignal = 0;
static int32_t gQstSignalEventfd = -1;
static char gQstProcessLogDirectory[4096] = {0};
static char gQstOverflowContentPath[4096] = {0};
static uint64_t gQstLogRotationBytes = 0;

static EQstCode qstMakePath(char *out_path, uint64_t out_bytes,
                            const char *parent, const char *name);

/* Hashes one immutable launcher input without depending on external tools. */
static EQstCode qstDigestFile(const char *path, SQstDigest256 *out_digest) {
  uint8_t bytes[4096] = {0};
  SQstSha256 digest = {0};
  uint64_t total = 0;
  int32_t fd = -1;

  if (NULL == path || NULL == out_digest) return QST_E_INVALID_ARGUMENT;
  fd = open(path, O_RDONLY | O_CLOEXEC);
  if (fd < 0) return QST_E_RESOURCE;
  qst_sha256_init(&digest);
  for (;;) {
    const ssize_t read_bytes = read(fd, bytes, sizeof(bytes));
    if (read_bytes < 0 && errno == EINTR) continue;
    if (read_bytes < 0) {
      (void)close(fd);
      return QST_E_RESOURCE;
    }
    if (read_bytes == 0) break;
    total += (uint64_t)read_bytes;
    qst_sha256_update(&digest, bytes, (uint64_t)read_bytes);
  }
  if (close(fd) != 0 || total == 0) return QST_E_RESOURCE;
  qst_sha256_final(&digest, out_digest->bytes);
  return QST_OK;
}

static void qstDigestBytes(const uint8_t *bytes, uint64_t byte_count,
                           SQstDigest256 *out_digest) {
  SQstSha256 digest = {0};
  qst_sha256_init(&digest);
  qst_sha256_update(&digest, bytes, byte_count);
  qst_sha256_final(&digest, out_digest->bytes);
}

static int32_t qstStableVersion(const char *version) {
  int32_t has_digit = 0;

  if (NULL == version || version[0] == '\0') return 0;
  for (const char *cursor = version; *cursor != '\0'; ++cursor) {
    if (*cursor >= '0' && *cursor <= '9') {
      has_digit = 1;
    } else if (*cursor != '.') {
      return 0;
    }
  }
  return has_digit;
}

/* Rejects malformed, incomplete, or pre-release dependency lock inputs. */
static EQstCode qstValidateDependencyLock(const char *path) {
  struct stat status = {0};
  tools_cJSON *root = NULL;
  tools_cJSON *format = NULL;
  tools_cJSON *dependencies = NULL;
  char *text = NULL;
  int32_t fd = -1;
  uint64_t read_bytes = 0;
  EQstCode code = QST_E_RESOURCE;

  if (NULL == path || stat(path, &status) != 0 || !S_ISREG(status.st_mode) ||
      status.st_size <= 0 || (uint64_t)status.st_size >= SIZE_MAX) {
    return QST_E_RESOURCE;
  }
  text = (char *)calloc((size_t)status.st_size + 1U, 1);
  if (NULL == text || (fd = open(path, O_RDONLY | O_CLOEXEC)) < 0) goto _return;
  while (read_bytes < (uint64_t)status.st_size) {
    const ssize_t result = read(fd, text + read_bytes,
                                (size_t)((uint64_t)status.st_size - read_bytes));
    if (result < 0 && errno == EINTR) continue;
    if (result <= 0) goto _return;
    read_bytes += (uint64_t)result;
  }
  if (close(fd) != 0) {
    fd = -1;
    goto _return;
  }
  fd = -1;
  root = tools_cJSON_ParseWithOpts(text, NULL, 1);
  format = tools_cJSON_GetObjectItemCaseSensitive(root, "format_version");
  dependencies = tools_cJSON_GetObjectItemCaseSensitive(root, "dependencies");
  if (!tools_cJSON_IsObject(root) || !tools_cJSON_IsNumber(format) ||
      format->valueint != 1 || !tools_cJSON_IsArray(dependencies) ||
      tools_cJSON_GetArraySize(dependencies) <= 0) goto _return;
  for (int32_t index = 0; index < tools_cJSON_GetArraySize(dependencies); ++index) {
    tools_cJSON *dependency = tools_cJSON_GetArrayItem(dependencies, index);
    tools_cJSON *name = tools_cJSON_GetObjectItemCaseSensitive(dependency, "name");
    tools_cJSON *version = tools_cJSON_GetObjectItemCaseSensitive(dependency, "version");
    tools_cJSON *source = tools_cJSON_GetObjectItemCaseSensitive(dependency, "source");
    if (!tools_cJSON_IsObject(dependency) || !tools_cJSON_IsString(name) ||
        !tools_cJSON_IsString(version) || !tools_cJSON_IsString(source) ||
        NULL == name->valuestring || NULL == source->valuestring ||
        name->valuestring[0] == '\0' || source->valuestring[0] == '\0' ||
        !qstStableVersion(version->valuestring)) goto _return;
  }
  code = QST_OK;
_return:
  if (fd >= 0) (void)close(fd);
  tools_cJSON_Delete(root);
  free(text);
  return code;
}

static EQstCode qstSendStopRequest(SQstShmMapping *mapping,
                                   EQstRole parent_role,
                                   uint32_t parent_generation,
                                   EQstRole role, uint32_t generation,
                                   uint32_t stop_generation);
static EQstCode qstWaitForStopAck(SQstShmMapping *mapping,
                                  EQstRole parent_role,
                                  uint32_t parent_generation, EQstRole role,
                                  uint32_t generation,
                                  uint32_t stop_generation);
static EQstCode qstWaitForExit(int32_t pidfd, uint32_t timeout_milliseconds,
                               int32_t *out_exited);
static EQstCode qstTerminateProcessGroup(pid_t pid, int32_t pidfd);
static EQstCode qstWaitForRoleFlow(const SQstShmMapping *mapping,
                                   EQstRole role, uint32_t slot_id,
                                   uint32_t generation,
                                   uint64_t deadline_mono_ns);
static EQstCode qstForwardFinalTask(SQstShmMapping *mapping,
                                    uint32_t generation, uint64_t gid,
                                    uint64_t dispatch_epoch,
                                    uint64_t transition_seq,
                                    uint64_t deadline_mono_ns);
static EQstCode qstAcknowledgeRoleStop(SQstShmMapping *mapping,
                                       EQstRole role, uint32_t generation,
                                       uint32_t stop_generation);
struct SQstPairAsyncProfile;
static EQstCode qstPairCaptureQueryResult(SQstShmMapping *mapping,
                                          const SQstTaskDispatch *dispatch,
                                          uint32_t pair_slot,
                                          uint32_t pair_generation,
                                          SQstNativeClient *client,
                                          void *native_result,
                                          SQstExecutePayload *out_payload,
                                          SQstResultCaptureCache *cache,
                                          struct SQstPairAsyncProfile *profile);
static int64_t qstRealtimeNowNs(void);

typedef struct {
  _Atomic uint32_t completed;
  void *result;
  int32_t code;
  int32_t native_error_code;
  int32_t event_fd;
  SQstNativeClient *client;
  SQstResultAsyncCapture *capture;
  SQstQueryResult captured_result;
  int32_t captured;
  int32_t capture_attempted;
  EQstCode capture_code;
  uint64_t capture_started_ns;
  uint64_t capture_finished_ns;
} SQstPairQueryCompletion;

/* Pair-local diagnostics that never participate in task ownership or flow. */
typedef struct SQstPairAsyncProfile {
  int32_t enabled;
  uint64_t batch_count;
  uint64_t batch_entry_count;
  uint64_t claim_ns;
  uint64_t submit_ns;
  uint64_t callback_wait_ns;
  uint64_t callback_poll_count;
  uint64_t finalize_ns;
  uint64_t final_result_capture_ns;
  uint64_t result_native_capture_ns;
  uint64_t result_block_alloc_ns;
  uint64_t result_block_write_seal_ns;
  uint64_t result_storage_ns;
  uint64_t result_document_count;
  uint64_t result_document_bytes;
  uint64_t result_row_count;
  uint64_t result_zero_row_count;
  uint64_t raw_result_capture_count;
  uint64_t raw_result_capture_fallback_count;
  uint64_t final_timestamp_ns;
  uint64_t final_task_publish_ns;
  uint64_t final_business_ns;
  uint64_t forward_ns;
  uint64_t final_lookup_ns;
  uint64_t final_queue_publish_ns;
  uint64_t final_queue_wait_ns;
  uint64_t final_confirmation_ns;
  uint64_t final_queue_full_count;
  /* Absolute monotonic time of the latest successful FINAL forwarding batch. */
  uint64_t last_forward_completed_mono_ns;
} SQstPairAsyncProfile;

static void qstPairAsyncProfileAddElapsed(SQstPairAsyncProfile *profile,
                                          uint64_t started_ns,
                                          uint64_t finished_ns,
                                          uint64_t *total_ns);

/* Judge-local diagnostics that never participate in durable task state. */
typedef struct {
  int32_t enabled;
  uint64_t claim_count;
  uint64_t empty_claim_count;
  uint64_t verdict_ns;
  uint64_t persist_prepare_and_write_ns;
  uint64_t persist_batch_write_ns;
  uint64_t persist_publish_ns;
  uint64_t native_batch_count;
  uint64_t native_batch_claim_count;
  uint64_t native_batch_record_count;
  uint64_t native_batch_max_record_count;
} SQstJudgeProfile;

/* Main-process diagnostics for the per-message persistence drain path. */
typedef struct {
  int32_t enabled;
  uint64_t claim_count;
  uint64_t claim_ns;
  uint64_t mark_durable_ns;
  uint64_t queue_ack_ns;
  uint64_t payload_release_ns;
  uint64_t stable_key_release_ns;
  uint64_t pending_file_release_ns;
  uint64_t persisted_blocks_release_ns;
  uint64_t business_ns;
  uint64_t reclaim_ns;
} SQstMainPersistProfile;

typedef struct {
  char root_dir[4096];
  char config_dir[4096];
  char data_dir[128];
  char data_link[4096];
  char logs_dir[4096];
  char cores_dir[4096];
  int64_t begin_ts_ns;
  int64_t end_ts_ns;
  uint16_t port;
  uint32_t slot;
  uint32_t generation;
  pid_t taosd_pid;
  pid_t taosc_pid;
  int32_t taosd_pidfd;
  uint32_t taosd_slot_id;
  uint32_t taosd_generation;
  uint32_t taosd_crash_count;
  uint32_t taosd_exit_count;
  uint32_t taosc_crash_count;
  uint32_t taosc_exit_count;
  uint32_t async_window;
  uint32_t max_async_batch_size;
  /* Held by the pair supervisor for its complete server lifetime.  The lease
   * serializes port-block selection across independent tool invocations. */
  int32_t port_lease_fd;
  pid_t last_taosd_crash_pid;
  /* Each in-flight SQL owns one native connection.  The native client can
   * execute asynchronously, but it cannot safely accept overlapping requests
   * through the same connection. */
  SQstNativeClient clients[QST_PAIR_MAX_INFLIGHT];
  SQstResultCaptureCache result_capture_cache;
  SQstPairAsyncProfile async_profile;
} SQstPairRuntime;

typedef struct {
  SQstTaskDispatch dispatch;
  SQstPairQueryCompletion completion;
  SQstExecutePayload final_payload;
  uint64_t query_deadline;
  int32_t submitted;
} SQstPairAsyncBatchEntry;

/* Capture workers only consume result handles owned by their assigned entries.
 * Shared task state remains ordered and supervisor-owned after join. */
typedef struct {
  pthread_t thread;
  SQstPairAsyncBatchEntry *entries;
  uint32_t entry_count;
  _Atomic uint32_t *next_index;
} SQstPairResultCaptureWorker;

typedef struct {
  pthread_t thread;
  _Atomic uint32_t completed;
  SQstShmMapping *mapping;
  SQstNativeClient *client;
  SQstNativeClient local_client;
  SQstTaskDispatch dispatch;
  uint32_t pair_slot;
  uint32_t pair_generation;
  pid_t taosd_pid;
  uint16_t port;
  _Atomic uint64_t *sid;
  EQstCode code;
  int32_t active;
} SQstPairWorker;

/* Main-process-only observations of bounded queue handoffs.  This is
 * diagnostic state and never participates in task ownership or recovery. */
typedef struct {
  uint64_t first_observed_mono_ns;
  uint64_t last_observed_mono_ns;
  uint64_t pre_manager_mono_ns;
  uint64_t managers_ready_mono_ns;
  uint64_t durable_mono_ns;
  uint64_t exec_manager_reaped_mono_ns;
  uint64_t judge_manager_reaped_mono_ns;
  uint64_t cleanup_complete_mono_ns;
  uint64_t sample_count;
  uint64_t qps_window_started_mono_ns;
  uint64_t qps_window_persisted_count;
  uint64_t persisted_qps_sample_count;
  uint64_t max_depth[4];
  double minimum_persisted_qps;
  int32_t enabled;
} SQstStageProfile;

typedef struct {
  int32_t pid;
  uint32_t generation;
  uint32_t role;
  uint64_t last_jiffies;
  uint64_t start_time;
  int32_t initialized;
  int32_t terminal_sampled;
} SQstCpuProcessSample;

/* The CPU window counts every run-private taosd. The native client runs in the
 * pair supervisor process, so that process is attributed to the tool side.
 * An external result-store requires an explicit PID declaration and is
 * sampled separately so shared services are never counted. */
typedef struct {
  SQstCpuProcessSample *processes;
  SQstCpuProcessSample launcher;
  SQstCpuProcessSample result_store_taosd;
  uint32_t process_count;
  uint64_t managers_ready_mono_ns;
  uint64_t last_sample_mono_ns;
  uint64_t tool_cpu_jiffies;
  uint64_t tool_cpu_jiffies_by_role[QST_ROLE_AI_WORKER + 1U];
  uint64_t taosd_cpu_jiffies;
  uint64_t pair_taosd_cpu_jiffies;
  uint64_t result_store_taosd_cpu_jiffies;
  uint32_t sampled_tool_process_count;
  uint32_t sampled_taosd_process_count;
  uint32_t unreadable_process_count;
  int32_t result_store_taosd_configured;
  int32_t result_store_taosd_requested;
  int32_t window_started;
  int32_t window_closed;
  int32_t sampling_complete;
} SQstCpuEfficiencyProfile;

typedef enum {
  QST_STAGE_PROFILE_PRE_MANAGER = 1,
  QST_STAGE_PROFILE_MANAGERS_READY = 2,
  QST_STAGE_PROFILE_DURABLE = 3,
  QST_STAGE_PROFILE_EXEC_MANAGER_REAPED = 4,
  QST_STAGE_PROFILE_JUDGE_MANAGER_REAPED = 5,
  QST_STAGE_PROFILE_CLEANUP_COMPLETE = 6,
} EQstStageProfileMark;

static SQstStageProfile gQstMainStageProfile = {0};
static SQstMainPersistProfile gQstMainPersistProfile = {0};
static SQstCpuEfficiencyProfile gQstMainCpuEfficiency = {0};

static EQstCode qstMainBuildPairInfo(const SQstRunConfig *config,
                                     char *out_json, uint64_t out_bytes);
static EQstCode qstMainBuildParallelInfo(
    const SQstShmMapping *mapping, const SQstRunConfig *config,
    const char *fact_stage, const char *dataset_state,
    const char *provider_readiness, const char *tdengine_readiness,
    const char *result_store_readiness, int32_t final_statistics,
    char *out_json, uint64_t out_bytes);
static void qstMainStageProfileObserve(const SQstShmMapping *mapping);
static void qstMainStageProfileMark(EQstStageProfileMark mark);
static void qstMainCpuEfficiencyObserve(const SQstShmMapping *mapping,
                                        int32_t force_sample);
static void qstMainCpuEfficiencyClose(const SQstShmMapping *mapping);
static EQstCode qstMainWriteStageProfile(const SQstRunConfig *config,
                                         const SQstShmMapping *mapping);
static EQstCode qstPairRewriteRuntimeState(uint32_t pair_slot,
                                           uint32_t pair_generation,
                                           const char *state,
                                           int32_t supervisor_crashed,
                                           const char *reaped_by);
static EQstCode qstLauncherReapCrashedPairServers(SQstShmMapping *mapping);
static EQstCode qstPairReleaseRuntimeData(SQstPairRuntime *runtime);
static EQstCode qstPairPrepareRuntimeData(SQstPairRuntime *runtime);

static void qstStopSignalHandler(int32_t signal_number) {
  const uint64_t event_count = 1;

  gQstStopSignal = signal_number;
  if (gQstSignalEventfd >= 0) {
    (void)write(gQstSignalEventfd, &event_count, sizeof(event_count));
  }
}

static EQstCode qstNextStopGeneration(SQstShmMapping *mapping,
                                      uint32_t *out_generation) {
  uint32_t generation = 0;

  if (NULL == mapping || NULL == mapping->header || NULL == out_generation) {
    return QST_E_INVALID_ARGUMENT;
  }
  generation = atomic_fetch_add_explicit(&mapping->header->stop_generation, 1,
                                         memory_order_acq_rel) + 1;
  if (generation == 0) {
    return QST_E_RESOURCE;
  }
  *out_generation = generation;
  return QST_OK;
}

static EQstCode qstStopDirectChild(SQstShmMapping *mapping,
                                   EQstRole parent_role,
                                   uint32_t parent_generation, EQstRole role,
                                   uint32_t generation, pid_t pid,
                                   int32_t pidfd) {
  int32_t exited = 0;
  uint32_t stop_generation = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || parent_generation == 0 || generation == 0 || pid <= 0 ||
      pidfd < 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qstNextStopGeneration(mapping, &stop_generation);
  if (code == QST_OK) {
    code = qstSendStopRequest(mapping, parent_role, parent_generation, role,
                              generation, stop_generation);
  }
  /* The main role owns an ordered drain and therefore intentionally does not
   * publish STOP_ACKED until its managers and persistence are durable.  Wait
   * for its process exit directly instead of requiring the manager-style ACK
   * that would force a premature SIGKILL during a long drain. */
  if (code == QST_OK && role == QST_ROLE_MAIN) {
    code = qstWaitForExit(pidfd, QST_TERM_GRACE_MILLISECONDS, &exited);
    if (code == QST_OK && exited != 0) {
      return QST_OK;
    }
    return qstTerminateProcessGroup(pid, pidfd);
  }
  if (code == QST_OK) {
    code = qstWaitForStopAck(mapping, parent_role, parent_generation, role,
                             generation, stop_generation);
  }
  if (code == QST_OK) {
    code = qstWaitForExit(pidfd, QST_TERM_GRACE_MILLISECONDS, &exited);
    if (code == QST_OK && exited != 0) {
      return QST_OK;
    }
  }
  return qstTerminateProcessGroup(pid, pidfd);
}

static EQstCode qstInstallStopHandlers(int32_t eventfd) {
  struct sigaction action = {0};

  if (eventfd < 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  action.sa_handler = qstStopSignalHandler;
  if (sigemptyset(&action.sa_mask) != 0 ||
      sigaction(SIGINT, &action, NULL) != 0 ||
      sigaction(SIGTERM, &action, NULL) != 0) {
    return QST_E_RESOURCE;
  }
  /* Do not clear a signal received during launcher setup.  The launcher
   * installs this handler before publishing latest_run_id, so a cooperative
   * stop cannot be lost in that observation window. */
  gQstSignalEventfd = eventfd;
  return QST_OK;
}

static int32_t qstOpenPidfd(pid_t pid) {
#ifdef SYS_pidfd_open
  return (int32_t)syscall(SYS_pidfd_open, pid, 0);
#else
  (void)pid;
  errno = ENOSYS;
  return -1;
#endif
}

static uint64_t qstMonotonicNowNs(void);

static uint64_t qstMonotonicNowMilliseconds(void) {
  struct timespec now = {0};

  if (clock_gettime(CLOCK_MONOTONIC, &now) != 0 || now.tv_sec < 0 ||
      (uint64_t)now.tv_sec > UINT64_MAX / UINT64_C(1000)) {
    return 0;
  }
  return (uint64_t)now.tv_sec * UINT64_C(1000) +
         (uint64_t)now.tv_nsec / UINT64_C(1000000);
}

static uint64_t qstMillisecondsToDeadlineNs(uint64_t milliseconds) {
  uint64_t now_ns = 0;

  if (milliseconds == 0 || milliseconds > UINT64_MAX / UINT64_C(1000000)) {
    return 0;
  }
  now_ns = qstMonotonicNowNs();
  if (now_ns == 0) {
    return 0;
  }
  return now_ns > UINT64_MAX - milliseconds * UINT64_C(1000000) ? 0 :
      now_ns + milliseconds * UINT64_C(1000000);
}

/* Consumes the eventfd counter after queue state has been rechecked. */
static EQstCode qstPollQueueEvent(struct pollfd *event, int32_t timeout_ms) {
  int32_t poll_result = 0;
  uint64_t wake_count = 0;

  if (NULL == event || event->fd < 0 || timeout_ms < 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  poll_result = poll(event, 1, timeout_ms);
  if (poll_result < 0) {
    return errno == EINTR ? QST_OK : QST_E_RESOURCE;
  }
  if (poll_result > 0 && (event->revents & POLLIN) != 0 &&
      read(event->fd, &wake_count, sizeof(wake_count)) < 0 &&
      errno != EAGAIN && errno != EINTR) {
    return QST_E_RESOURCE;
  }
  event->revents = 0;
  return QST_OK;
}

/* Sleeps only inside a bounded confirmation wait or a test injection. */
static void qstSleepMilliseconds(uint32_t milliseconds) {
  struct timespec delay = {
      .tv_sec = (time_t)(milliseconds / 1000U),
      .tv_nsec = (long)(milliseconds % 1000U) * 1000000L,
  };

  while (nanosleep(&delay, &delay) != 0 && errno == EINTR) {
  }
}

/* Tests can widen the publish-to-confirm window without changing production
 * queue ordering. Invalid values intentionally leave that window unchanged. */
static void qstPairTestDelayFinalConfirmation(void) {
  const char *text = getenv("QST_TEST_PAIR_FINAL_CONFIRM_DELAY_MILLISECONDS");
  char *end = NULL;
  unsigned long milliseconds = 0;

  if (NULL == text || text[0] == '\0') {
    return;
  }
  errno = 0;
  milliseconds = strtoul(text, &end, 10);
  if (errno != 0 || end == text || *end != '\0' || milliseconds == 0 ||
      milliseconds > UINT32_MAX) {
    return;
  }
  qstSleepMilliseconds((uint32_t)milliseconds);
}

static uint64_t qstMonotonicNowNs(void) {
  struct timespec now = {0};

  if (clock_gettime(CLOCK_MONOTONIC, &now) != 0 || now.tv_sec < 0 ||
      (uint64_t)now.tv_sec > UINT64_MAX / UINT64_C(1000000000)) {
    return 0;
  }
  return (uint64_t)now.tv_sec * UINT64_C(1000000000) + (uint64_t)now.tv_nsec;
}

/* Generates the immutable RANDOM seed before any run artifact is written. */
static EQstCode qstGenerateRandomSeed(uint64_t *out_seed) {
  uint64_t seed = 0;
  uint64_t read_bytes = 0;
  int32_t fd = -1;

  if (NULL == out_seed) {
    return QST_E_INVALID_ARGUMENT;
  }
  fd = open("/dev/urandom", O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    return QST_E_RESOURCE;
  }
  while (read_bytes < sizeof(seed)) {
    const ssize_t bytes = read(fd, (uint8_t *)&seed + read_bytes,
                               sizeof(seed) - (size_t)read_bytes);

    if (bytes < 0 && errno == EINTR) {
      continue;
    }
    if (bytes <= 0) {
      (void)close(fd);
      return QST_E_RESOURCE;
    }
    read_bytes += (uint64_t)bytes;
  }
  if (close(fd) != 0) {
    return QST_E_RESOURCE;
  }
  *out_seed = (seed & INT64_MAX) == 0 ? UINT64_C(1) : (seed & INT64_MAX);
  return QST_OK;
}

static int32_t qstMainFlowObserved(const SQstShmMapping *mapping,
                                   uint64_t *inout_flow_seq,
                                   uint64_t *inout_business_seq,
                                   uint64_t *inout_observed_ms,
                                   uint64_t *out_deadline_mono_ns) {
  SQstProcessSlot *slots = NULL;
  const SQstRegion *region = NULL;
  uint64_t now_ms = qstMonotonicNowMilliseconds();

  if (NULL == mapping || NULL == mapping->header || NULL == mapping->base ||
      NULL == inout_flow_seq || NULL == inout_business_seq ||
      NULL == inout_observed_ms || NULL == out_deadline_mono_ns ||
      now_ms == 0) {
    return -1;
  }
  region = &mapping->header->process_region;
  if (region->element_bytes != sizeof(SQstProcessSlot) ||
      region->element_count == 0 || region->region_offset > mapping->bytes ||
      region->region_bytes > mapping->bytes - region->region_offset) {
    return -1;
  }
  slots = (SQstProcessSlot *)((uint8_t *)mapping->base + region->region_offset);
  for (uint32_t index = 0; index < region->element_count; ++index) {
    uint64_t flow_seq = 0;
    uint64_t business_seq = 0;
    if (atomic_load_explicit(&slots[index].role, memory_order_acquire) !=
        QST_ROLE_MAIN) {
      continue;
    }
    flow_seq = atomic_load_explicit(&slots[index].flow_seq, memory_order_acquire);
    business_seq = atomic_load_explicit(&slots[index].business_seq,
                                        memory_order_acquire);
    *out_deadline_mono_ns = atomic_load_explicit(
        &slots[index].deadline_mono_ns, memory_order_acquire);
    if ((flow_seq != 0 && flow_seq != *inout_flow_seq) ||
        (business_seq != 0 && business_seq != *inout_business_seq)) {
      *inout_flow_seq = flow_seq;
      *inout_business_seq = business_seq;
      *inout_observed_ms = now_ms;
    }
    return 1;
  }
  return 0;
}

static int32_t qstHasActiveTask(const SQstShmMapping *mapping) {
  const SQstRegion *region = NULL;
  SQstProcessSlot *slots = NULL;

  if (NULL == mapping || NULL == mapping->header || NULL == mapping->base) {
    return -1;
  }
  region = &mapping->header->process_region;
  if (region->element_bytes != sizeof(SQstProcessSlot) ||
      region->element_count == 0 || region->region_offset > mapping->bytes ||
      region->region_bytes > mapping->bytes - region->region_offset) {
    return -1;
  }
  slots = (SQstProcessSlot *)((uint8_t *)mapping->base + region->region_offset);
  for (uint32_t index = 0; index < region->element_count; ++index) {
    const uint64_t gid = atomic_load_explicit(&slots[index].current_gid,
                                              memory_order_acquire);
    const uint64_t deadline = atomic_load_explicit(&slots[index].deadline_mono_ns,
                                                   memory_order_acquire);

    if (gid != 0 && deadline != 0) {
      return 1;
    }
  }
  return 0;
}

static EQstCode qstWaitForExit(int32_t pidfd, uint32_t timeout_milliseconds,
                               int32_t *out_exited) {
  struct pollfd event = {0};

  if (pidfd < 0 || NULL == out_exited || timeout_milliseconds > INT_MAX) {
    return QST_E_INVALID_ARGUMENT;
  }
  event.fd = pidfd;
  event.events = POLLIN;
  for (;;) {
    const int32_t poll_result = poll(&event, 1, (int32_t)timeout_milliseconds);
    if (poll_result > 0) {
      *out_exited = 1;
      return QST_OK;
    }
    if (poll_result == 0) {
      *out_exited = 0;
      return QST_OK;
    }
    if (errno != EINTR) {
      return QST_E_RESOURCE;
    }
  }
}

static EQstCode qstTerminateProcessGroup(pid_t pid, int32_t pidfd) {
  int32_t exited = 0;
  EQstCode code = QST_OK;

  if (pid <= 0 || pidfd < 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (kill(-pid, SIGTERM) != 0 && errno != ESRCH) {
    return QST_E_RESOURCE;
  }
  code = qstWaitForExit(pidfd, QST_TERM_GRACE_MILLISECONDS, &exited);
  if (code != QST_OK || exited != 0) {
    return code;
  }
  if (kill(-pid, SIGKILL) != 0 && errno != ESRCH) {
    return QST_E_RESOURCE;
  }
  code = qstWaitForExit(pidfd, QST_KILL_GRACE_MILLISECONDS, &exited);
  if (code != QST_OK) {
    return code;
  }
  return exited != 0 ? QST_OK : QST_E_DEADLINE;
}

static EQstCode qstWaitForMain(SQstShmMapping *mapping, pid_t pid,
                               int32_t pidfd, uint32_t generation) {
  struct pollfd event = {0};
  int32_t stop_requested = 0;
  int32_t force_stop = 0;
  SQstQueueStallObserver persist_observer = {0};
  uint64_t last_flow_seq = 0;
  uint64_t last_business_seq = 0;
  uint64_t last_flow_observed_ms = 0;

  if (NULL == mapping || pid <= 0 || pidfd < 0 || generation == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  event.fd = pidfd;
  event.events = POLLIN;
  last_flow_observed_ms = qstMonotonicNowMilliseconds();
  if (last_flow_observed_ms == 0) {
    return QST_E_RESOURCE;
  }
  for (;;) {
    const int32_t poll_result = poll(&event, 1, QST_MAIN_FLOW_POLL_MILLISECONDS);
    if (poll_result > 0) {
      break;
    }
    if (poll_result < 0 && errno != EINTR) {
      return QST_E_RESOURCE;
    }
    if (gQstStopSignal != 0) {
      atomic_store_explicit(&mapping->header->stop_reason,
                            QST_STOP_USER_REQUEST, memory_order_release);
      stop_requested = 1;
      force_stop = 1;
      break;
    }
    {
      uint64_t main_deadline_mono_ns = 0;
      const int32_t flow_state = qstMainFlowObserved(
          mapping, &last_flow_seq, &last_business_seq, &last_flow_observed_ms,
          &main_deadline_mono_ns);
      const uint64_t now_ms = qstMonotonicNowMilliseconds();
      const uint64_t now_ns = qstMonotonicNowNs();
      const int32_t active_task = qstHasActiveTask(mapping);
      int32_t persist_stalled = 0;

      if (flow_state < 0 || now_ms == 0 || now_ns == 0 || active_task < 0 ||
          qst_supervisor_observe_queue(mapping, QST_QUEUE_PERSIST, 1,
                                       active_task, now_ms, &persist_observer,
                                       &persist_stalled) != QST_OK) {
        return QST_E_SHM_CORRUPT;
      }
      if (persist_stalled != 0) {
        force_stop = 1;
        break;
      }
      if (last_flow_observed_ms != 0 &&
          now_ms >= last_flow_observed_ms &&
          now_ms - last_flow_observed_ms >= QST_FLOW_LEASE_MILLISECONDS &&
          (main_deadline_mono_ns == 0 || now_ns >= main_deadline_mono_ns)) {
        force_stop = 1;
        break;
      }
    }
  }
  if (force_stop != 0) {
    return stop_requested != 0 ? QST_E_STOPPING : QST_E_DEADLINE;
  }
  return QST_OK;
}

static EQstCode qstMkdir(const char *path) {
  char copy[4096] = {0};
  uint64_t length = 0;

  if (NULL == path || path[0] == '\0' || strlen(path) >= sizeof(copy)) {
    return QST_E_INVALID_ARGUMENT;
  }
  memcpy(copy, path, strlen(path) + 1);
  length = strlen(copy);
  for (uint64_t index = 1; index < length; ++index) {
    if (copy[index] == '/') {
      copy[index] = '\0';
      if (mkdir(copy, 0750) != 0 && errno != EEXIST) {
        return QST_E_RESOURCE;
      }
      copy[index] = '/';
    }
  }
  if (mkdir(copy, 0750) != 0 && errno != EEXIST) {
    return QST_E_RESOURCE;
  }
  return QST_OK;
}

static const char *qstRoleName(EQstRole role) {
  switch (role) {
    case QST_ROLE_LAUNCHER:
      return "launcher";
    case QST_ROLE_MAIN:
      return "main";
    case QST_ROLE_GENERATOR:
      return "generator";
    case QST_ROLE_EXEC_MANAGER:
      return "exec_manager";
    case QST_ROLE_PAIR_SUPERVISOR:
      return "pair_supervisor";
    case QST_ROLE_TAOSD:
      return "taosd";
    case QST_ROLE_TAOSC:
      return "taosc";
    case QST_ROLE_JUDGE_MANAGER:
      return "judge_manager";
    case QST_ROLE_JUDGE_WORKER:
      return "judge_worker";
    case QST_ROLE_AI_WORKER:
      return "ai_worker";
    default:
      return "invalid";
  }
}

/*
 * Test-only crash injection. The hook is inert unless the caller explicitly
 * selects one role through QST_TEST_KILL_ROLE; production runs never set it.
 * A FIFO notification makes the SIGKILL point observable without polling.
 */
static void qstTestMaybeKillRoleFromEnv(const char *environment_name,
                                        EQstRole role, uint32_t generation,
                                        uint32_t slot) {
  const char *target = NULL;
  const char *fifo_path = NULL;
  const char *claim_path = NULL;

  if (NULL == environment_name || environment_name[0] == '\0') {
    return;
  }
  target = getenv(environment_name);
  if (NULL == target || strcmp(target, qstRoleName(role)) != 0) {
    return;
  }
  if (slot != UINT32_MAX) {
    const char *slot_text = getenv("QST_TEST_KILL_SLOT");
    if (NULL != slot_text && slot_text[0] != '\0') {
      char *end = NULL;
      unsigned long requested_slot = strtoul(slot_text, &end, 10);
      if (end == slot_text || *end != '\0' || requested_slot > UINT32_MAX ||
          (uint32_t)requested_slot != slot) {
        return;
      }
    }
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
          line, sizeof(line), "role=%s pid=%d generation=%u slot=%u\n",
          qstRoleName(role), (int32_t)getpid(), generation, slot);
      if (written > 0 && (uint64_t)written < sizeof(line)) {
        (void)write(fd, line, (size_t)written);
      }
      (void)close(fd);
    }
  }
  (void)kill(getpid(), SIGKILL);
  _exit(128 + SIGKILL);
}

static void qstTestMaybeKillRole(EQstRole role, uint32_t generation,
                                 uint32_t slot) {
  qstTestMaybeKillRoleFromEnv("QST_TEST_KILL_ROLE", role, generation, slot);
}

/* Exercises a child loss after its parent has published STOP_REQUESTED. */
static void qstTestMaybeKillRoleOnStop(EQstRole role, uint32_t generation,
                                       uint32_t slot) {
  qstTestMaybeKillRoleFromEnv("QST_TEST_KILL_ROLE_ON_STOP", role, generation,
                              slot);
}

/* Test-only startup delay used to verify the launcher startup-flow lease. */
static void qstTestMaybeDelayMainStartup(void) {
  const char *text = getenv("QST_TEST_MAIN_STARTUP_DELAY_MILLISECONDS");
  char *end = NULL;
  unsigned long milliseconds = 0;

  if (NULL == text || text[0] == '\0') {
    return;
  }
  errno = 0;
  milliseconds = strtoul(text, &end, 10);
  if (errno != 0 || end == text || *end != '\0' ||
      milliseconds > QST_FLOW_LEASE_MILLISECONDS / 2U) {
    return;
  }
  (void)poll(NULL, 0, (int32_t)milliseconds);
}

/* Test-only delay after the main process publishes its bounded shutdown lease. */
static void qstTestMaybeDelayMainShutdown(void) {
  const char *text = getenv("QST_TEST_MAIN_SHUTDOWN_DELAY_MILLISECONDS");
  char *end = NULL;
  unsigned long milliseconds = 0;

  if (NULL == text || text[0] == '\0') {
    return;
  }
  errno = 0;
  milliseconds = strtoul(text, &end, 10);
  if (errno != 0 || end == text || *end != '\0' ||
      milliseconds <= QST_FLOW_LEASE_MILLISECONDS ||
      milliseconds > QST_DRAIN_LEASE_MILLISECONDS / 2U) {
    return;
  }
  (void)poll(NULL, 0, (int32_t)milliseconds);
}

static EQstCode qstSetProcessLogDirectory(const SQstRunConfig *config) {
  if (NULL == config || NULL == config->result_dir) {
    gQstProcessLogDirectory[0] = '\0';
    gQstOverflowContentPath[0] = '\0';
    gQstLogRotationBytes = 0;
    return QST_OK;
  }
  if (qstMakePath(gQstProcessLogDirectory, sizeof(gQstProcessLogDirectory),
                  config->result_dir, "logs") != QST_OK) {
    return QST_E_RESOURCE;
  }
  if (NULL == config->tool_root) {
    gQstOverflowContentPath[0] = '\0';
  } else if (qstMakePath(gQstOverflowContentPath, sizeof(gQstOverflowContentPath),
                         config->tool_root, "overflow_content.ndjson") != QST_OK) {
    return QST_E_RESOURCE;
  }
  gQstLogRotationBytes = config->log_rotation_bytes;
  return qstMkdir(gQstProcessLogDirectory);
}

static EQstCode qstLogDirectoryBytes(uint64_t *out_bytes) {
  DIR *directory = NULL;
  struct dirent *entry = NULL;
  uint64_t total = 0;

  if (NULL == out_bytes || gQstProcessLogDirectory[0] == '\0') {
    return QST_E_INVALID_ARGUMENT;
  }
  directory = opendir(gQstProcessLogDirectory);
  if (NULL == directory) {
    return QST_E_RESOURCE;
  }
  while ((entry = readdir(directory)) != NULL) {
    char path[4096] = {0};
    struct stat status = {0};
    int32_t written = 0;

    if (entry->d_name[0] == '.') {
      continue;
    }
    written = snprintf(path, sizeof(path), "%s/%s", gQstProcessLogDirectory,
                       entry->d_name);
    if (written < 0 || (uint64_t)written >= sizeof(path) ||
        stat(path, &status) != 0 || !S_ISREG(status.st_mode) ||
        status.st_size < 0 || total > UINT64_MAX - (uint64_t)status.st_size) {
      (void)closedir(directory);
      return QST_E_RESOURCE;
    }
    total += (uint64_t)status.st_size;
  }
  if (closedir(directory) != 0) {
    return QST_E_RESOURCE;
  }
  *out_bytes = total;
  return QST_OK;
}

/* Opens one role/generation log without overwriting older rotation segments. */
static EQstCode qstOpenRotatingLog(const char *path, uint64_t write_bytes,
                                   int32_t *out_fd, int32_t *out_rotated) {
  struct stat status = {0};
  uint64_t directory_bytes = 0;
  uint64_t segment_bytes = 0;
  int32_t rotated = 0;
  int32_t fd = -1;
  int32_t stat_result = 0;

  if (NULL == path || NULL == out_fd || NULL == out_rotated ||
      write_bytes == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_fd = -1;
  *out_rotated = 0;
  if (gQstLogRotationBytes != 0) {
    if (write_bytes > gQstLogRotationBytes ||
        qstLogDirectoryBytes(&directory_bytes) != QST_OK ||
        directory_bytes > gQstLogRotationBytes - write_bytes) {
      return QST_E_RESOURCE;
    }
    segment_bytes = gQstLogRotationBytes / QST_LOG_ROTATION_SEGMENT_COUNT;
    if (segment_bytes == 0 || write_bytes > segment_bytes) {
      return QST_E_RESOURCE;
    }
    stat_result = stat(path, &status);
    if (stat_result == 0 && S_ISREG(status.st_mode) &&
        status.st_size >= 0 &&
        (uint64_t)status.st_size > segment_bytes - write_bytes) {
      for (uint32_t index = 1; index <= UINT32_MAX; ++index) {
        char rotated_path[4096] = {0};
        const int32_t written = snprintf(rotated_path, sizeof(rotated_path),
                                         "%s.%u", path, index);

        if (written < 0 || (uint64_t)written >= sizeof(rotated_path)) {
          return QST_E_RESOURCE;
        }
        if (access(rotated_path, F_OK) != 0) {
          if (errno != ENOENT || rename(path, rotated_path) != 0) {
            return QST_E_RESOURCE;
          }
          rotated = 1;
          break;
        }
      }
      if (rotated == 0) {
        return QST_E_RESOURCE;
      }
    } else if (stat_result != 0 && errno != ENOENT) {
      return QST_E_RESOURCE;
    }
  }
  fd = open(path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0640);
  if (fd < 0) {
    return QST_E_RESOURCE;
  }
  *out_fd = fd;
  *out_rotated = rotated;
  return QST_OK;
}

static EQstCode qstRedirectProcessStderr(EQstRole role, uint32_t generation) {
  char path[4096] = {0};
  int32_t fd = -1;
  int32_t written = 0;
  int32_t rotated = 0;

  if (gQstProcessLogDirectory[0] == '\0' || generation == 0) {
    return QST_OK;
  }
  written = snprintf(path, sizeof(path), "%s/%s.%u.%d.log",
                     gQstProcessLogDirectory, qstRoleName(role), generation,
                     (int32_t)getpid());
  if (written < 0 || (uint64_t)written >= sizeof(path)) {
    return QST_E_RESOURCE;
  }
  if (qstOpenRotatingLog(path, 1, &fd, &rotated) != QST_OK ||
      dup2(fd, STDERR_FILENO) < 0) {
    if (fd >= 0) {
      (void)close(fd);
    }
    return QST_E_RESOURCE;
  }
  if (fd != STDERR_FILENO) {
    (void)close(fd);
  }
  (void)setvbuf(stderr, NULL, _IONBF, 0);
  return QST_OK;
}

static EQstCode qstWriteLifecycleLog(EQstRole self_role,
                                     uint32_t self_generation,
                                     const char *event_name,
                                     EQstRole child_role,
                                     uint32_t child_generation,
                                     int32_t child_pid, int32_t child_pgid,
                                     EQstCode reason, int32_t exit_status) {
  char path[4096] = {0};
  char line[1024] = {0};
  int32_t fd = -1;
  int32_t path_written = 0;
  int32_t line_written = 0;
  int32_t written = 0;
  int32_t rotated = 0;

  if (gQstProcessLogDirectory[0] == '\0') {
    return QST_OK;
  }
  if (NULL == event_name || self_generation == 0 || child_generation == 0 ||
      child_pid <= 0 || child_pgid <= 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  path_written = snprintf(path, sizeof(path), "%s/%s.%u.%d.log",
                          gQstProcessLogDirectory, qstRoleName(self_role),
                          self_generation, (int32_t)getpid());
  line_written = snprintf(
      line, sizeof(line),
      "event=%s parent_role=%s parent_pid=%d child_role=%s child_pid=%d "
      "child_ppid=%d child_pgid=%d child_generation=%u reason=%d "
      "exit_status=%d\n",
      event_name, qstRoleName(self_role), (int32_t)getpid(),
      qstRoleName(child_role), child_pid, (int32_t)getpid(), child_pgid,
      child_generation, reason, exit_status);
  if (path_written < 0 || (uint64_t)path_written >= sizeof(path) ||
      line_written < 0 || (uint64_t)line_written >= sizeof(line)) {
    return QST_E_RESOURCE;
  }
  if (qstOpenRotatingLog(path, (uint64_t)line_written, &fd, &rotated) !=
      QST_OK) {
    return QST_E_RESOURCE;
  }
  (void)rotated;
  if (rotated != 0 && self_role == QST_ROLE_MAIN &&
      dup2(fd, STDERR_FILENO) < 0) {
    (void)close(fd);
    return QST_E_RESOURCE;
  }
  written = line_written;
  if (write(fd, line, (size_t)written) != written) {
    (void)close(fd);
    return QST_E_RESOURCE;
  }
  if (close(fd) != 0) {
    return QST_E_RESOURCE;
  }
  return QST_OK;
}

/* Writes the first child-owned line without sharing a parent log descriptor. */
static EQstCode qstWriteProcessStartLog(EQstRole role, uint32_t generation,
                                        EQstRole parent_role,
                                        uint32_t parent_generation) {
  char path[4096] = {0};
  char line[1024] = {0};
  int32_t fd = -1;
  int32_t path_written = 0;
  int32_t line_written = 0;
  int32_t rotated = 0;

  if (gQstProcessLogDirectory[0] == '\0') {
    return QST_OK;
  }
  if (generation == 0 || parent_generation == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  path_written = snprintf(path, sizeof(path), "%s/%s.%u.%d.log",
                          gQstProcessLogDirectory, qstRoleName(role),
                          generation, (int32_t)getpid());
  line_written = snprintf(
      line, sizeof(line),
      "event=PROCESS_START role=%s pid=%d ppid=%d pgid=%d generation=%u "
      "parent_role=%s parent_generation=%u\n",
      qstRoleName(role), (int32_t)getpid(), (int32_t)getppid(),
      (int32_t)getpgrp(), generation, qstRoleName(parent_role),
      parent_generation);
  if (path_written < 0 || (uint64_t)path_written >= sizeof(path) ||
      line_written < 0 || (uint64_t)line_written >= sizeof(line)) {
    return QST_E_RESOURCE;
  }
  if (qstOpenRotatingLog(path, (uint64_t)line_written, &fd, &rotated) !=
      QST_OK) {
    return QST_E_RESOURCE;
  }
  if (write(fd, line, (size_t)line_written) != line_written) {
    (void)close(fd);
    return QST_E_RESOURCE;
  }
  return close(fd) == 0 ? QST_OK : QST_E_RESOURCE;
}

static EQstCode qstMakePath(char *out_path, uint64_t out_bytes,
                            const char *parent, const char *name) {
  const int32_t written = snprintf(out_path, (size_t)out_bytes, "%s/%s", parent,
                                   name);

  if (written < 0 || (uint64_t)written >= out_bytes) {
    return QST_E_RESOURCE;
  }
  return QST_OK;
}

static EQstCode qstWriteTextFile(const char *path, const char *text) {
  int32_t fd = -1;
  uint64_t length = 0;

  if (NULL == path || NULL == text) {
    return QST_E_INVALID_ARGUMENT;
  }
  fd = open(path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0640);
  if (fd < 0) {
    return QST_E_RESOURCE;
  }
  length = strlen(text);
  if (write(fd, text, (size_t)length) != (ssize_t)length || fsync(fd) != 0) {
    (void)close(fd);
    return QST_E_RESOURCE;
  }
  if (close(fd) != 0) {
    return QST_E_RESOURCE;
  }
  return QST_OK;
}

static EQstCode qstMainCpuEfficiencyGetSlots(
    const SQstShmMapping *mapping, SQstProcessSlot **out_slots,
    uint32_t *out_count) {
  const SQstRegion *region = NULL;

  if (NULL == mapping || NULL == mapping->base || NULL == mapping->header ||
      NULL == out_slots || NULL == out_count) {
    return QST_E_INVALID_ARGUMENT;
  }
  region = &mapping->header->process_region;
  if (region->element_bytes != sizeof(SQstProcessSlot) ||
      region->element_count == 0 || region->region_offset > mapping->bytes ||
      region->region_bytes !=
          (uint64_t)region->element_count * region->element_bytes ||
      region->region_bytes > mapping->bytes - region->region_offset) {
    return QST_E_SHM_CORRUPT;
  }
  *out_slots = (SQstProcessSlot *)((uint8_t *)mapping->base +
                                    region->region_offset);
  *out_count = region->element_count;
  return QST_OK;
}

/* Only the deployment may opt an externally started result-store into this
 * audit. Reject a ledger-owned PID to prevent double-counting a pair taosd. */
static EQstCode qstMainCpuEfficiencyConfigureResultStoreTaosd(
    const SQstShmMapping *mapping, const SQstProcessSlot *slots,
    uint32_t slot_count) {
  const char *text = getenv("QST_RESULT_STORE_TAOSD_PID");
  char *end = NULL;
  long parsed_pid = 0;

  if (NULL == mapping || NULL == mapping->header ||
      (slot_count != 0 && NULL == slots)) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (gQstMainCpuEfficiency.result_store_taosd_configured != 0) {
    return QST_OK;
  }
  gQstMainCpuEfficiency.result_store_taosd_configured = 1;
  if (NULL == text || text[0] == '\0') {
    return QST_OK;
  }
  errno = 0;
  parsed_pid = strtol(text, &end, 10);
  if (errno != 0 || end == text || *end != '\0' || parsed_pid <= 0 ||
      parsed_pid > INT32_MAX || parsed_pid == mapping->header->launcher_pid) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (uint32_t index = 0; index < slot_count; ++index) {
    if (slots[index].pid == parsed_pid) {
      return QST_E_INVALID_ARGUMENT;
    }
  }
  gQstMainCpuEfficiency.result_store_taosd.pid = (int32_t)parsed_pid;
  gQstMainCpuEfficiency.result_store_taosd_requested = 1;
  return QST_OK;
}

static void qstMainCpuEfficiencyObserveResultStoreTaosd(void) {
  SQstCpuProcessSample *sample = &gQstMainCpuEfficiency.result_store_taosd;
  char comm[64] = {0};
  uint64_t jiffies = 0;
  uint64_t start_time = 0;
  uint64_t delta = 0;

  if (gQstMainCpuEfficiency.result_store_taosd_requested == 0) {
    return;
  }
  if (sample->pid <= 0 ||
      qst_resource_read_proc_comm(sample->pid, comm, sizeof(comm)) != QST_OK ||
      strcmp(comm, "taosd") != 0 ||
      qst_resource_read_proc_cpu_identity(sample->pid, &jiffies,
                                           &start_time) != QST_OK) {
    gQstMainCpuEfficiency.sampling_complete = 0;
    ++gQstMainCpuEfficiency.unreadable_process_count;
    return;
  }
  if (sample->initialized == 0) {
    sample->start_time = start_time;
    sample->last_jiffies = jiffies;
    sample->initialized = 1;
    ++gQstMainCpuEfficiency.sampled_taosd_process_count;
    return;
  }
  if (sample->start_time != start_time || jiffies < sample->last_jiffies) {
    gQstMainCpuEfficiency.sampling_complete = 0;
    ++gQstMainCpuEfficiency.unreadable_process_count;
    sample->start_time = start_time;
    sample->last_jiffies = jiffies;
    return;
  }
  delta = jiffies - sample->last_jiffies;
  if (gQstMainCpuEfficiency.taosd_cpu_jiffies > UINT64_MAX - delta ||
      gQstMainCpuEfficiency.result_store_taosd_cpu_jiffies >
          UINT64_MAX - delta) {
    gQstMainCpuEfficiency.sampling_complete = 0;
    ++gQstMainCpuEfficiency.unreadable_process_count;
  } else {
    gQstMainCpuEfficiency.taosd_cpu_jiffies += delta;
    gQstMainCpuEfficiency.result_store_taosd_cpu_jiffies += delta;
  }
  sample->last_jiffies = jiffies;
}

static void qstMainCpuEfficiencyAddToolDelta(uint32_t role, uint64_t delta) {
  if (role < QST_ROLE_LAUNCHER || role > QST_ROLE_AI_WORKER ||
      role == QST_ROLE_TAOSD ||
      gQstMainCpuEfficiency.tool_cpu_jiffies > UINT64_MAX - delta ||
      gQstMainCpuEfficiency.tool_cpu_jiffies_by_role[role] >
          UINT64_MAX - delta) {
    gQstMainCpuEfficiency.sampling_complete = 0;
    return;
  }
  gQstMainCpuEfficiency.tool_cpu_jiffies += delta;
  gQstMainCpuEfficiency.tool_cpu_jiffies_by_role[role] += delta;
}

static int32_t qstMainCpuEfficiencyIsTaosdSideRole(uint32_t role) {
  return role == QST_ROLE_TAOSD;
}

/* Only the actual server process contributes to the server CPU bucket. */
static void qstMainCpuEfficiencyAddTaosdDelta(uint32_t role, uint64_t delta) {
  if (qstMainCpuEfficiencyIsTaosdSideRole(role) == 0 ||
      gQstMainCpuEfficiency.taosd_cpu_jiffies > UINT64_MAX - delta ||
      (role == QST_ROLE_TAOSD &&
       gQstMainCpuEfficiency.pair_taosd_cpu_jiffies > UINT64_MAX - delta)) {
    gQstMainCpuEfficiency.sampling_complete = 0;
    return;
  }
  gQstMainCpuEfficiency.taosd_cpu_jiffies += delta;
  if (role == QST_ROLE_TAOSD) {
    gQstMainCpuEfficiency.pair_taosd_cpu_jiffies += delta;
  }
}

static void qstMainCpuEfficiencyObserve(const SQstShmMapping *mapping,
                                        int32_t force_sample) {
  SQstProcessSlot *slots = NULL;
  uint32_t slot_count = 0;
  uint64_t now_mono_ns = qstMonotonicNowNs();
  int32_t baseline_sample = 0;

  if (NULL == mapping || NULL == mapping->header || now_mono_ns == 0 ||
      gQstMainCpuEfficiency.window_closed != 0 ||
      gQstMainCpuEfficiency.managers_ready_mono_ns == 0) {
    return;
  }
  if (gQstMainCpuEfficiency.window_started == 0) {
    if (now_mono_ns - gQstMainCpuEfficiency.managers_ready_mono_ns <
        QST_STAGE_PROFILE_QPS_WARMUP_WINDOW_NS) {
      return;
    }
    if (qstMainCpuEfficiencyGetSlots(mapping, &slots, &slot_count) != QST_OK ||
        slot_count > SIZE_MAX / sizeof(*gQstMainCpuEfficiency.processes)) {
      gQstMainCpuEfficiency.sampling_complete = 0;
      return;
    }
    free(gQstMainCpuEfficiency.processes);
    gQstMainCpuEfficiency.processes =
        (SQstCpuProcessSample *)calloc(slot_count,
                                       sizeof(*gQstMainCpuEfficiency.processes));
    if (NULL == gQstMainCpuEfficiency.processes) {
      gQstMainCpuEfficiency.sampling_complete = 0;
      return;
    }
    gQstMainCpuEfficiency.process_count = slot_count;
    gQstMainCpuEfficiency.window_started = 1;
    gQstMainCpuEfficiency.sampling_complete = 1;
    if (qstMainCpuEfficiencyConfigureResultStoreTaosd(mapping, slots,
                                                       slot_count) != QST_OK) {
      gQstMainCpuEfficiency.sampling_complete = 0;
      ++gQstMainCpuEfficiency.unreadable_process_count;
    }
  } else if (force_sample == 0 &&
             now_mono_ns - gQstMainCpuEfficiency.last_sample_mono_ns <
                 QST_CPU_EFFICIENCY_SAMPLE_INTERVAL_NS) {
    return;
  }
  if (slots == NULL &&
      qstMainCpuEfficiencyGetSlots(mapping, &slots, &slot_count) != QST_OK) {
    gQstMainCpuEfficiency.sampling_complete = 0;
    return;
  }
  if (slot_count != gQstMainCpuEfficiency.process_count) {
    gQstMainCpuEfficiency.sampling_complete = 0;
    return;
  }
  baseline_sample = gQstMainCpuEfficiency.last_sample_mono_ns == 0;
  {
    SQstCpuProcessSample *launcher = &gQstMainCpuEfficiency.launcher;
    uint64_t jiffies = 0;

    if (launcher->initialized != 0 &&
        (launcher->pid != mapping->header->launcher_pid ||
         launcher->generation != mapping->header->launcher_generation)) {
      launcher->initialized = 0;
      gQstMainCpuEfficiency.sampling_complete = 0;
      ++gQstMainCpuEfficiency.unreadable_process_count;
    }
    if (qst_resource_read_proc_cpu_jiffies(mapping->header->launcher_pid,
                                           &jiffies) != QST_OK) {
      gQstMainCpuEfficiency.sampling_complete = 0;
      ++gQstMainCpuEfficiency.unreadable_process_count;
    } else if (launcher->initialized == 0) {
      launcher->pid = mapping->header->launcher_pid;
      launcher->generation = mapping->header->launcher_generation;
      launcher->role = QST_ROLE_LAUNCHER;
      launcher->last_jiffies = jiffies;
      launcher->initialized = 1;
      ++gQstMainCpuEfficiency.sampled_tool_process_count;
    } else if (jiffies < launcher->last_jiffies) {
      gQstMainCpuEfficiency.sampling_complete = 0;
      ++gQstMainCpuEfficiency.unreadable_process_count;
      launcher->last_jiffies = jiffies;
    } else {
      qstMainCpuEfficiencyAddToolDelta(
          QST_ROLE_LAUNCHER, jiffies - launcher->last_jiffies);
      launcher->last_jiffies = jiffies;
    }
  }
  qstMainCpuEfficiencyObserveResultStoreTaosd();
  for (uint32_t index = 0; index < slot_count; ++index) {
    SQstCpuProcessSample *sample = &gQstMainCpuEfficiency.processes[index];
    const uint32_t role = atomic_load_explicit(&slots[index].role,
                                               memory_order_acquire);
    const uint32_t generation = atomic_load_explicit(&slots[index].generation,
                                                     memory_order_acquire);
    const uint32_t exit_state = atomic_load_explicit(&slots[index].exit_state,
                                                     memory_order_acquire);
    uint64_t jiffies = 0;

    if (sample->initialized != 0 &&
        (role == 0 || sample->pid != slots[index].pid ||
         sample->generation != generation || sample->role != role)) {
      /* A process exited between samples, so its final CPU increment is not
       * observable from /proc. This run must not claim a passing ratio. */
      sample->initialized = 0;
      gQstMainCpuEfficiency.sampling_complete = 0;
      ++gQstMainCpuEfficiency.unreadable_process_count;
    }
    if (role < QST_ROLE_LAUNCHER || role > QST_ROLE_AI_WORKER ||
        slots[index].pid <= 0 || exit_state == QST_PROCESS_REAPED) {
      if (exit_state == QST_PROCESS_REAPED && sample->initialized != 0 &&
          sample->terminal_sampled != 0) {
        continue;
      }
      if (sample->initialized != 0) {
        /* No final read was made after this process had exited. */
        sample->initialized = 0;
        gQstMainCpuEfficiency.sampling_complete = 0;
        ++gQstMainCpuEfficiency.unreadable_process_count;
      }
      continue;
    }
    if (qst_resource_read_proc_cpu_jiffies(slots[index].pid, &jiffies) !=
        QST_OK) {
      gQstMainCpuEfficiency.sampling_complete = 0;
      ++gQstMainCpuEfficiency.unreadable_process_count;
      continue;
    }
    if (sample->initialized == 0) {
      if (baseline_sample == 0) {
        /* The process started after the CPU window baseline. Its CPU before
         * this first observation is unavailable, so this run cannot claim a
         * complete tool-to-taosd ratio. */
        gQstMainCpuEfficiency.sampling_complete = 0;
        ++gQstMainCpuEfficiency.unreadable_process_count;
      }
      sample->pid = slots[index].pid;
      sample->generation = generation;
      sample->role = role;
      sample->last_jiffies = jiffies;
      sample->initialized = 1;
      sample->terminal_sampled = exit_state == QST_PROCESS_EXITED;
      if (qstMainCpuEfficiencyIsTaosdSideRole(role) != 0) {
        ++gQstMainCpuEfficiency.sampled_taosd_process_count;
      } else if (qst_resource_is_tool_role(role) != 0) {
        ++gQstMainCpuEfficiency.sampled_tool_process_count;
      }
      continue;
    }
    if (jiffies < sample->last_jiffies) {
      gQstMainCpuEfficiency.sampling_complete = 0;
      ++gQstMainCpuEfficiency.unreadable_process_count;
      sample->last_jiffies = jiffies;
      continue;
    }
    if (qstMainCpuEfficiencyIsTaosdSideRole(role) != 0) {
      const uint64_t delta = jiffies - sample->last_jiffies;

      qstMainCpuEfficiencyAddTaosdDelta(role, delta);
    } else if (qst_resource_is_tool_role(role) != 0) {
      const uint64_t delta = jiffies - sample->last_jiffies;

      qstMainCpuEfficiencyAddToolDelta(role, delta);
    }
    sample->last_jiffies = jiffies;
    sample->terminal_sampled = exit_state == QST_PROCESS_EXITED;
  }
  gQstMainCpuEfficiency.last_sample_mono_ns = now_mono_ns;
}

static void qstMainCpuEfficiencyClose(const SQstShmMapping *mapping) {
  qstMainCpuEfficiencyObserve(mapping, 1);
  gQstMainCpuEfficiency.window_closed = 1;
}

/* Samples handoff depths from the main process without changing queue state. */
static void qstMainStageProfileObserve(const SQstShmMapping *mapping) {
  uint64_t now_mono_ns = 0;
  uint64_t persisted = 0;
  int32_t persisted_loaded = 0;

  if (gQstMainStageProfile.enabled == 0 || NULL == mapping ||
      NULL == mapping->header) {
    qstMainCpuEfficiencyObserve(mapping, 0);
    return;
  }
  qstMainCpuEfficiencyObserve(mapping, 0);
  now_mono_ns = qstMonotonicNowNs();
  if (now_mono_ns == 0) {
    return;
  }
  if (gQstMainStageProfile.last_observed_mono_ns == 0 ||
      now_mono_ns - gQstMainStageProfile.last_observed_mono_ns >=
          QST_STAGE_PROFILE_OBSERVATION_INTERVAL_NS) {
    persisted = atomic_load_explicit(&mapping->header->persisted_task_count,
                                     memory_order_acquire);
    persisted_loaded = 1;
    if (gQstMainStageProfile.first_observed_mono_ns == 0) {
      gQstMainStageProfile.first_observed_mono_ns = now_mono_ns;
    }
    gQstMainStageProfile.last_observed_mono_ns = now_mono_ns;
    ++gQstMainStageProfile.sample_count;
    for (uint32_t index = 0; index < 4; ++index) {
      SQstQueueSnapshot snapshot = {0};

      if (qst_queue_snapshot(mapping, (EQstQueueId)(index + 1U), &snapshot) ==
              QST_OK &&
          snapshot.enqueue_pos >= snapshot.dequeue_pos) {
        const uint64_t depth = snapshot.enqueue_pos - snapshot.dequeue_pos;

        if (depth > gQstMainStageProfile.max_depth[index]) {
          gQstMainStageProfile.max_depth[index] = depth;
        }
      }
    }
  }
  /* Measure only the active generator window. The ordinary post-stop drain
   * intentionally has little new work and must not distort the sustained QPS
   * admission measurement. */
  if (gQstMainStageProfile.managers_ready_mono_ns == 0 ||
      atomic_load_explicit(&mapping->header->stop_reason,
                           memory_order_acquire) != QST_STOP_NONE) {
    return;
  }
  if (now_mono_ns - gQstMainStageProfile.managers_ready_mono_ns <
      QST_STAGE_PROFILE_QPS_WARMUP_WINDOW_NS) {
    return;
  }
  if (gQstMainStageProfile.qps_window_started_mono_ns == 0) {
    if (persisted_loaded == 0) {
      persisted = atomic_load_explicit(&mapping->header->persisted_task_count,
                                       memory_order_acquire);
    }
    gQstMainStageProfile.qps_window_started_mono_ns = now_mono_ns;
    gQstMainStageProfile.qps_window_persisted_count = persisted;
    return;
  }
  if (now_mono_ns - gQstMainStageProfile.qps_window_started_mono_ns <
      QST_STAGE_PROFILE_QPS_SAMPLE_WINDOW_NS) {
    return;
  }
  {
    if (persisted_loaded == 0) {
      persisted = atomic_load_explicit(&mapping->header->persisted_task_count,
                                       memory_order_acquire);
    }
    const uint64_t elapsed_ns =
        now_mono_ns - gQstMainStageProfile.qps_window_started_mono_ns;
    const uint64_t completed =
        persisted >= gQstMainStageProfile.qps_window_persisted_count ?
            persisted - gQstMainStageProfile.qps_window_persisted_count :
            0;
    const double qps = (double)completed * 1000000000.0 / (double)elapsed_ns;

    if (gQstMainStageProfile.persisted_qps_sample_count == 0 ||
        qps < gQstMainStageProfile.minimum_persisted_qps) {
      gQstMainStageProfile.minimum_persisted_qps = qps;
    }
    ++gQstMainStageProfile.persisted_qps_sample_count;
    gQstMainStageProfile.qps_window_started_mono_ns = now_mono_ns;
    gQstMainStageProfile.qps_window_persisted_count = persisted;
  }
}

/* Records one main-process phase boundary for opt-in performance diagnostics. */
static void qstMainStageProfileMark(EQstStageProfileMark mark) {
  const uint64_t now_mono_ns = qstMonotonicNowNs();

  if (now_mono_ns == 0) {
    return;
  }
  if (mark == QST_STAGE_PROFILE_MANAGERS_READY) {
    gQstMainCpuEfficiency.managers_ready_mono_ns = now_mono_ns;
  }
  if (gQstMainStageProfile.enabled == 0) {
    return;
  }
  switch (mark) {
    case QST_STAGE_PROFILE_PRE_MANAGER:
      gQstMainStageProfile.pre_manager_mono_ns = now_mono_ns;
      break;
    case QST_STAGE_PROFILE_MANAGERS_READY:
      gQstMainStageProfile.managers_ready_mono_ns = now_mono_ns;
      break;
    case QST_STAGE_PROFILE_DURABLE:
      gQstMainStageProfile.durable_mono_ns = now_mono_ns;
      break;
    case QST_STAGE_PROFILE_EXEC_MANAGER_REAPED:
      gQstMainStageProfile.exec_manager_reaped_mono_ns = now_mono_ns;
      break;
    case QST_STAGE_PROFILE_JUDGE_MANAGER_REAPED:
      gQstMainStageProfile.judge_manager_reaped_mono_ns = now_mono_ns;
      break;
    case QST_STAGE_PROFILE_CLEANUP_COMPLETE:
      gQstMainStageProfile.cleanup_complete_mono_ns = now_mono_ns;
      break;
    default:
      break;
  }
}

static uint64_t qstMainStageProfileElapsed(uint64_t started_ns,
                                           uint64_t finished_ns) {
  return started_ns == 0 || finished_ns < started_ns ? 0 :
                                                        finished_ns - started_ns;
}

/* Writes one terminal performance profile for reproducible workload analysis. */
static EQstCode qstMainWriteStageProfile(const SQstRunConfig *config,
                                         const SQstShmMapping *mapping) {
  static const char *const kQueueNames[] = {
      "control", "generate", "execute", "persist"};
  SQstQueueSnapshot snapshots[4] = {{0}};
  char path[4096] = {0};
  char json[4096] = {0};
  char cpu_ratio_text[64] = "null";
  uint64_t observed_ns = 0;
  uint64_t generated = 0;
  uint64_t persisted = 0;
  uint64_t preparation_ns = 0;
  uint64_t manager_startup_ns = 0;
  uint64_t pipeline_drain_ns = 0;
  uint64_t exec_manager_shutdown_ns = 0;
  uint64_t judge_manager_shutdown_ns = 0;
  uint64_t cleanup_ns = 0;
  double tool_to_taosd_cpu_ratio = 0.0;
  double steady_state_qps = 0.0;
  double generator_items_per_second = 0.0;
  double pair_items_per_second = 0.0;
  double judge_items_per_second = 0.0;
  double persistence_items_per_second = 0.0;
  EQstCpuEfficiencyState cpu_efficiency = QST_CPU_EFFICIENCY_INDETERMINATE;
  const char *cpu_efficiency_state = "indeterminate";
  int32_t written = 0;

  if (NULL == config || NULL == config->result_dir || NULL == mapping ||
      NULL == mapping->header || config->queue_capacity == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  qstMainStageProfileObserve(mapping);
  for (uint32_t index = 0; index < 4; ++index) {
    if (qst_queue_snapshot(mapping, (EQstQueueId)(index + 1U),
                           &snapshots[index]) != QST_OK) {
      return QST_E_SHM_CORRUPT;
    }
  }
  if (gQstMainStageProfile.last_observed_mono_ns >=
      gQstMainStageProfile.first_observed_mono_ns) {
    observed_ns = gQstMainStageProfile.last_observed_mono_ns -
                  gQstMainStageProfile.first_observed_mono_ns;
  }
  preparation_ns = qstMainStageProfileElapsed(
      gQstMainStageProfile.first_observed_mono_ns,
      gQstMainStageProfile.pre_manager_mono_ns);
  manager_startup_ns = qstMainStageProfileElapsed(
      gQstMainStageProfile.pre_manager_mono_ns,
      gQstMainStageProfile.managers_ready_mono_ns);
  pipeline_drain_ns = qstMainStageProfileElapsed(
      gQstMainStageProfile.managers_ready_mono_ns,
      gQstMainStageProfile.durable_mono_ns);
  exec_manager_shutdown_ns = qstMainStageProfileElapsed(
      gQstMainStageProfile.durable_mono_ns,
      gQstMainStageProfile.exec_manager_reaped_mono_ns);
  judge_manager_shutdown_ns = qstMainStageProfileElapsed(
      gQstMainStageProfile.exec_manager_reaped_mono_ns,
      gQstMainStageProfile.judge_manager_reaped_mono_ns);
  cleanup_ns = qstMainStageProfileElapsed(
      gQstMainStageProfile.judge_manager_reaped_mono_ns,
      gQstMainStageProfile.cleanup_complete_mono_ns);
  generated = atomic_load_explicit(&mapping->header->generated_task_count,
                                   memory_order_acquire);
  persisted = atomic_load_explicit(&mapping->header->persisted_task_count,
                                   memory_order_acquire);
  if (pipeline_drain_ns != 0) {
    steady_state_qps = (double)persisted /
                       ((double)pipeline_drain_ns / 1000000000.0);
  }
  if (observed_ns != 0) {
    const double seconds = (double)observed_ns / 1000000000.0;

    generator_items_per_second = (double)generated / seconds;
    pair_items_per_second =
        (double)snapshots[QST_QUEUE_EXECUTE - 1].enqueue_pos / seconds;
    judge_items_per_second =
        (double)snapshots[QST_QUEUE_EXECUTE - 1].dequeue_pos / seconds;
    persistence_items_per_second = (double)persisted / seconds;
  }
  if (gQstMainCpuEfficiency.window_started != 0 &&
      gQstMainCpuEfficiency.window_closed != 0) {
    cpu_efficiency = qst_resource_evaluate_cpu_efficiency(
        gQstMainCpuEfficiency.tool_cpu_jiffies,
        gQstMainCpuEfficiency.taosd_cpu_jiffies,
        gQstMainCpuEfficiency.sampling_complete, &tool_to_taosd_cpu_ratio);
    cpu_efficiency_state = cpu_efficiency == QST_CPU_EFFICIENCY_PASS ? "pass" :
                           cpu_efficiency == QST_CPU_EFFICIENCY_FAIL ? "fail" :
                                                                         "indeterminate";
  }
  if (cpu_efficiency != QST_CPU_EFFICIENCY_INDETERMINATE) {
    if (snprintf(cpu_ratio_text, sizeof(cpu_ratio_text), "%.6f",
                 tool_to_taosd_cpu_ratio) < 0) {
      return QST_E_RESOURCE;
    }
  }
  written = snprintf(
      json, sizeof(json),
      "{\"measurement_scope\":\"actual_pipeline\",\"sample_count\":%llu,\"observation_window_ns\":%llu,"
      "\"throughput\":{\"warmup_window_ns\":%llu,\"sample_window_ns\":%llu,\"persisted_qps_sample_count\":%llu,\"minimum_persisted_qps\":%.2f},"
      "\"phase_timings_ns\":{\"preparation\":%llu,\"manager_startup\":%llu,"
      "\"pipeline_drain\":%llu,\"steady_state_window_ns\":%llu,"
      "\"steady_state_qps\":%.2f,\"exec_manager_shutdown\":%llu,"
      "\"judge_manager_shutdown\":%llu,\"cleanup\":%llu},\"queue_capacity\":%u,\"stages\":{"
      "\"generator\":{\"generated\":%llu,\"items_per_second\":%.2f},"
      "\"pair\":{\"final_enqueued\":%llu,\"items_per_second\":%.2f},"
      "\"judge\":{\"execute_claimed\":%llu,\"persist_enqueued\":%llu,\"items_per_second\":%.2f},"
      "\"persistence\":{\"persist_claimed\":%llu,\"durable\":%llu,\"items_per_second\":%.2f}},"
      "\"cpu_efficiency\":{\"window_started\":%s,\"window_closed\":%s,"
      "\"tool_cpu_jiffies\":%llu,\"tool_cpu_jiffies_by_role\":{"
      "\"launcher\":%llu,\"main\":%llu,\"generator\":%llu,"
      "\"exec_manager\":%llu,\"pair_supervisor\":%llu,\"taosc\":%llu,"
      "\"judge_manager\":%llu,\"judge_worker\":%llu,\"ai_worker\":%llu},"
      "\"taosd_cpu_jiffies\":%llu,"
      "\"pair_taosd_cpu_jiffies\":%llu,"
      "\"standalone_taosc_tool_cpu_jiffies\":%llu,"
      "\"pair_supervisor_tool_cpu_jiffies\":%llu,"
      "\"result_store_taosd_cpu_jiffies\":%llu,"
      "\"result_store_taosd_pid\":%d,"
      "\"result_store_taosd_included\":%s,"
      "\"tool_to_taosd_cpu_ratio\":%s,\"cpu_efficiency_state\":\"%s\","
      "\"cpu_efficiency_pass\":%s,\"sampled_tool_process_count\":%u,"
      "\"sampled_taosd_process_count\":%u,\"unreadable_process_count\":%u},"
      "\"queues\":["
      "{\"name\":\"%s\",\"enqueued\":%llu,\"dequeued\":%llu,\"max_depth\":%llu,\"max_saturation_percent\":%llu},"
      "{\"name\":\"%s\",\"enqueued\":%llu,\"dequeued\":%llu,\"max_depth\":%llu,\"max_saturation_percent\":%llu},"
      "{\"name\":\"%s\",\"enqueued\":%llu,\"dequeued\":%llu,\"max_depth\":%llu,\"max_saturation_percent\":%llu},"
      "{\"name\":\"%s\",\"enqueued\":%llu,\"dequeued\":%llu,\"max_depth\":%llu,\"max_saturation_percent\":%llu}]}\n",
      (unsigned long long)gQstMainStageProfile.sample_count,
      (unsigned long long)observed_ns,
      (unsigned long long)QST_STAGE_PROFILE_QPS_WARMUP_WINDOW_NS,
      (unsigned long long)QST_STAGE_PROFILE_QPS_SAMPLE_WINDOW_NS,
      (unsigned long long)gQstMainStageProfile.persisted_qps_sample_count,
      gQstMainStageProfile.minimum_persisted_qps,
      (unsigned long long)preparation_ns,
      (unsigned long long)manager_startup_ns,
      (unsigned long long)pipeline_drain_ns,
      (unsigned long long)pipeline_drain_ns, steady_state_qps,
      (unsigned long long)exec_manager_shutdown_ns,
      (unsigned long long)judge_manager_shutdown_ns,
      (unsigned long long)cleanup_ns, config->queue_capacity,
      (unsigned long long)generated, generator_items_per_second,
      (unsigned long long)snapshots[QST_QUEUE_EXECUTE - 1].enqueue_pos,
      pair_items_per_second,
      (unsigned long long)snapshots[QST_QUEUE_EXECUTE - 1].dequeue_pos,
      (unsigned long long)snapshots[QST_QUEUE_PERSIST - 1].enqueue_pos,
      judge_items_per_second,
      (unsigned long long)snapshots[QST_QUEUE_PERSIST - 1].dequeue_pos,
      (unsigned long long)persisted, persistence_items_per_second,
      gQstMainCpuEfficiency.window_started != 0 ? "true" : "false",
      gQstMainCpuEfficiency.window_closed != 0 ? "true" : "false",
      (unsigned long long)gQstMainCpuEfficiency.tool_cpu_jiffies,
      (unsigned long long)gQstMainCpuEfficiency
          .tool_cpu_jiffies_by_role[QST_ROLE_LAUNCHER],
      (unsigned long long)gQstMainCpuEfficiency
          .tool_cpu_jiffies_by_role[QST_ROLE_MAIN],
      (unsigned long long)gQstMainCpuEfficiency
          .tool_cpu_jiffies_by_role[QST_ROLE_GENERATOR],
      (unsigned long long)gQstMainCpuEfficiency
          .tool_cpu_jiffies_by_role[QST_ROLE_EXEC_MANAGER],
      (unsigned long long)gQstMainCpuEfficiency
          .tool_cpu_jiffies_by_role[QST_ROLE_PAIR_SUPERVISOR],
      (unsigned long long)gQstMainCpuEfficiency
          .tool_cpu_jiffies_by_role[QST_ROLE_TAOSC],
      (unsigned long long)gQstMainCpuEfficiency
          .tool_cpu_jiffies_by_role[QST_ROLE_JUDGE_MANAGER],
      (unsigned long long)gQstMainCpuEfficiency
          .tool_cpu_jiffies_by_role[QST_ROLE_JUDGE_WORKER],
      (unsigned long long)gQstMainCpuEfficiency
          .tool_cpu_jiffies_by_role[QST_ROLE_AI_WORKER],
      (unsigned long long)gQstMainCpuEfficiency.taosd_cpu_jiffies,
      (unsigned long long)gQstMainCpuEfficiency.pair_taosd_cpu_jiffies,
      (unsigned long long)gQstMainCpuEfficiency
          .tool_cpu_jiffies_by_role[QST_ROLE_TAOSC],
      (unsigned long long)gQstMainCpuEfficiency
          .tool_cpu_jiffies_by_role[QST_ROLE_PAIR_SUPERVISOR],
      (unsigned long long)gQstMainCpuEfficiency.result_store_taosd_cpu_jiffies,
      gQstMainCpuEfficiency.result_store_taosd.pid,
      gQstMainCpuEfficiency.result_store_taosd_requested != 0 ? "true" :
                                                                   "false",
      cpu_ratio_text,
      cpu_efficiency_state,
      strcmp(cpu_efficiency_state, "pass") == 0 ? "true" : "false",
      gQstMainCpuEfficiency.sampled_tool_process_count,
      gQstMainCpuEfficiency.sampled_taosd_process_count,
      gQstMainCpuEfficiency.unreadable_process_count,
      kQueueNames[0],
      (unsigned long long)snapshots[0].enqueue_pos,
      (unsigned long long)snapshots[0].dequeue_pos,
      (unsigned long long)gQstMainStageProfile.max_depth[0],
      (unsigned long long)(gQstMainStageProfile.max_depth[0] * 100U /
                           config->queue_capacity),
      kQueueNames[1], (unsigned long long)snapshots[1].enqueue_pos,
      (unsigned long long)snapshots[1].dequeue_pos,
      (unsigned long long)gQstMainStageProfile.max_depth[1],
      (unsigned long long)(gQstMainStageProfile.max_depth[1] * 100U /
                           config->queue_capacity),
      kQueueNames[2], (unsigned long long)snapshots[2].enqueue_pos,
      (unsigned long long)snapshots[2].dequeue_pos,
      (unsigned long long)gQstMainStageProfile.max_depth[2],
      (unsigned long long)(gQstMainStageProfile.max_depth[2] * 100U /
                           config->queue_capacity),
      kQueueNames[3], (unsigned long long)snapshots[3].enqueue_pos,
      (unsigned long long)snapshots[3].dequeue_pos,
      (unsigned long long)gQstMainStageProfile.max_depth[3],
      (unsigned long long)(gQstMainStageProfile.max_depth[3] * 100U /
                           config->queue_capacity));
  if (written < 0 || (uint64_t)written >= sizeof(json) ||
      qstMakePath(path, sizeof(path), config->result_dir,
                  "stage-profile.json") != QST_OK) {
    return QST_E_RESOURCE;
  }
  return qstWriteTextFile(path, json);
}

/* Writes main-process persistence drain timings only when profiling is enabled. */
static void qstMainWritePersistProfile(void) {
  if (gQstMainPersistProfile.enabled == 0) {
    return;
  }
  (void)fprintf(
      stderr,
      "taosQueryTest: main_persist_profile claims=%llu claim_ns=%llu "
      "mark_durable_ns=%llu queue_ack_ns=%llu payload_release_ns=%llu "
      "stable_key_release_ns=%llu pending_file_release_ns=%llu "
      "persisted_blocks_release_ns=%llu business_ns=%llu reclaim_ns=%llu\n",
      (unsigned long long)gQstMainPersistProfile.claim_count,
      (unsigned long long)gQstMainPersistProfile.claim_ns,
      (unsigned long long)gQstMainPersistProfile.mark_durable_ns,
      (unsigned long long)gQstMainPersistProfile.queue_ack_ns,
      (unsigned long long)gQstMainPersistProfile.payload_release_ns,
      (unsigned long long)gQstMainPersistProfile.stable_key_release_ns,
      (unsigned long long)gQstMainPersistProfile.pending_file_release_ns,
      (unsigned long long)gQstMainPersistProfile.persisted_blocks_release_ns,
      (unsigned long long)gQstMainPersistProfile.business_ns,
      (unsigned long long)gQstMainPersistProfile.reclaim_ns);
}

/* Emits one test-only fact after a task is visible to the shared task ledger. */
static EQstCode qstTestPublishGeneratorActivity(void) {
  static int32_t published = 0;
  const char *path = getenv("QST_TEST_GENERATOR_ACTIVE");

  if (published != 0 || NULL == path || path[0] == '\0') {
    return QST_OK;
  }
  if (qstWriteTextFile(path, "generated\n") != QST_OK) {
    return QST_E_RESOURCE;
  }
  published = 1;
  return QST_OK;
}

/* Publishes a test-only barrier after the launcher signal handlers are active. */
static EQstCode qstTestPublishLauncherReady(void) {
  const char *path = getenv("QST_TEST_LAUNCHER_READY");

  if (NULL == path || path[0] == '\0') {
    return QST_OK;
  }
  return qstWriteTextFile(path, "ready\n");
}

static EQstCode qstWriteRunArtifacts(const SQstRunConfig *config,
                                     const SQstResourceProfile *profile,
                                     const char *result_dir,
                                     const char *logs_dir,
                                     const char *status,
                                     EQstCode completion_code) {
  char config_path[4096] = {0};
  char resource_profile_path[4096] = {0};
  char log_path[4096] = {0};
  char json_path[4096] = {0};
  char markdown_path[4096] = {0};
  char content[4096] = {0};
  int32_t written = 0;

  if (NULL == config || NULL == profile || NULL == status ||
      qstMakePath(config_path, sizeof(config_path), result_dir,
                  "run-config.txt") != QST_OK ||
      qstMakePath(resource_profile_path, sizeof(resource_profile_path), result_dir,
                  "resource-profile.json") != QST_OK ||
      qstMakePath(log_path, sizeof(log_path), logs_dir, "launcher.log") != QST_OK ||
      qstMakePath(json_path, sizeof(json_path), result_dir, "report.json") != QST_OK ||
      qstMakePath(markdown_path, sizeof(markdown_path), result_dir,
                  "report.md") != QST_OK) {
    return QST_E_RESOURCE;
  }
  written = snprintf(content, sizeof(content),
                     "run_id=%llu\nmode=%u\nfocus=%u\ntarget_bits=%u\n"
                     "statement_timeout_seconds=%u\nstatement_retry_count=%u\n"
                     "shm_bytes=%llu\nprocess_slot_count=%u\ntask_entry_count=%u\n"
                     "ai_action_entry_count=%u\nqueue_capacity=%u\nblock_bytes=%u\n"
                     "block_count=%u\ncaptured_at_ns=%llu\nmemory_limit_bytes=%llu\n"
                     "memory_available_bytes=%llu\nshm_free_bytes=%llu\n"
                     "result_disk_free_bytes=%llu\nlog_disk_free_bytes=%llu\n"
                     "cpu_quota_micros=%llu\ncpu_period_micros=%llu\npid_limit=%llu\n"
                     "fd_limit=%llu\nexecution_pair_count=%u\npair_async_window=%u\n"
                     "judge_worker_count=%u\n"
                     "minimum_layout_bytes=%llu\nworking_set_layout_bytes=%llu\n"
                     "pending_quota_bytes=%llu\n"
                     "log_rotation_bytes=%llu\nresource_source_flags=%u\n",
                     (unsigned long long)config->run_id, config->mode, config->focus,
                     config->target_selection_bits, config->statement_timeout_seconds,
                     config->statement_retry_count, (unsigned long long)config->shm_bytes,
                     config->process_slot_count, config->task_entry_count,
                     config->ai_action_entry_count, config->queue_capacity,
                     config->block_bytes, config->block_count,
                     (unsigned long long)profile->inputs.captured_at_ns,
                     (unsigned long long)profile->inputs.memory_limit_bytes,
                     (unsigned long long)profile->inputs.memory_available_bytes,
                     (unsigned long long)profile->inputs.shm_free_bytes,
                     (unsigned long long)profile->inputs.result_disk_free_bytes,
                     (unsigned long long)profile->inputs.log_disk_free_bytes,
                     (unsigned long long)profile->inputs.cpu_quota_micros,
                     (unsigned long long)profile->inputs.cpu_period_micros,
                     (unsigned long long)profile->inputs.pid_limit,
                     (unsigned long long)profile->inputs.fd_limit,
                     profile->execution_pair_count, profile->pair_async_window,
                     profile->judge_worker_count,
                     (unsigned long long)profile->minimum_layout_bytes,
                     (unsigned long long)profile->working_set_layout_bytes,
                     (unsigned long long)profile->pending_quota_bytes,
                     (unsigned long long)profile->log_rotation_bytes,
                     profile->inputs.source_flags);
  if (written < 0 || (uint64_t)written >= sizeof(content) ||
      qstWriteTextFile(config_path, content) != QST_OK) {
    return QST_E_RESOURCE;
  }
  written = snprintf(
      content, sizeof(content),
      "{\"captured_at_ns\":%llu,\"source_flags\":%u,"
      "\"memory_limit_bytes\":%llu,\"memory_available_bytes\":%llu,"
      "\"shm_free_bytes\":%llu,\"result_disk_free_bytes\":%llu,"
      "\"log_disk_free_bytes\":%llu,\"cpu_quota_micros\":%llu,"
      "\"cpu_period_micros\":%llu,\"pid_limit\":%llu,\"fd_limit\":%llu,"
      "\"memory_reserve_bytes\":%llu,\"memory_usable_bytes\":%llu,"
      "\"minimum_layout_bytes\":%llu,\"working_set_layout_bytes\":%llu,"
      "\"shared_memory_bytes\":%llu,\"execution_pair_count\":%u,"
      "\"pair_async_window\":%u,\"judge_worker_count\":%u,"
      "\"queue_capacity\":%u,"
      "\"block_bytes\":%u,\"block_count\":%u,"
      "\"pending_quota_bytes\":%llu,\"log_rotation_bytes\":%llu}",
      (unsigned long long)profile->inputs.captured_at_ns,
      profile->inputs.source_flags,
      (unsigned long long)profile->inputs.memory_limit_bytes,
      (unsigned long long)profile->inputs.memory_available_bytes,
      (unsigned long long)profile->inputs.shm_free_bytes,
      (unsigned long long)profile->inputs.result_disk_free_bytes,
      (unsigned long long)profile->inputs.log_disk_free_bytes,
      (unsigned long long)profile->inputs.cpu_quota_micros,
      (unsigned long long)profile->inputs.cpu_period_micros,
      (unsigned long long)profile->inputs.pid_limit,
      (unsigned long long)profile->inputs.fd_limit,
      (unsigned long long)profile->memory_reserve_bytes,
      (unsigned long long)profile->memory_usable_bytes,
      (unsigned long long)profile->minimum_layout_bytes,
      (unsigned long long)profile->working_set_layout_bytes,
      (unsigned long long)config->shm_bytes, profile->execution_pair_count,
      profile->pair_async_window, profile->judge_worker_count,
      config->queue_capacity, config->block_bytes,
      config->block_count, (unsigned long long)profile->pending_quota_bytes,
      (unsigned long long)profile->log_rotation_bytes);
  if (written < 0 || (uint64_t)written >= sizeof(content) ||
      qstWriteTextFile(resource_profile_path, content) != QST_OK) {
    return QST_E_RESOURCE;
  }
  written = snprintf(content, sizeof(content), "run_id=%llu status=%s code=%d\n",
                     (unsigned long long)config->run_id, status, completion_code);
  if (written < 0 || (uint64_t)written >= sizeof(content) ||
      qstWriteTextFile(log_path, content) != QST_OK) {
    return QST_E_RESOURCE;
  }
  /* The main-process report is derived from shared facts. Launcher creates a
   * minimal report only before a main process exists, and never overwrites it. */
  if (access(json_path, F_OK) == 0 || access(markdown_path, F_OK) == 0) {
    return QST_OK;
  }
  written = snprintf(content, sizeof(content),
                     "{\"run_id\":%llu,\"status\":\"%s\",\"code\":%d,"
                     "\"shared_memory_bytes\":%llu,\"queue_capacity\":%u,"
                     "\"process_slot_count\":%u,\"task_entry_count\":%u,"
                     "\"execution_pair_count\":%u,\"judge_worker_count\":%u,"
                     "\"resource_profile\":{\"captured_at_ns\":%llu,"
                     "\"memory_limit_bytes\":%llu,\"memory_available_bytes\":%llu,"
                     "\"shm_free_bytes\":%llu,\"result_disk_free_bytes\":%llu,"
                     "\"log_disk_free_bytes\":%llu,\"cpu_quota_micros\":%llu,"
                     "\"cpu_period_micros\":%llu,\"pid_limit\":%llu,"
                     "\"fd_limit\":%llu,\"minimum_layout_bytes\":%llu,"
                     "\"working_set_layout_bytes\":%llu,\"pending_quota_bytes\":%llu,"
                     "\"log_rotation_bytes\":%llu,"
                     "\"source_flags\":%u}}\n",
                     (unsigned long long)config->run_id, status, completion_code,
                     (unsigned long long)config->shm_bytes, config->queue_capacity,
                     config->process_slot_count, config->task_entry_count,
                     profile->execution_pair_count, profile->judge_worker_count,
                     (unsigned long long)profile->inputs.captured_at_ns,
                     (unsigned long long)profile->inputs.memory_limit_bytes,
                     (unsigned long long)profile->inputs.memory_available_bytes,
                     (unsigned long long)profile->inputs.shm_free_bytes,
                     (unsigned long long)profile->inputs.result_disk_free_bytes,
                     (unsigned long long)profile->inputs.log_disk_free_bytes,
                     (unsigned long long)profile->inputs.cpu_quota_micros,
                     (unsigned long long)profile->inputs.cpu_period_micros,
                     (unsigned long long)profile->inputs.pid_limit,
                     (unsigned long long)profile->inputs.fd_limit,
                     (unsigned long long)profile->minimum_layout_bytes,
                     (unsigned long long)profile->working_set_layout_bytes,
                     (unsigned long long)profile->pending_quota_bytes,
                     (unsigned long long)profile->log_rotation_bytes,
                     profile->inputs.source_flags);
  if (written < 0 || (uint64_t)written >= sizeof(content) ||
      qstWriteTextFile(json_path, content) != QST_OK) {
    return QST_E_RESOURCE;
  }
  written = snprintf(content, sizeof(content), "# taosQueryTest Run\n\nrun_id: %llu\n\n"
                     "status: %s\n\ncode: %d\n\nexecution_pairs: %u\n\n"
                     "judge_workers: %u\n\nshared_memory_bytes: %llu\n",
                     (unsigned long long)config->run_id, status, completion_code,
                     profile->execution_pair_count, profile->judge_worker_count,
                     (unsigned long long)config->shm_bytes);
  if (written < 0 || (uint64_t)written >= sizeof(content) ||
      qstWriteTextFile(markdown_path, content) != QST_OK) {
    return QST_E_RESOURCE;
  }
  return QST_OK;
}

static uint64_t qstNowNs(void) {
  struct timespec now = {0};

  if (clock_gettime(CLOCK_REALTIME, &now) != 0 || now.tv_sec < 0 ||
      (uint64_t)now.tv_sec > UINT64_MAX / UINT64_C(1000000000)) {
    return 0;
  }
  return (uint64_t)now.tv_sec * UINT64_C(1000000000) + (uint64_t)now.tv_nsec;
}

static EQstCode qstAllocateRunId(const char *result_root, uint64_t *out_run_id,
                                 int32_t *out_lock_fd) {
  char lock_path[4096] = {0};
  char id_path[4096] = {0};
  char text[64] = {0};
  uint64_t previous = 0;
  uint64_t candidate = qstNowNs();
  int32_t lock_fd = -1;
  int32_t id_fd = -1;
  ssize_t bytes = 0;

  if (qstMakePath(lock_path, sizeof(lock_path), result_root,
                  "latest_run_id.lock") != QST_OK ||
      qstMakePath(id_path, sizeof(id_path), result_root, "latest_run_id") != QST_OK ||
      candidate == 0 || NULL == out_run_id || NULL == out_lock_fd) {
    return QST_E_RESOURCE;
  }
  lock_fd = open(lock_path, O_RDWR | O_CREAT | O_CLOEXEC, 0600);
  if (lock_fd < 0 || flock(lock_fd, LOCK_EX) != 0) {
    if (lock_fd >= 0) {
      (void)close(lock_fd);
    }
    return QST_E_RESOURCE;
  }
  id_fd = open(id_path, O_RDWR | O_CREAT | O_CLOEXEC, 0600);
  if (id_fd < 0) {
    (void)flock(lock_fd, LOCK_UN);
    (void)close(lock_fd);
    return QST_E_RESOURCE;
  }
  bytes = read(id_fd, text, sizeof(text) - 1);
  if (bytes > 0) {
    char *end = NULL;
    errno = 0;
    previous = strtoull(text, &end, 10);
    if (errno != 0 || end == text) {
      (void)close(id_fd);
      (void)flock(lock_fd, LOCK_UN);
      (void)close(lock_fd);
      return QST_E_RESOURCE;
    }
  }
  if (candidate <= previous) {
    if (previous == UINT64_MAX) {
      (void)close(id_fd);
      (void)flock(lock_fd, LOCK_UN);
      (void)close(lock_fd);
      return QST_E_RESOURCE;
    }
    candidate = previous + 1;
  }
  (void)snprintf(text, sizeof(text), "%llu\n", (unsigned long long)candidate);
  if (ftruncate(id_fd, 0) != 0 || lseek(id_fd, 0, SEEK_SET) < 0 ||
      write(id_fd, text, strlen(text)) != (ssize_t)strlen(text) ||
      fsync(id_fd) != 0) {
    (void)close(id_fd);
    (void)flock(lock_fd, LOCK_UN);
    (void)close(lock_fd);
    return QST_E_RESOURCE;
  }
  (void)close(id_fd);
  *out_run_id = candidate;
  *out_lock_fd = lock_fd;
  return QST_OK;
}

/* Releases the launcher-owned tool-root lock on every post-acquire path. */
static void qstReleaseRunLock(int32_t *inout_lock_fd) {
  if (NULL != inout_lock_fd && *inout_lock_fd >= 0) {
    (void)flock(*inout_lock_fd, LOCK_UN);
    (void)close(*inout_lock_fd);
    *inout_lock_fd = -1;
  }
}

typedef EQstCode (*QstRoleEntry)(SQstShmMapping *mapping,
                                 uint32_t generation);

/* Parent-owned handle for a direct manager child that remains running. */
typedef struct {
  EQstRole role;
  uint32_t generation;
  pid_t pid;
  int32_t pidfd;
  int32_t stop_requested;
} SQstManagerChild;

/* Parent-owned handle for one execution-pair supervisor child. */
typedef struct {
  uint32_t slot;
  uint32_t generation;
  pid_t pid;
  int32_t pidfd;
  int32_t retire_requested;
} SQstPairChild;

static EQstCode qstRequestManagerStopNoWait(SQstShmMapping *mapping,
                                            uint32_t parent_generation,
                                            SQstManagerChild *child);

/* Claims one generated task only after locating an idle supervised pair. */
static EQstCode qstDispatchGeneratedTask(
    SQstShmMapping *mapping, uint32_t generation,
    const SQstPairChild *pairs, uint32_t pair_count,
    uint64_t deadline_mono_ns, uint64_t *out_gid) {
  SQstClaim claim = {0};
  uint32_t pair_index = 0;
  uint64_t transition_seq = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || (pair_count != 0 && NULL == pairs) ||
      generation == 0 || deadline_mono_ns == 0 || NULL == out_gid) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (; pair_index < pair_count; ++pair_index) {
    int32_t is_idle = 0;

    if (pairs[pair_index].generation == 0 ||
        pairs[pair_index].retire_requested != 0) {
      continue;
    }
    code = qst_task_pair_is_idle(mapping, generation, pairs[pair_index].slot,
                                 pairs[pair_index].generation, &is_idle);
    if (code != QST_OK) {
      return code;
    }
    if (is_idle != 0) {
      break;
    }
  }
  if (pair_index == pair_count) {
    return QST_E_QUEUE_FULL;
  }
  code = qst_queue_claim(mapping, QST_QUEUE_GENERATE, QST_ROLE_EXEC_MANAGER,
                         generation, deadline_mono_ns, &claim);
  if (code != QST_OK) {
    return code;
  }
  code = qst_task_claim_dispatch(mapping, claim.message.gid,
                                 QST_ROLE_EXEC_MANAGER, generation,
                                 claim.message.dispatch_epoch,
                                 &claim.message.body.generate, &transition_seq);
  if (code != QST_OK) {
    (void)qst_queue_abandon(mapping, &claim, code);
    return code;
  }
  code = qst_task_assign_pair(mapping, claim.message.gid,
                              QST_ROLE_EXEC_MANAGER, generation,
                              claim.message.dispatch_epoch, transition_seq,
                              pairs[pair_index].slot,
                              pairs[pair_index].generation);
  if (code != QST_OK) {
    (void)qst_task_abort_dispatch(
        mapping, claim.message.gid, claim.message.dispatch_epoch,
        transition_seq, QST_ROLE_EXEC_MANAGER, 0, generation);
    (void)qst_queue_abandon(mapping, &claim, code);
    return code;
  }
  if (claim.message.body.generate.mode == QST_MODE_RANDOM) {
    const uint64_t now_mono_ns = qstMonotonicNowNs();

    if (now_mono_ns == 0) {
      (void)qst_task_abort_dispatch(
          mapping, claim.message.gid, claim.message.dispatch_epoch,
          transition_seq, QST_ROLE_PAIR_SUPERVISOR,
          pairs[pair_index].slot, pairs[pair_index].generation);
      (void)qst_queue_abandon(mapping, &claim, QST_E_RESOURCE);
      return QST_E_RESOURCE;
    }
    code = qst_generator_random_activate(
        mapping, claim.message.body.generate.generation_seq, now_mono_ns);
    if (code != QST_OK) {
      (void)qst_task_abort_dispatch(
          mapping, claim.message.gid, claim.message.dispatch_epoch,
          transition_seq, QST_ROLE_PAIR_SUPERVISOR,
          pairs[pair_index].slot, pairs[pair_index].generation);
      (void)qst_queue_abandon(mapping, &claim, code);
      return code;
    }
  }
  code = qst_queue_ack(mapping, &claim, QST_ACK_TASK_DISPATCHED,
                       transition_seq);
  if (code == QST_OK) {
    *out_gid = claim.message.gid;
  }
  return code;
}

static EQstCode qstStartPairSupervisor(SQstShmMapping *mapping,
                                       uint32_t parent_generation,
                                       uint32_t pair_slot,
                                       SQstPairChild *out_child,
                                       int32_t wait_for_flow);
static EQstCode qstStopAndReapPairSupervisor(SQstShmMapping *mapping,
                                              uint32_t parent_generation,
                                              SQstPairChild *child);
static EQstCode qstRecoverExitedPairSupervisor(SQstShmMapping *mapping,
                                                uint32_t parent_generation,
                                                SQstPairChild *child);

/* Claims one generated queue item for one serial pair worker. */
static EQstCode qstPairClaimGeneratedTask(SQstShmMapping *mapping,
                                          uint32_t pair_slot,
                                          uint32_t pair_generation,
                                          uint64_t deadline_mono_ns,
                                          SQstTaskDispatch *out_dispatch) {
  SQstClaim claim = {0};
  EQstCode code = QST_OK;

  if (NULL == mapping || pair_generation == 0 || NULL == out_dispatch ||
      deadline_mono_ns == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qst_queue_claim_instance(
      mapping, QST_QUEUE_GENERATE, QST_ROLE_PAIR_SUPERVISOR, pair_slot,
      pair_generation, deadline_mono_ns, &claim);
  if (code != QST_OK) {
    if (code != QST_E_QUEUE_EMPTY && code != QST_E_STALE_OWNER) {
      (void)fprintf(stderr,
                    "taosQueryTest: pair generated queue claim failed "
                    "slot=%u generation=%u code=%d\n",
                    pair_slot, pair_generation, code);
    }
    return code;
  }
  {
    uint32_t stop_generation = 0;
    uint32_t exit_state = 0;

    code = qst_process_get_stop_generation_instance(
        mapping, QST_ROLE_PAIR_SUPERVISOR, pair_slot, pair_generation,
        &stop_generation, &exit_state);
    if (code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: pair claimed task stop-state lookup failed "
                    "slot=%u generation=%u gid=%llu code=%d\n",
                    pair_slot, pair_generation,
                    (unsigned long long)claim.message.gid, code);
      (void)qst_queue_abandon(mapping, &claim, code);
      return code;
    }
    if (exit_state == QST_PROCESS_STOP_REQUESTED && stop_generation != 0) {
      (void)qst_queue_abandon(mapping, &claim, QST_E_STOPPING);
      return QST_E_STOPPING;
    }
    if (exit_state != QST_PROCESS_RUNNING) {
      (void)qst_queue_abandon(mapping, &claim, QST_E_STALE_OWNER);
      return QST_E_STALE_OWNER;
    }
  }
  code = qst_task_claim_dispatch_pair(
      mapping, claim.message.gid, pair_slot, pair_generation,
      claim.message.dispatch_epoch, &claim.message.body.generate, out_dispatch);
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: pair generated task claim failed "
                  "slot=%u generation=%u gid=%llu dispatch_epoch=%llu "
                  "code=%d\n",
                  pair_slot, pair_generation,
                  (unsigned long long)claim.message.gid,
                  (unsigned long long)claim.message.dispatch_epoch, code);
    (void)qst_queue_abandon(mapping, &claim, code);
    if (code == QST_E_TASK_STATE) {
      uint32_t stop_generation = 0;
      uint32_t exit_state = 0;

      if (qst_process_get_stop_generation_instance(
              mapping, QST_ROLE_PAIR_SUPERVISOR, pair_slot, pair_generation,
              &stop_generation, &exit_state) == QST_OK &&
          exit_state == QST_PROCESS_STOP_REQUESTED && stop_generation != 0) {
        return QST_E_STOPPING;
      }
    }
    return code;
  }
  if (claim.message.body.generate.mode == QST_MODE_RANDOM) {
    const uint64_t now_mono_ns = qstMonotonicNowNs();

    if (now_mono_ns == 0) {
      (void)qst_task_abort_dispatch(
          mapping, claim.message.gid, claim.message.dispatch_epoch,
          out_dispatch->transition_seq, QST_ROLE_PAIR_SUPERVISOR, pair_slot,
          pair_generation);
      (void)qst_queue_abandon(mapping, &claim, QST_E_RESOURCE);
      return QST_E_RESOURCE;
    }
    code = qst_generator_random_activate(
        mapping, claim.message.body.generate.generation_seq, now_mono_ns);
    if (code != QST_OK) {
      (void)qst_task_abort_dispatch(
          mapping, claim.message.gid, claim.message.dispatch_epoch,
          out_dispatch->transition_seq, QST_ROLE_PAIR_SUPERVISOR, pair_slot,
          pair_generation);
      (void)qst_queue_abandon(mapping, &claim, code);
      return code;
    }
  }
  code = qst_queue_ack(mapping, &claim, QST_ACK_TASK_DISPATCHED,
                       out_dispatch->transition_seq);
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: pair generated queue ack failed "
                  "slot=%u generation=%u gid=%llu dispatch_epoch=%llu "
                  "transition_seq=%llu code=%d\n",
                  pair_slot, pair_generation,
                  (unsigned long long)claim.message.gid,
                  (unsigned long long)claim.message.dispatch_epoch,
                  (unsigned long long)out_dispatch->transition_seq, code);
    (void)qst_task_abort_dispatch(
        mapping, claim.message.gid, claim.message.dispatch_epoch,
        out_dispatch->transition_seq, QST_ROLE_PAIR_SUPERVISOR, pair_slot,
        pair_generation);
    (void)qst_queue_abandon(mapping, &claim, code);
    return code;
  }
  return QST_OK;
}

/* Publishes one serial pair's FINAL evidence to the judge worker queue. */
static EQstCode qstPairForwardFinalTask(SQstShmMapping *mapping,
                                        uint32_t pair_slot,
                                        uint32_t pair_generation,
                                        const SQstTaskDispatch *dispatch,
                                        uint64_t deadline_mono_ns,
                                        SQstPairAsyncProfile *profile) {
  SQstExecutePayload payload = {0};
  SQstMessage message = {0};
  uint32_t target = 0;
  uint64_t phase_started_ns = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == dispatch || dispatch->gid == 0 ||
      dispatch->dispatch_epoch == 0 || dispatch->transition_seq == 0 ||
      pair_generation == 0 || deadline_mono_ns == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (NULL != profile && profile->enabled != 0) {
    phase_started_ns = qstMonotonicNowNs();
  }
  code = qst_task_get_final_pair(
      mapping, dispatch->gid, dispatch->dispatch_epoch,
      dispatch->transition_seq, pair_slot, pair_generation, &payload, &target);
  if (NULL != profile && profile->enabled != 0) {
    qstPairAsyncProfileAddElapsed(profile, phase_started_ns, qstMonotonicNowNs(),
                                  &profile->final_lookup_ns);
  }
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: pair final lookup failed slot=%u generation=%u "
                  "gid=%llu dispatch_epoch=%llu transition_seq=%llu code=%d\n",
                  pair_slot, pair_generation, (unsigned long long)dispatch->gid,
                  (unsigned long long)dispatch->dispatch_epoch,
                  (unsigned long long)dispatch->transition_seq, code);
    return code;
  }
  message.message_type = QST_MSG_EXECUTE_FINAL;
  message.producer_role = QST_ROLE_PAIR_SUPERVISOR;
  message.producer_slot_id = pair_slot;
  message.producer_generation = pair_generation;
  message.target = target;
  message.run_id = mapping->header->run_id;
  message.gid = dispatch->gid;
  message.dispatch_epoch = dispatch->dispatch_epoch;
  message.transition_seq = dispatch->transition_seq;
  message.body.execute = payload;
  for (;;) {
    if (NULL != profile && profile->enabled != 0) {
      phase_started_ns = qstMonotonicNowNs();
    }
    code = qst_queue_publish(mapping, QST_QUEUE_EXECUTE, &message,
                             deadline_mono_ns);
    if (NULL != profile && profile->enabled != 0) {
      qstPairAsyncProfileAddElapsed(
          profile, phase_started_ns, qstMonotonicNowNs(),
          &profile->final_queue_publish_ns);
    }
    if (code != QST_E_QUEUE_FULL) {
      break;
    }
    if (NULL != profile && profile->enabled != 0) {
      ++profile->final_queue_full_count;
      phase_started_ns = qstMonotonicNowNs();
    }
    if (qstMonotonicNowNs() >= deadline_mono_ns ||
        qstPollQueueEvent(&(struct pollfd){
                              .fd = mapping->queue_eventfd[QST_QUEUE_EXECUTE - 1],
                              .events = POLLIN},
                          QST_PAIR_QUERY_POLL_MILLISECONDS) != QST_OK) {
      if (NULL != profile && profile->enabled != 0) {
        qstPairAsyncProfileAddElapsed(profile, phase_started_ns,
                                      qstMonotonicNowNs(),
                                      &profile->final_queue_wait_ns);
      }
      return QST_E_DEADLINE;
    }
    if (NULL != profile && profile->enabled != 0) {
      qstPairAsyncProfileAddElapsed(profile, phase_started_ns,
                                    qstMonotonicNowNs(),
                                    &profile->final_queue_wait_ns);
    }
  }
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: pair final queue publish failed slot=%u "
                  "generation=%u gid=%llu dispatch_epoch=%llu "
                  "transition_seq=%llu code=%d\n",
                  pair_slot, pair_generation, (unsigned long long)dispatch->gid,
                  (unsigned long long)dispatch->dispatch_epoch,
                  (unsigned long long)dispatch->transition_seq, code);
    return code;
  }
  qstPairTestDelayFinalConfirmation();
  if (NULL != profile && profile->enabled != 0) {
    phase_started_ns = qstMonotonicNowNs();
  }
  code = qst_task_confirm_final_published_pair(
      mapping, dispatch->gid, dispatch->dispatch_epoch,
      dispatch->transition_seq, pair_slot, pair_generation, &payload);
  if (NULL != profile && profile->enabled != 0) {
    qstPairAsyncProfileAddElapsed(profile, phase_started_ns, qstMonotonicNowNs(),
                                  &profile->final_confirmation_ns);
  }
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: pair final publication confirmation failed "
                  "slot=%u generation=%u gid=%llu dispatch_epoch=%llu "
                  "transition_seq=%llu code=%d\n",
                  pair_slot, pair_generation, (unsigned long long)dispatch->gid,
                  (unsigned long long)dispatch->dispatch_epoch,
                  (unsigned long long)dispatch->transition_seq, code);
  }
  return code;
}

/* An execute queue message is visible before its pair atomically changes the
 * task from FINAL to FINAL_PUBLISHED. The judge must tolerate that short,
 * intentional publication gap, but must not hold a claimed message forever if
 * the producing pair disappeared before confirming it. */
static EQstCode qstJudgeWaitForPersistSnapshot(
    const SQstShmMapping *mapping, const SQstClaim *claim,
    SQstTaskPersistSnapshot *out_snapshot) {
  uint64_t confirmation_deadline = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == claim || NULL == out_snapshot ||
      claim->lease_deadline_mono_ns == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  confirmation_deadline =
      qstMillisecondsToDeadlineNs(QST_FLOW_CONFIRM_MILLISECONDS);
  if (confirmation_deadline == 0) {
    return QST_E_RESOURCE;
  }
  if (confirmation_deadline > claim->lease_deadline_mono_ns) {
    confirmation_deadline = claim->lease_deadline_mono_ns;
  }
  for (;;) {
    const uint64_t now = qstMonotonicNowNs();

    if (now == 0) {
      return QST_E_RESOURCE;
    }
    code = qst_task_get_persist_snapshot(
        mapping, claim->message.gid, claim->message.dispatch_epoch,
        claim->message.transition_seq, out_snapshot);
    if (code != QST_E_STALE_OWNER || now >= confirmation_deadline) {
      return code;
    }
    qstSleepMilliseconds(1);
  }
}

/* Returns a manager-owned retry to the shared generated-work queue. */
static EQstCode qstRequeueManagerDispatch(SQstShmMapping *mapping,
                                          uint32_t generation,
                                          const SQstTaskDispatch *dispatch,
                                          uint64_t deadline_mono_ns) {
  SQstGeneratePayload payload = {0};
  SQstMessage message = {0};
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == dispatch || generation == 0 ||
      dispatch->gid == 0 || dispatch->dispatch_epoch == 0 ||
      dispatch->transition_seq == 0 || deadline_mono_ns == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qst_task_abort_dispatch(
      mapping, dispatch->gid, dispatch->dispatch_epoch, dispatch->transition_seq,
      QST_ROLE_EXEC_MANAGER, 0, generation);
  if (code != QST_OK) {
    return code;
  }
  code = qst_task_get_generate_payload(mapping, dispatch->gid,
                                       dispatch->dispatch_epoch,
                                       QST_ROLE_EXEC_MANAGER, generation,
                                       &payload);
  if (code != QST_OK) {
    return code;
  }
  message.message_type = QST_MSG_GENERATE_WORK;
  message.producer_role = QST_ROLE_EXEC_MANAGER;
  message.producer_generation = generation;
  message.target = payload.target;
  message.run_id = mapping->header->run_id;
  message.gid = dispatch->gid;
  message.dispatch_epoch = dispatch->dispatch_epoch;
  message.transition_seq = dispatch->transition_seq;
  message.body.generate = payload;
  return qst_queue_publish(mapping, QST_QUEUE_GENERATE, &message,
                           deadline_mono_ns);
}

/* Assigns one timeout retry after optionally replacing its previous pair. */
static EQstCode qstDispatchTimeoutRetry(
    SQstShmMapping *mapping, uint32_t generation, SQstPairChild *pairs,
    uint32_t pair_count, const SQstTaskDispatch *dispatch,
    int32_t recycle_required, uint32_t previous_pair_slot,
    uint32_t previous_pair_generation, uint64_t deadline_mono_ns) {
  uint32_t pair_index = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == pairs || pair_count == 0 || NULL == dispatch ||
      dispatch->gid == 0 || dispatch->dispatch_epoch == 0 ||
      dispatch->transition_seq == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (recycle_required != 0) {
    for (; pair_index < pair_count; ++pair_index) {
      if (pairs[pair_index].slot == previous_pair_slot &&
          pairs[pair_index].generation == previous_pair_generation) {
        break;
      }
    }
    if (pair_index == pair_count) {
      return QST_E_STALE_OWNER;
    }
    code = qstStopAndReapPairSupervisor(mapping, generation,
                                        &pairs[pair_index]);
    if (code != QST_OK) {
      return code;
    }
    code = qstStartPairSupervisor(mapping, generation, previous_pair_slot,
                                  &pairs[pair_index], 1);
    if (code != QST_OK) {
      return code;
    }
  }
  return qstRequeueManagerDispatch(mapping, generation, dispatch,
                                   deadline_mono_ns);
}

static EQstCode qstStartPairSupervisor(SQstShmMapping *mapping,
                                       uint32_t parent_generation,
                                       uint32_t pair_slot,
                                       SQstPairChild *out_child,
                                       int32_t wait_for_flow) {
  int32_t generation_pipe[2] = {-1, -1};
  int32_t pidfd = -1;
  int32_t status = 0;
  pid_t pid = -1;
  uint32_t generation = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || parent_generation == 0 || NULL == out_child) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (pipe(generation_pipe) != 0) {
    return QST_E_RESOURCE;
  }
  pid = fork();
  if (pid < 0) {
    (void)close(generation_pipe[0]);
    (void)close(generation_pipe[1]);
    return QST_E_RESOURCE;
  }
  if (pid == 0) {
    uint32_t child_generation = 0;
    ssize_t bytes = 0;
    const pid_t parent_pid = getppid();

    (void)close(generation_pipe[1]);
    if (prctl(PR_SET_PDEATHSIG, SIGKILL) != 0 || getppid() != parent_pid ||
        setpgid(0, 0) != 0) {
      (void)close(generation_pipe[0]);
      _exit(1);
    }
    bytes = read(generation_pipe[0], &child_generation, sizeof(child_generation));
    (void)close(generation_pipe[0]);
    if (bytes != (ssize_t)sizeof(child_generation) || child_generation == 0) {
      _exit(1);
    }
    code = qst_pair_supervisor_run(mapping, child_generation, pair_slot);
    if (code != QST_OK) {
      (void)qstWriteLifecycleLog(
          QST_ROLE_PAIR_SUPERVISOR, child_generation, "PROCESS_EXIT_ERROR",
          QST_ROLE_PAIR_SUPERVISOR, child_generation, (int32_t)getpid(),
          (int32_t)getpgrp(), code, 0);
    }
    if (code == QST_OK) {
      code = qst_process_mark_exited_instance(
          mapping, QST_ROLE_PAIR_SUPERVISOR, pair_slot, child_generation, 0);
      if (code != QST_OK) {
        (void)fprintf(stderr,
                      "taosQueryTest: pair supervisor exit-state update "
                      "failed slot=%u generation=%u code=%d\n",
                      pair_slot, child_generation, code);
      }
    }
    _exit(code == QST_OK ? 0 : 1);
  }

  (void)close(generation_pipe[0]);
  if (setpgid(pid, pid) != 0 && errno != EACCES && errno != ESRCH) {
    code = QST_E_RESOURCE;
  }
  if (code == QST_OK) {
    code = qst_process_register_instance(
        mapping, QST_ROLE_EXEC_MANAGER, 0, parent_generation,
        QST_ROLE_PAIR_SUPERVISOR, pair_slot, (int32_t)pid, (int32_t)pid,
        &generation);
  }
  if (code == QST_OK &&
      write(generation_pipe[1], &generation, sizeof(generation)) !=
          (ssize_t)sizeof(generation)) {
    code = QST_E_RESOURCE;
  }
  (void)close(generation_pipe[1]);
  if (code == QST_OK) {
    pidfd = qstOpenPidfd(pid);
    code = pidfd < 0 ? QST_E_RESOURCE : QST_OK;
  }
  if (code == QST_OK && wait_for_flow != 0) {
    code = qstWaitForRoleFlow(mapping, QST_ROLE_PAIR_SUPERVISOR, pair_slot,
                              generation,
                              qstMillisecondsToDeadlineNs(
                                  QST_PAIR_STARTUP_MILLISECONDS));
  }
  if (code != QST_OK) {
    if (pidfd >= 0) {
      (void)qstStopDirectChild(mapping, QST_ROLE_EXEC_MANAGER,
                               parent_generation, QST_ROLE_PAIR_SUPERVISOR,
                               generation, pid, pidfd);
      (void)close(pidfd);
    } else {
      (void)kill(-pid, SIGKILL);
    }
    (void)waitpid(pid, &status, 0);
    if (generation != 0) {
      (void)qst_process_reap_generation_instance(
          mapping, QST_ROLE_EXEC_MANAGER, 0, parent_generation,
          QST_ROLE_PAIR_SUPERVISOR, pair_slot, generation, status);
    }
    return code;
  }
  out_child->slot = pair_slot;
  out_child->generation = generation;
  out_child->pid = pid;
  out_child->pidfd = pidfd;
  out_child->retire_requested = 0;
  return QST_OK;
}

static EQstCode qstStopAndReapPairSupervisor(SQstShmMapping *mapping,
                                              uint32_t parent_generation,
                                              SQstPairChild *child) {
  const int32_t profile_enabled = getenv("QST_PROFILE_PHASES") != NULL;
  uint64_t started_ns = 0;
  uint64_t wait_supervisor_ns = 0;
  uint64_t reap_supervisor_ns = 0;
  int32_t exited = 0;
  int32_t exit_error = QST_OK;
  int32_t status = 0;
  int32_t deadline_exited = 0;
  uint32_t stop_generation = 0;
  uint32_t exit_state = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == child || child->generation == 0 ||
      child->pid <= 0 || child->pidfd < 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (profile_enabled != 0) {
    started_ns = qstMonotonicNowNs();
  }
  code = qst_process_request_stop_instance(
      mapping, QST_ROLE_EXEC_MANAGER, 0, parent_generation,
      QST_ROLE_PAIR_SUPERVISOR, child->slot, child->generation, 1);
  if (code == QST_E_STALE_OWNER &&
      qst_process_get_stop_generation_instance(
          mapping, QST_ROLE_PAIR_SUPERVISOR, child->slot, child->generation,
          &stop_generation, &exit_state) == QST_OK &&
      (exit_state == QST_PROCESS_STOP_REQUESTED ||
       exit_state == QST_PROCESS_STOP_ACKED ||
       exit_state == QST_PROCESS_EXITED || exit_state == QST_PROCESS_REAPED)) {
    /* A group stop may already have requested or acknowledged this pair, or it
     * may have exited before this caller reached it. The OS exit status remains
     * authoritative. */
    code = QST_OK;
  }
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: pair stop request failed slot=%u generation=%u code=%d state=%u stop_generation=%u\n",
                  child->slot, child->generation, code, exit_state,
                  stop_generation);
  }
  if (code == QST_OK) {
    code = qstWaitForExit(child->pidfd, QST_STOP_ACK_MILLISECONDS, &exited);
  }
  if (code == QST_OK && exited == 0) {
    code = qstTerminateProcessGroup(child->pid, child->pidfd);
  }
  if (profile_enabled != 0) {
    const uint64_t finished_ns = qstMonotonicNowNs();

    if (finished_ns >= started_ns) {
      wait_supervisor_ns = finished_ns - started_ns;
    }
  }
  while (waitpid(child->pid, &status, 0) < 0) {
    if (errno != EINTR) {
      return QST_E_RESOURCE;
    }
  }
  (void)close(child->pidfd);
  child->pidfd = -1;
  if (qst_process_get_error_instance(
          mapping, QST_ROLE_PAIR_SUPERVISOR, child->slot, child->generation,
          &exit_error) != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: pair exit reason unavailable slot=%u generation=%u\n",
                  child->slot, child->generation);
    return QST_E_RESOURCE;
  }
  if (exit_error == QST_E_DEADLINE) {
    /* The deadline path deliberately abandons its callback-owned client.
     * Reaping that already-exited pair is the defined stop completion. */
    deadline_exited = 1;
  }
  if (qst_process_reap_generation_instance(
          mapping, QST_ROLE_EXEC_MANAGER, 0, parent_generation,
          QST_ROLE_PAIR_SUPERVISOR, child->slot, child->generation,
          status) != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: pair reap failed slot=%u generation=%u "
                  "status=%d error=%d deadline=%d\n",
                  child->slot, child->generation, status, exit_error,
                  deadline_exited);
    return QST_E_RESOURCE;
  }
  if (profile_enabled != 0) {
    const uint64_t finished_ns = qstMonotonicNowNs();

    if (finished_ns >= started_ns + wait_supervisor_ns) {
      reap_supervisor_ns = finished_ns - started_ns - wait_supervisor_ns;
    }
    (void)fprintf(stderr,
                  "taosQueryTest: pair_stop_profile slot=%u generation=%u "
                  "wait_supervisor_ns=%llu reap_supervisor_ns=%llu\n",
                  child->slot, child->generation,
                  (unsigned long long)wait_supervisor_ns,
                  (unsigned long long)reap_supervisor_ns);
  }
  if (deadline_exited == 0 &&
      (!WIFEXITED(status) || WEXITSTATUS(status) != 0)) {
    (void)fprintf(stderr,
                  "taosQueryTest: pair stop observed abnormal exit slot=%u "
                  "generation=%u status=%d error=%d\n",
                  child->slot, child->generation, status, exit_error);
    return QST_E_RESOURCE;
  }
  child->slot = 0;
  child->generation = 0;
  child->pid = -1;
  return code;
}

/* Replaces one unexpectedly exited pair and restores its unfinalized task. */
static EQstCode qstRecoverExitedPairSupervisor(SQstShmMapping *mapping,
                                                uint32_t parent_generation,
                                                SQstPairChild *child) {
  SQstTaskDispatch retry_dispatch = {0};
  SQstExecutePayload isolated_payload = {0};
  int32_t status = 0;
  int32_t recovered_dispatch = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == child || parent_generation == 0 ||
      child->generation == 0 || child->pid <= 0 || child->pidfd < 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  {
    const EQstCode fact_code = qstPairRewriteRuntimeState(
        child->slot, child->generation, "crashed", 1, "exec_manager");

    if (fact_code != QST_OK && fact_code != QST_E_PENDING_IO) {
      return fact_code;
    }
  }
  /* A crashed pair supervisor can leave its taosd child reparented before the
   * exec manager observes the supervisor exit.  Reap that orphan immediately
   * so its process-ledger slot becomes REAPED and can be reused by the next
   * AUTO taosd generation.  Waiting until launcher shutdown would exhaust the
   * fixed process region after repeated parser crashes. */
  code = qstLauncherReapCrashedPairServers(mapping);
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: crashed pair taosd orphan cleanup failed "
                  "slot=%u generation=%u code=%d\n",
                  child->slot, child->generation, code);
    /* Recovery remains owned by the exec manager.  The launcher-side sweep
     * is opportunistic: a taosd may already have been reaped by the manager,
     * in which case propagating its stale-ledger result would kill recovery
     * and strand the task. */
    code = QST_OK;
  }
  while (waitpid(child->pid, &status, 0) < 0) {
    if (errno != EINTR) {
      return QST_E_RESOURCE;
    }
  }
  code = qst_task_recover_pair_failure(
      mapping, parent_generation, child->slot, child->generation,
      &retry_dispatch);
  if (code == QST_OK) {
    recovered_dispatch = 1;
  } else if (code == QST_E_QUEUE_EMPTY) {
    uint64_t final_gid = 0;
    uint64_t final_dispatch_epoch = 0;
    uint64_t final_transition_seq = 0;
    const uint64_t deadline = qstMillisecondsToDeadlineNs(
        QST_FLOW_LEASE_MILLISECONDS);

    if (deadline == 0) {
      return QST_E_RESOURCE;
    }
    code = qst_task_find_final_for_pair_recovery(
        mapping, parent_generation, child->slot, child->generation,
        &final_gid, &final_dispatch_epoch, &final_transition_seq);
    if (code == QST_OK) {
      code = qstForwardFinalTask(mapping, parent_generation, final_gid,
                                 final_dispatch_epoch, final_transition_seq,
                                 deadline);
    }
    if (code == QST_E_QUEUE_EMPTY) {
      code = QST_OK;
    }
  }
  if (code == QST_E_STALE_OWNER &&
      atomic_load_explicit(&mapping->header->stop_reason,
                           memory_order_acquire) != QST_STOP_NONE) {
    /* A drain revokes the manager's task ownership while final evidence can
     * still be publishing. Reap the pair and leave that durable flow intact. */
    code = QST_OK;
  }
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: pair task recovery failed slot=%u "
                  "generation=%u code=%d\n",
                  child->slot, child->generation, code);
    return code;
  }
  if (WIFSIGNALED(status) && recovered_dispatch != 0) {
    const int64_t now_ns = qstRealtimeNowNs();

    if (now_ns == 0) {
      return QST_E_RESOURCE;
    }
    isolated_payload.statement_ts_ns = now_ns;
    isolated_payload.ended_ts_ns = now_ns;
    isolated_payload.execution_status = QST_EXEC_ERROR;
    isolated_payload.execution_error_code = QST_E_RESOURCE;
    isolated_payload.result_row_count = -1;
    code = qst_task_isolate(mapping, retry_dispatch.gid, QST_E_RESOURCE,
                            QST_ROLE_EXEC_MANAGER, parent_generation,
                            &isolated_payload);
    if (code != QST_OK) {
      return code;
    }
  }
  (void)fprintf(stderr,
                "taosQueryTest: pair exit recovery slot=%u generation=%u "
                "pid=%d status=%d task_recovery=%d retry_gid=%llu\n",
                child->slot, child->generation, (int)child->pid, status, code,
                (unsigned long long)retry_dispatch.gid);
  (void)close(child->pidfd);
  child->pidfd = -1;
  if (qst_process_reap_generation_instance(
          mapping, QST_ROLE_EXEC_MANAGER, 0, parent_generation,
          QST_ROLE_PAIR_SUPERVISOR, child->slot, child->generation,
          status) != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: recovered pair reap failed slot=%u "
                  "generation=%u status=%d\n",
                  child->slot, child->generation, status);
    return QST_E_RESOURCE;
  }
  if (recovered_dispatch == 0) {
    /* The pair can fail just after its final task was forwarded.  Once no task
     * needs a replacement, do not start a server into the draining run. */
    child->slot = 0;
    child->generation = 0;
    child->pid = -1;
    return QST_OK;
  }
  if (qstStartPairSupervisor(mapping, parent_generation, child->slot, child,
                             1) != QST_OK) {
    return QST_E_RESOURCE;
  }
  if (recovered_dispatch != 0 && !WIFSIGNALED(status)) {
    const uint64_t deadline = qstMillisecondsToDeadlineNs(
        QST_FLOW_LEASE_MILLISECONDS);

    if (deadline == 0) {
      return QST_E_RESOURCE;
    }
    return qstRequeueManagerDispatch(mapping, parent_generation,
                                     &retry_dispatch, deadline);
  }
  return QST_OK;
}

/* Starts every live pair server's existing cooperative shutdown in parallel.
 * Pair supervisors retain client cleanup and the only authority to reap their
 * direct taosd child. */
static EQstCode qstSignalPairServersForStop(SQstShmMapping *mapping,
                                            uint32_t parent_generation) {
  SQstProcessIdentity *processes = NULL;
  uint32_t process_count = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == mapping->header || parent_generation == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  processes = (SQstProcessIdentity *)calloc(
      mapping->header->process_region.element_count, sizeof(*processes));
  if (NULL == processes) {
    return QST_E_RESOURCE;
  }
  code = qst_process_list_descendants_postorder(
      mapping, QST_ROLE_EXEC_MANAGER, parent_generation, processes,
      mapping->header->process_region.element_count, &process_count);
  if (code != QST_OK) {
    free(processes);
    return code;
  }
  for (uint32_t index = 0; index < process_count; ++index) {
    const SQstProcessIdentity *identity = &processes[index];

    if (identity->role != QST_ROLE_TAOSD ||
        identity->exit_state != QST_PROCESS_RUNNING || identity->pid <= 0 ||
        identity->pgid <= 0) {
      continue;
    }
    if (kill(-identity->pgid, SIGTERM) != 0 && errno != ESRCH) {
      code = QST_E_RESOURCE;
      break;
    }
  }
  free(processes);
  return code;
}

static EQstCode qstStopAndReapPairSupervisors(
    SQstShmMapping *mapping, uint32_t parent_generation,
    SQstPairChild *children, uint32_t child_count) {
  EQstCode code = QST_OK;

  if (NULL == mapping || (child_count != 0 && NULL == children)) {
    return QST_E_INVALID_ARGUMENT;
  }
  /* Every pair owns a separate process group. Request every cooperative stop
   * before waiting for one so a manager shutdown is bounded by the slowest
   * pair, not by pair_count times the stop acknowledgement interval. */
  for (uint32_t index = 0; index < child_count; ++index) {
    EQstCode request_code = QST_OK;

    if (children[index].generation == 0) continue;
    request_code = qst_process_request_stop_instance(
        mapping, QST_ROLE_EXEC_MANAGER, 0, parent_generation,
        QST_ROLE_PAIR_SUPERVISOR, children[index].slot,
        children[index].generation, 1);
    if (request_code != QST_OK && request_code != QST_E_STALE_OWNER &&
        code == QST_OK) {
      code = request_code;
    }
  }
  if (code == QST_OK) {
    /* Once all pairs have entered STOP_REQUESTED, overlap isolated server
     * teardown. Serial reaping below still verifies every pair cleanup path. */
    code = qstSignalPairServersForStop(mapping, parent_generation);
  }
  for (uint32_t index = 0; index < child_count; ++index) {
    EQstCode stop_code = QST_OK;

    if (children[index].generation == 0) {
      continue;
    }
    stop_code = qstStopAndReapPairSupervisor(mapping, parent_generation,
                                              &children[index]);
    if (stop_code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: pair stop and reap failed slot=%u "
                    "generation=%u pid=%d code=%d\n",
                    children[index].slot, children[index].generation,
                    (int)children[index].pid, stop_code);
      code = QST_E_RESOURCE;
    }
  }
  return code;
}

/* Records a core-loop confirmation only after observing the selected queue. */
static EQstCode qstConfirmQueueFlow(SQstShmMapping *mapping, EQstRole role,
                                    uint32_t generation,
                                    EQstFlowStage flow_stage,
                                    EQstQueueId queue_id,
                                    uint64_t current_gid,
                                    uint64_t deadline_mono_ns) {
  SQstQueueSnapshot snapshot = {0};
  EQstCode code = QST_OK;

  if (NULL == mapping || generation == 0 ||
      flow_stage < QST_FLOW_STAGE_STARTUP ||
      flow_stage > QST_FLOW_STAGE_RECOVERING) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qst_queue_snapshot(mapping, queue_id, &snapshot);
  if (code != QST_OK) {
    return code;
  }
  return qst_process_confirm_flow(
      mapping, role, generation, flow_stage,
      queue_id == QST_QUEUE_CONTROL ? QST_FLOW_STATE_CONTROL_QUEUE :
          QST_FLOW_STATE_INPUT_QUEUE,
      snapshot.progress_epoch, current_gid, deadline_mono_ns);
}

/* Publishes only the immutable task-ledger snapshot owned by this manager. */
static EQstCode qstForwardFinalTask(SQstShmMapping *mapping,
                                    uint32_t generation, uint64_t gid,
                                    uint64_t dispatch_epoch,
                                    uint64_t transition_seq,
                                    uint64_t deadline_mono_ns) {
  SQstExecutePayload payload = {0};
  SQstMessage message = {0};
  uint32_t target = 0;
  EQstCode code = QST_OK;

  code = qst_task_get_final(mapping, gid, dispatch_epoch, transition_seq,
                            QST_ROLE_EXEC_MANAGER, generation, &payload,
                            &target);
  if (code != QST_OK) {
    return code;
  }
  message.message_type = QST_MSG_EXECUTE_FINAL;
  message.producer_role = QST_ROLE_EXEC_MANAGER;
  message.producer_generation = generation;
  message.target = target;
  message.run_id = mapping->header->run_id;
  message.gid = gid;
  message.dispatch_epoch = dispatch_epoch;
  message.transition_seq = transition_seq;
  message.body.execute = payload;
  code = qst_queue_publish(mapping, QST_QUEUE_EXECUTE, &message,
                           deadline_mono_ns);
  if (code != QST_OK) {
    return code;
  }
  return qst_task_confirm_final_published(
      mapping, gid, dispatch_epoch, transition_seq, QST_ROLE_EXEC_MANAGER,
      generation, &payload);
}

static EQstCode qstMakeControlMessage(const SQstShmMapping *mapping,
                                      EQstRole producer_role,
                                      uint32_t producer_generation,
                                      EQstControlOpcode opcode,
                                      EQstRole target_role,
                                      uint32_t target_generation,
                                      uint32_t stop_generation,
                                      SQstMessage *out_message) {
  if (NULL == mapping || NULL == mapping->header || NULL == out_message ||
      producer_role < QST_ROLE_LAUNCHER || producer_role > QST_ROLE_AI_WORKER ||
      producer_generation == 0 || target_role < QST_ROLE_MAIN ||
      target_role > QST_ROLE_AI_WORKER || target_generation == 0 ||
      stop_generation == 0 ||
      (opcode != QST_CONTROL_STOP_REQUEST && opcode != QST_CONTROL_STOP_ACK)) {
    return QST_E_INVALID_ARGUMENT;
  }
  memset(out_message, 0, sizeof(*out_message));
  out_message->message_type = QST_MSG_CONTROL;
  out_message->producer_role = producer_role;
  out_message->producer_generation = producer_generation;
  out_message->run_id = mapping->header->run_id;
  out_message->body.control.opcode = opcode;
  out_message->body.control.target_role = target_role;
  out_message->body.control.target_generation = target_generation;
  out_message->body.control.requested_stop_generation = stop_generation;
  return QST_OK;
}

static EQstCode qstAcknowledgeRoleStop(SQstShmMapping *mapping,
                                       EQstRole role, uint32_t generation,
                                       uint32_t stop_generation) {
  SQstMessage acknowledgement = {0};
  EQstCode code = QST_OK;

  if (NULL == mapping || generation == 0 || stop_generation == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qstConfirmQueueFlow(mapping, role, generation,
                             QST_FLOW_STAGE_STOPPING, QST_QUEUE_CONTROL, 0, 0);
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: stop flow confirmation failed role=%s "
                  "generation=%u stop_generation=%u code=%d\n",
                  qstRoleName(role), generation, stop_generation, code);
    return code;
  }
  code = qst_process_ack_stop(mapping, role, generation, stop_generation);
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: stop process acknowledgement failed role=%s "
                  "generation=%u stop_generation=%u code=%d\n",
                  qstRoleName(role), generation, stop_generation, code);
    return code;
  }
  code = qstMakeControlMessage(mapping, role, generation, QST_CONTROL_STOP_ACK,
                               role, generation, stop_generation,
                               &acknowledgement);
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: stop acknowledgement message failed role=%s "
                  "generation=%u stop_generation=%u code=%d\n",
                  qstRoleName(role), generation, stop_generation, code);
    return code;
  }
  code = qst_queue_publish(mapping, QST_QUEUE_CONTROL, &acknowledgement,
                           qstMillisecondsToDeadlineNs(
                               QST_STOP_ACK_MILLISECONDS));
  /* qst_process_ack_stop() publishes STOP_ACKED before this best-effort
   * notification. Queue producer validation consequently may observe this
   * process as no longer RUNNING. The process-table acknowledgement is the
   * authoritative stop proof, so that expected ownership rejection must not
   * turn a completed stop into a child failure. */
  if (code != QST_OK && code != QST_E_STALE_OWNER) {
    (void)fprintf(stderr,
                  "taosQueryTest: stop acknowledgement publish failed role=%s "
                  "generation=%u stop_generation=%u code=%d\n",
                  qstRoleName(role), generation, stop_generation, code);
  }
  return code == QST_E_STALE_OWNER ? QST_OK : code;
}

static EQstCode qstWaitForRoleFlow(const SQstShmMapping *mapping,
                                   EQstRole role, uint32_t slot_id,
                                   uint32_t generation,
                                   uint64_t deadline_mono_ns) {
  const SQstRegion *region = NULL;
  SQstProcessSlot *slots = NULL;
  struct pollfd event = {0};

  if (NULL == mapping || NULL == mapping->header || NULL == mapping->base ||
      role < QST_ROLE_MAIN || role > QST_ROLE_AI_WORKER || generation == 0 ||
      deadline_mono_ns == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  region = &mapping->header->process_region;
  if (region->element_bytes != sizeof(SQstProcessSlot) ||
      region->region_offset > mapping->bytes ||
      region->region_bytes > mapping->bytes - region->region_offset) {
    return QST_E_SHM_CORRUPT;
  }
  slots = (SQstProcessSlot *)((uint8_t *)mapping->base + region->region_offset);
  event.fd = mapping->queue_eventfd[QST_QUEUE_CONTROL - 1];
  event.events = POLLIN;
  for (;;) {
    if (gQstStopSignal != 0) {
      return QST_E_STOPPING;
    }
    for (uint32_t index = 0; index < region->element_count; ++index) {
      if (atomic_load_explicit(&slots[index].role, memory_order_acquire) == role &&
          slots[index].slot_id == slot_id &&
          atomic_load_explicit(&slots[index].generation, memory_order_acquire) ==
              generation) {
        if (atomic_load_explicit(&slots[index].flow_seq, memory_order_acquire) !=
            0) {
          return QST_OK;
        }
        if (atomic_load_explicit(&slots[index].exit_state,
                                 memory_order_acquire) != QST_PROCESS_RUNNING) {
          return QST_E_STALE_OWNER;
        }
      }
    }
    if (qstMillisecondsToDeadlineNs(1) == 0 ||
        qstMillisecondsToDeadlineNs(1) > deadline_mono_ns) {
      return QST_E_DEADLINE;
    }
    if (qstPollQueueEvent(&event, 20) != QST_OK) return QST_E_RESOURCE;
  }
}

static EQstCode qstSendStopRequest(SQstShmMapping *mapping,
                                   EQstRole parent_role,
                                   uint32_t parent_generation,
                                   EQstRole role, uint32_t generation,
                                   uint32_t stop_generation) {
  SQstMessage message = {0};
  EQstCode code = qstMakeControlMessage(
      mapping, parent_role, parent_generation, QST_CONTROL_STOP_REQUEST, role,
      generation, stop_generation, &message);

  if (code != QST_OK) {
    return code;
  }
  code = qst_process_request_stop(mapping, parent_role, parent_generation,
                                  role, generation, stop_generation);
  if (code != QST_OK) {
    return code;
  }
  return qst_queue_publish(mapping, QST_QUEUE_CONTROL, &message,
                           qstMillisecondsToDeadlineNs(
                               QST_STOP_ACK_MILLISECONDS));
}

static EQstCode qstWaitForStopAck(SQstShmMapping *mapping,
                                  EQstRole parent_role,
                                  uint32_t parent_generation, EQstRole role,
                                  uint32_t generation,
                                  uint32_t stop_generation) {
  const uint64_t deadline = qstMillisecondsToDeadlineNs(
      QST_STOP_ACK_MILLISECONDS);
  struct pollfd event = {0};

  if (NULL == mapping || deadline == 0) {
    return QST_E_RESOURCE;
  }
  event.fd = mapping->queue_eventfd[QST_QUEUE_CONTROL - 1];
  event.events = POLLIN;
  for (;;) {
    SQstClaim claim = {0};
    uint32_t observed_stop_generation = 0;
    uint32_t observed_exit_state = 0;
    EQstCode code = qst_queue_claim(mapping, QST_QUEUE_CONTROL, parent_role,
                                    parent_generation, deadline, &claim);

    /* Process-table acknowledgement remains authoritative if another observer
     * has already consumed the matching control message. */
    if (qst_process_get_stop_generation(mapping, role, generation,
                                        &observed_stop_generation,
                                        &observed_exit_state) == QST_OK &&
        observed_stop_generation == stop_generation &&
        (observed_exit_state == QST_PROCESS_STOP_ACKED ||
         observed_exit_state == QST_PROCESS_EXITED)) {
      return QST_OK;
    }

    if (code == QST_OK) {
      const SQstControlPayload *control = &claim.message.body.control;
      const int32_t expected_ack =
          control->opcode == QST_CONTROL_STOP_ACK &&
          claim.message.producer_role == role &&
          claim.message.producer_generation == generation &&
          control->target_role == role &&
          control->target_generation == generation &&
          control->requested_stop_generation == stop_generation;
      if (qst_queue_ack(mapping, &claim, QST_ACK_CONTROL_RESPONSE,
                        claim.message.message_id) != QST_OK) {
        return QST_E_SHM_CORRUPT;
      }
      if (expected_ack) {
        return QST_OK;
      }
      continue;
    }
    if (code != QST_E_QUEUE_EMPTY) {
      return code;
    }
    if (qstPollQueueEvent(&event, 20) != QST_OK) return QST_E_RESOURCE;
    if (qstMillisecondsToDeadlineNs(1) == 0 ||
        qstMillisecondsToDeadlineNs(1) > deadline) {
      return QST_E_DEADLINE;
    }
  }
}

static EQstCode qstStartManager(SQstShmMapping *mapping,
                                uint32_t parent_generation, EQstRole role,
                                QstRoleEntry entry,
                                SQstManagerChild *out_child) {
  int32_t generation_pipe[2] = {-1, -1};
  int32_t pidfd = -1;
  int32_t status = 0;
  pid_t pid = -1;
  uint32_t generation = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == entry || parent_generation == 0 ||
      (role != QST_ROLE_GENERATOR && role != QST_ROLE_EXEC_MANAGER &&
       role != QST_ROLE_JUDGE_MANAGER) || NULL == out_child) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (pipe(generation_pipe) != 0) {
    return QST_E_RESOURCE;
  }
  pid = fork();
  if (pid < 0) {
    (void)close(generation_pipe[0]);
    (void)close(generation_pipe[1]);
    return QST_E_RESOURCE;
  }
  if (pid == 0) {
    uint32_t child_generation = 0;
    ssize_t bytes = 0;
    const pid_t parent_pid = getppid();

    (void)close(generation_pipe[1]);
    if (prctl(PR_SET_PDEATHSIG, SIGKILL) != 0 || getppid() != parent_pid ||
        setpgid(0, 0) != 0) {
      (void)close(generation_pipe[0]);
      _exit(1);
    }
    bytes = read(generation_pipe[0], &child_generation, sizeof(child_generation));
    (void)close(generation_pipe[0]);
    if (bytes != (ssize_t)sizeof(child_generation) || child_generation == 0) {
      _exit(1);
    }
    code = entry(mapping, child_generation);
    if (code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: manager startup failed role=%s generation=%u code=%d\n",
                    qstRoleName(role), child_generation, code);
    }
    if (code == QST_E_STALE_OWNER || code == QST_E_STOPPING) {
      uint32_t stop_generation = 0;
      uint32_t exit_state = 0;
      if (qst_process_get_stop_generation(mapping, role, child_generation,
                                          &stop_generation, &exit_state) == QST_OK &&
          (exit_state == QST_PROCESS_STOP_REQUESTED ||
           exit_state == QST_PROCESS_STOP_ACKED ||
           exit_state == QST_PROCESS_KILLED)) {
        code = QST_OK;
      }
      if ((code == QST_E_STALE_OWNER || code == QST_E_STOPPING) &&
          atomic_load_explicit(&mapping->header->stop_reason,
                               memory_order_acquire) != QST_STOP_NONE) {
        /* A manager can observe a pair generation change after the generator
         * has already published the terminal stop decision. The stop reason
         * is the durable run-level fact; do not turn that expected teardown
         * race into a failed main run. */
        code = QST_OK;
      }
    }
    if (code != QST_OK) {
      (void)qstWriteLifecycleLog(role, child_generation, "PROCESS_EXIT_ERROR",
                                 role, child_generation, (int32_t)getpid(),
                                 (int32_t)getpgrp(), code, 0);
    }
    if (code == QST_OK) {
      code = qst_process_mark_exited(mapping, role, child_generation, 0);
    }
    _exit(code == QST_OK ? 0 : 1);
  }

  (void)close(generation_pipe[0]);
  if (setpgid(pid, pid) != 0 && errno != EACCES && errno != ESRCH) {
    code = QST_E_RESOURCE;
  }
  if (code == QST_OK) {
    code = qst_process_register(mapping, QST_ROLE_MAIN, parent_generation,
                                role, (int32_t)pid, (int32_t)pid,
                                &generation);
  }
  if (code == QST_OK) {
    code = qstWriteLifecycleLog(QST_ROLE_MAIN, parent_generation,
                                "CHILD_SPAWN", role, generation,
                                (int32_t)pid, (int32_t)pid, QST_OK, 0);
  }
  if (code == QST_OK &&
      write(generation_pipe[1], &generation, sizeof(generation)) !=
          (ssize_t)sizeof(generation)) {
    code = QST_E_RESOURCE;
  }
  (void)close(generation_pipe[1]);
  generation_pipe[1] = -1;
  if (code == QST_OK) {
    pidfd = qstOpenPidfd(pid);
    if (pidfd < 0) {
      code = QST_E_RESOURCE;
    }
  }
  if (code == QST_OK) {
    code = qstWaitForRoleFlow(mapping, role, 0, generation,
                              qstMillisecondsToDeadlineNs(
                                  QST_STOP_ACK_MILLISECONDS));
  }
  if (code == QST_OK) {
    code = qstWriteLifecycleLog(QST_ROLE_MAIN, parent_generation,
                                "CHILD_READY", role, generation,
                                (int32_t)pid, (int32_t)pid, QST_OK, 0);
  }
  if (code != QST_OK) {
    if (pidfd >= 0) {
      const EQstCode stop_code = qstStopDirectChild(
          mapping, QST_ROLE_MAIN, parent_generation, role, generation, pid,
          pidfd);
      if (stop_code != QST_OK) {
        code = stop_code;
      }
    } else {
      (void)kill(-pid, SIGKILL);
    }
  }
  if (code != QST_OK) {
    (void)waitpid(pid, &status, 0);
    if (pidfd >= 0) {
      (void)close(pidfd);
    }
    if (generation != 0) {
      (void)qst_process_reap_generation(mapping, QST_ROLE_MAIN,
                                        parent_generation, role, generation,
                                        status);
    }
    return code;
  }
  out_child->role = role;
  out_child->generation = generation;
  out_child->pid = pid;
  out_child->pidfd = pidfd;
  return QST_OK;
}

static EQstCode qstReapManager(SQstShmMapping *mapping,
                               uint32_t parent_generation,
                               SQstManagerChild *child) {
  int32_t status = 0;

  if (NULL == mapping || NULL == child || child->generation == 0 ||
      child->pid <= 0 || child->pidfd < 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  while (waitpid(child->pid, &status, 0) < 0) {
    if (errno != EINTR) {
      return QST_E_RESOURCE;
    }
  }
  (void)close(child->pidfd);
  child->pidfd = -1;
  if (qstWriteLifecycleLog(QST_ROLE_MAIN, parent_generation, "CHILD_EXITED",
                           child->role, child->generation, (int32_t)child->pid,
                           (int32_t)child->pid, QST_OK, status) != QST_OK ||
      qst_process_reap_generation(mapping, QST_ROLE_MAIN, parent_generation,
                                  child->role, child->generation, status) != QST_OK ||
      qstWriteLifecycleLog(QST_ROLE_MAIN, parent_generation, "CHILD_REAPED",
                           child->role, child->generation, (int32_t)child->pid,
                           (int32_t)child->pid, QST_OK, status) != QST_OK) {
    return QST_E_RESOURCE;
  }
  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
    (void)fprintf(stderr,
                  "taosQueryTest: manager exited unexpectedly role=%s "
                  "generation=%u pid=%d status=%d\n",
                  qstRoleName(child->role), child->generation,
                  (int32_t)child->pid, status);
    return QST_E_RESOURCE;
  }
  return QST_OK;
}

static EQstCode qstStopAndReapManager(SQstShmMapping *mapping,
                                      uint32_t parent_generation,
                                      SQstManagerChild *child) {
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == child || child->generation == 0 ||
      child->pid <= 0 || child->pidfd < 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (child->stop_requested != 0) {
    int32_t exited = 0;

    /* The stop request and control message were already published while the
     * persistence queue drained. Do not allocate a second stop generation;
     * simply wait for the manager's cooperative exit and fall back to the
     * existing SIGTERM/SIGKILL policy if it does not acknowledge in time. */
    code = qstWaitForExit(child->pidfd, QST_STOP_ACK_MILLISECONDS, &exited);
    if (code == QST_OK && exited == 0) {
      code = qstTerminateProcessGroup(child->pid, child->pidfd);
    }
  } else {
    code = qstWriteLifecycleLog(QST_ROLE_MAIN, parent_generation,
                                "CHILD_STOP_REQUEST", child->role,
                                child->generation, (int32_t)child->pid,
                                (int32_t)child->pid, QST_OK, 0);
    if (code == QST_OK) {
      code = qstStopDirectChild(mapping, QST_ROLE_MAIN, parent_generation,
                                child->role, child->generation, child->pid,
                                child->pidfd);
    }
  }
  if (code == QST_OK) {
    code = qstWriteLifecycleLog(QST_ROLE_MAIN, parent_generation,
                                "CHILD_STOP_ACK", child->role,
                                child->generation, (int32_t)child->pid,
                                (int32_t)child->pid, QST_OK, 0);
  }
  if (code != QST_OK && child->pid > 0) {
    (void)kill(-child->pid, SIGKILL);
    (void)kill(child->pid, SIGKILL);
  }
  if (qstReapManager(mapping, parent_generation, child) != QST_OK) {
    return QST_E_RESOURCE;
  }
  return code;
}

/* Returns true only when every published task completed its required durable bits. */
static EQstCode qstMainAllTasksDurable(const SQstShmMapping *mapping,
                                       int32_t *out_all_durable) {
  const SQstRegion *region = NULL;
  SQstTaskEntry *entries = NULL;

  if (NULL == mapping || NULL == mapping->header || NULL == mapping->base ||
      NULL == out_all_durable) {
    return QST_E_INVALID_ARGUMENT;
  }
  region = &mapping->header->task_region;
  if (region->element_bytes != sizeof(SQstTaskEntry) ||
      region->region_offset > mapping->bytes ||
      region->region_bytes !=
          (uint64_t)region->element_count * region->element_bytes ||
      region->region_bytes > mapping->bytes - region->region_offset) {
    return QST_E_SHM_CORRUPT;
  }
  entries = (SQstTaskEntry *)((uint8_t *)mapping->base + region->region_offset);
  *out_all_durable = 1;
  for (uint32_t index = 0; index < region->element_count; ++index) {
    if (entries[index].gid == 0) {
      continue;
    }
    if (atomic_load_explicit(&entries[index].phase, memory_order_acquire) !=
        QST_TASK_PERSISTED) {
      *out_all_durable = 0;
      return QST_OK;
    }
  }
  return QST_OK;
}

/* Applies one worker durability confirmation and releases its task blocks. */
static EQstCode qstMainDrainOnePersistence(SQstShmMapping *mapping,
                                           uint32_t main_generation) {
  SQstClaim claim = {0};
  const uint64_t deadline = qstMillisecondsToDeadlineNs(
      QST_FLOW_LEASE_MILLISECONDS);
  uint64_t started_ns = 0;
  uint64_t ended_ns = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || main_generation == 0 || deadline == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (gQstMainPersistProfile.enabled != 0) {
    started_ns = qstMonotonicNowNs();
  }
  code = qst_queue_claim(mapping, QST_QUEUE_PERSIST, QST_ROLE_MAIN,
                         main_generation, deadline, &claim);
  if (gQstMainPersistProfile.enabled != 0) {
    ended_ns = qstMonotonicNowNs();
    if (ended_ns >= started_ns) {
      gQstMainPersistProfile.claim_ns += ended_ns - started_ns;
    }
  }
  if (code == QST_E_QUEUE_EMPTY) {
    return code;
  }
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: persistence queue claim failed code=%d\n",
                  code);
    return code;
  }
  if (gQstMainPersistProfile.enabled != 0) {
    ++gQstMainPersistProfile.claim_count;
    started_ns = qstMonotonicNowNs();
  }
  code = qst_task_mark_durable(
      mapping, claim.message.gid, claim.message.dispatch_epoch,
      (claim.message.body.persist.reserved0 & QST_PERSIST_CONFIRM_BATCH) != 0 ?
          (uint32_t)claim.message.body.persist.stats_delta_mask :
      claim.message.body.persist.persist_kind == QST_PERSIST_STATEMENT ?
          QST_PERSIST_MASK_STATEMENT :
      claim.message.body.persist.persist_kind == QST_PERSIST_HISTORICAL_VERDICT ?
          QST_PERSIST_MASK_HISTORICAL_VERDICT : QST_PERSIST_MASK_INITIAL_RESULT,
      claim.message.body.persist.durable_source,
      claim.message.body.persist.record_seq, QST_ROLE_MAIN, main_generation);
  if (gQstMainPersistProfile.enabled != 0) {
    ended_ns = qstMonotonicNowNs();
    if (ended_ns >= started_ns) {
      gQstMainPersistProfile.mark_durable_ns += ended_ns - started_ns;
    }
  }
  if (code == QST_OK) {
    if (gQstMainPersistProfile.enabled != 0) {
      started_ns = qstMonotonicNowNs();
    }
    code = qst_queue_ack(mapping, &claim, QST_ACK_DURABILITY_APPLIED,
                         claim.message.body.persist.record_seq);
    if (gQstMainPersistProfile.enabled != 0) {
      ended_ns = qstMonotonicNowNs();
      if (ended_ns >= started_ns) {
        gQstMainPersistProfile.queue_ack_ns += ended_ns - started_ns;
      }
    }
  }
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: persistence task update failed gid=%llu "
                  "kind=%u record_seq=%llu code=%d\n",
                  (unsigned long long)claim.message.gid,
                  claim.message.body.persist.persist_kind,
                  (unsigned long long)claim.message.body.persist.record_seq,
                  code);
    (void)qst_queue_abandon(mapping, &claim, code);
    return code;
  }
  if (!qstRefIsEmpty(claim.message.body.persist.payload_ref)) {
    if (gQstMainPersistProfile.enabled != 0) {
      started_ns = qstMonotonicNowNs();
    }
    code = qst_block_release(mapping, claim.message.body.persist.payload_ref,
                             claim.message.gid);
    if (gQstMainPersistProfile.enabled != 0) {
      ended_ns = qstMonotonicNowNs();
      if (ended_ns >= started_ns) {
        gQstMainPersistProfile.payload_release_ns += ended_ns - started_ns;
      }
    }
    if (code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: persistence payload release failed gid=%llu "
                    "offset=%llu length=%u code=%d\n",
                    (unsigned long long)claim.message.gid,
                    (unsigned long long)claim.message.body.persist.payload_ref.offset,
                    claim.message.body.persist.payload_ref.length, code);
      return QST_E_SHM_CORRUPT;
    }
  }
  if (!qstRefIsEmpty(claim.message.body.persist.stable_key_ref)) {
    if (gQstMainPersistProfile.enabled != 0) {
      started_ns = qstMonotonicNowNs();
    }
    code = qst_block_release(mapping, claim.message.body.persist.stable_key_ref,
                             claim.message.gid);
    if (gQstMainPersistProfile.enabled != 0) {
      ended_ns = qstMonotonicNowNs();
      if (ended_ns >= started_ns) {
        gQstMainPersistProfile.stable_key_release_ns += ended_ns - started_ns;
      }
    }
    if (code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: persistence stable key release failed gid=%llu "
                    "offset=%llu length=%u code=%d\n",
                    (unsigned long long)claim.message.gid,
                    (unsigned long long)claim.message.body.persist.stable_key_ref.offset,
                    claim.message.body.persist.stable_key_ref.length, code);
      return QST_E_SHM_CORRUPT;
    }
  }
  if (!qstRefIsEmpty(claim.message.body.persist.pending_file_ref)) {
    if (gQstMainPersistProfile.enabled != 0) {
      started_ns = qstMonotonicNowNs();
    }
    code = qst_block_release(mapping, claim.message.body.persist.pending_file_ref,
                             claim.message.gid);
    if (gQstMainPersistProfile.enabled != 0) {
      ended_ns = qstMonotonicNowNs();
      if (ended_ns >= started_ns) {
        gQstMainPersistProfile.pending_file_release_ns += ended_ns - started_ns;
      }
    }
    if (code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: persistence pending-file release failed gid=%llu "
                    "offset=%llu length=%u code=%d\n",
                    (unsigned long long)claim.message.gid,
                    (unsigned long long)claim.message.body.persist.pending_file_ref.offset,
                    claim.message.body.persist.pending_file_ref.length, code);
      return QST_E_SHM_CORRUPT;
    }
  }
  if (gQstMainPersistProfile.enabled != 0) {
    started_ns = qstMonotonicNowNs();
  }
  code = qst_task_release_persisted_blocks(
      mapping, claim.message.gid, QST_ROLE_MAIN, main_generation);
  if (gQstMainPersistProfile.enabled != 0) {
    ended_ns = qstMonotonicNowNs();
    if (ended_ns >= started_ns) {
      gQstMainPersistProfile.persisted_blocks_release_ns += ended_ns - started_ns;
    }
  }
  if (code == QST_E_TASK_STATE) {
    /* This confirmation supplied only one of the task's durable bits. */
    code = QST_OK;
  }
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: persisted task block release failed gid=%llu "
                  "code=%d\n",
                  (unsigned long long)claim.message.gid, code);
    return code;
  }
  if (gQstMainPersistProfile.enabled != 0) {
    started_ns = qstMonotonicNowNs();
  }
  code = qst_process_record_business(mapping, QST_ROLE_MAIN, main_generation,
                                     claim.message.gid, deadline);
  if (gQstMainPersistProfile.enabled != 0) {
    ended_ns = qstMonotonicNowNs();
    if (ended_ns >= started_ns) {
      gQstMainPersistProfile.business_ns += ended_ns - started_ns;
    }
  }
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: persistence flow update failed gid=%llu code=%d\n",
                  (unsigned long long)claim.message.gid, code);
    return code;
  }
  if (gQstMainPersistProfile.enabled != 0) {
    started_ns = qstMonotonicNowNs();
  }
  code = qst_task_reclaim_persisted(mapping, claim.message.gid,
                                    QST_ROLE_MAIN, main_generation);
  if (gQstMainPersistProfile.enabled != 0) {
    ended_ns = qstMonotonicNowNs();
    if (ended_ns >= started_ns) {
      gQstMainPersistProfile.reclaim_ns += ended_ns - started_ns;
    }
  }
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: persisted task ledger reclaim failed gid=%llu "
                  "code=%d\n",
                  (unsigned long long)claim.message.gid, code);
    return code;
  }
  return QST_OK;
}

/* Requests exec-manager shutdown after no pair-owned execution input remains.
 * The manager then overlaps normal isolated-taosd teardown with judge-side
 * durability work; the later reap still verifies every child and process fact. */
static EQstCode qstRequestManagerStopNoWait(SQstShmMapping *mapping,
                                            uint32_t parent_generation,
                                            SQstManagerChild *child) {
  uint32_t stop_generation = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || parent_generation == 0 || NULL == child ||
      child->role != QST_ROLE_EXEC_MANAGER || child->generation == 0 ||
      child->pid <= 0 || child->pidfd < 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (child->stop_requested != 0) {
    return QST_OK;
  }
  code = qstNextStopGeneration(mapping, &stop_generation);
  if (code == QST_OK) {
    code = qstSendStopRequest(mapping, QST_ROLE_MAIN, parent_generation,
                              child->role, child->generation,
                              stop_generation);
  }
  if (code == QST_OK) {
    child->stop_requested = 1;
  }
  return code;
}

/* Keeps the launcher watchdog bounded while main reaps its direct managers. */
static EQstCode qstMainPublishShutdownLease(SQstShmMapping *mapping,
                                            uint32_t main_generation) {
  const uint64_t deadline = qstMillisecondsToDeadlineNs(
      QST_DRAIN_LEASE_MILLISECONDS);

  if (NULL == mapping || main_generation == 0 || deadline == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  return qst_process_confirm_flow(
      mapping, QST_ROLE_MAIN, main_generation, QST_FLOW_STAGE_STOPPING,
      QST_FLOW_STATE_DIRECT_CHILDREN, 0, 0, deadline);
}

/* Applies worker durability confirmations until every generated task is durable. */
static EQstCode qstMainDrainPersistence(SQstShmMapping *mapping,
                                        uint32_t main_generation,
                                        SQstManagerChild *exec_manager) {
  struct pollfd event = {0};
  uint64_t deadline = qstMillisecondsToDeadlineNs(
      QST_DRAIN_LEASE_MILLISECONDS);

  if (NULL == mapping || main_generation == 0 || deadline == 0 ||
      NULL == exec_manager) {
    return QST_E_INVALID_ARGUMENT;
  }
  event.fd = mapping->queue_eventfd[QST_QUEUE_PERSIST - 1];
  event.events = POLLIN;
  for (;;) {
    int32_t all_durable = 0;
    EQstCode code = qstMainAllTasksDurable(mapping, &all_durable);

    qstMainStageProfileObserve(mapping);

    if (exec_manager->stop_requested == 0) {
      int32_t execution_inputs_drained = 0;

      code = qst_task_execution_inputs_drained(mapping,
                                                &execution_inputs_drained);
      if (code != QST_OK) {
        return code;
      }
      if (execution_inputs_drained != 0) {
        code = qstRequestManagerStopNoWait(mapping, main_generation,
                                            exec_manager);
        if (code != QST_OK) {
          return code;
        }
      }
    }

    if (code != QST_OK || all_durable != 0) {
      if (code != QST_OK) {
        (void)fprintf(stderr,
                      "taosQueryTest: persistence durability check failed code=%d\n",
                      code);
      }
      return code;
    }
    code = qstMainDrainOnePersistence(mapping, main_generation);
    if (code == QST_OK) {
      /* Persistence progress renews the drain lease. A fixed deadline would
       * turn a healthy long queue into QST_E_DEADLINE while workers are still
       * making durable progress. */
      deadline = qstMillisecondsToDeadlineNs(QST_DRAIN_LEASE_MILLISECONDS);
      if (deadline == 0) {
        return QST_E_RESOURCE;
      }
      continue;
    }
    if (code != QST_E_QUEUE_EMPTY) {
      return code;
    }
    /* A live pair can still own the final async batch while persistence is
     * temporarily empty. Keep the launcher's main-process lease current for
     * this drain-only wait; no task deadline is extended because gid is zero. */
    code = qst_process_record_business(mapping, QST_ROLE_MAIN,
                                       main_generation, 0, deadline);
    if (code != QST_OK) {
      return code;
    }
    if (qstMillisecondsToDeadlineNs(1) == 0 ||
        qstMillisecondsToDeadlineNs(1) > deadline) {
      return QST_E_DEADLINE;
    }
    if (qstPollQueueEvent(&event, 20) != QST_OK) {
      return QST_E_RESOURCE;
    }
  }
}

/* Detect a manager loss without putting a pidfd poll on every persisted row. */
static EQstCode qstMainCheckManagerHealth(const SQstManagerChild *manager) {
  int32_t exited = 0;
  EQstCode code = QST_OK;

  if (NULL == manager || manager->generation == 0 || manager->pid <= 0 ||
      manager->pidfd < 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qstWaitForExit(manager->pidfd, 0, &exited);
  if (code != QST_OK) {
    return code;
  }
  if (exited != 0) {
    (void)fprintf(stderr,
                  "taosQueryTest: manager exited during active run role=%s "
                  "generation=%u pid=%d\n",
                  qstRoleName(manager->role), manager->generation,
                  (int32_t)manager->pid);
    return QST_E_RESOURCE;
  }
  return QST_OK;
}

/* Confirms main's idle persistence-consumer state at a bounded cadence. */
static EQstCode qstMainConfirmPersistenceFlow(
    SQstShmMapping *mapping, uint32_t main_generation, uint64_t now_ns,
    uint64_t *inout_next_confirmation_ns) {
  const uint64_t confirmation_interval_ns =
      (uint64_t)QST_MAIN_PERSIST_FLOW_CONFIRM_MILLISECONDS * UINT64_C(1000000);
  EQstCode code = QST_OK;

  if (NULL == mapping || main_generation == 0 || now_ns == 0 ||
      NULL == inout_next_confirmation_ns || confirmation_interval_ns == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (*inout_next_confirmation_ns != 0 &&
      now_ns < *inout_next_confirmation_ns) {
    return QST_OK;
  }
  if (now_ns > UINT64_MAX - confirmation_interval_ns) {
    return QST_E_RESOURCE;
  }
  code = qstConfirmQueueFlow(mapping, QST_ROLE_MAIN, main_generation,
                             QST_FLOW_STAGE_IDLE, QST_QUEUE_PERSIST, 0, 0);
  if (code != QST_OK) {
    return code;
  }
  *inout_next_confirmation_ns = now_ns + confirmation_interval_ns;
  return QST_OK;
}

/* Drains persistence while a backpressured generator remains eligible to run. */
static EQstCode qstMainWaitForGeneratorAndDrain(
    SQstShmMapping *mapping, uint32_t main_generation,
    SQstManagerChild *generator, const SQstManagerChild *exec_manager,
    const SQstManagerChild *judge_manager) {
  struct pollfd event = {0};
  uint64_t next_manager_health_scan_ns = 0;
  uint64_t next_generator_health_scan_ns = 0;
  uint64_t next_persist_flow_confirmation_ns = 0;

  if (NULL == mapping || main_generation == 0 || NULL == generator ||
      NULL == exec_manager || NULL == judge_manager || generator->generation == 0 ||
      generator->pidfd < 0 || exec_manager->generation == 0 ||
      exec_manager->pidfd < 0 || judge_manager->generation == 0 ||
      judge_manager->pidfd < 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  event.fd = mapping->queue_eventfd[QST_QUEUE_PERSIST - 1];
  event.events = POLLIN;
  for (;;) {
    int32_t exited = 0;
    int32_t queue_empty = 0;
    uint32_t stop_generation = 0;
    uint32_t exit_state = 0;
    EQstCode code = qst_process_get_stop_generation(
        mapping, QST_ROLE_MAIN, main_generation, &stop_generation,
        &exit_state);

    qstMainStageProfileObserve(mapping);

    if (code != QST_OK) {
      return code;
    }
    if (exit_state == QST_PROCESS_STOP_REQUESTED && stop_generation != 0) {
      return QST_E_STOPPING;
    }
    if (exit_state != QST_PROCESS_RUNNING) {
      return QST_E_STALE_OWNER;
    }
    {
      const uint64_t now_ns = qstMonotonicNowNs();

      if (now_ns == 0) {
        return QST_E_RESOURCE;
      }
      if (next_manager_health_scan_ns == 0 ||
          now_ns >= next_manager_health_scan_ns) {
        if (now_ns > UINT64_MAX -
                         (uint64_t)QST_PAIR_HEALTH_SCAN_MILLISECONDS *
                             UINT64_C(1000000)) {
          return QST_E_RESOURCE;
        }
        next_manager_health_scan_ns =
            now_ns + (uint64_t)QST_PAIR_HEALTH_SCAN_MILLISECONDS *
                         UINT64_C(1000000);
        code = qstMainCheckManagerHealth(exec_manager);
        if (code == QST_OK) {
          code = qstMainCheckManagerHealth(judge_manager);
        }
        if (code != QST_OK) {
          return code;
        }
      }
      if (next_generator_health_scan_ns == 0 ||
          now_ns >= next_generator_health_scan_ns) {
        if (now_ns > UINT64_MAX -
                         (uint64_t)QST_PAIR_HEALTH_SCAN_MILLISECONDS *
                             UINT64_C(1000000)) {
          return QST_E_RESOURCE;
        }
        next_generator_health_scan_ns =
            now_ns + (uint64_t)QST_PAIR_HEALTH_SCAN_MILLISECONDS *
                         UINT64_C(1000000);
        code = qstWaitForExit(generator->pidfd, 0, &exited);
        if (code != QST_OK) {
          return code;
        }
        if (exited != 0) {
          /* The generator is a zombie at this point, so /proc contains its
           * final jiffies. Capture that value before reaping removes it. */
          qstMainCpuEfficiencyObserve(mapping, 1);
          return qstReapManager(mapping, main_generation, generator);
        }
      }
      code = qstMainConfirmPersistenceFlow(
          mapping, main_generation, now_ns, &next_persist_flow_confirmation_ns);
      if (code != QST_OK) {
        return code;
      }
    }
    code = qstMainDrainOnePersistence(mapping, main_generation);

    if (code != QST_OK && code != QST_E_QUEUE_EMPTY) {
      return code;
    }
    queue_empty = code == QST_E_QUEUE_EMPTY;
    /* A persistence message already represents progress. The generator pidfd
     * is checked above on the same bounded cadence as other manager health;
     * polling it for every durable row turns a sustained run into a syscall
     * hot path. When no row is available, the eventfd wait below blocks until
     * either new persistence work or the next regular health check. */
    if (queue_empty != 0) {
      const uint64_t now_ns = qstMonotonicNowNs();

      if (now_ns == 0) {
        return QST_E_RESOURCE;
      }
      next_persist_flow_confirmation_ns = 0;
      code = qstMainConfirmPersistenceFlow(
          mapping, main_generation, now_ns, &next_persist_flow_confirmation_ns);
      if (code != QST_OK) {
        return code;
      }
      if (qstPollQueueEvent(&event, 20) != QST_OK) {
        return QST_E_RESOURCE;
      }
    }
  }
}

static const char *qstMainModeName(uint32_t mode) {
  switch (mode) {
    case QST_MODE_FULL:
      return "full";
    case QST_MODE_RANDOM:
      return "random";
    default:
      return NULL;
  }
}

static const char *qstMainFocusName(uint32_t focus) {
  switch (focus) {
    case QST_FOCUS_COVERAGE:
      return "coverage";
    case QST_FOCUS_CRASH:
      return "crash";
    case QST_FOCUS_CORRECTNESS:
      return "correctness";
    default:
      return NULL;
  }
}

static EQstCode qstMainTargetSet(uint32_t selection_bits, char *out_target_set,
                                 uint64_t target_set_bytes) {
  static const char *const kTargetNames[] = {"td", "mysql", "pg", "influx"};
  uint64_t used = 0;

  if (NULL == out_target_set || target_set_bytes == 0 || selection_bits == 0 ||
      (selection_bits & ~UINT32_C(0x0f)) != 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  out_target_set[0] = '\0';
  for (uint32_t index = 0; index < sizeof(kTargetNames) / sizeof(kTargetNames[0]);
       ++index) {
    const uint32_t bit = UINT32_C(1) << index;
    const uint64_t name_bytes = strlen(kTargetNames[index]);

    if ((selection_bits & bit) == 0) {
      continue;
    }
    if (used != 0) {
      if (used + 1U >= target_set_bytes) {
        return QST_E_RESOURCE;
      }
      out_target_set[used++] = ',';
    }
    if (name_bytes >= target_set_bytes - used) {
      return QST_E_RESOURCE;
    }
    memcpy(out_target_set + used, kTargetNames[index], name_bytes);
    used += name_bytes;
    out_target_set[used] = '\0';
  }
  return used == 0 ? QST_E_INVALID_ARGUMENT : QST_OK;
}

static void qstMainDigestHex(SQstDigest256 digest, char out_hex[65]) {
  static const char kHex[] = "0123456789abcdef";

  for (uint32_t index = 0; index < sizeof(digest.bytes); ++index) {
    out_hex[index * 2U] = kHex[digest.bytes[index] >> 4U];
    out_hex[index * 2U + 1U] = kHex[digest.bytes[index] & 0x0fU];
  }
  out_hex[64] = '\0';
}

static EQstCode qstMainNextRecordSeq(SQstShmMapping *mapping,
                                     uint64_t *out_record_seq) {
  uint64_t record_seq = 0;

  if (NULL == mapping || NULL == mapping->header || NULL == out_record_seq) {
    return QST_E_INVALID_ARGUMENT;
  }
  record_seq = atomic_fetch_add_explicit(&mapping->header->next_record_seq, 1,
                                         memory_order_acq_rel);
  if (record_seq == 0) {
    record_seq = atomic_fetch_add_explicit(&mapping->header->next_record_seq, 1,
                                           memory_order_acq_rel);
  }
  if (record_seq == 0) {
    return QST_E_RESOURCE;
  }
  *out_record_seq = record_seq;
  return QST_OK;
}

static EQstCode qstMainConfigureResultStore(
    const SQstRunConfig *config, SQstResultStore *out_store,
    SQstPendingStore *out_pending_store) {
  char pending_dir[4096] = {0};
  char quarantine_dir[4096] = {0};

  if (NULL == config || NULL == out_store || NULL == out_pending_store ||
      config->run_id == 0 || NULL == config->result_dir ||
      qstMakePath(pending_dir, sizeof(pending_dir), config->result_dir,
                  "pending-results") != QST_OK ||
      qstMakePath(quarantine_dir, sizeof(quarantine_dir), pending_dir,
                  "quarantine") != QST_OK ||
      qstMkdir(pending_dir) != QST_OK) {
    return QST_E_INVALID_ARGUMENT;
  }
  memset(out_store, 0, sizeof(*out_store));
  memset(out_pending_store, 0, sizeof(*out_pending_store));
  out_pending_store->run_id = config->run_id;
  out_pending_store->pending_dir = pending_dir;
  out_pending_store->quarantine_dir = quarantine_dir;
  out_pending_store->quota_bytes = config->pending_quota_bytes;
  out_store->run_id = config->run_id;
  if (gQstOverflowContentPath[0] != '\0') {
    memcpy(out_store->overflow_path, gQstOverflowContentPath,
           strlen(gQstOverflowContentPath) + 1U);
  }
  memcpy(out_store->pending_dir, pending_dir, strlen(pending_dir) + 1U);
  memcpy(out_store->quarantine_dir, quarantine_dir, strlen(quarantine_dir) + 1U);
  out_pending_store->pending_dir = out_store->pending_dir;
  out_pending_store->quarantine_dir = out_store->quarantine_dir;
  return QST_OK;
}

static EQstCode qstMainOpenResultStore(const SQstRunConfig *config,
                                       SQstResultStore *out_store,
                                       SQstPendingStore *out_pending_store) {
  SQstResultStoreConfig store_config = {0};
  char pending_dir[4096] = {0};
  char quarantine_dir[4096] = {0};
  char overflow_path[4096] = {0};
  EQstCode code = QST_OK;

  if (qstMainConfigureResultStore(config, out_store, out_pending_store) != QST_OK) {
    return QST_E_INVALID_ARGUMENT;
  }
  store_config.run_id = config->run_id;
  store_config.endpoint = getenv("QST_RESULT_STORE_ENDPOINT");
  store_config.config_dir = getenv("QST_RESULT_STORE_CONFIG_DIR");
  store_config.user = getenv("QST_RESULT_STORE_USER");
  store_config.password = getenv("QST_RESULT_STORE_PASSWORD");
  memcpy(pending_dir, out_store->pending_dir, strlen(out_store->pending_dir) + 1U);
  memcpy(quarantine_dir, out_store->quarantine_dir,
         strlen(out_store->quarantine_dir) + 1U);
  if (out_store->overflow_path[0] != '\0') {
    memcpy(overflow_path, out_store->overflow_path,
           strlen(out_store->overflow_path) + 1U);
  }
  store_config.pending_dir = pending_dir;
  store_config.quarantine_dir = quarantine_dir;
  store_config.overflow_path = overflow_path[0] == '\0' ? NULL : overflow_path;
  code = qst_result_store_open(&store_config, out_store);
  if (code != QST_OK) {
    /* Native unavailability is handled by the pending-results fallback. */
    out_store->native_taos = NULL;
    if (qst_result_store_configure_connection(&store_config, out_store) != QST_OK) {
      return code;
    }
    memcpy(out_store->pending_dir, store_config.pending_dir,
           strlen(store_config.pending_dir) + 1U);
    memcpy(out_store->quarantine_dir, store_config.quarantine_dir,
           strlen(store_config.quarantine_dir) + 1U);
    if (NULL != store_config.overflow_path) {
      memcpy(out_store->overflow_path, store_config.overflow_path,
             strlen(store_config.overflow_path) + 1U);
    }
    out_pending_store->pending_dir = out_store->pending_dir;
    out_pending_store->quarantine_dir = out_store->quarantine_dir;
    return QST_OK;
  }
  /* qst_result_store_open initializes its output object after native open;
   * restore the stable local fallback paths that were prepared above. */
  memcpy(out_store->pending_dir, store_config.pending_dir,
         strlen(store_config.pending_dir) + 1U);
  memcpy(out_store->quarantine_dir, store_config.quarantine_dir,
         strlen(store_config.quarantine_dir) + 1U);
  if (NULL != store_config.overflow_path) {
    memcpy(out_store->overflow_path, store_config.overflow_path,
           strlen(store_config.overflow_path) + 1U);
  }
  out_pending_store->pending_dir = out_store->pending_dir;
  out_pending_store->quarantine_dir = out_store->quarantine_dir;
  return QST_OK;
}

typedef enum {
  QST_MAIN_STORE_RUN = 1,
  QST_MAIN_STORE_DATASET = 2,
  QST_MAIN_STORE_PREPARE = 3,
  QST_MAIN_STORE_READINESS = 4,
} EQstMainStoreOperation;

typedef struct {
  int32_t code;
  uint32_t durable_source;
  char readiness[1024];
} SQstMainStoreReply;

static EQstCode qstMainWritePipe(int32_t fd, const void *data,
                                 uint64_t data_bytes) {
  const uint8_t *cursor = (const uint8_t *)data;
  uint64_t written_bytes = 0;

  if (fd < 0 || NULL == data || data_bytes == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  while (written_bytes < data_bytes) {
    const ssize_t written = write(fd, cursor + written_bytes,
                                  (size_t)(data_bytes - written_bytes));
    if (written < 0 && errno == EINTR) {
      continue;
    }
    if (written <= 0) {
      return QST_E_RESOURCE;
    }
    written_bytes += (uint64_t)written;
  }
  return QST_OK;
}

static EQstCode qstMainReadPipe(int32_t fd, void *data, uint64_t data_bytes) {
  uint8_t *cursor = (uint8_t *)data;
  uint64_t read_bytes = 0;

  if (fd < 0 || NULL == data || data_bytes == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  while (read_bytes < data_bytes) {
    const ssize_t bytes = read(fd, cursor + read_bytes,
                               (size_t)(data_bytes - read_bytes));
    if (bytes < 0 && errno == EINTR) {
      continue;
    }
    if (bytes <= 0) {
      return QST_E_RESOURCE;
    }
    read_bytes += (uint64_t)bytes;
  }
  return QST_OK;
}

/* Runs one native result-store operation in a process that has no inherited
 * connector state. The parent remains native-free for all later role forks. */
static EQstCode qstMainStoreOperation(
    SQstShmMapping *mapping, const SQstRunConfig *config,
    EQstMainStoreOperation operation, const SQstRunRecord *run_record,
    const SQstDatasetRecord *dataset_record, uint32_t target_selection_bits,
    SQstDigest256 dataset_digest, uint64_t record_seq,
    uint32_t *out_durable_source, char *out_readiness,
    uint64_t readiness_bytes) {
  int32_t result_pipe[2] = {-1, -1};
  SQstMainStoreReply reply = {QST_E_RESOURCE, 0, {0}};
  pid_t child = -1;
  int32_t status = 0;
  uint64_t operation_deadline_ns = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == config || config->run_id == 0 ||
      operation < QST_MAIN_STORE_RUN || operation > QST_MAIN_STORE_READINESS ||
      (operation == QST_MAIN_STORE_RUN &&
       (NULL == run_record || record_seq == 0)) ||
      (operation == QST_MAIN_STORE_DATASET &&
       (NULL == dataset_record || record_seq == 0)) ||
      (operation == QST_MAIN_STORE_PREPARE && target_selection_bits == 0) ||
      (operation == QST_MAIN_STORE_READINESS &&
       (NULL == out_readiness || readiness_bytes == 0))) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (pipe(result_pipe) != 0) {
    return QST_E_RESOURCE;
  }
  child = fork();
  if (child < 0) {
    (void)close(result_pipe[0]);
    (void)close(result_pipe[1]);
    return QST_E_RESOURCE;
  }
  if (child == 0) {
    SQstResultStore store = {0};
    SQstPendingStore pending_store = {0};
    uint64_t replayed_count = 0;

    (void)close(result_pipe[0]);
    code = qstMainOpenResultStore(config, &store, &pending_store);
    if (code == QST_OK) {
      const EQstCode replay_code = qst_pending_replay(
          mapping, &store, config->run_id, &replayed_count);

      if (replay_code != QST_OK) {
        (void)fprintf(stderr,
                      "taosQueryTest: pending replay deferred run_id=%llu code=%d\n",
                      (unsigned long long)config->run_id, replay_code);
      } else if (replayed_count != 0) {
        (void)fprintf(stderr,
                      "taosQueryTest: pending replay completed run_id=%llu count=%llu\n",
                      (unsigned long long)config->run_id,
                      (unsigned long long)replayed_count);
      }
    }
    if (code == QST_OK && operation == QST_MAIN_STORE_RUN) {
      code = qst_result_store_write_run_record(
          mapping, &store, &pending_store, run_record, record_seq,
          &reply.durable_source);
    } else if (code == QST_OK && operation == QST_MAIN_STORE_DATASET) {
      code = qst_result_store_write_dataset_record(
          mapping, &store, &pending_store, dataset_record, record_seq,
          &reply.durable_source);
    } else if (code == QST_OK && operation == QST_MAIN_STORE_PREPARE) {
      code = qst_result_store_prepare_run(
          &store, target_selection_bits, dataset_digest,
          mapping->header->judge_worker_count);
      if (code != QST_OK) {
        (void)fprintf(stderr,
                      "taosQueryTest: result table preparation unavailable code=%d; continuing with pending-results\n",
                      code);
        code = QST_OK;
      }
    } else if (code == QST_OK) {
      code = qst_result_store_readiness_json(&store, reply.readiness,
                                             sizeof(reply.readiness));
    }
    if (code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: result-store operation failed run_id=%llu "
                    "operation=%u code=%d\n",
                    (unsigned long long)config->run_id, (unsigned int)operation,
                    code);
    }
    if (qst_result_store_close(&store) != QST_OK && code == QST_OK) {
      code = QST_E_RESULT_STORE;
    }
    reply.code = code;
    (void)qstMainWritePipe(result_pipe[1], &reply, sizeof(reply));
    (void)close(result_pipe[1]);
    _exit(code == QST_OK ? 0 : 1);
  }
  (void)close(result_pipe[1]);
  operation_deadline_ns = qstMillisecondsToDeadlineNs(QST_FLOW_LEASE_MILLISECONDS);
  if (operation_deadline_ns == 0) {
    (void)close(result_pipe[0]);
    (void)kill(child, SIGKILL);
    (void)waitpid(child, NULL, 0);
    return QST_E_RESOURCE;
  }
  for (;;) {
    struct pollfd event = {0};
    uint32_t main_generation = 0;
    int32_t poll_result = 0;

    event.fd = result_pipe[0];
    event.events = POLLIN | POLLHUP;
    poll_result = poll(&event, 1, QST_MAIN_FLOW_POLL_MILLISECONDS);
    if (poll_result > 0) {
      code = qstMainReadPipe(result_pipe[0], &reply, sizeof(reply));
      break;
    }
    if (poll_result < 0) {
      if (errno == EINTR) {
        continue;
      }
      code = QST_E_RESOURCE;
      break;
    }
    if (qstMillisecondsToDeadlineNs(1) == 0 ||
        qstMillisecondsToDeadlineNs(1) > operation_deadline_ns) {
      code = QST_E_RESULT_STORE;
      (void)kill(child, SIGKILL);
      break;
    }
    code = qst_process_find_generation(mapping, QST_ROLE_MAIN,
                                       (int32_t)getpid(), &main_generation);
    if (code != QST_OK) {
      break;
    }
    code = qstConfirmQueueFlow(mapping, QST_ROLE_MAIN, main_generation,
                               QST_FLOW_STAGE_STARTUP, QST_QUEUE_CONTROL, 0, 0);
    if (code != QST_OK) {
      break;
    }
  }
  (void)close(result_pipe[0]);
  while (waitpid(child, &status, 0) < 0) {
    if (errno != EINTR) {
      return QST_E_RESOURCE;
    }
  }
  if (code != QST_OK) {
    return code;
  }
  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
    return reply.code != QST_OK ? (EQstCode)reply.code : QST_E_RESULT_STORE;
  }
  if (reply.code != QST_OK) {
    return (EQstCode)reply.code;
  }
  if (NULL != out_durable_source) {
    *out_durable_source = reply.durable_source;
  }
  if (operation == QST_MAIN_STORE_READINESS) {
    if (reply.readiness[0] == '\0' ||
        strlen(reply.readiness) >= readiness_bytes) {
      return QST_E_RESOURCE;
    }
    memcpy(out_readiness, reply.readiness, strlen(reply.readiness) + 1U);
  }
  return QST_OK;
}

typedef struct {
  int32_t code;
  SQstDatasetInfo dataset;
} SQstDatasetReply;

static EQstCode qstMainPrepareDataset(SQstShmMapping *mapping,
                                      const SQstRunConfig *config,
                                      uint32_t generation,
                                      SQstDatasetInfo *out_dataset) {
  int32_t result_pipe[2] = {-1, -1};
  SQstDatasetReply reply = {QST_E_RESOURCE, {0}};
  pid_t child = -1;
  int32_t status = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == config || generation == 0 ||
      NULL == out_dataset) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (pipe(result_pipe) != 0) {
    return QST_E_RESOURCE;
  }
  child = fork();
  if (child < 0) {
    (void)close(result_pipe[0]);
    (void)close(result_pipe[1]);
    return QST_E_RESOURCE;
  }
  if (child == 0) {
    (void)close(result_pipe[0]);
    reply.code = qst_dataset_prepare_for_generation(mapping, config, generation,
                                                    &reply.dataset);
    (void)qstMainWritePipe(result_pipe[1], &reply, sizeof(reply));
    (void)close(result_pipe[1]);
    _exit(reply.code == QST_OK ? 0 : 1);
  }
  (void)close(result_pipe[1]);
  code = qstMainReadPipe(result_pipe[0], &reply, sizeof(reply));
  (void)close(result_pipe[0]);
  while (waitpid(child, &status, 0) < 0) {
    if (errno != EINTR) {
      return QST_E_RESOURCE;
    }
  }
  if (code != QST_OK) {
    return code;
  }
  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
    return reply.code != QST_OK ? (EQstCode)reply.code : QST_E_RESOURCE;
  }
  if (reply.code != QST_OK) {
    return (EQstCode)reply.code;
  }
  *out_dataset = reply.dataset;
  return QST_OK;
}

static EQstCode qstMainCleanupDataset(SQstShmMapping *mapping,
                                      const SQstRunConfig *config,
                                      SQstDatasetInfo *inout_dataset) {
  int32_t result_pipe[2] = {-1, -1};
  int32_t child_code = QST_E_RESOURCE;
  pid_t child = -1;
  int32_t status = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == config || NULL == inout_dataset ||
      inout_dataset->run_id == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (pipe(result_pipe) != 0) {
    return QST_E_RESOURCE;
  }
  child = fork();
  if (child < 0) {
    (void)close(result_pipe[0]);
    (void)close(result_pipe[1]);
    return QST_E_RESOURCE;
  }
  if (child == 0) {
    (void)close(result_pipe[0]);
    child_code = qst_dataset_cleanup(mapping, config, inout_dataset);
    (void)qstMainWritePipe(result_pipe[1], &child_code, sizeof(child_code));
    (void)close(result_pipe[1]);
    _exit(child_code == QST_OK ? 0 : 1);
  }
  (void)close(result_pipe[1]);
  code = qstMainReadPipe(result_pipe[0], &child_code, sizeof(child_code));
  (void)close(result_pipe[0]);
  while (waitpid(child, &status, 0) < 0) {
    if (errno != EINTR) {
      return QST_E_RESOURCE;
    }
  }
  if (code != QST_OK) {
    return code;
  }
  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
    return child_code != QST_OK ? (EQstCode)child_code : QST_E_RESOURCE;
  }
  if (child_code != QST_OK) {
    return (EQstCode)child_code;
  }
  memset(inout_dataset, 0, sizeof(*inout_dataset));
  return QST_OK;
}

static void qstMainCollectRunStats(const SQstShmMapping *mapping,
                                   SQstRunRecord *out_record) {
  const SQstRegion *region = NULL;
  SQstTaskEntry *entries = NULL;

  if (NULL == mapping || NULL == mapping->header || NULL == mapping->base ||
      NULL == out_record) {
    return;
  }
  region = &mapping->header->task_region;
  if (region->element_bytes != sizeof(SQstTaskEntry) ||
      region->region_offset > mapping->bytes ||
      region->region_bytes > mapping->bytes - region->region_offset) {
    return;
  }
  entries = (SQstTaskEntry *)((uint8_t *)mapping->base + region->region_offset);
  (void)entries;
  out_record->executed_statement_count = atomic_load_explicit(
      &mapping->header->persisted_task_count, memory_order_acquire);
  out_record->succeeded_statement_count = atomic_load_explicit(
      &mapping->header->execution_success_count, memory_order_acquire);
  out_record->execution_error_statement_count = atomic_load_explicit(
      &mapping->header->execution_error_count, memory_order_acquire);
  out_record->timeout_statement_count = atomic_load_explicit(
      &mapping->header->execution_timeout_count, memory_order_acquire);
  out_record->cancelled_statement_count = atomic_load_explicit(
      &mapping->header->execution_cancelled_count, memory_order_acquire);
  out_record->passed_statement_count = atomic_load_explicit(
      &mapping->header->verdict_passed_count, memory_order_acquire);
  out_record->failed_statement_count = atomic_load_explicit(
      &mapping->header->verdict_failed_count, memory_order_acquire);
  out_record->unverified_statement_count = atomic_load_explicit(
      &mapping->header->verdict_unverified_count, memory_order_acquire);
}

static EQstCode qstMainReadTrimmedFile(const char *path, char **out_text) {
  struct stat status = {0};
  char *text = NULL;
  uint64_t read_bytes = 0;
  uint64_t first = 0;
  uint64_t last = 0;
  int32_t fd = -1;

  if (NULL == path || NULL == out_text || stat(path, &status) != 0 ||
      !S_ISREG(status.st_mode) || status.st_size <= 0 ||
      (uint64_t)status.st_size >= SIZE_MAX) {
    return QST_E_PENDING_IO;
  }
  *out_text = NULL;
  text = (char *)calloc((size_t)status.st_size + 1U, 1);
  if (NULL == text) {
    return QST_E_RESOURCE;
  }
  fd = open(path, O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    free(text);
    return QST_E_PENDING_IO;
  }
  while (read_bytes < (uint64_t)status.st_size) {
    const ssize_t bytes = read(fd, text + read_bytes,
                               (size_t)((uint64_t)status.st_size - read_bytes));

    if (bytes < 0 && errno == EINTR) {
      continue;
    }
    if (bytes <= 0) {
      (void)close(fd);
      free(text);
      return QST_E_PENDING_IO;
    }
    read_bytes += (uint64_t)bytes;
  }
  if (close(fd) != 0 || memchr(text, '\0', read_bytes) != NULL) {
    free(text);
    return QST_E_PENDING_IO;
  }
  while (first < read_bytes && (text[first] == ' ' || text[first] == '\t' ||
                                text[first] == '\r' || text[first] == '\n')) {
    ++first;
  }
  last = read_bytes;
  while (last > first && (text[last - 1U] == ' ' || text[last - 1U] == '\t' ||
                           text[last - 1U] == '\r' || text[last - 1U] == '\n')) {
    --last;
  }
  if (first == last) {
    free(text);
    return QST_E_PENDING_IO;
  }
  memmove(text, text + first, (size_t)(last - first));
  text[last - first] = '\0';
  *out_text = text;
  return QST_OK;
}

/* Returns the durable fixture configuration. Direct callers without a tool
 * root predate run-scoped assets, so retain their documented local fallback. */
static EQstCode qstMainLoadDatasetConfig(const SQstRunConfig *config,
                                         char **out_config_json) {
  static const char kDefaultConfig[] = "{\"fixture\":\"qst-local-fixture-v1\"}";
  char dataset_config_path[4096] = {0};

  if (NULL == config || NULL == config->result_dir || NULL == out_config_json ||
      qstMakePath(dataset_config_path, sizeof(dataset_config_path),
                  config->result_dir, "dataset/taosbenchmark.json") != QST_OK) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_config_json = NULL;
  if (access(dataset_config_path, F_OK) != 0) {
    if (errno != ENOENT || NULL != config->tool_root) {
      return QST_E_PENDING_IO;
    }
    *out_config_json = strdup(kDefaultConfig);
    return NULL == *out_config_json ? QST_E_RESOURCE : QST_OK;
  }
  return qstMainReadTrimmedFile(dataset_config_path, out_config_json);
}

/* Reads readiness collected by the dataset child without loading a native
 * connector in the main process before it forks result-store children. */
static void qstMainLoadDatasetReadiness(const SQstRunConfig *config,
                                        char *out_readiness,
                                        uint64_t readiness_bytes) {
  char readiness_path[4096] = {0};
  char *readiness = NULL;

  if (NULL == config || NULL == config->result_dir || NULL == out_readiness ||
      readiness_bytes == 0 ||
      qstMakePath(readiness_path, sizeof(readiness_path), config->result_dir,
                  "dataset/tdengine-readiness.json") != QST_OK ||
      qstMainReadTrimmedFile(readiness_path, &readiness) != QST_OK) {
    return;
  }
  if (strlen(readiness) < readiness_bytes) {
    memcpy(out_readiness, readiness, strlen(readiness) + 1U);
  }
  free(readiness);
}

/* Publishes one immutable startup fact before it is referenced by runs or
 * reports. The filename is fixed by callers and cannot escape dataset/. */
static EQstCode qstMainWriteStartupSnapshot(const SQstRunConfig *config,
                                            const char *name,
                                            const char *json) {
  char dataset_dir[4096] = {0};
  char path[4096] = {0};
  char temporary[4096] = {0};

  if (NULL == config || NULL == config->result_dir || NULL == name ||
      name[0] == '\0' || strchr(name, '/') != NULL || NULL == json ||
      json[0] != '{' || json[strlen(json) - 1U] != '}' ||
      qstMakePath(dataset_dir, sizeof(dataset_dir), config->result_dir,
                  "dataset") != QST_OK ||
      qstMakePath(path, sizeof(path), dataset_dir, name) != QST_OK ||
      snprintf(temporary, sizeof(temporary), "%s.tmp", path) < 0 ||
      strlen(temporary) >= sizeof(temporary)) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (mkdir(dataset_dir, 0750) != 0 && errno != EEXIST) {
    return QST_E_RESOURCE;
  }
  if (qstWriteTextFile(temporary, json) != QST_OK) {
    (void)unlink(temporary);
    return QST_E_RESOURCE;
  }
  if (rename(temporary, path) != 0) {
    (void)unlink(temporary);
    return QST_E_RESOURCE;
  }
  return QST_OK;
}

/* Freezes the local and result-store TDengine identities as one startup
 * artifact. The header digest therefore identifies the exact pair of server
 * facts rather than an end-of-run probe. */
static EQstCode qstMainFreezeTdengineStartup(
    SQstShmMapping *mapping, const SQstRunConfig *config,
    const char *local_readiness, const char *result_store_readiness) {
  SQstDigest256 digest = {{0}};
  char snapshot[16384] = {0};
  char path[4096] = {0};
  int32_t written = 0;

  if (NULL == mapping || NULL == config || NULL == local_readiness ||
      NULL == result_store_readiness || local_readiness[0] != '{' ||
      result_store_readiness[0] != '{' ||
      local_readiness[strlen(local_readiness) - 1U] != '}' ||
      result_store_readiness[strlen(result_store_readiness) - 1U] != '}') {
    return QST_E_INVALID_ARGUMENT;
  }
  written = snprintf(snapshot, sizeof(snapshot),
                     "{\"test_local\":%s,\"result_store\":%s}",
                     local_readiness, result_store_readiness);
  if (written < 0 || (uint64_t)written >= sizeof(snapshot) ||
      qstMainWriteStartupSnapshot(config, "tdengine-startup.json", snapshot) !=
          QST_OK ||
      qstMakePath(path, sizeof(path), config->result_dir,
                  "dataset/tdengine-startup.json") != QST_OK ||
      qstDigestFile(path, &digest) != QST_OK) {
    return QST_E_RESOURCE;
  }
  return qst_shm_set_tdengine_version_digest(mapping, digest);
}

/* Loads the launcher-frozen resource sample for the final run record. */
static EQstCode qstMainLoadResourceProfile(const SQstRunConfig *config,
                                           char *out_profile,
                                           uint64_t profile_bytes) {
  char profile_path[4096] = {0};
  char *profile = NULL;
  int32_t written = 0;
  EQstCode code = QST_OK;

  if (NULL == config || NULL == config->result_dir || NULL == out_profile ||
      profile_bytes < 2U ||
      qstMakePath(profile_path, sizeof(profile_path), config->result_dir,
                  "resource-profile.json") != QST_OK) {
    return QST_E_INVALID_ARGUMENT;
  }
  out_profile[0] = '\0';
  code = qstMainReadTrimmedFile(profile_path, &profile);
  if (code == QST_OK) {
    const uint64_t profile_length = strlen(profile);

    if (profile_length + 1U > profile_bytes || profile[0] != '{' ||
        profile[profile_length - 1U] != '}') {
      free(profile);
      return QST_E_RESOURCE;
    }
    memcpy(out_profile, profile, profile_length + 1U);
    free(profile);
    return QST_OK;
  }
  if (code != QST_E_PENDING_IO) {
    return code;
  }
  /* Direct qst_main_run unit callers do not pass through the launcher. Their
   * configured capacities remain observable, while production always has the
   * launcher-written sample above. */
  written = snprintf(
      out_profile, (size_t)profile_bytes,
      "{\"captured_at_ns\":0,\"source_flags\":0,"
      "\"shared_memory_bytes\":%llu,\"execution_pair_count\":%u,"
      "\"pair_async_window\":%u,\"judge_worker_count\":%u,"
      "\"queue_capacity\":%u,"
      "\"block_bytes\":%u,\"block_count\":%u}",
      (unsigned long long)config->shm_bytes, config->execution_pair_count,
      config->pair_async_window, config->judge_worker_count,
      config->queue_capacity, config->block_bytes,
      config->block_count);
  return written > 0 && (uint64_t)written < profile_bytes ? QST_OK :
                                                         QST_E_RESOURCE;
}

/* Builds every runs.parallel_test_info image from the same frozen inputs.
 * Pair facts naturally evolve from an empty list before manager startup to
 * reaped runtime records at the terminal stage. */
static EQstCode qstMainBuildParallelInfo(
    const SQstShmMapping *mapping, const SQstRunConfig *config,
    const char *fact_stage, const char *dataset_state,
    const char *provider_readiness, const char *tdengine_readiness,
    const char *result_store_readiness, int32_t final_statistics,
    char *out_json, uint64_t out_bytes) {
  char pair_info[QST_PARALLEL_INFO_BUFFER_BYTES] = {0};
  char resource_profile[4096] = {0};
  char resource_profile_digest[65] = {0};
  char dependency_lock_digest[65] = {0};
  char tdengine_version_digest[65] = {0};
  int32_t written = 0;

  if (NULL == mapping || NULL == mapping->header || NULL == config ||
      NULL == fact_stage || NULL == dataset_state || NULL == out_json ||
      out_bytes == 0 || (final_statistics != 0 && final_statistics != 1) ||
      qstMainBuildPairInfo(config, pair_info, sizeof(pair_info)) != QST_OK ||
      qstMainLoadResourceProfile(config, resource_profile,
                                 sizeof(resource_profile)) != QST_OK) {
    return QST_E_INVALID_ARGUMENT;
  }
  qstMainDigestHex(mapping->header->resource_profile_digest,
                   resource_profile_digest);
  qstMainDigestHex(mapping->header->dependency_lock_digest,
                   dependency_lock_digest);
  qstMainDigestHex(mapping->header->tdengine_version_digest,
                   tdengine_version_digest);
  written = snprintf(
      out_json, (size_t)out_bytes,
      "{%s,\"fact_stage\":\"%s\",\"dataset_state\":\"%s\","
      "\"resource_profile\":%s,\"startup_digests\":{"
      "\"resource_profile_sha256\":\"%s\","
      "\"dependency_lock_sha256\":\"%s\","
      "\"tdengine_version_sha256\":\"%s\"},\"tdengine_versions\":{"
      "\"test_local\":%s,\"result_store\":%s},"
      "\"provider_versions\":%s,\"final_statistics\":%s}",
      pair_info, fact_stage, dataset_state, resource_profile,
      resource_profile_digest, dependency_lock_digest, tdengine_version_digest,
      tdengine_readiness == NULL || tdengine_readiness[0] == '\0' ? "null" :
          tdengine_readiness,
      result_store_readiness == NULL || result_store_readiness[0] == '\0' ?
          "null" : result_store_readiness,
      provider_readiness == NULL || provider_readiness[0] == '\0' ? "null" :
          provider_readiness,
      final_statistics != 0 ? "true" : "false");
  return written >= 0 && (uint64_t)written < out_bytes ? QST_OK :
                                                        QST_E_RESOURCE;
}

/* Runs the SQL generation evidence path without creating result-store records
 * or starting any execution, judge, or pair process. */
static EQstCode qstMainRunGenerateOnly(SQstShmMapping *mapping,
                                       const SQstRunConfig *config,
                                       uint32_t generation) {
  SQstDatasetInfo dataset = {0};
  uint64_t generated_sql_count = 0;
  uint32_t stop_reason = QST_STOP_NONE;
  char report[512] = {0};
  int32_t written = 0;
  EQstCode code = QST_OK;

  if (mapping == NULL || mapping->header == NULL || config == NULL ||
      generation == 0 || config->generate_output_path == NULL ||
      config->generate_output_path[0] == '\0') {
    return QST_E_INVALID_ARGUMENT;
  }
  atomic_store_explicit(&mapping->header->run_state, QST_RUN_RUNNING,
                        memory_order_release);
  code = qstMainPrepareDataset(mapping, config, generation, &dataset);
  if (code == QST_OK) {
    code = qst_process_record_business(mapping, QST_ROLE_MAIN, generation, 0, 0);
  }
  if (code == QST_OK) {
    code = qst_generator_state_publish_dataset(
        mapping, generation, dataset.ds_fingerprint,
        dataset.fixture_manifest_digest, dataset.object_directory_ref,
        dataset.fixture_manifest_ref);
  }
  if (code == QST_OK) {
    code = qst_generator_write_only(mapping, generation,
                                    config->generate_output_path,
                                    &generated_sql_count);
  }
  if (dataset.run_id != 0) {
    const EQstCode cleanup_code = qstMainCleanupDataset(mapping, config, &dataset);

    if (code == QST_OK && cleanup_code != QST_OK) {
      code = cleanup_code;
    }
  }
  if (code == QST_OK) {
    stop_reason = atomic_load_explicit(&mapping->header->stop_reason,
                                       memory_order_acquire);
    if (stop_reason != QST_STOP_FULL_COMPLETE &&
        stop_reason != QST_STOP_STATEMENT_LIMIT &&
        stop_reason != QST_STOP_DURATION_LIMIT) {
      code = QST_E_SHM_CORRUPT;
    }
  }
  if (code == QST_OK) {
    written = snprintf(
        report, sizeof(report),
        "{\"format\":\"taosQueryTest-generation-debug-v1\","
        "\"mode\":\"%s\",\"generated_sql_count\":%llu,"
        "\"generated_task_count\":0,\"execution_pairs_started\":0,"
        "\"result_store_ledger\":false}",
        config->mode == QST_MODE_FULL ? "full" : "random",
        (unsigned long long)generated_sql_count);
    if (written < 0 || (uint64_t)written >= sizeof(report)) {
      code = QST_E_RESOURCE;
    } else {
      code = qstMainWriteStartupSnapshot(config, "generation-debug.json", report);
    }
  }
  if (code != QST_OK) {
    atomic_store_explicit(&mapping->header->run_state, QST_RUN_FAILED,
                          memory_order_release);
    if (atomic_load_explicit(&mapping->header->stop_reason,
                             memory_order_acquire) == QST_STOP_NONE) {
      atomic_store_explicit(&mapping->header->stop_reason,
                            dataset.run_id == 0 ? QST_STOP_PREPARE_FAILED :
                                                  QST_STOP_INFRASTRUCTURE_FAILED,
                            memory_order_release);
    }
  } else {
    atomic_store_explicit(&mapping->header->run_state, QST_RUN_COMPLETED,
                          memory_order_release);
  }
  if (config->result_dir != NULL && qst_report_refresh(mapping, config) != QST_OK &&
      code == QST_OK) {
    code = QST_E_RESOURCE;
    atomic_store_explicit(&mapping->header->run_state, QST_RUN_FAILED,
                          memory_order_release);
  }
  return qst_process_mark_exited(mapping, QST_ROLE_MAIN, generation,
                                 code == QST_OK ? 0 : (int32_t)code) == QST_OK ?
             code : QST_E_STALE_OWNER;
}

EQstCode qst_main_run(SQstShmMapping *mapping, const SQstRunConfig *config) {
  SQstDatasetInfo dataset = {0};
  SQstRunRecord initial_run = {0};
  SQstRunRecord final_run = {0};
  SQstDatasetRecord dataset_record = {0};
  const SQstGeneratorCatalog *catalog = NULL;
  SQstGenerationState *generation_state = NULL;
  SQstManagerChild generator = {0};
  SQstManagerChild exec_manager = {0};
  SQstManagerChild judge_manager = {0};
  char target_set[64] = {0};
  char ds_fingerprint[65] = {0};
  char provider_readiness[8192] = {0};
  char tdengine_readiness[8192] = {0};
  char result_store_readiness[8192] = {0};
  char parallel_info[QST_PARALLEL_INFO_BUFFER_BYTES] = {0};
  char *dataset_config_json = NULL;
  const char *stop_reason = NULL;
  const char *error_message = NULL;
  uint64_t record_seq = 0;
  uint32_t durable_source = 0;
  int64_t begin_ts_ns = 0;
  uint32_t generation = 0;
  int32_t user_stop_requested = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == config || config->run_id == 0 ||
      config->pending_quota_bytes == 0 || config->log_rotation_bytes == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  free(gQstMainCpuEfficiency.processes);
  memset(&gQstMainCpuEfficiency, 0, sizeof(gQstMainCpuEfficiency));
  memset(&gQstMainStageProfile, 0, sizeof(gQstMainStageProfile));
  memset(&gQstMainPersistProfile, 0, sizeof(gQstMainPersistProfile));
  /* Steady-state timing only needs main-process boundaries. Keep detailed
   * per-task worker diagnostics opt-in through QST_PROFILE_PHASES so their
   * clock and atomic traffic does not perturb the measured pipeline. */
  gQstMainStageProfile.enabled = getenv("QST_PROFILE_PHASES") != NULL ||
                                 getenv("QST_PROFILE_STEADY") != NULL;
  gQstMainPersistProfile.enabled = getenv("QST_PROFILE_PHASES") != NULL;
  qstMainStageProfileObserve(mapping);
  code = qstSetProcessLogDirectory(config);
  if (code != QST_OK) {
    return code;
  }
  code = qst_process_find_generation(mapping, QST_ROLE_MAIN,
                                     (int32_t)getpid(), &generation);
  if (code != QST_OK || generation !=
                             atomic_load_explicit(&mapping->header->main_generation,
                                                  memory_order_acquire)) {
    return code != QST_OK ? code : QST_E_STALE_OWNER;
  }
  code = qstRedirectProcessStderr(QST_ROLE_MAIN, generation);
  if (code != QST_OK) {
    return code;
  }
  code = qstWriteProcessStartLog(QST_ROLE_MAIN, generation,
                                 QST_ROLE_LAUNCHER, 1);
  if (code != QST_OK) {
    return code;
  }
  qstTestMaybeDelayMainStartup();
  code = qst_sql_dimension_catalog(&catalog);
  if (code == QST_OK) {
    code = qst_generator_state_get(mapping, &generation_state);
    if (code == QST_E_SHM_CORRUPT &&
        atomic_load_explicit(&mapping->header->run_state,
                             memory_order_acquire) == QST_RUN_PREPARING) {
      code = qst_generator_state_initialize(
          mapping, catalog, config->mode, config->focus,
          config->target_selection_bits, config->random_seed,
          config->statement_limit, config->duration_limit_minutes);
    } else if (code == QST_OK &&
               (generation_state->catalog_version != catalog->version ||
                memcmp(generation_state->catalog_digest.bytes,
                       catalog->digest.bytes,
                       sizeof(catalog->digest.bytes)) != 0 ||
                generation_state->mode != config->mode ||
                generation_state->focus != config->focus ||
                generation_state->target_selection_bits !=
                    config->target_selection_bits ||
                generation_state->random_seed != config->random_seed ||
                generation_state->statement_limit != config->statement_limit ||
                generation_state->duration_limit_minutes !=
                    config->duration_limit_minutes ||
                generation_state->pending_quota_bytes !=
                    config->pending_quota_bytes ||
                generation_state->log_rotation_bytes !=
                    config->log_rotation_bytes)) {
      code = QST_E_SHM_CORRUPT;
    }
  }
  if (code != QST_OK) {
    return code;
  }
  code = qst_generator_state_set_random_builder_count(
      mapping, generation,
      config->random_builder_count_override == 0 ?
          QST_RANDOM_PIPELINE_BUILDER_COUNT :
          config->random_builder_count_override);
  if (code != QST_OK) {
    return code;
  }
  /* The launcher lease covers main initialization, including potentially slow
   * result-store recovery. Publish after the shared generator state is valid,
   * before any native result-store I/O can block the parent watchdog. */
  code = qstConfirmQueueFlow(mapping, QST_ROLE_MAIN, generation,
                             QST_FLOW_STAGE_STARTUP, QST_QUEUE_CONTROL, 0, 0);
  if (code != QST_OK) {
    return code;
  }
  if (config->generate_output_path != NULL) {
    return qstMainRunGenerateOnly(mapping, config, generation);
  }
  code = qstMainTargetSet(config->target_selection_bits, target_set,
                          sizeof(target_set));
  if (code == QST_OK) {
    begin_ts_ns = qstRealtimeNowNs();
    code = begin_ts_ns == 0 ? QST_E_RESOURCE : QST_OK;
  }
  if (code == QST_OK) {
    code = qstMainStoreOperation(
        mapping, config, QST_MAIN_STORE_READINESS, NULL, NULL, 0,
        (SQstDigest256){{0}}, 0, NULL, result_store_readiness,
        sizeof(result_store_readiness));
  }
  if (code == QST_OK) {
    code = qstMainWriteStartupSnapshot(config, "result-store-readiness.json",
                                       result_store_readiness);
  }
  if (code == QST_OK) {
    code = qstMainBuildParallelInfo(
        mapping, config, "initial", "unprepared", NULL, NULL,
        result_store_readiness, 0, parallel_info, sizeof(parallel_info));
  }
  if (code == QST_OK) {
    initial_run.begin_ts_ns = begin_ts_ns;
    initial_run.status = 0;
    initial_run.has_random_seed = config->mode == QST_MODE_RANDOM;
    initial_run.random_seed = config->random_seed;
    if (config->statement_limit > INT64_MAX) {
      code = QST_E_INVALID_ARGUMENT;
    }
    initial_run.planned_statement_count = config->statement_limit == 0 ?
                                              -1 : (int64_t)config->statement_limit;
    initial_run.mode = qstMainModeName(config->mode);
    initial_run.focus = qstMainFocusName(config->focus);
    initial_run.target_set = target_set;
    initial_run.tool_version = "taosQueryTest-v1";
    initial_run.parallel_test_info = parallel_info;
    if (NULL == initial_run.mode || NULL == initial_run.focus) {
      code = QST_E_INVALID_ARGUMENT;
    }
  }
  if (code == QST_OK) {
    code = qstMainNextRecordSeq(mapping, &record_seq);
  }
  if (code == QST_OK) {
    code = qstMainStoreOperation(mapping, config, QST_MAIN_STORE_RUN,
                                 &initial_run, NULL, 0, (SQstDigest256){{0}},
                                 record_seq, &durable_source, NULL, 0);
  }
  if (code != QST_OK) {
    return code;
  }
  atomic_store_explicit(&mapping->header->run_state, QST_RUN_RUNNING,
                        memory_order_release);
  code = qstMainPrepareDataset(mapping, config, generation, &dataset);
  (void)fprintf(stderr, "taosQueryTest: dataset prepare code=%d run_id=%llu\n",
                code, (unsigned long long)dataset.run_id);
  if (code == QST_OK) {
    code = qst_process_record_business(mapping, QST_ROLE_MAIN, generation, 0, 0);
    if (code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: record dataset preparation failed code=%d run_id=%llu\n",
                    code, (unsigned long long)config->run_id);
    }
  }
  if (code == QST_OK) {
    code = qst_generator_state_publish_dataset(
        mapping, generation, dataset.ds_fingerprint,
        dataset.fixture_manifest_digest, dataset.object_directory_ref,
        dataset.fixture_manifest_ref);
    if (code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: publish dataset snapshot failed code=%d run_id=%llu\n",
                    code, (unsigned long long)config->run_id);
    }
  }
  if (code == QST_OK) {
    qstMainDigestHex(dataset.ds_fingerprint, ds_fingerprint);
    code = qstMainLoadDatasetConfig(config, &dataset_config_json);
    if (code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: load dataset config failed code=%d run_id=%llu\n",
                    code, (unsigned long long)config->run_id);
    }
  }
  if (code == QST_OK && tdengine_readiness[0] == '\0') {
    qstMainLoadDatasetReadiness(config, tdengine_readiness,
                                sizeof(tdengine_readiness));
  }
  if (code == QST_OK && tdengine_readiness[0] != '\0') {
    code = qstMainFreezeTdengineStartup(mapping, config, tdengine_readiness,
                                        result_store_readiness);
  }
  if (code == QST_OK &&
      qst_provider_readiness_json(config, provider_readiness,
                                  sizeof(provider_readiness)) != QST_OK) {
    provider_readiness[0] = '\0';
  }
  if (code == QST_OK) {
    code = qstMainBuildParallelInfo(
        mapping, config, "prepared", "published", provider_readiness,
        tdengine_readiness, result_store_readiness, 0, parallel_info,
        sizeof(parallel_info));
  }
  if (code == QST_OK) {
    dataset_record.defined_ts_ns = qstRealtimeNowNs();
    dataset_record.ds_fingerprint = ds_fingerprint;
    dataset_record.config_json = dataset_config_json;
    dataset_record.fingerprint_format =
        "qst-local-fixture-v1|config_digest_sha256|object_directory_sha256|"
        "fixture_manifest_sha256";
    code = dataset_record.defined_ts_ns == 0 ? QST_E_RESOURCE :
        qstMainNextRecordSeq(mapping, &record_seq);
  }
  if (code == QST_OK) {
    code = qstMainStoreOperation(mapping, config, QST_MAIN_STORE_DATASET,
                                 NULL, &dataset_record, 0,
                                 (SQstDigest256){{0}}, record_seq,
                                 &durable_source, NULL, 0);
    if (code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: persist dataset record failed code=%d run_id=%llu\n",
                    code, (unsigned long long)config->run_id);
    }
  }
  if (code == QST_OK) {
    SQstRunRecord prepared_run = initial_run;

    prepared_run.ds_fingerprint = ds_fingerprint;
    prepared_run.parallel_test_info = parallel_info;
    code = qstMainNextRecordSeq(mapping, &record_seq);
    if (code == QST_OK) {
      code = qstMainStoreOperation(mapping, config, QST_MAIN_STORE_RUN,
                                   &prepared_run, NULL, 0,
                                   (SQstDigest256){{0}}, record_seq,
                                   &durable_source, NULL, 0);
      if (code != QST_OK) {
        (void)fprintf(stderr,
                      "taosQueryTest: persist prepared run failed code=%d run_id=%llu\n",
                      code, (unsigned long long)config->run_id);
      }
    }
  }
  if (code == QST_OK) {
    /* The fingerprint is only available after data preparation, so the
     * selected child tables are created immediately before child processes. */
    code = qstMainStoreOperation(mapping, config, QST_MAIN_STORE_PREPARE,
                                 NULL, NULL, config->target_selection_bits,
                                 dataset.ds_fingerprint, 0, NULL, NULL, 0);
    if (code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: prepare result-store tables failed code=%d run_id=%llu\n",
                    code, (unsigned long long)config->run_id);
    }
  }
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: pre-manager setup failed code=%d run_id=%llu\n",
                  code, (unsigned long long)config->run_id);
  }
  free(dataset_config_json);
  dataset_config_json = NULL;
  if (code == QST_OK) {
    qstMainStageProfileMark(QST_STAGE_PROFILE_PRE_MANAGER);
    code = qstStartManager(mapping, generation, QST_ROLE_EXEC_MANAGER,
                           qst_exec_manager_run, &exec_manager);
  }
  if (code == QST_OK) {
    code = qstStartManager(mapping, generation, QST_ROLE_JUDGE_MANAGER,
                           qst_judge_manager_run, &judge_manager);
  }
  if (code == QST_OK) {
    code = qstStartManager(mapping, generation, QST_ROLE_GENERATOR,
                           qst_generator_run, &generator);
  }
  if (code == QST_OK) {
    qstMainStageProfileMark(QST_STAGE_PROFILE_MANAGERS_READY);
  }
  if (code == QST_OK) {
    uint32_t main_stop_generation = 0;
    uint32_t main_exit_state = 0;

    code = qstMainWaitForGeneratorAndDrain(mapping, generation, &generator,
                                           &exec_manager, &judge_manager);
    qstMainCpuEfficiencyClose(mapping);
    (void)fprintf(stderr,
                  "taosQueryTest: generator wait/drain returned code=%d run_id=%llu\n",
                  code, (unsigned long long)config->run_id);
    if (code == QST_E_STALE_OWNER &&
        qst_process_get_stop_generation(
            mapping, QST_ROLE_MAIN, generation, &main_stop_generation,
            &main_exit_state) == QST_OK &&
        main_stop_generation != 0 &&
        (main_exit_state == QST_PROCESS_STOP_REQUESTED ||
         main_exit_state == QST_PROCESS_STOP_ACKED)) {
      code = QST_E_STOPPING;
    }
    if (code == QST_E_STOPPING) {
      user_stop_requested = 1;
      /* User stop closes generator input only. Preserve both managers so
       * already-generated tasks can reach durable storage before teardown. */
      code = QST_OK;
    }
    if (generator.generation != 0 && generator.pidfd >= 0) {
      EQstCode stop_code = qstMainPublishShutdownLease(mapping, generation);

      if (stop_code == QST_OK) {
        stop_code = qstStopAndReapManager(mapping, generation, &generator);
      }

      (void)fprintf(stderr,
                    "taosQueryTest: generator stop/reap returned code=%d run_id=%llu\n",
                    stop_code, (unsigned long long)config->run_id);
      if (code == QST_OK && stop_code != QST_OK) {
        code = stop_code;
      }
    }
    if (code == QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: generator reaped stop_reason=%u\n",
                    atomic_load_explicit(&mapping->header->stop_reason,
                                         memory_order_acquire));
    }
  }
  /* A user stop closes only the generator input. Keep both managers alive
   * until every already-generated task has reached durable storage. */
  if (code == QST_OK) {
    code = qstMainDrainPersistence(mapping, generation, &exec_manager);
    if (code == QST_OK) {
      qstMainStageProfileMark(QST_STAGE_PROFILE_DURABLE);
    }
    (void)fprintf(stderr,
                  "taosQueryTest: main persistence drain returned code=%d run_id=%llu\n",
                  code, (unsigned long long)config->run_id);
    if (code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: main persistence drain failed code=%d\n",
                    code);
    }
  }
  if (exec_manager.generation != 0 || judge_manager.generation != 0) {
    const EQstCode lease_code = qstMainPublishShutdownLease(mapping, generation);

    if (code == QST_OK && lease_code != QST_OK) {
      code = lease_code;
    }
  }
  if (code == QST_OK) {
    qstTestMaybeDelayMainShutdown();
  }
  if (exec_manager.generation != 0) {
    int32_t exited = 0;
    EQstCode stop_code = QST_OK;
    if (exec_manager.pidfd >= 0 &&
        qstWaitForExit(exec_manager.pidfd, 1, &exited) == QST_OK &&
        exited != 0) {
      stop_code = qstReapManager(mapping, generation, &exec_manager);
    } else {
      stop_code = qstStopAndReapManager(mapping, generation, &exec_manager);
    }
    if (code == QST_OK && stop_code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: exec manager stop/reap returned code=%d run_id=%llu\n",
                    stop_code, (unsigned long long)config->run_id);
      code = stop_code;
    }
  }
  qstMainStageProfileMark(QST_STAGE_PROFILE_EXEC_MANAGER_REAPED);
  if (judge_manager.generation != 0) {
    int32_t exited = 0;
    EQstCode stop_code = QST_OK;
    if (judge_manager.pidfd >= 0 &&
        qstWaitForExit(judge_manager.pidfd, 1, &exited) == QST_OK &&
        exited != 0) {
      stop_code = qstReapManager(mapping, generation, &judge_manager);
    } else {
      stop_code = qstStopAndReapManager(mapping, generation, &judge_manager);
    }
    if (code == QST_OK && stop_code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: judge manager stop/reap returned code=%d run_id=%llu\n",
                    stop_code, (unsigned long long)config->run_id);
      code = stop_code;
    }
  }
  qstMainStageProfileMark(QST_STAGE_PROFILE_JUDGE_MANAGER_REAPED);
  if (dataset.run_id != 0) {
    const EQstCode cleanup_code = qstMainCleanupDataset(mapping, config, &dataset);
    if (cleanup_code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: dataset cleanup failed run_id=%llu code=%d\n",
                    (unsigned long long)dataset.run_id, cleanup_code);
    }
    if (code == QST_OK && cleanup_code != QST_OK) {
      code = cleanup_code;
    }
  }
  qstMainStageProfileMark(QST_STAGE_PROFILE_CLEANUP_COMPLETE);
  final_run = initial_run;
  final_run.end_ts_ns = qstRealtimeNowNs();
  /* A cooperative user stop is a durable cancellation, not a successful
   * completion, even though all already-generated work drained cleanly. */
  final_run.status = user_stop_requested != 0 ? 3U :
                     (code == QST_OK ? 1U : 2U);
  final_run.ds_fingerprint = ds_fingerprint[0] == '\0' ? NULL : ds_fingerprint;
  if (code == QST_OK) {
    const EQstCode parallel_info_code = qstMainBuildParallelInfo(
        mapping, config, "final", "finalized", provider_readiness,
        tdengine_readiness, result_store_readiness, 1, parallel_info,
        sizeof(parallel_info));
    if (parallel_info_code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: final parallel info build failed "
                    "code=%d run_id=%llu\n",
                    parallel_info_code, (unsigned long long)config->run_id);
    }
    code = parallel_info_code;
  }
  final_run.parallel_test_info = parallel_info;
  qstMainCollectRunStats(mapping, &final_run);
  if (config->result_dir != NULL) {
    const EQstCode profile_code = qstMainWriteStageProfile(config, mapping);

    if (profile_code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: stage profile write failed code=%d "
                    "run_id=%llu\n",
                    profile_code, (unsigned long long)config->run_id);
      if (code == QST_OK) {
        code = profile_code;
      }
    }
  }
  if (code == QST_OK && user_stop_requested == 0) {
    const uint32_t shared_stop_reason = atomic_load_explicit(
        &mapping->header->stop_reason, memory_order_acquire);

    if (shared_stop_reason == QST_STOP_FULL_COMPLETE) {
      stop_reason = "full_coverage_complete";
    } else if (shared_stop_reason == QST_STOP_STATEMENT_LIMIT) {
      stop_reason = "statement_limit";
    } else if (shared_stop_reason == QST_STOP_DURATION_LIMIT) {
      stop_reason = "duration_limit";
    } else {
      stop_reason = "internal_error";
      code = QST_E_SHM_CORRUPT;
    }
  } else if (user_stop_requested != 0) {
    atomic_store_explicit(&mapping->header->stop_reason, QST_STOP_USER_REQUEST,
                          memory_order_release);
    stop_reason = "user_request";
  } else {
    if (code == QST_E_STOPPING ||
        atomic_load_explicit(&mapping->header->stop_reason,
                             memory_order_acquire) == QST_STOP_USER_REQUEST) {
      atomic_store_explicit(&mapping->header->stop_reason, QST_STOP_USER_REQUEST,
                            memory_order_release);
      stop_reason = "user_request";
    } else if (dataset.run_id == 0) {
      atomic_store_explicit(&mapping->header->stop_reason, QST_STOP_PREPARE_FAILED,
                            memory_order_release);
      stop_reason = "prepare_failed";
    } else if (code == QST_E_SHM_CORRUPT || code == QST_E_SHM_ABI ||
               code == QST_E_SHM_FORMAT) {
      atomic_store_explicit(&mapping->header->stop_reason, QST_STOP_SHM_CORRUPT,
                            memory_order_release);
      stop_reason = "shared_memory_corrupt";
    } else {
      atomic_store_explicit(&mapping->header->stop_reason,
                            QST_STOP_INFRASTRUCTURE_FAILED,
                            memory_order_release);
      stop_reason = "infrastructure_failed";
    }
    error_message = "taosQueryTest main run failed";
    final_run.has_error = 1;
    final_run.error_code = code;
    final_run.error_message = error_message;
  }
  final_run.stop_reason = stop_reason;
  {
    EQstCode final_seq_code = final_run.end_ts_ns == 0
                                  ? QST_E_RESOURCE
                                  : qstMainNextRecordSeq(mapping, &record_seq);
    EQstCode final_store_code = final_seq_code == QST_OK
                                    ? qstMainStoreOperation(
                                          mapping, config, QST_MAIN_STORE_RUN,
                                          &final_run, NULL, 0,
                                          (SQstDigest256){{0}}, record_seq,
                                          &durable_source, NULL, 0)
                                    : final_seq_code;
    if (final_seq_code != QST_OK || final_store_code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: final run record persistence failed "
                    "seq_code=%d store_code=%d run_id=%llu\n",
                    final_seq_code, final_store_code,
                    (unsigned long long)config->run_id);
      if (code == QST_OK) {
        code = final_store_code != QST_OK ? final_store_code : final_seq_code;
      }
    }
  }
  if (code != QST_OK) {
    atomic_store_explicit(&mapping->header->run_state, QST_RUN_FAILED,
                          memory_order_release);
    if (NULL != config->result_dir) {
      const EQstCode report_code = qst_report_refresh(mapping, config);
      if (report_code != QST_OK) {
        (void)fprintf(stderr,
                      "taosQueryTest: failed-run report refresh failed "
                      "code=%d run_id=%llu\n",
                      report_code, (unsigned long long)config->run_id);
      }
    }
    qstMainWritePersistProfile();
    free(gQstMainCpuEfficiency.processes);
    gQstMainCpuEfficiency.processes = NULL;
    (void)qst_process_mark_exited(mapping, QST_ROLE_MAIN, generation,
                                  (int32_t)code);
    return code;
  }
  atomic_store_explicit(&mapping->header->run_state,
                        user_stop_requested != 0 ? QST_RUN_CANCELLED :
                                                     QST_RUN_COMPLETED,
                        memory_order_release);
  if (NULL != config->result_dir) {
    const EQstCode report_code = qst_report_refresh(mapping, config);
    if (report_code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: terminal report refresh failed "
                    "code=%d run_id=%llu\n",
                    report_code, (unsigned long long)config->run_id);
      qstMainWritePersistProfile();
      free(gQstMainCpuEfficiency.processes);
      gQstMainCpuEfficiency.processes = NULL;
      return report_code;
    }
  }
  qstMainWritePersistProfile();
  free(gQstMainCpuEfficiency.processes);
  gQstMainCpuEfficiency.processes = NULL;
  return qst_process_mark_exited(mapping, QST_ROLE_MAIN, generation, 0);
}

static EQstCode qstSpawnMainProcess(SQstShmMapping *mapping,
                                    const SQstRunConfig *config,
                                    pid_t *out_pid, int32_t *out_pidfd,
                                    uint32_t *out_generation) {
  int32_t generation_pipe[2] = {-1, -1};
  pid_t pid = -1;
  uint32_t generation = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == config || NULL == out_pid ||
      NULL == out_pidfd || NULL == out_generation) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (pipe(generation_pipe) != 0) {
    return QST_E_RESOURCE;
  }
  pid = fork();
  if (pid < 0) {
    (void)close(generation_pipe[0]);
    (void)close(generation_pipe[1]);
    return QST_E_RESOURCE;
  }
  if (pid == 0) {
    uint32_t child_generation = 0;
    ssize_t bytes = 0;
    EQstCode main_code = QST_OK;
    const pid_t parent_pid = getppid();

    (void)close(generation_pipe[1]);
    if (prctl(PR_SET_PDEATHSIG, SIGKILL) != 0 || getppid() != parent_pid ||
        setpgid(0, 0) != 0) {
      (void)close(generation_pipe[0]);
      _exit(1);
    }
    bytes = read(generation_pipe[0], &child_generation, sizeof(child_generation));
    (void)close(generation_pipe[0]);
    if (bytes != (ssize_t)sizeof(child_generation) || child_generation == 0) {
      _exit(1);
    }
    main_code = qst_main_run(mapping, config);
    (void)qst_shm_unmap(mapping);
    _exit(main_code == QST_OK ? 0 : 1);
  }
  (void)close(generation_pipe[0]);
  if (setpgid(pid, pid) != 0 && errno != EACCES && errno != ESRCH) {
    code = QST_E_RESOURCE;
  }
  if (code == QST_OK) {
    code = qst_process_register(mapping, QST_ROLE_LAUNCHER, 1,
                                QST_ROLE_MAIN, (int32_t)pid, (int32_t)pid,
                                &generation);
  }
  if (code == QST_OK &&
      write(generation_pipe[1], &generation, sizeof(generation)) !=
          (ssize_t)sizeof(generation)) {
    code = QST_E_RESOURCE;
  }
  (void)close(generation_pipe[1]);
  if (code != QST_OK) {
    (void)kill(-pid, SIGKILL);
    (void)waitpid(pid, NULL, 0);
    return code;
  }
  *out_pidfd = qstOpenPidfd(pid);
  if (*out_pidfd < 0) {
    (void)kill(-pid, SIGKILL);
    (void)waitpid(pid, NULL, 0);
    (void)qst_process_reap_generation(mapping, QST_ROLE_LAUNCHER, 1,
                                      QST_ROLE_MAIN, generation, 0);
    return QST_E_RESOURCE;
  }
  *out_pid = pid;
  *out_generation = generation;
  return QST_OK;
}

static EQstCode qstLauncherReapProcess(SQstShmMapping *mapping,
                                       const SQstProcessIdentity *identity) {
  int32_t pidfd = -1;
  int32_t status = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == identity || identity->pid <= 0 ||
      identity->pgid <= 0 || identity->role < QST_ROLE_MAIN ||
      identity->role > QST_ROLE_AI_WORKER || identity->generation == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  pidfd = qstOpenPidfd(identity->pid);
  if (pidfd >= 0) {
    code = qstTerminateProcessGroup(identity->pgid, pidfd);
    (void)close(pidfd);
    if (code != QST_OK) {
      return code;
    }
  } else if (kill(-identity->pgid, SIGKILL) != 0 && errno != ESRCH) {
    return QST_E_RESOURCE;
  }
  while (waitpid(identity->pid, &status, 0) < 0) {
    if (errno != EINTR) {
      return QST_E_RESOURCE;
    }
  }
  if (identity->role == QST_ROLE_MAIN) {
    return qst_process_reap_generation(
        mapping, QST_ROLE_LAUNCHER, 1, (EQstRole)identity->role,
        identity->generation, status);
  }
  code = qst_process_reap_orphaned_generation_instance(
      mapping, QST_ROLE_LAUNCHER, 1, (EQstRole)identity->role,
      identity->slot_id, identity->generation, status);
  if (code == QST_OK && identity->role == QST_ROLE_TAOSD) {
    code = qstPairRewriteRuntimeState(identity->slot_id,
                                      identity->generation, "reaped", 0,
                                      "launcher");
    /* The process ledger has already recorded the authoritative reaped state.
     * A pair fact can be temporarily unavailable while its supervisor is
     * concurrently unwinding startup, so do not turn that auxiliary pending
     * write into a failed launcher cleanup. */
    if (code == QST_E_PENDING_IO) {
      code = QST_OK;
    }
  }
  return code;
}

/* Reaps taosd children whose supervisor generation was replaced before the
 * process ledger could be traversed through the reused pair slot. */
static EQstCode qstLauncherReapCrashedPairServers(SQstShmMapping *mapping) {
  char result_dir[4096] = {0};
  char pairs_dir[4096] = {0};
  DIR *directory = NULL;
  struct dirent *entry = NULL;

  if (NULL == mapping || gQstProcessLogDirectory[0] == '\0' ||
      strlen(gQstProcessLogDirectory) <= strlen("/logs") ||
      strcmp(gQstProcessLogDirectory + strlen(gQstProcessLogDirectory) -
                 strlen("/logs"),
             "/logs") != 0 ||
      snprintf(result_dir, sizeof(result_dir), "%s", gQstProcessLogDirectory) <
          0) {
    return QST_E_INVALID_ARGUMENT;
  }
  result_dir[strlen(result_dir) - strlen("/logs")] = '\0';
  if (qstMakePath(pairs_dir, sizeof(pairs_dir), result_dir, "pairs") != QST_OK) {
    return QST_E_RESOURCE;
  }
  directory = opendir(pairs_dir);
  if (NULL == directory) {
    return errno == ENOENT ? QST_OK : QST_E_RESOURCE;
  }
  while ((entry = readdir(directory)) != NULL) {
    char runtime_path[4096] = {0};
    char fact[2048] = {0};
    FILE *file = NULL;
    size_t fact_bytes = 0;
    unsigned int pair_slot = 0;
    unsigned int pair_generation = 0;
    unsigned int taosd_slot = 0;
    unsigned int taosd_generation = 0;
    int32_t taosd_pid = 0;
    int32_t taosd_pgid = 0;
    int32_t pidfd = -1;
    int32_t status = 0;
    EQstCode code = QST_OK;

    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0 ||
        sscanf(entry->d_name, "slot_%u_generation_%u", &pair_slot,
               &pair_generation) != 2) {
      continue;
    }
    if (snprintf(runtime_path, sizeof(runtime_path), "%s/%s/runtime.json",
                 pairs_dir, entry->d_name) < 0) {
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
    if (strstr(fact, "\"supervisor_crashed\":true") == NULL ||
        strstr(fact, "\"state\":\"crashed\"") == NULL ||
        sscanf(fact,
               "{\"parallel_id\":%*u,\"slot\":%*u,\"generation\":%*u,"
               "\"taosd_pid\":%d,\"taosc_pid\":%*d,\"taosd_pgid\":%d,"
               "\"taosd_process_slot\":%u,\"taosd_process_generation\":%u,",
               &taosd_pid, &taosd_pgid, &taosd_slot, &taosd_generation) != 4 ||
        taosd_pid <= 0 || taosd_pgid <= 0 || taosd_generation == 0) {
      continue;
    }
    pidfd = qstOpenPidfd(taosd_pid);
    if (pidfd >= 0) {
      code = qstTerminateProcessGroup(taosd_pid, pidfd);
      (void)close(pidfd);
    } else if (kill(-taosd_pgid, SIGKILL) != 0 && errno != ESRCH) {
      code = QST_E_RESOURCE;
    }
    if (code == QST_OK) {
      errno = 0;
      while (waitpid(taosd_pid, &status, 0) < 0 && errno == EINTR) {
      }
      if (errno != 0 && errno != ECHILD) {
        code = QST_E_RESOURCE;
      }
    }
    if (code == QST_OK) {
      code = qst_process_reap_orphaned_generation_instance(
          mapping, QST_ROLE_LAUNCHER, 1, QST_ROLE_TAOSD, taosd_slot,
          taosd_generation, status);
    }
    if (code == QST_OK) {
      code = qstPairRewriteRuntimeState(pair_slot, pair_generation, "reaped",
                                         0, "launcher");
    }
    if (code != QST_OK) {
      (void)closedir(directory);
      return code;
    }
  }
  return closedir(directory) == 0 ? QST_OK : QST_E_RESOURCE;
}

static EQstCode qstLauncherReapGeneration(SQstShmMapping *mapping) {
  SQstProcessIdentity *descendants = NULL;
  uint32_t descendant_count = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == mapping->header) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qstLauncherReapCrashedPairServers(mapping);
  if (code != QST_OK) {
    /* During cooperative shutdown the main process may already have stopped
     * a crashed pair's taosd.  Its runtime fact remains useful evidence, but
     * a second orphan-ledger reap can legitimately report a stale resource.
     * Continue with the authoritative descendant ledger walk; normal runs
     * still fail fast on this condition. */
    if (gQstStopSignal == 0) {
      return code;
    }
  }
  code = qst_process_list_descendants_postorder(
      mapping, QST_ROLE_LAUNCHER, 1, NULL, 0, &descendant_count);
  if (code != QST_E_RESOURCE && code != QST_OK) {
    return code;
  }
  if (descendant_count == 0) {
    return QST_OK;
  }
  descendants = (SQstProcessIdentity *)calloc(descendant_count,
                                               sizeof(*descendants));
  if (NULL == descendants) {
    return QST_E_RESOURCE;
  }
  code = qst_process_list_descendants_postorder(
      mapping, QST_ROLE_LAUNCHER, 1, descendants, descendant_count,
      &descendant_count);
  if (code == QST_OK) {
    for (uint32_t index = 0; index < descendant_count; ++index) {
      if (descendants[index].role == QST_ROLE_MAIN) {
        continue;
      }
      code = qstLauncherReapProcess(mapping, &descendants[index]);
      if (code != QST_OK) {
        (void)fprintf(stderr,
                      "taosQueryTest: launcher reap descendant failed "
                      "role=%s slot=%u generation=%u pid=%d code=%d\n",
                      qstRoleName((EQstRole)descendants[index].role),
                      descendants[index].slot_id, descendants[index].generation,
                      (int)descendants[index].pid, code);
        break;
      }
    }
  }
  free(descendants);
  return code;
}

static EQstCode qstLauncherSignalDescendants(SQstShmMapping *mapping) {
  SQstProcessIdentity *descendants = NULL;
  uint32_t descendant_count = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qst_process_list_descendants_postorder(
      mapping, QST_ROLE_LAUNCHER, 1, NULL, 0, &descendant_count);
  if (code != QST_E_RESOURCE && code != QST_OK) {
    return code;
  }
  if (descendant_count == 0) {
    return QST_OK;
  }
  descendants = (SQstProcessIdentity *)calloc(descendant_count,
                                               sizeof(*descendants));
  if (NULL == descendants) {
    return QST_E_RESOURCE;
  }
  code = qst_process_list_descendants_postorder(
      mapping, QST_ROLE_LAUNCHER, 1, descendants, descendant_count,
      &descendant_count);
  if (code == QST_OK) {
    for (uint32_t index = 0; index < descendant_count; ++index) {
      if (descendants[index].role == QST_ROLE_MAIN) {
        continue;
      }
      if (kill(-descendants[index].pgid, SIGTERM) != 0 && errno != ESRCH) {
        code = QST_E_RESOURCE;
        break;
      }
    }
  }
  free(descendants);
  return code;
}

EQstCode qst_launcher_run(const SQstRunConfig *config) {
  SQstShmMapping mapping = {0};
  char result_root[4096] = {0};
  char run_name[64] = {0};
  char result_dir[4096] = {0};
  char logs_dir[4096] = {0};
  char cores_dir[4096] = {0};
  char pending_dir[4096] = {0};
  char quarantine_dir[4096] = {0};
  char resource_profile_path[4096] = {0};
  char dependency_lock_path[4096] = {0};
  SQstRunConfig local = {0};
  SQstResourceInputs resource_inputs = {0};
  SQstResourceProfile resource_profile = {0};
  pid_t main_pid = -1;
  int32_t main_pidfd = -1;
  int32_t run_lock_fd = -1;
  EQstCode code = QST_OK;

  if (NULL == config || NULL == config->tool_root) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) != 0) {
    return QST_E_RESOURCE;
  }
  /* Install a handler before creating/publishing the run id. Test harnesses
   * legitimately use that file as the startup barrier and may request stop
   * immediately afterwards, before the shared-memory eventfd exists. */
  {
    struct sigaction early_action = {0};

    gQstStopSignal = 0;
    gQstSignalEventfd = -1;
    early_action.sa_handler = qstStopSignalHandler;
    if (sigemptyset(&early_action.sa_mask) != 0 ||
        sigaction(SIGINT, &early_action, NULL) != 0 ||
        sigaction(SIGTERM, &early_action, NULL) != 0) {
      return QST_E_RESOURCE;
    }
  }
  local = *config;
  if (qstMakePath(result_root, sizeof(result_root), local.tool_root,
                  "results") != QST_OK || qstMkdir(result_root) != QST_OK) {
    return QST_E_RESOURCE;
  }
  if (local.run_id == 0) {
    code = qstAllocateRunId(result_root, &local.run_id, &run_lock_fd);
    if (code != QST_OK) {
      return code;
    }
  } else {
    char lock_path[4096] = {0};

    if (qstMakePath(lock_path, sizeof(lock_path), result_root,
                    "latest_run_id.lock") != QST_OK ||
        (run_lock_fd = open(lock_path, O_RDWR | O_CREAT | O_CLOEXEC, 0600)) < 0 ||
        flock(run_lock_fd, LOCK_EX) != 0) {
      if (run_lock_fd >= 0) {
        (void)close(run_lock_fd);
        run_lock_fd = -1;
      }
      return QST_E_RESOURCE;
    }
  }
  if (local.mode == QST_MODE_RANDOM && local.random_seed == 0) {
    code = qstGenerateRandomSeed(&local.random_seed);
    if (code != QST_OK) {
      qstReleaseRunLock(&run_lock_fd);
      return code;
    }
  }
  if (snprintf(run_name, sizeof(run_name), "%llu",
               (unsigned long long)local.run_id) < 0 ||
      qstMakePath(result_dir, sizeof(result_dir), result_root, run_name) != QST_OK ||
      qstMkdir(result_dir) != QST_OK ||
      qstMakePath(logs_dir, sizeof(logs_dir), result_dir, "logs") != QST_OK ||
      qstMkdir(logs_dir) != QST_OK ||
      qstMakePath(cores_dir, sizeof(cores_dir), result_dir, "cores") != QST_OK ||
      qstMkdir(cores_dir) != QST_OK ||
      qstMakePath(pending_dir, sizeof(pending_dir), result_dir,
                  "pending-results") != QST_OK ||
      qstMkdir(pending_dir) != QST_OK ||
      qstMakePath(quarantine_dir, sizeof(quarantine_dir), pending_dir,
                  "quarantine") != QST_OK ||
      qstMkdir(quarantine_dir) != QST_OK) {
    qstReleaseRunLock(&run_lock_fd);
    return QST_E_RESOURCE;
  }
  local.result_dir = result_dir;
  code = qstSetProcessLogDirectory(&local);
  if (code != QST_OK) {
    qstReleaseRunLock(&run_lock_fd);
    return code;
  }
  code = qst_resource_capture(result_dir, logs_dir, &resource_inputs);
  if (code == QST_OK) {
    code = qst_resource_calculate(&resource_inputs, &local, &resource_profile);
  }
  if (code == QST_OK) {
    code = qstSetProcessLogDirectory(&local);
  }
  if (code != QST_OK) {
    (void)qstWriteRunArtifacts(&local, &resource_profile, result_dir, logs_dir,
                               "failed", code);
    qstReleaseRunLock(&run_lock_fd);
    return code;
  }
  if (qstWriteRunArtifacts(&local, &resource_profile, result_dir, logs_dir,
                           "preparing", QST_OK) != QST_OK) {
    qstReleaseRunLock(&run_lock_fd);
    return QST_E_RESOURCE;
  }
  if (qstMakePath(resource_profile_path, sizeof(resource_profile_path), result_dir,
                  "resource-profile.json") != QST_OK ||
      qstMakePath(dependency_lock_path, sizeof(dependency_lock_path),
                  local.tool_root, "dependency-lock.json") != QST_OK ||
      qstValidateDependencyLock(dependency_lock_path) != QST_OK ||
      qstDigestFile(resource_profile_path, &local.resource_profile_digest) != QST_OK ||
      qstDigestFile(dependency_lock_path, &local.dependency_lock_digest) != QST_OK) {
    qstReleaseRunLock(&run_lock_fd);
    return QST_E_RESOURCE;
  }
  code = qst_shm_create(&local, &mapping);
  if (code != QST_OK) {
    (void)qstWriteRunArtifacts(&local, &resource_profile, result_dir, logs_dir,
                               "failed", code);
    qstReleaseRunLock(&run_lock_fd);
    return code;
  }
  if (qstInstallStopHandlers(mapping.queue_eventfd[QST_QUEUE_CONTROL - 1]) !=
      QST_OK) {
    (void)qst_shm_unmap(&mapping);
    (void)qstWriteRunArtifacts(&local, &resource_profile, result_dir, logs_dir,
                               "failed", QST_E_RESOURCE);
    qstReleaseRunLock(&run_lock_fd);
    return QST_E_RESOURCE;
  }
  if (qstTestPublishLauncherReady() != QST_OK) {
    (void)qst_shm_unmap(&mapping);
    (void)qstWriteRunArtifacts(&local, &resource_profile, result_dir, logs_dir,
                               "failed", QST_E_RESOURCE);
    qstReleaseRunLock(&run_lock_fd);
    return QST_E_RESOURCE;
  }
  for (;;) {
    int32_t wait_status = 0;
    uint32_t main_generation = 0;
    EQstCode wait_code = QST_OK;

    code = qstSpawnMainProcess(&mapping, &local, &main_pid, &main_pidfd,
                               &main_generation);
    if (code != QST_OK) {
      break;
    }
    code = qstWriteLifecycleLog(QST_ROLE_LAUNCHER, 1, "CHILD_SPAWN",
                                QST_ROLE_MAIN, main_generation,
                                (int32_t)main_pid, (int32_t)main_pid, QST_OK,
                                0);
    if (code != QST_OK) {
      (void)qstStopDirectChild(&mapping, QST_ROLE_LAUNCHER, 1,
                               QST_ROLE_MAIN, main_generation, main_pid,
                               main_pidfd);
      (void)waitpid(main_pid, NULL, 0);
      (void)close(main_pidfd);
      main_pidfd = -1;
      (void)qst_process_reap_generation(
          &mapping, QST_ROLE_LAUNCHER, 1, QST_ROLE_MAIN, main_generation, 0);
      break;
    }
    code = qstWaitForRoleFlow(&mapping, QST_ROLE_MAIN, 0, main_generation,
                              qstMillisecondsToDeadlineNs(
                                  QST_FLOW_LEASE_MILLISECONDS));
    if (code != QST_OK) {
      (void)qstStopDirectChild(&mapping, QST_ROLE_LAUNCHER, 1,
                               QST_ROLE_MAIN, main_generation, main_pid,
                               main_pidfd);
      (void)waitpid(main_pid, NULL, 0);
      (void)close(main_pidfd);
      main_pidfd = -1;
      (void)qst_process_reap_generation(
          &mapping, QST_ROLE_LAUNCHER, 1, QST_ROLE_MAIN, main_generation, 0);
      break;
    }
    code = qstWriteLifecycleLog(QST_ROLE_LAUNCHER, 1, "CHILD_READY",
                                QST_ROLE_MAIN, main_generation,
                                (int32_t)main_pid, (int32_t)main_pid, QST_OK,
                                0);
    if (code != QST_OK) {
      (void)qstStopDirectChild(&mapping, QST_ROLE_LAUNCHER, 1,
                               QST_ROLE_MAIN, main_generation, main_pid,
                               main_pidfd);
      (void)waitpid(main_pid, NULL, 0);
      (void)close(main_pidfd);
      main_pidfd = -1;
      (void)qst_process_reap_generation(
          &mapping, QST_ROLE_LAUNCHER, 1, QST_ROLE_MAIN, main_generation, 0);
      break;
    }
    wait_code = qstWaitForMain(&mapping, main_pid, main_pidfd,
                               main_generation);
    if (wait_code == QST_E_DEADLINE || wait_code == QST_E_STOPPING) {
      /* Main owns the ordered shutdown of its managers. Signalling their
       * process groups here races its generator/persistence drain protocol. */
      EQstCode stop_code = qstStopDirectChild(
          &mapping, QST_ROLE_LAUNCHER, 1, QST_ROLE_MAIN, main_generation,
          main_pid, main_pidfd);
      if (stop_code != QST_OK) {
        code = stop_code;
      }
    } else if (wait_code != QST_OK) {
      code = wait_code;
    }
    if (waitpid(main_pid, &wait_status, 0) < 0) {
      code = QST_E_RESOURCE;
    }
    if (qstWriteLifecycleLog(QST_ROLE_LAUNCHER, 1, "CHILD_EXITED",
                             QST_ROLE_MAIN, main_generation,
                             (int32_t)main_pid, (int32_t)main_pid, wait_code,
                             wait_status) != QST_OK && code == QST_OK) {
      code = QST_E_RESOURCE;
    }
    if (main_pidfd >= 0) {
      (void)close(main_pidfd);
      main_pidfd = -1;
    }
    {
      const EQstCode reap_code = qstLauncherReapGeneration(&mapping);

      if (reap_code != QST_OK && code == QST_OK) {
        code = reap_code;
      }
    }
    if (main_generation == 0 ||
        qst_process_reap_generation(&mapping, QST_ROLE_LAUNCHER, 1,
                                    QST_ROLE_MAIN, main_generation,
                                    wait_status) != QST_OK) {
      code = QST_E_RESOURCE;
    }
    if (code == QST_OK &&
        qstWriteLifecycleLog(QST_ROLE_LAUNCHER, 1, "CHILD_REAPED",
                             QST_ROLE_MAIN, main_generation,
                             (int32_t)main_pid, (int32_t)main_pid, wait_code,
                             wait_status) != QST_OK) {
      code = QST_E_RESOURCE;
    }
    if (code != QST_OK) {
      break;
    }
    if (wait_code == QST_E_STOPPING) {
      code = QST_E_STOPPING;
      break;
    }
    if (wait_code == QST_E_DEADLINE) {
      continue;
    }
    if (!WIFEXITED(wait_status) || WEXITSTATUS(wait_status) != 0) {
      code = QST_E_RESOURCE;
    }
    break;
  }
  if (gQstStopSignal != 0 && code != QST_OK) {
    /* User cancellation remains authoritative if teardown races a child exit. */
    code = QST_E_STOPPING;
  }
  if (code != QST_OK) {
    atomic_store_explicit(&mapping.header->run_state,
                          code == QST_E_STOPPING ? QST_RUN_CANCELLED : QST_RUN_FAILED,
                          memory_order_release);
  }
  /*
   * qst_main_run() writes the first terminal report before its own process
   * slot can be reaped by the launcher.  Refresh after the complete descendant
   * tree has been reaped so the durable report reflects the final ledger.
   */
  if (qst_report_refresh(&mapping, &local) != QST_OK && code == QST_OK) {
    code = QST_E_RESOURCE;
    atomic_store_explicit(&mapping.header->run_state, QST_RUN_FAILED,
                          memory_order_release);
    (void)qst_report_refresh(&mapping, &local);
  }
  if (qst_shm_unmap(&mapping) != QST_OK && code == QST_OK) {
    code = QST_E_RESOURCE;
  }
  if (qstWriteRunArtifacts(&local, &resource_profile, result_dir, logs_dir,
                           code == QST_OK ? "completed" :
                           code == QST_E_STOPPING ? "cancelled" : "failed", code) != QST_OK &&
      code == QST_OK) {
    code = QST_E_RESOURCE;
  }
  qstReleaseRunLock(&run_lock_fd);
  (void)qstSetProcessLogDirectory(NULL);
  return code;
}

static EQstCode qstRunRoleEntry(SQstShmMapping *mapping, EQstRole role,
                                uint32_t generation) {
  struct pollfd event = {0};
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == mapping->header || NULL == mapping->base ||
      generation == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qstWriteProcessStartLog(role, generation, QST_ROLE_MAIN,
                                 atomic_load_explicit(&mapping->header->main_generation,
                                                      memory_order_acquire));
  if (code != QST_OK) {
    return code;
  }
  code = qstConfirmQueueFlow(mapping, role, generation, QST_FLOW_STAGE_STARTUP,
                             QST_QUEUE_CONTROL, 0, 0);
  if (code != QST_OK) {
    return code;
  }
  if (gQstStopSignal != 0) {
    return qst_process_mark_exited(mapping, role, generation, 0) == QST_OK ?
               QST_E_STOPPING : QST_E_STALE_OWNER;
  }
  event.fd = mapping->queue_eventfd[QST_QUEUE_CONTROL - 1];
  event.events = POLLIN;
  for (;;) {
    uint32_t stop_generation = 0;
    uint32_t exit_state = 0;

    code = qst_process_get_stop_generation(mapping, role, generation,
                                           &stop_generation, &exit_state);
    if (code != QST_OK) {
      return code;
    }
    if (exit_state == QST_PROCESS_STOP_REQUESTED && stop_generation != 0) {
      return qstAcknowledgeRoleStop(mapping, role, generation,
                                    stop_generation);
    }
    if (exit_state != QST_PROCESS_RUNNING) {
      return QST_E_STALE_OWNER;
    }
    code = qstConfirmQueueFlow(mapping, role, generation, QST_FLOW_STAGE_IDLE,
                               QST_QUEUE_CONTROL, 0, 0);
    /* A parent may request stop after the state read above but before this
     * IDLE confirmation. Re-read the process slot to acknowledge that stop. */
    if (code == QST_E_STALE_OWNER) {
      continue;
    }
    if (code != QST_OK) {
      return code;
    }
    if (qstPollQueueEvent(&event, 20) != QST_OK) {
      return QST_E_RESOURCE;
    }
  }
}

/* ACKs the current generator stop request before returning from its work loop. */
static EQstCode qstGeneratorCheckStopRequest(SQstShmMapping *mapping,
                                             uint32_t generation,
                                             int32_t *out_acknowledged) {
  uint32_t stop_generation = 0;
  uint32_t exit_state = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || generation == 0 || NULL == out_acknowledged) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_acknowledged = 0;
  code = qst_process_get_stop_generation(mapping, QST_ROLE_GENERATOR,
                                         generation, &stop_generation,
                                         &exit_state);
  if (code != QST_OK) {
    return code;
  }
  if (exit_state == QST_PROCESS_STOP_REQUESTED && stop_generation != 0) {
    code = qstAcknowledgeRoleStop(mapping, QST_ROLE_GENERATOR, generation,
                                  stop_generation);
    if (code == QST_OK) {
      *out_acknowledged = 1;
    } else {
      (void)fprintf(stderr,
                    "taosQueryTest: generator stop acknowledgement failed "
                    "code=%d generation=%u stop_generation=%u\n",
                    code, generation, stop_generation);
    }
    return code;
  }
  if (exit_state != QST_PROCESS_RUNNING) {
    return QST_E_STALE_OWNER;
  }
  return gQstStopSignal != 0 ? QST_E_STOPPING : QST_OK;
}

/* Holds a benchmark fixture at an idle generator boundary until normal stop. */
static EQstCode qstGeneratorHoldForStageBenchmark(SQstShmMapping *mapping,
                                                   uint32_t generation) {
  struct pollfd event = {0};

  if (mapping == NULL || generation == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  event.fd = mapping->queue_eventfd[QST_QUEUE_CONTROL - 1];
  event.events = POLLIN;
  for (;;) {
    int32_t stop_acknowledged = 0;
    EQstCode code = qstGeneratorCheckStopRequest(
        mapping, generation, &stop_acknowledged);

    if (code != QST_OK || stop_acknowledged != 0) {
      return code;
    }
    code = qstConfirmQueueFlow(mapping, QST_ROLE_GENERATOR, generation,
                               QST_FLOW_STAGE_IDLE, QST_QUEUE_GENERATE, 0, 0);
    if (code != QST_OK) {
      return code;
    }
    if (qstPollQueueEvent(&event, 20) != QST_OK) {
      return QST_E_RESOURCE;
    }
  }
}

typedef struct {
  uint64_t generation_seq;
  SQstGeneratorRandomState random_state;
} SQstRandomStreamItem;

typedef struct {
  _Atomic uint32_t write_index;
  _Atomic uint32_t read_index;
  SQstRandomStreamItem items[QST_RANDOM_PIPELINE_RING_CAPACITY];
} SQstRandomStreamRing;

typedef struct {
  _Atomic uint32_t write_index;
  _Atomic uint32_t read_index;
  SQstGeneratorRandomPending items[QST_RANDOM_PIPELINE_RING_CAPACITY];
} SQstRandomSqlRing;

typedef struct SQstRandomPipeline SQstRandomPipeline;

typedef struct {
  SQstRandomPipeline *pipeline;
  uint32_t slot;
} SQstRandomPipelineBuilder;

struct SQstRandomPipeline {
  SQstShmMapping *mapping;
  uint32_t generation;
  const SQstGeneratorFragPlan *plan;
  SQstDigest256 dataset_digest;
  SQstGenerationState *state;
  int32_t profile_enabled;
  uint32_t builder_count;
  SQstGeneratorRandomRuntime runtimes[QST_RANDOM_PIPELINE_MAX_BUILDER_COUNT];
  SQstRandomStreamRing random_rings[QST_RANDOM_PIPELINE_MAX_BUILDER_COUNT];
  SQstRandomSqlRing sql_rings[QST_RANDOM_PIPELINE_MAX_BUILDER_COUNT];
  SQstRandomPipelineBuilder builders[QST_RANDOM_PIPELINE_MAX_BUILDER_COUNT];
  uint64_t first_generation_seq;
  pthread_t random_thread;
  pthread_t build_threads[QST_RANDOM_PIPELINE_MAX_BUILDER_COUNT];
  pthread_t publish_thread;
  pthread_mutex_t progress_mutex;
  pthread_cond_t random_item_conds[QST_RANDOM_PIPELINE_MAX_BUILDER_COUNT];
  pthread_cond_t random_space_conds[QST_RANDOM_PIPELINE_MAX_BUILDER_COUNT];
  pthread_cond_t sql_item_conds[QST_RANDOM_PIPELINE_MAX_BUILDER_COUNT];
  pthread_cond_t sql_space_conds[QST_RANDOM_PIPELINE_MAX_BUILDER_COUNT];
  uint32_t progress_condition_count;
  int32_t progress_initialized;
  _Atomic int32_t stopping;
  _Atomic int32_t producer_done;
  _Atomic uint32_t builder_done_count;
  _Atomic int32_t error_code;
  _Atomic uint64_t random_ring_produced;
  _Atomic uint64_t random_ring_consumed;
  _Atomic uint64_t random_ring_full_wait_ns;
  _Atomic uint64_t random_ring_empty_wait_count;
  _Atomic uint64_t random_ring_empty_wait_ns;
  _Atomic uint64_t frag_plan_sql_completed;
  _Atomic uint64_t frag_plan_ns;
  _Atomic uint64_t resource_prepare_count;
  _Atomic uint64_t resource_prepare_ns;
  _Atomic uint64_t sql_ready_published;
  _Atomic uint64_t sql_ready_full_wait_ns;
  _Atomic uint64_t sql_pending_wait_count;
  _Atomic uint64_t sql_pending_wait_ns;
  _Atomic uint64_t publish_not_ready_wait_count;
  _Atomic uint64_t publish_not_ready_wait_ns;
  _Atomic uint64_t publish_queue_full_wait_count;
  _Atomic uint64_t publish_queue_full_wait_ns;
  _Atomic uint64_t publish_ns;
  _Atomic uint64_t publish_gate_calls;
  _Atomic uint64_t publish_gate_ready;
  _Atomic uint64_t publish_gate_task_ledger_empty;
  _Atomic uint64_t publish_gate_activation_window_full;
  _Atomic uint64_t sql_pending_empty;
  _Atomic uint64_t publish_gate_ns;
  _Atomic uint64_t publish_business_ns;
  _Atomic uint64_t publish_flow_ns;
};

typedef int32_t (*QstRandomPipelineReady)(const void *arg);

static int32_t qstRandomPipelineRingIsPowerOfTwo(uint32_t builder_count) {
  return QST_RANDOM_PIPELINE_RING_CAPACITY != 0 &&
         (QST_RANDOM_PIPELINE_RING_CAPACITY &
          (QST_RANDOM_PIPELINE_RING_CAPACITY - 1U)) == 0 &&
         builder_count != 0 &&
         builder_count <= QST_RANDOM_PIPELINE_MAX_BUILDER_COUNT &&
         (builder_count & (builder_count - 1U)) == 0;
}

static int32_t qstRandomStreamRingHasSpace(const void *arg) {
  const SQstRandomStreamRing *ring = arg;
  uint32_t write_index = 0;
  uint32_t read_index = 0;

  if (NULL == ring) {
    return 0;
  }
  write_index = atomic_load_explicit(&ring->write_index, memory_order_acquire);
  read_index = atomic_load_explicit(&ring->read_index, memory_order_acquire);
  return write_index - read_index < QST_RANDOM_PIPELINE_RING_CAPACITY;
}

static int32_t qstRandomStreamRingHasItem(const void *arg) {
  const SQstRandomStreamRing *ring = arg;

  if (NULL == ring) {
    return 0;
  }
  return atomic_load_explicit(&ring->read_index, memory_order_acquire) !=
         atomic_load_explicit(&ring->write_index, memory_order_acquire);
}

static int32_t qstRandomSqlRingHasSpace(const void *arg) {
  const SQstRandomSqlRing *ring = arg;
  uint32_t write_index = 0;
  uint32_t read_index = 0;

  if (NULL == ring) {
    return 0;
  }
  write_index = atomic_load_explicit(&ring->write_index, memory_order_acquire);
  read_index = atomic_load_explicit(&ring->read_index, memory_order_acquire);
  return write_index - read_index < QST_RANDOM_PIPELINE_RING_CAPACITY;
}

static int32_t qstRandomSqlRingHasItem(const void *arg) {
  const SQstRandomSqlRing *ring = arg;
  uint32_t read_index = 0;
  uint32_t write_index = 0;

  if (NULL == ring) {
    return 0;
  }
  read_index = atomic_load_explicit(&ring->read_index, memory_order_acquire);
  write_index = atomic_load_explicit(&ring->write_index, memory_order_acquire);
  return read_index != write_index;
}

/* Waits with a predicate recheck so a state transition before cond_wait is not
 * lost. Each ring has separate data and space conditions, which prevents a
 * routine SPSC handoff from waking unrelated builders. */
static EQstCode qstRandomPipelineWaitFor(SQstRandomPipeline *pipeline,
                                         QstRandomPipelineReady ready,
                                         const void *ready_arg,
                                         pthread_cond_t *condition) {
  int32_t wait_code = 0;

  if (NULL == pipeline || NULL == ready || NULL == ready_arg ||
      NULL == condition || pipeline->progress_initialized == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (pthread_mutex_lock(&pipeline->progress_mutex) != 0) {
    return QST_E_RESOURCE;
  }
  while (atomic_load_explicit(&pipeline->stopping, memory_order_acquire) == 0 &&
         ready(ready_arg) == 0) {
    wait_code = pthread_cond_wait(condition, &pipeline->progress_mutex);
    if (wait_code != 0) {
      break;
    }
  }
  if (pthread_mutex_unlock(&pipeline->progress_mutex) != 0 || wait_code != 0) {
    return QST_E_RESOURCE;
  }
  return QST_OK;
}

static void qstRandomPipelineNotify(SQstRandomPipeline *pipeline,
                                    pthread_cond_t *condition) {
  if (NULL == pipeline || NULL == condition ||
      pipeline->progress_initialized == 0 ||
      pthread_mutex_lock(&pipeline->progress_mutex) != 0) {
    return;
  }
  (void)pthread_cond_signal(condition);
  (void)pthread_mutex_unlock(&pipeline->progress_mutex);
}

static void qstRandomPipelineNotifyAll(SQstRandomPipeline *pipeline) {
  if (NULL == pipeline || pipeline->progress_initialized == 0 ||
      pthread_mutex_lock(&pipeline->progress_mutex) != 0) {
    return;
  }
  for (uint32_t slot = 0; slot < pipeline->progress_condition_count; ++slot) {
    (void)pthread_cond_broadcast(&pipeline->random_item_conds[slot]);
    (void)pthread_cond_broadcast(&pipeline->random_space_conds[slot]);
    (void)pthread_cond_broadcast(&pipeline->sql_item_conds[slot]);
    (void)pthread_cond_broadcast(&pipeline->sql_space_conds[slot]);
  }
  (void)pthread_mutex_unlock(&pipeline->progress_mutex);
}

static void qstRandomPipelineStop(SQstRandomPipeline *pipeline,
                                  EQstCode error_code) {
  int32_t expected = QST_OK;

  if (NULL == pipeline) {
    return;
  }
  if (error_code != QST_OK) {
    (void)atomic_compare_exchange_strong_explicit(
        &pipeline->error_code, &expected, error_code, memory_order_acq_rel,
        memory_order_acquire);
  }
  atomic_store_explicit(&pipeline->stopping, 1, memory_order_release);
  qstRandomPipelineNotifyAll(pipeline);
}

static void qstRandomPipelineRecordPublishGate(
    SQstRandomPipeline *pipeline, EQstRandomPublishGate gate) {
  if (NULL == pipeline || pipeline->profile_enabled == 0) {
    return;
  }
  (void)atomic_fetch_add_explicit(&pipeline->publish_gate_calls, 1,
                                  memory_order_relaxed);
  switch (gate) {
    case QST_RANDOM_PUBLISH_GATE_READY:
      (void)atomic_fetch_add_explicit(&pipeline->publish_gate_ready, 1,
                                      memory_order_relaxed);
      break;
    case QST_RANDOM_PUBLISH_GATE_TASK_LEDGER_EMPTY:
      (void)atomic_fetch_add_explicit(
          &pipeline->publish_gate_task_ledger_empty, 1, memory_order_relaxed);
      break;
    case QST_RANDOM_PUBLISH_GATE_ACTIVATION_WINDOW_FULL:
      (void)atomic_fetch_add_explicit(
          &pipeline->publish_gate_activation_window_full, 1,
          memory_order_relaxed);
      break;
    case QST_RANDOM_PUBLISH_GATE_STOP:
      break;
    default:
      break;
  }
}

static int32_t qstRandomStreamRingTryPush(SQstRandomStreamRing *ring,
                                          const SQstRandomStreamItem *item) {
  uint32_t write_index = 0;
  uint32_t read_index = 0;

  if (NULL == ring || NULL == item) {
    return 0;
  }
  write_index = atomic_load_explicit(&ring->write_index, memory_order_relaxed);
  read_index = atomic_load_explicit(&ring->read_index, memory_order_acquire);
  if (write_index - read_index >= QST_RANDOM_PIPELINE_RING_CAPACITY) {
    return 0;
  }
  ring->items[write_index & (QST_RANDOM_PIPELINE_RING_CAPACITY - 1U)] = *item;
  atomic_store_explicit(&ring->write_index, write_index + 1U,
                        memory_order_release);
  return 1;
}

static int32_t qstRandomStreamRingTryPop(SQstRandomStreamRing *ring,
                                         SQstRandomStreamItem *out_item) {
  uint32_t read_index = 0;
  uint32_t write_index = 0;

  if (NULL == ring || NULL == out_item) {
    return 0;
  }
  read_index = atomic_load_explicit(&ring->read_index, memory_order_relaxed);
  write_index = atomic_load_explicit(&ring->write_index, memory_order_acquire);
  if (read_index == write_index) {
    return 0;
  }
  *out_item = ring->items[read_index & (QST_RANDOM_PIPELINE_RING_CAPACITY - 1U)];
  atomic_store_explicit(&ring->read_index, read_index + 1U,
                        memory_order_release);
  return 1;
}

static SQstGeneratorRandomPending *qstRandomSqlRingWriteSlot(
    SQstRandomSqlRing *ring) {
  uint32_t write_index = 0;
  uint32_t read_index = 0;

  if (NULL == ring) {
    return NULL;
  }
  write_index = atomic_load_explicit(&ring->write_index, memory_order_relaxed);
  read_index = atomic_load_explicit(&ring->read_index, memory_order_acquire);
  if (write_index - read_index >= QST_RANDOM_PIPELINE_RING_CAPACITY) {
    return NULL;
  }
  return &ring->items[write_index & (QST_RANDOM_PIPELINE_RING_CAPACITY - 1U)];
}

static void qstRandomSqlRingCommitWrite(SQstRandomSqlRing *ring) {
  uint32_t write_index = 0;

  if (NULL == ring) {
    return;
  }
  write_index = atomic_load_explicit(&ring->write_index, memory_order_relaxed);
  atomic_store_explicit(&ring->write_index, write_index + 1U,
                        memory_order_release);
}

static SQstGeneratorRandomPending *qstRandomSqlRingPeek(SQstRandomSqlRing *ring) {
  uint32_t read_index = 0;
  uint32_t write_index = 0;

  if (NULL == ring) {
    return NULL;
  }
  read_index = atomic_load_explicit(&ring->read_index, memory_order_relaxed);
  write_index = atomic_load_explicit(&ring->write_index, memory_order_acquire);
  if (read_index == write_index) {
    return NULL;
  }
  return &ring->items[read_index & (QST_RANDOM_PIPELINE_RING_CAPACITY - 1U)];
}

static void qstRandomSqlRingDrop(SQstRandomSqlRing *ring) {
  uint32_t read_index = 0;

  if (NULL == ring) {
    return;
  }
  read_index = atomic_load_explicit(&ring->read_index, memory_order_relaxed);
  atomic_store_explicit(&ring->read_index, read_index + 1U,
                        memory_order_release);
}

static void *qstRandomPipelineProduce(void *arg) {
  SQstRandomPipeline *pipeline = arg;
  uint64_t generation_seq = 0;

  if (NULL == pipeline || NULL == pipeline->state) {
    return NULL;
  }
  generation_seq = atomic_load_explicit(&pipeline->state->random_generation_seq,
                                        memory_order_acquire);
  while (atomic_load_explicit(&pipeline->stopping, memory_order_acquire) == 0) {
    SQstRandomStreamItem item = {0};

    if (generation_seq == 0 || generation_seq == UINT64_MAX ||
        (pipeline->state->statement_limit != 0 &&
         generation_seq > pipeline->state->statement_limit)) {
      break;
    }
    item.generation_seq = generation_seq;
    if (qst_generator_random_init(&item.random_state,
                                  pipeline->state->random_seed,
                                  generation_seq) != QST_OK) {
      qstRandomPipelineStop(pipeline, QST_E_RESOURCE);
      break;
    }
    const uint32_t builder_slot = (uint32_t)((generation_seq -
                                              pipeline->first_generation_seq) &
                                             (pipeline->builder_count - 1U));

    while (!qstRandomStreamRingTryPush(&pipeline->random_rings[builder_slot],
                                       &item)) {
      uint64_t started_ns =
          pipeline->profile_enabled != 0 ? qstMonotonicNowNs() : 0;

      if (atomic_load_explicit(&pipeline->stopping, memory_order_acquire) != 0) {
        break;
      }
      if (qstRandomPipelineWaitFor(pipeline, qstRandomStreamRingHasSpace,
                                   &pipeline->random_rings[builder_slot],
                                   &pipeline->random_space_conds[builder_slot]) !=
          QST_OK) {
        qstRandomPipelineStop(pipeline, QST_E_RESOURCE);
        break;
      }
      if (started_ns != 0) {
        const uint64_t finished_ns = qstMonotonicNowNs();

        if (finished_ns >= started_ns) {
          (void)atomic_fetch_add_explicit(&pipeline->random_ring_full_wait_ns,
                                          finished_ns - started_ns,
                                          memory_order_relaxed);
        }
      }
    }
    if (atomic_load_explicit(&pipeline->stopping, memory_order_acquire) != 0) {
      break;
    }
    qstRandomPipelineNotify(pipeline,
                            &pipeline->random_item_conds[builder_slot]);
    if (pipeline->profile_enabled != 0) {
      (void)atomic_fetch_add_explicit(&pipeline->random_ring_produced, 1,
                                      memory_order_relaxed);
    }
    ++generation_seq;
  }
  atomic_store_explicit(&pipeline->producer_done, 1, memory_order_release);
  qstRandomPipelineNotifyAll(pipeline);
  return NULL;
}

static void *qstRandomPipelineBuild(void *arg) {
  SQstRandomPipelineBuilder *builder = arg;
  SQstRandomPipeline *pipeline = NULL;
  uint32_t slot = 0;

  if (NULL == builder || NULL == builder->pipeline ||
      builder->slot >= builder->pipeline->builder_count) {
    return NULL;
  }
  pipeline = builder->pipeline;
  slot = builder->slot;
  while (atomic_load_explicit(&pipeline->stopping, memory_order_acquire) == 0) {
    SQstRandomStreamItem item = {0};
    SQstGeneratorRandomPending *pending = NULL;
    uint64_t started_ns = 0;
    EQstCode code = QST_OK;

    if (!qstRandomStreamRingTryPop(&pipeline->random_rings[slot], &item)) {
      uint64_t wait_started_ns = 0;

      if (atomic_load_explicit(&pipeline->producer_done, memory_order_acquire) != 0) {
        break;
      }
      if (pipeline->profile_enabled != 0) {
        wait_started_ns = qstMonotonicNowNs();
      }
      if (qstRandomPipelineWaitFor(pipeline, qstRandomStreamRingHasItem,
                                   &pipeline->random_rings[slot],
                                   &pipeline->random_item_conds[slot]) != QST_OK) {
        qstRandomPipelineStop(pipeline, QST_E_RESOURCE);
        break;
      }
      if (pipeline->profile_enabled != 0 && wait_started_ns != 0 &&
          qstMonotonicNowNs() >= wait_started_ns) {
        (void)atomic_fetch_add_explicit(&pipeline->random_ring_empty_wait_count,
                                        1, memory_order_relaxed);
        (void)atomic_fetch_add_explicit(&pipeline->random_ring_empty_wait_ns,
                                        qstMonotonicNowNs() - wait_started_ns,
                                        memory_order_relaxed);
      }
      continue;
    }
    qstRandomPipelineNotify(pipeline, &pipeline->random_space_conds[slot]);
    while ((pending = qstRandomSqlRingWriteSlot(&pipeline->sql_rings[slot])) ==
           NULL) {
      uint64_t wait_started_ns = pipeline->profile_enabled != 0 ?
                                     qstMonotonicNowNs() :
                                     0;

      if (atomic_load_explicit(&pipeline->stopping, memory_order_acquire) != 0) {
        break;
      }
      if (qstRandomPipelineWaitFor(pipeline, qstRandomSqlRingHasSpace,
                                   &pipeline->sql_rings[slot],
                                   &pipeline->sql_space_conds[slot]) != QST_OK) {
        qstRandomPipelineStop(pipeline, QST_E_RESOURCE);
        break;
      }
      if (pipeline->profile_enabled != 0 && wait_started_ns != 0) {
        const uint64_t wait_finished_ns = qstMonotonicNowNs();

        if (wait_finished_ns >= wait_started_ns) {
          (void)atomic_fetch_add_explicit(&pipeline->sql_ready_full_wait_ns,
                                          wait_finished_ns - wait_started_ns,
                                          memory_order_relaxed);
        }
      }
    }
    if (NULL == pending ||
        atomic_load_explicit(&pipeline->stopping, memory_order_acquire) != 0) {
      break;
    }
    started_ns = pipeline->profile_enabled != 0 ? qstMonotonicNowNs() : 0;
    code = qst_generator_random_plan_build_from_state(
        pipeline->mapping, QST_ROLE_GENERATOR, pipeline->generation, pipeline->plan,
        pipeline->dataset_digest, 1, item.generation_seq, &item.random_state,
        &pipeline->runtimes[slot], pending);
    if (pipeline->profile_enabled != 0 &&
        (started_ns == 0 || qstMonotonicNowNs() < started_ns)) {
      qst_generator_random_pending_destroy(pending);
      qstRandomPipelineStop(pipeline, QST_E_RESOURCE);
      break;
    }
    if (pipeline->profile_enabled != 0) {
      (void)atomic_fetch_add_explicit(&pipeline->frag_plan_ns,
                                      qstMonotonicNowNs() - started_ns,
                                      memory_order_relaxed);
    }
    if (code != QST_OK) {
      qst_generator_random_pending_destroy(pending);
      qstRandomPipelineStop(pipeline, code);
      break;
    }
    if (atomic_load_explicit(&pipeline->stopping, memory_order_acquire) != 0) {
      (void)qst_generator_random_release_resources(pipeline->mapping, pending);
      break;
    }
    /* Shared block ownership is prepared by the builder while the SQL is
     * already resident in its SPSC slot. Under normal pressure persistence
     * releases blocks asynchronously, so retry only transient budget misses. */
    for (;;) {
      const uint64_t next_publish_seq = atomic_load_explicit(
          &pipeline->state->random_generation_seq, memory_order_acquire);

      if (next_publish_seq == 0 || next_publish_seq == UINT64_MAX) {
        code = QST_E_SHM_CORRUPT;
        break;
      }
      if (item.generation_seq >= next_publish_seq &&
          item.generation_seq - next_publish_seq >=
              QST_RANDOM_RESOURCE_PREPARE_WINDOW) {
        const struct timespec wait = {0, 1000000};

        if (atomic_load_explicit(&pipeline->stopping, memory_order_acquire) != 0) {
          code = QST_E_STOPPING;
          break;
        }
        (void)nanosleep(&wait, NULL);
        continue;
      }
      {
        const uint64_t prepare_started_ns =
            pipeline->profile_enabled != 0 ? qstMonotonicNowNs() : 0;

        code = qst_generator_random_prepare_resources(
            pipeline->mapping, QST_ROLE_GENERATOR, pipeline->generation, pending);
        if (pipeline->profile_enabled != 0 && prepare_started_ns != 0) {
          const uint64_t prepare_finished_ns = qstMonotonicNowNs();

          if (prepare_finished_ns >= prepare_started_ns) {
            (void)atomic_fetch_add_explicit(&pipeline->resource_prepare_count,
                                            1, memory_order_relaxed);
            (void)atomic_fetch_add_explicit(
                &pipeline->resource_prepare_ns,
                prepare_finished_ns - prepare_started_ns, memory_order_relaxed);
          }
        }
      }
      if (code != QST_E_QUEUE_FULL && code != QST_E_BLOCK_NO_SPACE) {
        break;
      }
      if (atomic_load_explicit(&pipeline->stopping, memory_order_acquire) != 0) {
        break;
      }
      {
        const struct timespec wait = {0, 1000000};

        (void)nanosleep(&wait, NULL);
      }
    }
    if (code != QST_OK) {
      (void)qst_generator_random_release_resources(pipeline->mapping, pending);
      qst_generator_random_pending_destroy(pending);
      if (atomic_load_explicit(&pipeline->stopping, memory_order_acquire) == 0) {
        qstRandomPipelineStop(pipeline, code);
      }
      break;
    }
    qstRandomSqlRingCommitWrite(&pipeline->sql_rings[slot]);
    qstRandomPipelineNotify(pipeline, &pipeline->sql_item_conds[slot]);
    if (pipeline->profile_enabled != 0) {
      (void)atomic_fetch_add_explicit(&pipeline->random_ring_consumed, 1,
                                      memory_order_relaxed);
      (void)atomic_fetch_add_explicit(&pipeline->frag_plan_sql_completed, 1,
                                      memory_order_relaxed);
    }
  }
  (void)atomic_fetch_add_explicit(&pipeline->builder_done_count, 1,
                                  memory_order_release);
  qstRandomPipelineNotifyAll(pipeline);
  return NULL;
}

static void *qstRandomPipelinePublish(void *arg) {
  SQstRandomPipeline *pipeline = arg;
  struct pollfd event = {0};
  uint64_t published_count = 0;

  if (NULL == pipeline || NULL == pipeline->mapping || NULL == pipeline->state) {
    return NULL;
  }
  event.fd = pipeline->mapping->queue_eventfd[QST_QUEUE_GENERATE - 1];
  event.events = POLLIN;
  while (atomic_load_explicit(&pipeline->stopping, memory_order_acquire) == 0) {
    uint64_t expected_generation_seq = atomic_load_explicit(
        &pipeline->state->random_generation_seq, memory_order_acquire);
    SQstGeneratorRandomPending *pending = NULL;
    EQstStopReason stop_reason = QST_STOP_NONE;
    EQstRandomPublishGate publish_gate = QST_RANDOM_PUBLISH_GATE_READY;
    int32_t can_publish = 0;
    int32_t stop_acknowledged = 0;
    uint64_t gate_started_ns = 0;
    uint64_t started_ns = 0;
    uint64_t gid = 0;
    EQstCode code = QST_OK;

    if (pipeline->first_generation_seq == 0 || expected_generation_seq == 0) {
      qstRandomPipelineStop(pipeline, QST_E_SHM_CORRUPT);
      break;
    }
    pending = qstRandomSqlRingPeek(
        &pipeline->sql_rings[(expected_generation_seq -
                              pipeline->first_generation_seq) &
                             (pipeline->builder_count - 1U)]);

    code = qstGeneratorCheckStopRequest(pipeline->mapping, pipeline->generation,
                                        &stop_acknowledged);
    if (code != QST_OK || stop_acknowledged != 0) {
      qstRandomPipelineStop(pipeline, code != QST_OK ? code : QST_E_STOPPING);
      break;
    }
    gate_started_ns = qstMonotonicNowNs();
    code = qst_generator_random_can_publish(pipeline->mapping, gate_started_ns,
                                            &can_publish, &stop_reason,
                                            &publish_gate);
    if (pipeline->profile_enabled != 0 && gate_started_ns != 0 &&
        qstMonotonicNowNs() >= gate_started_ns) {
      (void)atomic_fetch_add_explicit(&pipeline->publish_gate_ns,
                                      qstMonotonicNowNs() - gate_started_ns,
                                      memory_order_relaxed);
    }
    if (code != QST_OK) {
      qstRandomPipelineStop(pipeline, code);
      break;
    }
    if (stop_reason != QST_STOP_NONE) {
      atomic_store_explicit(&pipeline->mapping->header->stop_reason, stop_reason,
                            memory_order_release);
      qstRandomPipelineStop(pipeline, QST_OK);
      break;
    }
    qstRandomPipelineRecordPublishGate(pipeline, publish_gate);
    if (NULL == pending && pipeline->profile_enabled != 0) {
      (void)atomic_fetch_add_explicit(&pipeline->sql_pending_empty, 1,
                                      memory_order_relaxed);
    }
    if (NULL == pending || can_publish == 0) {
      if (NULL == pending && atomic_load_explicit(&pipeline->builder_done_count,
                                                   memory_order_acquire) ==
                                 pipeline->builder_count) {
        qstRandomPipelineStop(pipeline, QST_E_RESOURCE);
        break;
      }
      if (can_publish == 0) {
        uint64_t wait_started_ns = 0;

        code = qstConfirmQueueFlow(pipeline->mapping, QST_ROLE_GENERATOR,
                                   pipeline->generation, QST_FLOW_STAGE_IDLE,
                                   QST_QUEUE_GENERATE, 0, 0);
        if (code != QST_OK && code != QST_E_STALE_OWNER) {
          qstRandomPipelineStop(pipeline, code);
          break;
        }
        if (pipeline->profile_enabled != 0) {
          wait_started_ns = qstMonotonicNowNs();
        }
        if (qstPollQueueEvent(&event, 20) != QST_OK) {
          qstRandomPipelineStop(pipeline, QST_E_RESOURCE);
          break;
        }
        if (pipeline->profile_enabled != 0 && wait_started_ns != 0 &&
            qstMonotonicNowNs() >= wait_started_ns) {
          (void)atomic_fetch_add_explicit(&pipeline->publish_not_ready_wait_count,
                                          1, memory_order_relaxed);
          (void)atomic_fetch_add_explicit(&pipeline->publish_not_ready_wait_ns,
                                          qstMonotonicNowNs() - wait_started_ns,
                                          memory_order_relaxed);
        }
      } else {
        uint64_t wait_started_ns = 0;

        if (pipeline->profile_enabled != 0) {
          wait_started_ns = qstMonotonicNowNs();
        }
        if (qstRandomPipelineWaitFor(
                pipeline, qstRandomSqlRingHasItem,
                &pipeline->sql_rings[(expected_generation_seq -
                                      pipeline->first_generation_seq) &
                                     (pipeline->builder_count - 1U)],
                &pipeline->sql_item_conds[(expected_generation_seq -
                                            pipeline->first_generation_seq) &
                                           (pipeline->builder_count - 1U)]) !=
            QST_OK) {
          qstRandomPipelineStop(pipeline, QST_E_RESOURCE);
          break;
        }
        if (pipeline->profile_enabled != 0 && wait_started_ns != 0 &&
            qstMonotonicNowNs() >= wait_started_ns) {
          (void)atomic_fetch_add_explicit(&pipeline->sql_pending_wait_count,
                                          1, memory_order_relaxed);
          (void)atomic_fetch_add_explicit(&pipeline->sql_pending_wait_ns,
                                          qstMonotonicNowNs() - wait_started_ns,
                                          memory_order_relaxed);
        }
      }
      continue;
    }
    if (pending->input.generation_seq != expected_generation_seq) {
      qstRandomPipelineStop(pipeline, QST_E_SHM_CORRUPT);
      break;
    }
    started_ns = pipeline->profile_enabled != 0 ? qstMonotonicNowNs() : 0;
    code = qst_generator_random_publish_prepared(
        pipeline->mapping, QST_ROLE_GENERATOR, pipeline->generation, pending,
        UINT64_MAX, &gid);
    if (pipeline->profile_enabled != 0 &&
        (started_ns == 0 || qstMonotonicNowNs() < started_ns)) {
      qstRandomPipelineStop(pipeline, QST_E_RESOURCE);
      break;
    }
    if (pipeline->profile_enabled != 0) {
      (void)atomic_fetch_add_explicit(&pipeline->publish_ns,
                                      qstMonotonicNowNs() - started_ns,
                                      memory_order_relaxed);
    }
    if (code == QST_E_QUEUE_FULL) {
      const uint64_t wait_started_ns = pipeline->profile_enabled != 0 ?
                                           qstMonotonicNowNs() :
                                           0;

      if (qstPollQueueEvent(&event, 20) != QST_OK) {
        qstRandomPipelineStop(pipeline, QST_E_RESOURCE);
        break;
      }
      if (pipeline->profile_enabled != 0 && wait_started_ns != 0 &&
          qstMonotonicNowNs() >= wait_started_ns) {
        (void)atomic_fetch_add_explicit(&pipeline->publish_queue_full_wait_count,
                                        1, memory_order_relaxed);
        (void)atomic_fetch_add_explicit(&pipeline->publish_queue_full_wait_ns,
                                        qstMonotonicNowNs() - wait_started_ns,
                                        memory_order_relaxed);
      }
      continue;
    }
    if (code != QST_OK) {
      qstRandomPipelineStop(pipeline, code);
      break;
    }
    qstRandomSqlRingDrop(
        &pipeline->sql_rings[(expected_generation_seq -
                              pipeline->first_generation_seq) &
                             (pipeline->builder_count - 1U)]);
    qstRandomPipelineNotify(
        pipeline,
        &pipeline->sql_space_conds[(expected_generation_seq -
                                    pipeline->first_generation_seq) &
                                   (pipeline->builder_count - 1U)]);
    {
      if (published_count != UINT64_MAX) {
        ++published_count;
      }
      if (pipeline->profile_enabled != 0) {
        (void)atomic_fetch_add_explicit(&pipeline->sql_ready_published, 1,
                                        memory_order_relaxed);
      }

      started_ns = pipeline->profile_enabled != 0 ? qstMonotonicNowNs() : 0;
      code = qst_process_record_business(
          pipeline->mapping, QST_ROLE_GENERATOR, pipeline->generation, gid,
          qstMillisecondsToDeadlineNs(QST_FLOW_LEASE_MILLISECONDS));
      if (pipeline->profile_enabled != 0 && started_ns != 0 &&
          qstMonotonicNowNs() >= started_ns) {
        (void)atomic_fetch_add_explicit(&pipeline->publish_business_ns,
                                        qstMonotonicNowNs() - started_ns,
                                        memory_order_relaxed);
      }
      if (code == QST_OK &&
          (published_count % pipeline->builder_count) == 0) {
        started_ns =
            pipeline->profile_enabled != 0 ? qstMonotonicNowNs() : 0;
        code = qstConfirmQueueFlow(pipeline->mapping, QST_ROLE_GENERATOR,
                                   pipeline->generation, QST_FLOW_STAGE_IDLE,
                                       QST_QUEUE_GENERATE, gid,
                                       qstMillisecondsToDeadlineNs(
                                           QST_FLOW_LEASE_MILLISECONDS));
        if (pipeline->profile_enabled != 0 && started_ns != 0 &&
            qstMonotonicNowNs() >= started_ns) {
          (void)atomic_fetch_add_explicit(&pipeline->publish_flow_ns,
                                          qstMonotonicNowNs() - started_ns,
                                          memory_order_relaxed);
        }
      }
    }
    if (code == QST_OK) {
      code = qstTestPublishGeneratorActivity();
    }
    if (code != QST_OK) {
      qstRandomPipelineStop(pipeline, code);
      break;
    }
  }
  return NULL;
}

static void qstRandomPipelineDestroyPending(SQstRandomPipeline *pipeline) {
  if (NULL == pipeline) {
    return;
  }
  for (uint32_t slot = 0; slot < QST_RANDOM_PIPELINE_MAX_BUILDER_COUNT;
       ++slot) {
    for (uint32_t index = 0; index < QST_RANDOM_PIPELINE_RING_CAPACITY;
         ++index) {
      (void)qst_generator_random_release_resources(
          pipeline->mapping, &pipeline->sql_rings[slot].items[index]);
      qst_generator_random_pending_destroy(&pipeline->sql_rings[slot].items[index]);
    }
    qst_generator_random_runtime_destroy(&pipeline->runtimes[slot]);
  }
}

static EQstCode qstRandomPipelineRun(SQstShmMapping *mapping,
                                     uint32_t generation,
                                     SQstGenerationState *state,
                                     const SQstGeneratorFragPlan *plan,
                                     SQstDigest256 dataset_digest,
                                     int32_t profile_enabled) {
  SQstRandomPipeline *pipeline = NULL;
  uint32_t random_started = 0;
  uint32_t builder_started = 0;
  uint32_t publisher_started = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == state || NULL == plan ||
      !qstRandomPipelineRingIsPowerOfTwo(
          state->random_builder_count == 0 ?
              QST_RANDOM_PIPELINE_BUILDER_COUNT :
              state->random_builder_count)) {
    return QST_E_INVALID_ARGUMENT;
  }
  pipeline = calloc(1, sizeof(*pipeline));
  if (NULL == pipeline) {
    return QST_E_RESOURCE;
  }
  pipeline->mapping = mapping;
  pipeline->generation = generation;
  pipeline->state = state;
  pipeline->profile_enabled = profile_enabled;
  pipeline->builder_count = state->random_builder_count == 0 ?
                               QST_RANDOM_PIPELINE_BUILDER_COUNT :
                               state->random_builder_count;
  for (uint32_t slot = 0; slot < pipeline->builder_count; ++slot) {
    pipeline->runtimes[slot].profile_enabled = profile_enabled;
    pipeline->runtimes[slot].disable_path_metadata = 1;
  }
  pipeline->plan = plan;
  pipeline->dataset_digest = dataset_digest;
  pipeline->first_generation_seq = atomic_load_explicit(
      &state->random_generation_seq, memory_order_acquire);
  if (pipeline->first_generation_seq == 0 ||
      pipeline->first_generation_seq == UINT64_MAX) {
    free(pipeline);
    return QST_E_RESOURCE;
  }
  if (pthread_mutex_init(&pipeline->progress_mutex, NULL) != 0) {
    free(pipeline);
    return QST_E_RESOURCE;
  }
  for (uint32_t slot = 0; slot < pipeline->builder_count; ++slot) {
    int32_t random_item_initialized = 0;
    int32_t random_space_initialized = 0;
    int32_t sql_item_initialized = 0;

    if (pthread_cond_init(&pipeline->random_item_conds[slot], NULL) == 0) {
      random_item_initialized = 1;
    }
    if (random_item_initialized != 0 &&
        pthread_cond_init(&pipeline->random_space_conds[slot], NULL) == 0) {
      random_space_initialized = 1;
    }
    if (random_space_initialized != 0 &&
        pthread_cond_init(&pipeline->sql_item_conds[slot], NULL) == 0) {
      sql_item_initialized = 1;
    }
    if (sql_item_initialized == 0 ||
        pthread_cond_init(&pipeline->sql_space_conds[slot], NULL) != 0) {
      if (sql_item_initialized != 0) {
        (void)pthread_cond_destroy(&pipeline->sql_item_conds[slot]);
      }
      if (random_space_initialized != 0) {
        (void)pthread_cond_destroy(&pipeline->random_space_conds[slot]);
      }
      if (random_item_initialized != 0) {
        (void)pthread_cond_destroy(&pipeline->random_item_conds[slot]);
      }
      for (uint32_t initialized = 0;
           initialized < pipeline->progress_condition_count; ++initialized) {
        (void)pthread_cond_destroy(&pipeline->random_item_conds[initialized]);
        (void)pthread_cond_destroy(&pipeline->random_space_conds[initialized]);
        (void)pthread_cond_destroy(&pipeline->sql_item_conds[initialized]);
        (void)pthread_cond_destroy(&pipeline->sql_space_conds[initialized]);
      }
      (void)pthread_mutex_destroy(&pipeline->progress_mutex);
      free(pipeline);
      return QST_E_RESOURCE;
    }
    ++pipeline->progress_condition_count;
  }
  pipeline->progress_initialized = 1;
  atomic_init(&pipeline->error_code, QST_OK);
  if (pthread_create(&pipeline->random_thread, NULL, qstRandomPipelineProduce,
                     pipeline) != 0) {
    code = QST_E_RESOURCE;
    goto _return;
  }
  random_started = 1;
  for (uint32_t slot = 0; slot < pipeline->builder_count; ++slot) {
    pipeline->builders[slot].pipeline = pipeline;
    pipeline->builders[slot].slot = slot;
    if (pthread_create(&pipeline->build_threads[slot], NULL,
                       qstRandomPipelineBuild, &pipeline->builders[slot]) != 0) {
      code = QST_E_RESOURCE;
      goto _return;
    }
    ++builder_started;
  }
  if (pthread_create(&pipeline->publish_thread, NULL, qstRandomPipelinePublish,
                     pipeline) != 0) {
    code = QST_E_RESOURCE;
    goto _return;
  }
  publisher_started = 1;
_return:
  if (code != QST_OK) {
    qstRandomPipelineStop(pipeline, code);
  }
  if (random_started != 0) {
    (void)pthread_join(pipeline->random_thread, NULL);
  }
  for (uint32_t slot = 0; slot < builder_started; ++slot) {
    (void)pthread_join(pipeline->build_threads[slot], NULL);
  }
  if (publisher_started != 0) {
    (void)pthread_join(pipeline->publish_thread, NULL);
  }
  if (code == QST_OK) {
    code = atomic_load_explicit(&pipeline->error_code, memory_order_acquire);
  }
  if (profile_enabled != 0) {
    uint64_t runtime_initialize_count = 0;
    uint64_t runtime_initialize_ns = 0;
    uint64_t plan_walk_count = 0;
    uint64_t plan_walk_ns = 0;
    uint64_t plan_select_count = 0;
    uint64_t plan_select_ns = 0;
    uint64_t plan_select_value_examined = 0;
    uint64_t runtime_candidate_enumeration_count = 0;
    uint64_t runtime_candidate_enumeration_ns = 0;
    uint64_t runtime_append_count = 0;
    uint64_t runtime_append_ns = 0;

    for (uint32_t slot = 0; slot < pipeline->builder_count; ++slot) {
      const SQstGeneratorRandomRuntime *runtime = &pipeline->runtimes[slot];

      runtime_initialize_count += runtime->profile_runtime_initialize_count;
      runtime_initialize_ns += runtime->profile_runtime_initialize_ns;
      plan_walk_count += runtime->profile_plan_walk_count;
      plan_walk_ns += runtime->profile_plan_walk_ns;
      plan_select_count += runtime->profile_plan_select_count;
      plan_select_ns += runtime->profile_plan_select_ns;
      plan_select_value_examined += runtime->profile_plan_select_value_examined;
      runtime_candidate_enumeration_count +=
          runtime->profile_runtime_candidate_enumeration_count;
      runtime_candidate_enumeration_ns +=
          runtime->profile_runtime_candidate_enumeration_ns;
      runtime_append_count += runtime->profile_runtime_append_count;
      runtime_append_ns += runtime->profile_runtime_append_ns;
    }
    (void)fprintf(stderr,
                  "taosQueryTest: random pipeline profile random_ring_produced=%llu "
                  "random_ring_consumed=%llu random_ring_full_wait_ns=%llu "
                  "random_ring_empty_wait_count=%llu random_ring_empty_wait_ns=%llu "
                  "frag_plan_sql_completed=%llu frag_plan_ns=%llu "
                  "sql_ready_published=%llu sql_ready_full_wait_ns=%llu "
                  "sql_pending_wait_count=%llu sql_pending_wait_ns=%llu "
                  "publish_not_ready_wait_count=%llu publish_not_ready_wait_ns=%llu "
                  "publish_queue_full_wait_count=%llu publish_queue_full_wait_ns=%llu "
                  "publish_ns=%llu "
                  "publish_gate_calls=%llu publish_gate_ready=%llu "
                  "publish_gate_task_ledger_empty=%llu "
                  "publish_gate_activation_window_full=%llu sql_pending_empty=%llu "
                  "publish_gate_ns=%llu publish_business_ns=%llu publish_flow_ns=%llu\n",
                  (unsigned long long)atomic_load_explicit(
                      &pipeline->random_ring_produced, memory_order_relaxed),
                  (unsigned long long)atomic_load_explicit(
                      &pipeline->random_ring_consumed, memory_order_relaxed),
                  (unsigned long long)atomic_load_explicit(
                      &pipeline->random_ring_full_wait_ns, memory_order_relaxed),
                  (unsigned long long)atomic_load_explicit(
                      &pipeline->random_ring_empty_wait_count, memory_order_relaxed),
                  (unsigned long long)atomic_load_explicit(
                      &pipeline->random_ring_empty_wait_ns, memory_order_relaxed),
                  (unsigned long long)atomic_load_explicit(
                      &pipeline->frag_plan_sql_completed, memory_order_relaxed),
                  (unsigned long long)atomic_load_explicit(&pipeline->frag_plan_ns,
                                                          memory_order_relaxed),
                  (unsigned long long)atomic_load_explicit(
                      &pipeline->sql_ready_published, memory_order_relaxed),
                  (unsigned long long)atomic_load_explicit(
                      &pipeline->sql_ready_full_wait_ns, memory_order_relaxed),
                  (unsigned long long)atomic_load_explicit(
                      &pipeline->sql_pending_wait_count, memory_order_relaxed),
                  (unsigned long long)atomic_load_explicit(
                      &pipeline->sql_pending_wait_ns, memory_order_relaxed),
                  (unsigned long long)atomic_load_explicit(
                      &pipeline->publish_not_ready_wait_count,
                      memory_order_relaxed),
                  (unsigned long long)atomic_load_explicit(
                      &pipeline->publish_not_ready_wait_ns,
                      memory_order_relaxed),
                  (unsigned long long)atomic_load_explicit(
                      &pipeline->publish_queue_full_wait_count,
                      memory_order_relaxed),
                  (unsigned long long)atomic_load_explicit(
                      &pipeline->publish_queue_full_wait_ns,
                      memory_order_relaxed),
                  (unsigned long long)atomic_load_explicit(&pipeline->publish_ns,
                                                          memory_order_relaxed),
                  (unsigned long long)atomic_load_explicit(
                      &pipeline->publish_gate_calls, memory_order_relaxed),
                  (unsigned long long)atomic_load_explicit(
                      &pipeline->publish_gate_ready, memory_order_relaxed),
                  (unsigned long long)atomic_load_explicit(
                      &pipeline->publish_gate_task_ledger_empty,
                      memory_order_relaxed),
                  (unsigned long long)atomic_load_explicit(
                      &pipeline->publish_gate_activation_window_full,
                      memory_order_relaxed),
                  (unsigned long long)atomic_load_explicit(
                      &pipeline->sql_pending_empty, memory_order_relaxed),
                  (unsigned long long)atomic_load_explicit(
                      &pipeline->publish_gate_ns, memory_order_relaxed),
                  (unsigned long long)atomic_load_explicit(
                      &pipeline->publish_business_ns, memory_order_relaxed),
                  (unsigned long long)atomic_load_explicit(
                      &pipeline->publish_flow_ns, memory_order_relaxed));
    (void)fprintf(stderr,
                  "taosQueryTest: random_resource_profile count=%llu ns=%llu\n",
                  (unsigned long long)atomic_load_explicit(
                      &pipeline->resource_prepare_count, memory_order_relaxed),
                  (unsigned long long)atomic_load_explicit(
                      &pipeline->resource_prepare_ns, memory_order_relaxed));
    (void)fprintf(stderr,
                  "taosQueryTest: random_catalog_profile builders=%u "
                  "runtime_initialize_count=%llu runtime_initialize_ns=%llu "
                  "plan_walk_count=%llu plan_walk_ns=%llu "
                  "plan_select_count=%llu plan_select_ns=%llu "
                  "plan_select_value_examined=%llu "
                  "runtime_candidate_enumeration_count=%llu "
                  "runtime_candidate_enumeration_ns=%llu "
                  "runtime_append_count=%llu runtime_append_ns=%llu\n",
                  pipeline->builder_count,
                  (unsigned long long)runtime_initialize_count,
                  (unsigned long long)runtime_initialize_ns,
                  (unsigned long long)plan_walk_count,
                  (unsigned long long)plan_walk_ns,
                  (unsigned long long)plan_select_count,
                  (unsigned long long)plan_select_ns,
                  (unsigned long long)plan_select_value_examined,
                  (unsigned long long)runtime_candidate_enumeration_count,
                  (unsigned long long)runtime_candidate_enumeration_ns,
                  (unsigned long long)runtime_append_count,
                  (unsigned long long)runtime_append_ns);
  }
  qstRandomPipelineDestroyPending(pipeline);
  if (pipeline->progress_initialized != 0) {
    for (uint32_t slot = 0; slot < pipeline->progress_condition_count; ++slot) {
      (void)pthread_cond_destroy(&pipeline->random_item_conds[slot]);
      (void)pthread_cond_destroy(&pipeline->random_space_conds[slot]);
      (void)pthread_cond_destroy(&pipeline->sql_item_conds[slot]);
      (void)pthread_cond_destroy(&pipeline->sql_space_conds[slot]);
    }
    (void)pthread_mutex_destroy(&pipeline->progress_mutex);
  }
  free(pipeline);
  return code;
}

enum { QST_FULL_PIPELINE_PENDING_DEPTH = 2 };

static void qstFullPendingArrayDestroy(
    SQstGeneratorFullPending
        pending_by_target[QST_GENERATOR_TARGET_COUNT]
                         [QST_FULL_PIPELINE_PENDING_DEPTH]) {
  for (uint32_t target_index = 0;
       target_index < QST_GENERATOR_TARGET_COUNT; ++target_index) {
    for (uint32_t slot_index = 0;
         slot_index < QST_FULL_PIPELINE_PENDING_DEPTH; ++slot_index) {
      qst_generator_full_pending_destroy(
          &pending_by_target[target_index][slot_index]);
    }
  }
}

typedef struct SQstFullPipeline SQstFullPipeline;

typedef struct {
  SQstFullPipeline *pipeline;
  uint32_t target_index;
  uint32_t next_slot_index;
  uint32_t seed_ready;
  SQstGeneratorFullPathSeed successor_seed;
} SQstFullPipelineBuilder;

/* FULL builders own distinct target-local caches and two SPSC pending slots.
 * The publisher is deliberately the sole shared-state writer. */
struct SQstFullPipeline {
  SQstShmMapping *mapping;
  uint32_t generation;
  const SQstGeneratorCatalog *catalog;
  SQstDigest256 dataset_digest;
  uint32_t target_mask;
  SQstGeneratorFullPending
      pending[QST_GENERATOR_TARGET_COUNT][QST_FULL_PIPELINE_PENDING_DEPTH];
  SQstFullPipelineBuilder builders[QST_GENERATOR_TARGET_COUNT];
  pthread_t threads[QST_GENERATOR_TARGET_COUNT];
  pthread_mutex_t mutex;
  pthread_cond_t condition;
  int32_t synchronization_initialized;
  _Atomic int32_t stopping;
  _Atomic int32_t error_code;
  _Atomic uint32_t ready_count[QST_GENERATOR_TARGET_COUNT];
  uint32_t consume_slot_index[QST_GENERATOR_TARGET_COUNT];
  _Atomic uint32_t finished[QST_GENERATOR_TARGET_COUNT];
};

static void qstFullPipelineNotify(SQstFullPipeline *pipeline) {
  if (NULL == pipeline || pipeline->synchronization_initialized == 0 ||
      pthread_mutex_lock(&pipeline->mutex) != 0) {
    return;
  }
  (void)pthread_cond_broadcast(&pipeline->condition);
  (void)pthread_mutex_unlock(&pipeline->mutex);
}

static void qstFullPipelineStop(SQstFullPipeline *pipeline, EQstCode code) {
  int32_t expected = QST_OK;

  if (NULL == pipeline) {
    return;
  }
  if (code != QST_OK) {
    (void)atomic_compare_exchange_strong_explicit(
        &pipeline->error_code, &expected, code, memory_order_acq_rel,
        memory_order_acquire);
  }
  atomic_store_explicit(&pipeline->stopping, 1, memory_order_release);
  qstFullPipelineNotify(pipeline);
}

static void *qstFullPipelineBuild(void *arg) {
  SQstFullPipelineBuilder *builder = arg;
  SQstFullPipeline *pipeline = NULL;
  uint32_t target_index = 0;

  if (NULL == builder || NULL == builder->pipeline ||
      builder->target_index >= QST_GENERATOR_TARGET_COUNT) {
    return NULL;
  }
  pipeline = builder->pipeline;
  target_index = builder->target_index;
  if ((pipeline->target_mask & (UINT32_C(1) << target_index)) == 0) {
    atomic_store_explicit(&pipeline->finished[target_index], 1,
                          memory_order_release);
    qstFullPipelineNotify(pipeline);
    return NULL;
  }
  while (atomic_load_explicit(&pipeline->stopping, memory_order_acquire) == 0) {
    EQstCode code = QST_OK;
    uint32_t slot_index = 0;
    int32_t final_pending = 0;

    if (atomic_load_explicit(&pipeline->ready_count[target_index],
                             memory_order_acquire) ==
        QST_FULL_PIPELINE_PENDING_DEPTH) {
      if (pipeline->synchronization_initialized == 0 ||
          pthread_mutex_lock(&pipeline->mutex) != 0) {
        qstFullPipelineStop(pipeline, QST_E_RESOURCE);
        break;
      }
      while (atomic_load_explicit(&pipeline->stopping, memory_order_acquire) == 0 &&
             atomic_load_explicit(&pipeline->ready_count[target_index],
                                  memory_order_acquire) ==
                 QST_FULL_PIPELINE_PENDING_DEPTH) {
        if (pthread_cond_wait(&pipeline->condition, &pipeline->mutex) != 0) {
          (void)pthread_mutex_unlock(&pipeline->mutex);
          qstFullPipelineStop(pipeline, QST_E_RESOURCE);
          return NULL;
        }
      }
      if (pthread_mutex_unlock(&pipeline->mutex) != 0) {
        qstFullPipelineStop(pipeline, QST_E_RESOURCE);
        break;
      }
      continue;
    }
    slot_index = builder->next_slot_index;
    if (builder->seed_ready == 0) {
      code = qst_generator_full_prepare_pending_for_target(
          pipeline->mapping, QST_ROLE_GENERATOR, pipeline->generation,
          pipeline->catalog, pipeline->dataset_digest, target_index,
          &pipeline->pending[target_index][slot_index]);
    } else if (builder->successor_seed.exhausted != 0) {
      atomic_store_explicit(&pipeline->finished[target_index], 1,
                            memory_order_release);
      qstFullPipelineNotify(pipeline);
      break;
    } else {
      code = qst_generator_full_prepare_pending_from_seed(
          pipeline->mapping, QST_ROLE_GENERATOR, pipeline->generation,
          pipeline->catalog, pipeline->dataset_digest, &builder->successor_seed,
          &pipeline->pending[target_index][slot_index]);
    }
    if (code == QST_E_QUEUE_EMPTY) {
      atomic_store_explicit(&pipeline->finished[target_index], 1,
                            memory_order_release);
      qstFullPipelineNotify(pipeline);
      break;
    }
    if (code != QST_OK) {
      qstFullPipelineStop(pipeline, code);
      break;
    }
    code = qst_generator_full_pending_capture_successor(
        &pipeline->pending[target_index][slot_index], &builder->successor_seed);
    if (code == QST_E_QUEUE_EMPTY) {
      builder->seed_ready = 1;
      final_pending = 1;
    } else if (code != QST_OK) {
      qstFullPipelineStop(pipeline, code);
      break;
    } else {
      builder->seed_ready = 1;
    }
    builder->next_slot_index =
        (slot_index + 1U) % QST_FULL_PIPELINE_PENDING_DEPTH;
    atomic_fetch_add_explicit(&pipeline->ready_count[target_index], 1,
                              memory_order_release);
    if (final_pending != 0) {
      atomic_store_explicit(&pipeline->finished[target_index], 1,
                            memory_order_release);
    }
    qstFullPipelineNotify(pipeline);
  }
  return NULL;
}

static EQstCode qstFullPipelineStart(SQstFullPipeline *pipeline,
                                     SQstShmMapping *mapping,
                                     uint32_t generation,
                                     SQstGenerationState *state,
                                     const SQstGeneratorCatalog *catalog,
                                     SQstDigest256 dataset_digest) {
  uint32_t started = 0;

  if (NULL == pipeline || NULL == mapping || NULL == state || NULL == catalog ||
      generation == 0 || state->target_selection_bits == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  pipeline->mapping = mapping;
  pipeline->generation = generation;
  pipeline->catalog = catalog;
  pipeline->dataset_digest = dataset_digest;
  pipeline->target_mask = state->target_selection_bits;
  atomic_init(&pipeline->stopping, 0);
  atomic_init(&pipeline->error_code, QST_OK);
  for (uint32_t target_index = 0; target_index < QST_GENERATOR_TARGET_COUNT;
       ++target_index) {
    atomic_init(&pipeline->ready_count[target_index], 0);
    pipeline->consume_slot_index[target_index] = 0;
    atomic_init(&pipeline->finished[target_index], 0);
  }
  if (pthread_mutex_init(&pipeline->mutex, NULL) != 0) {
    return QST_E_RESOURCE;
  }
  if (pthread_cond_init(&pipeline->condition, NULL) != 0) {
    (void)pthread_mutex_destroy(&pipeline->mutex);
    return QST_E_RESOURCE;
  }
  pipeline->synchronization_initialized = 1;
  for (uint32_t target_index = 0; target_index < QST_GENERATOR_TARGET_COUNT;
       ++target_index) {
    pipeline->builders[target_index].pipeline = pipeline;
    pipeline->builders[target_index].target_index = target_index;
    if (pthread_create(&pipeline->threads[target_index], NULL,
                       qstFullPipelineBuild,
                       &pipeline->builders[target_index]) != 0) {
      qstFullPipelineStop(pipeline, QST_E_RESOURCE);
      for (uint32_t index = 0; index < started; ++index) {
        (void)pthread_join(pipeline->threads[index], NULL);
      }
      (void)pthread_cond_destroy(&pipeline->condition);
      (void)pthread_mutex_destroy(&pipeline->mutex);
      pipeline->synchronization_initialized = 0;
      return QST_E_RESOURCE;
    }
    ++started;
  }
  return QST_OK;
}

static void qstFullPipelineDestroy(SQstFullPipeline *pipeline) {
  if (NULL == pipeline) {
    return;
  }
  qstFullPipelineStop(pipeline, QST_OK);
  for (uint32_t target_index = 0; target_index < QST_GENERATOR_TARGET_COUNT;
       ++target_index) {
    if (pipeline->builders[target_index].pipeline == pipeline) {
      (void)pthread_join(pipeline->threads[target_index], NULL);
    }
  }
  qstFullPendingArrayDestroy(pipeline->pending);
  for (uint32_t target_index = 0;
       target_index < QST_GENERATOR_TARGET_COUNT; ++target_index) {
    qst_generator_full_path_seed_destroy(
        &pipeline->builders[target_index].successor_seed);
  }
  if (pipeline->synchronization_initialized != 0) {
    (void)pthread_cond_destroy(&pipeline->condition);
    (void)pthread_mutex_destroy(&pipeline->mutex);
  }
  memset(pipeline, 0, sizeof(*pipeline));
}

static uint32_t qstFullPipelineSelectTarget(const SQstGenerationState *state) {
  uint32_t done_mask = 0;
  uint32_t start_target = 0;

  if (NULL == state) {
    return QST_GENERATOR_TARGET_COUNT;
  }
  done_mask = atomic_load_explicit(&state->full_target_done_mask,
                                   memory_order_acquire);
  start_target = atomic_load_explicit(&state->full_target_index,
                                      memory_order_acquire);
  if (start_target >= QST_GENERATOR_TARGET_COUNT) {
    return QST_GENERATOR_TARGET_COUNT;
  }
  for (uint32_t offset = 0; offset < QST_GENERATOR_TARGET_COUNT; ++offset) {
    const uint32_t target_index =
        (start_target + offset) % QST_GENERATOR_TARGET_COUNT;

    if ((state->target_selection_bits & (UINT32_C(1) << target_index)) != 0 &&
        (done_mask & (UINT32_C(1) << target_index)) == 0) {
      return target_index;
    }
  }
  return QST_GENERATOR_TARGET_COUNT;
}

static SQstGeneratorFullPending *qstFullPipelineCurrentPending(
    SQstFullPipeline *pipeline, uint32_t target_index) {
  uint32_t slot_index = 0;

  if (NULL == pipeline || target_index >= QST_GENERATOR_TARGET_COUNT ||
      atomic_load_explicit(&pipeline->ready_count[target_index],
                           memory_order_acquire) == 0) {
    return NULL;
  }
  slot_index = pipeline->consume_slot_index[target_index];
  if (slot_index >= QST_FULL_PIPELINE_PENDING_DEPTH) {
    (void)fprintf(stderr,
                  "taosQueryTest: full pipeline invalid consume slot "
                  "target=%u slot=%u ready=%u\n",
                  target_index, slot_index,
                  atomic_load_explicit(&pipeline->ready_count[target_index],
                                       memory_order_acquire));
    return NULL;
  }
  return &pipeline->pending[target_index][slot_index];
}

static EQstCode qstFullPipelineConsumePending(SQstFullPipeline *pipeline,
                                              uint32_t target_index) {
  uint32_t ready_count = 0;

  if (NULL == pipeline || target_index >= QST_GENERATOR_TARGET_COUNT) {
    return QST_E_INVALID_ARGUMENT;
  }
  ready_count = atomic_load_explicit(&pipeline->ready_count[target_index],
                                     memory_order_acquire);
  if (ready_count == 0 || ready_count > QST_FULL_PIPELINE_PENDING_DEPTH) {
    (void)fprintf(stderr,
                  "taosQueryTest: full pipeline invalid ready count "
                  "target=%u ready=%u consume_slot=%u\n",
                  target_index, ready_count,
                  pipeline->consume_slot_index[target_index]);
    return QST_E_SHM_CORRUPT;
  }
  pipeline->consume_slot_index[target_index] =
      (pipeline->consume_slot_index[target_index] + 1U) %
      QST_FULL_PIPELINE_PENDING_DEPTH;
  atomic_fetch_sub_explicit(&pipeline->ready_count[target_index], 1,
                            memory_order_release);
  qstFullPipelineNotify(pipeline);
  return QST_OK;
}

static EQstCode qstFullPipelineWaitReady(SQstFullPipeline *pipeline,
                                         uint32_t target_index) {
  EQstCode code = QST_OK;

  if (NULL == pipeline || target_index >= QST_GENERATOR_TARGET_COUNT ||
      pipeline->synchronization_initialized == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (pthread_mutex_lock(&pipeline->mutex) != 0) {
    return QST_E_RESOURCE;
  }
  while (atomic_load_explicit(&pipeline->stopping, memory_order_acquire) == 0 &&
         atomic_load_explicit(&pipeline->ready_count[target_index],
                              memory_order_acquire) == 0 &&
         atomic_load_explicit(&pipeline->finished[target_index],
                              memory_order_acquire) == 0) {
    if (pthread_cond_wait(&pipeline->condition, &pipeline->mutex) != 0) {
      code = QST_E_RESOURCE;
      break;
    }
  }
  if (pthread_mutex_unlock(&pipeline->mutex) != 0 && code == QST_OK) {
    code = QST_E_RESOURCE;
  }
  if (code == QST_OK &&
      atomic_load_explicit(&pipeline->stopping, memory_order_acquire) != 0) {
    code = atomic_load_explicit(&pipeline->error_code, memory_order_acquire);
    if (code == QST_OK) {
      code = QST_E_STOPPING;
    }
  }
  return code;
}

EQstCode qst_generator_run(SQstShmMapping *mapping, uint32_t generation) {
  const SQstGeneratorCatalog *catalog = NULL;
  SQstGenerationState *state = NULL;
  SQstDigest256 dataset_digest = {0};
  SQstDigest256 fixture_manifest_digest = {0};
  SQstRef object_directory_ref = {0};
  SQstRef fixture_manifest_ref = {0};
  SQstFullPipeline full_pipeline = {0};
  SQstGeneratorFragPlan random_plan = {0};
  struct pollfd full_queue_event = {0};
  uint64_t full_duration_deadline = 0;
  int32_t random_profile_enabled = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || generation == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qstWriteProcessStartLog(
      QST_ROLE_GENERATOR, generation, QST_ROLE_MAIN,
      atomic_load_explicit(&mapping->header->main_generation,
                           memory_order_acquire));
  if (code != QST_OK ||
      qstConfirmQueueFlow(mapping, QST_ROLE_GENERATOR, generation,
                          QST_FLOW_STAGE_STARTUP, QST_QUEUE_GENERATE, 0, 0) !=
          QST_OK ||
      qst_sql_dimension_catalog(&catalog) != QST_OK ||
      qst_generator_state_get(mapping, &state) != QST_OK ||
      qst_generator_state_get_dataset(
          mapping, &dataset_digest, &fixture_manifest_digest,
          &object_directory_ref, &fixture_manifest_ref) != QST_OK) {
    return QST_E_SHM_CORRUPT;
  }
  qstTestMaybeKillRole(QST_ROLE_GENERATOR, generation, UINT32_MAX);
  (void)fixture_manifest_digest;
  (void)object_directory_ref;
  (void)fixture_manifest_ref;
  if (getenv("QST_GENERATOR_STAGE_BENCH_HOLD") != NULL) {
    return qstGeneratorHoldForStageBenchmark(mapping, generation);
  }
  if (state->mode == QST_MODE_RANDOM) {
    /* Stage timing is collected by the main process. Keep the per-choice
     * catalog counters opt-in so steady-state timing is not perturbed by a
     * clock read and atomic updates for every FRAG selection. */
    random_profile_enabled = getenv("QST_PROFILE_CATALOG") != NULL;
    code = qst_generator_frag_plan_prepare(&random_plan, catalog);
    if (code != QST_OK) {
      return code;
    }
    code = qstRandomPipelineRun(mapping, generation, state, &random_plan,
                                dataset_digest, random_profile_enabled);
    qst_generator_frag_plan_destroy(&random_plan);
    if (code == QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: random generator completed stop_reason=%u "
                    "activated=%llu\n",
                    atomic_load_explicit(&mapping->header->stop_reason,
                                         memory_order_acquire),
                    (unsigned long long)atomic_load_explicit(
                        &state->random_activated_count, memory_order_acquire));
    }
    return code;
  }
  if (state->mode != QST_MODE_FULL) {
    return QST_E_SHM_CORRUPT;
  }
  for (uint32_t target_index = 0;
       target_index < QST_GENERATOR_TARGET_COUNT; ++target_index) {
    while ((state->target_selection_bits & (UINT32_C(1) << target_index)) !=
           0) {
      uint64_t gid = 0;

      code = qst_generator_full_publish_coverage_seed(
          mapping, QST_ROLE_GENERATOR, generation, catalog, dataset_digest,
          target_index, 1, UINT64_MAX, &gid);
      if (code == QST_E_QUEUE_EMPTY) {
        break;
      }
      if (code == QST_E_QUEUE_FULL || code == QST_E_BLOCK_NO_SPACE) {
        if (qstPollQueueEvent(&(struct pollfd){
                                  .fd = mapping->queue_eventfd[QST_QUEUE_GENERATE - 1U],
                                  .events = POLLIN},
                              20) != QST_OK) {
          return QST_E_RESOURCE;
        }
        continue;
      }
      if (code != QST_OK) {
        return code;
      }
      code = qst_process_record_business(
          mapping, QST_ROLE_GENERATOR, generation, gid,
          qstMillisecondsToDeadlineNs(QST_FLOW_LEASE_MILLISECONDS));
      if (code == QST_OK) {
        code = qstConfirmQueueFlow(
            mapping, QST_ROLE_GENERATOR, generation, QST_FLOW_STAGE_IDLE,
            QST_QUEUE_GENERATE, gid,
            qstMillisecondsToDeadlineNs(QST_FLOW_LEASE_MILLISECONDS));
      }
      if (code != QST_OK) {
        return code;
      }
    }
  }
  full_queue_event.fd = mapping->queue_eventfd[QST_QUEUE_GENERATE - 1];
  full_queue_event.events = POLLIN;
  if (state->duration_limit_minutes != 0) {
    const uint64_t now_mono_ns = qstMonotonicNowNs();
    const uint64_t duration_ns = state->duration_limit_minutes *
                                 UINT64_C(60) * UINT64_C(1000000000);

    if (now_mono_ns == 0 || duration_ns == 0 ||
        now_mono_ns > UINT64_MAX - duration_ns) {
      return QST_E_RESOURCE;
    }
    full_duration_deadline = now_mono_ns + duration_ns;
  }
  code = qstFullPipelineStart(&full_pipeline, mapping, generation, state,
                              catalog, dataset_digest);
  if (code != QST_OK) {
    return code;
  }
  for (;;) {
    uint64_t gid = 0;
    uint32_t target_index = QST_GENERATOR_TARGET_COUNT;
    SQstGeneratorFullPending *pending = NULL;
    int32_t stop_acknowledged = 0;
    int32_t duration_expired = 0;
    const uint64_t flow_deadline = qstMillisecondsToDeadlineNs(
        QST_FLOW_LEASE_MILLISECONDS);

    if (flow_deadline == 0) {
      qstFullPipelineDestroy(&full_pipeline);
      return QST_E_RESOURCE;
    }
    code = qstGeneratorCheckStopRequest(mapping, generation,
                                        &stop_acknowledged);
    if (code != QST_OK || stop_acknowledged != 0) {
      qstFullPipelineDestroy(&full_pipeline);
      return code;
    }
    code = qst_generator_full_duration_expired(full_duration_deadline,
                                               qstMonotonicNowNs(),
                                               &duration_expired);
    if (code != QST_OK) {
      qstFullPipelineDestroy(&full_pipeline);
      return code;
    }
    if (duration_expired != 0) {
      qstFullPipelineDestroy(&full_pipeline);
      atomic_store_explicit(&mapping->header->stop_reason,
                            QST_STOP_DURATION_LIMIT, memory_order_release);
      return QST_OK;
    }
    target_index = qstFullPipelineSelectTarget(state);
    if (target_index == QST_GENERATOR_TARGET_COUNT) {
      code = QST_E_QUEUE_EMPTY;
    } else {
      code = qstFullPipelineWaitReady(&full_pipeline, target_index);
      if (code != QST_OK) {
        qstFullPipelineDestroy(&full_pipeline);
        return code;
      }
      pending = qstFullPipelineCurrentPending(&full_pipeline, target_index);
      if (NULL == pending) {
        (void)fprintf(stderr,
                      "taosQueryTest: full pipeline has no pending slot "
                      "target=%u ready=%u finished=%u\n",
                      target_index,
                      atomic_load_explicit(
                          &full_pipeline.ready_count[target_index],
                          memory_order_acquire),
                      atomic_load_explicit(&full_pipeline.finished[target_index],
                                           memory_order_acquire));
        qstFullPipelineDestroy(&full_pipeline);
        return QST_E_SHM_CORRUPT;
      }
      code = qst_generator_full_publish_pending(
          mapping, QST_ROLE_GENERATOR, generation, catalog, dataset_digest,
          1, UINT64_MAX, pending, &gid);
    }
    if (code == QST_E_QUEUE_EMPTY) {
      int32_t restart_full_round = 0;

      if (full_duration_deadline != 0) {
        code = qst_generator_full_duration_expired(full_duration_deadline,
                                                   qstMonotonicNowNs(),
                                                   &duration_expired);
        if (code != QST_OK) {
          qstFullPipelineDestroy(&full_pipeline);
          return code;
        }
        if (duration_expired == 0) {
          restart_full_round = 1;
        }
      }
      qstFullPipelineDestroy(&full_pipeline);
      code = qst_generator_full_complete_round(mapping, restart_full_round);
      if (code != QST_OK) {
        return code;
      }
      if (restart_full_round != 0) {
        code = qstFullPipelineStart(&full_pipeline, mapping, generation, state,
                                    catalog, dataset_digest);
        if (code != QST_OK) {
          return code;
        }
        continue;
      }
      atomic_store_explicit(&mapping->header->stop_reason,
                            full_duration_deadline != 0 ? QST_STOP_DURATION_LIMIT :
                                                        QST_STOP_FULL_COMPLETE,
                            memory_order_release);
      return QST_OK;
    }
    /*
     * The execute manager owns dequeueing generate_queue.  A full queue is
     * therefore normal backpressure, rather than generator failure.  The
     * generator must retain its cursor and retry after a bounded wait; the
     * publish helper commits the cursor only after the message is visible.
     */
    if (code == QST_E_STOPPING || code == QST_E_STALE_OWNER) {
      EQstCode stop_code = qstGeneratorCheckStopRequest(
          mapping, generation, &stop_acknowledged);

      if (stop_code != QST_OK || stop_acknowledged != 0) {
        qstFullPipelineDestroy(&full_pipeline);
        return stop_code;
      }
    }
    if (code == QST_E_QUEUE_FULL || code == QST_E_BLOCK_NO_SPACE) {
      if (qstPollQueueEvent(&full_queue_event, 20) != QST_OK) {
        qstFullPipelineDestroy(&full_pipeline);
        return QST_E_RESOURCE;
      }
      continue;
    }
    if (code != QST_OK) {
      qstFullPipelineDestroy(&full_pipeline);
      return code;
    }
    code = qstFullPipelineConsumePending(&full_pipeline, target_index);
    if (code != QST_OK) {
      qstFullPipelineDestroy(&full_pipeline);
      return code;
    }
    code = qst_process_record_business(mapping, QST_ROLE_GENERATOR, generation,
                                       gid, flow_deadline);
    if (code == QST_OK) {
      code = qstConfirmQueueFlow(mapping, QST_ROLE_GENERATOR, generation,
                                 QST_FLOW_STAGE_IDLE, QST_QUEUE_GENERATE, gid,
                                 flow_deadline);
    }
    if (code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: full generator flow confirmation failed "
                    "code=%d generation=%u gid=%llu deadline_ns=%llu\n",
                    code, generation, (unsigned long long)gid,
                    (unsigned long long)flow_deadline);
      qstFullPipelineDestroy(&full_pipeline);
      return code;
    }
  }
}

EQstCode qst_exec_manager_run(SQstShmMapping *mapping, uint32_t generation) {
  struct pollfd event = {0};
  SQstPairChild *pairs = NULL;
  uint32_t pair_count = 0;
  uint64_t next_health_scan_ns = 0;
  int32_t pairs_reaped_early = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || generation == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qstWriteProcessStartLog(
      QST_ROLE_EXEC_MANAGER, generation, QST_ROLE_MAIN,
      atomic_load_explicit(&mapping->header->main_generation,
                           memory_order_acquire));
  if (code != QST_OK) {
    return code;
  }
  code = qstConfirmQueueFlow(mapping, QST_ROLE_EXEC_MANAGER, generation,
                             QST_FLOW_STAGE_STARTUP, QST_QUEUE_GENERATE, 0, 0);
  if (code != QST_OK) {
    uint32_t stop_generation = 0;
    uint32_t exit_state = 0;
    if (code == QST_E_STALE_OWNER &&
        qst_process_get_stop_generation(mapping, QST_ROLE_EXEC_MANAGER,
                                        generation, &stop_generation,
                                        &exit_state) == QST_OK &&
        exit_state == QST_PROCESS_STOP_REQUESTED && stop_generation != 0) {
      return qstAcknowledgeRoleStop(mapping, QST_ROLE_EXEC_MANAGER, generation,
                                    stop_generation);
    }
    return code;
  }
  qstTestMaybeKillRole(QST_ROLE_EXEC_MANAGER, generation, UINT32_MAX);
  pair_count = mapping->header->execution_pair_count;
  if (pair_count == 0 || pair_count > mapping->header->process_region.element_count) {
    return QST_E_SHM_CORRUPT;
  }
  pairs = (SQstPairChild *)calloc(pair_count, sizeof(*pairs));
  if (NULL == pairs) {
    return QST_E_RESOURCE;
  }
  /* Pair runtime setup starts the isolated taosd and waits for its own socket.
   * Launch every supervisor before waiting so independent pair startups overlap,
   * while the subsequent readiness pass keeps the original failure boundary. */
  for (uint32_t index = 0; index < pair_count; ++index) {
    code = qstStartPairSupervisor(mapping, generation, index, &pairs[index], 0);
    if (code != QST_OK) {
      (void)qstStopAndReapPairSupervisors(mapping, generation, pairs, index);
      free(pairs);
      return code;
    }
  }
  for (uint32_t index = 0; index < pair_count; ++index) {
    code = qstWaitForRoleFlow(mapping, QST_ROLE_PAIR_SUPERVISOR,
                              pairs[index].slot, pairs[index].generation,
                              qstMillisecondsToDeadlineNs(
                                  QST_PAIR_STARTUP_MILLISECONDS));
    if (code != QST_OK) {
      (void)qstStopAndReapPairSupervisors(mapping, generation, pairs,
                                          pair_count);
      free(pairs);
      return code;
    }
  }
  event.fd = mapping->queue_eventfd[QST_QUEUE_CONTROL - 1];
  event.events = POLLIN;
  for (;;) {
    uint64_t gid = 0;
    uint64_t dispatch_epoch = 0;
    uint64_t transition_seq = 0;
    uint32_t stop_generation = 0;
    uint32_t exit_state = 0;
    const uint64_t deadline = qstMillisecondsToDeadlineNs(
        QST_FLOW_LEASE_MILLISECONDS);

    if (deadline == 0) {
      (void)qstStopAndReapPairSupervisors(mapping, generation, pairs, pair_count);
      free(pairs);
      return QST_E_RESOURCE;
    }
    code = qst_process_get_stop_generation(mapping, QST_ROLE_EXEC_MANAGER,
                                           generation, &stop_generation,
                                           &exit_state);
    if (code != QST_OK) {
      (void)qstStopAndReapPairSupervisors(mapping, generation, pairs, pair_count);
      free(pairs);
      return code;
    }
    if (exit_state == QST_PROCESS_STOP_REQUESTED && stop_generation != 0) {
      EQstCode stop_code = qstStopAndReapPairSupervisors(
          mapping, generation, pairs, pair_count);

      if (stop_code == QST_OK) {
        stop_code = qstAcknowledgeRoleStop(
            mapping, QST_ROLE_EXEC_MANAGER, generation, stop_generation);
      }
      if (stop_code != QST_OK) {
        (void)fprintf(stderr,
                      "taosQueryTest: exec manager pair stop failed code=%d generation=%u\n",
                      stop_code, generation);
      }
      free(pairs);
      if (stop_code != QST_OK) {
        return stop_code;
      }
      return QST_OK;
    }
    if (exit_state != QST_PROCESS_RUNNING) {
      (void)fprintf(stderr, "taosQueryTest: exec manager non-running exit_state=%u stop_generation=%u\n",
                    exit_state, stop_generation);
      (void)qstStopAndReapPairSupervisors(mapping, generation, pairs, pair_count);
      free(pairs);
      return QST_E_STALE_OWNER;
    }
    {
      const uint64_t now_ns = qstMonotonicNowNs();

      if (now_ns == 0) {
        (void)qstStopAndReapPairSupervisors(mapping, generation, pairs,
                                            pair_count);
        free(pairs);
        return QST_E_RESOURCE;
      }
      if (next_health_scan_ns == 0 || now_ns >= next_health_scan_ns) {
        if (now_ns > UINT64_MAX -
                         (uint64_t)QST_PAIR_HEALTH_SCAN_MILLISECONDS *
                             UINT64_C(1000000)) {
          (void)qstStopAndReapPairSupervisors(mapping, generation, pairs,
                                              pair_count);
          free(pairs);
          return QST_E_RESOURCE;
        }
        next_health_scan_ns =
            now_ns + (uint64_t)QST_PAIR_HEALTH_SCAN_MILLISECONDS *
                          UINT64_C(1000000);
        for (uint32_t index = 0; index < pair_count; ++index) {
          int32_t pair_exited = 0;

          if (pairs[index].generation == 0) {
            continue;
          }
          /* This is a liveness scan, not a wait. Keep it periodic so the
           * healthy steady-state path performs no per-task poll storm. */
          code = qstWaitForExit(pairs[index].pidfd, 0, &pair_exited);
          if (code != QST_OK) {
            (void)qstStopAndReapPairSupervisors(mapping, generation, pairs,
                                                pair_count);
            free(pairs);
            return code;
          }
          if (pair_exited != 0) {
            if (pairs[index].retire_requested != 0) {
              code = qstStopAndReapPairSupervisor(mapping, generation,
                                                   &pairs[index]);
            } else {
              code = qstRecoverExitedPairSupervisor(mapping, generation,
                                                     &pairs[index]);
            }
            if (code != QST_OK) {
              (void)qstStopAndReapPairSupervisors(mapping, generation, pairs,
                                                  pair_count);
              free(pairs);
              return code;
            }
          }
        }
      }
    }
    code = qst_task_find_final_for_exec(mapping, generation, &gid,
                                        &dispatch_epoch, &transition_seq);
    if (code == QST_E_STALE_OWNER) {
      continue;
    }
    if (code == QST_E_QUEUE_EMPTY) {
      /* Pair ownership can change from idle to EXECUTING after an individual
       * retirement snapshot. Stop all pairs only after the shared ledger
       * confirms that no task can still require a pair supervisor. */
      if (pairs_reaped_early == 0 &&
          atomic_load_explicit(&mapping->header->stop_reason,
                               memory_order_acquire) != QST_STOP_NONE) {
        int32_t inputs_drained = 0;

        code = qst_task_execution_inputs_drained(mapping, &inputs_drained);
        if (code != QST_OK) {
          (void)qstStopAndReapPairSupervisors(mapping, generation, pairs,
                                              pair_count);
          free(pairs);
          return code;
        }
        if (inputs_drained != 0) {
          if (getenv("QST_PROFILE_PHASES") != NULL) {
            (void)fprintf(stderr,
                          "taosQueryTest: exec manager early pair reap "
                          "generation=%u\n",
                          generation);
          }
          /* Generator completion can race with a pair failure after the pair
           * published its final result.  Recover an already-exited live pair
           * before the cooperative early-stop path judges its SIGKILL status
           * as a shutdown failure. */
          for (uint32_t index = 0; index < pair_count; ++index) {
            int32_t pair_exited = 0;

            if (pairs[index].generation == 0 ||
                pairs[index].retire_requested != 0) {
              continue;
            }
            code = qstWaitForExit(pairs[index].pidfd, 0, &pair_exited);
            if (code != QST_OK) {
              (void)qstStopAndReapPairSupervisors(mapping, generation, pairs,
                                                  pair_count);
              free(pairs);
              return code;
            }
            if (pair_exited != 0) {
              code = qstRecoverExitedPairSupervisor(mapping, generation,
                                                     &pairs[index]);
              if (code != QST_OK) {
                (void)qstStopAndReapPairSupervisors(mapping, generation, pairs,
                                                    pair_count);
                free(pairs);
                return code;
              }
            }
          }
          code = qstStopAndReapPairSupervisors(mapping, generation, pairs,
                                               pair_count);
          if (code != QST_OK) {
            free(pairs);
            return code;
          }
          pairs_reaped_early = 1;
        }
      }
      /* Pair supervisors are the QST_QUEUE_GENERATE consumers. The manager
       * only supervises pair lifecycle and forwards completed evidence. */
      code = qstConfirmQueueFlow(mapping, QST_ROLE_EXEC_MANAGER, generation,
                                 QST_FLOW_STAGE_IDLE, QST_QUEUE_GENERATE, 0,
                                 0);
      if (code == QST_E_STALE_OWNER) {
        continue;
      }
      if (code != QST_OK) {
        (void)qstStopAndReapPairSupervisors(mapping, generation, pairs,
                                            pair_count);
        free(pairs);
        return code;
      }
      if (qstPollQueueEvent(&event, 20) != QST_OK) {
        (void)qstStopAndReapPairSupervisors(mapping, generation, pairs,
                                            pair_count);
        free(pairs);
        return QST_E_RESOURCE;
      }
      continue;
    }
    if (code != QST_OK) {
      (void)qstStopAndReapPairSupervisors(mapping, generation, pairs,
                                          pair_count);
      free(pairs);
      return code;
    }
    {
      SQstExecutePayload final_payload = {0};
      SQstTaskDispatch retry_dispatch = {0};
      int32_t recycle_required = 0;
      uint32_t previous_pair_slot = 0;
      uint32_t previous_pair_generation = 0;

      code = qst_task_get_final(mapping, gid, dispatch_epoch, transition_seq,
                                QST_ROLE_EXEC_MANAGER, generation,
                                &final_payload, &previous_pair_slot);
      if (code == QST_E_STALE_OWNER) {
        continue;
      }
      if (code != QST_OK) {
        (void)qstStopAndReapPairSupervisors(mapping, generation, pairs,
                                            pair_count);
        free(pairs);
        return code;
      }
      if (final_payload.execution_status == QST_EXEC_TIMEOUT) {
        code = qst_task_retry_timeout(
            mapping, gid, dispatch_epoch, transition_seq, generation,
            mapping->header->statement_retry_count, &retry_dispatch,
            &recycle_required, &previous_pair_slot, &previous_pair_generation);
        if (code == QST_OK) {
          code = qstDispatchTimeoutRetry(
              mapping, generation, pairs, pair_count, &retry_dispatch,
              recycle_required, previous_pair_slot, previous_pair_generation,
              deadline);
          if (code == QST_OK) {
            code = qst_process_record_business(mapping, QST_ROLE_EXEC_MANAGER,
                                               generation, gid, deadline);
          }
          if (code == QST_OK) {
            continue;
          }
        }
        if (code == QST_E_STALE_OWNER) {
          continue;
        }
        if (code != QST_E_DEADLINE) {
          (void)qstStopAndReapPairSupervisors(mapping, generation, pairs,
                                              pair_count);
          free(pairs);
          return code;
        }
      }
    }
    code = qstForwardFinalTask(mapping, generation, gid, dispatch_epoch,
                               transition_seq, deadline);
    if (code == QST_E_QUEUE_FULL) {
      if (qstPollQueueEvent(&event, 20) != QST_OK) {
        (void)qstStopAndReapPairSupervisors(mapping, generation, pairs,
                                            pair_count);
        free(pairs);
        return QST_E_RESOURCE;
      }
      continue;
    }
    if (code == QST_E_STALE_OWNER) {
      continue;
    }
    if (code != QST_OK) {
      (void)qstStopAndReapPairSupervisors(mapping, generation, pairs,
                                          pair_count);
      free(pairs);
      return code;
    }
    code = qst_process_record_business(mapping, QST_ROLE_EXEC_MANAGER,
                                       generation, gid, deadline);
    if (code == QST_E_STALE_OWNER) {
      continue;
    }
    if (code != QST_OK) {
      (void)qstStopAndReapPairSupervisors(mapping, generation, pairs,
                                          pair_count);
      free(pairs);
      return code;
    }
  }
}

static int64_t qstRealtimeNowNs(void) {
  struct timespec now = {0};

  if (clock_gettime(CLOCK_REALTIME, &now) != 0 || now.tv_sec < 0 ||
      (uint64_t)now.tv_sec > INT64_MAX / UINT64_C(1000000000)) {
    return 0;
  }
  return (int64_t)((uint64_t)now.tv_sec * UINT64_C(1000000000) +
                   (uint64_t)now.tv_nsec);
}

static void qstPairSignalQueryCompletion(SQstPairQueryCompletion *completion) {
  const uint64_t event_count = 1;

  if (completion == NULL) {
    return;
  }
  atomic_store_explicit(&completion->completed, 1, memory_order_release);
  if (completion->event_fd >= 0 &&
      write(completion->event_fd, &event_count, sizeof(event_count)) < 0 &&
      errno != EAGAIN && errno != EINTR) {
    /* The completion flag remains authoritative on a best-effort wakeup
     * failure; the pair checks it on the bounded polling path. */
  }
}

/* Converts a native query/fetch error into the same canonical document used by
 * the synchronous path. The execution code remains in completion->code so the
 * statement ledger records the failed execution while the correctness result
 * remains comparable and durable. */
static void qstPairCaptureNativeError(SQstPairQueryCompletion *completion,
                                      void *result, int32_t error_code) {
  const char *message = "native query error";
  int32_t normalized_code = error_code == 0 ? -1 : error_code;

  if (completion == NULL) {
    return;
  }
  if (result != NULL && completion->client != NULL &&
      completion->client->errstr != NULL) {
    const char *native_message = completion->client->errstr(result);

    if (native_message != NULL && native_message[0] != '\0') {
      message = native_message;
    }
  }
  completion->code = normalized_code;
  completion->native_error_code = normalized_code;
  if (qst_result_build_error(normalized_code, message,
                             &completion->captured_result) == QST_OK) {
    completion->captured = 1;
  } else {
    completion->captured = 0;
  }
}

/* Consumes one asynchronous result block and requests the next one. The
 * result handle remains owned by the pair supervisor and is freed only after
 * the normalized document has been sealed into shared memory. */
static void qstPairFetchRowsCallback(void *param, void *result,
                                     int32_t row_count) {
  SQstPairQueryCompletion *completion = (SQstPairQueryCompletion *)param;
  EQstCode qst_code = QST_OK;

  if (completion == NULL || completion->client == NULL ||
      completion->capture == NULL || result == NULL || row_count < 0) {
    if (completion != NULL && completion->capture != NULL) {
      qst_result_async_capture_abort(completion->capture);
      completion->capture = NULL;
    }
    if (completion != NULL) {
      completion->result = result;
      qstPairCaptureNativeError(completion, result,
                                row_count < 0 ? row_count : -1);
      if (getenv("QST_ASYNC_DIAGNOSTICS") != NULL) {
        (void)fprintf(stderr,
                      "taosQueryTest: async fetch callback failed result=%p "
                      "row_count=%d code=%d\n",
                      result, row_count, completion->code);
      }
      qstPairSignalQueryCompletion(completion);
    }
    return;
  }
  if (row_count == 0) {
    qst_code = qst_result_async_capture_finish(completion->capture,
                                                &completion->captured_result);
    completion->capture = NULL;
    completion->result = result;
    completion->code = qst_code == QST_OK ? 0 : -1;
    completion->captured = qst_code == QST_OK ? 1 : 0;
    completion->capture_finished_ns = qstMonotonicNowNs();
    qstPairSignalQueryCompletion(completion);
    return;
  }
  qst_code = qst_result_async_capture_rows(completion->client, result,
                                           row_count, completion->capture);
  if (qst_code != QST_OK) {
    qst_result_async_capture_abort(completion->capture);
    completion->capture = NULL;
    completion->capture_finished_ns = qstMonotonicNowNs();
    completion->result = result;
    qstPairCaptureNativeError(completion, result, -1);
    if (getenv("QST_ASYNC_DIAGNOSTICS") != NULL) {
      (void)fprintf(stderr,
                    "taosQueryTest: async fetch row capture failed "
                    "result=%p row_count=%d qst_code=%d\n",
                    result, row_count, qst_code);
    }
    qstPairSignalQueryCompletion(completion);
    return;
  }
  completion->client->fetch_rows_a(result, qstPairFetchRowsCallback,
                                   completion);
}

/* Records native completion for the owning pair supervisor. */
static void qstPairQueryCallback(void *param, void *result, int32_t code) {
  SQstPairQueryCompletion *completion = (SQstPairQueryCompletion *)param;
  int32_t native_error_code = code;
  int32_t field_count = 0;

  if (NULL == completion) {
    return;
  }
  if (result != NULL && completion->client != NULL &&
      completion->client->errno_fn != NULL) {
    native_error_code = completion->client->errno_fn(result);
  }
  if (result != NULL && completion->client != NULL &&
      completion->client->num_fields != NULL && native_error_code == 0) {
    field_count = completion->client->num_fields(result);
  }
  /* taos_fetch_rows_a is valid only for a result set. DML results have no
   * fields and deliberately stay on the synchronous normalizer path. */
  if (getenv("QST_ENABLE_ASYNC_CAPTURE") != NULL && code == 0 &&
      native_error_code == 0 && result != NULL &&
      completion->client != NULL && field_count > 0 &&
      completion->client->fetch_rows_a != NULL) {
    completion->capture_started_ns = qstMonotonicNowNs();
    const EQstCode qst_code = qst_result_async_capture_begin(
        completion->client, result, &completion->capture);

    if (qst_code == QST_OK) {
      completion->client->fetch_rows_a(result, qstPairFetchRowsCallback,
                                       completion);
      return;
    }
    if (getenv("QST_ASYNC_DIAGNOSTICS") != NULL) {
      (void)fprintf(stderr,
                    "taosQueryTest: async capture begin failed result=%p "
                    "fields=%d qst_code=%d native_code=%d\n",
                    result, field_count, qst_code, native_error_code);
    }
    completion->capture = NULL;
  }
  completion->result = result;
  if (code != 0 || native_error_code != 0) {
    qstPairCaptureNativeError(completion, result,
                              native_error_code != 0 ? native_error_code : code);
  } else {
    completion->code = 0;
    completion->native_error_code = 0;
  }
  if (getenv("QST_ASYNC_DIAGNOSTICS") != NULL &&
      (code != 0 || native_error_code != 0 || field_count <= 0)) {
    (void)fprintf(stderr,
                  "taosQueryTest: async query fallback result=%p code=%d "
                  "native_code=%d fields=%d\n",
                  result, code, native_error_code, field_count);
  }
  qstPairSignalQueryCompletion(completion);
}

/* Copies one sealed SQL chain into a caller-owned NUL-terminated buffer. */
static EQstCode qstPairCopySqlInto(const SQstShmMapping *mapping,
                                   const SQstTaskDispatch *dispatch,
                                   char *sql, uint64_t sql_bytes) {
  uint64_t bytes = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == dispatch || NULL == sql || sql_bytes == 0 ||
      dispatch->gid == 0 || qstRefIsEmpty(dispatch->sql_ref)) {
    return QST_E_INVALID_ARGUMENT;
  }
  bytes = dispatch->sql_ref.length;
  if (bytes == 0 || bytes >= sql_bytes) {
    return QST_E_RESOURCE;
  }
  code = qst_block_copy(mapping, dispatch->sql_ref, dispatch->gid, 0, sql,
                        bytes);
  if (code != QST_OK) {
    return code;
  }
  sql[bytes] = '\0';
  return QST_OK;
}

/* Copies one sealed SQL chain into a private NUL-terminated execution buffer. */
static EQstCode qstPairCopySql(const SQstShmMapping *mapping,
                               const SQstTaskDispatch *dispatch,
                               char **out_sql) {
  char *sql = NULL;
  uint64_t bytes = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == dispatch || NULL == out_sql ||
      dispatch->gid == 0 || qstRefIsEmpty(dispatch->sql_ref)) {
    return QST_E_INVALID_ARGUMENT;
  }
  bytes = dispatch->sql_ref.length;
  if (bytes == 0 || bytes >= SIZE_MAX) {
    return QST_E_RESOURCE;
  }
  sql = (char *)calloc((size_t)bytes + 1U, 1);
  if (NULL == sql) {
    return QST_E_RESOURCE;
  }
  code = qstPairCopySqlInto(mapping, dispatch, sql, bytes + 1U);
  if (code != QST_OK) {
    free(sql);
    return code;
  }
  *out_sql = sql;
  return QST_OK;
}

static int32_t qstPairPortBlockAvailable(uint16_t port) {
  int32_t sockets[QST_PAIR_PORT_BLOCK_BYTES] = {0};
  int32_t available = 1;

  if (port < QST_PAIR_PORT_MIN ||
      (uint32_t)port + QST_PAIR_PORT_BLOCK_BYTES - 1U > QST_PAIR_PORT_MAX) {
    return 0;
  }
  for (uint32_t index = 0; index < QST_PAIR_PORT_BLOCK_BYTES; ++index) {
    struct sockaddr_in address = {0};
    int32_t fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);

    sockets[index] = -1;
    if (fd < 0) {
      available = 0;
      break;
    }
    address.sin_family = AF_INET;
    address.sin_port = htons((uint16_t)(port + index));
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (bind(fd, (const struct sockaddr *)&address, sizeof(address)) != 0) {
      (void)close(fd);
      available = 0;
      break;
    }
    sockets[index] = fd;
  }
  for (uint32_t index = 0; index < QST_PAIR_PORT_BLOCK_BYTES; ++index) {
    if (sockets[index] >= 0) {
      (void)close(sockets[index]);
    }
  }
  return available;
}

/* Resolves the shared port-lease directory without assuming a system temp path. */
static EQstCode qstPairPortLeaseDirectory(const SQstPairRuntime *runtime,
                                          char *out_directory,
                                          uint64_t out_directory_bytes) {
  const char *configured = getenv("QST_PAIR_PORT_LEASE_DIR");
  const char *runtime_root = NULL;
  int32_t written = 0;

  if (NULL == runtime || NULL == out_directory || out_directory_bytes == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (configured != NULL && configured[0] == '/') {
    written = snprintf(out_directory, out_directory_bytes, "%s", configured);
    return written >= 0 && (uint64_t)written < out_directory_bytes ? QST_OK :
        QST_E_RESOURCE;
  }
  runtime_root = getenv("QST_RUNTIME_TEMP_ROOT");
  if (runtime_root == NULL || runtime_root[0] != '/') {
    runtime_root = getenv("TMPDIR");
  }
  if (runtime_root != NULL && runtime_root[0] == '/') {
    written = snprintf(out_directory, out_directory_bytes,
                       "%s/taosQueryTest-port-leases", runtime_root);
    return written >= 0 && (uint64_t)written < out_directory_bytes ? QST_OK :
        QST_E_RESOURCE;
  }
  if (access("/tmp", W_OK) == 0) {
    written = snprintf(out_directory, out_directory_bytes,
                       "/tmp/taosQueryTest-port-leases");
    return written >= 0 && (uint64_t)written < out_directory_bytes ? QST_OK :
        QST_E_RESOURCE;
  }
  return qstMakePath(out_directory, out_directory_bytes, runtime->root_dir,
                     "port-leases");
}

/* Releases the cross-process lease that protects one pair server port block. */
static EQstCode qstPairReleasePortLease(SQstPairRuntime *runtime) {
  const int32_t lease_fd = runtime == NULL ? -1 : runtime->port_lease_fd;
  int32_t unlock_failed = 0;
  int32_t close_failed = 0;

  if (NULL == runtime) {
    return QST_E_INVALID_ARGUMENT;
  }
  runtime->port_lease_fd = -1;
  if (lease_fd < 0) {
    return QST_OK;
  }
  unlock_failed = flock(lease_fd, LOCK_UN) != 0;
  close_failed = close(lease_fd) != 0;
  if (unlock_failed != 0 || close_failed != 0) {
    (void)fprintf(stderr,
                  "taosQueryTest: pair port lease release failed slot=%u "
                  "generation=%u port=%u errno=%d message=%s\n",
                  runtime->slot, runtime->generation, (unsigned int)runtime->port,
                  errno, strerror(errno));
    return QST_E_RESOURCE;
  }
  return QST_OK;
}

/* Acquires one whole port-block lease before probing its TCP availability.
 * The caller retains it until the corresponding pair runtime is stopped. */
static EQstCode qstPairAcquirePortLease(SQstPairRuntime *runtime,
                                        uint16_t port) {
  char directory[4096] = {0};
  char path[4096] = {0};
  struct stat status = {0};
  int32_t fd = -1;
  int32_t written = 0;

  if (NULL == runtime || port < QST_PAIR_PORT_MIN ||
      (uint32_t)port + QST_PAIR_PORT_BLOCK_BYTES - 1U > QST_PAIR_PORT_MAX ||
      runtime->port_lease_fd >= 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (qstPairPortLeaseDirectory(runtime, directory, sizeof(directory)) != QST_OK ||
      (mkdir(directory, 0700) != 0 && errno != EEXIST)) {
    return QST_E_RESOURCE;
  }
  if (lstat(directory, &status) != 0 ||
      !S_ISDIR(status.st_mode)) {
    return QST_E_RESOURCE;
  }
  written = snprintf(path, sizeof(path), "%s/port-%u.lock",
                     directory, (unsigned int)port);
  if (written < 0 || (uint64_t)written >= sizeof(path)) {
    return QST_E_RESOURCE;
  }
  fd = open(path, O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW, 0600);
  if (fd < 0) {
    return QST_E_RESOURCE;
  }
  if (fstat(fd, &status) != 0 || !S_ISREG(status.st_mode) ||
      flock(fd, LOCK_EX | LOCK_NB) != 0) {
    (void)close(fd);
    return QST_E_RESOURCE;
  }
  if (qstPairPortBlockAvailable(port) == 0) {
    (void)flock(fd, LOCK_UN);
    (void)close(fd);
    return QST_E_RESOURCE;
  }
  runtime->port = port;
  runtime->port_lease_fd = fd;
  return QST_OK;
}

static EQstCode qstPairRemoveDataDirectory(const char *path) {
  DIR *directory = NULL;
  struct dirent *entry = NULL;

  if (NULL == path || path[0] == '\0') {
    return QST_E_INVALID_ARGUMENT;
  }
  directory = opendir(path);
  if (NULL == directory) {
    return errno == ENOENT ? QST_OK : QST_E_RESOURCE;
  }
  while ((entry = readdir(directory)) != NULL) {
    char child_path[4096] = {0};
    struct stat status = {0};
    int32_t written = 0;

    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    written = snprintf(child_path, sizeof(child_path), "%s/%s", path,
                       entry->d_name);
    if (written < 0 || (uint64_t)written >= sizeof(child_path) ||
        lstat(child_path, &status) != 0 ||
        (S_ISDIR(status.st_mode) ?
             qstPairRemoveDataDirectory(child_path) != QST_OK :
             unlink(child_path) != 0)) {
      (void)closedir(directory);
      return QST_E_RESOURCE;
    }
  }
  if (closedir(directory) != 0 || rmdir(path) != 0) {
    return QST_E_RESOURCE;
  }
  return QST_OK;
}

static EQstCode qstPairPrepareRuntimeData(SQstPairRuntime *runtime) {
  struct stat status = {0};

  if (NULL == runtime || runtime->data_dir[0] == '\0' ||
      runtime->data_link[0] == '\0') {
    return QST_E_INVALID_ARGUMENT;
  }
  if (lstat(runtime->data_link, &status) == 0) {
    if (S_ISLNK(status.st_mode)) {
      if (unlink(runtime->data_link) != 0) {
        (void)fprintf(stderr,
                      "taosQueryTest: pair data link unlink failed link=%s "
                      "errno=%d message=%s\n",
                      runtime->data_link, errno, strerror(errno));
        return QST_E_RESOURCE;
      }
    } else if (S_ISDIR(status.st_mode)) {
      if (qstPairRemoveDataDirectory(runtime->data_link) != QST_OK) {
        (void)fprintf(stderr,
                      "taosQueryTest: pair stale data link cleanup failed "
                      "link=%s errno=%d message=%s\n",
                      runtime->data_link, errno, strerror(errno));
        return QST_E_RESOURCE;
      }
    } else {
      (void)fprintf(stderr,
                    "taosQueryTest: pair data link has unsupported type "
                    "link=%s mode=%o\n",
                    runtime->data_link, status.st_mode);
      return QST_E_RESOURCE;
    }
  } else if (errno != ENOENT) {
    (void)fprintf(stderr,
                  "taosQueryTest: pair data link stat failed link=%s "
                  "errno=%d message=%s\n",
                  runtime->data_link, errno, strerror(errno));
    return QST_E_RESOURCE;
  }
  if (lstat(runtime->data_dir, &status) == 0) {
    if (!S_ISDIR(status.st_mode) ||
        qstPairRemoveDataDirectory(runtime->data_dir) != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: pair stale data directory cleanup failed "
                    "path=%s mode=%o errno=%d message=%s\n",
                    runtime->data_dir, status.st_mode, errno, strerror(errno));
      return QST_E_RESOURCE;
    }
  } else if (errno != ENOENT) {
    (void)fprintf(stderr,
                  "taosQueryTest: pair data directory stat failed path=%s "
                  "errno=%d message=%s\n",
                  runtime->data_dir, errno, strerror(errno));
    return QST_E_RESOURCE;
  }
  if (qstMkdir(runtime->data_dir) != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: pair data directory create failed path=%s "
                  "errno=%d message=%s\n",
                  runtime->data_dir, errno, strerror(errno));
    return QST_E_RESOURCE;
  }
  if (symlink(runtime->data_dir, runtime->data_link) != 0) {
    (void)fprintf(stderr,
                  "taosQueryTest: pair data link create failed link=%s "
                  "target=%s errno=%d message=%s\n",
                  runtime->data_link, runtime->data_dir, errno, strerror(errno));
    (void)qstPairRemoveDataDirectory(runtime->data_dir);
    return QST_E_RESOURCE;
  }
  return QST_OK;
}

static EQstCode qstPairPrepareRuntime(SQstShmMapping *mapping,
                                      uint32_t pair_slot,
                                      uint32_t pair_generation,
                                      SQstPairRuntime *out_runtime) {
  const char *data_root = getenv("QST_PAIR_DATA_ROOT");
  char result_dir[4096] = {0};
  char pairs_dir[4096] = {0};
  char pair_name[96] = {0};
  int32_t data_link_written = 0;
  uint64_t port_block_count = 0;
  uint64_t start_block = 0;

  if (NULL == mapping || NULL == mapping->header || NULL == out_runtime ||
      pair_generation == 0 || mapping->header->pair_async_window == 0 ||
      mapping->header->pair_async_window > QST_PAIR_MAX_INFLIGHT ||
      gQstProcessLogDirectory[0] == '\0') {
    return QST_E_INVALID_ARGUMENT;
  }
  /* Callers construct runtimes with {0}; descriptor zero is not a lease. */
  out_runtime->port_lease_fd = -1;
  if (strlen(gQstProcessLogDirectory) <= strlen("/logs") ||
      strcmp(gQstProcessLogDirectory + strlen(gQstProcessLogDirectory) -
                 strlen("/logs"),
             "/logs") != 0) {
    return QST_E_RESOURCE;
  }
  if (snprintf(result_dir, sizeof(result_dir), "%s", gQstProcessLogDirectory) <
      0) {
    return QST_E_RESOURCE;
  }
  result_dir[strlen(result_dir) - strlen("/logs")] = '\0';
  if (qstMakePath(pairs_dir, sizeof(pairs_dir), result_dir, "pairs") != QST_OK ||
      qstMkdir(pairs_dir) != QST_OK ||
      snprintf(pair_name, sizeof(pair_name), "slot_%u_generation_%u", pair_slot,
               pair_generation) < 0 ||
      qstMakePath(out_runtime->root_dir, sizeof(out_runtime->root_dir), pairs_dir,
                  pair_name) != QST_OK ||
      qstMkdir(out_runtime->root_dir) != QST_OK ||
      qstMakePath(out_runtime->config_dir, sizeof(out_runtime->config_dir),
                  out_runtime->root_dir, "config") != QST_OK ||
      qstMkdir(out_runtime->config_dir) != QST_OK ||
      qstMakePath(out_runtime->data_link, sizeof(out_runtime->data_link),
                  out_runtime->root_dir, "data") != QST_OK ||
      qstMakePath(out_runtime->logs_dir, sizeof(out_runtime->logs_dir),
                  out_runtime->root_dir, "logs") != QST_OK ||
      qstMkdir(out_runtime->logs_dir) != QST_OK ||
      qstMakePath(out_runtime->cores_dir, sizeof(out_runtime->cores_dir),
                  out_runtime->root_dir, "cores") != QST_OK ||
      qstMkdir(out_runtime->cores_dir) != QST_OK) {
    return QST_E_RESOURCE;
  }
  if (data_root == NULL || data_root[0] != '/') {
    data_root = getenv("QST_RUNTIME_TEMP_ROOT");
  }
  if (data_root == NULL || data_root[0] != '/') {
    data_root = getenv("TMPDIR");
  }
  if ((data_root == NULL || data_root[0] != '/') &&
      access("/tmp", W_OK) == 0) {
    data_root = "/tmp";
  }
  if (data_root == NULL || data_root[0] != '/') {
    data_root = pairs_dir;
  }
  port_block_count =
      (QST_PAIR_PORT_MAX - QST_PAIR_PORT_MIN + 1U) / QST_PAIR_PORT_BLOCK_BYTES;
  start_block = (mapping->header->run_id + (uint64_t)pair_slot * 97U +
                 (uint64_t)pair_generation * 131U) % port_block_count;
  for (uint64_t attempt = 0; attempt < port_block_count; ++attempt) {
    const uint16_t port = (uint16_t)(QST_PAIR_PORT_MIN +
        ((start_block + attempt) % port_block_count) * QST_PAIR_PORT_BLOCK_BYTES);

    if (qstPairAcquirePortLease(out_runtime, port) == QST_OK) {
      data_link_written = snprintf(
          out_runtime->data_dir, sizeof(out_runtime->data_dir),
          "%s/qst-pair-%016llx-%u-%u", data_root,
          (unsigned long long)mapping->header->run_id, pair_slot,
          pair_generation);
      if (data_link_written < 0 ||
          (uint64_t)data_link_written >= sizeof(out_runtime->data_dir)) {
        (void)qstPairReleasePortLease(out_runtime);
        return QST_E_RESOURCE;
      }
      out_runtime->slot = pair_slot;
      out_runtime->generation = pair_generation;
      out_runtime->async_window = mapping->header->pair_async_window;
      out_runtime->taosd_pid = -1;
      out_runtime->taosd_slot_id = QST_PROCESS_SLOT_AUTO;
      out_runtime->taosc_pid = getpid();
      out_runtime->taosd_pidfd = -1;
      out_runtime->begin_ts_ns = qstRealtimeNowNs();
      if (out_runtime->begin_ts_ns == 0) {
        (void)qstPairReleasePortLease(out_runtime);
        return QST_E_RESOURCE;
      }
      if (qstPairPrepareRuntimeData(out_runtime) != QST_OK) {
        (void)qstPairReleasePortLease(out_runtime);
        return QST_E_RESOURCE;
      }
      return QST_OK;
    }
  }
  return QST_E_RESOURCE;
}

static EQstCode qstPairReleaseRuntimeData(SQstPairRuntime *runtime) {
  struct stat status = {0};
  EQstCode code = QST_OK;

  if (NULL == runtime) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (runtime->data_dir[0] == '\0' || runtime->data_link[0] == '\0') {
    return qstPairReleasePortLease(runtime);
  }
  if (lstat(runtime->data_link, &status) == 0) {
    if (S_ISLNK(status.st_mode)) {
      if (unlink(runtime->data_link) != 0) {
        (void)fprintf(stderr,
                      "taosQueryTest: pair data link release failed link=%s "
                      "errno=%d message=%s\n",
                      runtime->data_link, errno, strerror(errno));
        code = QST_E_RESOURCE;
        goto _return;
      }
    } else if (S_ISDIR(status.st_mode)) {
      if (qstPairRemoveDataDirectory(runtime->data_link) != QST_OK) {
        (void)fprintf(stderr,
                      "taosQueryTest: pair stale data link release failed "
                      "link=%s errno=%d message=%s\n",
                      runtime->data_link, errno, strerror(errno));
        code = QST_E_RESOURCE;
        goto _return;
      }
    } else {
      (void)fprintf(stderr,
                    "taosQueryTest: pair data link release has unsupported "
                    "type link=%s mode=%o\n",
                    runtime->data_link, status.st_mode);
      code = QST_E_RESOURCE;
      goto _return;
    }
  } else if (errno != ENOENT) {
    (void)fprintf(stderr,
                  "taosQueryTest: pair data link release stat failed link=%s "
                  "errno=%d message=%s\n",
                  runtime->data_link, errno, strerror(errno));
    code = QST_E_RESOURCE;
    goto _return;
  }
  if (qstPairRemoveDataDirectory(runtime->data_dir) != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: pair data directory release failed path=%s "
                  "errno=%d message=%s\n",
                  runtime->data_dir, errno, strerror(errno));
    code = QST_E_RESOURCE;
    goto _return;
  }
  if (qstMkdir(runtime->data_link) != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: pair data link directory create failed "
                  "link=%s errno=%d message=%s\n",
                  runtime->data_link, errno, strerror(errno));
    code = QST_E_RESOURCE;
    goto _return;
  }
_return:
  {
    const EQstCode lease_code = qstPairReleasePortLease(runtime);

    if (code == QST_OK && lease_code != QST_OK) {
      code = lease_code;
    }
  }
  if (code != QST_OK) {
    return code;
  }
  return QST_OK;
}

static EQstCode qstPairWriteServerConfig(const SQstPairRuntime *runtime) {
  char path[4096] = {0};
  char contents[16384] = {0};
  int32_t written = 0;

  if (NULL == runtime || runtime->port == 0 || runtime->config_dir[0] == '\0' ||
      runtime->data_dir[0] == '\0' || runtime->logs_dir[0] == '\0') {
    return QST_E_INVALID_ARGUMENT;
  }
  if (qstMakePath(path, sizeof(path), runtime->config_dir, "taos.cfg") != QST_OK) {
    return QST_E_RESOURCE;
  }
  written = snprintf(contents, sizeof(contents),
                     "fqdn localhost\nserverPort %u\nfirstEp localhost:%u\n"
                     "secondEp localhost:%u\ndataDir %s 0 1\ntempDir %s\n"
                     "logDir %s\n"
                     /* Pair lifecycle and task failures are written by the
                      * tool itself. Each module must be explicit because a
                      * zero global debug flag leaves its module default active. */
                     "federatedQueryEnable 1\n"
                     /* The pair client otherwise creates and destroys one
                      * node allocator for every non-insert request. Keep the
                      * allocator policy local to this isolated client config
                      * so the result-store client remains unchanged. */
                     "queryUseNodeAllocator 0\n"
                     "queryUseMemoryPool 0\n"
                     "asyncLog 0\ndebugFlag 0\n"
                     "simDebugFlag 0\ntmrDebugFlag 0\nuDebugFlag 0\n"
                     "rpcDebugFlag 0\njniDebugFlag 0\nqDebugFlag 0\n"
                     "cDebugFlag 0\ntqClientDebugFlag 0\ndDebugFlag 0\n"
                     "vDebugFlag 0\nmDebugFlag 0\nwDebugFlag 0\n"
                     "azDebugFlag 0\ntssDebugFlag 0\nsDebugFlag 0\n"
                     "tsdbDebugFlag 0\ntqDebugFlag 0\ntqClientDebug 0\n"
                     "fsDebugFlag 0\nudfDebugFlag 0\nsmaDebugFlag 0\n"
                     "idxDebugFlag 0\ntdbDebugFlag 0\nmetaDebugFlag 0\n"
                     "stDebugFlag 0\nsndDebugFlag 0\nbseDebugFlag 0\n"
                     "bndDebugFlag 0\nxndDebugFlag 0\n"
                     /* One server is dedicated to each pair. Smaller worker
                      * pools avoid host-wide scheduling contention across all
                      * sixteen isolated servers while retaining its 96 native
                      * client connections. */
                     "numOfRpcThreads 1\nnumOfTaskQueueThreads 4\n"
                     "supportVnodes 3\nnumOfCommitThreads 1\n"
                     "numOfCompactThreads 1\nnumOfMnodeQueryThreads 1\n"
                     "numOfMnodeReadThreads 1\nnumOfVnodeMgmtThreads 1\n"
                     "numOfVnodeQueryThreads 4\nnumOfVnodeFetchThreads 10\n"
                     "numOfVnodeRsmaThreads 1\nnumOfQnodeQueryThreads 1\n"
                     "numOfMnodeStreamMgmtThreads 2\n"
                     "numOfStreamMgmtThreads 2\n"
                     "numOfVnodeStreamReaderThreads 2\n"
                     "numOfStreamTriggerThreads 4\n"
                     "numOfStreamRunnerThreads 4\n"
                     "streamNotifyWorkerNum 1\ndisableStream 1\nudf 0\n",
                     (unsigned int)runtime->port, (unsigned int)runtime->port,
                     (unsigned int)runtime->port, runtime->data_dir,
                     runtime->data_dir, runtime->logs_dir);
  if (written < 0 || (uint64_t)written >= sizeof(contents)) {
    return QST_E_RESOURCE;
  }
  return qstWriteTextFile(path, contents);
}

static EQstCode qstPairWriteRuntimeFact(const SQstPairRuntime *runtime,
                                        const char *state) {
  char path[4096] = {0};
  char contents[512] = {0};
  int32_t written = 0;

  if (NULL == runtime || NULL == state || runtime->root_dir[0] == '\0' ||
      runtime->slot == UINT32_MAX || runtime->generation == 0 ||
      runtime->port == 0 || runtime->taosc_pid <= 0 ||
      runtime->begin_ts_ns <= 0 || runtime->end_ts_ns < 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (qstMakePath(path, sizeof(path), runtime->root_dir, "runtime.json") !=
      QST_OK) {
    return QST_E_RESOURCE;
  }
  written = snprintf(
      contents, sizeof(contents),
      "{\"parallel_id\":%u,\"slot\":%u,\"generation\":%u,"
      "\"taosd_pid\":%d,\"taosc_pid\":%d,\"taosd_pgid\":%d,"
      "\"taosd_process_slot\":%u,\"taosd_process_generation\":%u,"
      "\"port\":%u,\"begin_ts\":%lld,\"end_ts\":%lld,"
      "\"native_connection_count\":%u,\"async_batch_limit\":%u,"
      "\"max_async_batch_size\":%u,"
      "\"taosd_crash_count\":%u,\"taosd_exit_count\":%u,"
      "\"taosc_crash_count\":%u,\"taosc_exit_count\":%u,"
      "\"state\":\"%s\"}\n",
      runtime->slot, runtime->slot, runtime->generation,
      (int32_t)runtime->taosd_pid, (int32_t)runtime->taosc_pid,
      (int32_t)runtime->taosd_pid, runtime->taosd_slot_id,
      runtime->taosd_generation,
      (unsigned int)runtime->port,
      (long long)(runtime->begin_ts_ns / INT64_C(1000000)),
      (long long)(runtime->end_ts_ns / INT64_C(1000000)),
      runtime->async_window, runtime->async_window,
      runtime->max_async_batch_size,
      runtime->taosd_crash_count, runtime->taosd_exit_count,
      runtime->taosc_crash_count, runtime->taosc_exit_count, state);
  if (written < 0 || (uint64_t)written >= sizeof(contents)) {
    return QST_E_RESOURCE;
  }
  return qstWriteTextFile(path, contents);
}

/* Rewrites only a generated pair fact after its supervisor was lost. */
static EQstCode qstPairRewriteRuntimeState(uint32_t pair_slot,
                                           uint32_t pair_generation,
                                           const char *state,
                                           int32_t supervisor_crashed,
                                           const char *reaped_by) {
  char result_dir[4096] = {0};
  char pairs_dir[4096] = {0};
  char pair_name[96] = {0};
  char pair_dir[4096] = {0};
  char runtime_path[4096] = {0};
  char fact[1024] = {0};
  char rewritten[1024] = {0};
  FILE *file = NULL;
  size_t fact_bytes = 0;
  int32_t written = 0;
  uint32_t parallel_id = 0;
  uint32_t slot = 0;
  uint32_t generation = 0;
  uint32_t taosd_slot_id = 0;
  uint32_t taosd_generation = 0;
  uint16_t port = 0;
  int32_t taosd_pid = 0;
  int32_t taosc_pid = 0;
  int32_t taosd_pgid = 0;
  long long begin_ts = 0;
  long long end_ts = 0;
  uint32_t taosd_crash_count = 0;
  uint32_t taosd_exit_count = 0;
  uint32_t taosc_crash_count = 0;
  uint32_t taosc_exit_count = 0;
  uint32_t async_batch_limit = 0;
  uint32_t max_async_batch_size = 0;
  uint32_t native_connection_count = 0;
  char old_state[32] = {0};
  int32_t assigned = 0;

  if (pair_generation == 0 || NULL == state || state[0] == '\0' ||
      (supervisor_crashed != 0 &&
       (NULL == reaped_by || reaped_by[0] == '\0')) ||
      gQstProcessLogDirectory[0] == '\0' ||
      strlen(gQstProcessLogDirectory) <= strlen("/logs") ||
      strcmp(gQstProcessLogDirectory + strlen(gQstProcessLogDirectory) -
                 strlen("/logs"),
             "/logs") != 0 ||
      snprintf(result_dir, sizeof(result_dir), "%s", gQstProcessLogDirectory) <
          0) {
    return QST_E_PENDING_IO;
  }
  result_dir[strlen(result_dir) - strlen("/logs")] = '\0';
  if (qstMakePath(pairs_dir, sizeof(pairs_dir), result_dir, "pairs") != QST_OK ||
      snprintf(pair_name, sizeof(pair_name), "slot_%u_generation_%u", pair_slot,
               pair_generation) < 0 ||
      qstMakePath(pair_dir, sizeof(pair_dir), pairs_dir, pair_name) != QST_OK ||
      qstMakePath(runtime_path, sizeof(runtime_path), pair_dir, "runtime.json") !=
          QST_OK) {
    return QST_E_RESOURCE;
  }
  file = fopen(runtime_path, "r");
  if (NULL == file) {
    return errno == ENOENT ? QST_E_PENDING_IO : QST_E_RESOURCE;
  }
  fact_bytes = fread(fact, 1, sizeof(fact) - 1U, file);
  if (ferror(file) != 0 || fclose(file) != 0 || fact_bytes == 0 ||
      fact_bytes >= sizeof(fact) - 1U) {
    return QST_E_RESOURCE;
  }
  fact[fact_bytes] = '\0';
  {
    const int32_t was_supervisor_crashed =
        strstr(fact, "\"supervisor_crashed\":true") != NULL;

    assigned = sscanf(fact,
             "{\"parallel_id\":%u,\"slot\":%u,\"generation\":%u,"
             "\"taosd_pid\":%d,\"taosc_pid\":%d,\"taosd_pgid\":%d,"
             "\"taosd_process_slot\":%u,\"taosd_process_generation\":%u,"
             "\"port\":%hu,"
             "\"begin_ts\":%lld,\"end_ts\":%lld,"
             "\"native_connection_count\":%u,\"async_batch_limit\":%u,"
             "\"max_async_batch_size\":%u,"
             "\"taosd_crash_count\":%u,\"taosd_exit_count\":%u,"
             "\"taosc_crash_count\":%u,\"taosc_exit_count\":%u,"
             "\"state\":\"%31[^\"]\"",
             &parallel_id, &slot, &generation, &taosd_pid, &taosc_pid,
             &taosd_pgid, &taosd_slot_id, &taosd_generation, &port, &begin_ts,
             &end_ts, &native_connection_count, &async_batch_limit,
             &max_async_batch_size,
             &taosd_crash_count, &taosd_exit_count, &taosc_crash_count,
             &taosc_exit_count, old_state);
    if ((supervisor_crashed != 0 && was_supervisor_crashed != 0) ||
        (supervisor_crashed == 0 && was_supervisor_crashed == 0) ||
        assigned != 19 || parallel_id != pair_slot || slot != pair_slot ||
        generation != pair_generation || begin_ts <= 0 || port == 0 ||
        native_connection_count != async_batch_limit ||
        taosd_crash_count == UINT32_MAX || taosd_exit_count == UINT32_MAX ||
        taosc_crash_count == UINT32_MAX || taosc_exit_count == UINT32_MAX) {
      (void)fprintf(stderr,
                    "taosQueryTest: pair runtime fact rewrite parse failed "
                    "path=%s assigned=%d slot=%u generation=%u expected_slot=%u "
                    "expected_generation=%u supervisor_crashed=%d "
                    "was_supervisor_crashed=%d\n",
                    runtime_path, assigned, slot, generation, pair_slot,
                    pair_generation, supervisor_crashed, was_supervisor_crashed);
      return QST_E_RESOURCE;
    }
    written = snprintf(
      rewritten, sizeof(rewritten),
        "{\"parallel_id\":%u,\"slot\":%u,\"generation\":%u,"
        "\"taosd_pid\":%d,\"taosc_pid\":%d,\"taosd_pgid\":%d,"
        "\"taosd_process_slot\":%u,\"taosd_process_generation\":%u,"
        "\"port\":%u,"
        "\"begin_ts\":%lld,\"end_ts\":%lld,"
        "\"native_connection_count\":%u,\"async_batch_limit\":%u,"
        "\"max_async_batch_size\":%u,"
        "\"taosd_crash_count\":%u,\"taosd_exit_count\":%u,"
      "\"taosc_crash_count\":%u,\"taosc_exit_count\":%u,"
      "\"state\":\"%s\",\"supervisor_crashed\":true,"
      "\"reaped_by\":\"%s\"}\n",
      parallel_id, slot, generation, taosd_pid, taosc_pid, taosd_pgid,
        taosd_slot_id, taosd_generation, (unsigned int)port, begin_ts,
        (long long)(qstRealtimeNowNs() / INT64_C(1000000)),
        native_connection_count, async_batch_limit,
        max_async_batch_size,
        taosd_crash_count + (supervisor_crashed != 0 ? 1U : 0U),
        taosd_exit_count + (supervisor_crashed == 0 ? 1U : 0U),
        taosc_crash_count + (supervisor_crashed != 0 ? 1U : 0U),
        taosc_exit_count + (supervisor_crashed != 0 ? 1U : 0U), state,
        reaped_by);
  }
  if (written < 0 || (uint64_t)written >= sizeof(rewritten)) {
    return QST_E_RESOURCE;
  }
  return qstWriteTextFile(runtime_path, rewritten);
}

static EQstCode qstPairStopRuntime(SQstShmMapping *mapping,
                                   SQstPairRuntime *runtime) {
  const int32_t profile_enabled = getenv("QST_PROFILE_PHASES") != NULL;
  uint64_t close_client_ns = 0;
  uint64_t terminate_taosd_ns = 0;
  uint64_t reap_and_release_ns = 0;
  uint64_t started_ns = 0;
  int32_t status = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == runtime) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (profile_enabled != 0) {
    started_ns = qstMonotonicNowNs();
  }
  for (uint32_t index = 0; index < runtime->async_window; ++index) {
    if (runtime->clients[index].library != NULL &&
        qst_native_close(&runtime->clients[index]) != QST_OK && code == QST_OK) {
      code = QST_E_RESOURCE;
    }
  }
  qst_result_capture_cache_destroy(&runtime->result_capture_cache);
  if (profile_enabled != 0) {
    const uint64_t finished_ns = qstMonotonicNowNs();

    if (finished_ns >= started_ns) {
      close_client_ns = finished_ns - started_ns;
    }
  }
  if (runtime->taosd_pid <= 0 || runtime->taosd_pidfd < 0) {
    if (runtime->data_dir[0] == '\0' && runtime->data_link[0] == '\0') {
      return QST_OK;
    }
    code = qstPairReleaseRuntimeData(runtime);
    if (code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: pair data cleanup failed slot=%u "
                    "generation=%u code=%d\n",
                    runtime->slot, runtime->generation, code);
    }
    return code;
  }
  code = qstTerminateProcessGroup(runtime->taosd_pid, runtime->taosd_pidfd);
  if (profile_enabled != 0) {
    const uint64_t finished_ns = qstMonotonicNowNs();

    if (finished_ns >= started_ns + close_client_ns) {
      terminate_taosd_ns = finished_ns - started_ns - close_client_ns;
    }
  }
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: pair taosd termination failed slot=%u "
                  "generation=%u taosd_pid=%d code=%d\n",
                  runtime->slot, runtime->generation, (int)runtime->taosd_pid,
                  code);
  }
  errno = 0;
  while (waitpid(runtime->taosd_pid, &status, 0) < 0 && errno == EINTR) {
  }
  if (errno != 0 && errno != ECHILD && code == QST_OK) {
    code = QST_E_RESOURCE;
  }
  (void)close(runtime->taosd_pidfd);
  runtime->taosd_pidfd = -1;
  if (runtime->taosd_exit_count == UINT32_MAX) {
    code = QST_E_RESOURCE;
  } else {
    ++runtime->taosd_exit_count;
    if (runtime->taosd_generation == 0 ||
        qst_process_reap_generation_instance(
            mapping, QST_ROLE_PAIR_SUPERVISOR, runtime->slot,
            runtime->generation, QST_ROLE_TAOSD, runtime->taosd_slot_id,
            runtime->taosd_generation, status) != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: pair taosd reap failed slot=%u generation=%u "
                    "taosd_slot=%u taosd_generation=%u status=%d\n",
                    runtime->slot, runtime->generation, runtime->taosd_slot_id,
                    runtime->taosd_generation, status);
      code = QST_E_RESOURCE;
    } else {
      const EQstCode fact_code = qstPairWriteRuntimeFact(runtime, "reaped");

      if (fact_code != QST_OK && code == QST_OK) {
        (void)fprintf(stderr,
                      "taosQueryTest: pair reaped runtime fact failed slot=%u "
                      "generation=%u code=%d\n",
                      runtime->slot, runtime->generation, fact_code);
        code = QST_E_RESOURCE;
      }
    }
  }
  {
    const EQstCode release_code = qstPairReleaseRuntimeData(runtime);

    if (release_code != QST_OK && code == QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: pair data cleanup failed slot=%u "
                    "generation=%u code=%d\n",
                    runtime->slot, runtime->generation, release_code);
      code = QST_E_RESOURCE;
    }
  }
  if (profile_enabled != 0) {
    const uint64_t finished_ns = qstMonotonicNowNs();

    if (finished_ns >= started_ns + close_client_ns + terminate_taosd_ns) {
      reap_and_release_ns = finished_ns - started_ns - close_client_ns -
                            terminate_taosd_ns;
    }
    (void)fprintf(stderr,
                  "taosQueryTest: pair_runtime_stop_profile slot=%u generation=%u "
                  "close_client_ns=%llu terminate_taosd_ns=%llu "
                  "reap_and_release_ns=%llu\n",
                  runtime->slot, runtime->generation,
                  (unsigned long long)close_client_ns,
                  (unsigned long long)terminate_taosd_ns,
                  (unsigned long long)reap_and_release_ns);
  }
  return code;
}

/* Completes the pair-owned fact before the supervisor itself exits. */
static EQstCode qstPairFinalizeRuntime(SQstPairRuntime *runtime) {
  if (NULL == runtime) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (runtime->begin_ts_ns == 0) {
    return QST_OK;
  }
  runtime->end_ts_ns = qstRealtimeNowNs();
  if (runtime->end_ts_ns == 0 || runtime->end_ts_ns < runtime->begin_ts_ns) {
    return QST_E_RESOURCE;
  }
  if (runtime->taosc_exit_count == UINT32_MAX) {
    return QST_E_RESOURCE;
  }
  ++runtime->taosc_exit_count;
  return qstPairWriteRuntimeFact(runtime, "reaped");
}

static EQstCode qstPairStartRuntime(SQstShmMapping *mapping,
                                    uint32_t pair_slot,
                                    uint32_t pair_generation,
                                    SQstPairRuntime *out_runtime) {
  EQstCode code = qstPairPrepareRuntime(mapping, pair_slot, pair_generation,
                                        out_runtime);
  uint16_t candidate_port = 0;
  int32_t last_fixture_native_code = 0;
  EQstCode last_fixture_code = QST_OK;

  if (code != QST_OK) {
    return code;
  }
  candidate_port = out_runtime->port;
  for (uint32_t attempt = 0; attempt < QST_PAIR_STARTUP_ATTEMPTS; ++attempt) {
    const pid_t supervisor_pid = getpid();
    int32_t start_pipe[2] = {-1, -1};
    const uint8_t start_signal = 1;
    char taosd_pid_text[32] = {0};
    pid_t pid = -1;
    int32_t pidfd = -1;
    int32_t exited = 0;

    if (attempt != 0) {
      uint32_t next_port = (uint32_t)candidate_port +
                           QST_PAIR_PORT_BLOCK_BYTES;

      if (next_port + QST_PAIR_PORT_BLOCK_BYTES - 1U > QST_PAIR_PORT_MAX) {
        next_port = QST_PAIR_PORT_MIN;
      }
      candidate_port = (uint16_t)next_port;
      if (qstPairReleasePortLease(out_runtime) != QST_OK ||
          qstPairAcquirePortLease(out_runtime, candidate_port) != QST_OK) {
        continue;
      }
    }
    code = qstPairWriteServerConfig(out_runtime);
    if (code != QST_OK) {
      (void)qstPairReleaseRuntimeData(out_runtime);
      return code;
    }
    if (pipe(start_pipe) != 0) {
      (void)qstPairReleaseRuntimeData(out_runtime);
      return QST_E_RESOURCE;
    }
    pid = fork();
    if (pid < 0) {
      (void)close(start_pipe[0]);
      (void)close(start_pipe[1]);
      (void)qstPairReleaseRuntimeData(out_runtime);
      return QST_E_RESOURCE;
    }
    if (pid == 0) {
      uint8_t start = 0;

      (void)close(start_pipe[1]);
      if (read(start_pipe[0], &start, sizeof(start)) != (ssize_t)sizeof(start) ||
          close(start_pipe[0]) != 0 || start != 1U) {
        _exit(127);
      }
      /* The supervisor is the only owner allowed to retain this taosd.  A
       * SIGKILL/restart of the supervisor must not leave an orphan server
       * listening on its pair port while the replacement pair starts. */
      if (prctl(PR_SET_PDEATHSIG, SIGTERM) != 0 || getppid() != supervisor_pid ||
          setpgid(0, 0) != 0) {
        _exit(127);
      }
      /* The parent uses TAOS_CONFIG_DIR for its result store. A pair server
       * must honor only its own -c directory, otherwise pair storage collides. */
      if (unsetenv("TAOS_CONFIG_DIR") != 0) {
        _exit(127);
      }
      /* A default core pattern writes relative to the crashing process cwd.
       * Keep every isolated server's artifacts under its pair runtime. */
      if (chdir(out_runtime->cores_dir) != 0) {
        _exit(127);
      }
      execlp("taosd", "taosd", "-c", out_runtime->config_dir, "-o",
             out_runtime->logs_dir, (char *)NULL);
      _exit(127);
    }
    (void)close(start_pipe[0]);
    if (setpgid(pid, pid) != 0 && errno != EACCES && errno != ESRCH) {
      (void)close(start_pipe[1]);
      (void)kill(pid, SIGKILL);
      (void)waitpid(pid, NULL, 0);
      (void)qstPairReleaseRuntimeData(out_runtime);
      return QST_E_RESOURCE;
    }
    code = qst_process_register_instance_ex(
        mapping, QST_ROLE_PAIR_SUPERVISOR, pair_slot, pair_generation,
        QST_ROLE_TAOSD, QST_PROCESS_SLOT_AUTO, (int32_t)pid, (int32_t)pid,
        &out_runtime->taosd_slot_id,
        &out_runtime->taosd_generation);
    if (code != QST_OK ||
        write(start_pipe[1], &start_signal, sizeof(start_signal)) !=
            (ssize_t)sizeof(start_signal)) {
      (void)close(start_pipe[1]);
      (void)kill(-pid, SIGKILL);
      (void)waitpid(pid, NULL, 0);
      (void)qstPairReleaseRuntimeData(out_runtime);
      return code == QST_OK ? QST_E_RESOURCE : code;
    }
    (void)close(start_pipe[1]);
    pidfd = qstOpenPidfd(pid);
    if (pidfd < 0) {
      (void)kill(-pid, SIGKILL);
      (void)waitpid(pid, NULL, 0);
      (void)qst_process_reap_generation_instance(
          mapping, QST_ROLE_PAIR_SUPERVISOR, pair_slot, pair_generation,
          QST_ROLE_TAOSD, out_runtime->taosd_slot_id,
          out_runtime->taosd_generation, 0);
      (void)qstPairReleaseRuntimeData(out_runtime);
      return QST_E_RESOURCE;
    }
    out_runtime->taosd_pid = pid;
    out_runtime->taosd_pidfd = pidfd;
    if (snprintf(taosd_pid_text, sizeof(taosd_pid_text), "%d", (int)pid) < 0 ||
        setenv("QST_FAKE_TAOSD_PID", taosd_pid_text, 1) != 0 ||
        setenv("QST_FAKE_TAOSD_CORES_DIR", out_runtime->cores_dir, 1) != 0) {
      (void)qstPairStopRuntime(mapping, out_runtime);
      return QST_E_RESOURCE;
    }
    /* A forked server can fail before it begins listening. Wait briefly before
     * the first connector attempt so an immediate exit is retried as startup,
     * not discovered later while a task is already owned by this pair. */
    if (qstWaitForExit(pidfd, QST_PAIR_STARTUP_SETTLE_MILLISECONDS, &exited) !=
            QST_OK ||
        exited != 0) {
      (void)qstPairStopRuntime(mapping, out_runtime);
      if (qstPairPrepareRuntimeData(out_runtime) != QST_OK) {
        return QST_E_RESOURCE;
      }
      continue;
    }
    for (uint32_t wait_count = 0;
         wait_count < QST_PAIR_STARTUP_MILLISECONDS / QST_PAIR_QUERY_POLL_MILLISECONDS;
         ++wait_count) {
      /* The pair server needs its private config directory, but the client
       * must retain the caller's established config and external providers.
       * Passing the pair directory here reconfigures the process-global native
       * client after fork and loses the caller's federation settings. */
      code = QST_OK;
      for (uint32_t client_index = 0;
           client_index < out_runtime->async_window; ++client_index) {
        code = qst_native_open_with_endpoint(
            &out_runtime->clients[client_index], "127.0.0.1", out_runtime->port,
            out_runtime->config_dir);
        if (code != QST_OK) {
          (void)fprintf(stderr,
                        "taosQueryTest: pair runtime client open failed "
                        "slot=%u generation=%u port=%u client_index=%u "
                        "code=%d wait=%u\n",
                        out_runtime->slot, out_runtime->generation,
                        (unsigned int)out_runtime->port, client_index, code,
                        wait_count);
          break;
        }
      }
      if (code == QST_OK) {
        code = qst_dataset_materialize_local_fixture_silent(
            &out_runtime->clients[0], &last_fixture_native_code);
        last_fixture_code = code;
        if (code != QST_OK &&
            last_fixture_native_code != QST_NATIVE_CODE_NO_ENOUGH_DNODES) {
          (void)fprintf(stderr,
                        "taosQueryTest: pair fixture materialization failed "
                        "slot=%u generation=%u wait=%u code=%d native_code=%d\n",
                        out_runtime->slot, out_runtime->generation, wait_count,
                        code, last_fixture_native_code);
        }
      }
      if (code == QST_OK) {
        code = qstPairWriteRuntimeFact(out_runtime, "ready");
        if (code != QST_OK) {
          (void)fprintf(stderr,
                        "taosQueryTest: pair runtime ready fact failed slot=%u "
                        "generation=%u code=%d\n",
                        out_runtime->slot, out_runtime->generation, code);
        }
        return code;
      }
      for (uint32_t client_index = 0;
           client_index < out_runtime->async_window; ++client_index) {
        if (out_runtime->clients[client_index].library != NULL &&
            qst_native_close(&out_runtime->clients[client_index]) != QST_OK) {
          (void)qstPairStopRuntime(mapping, out_runtime);
          return QST_E_RESOURCE;
        }
      }
      if (qstWaitForExit(pidfd, QST_PAIR_QUERY_POLL_MILLISECONDS, &exited) !=
              QST_OK ||
          exited != 0) {
        break;
      }
    }
    if (last_fixture_code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: pair runtime fixture readiness timed out "
                    "slot=%u generation=%u code=%d native_code=%d\n",
                    out_runtime->slot, out_runtime->generation,
                    last_fixture_code, last_fixture_native_code);
    }
    (void)qstPairStopRuntime(mapping, out_runtime);
    if (qstPairPrepareRuntimeData(out_runtime) != QST_OK) {
      return QST_E_RESOURCE;
    }
  }
  (void)qstPairReleaseRuntimeData(out_runtime);
  return QST_E_RESOURCE;
}

/* Reaps an already-exited server while retaining the pair supervisor and its
 * task ownership. The caller must have drained every callback using its native
 * client before invoking this function. */
static EQstCode qstPairReapExitedTaosd(SQstShmMapping *mapping,
                                       SQstPairRuntime *runtime,
                                       int32_t *out_signaled) {
  int32_t status = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == runtime || NULL == out_signaled ||
      runtime->taosd_pid <= 0 || runtime->taosd_pidfd < 0 ||
      runtime->taosd_generation == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  while (waitpid(runtime->taosd_pid, &status, 0) < 0) {
    if (errno != EINTR) {
      return QST_E_RESOURCE;
    }
  }
  *out_signaled = WIFSIGNALED(status) ? 1 : 0;
  if (*out_signaled != 0) {
    if (runtime->taosd_crash_count == UINT32_MAX) {
      return QST_E_RESOURCE;
    }
    runtime->last_taosd_crash_pid = runtime->taosd_pid;
    ++runtime->taosd_crash_count;
  }
  if (runtime->taosd_exit_count == UINT32_MAX) {
    return QST_E_RESOURCE;
  }
  ++runtime->taosd_exit_count;
  (void)close(runtime->taosd_pidfd);
  runtime->taosd_pidfd = -1;
  runtime->taosd_pid = -1;
  code = qst_process_reap_generation_instance(
      mapping, QST_ROLE_PAIR_SUPERVISOR, runtime->slot, runtime->generation,
      QST_ROLE_TAOSD, runtime->taosd_slot_id, runtime->taosd_generation, status);
  return code == QST_OK ? QST_OK : QST_E_RESOURCE;
}

/* Restarts only the isolated server and native connection after a server crash.
 * The supervisor generation and all batch task ownership remain unchanged. */
static EQstCode qstPairRestartAfterTaosdCrash(SQstShmMapping *mapping,
                                               SQstPairRuntime *runtime) {
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == runtime || runtime->slot == UINT32_MAX ||
      runtime->generation == 0 || runtime->taosd_pidfd >= 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (uint32_t index = 0; index < runtime->async_window; ++index) {
    if (runtime->clients[index].library != NULL &&
        qst_native_close(&runtime->clients[index]) != QST_OK) {
      return QST_E_RESOURCE;
    }
  }
  code = qstPairReleaseRuntimeData(runtime);
  if (code != QST_OK) {
    return code;
  }
  return qstPairStartRuntime(mapping, runtime->slot, runtime->generation,
                             runtime);
}

static int32_t qstPairHasCoreArtifact(const SQstPairRuntime *runtime) {
  DIR *directory = NULL;
  struct dirent *entry = NULL;
  char pid_text[32] = {0};
  int32_t found = 0;

  if (NULL == runtime || runtime->cores_dir[0] == '\0' ||
      runtime->taosd_pid <= 0 ||
      snprintf(pid_text, sizeof(pid_text), "%d", (int)runtime->taosd_pid) < 0) {
    return 0;
  }
  directory = opendir(runtime->cores_dir);
  if (NULL == directory) {
    return 0;
  }
  while ((entry = readdir(directory)) != NULL) {
    char path[4096] = {0};
    struct stat status = {0};

    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0 ||
        strstr(entry->d_name, pid_text) == NULL ||
        snprintf(path, sizeof(path), "%s/%s", runtime->cores_dir,
                 entry->d_name) < 0 ||
        lstat(path, &status) != 0) {
      continue;
    }
    if (S_ISREG(status.st_mode)) {
      found = 1;
      break;
    }
  }
  (void)closedir(directory);
  return found;
}

/* Captures a retained core artifact path relative to the run directory. */
static EQstCode qstPairAttachCorePath(SQstShmMapping *mapping,
                                      const SQstPairRuntime *runtime,
                                      uint64_t gid, SQstRef *out_ref) {
  DIR *directory = NULL;
  struct dirent *entry = NULL;
  const char *relative_root = NULL;
  char crashed_pid_text[32] = {0};
  char selected_name[256] = {0};
  char fallback_name[256] = {0};
  char relative_path[4096] = {0};
  int32_t written = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == runtime || NULL == out_ref || gid == 0 ||
      runtime->cores_dir[0] == '\0') {
    return QST_E_INVALID_ARGUMENT;
  }
  memset(out_ref, 0, sizeof(*out_ref));
  if (runtime->last_taosd_crash_pid > 0 &&
      snprintf(crashed_pid_text, sizeof(crashed_pid_text), "%d",
               (int)runtime->last_taosd_crash_pid) < 0) {
    return QST_E_RESOURCE;
  }
  directory = opendir(runtime->cores_dir);
  if (NULL == directory) {
    return errno == ENOENT ? QST_OK : QST_E_RESOURCE;
  }
  relative_root = strstr(runtime->root_dir, "/pairs/");
  if (NULL != relative_root) {
    ++relative_root;
  }
  while ((entry = readdir(directory)) != NULL) {
    char candidate[4096] = {0};
    struct stat status = {0};

    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    if (snprintf(candidate, sizeof(candidate), "%s/%s", runtime->cores_dir,
                 entry->d_name) < 0 ||
        lstat(candidate, &status) != 0 || !S_ISREG(status.st_mode)) {
      continue;
    }
    if (fallback_name[0] == '\0') {
      /* Keep the first pair-local artifact as a fallback for core patterns
       * that do not include a PID in the generated filename. */
      written = snprintf(fallback_name, sizeof(fallback_name), "%s", entry->d_name);
      if (written < 0 || (uint64_t)written >= sizeof(fallback_name)) {
        fallback_name[0] = '\0';
      }
    }
    if (crashed_pid_text[0] == '\0' ||
        strstr(entry->d_name, crashed_pid_text) != NULL) {
      written = snprintf(selected_name, sizeof(selected_name), "%s", entry->d_name);
      if (written < 0 || (uint64_t)written >= sizeof(selected_name)) {
        (void)closedir(directory);
        return QST_E_RESOURCE;
      }
      break;
    }
  }
  if (closedir(directory) != 0) {
    return QST_E_RESOURCE;
  }
  if (selected_name[0] == '\0' && fallback_name[0] != '\0') {
    written = snprintf(selected_name, sizeof(selected_name), "%s", fallback_name);
    if (written < 0 || (uint64_t)written >= sizeof(selected_name)) {
      return QST_E_RESOURCE;
    }
  }
  if (selected_name[0] == '\0') {
    return QST_OK;
  }
  if (relative_root == NULL) {
    return QST_E_RESOURCE;
  }
  written = snprintf(relative_path, sizeof(relative_path), "%s/cores/%s",
                     relative_root, selected_name);
  if (written < 0 || (uint64_t)written >= sizeof(relative_path)) {
    return QST_E_RESOURCE;
  }
  code = qst_block_alloc(mapping, gid, 1, strlen(relative_path), out_ref);
  if (code != QST_OK) {
    return code;
  }
  code = qst_block_write_seal(mapping, out_ref, relative_path,
                              strlen(relative_path));
  if (code != QST_OK) {
    (void)qst_block_abort(mapping, *out_ref, gid);
    memset(out_ref, 0, sizeof(*out_ref));
  }
  return code;
}

/* Publishes the SQL that reproduced the server crash during serial replay. */
static EQstCode qstPairPublishCoreFinal(SQstShmMapping *mapping,
                                        SQstPairRuntime *runtime,
                                        const SQstTaskDispatch *dispatch) {
  SQstExecutePayload payload = {0};
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == runtime || NULL == dispatch ||
      dispatch->gid == 0 || runtime->generation == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  payload.statement_ts_ns = qstRealtimeNowNs();
  payload.ended_ts_ns = payload.statement_ts_ns;
  payload.taosc_pid = (int32_t)getpid();
  payload.taosd_pid = (int32_t)runtime->last_taosd_crash_pid;
  payload.execution_status = QST_EXEC_ERROR;
  payload.execution_error_code = QST_E_RESOURCE;
  payload.result_row_count = -1;
  if (payload.statement_ts_ns == 0 ||
      qst_task_set_result_storage(
          mapping, dispatch->gid, dispatch->dispatch_epoch,
          dispatch->transition_seq, runtime->slot, runtime->generation,
          QST_RESULT_UNAVAILABLE) != QST_OK) {
    return QST_E_RESOURCE;
  }
  code = qstPairAttachCorePath(mapping, runtime, dispatch->gid,
                               &payload.core_path_ref);
  if (code != QST_OK) {
    return code;
  }
  code = qst_task_publish_final(
      mapping, dispatch->gid, dispatch->dispatch_epoch,
      dispatch->transition_seq, QST_ROLE_PAIR_SUPERVISOR, runtime->slot,
      runtime->generation, &payload);
  if (code != QST_OK) {
    if (!qstRefIsEmpty(payload.core_path_ref)) {
      (void)qst_block_release(mapping, payload.core_path_ref, dispatch->gid);
    }
    return code;
  }
  return qstPairForwardFinalTask(
      mapping, runtime->slot, runtime->generation, dispatch,
      qstMillisecondsToDeadlineNs(QST_FLOW_LEASE_MILLISECONDS),
      &runtime->async_profile);
}

static EQstCode qstMainBuildPairInfo(const SQstRunConfig *config,
                                     char *out_json, uint64_t out_bytes) {
  char pairs_dir[4096] = {0};
  uint64_t used = 0;
  uint32_t pair_count = 0;
  DIR *directory = NULL;
  struct dirent *entry = NULL;

  if (NULL == config || NULL == config->result_dir || NULL == out_json ||
      out_bytes == 0 ||
      qstMakePath(pairs_dir, sizeof(pairs_dir), config->result_dir, "pairs") !=
          QST_OK) {
    return QST_E_INVALID_ARGUMENT;
  }
  directory = opendir(pairs_dir);
  if (directory == NULL) {
    if (errno == ENOENT) {
      const int32_t written = snprintf(
          out_json, (size_t)out_bytes, "\"parallel_pair_count\":0,\"pairs\":[]");

      return written >= 0 && (uint64_t)written < out_bytes ? QST_OK :
                                                             QST_E_RESOURCE;
    }
    return QST_E_RESOURCE;
  }
  out_json[0] = '\0';
  used = 0;
  while ((entry = readdir(directory)) != NULL) {
    char pair_dir[4096] = {0};
    char runtime_path[4096] = {0};
    char fact[1024] = {0};
    FILE *file = NULL;
    size_t fact_bytes = 0;
    int32_t written = 0;

    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    if (qstMakePath(pair_dir, sizeof(pair_dir), pairs_dir, entry->d_name) !=
            QST_OK ||
        qstMakePath(runtime_path, sizeof(runtime_path), pair_dir,
                    "runtime.json") != QST_OK) {
      (void)closedir(directory);
      return QST_E_RESOURCE;
    }
    file = fopen(runtime_path, "r");
    if (file == NULL) {
      (void)closedir(directory);
      return QST_E_RESOURCE;
    }
    fact_bytes = fread(fact, 1, sizeof(fact) - 1U, file);
    if (ferror(file) != 0 || fclose(file) != 0 || fact_bytes == 0 ||
        fact_bytes >= sizeof(fact) - 1U ||
        strncmp(fact, "{\"parallel_id\":", 15) != 0 ||
        strstr(fact, "\"taosd_pid\":") == NULL ||
        strstr(fact, "\"taosc_pid\":") == NULL ||
        strstr(fact, "\"begin_ts\":") == NULL ||
        strstr(fact, "\"end_ts\":") == NULL ||
        strstr(fact, "\"taosd_crash_count\":") == NULL ||
        strstr(fact, "\"taosd_exit_count\":") == NULL ||
        strstr(fact, "\"taosc_crash_count\":") == NULL ||
        strstr(fact, "\"taosc_exit_count\":") == NULL ||
        (strstr(fact, "\"state\":\"reaped\"") == NULL &&
         (strstr(fact, "\"state\":\"crashed\"") == NULL ||
          strstr(fact, "\"supervisor_crashed\":true") == NULL))) {
      (void)closedir(directory);
      return QST_E_RESOURCE;
    }
    fact[fact_bytes] = '\0';
    while (fact_bytes > 0 && (fact[fact_bytes - 1U] == '\n' ||
                              fact[fact_bytes - 1U] == '\r')) {
      fact[--fact_bytes] = '\0';
    }
    written = snprintf(out_json + used, (size_t)(out_bytes - used), "%s%s",
                       pair_count == 0 ? "" : ",", fact);
    if (written < 0 || (uint64_t)written >= out_bytes - used) {
      (void)closedir(directory);
      return QST_E_RESOURCE;
    }
    used += (uint64_t)written;
    ++pair_count;
  }
  if (closedir(directory) != 0) {
    return QST_E_RESOURCE;
  }
  {
    char prefix[128] = {0};
    const int32_t prefix_bytes = snprintf(
        prefix, sizeof(prefix), "\"parallel_pair_count\":%u,\"pairs\":[",
        pair_count);
    if (prefix_bytes < 0 || (uint64_t)prefix_bytes >= sizeof(prefix) ||
        out_bytes <= (uint64_t)prefix_bytes + 2U ||
        used > out_bytes - (uint64_t)prefix_bytes - 2U) {
      return QST_E_RESOURCE;
    }
    /* The directory walk writes the comma-separated facts at the beginning
     * of out_json. Prefix them in place so the complete list can exceed the
     * historical 8 KiB result-column width and be handled by overflow
     * storage at the result-store boundary. */
    memmove(out_json + prefix_bytes, out_json, (size_t)used + 1U);
    memcpy(out_json, prefix, (size_t)prefix_bytes);
    out_json[prefix_bytes + used] = ']';
    out_json[prefix_bytes + used + 1U] = '\0';
  }
  return QST_OK;
}

/* Captures correctness results before their native TAOS_RES is released. */
static EQstCode qstPairStoreNormalizedResult(
    SQstShmMapping *mapping, const SQstTaskDispatch *dispatch,
    uint32_t pair_slot, uint32_t pair_generation, SQstQueryResult *normalized,
    SQstExecutePayload *out_payload, SQstPairAsyncProfile *profile) {
  SQstRef result_ref = {0};
  int64_t row_count = 0;
  int32_t sealed = 0;
  uint64_t stage_started_ns = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == dispatch || normalized == NULL ||
      NULL == out_payload) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (dispatch->focus != QST_FOCUS_CORRECTNESS) {
    return qst_task_set_result_storage(
        mapping, dispatch->gid, dispatch->dispatch_epoch,
        dispatch->transition_seq, pair_slot, pair_generation,
        QST_RESULT_NOT_REQUESTED);
  }
  if (normalized->row_count < 0) {
    code = QST_E_RESULT_STORE;
  }
  if (profile != NULL && profile->enabled != 0 && normalized->row_count >= 0) {
    ++profile->result_document_count;
    profile->result_document_bytes += normalized->bytes;
    profile->result_row_count += (uint64_t)normalized->row_count;
    if (normalized->row_count == 0) {
      ++profile->result_zero_row_count;
    }
  }
  if (code == QST_OK) {
    code = qst_block_alloc(mapping, dispatch->gid, 1, normalized->bytes,
                           &result_ref);
  }
  if (profile != NULL && profile->enabled != 0) {
    qstPairAsyncProfileAddElapsed(profile, stage_started_ns,
                                  qstMonotonicNowNs(),
                                  &profile->result_block_alloc_ns);
    stage_started_ns = qstMonotonicNowNs();
  }
  if (code == QST_OK) {
    code = qst_block_write_seal(mapping, &result_ref, normalized->data,
                                normalized->bytes);
    sealed = code == QST_OK ? 1 : 0;
  }
  if (profile != NULL && profile->enabled != 0) {
    qstPairAsyncProfileAddElapsed(profile, stage_started_ns,
                                  qstMonotonicNowNs(),
                                  &profile->result_block_write_seal_ns);
  }
  row_count = normalized->row_count;
  qst_result_reset(normalized);
  if (code != QST_OK) {
    if (!qstRefIsEmpty(result_ref)) {
      if (sealed != 0) {
        (void)qst_block_release(mapping, result_ref, dispatch->gid);
      } else {
        (void)qst_block_abort(mapping, result_ref, dispatch->gid);
      }
    }
    return qst_task_set_result_storage(
        mapping, dispatch->gid, dispatch->dispatch_epoch,
        dispatch->transition_seq, pair_slot, pair_generation,
        QST_RESULT_UNAVAILABLE);
  }
  out_payload->result_ref = result_ref;
  out_payload->result_row_count = row_count;
  if (profile != NULL && profile->enabled != 0) {
    stage_started_ns = qstMonotonicNowNs();
  }
  code = qst_task_set_result_storage(
      mapping, dispatch->gid, dispatch->dispatch_epoch, dispatch->transition_seq,
      pair_slot, pair_generation, QST_RESULT_MEMORY);
  if (profile != NULL && profile->enabled != 0) {
    qstPairAsyncProfileAddElapsed(profile, stage_started_ns,
                                  qstMonotonicNowNs(),
                                  &profile->result_storage_ns);
  }
  return code;
}

/* Captures correctness results before their native TAOS_RES is released. */
static EQstCode qstPairCaptureQueryResult(SQstShmMapping *mapping,
                                          const SQstTaskDispatch *dispatch,
                                          uint32_t pair_slot,
                                          uint32_t pair_generation,
                                          SQstNativeClient *client,
                                          void *native_result,
                                          SQstExecutePayload *out_payload,
                                          SQstResultCaptureCache *cache,
                                          SQstPairAsyncProfile *profile) {
  SQstQueryResult normalized = {0};
  uint64_t started_ns = 0;
  EQstCode code = QST_OK;

  if (profile != NULL && profile->enabled != 0) {
    started_ns = qstMonotonicNowNs();
  }
  if (getenv("QST_ENABLE_VERIFIED_RESULT_CACHE") != NULL) {
    code = qst_result_capture_verified_cached(
        client, native_result, &dispatch->sql_sha256, cache, &normalized);
  } else if (getenv("QST_ENABLE_RAW_RESULT_CAPTURE") != NULL) {
    code = qst_native_capture_raw_result(client, native_result, &normalized);
    if (code == QST_OK) {
      if (profile != NULL && profile->enabled != 0) {
        ++profile->raw_result_capture_count;
      }
    } else if (code == QST_E_VERSION_CAPABILITY) {
      if (profile != NULL && profile->enabled != 0) {
        ++profile->raw_result_capture_fallback_count;
      }
      code = qst_native_capture_result(client, native_result, &normalized);
    }
  } else {
    code = qst_native_capture_result(client, native_result, &normalized);
  }
  if (profile != NULL && profile->enabled != 0) {
    qstPairAsyncProfileAddElapsed(profile, started_ns, qstMonotonicNowNs(),
                                  &profile->result_native_capture_ns);
  }
  if (code != QST_OK) {
    return qst_task_set_result_storage(
        mapping, dispatch->gid, dispatch->dispatch_epoch,
        dispatch->transition_seq, pair_slot, pair_generation,
        QST_RESULT_UNAVAILABLE);
  }
  return qstPairStoreNormalizedResult(mapping, dispatch, pair_slot,
                                      pair_generation, &normalized,
                                      out_payload, profile);
}

/* Fetches and normalizes only private native result handles. Shared task and
 * block state remains owned by the pair supervisor after all workers join. */
static EQstCode qstPairAllocateSid(_Atomic uint64_t *inout_sid,
                                   uint64_t *out_sid) {
  uint64_t expected = 0;

  if (NULL == inout_sid || NULL == out_sid) {
    return QST_E_INVALID_ARGUMENT;
  }
  expected = atomic_load_explicit(inout_sid, memory_order_acquire);
  for (;;) {
    if (expected == UINT64_MAX ||
        !atomic_compare_exchange_weak_explicit(
            inout_sid, &expected, expected + 1U, memory_order_acq_rel,
            memory_order_acquire)) {
      if (expected == UINT64_MAX) {
        return QST_E_RESOURCE;
      }
      continue;
    }
    *out_sid = expected + 1U;
    return QST_OK;
  }
}

/* Executes one ledger-owned task and publishes only its immutable final evidence. */
static EQstCode qstPairExecuteAssignedTask(SQstShmMapping *mapping,
                                           uint32_t pair_slot,
                                           uint32_t pair_generation,
                                           SQstNativeClient *runtime_client,
                                           pid_t taosd_pid,
                                           const SQstTaskDispatch *dispatch,
                                           _Atomic uint64_t *inout_sid) {
  SQstNativeClient local_client = {0};
  SQstNativeClient *client = runtime_client;
  SQstPairQueryCompletion completion = {0};
  SQstExecutePayload final_payload = {0};
  char *sql = NULL;
  uint64_t query_deadline = 0;
  uint64_t cancel_deadline = 0;
  int32_t cancel_requested = 0;
  int32_t stop_requested = 0;
  int32_t local_client_owned = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == mapping->header || NULL == dispatch ||
      NULL == inout_sid ||
      pair_generation == 0 || dispatch->gid == 0 ||
      mapping->header->statement_timeout_seconds == 0 ||
      mapping->header->statement_timeout_seconds > UINT32_MAX / 1000U) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qstPairCopySql(mapping, dispatch, &sql);
  if (code != QST_OK) {
    if (local_client_owned != 0) {
      (void)qst_native_close(&local_client);
    }
    return code;
  }
  final_payload.statement_ts_ns = qstRealtimeNowNs();
  if (final_payload.statement_ts_ns == 0) {
    free(sql);
    return QST_E_RESOURCE;
  }
  final_payload.taosc_pid = (int32_t)getpid();
  final_payload.taosd_pid = taosd_pid > 0 ? (int32_t)taosd_pid : 0;
  atomic_init(&completion.completed, 0);
  if (runtime_client == NULL) {
    code = qst_native_open(&local_client);
    local_client_owned = code == QST_OK ? 1 : 0;
    client = &local_client;
  } else {
    if (runtime_client->connection == NULL) {
      free(sql);
      return QST_E_INVALID_ARGUMENT;
    }
    client = runtime_client;
  }
  completion.client = client;
  completion.event_fd = -1;
  if (code == QST_OK) {
    query_deadline = qstMillisecondsToDeadlineNs(
        (uint64_t)mapping->header->statement_timeout_seconds * 1000U);
    if (query_deadline == 0) {
      code = QST_E_RESOURCE;
    }
  }
  if (code == QST_OK) {
    code = qst_native_execute_async(client, sql, qstPairQueryCallback,
                                    &completion);
    if (code == QST_OK) {
      code = qstPairAllocateSid(inout_sid, &final_payload.sid);
    }
  }
  free(sql);
  sql = NULL;
  if (code != QST_OK) {
    final_payload.execution_status = QST_EXEC_ERROR;
    final_payload.execution_error_code = code;
    final_payload.ended_ts_ns = qstRealtimeNowNs();
    return final_payload.ended_ts_ns == 0 ? QST_E_RESOURCE :
        qst_task_publish_final(mapping, dispatch->gid, dispatch->dispatch_epoch,
                               dispatch->transition_seq,
                               QST_ROLE_PAIR_SUPERVISOR, pair_slot,
                               pair_generation, &final_payload);
  }

  for (;;) {
    uint32_t stop_generation = 0;
    uint32_t exit_state = 0;
    const uint64_t now_deadline = qstMillisecondsToDeadlineNs(1);

    if (atomic_load_explicit(&completion.completed, memory_order_acquire) != 0) {
      final_payload.execution_status = stop_requested != 0 ? QST_EXEC_CANCELLED :
          cancel_requested != 0 ? QST_EXEC_TIMEOUT :
          completion.code == 0 ? QST_EXEC_SUCCESS : QST_EXEC_ERROR;
      final_payload.execution_error_code = completion.code;
      if (NULL != completion.result) {
        if (completion.captured != 0 && cancel_requested == 0) {
          code = qstPairStoreNormalizedResult(
              mapping, dispatch, pair_slot, pair_generation,
              &completion.captured_result, &final_payload, NULL);
        } else if (completion.code == 0 && cancel_requested == 0) {
          code = qstPairCaptureQueryResult(
              mapping, dispatch, pair_slot, pair_generation, client,
              completion.result, &final_payload, NULL, NULL);
        } else if (cancel_requested == 0) {
          code = qst_task_set_result_storage(
              mapping, dispatch->gid, dispatch->dispatch_epoch,
              dispatch->transition_seq, pair_slot, pair_generation,
              dispatch->focus == QST_FOCUS_CORRECTNESS ?
                  QST_RESULT_UNAVAILABLE : QST_RESULT_NOT_REQUESTED);
        }
        client->free_result(completion.result);
        completion.result = NULL;
        if (code != QST_OK) {
          if (local_client_owned != 0) {
            (void)qst_native_close(&local_client);
          }
          return code;
        }
      } else if (cancel_requested == 0) {
        code = qst_task_set_result_storage(
            mapping, dispatch->gid, dispatch->dispatch_epoch,
            dispatch->transition_seq, pair_slot, pair_generation,
            dispatch->focus == QST_FOCUS_CORRECTNESS ? QST_RESULT_UNAVAILABLE :
                QST_RESULT_NOT_REQUESTED);
        if (code != QST_OK) {
          if (local_client_owned != 0) {
            (void)qst_native_close(&local_client);
          }
          return code;
        }
      }
      if (local_client_owned != 0 && qst_native_close(&local_client) != QST_OK) {
        return QST_E_RESOURCE;
      }
      final_payload.ended_ts_ns = qstRealtimeNowNs();
      if (final_payload.ended_ts_ns == 0) {
        return QST_E_RESOURCE;
      }
      code = qst_task_publish_final(
          mapping, dispatch->gid, dispatch->dispatch_epoch,
          dispatch->transition_seq, QST_ROLE_PAIR_SUPERVISOR, pair_slot,
          pair_generation, &final_payload);
      if (code != QST_OK) {
        if (!qstRefIsEmpty(final_payload.result_ref)) {
          (void)qst_block_release(mapping, final_payload.result_ref,
                                  dispatch->gid);
        }
        return code;
      }
      return qst_process_record_business_instance(
          mapping, QST_ROLE_PAIR_SUPERVISOR, pair_slot, pair_generation,
          dispatch->gid, query_deadline);
    }
    if (now_deadline == 0) {
      return QST_E_RESOURCE;
    }
    code = qst_process_get_stop_generation_instance(
        mapping, QST_ROLE_PAIR_SUPERVISOR, pair_slot, pair_generation,
        &stop_generation, &exit_state);
    if (code != QST_OK) {
      return code;
    }
    if (exit_state == QST_PROCESS_STOP_REQUESTED && stop_generation != 0) {
      if (runtime_client == NULL) {
        qstTestMaybeKillRoleOnStop(QST_ROLE_PAIR_SUPERVISOR, pair_generation,
                                   pair_slot);
        (void)qst_native_kill_query(client);
        return qst_process_ack_stop_instance(
            mapping, QST_ROLE_PAIR_SUPERVISOR, pair_slot, pair_generation,
            stop_generation);
      }
      if (cancel_requested == 0) {
        qstTestMaybeKillRoleOnStop(QST_ROLE_PAIR_SUPERVISOR, pair_generation,
                                   pair_slot);
        code = qst_native_kill_query(client);
        if (code != QST_OK) {
          return code;
        }
        cancel_deadline = qstMillisecondsToDeadlineNs(
            QST_QUERY_CANCEL_GRACE_MILLISECONDS);
        if (cancel_deadline == 0) {
          return QST_E_RESOURCE;
        }
        cancel_requested = 1;
        stop_requested = 1;
      }
      continue;
    }
    if (exit_state != QST_PROCESS_RUNNING) {
      return QST_E_STALE_OWNER;
    }
    if (cancel_requested == 0 && now_deadline >= query_deadline) {
      code = qst_native_kill_query(client);
      if (code != QST_OK) {
        return code;
      }
      cancel_deadline = qstMillisecondsToDeadlineNs(
          QST_QUERY_CANCEL_GRACE_MILLISECONDS);
      if (cancel_deadline == 0) {
        return QST_E_RESOURCE;
      }
      cancel_requested = 1;
    } else if (cancel_requested != 0 && now_deadline >= cancel_deadline) {
      /* The callback can still reference local connector state. Let process
       * recovery terminate this pair instead of closing it underneath callback. */
      if (qst_process_record_error_instance(
              mapping, QST_ROLE_PAIR_SUPERVISOR, pair_slot, pair_generation,
              QST_E_DEADLINE) != QST_OK) {
        return QST_E_RESOURCE;
      }
      return QST_E_DEADLINE;
    }
    code = qst_process_confirm_flow_instance(
        mapping, QST_ROLE_PAIR_SUPERVISOR, pair_slot, pair_generation,
        QST_FLOW_STAGE_IDLE, QST_FLOW_STATE_INPUT_QUEUE,
        dispatch->transition_seq, dispatch->gid, query_deadline);
    if (code != QST_OK) {
      return code;
    }
    {
      struct pollfd event = {0};

      event.fd = mapping->queue_eventfd[QST_QUEUE_CONTROL - 1];
      event.events = POLLIN;
      if (poll(&event, 1, QST_PAIR_QUERY_POLL_MILLISECONDS) < 0 &&
          errno != EINTR) {
        return QST_E_RESOURCE;
      }
    }
  }
}

static void qstPairAsyncProfileAddElapsed(SQstPairAsyncProfile *profile,
                                          uint64_t started_ns,
                                          uint64_t finished_ns,
                                          uint64_t *total_ns) {
  uint64_t elapsed_ns = 0;

  if (NULL == profile || NULL == total_ns || profile->enabled == 0 ||
      started_ns == 0 || finished_ns < started_ns) {
    return;
  }
  elapsed_ns = finished_ns - started_ns;
  *total_ns = *total_ns > UINT64_MAX - elapsed_ns ? UINT64_MAX :
                                                     *total_ns + elapsed_ns;
}

/* Result normalization remains ordered with task publication so shared ledger
 * ownership and block transitions are serialized by the pair supervisor. */
static void qstPairWriteAsyncProfile(const SQstPairRuntime *runtime) {
  const SQstPairAsyncProfile *profile = NULL;

  if (NULL == runtime || runtime->async_profile.enabled == 0) {
    return;
  }
  profile = &runtime->async_profile;
  (void)fprintf(
      stderr,
      "taosQueryTest: pair_async_profile slot=%u generation=%u batches=%llu "
      "entries=%llu claim_ns=%llu submit_ns=%llu callback_wait_ns=%llu "
      "callback_poll_count=%llu finalize_ns=%llu forward_ns=%llu "
      "final_result_capture_ns=%llu result_native_capture_ns=%llu "
      "result_block_alloc_ns=%llu result_block_write_seal_ns=%llu "
      "result_storage_ns=%llu result_document_count=%llu "
      "result_document_bytes=%llu result_row_count=%llu "
      "result_zero_row_count=%llu raw_result_capture_count=%llu "
      "raw_result_capture_fallback_count=%llu final_timestamp_ns=%llu "
      "final_task_publish_ns=%llu final_business_ns=%llu "
      "final_lookup_ns=%llu final_queue_publish_ns=%llu "
      "final_queue_wait_ns=%llu final_confirmation_ns=%llu "
      "final_queue_full_count=%llu "
      "last_forward_completed_mono_ns=%llu\n",
      runtime->slot, runtime->generation,
      (unsigned long long)profile->batch_count,
      (unsigned long long)profile->batch_entry_count,
      (unsigned long long)profile->claim_ns,
      (unsigned long long)profile->submit_ns,
      (unsigned long long)profile->callback_wait_ns,
      (unsigned long long)profile->callback_poll_count,
      (unsigned long long)profile->finalize_ns,
      (unsigned long long)profile->forward_ns,
      (unsigned long long)profile->final_result_capture_ns,
      (unsigned long long)profile->result_native_capture_ns,
      (unsigned long long)profile->result_block_alloc_ns,
      (unsigned long long)profile->result_block_write_seal_ns,
      (unsigned long long)profile->result_storage_ns,
      (unsigned long long)profile->result_document_count,
      (unsigned long long)profile->result_document_bytes,
      (unsigned long long)profile->result_row_count,
      (unsigned long long)profile->result_zero_row_count,
      (unsigned long long)profile->raw_result_capture_count,
      (unsigned long long)profile->raw_result_capture_fallback_count,
      (unsigned long long)profile->final_timestamp_ns,
      (unsigned long long)profile->final_task_publish_ns,
      (unsigned long long)profile->final_business_ns,
      (unsigned long long)profile->final_lookup_ns,
      (unsigned long long)profile->final_queue_publish_ns,
      (unsigned long long)profile->final_queue_wait_ns,
      (unsigned long long)profile->final_confirmation_ns,
      (unsigned long long)profile->final_queue_full_count,
      (unsigned long long)profile->last_forward_completed_mono_ns);
}

static void qstPairCaptureOneCompletedResult(
    SQstPairAsyncBatchEntry *entry) {
  SQstPairQueryCompletion *completion = NULL;

  if (NULL == entry) {
    return;
  }
  completion = &entry->completion;
  if (NULL == completion->result || completion->captured != 0 ||
      completion->code != 0 || NULL == completion->client) {
    return;
  }
  completion->capture_attempted = 1;
  completion->capture_started_ns = qstMonotonicNowNs();
  completion->capture_code = qst_native_capture_result(
      completion->client, completion->result, &completion->captured_result);
  completion->capture_finished_ns = qstMonotonicNowNs();
  if (completion->capture_code == QST_OK) {
    completion->captured = 1;
  }
}

static void *qstPairResultCaptureWorkerRun(void *argument) {
  SQstPairResultCaptureWorker *worker =
      (SQstPairResultCaptureWorker *)argument;

  if (NULL == worker || NULL == worker->entries || worker->next_index == NULL) {
    return NULL;
  }
  for (;;) {
    const uint32_t index = atomic_fetch_add_explicit(
        worker->next_index, 1, memory_order_relaxed);

    if (index >= worker->entry_count) {
      break;
    }
    qstPairCaptureOneCompletedResult(&worker->entries[index]);
  }
  return NULL;
}

static EQstCode qstPairCaptureCompletedResults(SQstPairAsyncBatchEntry *entries,
                                               uint32_t entry_count) {
  SQstPairResultCaptureWorker workers[QST_PAIR_RESULT_CAPTURE_WORKER_COUNT] =
      {{0}};
  _Atomic uint32_t next_index = 0;
  uint32_t worker_count = 0;
  uint32_t started_count = 0;

  if (NULL == entries || entry_count == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  worker_count = entry_count < QST_PAIR_RESULT_CAPTURE_WORKER_COUNT ?
                     entry_count : QST_PAIR_RESULT_CAPTURE_WORKER_COUNT;
  for (uint32_t index = 0; index < worker_count; ++index) {
    workers[index].entries = entries;
    workers[index].entry_count = entry_count;
    workers[index].next_index = &next_index;
    if (pthread_create(&workers[index].thread, NULL, qstPairResultCaptureWorkerRun,
                       &workers[index]) != 0) {
      break;
    }
    ++started_count;
  }
  for (uint32_t index = 0; index < started_count; ++index) {
    if (pthread_join(workers[index].thread, NULL) != 0) {
      return QST_E_RESOURCE;
    }
  }
  /* Preserve serial capture for entries left untouched by a partial start. */
  for (uint32_t index = 0; index < entry_count; ++index) {
    if (entries[index].completion.capture_attempted == 0) {
      qstPairCaptureOneCompletedResult(&entries[index]);
    }
  }
  return QST_OK;
}

/* Executes one bounded batch with one native connection per in-flight SQL.
 * Submission is sequential, but every SQL in the batch is in flight before
 * result processing begins. Ordinary failures and timeouts are left to pair
 * recovery, preserving the existing task retry ownership rules. A detected
 * taosd crash is recovered by the current pair supervisor, which serially
 * replays the batch to attribute the crashing SQL without changing ownership. */
static EQstCode qstPairExecuteAsyncBatch(
    SQstShmMapping *mapping, uint32_t pair_slot, uint32_t pair_generation,
    SQstPairRuntime *runtime, const SQstTaskDispatch *dispatches,
    uint32_t dispatch_count, _Atomic uint64_t *inout_sid,
    int32_t *out_taosd_crashed) {
  SQstPairAsyncBatchEntry entries[QST_PAIR_MAX_INFLIGHT] = {{0}};
  uint32_t submitted_count = 0;
  uint32_t completed_count = 0;
  uint64_t callback_deadline = 0;
  uint64_t submit_started_ns = 0;
  uint64_t callback_wait_started_ns = 0;
  uint64_t finalize_started_ns = 0;
  uint64_t forward_started_ns = 0;
  int32_t taosd_exited = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == mapping->header || NULL == runtime ||
      NULL == dispatches ||
      NULL == inout_sid || NULL == out_taosd_crashed ||
      dispatch_count == 0 || dispatch_count > runtime->async_window ||
      pair_generation == 0 ||
      mapping->header->statement_timeout_seconds == 0 ||
      mapping->header->statement_timeout_seconds > UINT32_MAX / 1000U) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_taosd_crashed = 0;
  if (runtime->async_profile.enabled != 0) {
    submit_started_ns = qstMonotonicNowNs();
    ++runtime->async_profile.batch_count;
    runtime->async_profile.batch_entry_count += dispatch_count;
  }
  for (uint32_t index = 0; index < dispatch_count; ++index) {
    SQstPairAsyncBatchEntry *entry = &entries[index];
    char *sql = NULL;

    if (runtime->clients[index].connection == NULL) {
      return QST_E_RESOURCE;
    }
    entry->dispatch = dispatches[index];
    if (entry->dispatch.gid == 0) {
      return QST_E_INVALID_ARGUMENT;
    }
    entry->final_payload.statement_ts_ns = qstRealtimeNowNs();
    entry->final_payload.taosc_pid = (int32_t)getpid();
    entry->final_payload.taosd_pid = runtime->taosd_pid > 0 ?
        (int32_t)runtime->taosd_pid : 0;
    if (entry->final_payload.statement_ts_ns == 0 ||
        qstPairAllocateSid(inout_sid, &entry->final_payload.sid) != QST_OK ||
        qstPairCopySql(mapping, &entry->dispatch, &sql) != QST_OK) {
      code = QST_E_RESOURCE;
      free(sql);
      break;
    }
    entry->query_deadline = qstMillisecondsToDeadlineNs(
        (uint64_t)mapping->header->statement_timeout_seconds * 1000U);
    atomic_init(&entry->completion.completed, 0);
    entry->completion.event_fd = -1;
    entry->completion.client = &runtime->clients[index];
    if (entry->query_deadline == 0) {
      free(sql);
      code = QST_E_RESOURCE;
      break;
    }
    code = qst_native_execute_async(&runtime->clients[index], sql,
                                    qstPairQueryCallback, &entry->completion);
    free(sql);
    if (code != QST_OK) {
      break;
    }
    entry->submitted = 1;
    ++submitted_count;
  }
  if (code != QST_OK) {
    return code;
  }
  if (runtime->async_profile.enabled != 0) {
    qstPairAsyncProfileAddElapsed(
        &runtime->async_profile, submit_started_ns, qstMonotonicNowNs(),
        &runtime->async_profile.submit_ns);
    callback_wait_started_ns = qstMonotonicNowNs();
  }
  while (completed_count < submitted_count) {
    uint32_t stop_generation = 0;
    uint32_t exit_state = 0;
    const uint64_t now = qstMonotonicNowNs();

    if (now == 0 || qst_process_get_stop_generation_instance(
                        mapping, QST_ROLE_PAIR_SUPERVISOR, pair_slot,
                        pair_generation, &stop_generation, &exit_state) != QST_OK) {
      return QST_E_RESOURCE;
    }
    if (runtime->taosd_pidfd >= 0 &&
        qstWaitForExit(runtime->taosd_pidfd, 0, &taosd_exited) != QST_OK) {
      return QST_E_RESOURCE;
    }
    if (taosd_exited != 0) {
      if (callback_deadline == 0) {
        callback_deadline = qstMillisecondsToDeadlineNs(
            QST_PAIR_CORE_CALLBACK_GRACE_MILLISECONDS);
        if (callback_deadline == 0) {
          return QST_E_RESOURCE;
        }
      }
      for (uint32_t index = 0; index < submitted_count; ++index) {
        if (entries[index].submitted != 0) {
          (void)qst_native_kill_query(&runtime->clients[index]);
        }
      }
    }
    if (exit_state == QST_PROCESS_STOP_REQUESTED && stop_generation != 0) {
      for (uint32_t index = 0; index < submitted_count; ++index) {
        if (entries[index].submitted != 0) {
          (void)qst_native_kill_query(&runtime->clients[index]);
        }
      }
      return QST_E_STOPPING;
    }
    if (exit_state != QST_PROCESS_RUNNING) {
      return QST_E_STALE_OWNER;
    }
    for (uint32_t index = 0; index < submitted_count; ++index) {
      SQstPairAsyncBatchEntry *entry = &entries[index];

      if (entry->submitted != 0 &&
          atomic_load_explicit(&entry->completion.completed,
                               memory_order_acquire) != 0) {
        entry->submitted = 0;
        ++completed_count;
      } else if (entry->submitted != 0 && now >= entry->query_deadline) {
        if (getenv("QST_ASYNC_DIAGNOSTICS") != NULL) {
          const uint64_t timeout_ns =
              (uint64_t)mapping->header->statement_timeout_seconds *
              UINT64_C(1000000000);
          char *diagnostic_sql = NULL;
          const EQstCode sql_code =
              qstPairCopySql(mapping, &entry->dispatch, &diagnostic_sql);
          (void)fprintf(
              stderr,
              "taosQueryTest: async batch entry deadline slot=%u index=%u "
              "gid=%llu completed=%u code=%d native_code=%d captured=%d "
              "result=%p capture=%p age_ns=%llu sql_code=%d sql=%.*s\n",
              pair_slot, index, (unsigned long long)entry->dispatch.gid,
              (unsigned)atomic_load_explicit(&entry->completion.completed,
                                             memory_order_acquire),
              entry->completion.code, entry->completion.native_error_code,
              entry->completion.captured, entry->completion.result,
              (void *)entry->completion.capture,
              (unsigned long long)(timeout_ns != 0 && now >= entry->query_deadline
                                       ? timeout_ns +
                                             (now - entry->query_deadline)
                                       : 0),
              sql_code, diagnostic_sql != NULL ? 8192 : 0,
              diagnostic_sql != NULL ? diagnostic_sql : "");
          free(diagnostic_sql);
        }
        (void)qst_native_kill_query(&runtime->clients[index]);
        return QST_E_DEADLINE;
      }
    }
    if (taosd_exited != 0 && completed_count < submitted_count &&
        now >= callback_deadline) {
      return QST_E_DEADLINE;
    }
    if (completed_count < submitted_count &&
        qstPollQueueEvent(&(struct pollfd){
                              .fd = mapping->queue_eventfd[QST_QUEUE_CONTROL - 1],
                              .events = POLLIN},
                          QST_PAIR_QUERY_POLL_MILLISECONDS) != QST_OK) {
      return QST_E_RESOURCE;
    }
    if (completed_count < submitted_count &&
        runtime->async_profile.enabled != 0) {
      ++runtime->async_profile.callback_poll_count;
    }
  }
  if (runtime->async_profile.enabled != 0) {
    qstPairAsyncProfileAddElapsed(
        &runtime->async_profile, callback_wait_started_ns, qstMonotonicNowNs(),
        &runtime->async_profile.callback_wait_ns);
  }
  code = qstPairCaptureCompletedResults(entries, submitted_count);
  if (code != QST_OK) {
    return code;
  }
  if (runtime->async_profile.enabled != 0) {
    for (uint32_t index = 0; index < submitted_count; ++index) {
      qstPairAsyncProfileAddElapsed(
          &runtime->async_profile, entries[index].completion.capture_started_ns,
          entries[index].completion.capture_finished_ns,
          &runtime->async_profile.result_native_capture_ns);
    }
  }
  if (runtime->taosd_pidfd >= 0 &&
      qstWaitForExit(runtime->taosd_pidfd, QST_PAIR_QUERY_POLL_MILLISECONDS,
                     &taosd_exited) != QST_OK) {
    return QST_E_RESOURCE;
  }
  if (taosd_exited == 0 && qstPairHasCoreArtifact(runtime) != 0) {
    if (qstWaitForExit(runtime->taosd_pidfd,
                       QST_PAIR_CORE_CALLBACK_GRACE_MILLISECONDS,
                       &taosd_exited) != QST_OK ||
        taosd_exited == 0) {
      return QST_E_RESOURCE;
    }
  }
  if (taosd_exited != 0) {
    int32_t signaled = 0;

    code = qstPairReapExitedTaosd(mapping, runtime, &signaled);
    if (code != QST_OK) {
      return code;
    }
    *out_taosd_crashed = signaled;
    return QST_E_RESOURCE;
  }
  if (runtime->async_profile.enabled != 0) {
    finalize_started_ns = qstMonotonicNowNs();
  }
  for (uint32_t index = 0; index < submitted_count; ++index) {
    SQstPairAsyncBatchEntry *entry = &entries[index];
    const int32_t native_code = entry->completion.code;
    uint64_t phase_started_ns = 0;

    entry->final_payload.execution_status = native_code == 0 ?
        QST_EXEC_SUCCESS : QST_EXEC_ERROR;
    entry->final_payload.execution_error_code = native_code;
    if (runtime->async_profile.enabled != 0) {
      phase_started_ns = qstMonotonicNowNs();
    }
    if (entry->completion.result != NULL) {
      if (entry->completion.captured != 0) {
        code = qstPairStoreNormalizedResult(
            mapping, &entry->dispatch, pair_slot, pair_generation,
            &entry->completion.captured_result, &entry->final_payload,
            &runtime->async_profile);
      } else if (entry->completion.capture_attempted != 0 &&
                 entry->completion.capture_code != QST_OK) {
        code = qst_task_set_result_storage(
            mapping, entry->dispatch.gid, entry->dispatch.dispatch_epoch,
            entry->dispatch.transition_seq, pair_slot, pair_generation,
            QST_RESULT_UNAVAILABLE);
      } else if (native_code == 0) {
        code = qstPairCaptureQueryResult(
            mapping, &entry->dispatch, pair_slot, pair_generation,
            &runtime->clients[index], entry->completion.result,
            &entry->final_payload, &runtime->result_capture_cache,
            &runtime->async_profile);
      } else {
        code = qst_task_set_result_storage(
            mapping, entry->dispatch.gid, entry->dispatch.dispatch_epoch,
            entry->dispatch.transition_seq, pair_slot, pair_generation,
            entry->dispatch.focus == QST_FOCUS_CORRECTNESS ?
                QST_RESULT_UNAVAILABLE : QST_RESULT_NOT_REQUESTED);
      }
      runtime->clients[index].free_result(entry->completion.result);
      entry->completion.result = NULL;
    } else {
      code = qst_task_set_result_storage(
          mapping, entry->dispatch.gid, entry->dispatch.dispatch_epoch,
          entry->dispatch.transition_seq, pair_slot, pair_generation,
          entry->dispatch.focus == QST_FOCUS_CORRECTNESS ?
              QST_RESULT_UNAVAILABLE : QST_RESULT_NOT_REQUESTED);
    }
    if (runtime->async_profile.enabled != 0) {
      qstPairAsyncProfileAddElapsed(
          &runtime->async_profile, phase_started_ns, qstMonotonicNowNs(),
          &runtime->async_profile.final_result_capture_ns);
      phase_started_ns = qstMonotonicNowNs();
    }
    if (code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: pair async result capture failed slot=%u "
                    "generation=%u gid=%llu dispatch_epoch=%llu "
                    "transition_seq=%llu code=%d\n",
                    pair_slot, pair_generation,
                    (unsigned long long)entry->dispatch.gid,
                    (unsigned long long)entry->dispatch.dispatch_epoch,
                    (unsigned long long)entry->dispatch.transition_seq, code);
      return code;
    }
    entry->final_payload.ended_ts_ns = qstRealtimeNowNs();
    if (runtime->async_profile.enabled != 0) {
      qstPairAsyncProfileAddElapsed(
          &runtime->async_profile, phase_started_ns, qstMonotonicNowNs(),
          &runtime->async_profile.final_timestamp_ns);
      phase_started_ns = qstMonotonicNowNs();
    }
    if (entry->final_payload.ended_ts_ns == 0) {
      return QST_E_RESOURCE;
    }
    code = qst_task_publish_final(
        mapping, entry->dispatch.gid, entry->dispatch.dispatch_epoch,
        entry->dispatch.transition_seq, QST_ROLE_PAIR_SUPERVISOR, pair_slot,
        pair_generation, &entry->final_payload);
    if (runtime->async_profile.enabled != 0) {
      qstPairAsyncProfileAddElapsed(
          &runtime->async_profile, phase_started_ns, qstMonotonicNowNs(),
          &runtime->async_profile.final_task_publish_ns);
      phase_started_ns = qstMonotonicNowNs();
    }
    if (code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: pair async final publish failed slot=%u "
                    "generation=%u gid=%llu dispatch_epoch=%llu "
                    "transition_seq=%llu code=%d\n",
                    pair_slot, pair_generation,
                    (unsigned long long)entry->dispatch.gid,
                    (unsigned long long)entry->dispatch.dispatch_epoch,
                    (unsigned long long)entry->dispatch.transition_seq, code);
      return code;
    }
    code = qst_process_record_business_instance(
        mapping, QST_ROLE_PAIR_SUPERVISOR, pair_slot, pair_generation,
        entry->dispatch.gid, entry->query_deadline);
    if (runtime->async_profile.enabled != 0) {
      qstPairAsyncProfileAddElapsed(
          &runtime->async_profile, phase_started_ns, qstMonotonicNowNs(),
          &runtime->async_profile.final_business_ns);
    }
    if (code != QST_OK) {
      return code;
    }
  }
  if (runtime->async_profile.enabled != 0) {
    qstPairAsyncProfileAddElapsed(
        &runtime->async_profile, finalize_started_ns, qstMonotonicNowNs(),
        &runtime->async_profile.finalize_ns);
    forward_started_ns = qstMonotonicNowNs();
  }
  for (uint32_t index = 0; index < submitted_count; ++index) {
    const uint64_t deadline = qstMillisecondsToDeadlineNs(
        QST_FLOW_LEASE_MILLISECONDS);

    if (deadline == 0) {
      return QST_E_RESOURCE;
    }
    code = qstPairForwardFinalTask(mapping, pair_slot, pair_generation,
                                   &entries[index].dispatch, deadline,
                                   &runtime->async_profile);
    if (code != QST_OK) {
      return code;
    }
  }
  if (runtime->async_profile.enabled != 0) {
    const uint64_t forward_finished_ns = qstMonotonicNowNs();

    qstPairAsyncProfileAddElapsed(
        &runtime->async_profile, forward_started_ns, forward_finished_ns,
        &runtime->async_profile.forward_ns);
    runtime->async_profile.last_forward_completed_mono_ns =
        forward_finished_ns;
  }
  return QST_OK;
}

/* Replays a crashed async batch on fresh isolated servers one SQL at a time.
 * A replay crash is final evidence for that SQL; remaining statements continue
 * on a newly restarted runtime without changing their task epoch or owner. */
static EQstCode qstPairExecuteBatchWithCoreRecovery(
    SQstShmMapping *mapping, SQstPairRuntime *runtime,
    const SQstTaskDispatch *dispatches, uint32_t dispatch_count,
    _Atomic uint64_t *inout_sid) {
  int32_t taosd_crashed = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == runtime || NULL == dispatches ||
      NULL == inout_sid || dispatch_count == 0 ||
      dispatch_count > runtime->async_window) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qstPairExecuteAsyncBatch(mapping, runtime->slot, runtime->generation,
                                  runtime, dispatches, dispatch_count, inout_sid,
                                  &taosd_crashed);
  if (taosd_crashed == 0) {
    return code;
  }
  code = qstPairRestartAfterTaosdCrash(mapping, runtime);
  if (code != QST_OK) {
    return code;
  }
  for (uint32_t index = 0; index < dispatch_count; ++index) {
    taosd_crashed = 0;
    code = qstPairExecuteAsyncBatch(mapping, runtime->slot, runtime->generation,
                                    runtime, &dispatches[index], 1, inout_sid,
                                    &taosd_crashed);
    if (taosd_crashed == 0) {
      if (code != QST_OK) {
        return code;
      }
      continue;
    }
    code = qstPairPublishCoreFinal(mapping, runtime, &dispatches[index]);
    if (code != QST_OK) {
      return code;
    }
    code = qstPairRestartAfterTaosdCrash(mapping, runtime);
    if (code != QST_OK) {
      return code;
    }
  }
  return QST_OK;
}

static void *qstPairWorkerRun(void *argument) {
  SQstPairWorker *worker = (SQstPairWorker *)argument;
  SQstNativeClient *client = NULL;
  int32_t local_client_owned = 0;

  if (worker == NULL) {
    return NULL;
  }
  client = worker->client;
  if (client == NULL || client->connection == NULL) {
    worker->code = qst_native_open_with_endpoint(
        &worker->local_client, "127.0.0.1", worker->port, NULL);
    if (worker->code == QST_OK) {
      client = &worker->local_client;
      local_client_owned = 1;
    }
  }
  if (worker->code == QST_OK) {
    worker->code = qstPairExecuteAssignedTask(
        worker->mapping, worker->pair_slot, worker->pair_generation, client,
        worker->taosd_pid > 0 ? worker->taosd_pid : getpid(), &worker->dispatch,
        worker->sid);
  }
  if (local_client_owned != 0 && qst_native_close(&worker->local_client) !=
                                      QST_OK && worker->code == QST_OK) {
    worker->code = QST_E_RESOURCE;
  }
  if (worker->code != QST_OK && worker->code != QST_E_STOPPING) {
    (void)fprintf(stderr,
                  "taosQueryTest: pair worker failed slot=%u gid=%llu code=%d\n",
                  worker->pair_slot, (unsigned long long)worker->dispatch.gid,
                  worker->code);
  }
  /* A deadline is recovered by the exec manager only after this pair exits;
   * publishing FINAL here would consume the retry transition prematurely. */
  if (worker->code != QST_OK && worker->code != QST_E_STOPPING &&
      worker->code != QST_E_DEADLINE) {
    SQstExecutePayload payload = {0};

    payload.statement_ts_ns = qstRealtimeNowNs();
    payload.ended_ts_ns = payload.statement_ts_ns;
    payload.execution_status = worker->code == QST_E_STOPPING
                                   ? QST_EXEC_CANCELLED
                                   : QST_EXEC_TIMEOUT;
    payload.execution_error_code = worker->code;
    payload.result_row_count = -1;
    if (payload.statement_ts_ns == 0 ||
        qst_task_publish_final(
            worker->mapping, worker->dispatch.gid,
            worker->dispatch.dispatch_epoch, worker->dispatch.transition_seq,
            QST_ROLE_PAIR_SUPERVISOR, worker->pair_slot,
            worker->pair_generation, &payload) != QST_OK) {
      worker->code = QST_E_RESOURCE;
    }
  }
  atomic_store_explicit(&worker->completed, 1, memory_order_release);
  return NULL;
}

static EQstCode qstPairJoinCompletedWorkers(SQstPairWorker *workers,
                                            uint32_t worker_count,
                                            int32_t stopping) {
  if (workers == NULL || worker_count != QST_PAIR_MAX_INFLIGHT) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (uint32_t index = 0; index < worker_count; ++index) {
    if (workers[index].active == 0 ||
        atomic_load_explicit(&workers[index].completed, memory_order_acquire) ==
            0) {
      continue;
    }
    if (pthread_join(workers[index].thread, NULL) != 0) {
      return QST_E_RESOURCE;
    }
    workers[index].active = 0;
    if (workers[index].code != QST_OK &&
        !(stopping != 0 && workers[index].code == QST_E_STOPPING)) {
      return workers[index].code;
    }
  }
  return QST_OK;
}

static EQstCode qstPairStopWorkers(SQstPairWorker *workers,
                                   uint32_t worker_count) {
  if (workers == NULL || worker_count != QST_PAIR_MAX_INFLIGHT) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (uint32_t index = 0; index < worker_count; ++index) {
    SQstNativeClient *client = workers[index].client;

    if (client == NULL && workers[index].local_client.connection != NULL) {
      client = &workers[index].local_client;
    }
    if (workers[index].active != 0 && client != NULL &&
        qst_native_kill_query(client) != QST_OK) {
      return QST_E_RESOURCE;
    }
  }
  for (uint32_t index = 0; index < worker_count; ++index) {
    if (workers[index].active != 0 && pthread_join(workers[index].thread, NULL) !=
                                        0) {
      return QST_E_RESOURCE;
    }
    workers[index].active = 0;
  }
  return QST_OK;
}

EQstCode qst_pair_supervisor_run(SQstShmMapping *mapping,
                                 uint32_t generation, uint32_t pair_slot) {
  struct pollfd event = {0};
  SQstPairRuntime runtime = {.taosd_pid = -1, .taosd_pidfd = -1};
  SQstPairWorker workers[QST_PAIR_MAX_INFLIGHT] = {{0}};
  _Atomic uint64_t sid = 0;
  const char *step = "initialization";
  EQstCode code = QST_OK;

  if (NULL == mapping || generation == 0 || pair_slot == UINT32_MAX) {
    return QST_E_INVALID_ARGUMENT;
  }
  runtime.async_profile.enabled = getenv("QST_PROFILE_PHASES") != NULL;
  step = "redirect stderr";
  code = qstRedirectProcessStderr(QST_ROLE_PAIR_SUPERVISOR, generation);
  if (code != QST_OK) {
    return code;
  }
  step = "write process start";
  code = qstWriteProcessStartLog(
      QST_ROLE_PAIR_SUPERVISOR, generation, QST_ROLE_EXEC_MANAGER,
      atomic_load_explicit(&mapping->header->main_generation,
                           memory_order_acquire));
  if (code != QST_OK) {
    return code;
  }
  if (gQstProcessLogDirectory[0] != '\0') {
    step = "start runtime";
    code = qstPairStartRuntime(mapping, pair_slot, generation, &runtime);
    if (code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: pair runtime start failed slot=%u "
                    "generation=%u code=%d\n",
                    pair_slot, generation, code);
      goto _return;
    }
  }
  step = "confirm startup flow";
  code = qst_process_confirm_flow_instance(
      mapping, QST_ROLE_PAIR_SUPERVISOR, pair_slot, generation,
      QST_FLOW_STAGE_STARTUP, QST_FLOW_STATE_CONTROL_QUEUE, 0, 0, 0);
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: pair startup flow failed slot=%u "
                  "generation=%u code=%d\n",
                  pair_slot, generation, code);
    goto _return;
  }
  qstTestMaybeKillRole(QST_ROLE_PAIR_SUPERVISOR, generation, pair_slot);
  event.fd = mapping->queue_eventfd[QST_QUEUE_CONTROL - 1];
  event.events = POLLIN;
  for (;;) {
    uint32_t stop_generation = 0;
    uint32_t exit_state = 0;
    uint32_t active_count = 0;
    uint32_t started_count = 0;

    step = "join completed workers";
    code = qstPairJoinCompletedWorkers(workers, QST_PAIR_MAX_INFLIGHT, 0);
    if (code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: pair worker join failed slot=%u "
                    "generation=%u code=%d\n",
                    pair_slot, generation, code);
      goto _return;
    }
    step = "read stop state";
    code = qst_process_get_stop_generation_instance(
        mapping, QST_ROLE_PAIR_SUPERVISOR, pair_slot, generation,
        &stop_generation, &exit_state);
    if (code != QST_OK) {
      goto _return;
    }
    if (exit_state == QST_PROCESS_STOP_REQUESTED && stop_generation != 0) {
      qstTestMaybeKillRoleOnStop(QST_ROLE_PAIR_SUPERVISOR, generation,
                                 pair_slot);
      step = "stop workers for requested stop";
      code = qstPairStopWorkers(workers, QST_PAIR_MAX_INFLIGHT);
      if (code == QST_OK) {
        step = "acknowledge requested stop";
        code = qst_process_ack_stop_instance(
            mapping, QST_ROLE_PAIR_SUPERVISOR, pair_slot, generation,
            stop_generation);
      }
      goto _return;
    }
    if (exit_state != QST_PROCESS_RUNNING) {
      code = QST_OK;
      goto _return;
    }
    if (gQstStopSignal != 0) {
      /* SIGTERM may arrive between the state read and this branch. Re-read the
       * shared stop state so the child never exits from STOP_REQUESTED. */
      step = "reread stop state after signal";
      code = qst_process_get_stop_generation_instance(
          mapping, QST_ROLE_PAIR_SUPERVISOR, pair_slot, generation,
          &stop_generation, &exit_state);
      if (code != QST_OK) {
        goto _return;
      }
      if (exit_state == QST_PROCESS_STOP_REQUESTED && stop_generation != 0) {
        qstTestMaybeKillRoleOnStop(QST_ROLE_PAIR_SUPERVISOR, generation,
                                   pair_slot);
        step = "stop workers after signal";
        code = qstPairStopWorkers(workers, QST_PAIR_MAX_INFLIGHT);
        if (code == QST_OK) {
          step = "acknowledge signal stop";
          code = qst_process_ack_stop_instance(
              mapping, QST_ROLE_PAIR_SUPERVISOR, pair_slot, generation,
              stop_generation);
        }
        goto _return;
      }
      code = exit_state == QST_PROCESS_RUNNING ? QST_OK : QST_E_STALE_OWNER;
      goto _return;
    }
    if (gQstProcessLogDirectory[0] == '\0') {
      SQstTaskDispatch dispatch = {0};
      const uint64_t deadline = qstMillisecondsToDeadlineNs(
          QST_FLOW_LEASE_MILLISECONDS);

      if (deadline == 0) {
        code = QST_E_RESOURCE;
        goto _return;
      }
      /* The exec manager only supervises pairs. Direct/fake-runtime pairs
       * must claim generated work themselves, just as runtime-backed pairs
       * do below. */
      step = "claim generated task";
      code = qstPairClaimGeneratedTask(mapping, pair_slot, generation,
                                       deadline, &dispatch);
      if (code == QST_OK) {
        step = "execute assigned task";
        code = qstPairExecuteAssignedTask(mapping, pair_slot, generation, NULL,
                                          runtime.taosd_pid, &dispatch, &sid);
        if (code == QST_OK) {
          continue;
        }
        if (code == QST_E_STOPPING || code == QST_E_STALE_OWNER) {
          if (code == QST_E_STALE_OWNER) {
            continue;
          }
          code = QST_OK;
          goto _return;
        }
        goto _return;
      }
      if (code == QST_E_STALE_OWNER) {
        continue;
      }
      if (code != QST_E_QUEUE_EMPTY) {
        goto _return;
      }
      step = "confirm control queue idle";
      code = qst_process_confirm_flow_instance(
          mapping, QST_ROLE_PAIR_SUPERVISOR, pair_slot, generation,
          QST_FLOW_STAGE_IDLE, QST_FLOW_STATE_CONTROL_QUEUE, 0, 0, 0);
      if (code != QST_OK && code != QST_E_STALE_OWNER) {
        goto _return;
      }
      if (qstPollQueueEvent(&event, QST_PAIR_ASSIGN_POLL_MILLISECONDS) !=
          QST_OK) {
        code = QST_E_RESOURCE;
        goto _return;
      }
      continue;
    }
    {
      SQstTaskDispatch dispatches[QST_PAIR_MAX_INFLIGHT] = {{0}};
      const uint64_t deadline = qstMillisecondsToDeadlineNs(
          QST_FLOW_LEASE_MILLISECONDS);
      const uint64_t batch_fill_deadline = qstMillisecondsToDeadlineNs(
          QST_PAIR_BATCH_FILL_MILLISECONDS);

      if (deadline == 0 || batch_fill_deadline == 0) {
        code = QST_E_RESOURCE;
        goto _return;
      }
      for (uint32_t index = 0; index < runtime.async_window; ++index) {
        step = "claim async batch task";
        code = qstPairClaimGeneratedTask(mapping, pair_slot, generation,
                                         deadline, &dispatches[index]);
        if (code == QST_OK) {
          ++started_count;
          continue;
        }
        if (code == QST_E_QUEUE_EMPTY || code == QST_E_STALE_OWNER) {
          const EQstCode claim_code = code;
          const uint64_t now = qstMonotonicNowNs();

          code = QST_OK;
          if (now == 0) {
            code = QST_E_RESOURCE;
            goto _return;
          }
          if (started_count == 0 || claim_code == QST_E_STALE_OWNER ||
              now >= batch_fill_deadline) {
            break;
          }
          if (qstPollQueueEvent(&(struct pollfd){
                                .fd = mapping->queue_eventfd[QST_QUEUE_GENERATE - 1],
                                .events = POLLIN},
                                QST_PAIR_ASSIGN_POLL_MILLISECONDS) != QST_OK) {
            code = QST_E_RESOURCE;
            goto _return;
          }
          --index;
          continue;
        }
        goto _return;
      }
      if (started_count != 0) {
        step = "execute async batch";
        code = qstPairExecuteBatchWithCoreRecovery(
            mapping, &runtime, dispatches, started_count, &sid);
        if (code == QST_OK) {
          if (started_count > runtime.max_async_batch_size) {
            runtime.max_async_batch_size = started_count;
          }
          qstTestMaybeKillRoleFromEnv("QST_TEST_KILL_AFTER_FINAL_ROLE",
                                      QST_ROLE_PAIR_SUPERVISOR, generation,
                                      pair_slot);
          continue;
        }
        (void)fprintf(stderr,
                      "taosQueryTest: pair async batch failed slot=%u "
                      "generation=%u batch_count=%u code=%d\n",
                      pair_slot, generation, started_count, code);
        if (code == QST_E_STOPPING || code == QST_E_STALE_OWNER) {
          code = code == QST_E_STALE_OWNER ? QST_OK : QST_E_STOPPING;
        }
        goto _return;
      }
    }
    step = "confirm input queue idle";
    code = qst_process_confirm_flow_instance(
        mapping, QST_ROLE_PAIR_SUPERVISOR, pair_slot, generation,
        QST_FLOW_STAGE_IDLE, QST_FLOW_STATE_INPUT_QUEUE, 0, 0, 0);
    if (code != QST_OK && code != QST_E_STALE_OWNER) {
      goto _return;
    }
    event.fd = mapping->queue_eventfd[QST_QUEUE_GENERATE - 1];
    if (qstPollQueueEvent(&event, QST_PAIR_ASSIGN_POLL_MILLISECONDS) != QST_OK) {
      code = QST_E_RESOURCE;
      goto _return;
    }
  }

_return:
  {
    const EQstCode worker_stop_code =
        qstPairStopWorkers(workers, QST_PAIR_MAX_INFLIGHT);

    if (code == QST_OK && worker_stop_code != QST_OK) {
      code = worker_stop_code;
    }
  }
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: pair supervisor failed slot=%u generation=%u "
                  "step=%s code=%d\n",
                  pair_slot, generation, step, code);
  }
  qstPairWriteAsyncProfile(&runtime);
  {
    const EQstCode stop_code = qstPairStopRuntime(mapping, &runtime);
    if (code == QST_OK && stop_code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: pair runtime stop failed slot=%u "
                    "generation=%u code=%d\n",
                    pair_slot, generation, stop_code);
      code = stop_code;
    }
  }
  {
    const EQstCode fact_code = qstPairFinalizeRuntime(&runtime);
    if (code == QST_OK && fact_code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: pair fact finalize failed slot=%u "
                    "generation=%u code=%d\n",
                    pair_slot, generation, fact_code);
      code = fact_code;
    }
  }
  return code;
}

typedef struct {
  uint32_t slot;
  uint32_t generation;
  pid_t pid;
  int32_t pidfd;
} SQstJudgeWorkerChild;

typedef struct {
  uint32_t verdict;
  const char *validator;
  const char *diff_summary;
} SQstJudgeVerdict;

static const char *qstJudgeTargetName(uint32_t target) {
  switch (target) {
    case QST_TARGET_TD:
      return "td";
    case QST_TARGET_MYSQL:
      return "mysql";
    case QST_TARGET_PG:
      return "pg";
    case QST_TARGET_INFLUX:
      return "influx";
    default:
      return NULL;
  }
}

static void qstJudgeDigestHex(SQstDigest256 digest, char *out_hex) {
  static const char kHex[] = "0123456789abcdef";

  for (uint32_t index = 0; index < sizeof(digest.bytes); ++index) {
    out_hex[index * 2U] = kHex[digest.bytes[index] >> 4U];
    out_hex[index * 2U + 1U] = kHex[digest.bytes[index] & 0x0fU];
  }
  out_hex[sizeof(digest.bytes) * 2U] = '\0';
}

/* Publishes one persistence confirmation while honoring persist_queue backpressure. */
static EQstCode qstJudgePublishPersist(
    SQstShmMapping *mapping, const SQstMessage *message,
    uint64_t deadline_mono_ns) {
  struct pollfd event = {0};
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == message || deadline_mono_ns == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  event.fd = mapping->queue_eventfd[QST_QUEUE_PERSIST - 1];
  event.events = POLLIN;
  for (;;) {
    code = qst_queue_publish(mapping, QST_QUEUE_PERSIST, message,
                             deadline_mono_ns);
    if (code != QST_E_QUEUE_FULL) {
      return code;
    }
    if (qstPollQueueEvent(&event, 20) != QST_OK) {
      return QST_E_RESOURCE;
    }
  }
}

/* Seals one worker-produced UTF-8 record under the task's logical owner. */
static EQstCode qstJudgeSealText(SQstShmMapping *mapping, uint64_t gid,
                                 uint32_t generation, const char *text,
                                 SQstRef *out_ref) {
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == text || NULL == out_ref || gid == 0 ||
      generation == 0 || text[0] == '\0') {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qst_block_alloc(mapping, gid, generation, strlen(text), out_ref);
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: judge record block allocation failed "
                  "code=%d gid=%llu generation=%u bytes=%zu\n",
                  code, (unsigned long long)gid, generation, strlen(text));
    return code;
  }
  code = qst_block_write_seal(mapping, out_ref, text, strlen(text));
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: judge record block seal failed code=%d "
                  "gid=%llu generation=%u offset=%llu length=%u bytes=%zu\n",
                  code, (unsigned long long)gid, generation,
                  (unsigned long long)out_ref->offset, out_ref->length,
                  strlen(text));
    (void)qst_block_abort(mapping, *out_ref, gid);
  }
  return code;
}

/* Writes one durable object, then exposes its immutable confirmation to main. */
static EQstCode qstJudgePersistRecord(
    SQstShmMapping *mapping, SQstResultStore *store,
    const SQstPendingStore *pending_store, SQstPendingBatch *pending_batch,
    uint32_t worker_slot,
    uint32_t worker_generation, const SQstClaim *execute_claim,
    uint32_t persist_kind, uint32_t persist_mask,
    const SQstJudgeVerdict *verdict, uint64_t record_seq,
    int32_t defer_native_write,
    SQstRef *out_stable_key_ref, SQstRef *out_payload_ref,
    SQstMessage *out_message, int32_t *out_uses_pending_batch) {
  SQstPersistPayload persist = {0};
  SQstMessage message = {0};
  SQstDigest256 digest = {0};
  SQstTaskPersistSnapshot snapshot = {0};
  const SQstExecutePayload *execute = NULL;
  const char *target_name = NULL;
  SQstGenerationState *generation_state = NULL;
  char stable_key[256] = {0};
  char record[512] = {0};
  char ds_fingerprint[65] = {0};
  char sql_sha256[65] = {0};
  int32_t written = 0;
  int32_t taos_code = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || (NULL == store && NULL == pending_store) ||
      NULL == execute_claim ||
      NULL == out_stable_key_ref || NULL == out_payload_ref ||
      NULL == out_message || NULL == out_uses_pending_batch ||
      (persist_kind != QST_PERSIST_STATEMENT &&
       persist_kind != QST_PERSIST_INITIAL_RESULT &&
       persist_kind != QST_PERSIST_HISTORICAL_VERDICT) ||
      (persist_mask != QST_PERSIST_MASK_STATEMENT &&
       persist_mask != QST_PERSIST_MASK_INITIAL_RESULT &&
       persist_mask != QST_PERSIST_MASK_HISTORICAL_VERDICT) || record_seq == 0 ||
      (defer_native_write != 0 && defer_native_write != 1) ||
      ((persist_kind == QST_PERSIST_INITIAL_RESULT ||
        persist_kind == QST_PERSIST_HISTORICAL_VERDICT) && NULL == verdict)) {
    return QST_E_INVALID_ARGUMENT;
  }
  memset(out_stable_key_ref, 0, sizeof(*out_stable_key_ref));
  memset(out_payload_ref, 0, sizeof(*out_payload_ref));
  *out_uses_pending_batch = 0;
  target_name = qstJudgeTargetName(execute_claim->message.target);
  execute = &execute_claim->message.body.execute;
  if (NULL == target_name || execute_claim->message.gid == 0 ||
      execute_claim->message.dispatch_epoch == 0 ||
      execute_claim->message.transition_seq == 0) {
    return QST_E_SHM_CORRUPT;
  }
  persist.persist_kind = persist_kind;
  persist.durable_source = QST_DURABLE_RESULT_STORE;
  persist.target = execute_claim->message.target;
  /* The zero slot retains the pre-sharding table name. Nonzero slots are
   * durable routing data, so both native and pending DML target the same shard. */
  persist.reserved0 = worker_slot;
  persist.gid = execute_claim->message.gid;
  persist.dispatch_epoch = execute_claim->message.dispatch_epoch;
  persist.transition_seq = execute_claim->message.transition_seq;
  persist.record_seq = record_seq;
  /* Task statement DML is derived exclusively from the immutable task
   * snapshot. Unlike result and historical records, it never consumes the
   * generic stable-key/payload transport objects, so avoid allocating two
   * shared blocks only to release them after the native batch succeeds. */
  if (persist_kind == QST_PERSIST_STATEMENT) {
    goto persist_ready;
  }
  if (qst_generator_state_get(mapping, &generation_state) != QST_OK) {
    return QST_E_SHM_CORRUPT;
  }
  qstJudgeDigestHex(generation_state->dataset_digest, ds_fingerprint);
  if (persist_kind == QST_PERSIST_HISTORICAL_VERDICT) {
    code = qst_task_get_persist_snapshot(
        mapping, execute_claim->message.gid,
        execute_claim->message.dispatch_epoch,
        execute_claim->message.transition_seq, &snapshot);
    if (code != QST_OK) {
      return code;
    }
    qstJudgeDigestHex(snapshot.sql_sha256, sql_sha256);
  }
  if (persist_kind == QST_PERSIST_STATEMENT) {
    written = snprintf(stable_key, sizeof(stable_key),
                       "statements|%llu|%s|%llu|%lld",
                       (unsigned long long)mapping->header->run_id, target_name,
                       (unsigned long long)execute_claim->message.gid,
                       (long long)execute->statement_ts_ns);
    if (written >= 0 && (uint64_t)written < sizeof(stable_key)) {
      written = snprintf(record, sizeof(record),
                         "{\"kind\":\"statement\",\"gid\":%llu,"
                         "\"target\":\"%s\",\"statement_ts_ns\":%lld}",
                         (unsigned long long)execute_claim->message.gid,
                         target_name, (long long)execute->statement_ts_ns);
    }
  } else if (persist_kind == QST_PERSIST_INITIAL_RESULT) {
    written = snprintf(stable_key, sizeof(stable_key),
                       "results|%llu|%s|%llu|%lld",
                       (unsigned long long)mapping->header->run_id, target_name,
                       (unsigned long long)execute_claim->message.gid,
                       (long long)execute->ended_ts_ns);
    if (written >= 0 && (uint64_t)written < sizeof(stable_key)) {
      written = snprintf(record, sizeof(record),
                         "{\"kind\":\"result\",\"verdict\":%u,"
                         "\"validator\":\"%s\",\"diff_summary\":\"%s\"}",
                         verdict->verdict, verdict->validator,
                         verdict->diff_summary);
    }
  } else {
    written = snprintf(stable_key, sizeof(stable_key),
                       "historical_verdicts|%s|%s|%s", ds_fingerprint,
                       target_name, sql_sha256);
    if (written >= 0 && (uint64_t)written < sizeof(stable_key)) {
      written = snprintf(record, sizeof(record),
                         "{\"kind\":\"historical_verdict\",\"verdict\":%u,"
                         "\"validator\":\"%s\"}", verdict->verdict,
                         verdict->validator);
    }
  }
  if (written < 0 || (uint64_t)written >= sizeof(record)) {
    return QST_E_RESOURCE;
  }
  code = qstJudgeSealText(mapping, execute_claim->message.gid,
                          worker_generation, stable_key, out_stable_key_ref);
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: judge stable-key block failed code=%d "
                  "gid=%llu worker_slot=%u worker_generation=%u "
                  "persist_kind=%u key_bytes=%zu\n",
                  code, (unsigned long long)execute_claim->message.gid,
                  worker_slot, worker_generation, persist_kind,
                  strlen(stable_key));
  }
  if (code == QST_OK) {
    code = qstJudgeSealText(mapping, execute_claim->message.gid,
                            worker_generation, record, out_payload_ref);
    if (code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: judge payload block failed code=%d "
                    "gid=%llu worker_slot=%u worker_generation=%u "
                    "persist_kind=%u payload_bytes=%zu\n",
                    code, (unsigned long long)execute_claim->message.gid,
                    worker_slot, worker_generation, persist_kind,
                    strlen(record));
    }
  }
  if (code != QST_OK) {
    if (!qstRefIsEmpty(*out_stable_key_ref)) {
      (void)qst_block_release(mapping, *out_stable_key_ref,
                              execute_claim->message.gid);
    }
    return code;
  }
  code = qst_block_sha256(mapping, *out_payload_ref, execute_claim->message.gid,
                          &digest);
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: judge payload sha256 failed code=%d "
                  "gid=%llu worker_slot=%u worker_generation=%u "
                  "persist_kind=%u offset=%llu length=%u\n",
                  code, (unsigned long long)execute_claim->message.gid,
                  worker_slot, worker_generation, persist_kind,
                  (unsigned long long)out_payload_ref->offset,
                  out_payload_ref->length);
    (void)qst_block_release(mapping, *out_payload_ref,
                            execute_claim->message.gid);
    (void)qst_block_release(mapping, *out_stable_key_ref,
                            execute_claim->message.gid);
    return code;
  }
  persist.payload_sha256 = digest;
  persist.stable_key_ref = *out_stable_key_ref;
  persist.payload_ref = *out_payload_ref;
persist_ready:
  if (NULL != store && NULL != store->native_taos && defer_native_write == 0) {
    code = qst_result_store_write(mapping, store, &persist, &taos_code);
  } else if (NULL != store && NULL != store->native_taos) {
    /* The judge holds the execute claim until its owner-only batch reaches
     * result-store or the pending stream. */
    code = QST_OK;
  } else {
    code = QST_E_RESULT_STORE;
  }
  if (code != QST_OK && NULL != pending_store && NULL != pending_batch) {
    SQstPendingRecord pending_record = {0};
    char *pending_dml = NULL;

    code = qst_result_store_build_task_dml(mapping, store, &persist, &pending_dml);
    if (code == QST_OK) {
      pending_record.persist_kind = persist_kind;
      pending_record.target = persist.target;
      pending_record.run_id = mapping->header->run_id;
      pending_record.gid = persist.gid;
      pending_record.record_seq = persist.record_seq;
      pending_record.ds_fingerprint = ds_fingerprint;
      pending_record.stable_key = stable_key;
      pending_record.payload = pending_dml;
      pending_record.payload_bytes = strlen(pending_dml);
      code = qst_pending_batch_append(pending_batch, &pending_record);
    }
    free(pending_dml);
    if (code == QST_OK) {
      persist.durable_source = QST_DURABLE_PENDING_FILE;
      /* The append stream coalesces JSONL bytes before one write() call.
       * Confirm the claim only after the buffered segment reaches the file. */
      *out_uses_pending_batch = 1;
    }
  }
  if (code != QST_OK) {
    (void)qst_block_release(mapping, *out_payload_ref,
                            execute_claim->message.gid);
    (void)qst_block_release(mapping, *out_stable_key_ref,
                            execute_claim->message.gid);
    return code;
  }
  message.message_type = QST_MSG_PERSIST_CONFIRM;
  message.producer_role = QST_ROLE_JUDGE_WORKER;
  message.producer_slot_id = worker_slot;
  message.producer_generation = worker_generation;
  message.target = execute_claim->message.target;
  message.run_id = mapping->header->run_id;
  message.gid = execute_claim->message.gid;
  message.dispatch_epoch = execute_claim->message.dispatch_epoch;
  message.transition_seq = execute_claim->message.transition_seq;
  message.body.persist = persist;
  *out_message = message;
  return QST_OK;
}

static EQstCode qstJudgeCopyResult(const SQstShmMapping *mapping, SQstRef ref,
                                   uint64_t gid, SQstQueryResult *out_result) {
  uint8_t *data = NULL;

  if (NULL == mapping || NULL == out_result || qstRefIsEmpty(ref) ||
      ref.length == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  data = (uint8_t *)calloc(ref.length, 1);
  if (NULL == data) {
    return QST_E_RESOURCE;
  }
  const EQstCode copy_code = qst_block_copy(mapping, ref, gid, 0, data,
                                             ref.length);

  if (copy_code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: judge result block copy failed code=%d "
                  "gid=%llu offset=%llu length=%u crc32c=%u\n",
                  copy_code, (unsigned long long)gid,
                  (unsigned long long)ref.offset, ref.length, ref.crc32c);
    free(data);
    return QST_E_BLOCK_INVALID;
  }
  out_result->data = data;
  out_result->bytes = ref.length;
  return QST_OK;
}

static EQstCode qstJudgeSelectVerdict(
    SQstShmMapping *mapping, SQstResultStore *store, const SQstClaim *claim,
    SQstJudgeVerdict *out_verdict) {
  SQstGenerationState *state = NULL;
  SQstTaskPersistSnapshot snapshot = {0};
  SQstQueryResult actual = {0};
  SQstQueryResult historical = {0};
  SQstResultDiff diff = {0};
  char fingerprint[65] = {0};
  char validator[32] = {0};
  int32_t historical_empty = 0;
  int32_t found = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == store || NULL == claim ||
      NULL == out_verdict) {
    return QST_E_INVALID_ARGUMENT;
  }
  out_verdict->verdict = 2;
  out_verdict->validator = "manual";
  out_verdict->diff_summary = "no deterministic validator is available";
  if (claim->message.body.execute.execution_status != QST_EXEC_SUCCESS ||
      qstRefIsEmpty(claim->message.body.execute.result_ref) ||
      claim->message.body.execute.result_ref.length == 0 ||
      NULL == store->native_taos || qst_generator_state_get(mapping, &state) != QST_OK ||
      qst_task_get_persist_snapshot(mapping, claim->message.gid,
                                    claim->message.dispatch_epoch,
                                    claim->message.transition_seq,
                                    &snapshot) != QST_OK) {
    return QST_OK;
  }
  qstJudgeDigestHex(state->dataset_digest, fingerprint);
  code = qst_result_store_historical_is_empty(
      store, fingerprint, claim->message.target, &historical_empty);
  if (code != QST_OK || historical_empty != 0) {
    return QST_OK;
  }
  code = qst_result_store_lookup_historical(
      store, fingerprint, claim->message.target,
      snapshot.sql_sha256,
      &historical, validator, sizeof(validator), &found);
  if (code != QST_OK || found == 0) {
    qst_result_reset(&historical);
    return QST_OK;
  }
  code = qstJudgeCopyResult(mapping, claim->message.body.execute.result_ref,
                             claim->message.gid, &actual);
  if (code == QST_OK) {
    code = qst_result_compare(&actual, &historical, 1, &diff);
  }
  qst_result_reset(&actual);
  qst_result_reset(&historical);
  if (code != QST_OK) {
    return code;
  }
  out_verdict->verdict = diff.kind == QST_RESULT_DIFF_NONE ? 0 : 1;
  out_verdict->validator = "historical";
  out_verdict->diff_summary = diff.kind == QST_RESULT_DIFF_NONE ?
      "historical result matched" : diff.summary;
  return QST_OK;
}

enum {
  QST_JUDGE_MAX_PERSIST_MESSAGES = 3U,
  /* Result-store DML is atomic for one same-kind/target INSERT. One bounded
   * claim group amortizes native round trips while the
   * sole judge worker still owns every member until durability succeeds. */
  QST_JUDGE_NATIVE_BATCH_CLAIMS = 128U,
  QST_JUDGE_NATIVE_BATCH_MESSAGES =
      QST_JUDGE_NATIVE_BATCH_CLAIMS * QST_JUDGE_MAX_PERSIST_MESSAGES,
  /* Small queues cannot hold a large claimed group plus a long producer
   * backlog. A four-group queue can safely wait long enough to fill one. */
  QST_JUDGE_NATIVE_BATCH_SHORT_FILL_MILLISECONDS = 20U,
  QST_JUDGE_NATIVE_BATCH_LONG_FILL_MILLISECONDS = 250U,
  QST_JUDGE_NATIVE_BATCH_LONG_FILL_CAPACITY_MULTIPLIER = 4U,
};

typedef struct {
  SQstClaim claim;
  SQstMessage messages[QST_JUDGE_MAX_PERSIST_MESSAGES];
  uint32_t message_count;
  uint64_t first_record_seq;
  uint64_t deadline;
} SQstJudgePendingClaim;

/* This workspace is deliberately heap-backed: a full claim group can contain
 * three persist records per claim, which is too large for a worker stack. */
typedef struct {
  uint8_t *written;
  SQstPersistPayload *payloads;
  SQstMessage **messages;
  SQstPersistPayload *batch;
  uint32_t *members;
} SQstJudgeNativeBatchWorkspace;

static void qstJudgeProfileAddElapsed(SQstJudgeProfile *profile,
                                      uint64_t started_ns, uint64_t finished_ns,
                                      uint64_t *total_ns);

static uint64_t qstJudgeNativeBatchFillDeadline(
    const SQstShmMapping *mapping) {
  uint64_t milliseconds = QST_JUDGE_NATIVE_BATCH_SHORT_FILL_MILLISECONDS;

  if (NULL != mapping && NULL != mapping->header &&
      mapping->header->queue_region[QST_QUEUE_EXECUTE - 1U].element_count >=
          QST_JUDGE_NATIVE_BATCH_CLAIMS *
              QST_JUDGE_NATIVE_BATCH_LONG_FILL_CAPACITY_MULTIPLIER) {
    milliseconds = QST_JUDGE_NATIVE_BATCH_LONG_FILL_MILLISECONDS;
  }
  return qstMillisecondsToDeadlineNs(milliseconds);
}

static void qstJudgeNativeBatchWorkspaceDestroy(
    SQstJudgeNativeBatchWorkspace *workspace) {
  if (NULL == workspace) {
    return;
  }
  free(workspace->members);
  free(workspace->batch);
  free(workspace->messages);
  free(workspace->payloads);
  free(workspace->written);
  *workspace = (SQstJudgeNativeBatchWorkspace){0};
}

static EQstCode qstJudgeNativeBatchWorkspaceInit(
    SQstJudgeNativeBatchWorkspace *workspace, uint32_t message_capacity) {
  if (NULL == workspace || message_capacity == 0 ||
      message_capacity > SIZE_MAX / sizeof(*workspace->written) ||
      message_capacity > SIZE_MAX / sizeof(*workspace->payloads) ||
      message_capacity > SIZE_MAX / sizeof(*workspace->messages) ||
      message_capacity > SIZE_MAX / sizeof(*workspace->batch) ||
      message_capacity > SIZE_MAX / sizeof(*workspace->members)) {
    return QST_E_INVALID_ARGUMENT;
  }
  *workspace = (SQstJudgeNativeBatchWorkspace){0};
  workspace->written = (uint8_t *)calloc(message_capacity,
                                          sizeof(*workspace->written));
  workspace->payloads = (SQstPersistPayload *)calloc(
      message_capacity, sizeof(*workspace->payloads));
  workspace->messages = (SQstMessage **)calloc(message_capacity,
                                                sizeof(*workspace->messages));
  workspace->batch = (SQstPersistPayload *)calloc(message_capacity,
                                                   sizeof(*workspace->batch));
  workspace->members = (uint32_t *)calloc(message_capacity,
                                           sizeof(*workspace->members));
  if (NULL == workspace->written || NULL == workspace->payloads ||
      NULL == workspace->messages || NULL == workspace->batch ||
      NULL == workspace->members) {
    qstJudgeNativeBatchWorkspaceDestroy(workspace);
    return QST_E_RESOURCE;
  }
  return QST_OK;
}

/* The direct judge-completion path bypasses persist_queue, so it must release
 * the per-record transport blocks that main previously released per queue
 * confirmation. Task-owned blocks are released separately after the task
 * reaches PERSISTED. */
static EQstCode qstJudgeReleasePersistMessageBlocks(
    SQstShmMapping *mapping, const SQstJudgePendingClaim *pending) {
  SQstRef refs[QST_JUDGE_MAX_PERSIST_MESSAGES * 3U] = {{0}};
  uint32_t ref_count = 0;

  if (NULL == mapping || NULL == pending || pending->message_count == 0 ||
      pending->message_count > QST_JUDGE_MAX_PERSIST_MESSAGES ||
      pending->claim.message.gid == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (uint32_t index = 0; index < pending->message_count; ++index) {
    const SQstPersistPayload *persist =
        &pending->messages[index].body.persist;

    if (pending->messages[index].message_type != QST_MSG_PERSIST_CONFIRM ||
        persist->gid != pending->claim.message.gid ||
        ref_count > QST_JUDGE_MAX_PERSIST_MESSAGES * 3U - 3U) {
      return QST_E_SHM_CORRUPT;
    }
    refs[ref_count++] = persist->stable_key_ref;
    refs[ref_count++] = persist->payload_ref;
    refs[ref_count++] = persist->pending_file_ref;
  }
  return qst_block_release_many(mapping, refs, ref_count,
                                pending->claim.message.gid);
}

static EQstCode qstJudgeCommitPersistMessages(
    SQstShmMapping *mapping, uint32_t worker_slot, uint32_t generation,
    const SQstJudgePendingClaim *pending) {
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == pending || pending->message_count == 0 ||
      pending->message_count > QST_JUDGE_MAX_PERSIST_MESSAGES ||
      pending->first_record_seq == 0 || pending->deadline == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  {
    uint32_t persist_mask = 0;
    uint32_t durable_source = QST_DURABLE_RESULT_STORE;
    uint64_t last_record_seq = 0;

    for (uint32_t index = 0; index < pending->message_count; ++index) {
      const SQstPersistPayload *persist =
          &pending->messages[index].body.persist;

      persist_mask |= persist->persist_kind == QST_PERSIST_STATEMENT ?
                          QST_PERSIST_MASK_STATEMENT :
                      persist->persist_kind == QST_PERSIST_INITIAL_RESULT ?
                          QST_PERSIST_MASK_INITIAL_RESULT :
                          QST_PERSIST_MASK_HISTORICAL_VERDICT;
      if (persist->durable_source == QST_DURABLE_PENDING_FILE) {
        durable_source = QST_DURABLE_PENDING_FILE;
      }
      if (persist->record_seq > last_record_seq) {
        last_record_seq = persist->record_seq;
      }
    }
    code = qstJudgeReleasePersistMessageBlocks(mapping, pending);
    if (code == QST_OK) {
      code = qst_queue_ack(mapping, &pending->claim,
                           QST_ACK_PERSIST_WORK_ACCEPTED,
                           pending->first_record_seq);
    }
    if (code == QST_OK) {
      code = qst_task_complete_persisted_by_judge(
        mapping, pending->claim.message.gid,
        pending->claim.message.dispatch_epoch, persist_mask, durable_source,
        last_record_seq, worker_slot, generation);
    }
  }
  if (code != QST_OK) {
    return code;
  }
  return qst_process_record_business_instance(
      mapping, QST_ROLE_JUDGE_WORKER, worker_slot, generation,
      pending->claim.message.gid, pending->deadline);
}

/* Appends one already-sealed confirmation to the durable pending stream. */
static EQstCode qstJudgeAppendPendingMessage(
    SQstShmMapping *mapping, SQstResultStore *store,
    const SQstPendingStore *pending_store, SQstPendingBatch *pending_batch,
    SQstMessage *message) {
  SQstPendingRecord record = {0};
  char *dml = NULL;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == store || NULL == pending_store ||
      NULL == pending_batch || NULL == message ||
      message->message_type != QST_MSG_PERSIST_CONFIRM ||
      message->body.persist.durable_source != QST_DURABLE_RESULT_STORE) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qst_result_store_build_task_dml(mapping, store,
                                         &message->body.persist, &dml);
  if (code != QST_OK) {
    return code;
  }
  record.persist_kind = message->body.persist.persist_kind;
  record.target = message->body.persist.target;
  record.run_id = mapping->header->run_id;
  record.gid = message->body.persist.gid;
  record.record_seq = message->body.persist.record_seq;
  record.payload = dml;
  record.payload_bytes = strlen(dml);
  code = qst_pending_batch_append(pending_batch, &record);
  free(dml);
  if (code == QST_OK) {
    message->body.persist.durable_source = QST_DURABLE_PENDING_FILE;
  }
  return code;
}

/* Flushes native confirmations in same-kind/target batches. A failed native
 * batch has not made its members durable, so stage every remaining member in
 * the existing pending stream before queue acknowledgement. */
static EQstCode qstJudgeWriteNativeBatches(
    SQstShmMapping *mapping, SQstResultStore *store,
    const SQstPendingStore *pending_store, SQstPendingBatch *pending_batch,
    SQstJudgePendingClaim *claims, uint32_t claim_count,
    SQstJudgeProfile *profile) {
  /* A judge worker is a dedicated process and calls this helper serially.
   * Keep one private workspace for its lifetime instead of allocating five
   * fixed arrays for every native 128-claim flush. */
  static SQstJudgeNativeBatchWorkspace workspace = {0};
  uint32_t message_count = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == store || NULL == pending_store ||
      NULL == pending_batch || NULL == claims || claim_count == 0 ||
      claim_count > QST_JUDGE_NATIVE_BATCH_CLAIMS) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (workspace.written == NULL) {
    code = qstJudgeNativeBatchWorkspaceInit(
        &workspace, QST_JUDGE_NATIVE_BATCH_MESSAGES);
    if (code != QST_OK) {
      return code;
    }
  }
  memset(workspace.written, 0, QST_JUDGE_NATIVE_BATCH_MESSAGES);
  for (uint32_t claim_index = 0; claim_index < claim_count; ++claim_index) {
    if (claims[claim_index].message_count == 0 ||
        claims[claim_index].message_count > QST_JUDGE_MAX_PERSIST_MESSAGES ||
        message_count > QST_JUDGE_NATIVE_BATCH_MESSAGES -
                            claims[claim_index].message_count) {
      return QST_E_INVALID_ARGUMENT;
    }
    for (uint32_t message_index = 0;
         message_index < claims[claim_index].message_count; ++message_index) {
      SQstMessage *message = &claims[claim_index].messages[message_index];

      if (message->message_type != QST_MSG_PERSIST_CONFIRM) {
        return QST_E_SHM_CORRUPT;
      }
      workspace.messages[message_count] = message;
      workspace.payloads[message_count++] = message->body.persist;
    }
  }
  for (uint32_t first = 0; first < message_count; ++first) {
    uint32_t batch_count = 0;
    int32_t taos_code = 0;
    uint64_t started_ns = 0;

    if (workspace.written[first] != 0 ||
        workspace.payloads[first].durable_source != QST_DURABLE_RESULT_STORE) {
      continue;
    }
    for (uint32_t index = first; index < message_count; ++index) {
      if (workspace.written[index] == 0 &&
          workspace.payloads[index].durable_source == QST_DURABLE_RESULT_STORE &&
          workspace.payloads[index].persist_kind ==
              workspace.payloads[first].persist_kind &&
          workspace.payloads[index].target == workspace.payloads[first].target) {
        workspace.batch[batch_count] = workspace.payloads[index];
        workspace.members[batch_count++] = index;
      }
    }
    if (profile != NULL && profile->enabled != 0) {
      started_ns = qstMonotonicNowNs();
    }
    code = qst_result_store_write_batch(mapping, store, workspace.batch,
                                        batch_count, &taos_code);
    if (profile != NULL && profile->enabled != 0) {
      qstJudgeProfileAddElapsed(profile, started_ns, qstMonotonicNowNs(),
                                &profile->persist_batch_write_ns);
    }
    if (code == QST_OK) {
      if (profile != NULL && profile->native_batch_count != UINT64_MAX) {
        ++profile->native_batch_count;
      }
      if (profile != NULL && profile->enabled != 0) {
        profile->native_batch_claim_count =
            profile->native_batch_claim_count > UINT64_MAX - claim_count ?
                UINT64_MAX : profile->native_batch_claim_count + claim_count;
        profile->native_batch_record_count =
            profile->native_batch_record_count > UINT64_MAX - batch_count ?
                UINT64_MAX : profile->native_batch_record_count + batch_count;
        if (profile->native_batch_max_record_count < batch_count) {
          profile->native_batch_max_record_count = batch_count;
        }
      }
      for (uint32_t index = 0; index < batch_count; ++index) {
        workspace.written[workspace.members[index]] = 1;
      }
      continue;
    }
    /* qst_result_store_write_batch is all-or-error. Do not retry only a
     * subset after an error; stage every still-unwritten record together. */
    for (uint32_t index = 0; index < message_count; ++index) {
      if (workspace.written[index] == 0 &&
          workspace.payloads[index].durable_source == QST_DURABLE_RESULT_STORE) {
        code = qstJudgeAppendPendingMessage(mapping, store, pending_store,
                                            pending_batch,
                                            workspace.messages[index]);
        if (code != QST_OK) {
          return code;
        }
        workspace.payloads[index] = workspace.messages[index]->body.persist;
      }
    }
    return QST_OK;
  }
  return QST_OK;
}

/* Publishes only confirmations whose native batch or pending stream is durable. */
static EQstCode qstJudgeFlushPendingClaims(
    SQstShmMapping *mapping, SQstResultStore *store,
    const SQstPendingStore *pending_store, SQstPendingBatch *pending_batch,
    uint32_t worker_slot, uint32_t generation, SQstJudgePendingClaim *claims,
    uint32_t *inout_claim_count, SQstJudgeProfile *profile) {
  uint32_t claim_count = 0;
  int32_t needs_pending_flush = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == store || NULL == pending_store ||
      NULL == pending_batch || NULL == claims || NULL == inout_claim_count ||
      *inout_claim_count == 0 ||
      *inout_claim_count > QST_JUDGE_NATIVE_BATCH_CLAIMS) {
    return QST_E_INVALID_ARGUMENT;
  }
  claim_count = *inout_claim_count;
  code = qstJudgeWriteNativeBatches(mapping, store, pending_store, pending_batch,
                                    claims, claim_count, profile);
  if (code != QST_OK) {
    return code;
  }
  for (uint32_t claim_index = 0; claim_index < claim_count; ++claim_index) {
    for (uint32_t message_index = 0;
         message_index < claims[claim_index].message_count; ++message_index) {
      if (claims[claim_index].messages[message_index].body.persist.durable_source ==
          QST_DURABLE_PENDING_FILE) {
        needs_pending_flush = 1;
      }
    }
  }
  if (needs_pending_flush != 0) {
    code = qst_pending_batch_flush(pending_batch);
    if (code != QST_OK) {
      return code;
    }
  }
  for (uint32_t claim_index = 0; claim_index < claim_count; ++claim_index) {
    code = qstJudgeCommitPersistMessages(mapping, worker_slot, generation,
                                         &claims[claim_index]);
    if (code != QST_OK) {
      return code;
    }
  }
  *inout_claim_count = 0;
  return QST_OK;
}

static void qstJudgeAbandonPendingClaims(SQstShmMapping *mapping,
                                         SQstJudgePendingClaim *claims,
                                         uint32_t claim_count,
                                         EQstCode reason) {
  if (NULL == mapping || NULL == claims) {
    return;
  }
  for (uint32_t index = 0; index < claim_count; ++index) {
    (void)qst_queue_abandon(mapping, &claims[index].claim, reason);
  }
}

static void qstJudgeProfileAddElapsed(SQstJudgeProfile *profile,
                                      uint64_t started_ns, uint64_t finished_ns,
                                      uint64_t *total_ns) {
  uint64_t elapsed_ns = 0;

  if (NULL == profile || NULL == total_ns || profile->enabled == 0 ||
      started_ns == 0 || finished_ns < started_ns) {
    return;
  }
  elapsed_ns = finished_ns - started_ns;
  *total_ns = *total_ns > UINT64_MAX - elapsed_ns ? UINT64_MAX :
                                                     *total_ns + elapsed_ns;
}

static void qstJudgeWriteProfile(const SQstJudgeProfile *profile,
                                 uint32_t worker_slot, uint32_t generation) {
  if (NULL == profile || profile->enabled == 0) {
    return;
  }
  (void)fprintf(
      stderr,
      "taosQueryTest: judge_profile slot=%u generation=%u claims=%llu "
      "empty_claims=%llu "
      "verdict_ns=%llu persist_prepare_and_write_ns=%llu "
      "persist_batch_write_ns=%llu persist_publish_ns=%llu "
      "native_batch_count=%llu native_batch_claim_count=%llu "
      "native_batch_record_count=%llu native_batch_max_record_count=%llu\n",
      worker_slot, generation, (unsigned long long)profile->claim_count,
      (unsigned long long)profile->empty_claim_count,
      (unsigned long long)profile->verdict_ns,
      (unsigned long long)profile->persist_prepare_and_write_ns,
      (unsigned long long)profile->persist_batch_write_ns,
      (unsigned long long)profile->persist_publish_ns,
      (unsigned long long)profile->native_batch_count,
      (unsigned long long)profile->native_batch_claim_count,
      (unsigned long long)profile->native_batch_record_count,
      (unsigned long long)profile->native_batch_max_record_count);
}

/* A worker owns execute_queue claims and performs all process-local result I/O. */
EQstCode qst_judge_worker_run(SQstShmMapping *mapping, uint32_t generation,
                              uint32_t worker_slot) {
  SQstResultStore store = {0};
  SQstResultStoreConfig store_config = {0};
  SQstPendingStore pending_store = {0};
  SQstGenerationState *generation_state = NULL;
  struct pollfd event = {0};
  char pending_dir[4096] = {0};
  char quarantine_dir[4096] = {0};
  const char *failure_stage = "initialization";
  uint64_t failure_gid = 0;
  uint64_t failure_dispatch_epoch = 0;
  uint32_t failure_transition_seq = 0;
  SQstPendingBatch pending_batch = {.fd = -1};
  SQstJudgePendingClaim *pending_claims = NULL;
  uint32_t pending_claim_count = 0;
  uint32_t pending_claim_capacity = 0;
  uint64_t pending_batch_fill_deadline = 0;
  SQstJudgeProfile profile = {0};
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == mapping->header || generation == 0 ||
      worker_slot >= mapping->header->judge_worker_count) {
    return QST_E_INVALID_ARGUMENT;
  }
  profile.enabled = getenv("QST_PROFILE_PHASES") != NULL;
  code = qst_generator_state_get(mapping, &generation_state);
  if (code != QST_OK || generation_state->pending_quota_bytes == 0 ||
      generation_state->log_rotation_bytes == 0) {
    return code != QST_OK ? code : QST_E_SHM_CORRUPT;
  }
  code = qstWriteProcessStartLog(
      QST_ROLE_JUDGE_WORKER, generation, QST_ROLE_JUDGE_MANAGER,
      atomic_load_explicit(&mapping->header->main_generation,
                           memory_order_acquire));
  if (code != QST_OK) {
    return code;
  }
  code = qst_process_confirm_flow_instance(
      mapping, QST_ROLE_JUDGE_WORKER, worker_slot, generation,
      QST_FLOW_STAGE_STARTUP, QST_FLOW_STATE_INPUT_QUEUE, 0, 0, 0);
  if (code != QST_OK) {
    return code;
  }
  qstTestMaybeKillRole(QST_ROLE_JUDGE_WORKER, generation, worker_slot);
  store_config.run_id = mapping->header->run_id;
  store_config.endpoint = getenv("QST_RESULT_STORE_ENDPOINT");
  store_config.config_dir = getenv("QST_RESULT_STORE_CONFIG_DIR");
  store_config.user = getenv("QST_RESULT_STORE_USER");
  store_config.password = getenv("QST_RESULT_STORE_PASSWORD");
  if (gQstOverflowContentPath[0] != '\0') {
    store_config.overflow_path = gQstOverflowContentPath;
  }
  if (gQstProcessLogDirectory[0] != '\0') {
    const size_t log_bytes = strlen(gQstProcessLogDirectory);

    if (log_bytes <= strlen("/logs") ||
        strcmp(gQstProcessLogDirectory + log_bytes - strlen("/logs"),
               "/logs") != 0 ||
        snprintf(pending_dir, sizeof(pending_dir), "%.*s/pending-results",
                 (int)(log_bytes - strlen("/logs")),
                 gQstProcessLogDirectory) < 0 ||
        snprintf(quarantine_dir, sizeof(quarantine_dir), "%s/quarantine",
                 pending_dir) < 0) {
      return QST_E_RESOURCE;
    }
    store_config.pending_dir = pending_dir;
    store_config.quarantine_dir = quarantine_dir;
    pending_store.run_id = mapping->header->run_id;
    pending_store.pending_dir = pending_dir;
    pending_store.quarantine_dir = quarantine_dir;
    pending_store.quota_bytes = generation_state->pending_quota_bytes;
  }
  code = qst_result_store_open(&store_config, &store);
  if (code != QST_OK && NULL == pending_store.pending_dir) {
    return code;
  }
  if (code != QST_OK) {
    memset(&store, 0, sizeof(store));
    if (qst_result_store_configure_connection(&store_config, &store) != QST_OK) {
      return code;
    }
    if (store_config.pending_dir != NULL) {
      (void)snprintf(store.pending_dir, sizeof(store.pending_dir), "%s",
                     store_config.pending_dir);
    }
    if (store_config.quarantine_dir != NULL) {
      (void)snprintf(store.quarantine_dir, sizeof(store.quarantine_dir), "%s",
                     store_config.quarantine_dir);
    }
    if (store_config.overflow_path != NULL) {
      (void)snprintf(store.overflow_path, sizeof(store.overflow_path), "%s",
                     store_config.overflow_path);
    }
    code = QST_OK;
  }
  if (pending_store.pending_dir != NULL) {
    code = qst_pending_batch_begin(mapping, &pending_store, &pending_batch);
    if (code != QST_OK) {
      (void)qst_result_store_close(&store);
      return code;
    }
  }
  event.fd = mapping->queue_eventfd[QST_QUEUE_EXECUTE - 1];
  event.events = POLLIN;
  for (;;) {
    SQstClaim claim = {0};
    SQstJudgePendingClaim pending_claim = {0};
    SQstRef statement_key_ref = {0};
    SQstRef statement_payload_ref = {0};
    SQstRef result_key_ref = {0};
    SQstRef result_payload_ref = {0};
    SQstRef historical_key_ref = {0};
    SQstRef historical_payload_ref = {0};
    SQstJudgeVerdict verdict = {0};
    SQstTaskPersistSnapshot snapshot = {0};
    uint32_t stop_generation = 0;
    uint32_t exit_state = 0;
    uint64_t first_record_seq = 0;
    int32_t record_uses_pending_batch = 0;
    uint64_t deadline = qstMillisecondsToDeadlineNs(QST_FLOW_LEASE_MILLISECONDS);
    uint64_t verdict_started_ns = 0;
    uint64_t persist_started_ns = 0;
    uint64_t publish_started_ns = 0;

    if (deadline == 0) {
      code = QST_E_RESOURCE;
      break;
    }
    failure_stage = "get_stop_generation";
    code = qst_process_get_stop_generation_instance(
        mapping, QST_ROLE_JUDGE_WORKER, worker_slot, generation,
        &stop_generation, &exit_state);
    if (code != QST_OK) {
      break;
    }
    if (exit_state == QST_PROCESS_STOP_REQUESTED && stop_generation != 0) {
      if (pending_claim_count != 0) {
        failure_stage = "flush_stop_persist_batch";
        code = qstJudgeFlushPendingClaims(
            mapping, &store, &pending_store, &pending_batch, worker_slot,
            generation, pending_claims, &pending_claim_count, &profile);
        if (code != QST_OK) {
          break;
        }
      }
      qst_pending_batch_abort(&pending_batch);
      failure_stage = "ack_stop";
      code = qst_process_ack_stop_instance(
          mapping, QST_ROLE_JUDGE_WORKER, worker_slot, generation,
          stop_generation);
      break;
    }
    if (exit_state != QST_PROCESS_RUNNING) {
      code = QST_OK;
      break;
    }
    failure_stage = "claim_execute";
    code = qst_queue_claim_instance(mapping, QST_QUEUE_EXECUTE,
                                    QST_ROLE_JUDGE_WORKER, worker_slot,
                                    generation, deadline, &claim);
    if (code == QST_E_QUEUE_EMPTY) {
      if (profile.enabled != 0 && profile.empty_claim_count != UINT64_MAX) {
        ++profile.empty_claim_count;
      }
      if (pending_claim_count != 0) {
        if (pending_batch_fill_deadline == 0) {
          pending_batch_fill_deadline = qstJudgeNativeBatchFillDeadline(mapping);
          if (pending_batch_fill_deadline == 0) {
            code = QST_E_RESOURCE;
            break;
          }
        }
        if (qstMonotonicNowNs() < pending_batch_fill_deadline) {
          code = qstPollQueueEvent(
              &event, QST_JUDGE_NATIVE_BATCH_SHORT_FILL_MILLISECONDS);
          if (code != QST_OK) {
            break;
          }
          continue;
        }
        failure_stage = "flush_native_persist_batch";
        if (profile.enabled != 0) {
          publish_started_ns = qstMonotonicNowNs();
        }
        code = qstJudgeFlushPendingClaims(
            mapping, &store, &pending_store, &pending_batch, worker_slot,
            generation, pending_claims, &pending_claim_count, &profile);
        if (profile.enabled != 0) {
          qstJudgeProfileAddElapsed(&profile, publish_started_ns,
                                    qstMonotonicNowNs(),
                                    &profile.persist_publish_ns);
        }
        if (code != QST_OK) {
          break;
        }
        pending_batch_fill_deadline = 0;
        continue;
      }
      failure_stage = "confirm_idle";
      code = qst_process_confirm_flow_instance(
          mapping, QST_ROLE_JUDGE_WORKER, worker_slot, generation,
          QST_FLOW_STAGE_IDLE, QST_FLOW_STATE_INPUT_QUEUE, 0, 0, 0);
      if (code == QST_OK && qstPollQueueEvent(&event, 20) != QST_OK) {
        code = QST_E_RESOURCE;
      }
      if (code == QST_E_STALE_OWNER) {
        code = QST_OK;
        continue;
      }
      if (code == QST_OK) {
        qstSleepMilliseconds(QST_JUDGE_EMPTY_CLAIM_BACKOFF_MILLISECONDS);
        continue;
      }
      break;
    }
    if (code != QST_OK) {
      break;
    }
    if (profile.enabled != 0) {
      ++profile.claim_count;
    }
    failure_gid = claim.message.gid;
    failure_dispatch_epoch = claim.message.dispatch_epoch;
    failure_transition_seq = claim.message.transition_seq;
    failure_stage = "wait_final_publication";
    code = qstJudgeWaitForPersistSnapshot(mapping, &claim, &snapshot);
    if (code != QST_OK) {
      (void)qst_queue_abandon(mapping, &claim, code);
      break;
    }
    failure_stage = "select_verdict";
    if (profile.enabled != 0) {
      verdict_started_ns = qstMonotonicNowNs();
    }
    code = qstJudgeSelectVerdict(mapping, &store, &claim, &verdict);
    if (profile.enabled != 0) {
      qstJudgeProfileAddElapsed(&profile, verdict_started_ns,
                                qstMonotonicNowNs(), &profile.verdict_ns);
      persist_started_ns = qstMonotonicNowNs();
    }
    if (code == QST_OK) {
      const uint32_t persist_mask =
          snapshot.focus == QST_FOCUS_CORRECTNESS && verdict.verdict == 0 &&
                  claim.message.body.execute.result_ref.length != 0 ?
              QST_PERSIST_MASK_STATEMENT | QST_PERSIST_MASK_INITIAL_RESULT |
                  QST_PERSIST_MASK_HISTORICAL_VERDICT :
          snapshot.focus == QST_FOCUS_CORRECTNESS ?
              QST_PERSIST_MASK_STATEMENT | QST_PERSIST_MASK_INITIAL_RESULT :
              QST_PERSIST_MASK_STATEMENT;

      failure_stage = "begin_persist";
      code = qst_task_begin_persist(
        mapping, claim.message.gid, claim.message.dispatch_epoch,
        claim.message.transition_seq, QST_ROLE_JUDGE_WORKER, worker_slot,
        generation, snapshot.focus == QST_FOCUS_CORRECTNESS ?
                        (int32_t)verdict.verdict : -1,
        persist_mask);
    }
    if (code == QST_OK) {
      first_record_seq = atomic_fetch_add_explicit(
          &mapping->header->next_record_seq, 1, memory_order_acq_rel);
      if (first_record_seq == 0) {
        first_record_seq = atomic_fetch_add_explicit(
            &mapping->header->next_record_seq, 1, memory_order_acq_rel);
      }
      failure_stage = "persist_statement";
      code = qstJudgePersistRecord(
          mapping, &store, pending_store.pending_dir == NULL ? NULL : &pending_store,
          pending_store.pending_dir == NULL ? NULL : &pending_batch,
          worker_slot, generation, &claim,
          QST_PERSIST_STATEMENT, QST_PERSIST_MASK_STATEMENT, NULL, first_record_seq,
          1,
          &statement_key_ref, &statement_payload_ref,
          &pending_claim.messages[pending_claim.message_count],
          &record_uses_pending_batch);
      if (code == QST_OK) {
        ++pending_claim.message_count;
      }
    }
    if (code == QST_OK && snapshot.focus == QST_FOCUS_CORRECTNESS) {
      const uint64_t result_record_seq = atomic_fetch_add_explicit(
          &mapping->header->next_record_seq, 1, memory_order_acq_rel);
      failure_stage = "persist_initial_result";
      code = result_record_seq == 0 ? QST_E_RESOURCE : qstJudgePersistRecord(
          mapping, &store, pending_store.pending_dir == NULL ? NULL : &pending_store,
          pending_store.pending_dir == NULL ? NULL : &pending_batch,
          worker_slot, generation, &claim,
          QST_PERSIST_INITIAL_RESULT, QST_PERSIST_MASK_INITIAL_RESULT,
          &verdict, result_record_seq, 1, &result_key_ref, &result_payload_ref,
          &pending_claim.messages[pending_claim.message_count],
          &record_uses_pending_batch);
      if (code == QST_OK) {
        ++pending_claim.message_count;
      }
    }
    if (code == QST_OK && snapshot.focus == QST_FOCUS_CORRECTNESS &&
        verdict.verdict == 0 &&
        claim.message.body.execute.result_ref.length != 0) {
      const uint64_t historical_record_seq = atomic_fetch_add_explicit(
          &mapping->header->next_record_seq, 1, memory_order_acq_rel);

      failure_stage = "persist_historical_verdict";
      code = historical_record_seq == 0 ? QST_E_RESOURCE : qstJudgePersistRecord(
          mapping, &store, pending_store.pending_dir == NULL ? NULL : &pending_store,
          pending_store.pending_dir == NULL ? NULL : &pending_batch,
          worker_slot, generation, &claim, QST_PERSIST_HISTORICAL_VERDICT,
          QST_PERSIST_MASK_HISTORICAL_VERDICT, &verdict, historical_record_seq,
          1, &historical_key_ref, &historical_payload_ref,
          &pending_claim.messages[pending_claim.message_count],
          &record_uses_pending_batch);
      if (code == QST_OK) {
        ++pending_claim.message_count;
      }
    }
    if (code == QST_OK) {
      pending_claim.claim = claim;
      pending_claim.first_record_seq = first_record_seq;
      pending_claim.deadline = deadline;
      if (profile.enabled != 0) {
        qstJudgeProfileAddElapsed(&profile, persist_started_ns,
                                  qstMonotonicNowNs(),
                                  &profile.persist_prepare_and_write_ns);
        publish_started_ns = qstMonotonicNowNs();
      }
      if (pending_claim_count == pending_claim_capacity) {
        const uint32_t next_capacity = pending_claim_capacity == 0 ?
            QST_JUDGE_NATIVE_BATCH_CLAIMS : pending_claim_capacity * 2U;
        SQstJudgePendingClaim *next_claims = NULL;

        if (next_capacity < pending_claim_capacity ||
            next_capacity > SIZE_MAX / sizeof(*pending_claims)) {
          code = QST_E_RESOURCE;
        } else {
          next_claims = (SQstJudgePendingClaim *)realloc(
              pending_claims, next_capacity * sizeof(*pending_claims));
          if (next_claims == NULL) {
            code = QST_E_RESOURCE;
          } else {
            pending_claims = next_claims;
            pending_claim_capacity = next_capacity;
          }
        }
      }
      if (code == QST_OK) {
        pending_claims[pending_claim_count++] = pending_claim;
      }
      if (code == QST_OK &&
          pending_claim_count == QST_JUDGE_NATIVE_BATCH_CLAIMS) {
        failure_stage = "flush_native_persist_batch";
        if (profile.enabled != 0) {
          publish_started_ns = qstMonotonicNowNs();
        }
        code = qstJudgeFlushPendingClaims(
            mapping, &store, &pending_store, &pending_batch, worker_slot,
            generation, pending_claims, &pending_claim_count, &profile);
        if (profile.enabled != 0) {
          qstJudgeProfileAddElapsed(&profile, publish_started_ns,
                                    qstMonotonicNowNs(),
                                    &profile.persist_publish_ns);
        }
        if (code == QST_OK) {
          pending_batch_fill_deadline = 0;
        }
      }
    }
    if (code != QST_OK) {
      qstJudgeAbandonPendingClaims(mapping, &pending_claim, 1, code);
      break;
    }
  }
  if (pending_claim_count != 0 &&
      (code == QST_OK || code == QST_E_STOPPING || code == QST_E_QUEUE_EMPTY)) {
    failure_stage = "flush_final_persist_batch";
    code = qstJudgeFlushPendingClaims(mapping, &store, &pending_store,
                                      &pending_batch, worker_slot, generation,
                                      pending_claims, &pending_claim_count,
                                      &profile);
  }
  qst_pending_batch_abort(&pending_batch);
  qstJudgeAbandonPendingClaims(mapping, pending_claims, pending_claim_count,
                               code == QST_OK ? QST_E_STOPPING : code);
  free(pending_claims);
  qstJudgeWriteProfile(&profile, worker_slot, generation);
  if (qst_result_store_close(&store) != QST_OK && code == QST_OK) {
    code = QST_E_RESOURCE;
    failure_stage = "close_result_store";
  }
  if (code != QST_OK && code != QST_E_QUEUE_EMPTY && code != QST_E_STOPPING) {
    (void)fprintf(stderr,
                  "taosQueryTest: judge worker failed code=%d stage=%s "
                  "slot=%u generation=%u gid=%llu dispatch_epoch=%llu "
                  "transition_seq=%u\n",
                  code, failure_stage, worker_slot, generation,
                  (unsigned long long)failure_gid,
                  (unsigned long long)failure_dispatch_epoch,
                  failure_transition_seq);
  }
  return code;
}

static EQstCode qstStartJudgeWorker(SQstShmMapping *mapping,
                                    uint32_t parent_generation,
                                    uint32_t worker_slot,
                                    SQstJudgeWorkerChild *out_child) {
  int32_t generation_pipe[2] = {-1, -1};
  int32_t pidfd = -1;
  int32_t status = 0;
  pid_t pid = -1;
  uint32_t generation = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == out_child || parent_generation == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (pipe(generation_pipe) != 0) {
    return QST_E_RESOURCE;
  }
  pid = fork();
  if (pid < 0) {
    (void)close(generation_pipe[0]);
    (void)close(generation_pipe[1]);
    return QST_E_RESOURCE;
  }
  if (pid == 0) {
    uint32_t child_generation = 0;
    const ssize_t bytes = read(generation_pipe[0], &child_generation,
                               sizeof(child_generation));
    const pid_t parent_pid = getppid();

    (void)close(generation_pipe[1]);
    if (prctl(PR_SET_PDEATHSIG, SIGKILL) != 0 || getppid() != parent_pid ||
        setpgid(0, 0) != 0 || bytes != (ssize_t)sizeof(child_generation) ||
        child_generation == 0) {
      (void)close(generation_pipe[0]);
      _exit(1);
    }
    (void)close(generation_pipe[0]);
    code = qst_judge_worker_run(mapping, child_generation, worker_slot);
    if (code != QST_OK) {
      (void)qstWriteLifecycleLog(
          QST_ROLE_JUDGE_WORKER, child_generation, "PROCESS_EXIT_ERROR",
          QST_ROLE_JUDGE_WORKER, child_generation, (int32_t)getpid(),
          (int32_t)getpgrp(), code, 0);
    }
    if (code == QST_OK) {
      code = qst_process_mark_exited_instance(
          mapping, QST_ROLE_JUDGE_WORKER, worker_slot, child_generation, 0);
    }
    _exit(code == QST_OK ? 0 : 1);
  }
  (void)close(generation_pipe[0]);
  if (setpgid(pid, pid) != 0 && errno != EACCES && errno != ESRCH) {
    code = QST_E_RESOURCE;
  }
  if (code == QST_OK) {
    code = qst_process_register_instance(
        mapping, QST_ROLE_JUDGE_MANAGER, 0, parent_generation,
        QST_ROLE_JUDGE_WORKER, worker_slot, (int32_t)pid, (int32_t)pid,
        &generation);
  }
  if (code == QST_OK &&
      write(generation_pipe[1], &generation, sizeof(generation)) !=
          (ssize_t)sizeof(generation)) {
    code = QST_E_RESOURCE;
  }
  (void)close(generation_pipe[1]);
  if (code == QST_OK) {
    pidfd = qstOpenPidfd(pid);
    code = pidfd < 0 ? QST_E_RESOURCE : QST_OK;
  }
  if (code == QST_OK) {
    code = qstWaitForRoleFlow(mapping, QST_ROLE_JUDGE_WORKER, worker_slot,
                              generation, qstMillisecondsToDeadlineNs(
                                  QST_STOP_ACK_MILLISECONDS));
  }
  if (code != QST_OK) {
    if (pidfd >= 0) {
      (void)qstStopDirectChild(mapping, QST_ROLE_JUDGE_MANAGER,
                               parent_generation, QST_ROLE_JUDGE_WORKER,
                               generation, pid, pidfd);
      (void)close(pidfd);
    } else {
      (void)kill(-pid, SIGKILL);
    }
    (void)waitpid(pid, &status, 0);
    if (generation != 0) {
      (void)qst_process_reap_generation_instance(
          mapping, QST_ROLE_JUDGE_MANAGER, 0, parent_generation,
          QST_ROLE_JUDGE_WORKER, worker_slot, generation, status);
    }
    return code;
  }
  out_child->slot = worker_slot;
  out_child->generation = generation;
  out_child->pid = pid;
  out_child->pidfd = pidfd;
  return QST_OK;
}

static EQstCode qstStopAndReapJudgeWorkers(
    SQstShmMapping *mapping, uint32_t parent_generation,
    SQstJudgeWorkerChild *workers, uint32_t worker_count) {
  EQstCode first_error = QST_OK;
  uint8_t *exited = NULL;
  uint64_t deadline = 0;

  if (NULL == mapping || (worker_count != 0 && NULL == workers)) {
    return QST_E_INVALID_ARGUMENT;
  }
  exited = (uint8_t *)calloc(worker_count, sizeof(*exited));
  deadline = qstMillisecondsToDeadlineNs(QST_STOP_ACK_MILLISECONDS);
  if ((worker_count != 0 && NULL == exited) || deadline == 0) {
    free(exited);
    return QST_E_RESOURCE;
  }

  /* Broadcast before waiting so one manager deadline covers every worker. */
  for (uint32_t index = 0; index < worker_count; ++index) {
    EQstCode code = QST_OK;

    if (workers[index].generation == 0) continue;
    code = qst_process_request_stop_instance(
        mapping, QST_ROLE_JUDGE_MANAGER, 0, parent_generation,
        QST_ROLE_JUDGE_WORKER, workers[index].slot, workers[index].generation, 1);
    if (first_error == QST_OK && code != QST_OK) first_error = code;
  }
  for (;;) {
    int32_t all_exited = 1;

    for (uint32_t index = 0; index < worker_count; ++index) {
      int32_t process_exited = 0;
      EQstCode code = QST_OK;

      if (workers[index].generation == 0 || exited[index] != 0) continue;
      code = qstWaitForExit(workers[index].pidfd, 20, &process_exited);
      if (code != QST_OK) {
        if (first_error == QST_OK) first_error = code;
        continue;
      }
      exited[index] = process_exited != 0;
      if (exited[index] == 0) all_exited = 0;
    }
    if (all_exited != 0 || qstMillisecondsToDeadlineNs(1) == 0 ||
        qstMillisecondsToDeadlineNs(1) > deadline) {
      break;
    }
  }
  for (uint32_t index = 0; index < worker_count; ++index) {
    if (workers[index].generation != 0 && exited[index] == 0 &&
        kill(-workers[index].pid, SIGKILL) != 0 && errno != ESRCH &&
        first_error == QST_OK) {
      first_error = QST_E_RESOURCE;
    }
  }
  for (uint32_t index = 0; index < worker_count; ++index) {
    int32_t status = 0;
    int32_t process_exited = 0;
    EQstCode code = QST_OK;

    if (workers[index].generation == 0) continue;
    if (exited[index] == 0) {
      code = qstWaitForExit(workers[index].pidfd, QST_KILL_GRACE_MILLISECONDS,
                            &process_exited);
    }
    while (waitpid(workers[index].pid, &status, 0) < 0 && errno == EINTR) {
    }
    (void)close(workers[index].pidfd);
    if (qst_process_reap_generation_instance(
            mapping, QST_ROLE_JUDGE_MANAGER, 0, parent_generation,
            QST_ROLE_JUDGE_WORKER, workers[index].slot,
            workers[index].generation, status) != QST_OK && code == QST_OK) {
      code = QST_E_RESOURCE;
    }
    if ((!WIFEXITED(status) || WEXITSTATUS(status) != 0) && code == QST_OK) {
      code = QST_E_RESOURCE;
    }
    if (first_error == QST_OK && code != QST_OK) first_error = code;
  }
  free(exited);
  return first_error;
}

/* Detect a failed judge worker before its claimed execute work can stall the run. */
static EQstCode qstCheckJudgeWorkerHealth(
    const SQstJudgeWorkerChild *workers, uint32_t worker_count) {
  if (worker_count != 0 && NULL == workers) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (uint32_t index = 0; index < worker_count; ++index) {
    int32_t exited = 0;
    EQstCode code = QST_OK;

    if (workers[index].generation == 0) {
      continue;
    }
    code = qstWaitForExit(workers[index].pidfd, 1, &exited);
    if (code != QST_OK) {
      return code;
    }
    if (exited != 0) {
      (void)fprintf(stderr,
                    "taosQueryTest: judge worker exited unexpectedly "
                    "slot=%u generation=%u pid=%d\n",
                    workers[index].slot, workers[index].generation,
                    (int)workers[index].pid);
      return QST_E_RESOURCE;
    }
  }
  return QST_OK;
}

EQstCode qst_judge_manager_run(SQstShmMapping *mapping, uint32_t generation) {
  SQstJudgeWorkerChild *workers = NULL;
  struct pollfd event = {0};
  uint32_t worker_count = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == mapping->header || generation == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qstWriteProcessStartLog(QST_ROLE_JUDGE_MANAGER, generation,
                                 QST_ROLE_MAIN,
                                 atomic_load_explicit(&mapping->header->main_generation,
                                                      memory_order_acquire));
  if (code != QST_OK) {
    return code;
  }
  code = qstConfirmQueueFlow(mapping, QST_ROLE_JUDGE_MANAGER, generation,
                             QST_FLOW_STAGE_STARTUP, QST_QUEUE_EXECUTE, 0, 0);
  if (code != QST_OK) {
    uint32_t stop_generation = 0;
    uint32_t exit_state = 0;
    if (code == QST_E_STALE_OWNER &&
        qst_process_get_stop_generation(mapping, QST_ROLE_JUDGE_MANAGER,
                                        generation, &stop_generation,
                                        &exit_state) == QST_OK &&
        exit_state == QST_PROCESS_STOP_REQUESTED && stop_generation != 0) {
      return qstAcknowledgeRoleStop(mapping, QST_ROLE_JUDGE_MANAGER, generation,
                                    stop_generation);
    }
    return code;
  }
  qstTestMaybeKillRole(QST_ROLE_JUDGE_MANAGER, generation, UINT32_MAX);
  worker_count = mapping->header->judge_worker_count;
  if (worker_count == 0 || worker_count > mapping->header->process_region.element_count) {
    return QST_E_SHM_CORRUPT;
  }
  workers = (SQstJudgeWorkerChild *)calloc(worker_count, sizeof(*workers));
  if (NULL == workers) {
    return QST_E_RESOURCE;
  }
  for (uint32_t index = 0; index < worker_count; ++index) {
    code = qstStartJudgeWorker(mapping, generation, index, &workers[index]);
    if (code != QST_OK) {
      (void)qstStopAndReapJudgeWorkers(mapping, generation, workers, index);
      free(workers);
      return code;
    }
  }
  event.fd = mapping->queue_eventfd[QST_QUEUE_CONTROL - 1];
  event.events = POLLIN;
  for (;;) {
    uint32_t stop_generation = 0;
    uint32_t exit_state = 0;

    code = qst_process_get_stop_generation(mapping, QST_ROLE_JUDGE_MANAGER,
                                           generation, &stop_generation,
                                           &exit_state);
    if (code != QST_OK) {
      break;
    }
    if (exit_state == QST_PROCESS_STOP_REQUESTED && stop_generation != 0) {
      const EQstCode stop_code = qstStopAndReapJudgeWorkers(
          mapping, generation, workers, worker_count);
      free(workers);
      if (stop_code != QST_OK) {
        return stop_code;
      }
      return qstAcknowledgeRoleStop(mapping, QST_ROLE_JUDGE_MANAGER, generation,
                                    stop_generation);
    }
    if (exit_state != QST_PROCESS_RUNNING) {
      code = QST_OK;
      break;
    }
    code = qstCheckJudgeWorkerHealth(workers, worker_count);
    if (code != QST_OK) {
      break;
    }
    code = qstConfirmQueueFlow(mapping, QST_ROLE_JUDGE_MANAGER, generation,
                               QST_FLOW_STAGE_IDLE, QST_QUEUE_EXECUTE, 0, 0);
    if (code == QST_E_STALE_OWNER) {
      continue;
    }
    if (code != QST_OK || qstPollQueueEvent(&event, 20) != QST_OK) {
      if (code == QST_OK) {
        code = QST_E_RESOURCE;
      }
      break;
    }
  }
  {
    const EQstCode stop_code = qstStopAndReapJudgeWorkers(
        mapping, generation, workers, worker_count);
    free(workers);
    return code == QST_OK ? stop_code : code;
  }
}
