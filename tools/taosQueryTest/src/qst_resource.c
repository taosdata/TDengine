/* Runtime resource sampling and overflow-checked capacity calculation. */

#define _POSIX_C_SOURCE 200809L

#include "qst_resource.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/statvfs.h>
#include <time.h>
#include <unistd.h>

#include "qst_ai.h"
#include "qst_block.h"
#include "qst_process.h"
#include "qst_queue.h"
#include "qst_supervisor.h"
#include "qst_task.h"

enum {
  QST_RESOURCE_MEBIBYTE = 1024U * 1024U,
  QST_RESOURCE_MIN_RESERVE_BYTES = 64U * QST_RESOURCE_MEBIBYTE,
  QST_RESOURCE_PAIR_PROCESS_COUNT = 3,
  QST_RESOURCE_MANDATORY_PROCESS_COUNT = 5,
  QST_RESOURCE_PAIR_FD_COUNT = 8,
  QST_RESOURCE_JUDGE_FD_COUNT = 5,
  QST_RESOURCE_MANAGER_FD_RESERVE = 16,
  QST_RESOURCE_MIN_BLOCK_BYTES = 4096,
  QST_RESOURCE_CONTROL_BLOCK_RESERVE = 16,
  /* Dataset preparation publishes the complete local fixture before any task
   * can be generated. Reserve its bounded object directory and manifests so a
   * small statement limit cannot consume the entire block region up front. */
  QST_RESOURCE_FIXTURE_BLOCK_RESERVE = 256,
  QST_RESOURCE_MIN_BLOCK_COUNT = QST_RESOURCE_CONTROL_BLOCK_RESERVE +
                                 QST_RESOURCE_FIXTURE_BLOCK_RESERVE,
  /* Shutdown can overlap control acknowledgements from the exec manager,
   * judge manager, and pair supervisor. Keep room for that protocol even
   * when a short run is limited to one statement and one pair. */
  QST_RESOURCE_MIN_QUEUE_CAPACITY = 4,
  QST_RESOURCE_MIN_AI_ACTIONS = 1,
  /* RANDOM generation prepares a 16-item deterministic batch. Preserve that
   * automatic width, but allow an explicit low-thread isolated-pair request to
   * use the larger capacity supported by the process and descriptor budgets. */
  QST_RESOURCE_MAX_AUTOMATIC_EXECUTION_PAIRS = 16,
  QST_RESOURCE_MAX_EXECUTION_PAIRS = 1024,
  QST_RESOURCE_MAX_JUDGE_WORKERS = 16,
  /* The asynchronous native client can overlap requests on one connection.
   * Start from a CPU-proportional total work budget, then divide it between
   * the isolated pairs selected for this exact run. */
  /* The sustained-QPS profile uses the available CPU quota primarily in the
   * isolated taosd pair servers. Keep enough overlap per CPU to fill the
   * native async pipeline; the measured tool/taosd ratio remains the final
   * safety gate for accepting a run. */
  QST_RESOURCE_ASYNC_REQUESTS_PER_CPU = 48,
  /* Execution pairs own isolated query servers, while judge workers share one
   * result-store endpoint. Keep writer connections bounded independently of
   * CPU-derived execution width so a large pair set cannot exhaust it. */
  QST_RESOURCE_MAX_RESULT_STORE_WRITERS = 16,
};

static uint64_t qstMinimum(uint64_t left, uint64_t right) {
  return left < right ? left : right;
}

static uint64_t qstMaximum(uint64_t left, uint64_t right) {
  return left > right ? left : right;
}

static EQstCode qstAdd(uint64_t left, uint64_t right, uint64_t *out_value) {
  if (NULL == out_value || left > UINT64_MAX - right) {
    return QST_E_RESOURCE;
  }
  *out_value = left + right;
  return QST_OK;
}

static EQstCode qstMultiply(uint64_t left, uint64_t right,
                            uint64_t *out_value) {
  if (NULL == out_value || (left != 0 && right > UINT64_MAX / left)) {
    return QST_E_RESOURCE;
  }
  *out_value = left * right;
  return QST_OK;
}

static EQstCode qstAlign(uint64_t value, uint64_t alignment,
                         uint64_t *out_value) {
  uint64_t padding = 0;

  if (NULL == out_value || alignment == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  padding = (alignment - value % alignment) % alignment;
  return qstAdd(value, padding, out_value);
}

static EQstCode qstReadLine(const char *path, char *out_text,
                            uint64_t out_bytes) {
  FILE *file = NULL;

  if (NULL == path || NULL == out_text || out_bytes < 2) {
    return QST_E_INVALID_ARGUMENT;
  }
  file = fopen(path, "r");
  if (NULL == file) {
    return QST_E_RESOURCE;
  }
  if (NULL == fgets(out_text, (int)out_bytes, file) || ferror(file)) {
    (void)fclose(file);
    return QST_E_RESOURCE;
  }
  if (fclose(file) != 0) {
    return QST_E_RESOURCE;
  }
  out_text[strcspn(out_text, "\r\n")] = '\0';
  return QST_OK;
}

EQstCode qst_resource_parse_proc_stat_cpu_jiffies(const char *stat_text,
                                                   uint64_t *out_jiffies) {
  const char *cursor = NULL;
  const char *left_parenthesis = NULL;
  const char *right_parenthesis = NULL;
  uint64_t utime = 0;
  uint64_t stime = 0;

  if (NULL == stat_text || NULL == out_jiffies) {
    return QST_E_INVALID_ARGUMENT;
  }
  left_parenthesis = strchr(stat_text, '(');
  right_parenthesis = strrchr(stat_text, ')');
  if (NULL == left_parenthesis || NULL == right_parenthesis ||
      left_parenthesis >= right_parenthesis || right_parenthesis == stat_text ||
      right_parenthesis[1] == '\0') {
    return QST_E_RESOURCE;
  }
  cursor = right_parenthesis + 1;
  for (uint32_t field = 3; field <= 15; ++field) {
    char *end = NULL;
    unsigned long long value = 0;

    while (*cursor == ' ') {
      ++cursor;
    }
    if (*cursor == '\0') {
      return QST_E_RESOURCE;
    }
    if (field == 3) {
      while (*cursor != '\0' && *cursor != ' ') {
        ++cursor;
      }
      continue;
    }
    if (field < 14) {
      while (*cursor != '\0' && *cursor != ' ') {
        ++cursor;
      }
      continue;
    }
    errno = 0;
    value = strtoull(cursor, &end, 10);
    if (*cursor == '-' || errno != 0 || end == cursor ||
        (*end != '\0' && *end != ' ')) {
      return QST_E_RESOURCE;
    }
    if (field == 14) {
      utime = (uint64_t)value;
    } else {
      stime = (uint64_t)value;
    }
    cursor = end;
  }
  if (utime > UINT64_MAX - stime) {
    return QST_E_RESOURCE;
  }
  *out_jiffies = utime + stime;
  return QST_OK;
}

EQstCode qst_resource_read_proc_cpu_jiffies(int32_t pid,
                                             uint64_t *out_jiffies) {
  char path[64] = {0};
  char stat_text[4096] = {0};
  int32_t written = 0;

  if (pid <= 0 || NULL == out_jiffies) {
    return QST_E_INVALID_ARGUMENT;
  }
  written = snprintf(path, sizeof(path), "/proc/%d/stat", pid);
  if (written <= 0 || (uint64_t)written >= sizeof(path) ||
      qstReadLine(path, stat_text, sizeof(stat_text)) != QST_OK) {
    return QST_E_RESOURCE;
  }
  return qst_resource_parse_proc_stat_cpu_jiffies(stat_text, out_jiffies);
}

EQstCode qst_resource_read_proc_cpu_identity(int32_t pid,
                                              uint64_t *out_jiffies,
                                              uint64_t *out_start_time) {
  char path[64] = {0};
  char stat_text[4096] = {0};
  const char *cursor = NULL;
  const char *left_parenthesis = NULL;
  const char *right_parenthesis = NULL;
  int32_t written = 0;
  uint64_t utime = 0;
  uint64_t stime = 0;
  uint64_t start_time = 0;

  if (pid <= 0 || NULL == out_jiffies || NULL == out_start_time) {
    return QST_E_INVALID_ARGUMENT;
  }
  written = snprintf(path, sizeof(path), "/proc/%d/stat", pid);
  if (written <= 0 || (uint64_t)written >= sizeof(path) ||
      qstReadLine(path, stat_text, sizeof(stat_text)) != QST_OK) {
    return QST_E_RESOURCE;
  }
  left_parenthesis = strchr(stat_text, '(');
  right_parenthesis = strrchr(stat_text, ')');
  if (NULL == left_parenthesis || NULL == right_parenthesis ||
      left_parenthesis >= right_parenthesis || right_parenthesis == stat_text ||
      right_parenthesis[1] == '\0') {
    return QST_E_RESOURCE;
  }
  cursor = right_parenthesis + 1;
  for (uint32_t field = 3; field <= 22; ++field) {
    char *end = NULL;
    unsigned long long value = 0;

    while (*cursor == ' ') {
      ++cursor;
    }
    if (*cursor == '\0') {
      return QST_E_RESOURCE;
    }
    if (field == 3 || (field != 14 && field != 15 && field != 22)) {
      while (*cursor != '\0' && *cursor != ' ') {
        ++cursor;
      }
      continue;
    }
    errno = 0;
    value = strtoull(cursor, &end, 10);
    if (*cursor == '-' || errno != 0 || end == cursor ||
        (*end != '\0' && *end != ' ')) {
      return QST_E_RESOURCE;
    }
    if (field == 14) {
      utime = (uint64_t)value;
    } else if (field == 15) {
      stime = (uint64_t)value;
    } else {
      start_time = (uint64_t)value;
    }
    cursor = end;
  }
  if (utime > UINT64_MAX - stime || start_time == 0) {
    return QST_E_RESOURCE;
  }
  *out_jiffies = utime + stime;
  *out_start_time = start_time;
  return QST_OK;
}

EQstCode qst_resource_read_proc_comm(int32_t pid, char *out_comm,
                                      uint64_t out_comm_bytes) {
  char path[64] = {0};
  int32_t written = 0;

  if (pid <= 0 || NULL == out_comm || out_comm_bytes < 2) {
    return QST_E_INVALID_ARGUMENT;
  }
  written = snprintf(path, sizeof(path), "/proc/%d/comm", pid);
  if (written <= 0 || (uint64_t)written >= sizeof(path) ||
      qstReadLine(path, out_comm, out_comm_bytes) != QST_OK ||
      out_comm[0] == '\0') {
    return QST_E_RESOURCE;
  }
  return QST_OK;
}

int32_t qst_resource_is_tool_role(uint32_t role) {
  return role >= QST_ROLE_LAUNCHER && role <= QST_ROLE_AI_WORKER &&
         role != QST_ROLE_TAOSD;
}

EQstCpuEfficiencyState qst_resource_evaluate_cpu_efficiency(
    uint64_t tool_jiffies, uint64_t taosd_jiffies, int32_t sampling_complete,
    double *out_ratio) {
  double ratio = 0.0;

  if (NULL != out_ratio) {
    *out_ratio = 0.0;
  }
  if (sampling_complete == 0 || taosd_jiffies == 0) {
    return QST_CPU_EFFICIENCY_INDETERMINATE;
  }
  ratio = (double)tool_jiffies / (double)taosd_jiffies;
  if (NULL != out_ratio) {
    *out_ratio = ratio;
  }
  return ratio <= 2.0 ? QST_CPU_EFFICIENCY_PASS : QST_CPU_EFFICIENCY_FAIL;
}

static EQstCode qstParseLimit(const char *text, uint64_t *out_value,
                              int32_t *out_unlimited) {
  char *end = NULL;
  unsigned long long value = 0;

  if (NULL == text || NULL == out_value || NULL == out_unlimited) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (strcmp(text, "max") == 0) {
    *out_value = UINT64_MAX;
    *out_unlimited = 1;
    return QST_OK;
  }
  if (*text == '-' || *text == '\0') {
    return QST_E_RESOURCE;
  }
  errno = 0;
  value = strtoull(text, &end, 10);
  if (errno != 0 || end == text || *end != '\0' || value == 0) {
    return QST_E_RESOURCE;
  }
  *out_value = (uint64_t)value;
  *out_unlimited = 0;
  return QST_OK;
}

static EQstCode qstReadCgroupLimit(const char *name, uint64_t *out_value,
                                   int32_t *out_unlimited) {
  char path[128] = {0};
  char text[128] = {0};
  int32_t written = 0;

  written = snprintf(path, sizeof(path), "/sys/fs/cgroup/%s", name);
  if (written < 0 || (uint64_t)written >= sizeof(path) ||
      qstReadLine(path, text, sizeof(text)) != QST_OK) {
    return QST_E_RESOURCE;
  }
  return qstParseLimit(text, out_value, out_unlimited);
}

static EQstCode qstReadCpuMax(uint64_t *out_quota, uint64_t *out_period,
                              int32_t *out_unlimited) {
  char text[128] = {0};
  char quota_text[64] = {0};
  char period_text[64] = {0};
  char *end = NULL;
  unsigned long long period = 0;

  if (NULL == out_quota || NULL == out_period || NULL == out_unlimited ||
      qstReadLine("/sys/fs/cgroup/cpu.max", text, sizeof(text)) != QST_OK ||
      sscanf(text, "%63s %63s", quota_text, period_text) != 2) {
    return QST_E_RESOURCE;
  }
  errno = 0;
  period = strtoull(period_text, &end, 10);
  if (errno != 0 || end == period_text || *end != '\0' || period == 0) {
    return QST_E_RESOURCE;
  }
  *out_period = (uint64_t)period;
  return qstParseLimit(quota_text, out_quota, out_unlimited);
}

static EQstCode qstReadMemAvailable(uint64_t *out_bytes) {
  char line[256] = {0};
  FILE *file = NULL;

  if (NULL == out_bytes) {
    return QST_E_INVALID_ARGUMENT;
  }
  file = fopen("/proc/meminfo", "r");
  if (NULL == file) {
    return QST_E_RESOURCE;
  }
  while (fgets(line, sizeof(line), file) != NULL) {
    unsigned long long kibibytes = 0;
    if (sscanf(line, "MemAvailable: %llu kB", &kibibytes) == 1) {
      if ((uint64_t)kibibytes > UINT64_MAX / 1024U) {
        (void)fclose(file);
        return QST_E_RESOURCE;
      }
      *out_bytes = (uint64_t)kibibytes * 1024U;
      if (fclose(file) != 0) {
        return QST_E_RESOURCE;
      }
      return QST_OK;
    }
  }
  (void)fclose(file);
  return QST_E_RESOURCE;
}

static EQstCode qstFilesystemFree(const char *path, uint64_t *out_bytes) {
  struct statvfs fs = {0};

  if (NULL == path || NULL == out_bytes || statvfs(path, &fs) != 0 ||
      fs.f_frsize == 0 || fs.f_bavail > UINT64_MAX / fs.f_frsize) {
    return QST_E_RESOURCE;
  }
  *out_bytes = (uint64_t)fs.f_bavail * fs.f_frsize;
  return QST_OK;
}

static EQstCode qstNowNs(uint64_t *out_ns) {
  struct timespec now = {0};

  if (NULL == out_ns || clock_gettime(CLOCK_REALTIME, &now) != 0 ||
      now.tv_sec < 0 || (uint64_t)now.tv_sec > UINT64_MAX / UINT64_C(1000000000)) {
    return QST_E_RESOURCE;
  }
  *out_ns = (uint64_t)now.tv_sec * UINT64_C(1000000000) + (uint64_t)now.tv_nsec;
  return QST_OK;
}

EQstCode qst_resource_capture(const char *result_dir, const char *logs_dir,
                              SQstResourceInputs *out_inputs) {
  struct rlimit limit = {0};
  uint64_t cgroup_limit = 0;
  uint64_t cgroup_high = 0;
  uint64_t cgroup_pids = 0;
  uint64_t quota = 0;
  uint64_t period = 0;
  long processors = 0;
  int32_t memory_unlimited = 0;
  int32_t high_unlimited = 0;
  int32_t pids_unlimited = 0;
  int32_t cpu_unlimited = 0;

  if (NULL == result_dir || NULL == logs_dir || NULL == out_inputs) {
    return QST_E_INVALID_ARGUMENT;
  }
  memset(out_inputs, 0, sizeof(*out_inputs));
  if (qstNowNs(&out_inputs->captured_at_ns) != QST_OK ||
      qstReadMemAvailable(&out_inputs->memory_available_bytes) != QST_OK ||
      qstFilesystemFree(result_dir, &out_inputs->shm_free_bytes) != QST_OK ||
      qstFilesystemFree(result_dir, &out_inputs->result_disk_free_bytes) != QST_OK ||
      qstFilesystemFree(logs_dir, &out_inputs->log_disk_free_bytes) != QST_OK) {
    return QST_E_RESOURCE;
  }
  out_inputs->source_flags = QST_RESOURCE_SOURCE_PROC | QST_RESOURCE_SOURCE_FILESYSTEM;

  if (qstReadCgroupLimit("memory.max", &cgroup_limit, &memory_unlimited) == QST_OK &&
      qstReadCgroupLimit("memory.high", &cgroup_high, &high_unlimited) == QST_OK) {
    out_inputs->source_flags |= QST_RESOURCE_SOURCE_CGROUP;
    out_inputs->memory_limit_bytes = memory_unlimited ? UINT64_MAX : cgroup_limit;
    if (!high_unlimited) {
      out_inputs->memory_limit_bytes = qstMinimum(out_inputs->memory_limit_bytes,
                                                  cgroup_high);
    }
  } else if (getrlimit(RLIMIT_AS, &limit) == 0 && limit.rlim_cur != RLIM_INFINITY &&
             (uintmax_t)limit.rlim_cur <= UINT64_MAX) {
    out_inputs->source_flags |= QST_RESOURCE_SOURCE_RLIMIT;
    out_inputs->memory_limit_bytes = (uint64_t)limit.rlim_cur;
  } else {
    out_inputs->memory_limit_bytes = out_inputs->memory_available_bytes;
  }

  if (qstReadCgroupLimit("pids.max", &cgroup_pids, &pids_unlimited) == QST_OK &&
      !pids_unlimited) {
    out_inputs->source_flags |= QST_RESOURCE_SOURCE_CGROUP;
    out_inputs->pid_limit = cgroup_pids;
  } else if (getrlimit(RLIMIT_NPROC, &limit) == 0 && limit.rlim_cur != RLIM_INFINITY &&
             (uintmax_t)limit.rlim_cur <= UINT64_MAX) {
    out_inputs->source_flags |= QST_RESOURCE_SOURCE_RLIMIT;
    out_inputs->pid_limit = (uint64_t)limit.rlim_cur;
  } else {
    return QST_E_RESOURCE;
  }

  processors = sysconf(_SC_NPROCESSORS_ONLN);
  if (processors <= 0) {
    return QST_E_RESOURCE;
  }
  if (qstReadCpuMax(&quota, &period, &cpu_unlimited) == QST_OK && !cpu_unlimited) {
    out_inputs->source_flags |= QST_RESOURCE_SOURCE_CGROUP;
    out_inputs->cpu_quota_micros = quota;
    out_inputs->cpu_period_micros = period;
  } else {
    if ((uint64_t)processors > UINT64_MAX / UINT64_C(100000)) {
      return QST_E_RESOURCE;
    }
    out_inputs->source_flags |= QST_RESOURCE_SOURCE_RLIMIT;
    out_inputs->cpu_quota_micros = (uint64_t)processors * UINT64_C(100000);
    out_inputs->cpu_period_micros = UINT64_C(100000);
  }
  if (getrlimit(RLIMIT_NOFILE, &limit) != 0 || limit.rlim_cur == RLIM_INFINITY ||
      (uintmax_t)limit.rlim_cur > UINT64_MAX || limit.rlim_cur == 0) {
    return QST_E_RESOURCE;
  }
  out_inputs->source_flags |= QST_RESOURCE_SOURCE_RLIMIT;
  out_inputs->fd_limit = (uint64_t)limit.rlim_cur;
  return QST_OK;
}

static EQstCode qstMinimumLayout(uint32_t process_slots, uint32_t task_entries,
                                 uint32_t ai_actions, uint32_t queue_capacity,
                                 uint32_t block_bytes, uint32_t block_count,
                                 uint64_t *out_bytes) {
  uint64_t offset = sizeof(SQstShmHeader);
  const uint64_t region_bytes[] = {
      (uint64_t)process_slots * sizeof(SQstProcessSlot),
      (uint64_t)task_entries * sizeof(SQstTaskEntry),
      (uint64_t)ai_actions * sizeof(SQstAiActionEntry),
      sizeof(SQstQueueDescriptor) + (uint64_t)queue_capacity * sizeof(SQstQueueSlot),
      sizeof(SQstQueueDescriptor) + (uint64_t)queue_capacity * sizeof(SQstQueueSlot),
      sizeof(SQstQueueDescriptor) + (uint64_t)queue_capacity * sizeof(SQstQueueSlot),
      sizeof(SQstQueueDescriptor) + (uint64_t)queue_capacity * sizeof(SQstQueueSlot),
      (uint64_t)block_bytes * block_count,
      sizeof(SQstGenerationState),
  };

  if (NULL == out_bytes) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (uint32_t index = 0; index < sizeof(region_bytes) / sizeof(region_bytes[0]); ++index) {
    if (qstAlign(offset, QST_CACHELINE_BYTES, &offset) != QST_OK ||
        qstAdd(offset, region_bytes[index], &offset) != QST_OK) {
      return QST_E_RESOURCE;
    }
  }
  return qstAlign(offset, 4096, out_bytes);
}

static EQstCode qstPowerOfTwoAtLeast(uint64_t value, uint32_t *out_result) {
  uint32_t result = 1;

  if (value == 0 || out_result == NULL) {
    return QST_E_INVALID_ARGUMENT;
  }
  while ((uint64_t)result < value) {
    if (result > UINT32_MAX / 2U) {
      return QST_E_RESOURCE;
    }
    result <<= 1U;
  }
  *out_result = result;
  return QST_OK;
}

static int32_t qstIsPowerOfTwo(uint32_t value) {
  return value > 1U && (value & (value - 1U)) == 0;
}

EQstCode qst_resource_calculate(const SQstResourceInputs *inputs,
                                SQstRunConfig *inout_config,
                                SQstResourceProfile *out_profile) {
  const char *test_pair_limit_text = NULL;
  uint64_t memory_budget = 0;
  uint64_t reserve = 0;
  uint64_t usable = 0;
  uint64_t cpu_pairs = 0;
  uint64_t pid_pairs = 0;
  uint64_t fd_pairs = 0;
  uint64_t memory_pairs = 0;
  uint64_t pairs = 0;
  uint64_t async_window = 0;
  uint64_t judge_by_memory = 0;
  uint64_t judge_by_fd = 0;
  uint64_t judge_workers = 0;
  uint64_t shm_budget = 0;
  uint64_t task_entries = 0;
  uint64_t queue_capacity = 0;
  uint64_t block_count = 0;
  uint64_t judge_runtime_bytes = 0;
  uint64_t effective_requested_pairs = 0;
  uint64_t effective_requested_judges = 0;
  uint32_t process_slots = 0;
  uint32_t queue_capacity_power = 0;
  uint32_t block_bytes = QST_RESOURCE_MIN_BLOCK_BYTES;
  uint32_t test_pair_limit = 0;
  const uint32_t requested_pairs = inout_config->execution_pair_count_override;
  const uint32_t requested_judges = inout_config->judge_worker_count_override;
  const uint32_t requested_queue_capacity =
      inout_config->queue_capacity_override;
  const uint32_t requested_async_window =
      inout_config->pair_async_window_cap;
  EQstCode code = QST_OK;

  if (NULL == inputs || NULL == inout_config || NULL == out_profile ||
      inputs->memory_limit_bytes == 0 || inputs->memory_available_bytes == 0 ||
      inputs->shm_free_bytes == 0 || inputs->result_disk_free_bytes == 0 ||
      inputs->log_disk_free_bytes == 0 || inputs->cpu_quota_micros == 0 ||
      inputs->cpu_period_micros == 0 || inputs->pid_limit == 0 ||
      inputs->fd_limit == 0 || inputs->captured_at_ns == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  memset(out_profile, 0, sizeof(*out_profile));
  out_profile->inputs = *inputs;
  memory_budget = qstMinimum(inputs->memory_limit_bytes, inputs->memory_available_bytes);
  reserve = qstMaximum((uint64_t)QST_RESOURCE_MIN_RESERVE_BYTES,
                       memory_budget / 10U);
  if (memory_budget <= reserve) {
    return QST_E_RESOURCE;
  }
  usable = memory_budget - reserve;
  if (inputs->cpu_quota_micros < inputs->cpu_period_micros ||
      inputs->pid_limit <= QST_RESOURCE_MANDATORY_PROCESS_COUNT ||
      inputs->fd_limit <= QST_RESOURCE_MANAGER_FD_RESERVE) {
    return QST_E_RESOURCE;
  }
  cpu_pairs = inputs->cpu_quota_micros / inputs->cpu_period_micros;
  pid_pairs = (inputs->pid_limit - QST_RESOURCE_MANDATORY_PROCESS_COUNT) /
              QST_RESOURCE_PAIR_PROCESS_COUNT;
  fd_pairs = (inputs->fd_limit - QST_RESOURCE_MANAGER_FD_RESERVE) /
             QST_RESOURCE_PAIR_FD_COUNT;
  code = qstAdd(sizeof(SQstProcessSlot) * QST_RESOURCE_PAIR_PROCESS_COUNT,
                sizeof(SQstTaskEntry) * 4U, &judge_runtime_bytes);
  if (code != QST_OK ||
      qstAdd(judge_runtime_bytes, (uint64_t)block_bytes * 4U,
             &judge_runtime_bytes) != QST_OK) {
    return QST_E_RESOURCE;
  }
  if (qstMultiply(judge_runtime_bytes, 2U, &memory_pairs) != QST_OK ||
      memory_pairs == 0) {
    return QST_E_RESOURCE;
  }
  memory_pairs = usable / memory_pairs;
  pairs = qstMinimum(qstMinimum(cpu_pairs, pid_pairs),
                     qstMinimum(fd_pairs, memory_pairs));
  pairs = qstMinimum(pairs, QST_RESOURCE_MAX_AUTOMATIC_EXECUTION_PAIRS);
  if (inout_config->statement_limit != 0) {
    pairs = qstMinimum(pairs, inout_config->statement_limit);
  }
  if (pairs == 0 || pairs > UINT32_MAX) {
    return QST_E_RESOURCE;
  }
  judge_by_memory = usable / judge_runtime_bytes;
  judge_by_fd = (inputs->fd_limit - QST_RESOURCE_MANAGER_FD_RESERVE) /
                QST_RESOURCE_JUDGE_FD_COUNT;
  judge_workers = qstMinimum(
      cpu_pairs, qstMinimum(QST_RESOURCE_MAX_JUDGE_WORKERS,
                             qstMinimum(QST_RESOURCE_MAX_RESULT_STORE_WRITERS,
                                        qstMinimum(judge_by_memory,
                                                   judge_by_fd))));
  test_pair_limit_text = getenv("QST_TEST_EXECUTION_PAIR_COUNT");
  if (NULL != test_pair_limit_text && test_pair_limit_text[0] != '\0') {
    char *end = NULL;
    const unsigned long parsed = strtoul(test_pair_limit_text, &end, 10);

    if (end == test_pair_limit_text || *end != '\0' || parsed == 0 ||
        parsed > QST_RESOURCE_MAX_EXECUTION_PAIRS) {
      return QST_E_INVALID_ARGUMENT;
    }
    test_pair_limit = (uint32_t)parsed;
    pairs = qstMinimum(pairs, test_pair_limit);
    judge_workers = qstMinimum(judge_workers, test_pair_limit);
  }
  if (inout_config->statement_limit != 0) {
    judge_workers = qstMinimum(judge_workers, inout_config->statement_limit);
  }
  if (requested_pairs != 0) {
    const uint64_t explicit_pair_capacity = qstMinimum(
        qstMinimum(pid_pairs, fd_pairs),
        qstMinimum(memory_pairs, QST_RESOURCE_MAX_EXECUTION_PAIRS));

    if (requested_pairs > explicit_pair_capacity) {
      return QST_E_RESOURCE;
    }
    effective_requested_pairs = requested_pairs;
    if (inout_config->statement_limit != 0) {
      effective_requested_pairs = qstMinimum(
          effective_requested_pairs, inout_config->statement_limit);
    }
    pairs = effective_requested_pairs;
  }
  if (requested_judges != 0) {
    effective_requested_judges = requested_judges;
    if (inout_config->statement_limit != 0) {
      effective_requested_judges = qstMinimum(
          effective_requested_judges, inout_config->statement_limit);
    }
    if (effective_requested_judges > judge_workers) {
      (void)fprintf(stderr,
                    "taosQueryTest: requested judge workers exceed resource "
                    "limit requested=%llu available=%llu pairs=%llu "
                    "test_pair_limit=%u\n",
                    (unsigned long long)effective_requested_judges,
                    (unsigned long long)judge_workers,
                    (unsigned long long)pairs, test_pair_limit);
      return QST_E_RESOURCE;
    }
    judge_workers = effective_requested_judges;
  }
  if (cpu_pairs > UINT64_MAX / QST_RESOURCE_ASYNC_REQUESTS_PER_CPU) {
    return QST_E_RESOURCE;
  }
  async_window = (cpu_pairs * QST_RESOURCE_ASYNC_REQUESTS_PER_CPU) / pairs;
  if (async_window == 0) {
    async_window = 1;
  }
  async_window = qstMinimum(async_window, QST_PAIR_MAX_INFLIGHT);
  if (requested_async_window != 0) {
    async_window = qstMinimum(async_window, requested_async_window);
  }
  if (judge_workers == 0 || judge_workers > UINT32_MAX ||
      pairs > (UINT32_MAX - QST_RESOURCE_MANDATORY_PROCESS_COUNT) /
                  QST_RESOURCE_PAIR_PROCESS_COUNT) {
    return QST_E_RESOURCE;
  }
  process_slots = QST_RESOURCE_MANDATORY_PROCESS_COUNT +
                  (uint32_t)pairs * QST_RESOURCE_PAIR_PROCESS_COUNT +
                  (uint32_t)judge_workers + 1U;
  code = qstPowerOfTwoAtLeast(pairs * async_window, &queue_capacity_power);
  if (code != QST_OK) {
    return code;
  }
  queue_capacity = queue_capacity_power;
  if (queue_capacity < QST_RESOURCE_MIN_QUEUE_CAPACITY) {
    queue_capacity = QST_RESOURCE_MIN_QUEUE_CAPACITY;
  }
  if (requested_queue_capacity != 0) {
    if (!qstIsPowerOfTwo(requested_queue_capacity)) {
      return QST_E_INVALID_ARGUMENT;
    }
    queue_capacity = requested_queue_capacity;
  }
  if (queue_capacity > UINT32_MAX) {
    return QST_E_RESOURCE;
  }
  /* Reserve ledger entries for each buffered item and both phases of every
   * statement that a pair may have in flight while the queue is full. */
  if (pairs > (UINT64_MAX - queue_capacity) ||
      pairs > UINT64_MAX / async_window / 2U) {
    return QST_E_RESOURCE;
  }
  task_entries = qstMaximum(
      pairs * async_window * 2U, queue_capacity + pairs);
  if (task_entries < 32U) {
    task_entries = 32U;
  }
  if (task_entries > UINT32_MAX) {
    return QST_E_RESOURCE;
  }
  shm_budget = qstMinimum(usable / 2U, inputs->shm_free_bytes / 2U);
  if (shm_budget < 4096U) {
    return QST_E_RESOURCE;
  }
  code = qstMinimumLayout(process_slots, (uint32_t)task_entries,
                          QST_RESOURCE_MIN_AI_ACTIONS, (uint32_t)queue_capacity,
                          block_bytes, QST_RESOURCE_MIN_BLOCK_COUNT,
                          &out_profile->minimum_layout_bytes);
  if (code != QST_OK || shm_budget < out_profile->minimum_layout_bytes) {
    return QST_E_RESOURCE;
  }
  if (task_entries > (UINT64_MAX - QST_RESOURCE_MIN_BLOCK_COUNT -
                      queue_capacity * 4U - QST_RESOURCE_MIN_AI_ACTIONS * 4U) /
                         4U) {
    return QST_E_RESOURCE;
  }
  /* Four block chains cover SQL, binding, final result, and diagnostics. */
  block_count = task_entries * 4U + queue_capacity * 4U +
                QST_RESOURCE_MIN_AI_ACTIONS * 4U + QST_RESOURCE_MIN_BLOCK_COUNT;
  if (block_count > UINT32_MAX) {
    return QST_E_RESOURCE;
  }
  code = qstMinimumLayout(process_slots, (uint32_t)task_entries,
                          QST_RESOURCE_MIN_AI_ACTIONS, (uint32_t)queue_capacity,
                          block_bytes, (uint32_t)block_count,
                          &out_profile->working_set_layout_bytes);
  if (code != QST_OK || out_profile->working_set_layout_bytes > shm_budget) {
    return QST_E_RESOURCE;
  }
  inout_config->shm_bytes = out_profile->working_set_layout_bytes;
  inout_config->process_slot_count = process_slots;
  inout_config->task_entry_count = (uint32_t)task_entries;
  inout_config->ai_action_entry_count = QST_RESOURCE_MIN_AI_ACTIONS;
  inout_config->queue_capacity = (uint32_t)queue_capacity;
  inout_config->block_bytes = block_bytes;
  inout_config->block_count = (uint32_t)block_count;
  inout_config->execution_pair_count = (uint32_t)pairs;
  inout_config->pair_async_window = (uint32_t)async_window;
  inout_config->judge_worker_count = (uint32_t)judge_workers;
  out_profile->memory_reserve_bytes = reserve;
  out_profile->memory_usable_bytes = usable;
  out_profile->pending_quota_bytes = inputs->result_disk_free_bytes / 2U;
  out_profile->log_rotation_bytes = inputs->log_disk_free_bytes / 10U;
  inout_config->pending_quota_bytes = out_profile->pending_quota_bytes;
  inout_config->log_rotation_bytes = out_profile->log_rotation_bytes;
  out_profile->execution_pair_count = (uint32_t)pairs;
  out_profile->pair_async_window = (uint32_t)async_window;
  out_profile->judge_worker_count = (uint32_t)judge_workers;
  out_profile->mandatory_process_count = QST_RESOURCE_MANDATORY_PROCESS_COUNT;
  out_profile->pair_process_count = QST_RESOURCE_PAIR_PROCESS_COUNT;
  out_profile->pair_fd_count = QST_RESOURCE_PAIR_FD_COUNT;
  out_profile->judge_worker_fd_count = QST_RESOURCE_JUDGE_FD_COUNT;
  return QST_OK;
}
