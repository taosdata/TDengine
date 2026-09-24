/* Runtime resource sampling and frozen capacity calculation. */

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_RESOURCE_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_RESOURCE_H_

#include <stdint.h>

#include "qst_runner.h"

enum {
  QST_RESOURCE_SOURCE_CGROUP = 1U << 0,
  QST_RESOURCE_SOURCE_PROC = 1U << 1,
  QST_RESOURCE_SOURCE_RLIMIT = 1U << 2,
  QST_RESOURCE_SOURCE_FILESYSTEM = 1U << 3,
};

/* Inputs are public to the unit-test target only; production captures them once. */
typedef struct {
  uint64_t memory_limit_bytes;
  uint64_t memory_available_bytes;
  uint64_t shm_free_bytes;
  uint64_t result_disk_free_bytes;
  uint64_t log_disk_free_bytes;
  uint64_t cpu_quota_micros;
  uint64_t cpu_period_micros;
  uint64_t pid_limit;
  uint64_t fd_limit;
  uint64_t captured_at_ns;
  uint32_t source_flags;
} SQstResourceInputs;

typedef struct {
  SQstResourceInputs inputs;
  uint64_t memory_reserve_bytes;
  uint64_t memory_usable_bytes;
  uint64_t minimum_layout_bytes;
  uint64_t working_set_layout_bytes;
  uint64_t pending_quota_bytes;
  uint64_t log_rotation_bytes;
  uint32_t execution_pair_count;
  uint32_t pair_async_window;
  uint32_t judge_worker_count;
  uint32_t mandatory_process_count;
  uint32_t pair_process_count;
  uint32_t pair_fd_count;
  uint32_t judge_worker_fd_count;
} SQstResourceProfile;

typedef enum {
  QST_CPU_EFFICIENCY_INDETERMINATE = 0,
  QST_CPU_EFFICIENCY_PASS = 1,
  QST_CPU_EFFICIENCY_FAIL = 2,
} EQstCpuEfficiencyState;

EQstCode qst_resource_capture(const char *result_dir, const char *logs_dir,
                              SQstResourceInputs *out_inputs);
EQstCode qst_resource_calculate(const SQstResourceInputs *inputs,
                                SQstRunConfig *inout_config,
                                SQstResourceProfile *out_profile);
/* Parses fields 14 and 15 from Linux /proc/<pid>/stat without assuming that
 * the parenthesized command name has no spaces or closing parentheses. */
EQstCode qst_resource_parse_proc_stat_cpu_jiffies(const char *stat_text,
                                                  uint64_t *out_jiffies);
EQstCode qst_resource_read_proc_cpu_jiffies(int32_t pid,
                                            uint64_t *out_jiffies);
/* Reads CPU jiffies and field 22 start time together so an externally
 * declared private taosd cannot be silently replaced by a reused PID. */
EQstCode qst_resource_read_proc_cpu_identity(int32_t pid,
                                             uint64_t *out_jiffies,
                                             uint64_t *out_start_time);
/* Reads the kernel command name without accepting an arbitrary process as a
 * declared private taosd. */
EQstCode qst_resource_read_proc_comm(int32_t pid, char *out_comm,
                                     uint64_t out_comm_bytes);
/* The CPU efficiency numerator excludes every role attributed to the taosd
 * side: private taosd, standalone taosc, and the pair supervisor that hosts
 * the in-process native taosc client. */
int32_t qst_resource_is_tool_role(uint32_t role);
EQstCpuEfficiencyState qst_resource_evaluate_cpu_efficiency(
    uint64_t tool_jiffies, uint64_t taosd_jiffies, int32_t sampling_complete,
    double *out_ratio);

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_RESOURCE_H_
