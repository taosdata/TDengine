/* Configuration-parser regression tests invoked by qstCoreTest. */

#define _POSIX_C_SOURCE 200809L

#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/file.h>
#include <sys/inotify.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "qst_config.h"
#include "qst_resource.h"
#include "qst_sha256.h"
#include "qst_task.h"

static const char *qstTestTemporaryRoot(void) {
  const char *path = getenv("QST_RUNTIME_TEMP_ROOT");

  if (path != NULL && path[0] == '/') {
    return path;
  }
  path = getenv("TMPDIR");
  if (path != NULL && path[0] == '/') {
    return path;
  }
  return "/tmp";
}

void qstTestConfigParsing(void) {
  char root[QST_PATH_BYTES] = {0};
  char yaml_path[QST_PATH_BYTES] = {0};
  char error[1024] = {0};
  FILE *yaml = NULL;
  SQstCliConfig config = {0};
  char *valid_argv[] = {"taosQueryTest", "-t", "random", "-n", "3",
                        "-q", "td,pg", NULL};
  char *deduplicated_argv[] = {"taosQueryTest", "-t", "random", "-n",
                                "3", "-q", "td,pg,td,pg", NULL};
  char *invalid_argv[] = {"taosQueryTest", "-t", "full", "-n", "1", NULL};
  char *invalid_separator_argv[] = {"taosQueryTest", "-q", "td,,pg", NULL};
  char *invalid_all_argv[] = {"taosQueryTest", "-q", "all,td", NULL};
  char *correctness_argv[] = {"taosQueryTest", "-f", "correctness", "-n", "3",
                              NULL};
  char *generate_full_argv[] = {"taosQueryTest", "-t", "full", "-g",
                                "/tmp/generated-full.jsonl", NULL};
  char *generate_random_argv[] = {"taosQueryTest", "-n", "3", "-g",
                                  "/tmp/generated-random.jsonl", NULL};
  char *generate_random_unbounded_argv[] = {
      "taosQueryTest", "-g", "/tmp/generated-random.jsonl", NULL};
  char *generate_ai_argv[] = {"taosQueryTest", "-v", "1", "-g",
                              "/tmp/generated-ai.jsonl", NULL};

  assert(snprintf(root, sizeof(root), "%s/taosQueryTest-config-XXXXXX",
                  qstTestTemporaryRoot()) > 0);
  assert(mkdtemp(root) != NULL);
  assert(snprintf(yaml_path, sizeof(yaml_path), "%s/taosQueryTest.yaml", root) >
         0);
  yaml = fopen(yaml_path, "w");
  assert(yaml != NULL);
  assert(fputs("version: 1\n"
               "execution:\n"
               "  statement_timeout_seconds: 120\n"
               "  statement_retry_count: 2\n"
               "  pair_count: 1\n"
               "  judge_worker_count: 1\n"
               "  queue_capacity: 4\n"
               "  pair_async_window: 10\n"
               "  random_builder_count: 8\n"
               "  random_seed: 20260902\n"
               "ai:\n"
               "  provider: codex\n"
               "  command: codex\n"
               "  working_directory: \"\"\n",
               yaml) >= 0);
  assert(fclose(yaml) == 0);
  assert(qst_config_parse(7, valid_argv, root, &config, error, sizeof(error)) ==
         QST_OK);
  assert(config.run_config.mode == QST_MODE_RANDOM);
  assert(config.run_config.statement_limit == 3);
  assert(config.run_config.target_selection_bits == ((1U << 0) | (1U << 2)));
  assert(config.run_config.statement_timeout_seconds == 120);
  assert(config.run_config.statement_retry_count == 2);
  assert(config.run_config.execution_pair_count_override == 1);
  assert(config.run_config.judge_worker_count_override == 1);
  assert(config.run_config.queue_capacity_override == 4);
  assert(config.run_config.pair_async_window_cap == 10);
  assert(config.run_config.random_builder_count_override == 8);
  assert(config.run_config.random_seed == UINT64_C(20260902));
  assert(qst_config_parse(7, valid_argv, QST_TOOL_ROOT, &config, error,
                          sizeof(error)) == QST_OK);
  assert(config.run_config.statement_timeout_seconds == 300);
  assert(config.run_config.execution_pair_count_override == 16);
  assert(config.run_config.judge_worker_count_override == 14);
  assert(config.run_config.queue_capacity_override == 4096);
  assert(config.run_config.pair_async_window_cap == 96);
  assert(config.run_config.random_builder_count_override == 4);
  assert(QST_PAIR_MAX_INFLIGHT == 128U);
  assert(qst_config_parse(7, deduplicated_argv, root, &config, error,
                          sizeof(error)) == QST_OK);
  assert(config.run_config.target_selection_bits == ((1U << 0) | (1U << 2)));
  assert(qst_config_parse(5, invalid_argv, root, &config, error, sizeof(error)) ==
         QST_E_INVALID_ARGUMENT);
  {
    char *full_duration_argv[] = {"taosQueryTest", "-t", "full", "-m", "1",
                                  NULL};

    assert(qst_config_parse(5, full_duration_argv, root, &config, error,
                            sizeof(error)) == QST_OK);
    assert(config.run_config.mode == QST_MODE_FULL);
    assert(config.run_config.duration_limit_minutes == 1);
  }
  assert(qst_config_parse(3, invalid_separator_argv, root, &config, error,
                          sizeof(error)) == QST_E_INVALID_ARGUMENT);
  assert(qst_config_parse(3, invalid_all_argv, root, &config, error,
                          sizeof(error)) == QST_E_INVALID_ARGUMENT);
  assert(qst_config_parse(5, correctness_argv, root, &config, error,
                          sizeof(error)) == QST_OK);
  assert(config.run_config.focus == QST_FOCUS_CORRECTNESS);
  assert(qst_config_parse(5, generate_full_argv, root, &config, error,
                          sizeof(error)) == QST_OK);
  assert(config.run_config.mode == QST_MODE_FULL);
  assert(strcmp(config.run_config.generate_output_path,
                "/tmp/generated-full.jsonl") == 0);
  assert(qst_config_parse(5, generate_random_argv, root, &config, error,
                          sizeof(error)) == QST_OK);
  assert(config.run_config.mode == QST_MODE_RANDOM);
  assert(config.run_config.statement_limit == 3);
  assert(strcmp(config.run_config.generate_output_path,
                "/tmp/generated-random.jsonl") == 0);
  assert(qst_config_parse(3, generate_random_unbounded_argv, root, &config,
                          error, sizeof(error)) == QST_E_INVALID_ARGUMENT);
  assert(qst_config_parse(5, generate_ai_argv, root, &config, error,
                          sizeof(error)) == QST_E_INVALID_ARGUMENT);
  assert(unlink(yaml_path) == 0);
  assert(rmdir(root) == 0);
}

void qstTestResourceCapacityCalculation(void) {
  uint64_t jiffies = 0;
  uint64_t start_time = 0;
  double ratio = 0.0;
  SQstResourceInputs inputs = {
      .memory_limit_bytes = UINT64_C(1024) * 1024 * 1024,
      .memory_available_bytes = UINT64_C(1024) * 1024 * 1024,
      .shm_free_bytes = UINT64_C(512) * 1024 * 1024,
      .result_disk_free_bytes = UINT64_C(1024) * 1024 * 1024,
      .log_disk_free_bytes = UINT64_C(1024) * 1024 * 1024,
      .cpu_quota_micros = 200000,
      .cpu_period_micros = 100000,
      .pid_limit = 32,
      .fd_limit = 128,
      .captured_at_ns = 1,
      .source_flags = QST_RESOURCE_SOURCE_CGROUP |
                      QST_RESOURCE_SOURCE_PROC |
                      QST_RESOURCE_SOURCE_FILESYSTEM,
  };
  SQstResourceProfile profile = {0};
  SQstRunConfig config = {0};

  assert(qst_resource_parse_proc_stat_cpu_jiffies(
             "42 (pair worker) S 1 2 3 4 5 6 7 8 9 10 101 202", &jiffies) ==
         QST_OK);
  assert(jiffies == 303U);
  assert(qst_resource_parse_proc_stat_cpu_jiffies(
             "42 (worker ) name) S 1 2 3 4 5 6 7 8 9 10 9 11", &jiffies) ==
         QST_OK);
  assert(jiffies == 20U);
  assert(qst_resource_parse_proc_stat_cpu_jiffies("42 (worker S 1", &jiffies) ==
         QST_E_RESOURCE);
  assert(qst_resource_parse_proc_stat_cpu_jiffies(
             "42 worker) S 1 2 3 4 5 6 7 8 9 10 9 11", &jiffies) ==
         QST_E_RESOURCE);
  assert(qst_resource_read_proc_cpu_jiffies((int32_t)getpid(), &jiffies) ==
         QST_OK);
  assert(qst_resource_read_proc_cpu_identity(
             (int32_t)getpid(), &jiffies, &start_time) == QST_OK);
  assert(start_time != 0);
  assert(qst_resource_is_tool_role(QST_ROLE_MAIN) != 0);
  assert(qst_resource_is_tool_role(QST_ROLE_TAOSD) == 0);
  assert(qst_resource_is_tool_role(QST_ROLE_TAOSC) != 0);
  assert(qst_resource_is_tool_role(QST_ROLE_PAIR_SUPERVISOR) != 0);
  assert(qst_resource_is_tool_role(0) == 0);
  assert(qst_resource_evaluate_cpu_efficiency(1, 0, 1, NULL) ==
         QST_CPU_EFFICIENCY_INDETERMINATE);
  assert(qst_resource_evaluate_cpu_efficiency(200, 100, 1, &ratio) ==
         QST_CPU_EFFICIENCY_PASS);
  assert(ratio == 2.0);
  assert(qst_resource_evaluate_cpu_efficiency(201, 100, 1, &ratio) ==
         QST_CPU_EFFICIENCY_FAIL);
  assert(qst_resource_evaluate_cpu_efficiency(100, 100, 0, &ratio) ==
         QST_CPU_EFFICIENCY_INDETERMINATE);

  assert(qst_resource_calculate(&inputs, &config, &profile) == QST_OK);
  assert(profile.execution_pair_count == 2);
  assert(profile.pair_async_window == 48);
  assert(profile.judge_worker_count == 2);
  assert(config.process_slot_count == 14);
  assert(config.queue_capacity == 128);
  assert(config.task_entry_count == 192);
  assert(config.block_bytes == 4096);
  assert(config.block_count > 0);
  assert(config.shm_bytes == profile.working_set_layout_bytes);
  assert(config.shm_bytes >= profile.minimum_layout_bytes);
  assert(config.shm_bytes <= inputs.shm_free_bytes / 2);
  assert(profile.pending_quota_bytes == inputs.result_disk_free_bytes / 2);
  assert(profile.log_rotation_bytes == inputs.log_disk_free_bytes / 10);

  /* The fixture must leave the generator's downstream reserve intact even
   * when the caller asks for only two statements. */
  config.statement_limit = 2;
  assert(qst_resource_calculate(&inputs, &config, &profile) == QST_OK);
  assert(config.block_count >=
         4U * (config.task_entry_count + config.queue_capacity));

  /* A bounded run must not create more execution pairs than it can use. */
  config.statement_limit = 1;
  inputs.cpu_quota_micros = 1600000;
  inputs.pid_limit = 512;
  inputs.fd_limit = 4096;
  assert(qst_resource_calculate(&inputs, &config, &profile) == QST_OK);
  assert(profile.execution_pair_count == 1);
  assert(profile.pair_async_window == QST_PAIR_MAX_INFLIGHT);
  assert(profile.judge_worker_count == 1);

  /* Each pair keeps one native connection; the resource profile distributes
   * a bounded request budget between the isolated pairs. */
  config.statement_limit = 0;
  assert(qst_resource_calculate(&inputs, &config, &profile) == QST_OK);
  assert(profile.execution_pair_count == 16);
  assert(profile.pair_async_window == 48);
  assert(profile.judge_worker_count == 16);
  assert(config.process_slot_count == 70);
  assert(config.task_entry_count == 1536);
  assert(config.queue_capacity == 1024);

  /* Result-store writers remain bounded by the independent result-store
   * connection cap even when the host exposes more CPU. */
  inputs.cpu_quota_micros = 4400000;
  assert(qst_resource_calculate(&inputs, &config, &profile) == QST_OK);
  assert(profile.execution_pair_count == 16);
  assert(profile.pair_async_window == QST_PAIR_MAX_INFLIGHT);
  assert(profile.judge_worker_count == 16);
  assert(config.process_slot_count == 70);

  /* Explicit pair and queue overrides do not change the native connection
   * count. The async cap can only lower the resource-derived window. */
  config.execution_pair_count_override = 16;
  config.judge_worker_count_override = 14;
  config.queue_capacity_override = 4096;
  assert(qst_resource_calculate(&inputs, &config, &profile) == QST_OK);
  assert(profile.execution_pair_count == 16);
  assert(profile.pair_async_window == QST_PAIR_MAX_INFLIGHT);
  assert(profile.judge_worker_count == 14);
  assert(config.process_slot_count == 68);
  assert(config.task_entry_count == 4112);
  assert(config.queue_capacity == 4096);

  /* A small bounded run must not reject the package defaults merely because
   * their steady-state pair and judge caps exceed the requested statement count. */
  config.statement_limit = 4;
  assert(qst_resource_calculate(&inputs, &config, &profile) == QST_OK);
  assert(profile.execution_pair_count == 4);
  assert(profile.judge_worker_count == 4);
  assert(config.execution_pair_count == 4);
  assert(config.judge_worker_count == 4);
  config.statement_limit = 0;

  /* Automatic execute capacity must cover the complete single-pair async
   * batch; rounding down from ten requests to eight forces avoidable FINAL
   * queue stalls. */
  config.execution_pair_count_override = 1;
  config.judge_worker_count_override = 1;
  config.queue_capacity_override = 0;
  config.pair_async_window_cap = 10;
  assert(qst_resource_calculate(&inputs, &config, &profile) == QST_OK);
  assert(profile.execution_pair_count == 1);
  assert(profile.pair_async_window == 10);
  assert(config.queue_capacity == 16);

  config.execution_pair_count_override = 1;
  config.judge_worker_count_override = 1;
  config.queue_capacity_override = 64;
  config.pair_async_window_cap = 10;
  assert(qst_resource_calculate(&inputs, &config, &profile) == QST_OK);
  assert(profile.execution_pair_count == 1);
  assert(profile.pair_async_window == 10);
  assert(profile.judge_worker_count == 1);
  assert(config.queue_capacity == 64);
  assert(config.task_entry_count >= 65);
  inputs.cpu_quota_micros = 4400000;
  inputs.pid_limit = 4096;
  inputs.fd_limit = 16384;
  inputs.memory_limit_bytes = UINT64_C(8) * 1024 * 1024 * 1024;
  inputs.memory_available_bytes = UINT64_C(8) * 1024 * 1024 * 1024;
  inputs.shm_free_bytes = UINT64_C(4) * 1024 * 1024 * 1024;
  config.execution_pair_count_override = 512;
  config.judge_worker_count_override = 1;
  config.pair_async_window_cap = 0;
  assert(qst_resource_calculate(&inputs, &config, &profile) == QST_OK);
  assert(profile.execution_pair_count == 512);
  assert(profile.pair_async_window == 4);
  assert(config.process_slot_count == 1543);
  assert(config.task_entry_count == 4096);
  config.execution_pair_count_override = 1025;
  assert(qst_resource_calculate(&inputs, &config, &profile) == QST_E_RESOURCE);
  config.execution_pair_count_override = 0;
  config.judge_worker_count_override = 0;
  config.queue_capacity_override = 0;
  config.pair_async_window_cap = 0;

  assert(setenv("QST_TEST_EXECUTION_PAIR_COUNT", "1", 1) == 0);
  assert(qst_resource_calculate(&inputs, &config, &profile) == QST_OK);
  assert(profile.execution_pair_count == 1);
  assert(profile.pair_async_window == QST_PAIR_MAX_INFLIGHT);
  assert(profile.judge_worker_count == 1);
  config.execution_pair_count_override = 1;
  config.judge_worker_count_override = 2;
  assert(qst_resource_calculate(&inputs, &config, &profile) == QST_E_RESOURCE);
  config.execution_pair_count_override = 0;
  config.judge_worker_count_override = 0;
  assert(setenv("QST_TEST_EXECUTION_PAIR_COUNT", "0", 1) == 0);
  assert(qst_resource_calculate(&inputs, &config, &profile) ==
         QST_E_INVALID_ARGUMENT);
  assert(setenv("QST_TEST_EXECUTION_PAIR_COUNT", "invalid", 1) == 0);
  assert(qst_resource_calculate(&inputs, &config, &profile) ==
         QST_E_INVALID_ARGUMENT);
  assert(setenv("QST_TEST_EXECUTION_PAIR_COUNT", "1025", 1) == 0);
  assert(qst_resource_calculate(&inputs, &config, &profile) ==
         QST_E_INVALID_ARGUMENT);
  assert(unsetenv("QST_TEST_EXECUTION_PAIR_COUNT") == 0);

  inputs.cpu_quota_micros = 99999;
  assert(qst_resource_calculate(&inputs, &config, &profile) == QST_E_RESOURCE);
  inputs.cpu_quota_micros = 200000;
  inputs.shm_free_bytes = 4096;
  assert(qst_resource_calculate(&inputs, &config, &profile) == QST_E_RESOURCE);
}

void qstTestSha256(void) {
  static const uint8_t expected[32] = {
      0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea,
      0x41, 0x41, 0x40, 0xde, 0x5d, 0xae, 0x22, 0x23,
      0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c,
      0xb4, 0x10, 0xff, 0x61, 0xf2, 0x00, 0x15, 0xad,
  };
  static const uint8_t full_block_expected[32] = {
      0xff, 0xe0, 0x54, 0xfe, 0x7a, 0xe0, 0xcb, 0x6d,
      0xc6, 0x5c, 0x3a, 0xf9, 0xb6, 0x1d, 0x52, 0x09,
      0xf4, 0x39, 0x85, 0x1d, 0xb4, 0x3d, 0x0b, 0xa5,
      0x99, 0x73, 0x37, 0xdf, 0x15, 0x46, 0x68, 0xeb,
  };
  static const uint8_t mixed_blocks_expected[32] = {
      0x7c, 0xee, 0x24, 0x62, 0x8d, 0x29, 0x0c, 0x16,
      0x18, 0x35, 0x32, 0x71, 0x6c, 0xc5, 0xa8, 0xa8,
      0x89, 0xbc, 0x95, 0x1b, 0x4b, 0x0a, 0x15, 0x07,
      0xc3, 0x2b, 0x8e, 0x29, 0xce, 0xe0, 0x10, 0x52,
  };
  SQstSha256 context = {0};
  uint8_t blocks[192] = {0};
  uint8_t digest[32] = {0};

  qst_sha256_init(&context);
  qst_sha256_update(&context, (const uint8_t *)"a", 1);
  qst_sha256_update(&context, (const uint8_t *)"bc", 2);
  qst_sha256_final(&context, digest);
  assert(memcmp(digest, expected, sizeof(expected)) == 0);

  memset(blocks, 'a', sizeof(blocks));
  qst_sha256_init(&context);
  qst_sha256_update(&context, blocks, 64);
  qst_sha256_final(&context, digest);
  assert(memcmp(digest, full_block_expected, sizeof(digest)) == 0);

  qst_sha256_init(&context);
  qst_sha256_update(&context, blocks, 1);
  qst_sha256_update(&context, blocks + 1, 63);
  qst_sha256_update(&context, blocks + 64, 128);
  qst_sha256_final(&context, digest);
  assert(memcmp(digest, mixed_blocks_expected, sizeof(digest)) == 0);
}

static void qstReadRunId(const char *root, uint64_t *out_run_id) {
  char path[QST_PATH_BYTES] = {0};
  char text[64] = {0};
  char *end = NULL;
  FILE *file = NULL;

  assert(snprintf(path, sizeof(path), "%s/results/latest_run_id", root) > 0);
  file = fopen(path, "r");
  assert(file != NULL);
  assert(fgets(text, sizeof(text), file) != NULL);
  assert(fclose(file) == 0);
  errno = 0;
  *out_run_id = strtoull(text, &end, 10);
  assert(errno == 0 && end != text && *end == '\n');
}

static void qstWriteFixtureFile(const char *path, const char *contents) {
  FILE *file = fopen(path, "w");

  assert(file != NULL);
  assert(fputs(contents, file) >= 0);
  assert(fclose(file) == 0);
}

static void qstCreateDatasetAssets(const char *root) {
  char dataset_dir[QST_PATH_BYTES] = {0};
  char path[QST_PATH_BYTES] = {0};

  assert(snprintf(dataset_dir, sizeof(dataset_dir), "%s/dataset", root) > 0);
  assert(mkdir(dataset_dir, 0750) == 0);
  assert(snprintf(path, sizeof(path), "%s/taosbenchmark.json", dataset_dir) > 0);
  qstWriteFixtureFile(path, "{\"version\":\"unit\"}\n");
  assert(snprintf(path, sizeof(path), "%s/dependency-lock.json", root) > 0);
  qstWriteFixtureFile(
      path,
      "{\"format_version\":1,\"dependencies\":[{\"name\":\"unit\","
      "\"version\":\"1.0.0\",\"source\":\"unit test\"}]}\n");
}

static void qstRemoveDirectoryTree(const char *path) {
  DIR *directory = NULL;
  struct dirent *entry = NULL;

  assert(path != NULL);
  directory = opendir(path);
  assert(directory != NULL);
  while ((entry = readdir(directory)) != NULL) {
    char child[QST_PATH_BYTES] = {0};
    struct stat status = {0};

    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    assert(snprintf(child, sizeof(child), "%s/%s", path, entry->d_name) > 0);
    assert(lstat(child, &status) == 0);
    if (S_ISDIR(status.st_mode)) {
      qstRemoveDirectoryTree(child);
    } else {
      assert(unlink(child) == 0);
    }
  }
  assert(closedir(directory) == 0);
  assert(rmdir(path) == 0);
}

/* Waits for the launcher to durably publish the run id without timing polling. */
static void qstWaitForInotifyFile(int32_t inotify_fd, const char *name) {
  struct pollfd event = {0};
  uint8_t buffer[4096] = {0};

  assert(inotify_fd >= 0 && name != NULL && name[0] != '\0');
  event.fd = inotify_fd;
  event.events = POLLIN;
  for (;;) {
    ssize_t bytes = 0;

    assert(poll(&event, 1, 30000) == 1);
    bytes = read(inotify_fd, buffer, sizeof(buffer));
    assert(bytes > 0);
    for (ssize_t offset = 0; offset < bytes;) {
      const struct inotify_event *notice =
          (const struct inotify_event *)(buffer + offset);

      assert((uint64_t)offset + sizeof(*notice) + notice->len <=
             (uint64_t)bytes);
      if ((notice->mask & IN_CLOSE_WRITE) != 0 &&
          strcmp(notice->name, name) == 0) {
        return;
      }
      offset += (ssize_t)(sizeof(*notice) + notice->len);
    }
  }
}

static int32_t qstFileContains(const char *path, const char *needle) {
  char buffer[4096] = {0};
  FILE *file = NULL;

  assert(path != NULL && needle != NULL && needle[0] != '\0');
  file = fopen(path, "rb");
  assert(file != NULL);
  assert(fread(buffer, 1, sizeof(buffer) - 1U, file) > 0);
  assert(fclose(file) == 0);
  return strstr(buffer, needle) != NULL;
}

static void qstAssertDependencyLockRejectsBeforeStartup(
    const char *lock_text, uint64_t run_id) {
  char root[QST_PATH_BYTES] = {0};
  char path[QST_PATH_BYTES] = {0};
  SQstRunConfig config = {0};
  int32_t status = 0;
  pid_t child = -1;

  assert(lock_text != NULL && run_id != 0);
  assert(snprintf(root, sizeof(root),
                  "%s/taosQueryTest-dependency-lock-XXXXXX",
                  qstTestTemporaryRoot()) > 0);
  assert(mkdtemp(root) != NULL);
  qstCreateDatasetAssets(root);
  assert(snprintf(path, sizeof(path), "%s/dependency-lock.json", root) > 0);
  qstWriteFixtureFile(path, lock_text);
  config.run_id = run_id;
  config.mode = QST_MODE_RANDOM;
  config.focus = QST_FOCUS_COVERAGE;
  config.target_selection_bits = 1;
  config.statement_limit = 1;
  config.statement_timeout_seconds = 1;
  config.tool_root = root;
  child = fork();
  assert(child >= 0);
  if (child == 0) {
    _exit(qst_launcher_run(&config) == QST_E_RESOURCE ? 0 : 1);
  }
  assert(waitpid(child, &status, 0) == child);
  assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
  assert(snprintf(path, sizeof(path), "%s/results/%llu/logs/launcher.log", root,
                  (unsigned long long)run_id) > 0);
  assert(qstFileContains(path, "event=CHILD_SPAWN") == 0);
  qstRemoveDirectoryTree(root);
}

void qstTestDependencyLockPreflight(void) {
  static const char kMalformed[] = "{\"format_version\":1,";
  static const char kEmpty[] =
      "{\"format_version\":1,\"dependencies\":[]}";
  static const char kMissingField[] =
      "{\"format_version\":1,\"dependencies\":[{\"name\":\"unit\","
      "\"version\":\"1.0.0\"}]}";
  static const char kPreRelease[] =
      "{\"format_version\":1,\"dependencies\":[{\"name\":\"unit\","
      "\"version\":\"1.0.0-rc1\",\"source\":\"unit test\"}]}";
  static const char kBuildMetadata[] =
      "{\"format_version\":1,\"dependencies\":[{\"name\":\"unit\","
      "\"version\":\"1.0.0+build.1\",\"source\":\"unit test\"}]}";

  qstAssertDependencyLockRejectsBeforeStartup(kMalformed, UINT64_C(91001));
  qstAssertDependencyLockRejectsBeforeStartup(kEmpty, UINT64_C(91002));
  qstAssertDependencyLockRejectsBeforeStartup(kMissingField, UINT64_C(91003));
  qstAssertDependencyLockRejectsBeforeStartup(kPreRelease, UINT64_C(91004));
  qstAssertDependencyLockRejectsBeforeStartup(kBuildMetadata, UINT64_C(91005));
}

static void qstCleanupRun(const char *root, uint64_t run_id) {
  char run_dir[QST_PATH_BYTES] = {0};
  char path[QST_PATH_BYTES] = {0};
  DIR *logs = NULL;
  struct dirent *entry = NULL;

  assert(snprintf(run_dir, sizeof(run_dir), "%s/results/%llu", root,
                  (unsigned long long)run_id) > 0);
  assert(snprintf(path, sizeof(path), "%s/run-config.txt", run_dir) > 0);
  assert(unlink(path) == 0);
  assert(snprintf(path, sizeof(path), "%s/resource-profile.json", run_dir) > 0);
  assert(unlink(path) == 0);
  assert(snprintf(path, sizeof(path), "%s/stage-profile.json", run_dir) > 0);
  assert(unlink(path) == 0);
  assert(snprintf(path, sizeof(path), "%s/report.json", run_dir) > 0);
  assert(unlink(path) == 0);
  assert(snprintf(path, sizeof(path), "%s/report.md", run_dir) > 0);
  assert(unlink(path) == 0);
  assert(snprintf(path, sizeof(path), "%s/pairs", run_dir) > 0);
  qstRemoveDirectoryTree(path);
  assert(snprintf(path, sizeof(path), "%s/cores", run_dir) > 0);
  assert(rmdir(path) == 0);
  assert(snprintf(path, sizeof(path), "%s/pending-results/quarantine", run_dir) > 0);
  assert(rmdir(path) == 0);
  assert(snprintf(path, sizeof(path), "%s/pending-results", run_dir) > 0);
  assert(rmdir(path) == 0);
  assert(snprintf(path, sizeof(path), "%s/dataset/taosbenchmark.json", run_dir) >
         0);
  assert(unlink(path) == 0);
  assert(snprintf(path, sizeof(path), "%s/dataset/tdengine-readiness.json",
                  run_dir) > 0);
  assert(unlink(path) == 0);
  assert(snprintf(path, sizeof(path), "%s/dataset/result-store-readiness.json",
                  run_dir) > 0);
  assert(unlink(path) == 0);
  assert(snprintf(path, sizeof(path), "%s/dataset/tdengine-startup.json",
                  run_dir) > 0);
  assert(unlink(path) == 0);
  assert(snprintf(path, sizeof(path), "%s/dataset", run_dir) > 0);
  assert(rmdir(path) == 0);
  assert(snprintf(path, sizeof(path), "%s/logs", run_dir) > 0);
  logs = opendir(path);
  assert(logs != NULL);
  while ((entry = readdir(logs)) != NULL) {
    char log_path[QST_PATH_BYTES] = {0};

    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    assert(snprintf(log_path, sizeof(log_path), "%s/%s", path,
                    entry->d_name) > 0);
    assert(unlink(log_path) == 0);
  }
  assert(closedir(logs) == 0);
  assert(rmdir(path) == 0);
  assert(rmdir(run_dir) == 0);
}

static void qstAssertMainLifecycleLog(const char *root, uint64_t run_id) {
  char logs_path[QST_PATH_BYTES] = {0};
  char log_path[QST_PATH_BYTES] = {0};
  char contents[8192] = {0};
  DIR *logs = NULL;
  struct dirent *entry = NULL;
  FILE *file = NULL;

  assert(snprintf(logs_path, sizeof(logs_path), "%s/results/%llu/logs", root,
                  (unsigned long long)run_id) > 0);
  logs = opendir(logs_path);
  assert(logs != NULL);
  while ((entry = readdir(logs)) != NULL) {
    if (strncmp(entry->d_name, "main.", 5) == 0) {
      assert(snprintf(log_path, sizeof(log_path), "%s/%s", logs_path,
                      entry->d_name) > 0);
      break;
    }
  }
  assert(closedir(logs) == 0);
  assert(log_path[0] != '\0');
  file = fopen(log_path, "r");
  assert(file != NULL);
  assert(fread(contents, 1, sizeof(contents) - 1, file) > 0);
  assert(fclose(file) == 0);
  assert(strstr(contents, "event=CHILD_SPAWN") != NULL);
  assert(strstr(contents, "event=CHILD_READY") != NULL);
  assert(strstr(contents, "event=CHILD_STOP_REQUEST") != NULL);
  assert(strstr(contents, "event=CHILD_STOP_ACK") != NULL);
  assert(strstr(contents, "event=CHILD_EXITED") != NULL);
  assert(strstr(contents, "event=CHILD_REAPED") != NULL);
}

static void qstAssertLauncherLifecycleLog(const char *root, uint64_t run_id) {
  char logs_path[QST_PATH_BYTES] = {0};
  char log_path[QST_PATH_BYTES] = {0};
  char contents[8192] = {0};
  const char *spawn = NULL;
  const char *ready = NULL;
  const char *exited = NULL;
  const char *reaped = NULL;
  DIR *logs = NULL;
  struct dirent *entry = NULL;
  FILE *file = NULL;

  assert(snprintf(logs_path, sizeof(logs_path), "%s/results/%llu/logs",
                            root, (unsigned long long)run_id) > 0);
  logs = opendir(logs_path);
  assert(logs != NULL);
  while ((entry = readdir(logs)) != NULL) {
    if (strncmp(entry->d_name, "launcher.", 9) == 0 &&
        strcmp(entry->d_name, "launcher.log") != 0) {
      assert(snprintf(log_path, sizeof(log_path), "%s/%s", logs_path,
                                entry->d_name) > 0);
      break;
    }
  }
  assert(closedir(logs) == 0);
  assert(log_path[0] != '\0');
  file = fopen(log_path, "r");
  assert(file != NULL);
  assert(fread(contents, 1, sizeof(contents) - 1, file) > 0);
  assert(fclose(file) == 0);
  spawn = strstr(contents, "event=CHILD_SPAWN");
  ready = strstr(contents, "event=CHILD_READY");
  exited = strstr(contents, "event=CHILD_EXITED");
  reaped = strstr(contents, "event=CHILD_REAPED");
  assert(spawn != NULL && ready != NULL && exited != NULL &&
                   reaped != NULL);
  assert(spawn < ready && ready < exited && exited < reaped);
}

void qstTestLauncherLifecycle(void) {
  char root[] = "/tmp/taosQueryTest-launcher-XXXXXX";
  char report_path[QST_PATH_BYTES] = {0};
  char report[1024] = {0};
  char resource_profile[4096] = {0};
  SQstRunConfig config = {0};
  uint64_t first_run_id = 0;
  uint64_t second_run_id = 0;
  FILE *file = NULL;

  assert(mkdtemp(root) != NULL);
  qstCreateDatasetAssets(root);
  /* FULL enumerates the production catalog; launcher lifecycle needs one
   * bounded end-to-end run rather than exhaustive SQL enumeration. */
  config.mode = QST_MODE_RANDOM;
  config.focus = QST_FOCUS_COVERAGE;
  config.target_selection_bits = 1;
  config.statement_limit = 1;
  config.statement_timeout_seconds = 1;
  config.tool_root = root;
  assert(qst_launcher_run(&config) == QST_OK);
  qstReadRunId(root, &first_run_id);
  assert(qst_launcher_run(&config) == QST_OK);
  qstReadRunId(root, &second_run_id);
  assert(second_run_id > first_run_id);
  assert(snprintf(report_path, sizeof(report_path), "%s/results/%llu/report.json",
                  root, (unsigned long long)second_run_id) > 0);
  file = fopen(report_path, "r");
  assert(file != NULL);
  assert(fgets(report, sizeof(report), file) != NULL);
  assert(fclose(file) == 0);
  assert(strstr(report, "\"status\":\"completed\"") != NULL);
  assert(strstr(report, "\"pair_runtime\":{") != NULL);
  assert(strstr(report, "\"running\":0") != NULL);
  assert(strstr(report, "\"shared_memory_bytes\":") != NULL);
  assert(strstr(report, "\"has_random_seed\":true") != NULL);
  assert(strstr(report, "\"random_seed\":0") == NULL);
  assert(strstr(report, "\"stop_reason\":\"statement_limit\"") != NULL);
  assert(strstr(report, "\"processes\":{\"registered\":") != NULL);
  assert(strstr(report, "\"running\":0,\"reaped\":") != NULL);
  assert(strstr(report, "\"failed\":0}") != NULL);
  assert(snprintf(report_path, sizeof(report_path),
                  "%s/results/%llu/resource-profile.json", root,
                  (unsigned long long)second_run_id) > 0);
  file = fopen(report_path, "r");
  assert(file != NULL);
  assert(fread(resource_profile, 1, sizeof(resource_profile) - 1U, file) > 0);
  assert(fclose(file) == 0);
  assert(strstr(resource_profile, "\"captured_at_ns\":") != NULL);
  assert(strstr(resource_profile, "\"source_flags\":") != NULL);
  assert(strstr(resource_profile, "\"execution_pair_count\":") != NULL);
  assert(strstr(resource_profile, "\"pair_async_window\":") != NULL);
  assert(strstr(resource_profile, "\"pending_quota_bytes\":") != NULL);
  assert(snprintf(report_path, sizeof(report_path), "%s/results/%llu/pairs", root,
                  (unsigned long long)second_run_id) > 0);
  assert(access(report_path, R_OK | X_OK) == 0);
  assert(snprintf(report_path, sizeof(report_path), "%s/results/%llu/cores", root,
                  (unsigned long long)second_run_id) > 0);
  assert(access(report_path, F_OK) == 0);
  assert(snprintf(report_path, sizeof(report_path),
                  "%s/results/%llu/pending-results/quarantine", root,
                  (unsigned long long)second_run_id) > 0);
  assert(access(report_path, F_OK) == 0);
  qstAssertMainLifecycleLog(root, second_run_id);
  qstAssertLauncherLifecycleLog(root, second_run_id);
  qstCleanupRun(root, first_run_id);
  qstCleanupRun(root, second_run_id);
  assert(snprintf(report_path, sizeof(report_path), "%s/results/latest_run_id", root) >
         0);
  assert(unlink(report_path) == 0);
  assert(snprintf(report_path, sizeof(report_path), "%s/results/latest_run_id.lock", root) >
         0);
  assert(unlink(report_path) == 0);
  assert(snprintf(report_path, sizeof(report_path), "%s/results", root) > 0);
  assert(rmdir(report_path) == 0);
  assert(snprintf(report_path, sizeof(report_path), "%s/overflow_content.ndjson",
                  root) > 0);
  if (access(report_path, F_OK) == 0) {
    assert(unlink(report_path) == 0);
  }
  assert(snprintf(report_path, sizeof(report_path), "%s/dataset/taosbenchmark.json",
                  root) > 0);
  assert(unlink(report_path) == 0);
  assert(snprintf(report_path, sizeof(report_path), "%s/dataset", root) > 0);
  assert(rmdir(report_path) == 0);
  assert(snprintf(report_path, sizeof(report_path), "%s/dependency-lock.json",
                  root) > 0);
  assert(unlink(report_path) == 0);
  assert(rmdir(root) == 0);
}

void qstTestLauncherFailureReleasesRunLock(void) {
  char root[] = "/tmp/taosQueryTest-lock-XXXXXX";
  char path[QST_PATH_BYTES] = {0};
  SQstRunConfig config = {0};
  pid_t child = -1;
  int32_t status = 0;

  assert(mkdtemp(root) != NULL);
  assert(snprintf(path, sizeof(path), "%s/results", root) > 0);
  assert(mkdir(path, 0750) == 0);
  assert(snprintf(path, sizeof(path), "%s/results/123", root) > 0);
  qstWriteFixtureFile(path, "occupied\n");
  config.run_id = 123;
  config.mode = QST_MODE_FULL;
  config.focus = QST_FOCUS_COVERAGE;
  config.target_selection_bits = 1;
  config.tool_root = root;
  assert(qst_launcher_run(&config) == QST_E_RESOURCE);

  child = fork();
  assert(child >= 0);
  if (child == 0) {
    int32_t lock_fd = -1;

    assert(snprintf(path, sizeof(path), "%s/results/latest_run_id.lock", root) > 0);
    lock_fd = open(path, O_RDWR | O_CLOEXEC);
    if (lock_fd < 0 || flock(lock_fd, LOCK_EX | LOCK_NB) != 0) {
      _exit(1);
    }
    (void)flock(lock_fd, LOCK_UN);
    (void)close(lock_fd);
    _exit(0);
  }
  assert(waitpid(child, &status, 0) == child);
  assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
  assert(snprintf(path, sizeof(path), "%s/results/123", root) > 0);
  assert(unlink(path) == 0);
  assert(snprintf(path, sizeof(path), "%s/results/latest_run_id.lock", root) > 0);
  assert(unlink(path) == 0);
  assert(snprintf(path, sizeof(path), "%s/results", root) > 0);
  assert(rmdir(path) == 0);
  assert(rmdir(root) == 0);
}

void qstTestLauncherHoldsRunLock(void) {
  char root[] = "/tmp/taosQueryTest-running-lock-XXXXXX";
  char results_dir[QST_PATH_BYTES] = {0};
  char lock_path[QST_PATH_BYTES] = {0};
  char ready_path[QST_PATH_BYTES] = {0};
  SQstRunConfig config = {0};
  pid_t launcher_pid = -1;
  int32_t inotify_fd = -1;
  int32_t watch_fd = -1;
  int32_t lock_fd = -1;
  int32_t status = 0;

  assert(mkdtemp(root) != NULL);
  qstCreateDatasetAssets(root);
  assert(snprintf(results_dir, sizeof(results_dir), "%s/results", root) > 0);
  assert(mkdir(results_dir, 0750) == 0);
  assert(snprintf(ready_path, sizeof(ready_path), "%s/launcher-ready",
                  results_dir) > 0);
  inotify_fd = inotify_init1(IN_CLOEXEC);
  assert(inotify_fd >= 0);
  watch_fd = inotify_add_watch(inotify_fd, results_dir, IN_CLOSE_WRITE);
  assert(watch_fd >= 0);
  config.mode = QST_MODE_RANDOM;
  config.focus = QST_FOCUS_COVERAGE;
  config.target_selection_bits = 1;
  config.statement_limit = 1;
  config.statement_timeout_seconds = 1;
  config.tool_root = root;
  assert(setenv("QST_TEST_LAUNCHER_READY", ready_path, 1) == 0);
  /* Keep the launcher alive after the readiness barrier so this test observes
   * the lock lifecycle instead of racing a fast Release-mode completion. */
  assert(setenv("QST_TEST_MAIN_STARTUP_DELAY_MILLISECONDS", "15000", 1) == 0);

  launcher_pid = fork();
  assert(launcher_pid >= 0);
  if (launcher_pid == 0) {
    const EQstCode code = qst_launcher_run(&config);

    _exit(code == QST_E_STOPPING ? 0 : 1);
  }
  qstWaitForInotifyFile(inotify_fd, "launcher-ready");
  assert(snprintf(lock_path, sizeof(lock_path), "%s/latest_run_id.lock",
                  results_dir) > 0);
  lock_fd = open(lock_path, O_RDWR | O_CLOEXEC);
  assert(lock_fd >= 0);
  errno = 0;
  assert(flock(lock_fd, LOCK_EX | LOCK_NB) != 0);
  assert(errno == EWOULDBLOCK || errno == EAGAIN);
  assert(close(lock_fd) == 0);
  lock_fd = -1;
  assert(kill(launcher_pid, SIGTERM) == 0);
  assert(waitpid(launcher_pid, &status, 0) == launcher_pid);
  assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);

  lock_fd = open(lock_path, O_RDWR | O_CLOEXEC);
  assert(lock_fd >= 0);
  assert(flock(lock_fd, LOCK_EX | LOCK_NB) == 0);
  assert(flock(lock_fd, LOCK_UN) == 0);
  assert(close(lock_fd) == 0);
  assert(inotify_rm_watch(inotify_fd, watch_fd) == 0);
  assert(close(inotify_fd) == 0);
  assert(unsetenv("QST_TEST_LAUNCHER_READY") == 0);
  assert(unsetenv("QST_TEST_MAIN_STARTUP_DELAY_MILLISECONDS") == 0);
  qstRemoveDirectoryTree(root);
}

void qstTestLauncherStopDrainsPersistence(void) {
  char root[] = "/tmp/taosQueryTest-stop-drain-XXXXXX";
  char results_dir[QST_PATH_BYTES] = {0};
  char activity_path[QST_PATH_BYTES] = {0};
  char report_path[QST_PATH_BYTES] = {0};
  char report[16384] = {0};
  SQstRunConfig config = {0};
  uint64_t run_id = 0;
  unsigned long long generated = 0;
  unsigned long long persisted = 0;
  pid_t launcher_pid = -1;
  int32_t inotify_fd = -1;
  int32_t watch_fd = -1;
  int32_t status = 0;
  FILE *file = NULL;

  assert(mkdtemp(root) != NULL);
  qstCreateDatasetAssets(root);
  assert(snprintf(results_dir, sizeof(results_dir), "%s/results", root) > 0);
  assert(mkdir(results_dir, 0750) == 0);
  assert(snprintf(activity_path, sizeof(activity_path), "%s/generator-active",
                  results_dir) > 0);
  inotify_fd = inotify_init1(IN_CLOEXEC);
  assert(inotify_fd >= 0);
  watch_fd = inotify_add_watch(inotify_fd, results_dir, IN_CLOSE_WRITE);
  assert(watch_fd >= 0);
  config.mode = QST_MODE_RANDOM;
  config.focus = QST_FOCUS_COVERAGE;
  config.target_selection_bits = 1;
  config.statement_timeout_seconds = 1;
  config.tool_root = root;
  assert(setenv("QST_FAKE_TAOS_ASYNC_DELAY_MS", "100", 1) == 0);
  assert(setenv("QST_TEST_GENERATOR_ACTIVE", activity_path, 1) == 0);

  launcher_pid = fork();
  assert(launcher_pid >= 0);
  if (launcher_pid == 0) {
    const EQstCode code = qst_launcher_run(&config);

    _exit(code == QST_E_STOPPING ? 0 : 1);
  }
  qstWaitForInotifyFile(inotify_fd, "latest_run_id");
  qstReadRunId(root, &run_id);
  qstWaitForInotifyFile(inotify_fd, "generator-active");
  assert(kill(launcher_pid, SIGTERM) == 0);
  assert(waitpid(launcher_pid, &status, 0) == launcher_pid);
  assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
  assert(snprintf(report_path, sizeof(report_path),
                  "%s/results/%llu/report.json", root,
                  (unsigned long long)run_id) > 0);
  file = fopen(report_path, "r");
  assert(file != NULL);
  assert(fread(report, 1, sizeof(report) - 1U, file) > 0);
  assert(fclose(file) == 0);
  assert(strstr(report, "\"status\":\"cancelled\"") != NULL);
  assert(strstr(report, "\"stop_reason\":\"user_request\"") != NULL);
  assert(strstr(report, "\"phases\":{\"run\":\"cancelled\",\"persistence\":\"durable\"}") != NULL);
  assert(strstr(report, "\"executing\":0,\"final\":0,\"final_published\":0,\"persisting\":0,") != NULL);
  assert(strstr(report, "\"running\":0") != NULL);
  assert(sscanf(strstr(report, "\"tasks\":{\"generated\":"),
                "\"tasks\":{\"generated\":%llu,\"executing\":%*u,\"final\":%*u,"
                "\"final_published\":%*u,\"persisting\":%*u,\"persisted\":%llu",
                &generated, &persisted) == 2);
  assert(generated > 0 && generated == persisted);
  assert(inotify_rm_watch(inotify_fd, watch_fd) == 0);
  assert(close(inotify_fd) == 0);
  assert(unsetenv("QST_FAKE_TAOS_ASYNC_DELAY_MS") == 0);
  assert(unsetenv("QST_TEST_GENERATOR_ACTIVE") == 0);
  qstRemoveDirectoryTree(root);
}

void qstTestLauncherAllowsMainStartupLease(void) {
  char root[] = "/tmp/taosQueryTest-main-startup-XXXXXX";
  SQstRunConfig config = {0};

  assert(mkdtemp(root) != NULL);
  qstCreateDatasetAssets(root);
  config.mode = QST_MODE_RANDOM;
  config.focus = QST_FOCUS_COVERAGE;
  config.target_selection_bits = 1;
  config.statement_limit = 1;
  config.statement_timeout_seconds = 1;
  /* Startup and ordered stop each publish control flow records. Keep this
   * lifecycle test above the automatic minimum queue capacity. */
  config.queue_capacity_override = 4;
  config.tool_root = root;
  assert(setenv("QST_TEST_MAIN_STARTUP_DELAY_MILLISECONDS", "2500", 1) == 0);
  assert(qst_launcher_run(&config) == QST_OK);
  assert(unsetenv("QST_TEST_MAIN_STARTUP_DELAY_MILLISECONDS") == 0);
  qstRemoveDirectoryTree(root);
}

void qstTestLauncherAllowsMainShutdownLease(void) {
  char root[] = "/tmp/taosQueryTest-main-shutdown-XXXXXX";
  SQstRunConfig config = {0};

  assert(mkdtemp(root) != NULL);
  qstCreateDatasetAssets(root);
  config.mode = QST_MODE_RANDOM;
  config.focus = QST_FOCUS_COVERAGE;
  config.target_selection_bits = 1;
  config.statement_limit = 1;
  config.statement_timeout_seconds = 1;
  config.queue_capacity_override = 4;
  config.tool_root = root;
  assert(setenv("QST_TEST_MAIN_SHUTDOWN_DELAY_MILLISECONDS", "32000", 1) ==
         0);
  assert(qst_launcher_run(&config) == QST_OK);
  assert(unsetenv("QST_TEST_MAIN_SHUTDOWN_DELAY_MILLISECONDS") == 0);
  qstRemoveDirectoryTree(root);
}

void qstTestLauncherPairSupervisorCrashRecovery(void) {
  char root[] = "/tmp/taosQueryTest-pair-crash-XXXXXX";
  char claim_path[QST_PATH_BYTES] = {0};
  char run_path[QST_PATH_BYTES] = {0};
  char pair_path[QST_PATH_BYTES] = {0};
  char fact[2048] = {0};
  char report[16384] = {0};
  SQstRunConfig config = {0};
  uint64_t run_id = 0;
  FILE *file = NULL;
  DIR *pairs = NULL;
  struct dirent *entry = NULL;
  int32_t found_crash = 0;
  int32_t found_reaped = 0;

  assert(mkdtemp(root) != NULL);
  qstCreateDatasetAssets(root);
  assert(snprintf(claim_path, sizeof(claim_path), "%s/kill-once", root) > 0);
  assert(setenv("QST_TEST_KILL_AFTER_FINAL_ROLE", "pair_supervisor", 1) ==
         0);
  assert(setenv("QST_TEST_KILL_SLOT", "0", 1) == 0);
  assert(setenv("QST_TEST_KILL_ONCE", claim_path, 1) == 0);
  config.mode = QST_MODE_RANDOM;
  config.focus = QST_FOCUS_COVERAGE;
  config.target_selection_bits = 1;
  config.statement_limit = 1;
  config.statement_timeout_seconds = 1;
  config.tool_root = root;
  assert(qst_launcher_run(&config) == QST_OK);
  assert(unsetenv("QST_TEST_KILL_AFTER_FINAL_ROLE") == 0);
  assert(unsetenv("QST_TEST_KILL_SLOT") == 0);
  assert(unsetenv("QST_TEST_KILL_ONCE") == 0);
  qstReadRunId(root, &run_id);
  assert(snprintf(run_path, sizeof(run_path), "%s/results/%llu/report.json", root,
                  (unsigned long long)run_id) > 0);
  file = fopen(run_path, "r");
  assert(file != NULL);
  assert(fread(report, 1, sizeof(report) - 1U, file) > 0);
  assert(fclose(file) == 0);
  assert(strstr(report, "\"status\":\"completed\"") != NULL);
  assert(strstr(report, "\"supervisor_crashed\":true") != NULL);
  assert(snprintf(pair_path, sizeof(pair_path), "%s/results/%llu/pairs", root,
                  (unsigned long long)run_id) > 0);
  pairs = opendir(pair_path);
  assert(pairs != NULL);
  while ((entry = readdir(pairs)) != NULL) {
    char fact_path[QST_PATH_BYTES] = {0};

    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    assert(snprintf(fact_path, sizeof(fact_path), "%s/%s/runtime.json",
                    pair_path, entry->d_name) > 0);
    file = fopen(fact_path, "r");
    assert(file != NULL);
    memset(fact, 0, sizeof(fact));
    assert(fread(fact, 1, sizeof(fact) - 1U, file) > 0);
    assert(fclose(file) == 0);
    if (strstr(fact, "\"supervisor_crashed\":true") != NULL) {
      found_crash = 1;
      assert(strstr(fact, "\"state\":\"reaped\"") != NULL);
    }
    if (strstr(fact, "\"state\":\"reaped\"") != NULL) {
      found_reaped = 1;
    }
    assert(strstr(fact, "\"taosd_process_generation\":") != NULL);
    assert(strstr(fact, "\"taosd_exit_count\":") != NULL);
  }
  assert(closedir(pairs) == 0);
  assert(found_crash != 0 && found_reaped != 0);
  qstRemoveDirectoryTree(root);
}

void qstTestLauncherPairSupervisorStopCrashFailsRun(void) {
  char root[] = "/tmp/taosQueryTest-pair-stop-crash-XXXXXX";
  char claim_path[QST_PATH_BYTES] = {0};
  char report_path[QST_PATH_BYTES] = {0};
  char report[16384] = {0};
  SQstRunConfig config = {0};
  uint64_t run_id = 0;
  FILE *file = NULL;

  assert(mkdtemp(root) != NULL);
  qstCreateDatasetAssets(root);
  assert(snprintf(claim_path, sizeof(claim_path), "%s/kill-once", root) > 0);
  assert(setenv("QST_TEST_KILL_ROLE_ON_STOP", "pair_supervisor", 1) == 0);
  assert(setenv("QST_TEST_KILL_SLOT", "0", 1) == 0);
  assert(setenv("QST_TEST_KILL_ONCE", claim_path, 1) == 0);
  config.mode = QST_MODE_RANDOM;
  config.focus = QST_FOCUS_COVERAGE;
  config.target_selection_bits = 1;
  config.statement_limit = 1;
  config.statement_timeout_seconds = 1;
  config.tool_root = root;
  assert(qst_launcher_run(&config) != QST_OK);
  assert(unsetenv("QST_TEST_KILL_ROLE_ON_STOP") == 0);
  assert(unsetenv("QST_TEST_KILL_SLOT") == 0);
  assert(unsetenv("QST_TEST_KILL_ONCE") == 0);
  qstReadRunId(root, &run_id);
  assert(snprintf(report_path, sizeof(report_path), "%s/results/%llu/report.json",
                  root, (unsigned long long)run_id) > 0);
  file = fopen(report_path, "r");
  assert(file != NULL);
  assert(fread(report, 1, sizeof(report) - 1U, file) > 0);
  assert(fclose(file) == 0);
  assert(strstr(report, "\"status\":\"failed\"") != NULL);
  assert(strstr(report, "\"status\":\"completed\"") == NULL);
  assert(strstr(report, "\"running\":0") != NULL);
  qstRemoveDirectoryTree(root);
}

void qstTestLauncherExecManagerCrashFailsRun(void) {
  char root[] = "/tmp/taosQueryTest-exec-manager-crash-XXXXXX";
  char claim_path[QST_PATH_BYTES] = {0};
  char report_path[QST_PATH_BYTES] = {0};
  char report[16384] = {0};
  SQstRunConfig config = {0};
  uint64_t run_id = 0;
  FILE *file = NULL;

  assert(mkdtemp(root) != NULL);
  qstCreateDatasetAssets(root);
  assert(snprintf(claim_path, sizeof(claim_path), "%s/kill-once", root) > 0);
  assert(setenv("QST_TEST_KILL_ROLE", "exec_manager", 1) == 0);
  assert(setenv("QST_TEST_KILL_ONCE", claim_path, 1) == 0);
  config.mode = QST_MODE_RANDOM;
  config.focus = QST_FOCUS_COVERAGE;
  config.target_selection_bits = 1;
  config.statement_limit = 1;
  config.statement_timeout_seconds = 1;
  config.tool_root = root;
  assert(qst_launcher_run(&config) != QST_OK);
  assert(unsetenv("QST_TEST_KILL_ROLE") == 0);
  assert(unsetenv("QST_TEST_KILL_ONCE") == 0);
  qstReadRunId(root, &run_id);
  assert(snprintf(report_path, sizeof(report_path), "%s/results/%llu/report.json",
                  root, (unsigned long long)run_id) > 0);
  file = fopen(report_path, "r");
  assert(file != NULL);
  assert(fread(report, 1, sizeof(report) - 1U, file) > 0);
  assert(fclose(file) == 0);
  assert(strstr(report, "\"status\":\"failed\"") != NULL);
  assert(strstr(report, "\"status\":\"completed\"") == NULL);
  assert(strstr(report, "\"running\":0") != NULL);
  qstRemoveDirectoryTree(root);
}

void qstTestLauncherJudgeWorkerCrashFailsRun(void) {
  char root[] = "/tmp/taosQueryTest-judge-worker-crash-XXXXXX";
  char claim_path[QST_PATH_BYTES] = {0};
  char report_path[QST_PATH_BYTES] = {0};
  char report[16384] = {0};
  SQstRunConfig config = {0};
  uint64_t run_id = 0;
  FILE *file = NULL;

  assert(mkdtemp(root) != NULL);
  qstCreateDatasetAssets(root);
  assert(snprintf(claim_path, sizeof(claim_path), "%s/kill-once", root) > 0);
  assert(setenv("QST_TEST_KILL_ROLE", "judge_worker", 1) == 0);
  assert(setenv("QST_TEST_KILL_SLOT", "0", 1) == 0);
  assert(setenv("QST_TEST_KILL_ONCE", claim_path, 1) == 0);
  config.mode = QST_MODE_RANDOM;
  config.focus = QST_FOCUS_COVERAGE;
  config.target_selection_bits = 1;
  config.statement_limit = 1;
  config.statement_timeout_seconds = 1;
  config.tool_root = root;
  assert(qst_launcher_run(&config) != QST_OK);
  assert(unsetenv("QST_TEST_KILL_ROLE") == 0);
  assert(unsetenv("QST_TEST_KILL_SLOT") == 0);
  assert(unsetenv("QST_TEST_KILL_ONCE") == 0);
  qstReadRunId(root, &run_id);
  assert(snprintf(report_path, sizeof(report_path), "%s/results/%llu/report.json",
                  root, (unsigned long long)run_id) > 0);
  file = fopen(report_path, "r");
  assert(file != NULL);
  assert(fread(report, 1, sizeof(report) - 1U, file) > 0);
  assert(fclose(file) == 0);
  assert(strstr(report, "\"status\":\"failed\"") != NULL);
  assert(strstr(report, "\"status\":\"completed\"") == NULL);
  assert(strstr(report, "\"running\":0") != NULL);
  qstRemoveDirectoryTree(root);
}
