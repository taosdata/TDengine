# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

add_library(qstFakeTaos SHARED qstFakeTaos.c)
target_link_libraries(qstFakeTaos PRIVATE pthread)
set_target_properties(qstFakeTaos PROPERTIES
  OUTPUT_NAME taos
  LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/fake-native"
)

add_executable(qstFakeTaosd qstFakeTaosd.c)
set_target_properties(qstFakeTaosd PROPERTIES
  OUTPUT_NAME taosd
  RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/fake-bin"
)

add_executable(taosQueryTestCoreTest qstCoreTest.c qstConfigTest.c)
target_link_libraries(taosQueryTestCoreTest PRIVATE taosQueryTestCore)
add_dependencies(taosQueryTestCoreTest qstFakeTaos qstFakeTaosd)
target_include_directories(taosQueryTestCoreTest PRIVATE
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
  "${CMAKE_CURRENT_SOURCE_DIR}/../src"
)
target_compile_definitions(taosQueryTestCoreTest PRIVATE
  QST_DATASET_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../dataset"
  QST_TOOL_ROOT="${CMAKE_CURRENT_SOURCE_DIR}/.."
)
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
  # The test executable uses assert() as its failure mechanism. Keep it
  # active when the surrounding project is configured as Release.
  target_compile_options(taosQueryTestCoreTest PRIVATE -UNDEBUG)
endif()
add_test(NAME taosQueryTestCoreTest COMMAND taosQueryTestCoreTest)
set_tests_properties(taosQueryTestCoreTest PROPERTIES
  ENVIRONMENT "LD_LIBRARY_PATH=$<TARGET_FILE_DIR:qstFakeTaos>;PATH=$<TARGET_FILE_DIR:qstFakeTaosd>:$ENV{PATH}"
)

function(qst_add_core_selector_test test_name selector)
  add_test(NAME "${test_name}" COMMAND taosQueryTestCoreTest)
  set_tests_properties("${test_name}" PROPERTIES
    ENVIRONMENT "${selector}=1;LD_LIBRARY_PATH=$<TARGET_FILE_DIR:qstFakeTaos>;PATH=$<TARGET_FILE_DIR:qstFakeTaosd>:$ENV{PATH}"
  )
endfunction()

# Keep behavior failures local to their owning DS-F area rather than exposing
# them only through the broad end-to-end core test.
qst_add_core_selector_test(taosQueryTestBlockTest QST_CORE_BLOCK_ONLY)
qst_add_core_selector_test(taosQueryTestResultTest QST_CORE_RESULT_ONLY)
qst_add_core_selector_test(taosQueryTestDatasetTest QST_CORE_DATASET_ONLY)
qst_add_core_selector_test(taosQueryTestProviderTest QST_CORE_PROVIDER_ONLY)
qst_add_core_selector_test(taosQueryTestQueueTest QST_CORE_QUEUE_ONLY)
qst_add_core_selector_test(taosQueryTestProcessTest QST_CORE_PROCESS_ONLY)
qst_add_core_selector_test(taosQueryTestConfigResourceTest QST_CORE_CONFIG_RESOURCE_ONLY)
qst_add_core_selector_test(taosQueryTestAiReportTest QST_CORE_AI_REPORT_ONLY)
qst_add_core_selector_test(taosQueryTestStartupLeaseTest QST_CORE_STARTUP_LEASE_ONLY)
qst_add_core_selector_test(taosQueryTestTaskStatsTest QST_CORE_TASK_STATS_ONLY)
qst_add_core_selector_test(taosQueryTestResultStoreRecoveryTest QST_CORE_RESULT_STORE_RECOVERY_ONLY)
qst_add_core_selector_test(taosQueryTestPairDeadlineTest QST_CORE_PAIR_DEADLINE_ONLY)
qst_add_core_selector_test(taosQueryTestPairBatchTest QST_CORE_PAIR_BATCH_ONLY)
qst_add_core_selector_test(taosQueryTestPairPortRetryTest
  QST_CORE_PAIR_PORT_RETRY_ONLY)
qst_add_core_selector_test(taosQueryTestFinalConfirmationRaceTest
  QST_CORE_FINAL_CONFIRMATION_RACE_ONLY)
qst_add_core_selector_test(taosQueryTestPairCoreAttributionTest QST_CORE_PAIR_CORE_ONLY)
# qstFakeTaosd deliberately raises SIGSEGV to simulate a server core. Keep the
# signal visible to the parent when this test is built with AddressSanitizer.
set_property(TEST taosQueryTestPairCoreAttributionTest APPEND PROPERTY
  ENVIRONMENT "ASAN_OPTIONS=handle_segv=0"
)
qst_add_core_selector_test(taosQueryTestExecManagerCrashTest
  QST_CORE_EXEC_MANAGER_CRASH_ONLY)
qst_add_core_selector_test(taosQueryTestRunLockTest QST_CORE_RUN_LOCK_ONLY)
qst_add_core_selector_test(taosQueryTestDependencyLockTest QST_CORE_DEPENDENCY_LOCK_ONLY)
add_test(
  NAME taosQueryTestPrepareMachineTest
  COMMAND bash "${CMAKE_CURRENT_SOURCE_DIR}/prepare-test-machine-test.sh"
          "${CMAKE_CURRENT_SOURCE_DIR}/../prepare-test-machine.sh"
          "$<TARGET_FILE_DIR:taosQueryTest>/.."
)
add_test(
  NAME taosQueryTestCrashSupervisorTest
  COMMAND bash "${CMAKE_CURRENT_SOURCE_DIR}/run_crash_injection.sh"
          "$<TARGET_FILE:taosQueryTestCoreTest>"
          "$<TARGET_FILE_DIR:qstFakeTaos>"
          "$<TARGET_FILE_DIR:qstFakeTaosd>"
)

add_test(
  NAME taosQueryTestPortLeaseConcurrencyTest
  COMMAND bash "${CMAKE_CURRENT_SOURCE_DIR}/run_port_lease_concurrency.sh"
          "$<TARGET_FILE:taosQueryTestCoreTest>"
          "$<TARGET_FILE_DIR:qstFakeTaos>"
          "$<TARGET_FILE_DIR:qstFakeTaosd>"
)

qst_add_core_selector_test(taosQueryTestJudgeWorkerCrashTest
  QST_CORE_JUDGE_WORKER_CRASH_ONLY)

add_test(NAME taosQueryTestGeneratorTest COMMAND taosQueryTestCoreTest)
set_tests_properties(taosQueryTestGeneratorTest PROPERTIES
  ENVIRONMENT "QST_CORE_GENERATOR_ONLY=1;LD_LIBRARY_PATH=$<TARGET_FILE_DIR:qstFakeTaos>;PATH=$<TARGET_FILE_DIR:qstFakeTaosd>:$ENV{PATH}"
)

add_test(
  NAME taosQueryTestDimensionCatalogManifestTest
  COMMAND bash "${CMAKE_CURRENT_SOURCE_DIR}/catalog-manifest-test.sh"
          "${CMAKE_CURRENT_SOURCE_DIR}/../tools/generate_sql_dimension_catalog.py"
          "${CMAKE_CURRENT_SOURCE_DIR}/../../../../../docs/releases/TSDB-v3.4.3-[20260930]/05-设计文档/查询 SQL 测试覆盖工具 DS.md"
          "${CMAKE_CURRENT_SOURCE_DIR}/../inc/sql_dimension_ids.h"
          "${CMAKE_CURRENT_SOURCE_DIR}/../src/sql_dimension_catalog_source.c"
)

add_test(
  NAME taosQueryTestPackageTest
  COMMAND bash "${CMAKE_CURRENT_SOURCE_DIR}/package-test.sh"
          "${CMAKE_CURRENT_SOURCE_DIR}/../package-taosQueryTest.sh"
          "$<TARGET_FILE:taosQueryTest>"
          "$<TARGET_FILE_DIR:qstFakeTaos>"
          "${CMAKE_CURRENT_BINARY_DIR}/package-test-work"
)
