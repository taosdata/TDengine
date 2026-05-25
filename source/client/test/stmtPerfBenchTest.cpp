/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can redistribute it and/or modify
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

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>
#include <string>
#include <vector>

#include "taos.h"
#include "stmtPerfBenchCommon.h"

namespace {

StmtPerfBenchCase makeCase(StmtPerfBenchScenario scenario, int32_t tables, int32_t rowsPerTable, uint32_t seed = 7,
                           StmtPerfBenchApi api = StmtPerfBenchApi::kStmt,
                           StmtPerfBenchMode mode = StmtPerfBenchMode::kNormal) {
  return {api, mode, scenario, tables, rowsPerTable, 1, 0, 1, seed};
}

}  // namespace

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(StmtPerfBenchConfig, ExpandsAllApiAndModeForSingleScenario) {
  const char *argv[] = {"stmtPerfBench", "--api=all", "--mode=all", "--scenario=single"};

  StmtPerfBenchConfig config;
  ASSERT_TRUE(stmtPerfBenchParseConfig(4, argv, &config, nullptr));

  std::vector<StmtPerfBenchCase> cases = stmtPerfBenchExpandCases(config);
  ASSERT_EQ(cases.size(), 4U);
  EXPECT_EQ(cases[0].api, StmtPerfBenchApi::kStmt);
  EXPECT_EQ(cases[0].mode, StmtPerfBenchMode::kNormal);
  EXPECT_EQ(cases[0].scenario, StmtPerfBenchScenario::kSingle);
  EXPECT_EQ(cases[3].api, StmtPerfBenchApi::kStmt2);
  EXPECT_EQ(cases[3].mode, StmtPerfBenchMode::kInterlace);
  EXPECT_EQ(cases[3].scenario, StmtPerfBenchScenario::kSingle);
}

TEST(StmtPerfBenchConfig, RejectsInvalidOutputValue) {
  const char *argv[] = {"stmtPerfBench", "--output=xml"};

  StmtPerfBenchConfig config;
  std::string         error;
  EXPECT_FALSE(stmtPerfBenchParseConfig(2, argv, &config, &error));
  EXPECT_NE(error.find("--output"), std::string::npos);
}

TEST(StmtPerfBenchConfig, ParsesStmt2AndInterlaceFlags) {
  const char *argv[] = {"stmtPerfBench", "--api", "stmt2", "--mode", "interlace"};

  StmtPerfBenchConfig config;
  ASSERT_TRUE(stmtPerfBenchParseConfig(5, argv, &config, nullptr));
  EXPECT_EQ(config.api, StmtPerfBenchApi::kStmt2);
  EXPECT_EQ(config.mode, StmtPerfBenchMode::kInterlace);
}

TEST(StmtPerfBenchConfig, ExpandsAllScenarioSelectors) {
  const char *argv[] = {"stmtPerfBench", "--api=all", "--mode=all", "--scenario=all"};

  StmtPerfBenchConfig config;
  ASSERT_TRUE(stmtPerfBenchParseConfig(4, argv, &config, nullptr));

  std::vector<StmtPerfBenchCase> cases = stmtPerfBenchExpandCases(config);
  EXPECT_EQ(cases.size(), 28U);
}

TEST(StmtPerfBenchConfig, ParsesSupportedOutputs) {
  const char *argvTable[] = {"stmtPerfBench", "--output=table"};
  const char *argvCsv[] = {"stmtPerfBench", "--output=csv"};
  const char *argvJson[] = {"stmtPerfBench", "--output=json"};

  StmtPerfBenchConfig config;
  ASSERT_TRUE(stmtPerfBenchParseConfig(2, argvTable, &config, nullptr));
  EXPECT_EQ(config.output, StmtPerfBenchOutput::kTable);

  config = StmtPerfBenchConfig{};
  ASSERT_TRUE(stmtPerfBenchParseConfig(2, argvCsv, &config, nullptr));
  EXPECT_EQ(config.output, StmtPerfBenchOutput::kCsv);

  config = StmtPerfBenchConfig{};
  ASSERT_TRUE(stmtPerfBenchParseConfig(2, argvJson, &config, nullptr));
  EXPECT_EQ(config.output, StmtPerfBenchOutput::kJson);
}

TEST(StmtPerfBenchConfig, RejectsInvalidNumericArguments) {
  const char *argv[] = {"stmtPerfBench", "--cycles=0"};

  StmtPerfBenchConfig config;
  std::string         error;
  EXPECT_FALSE(stmtPerfBenchParseConfig(2, argv, &config, &error));
  EXPECT_NE(error.find("--cycles"), std::string::npos);
}

TEST(StmtPerfBenchConfig, AppliesDefaultExecutionFlags) {
  const char *argv[] = {"stmtPerfBench"};

  StmtPerfBenchConfig config;
  ASSERT_TRUE(stmtPerfBenchParseConfig(1, argv, &config, nullptr));
  EXPECT_EQ(config.warmup, 1);
  EXPECT_EQ(config.cycles, 3);
  EXPECT_TRUE(config.dropDb);
  EXPECT_TRUE(config.verify);
}

TEST(StmtPerfBenchConfig, ParsesVerboseMetricsFlag) {
  const char *argv[] = {"stmtPerfBench", "--verbose-metrics=yes"};

  StmtPerfBenchConfig config;
  ASSERT_TRUE(stmtPerfBenchParseConfig(2, argv, &config, nullptr));
  EXPECT_TRUE(config.verboseMetrics);
}

TEST(StmtPerfBenchWorkload, SingleScenarioUsesOneTableId) {
  StmtPerfBenchWorkload workload;
  std::string           error;
  ASSERT_TRUE(stmtPerfBenchBuildWorkload(makeCase(StmtPerfBenchScenario::kSingle, 1, 4), &workload, &error));
  EXPECT_EQ(workload.tableOrder, std::vector<int32_t>({0, 0, 0, 0}));
}

TEST(StmtPerfBenchWorkload, FewSeqGroupsTableIdsContiguously) {
  StmtPerfBenchWorkload workload;
  std::string           error;
  ASSERT_TRUE(stmtPerfBenchBuildWorkload(makeCase(StmtPerfBenchScenario::kFewSeq, 4, 3), &workload, &error));
  EXPECT_EQ(workload.tableOrder, std::vector<int32_t>({0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3}));
}

TEST(StmtPerfBenchWorkload, FewInterlaceAlternatesRoundRobin) {
  StmtPerfBenchWorkload workload;
  std::string           error;
  ASSERT_TRUE(stmtPerfBenchBuildWorkload(makeCase(StmtPerfBenchScenario::kFewInterlace, 4, 3), &workload, &error));
  EXPECT_EQ(workload.tableOrder, std::vector<int32_t>({0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3}));
}

TEST(StmtPerfBenchWorkload, FewRevisitRepeatsHotTablePattern) {
  StmtPerfBenchWorkload workload;
  std::string           error;
  ASSERT_TRUE(stmtPerfBenchBuildWorkload(makeCase(StmtPerfBenchScenario::kFewRevisit, 4, 2), &workload, &error));
  EXPECT_EQ(workload.tableOrder, std::vector<int32_t>({0, 1, 0, 1, 2, 3, 2, 3}));
}

TEST(StmtPerfBenchWorkload, IdenticalSeedsGenerateIdenticalWorkloads) {
  StmtPerfBenchWorkload lhs;
  StmtPerfBenchWorkload rhs;
  std::string           error;
  ASSERT_TRUE(stmtPerfBenchBuildWorkload(makeCase(StmtPerfBenchScenario::kManyInterlace, 8, 2, 42), &lhs, &error));
  ASSERT_TRUE(stmtPerfBenchBuildWorkload(makeCase(StmtPerfBenchScenario::kManyInterlace, 8, 2, 42), &rhs, &error));
  EXPECT_EQ(lhs.tableOrder, rhs.tableOrder);
  EXPECT_EQ(lhs.timestamps, rhs.timestamps);
  EXPECT_EQ(lhs.values, rhs.values);
}

TEST(StmtPerfBenchEnvironment, UsesExplicitDatabaseName) {
  StmtPerfBenchConfig config;
  config.db = "bench_db";

  StmtPerfBenchNames names;
  std::string        error;
  ASSERT_TRUE(stmtPerfBenchBuildNames(config, makeCase(StmtPerfBenchScenario::kSingle, 1, 1), "unit", &names, &error));
  EXPECT_EQ(names.dbName, "bench_db");
  EXPECT_EQ(names.stableName, "stb_unit_stmt_normal_single");
  ASSERT_EQ(names.tableNames.size(), 1U);
  EXPECT_EQ(names.tableNames[0], "tb_unit_stmt_normal_single_0000");
}

TEST(StmtPerfBenchEnvironment, GeneratesDefaultDatabaseStableAndTableNames) {
  StmtPerfBenchConfig config;

  StmtPerfBenchNames names;
  std::string        error;
  ASSERT_TRUE(stmtPerfBenchBuildNames(config,
                                      makeCase(StmtPerfBenchScenario::kFewInterlace, 2, 3, 9, StmtPerfBenchApi::kStmt2,
                                               StmtPerfBenchMode::kInterlace),
                                      "run42", &names, &error));
  EXPECT_EQ(names.dbName, "stmt_perf_bench_run42");
  EXPECT_EQ(names.stableName, "stb_run42_stmt2_interlace_few_interlace");
  ASSERT_EQ(names.tableNames.size(), 2U);
  EXPECT_EQ(names.tableNames[0], "tb_run42_stmt2_interlace_few_interlace_0000");
  EXPECT_EQ(names.tableNames[1], "tb_run42_stmt2_interlace_few_interlace_0001");
}

TEST(StmtPerfBenchRuntime, StmtNormalSingleReportsSegmentedMetrics) {
  TAOS *taos = taos_connect("127.0.0.1", "root", "taosdata", nullptr, 6030);
  if (taos == nullptr) {
    GTEST_SKIP() << "TDengine is not available";
  }
  taos_close(taos);

  std::vector<std::string> args = {"stmtPerfBench", "--api=stmt", "--mode=normal", "--scenario=single", "--tables=1",
                                   "--rows-per-table=8", "--batch-rows=2", "--warmup=0", "--cycles=1",
                                   "--db=stmt_perf_bench_runtime_stmt_single", "--output=table", "--drop-db=yes",
                                   "--verify=yes"};
  std::vector<char *> argv;
  argv.reserve(args.size());
  for (std::string &arg : args) {
    argv.push_back(&arg[0]);
  }

  testing::internal::CaptureStdout();
  int exitCode = stmtPerfBenchMain(static_cast<int>(argv.size()), argv.data());
  std::string output = testing::internal::GetCapturedStdout();

  ASSERT_EQ(exitCode, 0);
  EXPECT_NE(output.find("case=stmt/normal/single"), std::string::npos);
  EXPECT_NE(output.find("prepare_us="), std::string::npos);
  EXPECT_NE(output.find("bind_us="), std::string::npos);
  EXPECT_NE(output.find("execute_us="), std::string::npos);
  EXPECT_NE(output.find("total_us="), std::string::npos);
  EXPECT_NE(output.find("rows_total=8"), std::string::npos);
  EXPECT_NE(output.find("verify_rows=8"), std::string::npos);
}

TEST(StmtPerfBenchRuntime, StmtInterlaceFewInterlaceReportsInsertedRows) {
  TAOS *taos = taos_connect("127.0.0.1", "root", "taosdata", nullptr, 6030);
  if (taos == nullptr) {
    GTEST_SKIP() << "TDengine is not available";
  }
  taos_close(taos);

  std::vector<std::string> args = {"stmtPerfBench", "--api=stmt", "--mode=interlace", "--scenario=few_interlace",
                                   "--tables=4", "--rows-per-table=4", "--batch-rows=1", "--warmup=0", "--cycles=1",
                                   "--db=stmt_perf_bench_runtime_stmt_interlace", "--output=table", "--drop-db=yes",
                                   "--verify=yes"};
  std::vector<char *> argv;
  argv.reserve(args.size());
  for (std::string &arg : args) {
    argv.push_back(&arg[0]);
  }

  testing::internal::CaptureStdout();
  int exitCode = stmtPerfBenchMain(static_cast<int>(argv.size()), argv.data());
  std::string output = testing::internal::GetCapturedStdout();

  ASSERT_EQ(exitCode, 0);
  EXPECT_NE(output.find("case=stmt/interlace/few_interlace"), std::string::npos);
  EXPECT_NE(output.find("rows_total=16"), std::string::npos);
  EXPECT_NE(output.find("verify_rows=16"), std::string::npos);
  EXPECT_NE(output.find("execute_calls="), std::string::npos);
}

TEST(StmtPerfBenchRuntime, Stmt2NormalSingleReportsSegmentedMetrics) {
  TAOS *taos = taos_connect("127.0.0.1", "root", "taosdata", nullptr, 6030);
  if (taos == nullptr) {
    GTEST_SKIP() << "TDengine is not available";
  }
  taos_close(taos);

  std::vector<std::string> args = {"stmtPerfBench", "--api=stmt2", "--mode=normal", "--scenario=single", "--tables=1",
                                   "--rows-per-table=8", "--batch-rows=2", "--warmup=0", "--cycles=1",
                                   "--db=stmt_perf_bench_runtime_stmt2_single", "--output=table", "--drop-db=yes",
                                   "--verify=yes"};
  std::vector<char *> argv;
  argv.reserve(args.size());
  for (std::string &arg : args) {
    argv.push_back(&arg[0]);
  }

  testing::internal::CaptureStdout();
  int exitCode = stmtPerfBenchMain(static_cast<int>(argv.size()), argv.data());
  std::string output = testing::internal::GetCapturedStdout();

  ASSERT_EQ(exitCode, 0);
  EXPECT_NE(output.find("case=stmt2/normal/single"), std::string::npos);
  EXPECT_NE(output.find("prepare_us="), std::string::npos);
  EXPECT_NE(output.find("bind_us="), std::string::npos);
  EXPECT_NE(output.find("execute_us="), std::string::npos);
  EXPECT_NE(output.find("rows_total=8"), std::string::npos);
  EXPECT_NE(output.find("bind_calls=1"), std::string::npos);
  EXPECT_NE(output.find("execute_calls=1"), std::string::npos);
  EXPECT_NE(output.find("verify_rows=8"), std::string::npos);
}

TEST(StmtPerfBenchRuntime, Stmt2InterlaceFewInterlaceReportsInsertedRows) {
  TAOS *taos = taos_connect("127.0.0.1", "root", "taosdata", nullptr, 6030);
  if (taos == nullptr) {
    GTEST_SKIP() << "TDengine is not available";
  }
  taos_close(taos);

  std::vector<std::string> args = {"stmtPerfBench", "--api=stmt2", "--mode=interlace", "--scenario=few_interlace",
                                   "--tables=4", "--rows-per-table=4", "--batch-rows=1", "--warmup=0", "--cycles=1",
                                   "--db=stmt_perf_bench_runtime_stmt2_interlace", "--output=table", "--drop-db=yes",
                                   "--verify=yes"};
  std::vector<char *> argv;
  argv.reserve(args.size());
  for (std::string &arg : args) {
    argv.push_back(&arg[0]);
  }

  testing::internal::CaptureStdout();
  int exitCode = stmtPerfBenchMain(static_cast<int>(argv.size()), argv.data());
  std::string output = testing::internal::GetCapturedStdout();

  ASSERT_EQ(exitCode, 0);
  EXPECT_NE(output.find("case=stmt2/interlace/few_interlace"), std::string::npos);
  EXPECT_NE(output.find("rows_total=16"), std::string::npos);
  EXPECT_NE(output.find("verify_rows=16"), std::string::npos);
  EXPECT_NE(output.find("execute_calls="), std::string::npos);
}

TEST(StmtPerfBenchRuntime, WritesJsonOutputToConfiguredFile) {
  TAOS *taos = taos_connect("127.0.0.1", "root", "taosdata", nullptr, 6030);
  if (taos == nullptr) {
    GTEST_SKIP() << "TDengine is not available";
  }
  taos_close(taos);

  char pathTemplate[] = "/tmp/stmt_perf_bench_output_XXXXXX";
  int  fd = mkstemp(pathTemplate);
  ASSERT_GE(fd, 0);
  close(fd);
  ASSERT_EQ(std::remove(pathTemplate), 0);

  std::string outputPath = std::string(pathTemplate) + ".json";

  std::vector<std::string> args = {"stmtPerfBench",
                                   "--api=stmt",
                                   "--mode=normal",
                                   "--scenario=single",
                                   "--tables=1",
                                   "--rows-per-table=2",
                                   "--batch-rows=1",
                                   "--warmup=0",
                                   "--cycles=1",
                                   "--db=stmt_perf_bench_runtime_output_file",
                                   "--output=json",
                                   "--output-file=" + outputPath,
                                   "--drop-db=yes",
                                   "--verify=yes"};
  std::vector<char *> argv;
  argv.reserve(args.size());
  for (std::string &arg : args) {
    argv.push_back(&arg[0]);
  }

  testing::internal::CaptureStdout();
  int exitCode = stmtPerfBenchMain(static_cast<int>(argv.size()), argv.data());
  std::string stdoutOutput = testing::internal::GetCapturedStdout();

  ASSERT_EQ(exitCode, 0);
  EXPECT_TRUE(stdoutOutput.empty());

  std::ifstream file(outputPath.c_str());
  ASSERT_TRUE(file.is_open());
  std::string fileOutput((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
  EXPECT_NE(fileOutput.find("\"api\":\"stmt\""), std::string::npos);
  EXPECT_NE(fileOutput.find("\"mode\":\"normal\""), std::string::npos);
  EXPECT_NE(fileOutput.find("\"verify_rows\":2"), std::string::npos);

  EXPECT_EQ(std::remove(outputPath.c_str()), 0);
}

TEST(StmtPerfBenchRuntime, VerboseMetricsAddsThroughputFields) {
  TAOS *taos = taos_connect("127.0.0.1", "root", "taosdata", nullptr, 6030);
  if (taos == nullptr) {
    GTEST_SKIP() << "TDengine is not available";
  }
  taos_close(taos);

  std::vector<std::string> args = {"stmtPerfBench",
                                   "--api=stmt",
                                   "--mode=normal",
                                   "--scenario=single",
                                   "--tables=1",
                                   "--rows-per-table=8",
                                   "--batch-rows=2",
                                   "--warmup=0",
                                   "--cycles=1",
                                   "--db=stmt_perf_bench_runtime_verbose_metrics",
                                   "--output=table",
                                   "--verbose-metrics=yes",
                                   "--drop-db=yes",
                                   "--verify=yes"};
  std::vector<char *> argv;
  argv.reserve(args.size());
  for (std::string &arg : args) {
    argv.push_back(&arg[0]);
  }

  testing::internal::CaptureStdout();
  int exitCode = stmtPerfBenchMain(static_cast<int>(argv.size()), argv.data());
  std::string output = testing::internal::GetCapturedStdout();

  ASSERT_EQ(exitCode, 0);
  EXPECT_NE(output.find("rows_per_sec="), std::string::npos);
  EXPECT_NE(output.find("bind_rows_per_sec="), std::string::npos);
}

TEST(StmtPerfBenchRuntime, MultiCaseTableOutputIncludesSummarySection) {
  TAOS *taos = taos_connect("127.0.0.1", "root", "taosdata", nullptr, 6030);
  if (taos == nullptr) {
    GTEST_SKIP() << "TDengine is not available";
  }
  taos_close(taos);

  std::vector<std::string> args = {"stmtPerfBench",
                                   "--api=all",
                                   "--mode=all",
                                   "--scenario=single",
                                   "--tables=1",
                                   "--rows-per-table=2",
                                   "--batch-rows=1",
                                   "--warmup=0",
                                   "--cycles=1",
                                   "--db=stmt_perf_bench_runtime_summary",
                                   "--output=table",
                                   "--drop-db=yes",
                                   "--verify=yes"};
  std::vector<char *> argv;
  argv.reserve(args.size());
  for (std::string &arg : args) {
    argv.push_back(&arg[0]);
  }

  testing::internal::CaptureStdout();
  int exitCode = stmtPerfBenchMain(static_cast<int>(argv.size()), argv.data());
  std::string output = testing::internal::GetCapturedStdout();

  ASSERT_EQ(exitCode, 0);
  EXPECT_NE(output.find("case=stmt/normal/single"), std::string::npos);
  EXPECT_NE(output.find("case=stmt2/interlace/single"), std::string::npos);
  EXPECT_NE(output.find("summary:"), std::string::npos);
  EXPECT_NE(output.find("rows_per_sec"), std::string::npos);
}
