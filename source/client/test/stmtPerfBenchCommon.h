#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "taos.h"

enum class StmtPerfBenchApi {
  kStmt,
  kStmt2,
  kAll,
};

enum class StmtPerfBenchMode {
  kNormal,
  kInterlace,
  kAll,
};

enum class StmtPerfBenchScenario {
  kSingle,
  kFewSeq,
  kFewInterlace,
  kFewRevisit,
  kManySeq,
  kManyInterlace,
  kManyRevisit,
  kAll,
};

enum class StmtPerfBenchOutput {
  kTable,
  kCsv,
  kJson,
};

struct StmtPerfBenchConfig {
  StmtPerfBenchApi      api = StmtPerfBenchApi::kAll;
  StmtPerfBenchMode     mode = StmtPerfBenchMode::kAll;
  StmtPerfBenchScenario scenario = StmtPerfBenchScenario::kSingle;
  StmtPerfBenchOutput   output = StmtPerfBenchOutput::kTable;
  int32_t               tables = 0;
  int32_t               rowsPerTable = 1000;
  int32_t               batchRows = 1;
  int32_t               warmup = 1;
  int32_t               cycles = 3;
  uint32_t              seed = 1;
  std::string           host = "127.0.0.1";
  uint16_t              port = 6030;
  std::string           user = "root";
  std::string           pass = "taosdata";
  std::string           db;
  bool                  dropDb = true;
  bool                  verify = true;
  std::string           outputFile;
  bool                  verboseMetrics = false;
  bool                  help = false;
};

struct StmtPerfBenchCase {
  StmtPerfBenchApi      api = StmtPerfBenchApi::kStmt;
  StmtPerfBenchMode     mode = StmtPerfBenchMode::kNormal;
  StmtPerfBenchScenario scenario = StmtPerfBenchScenario::kSingle;
  int32_t               tables = 0;
  int32_t               rowsPerTable = 0;
  int32_t               batchRows = 0;
  int32_t               warmup = 0;
  int32_t               cycles = 0;
  uint32_t              seed = 0;

  StmtPerfBenchCase() = default;
  StmtPerfBenchCase(StmtPerfBenchApi apiValue, StmtPerfBenchMode modeValue,
                    StmtPerfBenchScenario scenarioValue, int32_t tablesValue, int32_t rowsPerTableValue,
                    int32_t batchRowsValue, int32_t warmupValue, int32_t cyclesValue, uint32_t seedValue)
      : api(apiValue),
        mode(modeValue),
        scenario(scenarioValue),
        tables(tablesValue),
        rowsPerTable(rowsPerTableValue),
        batchRows(batchRowsValue),
        warmup(warmupValue),
        cycles(cyclesValue),
        seed(seedValue) {}
};

struct StmtPerfBenchWorkload {
  std::vector<int32_t> tableOrder;
  std::vector<int64_t> timestamps;
  std::vector<int32_t> values;
  int32_t              rowsTotal = 0;
};

struct StmtPerfBenchNames {
  std::string              dbName;
  std::string              stableName;
  std::vector<std::string> tableNames;
};

struct StmtPerfBenchRunMetrics {
  int64_t prepareUs = 0;
  int64_t bindUs = 0;
  int64_t executeUs = 0;
  int64_t totalUs = 0;
  int32_t rowsTotal = 0;
  int32_t tablesTotal = 0;
  int32_t bindCalls = 0;
  int32_t addBatchCalls = 0;
  int32_t executeCalls = 0;
  double  rowsPerSec = 0.0;
  double  bindRowsPerSec = 0.0;
};

struct StmtPerfBenchRunResult {
  StmtPerfBenchCase       benchCase;
  StmtPerfBenchRunMetrics metrics;
  int64_t                 expectedRows = 0;
  int64_t                 actualRows = 0;
  int32_t                 errorCode = 0;
  std::string             errorMessage;
};

bool stmtPerfBenchParseArgs(int argc, char *const argv[], StmtPerfBenchConfig *config, std::string *err);
bool stmtPerfBenchParseConfig(int argc, const char *const argv[], StmtPerfBenchConfig *config, std::string *err);

bool stmtPerfBenchExpandCases(const StmtPerfBenchConfig &config, std::vector<StmtPerfBenchCase> *cases,
                              std::string *err);
std::vector<StmtPerfBenchCase> stmtPerfBenchExpandCases(const StmtPerfBenchConfig &config);

bool stmtPerfBenchBuildWorkload(const StmtPerfBenchCase &benchCase, StmtPerfBenchWorkload *workload,
                                std::string *err);
StmtPerfBenchWorkload stmtPerfBenchBuildWorkload(const StmtPerfBenchCase &benchCase);

bool stmtPerfBenchBuildNames(const StmtPerfBenchConfig &config, const StmtPerfBenchCase &benchCase,
                             const std::string &runId, StmtPerfBenchNames *names, std::string *err);
StmtPerfBenchNames stmtPerfBenchBuildNames(const StmtPerfBenchConfig &config, const StmtPerfBenchCase &benchCase,
                                           const std::string &runId);

bool stmtPerfBenchRunCase(TAOS *taos, const StmtPerfBenchConfig &config, const StmtPerfBenchCase &benchCase,
                          const StmtPerfBenchNames &names, const StmtPerfBenchWorkload &workload,
                          StmtPerfBenchRunResult *result, std::string *err);

const char *stmtPerfBenchUsage();
int         stmtPerfBenchMain(int argc, char **argv);
