#include "stmtPerfBenchCommon.h"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>
#include <unistd.h>
#include <vector>

#include "taos.h"
#include "taoserror.h"

namespace {

struct ScenarioSpec {
  StmtPerfBenchScenario scenario;
  const char           *name;
  int32_t               tables;
};

const ScenarioSpec kScenarioSpecs[] = {
    {StmtPerfBenchScenario::kSingle, "single", 1},
    {StmtPerfBenchScenario::kFewSeq, "few_seq", 4},
    {StmtPerfBenchScenario::kFewInterlace, "few_interlace", 4},
    {StmtPerfBenchScenario::kFewRevisit, "few_revisit", 4},
    {StmtPerfBenchScenario::kManySeq, "many_seq", 64},
    {StmtPerfBenchScenario::kManyInterlace, "many_interlace", 64},
    {StmtPerfBenchScenario::kManyRevisit, "many_revisit", 64},
};

const char *apiName(StmtPerfBenchApi api) {
  switch (api) {
    case StmtPerfBenchApi::kStmt:
      return "stmt";
    case StmtPerfBenchApi::kStmt2:
      return "stmt2";
    case StmtPerfBenchApi::kAll:
      return "all";
  }

  return "unknown";
}

const char *modeName(StmtPerfBenchMode mode) {
  switch (mode) {
    case StmtPerfBenchMode::kNormal:
      return "normal";
    case StmtPerfBenchMode::kInterlace:
      return "interlace";
    case StmtPerfBenchMode::kAll:
      return "all";
  }

  return "unknown";
}

const char *scenarioName(StmtPerfBenchScenario scenario) {
  switch (scenario) {
    case StmtPerfBenchScenario::kSingle:
      return "single";
    case StmtPerfBenchScenario::kFewSeq:
      return "few_seq";
    case StmtPerfBenchScenario::kFewInterlace:
      return "few_interlace";
    case StmtPerfBenchScenario::kFewRevisit:
      return "few_revisit";
    case StmtPerfBenchScenario::kManySeq:
      return "many_seq";
    case StmtPerfBenchScenario::kManyInterlace:
      return "many_interlace";
    case StmtPerfBenchScenario::kManyRevisit:
      return "many_revisit";
    case StmtPerfBenchScenario::kAll:
      return "all";
  }

  return "unknown";
}

bool parseApi(const std::string &value, StmtPerfBenchApi *api) {
  if (value == "stmt") {
    *api = StmtPerfBenchApi::kStmt;
    return true;
  }
  if (value == "stmt2") {
    *api = StmtPerfBenchApi::kStmt2;
    return true;
  }
  if (value == "all") {
    *api = StmtPerfBenchApi::kAll;
    return true;
  }

  return false;
}

bool parseMode(const std::string &value, StmtPerfBenchMode *mode) {
  if (value == "normal") {
    *mode = StmtPerfBenchMode::kNormal;
    return true;
  }
  if (value == "interlace") {
    *mode = StmtPerfBenchMode::kInterlace;
    return true;
  }
  if (value == "all") {
    *mode = StmtPerfBenchMode::kAll;
    return true;
  }

  return false;
}

bool parseScenario(const std::string &value, StmtPerfBenchScenario *scenario) {
  for (const ScenarioSpec &spec : kScenarioSpecs) {
    if (value == spec.name) {
      *scenario = spec.scenario;
      return true;
    }
  }
  if (value == "all") {
    *scenario = StmtPerfBenchScenario::kAll;
    return true;
  }

  return false;
}

bool parseOutput(const std::string &value, StmtPerfBenchOutput *output) {
  if (value == "table") {
    *output = StmtPerfBenchOutput::kTable;
    return true;
  }
  if (value == "csv") {
    *output = StmtPerfBenchOutput::kCsv;
    return true;
  }
  if (value == "json") {
    *output = StmtPerfBenchOutput::kJson;
    return true;
  }

  return false;
}

bool parseBool(const std::string &name, const std::string &value, bool *out, std::string *err) {
  if (value == "yes") {
    *out = true;
    return true;
  }
  if (value == "no") {
    *out = false;
    return true;
  }

  if (err != nullptr) {
    *err = "invalid " + name + " value: " + value;
  }
  return false;
}

bool parseInt32(const std::string &name, const std::string &value, int32_t minValue, int32_t *out, std::string *err) {
  char *end = nullptr;
  long  parsed = std::strtol(value.c_str(), &end, 10);
  if (end == value.c_str() || *end != '\0') {
    if (err != nullptr) {
      *err = "invalid " + name + " value: " + value;
    }
    return false;
  }
  if (parsed < minValue || parsed > std::numeric_limits<int32_t>::max()) {
    if (err != nullptr) {
      *err = "invalid " + name + " value: " + value;
    }
    return false;
  }

  *out = static_cast<int32_t>(parsed);
  return true;
}

bool parseUint16(const std::string &name, const std::string &value, uint16_t *out, std::string *err) {
  int32_t parsed = 0;
  if (!parseInt32(name, value, 1, &parsed, err)) {
    return false;
  }
  if (parsed > std::numeric_limits<uint16_t>::max()) {
    if (err != nullptr) {
      *err = "invalid " + name + " value: " + value;
    }
    return false;
  }

  *out = static_cast<uint16_t>(parsed);
  return true;
}

bool parseUint32(const std::string &name, const std::string &value, uint32_t *out, std::string *err) {
  int32_t parsed = 0;
  if (!parseInt32(name, value, 0, &parsed, err)) {
    return false;
  }

  *out = static_cast<uint32_t>(parsed);
  return true;
}

int32_t scenarioTableCount(StmtPerfBenchScenario scenario) {
  for (const ScenarioSpec &spec : kScenarioSpecs) {
    if (scenario == spec.scenario) {
      return spec.tables;
    }
  }

  return 0;
}

void appendRepeated(std::vector<int32_t> *tableOrder, int32_t tableId, int32_t rows) {
  for (int32_t i = 0; i < rows; ++i) {
    tableOrder->push_back(tableId);
  }
}

void buildSequentialOrder(int32_t tables, int32_t rowsPerTable, std::vector<int32_t> *tableOrder) {
  for (int32_t tableId = 0; tableId < tables; ++tableId) {
    appendRepeated(tableOrder, tableId, rowsPerTable);
  }
}

void buildInterlaceOrder(int32_t tables, int32_t rowsPerTable, std::vector<int32_t> *tableOrder) {
  for (int32_t rowId = 0; rowId < rowsPerTable; ++rowId) {
    for (int32_t tableId = 0; tableId < tables; ++tableId) {
      tableOrder->push_back(tableId);
    }
  }
}

void buildRevisitOrder(int32_t tables, int32_t rowsPerTable, std::vector<int32_t> *tableOrder) {
  int32_t pairRounds = rowsPerTable / 2;
  int32_t remainder = rowsPerTable % 2;

  for (int32_t tableId = 0; tableId + 1 < tables; tableId += 2) {
    for (int32_t round = 0; round < pairRounds; ++round) {
      tableOrder->push_back(tableId);
      tableOrder->push_back(tableId + 1);
      tableOrder->push_back(tableId);
      tableOrder->push_back(tableId + 1);
    }
  }

  if (remainder > 0) {
    for (int32_t tableId = 0; tableId < tables; ++tableId) {
      tableOrder->push_back(tableId);
    }
  }

  if ((tables % 2) != 0) {
    appendRepeated(tableOrder, tables - 1, rowsPerTable);
  }
}

bool buildTableOrder(const StmtPerfBenchCase &benchCase, std::vector<int32_t> *tableOrder, std::string *err) {
  if (benchCase.tables <= 0) {
    if (err != nullptr) {
      *err = "benchCase.tables must be positive";
    }
    return false;
  }
  if (benchCase.rowsPerTable <= 0) {
    if (err != nullptr) {
      *err = "benchCase.rowsPerTable must be positive";
    }
    return false;
  }

  tableOrder->clear();
  tableOrder->reserve(static_cast<size_t>(benchCase.tables) * static_cast<size_t>(benchCase.rowsPerTable));

  switch (benchCase.scenario) {
    case StmtPerfBenchScenario::kSingle:
    case StmtPerfBenchScenario::kFewSeq:
    case StmtPerfBenchScenario::kManySeq:
      buildSequentialOrder(benchCase.tables, benchCase.rowsPerTable, tableOrder);
      return true;
    case StmtPerfBenchScenario::kFewInterlace:
    case StmtPerfBenchScenario::kManyInterlace:
      buildInterlaceOrder(benchCase.tables, benchCase.rowsPerTable, tableOrder);
      return true;
    case StmtPerfBenchScenario::kFewRevisit:
    case StmtPerfBenchScenario::kManyRevisit:
      buildRevisitOrder(benchCase.tables, benchCase.rowsPerTable, tableOrder);
      return true;
    case StmtPerfBenchScenario::kAll:
      if (err != nullptr) {
        *err = "cannot build workload for scenario=all";
      }
      return false;
  }

  if (err != nullptr) {
    *err = "unknown scenario";
  }
  return false;
}

void fillGeneratedData(const StmtPerfBenchCase &benchCase, StmtPerfBenchWorkload *workload) {
  uint32_t state = benchCase.seed == 0 ? 1U : benchCase.seed;
  int64_t  baseTs = 1700000000000LL + static_cast<int64_t>(benchCase.seed) * 1000000LL;

  workload->timestamps.clear();
  workload->values.clear();
  workload->timestamps.reserve(workload->tableOrder.size());
  workload->values.reserve(workload->tableOrder.size());

  for (size_t rowId = 0; rowId < workload->tableOrder.size(); ++rowId) {
    state = state * 1664525U + 1013904223U;
    workload->timestamps.push_back(baseTs + static_cast<int64_t>(rowId));
    workload->values.push_back(static_cast<int32_t>((state >> 8) & 0x7fffffff));
  }
}

bool parseOption(const std::string &key, const std::string &value, StmtPerfBenchConfig *config, std::string *err) {
  if (key == "--api") {
    if (!parseApi(value, &config->api)) {
      if (err != nullptr) {
        *err = "invalid --api value: " + value;
      }
      return false;
    }
    return true;
  }

  if (key == "--mode") {
    if (!parseMode(value, &config->mode)) {
      if (err != nullptr) {
        *err = "invalid --mode value: " + value;
      }
      return false;
    }
    return true;
  }

  if (key == "--scenario") {
    if (!parseScenario(value, &config->scenario)) {
      if (err != nullptr) {
        *err = "invalid --scenario value: " + value;
      }
      return false;
    }
    return true;
  }

  if (key == "--output") {
    if (!parseOutput(value, &config->output)) {
      if (err != nullptr) {
        *err = "invalid --output value: " + value;
      }
      return false;
    }
    return true;
  }

  if (key == "--tables") {
    return parseInt32(key, value, 1, &config->tables, err);
  }
  if (key == "--rows-per-table") {
    return parseInt32(key, value, 1, &config->rowsPerTable, err);
  }
  if (key == "--batch-rows") {
    return parseInt32(key, value, 1, &config->batchRows, err);
  }
  if (key == "--warmup") {
    return parseInt32(key, value, 0, &config->warmup, err);
  }
  if (key == "--cycles") {
    return parseInt32(key, value, 1, &config->cycles, err);
  }
  if (key == "--seed") {
    return parseUint32(key, value, &config->seed, err);
  }
  if (key == "--host") {
    config->host = value;
    return true;
  }
  if (key == "--port") {
    return parseUint16(key, value, &config->port, err);
  }
  if (key == "--user") {
    config->user = value;
    return true;
  }
  if (key == "--pass") {
    config->pass = value;
    return true;
  }
  if (key == "--db") {
    config->db = value;
    return true;
  }
  if (key == "--drop-db") {
    return parseBool(key, value, &config->dropDb, err);
  }
  if (key == "--verify") {
    return parseBool(key, value, &config->verify, err);
  }
  if (key == "--output-file") {
    config->outputFile = value;
    return true;
  }
  if (key == "--verbose-metrics") {
    return parseBool(key, value, &config->verboseMetrics, err);
  }

  if (err != nullptr) {
    *err = "unknown option: " + key;
  }
  return false;
}

template <typename Argv>
bool parseConfigImpl(int argc, Argv argv, StmtPerfBenchConfig *config, std::string *err) {
  if (config == nullptr) {
    if (err != nullptr) {
      *err = "config pointer is null";
    }
    return false;
  }

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--help" || arg == "-h") {
      config->help = true;
      return true;
    }

    std::string key;
    std::string value;
    size_t      pos = arg.find('=');
    if (pos != std::string::npos) {
      key = arg.substr(0, pos);
      value = arg.substr(pos + 1);
    } else {
      key = arg;
      if (i + 1 >= argc) {
        if (err != nullptr) {
          *err = "missing value for option: " + arg;
        }
        return false;
      }
      value = argv[++i];
    }

    if (!parseOption(key, value, config, err)) {
      return false;
    }
  }

  return true;
}

bool executeQuery(TAOS *taos, const std::string &sql, std::string *err) {
  TAOS_RES *result = taos_query(taos, sql.c_str());
  if (result == nullptr) {
    if (err != nullptr) {
      *err = "query returned null for sql: " + sql;
    }
    return false;
  }

  int code = taos_errno(result);
  while (code == TSDB_CODE_MND_DB_IN_CREATING || code == TSDB_CODE_MND_DB_IN_DROPPING) {
    taos_free_result(result);
    usleep(200 * 1000);
    result = taos_query(taos, sql.c_str());
    if (result == nullptr) {
      if (err != nullptr) {
        *err = "query returned null for sql: " + sql;
      }
      return false;
    }
    code = taos_errno(result);
  }

  if (code != TSDB_CODE_SUCCESS) {
    if (err != nullptr) {
      *err = "sql failed: " + sql + ", err: " + taos_errstr(result);
    }
    taos_free_result(result);
    return false;
  }

  taos_free_result(result);
  return true;
}

bool fetchCount(TAOS *taos, const std::string &sql, int64_t *count, std::string *err) {
  TAOS_RES *result = taos_query(taos, sql.c_str());
  if (result == nullptr) {
    if (err != nullptr) {
      *err = "query returned null for sql: " + sql;
    }
    return false;
  }

  if (taos_errno(result) != TSDB_CODE_SUCCESS) {
    if (err != nullptr) {
      *err = "sql failed: " + sql + ", err: " + taos_errstr(result);
    }
    taos_free_result(result);
    return false;
  }

  TAOS_ROW row = taos_fetch_row(result);
  if (row == nullptr || row[0] == nullptr) {
    if (err != nullptr) {
      *err = "count query returned no rows: " + sql;
    }
    taos_free_result(result);
    return false;
  }

  *count = *(int64_t *)row[0];
  taos_free_result(result);
  return true;
}

bool connectTaos(const StmtPerfBenchConfig &config, TAOS **taos, std::string *err) {
  *taos = taos_connect(config.host.c_str(), config.user.c_str(), config.pass.c_str(), nullptr, config.port);
  if (*taos == nullptr) {
    if (err != nullptr) {
      *err = "failed to connect to TDengine: " + std::string(taos_errstr(nullptr));
    }
    return false;
  }

  return true;
}

bool setupEnvironment(TAOS *taos, const StmtPerfBenchCase &benchCase, const StmtPerfBenchNames &names, std::string *err) {
  std::string sql = "CREATE DATABASE IF NOT EXISTS " + names.dbName;
  if (!executeQuery(taos, sql, err)) {
    return false;
  }

  sql = "CREATE STABLE IF NOT EXISTS " + names.dbName + "." + names.stableName +
        " (ts TIMESTAMP, val INT) TAGS (group_id INT, location BINARY(32))";
  if (!executeQuery(taos, sql, err)) {
    return false;
  }

  for (int32_t i = 0; i < benchCase.tables; ++i) {
    char location[32] = {0};
    std::snprintf(location, sizeof(location), "loc_%04d", i);
    sql = "CREATE TABLE IF NOT EXISTS " + names.dbName + "." + names.tableNames[static_cast<size_t>(i)] + " USING " +
          names.dbName + "." + names.stableName + " TAGS (" + std::to_string(i) + ", '" + location + "')";
    if (!executeQuery(taos, sql, err)) {
      return false;
    }
  }

  return true;
}

bool verifyRowCount(TAOS *taos, const StmtPerfBenchNames &names, int64_t expectedRows, int64_t *actualRows,
                    std::string *err) {
  std::string sql = "SELECT COUNT(*) FROM " + names.dbName + "." + names.stableName;
  if (!fetchCount(taos, sql, actualRows, err)) {
    return false;
  }
  if (*actualRows != expectedRows) {
    if (err != nullptr) {
      *err = "row count mismatch, expected " + std::to_string(expectedRows) + ", actual " +
             std::to_string(*actualRows);
    }
    return false;
  }

  return true;
}

bool dropDatabase(TAOS *taos, const std::string &dbName, std::string *err) {
  return executeQuery(taos, "DROP DATABASE IF EXISTS " + dbName, err);
}

std::string makeRunId() {
  auto nowUs = std::chrono::duration_cast<std::chrono::microseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
                   .count();
  std::ostringstream oss;
  oss << nowUs << '_' << static_cast<long long>(getpid());
  return oss.str();
}

std::string describeCase(const StmtPerfBenchCase &benchCase) {
  return std::string(apiName(benchCase.api)) + "/" + modeName(benchCase.mode) + "/" + scenarioName(benchCase.scenario);
}

int64_t elapsedUs(std::chrono::steady_clock::time_point start, std::chrono::steady_clock::time_point end) {
  return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
}

bool stmtCodeOk(TAOS_STMT *stmt, int code, const char *action, std::string *err) {
  if (code == TSDB_CODE_SUCCESS) {
    return true;
  }

  if (err != nullptr) {
    *err = std::string(action) + " failed: " + taos_stmt_errstr(stmt);
  }
  return false;
}

bool bindStmtBatch(TAOS_STMT *stmt, const StmtPerfBenchWorkload &workload, size_t begin, int32_t rows,
                   int64_t tsOffset, std::string *err) {
  std::vector<int64_t> timestamps(static_cast<size_t>(rows));
  std::vector<int32_t> values(static_cast<size_t>(rows));
  std::vector<int32_t> tsLengths(static_cast<size_t>(rows), sizeof(int64_t));
  std::vector<int32_t> valueLengths(static_cast<size_t>(rows), sizeof(int32_t));

  for (int32_t i = 0; i < rows; ++i) {
    timestamps[static_cast<size_t>(i)] = workload.timestamps[begin + static_cast<size_t>(i)] + tsOffset;
    values[static_cast<size_t>(i)] = workload.values[begin + static_cast<size_t>(i)];
  }

  TAOS_MULTI_BIND params[2] = {0};
  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer = timestamps.data();
  params[0].buffer_length = sizeof(int64_t);
  params[0].length = tsLengths.data();
  params[0].is_null = nullptr;
  params[0].num = rows;

  params[1].buffer_type = TSDB_DATA_TYPE_INT;
  params[1].buffer = values.data();
  params[1].buffer_length = sizeof(int32_t);
  params[1].length = valueLengths.data();
  params[1].is_null = nullptr;
  params[1].num = rows;

  int code = taos_stmt_bind_param_batch(stmt, params);
  if (!stmtCodeOk(stmt, code, "taos_stmt_bind_param_batch", err)) {
    return false;
  }

  code = taos_stmt_add_batch(stmt);
  return stmtCodeOk(stmt, code, "taos_stmt_add_batch", err);
}

bool runStmtCycle(TAOS *taos, const StmtPerfBenchCase &benchCase, const StmtPerfBenchNames &names,
                  const StmtPerfBenchWorkload &workload, int32_t cycleOrdinal, StmtPerfBenchRunMetrics *metrics,
                  std::string *err) {
  if (metrics == nullptr) {
    if (err != nullptr) {
      *err = "metrics pointer is null";
    }
    return false;
  }

  TAOS_STMT_OPTIONS options = {0, true, true};
  TAOS_STMT        *stmt = benchCase.mode == StmtPerfBenchMode::kInterlace ? taos_stmt_init_with_options(taos, &options)
                                                                            : taos_stmt_init(taos);
  if (stmt == nullptr) {
    if (err != nullptr) {
      *err = "failed to initialize TAOS_STMT";
    }
    return false;
  }

  const int64_t tsOffset = static_cast<int64_t>(cycleOrdinal) * 1000000000LL;
  bool          ok = false;
  auto          totalStart = std::chrono::steady_clock::now();

  size_t pos = 0;

  auto prepareStart = std::chrono::steady_clock::now();
  int  code = taos_stmt_prepare(stmt, "INSERT INTO ? VALUES (?,?)", 0);
  auto prepareEnd = std::chrono::steady_clock::now();
  metrics->prepareUs = elapsedUs(prepareStart, prepareEnd);
  if (!stmtCodeOk(stmt, code, "taos_stmt_prepare", err)) {
    goto cleanup;
  }

  while (pos < workload.tableOrder.size()) {
    int32_t tableId = workload.tableOrder[pos];
    size_t  runEnd = pos + 1;
    while (runEnd < workload.tableOrder.size() && workload.tableOrder[runEnd] == tableId) {
      ++runEnd;
    }

    auto bindStart = std::chrono::steady_clock::now();
    code = taos_stmt_set_tbname(stmt, names.tableNames[static_cast<size_t>(tableId)].c_str());
    auto bindEnd = std::chrono::steady_clock::now();
    metrics->bindUs += elapsedUs(bindStart, bindEnd);
    if (!stmtCodeOk(stmt, code, "taos_stmt_set_tbname", err)) {
      goto cleanup;
    }

    size_t batchBegin = pos;
    while (batchBegin < runEnd) {
      int32_t batchRows = static_cast<int32_t>(runEnd - batchBegin);
      if (batchRows > benchCase.batchRows) {
        batchRows = benchCase.batchRows;
      }

      bindStart = std::chrono::steady_clock::now();
      if (!bindStmtBatch(stmt, workload, batchBegin, batchRows, tsOffset, err)) {
        goto cleanup;
      }
      bindEnd = std::chrono::steady_clock::now();
      metrics->bindUs += elapsedUs(bindStart, bindEnd);

      metrics->bindCalls += 1;
      metrics->addBatchCalls += 1;
      batchBegin += static_cast<size_t>(batchRows);
    }

    if (benchCase.mode == StmtPerfBenchMode::kNormal) {
      auto executeStart = std::chrono::steady_clock::now();
      code = taos_stmt_execute(stmt);
      auto executeEnd = std::chrono::steady_clock::now();
      metrics->executeUs += elapsedUs(executeStart, executeEnd);
      if (!stmtCodeOk(stmt, code, "taos_stmt_execute", err)) {
        goto cleanup;
      }
      metrics->executeCalls += 1;
    }

    pos = runEnd;
  }

  if (benchCase.mode == StmtPerfBenchMode::kInterlace && !workload.tableOrder.empty()) {
    auto executeStart = std::chrono::steady_clock::now();
    code = taos_stmt_execute(stmt);
    auto executeEnd = std::chrono::steady_clock::now();
    metrics->executeUs += elapsedUs(executeStart, executeEnd);
    if (!stmtCodeOk(stmt, code, "taos_stmt_execute", err)) {
      goto cleanup;
    }
    metrics->executeCalls += 1;
  }

  metrics->rowsTotal = workload.rowsTotal;
  metrics->tablesTotal = benchCase.tables;
  metrics->totalUs = elapsedUs(totalStart, std::chrono::steady_clock::now());
  ok = true;

cleanup:
  if (!ok) {
    metrics->totalUs = elapsedUs(totalStart, std::chrono::steady_clock::now());
  }
  taos_stmt_close(stmt);
  return ok;
}

bool stmt2CodeOk(TAOS_STMT2 *stmt, int code, const char *action, std::string *err) {
  if (code == TSDB_CODE_SUCCESS) {
    return true;
  }

  if (err != nullptr) {
    *err = std::string(action) + " failed: " + taos_stmt2_error(stmt);
  }
  return false;
}

struct Stmt2TableBindData {
  std::vector<int64_t> timestamps;
  std::vector<int32_t> values;
  std::vector<int32_t> tsLengths;
  std::vector<int32_t> valueLengths;
  TAOS_STMT2_BIND      cols[2];
};

void finalizeStmt2TableBindData(Stmt2TableBindData *data) {
  data->tsLengths.assign(data->timestamps.size(), sizeof(int64_t));
  data->valueLengths.assign(data->values.size(), sizeof(int32_t));

  data->cols[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  data->cols[0].buffer = data->timestamps.data();
  data->cols[0].length = data->tsLengths.data();
  data->cols[0].is_null = nullptr;
  data->cols[0].num = static_cast<int>(data->timestamps.size());

  data->cols[1].buffer_type = TSDB_DATA_TYPE_INT;
  data->cols[1].buffer = data->values.data();
  data->cols[1].length = data->valueLengths.data();
  data->cols[1].is_null = nullptr;
  data->cols[1].num = static_cast<int>(data->values.size());
}

void fillStmt2TableBindData(const StmtPerfBenchWorkload &workload, size_t begin, int32_t rows, int64_t tsOffset,
                            Stmt2TableBindData *data) {
  data->timestamps.resize(static_cast<size_t>(rows));
  data->values.resize(static_cast<size_t>(rows));
  for (int32_t i = 0; i < rows; ++i) {
    data->timestamps[static_cast<size_t>(i)] = workload.timestamps[begin + static_cast<size_t>(i)] + tsOffset;
    data->values[static_cast<size_t>(i)] = workload.values[begin + static_cast<size_t>(i)];
  }
  finalizeStmt2TableBindData(data);
}

bool runStmt2Cycle(TAOS *taos, const StmtPerfBenchCase &benchCase, const StmtPerfBenchNames &names,
                   const StmtPerfBenchWorkload &workload, int32_t cycleOrdinal, StmtPerfBenchRunMetrics *metrics,
                   std::string *err) {
  if (metrics == nullptr) {
    if (err != nullptr) {
      *err = "metrics pointer is null";
    }
    return false;
  }

  TAOS_STMT2_OPTION option = {0, benchCase.mode == StmtPerfBenchMode::kInterlace,
                              benchCase.mode == StmtPerfBenchMode::kInterlace, NULL, NULL};
  TAOS_STMT2       *stmt = taos_stmt2_init(taos, &option);
  if (stmt == nullptr) {
    if (err != nullptr) {
      *err = "failed to initialize TAOS_STMT2";
    }
    return false;
  }

  const int64_t tsOffset = static_cast<int64_t>(cycleOrdinal) * 1000000000LL;
  bool          ok = false;
  auto          totalStart = std::chrono::steady_clock::now();

  auto prepareStart = std::chrono::steady_clock::now();
  int  code = taos_stmt2_prepare(stmt, "INSERT INTO ? VALUES (?,?)", 0);
  auto prepareEnd = std::chrono::steady_clock::now();
  metrics->prepareUs = elapsedUs(prepareStart, prepareEnd);
  if (!stmt2CodeOk(stmt, code, "taos_stmt2_prepare", err)) {
    metrics->totalUs = elapsedUs(totalStart, std::chrono::steady_clock::now());
    taos_stmt2_close(stmt);
    return false;
  }

  std::vector<Stmt2TableBindData> tableData(static_cast<size_t>(benchCase.tables));
  for (size_t rowId = 0; rowId < workload.tableOrder.size(); ++rowId) {
    int32_t tableId = workload.tableOrder[rowId];
    tableData[static_cast<size_t>(tableId)].timestamps.push_back(workload.timestamps[rowId] + tsOffset);
    tableData[static_cast<size_t>(tableId)].values.push_back(workload.values[rowId]);
  }

  std::vector<char *>            tableNames;
  std::vector<TAOS_STMT2_BIND *> bindCols;
  tableNames.reserve(static_cast<size_t>(benchCase.tables));
  bindCols.reserve(static_cast<size_t>(benchCase.tables));

  auto bindStart = std::chrono::steady_clock::now();
  for (int32_t tableId = 0; tableId < benchCase.tables; ++tableId) {
    Stmt2TableBindData &data = tableData[static_cast<size_t>(tableId)];
    if (data.timestamps.empty()) {
      continue;
    }
    finalizeStmt2TableBindData(&data);
    tableNames.push_back(const_cast<char *>(names.tableNames[static_cast<size_t>(tableId)].c_str()));
    bindCols.push_back(&data.cols[0]);
  }

  TAOS_STMT2_BINDV bindv = {static_cast<int>(tableNames.size()), tableNames.data(), NULL, bindCols.data()};
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  auto bindEnd = std::chrono::steady_clock::now();
  metrics->bindUs += elapsedUs(bindStart, bindEnd);
  if (!stmt2CodeOk(stmt, code, "taos_stmt2_bind_param", err)) {
    metrics->totalUs = elapsedUs(totalStart, std::chrono::steady_clock::now());
    taos_stmt2_close(stmt);
    return false;
  }

  metrics->bindCalls += 1;

  int  affectedRows = 0;
  auto executeStart = std::chrono::steady_clock::now();
  code = taos_stmt2_exec(stmt, &affectedRows);
  auto executeEnd = std::chrono::steady_clock::now();
  metrics->executeUs += elapsedUs(executeStart, executeEnd);
  if (!stmt2CodeOk(stmt, code, "taos_stmt2_exec", err)) {
    metrics->totalUs = elapsedUs(totalStart, std::chrono::steady_clock::now());
    taos_stmt2_close(stmt);
    return false;
  }
  metrics->executeCalls += 1;

  metrics->rowsTotal = workload.rowsTotal;
  metrics->tablesTotal = benchCase.tables;
  metrics->totalUs = elapsedUs(totalStart, std::chrono::steady_clock::now());
  ok = true;

cleanup:
  if (!ok) {
    metrics->totalUs = elapsedUs(totalStart, std::chrono::steady_clock::now());
  }
  taos_stmt2_close(stmt);
  return ok;
}
void accumulateMetrics(const StmtPerfBenchRunMetrics &src, StmtPerfBenchRunMetrics *dst) {
  dst->prepareUs += src.prepareUs;
  dst->bindUs += src.bindUs;
  dst->executeUs += src.executeUs;
  dst->totalUs += src.totalUs;
  dst->rowsTotal += src.rowsTotal;
  dst->bindCalls += src.bindCalls;
  dst->addBatchCalls += src.addBatchCalls;
  dst->executeCalls += src.executeCalls;
  dst->tablesTotal = src.tablesTotal;
}

void finalizeMetrics(StmtPerfBenchRunMetrics *metrics) {
  if (metrics->totalUs > 0) {
    metrics->rowsPerSec = static_cast<double>(metrics->rowsTotal) * 1000000.0 / static_cast<double>(metrics->totalUs);
  }
  if (metrics->bindUs > 0) {
    metrics->bindRowsPerSec = static_cast<double>(metrics->rowsTotal) * 1000000.0 / static_cast<double>(metrics->bindUs);
  }
}

void printTableResult(std::ostream *out, const StmtPerfBenchRunResult &result, const StmtPerfBenchNames &names,
                      bool verboseMetrics) {
  *out << "case=" << describeCase(result.benchCase) << " db=" << names.dbName << " stable=" << names.stableName
       << " tables=" << result.benchCase.tables << " prepare_us=" << result.metrics.prepareUs
       << " bind_us=" << result.metrics.bindUs << " execute_us=" << result.metrics.executeUs
       << " total_us=" << result.metrics.totalUs << " rows_total=" << result.metrics.rowsTotal
       << " bind_calls=" << result.metrics.bindCalls << " add_batch_calls=" << result.metrics.addBatchCalls
       << " execute_calls=" << result.metrics.executeCalls << " verify_rows=" << result.actualRows;
  if (verboseMetrics) {
    *out << " rows_per_sec=" << result.metrics.rowsPerSec << " bind_rows_per_sec=" << result.metrics.bindRowsPerSec;
  }
  *out << std::endl;
}

void printTableSummary(std::ostream *out, const std::vector<StmtPerfBenchRunResult> &results, bool verboseMetrics) {
  if (results.size() <= 1) {
    return;
  }

  size_t caseWidth = std::string("case").size();
  for (const StmtPerfBenchRunResult &result : results) {
    caseWidth = std::max(caseWidth, describeCase(result.benchCase).size());
  }

  *out << "summary:" << std::endl;

  std::ios::fmtflags oldFlags = out->flags();
  std::streamsize    oldPrecision = out->precision();
  char               oldFill = out->fill();

  *out << std::left << std::setw(static_cast<int>(caseWidth)) << "case"
       << "  " << std::right << std::setw(6) << "tables"
       << "  " << std::setw(8) << "rows"
       << "  " << std::setw(10) << "total_us"
       << "  " << std::setw(10) << "bind_us"
       << "  " << std::setw(10) << "exec_us"
       << "  " << std::setw(12) << "rows_per_sec";
  if (verboseMetrics) {
    *out << "  " << std::setw(16) << "bind_rows_per_sec";
  }
  *out << "  " << std::setw(10) << "exec_calls"
       << "  " << std::setw(10) << "verify_rows" << std::endl;

  *out << std::fixed << std::setprecision(1);
  for (const StmtPerfBenchRunResult &result : results) {
    *out << std::left << std::setw(static_cast<int>(caseWidth)) << describeCase(result.benchCase)
         << "  " << std::right << std::setw(6) << result.benchCase.tables
         << "  " << std::setw(8) << result.metrics.rowsTotal
         << "  " << std::setw(10) << result.metrics.totalUs
         << "  " << std::setw(10) << result.metrics.bindUs
         << "  " << std::setw(10) << result.metrics.executeUs
         << "  " << std::setw(12) << result.metrics.rowsPerSec;
    if (verboseMetrics) {
      *out << "  " << std::setw(16) << result.metrics.bindRowsPerSec;
    }
    *out << "  " << std::setw(10) << result.metrics.executeCalls
         << "  " << std::setw(10) << result.actualRows << std::endl;
  }

  out->flags(oldFlags);
  out->precision(oldPrecision);
  out->fill(oldFill);
}

void printCsvResults(std::ostream *out, const std::vector<StmtPerfBenchRunResult> &results, bool verboseMetrics) {
  *out << "api,mode,scenario,tables,prepare_us,bind_us,execute_us,total_us,rows_total,bind_calls,add_batch_calls,execute_calls,verify_rows";
  if (verboseMetrics) {
    *out << ",rows_per_sec,bind_rows_per_sec";
  }
  *out << std::endl;
  for (const StmtPerfBenchRunResult &result : results) {
    *out << apiName(result.benchCase.api) << ',' << modeName(result.benchCase.mode) << ','
         << scenarioName(result.benchCase.scenario) << ',' << result.benchCase.tables << ','
         << result.metrics.prepareUs << ',' << result.metrics.bindUs << ',' << result.metrics.executeUs << ','
         << result.metrics.totalUs << ',' << result.metrics.rowsTotal << ',' << result.metrics.bindCalls << ','
         << result.metrics.addBatchCalls << ',' << result.metrics.executeCalls << ',' << result.actualRows;
    if (verboseMetrics) {
      *out << ',' << result.metrics.rowsPerSec << ',' << result.metrics.bindRowsPerSec;
    }
    *out << std::endl;
  }
}

void printJsonResults(std::ostream *out, const std::vector<StmtPerfBenchRunResult> &results, bool verboseMetrics) {
  *out << "{\"results\":[";
  for (size_t i = 0; i < results.size(); ++i) {
    const StmtPerfBenchRunResult &result = results[i];
    if (i > 0) {
      *out << ',';
    }
    *out << "{\"api\":\"" << apiName(result.benchCase.api) << "\",\"mode\":\"" << modeName(result.benchCase.mode)
         << "\",\"scenario\":\"" << scenarioName(result.benchCase.scenario) << "\",\"tables\":"
         << result.benchCase.tables << ",\"prepare_us\":" << result.metrics.prepareUs << ",\"bind_us\":"
         << result.metrics.bindUs << ",\"execute_us\":" << result.metrics.executeUs << ",\"total_us\":"
         << result.metrics.totalUs << ",\"rows_total\":" << result.metrics.rowsTotal << ",\"bind_calls\":"
         << result.metrics.bindCalls << ",\"add_batch_calls\":" << result.metrics.addBatchCalls
         << ",\"execute_calls\":" << result.metrics.executeCalls << ",\"verify_rows\":" << result.actualRows;
    if (verboseMetrics) {
      *out << ",\"rows_per_sec\":" << result.metrics.rowsPerSec << ",\"bind_rows_per_sec\":"
           << result.metrics.bindRowsPerSec;
    }
    *out << '}';
  }
  *out << "]}" << std::endl;
}
}  // namespace

bool stmtPerfBenchParseArgs(int argc, char *const argv[], StmtPerfBenchConfig *config, std::string *err) {
  return parseConfigImpl(argc, argv, config, err);
}

bool stmtPerfBenchParseConfig(int argc, const char *const argv[], StmtPerfBenchConfig *config, std::string *err) {
  return parseConfigImpl(argc, argv, config, err);
}

bool stmtPerfBenchExpandCases(const StmtPerfBenchConfig &config, std::vector<StmtPerfBenchCase> *cases,
                              std::string *err) {
  if (cases == nullptr) {
    if (err != nullptr) {
      *err = "cases pointer is null";
    }
    return false;
  }

  cases->clear();

  std::vector<StmtPerfBenchApi> apis;
  if (config.api == StmtPerfBenchApi::kAll) {
    apis.push_back(StmtPerfBenchApi::kStmt);
    apis.push_back(StmtPerfBenchApi::kStmt2);
  } else {
    apis.push_back(config.api);
  }

  std::vector<StmtPerfBenchMode> modes;
  if (config.mode == StmtPerfBenchMode::kAll) {
    modes.push_back(StmtPerfBenchMode::kNormal);
    modes.push_back(StmtPerfBenchMode::kInterlace);
  } else {
    modes.push_back(config.mode);
  }

  std::vector<StmtPerfBenchScenario> scenarios;
  if (config.scenario == StmtPerfBenchScenario::kAll) {
    for (const ScenarioSpec &spec : kScenarioSpecs) {
      scenarios.push_back(spec.scenario);
    }
  } else {
    scenarios.push_back(config.scenario);
  }

  for (StmtPerfBenchApi api : apis) {
    for (StmtPerfBenchMode mode : modes) {
      for (StmtPerfBenchScenario scenario : scenarios) {
        int32_t tables = config.tables > 0 ? config.tables : scenarioTableCount(scenario);
        if (tables <= 0) {
          if (err != nullptr) {
            *err = "failed to resolve scenario table count";
          }
          return false;
        }

        cases->push_back({api, mode, scenario, tables, config.rowsPerTable, config.batchRows, config.warmup,
                          config.cycles, config.seed});
      }
    }
  }

  return true;
}

std::vector<StmtPerfBenchCase> stmtPerfBenchExpandCases(const StmtPerfBenchConfig &config) {
  std::vector<StmtPerfBenchCase> cases;
  std::string                    err;
  if (!stmtPerfBenchExpandCases(config, &cases, &err)) {
    return {};
  }

  return cases;
}

bool stmtPerfBenchBuildWorkload(const StmtPerfBenchCase &benchCase, StmtPerfBenchWorkload *workload,
                                std::string *err) {
  if (workload == nullptr) {
    if (err != nullptr) {
      *err = "workload pointer is null";
    }
    return false;
  }

  if (!buildTableOrder(benchCase, &workload->tableOrder, err)) {
    return false;
  }

  fillGeneratedData(benchCase, workload);
  workload->rowsTotal = static_cast<int32_t>(workload->tableOrder.size());
  return true;
}

StmtPerfBenchWorkload stmtPerfBenchBuildWorkload(const StmtPerfBenchCase &benchCase) {
  StmtPerfBenchWorkload workload;
  std::string           err;
  if (!stmtPerfBenchBuildWorkload(benchCase, &workload, &err)) {
    return {};
  }

  return workload;
}

bool stmtPerfBenchBuildNames(const StmtPerfBenchConfig &config, const StmtPerfBenchCase &benchCase,
                             const std::string &runId, StmtPerfBenchNames *names, std::string *err) {
  if (names == nullptr) {
    if (err != nullptr) {
      *err = "names pointer is null";
    }
    return false;
  }
  if (runId.empty()) {
    if (err != nullptr) {
      *err = "runId must not be empty";
    }
    return false;
  }

  names->dbName = config.db.empty() ? "stmt_perf_bench_" + runId : config.db;
  names->stableName = std::string("stb_") + runId + '_' + apiName(benchCase.api) + '_' + modeName(benchCase.mode) + '_' +
                      scenarioName(benchCase.scenario);
  names->tableNames.clear();
  names->tableNames.reserve(static_cast<size_t>(benchCase.tables));
  for (int32_t i = 0; i < benchCase.tables; ++i) {
    char suffix[16] = {0};
    std::snprintf(suffix, sizeof(suffix), "%04d", i);
    names->tableNames.push_back(std::string("tb_") + runId + '_' + apiName(benchCase.api) + '_' + modeName(benchCase.mode) +
                                '_' + scenarioName(benchCase.scenario) + '_' + suffix);
  }

  return true;
}

StmtPerfBenchNames stmtPerfBenchBuildNames(const StmtPerfBenchConfig &config, const StmtPerfBenchCase &benchCase,
                                           const std::string &runId) {
  StmtPerfBenchNames names;
  std::string        err;
  if (!stmtPerfBenchBuildNames(config, benchCase, runId, &names, &err)) {
    return {};
  }

  return names;
}

bool stmtPerfBenchRunCase(TAOS *taos, const StmtPerfBenchConfig &config, const StmtPerfBenchCase &benchCase,
                          const StmtPerfBenchNames &names, const StmtPerfBenchWorkload &workload,
                          StmtPerfBenchRunResult *result, std::string *err) {
  if (result == nullptr) {
    if (err != nullptr) {
      *err = "result pointer is null";
    }
    return false;
  }

  *result = StmtPerfBenchRunResult{};
  result->benchCase = benchCase;
  result->expectedRows = static_cast<int64_t>(workload.rowsTotal) * static_cast<int64_t>(benchCase.warmup + benchCase.cycles);

  if (!executeQuery(taos, "USE " + names.dbName, err)) {
    result->errorCode = -1;
    if (err != nullptr) {
      result->errorMessage = *err;
    }
    return false;
  }

    for (int32_t cycle = 0; cycle < benchCase.warmup + benchCase.cycles; ++cycle) {
    StmtPerfBenchRunMetrics cycleMetrics;
    bool ok = false;
    if (benchCase.api == StmtPerfBenchApi::kStmt) {
      ok = runStmtCycle(taos, benchCase, names, workload, cycle, &cycleMetrics, err);
    } else if (benchCase.api == StmtPerfBenchApi::kStmt2) {
      ok = runStmt2Cycle(taos, benchCase, names, workload, cycle, &cycleMetrics, err);
    } else {
      if (err != nullptr) {
        *err = "unsupported benchmark api";
      }
    }

    if (!ok) {
      result->errorCode = -1;
      if (err != nullptr) {
        result->errorMessage = *err;
      }
      return false;
    }

    if (cycle >= benchCase.warmup) {
      accumulateMetrics(cycleMetrics, &result->metrics);
    }
  }

  finalizeMetrics(&result->metrics);

  if (config.verify) {
    if (!verifyRowCount(taos, names, result->expectedRows, &result->actualRows, err)) {
      result->errorCode = -1;
      if (err != nullptr) {
        result->errorMessage = *err;
      }
      return false;
    }
    return true;
  }

  std::string sql = "SELECT COUNT(*) FROM " + names.dbName + "." + names.stableName;
  if (!fetchCount(taos, sql, &result->actualRows, err)) {
    result->errorCode = -1;
    if (err != nullptr) {
      result->errorMessage = *err;
    }
    return false;
  }

  return true;
}
const char *stmtPerfBenchUsage() {
  return "Usage: stmtPerfBench [-h|--help] [--api stmt|stmt2|all] [--mode normal|interlace|all] "
         "[--scenario single|few_seq|few_interlace|few_revisit|many_seq|many_interlace|many_revisit|all] "
         "[--tables N] [--rows-per-table N] [--batch-rows N] [--warmup N] [--cycles N] [--seed N] "
         "[--host HOST] [--port PORT] [--user USER] [--pass PASS] [--db NAME] [--drop-db yes|no] "
         "[--verify yes|no] [--output table|csv|json] [--output-file PATH] [--verbose-metrics yes|no]";
}

int stmtPerfBenchMain(int argc, char **argv) {
  StmtPerfBenchConfig config;
  std::string         err;
  if (!stmtPerfBenchParseArgs(argc, argv, &config, &err)) {
    std::cerr << err << "\n" << stmtPerfBenchUsage() << std::endl;
    return 1;
  }

  if (config.help) {
    std::cout << stmtPerfBenchUsage() << std::endl;
    return 0;
  }

  std::vector<StmtPerfBenchCase> cases;
  if (!stmtPerfBenchExpandCases(config, &cases, &err)) {
    std::cerr << err << std::endl;
    return 1;
  }
  if (cases.empty()) {
    std::cerr << "no benchmark cases selected" << std::endl;
    return 1;
  }

  TAOS *taos = nullptr;
  if (!connectTaos(config, &taos, &err)) {
    std::cerr << err << std::endl;
    return 1;
  }

  const std::string runId = makeRunId();
  std::vector<StmtPerfBenchNames> namesList;
  namesList.reserve(cases.size());
  for (const StmtPerfBenchCase &benchCase : cases) {
    StmtPerfBenchNames names;
    if (!stmtPerfBenchBuildNames(config, benchCase, runId, &names, &err)) {
      std::cerr << err << std::endl;
      taos_close(taos);
      return 1;
    }
    namesList.push_back(names);
  }

  if (config.dropDb && !namesList.empty()) {
    if (!dropDatabase(taos, namesList[0].dbName, &err)) {
      std::cerr << err << std::endl;
      taos_close(taos);
      return 1;
    }
  }

  int                                exitCode = 0;
  std::vector<StmtPerfBenchRunResult> results;
  std::vector<StmtPerfBenchNames>     resultNames;
  results.reserve(cases.size());
  resultNames.reserve(cases.size());

  for (size_t i = 0; i < cases.size(); ++i) {
    const StmtPerfBenchCase  &benchCase = cases[i];
    const StmtPerfBenchNames &names = namesList[i];

    StmtPerfBenchWorkload workload;
    if (!stmtPerfBenchBuildWorkload(benchCase, &workload, &err)) {
      std::cerr << err << std::endl;
      exitCode = 1;
      continue;
    }

    if (!setupEnvironment(taos, benchCase, names, &err)) {
      std::cerr << err << std::endl;
      exitCode = 1;
      continue;
    }

    StmtPerfBenchRunResult result;
    if (!stmtPerfBenchRunCase(taos, config, benchCase, names, workload, &result, &err)) {
      std::cerr << describeCase(benchCase) << ": " << err << std::endl;
      exitCode = 1;
      continue;
    }

    results.push_back(result);
    resultNames.push_back(names);
  }

  std::ofstream fileOutput;
  std::ostream *out = &std::cout;
  if (!config.outputFile.empty()) {
    fileOutput.open(config.outputFile.c_str(), std::ios::out | std::ios::trunc);
    if (!fileOutput.is_open()) {
      std::cerr << "failed to open output file: " << config.outputFile << std::endl;
      taos_close(taos);
      return 1;
    }
    out = &fileOutput;
  }

  if (config.output == StmtPerfBenchOutput::kCsv) {
    printCsvResults(out, results, config.verboseMetrics);
  } else if (config.output == StmtPerfBenchOutput::kJson) {
    printJsonResults(out, results, config.verboseMetrics);
  } else {
    for (size_t i = 0; i < results.size(); ++i) {
      printTableResult(out, results[i], resultNames[i], config.verboseMetrics);
    }
    printTableSummary(out, results, config.verboseMetrics);
  }

  if (config.dropDb && !namesList.empty()) {
    std::string dropErr;
    if (!dropDatabase(taos, namesList[0].dbName, &dropErr)) {
      std::cerr << dropErr << std::endl;
      exitCode = 1;
    }
  }

  taos_close(taos);
  return exitCode;
}
