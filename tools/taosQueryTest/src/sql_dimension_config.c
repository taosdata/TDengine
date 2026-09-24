/* Initial compiled FRAG/VAL catalog; later entries append without ID reuse. */

#include "qst_sql_dimension_config.h"

#include <stddef.h>

#include "sql_dimension_ids.h"

static const SQstGeneratorChildCall kRootChildren[] = {
    {SQL_DIMENSION_FRAG_QUERY_TOP_LEVEL, QST_GENERATOR_CALL_REQUIRED, 0, 0, 0,
     0},
    {SQL_DIMENSION_FRAG_QUERY_TERMINATOR, QST_GENERATOR_CALL_REQUIRED, 0, 0,
     0, 0},
};
static const SQstGeneratorChildCall kTopLevelChildren[] = {
    {SQL_DIMENSION_FRAG_QUERY_STMT, QST_GENERATOR_CALL_REQUIRED, 0, 0, 0, 0},
};
static const SQstGeneratorChildCall kStatementSelectChildren[] = {
    {SQL_DIMENSION_FRAG_LITERAL, QST_GENERATOR_CALL_REQUIRED, 0, 0, 0, 0},
};

static const SQstGeneratorValue kQueryRootValues[] = {
    {SQL_DIMENSION_VAL_QUERY_ROOT, QST_GENERATOR_VALUE_COMPOSE,
     QST_GENERATOR_CLASS_NORMAL, QST_GENERATOR_RETURN_NON_EMPTY, 1,
     sizeof(kRootChildren) / sizeof(kRootChildren[0]), 0, kRootChildren, ""},
};
static const SQstGeneratorValue kQueryTopLevelValues[] = {
    {SQL_DIMENSION_VAL_QUERY_TOP_LEVEL_STMT, QST_GENERATOR_VALUE_COMPOSE,
     QST_GENERATOR_CLASS_NORMAL, QST_GENERATOR_RETURN_NON_EMPTY, 1,
     sizeof(kTopLevelChildren) / sizeof(kTopLevelChildren[0]), 0,
     kTopLevelChildren, ""},
};
static const SQstGeneratorValue kQueryStatementValues[] = {
    {SQL_DIMENSION_VAL_QUERY_STMT_SELECT, QST_GENERATOR_VALUE_COMPOSE,
     QST_GENERATOR_CLASS_NORMAL, QST_GENERATOR_RETURN_NON_EMPTY, 950000,
     sizeof(kStatementSelectChildren) / sizeof(kStatementSelectChildren[0]),
     0, kStatementSelectChildren, "SELECT "},
    {SQL_DIMENSION_VAL_QUERY_STMT_ABNORMAL, QST_GENERATOR_VALUE_COMPOSE,
     QST_GENERATOR_CLASS_ABNORMAL, QST_GENERATOR_RETURN_NON_EMPTY, 50000,
     sizeof(kStatementSelectChildren) / sizeof(kStatementSelectChildren[0]),
     0, kStatementSelectChildren, "SELEC "},
    {SQL_DIMENSION_VAL_QUERY_STMT_COUNT, QST_GENERATOR_VALUE_ATOMIC,
     QST_GENERATOR_CLASS_NORMAL, QST_GENERATOR_RETURN_NON_EMPTY, 950000, 0,
     0, NULL, "SELECT COUNT(*) FROM db_frag.fq_local_series"},
    {SQL_DIMENSION_VAL_QUERY_STMT_FILTER, QST_GENERATOR_VALUE_ATOMIC,
     QST_GENERATOR_CLASS_NORMAL, QST_GENERATOR_RETURN_NON_EMPTY, 950000, 0,
     0, NULL, "SELECT device_id, reading FROM db_frag.fq_local_series WHERE reading >= 10"},
    {SQL_DIMENSION_VAL_QUERY_STMT_ORDER, QST_GENERATOR_VALUE_ATOMIC,
     QST_GENERATOR_CLASS_NORMAL, QST_GENERATOR_RETURN_NON_EMPTY, 950000, 0,
     0, NULL, "SELECT ts, device_id, region FROM db_frag.fq_local_series ORDER BY ts DESC"},
    {SQL_DIMENSION_VAL_QUERY_STMT_JOIN, QST_GENERATOR_VALUE_ATOMIC,
     QST_GENERATOR_CLASS_NORMAL, QST_GENERATOR_RETURN_NON_EMPTY, 950000, 0,
     0, NULL, "SELECT s.device_id, d.device_name FROM db_frag.fq_local_series s JOIN db_frag.fq_local_device d ON s.ts = d.ts AND s.device_id = d.device_id"},
    {SQL_DIMENSION_VAL_QUERY_STMT_AGGREGATE, QST_GENERATOR_VALUE_ATOMIC,
     QST_GENERATOR_CLASS_NORMAL, QST_GENERATOR_RETURN_NON_EMPTY, 950000, 0,
     0, NULL, "SELECT region, AVG(reading), MIN(reading), MAX(reading) FROM db_frag.fq_local_series GROUP BY region"},
    {SQL_DIMENSION_VAL_QUERY_STMT_TYPES, QST_GENERATOR_VALUE_ATOMIC,
     QST_GENERATOR_CLASS_NORMAL, QST_GENERATOR_RETURN_NON_EMPTY, 950000, 0,
     0, NULL, "SELECT c_bool, c_int, c_double, c_binary, c_varbinary FROM db_frag.frag_all_types ORDER BY ts"},
    {SQL_DIMENSION_VAL_QUERY_STMT_NULLS, QST_GENERATOR_VALUE_ATOMIC,
     QST_GENERATOR_CLASS_NORMAL, QST_GENERATOR_RETURN_NON_EMPTY, 950000, 0,
     0, NULL, "SELECT ts, note, payload FROM db_frag.fq_local_series WHERE note IS NULL OR payload IS NOT NULL"},
    {SQL_DIMENSION_VAL_QUERY_STMT_SUBQUERY, QST_GENERATOR_VALUE_ATOMIC,
     QST_GENERATOR_CLASS_NORMAL, QST_GENERATOR_RETURN_NON_EMPTY, 950000, 0,
     0, NULL, "SELECT device_id FROM (SELECT device_id, reading FROM db_frag.fq_local_series WHERE reading > 0) q ORDER BY device_id"},
    {SQL_DIMENSION_VAL_QUERY_STMT_ABNORMAL_TABLE, QST_GENERATOR_VALUE_ATOMIC,
     QST_GENERATOR_CLASS_ABNORMAL, QST_GENERATOR_RETURN_NON_EMPTY, 50000, 0,
     0, NULL, "SELECT * FROM db_frag.qst_missing_table"},
    {SQL_DIMENSION_VAL_QUERY_STMT_ABNORMAL_SYNTAX, QST_GENERATOR_VALUE_ATOMIC,
     QST_GENERATOR_CLASS_ABNORMAL, QST_GENERATOR_RETURN_NON_EMPTY, 50000, 0,
     0, NULL, "SELECT FROM db_frag.fq_local_series"},
};
static const SQstGeneratorValue kQueryTerminatorValues[] = {
    {SQL_DIMENSION_VAL_QUERY_TERMINATOR_SEMICOLON, QST_GENERATOR_VALUE_ATOMIC,
     QST_GENERATOR_CLASS_NORMAL, QST_GENERATOR_RETURN_NON_EMPTY, 950000, 0,
     0, NULL, ";"},
    {SQL_DIMENSION_VAL_QUERY_TERMINATOR_EMPTY, QST_GENERATOR_VALUE_ATOMIC,
     QST_GENERATOR_CLASS_ABNORMAL, QST_GENERATOR_RETURN_NON_EMPTY, 50000, 0,
     0, NULL, ""},
};
static const SQstGeneratorValue kLiteralValues[] = {
    {SQL_DIMENSION_VAL_LITERAL_ZERO, QST_GENERATOR_VALUE_ATOMIC,
     QST_GENERATOR_CLASS_NORMAL, QST_GENERATOR_RETURN_NON_EMPTY, 950000, 0,
     0, NULL, "0"},
    {SQL_DIMENSION_VAL_LITERAL_ONE, QST_GENERATOR_VALUE_ATOMIC,
     QST_GENERATOR_CLASS_NORMAL, QST_GENERATOR_RETURN_NON_EMPTY, 950000, 0,
     0, NULL, "1"},
};
static const SQstGeneratorFragment kFragments[] = {
    {SQL_DIMENSION_FRAG_QUERY_ROOT, 1,
     sizeof(kQueryRootValues) / sizeof(kQueryRootValues[0]), kQueryRootValues},
    {SQL_DIMENSION_FRAG_QUERY_TOP_LEVEL, 1,
     sizeof(kQueryTopLevelValues) / sizeof(kQueryTopLevelValues[0]),
     kQueryTopLevelValues},
    {SQL_DIMENSION_FRAG_QUERY_STMT, 1,
     sizeof(kQueryStatementValues) / sizeof(kQueryStatementValues[0]),
     kQueryStatementValues},
    {SQL_DIMENSION_FRAG_QUERY_TERMINATOR, 1,
     sizeof(kQueryTerminatorValues) / sizeof(kQueryTerminatorValues[0]),
     kQueryTerminatorValues},
    {SQL_DIMENSION_FRAG_LITERAL, 1,
     sizeof(kLiteralValues) / sizeof(kLiteralValues[0]), kLiteralValues},
};

EQstCode qst_sql_dimension_catalog(const SQstGeneratorCatalog **out_catalog) {
  static SQstGeneratorCatalog catalog = {
      SQL_DIMENSION_CONFIG_VERSION,
      SQL_DIMENSION_FRAG_QUERY_ROOT,
      sizeof(kFragments) / sizeof(kFragments[0]),
      kFragments,
      {{0}},
  };
  static int32_t initialized = 0;
  EQstCode code = QST_OK;

  if (NULL == out_catalog) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (initialized == 0) {
    code = qst_generator_catalog_digest(&catalog, &catalog.digest);
    if (code != QST_OK) {
      return code;
    }
    initialized = 1;
  }
  code = qst_generator_catalog_validate(&catalog);
  if (code != QST_OK) {
    return code;
  }
  *out_catalog = &catalog;
  return QST_OK;
}
