/*
 * MIT License
 *
 * Copyright (c) 2022-2023 freemine <freemine@yeah.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "internal.h"

#include "typesinfo.h"

#include "errs.h"
#include "log.h"
#include "taos_helpers.h"
#include "tsdb.h"

#include <errno.h>

static TAOS_FIELD _fields[] = {
  {
    /* 1 */
    /* name                            */ "TYPE_NAME",
    /* type                            */ TSDB_DATA_TYPE_VARCHAR,
    /* bytes                           */ 1024,               /* hard-coded, big enough */
  },{
    /* 2 */
    /* name                            */ "DATA_TYPE",
    /* type                            */ TSDB_DATA_TYPE_SMALLINT,
    /* bytes                           */ 2,
  },{
    /* 3 */
    /* name                            */ "COLUMN_SIZE",
    /* type                            */ TSDB_DATA_TYPE_INT,
    /* bytes                           */ 4,
  },{
    /* 4 */
    /* name                            */ "LITERAL_PREFIX",
    /* type                            */ TSDB_DATA_TYPE_VARCHAR,
    /* bytes                           */ 1024,               /* hard-coded, big enough */
  },{
    /* 5 */
    /* name                            */ "LITERAL_SUFFIX",
    /* type                            */ TSDB_DATA_TYPE_VARCHAR,
    /* bytes                           */ 1024,               /* hard-coded, big enough */
  },{
    /* 6 */
    /* name                            */ "CREATE_PARAMS",
    /* type                            */ TSDB_DATA_TYPE_VARCHAR,
    /* bytes                           */ 1024,               /* hard-coded, big enough */
  },{
    /* 7 */
    /* name                            */ "NULLABLE",
    /* type                            */ TSDB_DATA_TYPE_SMALLINT,
    /* bytes                           */ 2,
  },{
    /* 8 */
    /* name                            */ "CASE_SENSITIVE",
    /* type                            */ TSDB_DATA_TYPE_SMALLINT,
    /* bytes                           */ 2,
  },{
    /* 9 */
    /* name                            */ "SEARCHABLE",
    /* type                            */ TSDB_DATA_TYPE_SMALLINT,
    /* bytes                           */ 2,
  },{
    /* 10 */
    /* name                            */ "UNSIGNED_ATTRIBUTE",
    /* type                            */ TSDB_DATA_TYPE_SMALLINT,
    /* bytes                           */ 2,
  },{
    /* 11 */
    /* name                            */ "FIXED_PREC_SCALE",
    /* type                            */ TSDB_DATA_TYPE_SMALLINT,
    /* bytes                           */ 2,
  },{
    /* 12 */
    /* name                            */ "AUTO_UNIQUE_VALUE",
    /* type                            */ TSDB_DATA_TYPE_SMALLINT,
    /* bytes                           */ 2,
  },{
    /* 13 */
    /* name                            */ "LOCAL_TYPE_NAME",
    /* type                            */ TSDB_DATA_TYPE_VARCHAR,
    /* bytes                           */ 1024,               /* hard-coded, big enough */
  },{
    /* 14 */
    /* name                            */ "MINIMUM_SCALE",
    /* type                            */ TSDB_DATA_TYPE_SMALLINT,
    /* bytes                           */ 2,
  },{
    /* 15 */
    /* name                            */ "MAXIMUM_SCALE",
    /* type                            */ TSDB_DATA_TYPE_SMALLINT,
    /* bytes                           */ 2,
  },{
    /* 16 */
    /* name                            */ "SQL_DATA_TYPE",
    /* type                            */ TSDB_DATA_TYPE_SMALLINT,
    /* bytes                           */ 2,
  },{
    /* 17 */
    /* name                            */ "SQL_DATETIME_SUB",
    /* type                            */ TSDB_DATA_TYPE_SMALLINT,
    /* bytes                           */ 2,
  },{
    /* 18 */
    /* name                            */ "NUM_PREC_RADIX",
    /* type                            */ TSDB_DATA_TYPE_INT,
    /* bytes                           */ 4,
  },{
    /* 19 */
    /* name                            */ "INTERVAL_PRECISION",
    /* type                            */ TSDB_DATA_TYPE_SMALLINT,
    /* bytes                           */ 2,
  },
};

typedef struct type_info_rec_s                  type_info_rec_t;
struct type_info_rec_s {
  /*  1 */ const char                *TYPE_NAME;
  /*  2 */ int64_t                    DATA_TYPE;
  /*  3 */ int64_t                    COLUMN_SIZE;
  /*  4 */ const char                *LITERAL_PREFIX;
  /*  5 */ const char                *LITERAL_SUFFIX;
  /*  6 */ const char                *CREATE_PARAMS;
  /*  7 */ int64_t                    NULLABLE;
  /*  8 */ int64_t                    CASE_SENSITIVE;
  /*  9 */ int64_t                    SEARCHABLE;
  /* 10 */ int64_t                    UNSIGNED_ATTRIBUTE;
  /* 11 */ int64_t                    FIXED_PREC_SCALE;
  /* 12 */ int64_t                    AUTO_UNIQUE_VALUE;
  /* 13 */ const char                *LOCAL_TYPE_NAME;
  /* 14 */ int64_t                    MINIMUM_SCALE;
  /* 15 */ int64_t                    MAXIMUM_SCALE;
  /* 16 */ int64_t                    SQL_DATA_TYPE;
  /* 17 */ int64_t                    SQL_DATETIME_SUB;
  /* 18 */ int64_t                    NUM_PREC_RADIX;
  /* 19 */ int64_t                    INTERVAL_PRECISION;
};

// NOTE: this is tricky approach
#define NULL_FIELD       ((int64_t)0x8000000000000000L)
static const type_info_rec_t _records[] = {
  {
    /* TYPE_NAME             */      "VARCHAR",
    /* DATA_TYPE             */      SQL_VARCHAR,
    /* COLUMN_SIZE           */      16384,            // NOTE: hard-coded
    /* LITERAL_PREFIX        */      "'",
    /* LITERAL_SUFFIX        */      "'",
    /* CREATE_PARAMS         */      "max length",     // NOTE: what does `max` means?
    /* NULLABLE              */      SQL_NULLABLE,
    /* CASE_SENSITIVE        */      SQL_FALSE,
    /* SEARCHABLE            */      SQL_SEARCHABLE,
    /* UNSIGNED_ATTRIBUTE    */      NULL_FIELD,
    /* FIXED_PREC_SCALE      */      SQL_FALSE,
    /* AUTO_UNIQUE_VALUE     */      NULL_FIELD,
    /* LOCAL_TYPE_NAME       */      "VARCHAR",
    /* MINIMUM_SCALE         */      NULL_FIELD,
    /* MAXIMUM_SCALE         */      NULL_FIELD,
    /* SQL_DATA_TYPE         */      SQL_VARCHAR,
    /* SQL_DATETIME_SUB      */      NULL_FIELD,
    /* NUM_PREC_RADIX        */      NULL_FIELD,
    /* INTERVAL_PRECISION    */      NULL_FIELD,
  },{
    /* TYPE_NAME             */      "JSON",
    /* DATA_TYPE             */      SQL_WVARCHAR,
    /* COLUMN_SIZE           */      16384,            // NOTE: hard-coded
    /* LITERAL_PREFIX        */      "'",
    /* LITERAL_SUFFIX        */      "'",
    /* CREATE_PARAMS         */      "max length",     // NOTE: what does `max` means?
    /* NULLABLE              */      SQL_NULLABLE,
    /* CASE_SENSITIVE        */      SQL_FALSE,
    /* SEARCHABLE            */      SQL_SEARCHABLE,
    /* UNSIGNED_ATTRIBUTE    */      NULL_FIELD,
    /* FIXED_PREC_SCALE      */      SQL_FALSE,
    /* AUTO_UNIQUE_VALUE     */      NULL_FIELD,
    /* LOCAL_TYPE_NAME       */      "JSON",
    /* MINIMUM_SCALE         */      NULL_FIELD,
    /* MAXIMUM_SCALE         */      NULL_FIELD,
    /* SQL_DATA_TYPE         */      SQL_WVARCHAR,
    /* SQL_DATETIME_SUB      */      NULL_FIELD,
    /* NUM_PREC_RADIX        */      NULL_FIELD,
    /* INTERVAL_PRECISION    */      NULL_FIELD,
  },{
    /* TYPE_NAME             */      "INT",
    /* DATA_TYPE             */      SQL_INTEGER,
    /* COLUMN_SIZE           */      10,
    /* LITERAL_PREFIX        */      NULL,
    /* LITERAL_SUFFIX        */      NULL,
    /* CREATE_PARAMS         */      NULL,
    /* NULLABLE              */      SQL_NULLABLE,
    /* CASE_SENSITIVE        */      SQL_FALSE,
    /* SEARCHABLE            */      SQL_PRED_BASIC,
    /* UNSIGNED_ATTRIBUTE    */      SQL_FALSE,
    /* FIXED_PREC_SCALE      */      SQL_FALSE,
    /* AUTO_UNIQUE_VALUE     */      SQL_FALSE,
    /* LOCAL_TYPE_NAME       */      "INT",
    /* MINIMUM_SCALE         */      SQL_FALSE,
    /* MAXIMUM_SCALE         */      SQL_FALSE,
    /* SQL_DATA_TYPE         */      SQL_INTEGER,
    /* SQL_DATETIME_SUB      */      NULL_FIELD,
    /* NUM_PREC_RADIX        */      10,
    /* INTERVAL_PRECISION    */      NULL_FIELD,
  },{
    /* TYPE_NAME             */      "TIMESTAMP",
    /* DATA_TYPE             */      SQL_BINARY,
    /* COLUMN_SIZE           */      8,
    /* LITERAL_PREFIX        */      "0x",
    /* LITERAL_SUFFIX        */      NULL,
    /* CREATE_PARAMS         */      NULL,
    /* NULLABLE              */      SQL_NO_NULLS,
    /* CASE_SENSITIVE        */      SQL_FALSE,
    /* SEARCHABLE            */      SQL_PRED_BASIC,
    /* UNSIGNED_ATTRIBUTE    */      NULL_FIELD,
    /* FIXED_PREC_SCALE      */      SQL_FALSE,
    /* AUTO_UNIQUE_VALUE     */      NULL_FIELD,
    /* LOCAL_TYPE_NAME       */      "TIMESTAMP",
    /* MINIMUM_SCALE         */      NULL_FIELD,
    /* MAXIMUM_SCALE         */      NULL_FIELD,
    /* SQL_DATA_TYPE         */      SQL_BINARY,
    /* SQL_DATETIME_SUB      */      NULL_FIELD,
    /* NUM_PREC_RADIX        */      NULL_FIELD,
    /* INTERVAL_PRECISION    */      NULL_FIELD,
  }
};

void typesinfo_reset(typesinfo_t *typesinfo)
{
  if (!typesinfo) return;
}

void typesinfo_release(typesinfo_t *typesinfo)
{
  if (!typesinfo) return;
  typesinfo_reset(typesinfo);

  typesinfo->owner = NULL;
}

static SQLRETURN _prepare(stmt_base_t *base, const sqlc_tsdb_t *sqlc_tsdb)
{
  (void)sqlc_tsdb;

  typesinfo_t *typesinfo = (typesinfo_t*)base;
  (void)typesinfo;
  stmt_append_err(typesinfo->owner, "HY000", 0, "General error:internal logic error");
  return SQL_ERROR;
}

static SQLRETURN _execute(stmt_base_t *base)
{
  typesinfo_t *typesinfo = (typesinfo_t*)base;
  (void)typesinfo;
  stmt_append_err(typesinfo->owner, "HY000", 0, "General error:internal logic error");
  return SQL_ERROR;
}

static SQLRETURN _get_col_fields(stmt_base_t *base, TAOS_FIELD **fields, size_t *nr)
{
  (void)fields;
  (void)nr;
  typesinfo_t *typesinfo = (typesinfo_t*)base;
  (void)typesinfo;
  *fields = _fields;
  *nr = sizeof(_fields)/sizeof(_fields[0]);
  return SQL_SUCCESS;
}

static SQLRETURN _fetch_row(stmt_base_t *base)
{
  typesinfo_t *typesinfo = (typesinfo_t*)base;

again:

  if (typesinfo->pos >= sizeof(_records)/sizeof(_records[0])) return SQL_NO_DATA;

  typesinfo->pos += 1;

  if (typesinfo->data_type != SQL_ALL_TYPES) {
    const type_info_rec_t *rec = _records + typesinfo->pos - 1;
    if (rec->DATA_TYPE != typesinfo->data_type) {
      goto again;
    }
  }

  return SQL_SUCCESS;
}

static SQLRETURN _more_results(stmt_base_t *base)
{
  (void)base;
  return SQL_NO_DATA;
}

static SQLRETURN _describe_param(stmt_base_t *base,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT    *DataTypePtr,
    SQLULEN        *ParameterSizePtr,
    SQLSMALLINT    *DecimalDigitsPtr,
    SQLSMALLINT    *NullablePtr)
{
  (void)ParameterNumber;
  (void)DataTypePtr;
  (void)ParameterSizePtr;
  (void)DecimalDigitsPtr;
  (void)NullablePtr;

  typesinfo_t *typesinfo = (typesinfo_t*)base;
  (void)typesinfo;
  stmt_append_err(typesinfo->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _get_num_params(stmt_base_t *base, SQLSMALLINT *ParameterCountPtr)
{
  (void)ParameterCountPtr;

  typesinfo_t *typesinfo = (typesinfo_t*)base;
  (void)typesinfo;
  stmt_append_err(typesinfo->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _tsdb_field_by_param(stmt_base_t *base, int i_param, TAOS_FIELD_E **field)
{
  (void)i_param;
  (void)field;

  typesinfo_t *typesinfo = (typesinfo_t*)base;
  (void)typesinfo;
  stmt_append_err(typesinfo->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _row_count(stmt_base_t *base, SQLLEN *row_count_ptr)
{
  typesinfo_t *typesinfo = (typesinfo_t*)base;
  (void)typesinfo;

  if (row_count_ptr) *row_count_ptr = 0;

  return SQL_SUCCESS;
}

static SQLRETURN _get_num_cols(stmt_base_t *base, SQLSMALLINT *ColumnCountPtr)
{
  (void)base;

  size_t nr = sizeof(_fields) / sizeof(_fields[0]);

  *ColumnCountPtr = (SQLSMALLINT)nr;

  return SQL_SUCCESS;
}

static SQLRETURN _get_data(stmt_base_t *base, SQLUSMALLINT Col_or_Param_Num, tsdb_data_t *tsdb)
{
  (void)Col_or_Param_Num;
  (void)tsdb;
  typesinfo_t *typesinfo = (typesinfo_t*)base;
  (void)typesinfo;

  tsdb->is_null = 0;

  const type_info_rec_t *rec = _records + typesinfo->pos - 1;

  switch (Col_or_Param_Num) {
    case 1: // TYPE_NAME:
      tsdb->type = TSDB_DATA_TYPE_VARCHAR;
      tsdb->str.str = rec->TYPE_NAME;
      tsdb->str.len = strlen(rec->TYPE_NAME);
      break;
    case 2: // DATA_TYPE
      tsdb->type = TSDB_DATA_TYPE_BIGINT;
      tsdb->i64 = rec->DATA_TYPE;
      break;
    case 3: // COLUMN_SIZE
      tsdb->type = TSDB_DATA_TYPE_BIGINT;
      tsdb->i64 = rec->COLUMN_SIZE;
      break;
    case 4: // LITERAL_PREFIX
      tsdb->type = TSDB_DATA_TYPE_VARCHAR;
      if (rec->LITERAL_PREFIX == NULL) {
        tsdb->is_null = 1;
      } else {
        tsdb->str.str = rec->LITERAL_PREFIX;
        tsdb->str.len = strlen(rec->LITERAL_PREFIX);
      }
      break;
    case 5: // LITERAL_SUFFIX
      tsdb->type = TSDB_DATA_TYPE_VARCHAR;
      if (rec->LITERAL_SUFFIX == NULL) {
        tsdb->is_null = 1;
      } else {
        tsdb->str.str = rec->LITERAL_SUFFIX;
        tsdb->str.len = strlen(rec->LITERAL_SUFFIX);
      }
      break;
    case 6: // CREATE_PARAMS
      tsdb->type = TSDB_DATA_TYPE_VARCHAR;
      if (rec->CREATE_PARAMS == NULL) {
        tsdb->is_null = 1;
      } else {
        tsdb->str.str = rec->CREATE_PARAMS;
        tsdb->str.len = strlen(rec->CREATE_PARAMS);
      }
      break;
    case 7: // NULLABLE
      tsdb->type = TSDB_DATA_TYPE_BIGINT;
      tsdb->i64 = rec->NULLABLE;
      break;
    case 8: // CASE_SENSITIVE
      tsdb->type = TSDB_DATA_TYPE_BIGINT;
      tsdb->i64 = rec->CASE_SENSITIVE;
      break;
    case 9: // SEARCHABLE
      tsdb->type = TSDB_DATA_TYPE_BIGINT;
      tsdb->i64 = rec->SEARCHABLE;
      break;
    case 10: // UNSIGNED_ATTRIBUTE
      tsdb->type = TSDB_DATA_TYPE_BIGINT;
      if (rec->UNSIGNED_ATTRIBUTE == NULL_FIELD) {
        tsdb->is_null = 1;
      } else {
        tsdb->i64 = rec->UNSIGNED_ATTRIBUTE;
      }
      break;
    case 11: // FIXED_PREC_SCALE
      tsdb->type = TSDB_DATA_TYPE_BIGINT;
      if (rec->FIXED_PREC_SCALE == NULL_FIELD) {
        tsdb->is_null = 1;
      } else {
        tsdb->i64 = rec->FIXED_PREC_SCALE;
      }
      break;
    case 12: // AUTO_UNIQUE_VALUE
      tsdb->type = TSDB_DATA_TYPE_BIGINT;
      tsdb->i64 = rec->AUTO_UNIQUE_VALUE;
      break;
    case 13: // LOCAL_TYPE_NAME
      tsdb->type = TSDB_DATA_TYPE_VARCHAR;
      tsdb->str.str = rec->LOCAL_TYPE_NAME;
      tsdb->str.len = strlen(rec->LOCAL_TYPE_NAME);
      break;
    case 14: // MINIMUM_SCALE
      tsdb->type = TSDB_DATA_TYPE_BIGINT;
      if (rec->MINIMUM_SCALE == NULL_FIELD) {
        tsdb->is_null = 1;
      } else {
        tsdb->i64 = rec->MINIMUM_SCALE;
      }
      break;
    case 15: // MAXIMUM_SCALE
      tsdb->type = TSDB_DATA_TYPE_BIGINT;
      if (rec->MAXIMUM_SCALE == NULL_FIELD) {
        tsdb->is_null = 1;
      } else {
        tsdb->i64 = rec->MAXIMUM_SCALE;
      }
      break;
    case 16: // SQL_DATA_TYPE
      tsdb->type = TSDB_DATA_TYPE_BIGINT;
      tsdb->i64 = rec->SQL_DATA_TYPE;
      break;
    case 17: // SQL_DATETIME_SUB
      tsdb->type = TSDB_DATA_TYPE_BIGINT;
      if (rec->SQL_DATETIME_SUB == NULL_FIELD) {
        tsdb->is_null = 1;
      } else {
        tsdb->i64 = rec->SQL_DATETIME_SUB;
      }
      break;
    case 18: // NUM_PREC_RADIX
      tsdb->type = TSDB_DATA_TYPE_BIGINT;
      if (rec->NUM_PREC_RADIX == NULL_FIELD) {
        tsdb->is_null = 1;
      } else {
        tsdb->i64 = rec->NUM_PREC_RADIX;
      }
      break;
    case 19: // INTERVAL_PRECISION
      tsdb->type = TSDB_DATA_TYPE_BIGINT;
      if (rec->INTERVAL_PRECISION == NULL_FIELD) {
        tsdb->is_null = 1;
      } else {
        tsdb->i64 = rec->INTERVAL_PRECISION;
      }
      break;
    default:
      stmt_append_err_format(typesinfo->owner, "HY000", 0, "General error:Column[%d] not implemented yet", Col_or_Param_Num);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

void typesinfo_init(typesinfo_t *typesinfo, stmt_t *stmt)
{
  typesinfo->owner = stmt;

  stmt_base_t *base = &typesinfo->base;

  base->prepare                      = _prepare;
  base->execute                      = _execute;
  base->get_col_fields               = _get_col_fields;
  base->fetch_row                    = _fetch_row;
  base->more_results                 = _more_results;
  base->describe_param               = _describe_param;
  base->get_num_params               = _get_num_params;
  base->tsdb_field_by_param          = _tsdb_field_by_param;
  base->row_count                    = _row_count;
  base->get_num_cols                 = _get_num_cols;
  base->get_data                     = _get_data;
}

SQLRETURN typesinfo_open(
    typesinfo_t     *typesinfo,
    SQLSMALLINT      DataType)
{
  SQLRETURN sr = SQL_SUCCESS;

  typesinfo_reset(typesinfo);

  typesinfo->data_type = DataType;

  return sr;
}
