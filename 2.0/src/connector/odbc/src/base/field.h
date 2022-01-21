/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

#ifndef _field_h_
#define _field_h_

#include "../base.h"

typedef struct fieldset_s                  fieldset_t;
typedef struct col_binding_s               col_binding_t;
typedef struct field_arg_s                 field_arg_t;

// SQLDescribeCol
struct field_arg_s {
  SQLUSMALLINT        ColumnNumber;
  SQLCHAR            *ColumnName;
  SQLSMALLINT         BufferLength;
  SQLSMALLINT        *NameLength;
  SQLSMALLINT        *DataType;             // sql data type
  SQLULEN            *ColumnSize;
  SQLSMALLINT        *DecimalDigits;
  SQLSMALLINT        *Nullable;
};

struct field_s {
  SQLUSMALLINT    ColumnNumber;
  SQLCHAR         ColumnName[64];           // hard-coded
  SQLSMALLINT     DataType;                 // sql data type
  SQLULEN         ColumnSize;
  SQLSMALLINT     DecimalDigits;
  SQLSMALLINT     Nullable;
};

// SQLBindCol; SQLGetData
struct col_binding_s {
  SQLUSMALLINT    ColumnNumber;
  SQLSMALLINT     TargetType;               // sql c data type
  SQLPOINTER      TargetValue;
  SQLLEN          BufferLength;
  SQLLEN         *StrLen_or_IndPtr;
};


struct fieldset_s {
  todbc_buf_t              *fields_cache;
  field_t                  *fields;
  int                       n_fields;

  todbc_buf_t              *bindings_cache;
  col_binding_t            *bindings;
  int                       n_bindings;
};

void fieldset_init_fields(fieldset_t *fieldset);
void fieldset_init_bindings(fieldset_t *fieldset);

void fieldset_reclaim_fields(fieldset_t *fieldset);
void fieldset_reclaim_bindings(fieldset_t *fieldset);

void fieldset_release(fieldset_t *fields);

#endif // _field_h_



