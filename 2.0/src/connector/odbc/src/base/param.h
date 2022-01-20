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

#ifndef _param_h_
#define _param_h_

#include "../base.h"

typedef struct paramset_s                  paramset_t;
typedef struct param_binding_s             param_binding_t;
typedef struct param_val_s                 param_val_t;

// SQLDescribeParam
struct param_s {
  SQLUSMALLINT    ParameterNumber;
  SQLSMALLINT     DataType;                   // sql data type
  SQLULEN         ParameterSize;
  SQLSMALLINT     DecimalDigits;
  SQLSMALLINT     Nullable;
};

// SQLBindParameter
struct param_binding_s {
  SQLUSMALLINT    ParameterNumber;
  SQLSMALLINT     InputOutputType;
  SQLSMALLINT     ValueType;                  // sql c data type
  SQLSMALLINT     ParameterType;              // sql data type
  SQLULEN         ColumnSize;
  SQLSMALLINT     DecimalDigits;
  SQLPOINTER      ParameterValuePtr;
  SQLLEN          BufferLength;
  SQLLEN         *StrLen_or_IndPtr;
};

struct paramset_s {
  todbc_buf_t              *params_cache;
  param_t                  *params;
  int                       n_params;

  todbc_buf_t              *bindings_cache;
  param_binding_t          *bindings;
  int                       n_bindings;

  int                       i_row;
  int                       i_col;
};

void paramset_reclaim_params(paramset_t *paramset);
void paramset_reclaim_bindings(paramset_t *paramset);

void paramset_init_params_cache(paramset_t *paramset);
void paramset_init_bindings_cache(paramset_t *paramset);

void paramset_release(paramset_t *paramset);

#endif // _param_h_



