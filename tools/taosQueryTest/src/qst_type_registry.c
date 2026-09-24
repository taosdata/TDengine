/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

/* TDengine local type registry used by fixed-fixture materialization. */

#include "qst_dataset.h"

#include <stddef.h>

#include "taos.h"

_Static_assert(TSDB_DATA_TYPE_NULL == 0, "unexpected TDengine type ABI");
_Static_assert(TSDB_DATA_TYPE_DECIMAL64 == 21, "unexpected TDengine type ABI");
_Static_assert(TSDB_DATA_TYPE_MAX == 22, "update the local type registry");

static const SQstLocalTypeDescriptor kQstLocalTypes[TSDB_DATA_TYPE_MAX] = {
    [TSDB_DATA_TYPE_NULL] = {TSDB_DATA_TYPE_NULL, QST_TYPE_ROLE_PSEUDO,
                             "NULL", NULL},
    [TSDB_DATA_TYPE_BOOL] = {TSDB_DATA_TYPE_BOOL,
                             QST_TYPE_ROLE_COLUMN | QST_TYPE_ROLE_TAG,
                             "BOOL", "BOOL"},
    [TSDB_DATA_TYPE_TINYINT] = {TSDB_DATA_TYPE_TINYINT,
                                QST_TYPE_ROLE_COLUMN | QST_TYPE_ROLE_TAG,
                                "TINYINT", "TINYINT"},
    [TSDB_DATA_TYPE_SMALLINT] = {TSDB_DATA_TYPE_SMALLINT,
                                 QST_TYPE_ROLE_COLUMN | QST_TYPE_ROLE_TAG,
                                 "SMALLINT", "SMALLINT"},
    [TSDB_DATA_TYPE_INT] = {TSDB_DATA_TYPE_INT,
                            QST_TYPE_ROLE_COLUMN | QST_TYPE_ROLE_TAG,
                            "INT", "INT"},
    [TSDB_DATA_TYPE_BIGINT] = {TSDB_DATA_TYPE_BIGINT,
                               QST_TYPE_ROLE_COLUMN | QST_TYPE_ROLE_TAG,
                               "BIGINT", "BIGINT"},
    [TSDB_DATA_TYPE_FLOAT] = {TSDB_DATA_TYPE_FLOAT, QST_TYPE_ROLE_COLUMN,
                              "FLOAT", "FLOAT"},
    [TSDB_DATA_TYPE_DOUBLE] = {TSDB_DATA_TYPE_DOUBLE, QST_TYPE_ROLE_COLUMN,
                               "DOUBLE", "DOUBLE"},
    [TSDB_DATA_TYPE_VARCHAR] = {TSDB_DATA_TYPE_VARCHAR,
                                QST_TYPE_ROLE_COLUMN | QST_TYPE_ROLE_TAG,
                                "VARCHAR", "VARCHAR(8)"},
    [TSDB_DATA_TYPE_TIMESTAMP] = {TSDB_DATA_TYPE_TIMESTAMP,
                                  QST_TYPE_ROLE_COLUMN,
                                  "TIMESTAMP", "TIMESTAMP"},
    [TSDB_DATA_TYPE_NCHAR] = {TSDB_DATA_TYPE_NCHAR,
                              QST_TYPE_ROLE_COLUMN | QST_TYPE_ROLE_TAG,
                              "NCHAR", "NCHAR(8)"},
    [TSDB_DATA_TYPE_UTINYINT] = {TSDB_DATA_TYPE_UTINYINT,
                                 QST_TYPE_ROLE_COLUMN | QST_TYPE_ROLE_TAG,
                                 "TINYINT UNSIGNED", "TINYINT UNSIGNED"},
    [TSDB_DATA_TYPE_USMALLINT] = {TSDB_DATA_TYPE_USMALLINT,
                                  QST_TYPE_ROLE_COLUMN | QST_TYPE_ROLE_TAG,
                                  "SMALLINT UNSIGNED", "SMALLINT UNSIGNED"},
    [TSDB_DATA_TYPE_UINT] = {TSDB_DATA_TYPE_UINT,
                             QST_TYPE_ROLE_COLUMN | QST_TYPE_ROLE_TAG,
                             "INT UNSIGNED", "INT UNSIGNED"},
    [TSDB_DATA_TYPE_UBIGINT] = {TSDB_DATA_TYPE_UBIGINT,
                                QST_TYPE_ROLE_COLUMN | QST_TYPE_ROLE_TAG,
                                "BIGINT UNSIGNED", "BIGINT UNSIGNED"},
    [TSDB_DATA_TYPE_JSON] = {TSDB_DATA_TYPE_JSON, QST_TYPE_ROLE_TAG,
                             "JSON", "JSON"},
    [TSDB_DATA_TYPE_VARBINARY] = {TSDB_DATA_TYPE_VARBINARY,
                                  QST_TYPE_ROLE_COLUMN,
                                  "VARBINARY", "VARBINARY(8)"},
    [TSDB_DATA_TYPE_DECIMAL] = {TSDB_DATA_TYPE_DECIMAL, QST_TYPE_ROLE_COLUMN,
                                "DECIMAL", "DECIMAL(20,10)"},
    [TSDB_DATA_TYPE_BLOB] = {TSDB_DATA_TYPE_BLOB, QST_TYPE_ROLE_COLUMN,
                             "BLOB", "BLOB"},
    [TSDB_DATA_TYPE_MEDIUMBLOB] = {TSDB_DATA_TYPE_MEDIUMBLOB,
                                   QST_TYPE_ROLE_COLUMN,
                                   "MEDIUMBLOB", "MEDIUMBLOB"},
    [TSDB_DATA_TYPE_GEOMETRY] = {TSDB_DATA_TYPE_GEOMETRY,
                                 QST_TYPE_ROLE_COLUMN,
                                 "GEOMETRY", "GEOMETRY(8)"},
    [TSDB_DATA_TYPE_DECIMAL64] = {TSDB_DATA_TYPE_DECIMAL64,
                                  QST_TYPE_ROLE_COLUMN,
                                  "DECIMAL64", "DECIMAL(10,2)"},
};

EQstCode qst_dataset_local_type_registry(
    const SQstLocalTypeDescriptor **out_types, uint32_t *out_type_count) {
  if (NULL == out_types || NULL == out_type_count) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_types = kQstLocalTypes;
  *out_type_count = TSDB_DATA_TYPE_MAX;
  return QST_OK;
}
