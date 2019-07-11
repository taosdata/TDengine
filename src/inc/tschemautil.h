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

#ifndef TDENGINE_TSCHEMAUTIL_H
#define TDENGINE_TSCHEMAUTIL_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include "taosmsg.h"

#define VALIDNUMOFCOLS(x) ((x) >= TSDB_MIN_COLUMNS && (x) <= TSDB_MAX_COLUMNS)

struct SSchema;

/**
 * check if the schema is valid or not, including following aspects:
 * 1. number of columns
 * 2. column types
 * 3. column length
 * 4. column names
 * 5. total length
 *
 * @param pSchema
 * @param numOfCols
 * @return
 */
bool isValidSchema(struct SSchema *pSchema, int32_t numOfCols);

struct SSchema *tsGetSchema(SMeterMeta *pMeta);

struct SSchema *tsGetTagSchema(SMeterMeta *pMeta);

struct SSchema *tsGetSchemaColIdx(SMeterMeta *pMeta, int32_t startCol);

char *tsGetTagsValue(SMeterMeta *pMeta);

bool tsMeterMetaIdentical(SMeterMeta *p1, SMeterMeta *p2);

void extractMeterName(char *meterId, char *name);

void extractDBName(char *meterId, char *name);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSCHEMAUTIL_H
