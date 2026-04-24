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

// extTypeMap.h — external data source type mapping public interface
//
// Location: include/libs/qcom/extTypeMap.h
// Callers:  Parser (semantic validation), Planner (physical plan construction)
// NOT called by: External Connector (Connector only performs binary value
//                conversion based on the SExtColTypeMapping already in the plan)

#ifndef _EXT_TYPE_MAP_H_
#define _EXT_TYPE_MAP_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "tmsg.h"    // EExtSourceType, TSDB_CODE_* constants
#include "ttypes.h"  // SDataType

/**
 * Map an external data source type name to the corresponding TDengine type.
 *
 * @param srcType      The external source type (EXT_SOURCE_MYSQL,
 *                     EXT_SOURCE_POSTGRESQL, EXT_SOURCE_INFLUXDB).
 * @param extTypeName  The raw type name string returned by the external source
 *                     (e.g. "VARCHAR(255)", "bigint", "Utf8",
 *                     "DECIMAL(18,4)").
 * @param pTdType      [out] Filled with the mapped TDengine type info:
 *                       - type:      TSDB_DATA_TYPE_* enum value
 *                       - bytes:     storage byte width (e.g. 4 for INT,
 *                                    n+VARSTR_HEADER_SIZE for VARCHAR(n))
 *                       - precision: DECIMAL precision (0 for non-decimal)
 *                       - scale:     DECIMAL scale     (0 for non-decimal)
 *
 * @return TSDB_CODE_SUCCESS              — mapping succeeded.
 * @return TSDB_CODE_EXT_TYPE_NOT_MAPPABLE — unknown or unsupported type.
 */
int32_t extTypeNameToTDengineType(EExtSourceType srcType, const char *extTypeName, SDataType *pTdType);

#ifdef __cplusplus
}
#endif

#endif  // _EXT_TYPE_MAP_H_
