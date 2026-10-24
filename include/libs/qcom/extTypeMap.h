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

// extTypeMap.h  -  external data source type mapping public interface
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
 * Timezone class of an external timestamp type.
 *
 * Declared here rather than in libs/nodes/plannodes.h because the type mapper
 * owns the classification: the planner stores it in
 * SFederatedScanPhysiNode.pTimestampTypeHints and the executor/stream readers
 * derive it from the same remote type name.  Keeping the enum next to the
 * classifier also keeps qcom (a lower layer) free of node-layer headers.
 */
typedef enum EExtTimestampTzClass {
  EXT_TIMESTAMP_TZ_CLASS_UNKNOWN = 0,
  EXT_TIMESTAMP_TZ_CLASS_AWARE   = 1,
  EXT_TIMESTAMP_TZ_CLASS_NAIVE   = 2,
} EExtTimestampTzClass;

/**
 * Map an external data source type name to the corresponding TDengine type.
 *
 * @param srcType       The external source type (EXT_SOURCE_MYSQL,
 *                      EXT_SOURCE_POSTGRESQL, EXT_SOURCE_INFLUXDB).
 * @param extTypeName   The raw type name string returned by the external source
 *                      (e.g. "VARCHAR(255)", "bigint", "Utf8",
 *                      "DECIMAL(18,4)").
 * @param extCharsetName  Charset or encoding for the column (DS section 5.3.2):
 *                      - MySQL:     CHARACTER_SET_NAME from INFORMATION_SCHEMA
 *                                   (e.g. "utf8mb4", "latin1", "ascii").
 *                                   NULL or empty for non-character columns.
 *                      - PG:        Server-side database encoding from
 *                                   current_setting('server_encoding')
 *                                   (e.g. "UTF8", "SQL_ASCII").
 *                                   NULL or empty means unknown  ->  VARCHAR.
 *                      - InfluxDB:  Always NULL or empty; Utf8 maps to NCHAR
 *                                   unconditionally regardless of this value.
 * @param pTdType       [out] Filled with the mapped TDengine type info:
 *                        - type:      TSDB_DATA_TYPE_* enum value
 *                        - bytes:     storage byte width (e.g. 4 for INT,
 *                                     n+VARSTR_HEADER_SIZE for VARCHAR(n))
 *                        - precision: DECIMAL precision (0 for non-decimal)
 *                        - scale:     DECIMAL scale     (0 for non-decimal)
 *
 * @return TSDB_CODE_SUCCESS               -  mapping succeeded.
 * @return TSDB_CODE_EXT_TYPE_NOT_MAPPABLE  -  unknown or unsupported type.
 */
int32_t extTypeNameToTDengineType(EExtSourceType srcType, const char *extTypeName,
                                   const char *extCharsetName, SDataType *pTdType);

/**
 * Return the precision accepted by an external timestamp column in predicates.
 * MySQL DATETIME/TIMESTAMP, PostgreSQL TIMESTAMP and SQL Server
 * DATETIME2/DATETIMEOFFSET derive it from the fractional precision declared in
 * extTypeName; SQL Server SMALLDATETIME/DATE are millisecond-aligned, its
 * DATETIME (1/300 s ticks) and the remaining types fall back to microsecond,
 * and InfluxDB uses its native nanosecond precision.
 */
int8_t extTimestampTypePrecision(EExtSourceType srcType, const char *extTypeName,
                                  int8_t fallbackPrecision);

/**
 * Compare two external column names the way the source resolves identifiers.
 *
 * MySQL and SQL Server fold unquoted identifiers case-insensitively, so a
 * reference written in a different case still binds to the stored column;
 * PostgreSQL and InfluxDB compare identifiers literally.  This is the single
 * source of truth for that rule: the parser accepts an external column
 * reference with it, mnode binds the reader column mappings with it, and the
 * stream reader looks up the planner's timestamp-class hint with it.  Keeping
 * one implementation stops the three from drifting apart, which would make a
 * reference that DDL accepted unresolvable at reader deploy time.
 */
bool extColumnNameEqual(EExtSourceType srcType, const char *lhs, const char *rhs);

/**
 * Whether a declared external timestamp type carries fractional seconds that
 * the ext connectors cannot transport: they convert every timestamp to a
 * microsecond epoch and drop the remainder.
 *
 * SQL Server DATETIME2/DATETIMEOFFSET accept 0..7 fractional digits, and the
 * omitted modifier defaults to 7, so such a column can hold values that differ
 * only below one microsecond.  Virtual table predicates still work (they are
 * approximated to microseconds), but a stream reader must walk the column with
 * an advancing microsecond watermark: two rows whose truncated values are
 * equal cannot be told apart, so the reader either re-reads the watermark row
 * forever or skips a new row written into the same microsecond.  Callers that
 * need a monotonic timestamp cursor must reject such a column.
 */
bool extTimestampTypeHasSubMicroFraction(EExtSourceType srcType, const char *extTypeName);

/**
 * Timezone class of an external timestamp type, as EExtTimestampTzClass
 * (EXT_TIMESTAMP_TZ_CLASS_AWARE / _NAIVE / _UNKNOWN).
 *
 * The connectors decode timezone-aware source columns (MySQL TIMESTAMP,
 * PostgreSQL timestamptz, SQL Server datetimeoffset) as absolute instants and
 * timezone-naive ones (MySQL DATETIME, PostgreSQL timestamp without time zone,
 * SQL Server datetime2/datetime/smalldatetime/date) as a wall clock in the
 * client timezone.  Consumers that render remote SQL bounds or convert the
 * textual value must use the same interpretation.
 *
 * This is the single source of truth for that classification: the planner
 * writes it into SFederatedScanPhysiNode.pTimestampTypeHints, the executor
 * derives it at runtime, and stream readers that only carry the remote type
 * name (e.g. the virtual-table original-table reader) recover it from the
 * column mapping.  EXT_TIMESTAMP_TZ_CLASS_UNKNOWN is returned for a NULL/empty
 * type name or a non-temporal type.
 */
int8_t extTimestampTypeTzClass(EExtSourceType srcType, const char *extTypeName);

#ifdef __cplusplus
}
#endif

#endif  // _EXT_TYPE_MAP_H_
