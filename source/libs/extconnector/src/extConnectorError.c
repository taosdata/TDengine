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

// extConnectorError.c — error retryability helper (community + enterprise)
//
// Error-code mapping functions for MySQL, PG, and InfluxDB are implemented
// in the enterprise provider files (extConnectorMySQL.c, extConnectorPG.c,
// extConnectorInflux.cpp).  This file provides the shared retryability
// predicate and the EExtSQLDialect helper used by both editions.

#include "extConnector.h"
#include "taoserror.h"

// extConnectorIsRetryable — DS §5.3.9 retryability rules
//
// Connection-related and resource-exhaustion errors are transient and may be
// retried by the caller.  Authentication / access-denied errors are permanent
// and must NOT be retried (retrying leaks credentials and wastes resources).
bool extConnectorIsRetryable(int32_t errCode) {
  switch (errCode) {
    case TSDB_CODE_EXT_CONNECT_FAILED:
    case TSDB_CODE_EXT_QUERY_TIMEOUT:
    case TSDB_CODE_EXT_RESOURCE_EXHAUSTED:
      return true;
    default:
      return false;
  }
}
