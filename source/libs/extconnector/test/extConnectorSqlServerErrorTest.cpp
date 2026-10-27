/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include <gtest/gtest.h>

extern "C" {
#include "extConnectorInt.h"
}

/* Defined in extConnectorSQLServer.c. */
extern "C" int32_t sqlserverClassifyError(const char *state, int32_t nativeErr, const char *msg);

/*
 * A missing or unloadable SQL Server driver must be reported as
 * "external source type not supported" on every platform:
 *   - unixODBC: IM002 (not registered) / 01000 "Can't open lib"
 *   - Windows:  IM002 (not registered) / IM003 (driver DLL cannot be loaded)
 */
TEST(ExtConnectorSqlServerErrorTest, MissingDriverIsReportedAsNotSupported) {
  EXPECT_EQ(sqlserverClassifyError("IM002", 0, ""), TSDB_CODE_EXT_SOURCE_TYPE_NOT_SUPPORT);
  EXPECT_EQ(sqlserverClassifyError("IM003", 0, ""), TSDB_CODE_EXT_SOURCE_TYPE_NOT_SUPPORT);
  EXPECT_EQ(sqlserverClassifyError("01000", 0, "Can't open lib 'libmsodbcsql-18.so': file not found"),
            TSDB_CODE_EXT_SOURCE_TYPE_NOT_SUPPORT);
}

TEST(ExtConnectorSqlServerErrorTest, RemoteErrorsKeepTheirClassification) {
  EXPECT_EQ(sqlserverClassifyError("08S01", 0, ""), TSDB_CODE_EXT_CONNECT_FAILED);
  EXPECT_EQ(sqlserverClassifyError("28000", 0, ""), TSDB_CODE_EXT_AUTH_FAILED);
  EXPECT_EQ(sqlserverClassifyError("42000", 18456, ""), TSDB_CODE_EXT_AUTH_FAILED);
  EXPECT_EQ(sqlserverClassifyError("42000", 208, ""), TSDB_CODE_EXT_TABLE_NOT_EXIST);
  EXPECT_EQ(sqlserverClassifyError("42000", 207, ""), TSDB_CODE_EXT_COLUMN_NOT_EXIST);
  EXPECT_EQ(sqlserverClassifyError("42000", 99999, ""), TSDB_CODE_EXT_REMOTE_INTERNAL);
}
