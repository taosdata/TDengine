/*
 * Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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

#ifndef INC_COMPAT_AVRO_H_
#define INC_COMPAT_AVRO_H_

#include <stdbool.h>

// Check if dbPath is a taosdump AVRO-format backup directory.
// Returns true if dbs.sql exists under dbPath.
bool isAvroBackupDir(const char *dbPath);

// Restore a complete taosdump AVRO backup (DDL + meta + data).
// dbPath: database backup root directory (containing dbs.sql and data*/ subdirs).
// Returns 0 on success, non-zero on failure.
int restoreAvroDatabase(const char *dbPath);

#endif  // INC_COMPAT_AVRO_H_
