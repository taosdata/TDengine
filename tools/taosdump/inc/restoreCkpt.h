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

#ifndef INC_RESTORECKPT_H_
#define INC_RESTORECKPT_H_

#include "bck.h"

// Load restore checkpoint for a database
void loadRestoreCheckpoint(const char *dbName);

// Check if a file has been restored
bool isRestoreDone(const char *filePath);

// Mark a file as restored (write to checkpoint)
void markRestoreDone(const char *filePath);

// Insert file path into hash table
void insertCkptHash(const char *filePath);

// Flush checkpoint buffer
void flushCkptBuffer(void);

// Free checkpoint resources
void freeRestoreCheckpoint(void);

// Delete checkpoint file after successful restore
void deleteRestoreCheckpoint(void);

#endif  // INC_RESTORECKPT_H_
