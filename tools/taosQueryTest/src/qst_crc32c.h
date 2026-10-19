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

/* Internal CRC32C helper shared by immutable shared-memory formats. */

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_CRC32C_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_CRC32C_H_

#include <stdint.h>

uint32_t qstCrc32c(uint32_t crc, const void *data, uint64_t length);

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_CRC32C_H_
