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

/* Self-contained SHA-256 used by versioned on-disk and shared formats. */

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_SHA256_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_SHA256_H_

#include <stdint.h>

typedef struct {
  uint32_t state[8];
  uint64_t total_bytes;
  uint32_t buffered_bytes;
  uint8_t buffer[64];
} SQstSha256;

void qst_sha256_init(SQstSha256 *context);
void qst_sha256_update(SQstSha256 *context, const uint8_t *bytes,
                       uint64_t length);
void qst_sha256_final(SQstSha256 *context, uint8_t digest[32]);

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_SHA256_H_
