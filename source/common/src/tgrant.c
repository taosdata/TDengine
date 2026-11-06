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

#define _DEFAULT_SOURCE
#include <string.h>

#include "tgrant.h"

#ifndef _GRANT

int32_t grantCheck(EGrantType grant) { return TSDB_CODE_SUCCESS; }
int32_t grantCheckEx(EGrantType grant, void* param) { return TSDB_CODE_SUCCESS; }
int32_t grantCheckExpire(EGrantType grant) { return TSDB_CODE_SUCCESS; }

#else
#endif

/* Machine code format helpers, see the declaration in tgrant.h. Defined here so the
 * community build (which has no grant library) can still decode and test the format. */
int32_t taosMachineCodeSourceVariant(const char *machineId) {
  static const char b64[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

  if (machineId == NULL || machineId[0] == '\0' || machineId[1] == '\0') return -1;

  const char *hi = strchr(b64, machineId[0]);
  const char *lo = strchr(b64, machineId[1]);
  if (hi == NULL || lo == NULL) return -1;

  return (int32_t)(((uint8_t)(hi - b64) << 2) | ((uint8_t)(lo - b64) >> 4));
}

bool taosMachineCodeVariantMovedForward(const char *stored, const char *incoming) {
  int32_t storedVariant = taosMachineCodeSourceVariant(stored);
  int32_t incomingVariant = taosMachineCodeSourceVariant(incoming);
  if (storedVariant < 0 || incomingVariant < 0) return false;

  /*
   * Only the migrations actually shipped by a release are accepted.  Treating
   * "any larger variant" as an upgrade would let a forged heartbeat pick the
   * variant byte itself, rebind the dnode to an arbitrary machine code and then
   * make the genuine dnode look forged, so the list stays explicit.
   */
  static const struct {
    int32_t from;
    int32_t to;
  } kMigrations[] = {
      {1, 3},  // Windows: 3.4.1.0 -> 3.4.3.x moved the preferred source
  };

  for (size_t i = 0; i < sizeof(kMigrations) / sizeof(kMigrations[0]); ++i) {
    if (kMigrations[i].from == storedVariant && kMigrations[i].to == incomingVariant) return true;
  }
  return false;
}