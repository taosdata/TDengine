/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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

/*
 * Local error code / error string implementation (replaces the four symbols the
 * wrapper used to import from util/terror.c).
 *
 * Semantics (agreed in review):
 *   - `taosGetErrno()` returns a *local* thread local variable.  Same semantics as
 *     the engine's, but no longer shared with it, so a failure code set by the
 *     wrapper does not show up in the engine's `taos_errno()` -- it only affects
 *     wrapper diagnostics.
 *   - `tstrerror()` does not carry a copy of the engine's 1133 entry table: the
 *     system error range (0x80ff0000|errno) goes to libc `strerror()`, everything
 *     else returns an empty string and the caller prints the numeric code (0x%X).
 *     The two codes the wrapper raises itself when the driver is missing or older
 *     than the wrapper are the exception -- see tstrerror() below.
 *   - The public `taos_errno()` / `taos_errstr()` still come from the driver and
 *     are unaffected.
 */

#define _DEFAULT_SOURCE

#include <string.h>

#include "wrapperLite.h"
#include "taoserror.h"

static TD_LITE_TLS int32_t tsLiteErrno = 0;

int32_t *taosGetErrno(void) { return &tsLiteErrno; }

const char *tstrerror(int32_t err) {
  /* system error range: 0x80ff0000 | errno (see TAOS_SYSTEM_ERROR in taoserror.h) */
  if ((err & 0xFFFF0000) == (int32_t)0x80FF0000) {
    return strerror(err & 0x0000FFFF);
  }

  /* The two codes the wrapper itself raises when the driver library cannot be
   * loaded or is older than the wrapper.  taos_errstr() comes from the driver, so
   * with no driver loaded these are the only codes a caller (JDBC, through
   * taos_errstr()) can get: an empty string would leave it with a bare number and
   * no explanation.  Wording is the engine's, so a code reads the same whichever
   * library answers. */
  switch (err) {
    case TSDB_CODE_DLL_NOT_LOAD:
      return "Driver was not loaded";
    case TSDB_CODE_DLL_FUNC_NOT_LOAD:
      return "Function was not loaded from the driver";
    default:
      break;
  }

  /* no message table: empty string, the caller prints the numeric code */
  return "";
}

const char *terrstr(void) { return tstrerror(tsLiteErrno); }
